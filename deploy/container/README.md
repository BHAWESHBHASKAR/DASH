# DASH Container Setup

Container packaging for DASH. Four binaries (`ingestion`, `retrieval`,
`control-plane`, `segment-maintenance-daemon`) are built from one
Dockerfile. One image is produced per service and the `SERVICE` build
argument selects the default binary (`DASH_BIN`). The stack runs with
`docker compose`.

## Layout

```
deploy/container/
├── Dockerfile                  # multi-stage build (builder / dev / runtime)
├── docker-compose.yml          # ingestion, retrieval, segment-maintenance (+ control-plane profile)
├── docker-compose.dev.yml      # local dev overlay (hot-reload via cargo-watch)
├── .env.example                # secrets template; copying it as-is fails startup on purpose
├── .dockerignore
├── README.md
└── scripts/
    ├── entrypoint.sh           # dispatcher: DASH_BIN selects the binary
    └── healthcheck.sh          # readiness probe for the selected service
```

The build context is the repository root, so Dockerfile paths are
relative to it.

## Image naming

All deploy targets use one scheme: `ghcr.io/<owner>/dash-<service>:<tag>`
with `<service>` one of `ingestion`, `retrieval`, `control-plane`. The
release workflow builds exactly these images; Helm and the raw Kubernetes
manifests pull them. Locally, compose builds `dash-<service>:local`.

```bash
docker build -f deploy/container/Dockerfile --build-arg SERVICE=ingestion -t dash-ingestion:local .
```

## Image hardening

- Runs as non-root `dash` (UID 10001); binaries and scripts are owned by
  root and are not writable by the runtime user.
- Compose drops all capabilities, sets `no-new-privileges`, uses a
  read-only root filesystem with a `tmpfs` at `/tmp`, and persists state
  only on per-service named volumes at `/var/lib/dash` (`dash-ingestion-state`,
  `dash-retrieval-state`, `dash-control-plane-state`), so one compromised
  container cannot rewrite another service's WAL, audit log or lease.
- `HEALTHCHECK` runs `dash-healthcheck`, which probes `/v1/ready`
  (`/v1/control-plane/ready` for the control plane) on the service's port.
- Published ports bind to `127.0.0.1`. Set `DASH_PUBLISH_ADDR=0.0.0.0` only
  behind a TLS-terminating proxy.

## Quickstart

```bash
# from the repo root
scripts/generate-secrets.sh        # writes deploy/container/.env (mode 0600)
docker compose -f deploy/container/docker-compose.yml up -d --build

docker compose -f deploy/container/docker-compose.yml ps
```

The compose file requires every secret; it refuses to start without them.
If you copy `.env.example` instead of running the script, replace every
`REPLACE-ME` value; the services reject placeholders and secrets shorter
than 32 characters.

Smoke test (keys are in `deploy/container/.env`):

```bash
set -a; source deploy/container/.env; set +a
curl -fsS http://127.0.0.1:8081/v1/ready
curl -fsS http://127.0.0.1:8080/v1/ready

curl -fsS -X POST http://127.0.0.1:8081/v1/ingest \
  -H "authorization: Bearer ${DASH_INGEST_API_KEY}" \
  -H 'content-type: application/json' \
  -d '{"claim":{"claim_id":"c1","tenant_id":"t1","canonical_text":"The capital of France is Paris.","confidence":0.95},"evidence":[{"evidence_id":"e1","claim_id":"c1","source_id":"s1","stance":"supports","source_quality":0.9}]}'

curl -fsS -X POST http://127.0.0.1:8080/v1/retrieve \
  -H "authorization: Bearer ${DASH_RETRIEVAL_API_KEY}" \
  -H 'content-type: application/json' \
  -d '{"tenant_id":"t1","query":"capital of France","top_k":5}'
```

Optional control plane (published on `127.0.0.1:8090` only):

```bash
docker compose -f deploy/container/docker-compose.yml --profile control-plane up -d
```

Stop the stack with `docker compose -f deploy/container/docker-compose.yml down`.
The state volumes survive `down`; use `down --volumes` to wipe them.

## Data path

Ingestion is the only writer. Retrieval follows it over HTTP
(`DASH_RETRIEVAL_REPLICATION_SOURCE_URL=http://ingestion:8081`) using a
shared bearer token (`DASH_INGEST_REPLICATION_TOKEN` on ingestion,
`DASH_RETRIEVAL_REPLICATION_TOKEN` on retrieval; `generate-secrets.sh`
writes the same value to both). Retrieval keeps its own redb store.

### TLS

`docker-compose.tls.yml` turns on native TLS for every service and mutual TLS
for replication:

```bash
scripts/generate-dev-tls.sh            # dev CA + certificate in deploy/container/tls
docker compose -f deploy/container/docker-compose.yml \
               -f deploy/container/docker-compose.tls.yml up -d --build
curl --cacert deploy/container/tls/ca.crt https://127.0.0.1:8080/v1/ready
```

The overlay mounts `deploy/container/tls` read-only at `/etc/dash/tls`,
switches the retrieval source to `https://ingestion:8081`, withdraws
`DASH_REPLICATION_ALLOW_INSECURE_HTTP`, and points the healthcheck at the CA.
Use certificates from your own PKI (same file names) outside development. See
`docs/operations/tls.md`.

## Environment variables

All variables are read by the Rust binaries (or by the container scripts in
`scripts/`); compose only passes them through. CI runs
`scripts/check_deploy_env.sh`, which fails when a deploy artifact names a
variable the code does not read.

| Variable | Purpose |
|----------|---------|
| `DASH_BIN` | Selects the binary: `ingestion`, `retrieval`, `control-plane` or `segment-maintenance-daemon`. Defaults from the `SERVICE` build arg. |
| `DASH_HOME` | Install root (`/opt/dash`), read by the scripts. |
| `DASH_HEALTHCHECK_URL` | Overrides the URL `dash-healthcheck` probes. |
| `DASH_INGEST_BIND`, `DASH_RETRIEVAL_BIND`, `DASH_CONTROL_PLANE_BIND` | Listen addresses inside the container. |
| `DASH_INGEST_HTTP_WORKERS`, `DASH_INGEST_HTTP_QUEUE_CAPACITY`, `DASH_RETRIEVAL_HTTP_WORKERS`, `DASH_RETRIEVAL_HTTP_QUEUE_CAPACITY` | HTTP worker pools. |
| `DASH_INGEST_WAL_PATH` | Ingestion WAL file (`/var/lib/dash/wal/ingestion.wal`). |
| `DASH_INGEST_PERSISTENCE_PATH`, `DASH_RETRIEVAL_PERSISTENCE_PATH` | redb FILE paths under `/var/lib/dash/state/`. |
| `DASH_INGEST_SEGMENT_DIR`, `DASH_RETRIEVAL_SEGMENT_DIR` | Segment directories. |
| `DASH_INGEST_AUDIT_LOG_PATH`, `DASH_RETRIEVAL_AUDIT_LOG_PATH` | Audit logs on the state volume (`/var/lib/dash/audit/`). |
| `DASH_INGEST_WAL_SYNC_EVERY_RECORDS`, `DASH_INGEST_WAL_APPEND_BUFFER_RECORDS`, `DASH_INGEST_WAL_ASYNC_FLUSH_INTERVAL_MS`, `DASH_INGEST_WAL_BACKGROUND_FLUSH_ONLY`, `DASH_INGEST_WAL_SYNC_INTERVAL_MS` | WAL durability knobs. |
| `DASH_CHECKPOINT_MAX_WAL_RECORDS`, `DASH_CHECKPOINT_MAX_WAL_BYTES` | Checkpoint thresholds. |
| `DASH_INGEST_SEGMENT_MAINTENANCE_INTERVAL_MS`, `DASH_INGEST_SEGMENT_GC_MIN_STALE_AGE_MS` | Segment maintenance. |
| `DASH_RETRIEVAL_REPLICATION_SOURCE_URL`, `DASH_RETRIEVAL_REPLICATION_POLL_INTERVAL_MS`, `DASH_RETRIEVAL_REPLICATION_OFFSET_PATH` | Retrieval follower. |
| `DASH_EMBEDDING_PROVIDER`, `DASH_OLLAMA_ENDPOINT` | Embedding provider (`hash`, `openai`, `ollama`). |
| `DASH_STRICT_SECRETS` | Reject placeholder and short secrets (on). |
| `RUST_LOG` | `tracing` env-filter syntax. |

Secrets (generated into `.env`):

| Variable | Used by |
|----------|---------|
| `DASH_INGEST_API_KEY`, `DASH_INGEST_JWT_HS256_SECRET` | ingestion auth |
| `DASH_RETRIEVAL_API_KEY`, `DASH_RETRIEVAL_JWT_HS256_SECRET` | retrieval auth |
| `DASH_INGEST_REPLICATION_TOKEN`, `DASH_RETRIEVAL_REPLICATION_TOKEN` | WAL replication (same value) |
| `DASH_CONTROL_PLANE_TOKEN` | control plane API |
| `DASH_ROUTER_CONTROL_PLANE_TOKEN` | router client for the control plane (same value as above) |

## Where data lives

```
/var/lib/dash/                      # volume dash-ingestion-state
├── wal/ingestion.wal               # ingestion WAL
├── state/ingestion.redb
├── segments/ingestion/             # also mounted by segment-maintenance
└── audit/ingestion.audit.jsonl

/var/lib/dash/                      # volume dash-retrieval-state
├── state/retrieval.redb
├── state/retrieval-replication.offset
├── segments/retrieval/
└── audit/retrieval.audit.jsonl

/var/lib/dash/                      # volume dash-control-plane-state
└── state/control-plane.*           # control plane state, lease
```

Retrieval follows ingestion over HTTP and never reads its files; the control
plane shares nothing with the data plane. The segment-maintenance daemon
belongs to the ingestion trust domain and mounts the ingestion volume.

### Upgrading from the single `dash-state` volume

Earlier versions of this file used one shared `dash-state` volume. Compose
now creates three empty volumes, so copy the old data once before the first
`up` (replace the paths as needed), for example:

```bash
migrate() {  # migrate <service> <path under /var/lib/dash>...
  local svc="$1"; shift
  docker volume create "dash-${svc}-state" >/dev/null
  docker run --rm -v dash-state:/old:ro -v "dash-${svc}-state:/new" alpine \
    sh -c 'cd /old; for p in "$@"; do [ -e "$p" ] && cp -a --parents "$p" /new/; done
           chown 10001:10001 /new; chmod 0750 /new' sh "$@"
}
migrate ingestion wal state/ingestion.redb segments/ingestion audit/ingestion.audit.jsonl
migrate retrieval state/retrieval.redb state/retrieval-replication.offset \
  segments/retrieval audit/retrieval.audit.jsonl
migrate control-plane state/control-plane.csv state/control-plane.csv.sha256 \
  state/control-plane.lease
```

Retrieval can also start empty and re-sync from ingestion.

## Backup and restore

`scripts/backup_state_bundle.sh` and `scripts/restore_state_bundle.sh`
understand the WAL and snapshot contract. `scripts/backup_restore_drill.sh`
runs the full cycle against compose (it generates secrets first). Because
the root filesystem is read-only, restore by mounting the volume in a
one-off container (see the drill) rather than `docker cp` into a service.

## Upgrading

```bash
docker compose -f deploy/container/docker-compose.yml pull        # if using released images
docker compose -f deploy/container/docker-compose.yml up -d --build
```

State lives on the per-service volumes, so it survives upgrades.

## Local development

```bash
scripts/generate-secrets.sh
docker compose \
    -f deploy/container/docker-compose.yml \
    -f deploy/container/docker-compose.dev.yml \
    up
```

The overlay builds the `dev` target (toolchain plus `cargo-watch`), mounts
the repo at `/build`, keeps cargo caches and state on separate named
volumes, and runs as `DASH_DEV_UID:DASH_DEV_GID` (default `1000:1000`). It
reuses the secrets from `.env`; no weak dev secrets are defined.

## Healthchecks

Every service has a Docker `HEALTHCHECK`. `dash-healthcheck` probes
`/v1/ready` (`/v1/control-plane/ready` for the control plane), over
`https://` when the service's `DASH_*_TLS_CERT_FILE` is set (verified
against `DASH_HEALTHCHECK_CA_FILE` when given). The
`segment-maintenance` daemon has no HTTP endpoint, so its check is
process presence. `depends_on` uses `condition: service_healthy` so
retrieval starts only after ingestion is ready.
