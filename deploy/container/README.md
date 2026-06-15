# DASH Container Setup

Production-grade container packaging for the DASH retrieval engine.
Three services (`ingestion`, `retrieval`, `segment-maintenance`) ship in
a single multi-arch image and are orchestrated with `docker compose`.

## Layout

```
deploy/container/
├── Dockerfile                  # multi-stage, multi-arch build (builder / dev / runtime)
├── docker-compose.yml          # production stack: ingestion, retrieval, segment-maintenance
├── docker-compose.dev.yml      # local dev overlay (hot-reload via cargo-watch)
├── .dockerignore               # keeps the build context small
├── README.md                   # this file
└── scripts/
    ├── entrypoint.sh           # dispatcher: picks ingestion|retrieval|segment-maintenance-daemon
    └── healthcheck.sh          # HTTP probe against the running service's /health
```

The build context is the repository root (`../..`), so all paths in
`Dockerfile` (`pkg/`, `services/`, `tests/`, `tools/`, `Cargo.toml`,
`Cargo.lock`) are relative to the repo root, not to this directory.

## Image architecture

The `Dockerfile` exposes three build targets:

| target    | base                       | purpose                                                                                  |
|-----------|----------------------------|------------------------------------------------------------------------------------------|
| `builder` | `rust:1.83-bookworm`       | Compiles the full workspace in release mode and strips the three service binaries.      |
| `dev`     | `builder` + `cargo-watch`  | Extends `builder` with `cargo-watch` for local hot-reload. Not used in production.       |
| `runtime` | `debian:bookworm-slim`     | Minimal final image: three stripped binaries, dispatcher entrypoint, healthcheck, non-root `dash` user (UID 10001). |

Both base images are official multi-arch manifests, so
`docker buildx build --platform linux/amd64,linux/arm64 …` works out of
the box.

The container runs as a non-root user (`dash`, UID `10001`) with
`no-new-privileges`, all capabilities dropped, and only `CHOWN`,
`SETUID`, `SETGID`, and `DAC_OVERRIDE` re-added (required so the process
can chown data files inside the volume on first start).

## Quickstart

```bash
# from the repo root
docker compose -f deploy/container/docker-compose.yml build
docker compose -f deploy/container/docker-compose.yml up -d

# check status (wait for healthchecks to pass)
docker compose -f deploy/container/docker-compose.yml ps

# tail logs
docker compose -f deploy/container/docker-compose.yml logs -f

# smoke test
curl -s http://127.0.0.1:8080/health   # retrieval
curl -s http://127.0.0.1:8081/health   # ingestion
```

Stop the stack:

```bash
docker compose -f deploy/container/docker-compose.yml down
```

The named volume `dash-state` is preserved across `down` / `up` cycles.
Use `down --volumes` to wipe state.

## Environment variables

All variables are read by the Rust binaries themselves — the compose
file just passes them through. The matrix below is the complete set the
services honor in this deployment; see the binaries' source for the
full matrix (including ANN tuning, segment maintenance, and JWT knobs).

### Service selection

| Variable   | Default     | Description                                                                                |
|------------|-------------|--------------------------------------------------------------------------------------------|
| `DASH_BIN` | `retrieval` | Selects which binary the container's entrypoint dispatches to (`ingestion`, `retrieval`, or `segment-maintenance-daemon`). Set automatically by `docker-compose.yml`. |
| `DASH_HOME`| `/opt/dash` | Install root inside the container. Read by the entrypoint and healthcheck scripts.         |

### Bind / workers / runtime

| Variable                         | Default              | Service       |
|----------------------------------|----------------------|---------------|
| `DASH_INGEST_BIND`               | `127.0.0.1:8081`     | ingestion     |
| `DASH_RETRIEVAL_BIND`            | `127.0.0.1:8080`     | retrieval     |
| `DASH_INGEST_HTTP_WORKERS`       | `#cores` (clamped)   | ingestion     |
| `DASH_RETRIEVAL_HTTP_WORKERS`    | `#cores` (clamped)   | retrieval     |
| `DASH_INGEST_HTTP_QUEUE_CAPACITY`| `512`                | ingestion     |
| `DASH_INGEST_TRANSPORT_RUNTIME`  | `std`                | ingestion     |
| `DASH_RETRIEVAL_TRANSPORT_RUNTIME` | `std`              | retrieval     |

### Durability / WAL / checkpoints

| Variable                                | Default    | Description                                                                                       |
|-----------------------------------------|------------|---------------------------------------------------------------------------------------------------|
| `DASH_INGEST_WAL_PATH`                  | —          | Path to the durable WAL file. In compose this resolves to `/var/lib/dash/wal/ingestion.wal`.      |
| `DASH_RETRIEVAL_WAL_PATH`               | —          | Path to the durable WAL file. In compose this resolves to `/var/lib/dash/wal/retrieval.wal`.      |
| `DASH_INGEST_SEGMENT_DIR`               | —          | Where ingestion publishes immutable segments.                                                     |
| `DASH_RETRIEVAL_SEGMENT_DIR`            | —          | Where the retrieval service reads segments from.                                                  |
| `DASH_INGEST_SEGMENT_MAINTENANCE_INTERVAL_MS` | `30000` | How often the segment-maintenance daemon runs. `0` disables in-process maintenance on ingestion.  |
| `DASH_INGEST_SEGMENT_GC_MIN_STALE_AGE_MS`     | `60000`  | Minimum age (ms) before a stale segment is GC'd.                                                  |
| `DASH_CHECKPOINT_MAX_WAL_RECORDS`       | `50000`    | Force a checkpoint after this many WAL records.                                                   |
| `DASH_CHECKPOINT_MAX_WAL_BYTES`         | `52428800` | Force a checkpoint after this many WAL bytes.                                                     |
| `DASH_INGEST_WAL_SYNC_EVERY_RECORDS`    | `1`        | fsync cadence (records). Stricter is safer.                                                       |
| `DASH_INGEST_WAL_APPEND_BUFFER_RECORDS` | `1`        | WAL append buffer size (records).                                                                 |
| `DASH_INGEST_WAL_ASYNC_FLUSH_INTERVAL_MS` | `250`    | Background flush interval.                                                                        |
| `DASH_INGEST_WAL_BACKGROUND_FLUSH_ONLY`| `false`    | If true, request threads never block on flush.                                                    |
| `DASH_INGEST_WAL_SYNC_INTERVAL_MS`      | `1000`     | Caps the unsynced window when WAL batching is enabled.                                            |

### Security

| Variable                              | Default                       | Description                                                  |
|---------------------------------------|-------------------------------|--------------------------------------------------------------|
| `DASH_INGEST_API_KEY`                 | `change-me-ingest-key`        | Placeholder. **Replace before any non-dev deployment.**      |
| `DASH_RETRIEVAL_API_KEY`              | `change-me-retrieval-key`     | Placeholder. **Replace before any non-dev deployment.**      |
| `DASH_INGEST_JWT_HS256_SECRET`        | `change-me-ingest-jwt`        | Placeholder HS256 secret for ingestion JWT verification.     |
| `DASH_RETRIEVAL_JWT_HS256_SECRET`     | `change-me-retrieval-jwt`     | Placeholder HS256 secret for retrieval JWT verification.      |

To inject real secrets, set them in a `.env` file in this directory or
in your shell environment before running `docker compose up`. Example
`.env`:

```dotenv
DASH_INGEST_API_KEY=actual-secret
DASH_INGEST_JWT_HS256_SECRET=actual-jwt
DASH_RETRIEVAL_API_KEY=actual-secret
DASH_RETRIEVAL_JWT_HS256_SECRET=actual-jwt
RUST_LOG=info,dash=info
```

### Logging

| Variable   | Default              | Description                                                                                  |
|------------|----------------------|----------------------------------------------------------------------------------------------|
| `RUST_LOG` | `info,dash=debug`    | Standard `tracing` env-filter syntax. Override per environment (e.g. `info` for prod, `debug` for staging). |

### Healthcheck

| Variable                  | Default                            | Description                                                       |
|---------------------------|------------------------------------|-------------------------------------------------------------------|
| `DASH_HEALTHCHECK_URL`    | `http://127.0.0.1:8080/health`     | The HTTP endpoint the container's `HEALTHCHECK` will probe. Set per service in compose. |

## Where data lives

All persistent data lives under `/var/lib/dash` inside the container,
which is backed by the named volume `dash-state`:

```
/var/lib/dash/
├── wal/
│   ├── ingestion.wal              # ingestion service WAL (and its snapshot)
│   └── retrieval.wal              # retrieval service WAL (and its snapshot)
├── segments/
│   ├── ingestion/                 # published segments (tenants live below)
│   └── retrieval/                 # read-only segment cache (optional)
└── state/                         # free-form state dir for the daemon
```

To inspect the volume from the host:

```bash
docker volume inspect dash-state
docker run --rm -v dash-state:/data alpine ls -la /data
```

## Backing up the WAL volume

The two `*.wal` files (and their `*.wal.snapshot` siblings) inside
`/var/lib/dash/wal/` are the source of truth for service state. Back
them up while the services are running — both WALs are append-only
under steady state and use copy-on-write snapshots for checkpoints.

```bash
# quick tarball backup of the entire volume
docker run --rm \
    -v dash-state:/data:ro \
    -v $(pwd):/backup \
    alpine sh -c "tar czf /backup/dash-state-$(date +%F).tar.gz /data"
```

For application-aware backups, prefer the existing
`scripts/backup_state_bundle.sh` (it understands the WAL + snapshot
contract and writes a consistent bundle to a path you choose):

```bash
docker compose -f deploy/container/docker-compose.yml exec ingestion \
    /opt/dash/bin/ingestion  # or run scripts/backup_state_bundle.sh from the host
```

Restore with `scripts/restore_state_bundle.sh` after stopping the
stack and replacing `/var/lib/dash/wal/`.

## Upgrading

Pull new base images, rebuild, and restart in dependency order:

```bash
docker compose -f deploy/container/docker-compose.yml pull       # base images
docker compose -f deploy/container/docker-compose.yml build      # rebuild app image
docker compose -f deploy/container/docker-compose.yml up -d      # rolling restart
```

Because both the WAL and the segment directory live on the `dash-state`
named volume, no data is lost across upgrades. To roll back, point the
services at the previous image tag and re-run `up -d`:

```bash
docker tag dash/retrieval:latest dash/retrieval:previous
docker compose -f deploy/container/docker-compose.yml up -d \
    --scale retrieval=0    # briefly stop, then:
docker compose -f deploy/container/docker-compose.yml up -d
```

## Local development

For hot-reload, source mounts, and debug logging, use the dev overlay:

```bash
docker compose \
    -f deploy/container/docker-compose.yml \
    -f deploy/container/docker-compose.dev.yml \
    up
```

The dev overlay:

- builds the `dev` target of the Dockerfile (full rust toolchain + `cargo-watch`),
- mounts the repo at `/build` so source edits are picked up live,
- puts cargo's registry + target directories on named volumes
  (`dash-dev-cargo`, `dash-dev-target`) so incremental builds persist
  across restarts,
- uses isolated state volumes (`dash-dev-state`) so dev work never
  touches the production `dash-state` volume,
- defaults `RUST_LOG` to `debug`,
- runs as your host UID/GID via `DASH_DEV_UID` / `DASH_DEV_GID`
  (defaults to `1000:1000`).

If your host user is not UID 1000, set them explicitly:

```bash
DASH_DEV_UID=$(id -u) DASH_DEV_GID=$(id -g) \
    docker compose \
        -f deploy/container/docker-compose.yml \
        -f deploy/container/docker-compose.dev.yml \
        up
```

The dev containers do not run `cargo build` once at startup — instead
`cargo watch` compiles and launches each binary on demand and rebuilds
whenever a watched file changes.

## Healthchecks

Every service has a Docker `HEALTHCHECK`. The probe is a tiny
`wget --spider` against the service's HTTP `/health` endpoint (the
services expose this in both `services/ingestion/src/transport/read_routes.rs`
and `services/retrieval/src/transport.rs`). The probe URL is set per
service via `DASH_HEALTHCHECK_URL`, so a wrong-port healthcheck is
impossible by construction.

The `segment-maintenance` daemon has no HTTP endpoint, so its
healthcheck is process-presence (`pidof segment-maintenance-daemon`).
For tighter liveness checks, run the daemon with `--once` from a cron-
style orchestrator and alert on exit code.

`depends_on` in compose uses `condition: service_healthy` so dependent
services do not start until their upstream is healthy, avoiding the
cold-start race where a client hits retrieval before ingestion has
replayed its WAL.
