# Deploy

DASH ships as four Linux binaries built from one workspace: `ingestion`, `retrieval`, `control-plane` and `segment-maintenance-daemon`. One container image (built from `deploy/container/Dockerfile`) contains all four, and an entrypoint script chooses which to run. Supported packaging in this repository: Docker Compose, systemd units, raw Kubernetes manifests and a Helm chart.

!!! warning "Status"
    Docker Compose is the path exercised most. The raw Kubernetes manifests and the Helm chart had defects in the 2026-10 review (register SEC-03 and SEC-04: wrong secret variable names and well-known default secrets; also SEC-20) and are being corrected in the v0.3.0 hardening work. Do not run them with real data until that release notes them as fixed. No release images have been published yet: build from source.

## Docker Compose (recommended)

The stack is `deploy/container/docker-compose.yml`. It builds the image locally, starts ingestion (`:8081`), retrieval (`:8080`), control-plane (`:8090`) and the segment-maintenance daemon, and shares one named volume (`dash-state`).

```bash
git clone https://github.com/BHAWESHBHASKAR/DASH.git
cd DASH
./scripts/generate-secrets.sh                      # writes deploy/container/.env (git-ignored)
docker compose -f deploy/container/docker-compose.yml up -d --build
```

The compose file refuses to start if the required secrets are missing: it uses `${VAR:?message}` for `DASH_INGEST_API_KEY`, `DASH_INGEST_JWT_HS256_SECRET`, `DASH_RETRIEVAL_API_KEY` and `DASH_RETRIEVAL_JWT_HS256_SECRET`, and sets `DASH_STRICT_SECRETS=1`. v0.3.0 additionally requires replication and control-plane tokens (`DASH_INGEST_REPLICATION_TOKEN`, `DASH_RETRIEVAL_REPLICATION_TOKEN`, `DASH_CONTROL_PLANE_TOKEN`); see [Configuration](../reference/configuration.md).

Check health, then use the API with the generated keys (`x-api-key` header):

```bash
curl -fsS http://localhost:8081/health
curl -fsS http://localhost:8080/health
```

Notes on how the stack is wired:

- **Ingestion** owns the WAL at `/var/lib/dash/wal/ingestion.wal` and the redb file at `/var/lib/dash/state/ingestion.redb`.
- **Retrieval** follows ingestion by polling `DASH_RETRIEVAL_REPLICATION_SOURCE_URL=http://ingestion:8081` every 250 ms and records its offset in `/var/lib/dash/state/retrieval-replication.offset`. Retrieval reads are therefore slightly behind writes.
- All services bind `0.0.0.0:<port>` inside the container and the compose file publishes the ports on the host. Put a firewall or reverse proxy with TLS in front; the services speak plain HTTP.
- Ports `8081` (ingestion), `8090` (control-plane) and the `/internal/replication/*` routes are not meant for the public internet.
- The image entrypoint selects the binary from `DASH_BIN` (`ingestion`, `retrieval`, `control-plane`, `segment-maintenance-daemon`; default `retrieval`) and defaults the argument to `--serve`. There is no `DASH_SERVICE` variable.
- The container runs as UID 10001 with `no-new-privileges` and all capabilities dropped except `CHOWN`, `SETUID`, `SETGID`, `DAC_OVERRIDE`. Reducing the capability set further is tracked as SEC-20.

A development overlay with hot-reload is `deploy/container/docker-compose.dev.yml`:

```bash
docker compose -f deploy/container/docker-compose.yml \
               -f deploy/container/docker-compose.dev.yml up
```

To stop and delete data: `docker compose -f deploy/container/docker-compose.yml down -v`.

## systemd

Unit files and example environment files are in `deploy/systemd/` (`dash-ingestion.service`, `dash-retrieval.service`, `dash-segment-maintenance.service`, and matching `*.env.example`). `scripts/deploy_systemd.sh` installs them. The example environment files contain placeholder secrets such as `change-me-ingest-key`; with strict secret validation those are rejected, so replace every placeholder with a generated value of at least 32 characters.

## Kubernetes (raw manifests)

`deploy/k8s/` holds a namespace, ConfigMap, Secrets, retrieval and ingestion workloads, ingress, network policy, PodDisruptionBudget and HPA. Apply with `kubectl apply -k deploy/k8s`. Before use: replace every secret in `11-secrets.yaml` and verify that the variable names match [Configuration](../reference/configuration.md) (`DASH_INGEST_*`, not `DASH_INGESTION_*`). Health probes: `GET /health` (liveness), `GET /ready` (readiness; 503 if a configured redb file is unavailable).

## Helm

The chart is at `deploy/helm/dash/` (install from the checkout; no chart repository is published):

```bash
helm install dash ./deploy/helm/dash \
  --namespace dash-system --create-namespace \
  --set secret.retrieval.apiKey="$(openssl rand -hex 32)" \
  --set secret.ingestion.apiKey="$(openssl rand -hex 32)"
```

`deploy/helm/dash/values.yaml` and its `README.md` are the source of truth for chart values. The chart deploys retrieval, ingestion and control-plane as StatefulSets. Control-plane leader election is file-lease based, so more than one control-plane replica requires a shared ReadWriteMany volume.

## Operational limits to know about

- **Single writer.** Ingestion is one process per WAL. Retrieval replicas are read followers that poll; there is no consensus replication or automatic failover (planned, P3).
- **No encryption at rest.** Nothing DASH writes (WAL, redb, segments, audit log) is encrypted by DASH. Use an encrypted volume. `pkg/encryption` is a library that no service uses yet.
- **No TLS in the services.** Terminate TLS at a proxy or ingress.
- **Backups.** See [Backup](backup.md) and `scripts/backup_state_bundle.sh`.
