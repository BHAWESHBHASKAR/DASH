# Deploy

DASH ships as four Linux binaries built from one workspace: `ingestion`, `retrieval`, `control-plane` and `segment-maintenance-daemon`. One container image (built from `deploy/container/Dockerfile` with a per-service `SERVICE` build argument) is produced per service, and an entrypoint script chooses which binary to run. Supported packaging in this repository: Docker Compose, systemd units, raw Kubernetes manifests and a Helm chart.

!!! warning "Status"
    Docker Compose is the path exercised most (a CI drill runs it). The raw Kubernetes manifests, the Helm chart and the systemd units were corrected in 0.3.0 (secret variable names, no default secrets, replication wiring, routing, state directories, sandboxing) and CI validates the artifacts (`helm lint`/`template`, `kustomize build`, `kubeconform`, `docker compose config`, a check of deploy variable names against the code), but no cluster deployment is exercised in CI. No release images have been published yet: build from source. Upgrading from 0.2.x: see the [upgrade guide](upgrading.md).

All deployments need credentials: every service refuses to start without them. See [Configuration](../reference/configuration.md) and the [authentication guide](auth.md).

## Docker Compose (recommended)

The stack is `deploy/container/docker-compose.yml`. It builds the images locally, starts ingestion (`:8081`), retrieval (`:8080`) and the segment-maintenance daemon, and gives each service its own named volume (`dash-ingestion-state`, `dash-retrieval-state`, `dash-control-plane-state`); the segment-maintenance daemon mounts the ingestion volume. The control plane (`:8090`) is optional: it starts only with `--profile control-plane`.

```bash
git clone https://github.com/BHAWESHBHASKAR/DASH.git
cd DASH
./scripts/generate-secrets.sh                      # writes deploy/container/.env (mode 0600, git-ignored)
docker compose -f deploy/container/docker-compose.yml up -d --build
# optional control plane:
docker compose -f deploy/container/docker-compose.yml --profile control-plane up -d --build
```

The compose file refuses to start if the required secrets are missing: it uses `${VAR:?message}` for `DASH_INGEST_API_KEY`, `DASH_INGEST_JWT_HS256_SECRET`, `DASH_INGEST_REPLICATION_TOKEN`, `DASH_RETRIEVAL_API_KEY`, `DASH_RETRIEVAL_JWT_HS256_SECRET` and `DASH_RETRIEVAL_REPLICATION_TOKEN` (and `DASH_CONTROL_PLANE_TOKEN` for the control-plane profile), and sets `DASH_STRICT_SECRETS=1`. `scripts/generate-secrets.sh` writes all of them, with the replication and control-plane tokens shared between the two sides.

Check health, then use the API with the generated keys (`x-api-key` header):

```bash
curl -fsS http://localhost:8081/health
curl -fsS http://localhost:8080/health
```

Notes on how the stack is wired:

- **Ingestion** owns the WAL at `/var/lib/dash/wal/ingestion.wal` and the redb file at `/var/lib/dash/state/ingestion.redb`, and writes an audit log under `/var/lib/dash/audit/`.
- **Retrieval** follows ingestion by polling `DASH_RETRIEVAL_REPLICATION_SOURCE_URL=http://ingestion:8081` every 250 ms with the replication token, and records its offset in `/var/lib/dash/state/retrieval-replication.offset`. Retrieval reads are therefore slightly behind writes.
- All services bind `0.0.0.0:<port>` inside the container; the compose file publishes the host ports on `127.0.0.1` by default. To expose the API on other interfaces set `DASH_PUBLISH_ADDR` (for example `0.0.0.0`) and put a reverse proxy with TLS in front; the services speak plain HTTP.
- Ports `8081` (ingestion), `8090` (control-plane) and the `/internal/replication/*` routes are not meant for the public internet.
- The image entrypoint selects the binary from `DASH_BIN` (`ingestion`, `retrieval`, `control-plane`, `segment-maintenance-daemon`; default `retrieval`) and defaults the argument to `--serve`. There is no `DASH_SERVICE` variable. The healthcheck uses the readiness route.
- The containers run as UID 10001 with `no-new-privileges`, all capabilities dropped, a read-only root filesystem and a small `noexec` tmpfs for `/tmp`.

A development overlay with hot-reload is `deploy/container/docker-compose.dev.yml`:

```bash
docker compose -f deploy/container/docker-compose.yml \
               -f deploy/container/docker-compose.dev.yml up
```

To stop and delete data: `docker compose -f deploy/container/docker-compose.yml down -v`.

## systemd

Unit files and example environment files are in `deploy/systemd/` (`dash-ingestion.service`, `dash-retrieval.service`, `dash-control-plane.service`, `dash-segment-maintenance.service`, and matching `*.env.example`). `scripts/deploy_systemd.sh --mode apply` installs them (`--mode plan` is the default and changes nothing): env files are written with mode 0640, every `REPLACE-ME` placeholder is replaced with a freshly generated random secret, the replication token is shared between `ingestion.env` and `retrieval.env`, and an existing env file is never overwritten. Each service has its own state directory under `/var/lib/dash/<service>` and the units are sandboxed (`ProtectSystem`, restricted write paths). Services refuse to start with placeholder or short secrets, so do not hand-edit a placeholder back in.

## Kubernetes (raw manifests)

`deploy/k8s/` holds a namespace, ConfigMap, retrieval and ingestion workloads, a control plane, ingress, network policy and PodDisruptionBudgets. **Secrets are not committed**: create the three Secrets (`dash-retrieval-secrets`, `dash-ingestion-secrets`, `dash-control-plane-secrets`) before applying; `deploy/k8s/11-secrets.yaml` documents the keys and has a `kubectl create secret` example (values of at least 32 random characters, the replication token identical in the retrieval and ingestion Secrets). Then apply with `kubectl apply -k deploy/k8s`. There is no HorizontalPodAutoscaler: retrieval pods own their PVCs and are scaled manually (`kubectl -n dash-system scale statefulset dash-retrieval --replicas=N`); ingestion and the control plane stay at one replica. The workloads set a read-only root filesystem, no privilege escalation and drop all capabilities, and the manifests use the variable names `DASH_INGEST_*` (not `DASH_INGESTION_*`). Health probes use `/v1/live` and `/v1/ready` (readiness is 503 if a configured redb file is unavailable or a follower is lagging); `/internal/*` and `/metrics` are not routed by the ingress.

## Helm

The chart is at `deploy/helm/dash/` (install from the checkout; no chart repository is published). The chart ships **no default secrets**: `helm install` fails until each is supplied, and values must be at least 32 characters and not look like placeholders.

```bash
helm install dash ./deploy/helm/dash \
  --namespace dash-system --create-namespace \
  --set secret.retrieval.apiKey="$(openssl rand -hex 32)" \
  --set secret.retrieval.hs256Secret="$(openssl rand -hex 32)" \
  --set secret.ingestion.apiKey="$(openssl rand -hex 32)" \
  --set secret.ingestion.hs256Secret="$(openssl rand -hex 32)" \
  --set secret.replicationToken="$(openssl rand -hex 32)" \
  --set secret.controlPlane.token="$(openssl rand -hex 32)"
```

`deploy/helm/dash/values.yaml` and its `README.md` are the source of truth for chart values, including `secret.existingSecret.*` for GitOps and `controlPlane.enabled=false` to skip the control plane. The chart deploys retrieval, ingestion and (optionally) control-plane as StatefulSets with per-pod PVCs and no autoscaler. Images are named `<registry>/<repository>-<service>:<tag>`, matching what the release workflow publishes. Control-plane leader election is file-lease based; the chart runs one control-plane replica.

## Operational limits to know about

- **Single writer.** Ingestion is one process per WAL. Retrieval replicas are read followers that poll; there is no consensus replication or automatic failover (planned, P3).
- **No encryption at rest.** Nothing DASH writes (WAL, redb, segments, audit log) is encrypted by DASH. Use an encrypted volume. `pkg/encryption` is a library that no service uses yet.
- **No TLS in the services.** Terminate TLS at a proxy or ingress. Replication and control-plane tokens travel over plain HTTP inside the cluster; keep those routes on a private network.
- **Backups.** See [Backup](backup.md) and `scripts/backup_state_bundle.sh`.
