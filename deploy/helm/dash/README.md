# DASH Helm Chart

Helm chart for the DASH evidence-first vector store. It deploys three
`StatefulSet` workloads with per-pod `ReadWriteOnce` PVCs:

| Workload | Replicas | Role |
|----------|----------|------|
| `ingestion` | 1 (fixed), or `failover.replicas` with `failover.enabled` | Single writer: owns the WAL and redb store, serves `/v1/ingest*` and the replication endpoint. With failover, the control plane names one pod leader; the others follow it and take over automatically (see "Leader failover"). |
| `retrieval` | `replicas.retrieval` (default 2) | Serves `/v1/retrieve` and `/v1/embeddings`. Every replica has its own PVC and follows the single ingestion pod over WAL replication. |
| `control-plane` | 1 (fixed, optional) | Shard placement metadata and file-backed leader lease. |

There is **no HorizontalPodAutoscaler**: replicas of a StatefulSet that each
own a PVC are not interchangeable until the clustering phase. Scale retrieval
manually (`kubectl -n <ns> scale statefulset dash-retrieval --replicas=N` for a
release named `dash`; the prefix is `<release>-dash` when the release name
does not contain `dash`);
a new replica starts empty and catches up from ingestion. Never scale
ingestion or the control plane above 1.

## Images

One image per service, named `<registry>/<repository>-<service>:<tag>`, for
example `ghcr.io/bhaweshbhaskar/dash-retrieval:0.2.0`. This is what
`.github/workflows/release.yml` publishes and what `deploy/k8s` pulls. Each
image's entrypoint selects its binary from `DASH_BIN`.

## Secrets are required (no defaults)

The chart ships **no** default secrets. `helm template` and `helm install`
fail until every secret is supplied or an existing Secret is referenced.
Values must be at least 32 characters and must not look like placeholders.

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

For GitOps, create the Secrets yourself (External Secrets, Sealed Secrets,
SOPS, `kubectl create secret`) and reference them. The chart then renders no
Secret for that component:

```yaml
secret:
  existingSecret:
    retrieval: my-dash-retrieval      # DASH_RETRIEVAL_API_KEY, DASH_RETRIEVAL_JWT_HS256_SECRET,
                                      # DASH_RETRIEVAL_REPLICATION_TOKEN
    ingestion: my-dash-ingestion      # DASH_INGEST_API_KEY, DASH_INGEST_JWT_HS256_SECRET,
                                      # DASH_INGEST_REPLICATION_TOKEN
    controlPlane: my-dash-cp          # DASH_CONTROL_PLANE_TOKEN
```

The replication token must be identical in the retrieval and ingestion
Secrets. When `config.embeddingProvider=openai`, both Secrets also need
`DASH_OPENAI_API_KEY` (`secret.openaiApiKey` for chart-managed Secrets).
Set `controlPlane.enabled=false` to skip the control plane and its token.

## Prerequisites

| Component     | Version | Why |
|---------------|---------|-----|
| Kubernetes    | >= 1.25 | `seccompProfile` defaults, restricted pod security |
| Helm          | >= 3.10 | chart features |
| cert-manager  | >= 1.13 | TLS for the ingress (optional) |
| nginx-ingress | >= 1.9  | annotations target ingress-nginx |

## Leader failover

`--set failover.enabled=true` runs `failover.replicas` ingestion pods. The
control plane names one leader (with a fenced, time-bounded lease) and
promotes the most up-to-date follower when the leader is lost; with
`failover.minSyncReplicas=1` (the default) no acknowledged write is lost when
any single node is. Ingestion pods use `GET /v1/ready/leader` as readiness, so
the ingestion Service, the ingress `/v1/ingest*` routes and the retrieval
followers always reach the current leader; follower pods are not ready by
design. A headless `<fullname>-ingestion-peers` Service gives every pod a
stable name for replication between ingestion pods. The StatefulSet uses
`updateStrategy: OnDelete`: roll followers first by deleting them one at a
time, then step the leader down (`POST /v1/control-plane/ingest/step-down`)
and delete it. Enabling failover on an existing release changes the
StatefulSet's `serviceName`: delete the StatefulSet with `--cascade=orphan`
first. Details and guarantees: `docs/operations/failover.md`.

## Data path

```
 clients ──TLS──▶ ingress ─┬─ /v1/ingest*  ─▶ Service dash-ingestion  ─▶ StatefulSet ingestion (1 replica, PVC)
                           └─ /v1, /health ─▶ Service dash-retrieval  ─▶ StatefulSet retrieval (N replicas, N PVCs)
                                                                              │ polls /internal/replication/*
                                                                              └──────────▶ ingestion (token auth)
```

`/internal/*` and `/metrics` are never routed by the ingress. Set
`ingress.exposeIngestion=false` to keep the write API cluster-internal.

## TLS inside the cluster

By default the pods speak plain HTTP to each other and the ConfigMap sets
`DASH_REPLICATION_ALLOW_INSECURE_HTTP=1` to acknowledge it. Set
`tls.enabled=true` and `tls.secretName=<secret>` to serve HTTPS from every
listener instead:

```bash
helm upgrade --install dash ./deploy/helm/dash -n dash-system \
  --set tls.enabled=true --set tls.secretName=dash-internal-tls ...
```

The Secret must hold `tls.crt`, `tls.key` and `ca.crt` (a cert-manager
`Certificate` secret has this shape; `deploy/k8s-tls/certificate.example.yaml`
is a template). The certificate must name the service DNS names and carry the
`server auth` and `client auth` usages. With `tls.mutual=true` (default)
ingestion verifies client certificates against `ca.crt` and requires one on
`/internal/replication/*`; retrieval follows `https://` and presents the pod
certificate. Probes switch to `HTTPS`, Services to `appProtocol: https`, and
the ingress gets `backend-protocol: HTTPS`. Renewed files are picked up
without a restart. See `docs/operations/tls.md`.

## Encryption at rest

Off by default. Put a 32-byte key (64 hex characters) in a Secret and enable
it; ingestion and retrieval mount the Secret read-only (mode `0400`) and
`DASH_ENCRYPTION_KEY_FILE` points at it:

```bash
openssl rand -hex 32 > dash.key
kubectl -n dash-system create secret generic dash-encryption-key --from-file=active.key=dash.key
helm upgrade --install dash ./deploy/helm/dash -n dash-system \
  --set encryption.enabled=true --set encryption.secretName=dash-encryption-key ...
```

`encryption.keyName` (default `active.key`) names the active key inside the
Secret; `encryption.previousKeyNames` lists retired keys that must still
decrypt during a rotation. Store a copy of every key outside the cluster:
PVC snapshots hold ciphertext only. See `docs/operations/encryption.md`.

## Persistence layout (per pod, mounted at `config.persistencePath`)

```
/var/lib/dash/
├── wal/ingestion.wal
├── state/{ingestion.redb,retrieval.redb,retrieval-replication.offset,control-plane.*}
├── segments/{ingestion,retrieval}/
└── audit/{ingestion,retrieval}.audit.jsonl    # config.audit.enabled
```

The root filesystem is read-only; `/tmp` is a memory-backed `emptyDir`.
`/opt/dash` is not mounted over, so the image's entrypoint is intact. An init
container creates the data directories on a fresh PVC.

## Configuration

`values.yaml` is the source of truth. High-impact keys:

| Key | Default | Description |
|-----|---------|-------------|
| `image.registry` / `image.repository` / `image.tag` | `ghcr.io` / `bhaweshbhaskar/dash` / `0.2.0` | Image is `<registry>/<repository>-<service>:<tag>` |
| `replicas.retrieval` | `2` | Retrieval replicas (manual scaling) |
| `controlPlane.enabled` | `true` | Deploy the control plane |
| `config.logLevel` | `info` | `RUST_LOG` filter |
| `config.logFormat` | `json` | `DASH_LOG_FORMAT` (`json` lines or `text`) |
| `config.strictSecrets` | `true` | `DASH_STRICT_SECRETS` |
| `config.embeddingProvider` | `hash` | `DASH_EMBEDDING_PROVIDER` (`hash`, `openai`, `ollama`) |
| `config.ollamaEndpoint` | in-cluster URL | `DASH_OLLAMA_ENDPOINT` (only when provider is `ollama`) |
| `config.persistencePath` | `/var/lib/dash` | PVC mount path |
| `config.audit.enabled` | `true` | Audit logs on the PVC |
| `persistence.size` / `persistence.storageClassName` | `10Gi` / cluster default | Per-pod volume |
| `probes.*` | `/v1/live` (liveness, startup), `/v1/ready` (readiness) | Probe paths. Startup must not use `/v1/ready`: on retrieval it also needs a reachable, caught-up leader |
| `namespace.name` | `""` (release namespace) | Override the namespace of every object |
| `ingress.*` | nginx, `dash.example.com` | `/v1/ingest` to ingestion, `/v1` and `/health` to retrieval |
| `networkPolicy.enabled` | `true` | Default deny plus allow rules |
| `networkPolicy.ollama.enabled` | `false` | Egress to in-cluster Ollama on private CIDRs (port 11434) |
| `pdb.enabled` | `true` | PodDisruptionBudget for retrieval (and for ingestion with failover: followers evictable, leader not) |
| `failover.enabled` | `false` | Automatic failover of the ingestion leader (needs `controlPlane.enabled`) |
| `failover.replicas` | `3` | Ingestion pods with failover |
| `failover.leaseMs` / `failover.promotionGraceMs` / `failover.heartbeatIntervalMs` | `5000` / `1000` / `1000` | Leader lease, promotion grace, heartbeat interval |
| `failover.minSyncReplicas` | `1` | Synchronous replication (0: asynchronous) |
| `failover.syncTimeoutMs` / `failover.syncOnTimeout` | `5000` / `fail` | Synchronous replication timeout and policy (`fail` or `degrade`) |
| `secret.*` | none | See "Secrets are required" |
| `metrics.public` | `false` | `DASH_METRICS_PUBLIC`: `/metrics` without credentials |
| `metrics.serviceMonitor.enabled` | `false` | One Prometheus Operator ServiceMonitor per component |
| `metrics.prometheusRule.enabled` | `false` | PrometheusRule with the DASH recording and alert rules |
| `metrics.grafanaDashboards.enabled` | `false` | ConfigMap with the Grafana dashboards (sidecar label `grafana_dashboard: "1"`) |

For an in-cluster Ollama, set `config.embeddingProvider=ollama`,
`config.ollamaEndpoint=http://<service>:11434` and
`networkPolicy.ollama.enabled=true`.

## Monitoring

With the Prometheus Operator installed:

```bash
helm upgrade --install dash ./deploy/helm/dash ... \
  --set metrics.serviceMonitor.enabled=true \
  --set metrics.serviceMonitor.labels.release=kube-prometheus-stack \
  --set metrics.serviceMonitor.authorization.retrieval.secretName=dash-scrape \
  --set metrics.serviceMonitor.authorization.retrieval.key=retrieval \
  --set metrics.serviceMonitor.authorization.ingestion.secretName=dash-scrape \
  --set metrics.serviceMonitor.authorization.ingestion.key=ingestion \
  --set metrics.prometheusRule.enabled=true \
  --set metrics.grafanaDashboards.enabled=true
```

`dash-scrape` holds credentials with the `read_only` role for retrieval and
ingestion (or set `metrics.public=true`); the control plane is scraped with
its own token. With `networkPolicy.enabled`, `metrics.networkPolicy.from`
(default: namespace `monitoring`) may reach the HTTP ports. The rules and
dashboards in `files/` are copies of `deploy/observability/` (CI fails when
they differ); set `metrics.prometheusRule.runbookBaseUrl` to turn the
relative runbook paths into links. Alerts, runbooks and SLOs:
`docs/operations/runbooks/` and `docs/operations/slos.md`.

## Validation

```bash
helm lint deploy/helm/dash --set ... # all secret values as above
helm template dash deploy/helm/dash --set ... | kubeconform -strict -summary
scripts/check_deploy_env.sh          # every DASH_* env var must be read by the code
```

CI (`.github/workflows/rust.yml`, job `deploy-manifests`) runs these with
generated secrets.

`scripts/kind_e2e.sh` installs the chart on a local kind cluster and runs
ingest, retrieve, delete, pod kills, backup/restore, `helm upgrade` (also from
an older chart), the raw manifests and the TLS variant; CI runs it in
`.github/workflows/kind-e2e.yml`. See `docs/operations/kubernetes.md`.

## Upgrade, rollback, uninstall

```bash
helm upgrade dash ./deploy/helm/dash --reuse-values
helm history dash -n dash-system
helm rollback dash <revision> -n dash-system
helm uninstall dash -n dash-system
```

A change to `config.*` or to chart-managed `secret.*` rolls every pod (the
pod templates carry `checksum/config` and `checksum/secrets` annotations).
Back up first (`scripts/k8s_backup_restore.sh`, see
`docs/operations/kubernetes.md`).

PVCs are owned by the StatefulSets and are **not** deleted on uninstall or
rollback. Back up before deleting them; to wipe state:

```bash
kubectl delete pvc -n dash-system -l app.kubernetes.io/part-of=dash
```

A StatefulSet's `volumeClaimTemplates` are immutable, so changing
`persistence.*` on an existing release requires recreating the StatefulSet
(`kubectl delete statefulset <name> --cascade=orphan`, then `helm upgrade`).

## Verifying an install

```bash
kubectl get pods,pvc,ingress -n dash-system
curl -fsS https://dash.example.com/health
curl -fsS -X POST https://dash.example.com/v1/retrieve \
  -H "authorization: Bearer $RETRIEVAL_API_KEY" -H 'content-type: application/json' \
  -d '{"tenant_id":"t1","query":"hello","top_k":5}'
```

## File layout

```
deploy/helm/dash/
├── Chart.yaml
├── README.md
├── values.yaml
└── templates/
    ├── _helpers.tpl
    ├── namespace.yaml
    ├── config.yaml
    ├── secrets.yaml
    ├── serviceaccount.yaml
    ├── retrieval.yaml
    ├── ingestion.yaml
    ├── controlplane.yaml
    ├── ingress.yaml
    ├── networkpolicy.yaml
    ├── monitoring.yaml
    └── pdb.yaml
└── files/                # copies of deploy/observability (rules, dashboards)
```
