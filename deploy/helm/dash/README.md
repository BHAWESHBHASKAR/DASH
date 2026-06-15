# DASH Helm Chart

Production-grade Helm chart for the DASH vector store.

DASH exposes an OpenAI-compatible `/v1/embeddings` surface and a
parallel ingestion API. The chart deploys two workloads backed by
redb persistence: `retrieval` (serves `/v1/*`) and `ingestion`
(writes only). Each workload is a `StatefulSet` with per-pod
`ReadWriteOnce` PVCs so the redb store survives pod restarts and
re-scheduling.

## TL;DR

```bash
helm install dash ./deploy/helm/dash \
  --namespace dash-system \
  --create-namespace \
  --set secret.openaiApiKey="$OPENAI_API_KEY" \
  --set secret.retrieval.apiKey="$(openssl rand -hex 32)" \
  --set secret.ingestion.apiKey="$(openssl rand -hex 32)"
```

The install will:

1. Create the `dash-system` namespace (`--create-namespace`).
2. Render a `ConfigMap` and two per-service `Secret`s.
3. Create two `StatefulSet`s, each with a headless identity service
   and a `ClusterIP` fronting service.
4. Attach a `PodDisruptionBudget`, `HorizontalPodAutoscaler`, and
   `NetworkPolicy` set to each workload.
5. Mint a `cert-manager` certificate and wire up the
   `nginx`-class `Ingress` for `/v1/*` (and `/health`) on
   `dash.example.com` (override with `--set ingress.hosts[0].host=...`).

## Prerequisites

| Component    | Version     | Why                                |
|--------------|-------------|------------------------------------|
| Kubernetes   | >= 1.25     | HPA v2, `seccompProfile` defaults  |
| Helm         | >= 3.10     | API v2 chart features              |
| cert-manager | >= 1.13     | TLS for the ingress                |
| nginx-ingress| >= 1.9      | Annotations target nginx-ingress   |
| Metrics      | metrics-server | HPA needs CPU utilization       |

## Configuration

The full set of values lives in `values.yaml`. The table below
summarizes the high-impact knobs; the file is the source of truth.

| Key                                  | Default                                     | Description |
|--------------------------------------|---------------------------------------------|-------------|
| `image.registry`                     | `ghcr.io`                                   | Container registry |
| `image.repository`                   | `bhaweshbhaskar/dash`                       | Base image repo (per-service suffix is appended) |
| `image.tag`                          | `0.2.0`                                     | Image tag; pin in lockstep with `Chart.appVersion` |
| `image.pullPolicy`                   | `IfNotPresent`                              | K8s image pull policy |
| `image.pullSecrets`                  | `[]`                                        | List of `imagePullSecret` names |
| `replicas.retrieval`                 | `2`                                         | Initial retrieval replica count (HPA lower bound) |
| `replicas.ingestion`                 | `2`                                         | Initial ingestion replica count (HPA lower bound) |
| `service.type`                       | `ClusterIP`                                 | K8s service type |
| `service.ports.http`                 | `80`                                        | Fronting port; keep in sync with `ingress` |
| `ingress.enabled`                    | `true`                                      | Render the ingress |
| `ingress.className`                  | `nginx`                                     | IngressClass |
| `ingress.hosts[0].host`              | `dash.example.com`                          | Public hostname |
| `ingress.tls[0].secretName`          | `dash-retrieval-tls`                        | cert-manager managed TLS secret |
| `persistence.enabled`                | `true`                                      | Render per-pod PVCs |
| `persistence.size`                   | `10Gi`                                      | PVC size (RWO) |
| `persistence.storageClassName`       | `""`                                        | Empty = cluster default |
| `resources.requests.cpu`             | `250m`                                      | CPU request per pod |
| `resources.requests.memory`          | `256Mi`                                     | Memory request per pod |
| `resources.limits.cpu`               | `1000m`                                     | CPU limit per pod |
| `resources.limits.memory`            | `512Mi`                                     | Memory limit per pod |
| `hpa.enabled`                        | `true`                                      | Render HPAs |
| `hpa.minReplicas`                    | `2`                                         | HPA lower bound |
| `hpa.maxReplicas`                    | `10`                                        | HPA upper bound |
| `pdb.enabled`                        | `true`                                      | Render PDBs |
| `pdb.minAvailable`                   | `1`                                         | Always-alive floor |
| `networkPolicy.enabled`              | `true`                                      | Render default-deny + allow rules |
| `serviceAccount.create`              | `true`                                      | Create per-service SAs |
| `namespace.create`                   | `false`                                     | Render the `Namespace` resource |
| `namespace.name`                     | `dash-system`                               | Target namespace |
| `config.logLevel`                    | `info`                                      | `DASH_LOG_LEVEL` |
| `config.embeddingProvider`           | `hash`                                      | `DASH_EMBEDDING_PROVIDER` (hash/openai/ollama) |
| `config.persistencePath`             | `/var/lib/dash`                             | Mount path for the redb volume |
| `secret.openaiApiKey`                | `null`                                      | `DASH_OPENAI_API_KEY` (required when provider=openai) |
| `secret.jwtPublicKey`                | `null`                                      | PEM-encoded RS256/ES256 verification key |
| `secret.retrieval.apiKey`            | `null`                                      | Static API key for retrieval |
| `secret.ingestion.apiKey`            | `null`                                      | Static API key for ingestion |

## Install

The minimal install requires at least a JWT public key (RS256) or
HS256 shared secret. Generate one with:

```bash
# RSA keypair (preferred)
openssl genrsa -out jwt.pem 2048
openssl rsa -in jwt.pem -pubout -out jwt.pub

# Or just use a long random string for HS256
openssl rand -hex 32
```

Then install:

```bash
helm install dash ./deploy/helm/dash \
  --namespace dash-system \
  --create-namespace \
  --set-file secret.jwtPublicKey=jwt.pub
```

### Production install with sealed secrets

The chart splits the secret material across two `Secret`s
(`<release>-retrieval-secrets`, `<release>-ingestion-secrets`).
For production, encrypt them with [Sealed Secrets] and commit the
result:

```bash
kubectl create secret generic dash-retrieval-secrets \
  --namespace dash-system --dry-run=client -o yaml \
  --from-file=DASH_RETRIEVAL_JWT_PUBLIC_KEY=jwt.pub \
  --from-literal=DASH_RETRIEVAL_API_KEY="$RETRIEVAL_KEY" \
  --from-literal=DASH_OPENAI_API_KEY="$OPENAI_KEY" \
  | kubeseal -o yaml > deploy/overlays/prod/retrieval-secrets.yaml
```

Reference the sealed secret in a Kustomize patch and let it
override the chart-rendered Secret. The chart's `secret.create`
value can be flipped to `false` to drop the placeholder:

```bash
helm upgrade dash ./deploy/helm/dash \
  --reuse-values \
  --set secret.create=false
```

[Sealed Secrets]: https://github.com/bitnami-labs/sealed-secrets

## Upgrade

The chart follows standard SemVer:

- **Patch bumps** (`0.1.x`): safe to `helm upgrade` with `--reuse-values`.
  No resource spec changes; image tags and labels may shift.
- **Minor bumps** (`0.x.0`): review the *Upgrade Notes* below.
  Templates are backwards compatible; existing PVCs and Secrets
  are preserved.
- **Major bumps** (`x.0.0`): expect breaking changes. Read the
  `CHANGELOG.md` and pin a previous chart version explicitly:
  `helm install dash ./deploy/helm/dash --version 0.1.0 ...`.

### Schema change upgrade guide

When a chart release changes the set of `ConfigMap` keys, the
existing ConfigMap is updated in place (Helm applies the new
manifest directly). The new keys are picked up on the next pod
restart; deleted keys are ignored by the running pods but will
appear as "stale" in `kubectl describe configmap`.

For **secret schema** changes, the chart recreates the Secret only
if its data hash changes. If you change the secret shape manually,
delete the Secret first to avoid a stuck upgrade:

```bash
kubectl delete secret dash-retrieval-secrets -n dash-system
helm upgrade dash ./deploy/helm/dash --reuse-values
```

For **PVC schema** changes (e.g. switching from `standard` to
`ssd-csi` storage class), the StatefulSet will not recreate the
existing PVCs. Use one of:

1. `kubectl edit pvc` to retag in place (works for `storageClassName`
   only when the PV's `storageClass` is mutable).
2. Drain the workload, delete the PVC, and let the StatefulSet
   recreate it. **This wipes redb state - back up first**.

```bash
kubectl rollout pause statefulset/dash-retrieval -n dash-system
kubectl delete pvc data-dash-retrieval-0 data-dash-retrieval-1 -n dash-system
kubectl rollout resume statefulset/dash-retrieval -n dash-system
```

For **StatefulSet spec changes** (replicas, affinity, probes) the
rolling update is automatic. Force a fresh rollout when only
annotations change:

```bash
kubectl rollout restart statefulset/dash-retrieval -n dash-system
```

## Rollback

Helm keeps the last 10 releases. Roll back the workload to the
previous revision:

```bash
helm history dash -n dash-system
helm rollback dash 1 --namespace dash-system
```

`helm rollback` re-renders the previous templates and applies
them. **PVCs are not rolled back** - they persist across revisions
because their lifecycle is independent of the chart. If the
rollback introduces a breaking storage format, you must:

1. `helm rollback` the chart first.
2. Drain and delete the PVCs.
3. Restore the redb files from a backup (snapshot the PVC contents
   with `kubectl cp` or your storage provider's snapshot API).

To roll back **only** the image tag (most common case):

```bash
helm upgrade dash ./deploy/helm/dash \
  --reuse-values \
  --set image.tag=0.1.7
```

## Uninstall

```bash
helm uninstall dash --namespace dash-system
kubectl delete namespace dash-system
```

> **Warning:** Helm uninstall does not delete PVCs (they are
> owned by the StatefulSet, not the chart). To wipe redb state:
>
> ```bash
> kubectl delete pvc -n dash-system \
>   -l app.kubernetes.io/part-of=dash
> ```

## Architecture quick reference

```
   internet
      │
      ▼
 ┌──────────────┐    TLS    ┌─────────────────┐
 │  nginx-ingress│──────────▶│ Service:        │
 │  (dash.example.com /v1/*)││ dash-retrieval  │
 └──────────────┘           │  ClusterIP :80  │
                            └────────┬────────┘
                                     │ :8080
                          ┌──────────┴──────────┐
                          ▼                     ▼
                ┌────────────────┐    ┌────────────────┐
                │ StatefulSet:   │    │ StatefulSet:   │
                │ dash-retrieval │    │ dash-ingestion │
                │ (redb, RWO)    │◀──▶│ (redb, RWO)    │
                └────────────────┘    └───────┬────────┘
                       ▲                      │
                       │     NetworkPolicy     │
                       │  (in-cluster, :8081)  │
                       └──────────────────────┘
```

- **Egress to OpenAI/Ollama**: 443/11434 via NetworkPolicy.
- **Service-to-service**: retrieval → ingestion on :8081.
- **Public ingress**: only the retrieval service.

## Verifying an install

```bash
# 1. Workloads ready?
kubectl get pods -n dash-system -l app.kubernetes.io/part-of=dash

# 2. HPA wired?
kubectl get hpa -n dash-system

# 3. PVCs bound?
kubectl get pvc -n dash-system

# 4. Ingress has an address?
kubectl get ingress -n dash-system

# 5. End-to-end smoke test
curl -fsS https://dash.example.com/health
curl -fsS https://dash.example.com/v1/embeddings \
  -H 'content-type: application/json' \
  -d '{"model":"text-embedding-3-small","input":"hello"}'
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
    ├── serviceaccount.yaml
    ├── retrieval.yaml
    ├── ingestion.yaml
    ├── ingress.yaml
    ├── networkpolicy.yaml
    ├── pdb.yaml
    └── hpa.yaml
```
