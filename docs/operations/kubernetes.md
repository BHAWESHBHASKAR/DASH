# Kubernetes

How to run DASH on Kubernetes with the Helm chart (`deploy/helm/dash`) or the
raw manifests (`deploy/k8s`, TLS overlay `deploy/k8s-tls`): install, verify,
upgrade, back up and restore, scale, and what the deployment does not do yet.
Chart values are documented in `deploy/helm/dash/values.yaml` and
`deploy/helm/dash/README.md`; TLS in [tls.md](tls.md); version-to-version
data compatibility in [upgrades.md](upgrades.md).

Everything on this page is exercised by `scripts/kind_e2e.sh` (see
[Testing on kind](#testing-on-kind)).

## Topology

| Workload | Kind | Replicas | Storage | Role |
|---|---|---|---|---|
| `<fullname>-ingestion` | StatefulSet | 1, fixed | PVC `data-<fullname>-ingestion-0` | Single writer: WAL, snapshot, redb mirror, segments, audit log. Serves `/v1/ingest*`, deletes and `/internal/replication/*`. |
| `<fullname>-retrieval` | StatefulSet | `replicas.retrieval` (2) | one PVC per pod | Read path (`/v1/retrieve`, `/v1/embeddings`). Each pod is an independent follower of ingestion over WAL replication. |
| `<fullname>-control-plane` | StatefulSet | 1, fixed (optional) | one PVC | Placement metadata and a file-backed leader lease. |

`<fullname>` is the release name when it contains `dash` (release `dash` gives
`dash-ingestion`), otherwise `<release>-dash`. Objects are created in the
release namespace (`helm install --namespace`), or in `namespace.name` when
set.

Pods run as UID/GID 65532 with a read-only root filesystem and satisfy Pod
Security "restricted"; the PVCs are made writable through `fsGroup`, and an
init container creates the data directories. NetworkPolicies (on by default)
deny everything except DNS, retrieval -> ingestion (replication),
retrieval/ingestion <-> control plane, the ingress controller -> the HTTP
ports, and HTTPS egress to public addresses (embedding providers). They need a
CNI that enforces NetworkPolicy; without one they are silently ignored.

## Install

```bash
kubectl create namespace dash-system
kubectl label namespace dash-system pod-security.kubernetes.io/enforce=restricted

helm install dash ./deploy/helm/dash --namespace dash-system \
  --set secret.retrieval.apiKey="$(openssl rand -hex 32)" \
  --set secret.retrieval.hs256Secret="$(openssl rand -hex 32)" \
  --set secret.ingestion.apiKey="$(openssl rand -hex 32)" \
  --set secret.ingestion.hs256Secret="$(openssl rand -hex 32)" \
  --set secret.replicationToken="$(openssl rand -hex 32)" \
  --set secret.controlPlane.token="$(openssl rand -hex 32)" \
  --wait --timeout 10m
```

Keep the generated values (or create the Secrets yourself and reference them
with `secret.existingSecret.*`); `scripts/generate-secrets.sh --output FILE`
writes a full set to a file. `persistence.storageClassName` empty uses the
cluster default StorageClass. For native TLS add `--set tls.enabled=true
--set tls.secretName=<secret with tls.crt, tls.key, ca.crt>`; the certificate
must name the service DNS names (`<fullname>-ingestion.<ns>.svc.cluster.local`
and the retrieval and control-plane services) and carry both `serverAuth` and
`clientAuth`. A cert-manager `Certificate` produces that shape; any CA works
(the kind test uses a self-signed one).

Raw manifests: create the three Secrets described in `deploy/k8s/11-secrets.yaml`,
then `kubectl apply -k deploy/k8s` (or `deploy/k8s-tls`). The manifests use the
`standard` StorageClass and the `dash-system` namespace; change them with a
kustomize overlay.

## Verify

```bash
kubectl -n dash-system get pods,pvc
kubectl -n dash-system port-forward svc/dash-ingestion 8081:80 &
kubectl -n dash-system port-forward svc/dash-retrieval 8080:80 &

curl -fsS -H "x-api-key: $INGEST_KEY" -H 'content-type: application/json' \
  -d '{"claim":{"claim_id":"c1","tenant_id":"t1","canonical_text":"hello world","confidence":0.9}}' \
  http://127.0.0.1:8081/v1/ingest
curl -fsS -H "x-api-key: $RETRIEVAL_KEY" -H 'content-type: application/json' \
  -d '{"tenant_id":"t1","query":"hello","top_k":5}' http://127.0.0.1:8080/v1/retrieve
```

`/v1/ready` on a retrieval pod returns 503 (with a `reason` and the follower
state) until that pod reaches ingestion and has caught up; the Service routes
only to ready pods. Probes: startup and liveness use `/v1/live`, readiness
`/v1/ready`.

## Upgrade

```bash
helm upgrade dash ./deploy/helm/dash --namespace dash-system --reuse-values \
  --set image.tag=<new tag> --wait --timeout 10m
```

* Any change to `config.*` or to chart-managed `secret.*` rolls every pod: the
  pod templates carry `checksum/config` and `checksum/secrets` annotations
  (configuration is read from the environment only at start).
* Data lives on the PVCs and survives the roll. During the roll writes are
  unavailable while the ingestion pod restarts; retrieval pods keep serving
  what they have and turn ready again once they reach the new ingestion pod.
* Back up first (below). Releases that change on-disk formats have extra
  steps and a restore-only rollback: read [upgrades.md](upgrades.md) and the
  CHANGELOG before upgrading across minor versions. Helm restarts all three
  StatefulSets at once; a follower that comes up before the new ingestion
  pod reports not ready until it can replicate from it (the "followers
  first" case in upgrades.md), so no ordering is needed for correctness.
* `volumeClaimTemplates` are immutable: changing `persistence.*` needs
  `kubectl delete statefulset <name> --cascade=orphan` and then the upgrade
  (the PVCs and pods are adopted).
* `helm rollback` restores manifests, not data. After a release that rewrote
  the WAL format, roll back by restoring a backup.

## Backup and restore

`scripts/k8s_backup_restore.sh` backs up and restores the ingestion state of a
release. Only ingestion needs a backup: every retrieval pod rebuilds itself
from ingestion, and the redb mirror, the saved vector index and the WAL
lineage file are derived data.

```bash
# Back up (writes dash-backup-<label>.tar.gz and dash-audit-<label>.tar.gz)
scripts/k8s_backup_restore.sh backup --namespace dash-system --release dash \
  --out-dir ./backups --label "$(date -u +%Y%m%d-%H%M%S)"

# Restore
scripts/k8s_backup_restore.sh restore --namespace dash-system --release dash \
  --bundle ./backups/dash-backup-<label>.tar.gz
```

What it does:

* **Cold copy.** It scales the ingestion StatefulSet to 0, starts a helper pod
  (same image, same security context, `sleep`) that mounts the ingestion PVC,
  streams the files out (or in) with `kubectl exec ... tar`, deletes the
  helper and scales back to the original replica count. Writes fail for that
  window (usually well under a minute); retrieval keeps serving reads. A cold
  copy is always consistent; no write is half-copied.
* **Bundle format.** The backup is the format of `scripts/backup_state_bundle.sh`:
  the WAL, its snapshot (when one exists) and the segment directory, with
  SHA-256 checksums. The audit log goes to a separate tarball. Copy both off
  the cluster (object storage) and keep them as long as your retention policy
  requires; remember that a backup still contains data deleted afterwards
  ([data-deletion.md](data-deletion.md)).
* **Restore** verifies the checksums before touching the cluster, then, with
  ingestion stopped, empties the WAL directory (including derived files such
  as `<wal>.gen` and `<wal>.vindex`), replaces the segment directory, deletes
  the redb mirror and unpacks the bundle. Ingestion replays the restored WAL
  and starts a new WAL lineage; every retrieval pod sees the new lineage and
  resyncs from a full export, so writes made after the backup disappear from
  all pods. The audit tarball is unpacked only when the PVC has no audit log
  (an existing chain is never overwritten).
* **Control plane.** Its state (`state/control-plane.*` on its own PVC) is not
  covered by the script. It holds placement metadata and the lease; with the
  default single-shard deployment it can be recreated empty. To keep it,
  copy the files the same way (scale to 0, helper pod, `tar`).
* **Volume snapshots** are an alternative when the storage driver supports
  CSI `VolumeSnapshot`: snapshot `data-<fullname>-ingestion-0` while the
  StatefulSet is scaled to 0. Restoring one restores the derived files too;
  delete `wal/*.gen`, `wal/*.vindex` and `state/ingestion.redb` before scaling
  back up so the followers resync.

## Failures

| Event | Effect |
|---|---|
| Ingestion pod killed or rescheduled | Writes fail until the pod is back (single writer). Acknowledged writes are on the PVC (with the default `config.wal.ingestSyncEveryRecords: 1`, fsynced before the 200) and are replayed at start. Followers retry and catch up. |
| Retrieval pod killed | The StatefulSet restarts it on the same PVC; it resumes from its saved offset. Other replicas keep serving. |
| Retrieval PVC lost | A fresh pod starts empty and resyncs everything from ingestion. |
| Node lost with ingestion's volume on it | With node-local storage (local-path, hostPath) ingestion stays down until the node or volume returns, or until you restore a backup onto a new PVC. Use network-attached storage for ingestion. |
| Control plane down | Placement updates and lease renewals stop; ingest and retrieve keep working in the default single-shard setup. |

## Scaling and current limits

* **One ingestion pod.** There is no leader election or failover for the
  writer; it is a single point of failure for writes. Never scale it above 1.
* **Manual retrieval scaling, no autoscaler.** Use `helm upgrade --set
  replicas.retrieval=N` (a plain `kubectl scale` is undone by the next
  upgrade). A new replica starts empty and copies the full state from
  ingestion before it reports ready, which costs ingestion CPU and network
  proportional to the data set. Scaling down leaves the PVC of the removed
  pod behind (delete it if you will not scale up again).
* **One control plane.** The lease is file-based on its own PVC.
* **Placement routing / sharding** across several ingestion leaders is not
  part of the chart.
* **Anti-affinity is preferred, not required.** On small clusters several
  DASH pods share a node.
* **PodDisruptionBudget** covers retrieval only (`minAvailable: 1`); a budget
  on single-replica workloads would block node drains.
* **Ingress** assumes ingress-nginx annotations; replication and `/metrics`
  are never routed.

## Testing on kind

`scripts/kind_e2e.sh` runs the whole procedure on a local kind cluster:

1. installs kind, helm and kubectl at pinned versions (sha256-verified) and
   creates a cluster from a node image pinned by digest;
2. builds the image from `deploy/container/Dockerfile` and loads it with
   `kind load docker-image`;
3. installs the chart with `deploy/helm/dash/ci/kind-values.yaml` and secrets
   from `scripts/generate-secrets.sh`, in a namespace enforcing Pod Security
   "restricted", with NetworkPolicy enforced by kindnet and PVCs from kind's
   default StorageClass;
4. checks authentication, ingests claims, retrieves them from every retrieval
   pod, deletes one, kills the ingestion pod, kills a retrieval pod, deletes
   a retrieval pod together with its PVC, backs up, writes, restores (the
   post-backup write must disappear everywhere), and runs `helm upgrade` with
   a changed configuration value and 3 replicas (every pod must roll, no data
   may be lost);
5. optionally installs the chart from an older git ref and upgrades it
   (`DASH_E2E_UPGRADE_FROM_REF`);
6. applies the raw manifests (`deploy/k8s`) and checks ingest -> retrieve;
7. installs with `tls.enabled=true` and a self-signed CA generated by the
   script, and checks ingest -> retrieve over HTTPS (replication over mutual
   TLS).

```bash
scripts/kind_e2e.sh                                  # everything, ~6-8 minutes after the image build
DASH_E2E_PHASES="core" scripts/kind_e2e.sh           # one phase
DASH_E2E_UPGRADE_FROM_REF=origin/main scripts/kind_e2e.sh
DASH_E2E_KEEP_CLUSTER=1 scripts/kind_e2e.sh          # keep the cluster for debugging
```

On failure it writes pod logs (current and previous), `kubectl describe`
output, events and the kind node logs to `DASH_E2E_ARTIFACT_DIR`. The header
of the script lists every setting. CI runs it in
`.github/workflows/kind-e2e.yml` on pull requests that touch the deployment
or the services, and nightly. On hosts with cgroup v1, or where root cannot
lower `oom_score_adj` (some sandboxed VMs), the script patches the kind
configuration accordingly.
