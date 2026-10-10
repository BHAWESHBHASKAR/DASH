# DashWalWriteFailing

| | |
|---|---|
| Alert | `DashWalWriteFailing` |
| Severity | critical |
| Rules | [`deploy/observability/prometheus/dash-alerts.rules.yml`](../../../deploy/observability/prometheus/dash-alerts.rules.yml) |
| Dashboards | DASH / Overview, DASH / Ingestion and WAL, DASH / Retrieval, embeddings and replication |

## Meaning

Appending to the ingestion WAL fails (`dash_ingest_wal_write_failing == 1`), typically because the volume is full (`ENOSPC`) or the file cannot be opened. Unlike a poisoned WAL this recovers by itself: `/ready` probes the WAL directory with a 1 MiB scratch file and reports ready again once the write succeeds.

## Impact

Ingest requests fail with 5xx and the pod is out of the load balancer (`/ready` reports `wal_write_failed`). Nothing half-written stays in the log: a failed unit is truncated back.

## Diagnosis

```sh
kubectl -n dash-system exec <ingestion-pod> -- df -h /var/lib/dash
kubectl -n dash-system exec <ingestion-pod> -- df -i /var/lib/dash
kubectl -n dash-system logs <ingestion-pod> --since=30m | grep -i 'could not be persisted'
```

```promql
dash_wal_size_bytes
increase(dash_ingest_wal_write_failure_total[15m])
kubelet_volume_stats_available_bytes{persistentvolumeclaim=~".*dash-ingestion.*"}
```

Key queries:

```promql
dash_ingest_wal_write_failing
dash_wal_size_bytes
```

## Mitigation

* Full volume: expand the PVC (`kubectl edit pvc <pvc>`, needs an expandable storage
  class) or free space. A large WAL usually means checkpoints are not running: check
  [DashCheckpointFailing](dash-checkpoint-failing.md) and `DASH_CHECKPOINT_MAX_WAL_BYTES`. Large
  `*.closed.<generation>` files and audit logs on the same volume also count.
* Permissions or a read-only remount: fix the mount, then restart.
* The alert clears by itself once a probe write succeeds (`dash_ingest_wal_write_recovered_total`
  increments).

## Escalation

If the mitigation does not resolve the alert within 30 minutes, or data loss is suspected, escalate to the storage on-call (WAL, replication) or the platform on-call (deployment, network), and open an incident following [the incident response policy](../../compliance/policies/incident-response-policy.md).
