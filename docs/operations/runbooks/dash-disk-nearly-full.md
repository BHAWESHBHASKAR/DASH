# DashDiskNearlyFull

| | |
|---|---|
| Alert | `DashDiskNearlyFull` |
| Severity | warning |
| Rules | [`deploy/observability/prometheus/dash-alerts.rules.yml`](../../../deploy/observability/prometheus/dash-alerts.rules.yml) |
| Dashboards | DASH / Overview, DASH / Ingestion and WAL, DASH / Retrieval, embeddings and replication |

## Meaning

A DASH data volume has less than 10% free space for 10 minutes. Kubernetes: from kubelet volume stats of PVCs whose name contains `dash`. systemd: from node_exporter for mount points under `/var/lib/dash`.

## Impact

None yet. When the volume fills, WAL appends fail ([DashWalWriteFailing](dash-wal-write-failing.md)), checkpoints and vector index saves fail, and audit appends fail.

## Diagnosis

```sh
kubectl -n dash-system exec <pod> -- du -sh /var/lib/dash/*
kubectl -n dash-system exec <pod> -- sh -c 'ls -laS /var/lib/dash/wal | head'
```

What usually grows: the WAL between checkpoints (`dash_wal_size_bytes`), retained closed WAL
generations (`*.closed.<generation>`), replication exports
(`dash_ingest_replication_exports_retained_bytes`), audit logs, and segments.

Key queries:

```promql
kubelet_volume_stats_available_bytes{persistentvolumeclaim=~".*dash.*"} / kubelet_volume_stats_capacity_bytes{persistentvolumeclaim=~".*dash.*"}
dash_wal_size_bytes
```

## Mitigation

* Expand the PVC (`persistence.size` and `kubectl edit pvc`), or move to a larger volume.
* Make sure checkpoints run (`increase(dash_wal_checkpoints_total[1h]) > 0`) and lower
  `DASH_CHECKPOINT_MAX_WAL_BYTES` if the WAL grows too large between them.
* Archive and rotate audit logs per [the audit chain guide](../audit-chain.md).

## Escalation

If the mitigation does not resolve the alert within 30 minutes, or data loss is suspected, escalate to the storage on-call (WAL, replication) or the platform on-call (deployment, network), and open an incident following [the incident response policy](../../compliance/policies/incident-response-policy.md).
