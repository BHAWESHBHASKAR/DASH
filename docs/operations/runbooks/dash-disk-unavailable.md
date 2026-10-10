# DashDiskUnavailable

| | |
|---|---|
| Alert | `DashDiskUnavailable` |
| Severity | critical |
| Rules | [`deploy/observability/prometheus/dash-alerts.rules.yml`](../../../deploy/observability/prometheus/dash-alerts.rules.yml) |
| Dashboards | DASH / Overview, DASH / Ingestion and WAL, DASH / Retrieval, embeddings and replication |

## Meaning

The redb disk store of a node is unavailable (`dash_disk_unavailable == 1`); the node serves from memory only.

## Impact

`/ready` reports `disk_unavailable` and the node leaves the load balancer. Writes on ingestion are still protected by the WAL, but redb-backed restarts are slower and state written only in memory is lost if the process also dies.

## Diagnosis

```sh
kubectl -n dash-system logs <pod> --since=1h | grep -i 'disk'
kubectl -n dash-system exec <pod> -- ls -la /var/lib/dash/state
kubectl -n dash-system exec <pod> -- df -h /var/lib/dash
```

`dash_disk_recovering == 1` means the store is being reopened.

Key queries:

```promql
dash_disk_unavailable
dash_disk_recovering
```

## Mitigation

Fix the volume (space, permissions, device errors as in
[DashWalPoisoned](dash-wal-poisoned.md)), then restart the pod. If the redb file is damaged,
move it aside: on start, the store is rebuilt from the WAL and snapshot.

## Escalation

If the mitigation does not resolve the alert within 30 minutes, or data loss is suspected, escalate to the storage on-call (WAL, replication) or the platform on-call (deployment, network), and open an incident following [the incident response policy](../../compliance/policies/incident-response-policy.md).
