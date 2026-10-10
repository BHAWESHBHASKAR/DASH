# DashCheckpointFailing

| | |
|---|---|
| Alert | `DashCheckpointFailing` |
| Severity | warning |
| Rules | [`deploy/observability/prometheus/dash-alerts.rules.yml`](../../../deploy/observability/prometheus/dash-alerts.rules.yml) |
| Dashboards | DASH / Overview, DASH / Ingestion and WAL, DASH / Retrieval, embeddings and replication |

## Meaning

A WAL checkpoint (snapshot write plus WAL compaction) failed in the last 30 minutes (`dash_wal_checkpoint_failures_total` increased).

## Impact

The WAL keeps growing: restarts replay more records and the volume fills. Followers keep working (they follow the current generation).

## Diagnosis

```sh
kubectl -n dash-system logs <pod> --since=1h | grep -i checkpoint
kubectl -n dash-system exec <pod> -- df -h /var/lib/dash
```

```promql
increase(dash_wal_checkpoint_failures_total[1h])
time() - dash_wal_checkpoint_last_success_timestamp_seconds
dash_wal_size_bytes
```

A poisoned WAL also refuses checkpoints ([DashWalPoisoned](dash-wal-poisoned.md)).

Key queries:

```promql
increase(dash_wal_checkpoint_failures_total[1h])
dash_wal_size_bytes
```

## Mitigation

Free or add space (a checkpoint needs room for the new snapshot next to the old
one), fix permissions on the WAL directory, then let the next automatic checkpoint run (thresholds
`DASH_CHECKPOINT_MAX_WAL_RECORDS` / `DASH_CHECKPOINT_MAX_WAL_BYTES`, checked after each write).
A restart replays and keeps the WAL; it does not checkpoint by itself.

## Escalation

If the mitigation does not resolve the alert within 30 minutes, or data loss is suspected, escalate to the storage on-call (WAL, replication) or the platform on-call (deployment, network), and open an incident following [the incident response policy](../../compliance/policies/incident-response-policy.md).
