# DashWalPoisoned

| | |
|---|---|
| Alert | `DashWalPoisoned` |
| Severity | critical |
| Rules | [`deploy/observability/prometheus/dash-alerts.rules.yml`](../../../deploy/observability/prometheus/dash-alerts.rules.yml) |
| Dashboards | DASH / Overview, DASH / Ingestion and WAL, DASH / Retrieval, embeddings and replication |

## Meaning

An `fdatasync` of the ingestion WAL failed (`dash_ingest_wal_poisoned == 1`). Because the kernel may already have dropped the dirty pages, DASH never retries: the WAL refuses every later write until the process restarts (see [WAL durability](../wal-durability.md#fsync-failures-the-wal-is-poisoned)).

## Impact

**Ingestion is down**: every write (single, batch, raw, document, replication apply, checkpoint) fails with `503 wal_poisoned`, and `/ready` reports `wal_poisoned`. Requests of the failing batch have an unknown outcome. Reads keep working on retrieval followers.

## Diagnosis

1. Confirm and find the time of the failure:

   ```promql
   dash_ingest_wal_poisoned == 1
   increase(dash_wal_fsync_failures_total[1h])
   ```

2. Kernel and device errors around that time:

   ```sh
   dmesg -T | grep -iE 'i/o error|ext4|xfs|nvme|blk_update_request'
   kubectl -n dash-system describe pod <ingestion-pod>   # volume attach/detach events
   ```

3. Free space and inode exhaustion on the WAL volume (`df -h`, `df -i`), see
   [DashDiskNearlyFull](dash-disk-nearly-full.md).

Key queries:

```promql
dash_ingest_wal_poisoned
increase(dash_wal_fsync_failures_total[1h])
```

## Mitigation

1. Fix the storage problem first (replace the failing device, detach/reattach the
   volume, free space). Restarting on a still-broken disk poisons the WAL again.
2. Restart ingestion (`kubectl -n dash-system delete pod <ingestion-pod>` or
   `systemctl restart dash-ingestion`). Startup replays the WAL from disk and truncates a torn tail
   ([WAL recovery](../wal-recovery.md)).
3. Inspect the log before and after if in doubt: `wal-inspect` (tools/wal-inspect) reports torn
   tails and unparseable lines.
4. Tell clients that writes answered with `503` during the incident must be retried with their
   idempotency keys.

## Escalation

If the mitigation does not resolve the alert within 30 minutes, or data loss is suspected, escalate to the storage on-call (WAL, replication) or the platform on-call (deployment, network), and open an incident following [the incident response policy](../../compliance/policies/incident-response-policy.md).
