# DashWalFsyncSlow

| | |
|---|---|
| Alert | `DashWalFsyncSlow` |
| Severity | warning |
| Rules | [`deploy/observability/prometheus/dash-alerts.rules.yml`](../../../deploy/observability/prometheus/dash-alerts.rules.yml) |
| Dashboards | DASH / Overview, DASH / Ingestion and WAL, DASH / Retrieval, embeddings and replication |

## Meaning

The 99th percentile of WAL `fdatasync` latency has been above 100 ms for 10 minutes.

## Impact

Every acknowledged write waits for an fsync (group commit shares one fsync per batch), so ingest latency rises and throughput falls. Very slow fsyncs often precede device failures.

## Diagnosis

```promql
histogram_quantile(0.99, sum by (instance, le) (rate(dash_wal_fsync_duration_seconds_bucket[5m])))
histogram_quantile(0.99, sum by (instance, le) (rate(dash_wal_group_commit_batch_entries_bucket[5m])))
```

```sh
iostat -x 5 3        # on the node: await and %util of the device
dmesg -T | tail -50
```

Noisy neighbours on shared network storage, burst-credit exhaustion on cloud volumes, and a
checkpoint writing the snapshot at the same time are common causes.

Key queries:

```promql
histogram_quantile(0.99, sum by (instance, le) (rate(dash_wal_fsync_duration_seconds_bucket[5m])))
```

## Mitigation

Move the WAL to a faster or provisioned-IOPS volume. Do not relax durability
(`DASH_INGEST_WAL_SYNC_EVERY_RECORDS`, background flush) without an explicit decision: it trades
acknowledged-write durability for latency (see [WAL durability](../wal-durability.md)).

## Escalation

If the mitigation does not resolve the alert within 30 minutes, or data loss is suspected, escalate to the storage on-call (WAL, replication) or the platform on-call (deployment, network), and open an incident following [the incident response policy](../../compliance/policies/incident-response-policy.md).
