# DashReplicationResyncStorm

| | |
|---|---|
| Alert | `DashReplicationResyncStorm` |
| Severity | warning |
| Rules | [`deploy/observability/prometheus/dash-alerts.rules.yml`](../../../deploy/observability/prometheus/dash-alerts.rules.yml) |
| Dashboards | DASH / Overview, DASH / Ingestion and WAL, DASH / Retrieval, embeddings and replication |

## Meaning

A follower ran more than 3 full resyncs in 30 minutes (`dash_retrieval_replication_resyncs_total` or `dash_ingest_replication_resync_total`).

## Impact

Each resync downloads the leader's whole state as a chunked export: leader CPU and disk, network, and follower staleness during the download. A follower that keeps resyncing may never become ready.

## Diagnosis

A follower resyncs instead of switching generations when it misses a whole
generation (two checkpoints while it was behind), is ahead of the leader, or the leader rolled back
or lost its transitions file ([replication limits](../replication-limits.md#checkpoints-switching-generations-instead-of-resyncing)).

```promql
increase(dash_ingest_replication_generation_switches_total[30m])
increase(dash_wal_checkpoints_total[30m])
rate(dash_retrieval_replication_export_bytes_total[5m])
```

Checkpoints firing very often (small `DASH_CHECKPOINT_MAX_WAL_RECORDS`) combined with a slow
follower are the usual cause.

Key queries:

```promql
increase(dash_retrieval_replication_resyncs_total[30m])
increase(dash_wal_checkpoints_total[30m])
```

## Mitigation

Raise the checkpoint thresholds so checkpoints are rarer than the follower's worst
catch-up time, and make the follower faster (see the lagging runbook). Check the leader log for
rollbacks or a missing `<wal>.gen.transitions` file.

## Escalation

If the mitigation does not resolve the alert within 30 minutes, or data loss is suspected, escalate to the storage on-call (WAL, replication) or the platform on-call (deployment, network), and open an incident following [the incident response policy](../../compliance/policies/incident-response-policy.md).
