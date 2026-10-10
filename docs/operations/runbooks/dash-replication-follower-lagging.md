# DashReplicationFollowerLagging

| | |
|---|---|
| Alert | `DashReplicationFollowerLagging` |
| Severity | warning |
| Rules | [`deploy/observability/prometheus/dash-alerts.rules.yml`](../../../deploy/observability/prometheus/dash-alerts.rules.yml) |
| Dashboards | DASH / Overview, DASH / Ingestion and WAL, DASH / Retrieval, embeddings and replication |

## Meaning

A replication follower has not caught up with its leader for more than 60 seconds (`dash_retrieval_replication_lag_seconds`), or is more than 10000 records behind (`dash_retrieval_replication_lag_records`, or `dash_ingest_replication_lag_records` for an ingestion follower).

## Impact

Reads on that follower are stale: recent writes are not visible yet. If the lag exceeds `DASH_RETRIEVAL_REPLICATION_MAX_LAG_RECORDS` (default 100000) or the last successful pull is older than `DASH_RETRIEVAL_REPLICATION_MAX_STALENESS_MS` (default 5 minutes), the follower turns not ready ([DashReplicationFollowerNotReady](dash-replication-follower-not-ready.md)).

## Diagnosis

1. Is the follower pulling at all?

   ```promql
   dash_retrieval_replication_consecutive_failures
   dash_retrieval_replication_last_success_age_ms / 1000
   rate(dash_retrieval_replication_applied_records_total[5m])
   ```

2. Failure code: `replication.last_error` in the follower's `/ready` JSON
   (`source_unreachable`, `source_rejected_credentials`, `response_too_large`, ... see
   [replication limits](../replication-limits.md#what-ready-reports-about-a-failure)).
3. Leader write rate versus follower apply rate: a sustained ingest burst can outrun a follower
   with a small `DASH_RETRIEVAL_REPLICATION_MAX_RECORDS` per pull.
4. A full resync in progress downloads an export first
   (`rate(dash_retrieval_replication_export_bytes_total[5m])`); lag drops when it is applied.

Key queries:

```promql
dash_retrieval_replication_lag_seconds
dash_retrieval_replication_lag_records
dash_ingest_replication_lag_records
```

## Mitigation

* Unreachable leader or rejected credentials: fix networking, the NetworkPolicy, or the
  replication token (it must match on both sides).
* Throughput: raise `DASH_RETRIEVAL_REPLICATION_MAX_RECORDS` (and
  `DASH_RETRIEVAL_REPLICATION_MAX_RESPONSE_BYTES`), lower `DASH_RETRIEVAL_REPLICATION_POLL_INTERVAL_MS`,
  give the follower more CPU.
* Blocked (`response_too_large`, `group_too_large`): raise the follower's response limit above the
  leader's largest commit group; retrying does not help.

## Escalation

If the mitigation does not resolve the alert within 30 minutes, or data loss is suspected, escalate to the storage on-call (WAL, replication) or the platform on-call (deployment, network), and open an incident following [the incident response policy](../../compliance/policies/incident-response-policy.md).
