# DashIngestToVisibleLagHigh

| | |
|---|---|
| Alert | `DashIngestToVisibleLagHigh` |
| Severity | warning |
| Rules | [`deploy/observability/prometheus/dash-alerts.rules.yml`](../../../deploy/observability/prometheus/dash-alerts.rules.yml) |
| Dashboards | DASH / Overview, DASH / Ingestion and WAL, DASH / Retrieval, embeddings and replication |

## Meaning

The 95th percentile time between an ingest and the moment retrieval can see it (`dash_ingest_to_visible_lag_ms_p95`, over the last 2048 retrieve requests) is above 5 seconds.

## Impact

Freshly written claims are missing from search results for several seconds.

## Diagnosis

Usually replication lag ([DashReplicationFollowerLagging](dash-replication-follower-lagging.md))
or a slow poll interval (`DASH_RETRIEVAL_REPLICATION_POLL_INTERVAL_MS`). On a single node, check
segment refresh and the WAL flush interval (`DASH_INGEST_WAL_ASYNC_FLUSH_INTERVAL_MS`).

Key queries:

```promql
dash_ingest_to_visible_lag_ms_p95
dash_retrieval_replication_lag_seconds
```

## Mitigation

Address the replication or flush delay; lower the poll interval if the leader has headroom.

## Escalation

If the mitigation does not resolve the alert within 30 minutes, or data loss is suspected, escalate to the storage on-call (WAL, replication) or the platform on-call (deployment, network), and open an incident following [the incident response policy](../../compliance/policies/incident-response-policy.md).
