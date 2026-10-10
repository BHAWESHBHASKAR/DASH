# DashStorageDivergence

| | |
|---|---|
| Alert | `DashStorageDivergence` |
| Severity | warning |
| Rules | [`deploy/observability/prometheus/dash-alerts.rules.yml`](../../../deploy/observability/prometheus/dash-alerts.rules.yml) |
| Dashboards | DASH / Overview, DASH / Ingestion and WAL, DASH / Retrieval, embeddings and replication |

## Meaning

Retrieval's storage-visibility check found that the segment base and the WAL delta disagree on visible claims beyond the configured threshold (`dash_retrieve_storage_last_divergence_warn == 1`).

## Impact

Results may be served from the in-memory index while segments are stale, or the reverse; result sets can differ between the two execution modes.

## Diagnosis

```sh
curl -s -H "Authorization: Bearer $READ_ONLY_KEY" http://<retrieval>:8080/debug/storage-visibility | jq .
```

```promql
dash_retrieve_storage_last_divergence_ratio
increase(dash_retrieve_storage_divergence_warn_total[1h])
```

Segment maintenance (`dash_ingest_segment_maintenance_failure_total`,
`dash_ingest_segment_publish_failure_total`) not running is the usual cause.

Key queries:

```promql
dash_retrieve_storage_last_divergence_ratio
```

## Mitigation

Fix segment publication on ingestion (logs, volume space), or run the segment
maintenance daemon. Thresholds: `DASH_RETRIEVAL_STORAGE_DIVERGENCE_WARN_DELTA_COUNT` and
`DASH_RETRIEVAL_STORAGE_DIVERGENCE_WARN_RATIO`.

## Escalation

If the mitigation does not resolve the alert within 30 minutes, or data loss is suspected, escalate to the storage on-call (WAL, replication) or the platform on-call (deployment, network), and open an incident following [the incident response policy](../../compliance/policies/incident-response-policy.md).
