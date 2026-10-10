# DashRequestsShed

| | |
|---|---|
| Alert | `DashRequestsShed` |
| Severity | warning |
| Rules | [`deploy/observability/prometheus/dash-alerts.rules.yml`](../../../deploy/observability/prometheus/dash-alerts.rules.yml) |
| Dashboards | DASH / Overview, DASH / Ingestion and WAL, DASH / Retrieval, embeddings and replication |

## Meaning

A bounded queue has been rejecting requests for 10 minutes: the HTTP accept queue (`dash_retrieve_transport_queue_full_reject_total`, `dash_ingest_transport_queue_full_reject_total`) or the ingestion group-commit queue (`dash_ingest_wal_group_commit_queue_full_reject_total`).

## Impact

Clients receive `503` overload answers for part of their traffic.

## Diagnosis

```promql
dash_retrieve_transport_queue_depth / dash_retrieve_transport_queue_capacity
dash_http_server_requests_in_flight
rate(process_cpu_seconds_total[5m])
histogram_quantile(0.99, sum by (component, route, le) (rate(dash_http_server_request_duration_seconds_bucket[5m])))
```

Shedding with low CPU points at slow dependencies (fsync, embedding provider) holding workers;
high CPU means real overload. A single tenant can dominate traffic: check the ingress logs.

Key queries:

```promql
rate(dash_retrieve_transport_queue_full_reject_total[5m])
rate(dash_ingest_wal_group_commit_queue_full_reject_total[5m])
```

## Mitigation

Scale retrieval replicas; give the pods more CPU; raise `DASH_*_HTTP_WORKERS` only
when workers are blocked on I/O rather than CPU; apply per-tenant rate limits
(`DASH_*_RATE_LIMIT_PER_TENANT_RPS`). For the group-commit queue, see
[DashWalFsyncSlow](dash-wal-fsync-slow.md).

## Escalation

If the mitigation does not resolve the alert within 30 minutes, or data loss is suspected, escalate to the storage on-call (WAL, replication) or the platform on-call (deployment, network), and open an incident following [the incident response policy](../../compliance/policies/incident-response-policy.md).
