# DashHighErrorRate

| | |
|---|---|
| Alert | `DashHighErrorRate` |
| Severity | critical |
| Rules | [`deploy/observability/prometheus/dash-alerts.rules.yml`](../../../deploy/observability/prometheus/dash-alerts.rules.yml) |
| Dashboards | DASH / Overview, DASH / Ingestion and WAL, DASH / Retrieval, embeddings and replication |

## Meaning

More than 5% of non-probe requests to a component were answered with a 5xx for 5 minutes (with at least 0.1 requests per second, so idle services do not page).

## Impact

Clients see failed ingests or retrievals. Ingest 5xx answers are "unknown outcome": the write may or may not be durable, clients must retry with the same idempotency key.

## Diagnosis

1. Which route and code:

   ```promql
   sum by (component, route, code) (rate(dash_http_server_requests_total{code=~"5.."}[5m]))
   ```

2. Map the code to a cause:
   * `503` on ingest with `wal_poisoned` / `wal_write_failed` in the body: see
     [DashWalPoisoned](dash-wal-poisoned.md) and [DashWalWriteFailing](dash-wal-write-failing.md).
   * `503` with `audit log unavailable`: fail-closed audit, see
     [DashAuditWriteFailures](dash-audit-write-failures.md).
   * `503` on retrieve or `/v1/embeddings` with `Retry-After`: embedding provider unavailable or
     breaker open, see [DashEmbeddingBreakerOpen](dash-embedding-breaker-open.md).
   * `503` from the overload response: queues are full, see [DashRequestsShed](dash-requests-shed.md).
   * `500`: a handler error or panic. Search the logs for the request ids of failing requests (every
     error body carries `request_id`, and the `X-Request-Id` response header echoes it):

     ```sh
     kubectl -n dash-system logs <pod> --since=15m | grep -E '"level":"(WARN|ERROR)"' | tail -50
     ```

     With `RUST_LOG=info,dash_access=debug` every request logs one `dash_access` line with its
     status, latency, route and request id; 5xx answers are always logged at `warn`.
3. Correlate with deploys: `dash_build_info` shows the version and git commit of each process.

Key queries:

```promql
sum by (component, route, code) (rate(dash_http_server_requests_total{code=~"5.."}[5m]))
sum by (component) (rate(dash_http_server_requests_total{code=~"5..",route!~"health|live|ready|metrics"}[5m])) / sum by (component) (rate(dash_http_server_requests_total{route!~"health|live|ready|metrics"}[5m]))
```

## Mitigation

* A bad release: roll back (`helm rollback <release>`), then confirm the ratio drops.
* A dependency (disk, embedding provider, audit volume): follow the linked runbook.
* Overload: scale retrieval replicas, raise worker/queue settings (`DASH_*_HTTP_WORKERS`,
  `DASH_*_HTTP_QUEUE_CAPACITY`) only if CPU allows, or rate-limit the noisy tenant
  (`DASH_*_RATE_LIMIT_PER_TENANT_RPS`).

## Escalation

If the mitigation does not resolve the alert within 30 minutes, or data loss is suspected, escalate to the storage on-call (WAL, replication) or the platform on-call (deployment, network), and open an incident following [the incident response policy](../../compliance/policies/incident-response-policy.md).
