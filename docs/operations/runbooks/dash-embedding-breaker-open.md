# DashEmbeddingBreakerOpen

| | |
|---|---|
| Alert | `DashEmbeddingBreakerOpen` |
| Severity | warning |
| Rules | [`deploy/observability/prometheus/dash-alerts.rules.yml`](../../../deploy/observability/prometheus/dash-alerts.rules.yml) |
| Dashboards | DASH / Overview, DASH / Ingestion and WAL, DASH / Retrieval, embeddings and replication |

## Meaning

The circuit breaker in front of an embedding provider (`ollama` or `openai`) is open: after `DASH_EMBEDDING_BREAKER_THRESHOLD` consecutive upstream failures (timeouts, transport errors, 5xx) calls fail fast for `DASH_EMBEDDING_BREAKER_RESET_MS`, then one probe is admitted.

## Impact

Retrieve requests that need a query embedding and `/v1/embeddings` answer `503` with `Retry-After`; ingests that compute embeddings fail. Requests with precomputed embeddings are not affected.

## Diagnosis

```promql
max by (instance, provider) (dash_embedding_breaker_state)
sum by (provider, kind) (rate(dash_embedding_errors_total[5m]))
histogram_quantile(0.99, sum by (provider, le) (rate(dash_embedding_request_duration_seconds_bucket[5m])))
```

`kind` tells timeouts (`timeout`), unreachable hosts (`io`) and upstream errors (`http_5xx`) apart;
`http_429` means the provider is rate limiting (does not open the breaker).

```sh
kubectl -n dash-system exec <retrieval-pod> -- wget -qO- http://ollama.ollama.svc.cluster.local:11434/api/tags
```

Key queries:

```promql
dash_embedding_breaker_state
sum by (provider, kind) (rate(dash_embedding_errors_total[5m]))
```

## Mitigation

Restore the provider (Ollama pod, API key, quota, egress NetworkPolicy
`networkPolicy.ollama`). The breaker closes on the first successful probe (one probe per
`DASH_EMBEDDING_BREAKER_RESET_MS`); no restart is needed.

## Escalation

If the mitigation does not resolve the alert within 30 minutes, or data loss is suspected, escalate to the storage on-call (WAL, replication) or the platform on-call (deployment, network), and open an incident following [the incident response policy](../../compliance/policies/incident-response-policy.md).
