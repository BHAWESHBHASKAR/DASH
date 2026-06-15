# Observability

DASH exposes a Prometheus `/metrics` endpoint, emits structured JSON logs, and is being wired for OpenTelemetry tracing. This page describes the metrics surface, the scrape config, the log format, and the tracing roadmap.

## `/metrics` endpoint

The retrieval service exposes a Prometheus-format metrics endpoint on the same port as the HTTP API:

```text
GET /metrics
```

A sample of the metrics surface:

```text
# HELP dash_retrieve_requests_total Total number of retrieve requests.
# TYPE dash_retrieve_requests_total counter
dash_retrieve_requests_total{tenant_id="t1",outcome="ok"} 1283

# HELP dash_retrieve_latency_seconds Latency of retrieve requests.
# TYPE dash_retrieve_latency_seconds histogram
dash_retrieve_latency_seconds_bucket{le="0.001"} 920
dash_retrieve_latency_seconds_bucket{le="0.005"} 1180
dash_retrieve_latency_seconds_bucket{le="0.01"}  1250
dash_retrieve_latency_seconds_bucket{le="+Inf"}  1283
dash_retrieve_latency_seconds_sum   2.41
dash_retrieve_latency_seconds_count 1283

# HELP dash_ann_search_latency_seconds Latency of the ANN search.
# TYPE dash_ann_search_latency_seconds histogram
dash_ann_search_latency_seconds_bucket{le="0.0005"} 1100
dash_ann_search_latency_seconds_bucket{le="+Inf"}   1283
dash_ann_search_latency_seconds_sum   0.61
dash_ann_search_latency_seconds_count 1283

# HELP dash_embeddings_requests_total Total embeddings requests, by provider.
# TYPE dash_embeddings_requests_total counter
dash_embeddings_requests_total{provider="hash",outcome="ok"} 640

# HELP dash_redb_disk_bytes redb file size, in bytes.
# TYPE dash_redb_disk_bytes gauge
dash_redb_disk_bytes{service="retrieval"} 482344960

# HELP dash_audit_chain_head_seq The sequence number of the head of the audit chain.
# TYPE dash_audit_chain_head_seq gauge
dash_audit_chain_head_seq{tenant_id="t1"} 17842
```

The full list of metrics is generated at build time and lives in `services/*/src/metrics.rs`.

## Prometheus scrape config

A minimal scrape config:

```yaml
scrape_configs:
  - job_name: dash-retrieval
    metrics_path: /metrics
    static_configs:
      - targets:
          - dash-retrieval:8080
        labels:
          service: retrieval
    scrape_interval: 15s

  - job_name: dash-ingestion
    metrics_path: /metrics
    static_configs:
      - targets:
          - dash-ingestion:8081
        labels:
          service: ingestion
    scrape_interval: 15s
```

### Recommended alert rules

```yaml
groups:
  - name: dash
    rules:
      - alert: DashHighRetrieveLatency
        expr: histogram_quantile(0.99, sum(rate(dash_retrieve_latency_seconds_bucket[5m])) by (le)) > 0.05
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "DASH p99 retrieve latency is above 50ms for 10m"

      - alert: DashAuditChainStalled
        expr: rate(dash_audit_chain_head_seq[5m]) == 0
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "DASH audit chain has not advanced for 5m"

      - alert: DashRedbDiskHigh
        expr: dash_redb_disk_bytes > 1e12
        for: 30m
        labels:
          severity: warning
        annotations:
          summary: "DASH redb file is above 1 TB; consider snapshot + archive"
```

## Structured logs

DASH emits logs as one JSON object per line on stdout. The fields:

```json
{
  "ts": "2026-06-15T12:34:56.789Z",
  "level": "info",
  "service": "retrieval",
  "tenant_id": "t1",
  "request_id": "01HMRX...",
  "actor": "jwt:sub=alice",
  "msg": "retrieve complete",
  "took_us": 1240,
  "top_k": 5,
  "result_count": 3
}
```

The level is controlled by `DASH_LOG_LEVEL` (default: `info`; supported: `trace`, `debug`, `info`, `warn`, `error`). The format is **always** JSON — there is no text-mode log output.

In Kubernetes, the logs are picked up by the standard `kubectl logs` path. A `Fluent Bit` or `Vector` sidecar forwards them to the log backend. The JSON shape is stable; downstream parsers should key on `ts`, `level`, `service`, `tenant_id`, and `msg`.

## OpenTelemetry tracing (future)

OpenTelemetry tracing is on the [roadmap](https://github.com/BHAWESHBHASKAR/DASH/issues?q=is%3Aopen+label%3Aotel) but **not** in the current release. The plan is to instrument the request handlers with `tracing` spans, export via OTLP, and propagate the W3C `traceparent` header across the SDK → ingestion → retrieval path.

When it ships, the trace shape will be:

```text
HTTP server span (POST /v1/retrieve)
  ├── JWT verify span
  ├── Embedding provider span (calls Ollama or OpenAI)
  ├── ANN search span (usearch)
  ├── Lexical rerank span (BM25)
  └── Response build span
```

The migration will be additive — no breaking changes to the response shape or the log fields. Follow the [issue tracker](https://github.com/BHAWESHBHASKAR/DASH/issues?q=is%3Aopen+label%3Aotel) for the rollout.
