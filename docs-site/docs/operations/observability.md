# Observability

DASH exposes a Prometheus `/metrics` endpoint, writes logs through `tracing` (text, or JSON lines on request), and has no tracing export yet. This page describes the metrics surface, the scrape config, the log format, and the tracing roadmap.

## `/metrics` endpoint

The retrieval service exposes a Prometheus-format metrics endpoint on the same port as the HTTP API:

```text
GET /metrics
```

The ingestion service serves `GET /metrics` too. Metrics are Prometheus text lines (counters and gauges) with a service prefix: `dash_ingest_*` on ingestion, `dash_retrieve_*` and `dash_transport_*` on retrieval. There are no `tenant_id` or `outcome` labels. Retrieval exports one histogram, `dash_http_request_duration_ms` (labelled by `route`); other latency figures are precomputed percentiles. Representative names (not exhaustive):

| Area | Metrics |
|---|---|
| Retrieval requests | `dash_http_request_duration_ms` (histogram per route), `dash_retrieve_requests_total`, `dash_retrieve_success_total`, `dash_retrieve_client_error_total`, `dash_retrieve_server_error_total`, `dash_retrieve_latency_ms_p50` / `_p95` / `_p99`, `dash_retrieve_last_result_count` |
| Retrieval transport | `dash_retrieve_transport_queue_depth`, `dash_retrieve_transport_queue_capacity`, `dash_retrieve_transport_queue_full_reject_total`, `dash_transport_uptime_seconds` |
| Auth and audit (retrieval) | `dash_transport_auth_success_total`, `dash_transport_auth_failure_total`, `dash_transport_authz_denied_total`, `dash_retrieve_rate_limited_total`, `dash_transport_audit_events_total`, `dash_transport_audit_write_error_total`; both services also expose `dash_audit_records_total` and `dash_audit_write_failures_total` |
| Storage visibility | `dash_retrieve_storage_divergence_warn_total`, `dash_retrieve_storage_last_divergence_ratio`, `dash_retrieve_segment_cache_hits_total`, `dash_retrieve_segment_refresh_*`, `dash_ingest_to_visible_lag_ms_p50` / `_p95` |
| Disk | `dash_disk_unavailable`, `dash_disk_recovering` |
| Placement | `dash_retrieve_placement_*`, `dash_ingest_placement_*` |
| Ingest | `dash_ingest_claims_total`, `dash_ingest_success_total`, `dash_ingest_failed_total`, `dash_ingest_batch_*`, `dash_ingest_auth_*`, `dash_ingest_audit_*` |
| WAL | `dash_ingest_wal_buffered_records`, `dash_ingest_wal_unsynced_records`, `dash_ingest_wal_flush_*`, `dash_ingest_wal_async_flush_*` |
| Replication | on a retrieval follower: `dash_retrieval_replication_enabled`, `_ready`, `_generation`, `_offset`, `_lag_records`, `_last_success_age_ms`, `_consecutive_failures`, `_failures_total`, `_resyncs_total`, `_applied_records_total`; on an ingestion node: `dash_ingest_replication_*` (offsets, pulls, lag, generation, consecutive failures) |
| Segments | `dash_ingest_segment_publish_*`, `dash_ingest_segment_maintenance_*` |

An earlier version of this page listed metrics that do not exist (`dash_retrieve_latency_seconds` histograms, `dash_ann_search_latency_seconds`, `dash_embeddings_requests_total`, `dash_redb_disk_bytes`, `dash_audit_chain_head_seq`). The metric names are rendered in `services/retrieval/src/transport.rs` (`render_prometheus`) and `services/ingestion/src/transport.rs`; there is no `metrics.rs` per service and no generated metric list yet. Since 0.3.0 `/metrics` requires a credential with the `read_only` (or `admin`) role, so add one to your scrape config (Prometheus supports an `authorization` block, which sends `Authorization: Bearer <credential>`, and, in recent versions, `http_headers`); alternatively set `DASH_METRICS_PUBLIC=1` to exempt `/metrics` only. Health probes need no credential.

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
    authorization:
      credentials: <a retrieval key or JWT holding the read_only role>

  - job_name: dash-ingestion
    metrics_path: /metrics
    static_configs:
      - targets:
          - dash-ingestion:8081
        labels:
          service: ingestion
    scrape_interval: 15s
    authorization:
      credentials: <an ingestion key or JWT holding the read_only role>
```

### Recommended alert rules

The repository ships alert rules in `deploy/container/monitoring/prometheus-alert-rules.yml`: `DashDiskUnavailable`, `DashIngestToVisibleLagHigh`, `DashStorageDivergenceWarn`, `DashRetrieveServerErrorRate` and `DashReadyProbeFailing`. They are not validated by any automated test. Earlier examples on this page referenced histogram, audit-chain and redb-size metrics that DASH does not export.

## Logs

Services log through `tracing`. The default output is compact text on stderr/stdout; set `DASH_LOG_FORMAT=json` for JSON lines. Verbosity is controlled by the standard `RUST_LOG` variable (default `info`); `DASH_LOG_LEVEL` does not exist. Some startup diagnostics are still written with `eprintln!`, outside the `tracing` pipeline. There is no stable log schema: no per-request completion line is emitted, and log field names may change. (Audit records, which are a separate file, carry a `request_id` and an `actor` fingerprint; see [Audit chain](audit-chain.md).) Do not build parsers or alerts on them yet.

In Kubernetes, `kubectl logs` and a log-forwarding sidecar work as usual.

## OpenTelemetry tracing (future)

OpenTelemetry tracing is on the roadmap but **not** in the current release. The plan is to instrument the request handlers with `tracing` spans, export via OTLP, and propagate the W3C `traceparent` header across the SDK → ingestion → retrieval path.

When it ships, the trace shape will be:

```text
HTTP server span (POST /v1/retrieve)
  ├── JWT verify span
  ├── Embedding provider span (calls Ollama or OpenAI)
  ├── ANN search span
  ├── Lexical rerank span (BM25)
  └── Response build span
```

The migration will be additive — no breaking changes to the response shape or the log fields. 
