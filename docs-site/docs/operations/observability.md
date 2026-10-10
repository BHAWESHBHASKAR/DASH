# Observability

DASH exposes Prometheus metrics on `/metrics` of every service (ingestion, retrieval and the control plane), tags every request with a correlation id, writes logs through `tracing` (text, or JSON lines), and ships alert rules with a runbook per alert, SLO recording rules and Grafana dashboards. There is no distributed tracing export yet (see the end of this page).

## `/metrics` endpoint

```text
GET /metrics
```

The endpoint is served on the same port as the HTTP API. On ingestion and retrieval it needs a credential with the `read_only` (or `admin`) role; on the control plane it needs the control-plane token. `DASH_METRICS_PUBLIC=1` exempts `/metrics` (only) from authentication on all three services. The body is valid Prometheus text exposition: every family has a `# TYPE`, and the services' tests parse the full body with a strict validator (`dash_observe::validate`). New families also carry `# HELP` text.

### Families shared by every service

| Family | Type | Labels | Meaning |
|---|---|---|---|
| `dash_http_server_requests_total` | counter | `component`, `route`, `method`, `code` | Requests handled, by status code |
| `dash_http_server_request_duration_seconds` | histogram | `component`, `route`, `method` | Latency from the parsed request to the rendered response (buckets 0.5 ms to 10 s) |
| `dash_http_server_requests_in_flight` | gauge | `component` | Requests being handled |
| `dash_build_info` | gauge (always 1) | `component`, `version`, `git_sha` | Build of the running process |
| `process_resident_memory_bytes`, `process_virtual_memory_bytes`, `process_open_fds`, `process_max_fds`, `process_threads`, `process_cpu_seconds_total`, `process_start_time_seconds` | gauge / counter | none | Process resources (Linux, from `/proc/self`) |
| `dash_process_uptime_seconds` | gauge | none | Seconds since start |

Labels are bounded: `component` is `ingestion`, `retrieval` or `control-plane` (not `service`, which Kubernetes service discovery adds as a target label); `route` is a fixed name per service (`retrieve`, `ingest`, `ingest_batch`, `delete_claim`, `replication_wal`, `placement`, ...; unknown paths are `other`, and ids in delete paths are never used); `method` is folded to a fixed set; `code` is the HTTP status. No tenant, claim or credential value is ever a label.

### Storage (ingestion and retrieval)

| Family | Type | Meaning |
|---|---|---|
| `dash_wal_append_duration_seconds` | histogram | One WAL append unit: write plus the fsync the write policy requires |
| `dash_wal_fsync_duration_seconds` | histogram | WAL `fdatasync` latency |
| `dash_wal_fsync_failures_total` | counter | Failed fsyncs (each one poisons the WAL) |
| `dash_wal_appended_bytes_total` | counter | Bytes appended to the WAL |
| `dash_wal_size_bytes` | gauge | WAL file size (ingestion) |
| `dash_wal_group_commit_batch_entries` | histogram | Requests sharing one group-commit write and fsync |
| `dash_wal_checkpoint_duration_seconds` | histogram | Checkpoint duration (snapshot plus compaction) |
| `dash_wal_checkpoints_total`, `dash_wal_checkpoint_failures_total` | counter | Checkpoint outcomes |
| `dash_wal_checkpoint_last_success_timestamp_seconds` | gauge | Unix time of the last successful checkpoint (0 when none yet) |
| `dash_vector_index_save_duration_seconds`, `dash_vector_index_save_failures_total` | histogram / counter | Persisted vector index saves |
| `dash_vector_index_load_duration_seconds` | histogram | Vector index restore at startup (load plus WAL catch-up, or rebuild) |

### Embedding provider (ingestion and retrieval, network providers only)

| Family | Labels | Meaning |
|---|---|---|
| `dash_embedding_requests_total`, `dash_embedding_texts_total` | `provider` | Provider calls and texts embedded |
| `dash_embedding_request_duration_seconds` | `provider` | Call latency including retries and fast-failed breaker rejections |
| `dash_embedding_errors_total` | `provider`, `kind` | Failures by kind (`io`, `timeout`, `http_4xx`, `http_429`, `http_5xx`, `parse`, `dimension_mismatch`, `response_too_large`, `circuit_open`, `overloaded`, `invalid_config`) |
| `dash_embedding_breaker_state` | `provider` | 0 closed, 1 open, 2 half-open |
| `dash_embedding_breaker_consecutive_failures` | `provider` | Failures counted by the breaker |

`provider` is `ollama`, `openai` or `other`. The deterministic `hash` provider is local and not instrumented.

### Service-specific families

| Area | Metrics |
|---|---|
| Retrieval requests | `dash_http_request_duration_ms` (per-route histogram in milliseconds, kept for existing dashboards), `dash_retrieve_requests_total`, `dash_retrieve_success_total`, `dash_retrieve_client_error_total`, `dash_retrieve_server_error_total`, `dash_retrieve_latency_ms_p50` / `_p95` / `_p99`, `dash_retrieve_last_result_count` |
| Transport queues | `dash_retrieve_transport_queue_depth` / `_capacity` / `_full_reject_total`, `dash_ingest_transport_queue_*`, `dash_*_transport_read_error_total{status_class}` |
| Auth and audit | `dash_transport_auth_*`, `dash_ingest_auth_*`, `dash_*_authz_denied_total`, `dash_audit_records_total`, `dash_audit_write_failures_total`, `dash_audit_denials_dropped_total` |
| Storage visibility | `dash_retrieve_storage_*`, `dash_ingest_to_visible_lag_ms_p50` / `_p95` |
| Disk | `dash_disk_unavailable`, `dash_disk_recovering` |
| Placement | `dash_retrieve_placement_*`, `dash_ingest_placement_*` |
| Ingest | `dash_ingest_claims_total`, `dash_ingest_success_total`, `dash_ingest_failed_total`, `dash_ingest_batch_*`, `dash_ingest_delete_*` |
| WAL (ingestion) | `dash_ingest_wal_poisoned`, `dash_ingest_wal_write_failing`, `dash_ingest_wal_unsynced_records`, `dash_ingest_wal_flush_*`, `dash_ingest_wal_group_commit_*` |
| Replication (retrieval follower) | `dash_retrieval_replication_ready`, `_lag_records`, `_lag_seconds` (time since the follower was last caught up; 0 while caught up), `_last_success_age_ms`, `_consecutive_failures`, `_failures_total`, `_resyncs_total`, `_generation_switches_total`, `_export_bytes_total`, `_applied_records_total`, `_blocked_*` |
| Replication (ingestion) | `dash_ingest_replication_*` (follower offsets, pulls, lag, resyncs; leader exports and commit status) |
| Control plane | `dash_control_plane_is_leader`, `dash_control_plane_leader_epoch`, `dash_control_plane_leader_lease_remaining_seconds`, `dash_control_plane_placement_epoch`, `dash_control_plane_placements`, `dash_control_plane_state_error` |

Replication lag is reported in records and in seconds; offsets count records, so there is no lag-in-bytes figure.

## Prometheus scrape config

```yaml
scrape_configs:
  - job_name: dash-retrieval
    static_configs:
      - targets: [dash-retrieval:8080]
    authorization:
      credentials: <a retrieval key or JWT holding the read_only role>
  - job_name: dash-ingestion
    static_configs:
      - targets: [dash-ingestion:8081]
    authorization:
      credentials: <an ingestion key or JWT holding the read_only role>
  - job_name: dash-control-plane
    static_configs:
      - targets: [dash-control-plane:8090]
    authorization:
      credentials: <DASH_CONTROL_PLANE_TOKEN>
```

Keep `dash` in the job names: the alert rules select targets with `job=~".*dash.*"`.

On Kubernetes with the Prometheus Operator, the Helm chart renders the scrape configuration, rules and dashboards (all off by default):

```sh
helm upgrade --install dash deploy/helm/dash ... \
  --set metrics.serviceMonitor.enabled=true \
  --set metrics.serviceMonitor.labels.release=kube-prometheus-stack \
  --set metrics.serviceMonitor.authorization.retrieval.secretName=dash-scrape \
  --set metrics.serviceMonitor.authorization.retrieval.key=retrieval \
  --set metrics.serviceMonitor.authorization.ingestion.secretName=dash-scrape \
  --set metrics.serviceMonitor.authorization.ingestion.key=ingestion \
  --set metrics.prometheusRule.enabled=true \
  --set metrics.prometheusRule.runbookBaseUrl=https://github.com/<org>/<repo>/blob/<ref>/ \
  --set metrics.grafanaDashboards.enabled=true
```

The control plane is scraped with its own token by default. With `networkPolicy.enabled`, `metrics.networkPolicy.from` (default: the `monitoring` namespace) is allowed to reach the pods' HTTP port.

## Alerts, runbooks, SLOs and dashboards

Everything lives in `deploy/observability/`:

* `prometheus/dash-alerts.rules.yml`: 21 alerts (availability, latency, error-budget burn, WAL, disk, checkpoints, replication, audit, embedding breaker, load shedding, file descriptors). Each has `annotations.runbook_url` pointing at its runbook in [`docs/operations/runbooks/`](https://github.com/bhaweshbhaskar/dash/tree/main/docs/operations/runbooks).
* `prometheus/dash-recording.rules.yml`: request rates, latency quantiles and the SLIs of [`docs/operations/slos.md`](https://github.com/bhaweshbhaskar/dash/blob/main/docs/operations/slos.md).
* `prometheus/tests/`: `promtool test rules` unit tests; every alert has a firing and a quiet case.
* `grafana/`: *DASH / Overview*, *DASH / Ingestion and WAL* and *DASH / Retrieval, embeddings and replication* (Grafana 10+, Prometheus data source chosen through the `datasource` variable).

`scripts/check_observability.sh` (run by CI) downloads a pinned, checksum-verified `promtool`, checks and tests the rules, checks that every alert has an existing runbook, that every metric the rules and dashboards query is emitted by the code, that every dashboard query parses, and that the copies bundled in the Helm chart match. The disk-space alert needs kubelet volume stats (Kubernetes) or node_exporter (systemd hosts).

## Request correlation

Every request has a request id:

* a client (or proxy) may send `X-Request-Id`; it is kept when it is 1 to 128 characters of letters, digits and `-_.:/+=@`, otherwise it is replaced;
* without a valid one the server generates a 32-character hex id;
* the id is returned in the `X-Request-Id` response header of every answer, added as the first field (`"request_id"`) of every JSON error body (including requests rejected while being read), stored in the audit record of the request, and attached to every log event emitted while the request is handled.

When a client reports a failure, search the logs and the audit log for the id.

## Logs

Services log through `tracing`. The default output is compact text; set `DASH_LOG_FORMAT=json` for JSON lines (the Helm chart sets `json` by default through `config.logFormat`). Verbosity follows `RUST_LOG` (default `info`); `DASH_LOG_LEVEL` does not exist.

Request handling runs inside an `http_request` span with `service`, `route`, `method` and `request_id` fields, so JSON log lines carry them under `span`. One `dash_access` event is emitted per completed request with `status` and `duration_ms`: at `debug` level for non-5xx answers (enable with `RUST_LOG=info,dash_access=debug`) and always at `warn` for 5xx. Some startup and background diagnostics are still written with `eprintln!` outside `tracing` and carry no request id. The field names above are stable; other log text may change.

## OpenTelemetry tracing (not implemented)

There is no OTLP export. The services run on a synchronous thread-per-request HTTP stack, and an OpenTelemetry exporter needs an async runtime and a batch processor that would be added to every binary; the request spans described above are the integration point. Planned shape (follow-up work):

* an optional `otel` cargo feature and `DASH_OTEL_EXPORTER_OTLP_ENDPOINT` setting that installs a `tracing-opentelemetry` layer only when configured (no cost when off);
* W3C `traceparent` propagation from the SDKs through ingestion and retrieval, and on replication pulls;
* child spans for authentication, embedding calls, index search, WAL append and fsync.
