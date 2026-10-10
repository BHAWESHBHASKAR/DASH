# Distributed tracing (OpenTelemetry)

DASH can export OpenTelemetry spans over OTLP so a request can be followed
across services: a retrieve or ingest call, the WAL work it causes, the
replication pull that carries the write to a follower, and the failover
heartbeat that reaches the control plane. Tracing is **off by default**.

* Protocol: OTLP over HTTP with protobuf bodies (`POST <endpoint>/v1/traces`,
  the collector's port 4318). gRPC (4317) and `http/json` are not supported.
* Context propagation: W3C Trace Context (`traceparent`, `tracestate`).
* Configuration: the standard `OTEL_*` environment variables plus
  `DASH_OTEL_ENABLED` and `DASH_OTEL_BACKGROUND_SAMPLE_RATIO`.

## Turning it on

Tracing is enabled when `DASH_OTEL_ENABLED` is true, or, when
`DASH_OTEL_ENABLED` is unset, when `OTEL_EXPORTER_OTLP_ENDPOINT` or
`OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` is set. `DASH_OTEL_ENABLED=0`,
`OTEL_SDK_DISABLED=true` or `OTEL_TRACES_EXPORTER=none` keep it off.

```sh
export OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4318
export OTEL_TRACES_SAMPLER=parentbased_traceidratio
export OTEL_TRACES_SAMPLER_ARG=0.1
```

At startup each service logs `distributed tracing enabled (OTLP/HTTP
protobuf)` with the collector's scheme, host and port, or, at `debug`, why
tracing is off. A configuration the exporter cannot use (an unsupported
protocol or exporter, an endpoint that is not an `http://` or `https://`
URL, an unreadable CA file) keeps tracing off with a log line; the service
still starts. Values that have a safe default (a bad sampler name or ratio,
a malformed header entry, a non-numeric batch setting) fall back with a
warning.

Deployment artifacts:

* Helm: `observability.tracing.*` (see `deploy/helm/dash/README.md`); it also
  adds `k8s.namespace.name` / `k8s.pod.name` resource attributes and, with
  `networkPolicy.enabled`, an egress rule to the collector.
* Kubernetes manifests: commented keys in `deploy/k8s/10-config.yaml` and the
  optional `deploy/k8s/optional/otlp-egress.yaml`.
* Compose: the `deploy/container/docker-compose.tracing.yml` overlay, with a
  `tracing-backend` profile that starts a local Jaeger.
* systemd: commented keys in `deploy/systemd/*.env.example`.

## Settings

| Variable | Default | Meaning |
|---|---|---|
| `DASH_OTEL_ENABLED` | unset | `1` on, `0` off; unset means "on when an endpoint is set" |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | unset | Collector base URL; `/v1/traces` is appended |
| `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` | `http://localhost:4318/v1/traces` | Full traces URL, used as is; wins over the base endpoint |
| `OTEL_EXPORTER_OTLP_HEADERS` / `..._TRACES_HEADERS` | unset | `name=value,...` request headers (collector credentials). Never logged |
| `OTEL_EXPORTER_OTLP_PROTOCOL` / `..._TRACES_PROTOCOL` | `http/protobuf` | Anything else keeps tracing off |
| `OTEL_EXPORTER_OTLP_TIMEOUT` / `..._TRACES_TIMEOUT` | `10000` ms | One export request |
| `OTEL_EXPORTER_OTLP_CERTIFICATE` / `..._TRACES_CERTIFICATE` | unset | Extra CA bundle for an `https://` collector |
| `OTEL_SERVICE_NAME` | `dash-ingestion`, `dash-retrieval`, `dash-control-plane` | `service.name` |
| `OTEL_RESOURCE_ATTRIBUTES` | unset | Extra resource attributes |
| `OTEL_TRACES_SAMPLER` | `parentbased_always_on` | `always_on`, `always_off`, `traceidratio`, `parentbased_always_on`, `parentbased_always_off`, `parentbased_traceidratio` |
| `OTEL_TRACES_SAMPLER_ARG` | `1` | Ratio for the `traceidratio` samplers |
| `DASH_OTEL_BACKGROUND_SAMPLE_RATIO` | `1` | Extra ratio for traces started by background work |
| `OTEL_BSP_SCHEDULE_DELAY` | `5000` ms | Longest wait before a batch is sent |
| `OTEL_BSP_MAX_QUEUE_SIZE` | `2048` | Spans buffered for export; when full, new spans are dropped |
| `OTEL_BSP_MAX_EXPORT_BATCH_SIZE` | `512` | Spans per export request |

The generated reference is the "Distributed tracing (OpenTelemetry)" section
of the configuration reference (`docs-site/docs/reference/configuration.md`).

### Sampling

The ratio sampler decides from the trace id, so every service keeps the same
traces. With a `parentbased_*` sampler a caller that sends a sampled
`traceparent` is always traced and one that sends an unsampled one never is;
the decision is propagated downstream either way. New traces started by
background work (each replication poll of a follower, each failover
heartbeat, a checkpoint that does not run inside a request) are additionally
thinned by `DASH_OTEL_BACKGROUND_SAMPLE_RATIO`: an idle follower polls
several times a second, so a value like `0.01` keeps the occasional poll
trace without flooding the backend. Request sampling is not affected.

## What is traced

### Server spans

Every HTTP request gets a server span named `<METHOD> <route>`, where
`route` is the same bounded label the Prometheus metrics use (`ingest`,
`retrieve`, `replication_wal`, `ingest_heartbeat`, ...; unknown paths are
`other`). It continues the caller's `traceparent` when one is valid and
starts a new trace otherwise. Attributes:

| Attribute | Value |
|---|---|
| `http.request.method` | folded method |
| `http.route` | route label |
| `http.response.status_code` | status |
| `dash.component` | `ingestion`, `retrieval` or `control-plane` |
| `dash.request_id` | the request's `X-Request-Id` |
| `error.type` | status code, on 5xx (the span status is then `ERROR`) |

Probe and scrape routes (`health`, `live`, `ready`, `metrics`) are not
traced.

### Internal spans

| Span | Where | Attributes |
|---|---|---|
| `retrieve.segment_prefilter` | retrieval: resolving the segment candidate set (may read segment files) | |
| `retrieve.plan` | retrieval: request planning (filters, prefilter, short circuit) | `dash.retrieve.short_circuit` |
| `retrieve.candidates` | lexical and vector candidate generation | `dash.retrieve.top_k`, `dash.retrieve.hybrid`, `dash.retrieve.candidates` |
| `retrieve.vector_search` | ANN / flat vector search inside candidate generation | `dash.retrieve.depth`, `dash.retrieve.candidates` |
| `retrieve.bm25` | BM25 scoring of the candidates | |
| `retrieve.vector_score` | exact vector similarity of the candidates (hybrid queries) | `dash.retrieve.candidates` |
| `retrieve.fuse` | signals, priors, hybrid blend and sort | `dash.retrieve.candidates`, `dash.retrieve.results` |
| `retrieve.graph` | evidence graph expansion (`return_graph`) | |
| `wal.append` | one WAL append unit (write plus the fsync the policy requires) | `dash.wal.pending_records` |
| `wal.fsync` | WAL `fdatasync` | |
| `wal.group_commit_wait` | a request waiting for its group-commit batch | |
| `wal.group_commit` | the committer thread writing one batch; child of the first traced request in the batch, linked to the others (up to 16 links) | `dash.wal.batch_entries`, `dash.wal.batch_bytes` |
| `checkpoint` | a whole checkpoint; child of the request that triggered it or a background root | `dash.checkpoint.snapshot_records`, `dash.checkpoint.truncated_wal_records` |
| `checkpoint.begin` / `.write` / `.finish` | rotation and copy, snapshot write (often on a background thread), publication | `dash.checkpoint.snapshot_records` |
| `replication.poll` | one follower poll (retrieval or ingestion follower); a background root | `dash.component`, `dash.replication.outcome` |
| `replication.resync` | a full resync from the leader's export | |
| `replication.apply` | applying one replicated frame | `dash.replication.records` |
| `replication.http_request` | client span of a replication pull, export chunk or ack | `http.request.method`, `http.response.status_code` |
| `failover.heartbeat` | one ingestion heartbeat to the control plane; a background root | |
| `control_plane.http_request` | client span of a control-plane call (heartbeat, placement fetch) | `http.request.method`, `http.response.status_code` |

### Propagation across services

Outgoing internal calls carry the current span's `traceparent` (and
`tracestate`): replication pulls, export chunks and commit acks from both
follower kinds, and every control-plane call made through the placement
client (failover heartbeats, placement fetches). The receiving service's
server span is the child of the caller's client span, so a single trace
shows, for example:

```text
replication.poll                          (retrieval follower)
├── replication.http_request  CLIENT      (retrieval follower)
│   └── GET replication_wal   SERVER      (ingestion leader)
└── replication.apply                     (retrieval follower)
    └── wal.append
        └── wal.fsync
```

Calls to third-party embedding providers do not receive `traceparent`.

### Bounds

* Spans are opened at fixed points, never per record, candidate or result.
* Each local scope (one request on one worker thread, one background tick,
  one batch on the committer thread) records at most 64 child spans; further
  spans are dropped and counted (`reason="span_limit"`).
* At most 16 attributes and 16 links per span.
* The export queue is bounded (`OTEL_BSP_MAX_QUEUE_SIZE`); when it is full
  spans are dropped and counted (`reason="queue_full"`). Request threads
  never wait for the exporter or the collector.

### What is never recorded

Spans never carry claim text, query text, evidence, document content,
tenant ids, entity names, API keys, JWTs, replication or control-plane
tokens, or request and response headers. Attribute keys are compile-time
constants and values are route labels, status codes, counts, sizes,
outcomes and the request id. The OTLP headers are sent to the collector
only and never appear in a log line (a malformed entry is reported by its
position, not its value). The integration test
`services/retrieval/tests/otel_tracing.rs` checks that no exported span
contains the query, claim text or tenant id it used.

## Logs

When tracing is on, the `http_request` log span of every request gains a
`trace_id` field (32 hex characters), next to `request_id`, so with
`DASH_LOG_FORMAT=json` every log line of a traced request carries the id of
its trace. Search the logs for the trace id from the tracing backend, or
the backend for the `dash.request_id` a client reported.

## Exporter health

While tracing is on, `/metrics` adds:

| Family | Type | Meaning |
|---|---|---|
| `dash_trace_spans_started_total` | counter | Sampled spans started |
| `dash_trace_spans_exported_total` | counter | Spans accepted by the collector |
| `dash_trace_spans_dropped_total{reason}` | counter | `queue_full`, `span_limit`, `export_failed` |
| `dash_trace_export_failures_total` | counter | Export requests that failed |

A failing collector is logged once, then every 100th consecutive failure,
and once more when export recovers. Failed batches are dropped, not retried.

## Overhead

* **Off:** every instrumented point is one relaxed atomic load and a
  branch. No ids, clocks, allocations or thread-local accesses.
* **On, not sampled:** a server span parses `traceparent` and generates a
  span id so the decision can be propagated; child spans are no-ops.
* **On, sampled:** each span reads the clock twice, allocates one small
  record, and is handed to a bounded channel with a non-blocking send. The
  protobuf encoding and the HTTP request happen on the `dash-otlp-export`
  thread. A traced single ingest with group commit records 5 spans
  (server, group-commit wait, batch, WAL append, fsync; a checkpoint it
  triggers adds 4) and a traced hybrid retrieve 8.

## Design notes

The services serve HTTP from a pool of std threads (`pkg/http`), not from an
async runtime. The exporter therefore uses a batch processor on its own
thread and a blocking HTTP client (`ureq` over rustls, already used by the
replication client) rather than the OpenTelemetry Rust SDK, whose OTLP
exporters pull in a protobuf code generator, a gRPC stack or an async HTTP
client. The trace context and spans live in `pkg/observe/src/trace.rs`
(no dependencies, so the storage crate can open spans) and the exporter in
`pkg/observe/src/otlp.rs` (feature `otlp`). The protobuf messages are the
stable `opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest`
subset spans need, encoded by hand and covered by a decode round-trip test.

Spans follow the request's worker thread. Work that crosses threads (the
group committer, background checkpoint writes) carries the context
explicitly, so its spans stay in the right trace.

## Limits

* OTLP/HTTP protobuf only; no gRPC, no `http/json`, no client certificates
  towards the collector (run a local collector or sidecar for mutual TLS).
* Head sampling only; no tail sampling in the services (use the collector's
  tail sampling processor).
* Failed export batches are dropped, not retried.
* The client SDKs do not create spans or send `traceparent`; a caller that
  wants its own trace continued must send the header itself.
* Embedding provider calls and authentication are not separate spans; their
  time is inside the request's server span.
