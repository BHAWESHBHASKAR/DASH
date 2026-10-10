//! Observability building blocks shared by the DASH services.
//!
//! * [`histogram`]: lock-free fixed-bucket histograms rendered as Prometheus
//!   `histogram` families (cumulative `_bucket{le=...}`, `_sum`, `_count`).
//! * [`exposition`]: a small writer for the Prometheus text format (one
//!   `# HELP` / `# TYPE` header per family, escaped label values) and a strict
//!   validator used by tests to prove that every `/metrics` body parses.
//! * [`process`]: process metrics (resident memory, open file descriptors,
//!   CPU time, threads, start time, uptime) and the `dash_build_info` gauge.
//! * `http` (feature `http`, on by default): instrumentation for `dash-http`
//!   handlers: per-route request counters by status code, latency histograms,
//!   an in-flight gauge and a request-scoped tracing span carrying the
//!   request id; with tracing on, the request also gets an OpenTelemetry
//!   server span (W3C `traceparent` in, route/status/request id attributes).
//! * [`trace`]: the distributed tracing core (W3C Trace Context parsing and
//!   propagation, samplers, thread-scoped and detached spans). Always
//!   compiled, dependency-free, and a single atomic load per instrumented
//!   point while tracing is off.
//! * `otlp` (feature `otlp`): the OTLP/HTTP protobuf exporter (batch thread,
//!   blocking client) configured from the standard `OTEL_*` variables.
//!
//! Label cardinality is bounded by construction: route labels come from a
//! per-service classifier that maps every path to a fixed set of names,
//! methods are folded to a fixed set, and no tenant, claim or credential
//! value is ever used as a label.

pub mod exposition;
pub mod histogram;
#[cfg(feature = "http")]
pub mod http;
#[cfg(feature = "otlp")]
pub mod otlp;
pub mod process;
pub mod trace;

pub use exposition::{MetricKind, MetricsWriter, ValidationReport, validate};
pub use histogram::{
    BATCH_SIZE_BUCKETS, FSYNC_SECONDS_BUCKETS, Histogram, LATENCY_SECONDS_BUCKETS,
    SLOW_OPERATION_SECONDS_BUCKETS,
};
