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
//!   request id.
//!
//! Label cardinality is bounded by construction: route labels come from a
//! per-service classifier that maps every path to a fixed set of names,
//! methods are folded to a fixed set, and no tenant, claim or credential
//! value is ever used as a label.

pub mod exposition;
pub mod histogram;
#[cfg(feature = "http")]
pub mod http;
pub mod process;

pub use exposition::{MetricKind, MetricsWriter, ValidationReport, validate};
pub use histogram::{
    BATCH_SIZE_BUCKETS, FSYNC_SECONDS_BUCKETS, Histogram, LATENCY_SECONDS_BUCKETS,
    SLOW_OPERATION_SECONDS_BUCKETS,
};
