use std::time::Duration;

use crate::response::Response;

/// How the parser treats an `Expect` request header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpectPolicy {
    /// Any `Expect` header is answered with 417 before the body is read.
    Reject,
    /// `Expect: 100-continue` is acknowledged with `100 Continue` when the
    /// body has not fully arrived; other values are ignored.
    Continue100,
}

/// Everything the server and parser need, resolved by the owning service
/// (env var names and parsing stay in the service).
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Service name used as the prefix of log lines.
    pub name: &'static str,
    /// General worker threads.
    pub workers: usize,
    /// Accepted-but-unserved connections buffered before new ones get 503.
    pub queue_capacity: usize,
    /// Workers reserved for health-class requests. Zero disables the health
    /// lane (and the request-line peek) entirely.
    pub health_workers: usize,
    /// Capacity of the health lane queue (capped by `queue_capacity`).
    pub health_queue_capacity: usize,
    /// Maximum length of one header line, request line included.
    pub max_header_line_bytes: usize,
    /// Maximum size of the request line plus all headers.
    pub max_header_block_bytes: usize,
    /// Maximum number of header fields.
    pub max_header_count: usize,
    /// Maximum accepted `Content-Length`.
    pub max_body_bytes: usize,
    /// Whole-request deadline, measured from accept (queue wait counts).
    pub request_deadline: Duration,
    /// How long a new connection may stay silent before it is closed
    /// (health lane peek only).
    pub first_byte_timeout: Duration,
    /// Optional cap on any single read, on top of the whole-request deadline.
    pub read_timeout: Option<Duration>,
    /// Maximum time to wait for any single write.
    pub write_timeout: Duration,
    /// Write timeout for the 503 sent to rejected connections.
    pub reject_write_timeout: Duration,
    /// Per-IP concurrent connection cap (0 disables it).
    pub max_conns_per_ip: usize,
    /// Hard bound on connections waiting for their first bytes.
    pub max_pending: usize,
    /// Most bytes discarded while draining after an early error response.
    pub linger_max_bytes: usize,
    /// Longest time spent draining after an early error response.
    pub linger_max_time: Duration,
    /// `Expect` handling.
    pub expect: ExpectPolicy,
    /// Response sent when a connection is shed (queue full, per-IP cap).
    pub overload_response: Response,
}

impl ServerConfig {
    /// Defaults shared by retrieval and ingestion.
    pub fn new(name: &'static str, workers: usize, queue_capacity: usize) -> Self {
        Self {
            name,
            workers: workers.max(1),
            queue_capacity: queue_capacity.max(1),
            health_workers: 2,
            health_queue_capacity: 64,
            max_header_line_bytes: 8 * 1024,
            max_header_block_bytes: 32 * 1024,
            max_header_count: 100,
            max_body_bytes: 16 * 1024 * 1024,
            request_deadline: Duration::from_millis(10_000),
            first_byte_timeout: Duration::from_millis(2_000),
            read_timeout: None,
            write_timeout: Duration::from_secs(5),
            reject_write_timeout: Duration::from_secs(5),
            max_conns_per_ip: 64,
            max_pending: 4096,
            linger_max_bytes: 1024 * 1024,
            linger_max_time: Duration::from_millis(300),
            expect: ExpectPolicy::Reject,
            overload_response: Response::error(
                503,
                &format!("service unavailable: {name} worker queue full"),
            ),
        }
    }

    /// Control-plane defaults: 16 KiB header block, 8 MiB body, per-read
    /// timeout, no health lane and no per-IP cap.
    pub fn control_plane(workers: usize, queue_capacity: usize) -> Self {
        Self {
            health_workers: 0,
            max_header_line_bytes: 16 * 1024,
            max_header_block_bytes: 16 * 1024,
            max_body_bytes: 8 * 1024 * 1024,
            read_timeout: Some(Duration::from_secs(5)),
            reject_write_timeout: Duration::from_secs(1),
            max_conns_per_ip: 0,
            expect: ExpectPolicy::Continue100,
            overload_response: Response::error(503, "control-plane is overloaded")
                .with_header("Retry-After", "1"),
            ..Self::new("control-plane", workers, queue_capacity)
        }
    }
}
