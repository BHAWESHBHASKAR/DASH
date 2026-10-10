//! Control-plane server settings. Request parsing and the worker pool live in
//! `dash-http`; this module keeps the control-plane env var names and defaults
//! and maps them onto the shared configuration.

use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerConfig {
    /// Maximum size of the request line plus headers.
    pub max_header_bytes: usize,
    /// Maximum accepted `Content-Length`.
    pub max_body_bytes: usize,
    /// Maximum time to wait for any single read.
    pub read_timeout: Duration,
    /// Maximum total time to receive one complete request (slowloris guard).
    pub request_deadline: Duration,
    /// Maximum time to wait for any single write.
    pub write_timeout: Duration,
    /// Number of worker threads handling connections.
    pub workers: usize,
    /// Accepted-but-unserved connections buffered before new ones get 503.
    pub queue_depth: usize,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            max_header_bytes: 16 * 1024,
            max_body_bytes: 8 * 1024 * 1024,
            read_timeout: Duration::from_secs(5),
            request_deadline: Duration::from_secs(10),
            write_timeout: Duration::from_secs(5),
            workers: 8,
            queue_depth: 64,
        }
    }
}

impl ServerConfig {
    /// Defaults with environment overrides: `DASH_CONTROL_PLANE_WORKERS`,
    /// `DASH_CONTROL_PLANE_QUEUE_DEPTH`, `DASH_CONTROL_PLANE_MAX_BODY_BYTES`,
    /// `DASH_CONTROL_PLANE_READ_TIMEOUT_MS`,
    /// `DASH_CONTROL_PLANE_REQUEST_DEADLINE_MS`,
    /// `DASH_CONTROL_PLANE_WRITE_TIMEOUT_MS`.
    pub fn from_env() -> Self {
        let mut config = Self::default();
        let num = |name: &str| {
            std::env::var(name)
                .ok()
                .and_then(|value| value.trim().parse::<u64>().ok())
                .filter(|value| *value > 0)
        };
        if let Some(value) = num("DASH_CONTROL_PLANE_WORKERS") {
            config.workers = value as usize;
        }
        if let Some(value) = num("DASH_CONTROL_PLANE_QUEUE_DEPTH") {
            config.queue_depth = value as usize;
        }
        if let Some(value) = num("DASH_CONTROL_PLANE_MAX_BODY_BYTES") {
            config.max_body_bytes = value as usize;
        }
        if let Some(value) = num("DASH_CONTROL_PLANE_READ_TIMEOUT_MS") {
            config.read_timeout = Duration::from_millis(value);
        }
        if let Some(value) = num("DASH_CONTROL_PLANE_REQUEST_DEADLINE_MS") {
            config.request_deadline = Duration::from_millis(value);
        }
        if let Some(value) = num("DASH_CONTROL_PLANE_WRITE_TIMEOUT_MS") {
            config.write_timeout = Duration::from_millis(value);
        }
        config
    }
}

impl ServerConfig {
    /// The equivalent shared server configuration: no health lane and no
    /// per-IP cap, `Expect: 100-continue` acknowledged.
    pub(crate) fn to_http(&self) -> dash_http::ServerConfig {
        let mut config = dash_http::ServerConfig::control_plane(self.workers, self.queue_depth);
        config.max_header_block_bytes = self.max_header_bytes;
        config.max_header_line_bytes = self.max_header_bytes;
        config.max_body_bytes = self.max_body_bytes;
        config.read_timeout = Some(self.read_timeout);
        config.request_deadline = self.request_deadline;
        config.write_timeout = self.write_timeout;
        config
    }
}

/// Offset of the blank line ending the head (test helper for raw clients).
#[cfg(test)]
pub(crate) fn find_header_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|window| window == b"\r\n\r\n")
}
