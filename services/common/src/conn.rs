//! Environment-resolved HTTP admission settings shared by the retrieval and
//! ingestion servers. The server itself lives in `dash-http`; this module only
//! owns the env var names and their parsing.
//!
//! * `DASH_HTTP_REQUEST_TIMEOUT_MS`: whole-request deadline, measured from
//!   accept (default 10 s);
//! * `DASH_HTTP_FIRST_BYTE_TIMEOUT_MS`: how long a new connection may stay
//!   silent before it is closed (default 2 s);
//! * `DASH_HTTP_MAX_CONNS_PER_IP`: per-IP concurrent connection cap, `0`
//!   disables it (default 64).

use std::time::Duration;

/// Default time a new connection may stay silent before it is closed.
pub const DEFAULT_FIRST_BYTE_TIMEOUT_MS: u64 = 2_000;
/// Default per-IP concurrent connection cap (0 disables the cap).
pub const DEFAULT_MAX_CONNS_PER_IP: usize = 64;
/// Default whole-request read deadline.
pub const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 10_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConnConfig {
    pub first_byte_timeout: Duration,
    /// 0 disables the per-IP cap.
    pub max_per_ip: usize,
    /// Whole-request deadline, measured from accept.
    pub request_timeout: Duration,
}

impl ConnConfig {
    pub fn from_env() -> Self {
        let positive_ms = |name: &str, default: u64| {
            std::env::var(name)
                .ok()
                .and_then(|raw| raw.trim().parse::<u64>().ok())
                .filter(|v| *v > 0)
                .unwrap_or(default)
        };
        let max_per_ip = std::env::var("DASH_HTTP_MAX_CONNS_PER_IP")
            .ok()
            .and_then(|raw| raw.trim().parse::<usize>().ok())
            .unwrap_or(DEFAULT_MAX_CONNS_PER_IP);
        Self {
            first_byte_timeout: Duration::from_millis(positive_ms(
                "DASH_HTTP_FIRST_BYTE_TIMEOUT_MS",
                DEFAULT_FIRST_BYTE_TIMEOUT_MS,
            )),
            max_per_ip,
            request_timeout: Duration::from_millis(positive_ms(
                "DASH_HTTP_REQUEST_TIMEOUT_MS",
                DEFAULT_REQUEST_TIMEOUT_MS,
            )),
        }
    }
}
