//! Cross-service shutdown signaling.
//!
//! A small helper that wraps the `signal-hook` crate to convert
//! SIGTERM and SIGINT into an `Arc<AtomicBool>` that the accept
//! loop can poll. The polling loop is the only sync-friendly way
//! to get graceful shutdown on a blocking `TcpListener` without
//! refactoring to a tokio runtime.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use signal_hook::consts::{SIGINT, SIGTERM};
use signal_hook::flag;

pub mod audit;
pub mod policy;

pub use policy::{
    AuthDecision, AuthPolicy, PolicyCell, RawAuthConfig, RouteClass, ServiceAuthEnv,
    TenantRateLimiter, spawn_sighup_reload,
};

pub struct ShutdownSignal {
    flag: Arc<AtomicBool>,
}

impl ShutdownSignal {
    /// Register SIGTERM and SIGINT handlers that set the flag.
    /// Returns the signal so the caller can poll it in the
    /// accept loop. Panics if signal registration fails (which
    /// would mean we can't terminate cleanly).
    pub fn install() -> Arc<Self> {
        let flag = Arc::new(AtomicBool::new(false));
        flag::register(SIGTERM, Arc::clone(&flag))
            .expect("install SIGTERM handler for graceful shutdown");
        flag::register(SIGINT, Arc::clone(&flag))
            .expect("install SIGINT handler for graceful shutdown");
        Arc::new(Self { flag })
    }

    pub fn is_triggered(&self) -> bool {
        self.flag.load(Ordering::Relaxed)
    }

    /// A signal that is only ever set by [`ShutdownSignal::trigger`] (no OS
    /// signal handlers). Lets in-process tests stop a server they started.
    pub fn manual() -> Arc<Self> {
        Arc::new(Self {
            flag: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Request shutdown, as if SIGTERM had been received.
    pub fn trigger(&self) {
        self.flag.store(true, Ordering::Relaxed);
    }
}

/// Poll the shutdown signal with a bounded sleep between checks.
/// The accept loop calls this between non-blocking accept() calls.
/// `poll_interval` caps the shutdown latency: a shorter interval
/// means faster shutdown at the cost of more wakeups; the default
/// of 50ms gives 50ms p99 shutdown latency, which is below the
/// typical k8s `terminationGracePeriodSeconds` of 30 seconds.
pub fn wait_or_shutdown(signal: &ShutdownSignal, poll_interval: Duration) -> bool {
    if signal.is_triggered() {
        return true;
    }
    std::thread::sleep(poll_interval);
    signal.is_triggered()
}

/// Logs and waits up to `graceful_deadline` for in-flight handlers
/// to finish. Returns the actual shutdown duration for logging.
pub fn wait_for_drain(graceful_deadline: Duration) -> Duration {
    let start = Instant::now();
    let mut elapsed = Duration::ZERO;
    while elapsed < graceful_deadline {
        std::thread::sleep(Duration::from_millis(50));
        elapsed = start.elapsed();
    }
    start.elapsed()
}

/// Substrings that mark a value as a documentation/template placeholder.
const PLACEHOLDER_SUBSTRINGS: &[&str] = &[
    "change-me",
    "change_me",
    "changeme",
    "placeholder",
    "replace-me",
    "replace_me",
    "example",
    "sample",
];

/// Values that are placeholders when they equal, or start with, the pattern.
const PLACEHOLDER_PREFIXES: &[&str] = &["secret", "password", "passw0rd"];

/// Minimum length for API keys, scoped keys and replication tokens.
pub const SECRET_MIN_LENGTH: usize = 16;

/// Minimum length for HS256 JWT signing secrets (RFC 7518 recommends a key at
/// least as long as the hash output, 256 bits).
pub const JWT_SECRET_MIN_LENGTH: usize = 32;

fn is_placeholder(lower: &str) -> bool {
    // `<generate-a-32-char-random-string>` style template markers.
    if lower.starts_with('<') && lower.ends_with('>') {
        return true;
    }
    if lower.contains('<') && lower.contains('>') {
        return true;
    }
    PLACEHOLDER_SUBSTRINGS
        .iter()
        .any(|pattern| lower.contains(pattern))
        || PLACEHOLDER_PREFIXES
            .iter()
            .any(|pattern| lower.starts_with(pattern))
}

/// Validate that a secret is non-empty, is not a known placeholder, and meets
/// the default minimum length ([`SECRET_MIN_LENGTH`]). The error never
/// contains the secret value.
pub fn validate_secret(value: &str, name: &str) -> Result<(), String> {
    validate_secret_min_len(value, name, SECRET_MIN_LENGTH)
}

/// Like [`validate_secret`] with an explicit minimum length.
pub fn validate_secret_min_len(value: &str, name: &str, min_len: usize) -> Result<(), String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(format!("{name} is empty"));
    }
    if is_placeholder(&trimmed.to_lowercase()) {
        return Err(format!("{name} appears to be a placeholder value"));
    }
    if trimmed.chars().count() < min_len {
        return Err(format!(
            "{name} is too short ({len} chars, minimum {min_len})",
            len = trimmed.chars().count(),
        ));
    }
    Ok(())
}

/// Validate a comma-separated list of secrets, skipping empty entries.
pub fn validate_secret_csv(values: Option<&str>, name: &str) -> Result<(), String> {
    validate_secret_csv_min_len(values, name, SECRET_MIN_LENGTH)
}

/// Like [`validate_secret_csv`] with an explicit minimum length.
pub fn validate_secret_csv_min_len(
    values: Option<&str>,
    name: &str,
    min_len: usize,
) -> Result<(), String> {
    if let Some(raw) = values {
        for part in raw.split(',') {
            let trimmed = part.trim();
            if !trimmed.is_empty() {
                validate_secret_min_len(trimmed, name, min_len)?;
            }
        }
    }
    Ok(())
}

fn env_flag(name: &str) -> Option<bool> {
    let raw = std::env::var(name).ok()?;
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

/// True when `DASH_INSECURE_DEV_MODE` is truthy. Dev mode is the only way to
/// run a service with no authentication configured and the only way to relax
/// secret validation. It must never be set in production.
pub fn insecure_dev_mode_enabled() -> bool {
    env_flag("DASH_INSECURE_DEV_MODE").unwrap_or(false)
}

/// Strict secret validation is ON by default. It can only be turned off by
/// setting `DASH_STRICT_SECRETS=0` *and* `DASH_INSECURE_DEV_MODE=1`.
pub fn strict_secrets_enabled() -> bool {
    strict_secrets_from(env_flag("DASH_STRICT_SECRETS"), insecure_dev_mode_enabled())
}

fn strict_secrets_from(strict_flag: Option<bool>, dev_mode: bool) -> bool {
    !(strict_flag == Some(false) && dev_mode)
}

/// Result of applying the dev-mode bind policy to a requested bind address.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BindDecision {
    pub addr: String,
    /// Set when the requested host was replaced by `127.0.0.1`.
    pub overridden: bool,
}

fn host_is_loopback(host: &str) -> bool {
    let host = host.trim().trim_start_matches('[').trim_end_matches(']');
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
}

/// Pure bind policy. In dev mode (no authentication may be configured) the
/// service only listens on loopback: any other host is replaced by
/// `127.0.0.1`, keeping the port, unless `allow_non_loopback` is set. Outside
/// dev mode the requested address is used unchanged.
pub fn bind_for_mode(requested: &str, dev_mode: bool, allow_non_loopback: bool) -> BindDecision {
    let requested = requested.trim();
    if !dev_mode || allow_non_loopback {
        return BindDecision {
            addr: requested.to_string(),
            overridden: false,
        };
    }
    let (host, port) = match requested.rsplit_once(':') {
        Some((host, port)) => (host, port),
        None => (requested, "0"),
    };
    if host_is_loopback(host) {
        BindDecision {
            addr: requested.to_string(),
            overridden: false,
        }
    } else {
        BindDecision {
            addr: format!("127.0.0.1:{port}"),
            overridden: true,
        }
    }
}

/// Apply [`bind_for_mode`] using `DASH_INSECURE_DEV_MODE` and
/// `DASH_INSECURE_DEV_MODE_ALLOW_NON_LOOPBACK`, logging a warning when the
/// requested address is overridden.
pub fn resolve_bind_addr(requested: &str) -> String {
    let dev = insecure_dev_mode_enabled();
    let allow = env_flag("DASH_INSECURE_DEV_MODE_ALLOW_NON_LOOPBACK").unwrap_or(false);
    let decision = bind_for_mode(requested, dev, allow);
    if decision.overridden {
        tracing::warn!(
            "DASH_INSECURE_DEV_MODE=1: refusing to bind to non-loopback address '{requested}'; \
             binding to {} instead (set DASH_INSECURE_DEV_MODE_ALLOW_NON_LOOPBACK=1 to override)",
            decision.addr
        );
    } else if dev && allow {
        tracing::warn!(
            "DASH_INSECURE_DEV_MODE=1 with DASH_INSECURE_DEV_MODE_ALLOW_NON_LOOPBACK=1: \
             binding to {requested} with relaxed security"
        );
    }
    decision.addr
}

/// `bytes` random bytes from the operating system CSPRNG, as lower-case hex.
/// Used for unguessable file-name suffixes and per-process instance ids.
pub fn random_hex(bytes: usize) -> String {
    use rand::RngCore;
    let mut buf = vec![0u8; bytes];
    rand::rngs::OsRng.fill_bytes(&mut buf);
    audit::hex_lower(&buf)
}

/// Constant-time byte equality. Runs in time proportional to the longer input
/// regardless of where (or whether) the inputs differ.
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    let mut diff = a.len() ^ b.len();
    let len = a.len().max(b.len());
    for i in 0..len {
        let x = a.get(i).copied().unwrap_or(0);
        let y = b.get(i).copied().unwrap_or(0);
        diff |= usize::from(x ^ y);
    }
    std::hint::black_box(diff) == 0
}

/// Read `primary`, falling back to the legacy `fallback` variable.
pub fn env_with_fallback(primary: &str, fallback: &str) -> Option<String> {
    std::env::var(primary)
        .ok()
        .or_else(|| std::env::var(fallback).ok())
}

/// Initialize a `tracing` subscriber for the service.
///
/// When `DASH_LOG_FORMAT` is `json`, events are emitted as JSON lines;
/// otherwise a compact human-readable format is used. The default
/// `RUST_LOG` filter is applied from the environment.
pub fn init_logging() {
    let json_logs = std::env::var("DASH_LOG_FORMAT")
        .unwrap_or_default()
        .eq_ignore_ascii_case("json");
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    // Ignore "already initialized" so tests that spawn multiple service
    // binaries in the same process do not panic.
    if json_logs {
        let _ = tracing_subscriber::fmt()
            .json()
            .with_env_filter(env_filter)
            .try_init();
    } else {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .try_init();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strict_secrets_default_on_and_opt_out_requires_dev_mode() {
        assert!(strict_secrets_from(None, false));
        assert!(strict_secrets_from(None, true));
        assert!(strict_secrets_from(Some(true), true));
        assert!(strict_secrets_from(Some(false), false));
        assert!(!strict_secrets_from(Some(false), true));
    }

    #[test]
    fn dev_mode_forces_loopback_bind_unless_explicitly_allowed() {
        let d = bind_for_mode("0.0.0.0:8080", true, false);
        assert_eq!(d.addr, "127.0.0.1:8080");
        assert!(d.overridden);
        assert_eq!(
            bind_for_mode("[::]:9000", true, false).addr,
            "127.0.0.1:9000"
        );
        assert_eq!(
            bind_for_mode("10.1.2.3:81", true, false).addr,
            "127.0.0.1:81"
        );
        assert_eq!(bind_for_mode("myhost:81", true, false).addr, "127.0.0.1:81");
        for loopback in [
            "127.0.0.1:8080",
            "localhost:8080",
            "[::1]:8080",
            "127.0.0.2:1",
        ] {
            let d = bind_for_mode(loopback, true, false);
            assert_eq!(d.addr, loopback);
            assert!(!d.overridden);
        }
        // allowed explicitly, or not in dev mode: unchanged
        assert_eq!(
            bind_for_mode("0.0.0.0:8080", true, true).addr,
            "0.0.0.0:8080"
        );
        assert_eq!(
            bind_for_mode("0.0.0.0:8080", false, false).addr,
            "0.0.0.0:8080"
        );
    }

    #[test]
    fn placeholders_are_rejected_without_echoing_the_value() {
        for bad in [
            "<generate-a-32-char-random-string>",
            "<your-key-here-please-0123456789>",
            "change-me-retrieval-key",
            "changeme-changeme-changeme",
            "my-example-api-key-0123456789",
            "secret",
            "password-password-password",
        ] {
            let err = validate_secret(bad, "X").unwrap_err();
            assert!(!err.contains(bad), "leaked: {err}");
            assert!(err.contains("placeholder"), "{err}");
        }
        assert!(validate_secret("a8f3b1c9d2e47f60", "X").is_ok());
        assert!(validate_secret_min_len("a8f3b1c9d2e47f60", "X", 32).is_err());
    }
}
