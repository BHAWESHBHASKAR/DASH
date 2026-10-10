//! Bounded, retrying HTTP(S) JSON client used by the network providers.
//!
//! Built on `ureq` with rustls so https works, chunked and content-length
//! bodies are decoded by a real HTTP implementation, redirects are never
//! followed (so credentials cannot be forwarded to another host), and every
//! response body is read through a hard size cap.

use std::io::Read;
use std::net::IpAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use url::{Host, Url};

use crate::EmbeddingError;

/// Environment variable that permits sending credentials over plaintext
/// http:// to non-loopback hosts. Intended only for trusted private networks.
pub const ALLOW_INSECURE_HTTP_ENV: &str = "DASH_EMBEDDING_ALLOW_INSECURE_HTTP";

/// Maximum number of characters of an upstream error body kept in errors.
const ERROR_SNIPPET_CHARS: usize = 300;
/// Maximum number of bytes read from an upstream error body.
const ERROR_BODY_READ_BYTES: u64 = 4096;
/// Upper bound honored for a `Retry-After` header.
const MAX_RETRY_AFTER: Duration = Duration::from_secs(30);

/// Transport tuning shared by the network providers.
#[derive(Debug, Clone)]
pub struct HttpOptions {
    /// Connect timeout per attempt (also capped by the remaining deadline).
    pub connect_timeout: Duration,
    /// Total deadline across all attempts, including backoff sleeps.
    pub total_timeout: Duration,
    /// Maximum accepted response body size in bytes.
    pub max_response_bytes: usize,
    /// Number of retries after the first attempt (429/5xx/connect errors).
    pub max_retries: u32,
    /// Base delay for exponential backoff.
    pub backoff_base: Duration,
    /// Cap for a single backoff delay.
    pub backoff_max: Duration,
}

impl HttpOptions {
    pub const DEFAULT_MAX_RESPONSE_BYTES: usize = 32 * 1024 * 1024;

    pub fn with_total_timeout(total_timeout: Duration) -> Self {
        Self {
            connect_timeout: Duration::from_secs(5),
            total_timeout,
            max_response_bytes: Self::DEFAULT_MAX_RESPONSE_BYTES,
            max_retries: 2,
            backoff_base: Duration::from_millis(200),
            backoff_max: Duration::from_secs(5),
        }
    }
}

/// True for loopback destinations: `localhost`, 127.0.0.0/8 and `::1`.
pub fn is_loopback_host(host: &Host<&str>) -> bool {
    match host {
        Host::Domain(d) => {
            let d = d.trim_end_matches('.').to_ascii_lowercase();
            d == "localhost" || d.ends_with(".localhost")
        }
        Host::Ipv4(ip) => IpAddr::V4(*ip).is_loopback(),
        Host::Ipv6(ip) => IpAddr::V6(*ip).is_loopback(),
    }
}

/// Parse an endpoint URL, accepting only http and https with a host.
pub fn parse_endpoint(raw: &str) -> Result<Url, EmbeddingError> {
    let url = Url::parse(raw.trim())
        .map_err(|e| EmbeddingError::InvalidConfig(format!("invalid endpoint url: {e}")))?;
    match url.scheme() {
        "http" | "https" => {}
        other => {
            return Err(EmbeddingError::InvalidConfig(format!(
                "unsupported endpoint scheme '{other}' (expected http or https)"
            )));
        }
    }
    if url.host().is_none() {
        return Err(EmbeddingError::InvalidConfig(
            "endpoint url has no host".to_string(),
        ));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(EmbeddingError::InvalidConfig(
            "endpoint url must not embed credentials".to_string(),
        ));
    }
    Ok(url)
}

/// Refuse to send credentials over plaintext http:// to non-loopback hosts
/// unless `allow_insecure` is set.
pub fn ensure_secure_transport(
    url: &Url,
    carries_credentials: bool,
    allow_insecure: bool,
) -> Result<(), EmbeddingError> {
    if !carries_credentials || url.scheme() == "https" || allow_insecure {
        return Ok(());
    }
    match url.host() {
        Some(host) if is_loopback_host(&host) => Ok(()),
        _ => Err(EmbeddingError::InvalidConfig(format!(
            "refusing to send an API key over plaintext http to non-loopback host '{}'; \
             use https or set {ALLOW_INSECURE_HTTP_ENV}=1 to override",
            url.host_str().unwrap_or("?")
        ))),
    }
}

pub fn insecure_http_allowed_from_env() -> bool {
    std::env::var(ALLOW_INSECURE_HTTP_ENV)
        .map(|v| v.trim() == "1")
        .unwrap_or(false)
}

/// Produce a short, single-line, secret-free snippet of an upstream body.
pub fn sanitize_snippet(raw: &[u8], secrets: &[&str]) -> String {
    let mut text = String::from_utf8_lossy(raw).into_owned();
    for secret in secrets {
        if !secret.is_empty() {
            text = text.replace(secret, "[redacted]");
        }
    }
    text = redact_bearer(&text);
    let cleaned: String = text
        .chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect();
    let cleaned = cleaned.trim();
    if cleaned.chars().count() > ERROR_SNIPPET_CHARS {
        let mut out: String = cleaned.chars().take(ERROR_SNIPPET_CHARS).collect();
        out.push_str("...(truncated)");
        out
    } else {
        cleaned.to_string()
    }
}

fn redact_bearer(text: &str) -> String {
    let lower = text.to_ascii_lowercase();
    let mut out = String::with_capacity(text.len());
    let mut idx = 0;
    while let Some(pos) = lower[idx..].find("bearer ") {
        let token_start = idx + pos + "bearer ".len();
        out.push_str(&text[idx..token_start]);
        let token_len = text[token_start..]
            .find(|c: char| c.is_whitespace() || c == '"' || c == '\'' || c == ',')
            .unwrap_or(text.len() - token_start);
        out.push_str("[redacted]");
        idx = token_start + token_len;
    }
    out.push_str(&text[idx..]);
    out
}

/// Parse a `Retry-After` header given in delta-seconds.
pub fn parse_retry_after(value: &str) -> Option<Duration> {
    value
        .trim()
        .parse::<u64>()
        .ok()
        .map(Duration::from_secs)
        .map(|d| d.min(MAX_RETRY_AFTER))
}

pub fn is_retryable_status(status: u16) -> bool {
    matches!(status, 429 | 500 | 502 | 503 | 504)
}

fn jitter_fraction() -> f64 {
    static COUNTER: AtomicU64 = AtomicU64::new(0x9e37_79b9_7f4a_7c15);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64)
        .unwrap_or(0);
    let mut x = COUNTER
        .fetch_add(0x9e37_79b9_7f4a_7c15, Ordering::Relaxed)
        .wrapping_add(nanos);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^= x >> 31;
    // [0.5, 1.0): "equal jitter"
    0.5 + (x >> 11) as f64 / (1u64 << 53) as f64 * 0.5
}

/// Jittered exponential backoff for the given zero-based retry number.
pub fn backoff_delay(opts: &HttpOptions, retry: u32) -> Duration {
    let factor = 1u32.checked_shl(retry.min(16)).unwrap_or(u32::MAX);
    let raw = opts
        .backoff_base
        .saturating_mul(factor)
        .min(opts.backoff_max);
    raw.mul_f64(jitter_fraction())
}

enum Failure {
    Retryable {
        error: EmbeddingError,
        retry_after: Option<Duration>,
    },
    Fatal(EmbeddingError),
}

fn io_kind_in_chain(err: &(dyn std::error::Error + 'static)) -> Option<std::io::ErrorKind> {
    let mut cur: Option<&(dyn std::error::Error + 'static)> = Some(err);
    while let Some(e) = cur {
        if let Some(io) = e.downcast_ref::<std::io::Error>() {
            return Some(io.kind());
        }
        cur = e.source();
    }
    None
}

fn is_timeout_kind(kind: std::io::ErrorKind) -> bool {
    matches!(
        kind,
        std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock
    )
}

fn classify_transport(err: ureq::Transport, opts: &HttpOptions, secrets: &[&str]) -> Failure {
    let timed_out = io_kind_in_chain(&err).map(is_timeout_kind).unwrap_or(false);
    if timed_out {
        return Failure::Fatal(EmbeddingError::Timeout(opts.total_timeout.as_secs()));
    }
    let error = EmbeddingError::Io(sanitize_snippet(err.to_string().as_bytes(), secrets));
    match err.kind() {
        ureq::ErrorKind::Dns | ureq::ErrorKind::ConnectionFailed | ureq::ErrorKind::Io => {
            Failure::Retryable {
                error,
                retry_after: None,
            }
        }
        _ => Failure::Fatal(error),
    }
}

fn attempt(
    url: &Url,
    body: &str,
    headers: &[(&str, &str)],
    opts: &HttpOptions,
    remaining: Duration,
    secrets: &[&str],
) -> Result<String, Failure> {
    let agent = ureq::AgentBuilder::new()
        .redirects(0)
        .timeout_connect(opts.connect_timeout.min(remaining))
        .timeout(remaining)
        .user_agent("dash-embeddings/0.1")
        .build();
    let mut request = agent
        .post(url.as_str())
        .set("Content-Type", "application/json")
        .set("Accept", "application/json");
    for (k, v) in headers {
        request = request.set(k, v);
    }
    let response = match request.send_string(body) {
        Ok(r) => r,
        Err(ureq::Error::Status(status, resp)) => {
            return Err(status_failure(status, resp, secrets));
        }
        Err(ureq::Error::Transport(t)) => return Err(classify_transport(t, opts, secrets)),
    };
    let status = response.status();
    if !(200..300).contains(&status) {
        // Redirects are disabled, so 3xx lands here and is reported as an error.
        return Err(status_failure(status, response, secrets));
    }
    if let Some(len) = response
        .header("content-length")
        .and_then(|v| v.trim().parse::<u64>().ok())
        && len > opts.max_response_bytes as u64
    {
        return Err(Failure::Fatal(EmbeddingError::ResponseTooLarge {
            limit: opts.max_response_bytes,
        }));
    }
    let mut buf = Vec::new();
    let mut reader = response
        .into_reader()
        .take(opts.max_response_bytes as u64 + 1);
    if let Err(e) = reader.read_to_end(&mut buf) {
        return Err(if is_timeout_kind(e.kind()) {
            Failure::Fatal(EmbeddingError::Timeout(opts.total_timeout.as_secs()))
        } else {
            Failure::Retryable {
                error: EmbeddingError::Io(format!("reading response: {e}")),
                retry_after: None,
            }
        });
    }
    if buf.len() > opts.max_response_bytes {
        return Err(Failure::Fatal(EmbeddingError::ResponseTooLarge {
            limit: opts.max_response_bytes,
        }));
    }
    String::from_utf8(buf)
        .map_err(|e| Failure::Fatal(EmbeddingError::Parse(format!("response is not utf-8: {e}"))))
}

fn status_failure(status: u16, resp: ureq::Response, secrets: &[&str]) -> Failure {
    let retry_after = resp.header("retry-after").and_then(parse_retry_after);
    let mut raw = Vec::new();
    let _ = resp
        .into_reader()
        .take(ERROR_BODY_READ_BYTES)
        .read_to_end(&mut raw);
    let error = EmbeddingError::Http {
        status,
        body: sanitize_snippet(&raw, secrets),
        retry_after_secs: retry_after.map(|d| d.as_secs()),
    };
    if is_retryable_status(status) {
        Failure::Retryable { error, retry_after }
    } else {
        Failure::Fatal(error)
    }
}

/// POST a JSON body with bounded retries. Only use for idempotent calls.
///
/// `secrets` are values (API keys) that must never appear in returned errors.
pub fn post_json(
    url: &Url,
    body: &str,
    headers: &[(&str, &str)],
    opts: &HttpOptions,
    secrets: &[&str],
) -> Result<String, EmbeddingError> {
    let deadline = Instant::now() + opts.total_timeout;
    let mut retry = 0u32;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(EmbeddingError::Timeout(opts.total_timeout.as_secs()));
        }
        match attempt(url, body, headers, opts, remaining, secrets) {
            Ok(text) => return Ok(text),
            Err(Failure::Fatal(e)) => return Err(e),
            Err(Failure::Retryable { error, retry_after }) => {
                if retry >= opts.max_retries {
                    return Err(error);
                }
                let delay = retry_after.unwrap_or_else(|| backoff_delay(opts, retry));
                let remaining = deadline.saturating_duration_since(Instant::now());
                if delay >= remaining {
                    return Err(error);
                }
                std::thread::sleep(delay);
                retry += 1;
            }
        }
    }
}
