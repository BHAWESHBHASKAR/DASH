//! External key managers: shared HTTP transport, retries and the unwrap
//! cache used by [`VaultTransitKekProvider`] and [`AwsKmsKekProvider`].
//!
//! Both providers talk to their service over HTTPS with the workspace's
//! blocking client (`ureq` with rustls, roots from `webpki-roots` plus an
//! optional CA bundle). Redirects are never followed, so a token or a
//! signature cannot be forwarded to another host. Every call has a
//! per-attempt timeout and bounded retries with jittered exponential backoff
//! on connection errors, timeouts, 429 and 5xx. Error messages never contain
//! tokens, secret keys or key material: service error bodies are reduced to a
//! short single-line snippet with known secrets removed.

pub(crate) mod aws;
pub(crate) mod aws_creds;
pub(crate) mod chain;
#[cfg(any(test, feature = "mock-kms"))]
pub mod mock;
pub(crate) mod sigv4;
pub(crate) mod vault;

use std::collections::HashMap;
use std::io::Read;
use std::net::IpAddr;
use std::path::Path;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use sha2::{Digest, Sha256};
use url::{Host, Url};
use zeroize::Zeroizing;

use crate::{DEK_LEN, EncryptionError};

/// Largest response body read from a key manager.
const MAX_RESPONSE_BYTES: u64 = 1024 * 1024;
/// Characters of a service error body kept in an error message.
const SNIPPET_CHARS: usize = 200;
/// Upper bound for a `Retry-After` delay.
const MAX_RETRY_AFTER: Duration = Duration::from_secs(5);
/// Unwrapped DEKs kept in memory per provider.
const UNWRAP_CACHE_ENTRIES: usize = 1024;

/// Timeouts and retry policy for key manager calls.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KmsHttpOptions {
    /// Timeout of one attempt (connect, send and receive).
    pub attempt_timeout: Duration,
    /// Retries after the first attempt.
    pub max_retries: u32,
    /// First backoff delay; doubled per retry, jittered.
    pub backoff_base: Duration,
    /// Cap of one backoff delay.
    pub backoff_max: Duration,
}

impl Default for KmsHttpOptions {
    fn default() -> Self {
        Self {
            attempt_timeout: Duration::from_millis(DEFAULT_TIMEOUT_MS),
            max_retries: DEFAULT_MAX_RETRIES,
            backoff_base: Duration::from_millis(100),
            backoff_max: Duration::from_secs(2),
        }
    }
}

/// Default of `DASH_ENCRYPTION_KMS_TIMEOUT_MS`.
pub const DEFAULT_TIMEOUT_MS: u64 = 5000;
/// Default of `DASH_ENCRYPTION_KMS_MAX_RETRIES`.
pub const DEFAULT_MAX_RETRIES: u32 = 3;

/// A response: status, `Retry-After` and the body (cleared on drop, it can
/// hold a plaintext DEK).
pub(crate) struct Reply {
    pub status: u16,
    pub retry_after: Option<Duration>,
    pub body: Zeroizing<Vec<u8>>,
}

/// Why one attempt did not produce a response.
pub(crate) enum TransportFailure {
    /// Connection refused / reset, DNS failure, I/O error: worth a retry.
    Retryable(String),
    /// The attempt timed out: worth a retry.
    TimedOut,
    /// Anything else (TLS failure, invalid request): not retried.
    Fatal(String),
}

/// A blocking HTTP client for one key manager endpoint.
pub(crate) struct HttpClient {
    agent: ureq::Agent,
}

impl HttpClient {
    pub fn new(
        scheme_is_https: bool,
        ca_file: Option<&Path>,
        opts: &KmsHttpOptions,
    ) -> Result<Self, EncryptionError> {
        let mut builder = ureq::AgentBuilder::new()
            .redirects(0)
            .timeout_connect(opts.attempt_timeout)
            .user_agent("dash-encryption/0.1");
        if scheme_is_https || ca_file.is_some() {
            let tls = dash_http::client_config(ca_file, None).map_err(EncryptionError::Config)?;
            builder = builder.tls_config(tls);
        }
        Ok(Self {
            agent: builder.build(),
        })
    }

    /// Sends one request. Never follows redirects; reads at most
    /// [`MAX_RESPONSE_BYTES`] of the body.
    pub fn send(
        &self,
        method: &str,
        url: &str,
        headers: &[(&str, &str)],
        body: Option<&[u8]>,
        timeout: Duration,
        secrets: &[&str],
    ) -> Result<Reply, TransportFailure> {
        let mut request = self.agent.request(method, url).timeout(timeout);
        for (name, value) in headers {
            request = request.set(name, value);
        }
        let result = match body {
            Some(body) => request.send_bytes(body),
            None => request.call(),
        };
        let response = match result {
            Ok(response) => response,
            Err(ureq::Error::Status(_, response)) => response,
            Err(ureq::Error::Transport(err)) => return Err(classify_transport(err, secrets)),
        };
        let status = response.status();
        let retry_after = response.header("retry-after").and_then(parse_retry_after);
        let mut body = Zeroizing::new(Vec::new());
        let mut reader = response.into_reader().take(MAX_RESPONSE_BYTES + 1);
        if let Err(err) = reader.read_to_end(&mut body) {
            return Err(if is_timeout(err.kind()) {
                TransportFailure::TimedOut
            } else {
                TransportFailure::Retryable(format!("reading the response failed: {err}"))
            });
        }
        if body.len() as u64 > MAX_RESPONSE_BYTES {
            return Err(TransportFailure::Fatal(format!(
                "response exceeds {MAX_RESPONSE_BYTES} bytes"
            )));
        }
        Ok(Reply {
            status,
            retry_after,
            body,
        })
    }
}

fn is_timeout(kind: std::io::ErrorKind) -> bool {
    matches!(
        kind,
        std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock
    )
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

fn classify_transport(err: ureq::Transport, secrets: &[&str]) -> TransportFailure {
    if io_kind_in_chain(&err).is_some_and(is_timeout) {
        return TransportFailure::TimedOut;
    }
    let text = sanitize(err.to_string().as_bytes(), secrets);
    match err.kind() {
        ureq::ErrorKind::Dns | ureq::ErrorKind::ConnectionFailed | ureq::ErrorKind::Io => {
            TransportFailure::Retryable(text)
        }
        _ => TransportFailure::Fatal(text),
    }
}

fn parse_retry_after(value: &str) -> Option<Duration> {
    value
        .trim()
        .parse::<u64>()
        .ok()
        .map(|s| Duration::from_secs(s).min(MAX_RETRY_AFTER))
}

/// The outcome of one attempt inside [`with_retries`].
pub(crate) enum Step<T> {
    Done(T),
    /// Try again after a backoff (or `retry_after`), if retries remain.
    Retry {
        error: String,
        retry_after: Option<Duration>,
    },
    /// Try again at once without counting a retry (a credential was
    /// refreshed); allowed once per call.
    Again(String),
    Fail(EncryptionError),
}

/// Maps a transport failure to a step.
pub(crate) fn transport_step<T>(failure: TransportFailure, opts: &KmsHttpOptions) -> Step<T> {
    match failure {
        TransportFailure::Retryable(error) => Step::Retry {
            error,
            retry_after: None,
        },
        TransportFailure::TimedOut => Step::Retry {
            error: format!("no response within {} ms", opts.attempt_timeout.as_millis()),
            retry_after: None,
        },
        TransportFailure::Fatal(error) => Step::Fail(EncryptionError::Kms(error)),
    }
}

/// True for statuses worth a retry.
pub(crate) fn retryable_status(status: u16) -> bool {
    status == 429 || (500..=599).contains(&status)
}

/// Runs `attempt` with bounded retries. `what` names the operation in the
/// final error ("Vault Transit encrypt with vault:transit/dash").
pub(crate) fn with_retries<T>(
    opts: &KmsHttpOptions,
    what: &str,
    mut attempt: impl FnMut() -> Step<T>,
) -> Result<T, EncryptionError> {
    let mut retry = 0u32;
    let mut refreshed = false;
    loop {
        match attempt() {
            Step::Done(value) => return Ok(value),
            Step::Fail(err) => return Err(err),
            Step::Again(error) => {
                if refreshed {
                    return Err(EncryptionError::Kms(format!("{what}: {error}")));
                }
                refreshed = true;
            }
            Step::Retry { error, retry_after } => {
                if retry >= opts.max_retries {
                    let attempts = retry + 1;
                    return Err(EncryptionError::Kms(format!(
                        "{what}: {error} (gave up after {attempts} attempt{})",
                        if attempts == 1 { "" } else { "s" }
                    )));
                }
                std::thread::sleep(retry_after.unwrap_or_else(|| backoff_delay(opts, retry)));
                retry += 1;
            }
        }
    }
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
    0.5 + (x >> 11) as f64 / (1u64 << 53) as f64 * 0.5
}

fn backoff_delay(opts: &KmsHttpOptions, retry: u32) -> Duration {
    let factor = 1u32.checked_shl(retry.min(16)).unwrap_or(u32::MAX);
    opts.backoff_base
        .saturating_mul(factor)
        .min(opts.backoff_max)
        .mul_f64(jitter_fraction())
}

/// A short, single-line snippet of a service response with every value in
/// `secrets` removed.
pub(crate) fn sanitize(raw: &[u8], secrets: &[&str]) -> String {
    let mut text = String::from_utf8_lossy(raw).into_owned();
    for secret in secrets {
        if !secret.is_empty() {
            text = text.replace(secret, "[redacted]");
        }
    }
    let cleaned: String = text
        .chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect();
    let cleaned = cleaned.trim();
    if cleaned.chars().count() > SNIPPET_CHARS {
        let mut out: String = cleaned.chars().take(SNIPPET_CHARS).collect();
        out.push_str("...");
        out
    } else {
        cleaned.to_string()
    }
}

/// Parses a key manager address: http or https, a host, no credentials, no
/// query. Plain http is refused for non-loopback hosts unless
/// `allow_insecure` (credentials would cross the network in clear text).
pub(crate) fn parse_endpoint(
    raw: &str,
    setting: &str,
    allow_insecure: bool,
) -> Result<Url, EncryptionError> {
    let bad = |m: String| EncryptionError::Config(format!("{setting}: {m}"));
    let url = Url::parse(raw.trim()).map_err(|e| bad(format!("invalid URL: {e}")))?;
    match url.scheme() {
        "https" => {}
        "http" => {
            let loopback = url.host().is_some_and(|h| is_loopback(&h));
            if !loopback && !allow_insecure {
                return Err(bad(format!(
                    "refusing plain http to non-loopback host '{}': credentials would cross the network unencrypted; use https or set {ALLOW_INSECURE_HTTP_ENV}=1",
                    url.host_str().unwrap_or("?")
                )));
            }
        }
        other => return Err(bad(format!("unsupported scheme '{other}' (use https)"))),
    }
    if url.host().is_none() {
        return Err(bad("URL has no host".to_string()));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(bad("URL must not embed credentials".to_string()));
    }
    if url.query().is_some() || url.fragment().is_some() {
        return Err(bad("URL must not have a query or fragment".to_string()));
    }
    Ok(url)
}

/// Lets key manager calls use plain http to a non-loopback host (`1` only).
pub const ALLOW_INSECURE_HTTP_ENV: &str = "DASH_ENCRYPTION_KMS_ALLOW_INSECURE_HTTP";

pub(crate) fn is_loopback(host: &Host<&str>) -> bool {
    match host {
        Host::Domain(d) => {
            let d = d.trim_end_matches('.').to_ascii_lowercase();
            d == "localhost" || d.ends_with(".localhost")
        }
        Host::Ipv4(ip) => IpAddr::V4(*ip).is_loopback(),
        Host::Ipv6(ip) => IpAddr::V6(*ip).is_loopback(),
    }
}

/// `host[:port]` as it must appear in a `Host` header.
pub(crate) fn host_header(url: &Url) -> String {
    let host = match url.host() {
        Some(Host::Ipv6(ip)) => format!("[{ip}]"),
        Some(h) => h.to_string(),
        None => String::new(),
    };
    match url.port() {
        Some(port) => format!("{host}:{port}"),
        None => host,
    }
}

/// Reads a small secret file (a token): a regular file, at most 64 KiB, not
/// accessible to other users on Unix, one line of visible ASCII.
pub(crate) fn read_secret_file(
    path: &Path,
    what: &str,
) -> Result<Zeroizing<String>, EncryptionError> {
    let shown = path.display();
    let bad = |m: String| EncryptionError::Config(format!("{what} {shown}: {m}"));
    let meta = std::fs::metadata(path).map_err(|e| bad(e.to_string()))?;
    if !meta.is_file() {
        return Err(bad("not a regular file".to_string()));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = meta.permissions().mode() & 0o777;
        if mode & 0o007 != 0 {
            return Err(bad(format!(
                "mode {mode:03o} lets other users read it (chmod 600, 640 or 400)"
            )));
        }
    }
    if meta.len() > 64 * 1024 {
        return Err(bad("too large".to_string()));
    }
    let raw = Zeroizing::new(std::fs::read(path).map_err(|e| bad(e.to_string()))?);
    let text = std::str::from_utf8(&raw).map_err(|_| bad("not UTF-8".to_string()))?;
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Err(bad("is empty".to_string()));
    }
    if !trimmed.bytes().all(|b| b.is_ascii_graphic()) {
        return Err(bad(
            "must hold one token of visible ASCII characters".to_string()
        ));
    }
    Ok(Zeroizing::new(trimmed.to_string()))
}

/// Unwrapped DEKs by (key id, context, wrapped bytes). A file is opened
/// more than once (reader and writer, checks at start-up); the cache keeps
/// that to one key manager call. Cleared entirely when full.
pub(crate) struct UnwrapCache {
    entries: Mutex<HashMap<[u8; 32], Zeroizing<[u8; DEK_LEN]>>>,
}

impl UnwrapCache {
    pub fn new() -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
        }
    }

    fn slot(key_id: &str, wrapped: &[u8], context: &[u8]) -> [u8; 32] {
        let mut h = Sha256::new();
        h.update((key_id.len() as u64).to_le_bytes());
        h.update(key_id.as_bytes());
        h.update((context.len() as u64).to_le_bytes());
        h.update(context);
        h.update(wrapped);
        h.finalize().into()
    }

    pub fn get(
        &self,
        key_id: &str,
        wrapped: &[u8],
        context: &[u8],
    ) -> Option<Zeroizing<[u8; DEK_LEN]>> {
        let slot = Self::slot(key_id, wrapped, context);
        let entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        entries.get(&slot).map(|dek| Zeroizing::new(**dek))
    }

    pub fn put(&self, key_id: &str, wrapped: &[u8], context: &[u8], dek: &[u8; DEK_LEN]) {
        let slot = Self::slot(key_id, wrapped, context);
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        if entries.len() >= UNWRAP_CACHE_ENTRIES {
            entries.clear();
        }
        entries.insert(slot, Zeroizing::new(*dek));
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.entries.lock().unwrap().len()
    }
}

/// Compares two byte strings in time independent of where they differ.
pub(crate) fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// Unix seconds now.
pub(crate) fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Civil date (year, month, day) of a day count since 1970-01-01.
pub(crate) fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// Days since 1970-01-01 of a civil date.
pub(crate) fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400);
    let m = m as i64;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// Parses `YYYY-MM-DDTHH:MM:SS[.fff]Z` (the form AWS uses for credential
/// expiry) to Unix seconds.
pub(crate) fn parse_rfc3339_utc(text: &str) -> Option<u64> {
    let t = text.trim();
    let b = t.as_bytes();
    if b.len() < 20 || b[4] != b'-' || b[7] != b'-' || !matches!(b[10], b'T' | b't') {
        return None;
    }
    if b[13] != b':' || b[16] != b':' || !matches!(*b.last()?, b'Z' | b'z') {
        return None;
    }
    let num = |r: std::ops::Range<usize>| t.get(r)?.parse::<u32>().ok();
    let (y, mo, d) = (num(0..4)?, num(5..7)?, num(8..10)?);
    let (h, mi, s) = (num(11..13)?, num(14..16)?, num(17..19)?);
    if !(1..=12).contains(&mo) || !(1..=31).contains(&d) || h > 23 || mi > 59 || s > 60 {
        return None;
    }
    let frac = &t[19..t.len() - 1];
    if !frac.is_empty() && !(frac.starts_with('.') && frac[1..].bytes().all(|c| c.is_ascii_digit()))
    {
        return None;
    }
    let days = days_from_civil(y as i64, mo, d);
    let secs = days * 86_400 + (h * 3600 + mi * 60 + s) as i64;
    u64::try_from(secs).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dates_round_trip() {
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(civil_from_days(16_677), (2015, 8, 30));
        assert_eq!(days_from_civil(2015, 8, 30), 16_677);
        assert_eq!(days_from_civil(2000, 2, 29), 11_016);
        assert_eq!(civil_from_days(11_016), (2000, 2, 29));
        assert_eq!(
            parse_rfc3339_utc("2015-08-30T12:36:00Z"),
            Some(16_677 * 86_400 + 12 * 3600 + 36 * 60)
        );
        assert_eq!(
            parse_rfc3339_utc("2015-08-30T12:36:00.123Z"),
            parse_rfc3339_utc("2015-08-30T12:36:00Z")
        );
        assert!(parse_rfc3339_utc("2015-08-30 12:36:00").is_none());
        assert!(parse_rfc3339_utc("2015-13-30T12:36:00Z").is_none());
    }

    #[test]
    fn endpoints_are_checked() {
        assert!(parse_endpoint("https://vault.example:8200", "X", false).is_ok());
        assert!(parse_endpoint("http://127.0.0.1:8200", "X", false).is_ok());
        assert!(parse_endpoint("http://localhost:8200", "X", false).is_ok());
        let err = parse_endpoint("http://vault.example:8200", "X", false).unwrap_err();
        assert!(err.to_string().contains(ALLOW_INSECURE_HTTP_ENV), "{err}");
        assert!(parse_endpoint("http://vault.example:8200", "X", true).is_ok());
        assert!(parse_endpoint("ftp://vault.example", "X", false).is_err());
        assert!(parse_endpoint("https://user:pw@vault.example", "X", false).is_err());
        assert!(parse_endpoint("https://vault.example/?a=b", "X", false).is_err());
        assert!(parse_endpoint("not a url", "X", false).is_err());
    }

    #[test]
    fn host_header_keeps_the_port() {
        let url = Url::parse("https://kms.us-east-1.amazonaws.com/").unwrap();
        assert_eq!(host_header(&url), "kms.us-east-1.amazonaws.com");
        let url = Url::parse("http://127.0.0.1:4566/").unwrap();
        assert_eq!(host_header(&url), "127.0.0.1:4566");
        let url = Url::parse("http://[::1]:4566/").unwrap();
        assert_eq!(host_header(&url), "[::1]:4566");
    }

    #[test]
    fn sanitize_removes_secrets_and_control_characters() {
        let out = sanitize(b"bad token s.abc123\nline two", &["s.abc123"]);
        assert_eq!(out, "bad token [redacted] line two");
        let long = "x".repeat(500);
        assert!(sanitize(long.as_bytes(), &[]).len() < 210);
    }

    #[test]
    fn cache_is_keyed_by_everything_and_bounded() {
        let cache = UnwrapCache::new();
        cache.put("k", b"w", b"c", &[1; 32]);
        assert_eq!(*cache.get("k", b"w", b"c").unwrap(), [1; 32]);
        assert!(cache.get("k", b"w", b"d").is_none());
        assert!(cache.get("k2", b"w", b"c").is_none());
        assert!(cache.get("k", b"x", b"c").is_none());
        for n in 0..UNWRAP_CACHE_ENTRIES + 5 {
            cache.put("k", &n.to_le_bytes(), b"c", &[2; 32]);
        }
        assert!(cache.len() <= UNWRAP_CACHE_ENTRIES);
    }

    #[cfg(unix)]
    #[test]
    fn secret_files_are_checked() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("token");
        std::fs::write(&path, "  s.token-value\n").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o640)).unwrap();
        assert_eq!(
            &*read_secret_file(&path, "token file").unwrap(),
            "s.token-value"
        );
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        let err = read_secret_file(&path, "token file").unwrap_err();
        assert!(err.to_string().contains("chmod 600"), "{err}");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        std::fs::write(&path, "two words").unwrap();
        assert!(read_secret_file(&path, "token file").is_err());
        std::fs::write(&path, "\n").unwrap();
        assert!(read_secret_file(&path, "token file").is_err());
        assert!(read_secret_file(&dir.path().join("missing"), "token file").is_err());
    }
}
