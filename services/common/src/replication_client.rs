//! Bounded HTTP(S) client the replication followers use to pull from the
//! leader (ingestion). Both followers (retrieval and an ingestion node that
//! follows another) share it so size, timeout, redirect and token rules are
//! identical.
//!
//! * `http://` and `https://` source URLs are accepted. TLS is `rustls` via
//!   `ureq`; the server certificate is always verified, against the public
//!   web roots plus an optional PEM bundle in `DASH_REPLICATION_CA_FILE` for
//!   a private or mesh CA. `DASH_REPLICATION_CLIENT_CERT_FILE` and
//!   `DASH_REPLICATION_CLIENT_KEY_FILE` present a client certificate (mutual
//!   TLS against a leader with `DASH_INGEST_TLS_CLIENT_CA_FILE`).
//! * Redirects are never followed, so the replication token cannot be
//!   forwarded to another host.
//! * Response bodies are read through a hard cap.
//! * The replication token is only sent over `https://` or to a loopback
//!   host. Plain `http://` to anything else is refused unless
//!   `DASH_REPLICATION_ALLOW_INSECURE_HTTP=1` acknowledges the exposure
//!   (the same rule the embeddings client applies to API keys).
//!
//! The leader serves HTTPS itself when `DASH_INGEST_TLS_CERT_FILE` and
//! `DASH_INGEST_TLS_KEY_FILE` are set; see `docs/operations/tls.md`.

use std::io::Read;
use std::net::IpAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use url::{Host, Url};

/// Error a follower reports for a WAL frame or export without a `generation`
/// line: the leader runs a release older than 0.3.0. Its offset-only
/// protocol serves a fresh follower only the WAL tail (never the snapshot)
/// and cannot signal a compaction reliably, so following it could silently
/// skip records. Both followers refuse such frames and apply nothing; see the
/// upgrade order in `docs/operations/upgrades.md`.
pub const LEGACY_LEADER_ERROR: &str = "replication source sent a frame without a WAL generation: \
     the leader runs a release older than 0.3.0, which this follower cannot follow safely; \
     upgrade the leader (docs/operations/upgrades.md)";

/// Set to exactly `1` to allow sending the replication token over plaintext
/// `http://` to a non-loopback host.
pub const ALLOW_INSECURE_HTTP_ENV: &str = "DASH_REPLICATION_ALLOW_INSECURE_HTTP";
/// PEM bundle of extra trusted CA certificates for `https://` sources.
pub const CA_FILE_ENV: &str = "DASH_REPLICATION_CA_FILE";
/// PEM client certificate chain presented to an `https://` source.
pub const CLIENT_CERT_FILE_ENV: &str = "DASH_REPLICATION_CLIENT_CERT_FILE";
/// PEM private key of [`CLIENT_CERT_FILE_ENV`].
pub const CLIENT_KEY_FILE_ENV: &str = "DASH_REPLICATION_CLIENT_KEY_FILE";
/// Operator documentation referenced by the startup warnings.
pub const SECURITY_DOC: &str = "docs/operations/replication-security.md";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceResponse {
    pub status: u16,
    pub body: String,
}

#[derive(Debug, Clone)]
pub struct ClientOptions {
    pub allow_insecure_http: bool,
    pub ca_file: Option<PathBuf>,
    /// Client certificate chain for mutual TLS (with `client_key_file`).
    pub client_cert_file: Option<PathBuf>,
    pub client_key_file: Option<PathBuf>,
    pub connect_timeout: Duration,
    pub io_timeout: Duration,
    /// Overall deadline of a request; zero means none (only `io_timeout`
    /// per read applies).
    pub request_deadline: Duration,
}

impl Default for ClientOptions {
    fn default() -> Self {
        Self {
            allow_insecure_http: false,
            ca_file: None,
            client_cert_file: None,
            client_key_file: None,
            connect_timeout: Duration::from_secs(5),
            io_timeout: Duration::from_secs(10),
            request_deadline: Duration::from_secs(60),
        }
    }
}

fn env_path(name: &str) -> Option<PathBuf> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

impl ClientOptions {
    pub fn from_env() -> Self {
        Self {
            allow_insecure_http: insecure_http_allowed_from_env(),
            ca_file: env_path(CA_FILE_ENV),
            client_cert_file: env_path(CLIENT_CERT_FILE_ENV),
            client_key_file: env_path(CLIENT_KEY_FILE_ENV),
            ..Self::default()
        }
    }

    /// The verified TLS configuration for an `https://` source. Errors name
    /// the variable and the file, never key material.
    pub fn tls_config(&self) -> Result<Arc<dash_http::rustls::ClientConfig>, String> {
        let identity = match (
            self.client_cert_file.as_deref(),
            self.client_key_file.as_deref(),
        ) {
            (None, None) => None,
            (Some(cert), Some(key)) => Some((cert, key)),
            (Some(_), None) => {
                return Err(format!(
                    "{CLIENT_CERT_FILE_ENV} is set but {CLIENT_KEY_FILE_ENV} is not"
                ));
            }
            (None, Some(_)) => {
                return Err(format!(
                    "{CLIENT_KEY_FILE_ENV} is set but {CLIENT_CERT_FILE_ENV} is not"
                ));
            }
        };
        dash_http::client_config(self.ca_file.as_deref(), identity).map_err(|err| {
            format!(
                "replication TLS configuration rejected ({CA_FILE_ENV}, \
                 {CLIENT_CERT_FILE_ENV}, {CLIENT_KEY_FILE_ENV}): {err}"
            )
        })
    }

    /// Check the TLS files at startup so a typo fails fast instead of on
    /// every poll.
    pub fn validate(&self) -> Result<(), String> {
        if self.ca_file.is_none()
            && self.client_cert_file.is_none()
            && self.client_key_file.is_none()
        {
            return Ok(());
        }
        self.tls_config().map(|_| ())
    }
}

/// `DASH_REPLICATION_ALLOW_INSECURE_HTTP` is on only when it is exactly `1`.
pub fn insecure_http_allowed_from_env() -> bool {
    std::env::var(ALLOW_INSECURE_HTTP_ENV).is_ok_and(|value| value.trim() == "1")
}

/// Parse a replication source URL: `http` or `https` with a host.
pub fn parse_source_url(raw: &str) -> Result<Url, String> {
    let url = Url::parse(raw.trim())
        .map_err(|_| "replication source URL must start with http:// or https://".to_string())?;
    match url.scheme() {
        "http" | "https" => {}
        _ => return Err("replication source URL must start with http:// or https://".to_string()),
    }
    if url.host().is_none() {
        return Err("replication source URL missing host:port authority".to_string());
    }
    Ok(url)
}

/// True for `localhost`, 127.0.0.0/8 and `::1`.
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

fn url_is_loopback(url: &Url) -> bool {
    url.host().is_some_and(|host| is_loopback_host(&host))
}

/// True when a plaintext `http://` URL points away from this machine.
pub fn is_plaintext_remote(url: &Url) -> bool {
    url.scheme() == "http" && !url_is_loopback(url)
}

/// Refuse to send the replication token over plaintext http to a
/// non-loopback host unless the operator acknowledged it.
pub fn check_token_transport(
    url: &Url,
    token_present: bool,
    allow_insecure_http: bool,
) -> Result<(), String> {
    if token_present && is_plaintext_remote(url) && !allow_insecure_http {
        return Err(format!(
            "refusing to send the replication token over plain http to a non-loopback host; \
             use an https:// source URL or set {ALLOW_INSECURE_HTTP_ENV}=1 to accept the exposure \
             (see {SECURITY_DOC})"
        ));
    }
    Ok(())
}

/// Short, stable code for a follower failure, safe to expose on `/ready`.
///
/// The raw error text can carry hostnames, filesystem paths, `Debug` output of
/// internal errors and up to a few hundred bytes of the leader's response
/// body; it belongs in logs and in the in-process status only.
pub fn error_code(raw: &str) -> &'static str {
    let e = raw.to_ascii_lowercase();
    let has = |needle: &str| e.contains(needle);
    if has("replication_group_too_large") {
        "group_too_large"
    } else if has("byte limit") {
        "response_too_large"
    } else if has("refusing to send the replication token") {
        "token_transport_refused"
    } else if has("failed requesting replication source")
        || has("failed connecting")
        || has("failed resolving")
    {
        "source_unreachable"
    } else if has("timed out") {
        "source_timeout"
    } else if has("status 401") || has("status 403") {
        "source_rejected_credentials"
    } else if has("returned status") {
        "source_error_status"
    } else if has("ack failed") {
        "ack_failed"
    } else if has("apply") || has("failed to append") || has("wal") || has("diverg") {
        "apply_failed"
    } else if has("payload")
        || has("frame")
        || has("export")
        || has("truncated")
        || has("utf-8")
        || has("invalid")
        || has("missing")
    {
        "invalid_response"
    } else {
        "replication_error"
    }
}

/// Follower-side startup warning: a non-loopback `http://` source.
pub fn plaintext_source_warning(source_url: &str) -> Option<String> {
    let url = parse_source_url(source_url).ok()?;
    if !is_plaintext_remote(&url) {
        return None;
    }
    Some(format!(
        "replication source '{}' uses plain http:// to a non-loopback host: the WAL stream \
         (all tenants' data) and the replication token cross the network unencrypted. \
         Enable TLS on the leader (DASH_INGEST_TLS_CERT_FILE) and use an https:// source URL, \
         or restrict the path with a mesh/NetworkPolicy; see {SECURITY_DOC}",
        redact_url(&url)
    ))
}

/// Leader-side startup warning: replication token configured while the
/// listener is reachable from other machines over plain HTTP.
pub fn leader_exposure_warning(bind_addr: &str, token_set: bool, tls: bool) -> Option<String> {
    if !token_set || tls || bind_is_loopback(bind_addr) {
        return None;
    }
    Some(format!(
        "replication is enabled (token set) and the listener {bind_addr} is not loopback and \
         serves plain HTTP: /internal/replication/* sends every tenant's data and accepts the \
         token unencrypted. Set DASH_INGEST_TLS_CERT_FILE and DASH_INGEST_TLS_KEY_FILE (and \
         DASH_INGEST_TLS_CLIENT_CA_FILE for mutual TLS), or use a mesh with mTLS, and restrict \
         who can reach the port; see {SECURITY_DOC}"
    ))
}

fn bind_is_loopback(bind_addr: &str) -> bool {
    let host = match bind_addr.rsplit_once(':') {
        Some((host, _)) => host,
        None => bind_addr,
    };
    let host = host.trim_start_matches('[').trim_end_matches(']');
    if host.eq_ignore_ascii_case("localhost") {
        return true;
    }
    host.parse::<IpAddr>().is_ok_and(|ip| ip.is_loopback())
}

/// Host and port only: never log credentials embedded in a URL.
fn redact_url(url: &Url) -> String {
    let host = url.host_str().unwrap_or("");
    match url.port() {
        Some(port) => format!("{}://{host}:{port}", url.scheme()),
        None => format!("{}://{host}", url.scheme()),
    }
}

/// Perform one request. `max_body_bytes` bounds the response body; errors use
/// stable phrases (`exceeds N byte limit`) the followers classify on.
pub fn request(
    method: &str,
    url: &str,
    token: Option<&str>,
    max_body_bytes: usize,
    options: &ClientOptions,
) -> Result<SourceResponse, String> {
    let parsed = parse_source_url(url)?;
    check_token_transport(&parsed, token.is_some(), options.allow_insecure_http)?;

    let mut builder = ureq::AgentBuilder::new()
        .redirects(0)
        .timeout_connect(options.connect_timeout)
        .timeout_read(options.io_timeout)
        .timeout_write(options.io_timeout);
    // An overall deadline takes precedence over the per-read timeout in
    // ureq. A zero deadline leaves only the per-read timeout: a stalled
    // peer is detected after `io_timeout` while a large answer that keeps
    // arriving is never cut off.
    if !options.request_deadline.is_zero() {
        builder = builder.timeout(options.request_deadline);
    }
    if parsed.scheme() == "https" {
        builder = builder.tls_config(options.tls_config()?);
    }
    let agent = builder.build();

    let mut req = agent.request(method, parsed.as_str());
    if let Some(token) = token {
        req = req.set("x-replication-token", token);
    }
    let result = if method == "GET" {
        req.call()
    } else {
        req.send_bytes(&[])
    };
    let response = match result {
        Ok(response) => response,
        // 4xx/5xx still carry a body the caller interprets (for example a
        // 404 for an unknown commit).
        Err(ureq::Error::Status(_, response)) => response,
        Err(ureq::Error::Transport(err)) => {
            return Err(format!(
                "failed requesting replication source '{}': {err}",
                redact_url(&parsed)
            ));
        }
    };
    let status = response.status();
    if let Some(declared) = response
        .header("content-length")
        .and_then(|value| value.trim().parse::<usize>().ok())
        && declared > max_body_bytes
    {
        return Err(format!(
            "replication response exceeds {max_body_bytes} byte limit"
        ));
    }
    let mut body = Vec::new();
    response
        .into_reader()
        .take(max_body_bytes as u64 + 1)
        .read_to_end(&mut body)
        .map_err(|err| {
            if err.kind() == std::io::ErrorKind::UnexpectedEof {
                "replication response body is truncated".to_string()
            } else {
                format!("failed reading replication response: {err}")
            }
        })?;
    if body.len() > max_body_bytes {
        return Err(format!(
            "replication response exceeds {max_body_bytes} byte limit"
        ));
    }
    let body = String::from_utf8(body)
        .map_err(|_| "replication response is not valid UTF-8".to_string())?;
    Ok(SourceResponse { status, body })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_url_accepts_http_and_https_only() {
        assert!(parse_source_url("http://127.0.0.1:8081/x").is_ok());
        assert!(parse_source_url("https://ingest.example:8443").is_ok());
        for bad in ["ftp://h/x", "127.0.0.1:8081", "http://", "file:///etc"] {
            assert!(parse_source_url(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn token_is_refused_over_plain_http_to_a_remote_host() {
        let remote = parse_source_url("http://ingestion:8081").unwrap();
        let err = check_token_transport(&remote, true, false).unwrap_err();
        assert!(err.contains(ALLOW_INSECURE_HTTP_ENV), "{err}");
        assert!(check_token_transport(&remote, true, true).is_ok());
        assert!(check_token_transport(&remote, false, false).is_ok());
        for ok in [
            "http://127.0.0.1:8081",
            "http://localhost:8081",
            "http://[::1]:8081",
            "https://ingestion:8443",
        ] {
            let url = parse_source_url(ok).unwrap();
            assert!(check_token_transport(&url, true, false).is_ok(), "{ok}");
        }
    }

    #[test]
    fn error_codes_hide_hosts_paths_and_bodies() {
        let cases = [
            (
                "failed requesting replication source 'http://10.0.0.1:8081': refused",
                "source_unreachable",
            ),
            (
                "replication response exceeds 10 byte limit",
                "response_too_large",
            ),
            ("replication response body is truncated", "invalid_response"),
            (
                "replication source returned status 500 (internal: replication_group_too_large)",
                "group_too_large",
            ),
            (
                "replication source returned status 500 (boom)",
                "source_error_status",
            ),
            (
                "replication ack failed for commit_id 'c' with status 403",
                "source_rejected_credentials",
            ),
            (
                "replication delta apply failed: Io(\"/var/lib/x\")",
                "apply_failed",
            ),
            (
                "refusing to send the replication token over plain http",
                "token_transport_refused",
            ),
            ("something else entirely", "replication_error"),
        ];
        for (raw, code) in cases {
            assert_eq!(error_code(raw), code, "{raw}");
        }
    }

    #[test]
    fn follower_warning_only_for_remote_plaintext() {
        assert!(plaintext_source_warning("http://ingestion:8081").is_some());
        assert!(plaintext_source_warning("http://10.0.0.5:8081").is_some());
        assert!(plaintext_source_warning("http://127.0.0.1:8081").is_none());
        assert!(plaintext_source_warning("https://ingestion:8443").is_none());
        let message = plaintext_source_warning("http://user:pw@ingestion:8081").unwrap();
        assert!(!message.contains("pw"), "credentials must not be logged");
        assert!(message.contains(SECURITY_DOC));
    }

    #[test]
    fn leader_warning_needs_a_token_a_non_loopback_bind_and_plain_http() {
        assert!(leader_exposure_warning("0.0.0.0:8081", true, false).is_some());
        assert!(leader_exposure_warning("10.1.2.3:8081", true, false).is_some());
        assert!(leader_exposure_warning("[::]:8081", true, false).is_some());
        assert!(leader_exposure_warning("0.0.0.0:8081", false, false).is_none());
        assert!(leader_exposure_warning("127.0.0.1:8081", true, false).is_none());
        assert!(leader_exposure_warning("localhost:8081", true, false).is_none());
        assert!(leader_exposure_warning("[::1]:8081", true, false).is_none());
        // A TLS listener is not a plaintext exposure.
        assert!(leader_exposure_warning("0.0.0.0:8081", true, true).is_none());
    }
}
