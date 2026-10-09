//! Incremental, bounded HTTP/1.1 request reader used by the control plane.
//!
//! The previous implementation called `read_to_end` on the socket, which only
//! returns when the client half-closes. Normal clients (curl, browsers,
//! language HTTP libraries) keep the connection open while waiting for the
//! response, so every request hung. This reader parses the head, then reads
//! exactly `Content-Length` body bytes, under size caps and deadlines.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HttpRequest {
    pub method: String,
    pub target: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl HttpRequest {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }
}

#[derive(Debug)]
pub(crate) enum ReadError {
    /// Client closed before sending anything.
    Closed,
    Timeout,
    HeaderTooLarge,
    BodyTooLarge,
    Malformed(String),
    Io(std::io::Error),
}

pub(crate) fn find_header_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|window| window == b"\r\n\r\n")
}

/// Parse the request line and headers (everything before the blank line).
pub(crate) fn parse_head(head: &str) -> Result<HttpRequest, String> {
    let mut lines = head.split("\r\n");
    let request_line = lines
        .next()
        .filter(|line| !line.trim().is_empty())
        .ok_or_else(|| "missing request line".to_string())?;
    let mut parts = request_line.split_whitespace();
    let method = parts
        .next()
        .ok_or_else(|| "missing HTTP method".to_string())?;
    let target = parts
        .next()
        .ok_or_else(|| "missing request target".to_string())?;
    let mut headers = Vec::new();
    for line in lines {
        if line.is_empty() {
            continue;
        }
        let (name, value) = line
            .split_once(':')
            .ok_or_else(|| "malformed header line".to_string())?;
        let name = name.trim().to_string();
        // A repeated credential header is ambiguous between proxy and
        // server; refuse it instead of picking one.
        if name.eq_ignore_ascii_case("authorization")
            && headers
                .iter()
                .any(|(existing, _): &(String, String)| existing.eq_ignore_ascii_case(&name))
        {
            return Err("duplicate credential header is not allowed".to_string());
        }
        headers.push((name, value.trim().to_string()));
    }
    Ok(HttpRequest {
        method: method.to_string(),
        target: target.to_string(),
        headers,
        body: Vec::new(),
    })
}

/// Extract and validate the `Content-Length` (0 when absent). Rejects
/// conflicting duplicates and any `Transfer-Encoding`.
pub(crate) fn content_length(request: &HttpRequest) -> Result<usize, String> {
    if request.header("transfer-encoding").is_some() {
        return Err("Transfer-Encoding is not supported; send Content-Length".to_string());
    }
    let mut length: Option<usize> = None;
    for (name, value) in &request.headers {
        if name.eq_ignore_ascii_case("content-length") {
            let parsed = value
                .trim()
                .parse::<usize>()
                .map_err(|_| "invalid content-length header".to_string())?;
            match length {
                Some(previous) if previous != parsed => {
                    return Err("conflicting content-length headers".to_string());
                }
                _ => length = Some(parsed),
            }
        }
    }
    Ok(length.unwrap_or(0))
}

fn read_some(
    stream: &mut TcpStream,
    buf: &mut [u8],
    deadline: Instant,
    config: &ServerConfig,
) -> Result<usize, ReadError> {
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(ReadError::Timeout);
        }
        stream
            .set_read_timeout(Some(remaining.min(config.read_timeout)))
            .map_err(ReadError::Io)?;
        match stream.read(buf) {
            Ok(n) => return Ok(n),
            Err(err) if err.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(err)
                if matches!(
                    err.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                return Err(ReadError::Timeout);
            }
            Err(err) => return Err(ReadError::Io(err)),
        }
    }
}

/// Read one request without requiring the client to half-close.
pub(crate) fn read_request(
    stream: &mut TcpStream,
    config: &ServerConfig,
) -> Result<HttpRequest, ReadError> {
    let deadline = Instant::now() + config.request_deadline;
    let mut buf: Vec<u8> = Vec::with_capacity(1024);
    let mut chunk = [0u8; 4096];
    let header_end = loop {
        if let Some(pos) = find_header_end(&buf) {
            break pos;
        }
        if buf.len() > config.max_header_bytes {
            return Err(ReadError::HeaderTooLarge);
        }
        let n = read_some(stream, &mut chunk, deadline, config)?;
        if n == 0 {
            return Err(if buf.is_empty() {
                ReadError::Closed
            } else {
                ReadError::Malformed("connection closed before end of headers".to_string())
            });
        }
        buf.extend_from_slice(&chunk[..n]);
    };
    if header_end > config.max_header_bytes {
        return Err(ReadError::HeaderTooLarge);
    }
    let head = std::str::from_utf8(&buf[..header_end])
        .map_err(|_| ReadError::Malformed("request headers must be valid UTF-8".to_string()))?;
    let mut request = parse_head(head).map_err(ReadError::Malformed)?;
    let length = content_length(&request).map_err(ReadError::Malformed)?;
    if length > config.max_body_bytes {
        return Err(ReadError::BodyTooLarge);
    }
    let mut body = buf.split_off(header_end + 4);
    if body.len() < length
        && request
            .header("expect")
            .is_some_and(|value| value.eq_ignore_ascii_case("100-continue"))
    {
        let _ = stream
            .set_write_timeout(Some(config.write_timeout))
            .and_then(|_| stream.write_all(b"HTTP/1.1 100 Continue\r\n\r\n"));
    }
    while body.len() < length {
        let n = read_some(stream, &mut chunk, deadline, config)?;
        if n == 0 {
            return Err(ReadError::Malformed("request body truncated".to_string()));
        }
        body.extend_from_slice(&chunk[..n]);
    }
    body.truncate(length);
    request.body = body;
    Ok(request)
}
