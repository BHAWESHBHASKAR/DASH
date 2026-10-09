use std::collections::HashMap;
use std::io::{ErrorKind, Read, Write};
use std::net::TcpStream;
use std::time::Instant;

use crate::config::{ExpectPolicy, ServerConfig};
use crate::request::Request;

/// Credential headers that may appear at most once per request.
const SINGLETON_CREDENTIAL_HEADERS: [&str; 3] =
    ["authorization", "x-api-key", "x-replication-token"];
/// Parse error for a well-formed HTTP version other than 1.0/1.1 (answered
/// with 505).
const UNSUPPORTED_VERSION: &str = "unsupported HTTP version";
/// Upper bound on bytes allocated ahead of bytes actually received.
const READ_CHUNK_BYTES: usize = 4096;

/// A request-reading failure carrying the HTTP status that should be returned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadError {
    pub status: u16,
    pub message: String,
}

impl ReadError {
    fn new(status: u16, message: &str) -> Self {
        Self {
            status,
            message: message.to_string(),
        }
    }

    fn bad_request(message: &str) -> Self {
        Self::new(400, message)
    }
}

impl std::fmt::Display for ReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({})", self.message, self.status)
    }
}

impl std::error::Error for ReadError {}

fn map_io_error(err: &std::io::Error) -> ReadError {
    match err.kind() {
        ErrorKind::WouldBlock | ErrorKind::TimedOut => ReadError::new(408, "request timed out"),
        _ => ReadError::bad_request("failed to read request"),
    }
}

/// Read once from the stream, never waiting past the whole-request deadline
/// (or the per-read timeout, when configured).
fn read_with_deadline(
    stream: &mut TcpStream,
    buf: &mut [u8],
    deadline: Instant,
    cfg: &ServerConfig,
) -> Result<usize, ReadError> {
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(ReadError::new(408, "request timed out"));
        }
        let wait = cfg.read_timeout.map_or(remaining, |cap| remaining.min(cap));
        stream
            .set_read_timeout(Some(wait))
            .map_err(|err| map_io_error(&err))?;
        match stream.read(buf) {
            Ok(n) => return Ok(n),
            Err(err) if err.kind() == ErrorKind::Interrupted => continue,
            Err(err) => return Err(map_io_error(&err)),
        }
    }
}

/// Returns (end_of_headers, start_of_body). Accepts CRLFCRLF and bare LFLF.
fn find_header_end(buf: &[u8], from: usize) -> Option<(usize, usize)> {
    let mut i = from;
    while i < buf.len() {
        if buf[i] == b'\n' {
            if buf.get(i + 1) == Some(&b'\n') {
                return Some((i + 1, i + 2));
            }
            if buf.get(i + 1) == Some(&b'\r') && buf.get(i + 2) == Some(&b'\n') {
                return Some((i + 1, i + 3));
            }
        }
        i += 1;
    }
    None
}

/// A validated request line and header block.
struct Head {
    method: String,
    target: String,
    headers: HashMap<String, String>,
    content_length: usize,
    /// `Expect: 100-continue` under [`ExpectPolicy::Continue100`].
    expect_continue: bool,
}

fn too_large_body(max: usize) -> ReadError {
    ReadError::new(
        413,
        &format!("content-length exceeds max body size ({max} bytes)"),
    )
}

/// Validate the request line and headers (`head` ends with the blank line).
fn parse_head(head: &str, cfg: &ServerConfig) -> Result<Head, ReadError> {
    let mut lines = head
        .split('\n')
        .map(|line| line.strip_suffix('\r').unwrap_or(line));
    let request_line = lines.next().unwrap_or("");
    if request_line.len() > cfg.max_header_line_bytes {
        return Err(ReadError::new(431, "request line too large"));
    }
    let (method, target) = parse_request_line(request_line).map_err(|e| {
        let status = if e == UNSUPPORTED_VERSION { 505 } else { 400 };
        ReadError::new(status, &e)
    })?;

    let mut headers: HashMap<String, String> = HashMap::new();
    let mut header_count = 0usize;
    for line in lines {
        if line.is_empty() {
            continue;
        }
        if line.len() > cfg.max_header_line_bytes {
            return Err(ReadError::new(431, "request header line too large"));
        }
        header_count += 1;
        if header_count > cfg.max_header_count {
            return Err(ReadError::new(431, "too many request headers"));
        }
        if line.starts_with([' ', '\t']) {
            return Err(ReadError::bad_request(
                "obsolete header line folding is not supported",
            ));
        }
        let (name, value) = line
            .split_once(':')
            .ok_or_else(|| ReadError::bad_request("invalid HTTP header"))?;
        if name.is_empty() || name.contains([' ', '\t']) {
            return Err(ReadError::bad_request("invalid HTTP header name"));
        }
        let name = name.trim().to_ascii_lowercase();
        let value = value.trim().to_string();
        if name == "content-length"
            && let Some(existing) = headers.get(&name)
            && *existing != value
        {
            return Err(ReadError::bad_request("conflicting content-length headers"));
        }
        // A repeated credential header is ambiguous (last-wins differs between
        // proxies and servers); refuse it instead of picking one.
        if SINGLETON_CREDENTIAL_HEADERS.contains(&name.as_str()) && headers.contains_key(&name) {
            return Err(ReadError::bad_request(
                "duplicate credential header is not allowed",
            ));
        }
        headers.insert(name, value);
    }

    let mut expect_continue = false;
    if let Some(expect) = headers.get("expect") {
        match cfg.expect {
            // Answer immediately instead of waiting for a body the client
            // will not send until it sees `100 Continue`.
            ExpectPolicy::Reject => {
                return Err(ReadError::new(
                    417,
                    "the Expect header is not supported; send the body directly",
                ));
            }
            ExpectPolicy::Continue100 => {
                expect_continue = expect.eq_ignore_ascii_case("100-continue");
            }
        }
    }

    if headers.contains_key("transfer-encoding") {
        return Err(ReadError::new(
            501,
            "transfer-encoding is not supported; send a content-length body",
        ));
    }

    let content_length = match headers.get("content-length") {
        Some(raw) => {
            if raw.is_empty() || !raw.bytes().all(|b| b.is_ascii_digit()) {
                return Err(ReadError::bad_request("invalid content-length header"));
            }
            match raw.parse::<usize>() {
                Ok(value) => value,
                Err(_) => return Err(too_large_body(cfg.max_body_bytes)),
            }
        }
        None => 0,
    };
    if content_length > cfg.max_body_bytes {
        return Err(too_large_body(cfg.max_body_bytes));
    }

    Ok(Head {
        method,
        target,
        headers,
        content_length,
        expect_continue,
    })
}

fn parse_request_line(line: &str) -> Result<(String, String), String> {
    let line = line.trim();
    let mut parts = line.split_whitespace();
    let method = parts
        .next()
        .ok_or_else(|| "missing HTTP method".to_string())?;
    let target = parts
        .next()
        .ok_or_else(|| "missing HTTP target".to_string())?;
    let version = parts
        .next()
        .ok_or_else(|| "missing HTTP version".to_string())?;
    if parts.next().is_some() {
        return Err("malformed request line".to_string());
    }
    if !matches!(version, "HTTP/1.0" | "HTTP/1.1") {
        let digits = |s: &str| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit());
        let well_formed = version
            .strip_prefix("HTTP/")
            .and_then(|v| v.split_once('.'))
            .is_some_and(|(major, minor)| digits(major) && digits(minor));
        return Err(if well_formed {
            UNSUPPORTED_VERSION.to_string()
        } else {
            "malformed HTTP version".to_string()
        });
    }
    Ok((method.to_string(), target.to_string()))
}

/// Read one request, giving up at `deadline` (measured from accept, so time
/// spent queued counts). `Ok(None)` means the peer closed without sending
/// anything.
pub fn read_request(
    stream: &mut TcpStream,
    cfg: &ServerConfig,
    deadline: Instant,
) -> Result<Option<Request>, ReadError> {
    let mut buf: Vec<u8> = Vec::with_capacity(READ_CHUNK_BYTES);
    let mut chunk = [0u8; READ_CHUNK_BYTES];
    let mut scan_from = 0usize;

    let (head_end, body_start) = loop {
        if let Some(found) = find_header_end(&buf, scan_from) {
            break found;
        }
        scan_from = buf.len().saturating_sub(3);
        if buf.len() > cfg.max_header_block_bytes {
            return Err(ReadError::new(431, "request headers too large"));
        }
        let n = read_with_deadline(stream, &mut chunk, deadline, cfg)?;
        if n == 0 {
            if buf.is_empty() {
                return Ok(None);
            }
            return Err(ReadError::bad_request("unexpected end of request"));
        }
        buf.extend_from_slice(&chunk[..n]);
    };
    if head_end > cfg.max_header_block_bytes {
        return Err(ReadError::new(431, "request headers too large"));
    }

    let head = std::str::from_utf8(&buf[..head_end])
        .map_err(|_| ReadError::bad_request("request headers must be valid UTF-8"))?;
    let head = parse_head(head, cfg)?;
    let content_length = head.content_length;

    // Allocate only what has actually arrived (plus one chunk); the buffer
    // grows as bytes are received so a lying Content-Length costs nothing.
    let mut body: Vec<u8> = Vec::with_capacity(content_length.min(READ_CHUNK_BYTES));
    let already = &buf[body_start.min(buf.len())..];
    body.extend_from_slice(&already[..already.len().min(content_length)]);
    if body.len() < content_length && head.expect_continue {
        let _ = stream
            .set_write_timeout(Some(cfg.write_timeout))
            .and_then(|_| stream.write_all(b"HTTP/1.1 100 Continue\r\n\r\n"));
    }
    while body.len() < content_length {
        let want = (content_length - body.len()).min(READ_CHUNK_BYTES);
        let n = read_with_deadline(stream, &mut chunk[..want], deadline, cfg)?;
        if n == 0 {
            return Err(ReadError::bad_request("request body truncated"));
        }
        body.extend_from_slice(&chunk[..n]);
    }

    Ok(Some(Request {
        method: head.method,
        target: head.target,
        headers: head.headers,
        body,
        peer: None,
    }))
}

/// Parse a complete request held in memory with the same rules as
/// [`read_request`]. The body must be exactly `Content-Length` bytes.
pub fn parse_request_bytes(raw: &[u8], cfg: &ServerConfig) -> Result<Request, ReadError> {
    let (head_end, body_start) = find_header_end(raw, 0)
        .ok_or_else(|| ReadError::bad_request("missing HTTP header terminator"))?;
    if head_end > cfg.max_header_block_bytes {
        return Err(ReadError::new(431, "request headers too large"));
    }
    let head = std::str::from_utf8(&raw[..head_end])
        .map_err(|_| ReadError::bad_request("request must be valid UTF-8"))?;
    let head = parse_head(head, cfg)?;
    let body = &raw[body_start..];
    if head.content_length != body.len() {
        return Err(ReadError::bad_request(
            "content-length does not match body size",
        ));
    }
    Ok(Request {
        method: head.method,
        target: head.target,
        headers: head.headers,
        body: body.to_vec(),
        peer: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> ServerConfig {
        ServerConfig::new("test", 1, 1)
    }

    fn parse(raw: &str) -> Result<Request, ReadError> {
        parse_request_bytes(raw.as_bytes(), &cfg())
    }

    #[test]
    fn parses_a_simple_request() {
        let request =
            parse("POST /x?a=1 HTTP/1.1\r\nHost: h\r\nContent-Length: 2\r\n\r\nhi").unwrap();
        assert_eq!(request.method, "POST");
        assert_eq!(request.header("Host"), Some("h"));
        assert_eq!(request.body, b"hi");
    }

    #[test]
    fn framing_conflicts_are_rejected() {
        let status = |raw: &str| parse(raw).unwrap_err().status;
        assert_eq!(
            status("POST / HTTP/1.1\r\nContent-Length: 1\r\nContent-Length: 2\r\n\r\nab"),
            400
        );
        assert_eq!(
            status("POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\nContent-Length: 1\r\n\r\na"),
            501
        );
        assert_eq!(
            status("GET / HTTP/1.1\r\nExpect: 100-continue\r\n\r\n"),
            417
        );
        assert_eq!(status("GET / HTTP/1.1\r\nHost : x\r\n\r\n"), 400);
        assert_eq!(status("GET / HTTP/1.1\r\nA: b\r\n c\r\n\r\n"), 400);
        assert_eq!(status("GET / HTTP/2.0\r\n\r\n"), 505);
        assert_eq!(status("GET / HTTP/x\r\n\r\n"), 400);
        assert_eq!(status("GET / HTTP/1.1\r\nContent-Length: -1\r\n\r\n"), 400);
        assert_eq!(
            status("GET / HTTP/1.1\r\nAuthorization: a\r\nauthorization: b\r\n\r\n"),
            400
        );
    }

    #[test]
    fn identical_duplicate_content_length_is_accepted() {
        let request =
            parse("POST / HTTP/1.1\r\nContent-Length: 1\r\nContent-Length: 1\r\n\r\na").unwrap();
        assert_eq!(request.body, b"a");
    }

    #[test]
    fn bare_lf_framing_is_tolerated() {
        let request = parse("POST / HTTP/1.1\nContent-Length: 1\n\na").unwrap();
        assert_eq!(request.body, b"a");
    }

    #[test]
    fn caps_apply() {
        let mut config = cfg();
        config.max_body_bytes = 4;
        let ok = parse_request_bytes(b"POST / HTTP/1.1\r\nContent-Length: 4\r\n\r\nabcd", &config);
        assert!(ok.is_ok());
        let over = parse_request_bytes(
            b"POST / HTTP/1.1\r\nContent-Length: 5\r\n\r\nabcde",
            &config,
        );
        assert_eq!(over.unwrap_err().status, 413);
        let many = format!("GET / HTTP/1.1\r\n{}\r\n", "A: b\r\n".repeat(101));
        assert_eq!(parse(&many).unwrap_err().status, 431);
        let long = format!("GET / HTTP/1.1\r\nA: {}\r\n\r\n", "x".repeat(9000));
        assert_eq!(parse(&long).unwrap_err().status, 431);
    }

    #[test]
    fn continue_policy_ignores_other_expect_values() {
        let mut config = cfg();
        config.expect = ExpectPolicy::Continue100;
        let raw = b"GET / HTTP/1.1\r\nExpect: whatever\r\n\r\n";
        assert!(parse_request_bytes(raw, &config).is_ok());
    }
}
