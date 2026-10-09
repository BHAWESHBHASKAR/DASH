use std::{
    collections::HashMap,
    io::{ErrorKind, Read},
    net::TcpStream,
    time::{Duration, Instant},
};

use super::{HttpRequest, MAX_HTTP_BODY_BYTES};

/// Maximum length of a single header line (including the request line).
pub(crate) const MAX_HEADER_LINE_BYTES: usize = 8 * 1024;
/// Maximum total size of the request line plus all headers.
pub(crate) const MAX_HEADER_BLOCK_BYTES: usize = 32 * 1024;
/// Maximum number of header fields.
pub(crate) const MAX_HEADER_COUNT: usize = 100;
/// Default whole-request read deadline.
pub(crate) const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 10_000;
/// Upper bound on bytes allocated ahead of bytes actually received.
const READ_CHUNK_BYTES: usize = 4096;

/// A request-reading failure carrying the HTTP status that should be returned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HttpReadError {
    pub(crate) status: u16,
    pub(crate) message: String,
}

impl HttpReadError {
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

/// Resolve the whole-request deadline from `DASH_HTTP_REQUEST_TIMEOUT_MS`.
pub(crate) fn resolve_request_timeout() -> Duration {
    let millis = std::env::var("DASH_HTTP_REQUEST_TIMEOUT_MS")
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_REQUEST_TIMEOUT_MS);
    Duration::from_millis(millis)
}

fn map_io_error(err: &std::io::Error) -> HttpReadError {
    match err.kind() {
        ErrorKind::WouldBlock | ErrorKind::TimedOut => HttpReadError::new(408, "request timed out"),
        _ => HttpReadError::bad_request("failed to read request"),
    }
}

/// Read once from the stream, never waiting past the whole-request deadline.
fn read_with_deadline(
    stream: &mut TcpStream,
    buf: &mut [u8],
    deadline: Instant,
) -> Result<usize, HttpReadError> {
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(HttpReadError::new(408, "request timed out"));
        }
        stream
            .set_read_timeout(Some(remaining))
            .map_err(|err| map_io_error(&err))?;
        match stream.read(buf) {
            Ok(n) => return Ok(n),
            Err(err) if err.kind() == ErrorKind::Interrupted => continue,
            Err(err) => return Err(map_io_error(&err)),
        }
    }
}

fn find_header_end(buf: &[u8], from: usize) -> Option<(usize, usize)> {
    // Returns (end_of_headers, start_of_body). Accepts CRLFCRLF and bare LFLF.
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

/// Read one request, giving up at `deadline` (measured from accept, so time
/// spent queued counts).
pub(crate) fn read_http_request_until(
    stream: &mut TcpStream,
    deadline: Instant,
) -> Result<Option<HttpRequest>, HttpReadError> {
    let mut buf: Vec<u8> = Vec::with_capacity(READ_CHUNK_BYTES);
    let mut chunk = [0u8; READ_CHUNK_BYTES];
    let mut scan_from = 0usize;

    let (head_end, body_start) = loop {
        if let Some(found) = find_header_end(&buf, scan_from) {
            break found;
        }
        scan_from = buf.len().saturating_sub(3);
        if buf.len() > MAX_HEADER_BLOCK_BYTES {
            return Err(HttpReadError::new(431, "request headers too large"));
        }
        let n = read_with_deadline(stream, &mut chunk, deadline)?;
        if n == 0 {
            if buf.is_empty() {
                return Ok(None);
            }
            return Err(HttpReadError::bad_request("unexpected end of request"));
        }
        buf.extend_from_slice(&chunk[..n]);
    };
    if head_end > MAX_HEADER_BLOCK_BYTES {
        return Err(HttpReadError::new(431, "request headers too large"));
    }

    let head = std::str::from_utf8(&buf[..head_end])
        .map_err(|_| HttpReadError::bad_request("request headers must be valid UTF-8"))?;
    let mut lines = head
        .split('\n')
        .map(|line| line.strip_suffix('\r').unwrap_or(line));
    let request_line = lines.next().unwrap_or("");
    if request_line.len() > MAX_HEADER_LINE_BYTES {
        return Err(HttpReadError::new(431, "request line too large"));
    }
    let (method, target) =
        parse_request_line(request_line).map_err(|e| HttpReadError::bad_request(&e))?;

    let mut headers: HashMap<String, String> = HashMap::new();
    let mut header_count = 0usize;
    for line in lines {
        if line.is_empty() {
            continue;
        }
        if line.len() > MAX_HEADER_LINE_BYTES {
            return Err(HttpReadError::new(431, "request header line too large"));
        }
        header_count += 1;
        if header_count > MAX_HEADER_COUNT {
            return Err(HttpReadError::new(431, "too many request headers"));
        }
        let (name, value) = line
            .split_once(':')
            .ok_or_else(|| HttpReadError::bad_request("invalid HTTP header"))?;
        let name = name.trim().to_ascii_lowercase();
        let value = value.trim().to_string();
        if name == "content-length"
            && let Some(existing) = headers.get(&name)
            && *existing != value
        {
            return Err(HttpReadError::bad_request(
                "conflicting content-length headers",
            ));
        }
        headers.insert(name, value);
    }

    if headers.contains_key("transfer-encoding") {
        return Err(HttpReadError::new(
            501,
            "transfer-encoding is not supported; send a content-length body",
        ));
    }

    let content_length = match headers.get("content-length") {
        Some(raw) => {
            if raw.is_empty() || !raw.bytes().all(|b| b.is_ascii_digit()) {
                return Err(HttpReadError::bad_request("invalid content-length header"));
            }
            match raw.parse::<usize>() {
                Ok(value) => value,
                Err(_) => {
                    return Err(HttpReadError::new(
                        413,
                        &format!(
                            "content-length exceeds max body size ({MAX_HTTP_BODY_BYTES} bytes)"
                        ),
                    ));
                }
            }
        }
        None => 0,
    };
    if content_length > MAX_HTTP_BODY_BYTES {
        return Err(HttpReadError::new(
            413,
            &format!("content-length exceeds max body size ({MAX_HTTP_BODY_BYTES} bytes)"),
        ));
    }

    // Allocate only what has actually arrived (plus one chunk); the buffer
    // grows as bytes are received so a lying Content-Length costs nothing.
    let mut body: Vec<u8> = Vec::with_capacity(content_length.min(READ_CHUNK_BYTES));
    let already = &buf[body_start.min(buf.len())..];
    body.extend_from_slice(&already[..already.len().min(content_length)]);
    while body.len() < content_length {
        let want = (content_length - body.len()).min(READ_CHUNK_BYTES);
        let n = read_with_deadline(stream, &mut chunk[..want], deadline)?;
        if n == 0 {
            return Err(HttpReadError::bad_request("request body truncated"));
        }
        body.extend_from_slice(&chunk[..n]);
    }

    Ok(Some(HttpRequest {
        method,
        target,
        headers,
        body,
    }))
}

pub(super) fn split_target(target: &str) -> (String, HashMap<String, String>) {
    let (path, query_str) = target
        .split_once('?')
        .map(|(path, query)| (path, Some(query)))
        .unwrap_or((target, None));
    let mut query = HashMap::new();
    if let Some(query_str) = query_str {
        for pair in query_str.split('&') {
            if pair.is_empty() {
                continue;
            }
            let (raw_key, raw_value) = pair.split_once('=').unwrap_or((pair, ""));
            let (Ok(key), Ok(value)) = (url_decode(raw_key), url_decode(raw_value)) else {
                continue;
            };
            query.insert(key, value);
        }
    }
    (path.to_string(), query)
}

fn url_decode(raw: &str) -> Result<String, String> {
    let bytes = raw.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' => {
                if i + 2 >= bytes.len() {
                    return Err("incomplete percent escape".to_string());
                }
                let hi = decode_hex(bytes[i + 1])?;
                let lo = decode_hex(bytes[i + 2])?;
                out.push((hi << 4) | lo);
                i += 3;
            }
            other => {
                out.push(other);
                i += 1;
            }
        }
    }
    String::from_utf8(out).map_err(|_| "invalid UTF-8 in URL field".to_string())
}

fn decode_hex(byte: u8) -> Result<u8, String> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err("invalid hex digit".to_string()),
    }
}

pub(super) fn parse_query_usize(
    query: &HashMap<String, String>,
    key: &str,
) -> Result<Option<usize>, String> {
    match query.get(key) {
        None => Ok(None),
        Some(value) => value
            .parse::<usize>()
            .map(Some)
            .map_err(|_| format!("query parameter '{key}' must be a positive integer")),
    }
}

pub(super) fn parse_request_line(line: &str) -> Result<(String, String), String> {
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
    if !version.starts_with("HTTP/1.") {
        return Err("unsupported HTTP version".to_string());
    }
    Ok((method.to_string(), target.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_target_percent_decodes_query_values() {
        let (path, query) =
            split_target("/internal/replication/ack?commit_id=a%3Ab%2Fc&replica_id=r+1&flag");
        assert_eq!(path, "/internal/replication/ack");
        assert_eq!(query.get("commit_id").map(String::as_str), Some("a:b/c"));
        assert_eq!(query.get("replica_id").map(String::as_str), Some("r 1"));
        assert_eq!(query.get("flag").map(String::as_str), Some(""));
    }

    #[test]
    fn split_target_skips_malformed_escapes() {
        let (_, query) = split_target("/x?bad=%zz&ok=1");
        assert!(!query.contains_key("bad"));
        assert_eq!(query.get("ok").map(String::as_str), Some("1"));
    }
}
