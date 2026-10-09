use std::{
    collections::HashMap,
    io::{ErrorKind, Read, Write},
    net::TcpStream,
    time::{Duration, Instant},
};

use super::{HttpRequest, HttpResponse, MAX_HTTP_BODY_BYTES};

/// Maximum length of a single header line (including the request line).
pub(super) const MAX_HEADER_LINE_BYTES: usize = 8 * 1024;
/// Maximum total size of the request line plus all headers.
pub(super) const MAX_HEADER_BLOCK_BYTES: usize = 32 * 1024;
/// Maximum number of header fields.
pub(super) const MAX_HEADER_COUNT: usize = 100;
/// Default whole-request read deadline.
pub(super) const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 10_000;
/// Parse error for a well-formed HTTP version other than 1.0/1.1 (answered
/// with 505).
const UNSUPPORTED_VERSION: &str = "unsupported HTTP version";
/// Upper bound on bytes allocated ahead of bytes actually received.
const READ_CHUNK_BYTES: usize = 4096;

/// A request-reading failure carrying the HTTP status that should be returned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct HttpReadError {
    pub(super) status: u16,
    pub(super) message: String,
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
pub(super) fn resolve_request_timeout() -> Duration {
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
pub(super) fn read_http_request_until(
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
    let (method, target) = parse_request_line(request_line).map_err(|e| {
        let status = if e == UNSUPPORTED_VERSION { 505 } else { 400 };
        HttpReadError::new(status, &e)
    })?;

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
        if line.starts_with([' ', '\t']) {
            return Err(HttpReadError::bad_request(
                "obsolete header line folding is not supported",
            ));
        }
        let (name, value) = line
            .split_once(':')
            .ok_or_else(|| HttpReadError::bad_request("invalid HTTP header"))?;
        if name.is_empty() || name.contains([' ', '\t']) {
            return Err(HttpReadError::bad_request("invalid HTTP header name"));
        }
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

    if headers.contains_key("expect") {
        // Answer immediately instead of waiting for a body the client will
        // not send until it sees `100 Continue`.
        return Err(HttpReadError::new(
            417,
            "the Expect header is not supported; send the body directly",
        ));
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
            let key = match url_decode(raw_key) {
                Ok(value) => value,
                Err(_) => continue,
            };
            let value = match url_decode(raw_value) {
                Ok(value) => value,
                Err(_) => continue,
            };
            query.insert(key, value);
        }
    }
    (path.to_string(), query)
}

/// True when the query string contains an invalid percent-encoding (bad hex
/// digits, truncated escape, or non-UTF-8 bytes). `split_target` silently
/// skips such parameters, so callers must reject the request first.
pub(super) fn query_encoding_is_invalid(target: &str) -> bool {
    let Some((_, query)) = target.split_once('?') else {
        return false;
    };
    query.split('&').any(|pair| {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        url_decode(key).is_err() || url_decode(value).is_err()
    })
}

pub(super) fn write_response(
    stream: &mut TcpStream,
    response: HttpResponse,
) -> std::io::Result<()> {
    stream.write_all(render_response_text(&response).as_bytes())?;
    stream.flush()
}

pub(super) fn render_response_text(response: &HttpResponse) -> String {
    let status_text = match response.status {
        200 => "200 OK",
        400 => "400 Bad Request",
        401 => "401 Unauthorized",
        403 => "403 Forbidden",
        404 => "404 Not Found",
        405 => "405 Method Not Allowed",
        408 => "408 Request Timeout",
        411 => "411 Length Required",
        413 => "413 Payload Too Large",
        417 => "417 Expectation Failed",
        429 => "429 Too Many Requests",
        431 => "431 Request Header Fields Too Large",
        501 => "501 Not Implemented",
        502 => "502 Bad Gateway",
        503 => "503 Service Unavailable",
        505 => "505 HTTP Version Not Supported",
        _ => "500 Internal Server Error",
    };
    let body_len = response.body.len();
    let retry_after = response
        .retry_after_secs
        .map(|secs| format!("Retry-After: {secs}\r\n"))
        .unwrap_or_default();
    format!(
        "HTTP/1.1 {status_text}\r\nContent-Type: {}\r\nContent-Length: {body_len}\r\n{retry_after}Connection: close\r\n\r\n{}",
        response.content_type, response.body
    )
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

#[cfg(test)]
mod tests {
    use super::*;

    fn status_line_for(status: u16) -> String {
        let response = HttpResponse {
            status,
            content_type: "application/json",
            body: "{}".to_string(),
            retry_after_secs: None,
        };
        render_response_text(&response)
            .lines()
            .next()
            .unwrap_or_default()
            .to_string()
    }

    #[test]
    fn status_lines_cover_all_supported_codes() {
        for (code, expected) in [
            (400, "HTTP/1.1 400 Bad Request"),
            (401, "HTTP/1.1 401 Unauthorized"),
            (403, "HTTP/1.1 403 Forbidden"),
            (404, "HTTP/1.1 404 Not Found"),
            (405, "HTTP/1.1 405 Method Not Allowed"),
            (408, "HTTP/1.1 408 Request Timeout"),
            (411, "HTTP/1.1 411 Length Required"),
            (413, "HTTP/1.1 413 Payload Too Large"),
            (429, "HTTP/1.1 429 Too Many Requests"),
            (431, "HTTP/1.1 431 Request Header Fields Too Large"),
            (500, "HTTP/1.1 500 Internal Server Error"),
            (501, "HTTP/1.1 501 Not Implemented"),
            (502, "HTTP/1.1 502 Bad Gateway"),
            (503, "HTTP/1.1 503 Service Unavailable"),
        ] {
            assert_eq!(status_line_for(code), expected);
        }
    }

    #[test]
    fn error_with_status_preserves_status_code() {
        for code in [408u16, 411, 413, 429, 431, 501, 502] {
            assert_eq!(HttpResponse::error_with_status(code, "x").status, code);
        }
    }
}
