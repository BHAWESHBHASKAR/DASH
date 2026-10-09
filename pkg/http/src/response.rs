use std::borrow::Cow;

/// A response to render. `Content-Length` and `Connection: close` are added
/// by the renderer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Response {
    pub status: u16,
    pub content_type: Cow<'static, str>,
    pub body: String,
    /// Extra headers, rendered in order after `Content-Length` (for example
    /// `Retry-After`). Entries whose name or value contain CR or LF are
    /// dropped rather than allowing header injection.
    pub headers: Vec<(Cow<'static, str>, String)>,
}

impl Response {
    pub fn new(status: u16, content_type: &'static str, body: String) -> Self {
        Self {
            status,
            content_type: Cow::Borrowed(content_type),
            body,
            headers: Vec::new(),
        }
    }

    pub fn json(status: u16, body: String) -> Self {
        Self::new(status, "application/json", body)
    }

    /// `{"error":"<message>"}` with the given status.
    pub fn error(status: u16, message: &str) -> Self {
        Self::json(
            status,
            format!("{{\"error\":\"{}\"}}", json_escape(message)),
        )
    }

    pub fn with_header(mut self, name: &'static str, value: impl Into<String>) -> Self {
        self.headers.push((Cow::Borrowed(name), value.into()));
        self
    }
}

/// Reason phrase line (without the HTTP version) for the statuses the
/// services emit. Unknown statuses render as 500.
pub fn status_line(status: u16) -> &'static str {
    match status {
        200 => "200 OK",
        400 => "400 Bad Request",
        401 => "401 Unauthorized",
        403 => "403 Forbidden",
        404 => "404 Not Found",
        405 => "405 Method Not Allowed",
        408 => "408 Request Timeout",
        409 => "409 Conflict",
        411 => "411 Length Required",
        413 => "413 Payload Too Large",
        417 => "417 Expectation Failed",
        429 => "429 Too Many Requests",
        431 => "431 Request Header Fields Too Large",
        500 => "500 Internal Server Error",
        501 => "501 Not Implemented",
        502 => "502 Bad Gateway",
        503 => "503 Service Unavailable",
        505 => "505 HTTP Version Not Supported",
        _ => "500 Internal Server Error",
    }
}

/// Render the full response text.
pub fn render_response(response: &Response) -> String {
    let mut extra = String::new();
    for (name, value) in &response.headers {
        if name.contains(['\r', '\n']) || value.contains(['\r', '\n']) {
            continue;
        }
        extra.push_str(name);
        extra.push_str(": ");
        extra.push_str(value);
        extra.push_str("\r\n");
    }
    format!(
        "HTTP/1.1 {}\r\nContent-Type: {}\r\nContent-Length: {}\r\n{extra}Connection: close\r\n\r\n{}",
        status_line(response.status),
        response.content_type,
        response.body.len(),
        response.body
    )
}

/// Escape `raw` for use inside a JSON string literal. Every control
/// character below 0x20 is escaped, so the result is always valid JSON.
pub fn json_escape(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for ch in raw.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            ch if (ch as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", ch as u32)),
            _ => out.push(ch),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_lines_cover_all_supported_codes() {
        for (code, expected) in [
            (200, "HTTP/1.1 200 OK"),
            (400, "HTTP/1.1 400 Bad Request"),
            (401, "HTTP/1.1 401 Unauthorized"),
            (403, "HTTP/1.1 403 Forbidden"),
            (404, "HTTP/1.1 404 Not Found"),
            (405, "HTTP/1.1 405 Method Not Allowed"),
            (408, "HTTP/1.1 408 Request Timeout"),
            (409, "HTTP/1.1 409 Conflict"),
            (411, "HTTP/1.1 411 Length Required"),
            (413, "HTTP/1.1 413 Payload Too Large"),
            (417, "HTTP/1.1 417 Expectation Failed"),
            (429, "HTTP/1.1 429 Too Many Requests"),
            (431, "HTTP/1.1 431 Request Header Fields Too Large"),
            (500, "HTTP/1.1 500 Internal Server Error"),
            (501, "HTTP/1.1 501 Not Implemented"),
            (502, "HTTP/1.1 502 Bad Gateway"),
            (503, "HTTP/1.1 503 Service Unavailable"),
            (505, "HTTP/1.1 505 HTTP Version Not Supported"),
        ] {
            let text = render_response(&Response::json(code, "{}".into()));
            assert_eq!(text.lines().next().unwrap(), expected);
        }
    }

    #[test]
    fn extra_headers_render_in_order_and_injection_is_dropped() {
        let response = Response::json(429, "{}".into())
            .with_header("Retry-After", "3")
            .with_header("X-Bad", "a\r\nInjected: 1")
            .with_header("X-Ok", "v");
        let text = render_response(&response);
        assert!(text.contains(
            "Content-Length: 2\r\nRetry-After: 3\r\nX-Ok: v\r\nConnection: close\r\n\r\n{}"
        ));
        assert!(!text.contains("Injected"));
    }

    #[test]
    fn json_escape_produces_valid_json_for_control_characters() {
        let escaped = json_escape("a\u{0}b\u{1f}c\u{8}\n");
        assert_eq!(escaped, "a\\u0000b\\u001fc\\u0008\\n");
        assert_eq!(json_escape("q\"\\\r\t"), "q\\\"\\\\\\r\\t");
    }
}
