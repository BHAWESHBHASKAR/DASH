use std::{
    collections::HashMap,
    io::{BufRead, BufReader, Read, Write},
    net::TcpStream,
};

use super::{HttpRequest, HttpResponse, MAX_HTTP_BODY_BYTES};

pub(super) fn read_http_request(stream: &mut TcpStream) -> Result<Option<HttpRequest>, String> {
    let mut reader = BufReader::new(stream);

    let mut request_line = String::new();
    let bytes = reader
        .read_line(&mut request_line)
        .map_err(|e| e.to_string())?;
    if bytes == 0 {
        return Ok(None);
    }

    let (method, target) = parse_request_line(&request_line)?;

    let mut headers = HashMap::new();
    loop {
        let mut header_line = String::new();
        let bytes = reader
            .read_line(&mut header_line)
            .map_err(|e| e.to_string())?;
        if bytes == 0 || header_line == "\r\n" {
            break;
        }

        let (name, value) = header_line
            .split_once(':')
            .ok_or_else(|| "invalid HTTP header".to_string())?;
        headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_string());
    }

    let content_length = match headers.get("content-length") {
        Some(raw) => raw
            .parse::<usize>()
            .map_err(|_| "invalid content-length header".to_string())?,
        None => 0,
    };
    if content_length > MAX_HTTP_BODY_BYTES {
        return Err(format!(
            "content-length exceeds max body size ({MAX_HTTP_BODY_BYTES} bytes)"
        ));
    }

    let mut body = vec![0u8; content_length];
    if content_length > 0 {
        reader.read_exact(&mut body).map_err(|e| e.to_string())?;
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
    if !version.starts_with("HTTP/1.") {
        return Err("unsupported HTTP version".to_string());
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
        503 => "503 Service Unavailable",
        _ => "500 Internal Server Error",
    };
    let body_len = response.body.len();
    format!(
        "HTTP/1.1 {status_text}\r\nContent-Type: {}\r\nContent-Length: {body_len}\r\nConnection: close\r\n\r\n{}",
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
