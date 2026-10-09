//! A tiny blocking HTTP/1.1 client over `std::net::TcpStream`.
//!
//! It always asks for `Connection: close`, reads the whole response and
//! understands `Content-Length` and chunked bodies. It also exposes the raw
//! socket helpers the hostile-input tests need.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

use serde_json::Value;

#[derive(Debug, Clone)]
pub struct Resp {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: String,
}

impl Resp {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    pub fn json(&self) -> Value {
        serde_json::from_str(&self.body).unwrap_or_else(|e| {
            panic!(
                "response is not JSON ({e}): status={} body={:?}",
                self.status, self.body
            )
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Client {
    pub addr: SocketAddr,
    pub timeout: Duration,
}

impl Client {
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            timeout: Duration::from_secs(20),
        }
    }

    pub fn connect(&self) -> std::io::Result<TcpStream> {
        let s = TcpStream::connect_timeout(&self.addr, Duration::from_secs(5))?;
        s.set_read_timeout(Some(self.timeout))?;
        s.set_write_timeout(Some(self.timeout))?;
        s.set_nodelay(true).ok();
        Ok(s)
    }

    /// Send a request; `headers` are extra `(name, value)` pairs.
    pub fn request(
        &self,
        method: &str,
        target: &str,
        headers: &[(&str, &str)],
        body: Option<&[u8]>,
    ) -> std::io::Result<Resp> {
        let mut s = self.connect()?;
        let mut head = format!(
            "{method} {target} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n",
            self.addr
        );
        for (k, v) in headers {
            head.push_str(&format!("{k}: {v}\r\n"));
        }
        if let Some(b) = body {
            head.push_str(&format!("Content-Length: {}\r\n", b.len()));
        }
        head.push_str("\r\n");
        s.write_all(head.as_bytes())?;
        if let Some(b) = body {
            s.write_all(b)?;
        }
        read_response(&mut s)
    }

    pub fn get(&self, target: &str, headers: &[(&str, &str)]) -> Resp {
        self.request("GET", target, headers, None)
            .unwrap_or_else(|e| panic!("GET {target} failed: {e}"))
    }

    pub fn post_json(&self, target: &str, headers: &[(&str, &str)], body: &Value) -> Resp {
        self.try_post_json(target, headers, body)
            .unwrap_or_else(|e| panic!("POST {target} failed: {e}"))
    }

    /// Like `post_json` but returns the io error instead of panicking
    /// (used when the server may be killed mid-request).
    pub fn try_post_json(
        &self,
        target: &str,
        headers: &[(&str, &str)],
        body: &Value,
    ) -> std::io::Result<Resp> {
        let mut h: Vec<(&str, &str)> = vec![("Content-Type", "application/json")];
        h.extend_from_slice(headers);
        let bytes = serde_json::to_vec(body).unwrap();
        self.request("POST", target, &h, Some(&bytes))
    }

    /// Send arbitrary bytes and read whatever response comes back. `Ok(None)`
    /// means the server closed the connection without answering.
    pub fn raw(&self, bytes: &[u8]) -> std::io::Result<Option<Resp>> {
        let mut s = self.connect()?;
        // The server may reject early and close while we are still writing;
        // a write error is then not a failure of the exchange.
        let _ = s.write_all(bytes);
        let _ = s.flush();
        read_optional(&mut s)
    }
}

/// Read a response; connection-level termination without a head is `None`.
pub fn read_optional(s: &mut TcpStream) -> std::io::Result<Option<Resp>> {
    use std::io::ErrorKind::*;
    match read_response(s) {
        Ok(r) => Ok(Some(r)),
        Err(e) if matches!(e.kind(), UnexpectedEof | ConnectionReset | BrokenPipe) => Ok(None),
        Err(e) => Err(e),
    }
}

pub fn read_response(s: &mut TcpStream) -> std::io::Result<Resp> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        if let Some(he) = find(&buf, b"\r\n\r\n") {
            let head = String::from_utf8_lossy(&buf[..he]).to_string();
            let cl = head.lines().find_map(|l| {
                let (k, v) = l.split_once(':')?;
                if k.eq_ignore_ascii_case("content-length") {
                    v.trim().parse::<usize>().ok()
                } else {
                    None
                }
            });
            let chunked = head
                .to_ascii_lowercase()
                .contains("transfer-encoding: chunked");
            if let Some(cl) = cl {
                if buf.len() >= he + 4 + cl {
                    break;
                }
            } else if chunked && find(&buf[he + 4..], b"0\r\n\r\n").is_some() {
                break;
            }
        }
        match s.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
            Err(e) if !buf.is_empty() && e.kind() == std::io::ErrorKind::ConnectionReset => break,
            Err(e) => return Err(e),
        }
    }
    parse_response(&buf)
}

fn find(hay: &[u8], needle: &[u8]) -> Option<usize> {
    hay.windows(needle.len()).position(|w| w == needle)
}

fn parse_response(buf: &[u8]) -> std::io::Result<Resp> {
    let he = find(buf, b"\r\n\r\n").ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            format!("no complete response head ({} bytes)", buf.len()),
        )
    })?;
    let head = String::from_utf8_lossy(&buf[..he]).to_string();
    let mut lines = head.split("\r\n");
    let status_line = lines.next().unwrap_or_default();
    let status: u16 = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("bad status line {status_line:?}"),
            )
        })?;
    let headers: Vec<(String, String)> = lines
        .filter_map(|l| {
            l.split_once(':')
                .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
        })
        .collect();
    let raw_body = &buf[he + 4..];
    let chunked = headers.iter().any(|(k, v)| {
        k.eq_ignore_ascii_case("transfer-encoding") && v.to_ascii_lowercase().contains("chunked")
    });
    let body_bytes = if chunked {
        dechunk(raw_body)
    } else {
        raw_body.to_vec()
    };
    Ok(Resp {
        status,
        headers,
        body: String::from_utf8_lossy(&body_bytes).to_string(),
    })
}

fn dechunk(mut data: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let Some(p) = find(data, b"\r\n") else { break };
        let size =
            usize::from_str_radix(String::from_utf8_lossy(&data[..p]).trim(), 16).unwrap_or(0);
        data = &data[p + 2..];
        if size == 0 || data.len() < size {
            break;
        }
        out.extend_from_slice(&data[..size]);
        data = &data[(size + 2).min(data.len())..];
    }
    out
}
