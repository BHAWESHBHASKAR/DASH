//! Connection admission shared by the HTTP servers.
//!
//! The accept thread must never block on a client. Every accepted socket
//! first lands in [`ConnFrontend`], which:
//!
//! * stamps the accept time, so the whole-request deadline is measured from
//!   accept and not from the moment a worker finally dequeues the socket;
//! * enforces a per-IP concurrent-connection cap (`DASH_HTTP_MAX_CONNS_PER_IP`);
//! * peeks (non-blocking) at the request line so health-class requests are
//!   routed to a small reserved lane that is never queued behind slow work;
//! * closes sockets that send nothing within `DASH_HTTP_FIRST_BYTE_TIMEOUT_MS`,
//!   so idle sockets never occupy a worker.

use std::collections::HashMap;
use std::io::ErrorKind;
use std::net::{IpAddr, TcpStream};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Default time a new connection may stay silent before it is closed.
pub const DEFAULT_FIRST_BYTE_TIMEOUT_MS: u64 = 2_000;
/// Default per-IP concurrent connection cap (0 disables the cap).
pub const DEFAULT_MAX_CONNS_PER_IP: usize = 64;
/// Hard bound on connections waiting for their first bytes.
const MAX_PENDING: usize = 4096;
/// Bytes peeked to classify a request.
const PEEK_BYTES: usize = 256;
/// How often the accept loop should poll while connections are pending.
pub const PENDING_POLL_INTERVAL: Duration = Duration::from_millis(5);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConnConfig {
    pub first_byte_timeout: Duration,
    /// 0 disables the per-IP cap.
    pub max_per_ip: usize,
}

impl ConnConfig {
    pub fn from_env() -> Self {
        let first_byte = std::env::var("DASH_HTTP_FIRST_BYTE_TIMEOUT_MS")
            .ok()
            .and_then(|raw| raw.trim().parse::<u64>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(DEFAULT_FIRST_BYTE_TIMEOUT_MS);
        let max_per_ip = std::env::var("DASH_HTTP_MAX_CONNS_PER_IP")
            .ok()
            .and_then(|raw| raw.trim().parse::<usize>().ok())
            .unwrap_or(DEFAULT_MAX_CONNS_PER_IP);
        Self {
            first_byte_timeout: Duration::from_millis(first_byte),
            max_per_ip,
        }
    }
}

/// Which worker lane serves a connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lane {
    /// Liveness/readiness/metrics requests; served by reserved workers.
    Health,
    General,
}

#[derive(Debug, Default)]
struct IpCounts(Mutex<HashMap<IpAddr, usize>>);

/// A held per-IP connection slot, released on drop.
#[derive(Debug)]
pub struct IpPermit {
    counts: Arc<IpCounts>,
    ip: IpAddr,
}

impl Drop for IpPermit {
    fn drop(&mut self) {
        let mut map = self.counts.0.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(count) = map.get_mut(&self.ip) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                map.remove(&self.ip);
            }
        }
    }
}

/// An admitted connection travelling to a worker.
#[derive(Debug)]
pub struct Conn {
    pub stream: TcpStream,
    pub accepted_at: Instant,
    _permit: Option<IpPermit>,
}

impl Conn {
    /// A connection not subject to admission control (tests, one-shot mode).
    pub fn unmanaged(stream: TcpStream) -> Self {
        Self {
            stream,
            accepted_at: Instant::now(),
            _permit: None,
        }
    }

    /// Whole-request deadline, measured from accept.
    pub fn deadline(&self, request_timeout: Duration) -> Instant {
        self.accepted_at + request_timeout
    }

    /// True when the connection already waited out its request deadline in a
    /// queue; it is closed without doing any work.
    pub fn is_stale(&self, request_timeout: Duration) -> bool {
        self.accepted_at.elapsed() >= request_timeout
    }
}

#[derive(Debug)]
struct Pending {
    conn: Conn,
}

#[derive(Debug)]
pub struct ConnFrontend {
    cfg: ConnConfig,
    counts: Arc<IpCounts>,
    pending: Vec<Pending>,
    last_poll: Instant,
}

/// Why a connection was refused at admission.
#[derive(Debug)]
pub struct Rejected(pub TcpStream);

impl ConnFrontend {
    pub fn new(cfg: ConnConfig) -> Self {
        Self {
            cfg,
            counts: Arc::new(IpCounts::default()),
            pending: Vec::new(),
            last_poll: Instant::now(),
        }
    }

    pub fn has_pending(&self) -> bool {
        !self.pending.is_empty()
    }

    /// True when enough time passed since the last [`Self::poll`].
    pub fn poll_due(&self) -> bool {
        self.last_poll.elapsed() >= PENDING_POLL_INTERVAL
    }

    /// Admit a freshly accepted socket. `Err` hands the stream back when the
    /// per-IP cap or the pending bound is exceeded; the caller answers 503.
    /// A socket whose peer address cannot be read is simply closed.
    pub fn admit(&mut self, stream: TcpStream) -> Result<(), Rejected> {
        let accepted_at = Instant::now();
        let Ok(peer) = stream.peer_addr() else {
            return Ok(());
        };
        if self.pending.len() >= MAX_PENDING {
            return Err(Rejected(stream));
        }
        let ip = peer.ip().to_canonical();
        let permit = if self.cfg.max_per_ip > 0 {
            let mut map = self.counts.0.lock().unwrap_or_else(|p| p.into_inner());
            let count = map.entry(ip).or_insert(0);
            if *count >= self.cfg.max_per_ip {
                return Err(Rejected(stream));
            }
            *count += 1;
            Some(IpPermit {
                counts: Arc::clone(&self.counts),
                ip,
            })
        } else {
            None
        };
        if stream.set_nonblocking(true).is_err() {
            return Ok(());
        }
        self.pending.push(Pending {
            conn: Conn {
                stream,
                accepted_at,
                _permit: permit,
            },
        });
        Ok(())
    }

    /// Advance pending connections: route those whose request line has
    /// arrived, drop the silent or closed ones.
    pub fn poll(&mut self, ready: &mut Vec<(Lane, Conn)>) {
        self.last_poll = Instant::now();
        let first_byte = self.cfg.first_byte_timeout;
        let mut still_pending = Vec::with_capacity(self.pending.len());
        for entry in self.pending.drain(..) {
            let age = entry.conn.accepted_at.elapsed();
            let mut buf = [0u8; PEEK_BYTES];
            match entry.conn.stream.peek(&mut buf) {
                // Peer closed before sending anything.
                Ok(0) => {}
                Ok(n) => match classify(&buf[..n]) {
                    Some(lane) => ready.push((lane, entry.conn)),
                    None if age >= first_byte => ready.push((Lane::General, entry.conn)),
                    None => still_pending.push(entry),
                },
                Err(err)
                    if matches!(err.kind(), ErrorKind::WouldBlock | ErrorKind::Interrupted) =>
                {
                    if age < first_byte {
                        still_pending.push(entry);
                    }
                }
                Err(_) => {}
            }
        }
        self.pending = still_pending;
    }
}

/// Paths served by the reserved health lane.
pub fn is_health_class_path(path: &str) -> bool {
    matches!(
        path,
        "/health" | "/live" | "/ready" | "/v1/health" | "/v1/live" | "/v1/ready" | "/metrics"
    )
}

/// `Some(lane)` once the request line is decidable, `None` if more bytes
/// are needed.
fn classify(buf: &[u8]) -> Option<Lane> {
    let first_space = buf.iter().position(|b| *b == b' ');
    let Some(first_space) = first_space else {
        // Method not complete yet (or a line without a space).
        return if buf.contains(&b'\n') || buf.len() >= PEEK_BYTES {
            Some(Lane::General)
        } else {
            None
        };
    };
    if &buf[..first_space] != b"GET" {
        return Some(Lane::General);
    }
    let rest = &buf[first_space + 1..];
    let end = rest.iter().position(|b| matches!(*b, b' ' | b'\r' | b'\n'));
    let Some(end) = end else {
        return if rest.len() + first_space + 1 >= PEEK_BYTES {
            Some(Lane::General)
        } else {
            None
        };
    };
    let target = &rest[..end];
    let path = target
        .iter()
        .position(|b| *b == b'?')
        .map_or(target, |q| &target[..q]);
    match std::str::from_utf8(path) {
        Ok(path) if is_health_class_path(path) => Some(Lane::Health),
        _ => Some(Lane::General),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::net::TcpListener;

    fn pair() -> (TcpStream, TcpStream, TcpListener) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (server, _) = listener.accept().unwrap();
        (client, server, listener)
    }

    #[test]
    fn classifies_request_lines() {
        assert_eq!(classify(b"GET /health HTTP/1.1\r\n"), Some(Lane::Health));
        assert_eq!(classify(b"GET /v1/ready?x=1 HTTP/1.1"), Some(Lane::Health));
        assert_eq!(classify(b"GET /metrics "), Some(Lane::Health));
        assert_eq!(classify(b"GET /v1/retrieve HTTP/1.1"), Some(Lane::General));
        assert_eq!(classify(b"POST /health HTTP/1.1"), Some(Lane::General));
        assert_eq!(classify(b"GE"), None);
        assert_eq!(classify(b"GET /hea"), None);
        assert_eq!(classify(b"\r\n"), Some(Lane::General));
    }

    #[test]
    fn silent_connections_are_dropped_and_requests_routed() {
        let cfg = ConnConfig {
            first_byte_timeout: Duration::from_millis(100),
            max_per_ip: 0,
        };
        let mut front = ConnFrontend::new(cfg);
        let (_silent_client, silent, _l1) = pair();
        let (mut health_client, health, _l2) = pair();
        front.admit(silent).unwrap();
        front.admit(health).unwrap();
        health_client
            .write_all(b"GET /health HTTP/1.1\r\n\r\n")
            .unwrap();
        std::thread::sleep(Duration::from_millis(20));
        let mut ready = Vec::new();
        front.poll(&mut ready);
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].0, Lane::Health);
        assert!(front.has_pending());
        std::thread::sleep(Duration::from_millis(120));
        front.poll(&mut ready);
        assert!(!front.has_pending(), "silent socket must be closed");
        assert_eq!(ready.len(), 1);
    }

    #[test]
    fn per_ip_cap_rejects_excess_and_releases_on_drop() {
        let cfg = ConnConfig {
            first_byte_timeout: Duration::from_secs(5),
            max_per_ip: 2,
        };
        let mut front = ConnFrontend::new(cfg);
        let mut clients = Vec::new();
        let mut rejected = 0;
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        for _ in 0..4 {
            clients.push(TcpStream::connect(listener.local_addr().unwrap()).unwrap());
            let (server, _) = listener.accept().unwrap();
            if front.admit(server).is_err() {
                rejected += 1;
            }
        }
        assert_eq!(rejected, 2);
        drop(clients);
        std::thread::sleep(Duration::from_millis(20));
        let mut ready = Vec::new();
        front.poll(&mut ready);
        assert!(!front.has_pending());
        let c = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (server, _) = listener.accept().unwrap();
        assert!(front.admit(server).is_ok(), "slots released after close");
        drop(c);
    }
}
