//! Connection admission.
//!
//! The accept thread must never block on a client. Every accepted socket
//! first lands in [`ConnFrontend`], which:
//!
//! * stamps the accept time, so the whole-request deadline is measured from
//!   accept and not from the moment a worker finally dequeues the socket;
//! * enforces a per-IP concurrent-connection cap;
//! * when a health lane is configured, peeks (non-blocking) at the request
//!   line so health-class requests are routed to a small reserved lane that
//!   is never queued behind slow work;
//! * in that mode also closes sockets that send nothing within the
//!   first-byte timeout, so idle sockets never occupy a worker.

use std::collections::HashMap;
use std::io::ErrorKind;
use std::net::{IpAddr, SocketAddr, TcpStream};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::config::ServerConfig;
use crate::server::HealthClassifier;

/// Bytes peeked to classify a request.
const PEEK_BYTES: usize = 256;
/// How often the accept loop should poll while connections are pending.
pub(crate) const PENDING_POLL_INTERVAL: Duration = Duration::from_millis(5);

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
struct IpPermit {
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
    pub peer: SocketAddr,
    _permit: Option<IpPermit>,
}

impl Conn {
    /// A connection not subject to admission control (tests, one-shot mode).
    pub fn unmanaged(stream: TcpStream) -> Option<Self> {
        let peer = stream.peer_addr().ok()?;
        Some(Self {
            stream,
            accepted_at: Instant::now(),
            peer,
            _permit: None,
        })
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

/// A connection refused at admission; the caller answers 503.
#[derive(Debug)]
pub struct Rejected(pub TcpStream);

/// Admission control in front of the worker queues.
#[derive(Debug)]
pub struct ConnFrontend {
    first_byte_timeout: Duration,
    max_per_ip: usize,
    max_pending: usize,
    /// Peek at request lines (health lane enabled).
    peek: bool,
    classifier: HealthClassifier,
    counts: Arc<IpCounts>,
    pending: Vec<Conn>,
    immediate: Vec<Conn>,
    last_poll: Instant,
}

impl ConnFrontend {
    pub fn new(cfg: &ServerConfig, classifier: HealthClassifier) -> Self {
        Self {
            first_byte_timeout: cfg.first_byte_timeout,
            max_per_ip: cfg.max_conns_per_ip,
            max_pending: cfg.max_pending,
            peek: cfg.health_workers > 0,
            classifier,
            counts: Arc::new(IpCounts::default()),
            pending: Vec::new(),
            immediate: Vec::new(),
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
        if self.pending.len() >= self.max_pending {
            return Err(Rejected(stream));
        }
        let ip = peer.ip().to_canonical();
        let permit = if self.max_per_ip > 0 {
            let mut map = self.counts.0.lock().unwrap_or_else(|p| p.into_inner());
            let count = map.entry(ip).or_insert(0);
            if *count >= self.max_per_ip {
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
        if self.peek {
            if stream.set_nonblocking(true).is_err() {
                return Ok(());
            }
            self.pending.push(Conn {
                stream,
                accepted_at,
                peer,
                _permit: permit,
            });
        } else {
            self.immediate.push(Conn {
                stream,
                accepted_at,
                peer,
                _permit: permit,
            });
        }
        Ok(())
    }

    /// Move connections that need no classification (health lane disabled)
    /// into `ready`.
    pub fn take_immediate(&mut self, ready: &mut Vec<(Lane, Conn)>) {
        ready.extend(self.immediate.drain(..).map(|conn| (Lane::General, conn)));
    }

    /// Advance pending connections: route those whose request line has
    /// arrived, drop the silent or closed ones.
    pub fn poll(&mut self, ready: &mut Vec<(Lane, Conn)>) {
        self.last_poll = Instant::now();
        let first_byte = self.first_byte_timeout;
        let mut still_pending = Vec::with_capacity(self.pending.len());
        for conn in self.pending.drain(..) {
            let age = conn.accepted_at.elapsed();
            let mut buf = [0u8; PEEK_BYTES];
            match conn.stream.peek(&mut buf) {
                // Peer closed before sending anything.
                Ok(0) => {}
                Ok(n) => match classify(&buf[..n], self.classifier) {
                    Some(lane) => ready.push((lane, conn)),
                    None if age >= first_byte => ready.push((Lane::General, conn)),
                    None => still_pending.push(conn),
                },
                Err(err)
                    if matches!(err.kind(), ErrorKind::WouldBlock | ErrorKind::Interrupted) =>
                {
                    if age < first_byte {
                        still_pending.push(conn);
                    }
                }
                Err(_) => {}
            }
        }
        self.pending = still_pending;
    }
}

/// Close a connection after an early error response (for example 413 or 417)
/// while the client may still be sending its body: half-close, then read and
/// discard briefly. Closing with unread data pending makes the kernel send a
/// reset that can destroy the response before the client has read it.
pub fn linger_close(stream: &mut TcpStream, max_bytes: usize, max_time: Duration) {
    use std::io::Read;
    let _ = stream.shutdown(std::net::Shutdown::Write);
    let deadline = Instant::now() + max_time;
    let mut buf = [0u8; 16 * 1024];
    let mut total = 0usize;
    while total < max_bytes {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() || stream.set_read_timeout(Some(remaining)).is_err() {
            break;
        }
        match stream.read(&mut buf) {
            Ok(0) | Err(_) => break,
            Ok(n) => total += n,
        }
    }
}

/// Paths served by the reserved health lane.
pub fn is_health_class_path(path: &str) -> bool {
    matches!(
        path,
        "/health" | "/live" | "/ready" | "/v1/health" | "/v1/live" | "/v1/ready" | "/metrics"
    )
}

/// Health lane for `GET` requests to the standard liveness, readiness and
/// metrics paths.
pub fn default_health_classifier(method: &str, path: &str) -> bool {
    method == "GET" && is_health_class_path(path)
}

/// `Some(lane)` once the request line is decidable, `None` if more bytes
/// are needed.
fn classify(buf: &[u8], classifier: HealthClassifier) -> Option<Lane> {
    let first_space = buf.iter().position(|b| *b == b' ');
    let Some(first_space) = first_space else {
        // Method not complete yet (or a line without a space).
        return if buf.contains(&b'\n') || buf.len() >= PEEK_BYTES {
            Some(Lane::General)
        } else {
            None
        };
    };
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
    match (
        std::str::from_utf8(&buf[..first_space]),
        std::str::from_utf8(path),
    ) {
        (Ok(method), Ok(path)) if classifier(method, path) => Some(Lane::Health),
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

    fn test_cfg(first_byte: Duration, per_ip: usize) -> ServerConfig {
        let mut cfg = ServerConfig::new("test", 1, 1);
        cfg.first_byte_timeout = first_byte;
        cfg.max_conns_per_ip = per_ip;
        cfg
    }

    #[test]
    fn classifies_request_lines() {
        let c = default_health_classifier;
        assert_eq!(classify(b"GET /health HTTP/1.1\r\n", c), Some(Lane::Health));
        assert_eq!(classify(b"GET /v1/ready?x=1 HTTP/1.1", c), Some(Lane::Health));
        assert_eq!(classify(b"GET /metrics ", c), Some(Lane::Health));
        assert_eq!(classify(b"GET /v1/retrieve HTTP/1.1", c), Some(Lane::General));
        assert_eq!(classify(b"POST /health HTTP/1.1", c), Some(Lane::General));
        assert_eq!(classify(b"GE", c), None);
        assert_eq!(classify(b"GET /hea", c), None);
        assert_eq!(classify(b"\r\n", c), Some(Lane::General));
    }

    #[test]
    fn silent_connections_are_dropped_and_requests_routed() {
        let mut front = ConnFrontend::new(
            &test_cfg(Duration::from_millis(100), 0),
            default_health_classifier,
        );
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
        let mut front = ConnFrontend::new(
            &test_cfg(Duration::from_secs(5), 2),
            default_health_classifier,
        );
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
