use std::io::Write;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;

use crate::config::ServerConfig;
use crate::conn::{Conn, ConnFrontend, Lane, PENDING_POLL_INTERVAL, Rejected, linger_close};
use crate::parse::read_request;
use crate::request::Request;
use crate::response::{Response, render_response};

/// Maps a request to its response. Called on worker threads; a panic is
/// caught and answered with 500.
pub type Handler = Arc<dyn Fn(Request) -> Response + Send + Sync>;

/// Decides from the request method and path whether a request belongs on the
/// reserved health lane. It runs on the accept thread before the request is
/// read, so it only sees the request line.
pub type HealthClassifier = fn(method: &str, path: &str) -> bool;

/// Why a connection was shed before reaching a worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectReason {
    /// The lane's bounded queue was full.
    QueueFull,
    /// The peer already holds the maximum number of connections.
    PerIpCap,
}

/// Observation points so each service keeps its own metric names and output
/// format. All methods default to no-ops.
pub trait ServerHooks: Send + Sync {
    /// A connection is about to be queued for a worker.
    fn on_enqueued(&self) {}
    /// A queued connection left the queue (taken by a worker, or the queue
    /// refused it).
    fn on_dequeued(&self) {}
    /// A connection was answered with the overload response instead.
    fn on_reject(&self, _reason: RejectReason) {}
    /// A request failed while being read and was answered with `status`.
    fn on_read_error(&self, _status: u16) {}
    /// The accept loop stopped (shutdown requested); workers are draining.
    fn on_shutdown(&self) {}
}

/// Hooks that observe nothing.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoHooks;

impl ServerHooks for NoHooks {}

/// Shutdown flag polled by the accept loop.
pub trait Shutdown: Sync {
    fn is_triggered(&self) -> bool;

    /// True when the signal can never fire; the server then blocks in
    /// `accept` instead of polling.
    fn is_inert(&self) -> bool {
        false
    }
}

impl<F: Fn() -> bool + Sync> Shutdown for F {
    fn is_triggered(&self) -> bool {
        self()
    }
}

/// A shutdown signal that never fires.
#[derive(Debug, Default, Clone, Copy)]
pub struct NeverShutdown;

impl Shutdown for NeverShutdown {
    fn is_triggered(&self) -> bool {
        false
    }

    fn is_inert(&self) -> bool {
        true
    }
}

impl NeverShutdown {
    pub const fn new() -> Self {
        Self
    }
}

/// Source of connections; implemented for [`TcpListener`]. Tests wrap a
/// listener to inject accept failures.
pub trait Acceptor: Send {
    fn accept(&self) -> std::io::Result<(TcpStream, SocketAddr)>;
    fn set_nonblocking(&self, nonblocking: bool) -> std::io::Result<()>;
}

impl Acceptor for TcpListener {
    fn accept(&self) -> std::io::Result<(TcpStream, SocketAddr)> {
        TcpListener::accept(self)
    }

    fn set_nonblocking(&self, nonblocking: bool) -> std::io::Result<()> {
        TcpListener::set_nonblocking(self, nonblocking)
    }
}

/// Bounded exponential backoff (10ms .. 100ms) for repeated accept failures.
fn accept_error_backoff(streak: u32) -> Duration {
    let millis = 10u64.saturating_mul(1u64 << streak.saturating_sub(1).min(4));
    Duration::from_millis(millis.min(100))
}

fn write_response<W: Write + ?Sized>(stream: &mut W, response: &Response) -> std::io::Result<()> {
    stream.write_all(render_response(response).as_bytes())?;
    stream.flush()
}

/// Shed a connection refused at admission. Over TLS no handshake has
/// happened yet, so it is closed without an answer.
fn write_overload(mut stream: TcpStream, cfg: &ServerConfig) -> std::io::Result<()> {
    if cfg.tls.is_some() {
        return Ok(());
    }
    stream.set_write_timeout(Some(cfg.reject_write_timeout))?;
    stream.write_all(render_response(&cfg.overload_response).as_bytes())
}

/// Shed an admitted connection (queue full), encrypting the answer when the
/// connection speaks TLS.
fn write_overload_conn(conn: Conn, cfg: &ServerConfig) {
    conn.write_and_close(
        render_response(&cfg.overload_response).as_bytes(),
        cfg.reject_write_timeout,
    );
}

fn handle_connection(
    conn: &mut Conn,
    cfg: &ServerConfig,
    handler: &Handler,
    hooks: &dyn ServerHooks,
) -> std::io::Result<()> {
    let deadline = conn.deadline(cfg.request_deadline);
    let peer = conn.peer;
    let tls = conn.tls_info();
    conn.stream.set_nonblocking(false)?;
    conn.stream.set_write_timeout(Some(cfg.write_timeout))?;
    let stream = &mut conn.io();

    // The read deadline covers the whole request (headers and body), not
    // each individual read, so slow-trickle clients are dropped. It is
    // measured from accept, so queue wait counts against it.
    let mut request = match read_request(stream, cfg, deadline) {
        Ok(Some(request)) => request,
        Ok(None) => return Ok(()),
        Err(err) => {
            hooks.on_read_error(err.status);
            let result = write_response(stream, &Response::error(err.status, &err.message));
            stream.finish();
            linger_close(stream.socket(), cfg.linger_max_bytes, cfg.linger_max_time);
            return result;
        }
    };
    request.peer = Some(peer);
    request.tls = tls;

    let response = match catch_unwind(AssertUnwindSafe(|| handler(request))) {
        Ok(response) => response,
        Err(_) => {
            eprintln!("{} transport: request handler panicked", cfg.name);
            Response::error(500, "internal server error")
        }
    };
    write_response(stream, &response)?;
    stream.finish();
    Ok(())
}

/// Accept exactly one connection and serve it on the calling thread, without
/// admission control. A connection whose peer address cannot be read is
/// closed unanswered.
pub fn serve_once<A: Acceptor>(
    listener: &A,
    cfg: &ServerConfig,
    handler: &Handler,
    hooks: &dyn ServerHooks,
) -> std::io::Result<()> {
    let (stream, _) = listener.accept()?;
    let Some(mut conn) = Conn::unmanaged(stream) else {
        return Ok(());
    };
    if let Some(acceptor) = cfg.tls.as_ref()
        && !conn.handshake_blocking(acceptor, cfg.first_byte_timeout.min(cfg.request_deadline))
    {
        return Ok(());
    }
    handle_connection(&mut conn, cfg, handler, hooks)
}

type SharedRx = Arc<Mutex<mpsc::Receiver<Conn>>>;

/// Serve connections from `listener` until `shutdown` fires.
///
/// Accepted sockets are admitted (per-IP cap, optional health-lane
/// classification), queued on a bounded channel and handled by a fixed worker
/// pool. A full queue or an exhausted per-IP allowance is answered with
/// `cfg.overload_response` instead of blocking the accept thread. Accept
/// errors never end the loop; only `shutdown` does, after which queued
/// connections are drained before this function returns.
pub fn serve<A: Acceptor>(
    listener: A,
    cfg: ServerConfig,
    handler: Handler,
    health: HealthClassifier,
    shutdown: &dyn Shutdown,
    hooks: Arc<dyn ServerHooks>,
) -> std::io::Result<()> {
    let cfg = &cfg;
    let hooks = &*hooks;
    let handler = &handler;
    let workers = cfg.workers.max(1);
    let queue_capacity = cfg.queue_capacity.max(1);
    let (tx, rx) = mpsc::sync_channel::<Conn>(queue_capacity);
    let rx: SharedRx = Arc::new(Mutex::new(rx));
    // Reserved lane for health-class requests so slow work can never starve
    // liveness/readiness probes.
    let (health_tx, health_rx) =
        mpsc::sync_channel::<Conn>(queue_capacity.min(cfg.health_queue_capacity).max(1));
    let health_rx: SharedRx = Arc::new(Mutex::new(health_rx));
    let peek = cfg.health_workers > 0;
    // Polling is needed to interleave shutdown checks, pending-connection
    // classification and TLS handshakes with accept; otherwise block in
    // accept.
    let polling = peek || cfg.tls.is_some() || !shutdown.is_inert();

    std::thread::scope(|scope| -> std::io::Result<()> {
        let lanes = std::iter::repeat_n(&rx, workers)
            .chain(std::iter::repeat_n(&health_rx, cfg.health_workers));
        for lane_rx in lanes {
            let rx = Arc::clone(lane_rx);
            scope.spawn(move || {
                loop {
                    let mut conn = {
                        let guard = match rx.lock() {
                            Ok(guard) => guard,
                            Err(_) => break,
                        };
                        match guard.recv() {
                            Ok(conn) => {
                                hooks.on_dequeued();
                                conn
                            }
                            Err(_) => break,
                        }
                    };
                    // A connection that already waited out its request
                    // deadline in the queue is closed without any work.
                    if conn.is_stale(cfg.request_deadline) {
                        continue;
                    }
                    if let Err(err) = handle_connection(&mut conn, cfg, handler, hooks) {
                        eprintln!("{} transport error: {err}", cfg.name);
                    }
                }
            });
        }

        if polling {
            // Set the listener non-blocking so we can interleave accept()
            // calls with shutdown-flag polling. The 50ms sleep caps shutdown
            // latency and bounds CPU usage when idle.
            listener.set_nonblocking(true)?;
        }
        let mut accept_error_streak: u32 = 0;
        let mut frontend = ConnFrontend::new(cfg, health);
        let mut ready: Vec<(Lane, Conn)> = Vec::new();
        let dispatch = |ready: &mut Vec<(Lane, Conn)>| {
            for (lane, conn) in ready.drain(..) {
                hooks.on_enqueued();
                let target = if lane == Lane::Health {
                    &health_tx
                } else {
                    &tx
                };
                match target.try_send(conn) {
                    Ok(()) => {}
                    Err(mpsc::TrySendError::Full(conn)) => {
                        hooks.on_dequeued();
                        hooks.on_reject(RejectReason::QueueFull);
                        write_overload_conn(conn, cfg);
                    }
                    Err(mpsc::TrySendError::Disconnected(_)) => {
                        hooks.on_dequeued();
                        eprintln!("{} transport worker queue closed", cfg.name);
                    }
                }
            }
        };
        loop {
            if shutdown.is_triggered() {
                eprintln!(
                    "{}: shutdown signal received, draining in-flight requests",
                    cfg.name
                );
                break;
            }
            if frontend.has_pending() && frontend.poll_due() {
                frontend.poll(&mut ready);
                dispatch(&mut ready);
            }
            match listener.accept() {
                Ok((stream, _)) => {
                    accept_error_streak = 0;
                    match frontend.admit(stream) {
                        Ok(()) => {
                            frontend.take_immediate(&mut ready);
                            dispatch(&mut ready);
                        }
                        Err(Rejected(stream)) => {
                            // Per-IP cap or pending bound exceeded.
                            hooks.on_reject(RejectReason::PerIpCap);
                            if let Err(err) = write_overload(stream, cfg) {
                                eprintln!(
                                    "{} transport backpressure response failed: {err}",
                                    cfg.name
                                );
                            }
                        }
                    }
                }
                Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                    let nap = if frontend.has_pending() {
                        PENDING_POLL_INTERVAL
                    } else {
                        Duration::from_millis(50)
                    };
                    std::thread::sleep(nap);
                }
                Err(err) if err.kind() == std::io::ErrorKind::Interrupted => {}
                Err(err) => {
                    // Transient failures (EMFILE, ECONNABORTED, ...) must not
                    // take the server down; back off briefly and keep
                    // serving. Only the shutdown flag ends this loop.
                    accept_error_streak = accept_error_streak.saturating_add(1);
                    eprintln!("{} transport accept error: {err}", cfg.name);
                    std::thread::sleep(accept_error_backoff(accept_error_streak));
                }
            }
        }
        hooks.on_shutdown();
        drop(tx);
        drop(health_tx);
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accept_backoff_is_bounded() {
        assert_eq!(accept_error_backoff(1), Duration::from_millis(10));
        assert_eq!(accept_error_backoff(2), Duration::from_millis(20));
        assert_eq!(accept_error_backoff(5), Duration::from_millis(100));
        assert_eq!(accept_error_backoff(u32::MAX), Duration::from_millis(100));
    }
}
