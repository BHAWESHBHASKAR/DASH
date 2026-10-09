//! Shared harness: a real socket server around a tiny echo handler, so the
//! transport behaviour is tested independently of any service.
#![allow(dead_code)]

use std::{
    io::{Read, Write},
    net::{Shutdown, SocketAddr, TcpListener, TcpStream},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use dash_http::{
    Acceptor, Handler, HealthClassifier, RejectReason, Request, Response, ServerConfig,
    ServerHooks, default_health_classifier, json_escape, serve,
};

/// Counts every hook invocation.
#[derive(Default)]
pub struct CountingHooks {
    pub enqueued: AtomicUsize,
    pub dequeued: AtomicUsize,
    pub queue_full_rejects: AtomicUsize,
    pub per_ip_rejects: AtomicUsize,
    pub read_errors: Mutex<Vec<u16>>,
    pub shutdowns: AtomicUsize,
}

impl ServerHooks for CountingHooks {
    fn on_enqueued(&self) {
        self.enqueued.fetch_add(1, Ordering::SeqCst);
    }

    fn on_dequeued(&self) {
        self.dequeued.fetch_add(1, Ordering::SeqCst);
    }

    fn on_reject(&self, reason: RejectReason) {
        match reason {
            RejectReason::QueueFull => &self.queue_full_rejects,
            RejectReason::PerIpCap => &self.per_ip_rejects,
        }
        .fetch_add(1, Ordering::SeqCst);
    }

    fn on_read_error(&self, status: u16) {
        self.read_errors.lock().unwrap().push(status);
    }

    fn on_shutdown(&self) {
        self.shutdowns.fetch_add(1, Ordering::SeqCst);
    }
}

/// The echo service.
///
/// * `/health` answers `{"status":"ok"}`;
/// * `/panic` panics;
/// * `/slow?ms=N` sleeps `N` milliseconds;
/// * anything else echoes method, path, body length, peer and query (400 on
///   invalid percent-encoding).
pub fn echo(request: Request) -> Response {
    match request.path() {
        "/health" => Response::json(200, "{\"status\":\"ok\"}".to_string()),
        "/panic" => panic!("handler panic requested by the test"),
        "/slow" => {
            let ms = request
                .query()
                .get("ms")
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(1000);
            std::thread::sleep(Duration::from_millis(ms));
            Response::json(200, "{\"slow\":true}".to_string())
        }
        _ => match request.try_query() {
            Err(err) => Response::error(400, &err),
            Ok(query) => {
                let mut pairs: Vec<_> = query.into_iter().collect();
                pairs.sort();
                let query = pairs
                    .iter()
                    .map(|(k, v)| format!("\"{}\":\"{}\"", json_escape(k), json_escape(v)))
                    .collect::<Vec<_>>()
                    .join(",");
                Response::json(
                    200,
                    format!(
                        "{{\"method\":\"{}\",\"path\":\"{}\",\"body_len\":{},\"peer\":\"{}\",\"query\":{{{query}}}}}",
                        json_escape(&request.method),
                        json_escape(request.path()),
                        request.body.len(),
                        request.peer.map(|p| p.ip().to_string()).unwrap_or_default(),
                    ),
                )
            }
        },
    }
}

pub struct Harness {
    pub addr: String,
    pub hooks: Arc<CountingHooks>,
    pub stop: Arc<AtomicBool>,
    pub join: Option<std::thread::JoinHandle<std::io::Result<()>>>,
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
    }
}

/// Generous defaults for tests; individual tests override what they probe.
pub fn config() -> ServerConfig {
    let mut config = ServerConfig::new("test", 4, 64);
    config.first_byte_timeout = Duration::from_millis(400);
    config.max_conns_per_ip = 0;
    config
}

pub fn start(config: ServerConfig) -> Harness {
    start_with(config, Arc::new(echo), default_health_classifier, |l| l)
}

pub fn start_with<A: Acceptor + 'static>(
    config: ServerConfig,
    handler: Handler,
    classifier: HealthClassifier,
    wrap: impl FnOnce(TcpListener) -> A,
) -> Harness {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("addr").to_string();
    let acceptor = wrap(listener);
    let hooks = Arc::new(CountingHooks::default());
    let stop = Arc::new(AtomicBool::new(false));
    let join = {
        let hooks = Arc::clone(&hooks);
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            serve(
                acceptor,
                config,
                handler,
                classifier,
                &|| stop.load(Ordering::SeqCst),
                hooks,
            )
        })
    };
    // The listener is already bound: connections queue in the backlog even
    // before the accept loop starts.
    Harness {
        addr,
        hooks,
        stop,
        join: Some(join),
    }
}

/// Wraps a listener and fails the first `failures` accepts, like `EMFILE`.
pub struct FlakyListener {
    pub inner: TcpListener,
    pub failures: AtomicUsize,
}

impl Acceptor for FlakyListener {
    fn accept(&self) -> std::io::Result<(TcpStream, SocketAddr)> {
        let left = self.failures.load(Ordering::SeqCst);
        if left > 0 {
            self.failures.store(left - 1, Ordering::SeqCst);
            return Err(std::io::Error::from_raw_os_error(24));
        }
        Acceptor::accept(&self.inner)
    }

    fn set_nonblocking(&self, nonblocking: bool) -> std::io::Result<()> {
        Acceptor::set_nonblocking(&self.inner, nonblocking)
    }
}

pub fn send_raw(addr: &str, payload: &[u8]) -> String {
    let mut stream = TcpStream::connect(addr).expect("connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .expect("read timeout");
    let _ = stream.write_all(payload);
    let _ = stream.shutdown(Shutdown::Write);
    let mut out = Vec::new();
    let _ = stream.read_to_end(&mut out);
    String::from_utf8_lossy(&out).into_owned()
}

pub fn status_line(response: &str) -> &str {
    response.lines().next().unwrap_or("")
}

pub fn assert_alive(addr: &str) {
    let response = send_raw(addr, b"GET /health HTTP/1.1\r\nHost: t\r\n\r\n");
    assert!(
        status_line(&response).contains("200 OK"),
        "server must answer a normal request, got: {response:?}"
    );
    assert!(response.contains("\"status\":\"ok\""));
}

pub fn connect(addr: &str) -> TcpStream {
    let stream = TcpStream::connect(addr).expect("connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .expect("read timeout");
    stream
}

/// Poll `cond` (10 ms steps) until it holds; panics after 5 s.
pub fn wait_for(what: &str, cond: impl Fn() -> bool) {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !cond() {
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for {what}"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

impl Harness {
    /// Wait until `n` connections were queued and `taken` were picked up by
    /// workers.
    pub fn wait_queue(&self, n: usize, taken: usize) {
        wait_for("queue state", || {
            self.hooks.enqueued.load(Ordering::SeqCst) >= n
                && self.hooks.dequeued.load(Ordering::SeqCst) >= taken
        });
    }
}
