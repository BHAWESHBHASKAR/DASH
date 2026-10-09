//! Idle clients and slow embedding providers must not stall the ingestion
//! service. Everything goes through the real socket server.

use std::{
    io::{Read, Write},
    net::{Shutdown, TcpListener, TcpStream},
    sync::{Mutex, MutexGuard, OnceLock},
    time::{Duration, Instant},
};

use ingestion::transport::{IngestionRuntime, serve_http_with_workers};
use store::InMemoryStore;

fn env_lock() -> MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| {
        #[allow(unused_unsafe)]
        unsafe {
            std::env::set_var("DASH_INSECURE_DEV_MODE", "1");
            std::env::set_var("DASH_STRICT_SECRETS", "0");
        }
        Mutex::new(())
    })
    .lock()
    .unwrap_or_else(|p| p.into_inner())
}

/// Sets an environment variable for the scope of a test and restores the
/// previous value on drop. Callers hold `env_lock()`.
struct EnvGuard {
    key: &'static str,
    previous: Option<String>,
}

impl EnvGuard {
    fn set(key: &'static str, value: &str) -> Self {
        let previous = std::env::var(key).ok();
        #[allow(unused_unsafe)]
        unsafe {
            std::env::set_var(key, value)
        };
        Self { key, previous }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        #[allow(unused_unsafe)]
        unsafe {
            match &self.previous {
                Some(v) => std::env::set_var(self.key, v),
                None => std::env::remove_var(self.key),
            }
        }
    }
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind probe listener")
        .local_addr()
        .expect("local addr")
        .port()
}

fn start_server() -> String {
    let addr = format!("127.0.0.1:{}", free_port());
    let bind = addr.clone();
    let shutdown = dash_common::ShutdownSignal::manual();
    std::thread::spawn(move || {
        let runtime = IngestionRuntime::in_memory(InMemoryStore::new());
        let _ = serve_http_with_workers(runtime, &bind, 4, shutdown);
    });
    for _ in 0..100 {
        if TcpStream::connect(&addr).is_ok() {
            return addr;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!("server did not start");
}

fn send_raw(addr: &str, payload: &[u8]) -> String {
    let mut stream = TcpStream::connect(addr).expect("connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(15)))
        .expect("read timeout");
    let _ = stream.write_all(payload);
    let _ = stream.shutdown(Shutdown::Write);
    let mut out = Vec::new();
    let _ = stream.read_to_end(&mut out);
    String::from_utf8_lossy(&out).into_owned()
}

fn status_line(response: &str) -> &str {
    response.lines().next().unwrap_or("")
}

fn ingest_request(id: usize) -> String {
    let body = format!(
        "{{\"claim\":{{\"claim_id\":\"c{id}\",\"tenant_id\":\"t\",\"canonical_text\":\"text {id}\",\"confidence\":0.9}}}}"
    );
    format!(
        "POST /v1/ingest HTTP/1.1\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    )
}

/// Fake upstream: every connection is answered after `delay` with `reply`.
fn fake_upstream(delay: Duration, reply: &'static str) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind upstream");
    let port = listener.local_addr().expect("addr").port();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { return };
            std::thread::spawn(move || {
                let mut buf = [0u8; 8192];
                let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
                let _ = stream.read(&mut buf);
                std::thread::sleep(delay);
                let _ = stream.write_all(reply.as_bytes());
            });
        }
    });
    port
}

const OK_REPLY: &str = "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 31\r\nConnection: close\r\n\r\n{\"embeddings\":[[0.5,0.25,0.1]]}";

#[test]
fn health_stays_fast_while_embedding_calls_are_slow() {
    let _env = env_lock();
    let port = fake_upstream(Duration::from_secs(3), OK_REPLY);
    let _p = EnvGuard::set("DASH_EMBEDDING_PROVIDER", "ollama");
    let _e = EnvGuard::set(
        "DASH_OLLAMA_ENDPOINT",
        &format!("http://127.0.0.1:{port}/api/embed"),
    );
    let addr = start_server();
    let clients: Vec<_> = (0..4)
        .map(|i| {
            let addr = addr.clone();
            std::thread::spawn(move || send_raw(&addr, ingest_request(i).as_bytes()))
        })
        .collect();
    std::thread::sleep(Duration::from_millis(500));
    for path in ["/health", "/live", "/ready"] {
        let started = Instant::now();
        let response = send_raw(&addr, format!("GET {path} HTTP/1.1\r\n\r\n").as_bytes());
        assert!(
            status_line(&response).contains("200"),
            "{path}: {response:?}"
        );
        assert!(
            started.elapsed() < Duration::from_millis(200),
            "{path} took {:?} while embeddings were slow",
            started.elapsed()
        );
    }
    for client in clients {
        let response = client.join().expect("client");
        assert!(status_line(&response).contains("200"), "{response:?}");
    }
}

#[test]
fn provider_outage_on_ingest_is_503_with_retry_after() {
    let _env = env_lock();
    let port = fake_upstream(
        Duration::ZERO,
        "HTTP/1.1 429 Too Many Requests\r\nRetry-After: 5\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
    );
    let _p = EnvGuard::set("DASH_EMBEDDING_PROVIDER", "ollama");
    let _e = EnvGuard::set(
        "DASH_OLLAMA_ENDPOINT",
        &format!("http://127.0.0.1:{port}/api/embed"),
    );
    let _b = EnvGuard::set("DASH_EMBEDDING_BREAKER_THRESHOLD", "0");
    let addr = start_server();
    let response = send_raw(&addr, ingest_request(1).as_bytes());
    assert!(status_line(&response).contains("503"), "{response:?}");
    assert!(response.contains("Retry-After: 5"), "{response:?}");
    assert!(response.contains("embedding_unavailable"), "{response:?}");
}

#[test]
fn idle_sockets_do_not_starve_health_or_normal_requests() {
    let _env = env_lock();
    let _cap = EnvGuard::set("DASH_HTTP_MAX_CONNS_PER_IP", "0");
    let addr = start_server();
    let idle: Vec<TcpStream> = (0..300)
        .map(|_| TcpStream::connect(&addr).expect("connect idle"))
        .collect();

    let started = Instant::now();
    let health = send_raw(&addr, b"GET /health HTTP/1.1\r\n\r\n");
    assert!(status_line(&health).contains("200"), "{health:?}");
    let body = "{\"claim\":{\"claim_id\":\"c1\",\"tenant_id\":\"t\",\"canonical_text\":\"x\",\"confidence\":0.9},\"claim_embedding\":[0.1,0.2]}";
    let ingest = send_raw(
        &addr,
        format!(
            "POST /v1/ingest HTTP/1.1\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        )
        .as_bytes(),
    );
    assert!(status_line(&ingest).contains("200"), "{ingest:?}");
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "requests behind 300 idle sockets took {:?}",
        started.elapsed()
    );

    std::thread::sleep(Duration::from_millis(2600));
    for mut stream in idle.into_iter().take(20) {
        stream
            .set_read_timeout(Some(Duration::from_millis(500)))
            .expect("timeout");
        let mut buf = [0u8; 16];
        assert!(
            matches!(stream.read(&mut buf), Ok(0) | Err(_)),
            "idle socket must be closed without a worker"
        );
    }
    let health = send_raw(&addr, b"GET /health HTTP/1.1\r\n\r\n");
    assert!(status_line(&health).contains("200"), "{health:?}");
}

#[test]
fn per_ip_connection_cap_sheds_excess_and_recovers() {
    let _env = env_lock();
    let _cap = EnvGuard::set("DASH_HTTP_MAX_CONNS_PER_IP", "8");
    let _fb = EnvGuard::set("DASH_HTTP_FIRST_BYTE_TIMEOUT_MS", "800");
    let addr = start_server();
    let idle: Vec<TcpStream> = (0..8)
        .map(|_| TcpStream::connect(&addr).expect("connect idle"))
        .collect();
    // Poll until the cap is observed: the probe is shed only once all eight
    // idle connections have been admitted.
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let shed = send_raw(&addr, b"GET /health HTTP/1.1\r\n\r\n");
        if status_line(&shed).contains("503") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "connection over the per-IP cap must be shed: {shed:?}"
        );
        std::thread::sleep(Duration::from_millis(10));
    }

    drop(idle);
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let health = send_raw(&addr, b"GET /health HTTP/1.1\r\n\r\n");
        if status_line(&health).contains("200") {
            break;
        }
        assert!(Instant::now() < deadline, "cap must release: {health:?}");
        std::thread::sleep(Duration::from_millis(10));
    }
}
