//! Slow or failing embedding providers and idle clients must not stall the
//! service. Everything goes through the real socket server.

use std::{
    io::{Read, Write},
    net::{Shutdown, TcpListener, TcpStream},
    sync::{Arc, Mutex, MutexGuard, OnceLock, RwLock},
    time::{Duration, Instant},
};

use retrieval::transport::serve_http_with_workers;
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
    let store = Arc::new(RwLock::new(InMemoryStore::new()));
    let shutdown = dash_common::ShutdownSignal::manual();
    std::thread::spawn(move || {
        let _ = serve_http_with_workers(store, &bind, 4, shutdown);
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

fn post(path: &str, body: &str) -> String {
    format!(
        "POST {path} HTTP/1.1\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
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

fn ollama_env(port: u16) -> (EnvGuard, EnvGuard) {
    (
        EnvGuard::set("DASH_EMBEDDING_PROVIDER", "ollama"),
        EnvGuard::set(
            "DASH_OLLAMA_ENDPOINT",
            &format!("http://127.0.0.1:{port}/api/embed"),
        ),
    )
}

#[test]
fn health_stays_fast_while_embedding_calls_are_slow() {
    let _env = env_lock();
    let port = fake_upstream(Duration::from_secs(3), OK_REPLY);
    let _p = ollama_env(port);
    let addr = start_server();

    // Four concurrent slow embedding calls pin every general worker.
    let clients: Vec<_> = (0..4)
        .map(|i| {
            let addr = addr.clone();
            std::thread::spawn(move || {
                send_raw(
                    &addr,
                    post(
                        "/v1/embeddings",
                        &format!("{{\"input\":\"t{i}\",\"model\":\"m\"}}"),
                    )
                    .as_bytes(),
                )
            })
        })
        .collect();
    std::thread::sleep(Duration::from_millis(500));

    for path in ["/health", "/live", "/ready", "/v1/health"] {
        let started = Instant::now();
        let response = send_raw(&addr, format!("GET {path} HTTP/1.1\r\n\r\n").as_bytes());
        let elapsed = started.elapsed();
        assert!(
            status_line(&response).contains("200"),
            "{path}: {response:?}"
        );
        assert!(
            elapsed < Duration::from_millis(200),
            "{path} took {elapsed:?} while embeddings were slow"
        );
    }
    for client in clients {
        let response = client.join().expect("client");
        assert!(status_line(&response).contains("200"), "{response:?}");
    }
}

#[test]
fn embedding_concurrency_cap_fails_fast_with_503_and_retry_after() {
    let _env = env_lock();
    let port = fake_upstream(Duration::from_millis(1500), OK_REPLY);
    let _p = ollama_env(port);
    let _c = EnvGuard::set("DASH_EMBEDDING_MAX_CONCURRENCY", "1");
    let _w = EnvGuard::set("DASH_EMBEDDING_QUEUE_WAIT_MS", "50");
    let addr = start_server();

    let first_addr = addr.clone();
    let first = std::thread::spawn(move || {
        send_raw(
            &first_addr,
            post("/v1/embeddings", "{\"input\":\"a\",\"model\":\"m\"}").as_bytes(),
        )
    });
    std::thread::sleep(Duration::from_millis(300));

    let started = Instant::now();
    let response = send_raw(
        &addr,
        post("/v1/embeddings", "{\"input\":\"b\",\"model\":\"m\"}").as_bytes(),
    );
    assert!(
        status_line(&response).contains("503"),
        "second call must be shed: {response:?}"
    );
    assert!(response.contains("Retry-After: 1"), "{response:?}");
    assert!(response.contains("embedding_unavailable"), "{response:?}");
    assert!(started.elapsed() < Duration::from_millis(1000));

    let response = send_raw(
        &addr,
        post("/v1/retrieve", "{\"tenant_id\":\"t\",\"query\":\"hello\"}").as_bytes(),
    );
    assert!(status_line(&response).contains("503"), "{response:?}");
    assert!(response.contains("Retry-After: 1"), "{response:?}");

    let first = first.join().expect("first client");
    assert!(status_line(&first).contains("200"), "{first:?}");
}

#[test]
fn provider_failures_map_to_503_with_retry_after_and_bad_payloads_to_502() {
    let _env = env_lock();

    // Upstream 5xx with its own Retry-After.
    let port = fake_upstream(
        Duration::ZERO,
        "HTTP/1.1 503 Service Unavailable\r\nRetry-After: 7\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
    );
    {
        let _p = ollama_env(port);
        let _t = EnvGuard::set("DASH_EMBEDDING_BREAKER_THRESHOLD", "0");
        let addr = start_server();
        for (path, body) in [
            ("/v1/embeddings", "{\"input\":\"a\",\"model\":\"m\"}"),
            ("/v1/retrieve", "{\"tenant_id\":\"t\",\"query\":\"a\"}"),
        ] {
            let response = send_raw(&addr, post(path, body).as_bytes());
            assert!(
                status_line(&response).contains("503"),
                "{path}: {response:?}"
            );
            assert!(response.contains("Retry-After: 7"), "{path}: {response:?}");
            assert!(response.contains("embedding_unavailable"), "{path}");
        }
    }

    // Connection refused.
    {
        let _p = ollama_env(free_port());
        let _t = EnvGuard::set("DASH_EMBEDDING_BREAKER_THRESHOLD", "0");
        let addr = start_server();
        for (path, body) in [
            ("/v1/embeddings", "{\"input\":\"a\",\"model\":\"m\"}"),
            ("/v1/retrieve", "{\"tenant_id\":\"t\",\"query\":\"a\"}"),
        ] {
            let response = send_raw(&addr, post(path, body).as_bytes());
            assert!(
                status_line(&response).contains("503"),
                "{path}: {response:?}"
            );
            assert!(response.contains("Retry-After: 1"), "{path}: {response:?}");
        }
    }

    // Upstream rejects the input (4xx): a bad gateway, not an outage.
    let port = fake_upstream(
        Duration::ZERO,
        "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
    );
    {
        let _p = ollama_env(port);
        let addr = start_server();
        let response = send_raw(
            &addr,
            post("/v1/embeddings", "{\"input\":\"a\",\"model\":\"m\"}").as_bytes(),
        );
        assert!(status_line(&response).contains("502"), "{response:?}");
        assert!(!response.contains("Retry-After"), "{response:?}");
    }
}

#[test]
fn non_finite_provider_values_become_502_not_null() {
    let _env = env_lock();
    let reply: &'static str = "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 28\r\nConnection: close\r\n\r\n{\"embeddings\":[[1e39,0.5]]}\n";
    let port = fake_upstream(Duration::ZERO, reply);
    let _p = ollama_env(port);
    let addr = start_server();
    let response = send_raw(
        &addr,
        post("/v1/embeddings", "{\"input\":\"a\",\"model\":\"m\"}").as_bytes(),
    );
    assert!(status_line(&response).contains("502"), "{response:?}");
    assert!(!response.contains("[null"), "{response:?}");
}

#[test]
fn dimensions_param_is_accepted_before_the_provider_has_learned_its_size() {
    let _env = env_lock();
    let port = fake_upstream(Duration::ZERO, OK_REPLY);
    let _p = ollama_env(port);
    let addr = start_server();
    // Provider dimensions are unknown (0) until the first response.
    let response = send_raw(
        &addr,
        post(
            "/v1/embeddings",
            "{\"input\":\"a\",\"model\":\"m\",\"dimensions\":3}",
        )
        .as_bytes(),
    );
    assert!(status_line(&response).contains("200"), "{response:?}");
    // A wrong value is still rejected, validated against the response.
    let response = send_raw(
        &addr,
        post(
            "/v1/embeddings",
            "{\"input\":\"a\",\"model\":\"m\",\"dimensions\":5}",
        )
        .as_bytes(),
    );
    assert!(status_line(&response).contains("400"), "{response:?}");
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
    let retrieve = send_raw(
        &addr,
        post(
            "/v1/retrieve",
            "{\"tenant_id\":\"t\",\"query\":\"q\",\"query_embedding\":[0.1,0.2]}",
        )
        .as_bytes(),
    );
    assert!(status_line(&retrieve).contains("200"), "{retrieve:?}");
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "requests behind 300 idle sockets took {:?}",
        started.elapsed()
    );

    // Silent sockets are closed after the first-byte timeout (2 s), long
    // before workers x request deadline would have drained them.
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
    std::thread::sleep(Duration::from_millis(100));
    let shed = send_raw(&addr, b"GET /health HTTP/1.1\r\n\r\n");
    assert!(
        status_line(&shed).contains("503"),
        "connection over the per-IP cap must be shed: {shed:?}"
    );

    drop(idle);
    std::thread::sleep(Duration::from_millis(200));
    let health = send_raw(&addr, b"GET /health HTTP/1.1\r\n\r\n");
    assert!(status_line(&health).contains("200"), "{health:?}");
}
