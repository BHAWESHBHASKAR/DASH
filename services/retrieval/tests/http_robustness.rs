//! Hostile-input and robustness tests that go through the real socket server.

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
        // These tests exercise transport behaviour, not authentication, so
        // they run in explicit dev mode (the only open configuration).
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

#[allow(unused_unsafe)]
fn set_env(key: &str, value: &str) {
    unsafe { std::env::set_var(key, value) }
}

#[allow(unused_unsafe)]
fn unset_env(key: &str) {
    unsafe { std::env::remove_var(key) }
}

struct EnvGuard(&'static str);
impl EnvGuard {
    fn set(key: &'static str, value: &str) -> Self {
        set_env(key, value);
        Self(key)
    }
}
impl Drop for EnvGuard {
    fn drop(&mut self) {
        unset_env(self.0);
    }
}

fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind probe listener");
    listener.local_addr().expect("local addr").port()
}

/// Starts the real server on a background thread and waits until it accepts.
fn start_server(store: Arc<RwLock<InMemoryStore>>) -> String {
    let addr = format!("127.0.0.1:{}", free_port());
    let bind = addr.clone();
    let shutdown = dash_common::ShutdownSignal::install();
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
        .set_read_timeout(Some(Duration::from_secs(10)))
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

fn assert_server_alive(addr: &str) {
    let response = send_raw(addr, b"GET /health HTTP/1.1\r\nHost: t\r\n\r\n");
    assert!(
        status_line(&response).contains("200 OK"),
        "server must answer a normal request, got: {response:?}"
    );
    assert!(response.contains("\"status\":\"ok\""));
}

fn new_store() -> Arc<RwLock<InMemoryStore>> {
    Arc::new(RwLock::new(InMemoryStore::new()))
}

#[test]
fn huge_header_line_is_rejected_with_431_and_server_survives() {
    let _env = env_lock();
    let addr = start_server(new_store());
    let mut request = b"GET /health HTTP/1.1\r\nX-Big: ".to_vec();
    request.extend(std::iter::repeat_n(b'a', 64 * 1024));
    request.extend_from_slice(b"\r\n\r\n");
    let response = send_raw(&addr, &request);
    assert!(
        status_line(&response).contains("431"),
        "got: {}",
        status_line(&response)
    );
    assert_server_alive(&addr);
}

#[test]
fn single_header_line_over_8kib_is_rejected_with_431() {
    let _env = env_lock();
    let addr = start_server(new_store());
    let mut request = b"GET /health HTTP/1.1\r\nX-Big: ".to_vec();
    request.extend(std::iter::repeat_n(b'a', 9 * 1024));
    request.extend_from_slice(b"\r\n\r\n");
    let response = send_raw(&addr, &request);
    assert!(status_line(&response).contains("431"));
    assert_server_alive(&addr);
}

#[test]
fn more_than_100_headers_is_rejected_with_431() {
    let _env = env_lock();
    let addr = start_server(new_store());
    let mut request = String::from("GET /health HTTP/1.1\r\n");
    for i in 0..150 {
        request.push_str(&format!("X-H{i}: v\r\n"));
    }
    request.push_str("\r\n");
    let response = send_raw(&addr, request.as_bytes());
    assert!(status_line(&response).contains("431"));
    assert_server_alive(&addr);
}

#[test]
fn deeply_nested_json_returns_400_and_does_not_abort_process() {
    let _env = env_lock();
    let addr = start_server(new_store());
    for depth in [100_000usize, 1_000_000] {
        let body = "[".repeat(depth);
        let request = format!(
            "POST /v1/retrieve HTTP/1.1\r\nHost: t\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        );
        let response = send_raw(&addr, request.as_bytes());
        assert!(
            status_line(&response).contains("400"),
            "depth {depth}: {}",
            status_line(&response)
        );
        assert_server_alive(&addr);
    }
    let nested_objects = format!("{}1{}", "{\"a\":".repeat(5000), "}".repeat(5000));
    let request = format!(
        "POST /v1/retrieve HTTP/1.1\r\nHost: t\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{nested_objects}",
        nested_objects.len()
    );
    let response = send_raw(&addr, request.as_bytes());
    assert!(status_line(&response).contains("400"));
    assert_server_alive(&addr);
}

#[test]
fn bare_newline_request_is_handled() {
    let _env = env_lock();
    let addr = start_server(new_store());
    let response = send_raw(&addr, b"GET /health HTTP/1.1\nHost: t\n\n");
    assert!(
        status_line(&response).contains("200 OK"),
        "got: {response:?}"
    );
    assert_server_alive(&addr);
}

#[test]
fn conflicting_duplicate_content_length_is_rejected_with_400() {
    let _env = env_lock();
    let addr = start_server(new_store());
    let response = send_raw(
        &addr,
        b"POST /v1/retrieve HTTP/1.1\r\nContent-Length: 2\r\nContent-Length: 5\r\n\r\n{}",
    );
    assert!(status_line(&response).contains("400"), "got: {response:?}");
    assert_server_alive(&addr);
}

#[test]
fn invalid_utf8_body_returns_400() {
    let _env = env_lock();
    let addr = start_server(new_store());
    let body: &[u8] = &[0xff, 0xfe, 0xfd, b'{', b'}'];
    let mut request = format!(
        "POST /v1/retrieve HTTP/1.1\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
        body.len()
    )
    .into_bytes();
    request.extend_from_slice(body);
    let response = send_raw(&addr, &request);
    assert!(status_line(&response).contains("400"), "got: {response:?}");
    assert_server_alive(&addr);
}

#[test]
fn truncated_body_is_rejected_and_server_survives() {
    let _env = env_lock();
    let addr = start_server(new_store());
    let response = send_raw(
        &addr,
        b"POST /v1/retrieve HTTP/1.1\r\nContent-Type: application/json\r\nContent-Length: 100\r\n\r\n{\"ten",
    );
    assert!(status_line(&response).contains("400"), "got: {response:?}");
    assert_server_alive(&addr);
}

#[test]
fn chunked_transfer_encoding_is_rejected_cleanly_with_501() {
    let _env = env_lock();
    let addr = start_server(new_store());
    let response = send_raw(
        &addr,
        b"POST /v1/retrieve HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n2\r\n{}\r\n0\r\n\r\n",
    );
    assert!(
        status_line(&response).contains("501 Not Implemented"),
        "got: {response:?}"
    );
    assert_server_alive(&addr);
}

#[test]
fn oversized_content_length_is_rejected_with_413_before_body_is_sent() {
    let _env = env_lock();
    let addr = start_server(new_store());
    let response = send_raw(
        &addr,
        b"POST /v1/retrieve HTTP/1.1\r\nContent-Length: 999999999\r\n\r\n",
    );
    assert!(
        status_line(&response).contains("413 Payload Too Large"),
        "got: {response:?}"
    );
    assert_server_alive(&addr);
}

#[test]
fn slow_trickle_client_is_dropped_at_whole_request_deadline() {
    let _env = env_lock();
    let _timeout = EnvGuard::set("DASH_HTTP_REQUEST_TIMEOUT_MS", "600");
    let addr = start_server(new_store());

    let started = Instant::now();
    let mut stream = TcpStream::connect(&addr).expect("connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(8)))
        .expect("read timeout");
    // Each byte arrives well inside any per-read timeout, but the request
    // as a whole never completes.
    let trickle = b"GET /health HTTP/1.1\r\nX-Slow: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let mut dropped = false;
    for byte in trickle {
        if stream.write_all(&[*byte]).is_err() {
            dropped = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
        if started.elapsed() > Duration::from_secs(4) {
            break;
        }
    }
    let mut out = Vec::new();
    let _ = stream.read_to_end(&mut out);
    let response = String::from_utf8_lossy(&out);
    assert!(
        dropped || status_line(&response).contains("408"),
        "slow client must be cut off, got: {response:?}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(4),
        "deadline must bound the whole request, took {:?}",
        started.elapsed()
    );
    assert_server_alive(&addr);
}

#[test]
fn slow_clients_cannot_exhaust_worker_pool() {
    let _env = env_lock();
    let _timeout = EnvGuard::set("DASH_HTTP_REQUEST_TIMEOUT_MS", "500");
    let addr = start_server(new_store());
    // More idle trickling connections than workers (4).
    let mut hogs: Vec<TcpStream> = (0..8)
        .map(|_| {
            let mut s = TcpStream::connect(&addr).expect("connect");
            s.write_all(b"GET /health HTTP/1.1\r\nX-A: b")
                .expect("write");
            s
        })
        .collect();
    std::thread::sleep(Duration::from_millis(1800));
    assert_server_alive(&addr);
    hogs.clear();
}

#[test]
fn utf8_query_decodes_the_same_over_get_and_post() {
    let _env = env_lock();
    let addr = start_server(new_store());
    let body = "{\"tenant_id\":\"t\",\"query\":\"caf\u{e9} \\ud83d\\ude00\",\"query_embedding\":[0.1,0.2]}";
    let post = format!(
        "POST /v1/retrieve HTTP/1.1\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    let post_response = send_raw(&addr, post.as_bytes());
    assert!(
        status_line(&post_response).contains("200 OK"),
        "got: {post_response:?}"
    );
    let get_response = send_raw(
        &addr,
        b"GET /v1/retrieve?tenant_id=t&query=caf%C3%A9+%F0%9F%98%80&query_embedding=0.1,0.2 HTTP/1.1\r\n\r\n",
    );
    assert!(status_line(&get_response).contains("200 OK"));
}

#[test]
fn embedding_provider_failure_maps_to_502_without_leaking_detail() {
    let _env = env_lock();
    // Nothing listens on this port, so the provider fails with an I/O error.
    let dead_port = free_port();
    let _p = EnvGuard::set("DASH_EMBEDDING_PROVIDER", "ollama");
    let _e = EnvGuard::set(
        "DASH_OLLAMA_ENDPOINT",
        &format!("http://127.0.0.1:{dead_port}/api/embeddings"),
    );
    let addr = start_server(new_store());
    let body = "{\"tenant_id\":\"t\",\"query\":\"hello\"}";
    let post = format!(
        "POST /v1/retrieve HTTP/1.1\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    let response = send_raw(&addr, post.as_bytes());
    assert!(
        status_line(&response).contains("502 Bad Gateway"),
        "got: {response:?}"
    );
    assert!(
        !response.contains("127.0.0.1"),
        "provider detail must not leak: {response:?}"
    );

    let emb_body = "{\"input\":\"hello\",\"model\":\"m\"}";
    let emb = format!(
        "POST /v1/embeddings HTTP/1.1\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{emb_body}",
        emb_body.len()
    );
    let response = send_raw(&addr, emb.as_bytes());
    assert!(
        status_line(&response).contains("502 Bad Gateway"),
        "got: {response:?}"
    );
    assert!(!response.contains("127.0.0.1"));
}

#[test]
fn store_write_lock_is_not_blocked_by_slow_embedding_provider() {
    let _env = env_lock();
    // A provider endpoint that accepts and then stalls for a while.
    let stall = TcpListener::bind("127.0.0.1:0").expect("bind stall");
    let stall_port = stall.local_addr().expect("addr").port();
    std::thread::spawn(move || {
        if let Ok((mut stream, _)) = stall.accept() {
            let mut buf = [0u8; 4096];
            let _ = stream.read(&mut buf);
            std::thread::sleep(Duration::from_secs(3));
            let _ = stream
                .write_all(b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n");
        }
    });
    let _p = EnvGuard::set("DASH_EMBEDDING_PROVIDER", "ollama");
    let _e = EnvGuard::set(
        "DASH_OLLAMA_ENDPOINT",
        &format!("http://127.0.0.1:{stall_port}/api/embeddings"),
    );
    let store = new_store();
    let addr = start_server(Arc::clone(&store));

    let body = "{\"tenant_id\":\"t\",\"query\":\"hello\"}";
    let post = format!(
        "POST /v1/retrieve HTTP/1.1\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    let client_addr = addr.clone();
    let client = std::thread::spawn(move || send_raw(&client_addr, post.as_bytes()));
    // Let the request reach the (stalled) embedding call.
    std::thread::sleep(Duration::from_millis(500));

    let (tx, rx) = std::sync::mpsc::channel();
    let writer_store = Arc::clone(&store);
    std::thread::spawn(move || {
        let started = Instant::now();
        drop(writer_store.write().unwrap_or_else(|p| p.into_inner()));
        let _ = tx.send(started.elapsed());
    });
    let waited = rx
        .recv_timeout(Duration::from_millis(1500))
        .expect("writer must not wait for the remote embedding call");
    assert!(waited < Duration::from_millis(1500));
    let response = client.join().expect("client thread");
    assert!(status_line(&response).contains("502"), "got: {response:?}");
}

/// Review finding 8: a repeated credential header used to be last-wins, which
/// lets a proxy and the service disagree about which credential was sent.
#[test]
fn duplicate_credential_headers_are_rejected_with_400() {
    let _env = env_lock();
    let addr = start_server(new_store());
    for (label, headers) in [
        (
            "authorization",
            "Authorization: Bearer one\r\nauthorization: Bearer two\r\n",
        ),
        ("x-api-key", "X-API-Key: one\r\nx-api-key: two\r\n"),
        (
            "x-replication-token",
            "X-Replication-Token: one\r\nx-replication-token: two\r\n",
        ),
        (
            "identical authorization",
            "Authorization: Bearer same\r\nAuthorization: Bearer same\r\n",
        ),
    ] {
        let request = format!("GET /health HTTP/1.1\r\nHost: t\r\n{headers}\r\n");
        let response = send_raw(&addr, request.as_bytes());
        assert!(
            status_line(&response).contains("400"),
            "{label}: {response:?}"
        );
    }
    // A single credential header is still fine.
    let response = send_raw(
        &addr,
        b"GET /health HTTP/1.1\r\nHost: t\r\nAuthorization: Bearer one\r\n\r\n",
    );
    assert!(status_line(&response).contains("200 OK"), "{response:?}");
    assert_server_alive(&addr);
}
