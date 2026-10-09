//! Hostile-input and robustness tests that go through the real socket server.

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
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|p| p.into_inner())
}

struct EnvGuard(&'static str);
impl EnvGuard {
    #[allow(unused_unsafe)]
    fn set(key: &'static str, value: &str) -> Self {
        unsafe { std::env::set_var(key, value) };
        Self(key)
    }
}
impl Drop for EnvGuard {
    #[allow(unused_unsafe)]
    fn drop(&mut self) {
        unsafe { std::env::remove_var(self.0) };
    }
}

fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind probe listener");
    listener.local_addr().expect("local addr").port()
}

fn start_server() -> String {
    let addr = format!("127.0.0.1:{}", free_port());
    let bind = addr.clone();
    let shutdown = dash_common::ShutdownSignal::install();
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
}

#[test]
fn huge_header_and_too_many_headers_are_rejected_with_431() {
    let _env = env_lock();
    let addr = start_server();
    let mut request = b"GET /health HTTP/1.1\r\nX-Big: ".to_vec();
    request.extend(std::iter::repeat_n(b'a', 64 * 1024));
    request.extend_from_slice(b"\r\n\r\n");
    let response = send_raw(&addr, &request);
    assert!(status_line(&response).contains("431"), "{response:?}");
    assert_server_alive(&addr);

    let mut many = String::from("GET /health HTTP/1.1\r\n");
    for i in 0..150 {
        many.push_str(&format!("X-H{i}: v\r\n"));
    }
    many.push_str("\r\n");
    let response = send_raw(&addr, many.as_bytes());
    assert!(status_line(&response).contains("431"), "{response:?}");
    assert_server_alive(&addr);
}

#[test]
fn deeply_nested_json_is_rejected_and_server_survives() {
    let _env = env_lock();
    let addr = start_server();
    let body = "[".repeat(200_000);
    let request = format!(
        "POST /v1/ingest HTTP/1.1\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    let response = send_raw(&addr, request.as_bytes());
    assert!(status_line(&response).contains("400"), "{response:?}");
    assert_server_alive(&addr);
}

#[test]
fn bare_newline_duplicate_length_truncated_and_utf8_inputs() {
    let _env = env_lock();
    let addr = start_server();
    let response = send_raw(&addr, b"GET /health HTTP/1.1\nHost: t\n\n");
    assert!(status_line(&response).contains("200 OK"), "{response:?}");

    let response = send_raw(
        &addr,
        b"POST /v1/ingest HTTP/1.1\r\nContent-Length: 2\r\nContent-Length: 9\r\n\r\n{}",
    );
    assert!(status_line(&response).contains("400"), "{response:?}");

    let response = send_raw(
        &addr,
        b"POST /v1/ingest HTTP/1.1\r\nContent-Type: application/json\r\nContent-Length: 100\r\n\r\n{\"x",
    );
    assert!(status_line(&response).contains("400"), "{response:?}");

    let mut invalid =
        b"POST /v1/ingest HTTP/1.1\r\nContent-Type: application/json\r\nContent-Length: 3\r\n\r\n"
            .to_vec();
    invalid.extend_from_slice(&[0xff, 0xfe, 0xfd]);
    let response = send_raw(&addr, &invalid);
    assert!(status_line(&response).contains("400"), "{response:?}");
    assert_server_alive(&addr);
}

#[test]
fn chunked_is_501_and_oversized_length_is_413() {
    let _env = env_lock();
    let addr = start_server();
    let response = send_raw(
        &addr,
        b"POST /v1/ingest HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n2\r\n{}\r\n0\r\n\r\n",
    );
    assert!(
        status_line(&response).contains("501 Not Implemented"),
        "{response:?}"
    );
    let response = send_raw(
        &addr,
        b"POST /v1/ingest HTTP/1.1\r\nContent-Length: 999999999\r\n\r\n",
    );
    assert!(
        status_line(&response).contains("413 Payload Too Large"),
        "{response:?}"
    );
    assert_server_alive(&addr);
}

#[test]
fn slow_trickle_client_is_dropped_at_whole_request_deadline() {
    let _env = env_lock();
    let _timeout = EnvGuard::set("DASH_HTTP_REQUEST_TIMEOUT_MS", "600");
    let addr = start_server();
    let started = Instant::now();
    let mut stream = TcpStream::connect(&addr).expect("connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(8)))
        .expect("read timeout");
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
        "{response:?}"
    );
    assert!(started.elapsed() < Duration::from_secs(4));
    assert_server_alive(&addr);
}

#[test]
fn replication_ack_query_values_are_percent_decoded() {
    let _env = env_lock();
    let addr = start_server();
    let response = send_raw(
        &addr,
        b"POST /internal/replication/ack?commit_id=commit%3Aabc&replica_id=r1 HTTP/1.1\r\n\r\n",
    );
    // The commit does not exist, so a 404 is expected, but the identifier the
    // handler saw must be the decoded one (with ':'), not the raw escape.
    assert!(status_line(&response).contains("404"), "{response:?}");
    assert!(
        response.contains("commit:abc") && !response.contains("%3A"),
        "commit id must be percent-decoded: {response:?}"
    );
}
