//! Request-parser strictness checks that go through the real socket server.

use std::{
    io::{Read, Write},
    net::{Shutdown, TcpListener, TcpStream},
    time::{Duration, Instant},
};

use ingestion::transport::{IngestionRuntime, serve_http_with_workers};
use store::InMemoryStore;

fn start_server() -> String {
    #[allow(unused_unsafe)]
    unsafe {
        std::env::set_var("DASH_INSECURE_DEV_MODE", "1");
        std::env::set_var("DASH_STRICT_SECRETS", "0");
    }
    let port = TcpListener::bind("127.0.0.1:0")
        .expect("bind probe listener")
        .local_addr()
        .expect("local addr")
        .port();
    let addr = format!("127.0.0.1:{port}");
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

#[test]
fn malformed_or_unsupported_http_versions_are_rejected() {
    let addr = start_server();
    for (request, expected) in [
        ("GET /health HTTP/1.foo\r\n\r\n", "400"),
        ("GET /health HTTP/1.\r\n\r\n", "400"),
        ("GET /health HTTP/1.1x\r\n\r\n", "400"),
        ("GET /health FTP/1.1\r\n\r\n", "400"),
        ("GET /health HTTP/1.1 extra\r\n\r\n", "400"),
        ("GET /health HTTP/2.0\r\n\r\n", "505"),
        ("GET /health HTTP/0.9\r\n\r\n", "505"),
    ] {
        let response = send_raw(&addr, request.as_bytes());
        assert!(
            status_line(&response).contains(expected),
            "{request:?} -> {response:?}"
        );
    }
    for ok in ["HTTP/1.0", "HTTP/1.1"] {
        let response = send_raw(&addr, format!("GET /health {ok}\r\n\r\n").as_bytes());
        assert!(status_line(&response).contains("200"), "{ok}: {response:?}");
    }
}

#[test]
fn whitespace_before_header_colon_and_obs_fold_are_rejected() {
    let addr = start_server();
    for request in [
        "GET /health HTTP/1.1\r\nX-A : b\r\n\r\n",
        "GET /health HTTP/1.1\r\nX-A\t: b\r\n\r\n",
        "GET /health HTTP/1.1\r\nX-A: b\r\n continued\r\n\r\n",
        "GET /health HTTP/1.1\r\nX-A: b\r\n\tcontinued\r\n\r\n",
        "GET /health HTTP/1.1\r\n: nameless\r\n\r\n",
    ] {
        let response = send_raw(&addr, request.as_bytes());
        assert!(
            status_line(&response).contains("400"),
            "{request:?} -> {response:?}"
        );
    }
    let response = send_raw(&addr, b"GET /health HTTP/1.1\r\nX-A: b\r\nX-B:c\r\n\r\n");
    assert!(status_line(&response).contains("200"), "{response:?}");
}

#[test]
fn expect_100_continue_is_answered_with_417_without_stalling() {
    let addr = start_server();
    let mut stream = TcpStream::connect(&addr).expect("connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(3)))
        .expect("timeout");
    stream
        .write_all(
            b"POST /v1/ingest HTTP/1.1\r\nExpect: 100-continue\r\nContent-Length: 40\r\n\r\n",
        )
        .expect("write headers");
    let started = Instant::now();
    let mut out = vec![0u8; 512];
    let n = stream.read(&mut out).expect("a response, not a stall");
    let response = String::from_utf8_lossy(&out[..n]);
    assert!(status_line(&response).contains("417"), "{response:?}");
    assert!(started.elapsed() < Duration::from_secs(1));
}

#[test]
fn oversized_body_gets_a_readable_413_even_when_the_client_keeps_sending() {
    let addr = start_server();
    let mut stream = TcpStream::connect(&addr).expect("connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .expect("timeout");
    stream
        .write_all(b"POST /v1/ingest HTTP/1.1\r\nContent-Length: 999999999\r\n\r\n")
        .expect("write headers");
    // The client does not wait for the verdict and pushes body bytes.
    let chunk = vec![b'a'; 64 * 1024];
    for _ in 0..8 {
        if stream.write_all(&chunk).is_err() {
            break;
        }
    }
    let _ = stream.shutdown(Shutdown::Write);
    let mut out = Vec::new();
    let read = stream.read_to_end(&mut out);
    let response = String::from_utf8_lossy(&out);
    assert!(
        status_line(&response).contains("413"),
        "client must be able to read the 413 (read result: {read:?}, got {response:?})"
    );
}

#[test]
fn duplicate_json_keys_are_rejected_with_400() {
    let addr = start_server();
    let body = r#"{"claim":{"claim_id":"a","claim_id":"b","tenant_id":"t","canonical_text":"x","confidence":0.9},"claim_embedding":[0.1,0.2]}"#;
    let request = format!(
        "POST /v1/ingest HTTP/1.1\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    let response = send_raw(&addr, request.as_bytes());
    assert!(status_line(&response).contains("400"), "{response:?}");
}

#[test]
fn read_stage_failures_are_counted_per_status_class() {
    let addr = start_server();
    for request in [
        "GET /health HTTP/1.foo\r\n\r\n",
        "GET /health HTTP/1.1\r\nX-A : b\r\n\r\n",
        "GET /health HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n",
    ] {
        let _ = send_raw(&addr, request.as_bytes());
    }
    let metrics = send_raw(&addr, b"GET /metrics HTTP/1.1\r\n\r\n");
    let line = |class: &str| {
        let prefix = format!("dash_ingest_transport_read_error_total{{status_class=\"{class}\"}} ");
        metrics
            .lines()
            .find_map(|l| l.strip_prefix(prefix.as_str()))
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or_else(|| panic!("missing {class} counter in {metrics}"))
    };
    assert!(line("4xx") >= 2, "{metrics}");
    assert!(line("5xx") >= 1, "{metrics}");
}
