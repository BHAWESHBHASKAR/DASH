//! Service-independent socket tests for the shared server, run against a tiny
//! echo handler.

mod common;

use std::{
    io::{Read, Write},
    net::{Shutdown, TcpStream},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use common::*;
use dash_http::{ExpectPolicy, Response};

// ---------------------------------------------------------------------------
// Header and body caps
// ---------------------------------------------------------------------------

#[test]
fn huge_header_line_is_rejected_with_431_and_server_survives() {
    let server = start(config());
    let mut request = b"GET /x HTTP/1.1\r\nX-Big: ".to_vec();
    request.extend(std::iter::repeat_n(b'a', 64 * 1024));
    request.extend_from_slice(b"\r\n\r\n");
    let response = send_raw(&server.addr, &request);
    assert!(status_line(&response).contains("431"), "{response:?}");
    assert_alive(&server.addr);
}

#[test]
fn header_line_cap_is_exact() {
    let server = start(config());
    // "X-Big: " is 7 bytes; the line limit is 8 KiB.
    let line = |value_len: usize| {
        format!(
            "GET /x HTTP/1.1\r\nX-Big: {}\r\n\r\n",
            "a".repeat(value_len)
        )
    };
    let ok = send_raw(&server.addr, line(8 * 1024 - 7).as_bytes());
    assert!(status_line(&ok).contains("200"), "{ok:?}");
    let over = send_raw(&server.addr, line(8 * 1024 - 6).as_bytes());
    assert!(status_line(&over).contains("431"), "{over:?}");
}

#[test]
fn header_count_cap_is_exact() {
    let server = start(config());
    let request = |count: usize| {
        let mut text = String::from("GET /x HTTP/1.1\r\n");
        for i in 0..count {
            text.push_str(&format!("X-H{i}: v\r\n"));
        }
        text.push_str("\r\n");
        text
    };
    let ok = send_raw(&server.addr, request(100).as_bytes());
    assert!(status_line(&ok).contains("200"), "{ok:?}");
    let over = send_raw(&server.addr, request(101).as_bytes());
    assert!(status_line(&over).contains("431"), "{over:?}");
    assert_alive(&server.addr);
}

#[test]
fn header_block_cap_applies_across_many_lines() {
    let server = start(config());
    // 90 headers of ~400 bytes: each line and the count are legal, the
    // 32 KiB block is not.
    let mut text = String::from("GET /x HTTP/1.1\r\n");
    for i in 0..90 {
        text.push_str(&format!("X-H{i}: {}\r\n", "a".repeat(400)));
    }
    text.push_str("\r\n");
    let response = send_raw(&server.addr, text.as_bytes());
    assert!(status_line(&response).contains("431"), "{response:?}");
    assert_alive(&server.addr);
}

#[test]
fn body_cap_is_exact_at_plus_and_minus_one() {
    let mut cfg = config();
    cfg.max_body_bytes = 1024;
    let server = start(cfg);
    let post = |len: usize, sent: usize| {
        let mut request =
            format!("POST /echo HTTP/1.1\r\nContent-Length: {len}\r\n\r\n").into_bytes();
        request.extend(std::iter::repeat_n(b'a', sent));
        request
    };
    for len in [1023usize, 1024] {
        let response = send_raw(&server.addr, &post(len, len));
        assert!(
            status_line(&response).contains("200"),
            "{len}: {response:?}"
        );
        assert!(
            response.contains(&format!("\"body_len\":{len}")),
            "{response:?}"
        );
    }
    let response = send_raw(&server.addr, &post(1025, 1025));
    assert!(
        status_line(&response).contains("413 Payload Too Large"),
        "{response:?}"
    );
    assert!(response.contains("(1024 bytes)"), "{response:?}");
    assert_alive(&server.addr);
}

#[test]
fn content_length_that_overflows_usize_is_413() {
    let server = start(config());
    let response = send_raw(
        &server.addr,
        b"POST /echo HTTP/1.1\r\nContent-Length: 99999999999999999999999999\r\n\r\n",
    );
    assert!(status_line(&response).contains("413"), "{response:?}");
}

#[test]
fn oversized_content_length_is_rejected_before_the_body_is_sent() {
    let server = start(config());
    let response = send_raw(
        &server.addr,
        b"POST /echo HTTP/1.1\r\nContent-Length: 999999999\r\n\r\n",
    );
    assert!(
        status_line(&response).contains("413 Payload Too Large"),
        "{response:?}"
    );
    assert_eq!(server.hooks.read_errors.lock().unwrap().as_slice(), &[413]);
    assert_alive(&server.addr);
}

#[test]
fn oversized_body_gets_a_readable_413_even_when_the_client_keeps_sending() {
    let server = start(config());
    let mut stream = connect(&server.addr);
    stream
        .write_all(b"POST /echo HTTP/1.1\r\nContent-Length: 999999999\r\n\r\n")
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
fn truncated_body_is_rejected_and_server_survives() {
    let server = start(config());
    let response = send_raw(
        &server.addr,
        b"POST /echo HTTP/1.1\r\nContent-Length: 100\r\n\r\n{\"ten",
    );
    assert!(status_line(&response).contains("400"), "{response:?}");
    assert_alive(&server.addr);
}

#[test]
fn headers_that_are_not_utf8_are_rejected() {
    let server = start(config());
    let response = send_raw(&server.addr, b"GET /x HTTP/1.1\r\nX-A: \xff\xfe\r\n\r\n");
    assert!(status_line(&response).contains("400"), "{response:?}");
    assert_alive(&server.addr);
}

// ---------------------------------------------------------------------------
// Request smuggling and framing
// ---------------------------------------------------------------------------

#[test]
fn conflicting_duplicate_content_length_is_rejected_with_400() {
    let server = start(config());
    for headers in [
        "Content-Length: 2\r\nContent-Length: 5\r\n",
        "Content-Length: 2\r\ncontent-length: 5\r\n",
        "Content-Length: 5\r\nContent-Length: 05\r\n",
    ] {
        let request = format!("POST /echo HTTP/1.1\r\n{headers}\r\n{{}}");
        let response = send_raw(&server.addr, request.as_bytes());
        assert!(
            status_line(&response).contains("400"),
            "{headers:?}: {response:?}"
        );
    }
    assert_alive(&server.addr);
}

#[test]
fn identical_duplicate_content_length_is_accepted() {
    let server = start(config());
    let response = send_raw(
        &server.addr,
        b"POST /echo HTTP/1.1\r\nContent-Length: 2\r\nContent-Length: 2\r\n\r\n{}",
    );
    assert!(status_line(&response).contains("200"), "{response:?}");
    assert!(response.contains("\"body_len\":2"));
}

#[test]
fn malformed_content_length_values_are_rejected() {
    let server = start(config());
    for value in ["-1", "+2", "0x2", "2, 2", "2 2", "", "1e1", "２"] {
        let request = format!("POST /echo HTTP/1.1\r\nContent-Length: {value}\r\n\r\n{{}}");
        let response = send_raw(&server.addr, request.as_bytes());
        assert!(
            status_line(&response).contains("400"),
            "Content-Length {value:?}: {response:?}"
        );
    }
}

#[test]
fn chunked_and_any_transfer_encoding_is_501_even_with_content_length() {
    let server = start(config());
    for headers in [
        "Transfer-Encoding: chunked\r\n",
        "transfer-encoding: CHUNKED\r\n",
        "Transfer-Encoding: identity\r\n",
        "Transfer-Encoding:\tchunked\r\n",
        "Content-Length: 4\r\nTransfer-Encoding: chunked\r\n",
        "Transfer-Encoding: chunked\r\nContent-Length: 4\r\n",
    ] {
        let request = format!("POST /echo HTTP/1.1\r\n{headers}\r\n4\r\nabcd\r\n0\r\n\r\n");
        let response = send_raw(&server.addr, request.as_bytes());
        assert!(
            status_line(&response).contains("501 Not Implemented"),
            "{headers:?}: {response:?}"
        );
    }
    assert_alive(&server.addr);
}

#[test]
fn obfuscated_framing_headers_are_rejected() {
    let server = start(config());
    for request in [
        // Whitespace before the colon hides a header from some parsers.
        "POST /echo HTTP/1.1\r\nTransfer-Encoding : chunked\r\n\r\n",
        "POST /echo HTTP/1.1\r\nContent-Length : 2\r\n\r\n{}",
        // Obsolete line folding.
        "POST /echo HTTP/1.1\r\nTransfer-Encoding:\r\n chunked\r\n\r\n",
        "POST /echo HTTP/1.1\r\nContent-Length: 2\r\n\t\r\n\r\n{}",
        // Nameless and colon-less lines.
        "POST /echo HTTP/1.1\r\n: x\r\n\r\n",
        "POST /echo HTTP/1.1\r\nContent-Length 2\r\n\r\n{}",
    ] {
        let response = send_raw(&server.addr, request.as_bytes());
        assert!(
            status_line(&response).contains("400"),
            "{request:?} -> {response:?}"
        );
    }
}

#[test]
fn pipelined_second_request_is_never_served() {
    let server = start(config());
    let response = send_raw(
        &server.addr,
        b"POST /echo HTTP/1.1\r\nContent-Length: 2\r\n\r\n{}GET /health HTTP/1.1\r\n\r\n",
    );
    assert_eq!(response.matches("HTTP/1.1 ").count(), 1, "{response:?}");
    assert!(response.contains("\"body_len\":2"), "{response:?}");
}

#[test]
fn bare_newline_request_is_handled() {
    let server = start(config());
    let response = send_raw(&server.addr, b"GET /health HTTP/1.1\nHost: t\n\n");
    assert!(status_line(&response).contains("200 OK"), "{response:?}");
    let response = send_raw(
        &server.addr,
        b"POST /echo HTTP/1.1\nContent-Length: 2\n\n{}",
    );
    assert!(response.contains("\"body_len\":2"), "{response:?}");
    assert_alive(&server.addr);
}

#[test]
fn malformed_or_unsupported_http_versions_are_rejected() {
    let server = start(config());
    for (request, expected) in [
        ("GET /x HTTP/1.foo\r\n\r\n", "400"),
        ("GET /x HTTP/1.\r\n\r\n", "400"),
        ("GET /x HTTP/1.1x\r\n\r\n", "400"),
        ("GET /x FTP/1.1\r\n\r\n", "400"),
        ("GET /x HTTP/1.1 extra\r\n\r\n", "400"),
        ("GET /x\r\n\r\n", "400"),
        ("GET\r\n\r\n", "400"),
        ("GET /x HTTP/2.0\r\n\r\n", "505"),
        ("GET /x HTTP/0.9\r\n\r\n", "505"),
    ] {
        let response = send_raw(&server.addr, request.as_bytes());
        assert!(
            status_line(&response).contains(expected),
            "{request:?} -> {response:?}"
        );
    }
    for ok in ["HTTP/1.0", "HTTP/1.1"] {
        let response = send_raw(&server.addr, format!("GET /x {ok}\r\n\r\n").as_bytes());
        assert!(status_line(&response).contains("200"), "{ok}: {response:?}");
    }
}

#[test]
fn duplicate_credential_headers_are_rejected_with_400() {
    let server = start(config());
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
        let request = format!("GET /x HTTP/1.1\r\nHost: t\r\n{headers}\r\n");
        let response = send_raw(&server.addr, request.as_bytes());
        assert!(
            status_line(&response).contains("400"),
            "{label}: {response:?}"
        );
    }
    let response = send_raw(
        &server.addr,
        b"GET /x HTTP/1.1\r\nHost: t\r\nAuthorization: Bearer one\r\n\r\n",
    );
    assert!(status_line(&response).contains("200 OK"), "{response:?}");
}

#[test]
fn expect_100_continue_is_answered_with_417_without_stalling() {
    let server = start(config());
    let mut stream = connect(&server.addr);
    stream
        .write_all(b"POST /echo HTTP/1.1\r\nExpect: 100-continue\r\nContent-Length: 40\r\n\r\n")
        .expect("write headers");
    let started = Instant::now();
    let mut out = vec![0u8; 512];
    let n = stream.read(&mut out).expect("a response, not a stall");
    let response = String::from_utf8_lossy(&out[..n]);
    assert!(status_line(&response).contains("417"), "{response:?}");
    assert!(started.elapsed() < Duration::from_secs(1));
}

#[test]
fn continue100_policy_acknowledges_then_serves_the_body() {
    let mut cfg = config();
    cfg.expect = ExpectPolicy::Continue100;
    let server = start(cfg);
    let mut stream = connect(&server.addr);
    stream
        .write_all(b"POST /echo HTTP/1.1\r\nExpect: 100-continue\r\nContent-Length: 4\r\n\r\n")
        .expect("write headers");
    let mut interim = vec![0u8; 25];
    stream.read_exact(&mut interim).expect("100 Continue");
    assert_eq!(&interim, b"HTTP/1.1 100 Continue\r\n\r\n");
    stream.write_all(b"abcd").expect("body");
    let _ = stream.shutdown(Shutdown::Write);
    let mut out = String::new();
    stream.read_to_string(&mut out).expect("final response");
    assert!(status_line(&out).contains("200"), "{out:?}");
    assert!(out.contains("\"body_len\":4"));
}

#[test]
fn bad_percent_encoding_is_reported_and_good_encoding_decodes() {
    let server = start(config());
    for target in ["/echo?a=%zz", "/echo?a=%4", "/echo?a=%FF", "/echo?%=1"] {
        let response = send_raw(
            &server.addr,
            format!("GET {target} HTTP/1.1\r\n\r\n").as_bytes(),
        );
        assert!(
            status_line(&response).contains("400"),
            "{target}: {response:?}"
        );
    }
    let response = send_raw(
        &server.addr,
        b"GET /echo?q=caf%C3%A9+x&flag&k=a%3Ab HTTP/1.1\r\n\r\n",
    );
    assert!(
        response.contains("\"q\":\"caf\u{e9} x\"") && response.contains("\"k\":\"a:b\""),
        "{response:?}"
    );
    assert!(response.contains("\"flag\":\"\""), "{response:?}");
}

// ---------------------------------------------------------------------------
// Deadlines
// ---------------------------------------------------------------------------

#[test]
fn slow_trickle_client_is_dropped_at_whole_request_deadline() {
    let mut cfg = config();
    cfg.request_deadline = Duration::from_millis(600);
    let server = start(cfg);
    let started = Instant::now();
    let mut stream = connect(&server.addr);
    // Each byte arrives well inside any per-read timeout, but the request as
    // a whole never completes.
    let trickle = b"GET /x HTTP/1.1\r\nX-Slow: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
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
        "took {:?}",
        started.elapsed()
    );
    assert_alive(&server.addr);
}

#[test]
fn slow_body_is_cut_off_with_408() {
    let mut cfg = config();
    cfg.request_deadline = Duration::from_millis(500);
    let server = start(cfg);
    let mut stream = connect(&server.addr);
    stream
        .write_all(b"POST /echo HTTP/1.1\r\nContent-Length: 100\r\n\r\nabc")
        .unwrap();
    let started = Instant::now();
    let mut out = String::new();
    stream.read_to_string(&mut out).unwrap();
    assert!(status_line(&out).contains("408"), "{out:?}");
    assert!(started.elapsed() < Duration::from_secs(3));
    assert!(server.hooks.read_errors.lock().unwrap().contains(&408));
}

#[test]
fn per_read_timeout_applies_on_top_of_the_request_deadline() {
    let mut cfg = config();
    cfg.read_timeout = Some(Duration::from_millis(250));
    cfg.request_deadline = Duration::from_secs(5);
    let server = start(cfg);
    let mut stream = connect(&server.addr);
    stream.write_all(b"GET /x HTTP/1.1\r\nX-A").unwrap();
    let started = Instant::now();
    let mut out = String::new();
    stream.read_to_string(&mut out).unwrap();
    assert!(status_line(&out).contains("408"), "{out:?}");
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "{:?}",
        started.elapsed()
    );
}

#[test]
fn slow_clients_cannot_exhaust_worker_pool() {
    let mut cfg = config();
    cfg.request_deadline = Duration::from_millis(500);
    let server = start(cfg);
    // More trickling connections than general workers (4).
    let hogs: Vec<TcpStream> = (0..8)
        .map(|_| {
            let mut s = TcpStream::connect(&server.addr).expect("connect");
            s.write_all(b"GET /x HTTP/1.1\r\nX-A: b").expect("write");
            s
        })
        .collect();
    std::thread::sleep(Duration::from_millis(1800));
    assert_alive(&server.addr);
    drop(hogs);
}

#[test]
fn silent_sockets_are_closed_at_the_first_byte_timeout_without_a_worker() {
    let mut cfg = config();
    cfg.workers = 1;
    cfg.first_byte_timeout = Duration::from_millis(300);
    cfg.request_deadline = Duration::from_secs(30);
    let server = start(cfg);
    let idle: Vec<TcpStream> = (0..50)
        .map(|_| TcpStream::connect(&server.addr).expect("connect idle"))
        .collect();
    // With one general worker and 50 silent sockets, a real request is still
    // served promptly because silent sockets never reach the worker queue.
    let started = Instant::now();
    let response = send_raw(&server.addr, b"GET /echo HTTP/1.1\r\n\r\n");
    assert!(status_line(&response).contains("200"), "{response:?}");
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "{:?}",
        started.elapsed()
    );
    std::thread::sleep(Duration::from_millis(700));
    for mut stream in idle.into_iter().take(10) {
        stream
            .set_read_timeout(Some(Duration::from_millis(500)))
            .unwrap();
        let mut buf = [0u8; 16];
        // The server closes (or errors) without writing anything.
        assert!(matches!(stream.read(&mut buf), Ok(0) | Err(_)));
    }
}

#[test]
fn connections_that_outwait_the_deadline_in_the_queue_are_dropped_unanswered() {
    let mut cfg = config();
    cfg.workers = 1;
    cfg.health_workers = 0;
    cfg.queue_capacity = 4;
    cfg.request_deadline = Duration::from_millis(400);
    let server = start(cfg);
    let addr = server.addr.clone();
    let blocker =
        std::thread::spawn(move || send_raw(&addr, b"GET /slow?ms=1200 HTTP/1.1\r\n\r\n"));
    std::thread::sleep(Duration::from_millis(150));
    // Queued behind the 1.2 s request: by the time the worker is free it is
    // past its 400 ms deadline.
    let stale = send_raw(&server.addr, b"GET /echo HTTP/1.1\r\n\r\n");
    assert!(
        stale.is_empty(),
        "stale connection must be closed silently: {stale:?}"
    );
    let first = blocker.join().unwrap();
    // The blocker itself was read before its deadline, so it is answered.
    assert!(status_line(&first).contains("200"), "{first:?}");
    assert_alive(&server.addr);
}

// ---------------------------------------------------------------------------
// Admission: per-IP cap, bounded queue, health lane
// ---------------------------------------------------------------------------

#[test]
fn per_ip_connection_cap_sheds_excess_and_recovers() {
    let mut cfg = config();
    cfg.max_conns_per_ip = 8;
    cfg.first_byte_timeout = Duration::from_millis(800);
    let server = start(cfg);
    let idle: Vec<TcpStream> = (0..8)
        .map(|_| TcpStream::connect(&server.addr).expect("connect idle"))
        .collect();
    std::thread::sleep(Duration::from_millis(100));
    let shed = send_raw(&server.addr, b"GET /health HTTP/1.1\r\n\r\n");
    assert!(status_line(&shed).contains("503"), "{shed:?}");
    assert!(shed.contains("worker queue full"), "{shed:?}");
    assert!(server.hooks.per_ip_rejects.load(Ordering::SeqCst) >= 1);
    drop(idle);
    std::thread::sleep(Duration::from_millis(200));
    assert_alive(&server.addr);
}

#[test]
fn per_ip_cap_of_zero_disables_the_cap() {
    let server = start(config());
    let many: Vec<TcpStream> = (0..100)
        .map(|_| TcpStream::connect(&server.addr).expect("connect"))
        .collect();
    std::thread::sleep(Duration::from_millis(100));
    assert_alive(&server.addr);
    assert_eq!(server.hooks.per_ip_rejects.load(Ordering::SeqCst), 0);
    drop(many);
}

#[test]
fn full_queue_is_shed_with_the_overload_response() {
    let mut cfg = config();
    cfg.workers = 1;
    cfg.queue_capacity = 1;
    cfg.health_workers = 0;
    cfg.request_deadline = Duration::from_secs(10);
    cfg.overload_response = Response::error(503, "custom overload").with_header("Retry-After", "1");
    let server = start(cfg);
    let slow = |addr: String| {
        std::thread::spawn(move || send_raw(&addr, b"GET /slow?ms=1200 HTTP/1.1\r\n\r\n"))
    };
    // One request in the worker, one in the queue.
    let first = slow(server.addr.clone());
    std::thread::sleep(Duration::from_millis(150));
    let second = slow(server.addr.clone());
    std::thread::sleep(Duration::from_millis(150));
    let shed = send_raw(&server.addr, b"GET /echo HTTP/1.1\r\n\r\n");
    assert!(status_line(&shed).contains("503"), "{shed:?}");
    assert!(shed.contains("custom overload"), "{shed:?}");
    assert!(shed.contains("Retry-After: 1\r\n"), "{shed:?}");
    assert_eq!(server.hooks.queue_full_rejects.load(Ordering::SeqCst), 1);
    for client in [first, second] {
        assert!(status_line(&client.join().unwrap()).contains("200"));
    }
    // Queue accounting returns to zero once everything drained.
    let hooks = &server.hooks;
    assert_eq!(
        hooks.enqueued.load(Ordering::SeqCst),
        hooks.dequeued.load(Ordering::SeqCst)
    );
    assert_alive(&server.addr);
}

#[test]
fn health_lane_answers_while_the_general_lane_is_saturated() {
    let mut cfg = config();
    cfg.workers = 1;
    cfg.queue_capacity = 2;
    cfg.health_workers = 1;
    cfg.request_deadline = Duration::from_secs(10);
    let server = start(cfg);
    let clients: Vec<_> = (0..2)
        .map(|_| {
            let addr = server.addr.clone();
            std::thread::spawn(move || send_raw(&addr, b"GET /slow?ms=1500 HTTP/1.1\r\n\r\n"))
        })
        .collect();
    std::thread::sleep(Duration::from_millis(300));
    for path in ["/health", "/live", "/ready", "/metrics"] {
        let started = Instant::now();
        let response = send_raw(
            &server.addr,
            format!("GET {path} HTTP/1.1\r\n\r\n").as_bytes(),
        );
        assert!(!response.is_empty(), "{path} unanswered");
        assert!(
            started.elapsed() < Duration::from_millis(500),
            "{path} took {:?} behind slow work",
            started.elapsed()
        );
    }
    for client in clients {
        assert!(status_line(&client.join().unwrap()).contains("200"));
    }
}

#[test]
fn health_classifier_is_pluggable() {
    let mut cfg = config();
    cfg.workers = 1;
    cfg.queue_capacity = 2;
    cfg.health_workers = 1;
    let server = start_with(
        cfg,
        Arc::new(echo),
        |_method, path| path == "/health",
        |l| l,
    );
    let addr = server.addr.clone();
    let slow = std::thread::spawn(move || send_raw(&addr, b"GET /slow?ms=1200 HTTP/1.1\r\n\r\n"));
    let addr = server.addr.clone();
    let queued = std::thread::spawn(move || send_raw(&addr, b"GET /slow?ms=10 HTTP/1.1\r\n\r\n"));
    std::thread::sleep(Duration::from_millis(300));
    let started = Instant::now();
    assert_alive(&server.addr);
    assert!(started.elapsed() < Duration::from_millis(500));
    slow.join().unwrap();
    queued.join().unwrap();
}

// ---------------------------------------------------------------------------
// Resilience
// ---------------------------------------------------------------------------

#[test]
fn handler_panic_returns_500_and_does_not_kill_the_worker() {
    let mut cfg = config();
    cfg.workers = 1;
    cfg.health_workers = 0;
    let server = start(cfg);
    for _ in 0..3 {
        let response = send_raw(&server.addr, b"GET /panic HTTP/1.1\r\n\r\n");
        assert!(status_line(&response).contains("500"), "{response:?}");
        assert!(
            response.contains("\"error\":\"internal server error\""),
            "{response:?}"
        );
        assert!(!response.contains("boom"), "panic detail must not leak");
        // The single worker is still alive.
        assert_alive(&server.addr);
    }
}

#[test]
fn accept_errors_do_not_end_the_server() {
    let server = start_with(
        config(),
        Arc::new(echo),
        dash_http::default_health_classifier,
        |inner| FlakyListener {
            inner,
            failures: AtomicUsize::new(6),
        },
    );
    // The injected EMFILE-style failures are retried with bounded backoff.
    assert_alive(&server.addr);
    assert!(!server.join.as_ref().unwrap().is_finished());
}

#[test]
fn responses_are_framed_and_close_the_connection() {
    let server = start(config());
    let response = send_raw(&server.addr, b"GET /health HTTP/1.1\r\n\r\n");
    assert!(response.starts_with("HTTP/1.1 200 OK\r\n"), "{response:?}");
    assert!(response.contains("\r\nContent-Type: application/json\r\n"));
    assert!(response.contains("\r\nContent-Length: 15\r\n"));
    assert!(response.contains("\r\nConnection: close\r\n\r\n{\"status\":\"ok\"}"));
}

#[test]
fn peer_address_is_passed_to_the_handler() {
    let server = start(config());
    let response = send_raw(&server.addr, b"GET /echo HTTP/1.1\r\n\r\n");
    assert!(response.contains("\"peer\":\"127.0.0.1\""), "{response:?}");
}

#[test]
fn shutdown_drains_in_flight_requests_and_returns() {
    let mut server = start(config());
    let addr = server.addr.clone();
    let inflight =
        std::thread::spawn(move || send_raw(&addr, b"GET /slow?ms=600 HTTP/1.1\r\n\r\n"));
    std::thread::sleep(Duration::from_millis(200));
    server.stop.store(true, Ordering::SeqCst);
    let result = server.join.take().unwrap().join().expect("serve thread");
    assert!(result.is_ok());
    // The in-flight request completed during the drain.
    assert!(status_line(&inflight.join().unwrap()).contains("200"));
    assert_eq!(server.hooks.shutdowns.load(Ordering::SeqCst), 1);
}

#[test]
fn read_error_hook_sees_every_rejected_request() {
    let server = start(config());
    send_raw(&server.addr, b"GET /x HTTP/2.0\r\n\r\n");
    send_raw(
        &server.addr,
        b"POST /x HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n",
    );
    send_raw(
        &server.addr,
        b"GET /x HTTP/1.1\r\nExpect: 100-continue\r\n\r\n",
    );
    let mut seen = server.hooks.read_errors.lock().unwrap().clone();
    seen.sort_unstable();
    assert_eq!(seen, vec![417, 501, 505]);
}
