//! `/metrics` exposition validity, shared HTTP metrics and request-id
//! correlation, through the real socket server.

use std::{
    io::{Read, Write},
    net::{Shutdown, TcpListener, TcpStream},
    sync::{Arc, OnceLock, RwLock},
    time::Duration,
};

use retrieval::transport::serve_http_with_workers;
use store::InMemoryStore;

/// One server per test binary; the audit log goes to a temp file so the
/// request id written into audit records can be checked.
fn server() -> &'static (String, std::path::PathBuf) {
    static SERVER: OnceLock<(String, std::path::PathBuf)> = OnceLock::new();
    SERVER.get_or_init(|| {
        let dir = tempfile::tempdir().expect("tempdir").keep();
        let audit = dir.join("retrieval-audit.jsonl");
        #[allow(unused_unsafe)]
        unsafe {
            std::env::set_var("DASH_INSECURE_DEV_MODE", "1");
            std::env::set_var("DASH_STRICT_SECRETS", "0");
            std::env::set_var("DASH_RETRIEVAL_AUDIT_LOG_PATH", &audit);
        }
        let port = TcpListener::bind("127.0.0.1:0")
            .expect("bind probe listener")
            .local_addr()
            .expect("local addr")
            .port();
        let addr = format!("127.0.0.1:{port}");
        let bind = addr.clone();
        let store = Arc::new(RwLock::new(InMemoryStore::new()));
        let shutdown = dash_common::ShutdownSignal::manual();
        std::thread::spawn(move || {
            let _ = serve_http_with_workers(store, &bind, 4, shutdown);
        });
        for _ in 0..100 {
            if TcpStream::connect(&addr).is_ok() {
                return (addr, audit);
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        panic!("server did not start");
    })
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

fn body(response: &str) -> &str {
    response.split_once("\r\n\r\n").map_or("", |(_, b)| b)
}

fn header<'a>(response: &'a str, name: &str) -> Option<&'a str> {
    let head = response.split("\r\n\r\n").next()?;
    head.lines().skip(1).find_map(|line| {
        let (key, value) = line.split_once(':')?;
        key.eq_ignore_ascii_case(name).then(|| value.trim())
    })
}

#[test]
fn metrics_endpoint_is_valid_exposition_with_shared_families() {
    let (addr, _) = server();
    let _ = send_raw(addr, b"GET /health HTTP/1.1\r\n\r\n");
    let _ = send_raw(
        addr,
        b"GET /v1/retrieve?tenant_id=t&query=x HTTP/1.1\r\n\r\n",
    );
    let _ = send_raw(
        addr,
        b"GET /tenants/zz-secret-tenant/claims/zz-secret-claim HTTP/1.1\r\n\r\n",
    );
    let response = send_raw(addr, b"GET /metrics HTTP/1.1\r\n\r\n");
    let text = body(&response);
    let report = dash_observe::validate(text).unwrap_or_else(|e| panic!("{e}\n{text}"));

    for family in [
        // pre-existing service families
        "dash_retrieve_requests_total",
        "dash_http_request_duration_ms",
        // shared HTTP metrics
        "dash_http_server_requests_total",
        "dash_http_server_request_duration_seconds",
        "dash_http_server_requests_in_flight",
        // storage
        "dash_wal_fsync_duration_seconds",
        "dash_wal_checkpoint_duration_seconds",
        "dash_vector_index_load_duration_seconds",
        // audit
        "dash_audit_records_total",
        "dash_audit_denials_dropped_total",
        // process and build
        "process_start_time_seconds",
        "dash_process_uptime_seconds",
        "dash_build_info",
    ] {
        assert!(report.has_family(family), "missing {family}\n{text}");
    }
    let retrieve = report.sum_where(
        "dash_http_server_requests_total",
        &[("component", "retrieval"), ("route", "retrieve")],
    );
    assert!(retrieve >= 1.0, "{text}");
    let unknown = report.sum_where(
        "dash_http_server_requests_total",
        &[("route", "other"), ("code", "404")],
    );
    assert!(unknown >= 1.0, "{text}");
    assert!(
        !text.contains("zz-secret"),
        "a path segment leaked into a label\n{text}"
    );
    // The /metrics request itself is in flight while it renders.
    assert!(
        report
            .value(
                "dash_http_server_requests_in_flight",
                &[("component", "retrieval")]
            )
            .unwrap()
            >= 1.0
    );
    assert!(
        text.contains("dash_build_info{component=\"retrieval\",version=\""),
        "{text}"
    );
}

#[test]
fn request_ids_reach_the_response_error_body_and_the_audit_record() {
    let (addr, audit) = server();
    let response = send_raw(
        addr,
        b"GET /v1/retrieve?tenant_id=t-rid&query=x HTTP/1.1\r\nX-Request-Id: corr-retrieval-1\r\n\r\n",
    );
    assert_eq!(header(&response, "x-request-id"), Some("corr-retrieval-1"));
    let records = std::fs::read_to_string(audit).unwrap_or_default();
    assert!(
        records
            .lines()
            .any(|line| line.contains("\"request_id\":\"corr-retrieval-1\"")),
        "no audit record carries the request id:\n{records}"
    );

    let response = send_raw(
        addr,
        b"POST /v1/retrieve HTTP/1.1\r\nContent-Length: 3\r\nX-Request-Id: corr-retrieval-2\r\n\r\n{x}",
    );
    assert!(response.starts_with("HTTP/1.1 400"), "{response}");
    assert_eq!(header(&response, "x-request-id"), Some("corr-retrieval-2"));
    assert!(
        body(&response).starts_with("{\"request_id\":\"corr-retrieval-2\","),
        "{response}"
    );

    let response = send_raw(addr, b"GET /health HTTP/1.1\r\n\r\n");
    let generated = header(&response, "x-request-id").expect("generated request id");
    assert_eq!(generated.len(), 32);
}
