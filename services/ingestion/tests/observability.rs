//! `/metrics` exposition validity, shared HTTP and storage metrics, and
//! request-id correlation, through the real socket server.

use std::{
    io::{Read, Write},
    net::{Shutdown, TcpListener, TcpStream},
    sync::OnceLock,
    time::Duration,
};

use ingestion::transport::{IngestionRuntime, serve_http_with_workers};
use store::{CheckpointPolicy, FileWal, InMemoryStore};

/// One persistent (WAL-backed) server per test binary; the audit log goes to
/// a temp file so the request id in audit records can be checked.
fn server() -> &'static (String, std::path::PathBuf) {
    static SERVER: OnceLock<(String, std::path::PathBuf)> = OnceLock::new();
    SERVER.get_or_init(|| {
        let dir = tempfile::tempdir().expect("tempdir").keep();
        let audit = dir.join("ingest-audit.jsonl");
        #[allow(unused_unsafe)]
        unsafe {
            std::env::set_var("DASH_INSECURE_DEV_MODE", "1");
            std::env::set_var("DASH_STRICT_SECRETS", "0");
            std::env::set_var("DASH_INGEST_AUDIT_LOG_PATH", &audit);
        }
        let port = TcpListener::bind("127.0.0.1:0")
            .expect("bind probe listener")
            .local_addr()
            .expect("local addr")
            .port();
        let addr = format!("127.0.0.1:{port}");
        let bind = addr.clone();
        let wal_path = dir.join("ingest.wal");
        let shutdown = dash_common::ShutdownSignal::manual();
        std::thread::spawn(move || {
            let wal = FileWal::open_with_sync_every_records(&wal_path, 1).expect("open wal");
            let store = InMemoryStore::load_from_wal(&wal).expect("load wal");
            let runtime = IngestionRuntime::persistent(store, wal, CheckpointPolicy::default());
            let _ = serve_http_with_workers(runtime, &bind, 4, shutdown);
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

fn ingest(addr: &str, claim_id: &str, request_id: &str) -> String {
    let body = format!(
        r#"{{"claim":{{"claim_id":"{claim_id}","tenant_id":"tenant-obs","canonical_text":"Company X acquired Company Y","confidence":0.9}},"evidence":[]}}"#
    );
    let request = format!(
        "POST /v1/ingest HTTP/1.1\r\nContent-Type: application/json\r\nX-Request-Id: {request_id}\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    send_raw(addr, request.as_bytes())
}

#[test]
fn metrics_endpoint_is_valid_exposition_with_shared_and_storage_families() {
    let (addr, _) = server();
    let response = ingest(addr, "claim-obs-1", "obs-metrics-1");
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    let _ = send_raw(
        addr,
        b"DELETE /v1/claims/tenant-obs/claim-secret-id HTTP/1.1\r\n\r\n",
    );
    let response = send_raw(addr, b"GET /metrics HTTP/1.1\r\n\r\n");
    let text = body(&response);
    let report = dash_observe::validate(text).unwrap_or_else(|e| panic!("{e}\n{text}"));

    for family in [
        "dash_ingest_success_total",
        "dash_ingest_wal_poisoned",
        "dash_http_server_requests_total",
        "dash_http_server_request_duration_seconds",
        "dash_http_server_requests_in_flight",
        "dash_wal_append_duration_seconds",
        "dash_wal_fsync_duration_seconds",
        "dash_wal_appended_bytes_total",
        "dash_wal_size_bytes",
        "dash_wal_checkpoint_duration_seconds",
        "dash_wal_checkpoint_failures_total",
        "dash_wal_group_commit_batch_entries",
        "dash_vector_index_save_duration_seconds",
        "dash_audit_records_total",
        "dash_audit_write_failures_total",
        "dash_build_info",
        "process_start_time_seconds",
    ] {
        assert!(report.has_family(family), "missing {family}\n{text}");
    }
    assert!(
        report.sum_where(
            "dash_http_server_requests_total",
            &[
                ("component", "ingestion"),
                ("route", "ingest"),
                ("code", "200")
            ],
        ) >= 1.0,
        "{text}"
    );
    assert!(
        report.sum_where(
            "dash_http_server_requests_total",
            &[("route", "delete_claim")]
        ) >= 1.0,
        "{text}"
    );
    assert!(
        !text.contains("claim-secret-id"),
        "a path segment leaked into a label\n{text}"
    );
    // The ingest above appended to the WAL and synced it.
    assert!(
        report.value("dash_wal_size_bytes", &[]).unwrap() > 0.0,
        "{text}"
    );
    assert!(
        report
            .value("dash_wal_fsync_duration_seconds_count", &[])
            .unwrap()
            >= 1.0,
        "{text}"
    );
    assert!(
        report.value("dash_wal_appended_bytes_total", &[]).unwrap() > 0.0,
        "{text}"
    );
}

#[test]
fn request_ids_reach_the_response_error_body_and_the_audit_record() {
    let (addr, audit) = server();
    let response = ingest(addr, "claim-obs-2", "corr-ingest-1");
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    assert_eq!(header(&response, "x-request-id"), Some("corr-ingest-1"));
    let records = std::fs::read_to_string(audit).unwrap_or_default();
    assert!(
        records
            .lines()
            .any(|line| line.contains("\"request_id\":\"corr-ingest-1\"")),
        "no audit record carries the request id:\n{records}"
    );

    let response = send_raw(
        addr,
        b"POST /v1/ingest HTTP/1.1\r\nContent-Length: 3\r\nX-Request-Id: corr-ingest-2\r\n\r\n{x}",
    );
    assert!(response.starts_with("HTTP/1.1 400"), "{response}");
    assert_eq!(header(&response, "x-request-id"), Some("corr-ingest-2"));
    assert!(
        body(&response).starts_with("{\"request_id\":\"corr-ingest-2\","),
        "{response}"
    );
}
