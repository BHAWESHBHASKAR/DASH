//! HTTP API compatibility: the exact ingest requests clients sent to every
//! earlier release (with the API key header they used) are accepted by the
//! current ingestion service, and every response field the old release
//! returned is still present with the same value where the value is
//! deterministic. Retrieve answers are compared in `retrieve_results.rs`.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use dash_compat::{FIXTURES, INGEST_KEY, env_lock, ingest_requests, remove_env, set_env};
use ingestion::transport::{IngestionRuntime, serve_http_with_workers};
use serde_json::Value;
use store::{CheckpointPolicy, FileWal, InMemoryStore};

fn request(addr: &str, method: &str, path: &str, body: &str) -> (u16, Value) {
    let mut stream = TcpStream::connect(addr).expect("connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .expect("timeout");
    let req = format!(
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\nContent-Type: application/json\r\nx-api-key: {INGEST_KEY}\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(req.as_bytes()).expect("write");
    let mut raw = Vec::new();
    let _ = stream.read_to_end(&mut raw);
    dash_compat::parse_response(&raw)
}

/// Fields that depend on the node's checkpoint policy and WAL record
/// layout rather than on the request (present only when a checkpoint ran).
const RUN_DEPENDENT: &[&str] = &[
    "checkpoint_triggered",
    "checkpoint_snapshot_records",
    "checkpoint_truncated_wal_records",
];

#[test]
fn old_ingest_requests_are_accepted_with_compatible_responses() {
    let _env = env_lock();
    set_env("DASH_INGEST_API_KEY", INGEST_KEY);
    remove_env("DASH_INSECURE_DEV_MODE");
    remove_env("DASH_INGEST_REPLICATION_SOURCE_URL");
    for fixture in FIXTURES {
        let dir = tempfile::tempdir().expect("tempdir");
        let addr = {
            let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
            listener.local_addr().expect("addr").to_string()
        };
        let wal = FileWal::open(dir.path().join("ingest.wal")).expect("wal");
        let store = InMemoryStore::load_from_wal(&wal).expect("load");
        let runtime = IngestionRuntime::persistent(store, wal, CheckpointPolicy::default());
        let shutdown = dash_common::ShutdownSignal::manual();
        let server = {
            let addr = addr.clone();
            let shutdown = Arc::clone(&shutdown);
            thread::spawn(move || {
                serve_http_with_workers(runtime, &addr, 2, shutdown).expect("serve")
            })
        };
        let deadline = Instant::now() + Duration::from_secs(10);
        while TcpStream::connect(&addr).is_err() {
            assert!(Instant::now() < deadline, "server did not start");
            thread::sleep(Duration::from_millis(10));
        }
        let recorded = fixture.read_jsonl("http/ingest-responses.jsonl");
        for (index, row) in ingest_requests().iter().enumerate() {
            let body = row["body"].to_string();
            let (status, answer) = request(
                &addr,
                row["method"].as_str().expect("method"),
                row["path"].as_str().expect("path"),
                &body,
            );
            let old = &recorded[index];
            assert_eq!(
                u64::from(status),
                old["status"].as_u64().unwrap_or(0),
                "{} request {index}: {answer}",
                fixture.label
            );
            for (key, value) in old["body"].as_object().expect("old answer is an object") {
                if RUN_DEPENDENT.contains(&key.as_str()) {
                    continue;
                }
                assert!(
                    answer.get(key).is_some(),
                    "{} request {index}: field '{key}' missing in {answer}",
                    fixture.label
                );
                assert_eq!(
                    &answer[key], value,
                    "{} request {index}: field '{key}'",
                    fixture.label
                );
            }
        }
        shutdown.trigger();
        server.join().expect("server thread");
    }
    remove_env("DASH_INGEST_API_KEY");
}
