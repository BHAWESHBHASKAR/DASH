use std::{
    ffi::{OsStr, OsString},
    fs::{File, OpenOptions},
    io::Write,
    path::PathBuf,
    sync::{Arc, Mutex, OnceLock},
    thread,
    time::Duration,
    time::{SystemTime, UNIX_EPOCH},
};

use ingestion::transport::{IngestionRuntime, handle_http_request_bytes};
use store::InMemoryStore;

fn sample_runtime() -> Arc<Mutex<IngestionRuntime>> {
    Arc::new(Mutex::new(
        IngestionRuntime::in_memory(InMemoryStore::new()),
    ))
}

fn env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

#[allow(unused_unsafe)]
fn set_env_var_for_tests(key: &str, value: &OsStr) {
    unsafe {
        std::env::set_var(key, value);
    }
}

#[allow(unused_unsafe)]
fn restore_env_var_for_tests(key: &str, value: Option<&OsStr>) {
    match value {
        Some(value) => unsafe {
            std::env::set_var(key, value);
        },
        None => unsafe {
            std::env::remove_var(key);
        },
    }
}

struct EnvVarGuard {
    key: &'static str,
    previous: Option<OsString>,
}

impl EnvVarGuard {
    fn set(key: &'static str, value: &OsStr) -> Self {
        let previous = std::env::var_os(key);
        set_env_var_for_tests(key, value);
        Self { key, previous }
    }
}

impl Drop for EnvVarGuard {
    fn drop(&mut self) {
        restore_env_var_for_tests(self.key, self.previous.as_deref());
    }
}

fn temp_placement_csv(contents: &str) -> PathBuf {
    let mut out = std::env::temp_dir();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be monotonic")
        .as_nanos();
    out.push(format!(
        "dash-ingest-placement-it-{}-{}.csv",
        std::process::id(),
        nanos
    ));
    let mut file = File::create(&out).expect("placement file should be created");
    file.write_all(contents.as_bytes())
        .expect("placement file should be writable");
    out
}

fn overwrite_placement_csv(path: &PathBuf, contents: &str) {
    let mut file = OpenOptions::new()
        .truncate(true)
        .write(true)
        .open(path)
        .expect("placement file should be writable");
    file.write_all(contents.as_bytes())
        .expect("placement file should be writable");
    file.flush().expect("placement file should flush");
}

#[test]
fn transport_post_ingest_parses_json_and_returns_response() {
    let _guard = env_lock().lock().expect("env lock should be available");
    let runtime = sample_runtime();
    let body = r#"{
      "claim": {
        "claim_id": "claim-http",
        "tenant_id": "tenant-http",
        "canonical_text": "Company X acquired Company Y",
        "confidence": 0.95
      },
      "evidence": [
        {
          "evidence_id": "ev-http",
          "claim_id": "claim-http",
          "source_id": "source://transport-http",
          "stance": "supports",
          "source_quality": 0.96
        }
      ]
    }"#;
    let request = format!(
        "POST /v1/ingest HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    );

    let response = handle_http_request_bytes(&runtime, request.as_bytes())
        .expect("request should parse and return response");
    let response = String::from_utf8(response).expect("response should be UTF-8");
    assert!(response.starts_with("HTTP/1.1 200 OK"));
    assert!(response.contains("\"ingested_claim_id\":\"claim-http\""));
    assert!(response.contains("\"claims_total\":1"));
}

#[test]
fn transport_metrics_endpoint_returns_prometheus_payload() {
    let _guard = env_lock().lock().expect("env lock should be available");
    let runtime = sample_runtime();
    let request = b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    let response = handle_http_request_bytes(&runtime, request)
        .expect("request should parse and return response");
    let response = String::from_utf8(response).expect("response should be UTF-8");

    assert!(response.starts_with("HTTP/1.1 200 OK"));
    assert!(response.contains("Content-Type: text/plain; version=0.0.4; charset=utf-8"));
    assert!(response.contains("dash_ingest_success_total"));
}

#[test]
fn transport_rejects_oversized_body_via_content_length_guard() {
    let _guard = env_lock().lock().expect("env lock should be available");
    let runtime = sample_runtime();
    let request = b"POST /v1/ingest HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: 20000000\r\nConnection: close\r\n\r\n";
    let err = handle_http_request_bytes(&runtime, request)
        .expect_err("oversized payload should be rejected");
    assert!(err.contains("exceeds max body size"));
}

#[test]
fn transport_loads_placement_routing_from_env_csv() {
    let _guard = env_lock().lock().expect("env lock should be available");
    let placement_file = temp_placement_csv(
        "tenant-http,0,7,node-a,leader,healthy\n\
tenant-http,0,7,node-b,follower,healthy\n",
    );
    let _placement_env = EnvVarGuard::set("DASH_ROUTER_PLACEMENT_FILE", placement_file.as_os_str());
    let _local_node_env = EnvVarGuard::set("DASH_ROUTER_LOCAL_NODE_ID", OsStr::new("node-b"));

    let runtime = sample_runtime();
    let body = r#"{"claim":{"claim_id":"claim-routing","tenant_id":"tenant-http","canonical_text":"Placement route check","confidence":0.9,"entities":["company-x"]}}"#;
    let request = format!(
        "POST /v1/ingest HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    let response = handle_http_request_bytes(&runtime, request.as_bytes())
        .expect("request should parse and return response");
    let response = String::from_utf8(response).expect("response should be UTF-8");
    assert!(response.starts_with("HTTP/1.1 503"));

    let debug_request = b"GET /debug/placement?tenant_id=tenant-http&entity_key=company-x HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    let debug_response = handle_http_request_bytes(&runtime, debug_request)
        .expect("debug request should parse and return response");
    let debug_response = String::from_utf8(debug_response).expect("response should be UTF-8");
    assert!(debug_response.starts_with("HTTP/1.1 200 OK"));
    assert!(debug_response.contains("\"enabled\":true"));
    assert!(debug_response.contains("\"local_node_id\":\"node-b\""));
    assert!(debug_response.contains("\"target_node_id\":\"node-a\""));

    let _ = std::fs::remove_file(placement_file);
}

#[test]
fn transport_reloads_placement_routing_without_restart() {
    let _guard = env_lock().lock().expect("env lock should be available");
    let placement_file = temp_placement_csv(
        "tenant-http,0,7,node-a,leader,healthy\n\
tenant-http,0,7,node-b,follower,healthy\n",
    );
    let _placement_env = EnvVarGuard::set("DASH_ROUTER_PLACEMENT_FILE", placement_file.as_os_str());
    let _local_node_env = EnvVarGuard::set("DASH_ROUTER_LOCAL_NODE_ID", OsStr::new("node-b"));
    let _reload_interval_env =
        EnvVarGuard::set("DASH_ROUTER_PLACEMENT_RELOAD_INTERVAL_MS", OsStr::new("1"));

    let runtime = sample_runtime();
    let denied_body = r#"{"claim":{"claim_id":"claim-routing-before","tenant_id":"tenant-http","canonical_text":"Placement reload check","confidence":0.9,"entities":["company-x"]}}"#;
    let denied_request = format!(
        "POST /v1/ingest HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        denied_body.len(),
        denied_body
    );
    let denied_response = handle_http_request_bytes(&runtime, denied_request.as_bytes())
        .expect("request should parse and return response");
    let denied_response = String::from_utf8(denied_response).expect("response should be UTF-8");
    assert!(denied_response.starts_with("HTTP/1.1 503"));

    overwrite_placement_csv(
        &placement_file,
        "tenant-http,0,8,node-b,leader,healthy\n\
tenant-http,0,8,node-a,follower,healthy\n",
    );
    thread::sleep(Duration::from_millis(5));

    let accepted_body = r#"{"claim":{"claim_id":"claim-routing-after","tenant_id":"tenant-http","canonical_text":"Placement reload check","confidence":0.9,"entities":["company-x"]}}"#;
    let accepted_request = format!(
        "POST /v1/ingest HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        accepted_body.len(),
        accepted_body
    );
    let accepted_response = handle_http_request_bytes(&runtime, accepted_request.as_bytes())
        .expect("request should parse and return response");
    let accepted_response = String::from_utf8(accepted_response).expect("response should be UTF-8");
    assert!(accepted_response.starts_with("HTTP/1.1 200 OK"));

    let debug_request = b"GET /debug/placement?tenant_id=tenant-http&entity_key=company-x HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    let debug_response = handle_http_request_bytes(&runtime, debug_request)
        .expect("debug request should parse and return response");
    let debug_response = String::from_utf8(debug_response).expect("response should be UTF-8");
    assert!(debug_response.contains("\"epoch\":8"));
    assert!(debug_response.contains("\"local_admission\":true"));
    assert!(debug_response.contains("\"reload\":{\"enabled\":true"));

    let _ = std::fs::remove_file(placement_file);
}
