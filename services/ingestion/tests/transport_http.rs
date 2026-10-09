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

use auth::{encode_hs256_token, encode_hs256_token_with_kid};
use ingestion::transport::{IngestionRuntime, handle_http_request_bytes};
use store::InMemoryStore;

fn sample_runtime() -> Arc<Mutex<IngestionRuntime>> {
    ensure_dev_mode_env();
    Arc::new(Mutex::new(
        IngestionRuntime::in_memory(InMemoryStore::new()),
    ))
}

/// Tests that exercise handlers without configuring credentials run in
/// explicit dev mode (the only way to get an unauthenticated service).
#[allow(unused_unsafe)]
fn ensure_dev_mode_env() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| unsafe {
        std::env::set_var("DASH_INSECURE_DEV_MODE", "1");
        std::env::set_var("DASH_STRICT_SECRETS", "0");
    });
}

fn env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| {
        ensure_dev_mode_env();
        Mutex::new(())
    })
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

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be monotonic")
        .as_secs()
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
fn transport_post_ingest_raw_extracts_claims_and_supports_idempotent_replay() {
    let _guard = env_lock().lock().expect("env lock should be available");
    let runtime = sample_runtime();
    let body = r#"{
      "tenant_id": "tenant-http",
      "document_id": "doc-http-raw-1",
      "source_id": "source://doc-http-raw-1",
      "text": "Company X acquired Company Y in 2024. Revenue increased in Q4.",
      "min_sentence_chars": 10,
      "max_claims": 4
    }"#;
    let request = format!(
        "POST /v1/ingest/raw HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    );

    let first = handle_http_request_bytes(&runtime, request.as_bytes())
        .expect("first raw request should parse and return response");
    let first = String::from_utf8(first).expect("response should be UTF-8");
    assert!(first.starts_with("HTTP/1.1 200 OK"));
    assert!(first.contains("\"document_id\":\"doc-http-raw-1\""));
    assert!(first.contains("\"extracted_count\":2"));
    assert!(first.contains("\"idempotent_replay\":false"));
    assert!(first.contains("\"claims_total\":2"));

    let replay = handle_http_request_bytes(&runtime, request.as_bytes())
        .expect("second raw request should parse and return response");
    let replay = String::from_utf8(replay).expect("response should be UTF-8");
    assert!(replay.starts_with("HTTP/1.1 200 OK"));
    assert!(replay.contains("\"idempotent_replay\":true"));
    assert!(replay.contains("\"claims_total\":2"));
}

#[test]
fn transport_post_ingest_document_with_inline_text_extracts_claims() {
    let _guard = env_lock().lock().expect("env lock should be available");
    let runtime = sample_runtime();
    let body = r#"{
      "tenant_id": "tenant-http",
      "document_id": "doc-http-doc-1",
      "source_id": "source://doc-http-doc-1",
      "mime_type": "application/pdf",
      "text": "Company X acquired Company Y in 2024. Revenue increased in Q4.",
      "min_sentence_chars": 10,
      "max_claims": 4
    }"#;
    let request = format!(
        "POST /v1/ingest/document HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    );

    let response = handle_http_request_bytes(&runtime, request.as_bytes())
        .expect("document request should parse and return response");
    let response = String::from_utf8(response).expect("response should be UTF-8");
    assert!(response.starts_with("HTTP/1.1 200 OK"));
    assert!(response.contains("\"document_id\":\"doc-http-doc-1\""));
    assert!(response.contains("\"parser_provider\":\"inline_text\""));
    assert!(response.contains("\"extracted_count\":2"));
}

#[test]
fn transport_post_ingest_document_accepts_pdf_bytes_with_adapter_command() {
    let _guard = env_lock().lock().expect("env lock should be available");
    let _provider = EnvVarGuard::set(
        "DASH_INGEST_DOCUMENT_PARSER_PROVIDER",
        OsStr::new("adapter_command"),
    );
    let _cmd = EnvVarGuard::set("DASH_INGEST_DOCUMENT_ADAPTER_CMD", OsStr::new("cat"));
    let runtime = sample_runtime();
    let body = r#"{
      "tenant_id": "tenant-http",
      "document_id": "doc-http-doc-2",
      "source_id": "source://doc-http-doc-2",
      "mime_type": "application/pdf",
      "content_base64": "Q29tcGFueSBYIGFjcXVpcmVkIENvbXBhbnkgWS4gUmV2ZW51ZSByb3NlIGluIFE0Lg==",
      "min_sentence_chars": 10,
      "max_claims": 4
    }"#;
    let request = format!(
        "POST /v1/ingest/document HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    );

    let response = handle_http_request_bytes(&runtime, request.as_bytes())
        .expect("document request should parse and return response");
    let response = String::from_utf8(response).expect("response should be UTF-8");
    assert!(response.starts_with("HTTP/1.1 200 OK"));
    assert!(response.contains("\"document_id\":\"doc-http-doc-2\""));
    assert!(response.contains("\"parser_provider\":\"cat\""));
    assert!(response.contains("\"extracted_count\":2"));
}

#[test]
fn transport_post_ingest_document_generates_embeddings_when_enabled() {
    let _guard = env_lock().lock().expect("env lock should be available");
    let _embedding_provider =
        EnvVarGuard::set("DASH_INGEST_EMBEDDING_PROVIDER", OsStr::new("hash_vector"));
    let _embedding_dimensions =
        EnvVarGuard::set("DASH_INGEST_EMBEDDING_DIMENSIONS", OsStr::new("8"));
    let runtime = sample_runtime();
    let body = r#"{
      "tenant_id": "tenant-http",
      "document_id": "doc-http-doc-emb",
      "source_id": "source://doc-http-doc-emb",
      "mime_type": "text/plain",
      "text": "Company X acquired Company Y in 2024. Revenue increased in Q4.",
      "min_sentence_chars": 10,
      "max_claims": 4,
      "generate_embeddings": true,
      "embedding_model": "emb://hash-v1"
    }"#;
    let request = format!(
        "POST /v1/ingest/document HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    );

    let response = handle_http_request_bytes(&runtime, request.as_bytes())
        .expect("document request should parse and return response");
    let response = String::from_utf8(response).expect("response should be UTF-8");
    assert!(response.starts_with("HTTP/1.1 200 OK"));
    assert!(response.contains("\"document_id\":\"doc-http-doc-emb\""));
    assert!(response.contains("\"embedding_provider\":\"hash8\""));
    assert!(response.contains("\"embeddings_generated\":2"));
    assert!(response.contains("\"embedding_dimensions\":8"));
}

#[test]
fn transport_post_ingest_batch_parses_json_and_returns_commit_metadata() {
    let _guard = env_lock().lock().expect("env lock should be available");
    let runtime = sample_runtime();
    let body = r#"{
      "items": [
        {
          "claim": {
            "claim_id": "claim-batch-1",
            "tenant_id": "tenant-http",
            "canonical_text": "Batch claim one",
            "confidence": 0.95
          }
        },
        {
          "claim": {
            "claim_id": "claim-batch-2",
            "tenant_id": "tenant-http",
            "canonical_text": "Batch claim two",
            "confidence": 0.93
          }
        }
      ]
    }"#;
    let request = format!(
        "POST /v1/ingest/batch HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    );

    let response = handle_http_request_bytes(&runtime, request.as_bytes())
        .expect("request should parse and return response");
    let response = String::from_utf8(response).expect("response should be UTF-8");
    assert!(response.starts_with("HTTP/1.1 200 OK"));
    assert!(response.contains("\"commit_id\":\"commit-"));
    assert!(response.contains("\"idempotent_replay\":false"));
    assert!(response.contains("\"batch_size\":2"));
    assert!(response.contains("\"ingested_claim_ids\":[\"claim-batch-1\",\"claim-batch-2\"]"));
    assert!(response.contains("\"claims_total\":2"));
}

#[test]
fn transport_post_ingest_batch_replays_idempotently_for_same_commit_id() {
    let _guard = env_lock().lock().expect("env lock should be available");
    let runtime = sample_runtime();
    let body = r#"{
      "commit_id": "http-idem-1",
      "items": [
        {
          "claim": {
            "claim_id": "claim-http-idem-1",
            "tenant_id": "tenant-http",
            "canonical_text": "HTTP idempotent one",
            "confidence": 0.95
          }
        },
        {
          "claim": {
            "claim_id": "claim-http-idem-2",
            "tenant_id": "tenant-http",
            "canonical_text": "HTTP idempotent two",
            "confidence": 0.93
          }
        }
      ]
    }"#;
    let request = format!(
        "POST /v1/ingest/batch HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    let first = handle_http_request_bytes(&runtime, request.as_bytes())
        .expect("first request should parse and return response");
    let first = String::from_utf8(first).expect("first response should be UTF-8");
    assert!(first.starts_with("HTTP/1.1 200 OK"));
    assert!(first.contains("\"idempotent_replay\":false"));

    let second = handle_http_request_bytes(&runtime, request.as_bytes())
        .expect("second request should parse and return response");
    let second = String::from_utf8(second).expect("second response should be UTF-8");
    assert!(second.starts_with("HTTP/1.1 200 OK"));
    assert!(second.contains("\"idempotent_replay\":true"));
}

#[test]
fn transport_post_ingest_batch_commit_id_reuse_with_changed_content_is_an_update() {
    let _guard = env_lock().lock().expect("env lock should be available");
    let runtime = sample_runtime();
    let first_body = r#"{
      "commit_id": "http-conflict-1",
      "items": [
        {
          "claim": {
            "claim_id": "claim-http-conflict-1",
            "tenant_id": "tenant-http",
            "canonical_text": "HTTP conflict one",
            "confidence": 0.95
          }
        }
      ]
    }"#;
    let first_request = format!(
        "POST /v1/ingest/batch HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        first_body.len(),
        first_body
    );
    let first_response = handle_http_request_bytes(&runtime, first_request.as_bytes())
        .expect("first request should parse and return response");
    let first_response = String::from_utf8(first_response).expect("response should be UTF-8");
    assert!(first_response.starts_with("HTTP/1.1 200 OK"));

    let second_body = r#"{
      "commit_id": "http-conflict-1",
      "items": [
        {
          "claim": {
            "claim_id": "claim-http-conflict-2",
            "tenant_id": "tenant-http",
            "canonical_text": "HTTP conflict two",
            "confidence": 0.93
          }
        }
      ]
    }"#;
    let second_request = format!(
        "POST /v1/ingest/batch HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        second_body.len(),
        second_body
    );
    let second_response = handle_http_request_bytes(&runtime, second_request.as_bytes())
        .expect("second request should parse and return response");
    let second_response = String::from_utf8(second_response).expect("response should be UTF-8");
    assert!(
        second_response.starts_with("HTTP/1.1 200"),
        "response was: {second_response}"
    );
    assert!(second_response.contains("\"updated\":true"));
    assert!(second_response.contains("\"idempotent_replay\":false"));
}

#[test]
fn transport_rejects_cross_tenant_claim_id_collision_with_conflict_status() {
    let _guard = env_lock().lock().expect("env lock should be available");
    let runtime = sample_runtime();
    let first_body = r#"{
      "claim": {
        "claim_id": "claim-collision",
        "tenant_id": "tenant-a",
        "canonical_text": "Tenant A claim",
        "confidence": 0.95
      }
    }"#;
    let first_request = format!(
        "POST /v1/ingest HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        first_body.len(),
        first_body
    );
    let first_response = handle_http_request_bytes(&runtime, first_request.as_bytes())
        .expect("first request should parse and return response");
    let first_response = String::from_utf8(first_response).expect("response should be UTF-8");
    assert!(first_response.starts_with("HTTP/1.1 200 OK"));

    let second_body = r#"{
      "claim": {
        "claim_id": "claim-collision",
        "tenant_id": "tenant-b",
        "canonical_text": "Tenant B claim",
        "confidence": 0.95
      }
    }"#;
    let second_request = format!(
        "POST /v1/ingest HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        second_body.len(),
        second_body
    );
    let second_response = handle_http_request_bytes(&runtime, second_request.as_bytes())
        .expect("second request should parse and return response");
    let second_response = String::from_utf8(second_response).expect("response should be UTF-8");
    assert!(
        second_response.starts_with("HTTP/1.1 409"),
        "response was: {second_response}"
    );
    assert!(second_response.contains("state conflict"));
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
fn transport_document_parser_debug_endpoint_returns_json_payload() {
    let _guard = env_lock().lock().expect("env lock should be available");
    let _provider = EnvVarGuard::set(
        "DASH_INGEST_DOCUMENT_PARSER_PROVIDER",
        OsStr::new("adapter_command"),
    );
    let _cmd = EnvVarGuard::set("DASH_INGEST_DOCUMENT_ADAPTER_CMD", OsStr::new("cat"));
    let runtime = sample_runtime();
    let request =
        b"GET /debug/document-parser HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    let response = handle_http_request_bytes(&runtime, request)
        .expect("request should parse and return response");
    let response = String::from_utf8(response).expect("response should be UTF-8");

    assert!(response.starts_with("HTTP/1.1 200 OK"));
    assert!(response.contains("Content-Type: application/json"));
    assert!(response.contains("\"parser_provider\":\"adapter_command\""));
    assert!(response.contains("\"adapter_command_preview\":\"cat\""));
    assert!(response.contains("\"embedding_provider\""));
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

#[test]
fn transport_write_consistency_all_rejects_when_required_acks_unavailable() {
    let _guard = env_lock().lock().expect("env lock should be available");
    let placement_file = temp_placement_csv(
        "tenant-http,0,9,node-a,leader,healthy\n\
tenant-http,0,9,node-b,follower,healthy\n\
tenant-http,0,9,node-c,follower,unavailable\n",
    );
    let _placement_env = EnvVarGuard::set("DASH_ROUTER_PLACEMENT_FILE", placement_file.as_os_str());
    let _local_node_env = EnvVarGuard::set("DASH_ROUTER_LOCAL_NODE_ID", OsStr::new("node-a"));

    let runtime = sample_runtime();
    let body = r#"{"claim":{"claim_id":"claim-all-reject","tenant_id":"tenant-http","canonical_text":"all consistency reject","confidence":0.9}}"#;
    let request = format!(
        "POST /v1/ingest?write_consistency=all HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    let response = handle_http_request_bytes(&runtime, request.as_bytes())
        .expect("request should parse and return response");
    let response = String::from_utf8(response).expect("response should be UTF-8");
    assert!(
        response.starts_with("HTTP/1.1 503"),
        "response was: {response}"
    );
    assert!(response.contains("write_consistency=all"));
    assert!(response.contains("healthy_replicas=2"));
    assert!(response.contains("required_acks=3"));

    let _ = std::fs::remove_file(placement_file);
}

#[test]
fn transport_write_consistency_quorum_progresses_with_replication_ack() {
    let _guard = env_lock().lock().expect("env lock should be available");
    let placement_file = temp_placement_csv(
        "tenant-http,0,9,node-a,leader,healthy\n\
tenant-http,0,9,node-b,follower,healthy\n",
    );
    let _placement_env = EnvVarGuard::set("DASH_ROUTER_PLACEMENT_FILE", placement_file.as_os_str());
    let _local_node_env = EnvVarGuard::set("DASH_ROUTER_LOCAL_NODE_ID", OsStr::new("node-a"));

    let runtime = sample_runtime();
    let body = r#"{"claim":{"claim_id":"claim-quorum-ack","tenant_id":"tenant-http","canonical_text":"quorum ack progression","confidence":0.9}}"#;
    let ingest_request = format!(
        "POST /v1/ingest?write_consistency=quorum HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    let ingest_response = handle_http_request_bytes(&runtime, ingest_request.as_bytes())
        .expect("ingest request should parse and return response");
    let ingest_response = String::from_utf8(ingest_response).expect("response should be UTF-8");
    assert!(
        ingest_response.starts_with("HTTP/1.1 200 OK"),
        "response was: {ingest_response}"
    );
    assert!(ingest_response.contains("\"ack_count\":1"));
    assert!(ingest_response.contains("\"required_acks\":2"));
    assert!(ingest_response.contains("\"commit_status\":\"replication_pending\""));

    let status_before_ack = b"GET /internal/replication/commit-status?commit_id=claim-quorum-ack HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    let status_before_ack = handle_http_request_bytes(&runtime, status_before_ack)
        .expect("commit status request should parse");
    let status_before_ack = String::from_utf8(status_before_ack).expect("response should be UTF-8");
    assert!(status_before_ack.starts_with("HTTP/1.1 200 OK"));
    assert!(status_before_ack.contains("\"ack_count\":1"));
    assert!(status_before_ack.contains("\"commit_status\":\"replication_pending\""));

    let ack_request = b"POST /internal/replication/ack?commit_id=claim-quorum-ack&replica_id=node-b&ack_epoch=9 HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
    let ack_response = handle_http_request_bytes(&runtime, ack_request)
        .expect("ack request should parse and return response");
    let ack_response = String::from_utf8(ack_response).expect("response should be UTF-8");
    assert!(ack_response.starts_with("HTTP/1.1 200 OK"));
    assert!(ack_response.contains("\"ack_count\":2"));
    assert!(ack_response.contains("\"required_acks\":2"));
    assert!(ack_response.contains("\"commit_status\":\"replication_quorum_met\""));

    let duplicate_ack_response = handle_http_request_bytes(&runtime, ack_request)
        .expect("duplicate ack request should parse and return response");
    let duplicate_ack_response =
        String::from_utf8(duplicate_ack_response).expect("response should be UTF-8");
    assert!(duplicate_ack_response.starts_with("HTTP/1.1 200 OK"));
    assert!(duplicate_ack_response.contains("\"ack_count\":2"));
    assert!(!duplicate_ack_response.contains("\"ack_count\":3"));

    let _ = std::fs::remove_file(placement_file);
}

#[test]
fn transport_denies_cross_tenant_ingest_for_scoped_key() {
    let _guard = env_lock().lock().expect("env lock should be available");
    let _scope_env = EnvVarGuard::set(
        "DASH_INGEST_API_KEY_SCOPES",
        OsStr::new("scope-a:tenant-allowed"),
    );

    let runtime = sample_runtime();
    let body = r#"{"claim":{"claim_id":"claim-scope-deny","tenant_id":"tenant-blocked","canonical_text":"Scope deny check","confidence":0.9}}"#;
    let request = format!(
        "POST /v1/ingest HTTP/1.1\r\nHost: localhost\r\nX-API-Key: scope-a\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    );

    let response = handle_http_request_bytes(&runtime, request.as_bytes())
        .expect("request should parse and return response");
    let response = String::from_utf8(response).expect("response should be UTF-8");
    assert!(response.starts_with("HTTP/1.1 403"));
    assert!(response.contains("tenant is not allowed for this API key"));
}

#[test]
fn transport_denies_revoked_ingest_key() {
    let _guard = env_lock().lock().expect("env lock should be available");
    let _scope_env = EnvVarGuard::set(
        "DASH_INGEST_API_KEY_SCOPES",
        OsStr::new("scope-a:tenant-http"),
    );
    let _revoked_env = EnvVarGuard::set("DASH_INGEST_REVOKED_API_KEYS", OsStr::new("scope-a"));

    let runtime = sample_runtime();
    let body = r#"{"claim":{"claim_id":"claim-revoked","tenant_id":"tenant-http","canonical_text":"Revoked key check","confidence":0.9}}"#;
    let request = format!(
        "POST /v1/ingest HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer scope-a\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    );

    let response = handle_http_request_bytes(&runtime, request.as_bytes())
        .expect("request should parse and return response");
    let response = String::from_utf8(response).expect("response should be UTF-8");
    assert!(response.starts_with("HTTP/1.1 401"));
    assert!(response.contains("API key revoked"));
}

#[test]
fn transport_denies_cross_tenant_ingest_for_jwt_claim_scope() {
    let _guard = env_lock().lock().expect("env lock should be available");
    let _jwt_secret = EnvVarGuard::set("DASH_INGEST_JWT_HS256_SECRET", OsStr::new("jwt-secret"));
    let _jwt_issuer = EnvVarGuard::set("DASH_INGEST_JWT_ISSUER", OsStr::new("dash"));
    let _jwt_audience = EnvVarGuard::set("DASH_INGEST_JWT_AUDIENCE", OsStr::new("ingestion"));
    let exp = now_unix_secs() + 300;
    let token = encode_hs256_token(
        &format!(
            "{{\"tenant_id\":\"tenant-allowed\",\"iss\":\"dash\",\"aud\":\"ingestion\",\"exp\":{exp}}}"
        ),
        "jwt-secret",
    )
    .expect("token should encode");

    let runtime = sample_runtime();
    let body = r#"{"claim":{"claim_id":"claim-jwt-scope-deny","tenant_id":"tenant-blocked","canonical_text":"JWT scope deny check","confidence":0.9}}"#;
    let request = format!(
        "POST /v1/ingest HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        token,
        body.len(),
        body
    );

    let response = handle_http_request_bytes(&runtime, request.as_bytes())
        .expect("request should parse and return response");
    let response = String::from_utf8(response).expect("response should be UTF-8");
    assert!(response.starts_with("HTTP/1.1 403"));
    assert!(response.contains("tenant is not allowed for this JWT"));
}

#[test]
fn transport_denies_expired_ingest_jwt() {
    let _guard = env_lock().lock().expect("env lock should be available");
    let _jwt_secret = EnvVarGuard::set("DASH_INGEST_JWT_HS256_SECRET", OsStr::new("jwt-secret"));
    let _jwt_issuer = EnvVarGuard::set("DASH_INGEST_JWT_ISSUER", OsStr::new("dash"));
    let _jwt_audience = EnvVarGuard::set("DASH_INGEST_JWT_AUDIENCE", OsStr::new("ingestion"));
    let exp = now_unix_secs().saturating_sub(10);
    let token = encode_hs256_token(
        &format!(
            "{{\"tenant_id\":\"tenant-http\",\"iss\":\"dash\",\"aud\":\"ingestion\",\"exp\":{exp}}}"
        ),
        "jwt-secret",
    )
    .expect("token should encode");

    let runtime = sample_runtime();
    let body = r#"{"claim":{"claim_id":"claim-jwt-expired","tenant_id":"tenant-http","canonical_text":"JWT expiry check","confidence":0.9}}"#;
    let request = format!(
        "POST /v1/ingest HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        token,
        body.len(),
        body
    );

    let response = handle_http_request_bytes(&runtime, request.as_bytes())
        .expect("request should parse and return response");
    let response = String::from_utf8(response).expect("response should be UTF-8");
    assert!(response.starts_with("HTTP/1.1 401"));
    assert!(response.contains("JWT expired"));
}

#[test]
fn transport_allows_ingest_jwt_signed_with_rotation_fallback_secret() {
    let _guard = env_lock().lock().expect("env lock should be available");
    let _jwt_secret = EnvVarGuard::set("DASH_INGEST_JWT_HS256_SECRET", OsStr::new("active-secret"));
    let _jwt_secrets = EnvVarGuard::set(
        "DASH_INGEST_JWT_HS256_SECRETS",
        OsStr::new("previous-secret"),
    );
    let _jwt_issuer = EnvVarGuard::set("DASH_INGEST_JWT_ISSUER", OsStr::new("dash"));
    let _jwt_audience = EnvVarGuard::set("DASH_INGEST_JWT_AUDIENCE", OsStr::new("ingestion"));
    let exp = now_unix_secs() + 300;
    let token = encode_hs256_token(
        &format!(
            "{{\"tenant_id\":\"tenant-http\",\"iss\":\"dash\",\"aud\":\"ingestion\",\"exp\":{exp}}}"
        ),
        "previous-secret",
    )
    .expect("token should encode");

    let runtime = sample_runtime();
    let body = r#"{"claim":{"claim_id":"claim-jwt-rotation-fallback","tenant_id":"tenant-http","canonical_text":"JWT rotation fallback check","confidence":0.9}}"#;
    let request = format!(
        "POST /v1/ingest HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        token,
        body.len(),
        body
    );

    let response = handle_http_request_bytes(&runtime, request.as_bytes())
        .expect("request should parse and return response");
    let response = String::from_utf8(response).expect("response should be UTF-8");
    assert!(response.starts_with("HTTP/1.1 200 OK"));
}

#[test]
fn transport_allows_ingest_jwt_signed_with_kid_secret() {
    let _guard = env_lock().lock().expect("env lock should be available");
    let _jwt_secret = EnvVarGuard::set("DASH_INGEST_JWT_HS256_SECRET", OsStr::new("active-secret"));
    let _jwt_secrets_by_kid = EnvVarGuard::set(
        "DASH_INGEST_JWT_HS256_SECRETS_BY_KID",
        OsStr::new("next:next-secret;current:current-secret"),
    );
    let _jwt_issuer = EnvVarGuard::set("DASH_INGEST_JWT_ISSUER", OsStr::new("dash"));
    let _jwt_audience = EnvVarGuard::set("DASH_INGEST_JWT_AUDIENCE", OsStr::new("ingestion"));
    let exp = now_unix_secs() + 300;
    let token = encode_hs256_token_with_kid(
        &format!(
            "{{\"tenant_id\":\"tenant-http\",\"iss\":\"dash\",\"aud\":\"ingestion\",\"exp\":{exp}}}"
        ),
        "next-secret",
        Some("next"),
    )
    .expect("token should encode");

    let runtime = sample_runtime();
    let body = r#"{"claim":{"claim_id":"claim-jwt-kid-ok","tenant_id":"tenant-http","canonical_text":"JWT kid check","confidence":0.9}}"#;
    let request = format!(
        "POST /v1/ingest HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        token,
        body.len(),
        body
    );

    let response = handle_http_request_bytes(&runtime, request.as_bytes())
        .expect("request should parse and return response");
    let response = String::from_utf8(response).expect("response should be UTF-8");
    assert!(response.starts_with("HTTP/1.1 200 OK"));
}

// ---------------------------------------------------------------------------
// Deny-by-default regressions (SEC-02, SEC-03, SEC-06, SEC-08, SEC-09,
// SEC-10). These drive the public handler with the policy built from the
// DASH_INGEST_* environment, which is what the deployment manifests set.
// ---------------------------------------------------------------------------

const STRONG_JWT_SECRET: &str = "integration-hs256-signing-key-4b8e1d7a90c2f365";
const STRONG_API_KEY: &str = "integration-api-key-7d41c8e09ab35f26";
const STRONG_REPLICATION_TOKEN: &str = "integration-replication-token-3c9e51a7";

fn status_of(raw_response: Vec<u8>) -> String {
    let text = String::from_utf8(raw_response).expect("response should be UTF-8");
    text.split_whitespace()
        .nth(1)
        .unwrap_or_default()
        .to_string()
}

fn ingest_request(extra_headers: &str) -> String {
    let body = r#"{"claim":{"claim_id":"claim-auth","tenant_id":"tenant-http","canonical_text":"Auth regression check","confidence":0.9}}"#;
    format!(
        "POST /v1/ingest HTTP/1.1\r\nHost: localhost\r\n{extra_headers}Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    )
}

fn send(runtime: &Arc<Mutex<IngestionRuntime>>, raw: &str) -> String {
    status_of(handle_http_request_bytes(runtime, raw.as_bytes()).expect("request should parse"))
}

#[test]
fn transport_ingestion_reads_dash_ingest_credentials_and_rejects_anonymous_requests() {
    let _guard = env_lock().lock().expect("env lock should be available");
    // The variable names the Helm chart and k8s manifests set.
    let _api_key = EnvVarGuard::set("DASH_INGEST_API_KEY", OsStr::new(STRONG_API_KEY));
    let runtime = sample_runtime();
    assert_eq!(send(&runtime, &ingest_request("")), "401");
    assert_eq!(
        send(
            &runtime,
            &ingest_request(&format!("X-API-Key: {STRONG_API_KEY}\r\n"))
        ),
        "200"
    );
}

#[test]
fn transport_jwt_only_config_rejects_ingest_without_a_token() {
    let _guard = env_lock().lock().expect("env lock should be available");
    let _jwt_secret = EnvVarGuard::set(
        "DASH_INGEST_JWT_HS256_SECRET",
        OsStr::new(STRONG_JWT_SECRET),
    );
    let runtime = sample_runtime();
    for headers in [
        "",
        "Authorization: Bearer not-a-jwt\r\n",
        "X-API-Key: anything\r\n",
    ] {
        assert_eq!(
            send(&runtime, &ingest_request(headers)),
            "401",
            "headers {headers:?}"
        );
    }
}

#[test]
fn transport_metrics_and_debug_require_authentication_but_probes_stay_open() {
    let _guard = env_lock().lock().expect("env lock should be available");
    let _api_key = EnvVarGuard::set("DASH_INGEST_API_KEY", OsStr::new(STRONG_API_KEY));
    let runtime = sample_runtime();
    let get = |path: &str, headers: &str| {
        send(
            &runtime,
            &format!(
                "GET {path} HTTP/1.1\r\nHost: localhost\r\n{headers}Connection: close\r\n\r\n"
            ),
        )
    };
    for path in ["/metrics", "/debug/placement", "/debug/document-parser"] {
        assert_eq!(get(path, ""), "401", "{path} anonymous");
        assert_eq!(
            get(path, &format!("X-API-Key: {STRONG_API_KEY}\r\n")),
            "200",
            "{path} authenticated"
        );
    }
    for probe in ["/live", "/health", "/ready"] {
        assert_eq!(get(probe, ""), "200", "{probe}");
    }
}

#[test]
fn transport_replication_endpoints_are_closed_without_a_token_outside_dev_mode() {
    let _guard = env_lock().lock().expect("env lock should be available");
    let _dev = EnvVarGuard::set("DASH_INSECURE_DEV_MODE", OsStr::new("0"));
    let previous_dash = std::env::var_os("DASH_INGEST_REPLICATION_TOKEN");
    let previous_eme = std::env::var_os("EME_INGEST_REPLICATION_TOKEN");
    restore_env_var_for_tests("DASH_INGEST_REPLICATION_TOKEN", None);
    restore_env_var_for_tests("EME_INGEST_REPLICATION_TOKEN", None);

    let runtime = sample_runtime();
    let export =
        "GET /internal/replication/export HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    let wal = "GET /internal/replication/wal?from_offset=0 HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    let ack = "POST /internal/replication/ack?commit_id=c&replica_id=r HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
    let results = [
        send(&runtime, export),
        send(&runtime, wal),
        send(&runtime, ack),
    ];

    restore_env_var_for_tests("DASH_INGEST_REPLICATION_TOKEN", previous_dash.as_deref());
    restore_env_var_for_tests("EME_INGEST_REPLICATION_TOKEN", previous_eme.as_deref());
    assert_eq!(results, ["403", "403", "403"]);
}

#[test]
fn transport_replication_token_must_match_exactly() {
    let _guard = env_lock().lock().expect("env lock should be available");
    let _dev = EnvVarGuard::set("DASH_INSECURE_DEV_MODE", OsStr::new("0"));
    let _token = EnvVarGuard::set(
        "DASH_INGEST_REPLICATION_TOKEN",
        OsStr::new(STRONG_REPLICATION_TOKEN),
    );
    let runtime = sample_runtime();
    let status_for = |header: &str| {
        send(
            &runtime,
            &format!(
                "GET /internal/replication/commit-status?commit_id=unknown HTTP/1.1\r\nHost: localhost\r\n{header}Connection: close\r\n\r\n"
            ),
        )
    };
    assert_eq!(status_for(""), "403");
    assert_eq!(status_for("X-Replication-Token: wrong\r\n"), "403");
    // A prefix of the real token, and the token with extra bytes, both fail.
    let prefix = &STRONG_REPLICATION_TOKEN[..STRONG_REPLICATION_TOKEN.len() - 1];
    assert_eq!(
        status_for(&format!("X-Replication-Token: {prefix}\r\n")),
        "403"
    );
    assert_eq!(
        status_for(&format!(
            "X-Replication-Token: {STRONG_REPLICATION_TOKEN}x\r\n"
        )),
        "403"
    );
    // The correct token passes authorization (unknown commit id => 404).
    assert_eq!(
        status_for(&format!(
            "X-Replication-Token: {STRONG_REPLICATION_TOKEN}\r\n"
        )),
        "404"
    );
}

#[test]
fn transport_rate_limiter_keeps_state_across_requests_and_sets_retry_after() {
    let _guard = env_lock().lock().expect("env lock should be available");
    let _api_key = EnvVarGuard::set("DASH_INGEST_API_KEY", OsStr::new(STRONG_API_KEY));
    let _rps = EnvVarGuard::set("DASH_INGEST_RATE_LIMIT_PER_TENANT_RPS", OsStr::new("1"));
    let _burst = EnvVarGuard::set("DASH_INGEST_RATE_LIMIT_BURST", OsStr::new("2"));
    let runtime = sample_runtime();
    let headers = format!("X-API-Key: {STRONG_API_KEY}\r\n");
    let mut statuses = Vec::new();
    let mut last = String::new();
    for index in 0..4 {
        let body = format!(
            r#"{{"claim":{{"claim_id":"claim-rate-{index}","tenant_id":"tenant-http","canonical_text":"Rate limit check {index}","confidence":0.9}}}}"#
        );
        let request = format!(
            "POST /v1/ingest HTTP/1.1\r\nHost: localhost\r\n{headers}Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        let response = handle_http_request_bytes(&runtime, request.as_bytes())
            .expect("request should parse and return response");
        last = String::from_utf8(response).expect("response should be UTF-8");
        statuses.push(
            last.split_whitespace()
                .nth(1)
                .unwrap_or_default()
                .to_string(),
        );
    }
    assert_eq!(statuses, ["200", "200", "429", "429"]);
    assert!(last.contains("Retry-After: "), "{last}");
}

#[test]
fn transport_ingest_authorizes_before_calling_the_embedding_provider() {
    let _guard = env_lock().lock().expect("env lock should be available");
    let _api_key = EnvVarGuard::set("DASH_INGEST_API_KEY", OsStr::new(STRONG_API_KEY));
    // An unreachable provider makes any pre-auth embedding call visible: the
    // old code answered 400 "embedding failed" before checking credentials.
    let _provider = EnvVarGuard::set("DASH_EMBEDDING_PROVIDER", OsStr::new("ollama"));
    let _endpoint = EnvVarGuard::set("DASH_OLLAMA_ENDPOINT", OsStr::new("http://127.0.0.1:1"));
    let runtime = sample_runtime();
    assert_eq!(send(&runtime, &ingest_request("")), "401");
    assert_eq!(
        send(
            &runtime,
            &ingest_request(&format!("X-API-Key: {STRONG_API_KEY}\r\n"))
        ),
        // An unreachable provider is a retryable 503 with a short code and
        // no provider detail (endpoint, error text).
        "503"
    );
    let detail = String::from_utf8(
        handle_http_request_bytes(
            &runtime,
            ingest_request(&format!("X-API-Key: {STRONG_API_KEY}\r\n")).as_bytes(),
        )
        .expect("request should parse"),
    )
    .expect("response should be UTF-8");
    assert!(detail.contains("embedding_unavailable"), "{detail}");
    assert!(!detail.contains("127.0.0.1"), "{detail}");
}
