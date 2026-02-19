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
fn transport_post_ingest_batch_rejects_commit_id_reuse_with_different_payload() {
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
        second_response.starts_with("HTTP/1.1 409"),
        "response was: {second_response}"
    );
    assert!(second_response.contains("state conflict"));
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
