use std::{
    ffi::{OsStr, OsString},
    fs::File,
    io::Write,
    path::PathBuf,
    sync::{Mutex, OnceLock},
    time::{SystemTime, UNIX_EPOCH},
};

use auth::{encode_hs256_token, encode_hs256_token_with_kid};
use schema::{Claim, ClaimType, Evidence, Stance};
use store::InMemoryStore;

fn sample_store() -> InMemoryStore {
    let mut store = InMemoryStore::new();
    store
        .ingest_bundle(
            Claim {
                claim_id: "claim-http".into(),
                tenant_id: "tenant-http".into(),
                canonical_text: "Company X acquired Company Y".into(),
                confidence: 0.95,
                event_time_unix: Some(1_735_689_600),
                entities: vec![],
                embedding_ids: vec![],
                claim_type: Some(ClaimType::Factual),
                valid_from: Some(1_735_603_200),
                valid_to: Some(1_735_862_400),
                created_at: Some(1_735_603_200_000),
                updated_at: Some(1_735_689_600_000),
            },
            vec![Evidence {
                evidence_id: "ev-http".into(),
                claim_id: "claim-http".into(),
                source_id: "source://transport-http".into(),
                stance: Stance::Supports,
                source_quality: 0.96,
                chunk_id: None,
                span_start: None,
                span_end: None,
                doc_id: Some("doc://transport-http".into()),
                extraction_model: Some("extractor-v5".into()),
                ingested_at: Some(1_735_689_700_000),
            }],
            vec![],
        )
        .expect("sample ingest should succeed");
    store
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
        "dash-retrieve-placement-it-{}-{}.csv",
        std::process::id(),
        nanos
    ));
    let mut file = File::create(&out).expect("placement file should be created");
    file.write_all(contents.as_bytes())
        .expect("placement file should be writable");
    out
}

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be monotonic")
        .as_secs()
}

#[test]
fn transport_get_request_parses_and_returns_json_response() {
    let _guard = env_lock().lock().expect("env lock should be available");
    let store = sample_store();
    let request = b"GET /v1/retrieve?tenant_id=tenant-http&query=company+x&top_k=1 HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    let response = retrieval::transport::handle_http_request_bytes(&store, request)
        .expect("request should parse and return response");
    let response = String::from_utf8(response).expect("response should be UTF-8");

    assert!(response.starts_with("HTTP/1.1 200 OK"));
    assert!(response.contains("\"results\""));
    assert!(response.contains("\"claim_id\":\"claim-http\""));
    assert!(response.contains("\"evidence_id\":\"ev-http\""));
    assert!(response.contains("\"source_id\":\"source://transport-http\""));
    assert!(response.contains("\"claim_confidence\":0.950000"));
    assert!(response.contains("\"confidence_band\":\"high\""));
    assert!(response.contains("\"dominant_stance\":\"supports\""));
    assert!(response.contains("\"contradiction_risk\":0.000000"));
    assert!(response.contains("\"doc_id\":\"doc://transport-http\""));
    assert!(response.contains("\"extraction_model\":\"extractor-v5\""));
    assert!(response.contains("\"ingested_at\":1735689700000"));
    assert!(response.contains("\"event_time_unix\":1735689600"));
    assert!(response.contains("\"temporal_match_mode\":null"));
    assert!(response.contains("\"temporal_in_range\":null"));
    assert!(response.contains("\"claim_type\":\"factual\""));
    assert!(response.contains("\"valid_from\":1735603200"));
    assert!(response.contains("\"valid_to\":1735862400"));
    assert!(response.contains("\"created_at\":1735603200000"));
    assert!(response.contains("\"updated_at\":1735689600000"));
}

#[test]
fn transport_post_request_parses_json_body_and_returns_json_response() {
    let _guard = env_lock().lock().expect("env lock should be available");
    let store = sample_store();
    let body = "{\"tenant_id\":\"tenant-http\",\"query\":\"company x\",\"top_k\":1,\"stance_mode\":\"balanced\",\"return_graph\":false}";
    let request = format!(
        "POST /v1/retrieve HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    let response = retrieval::transport::handle_http_request_bytes(&store, request.as_bytes())
        .expect("request should parse and return response");
    let response = String::from_utf8(response).expect("response should be UTF-8");

    assert!(response.starts_with("HTTP/1.1 200 OK"));
    assert!(response.contains("\"results\""));
    assert!(response.contains("\"claim_id\":\"claim-http\""));
    assert!(response.contains("\"evidence_id\":\"ev-http\""));
    assert!(response.contains("\"claim_confidence\":0.950000"));
    assert!(response.contains("\"confidence_band\":\"high\""));
    assert!(response.contains("\"dominant_stance\":\"supports\""));
    assert!(response.contains("\"contradiction_risk\":0.000000"));
    assert!(response.contains("\"doc_id\":\"doc://transport-http\""));
    assert!(response.contains("\"claim_type\":\"factual\""));
    assert!(response.contains("\"event_time_unix\":1735689600"));
    assert!(response.contains("\"temporal_match_mode\":null"));
    assert!(response.contains("\"temporal_in_range\":null"));
}

#[test]
fn transport_metrics_endpoint_returns_prometheus_payload() {
    let _guard = env_lock().lock().expect("env lock should be available");
    let store = sample_store();
    let request = b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    let response = retrieval::transport::handle_http_request_bytes(&store, request)
        .expect("request should parse and return response");
    let response = String::from_utf8(response).expect("response should be UTF-8");

    assert!(response.starts_with("HTTP/1.1 200 OK"));
    assert!(response.contains("Content-Type: text/plain; version=0.0.4; charset=utf-8"));
    assert!(response.contains("dash_http_requests_total"));
}

#[test]
fn transport_rejects_oversized_body_via_content_length_guard() {
    let _guard = env_lock().lock().expect("env lock should be available");
    let store = sample_store();
    let request = b"POST /v1/retrieve HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: 20000000\r\nConnection: close\r\n\r\n";
    let err = retrieval::transport::handle_http_request_bytes(&store, request)
        .expect_err("oversized payload should be rejected");
    assert!(err.contains("exceeds max body size"));
}

#[test]
fn transport_loads_placement_routing_from_env_csv() {
    let _guard = env_lock().lock().expect("env lock should be available");
    let placement_file = temp_placement_csv(
        "tenant-http,0,9,node-a,leader,healthy\n\
tenant-http,0,9,node-b,follower,healthy\n",
    );
    let _placement_env = EnvVarGuard::set("DASH_ROUTER_PLACEMENT_FILE", placement_file.as_os_str());
    let _local_node_env = EnvVarGuard::set("DASH_ROUTER_LOCAL_NODE_ID", OsStr::new("node-b"));
    let _read_pref_env = EnvVarGuard::set("DASH_ROUTER_READ_PREFERENCE", OsStr::new("leader_only"));

    let store = sample_store();
    let retrieve_request = b"GET /v1/retrieve?tenant_id=tenant-http&query=company+x&top_k=1 HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    let retrieve_response =
        retrieval::transport::handle_http_request_bytes(&store, retrieve_request)
            .expect("request should parse and return response");
    let retrieve_response = String::from_utf8(retrieve_response).expect("response should be UTF-8");
    assert!(retrieve_response.starts_with("HTTP/1.1 503"));

    let debug_request = b"GET /debug/placement?tenant_id=tenant-http&entity_key=company-x HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    let debug_response = retrieval::transport::handle_http_request_bytes(&store, debug_request)
        .expect("debug request should parse and return response");
    let debug_response = String::from_utf8(debug_response).expect("response should be UTF-8");
    assert!(debug_response.starts_with("HTTP/1.1 200 OK"));
    assert!(debug_response.contains("\"enabled\":true"));
    assert!(debug_response.contains("\"local_node_id\":\"node-b\""));
    assert!(debug_response.contains("\"target_node_id\":\"node-a\""));
    assert!(debug_response.contains("\"read_preference\":\"leader_only\""));

    let _ = std::fs::remove_file(placement_file);
}

#[test]
fn transport_denies_cross_tenant_retrieval_for_scoped_key() {
    let _guard = env_lock().lock().expect("env lock should be available");
    let _scope_env = EnvVarGuard::set(
        "DASH_RETRIEVAL_API_KEY_SCOPES",
        OsStr::new("scope-a:tenant-http"),
    );
    let store = sample_store();
    let request = b"GET /v1/retrieve?tenant_id=tenant-blocked&query=company+x&top_k=1 HTTP/1.1\r\nHost: localhost\r\nX-API-Key: scope-a\r\nConnection: close\r\n\r\n";
    let response = retrieval::transport::handle_http_request_bytes(&store, request)
        .expect("request should parse and return response");
    let response = String::from_utf8(response).expect("response should be UTF-8");
    assert!(response.starts_with("HTTP/1.1 403"));
    assert!(response.contains("tenant is not allowed for this API key"));
}

#[test]
fn transport_denies_revoked_retrieval_key() {
    let _guard = env_lock().lock().expect("env lock should be available");
    let _scope_env = EnvVarGuard::set(
        "DASH_RETRIEVAL_API_KEY_SCOPES",
        OsStr::new("scope-a:tenant-http"),
    );
    let _revoked_env = EnvVarGuard::set("DASH_RETRIEVAL_REVOKED_API_KEYS", OsStr::new("scope-a"));
    let store = sample_store();
    let request = b"GET /v1/retrieve?tenant_id=tenant-http&query=company+x&top_k=1 HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer scope-a\r\nConnection: close\r\n\r\n";
    let response = retrieval::transport::handle_http_request_bytes(&store, request)
        .expect("request should parse and return response");
    let response = String::from_utf8(response).expect("response should be UTF-8");
    assert!(response.starts_with("HTTP/1.1 401"));
    assert!(response.contains("API key revoked"));
}

#[test]
fn transport_denies_cross_tenant_retrieval_for_jwt_claim_scope() {
    let _guard = env_lock().lock().expect("env lock should be available");
    let _jwt_secret = EnvVarGuard::set("DASH_RETRIEVAL_JWT_HS256_SECRET", OsStr::new("jwt-secret"));
    let _jwt_issuer = EnvVarGuard::set("DASH_RETRIEVAL_JWT_ISSUER", OsStr::new("dash"));
    let _jwt_audience = EnvVarGuard::set("DASH_RETRIEVAL_JWT_AUDIENCE", OsStr::new("retrieval"));
    let exp = now_unix_secs() + 300;
    let token = encode_hs256_token(
        &format!(
            "{{\"tenant_id\":\"tenant-http\",\"iss\":\"dash\",\"aud\":\"retrieval\",\"exp\":{exp}}}"
        ),
        "jwt-secret",
    )
    .expect("token should encode");

    let store = sample_store();
    let request = format!(
        "GET /v1/retrieve?tenant_id=tenant-blocked&query=company+x&top_k=1 HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {}\r\nConnection: close\r\n\r\n",
        token
    );
    let response = retrieval::transport::handle_http_request_bytes(&store, request.as_bytes())
        .expect("request should parse and return response");
    let response = String::from_utf8(response).expect("response should be UTF-8");
    assert!(response.starts_with("HTTP/1.1 403"));
    assert!(response.contains("tenant is not allowed for this JWT"));
}

#[test]
fn transport_denies_expired_retrieval_jwt() {
    let _guard = env_lock().lock().expect("env lock should be available");
    let _jwt_secret = EnvVarGuard::set("DASH_RETRIEVAL_JWT_HS256_SECRET", OsStr::new("jwt-secret"));
    let _jwt_issuer = EnvVarGuard::set("DASH_RETRIEVAL_JWT_ISSUER", OsStr::new("dash"));
    let _jwt_audience = EnvVarGuard::set("DASH_RETRIEVAL_JWT_AUDIENCE", OsStr::new("retrieval"));
    let exp = now_unix_secs().saturating_sub(10);
    let token = encode_hs256_token(
        &format!(
            "{{\"tenant_id\":\"tenant-http\",\"iss\":\"dash\",\"aud\":\"retrieval\",\"exp\":{exp}}}"
        ),
        "jwt-secret",
    )
    .expect("token should encode");

    let store = sample_store();
    let request = format!(
        "GET /v1/retrieve?tenant_id=tenant-http&query=company+x&top_k=1 HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {}\r\nConnection: close\r\n\r\n",
        token
    );
    let response = retrieval::transport::handle_http_request_bytes(&store, request.as_bytes())
        .expect("request should parse and return response");
    let response = String::from_utf8(response).expect("response should be UTF-8");
    assert!(response.starts_with("HTTP/1.1 401"));
    assert!(response.contains("JWT expired"));
}

#[test]
fn transport_allows_retrieval_jwt_signed_with_rotation_fallback_secret() {
    let _guard = env_lock().lock().expect("env lock should be available");
    let _jwt_secret = EnvVarGuard::set(
        "DASH_RETRIEVAL_JWT_HS256_SECRET",
        OsStr::new("active-secret"),
    );
    let _jwt_secrets = EnvVarGuard::set(
        "DASH_RETRIEVAL_JWT_HS256_SECRETS",
        OsStr::new("previous-secret"),
    );
    let _jwt_issuer = EnvVarGuard::set("DASH_RETRIEVAL_JWT_ISSUER", OsStr::new("dash"));
    let _jwt_audience = EnvVarGuard::set("DASH_RETRIEVAL_JWT_AUDIENCE", OsStr::new("retrieval"));
    let exp = now_unix_secs() + 300;
    let token = encode_hs256_token(
        &format!(
            "{{\"tenant_id\":\"tenant-http\",\"iss\":\"dash\",\"aud\":\"retrieval\",\"exp\":{exp}}}"
        ),
        "previous-secret",
    )
    .expect("token should encode");

    let store = sample_store();
    let request = format!(
        "GET /v1/retrieve?tenant_id=tenant-http&query=company+x&top_k=1 HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {}\r\nConnection: close\r\n\r\n",
        token
    );
    let response = retrieval::transport::handle_http_request_bytes(&store, request.as_bytes())
        .expect("request should parse and return response");
    let response = String::from_utf8(response).expect("response should be UTF-8");
    assert!(response.starts_with("HTTP/1.1 200 OK"));
}

#[test]
fn transport_allows_retrieval_jwt_signed_with_kid_secret() {
    let _guard = env_lock().lock().expect("env lock should be available");
    let _jwt_secret = EnvVarGuard::set(
        "DASH_RETRIEVAL_JWT_HS256_SECRET",
        OsStr::new("active-secret"),
    );
    let _jwt_secrets_by_kid = EnvVarGuard::set(
        "DASH_RETRIEVAL_JWT_HS256_SECRETS_BY_KID",
        OsStr::new("next:next-secret;current:current-secret"),
    );
    let _jwt_issuer = EnvVarGuard::set("DASH_RETRIEVAL_JWT_ISSUER", OsStr::new("dash"));
    let _jwt_audience = EnvVarGuard::set("DASH_RETRIEVAL_JWT_AUDIENCE", OsStr::new("retrieval"));
    let exp = now_unix_secs() + 300;
    let token = encode_hs256_token_with_kid(
        &format!(
            "{{\"tenant_id\":\"tenant-http\",\"iss\":\"dash\",\"aud\":\"retrieval\",\"exp\":{exp}}}"
        ),
        "next-secret",
        Some("next"),
    )
    .expect("token should encode");

    let store = sample_store();
    let request = format!(
        "GET /v1/retrieve?tenant_id=tenant-http&query=company+x&top_k=1 HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {}\r\nConnection: close\r\n\r\n",
        token
    );
    let response = retrieval::transport::handle_http_request_bytes(&store, request.as_bytes())
        .expect("request should parse and return response");
    let response = String::from_utf8(response).expect("response should be UTF-8");
    assert!(response.starts_with("HTTP/1.1 200 OK"));
}
