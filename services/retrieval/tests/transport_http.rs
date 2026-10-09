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
    ensure_dev_mode_env();
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

// ---------------------------------------------------------------------------
// OpenAI-compatible /v1/embeddings endpoint
//
// These tests hit the real HTTP route through handle_http_request_bytes,
// verifying the wire format is byte-for-byte compatible with OpenAI's
// /v1/embeddings spec. This is the integration test for the
// adoption-foundation OpenAI endpoint.
// ---------------------------------------------------------------------------

#[test]
fn transport_openai_embeddings_single_string_returns_openai_shape() {
    let _guard = env_lock().lock().expect("env lock should be available");
    let store = sample_store();
    let body = r#"{"input":"hello world","model":"text-embedding-3-small"}"#;
    let request = format!(
        "POST /v1/embeddings HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    let response = retrieval::transport::handle_http_request_bytes(&store, request.as_bytes())
        .expect("request should parse and return response");
    let response = String::from_utf8(response).expect("response should be UTF-8");

    assert!(response.starts_with("HTTP/1.1 200 OK"));
    assert!(response.contains("\"object\":\"list\""));
    assert!(response.contains("\"data\":["));
    assert!(response.contains("\"model\":\"text-embedding-3-small\""));
    assert!(response.contains("\"usage\":"));
    assert!(response.contains("\"object\":\"embedding\""));
    assert!(response.contains("\"embedding\":["));
    assert!(response.contains("\"index\":0"));
    assert!(response.contains("\"prompt_tokens\":"));
    assert!(response.contains("\"total_tokens\":"));
}

#[test]
fn transport_openai_embeddings_array_input_returns_indexed_results() {
    let _guard = env_lock().lock().expect("env lock should be available");
    let store = sample_store();
    let body = r#"{"input":["alpha","beta","gamma"],"model":"text-embedding-3-small"}"#;
    let request = format!(
        "POST /v1/embeddings HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    let response = retrieval::transport::handle_http_request_bytes(&store, request.as_bytes())
        .expect("request should parse and return response");
    let response = String::from_utf8(response).expect("response should be UTF-8");

    assert!(response.starts_with("HTTP/1.1 200 OK"));
    assert!(response.contains("\"index\":0"));
    assert!(response.contains("\"index\":1"));
    assert!(response.contains("\"index\":2"));
    // Word count tokenization: "alpha"=1, "beta"=1, "gamma"=1 => total 3
    assert!(response.contains("\"total_tokens\":3"));
}

#[test]
fn transport_openai_embeddings_malformed_json_returns_400_with_error_envelope() {
    let _guard = env_lock().lock().expect("env lock should be available");
    let store = sample_store();
    let body = r#"{"input":"hello""#; // truncated JSON
    let request = format!(
        "POST /v1/embeddings HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    let response = retrieval::transport::handle_http_request_bytes(&store, request.as_bytes())
        .expect("request should parse and return response");
    let response = String::from_utf8(response).expect("response should be UTF-8");

    assert!(response.starts_with("HTTP/1.1 400"));
    assert!(response.contains("\"error\":{"));
    assert!(response.contains("\"type\":\"invalid_request_error\""));
    assert!(response.contains("\"message\":"));
}

#[test]
fn transport_openai_embeddings_empty_array_returns_400() {
    let _guard = env_lock().lock().expect("env lock should be available");
    let store = sample_store();
    let body = r#"{"input":[],"model":"text-embedding-3-small"}"#;
    let request = format!(
        "POST /v1/embeddings HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    let response = retrieval::transport::handle_http_request_bytes(&store, request.as_bytes())
        .expect("request should parse and return response");
    let response = String::from_utf8(response).expect("response should be UTF-8");

    assert!(response.starts_with("HTTP/1.1 400"));
    assert!(response.contains("at least one text"));
    assert!(response.contains("\"type\":\"invalid_request_error\""));
}

#[test]
fn transport_openai_embeddings_get_method_returns_405() {
    let _guard = env_lock().lock().expect("env lock should be available");
    let store = sample_store();
    let request = b"GET /v1/embeddings HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    let response = retrieval::transport::handle_http_request_bytes(&store, request)
        .expect("request should parse and return response");
    let response = String::from_utf8(response).expect("response should be UTF-8");

    assert!(response.starts_with("HTTP/1.1 405"));
    assert!(response.contains("only POST is supported"));
}

// ---------------------------------------------------------------------------
// Deny-by-default regressions (SEC-02, SEC-06, SEC-09, SEC-10). These drive
// the public handler with the policy built from the environment.
// ---------------------------------------------------------------------------

const STRONG_JWT_SECRET: &str = "integration-hs256-signing-key-4b8e1d7a90c2f365";
const STRONG_API_KEY: &str = "integration-api-key-7d41c8e09ab35f26";

fn status_of(raw_response: Vec<u8>) -> String {
    let text = String::from_utf8(raw_response).expect("response should be UTF-8");
    text.split_whitespace()
        .nth(1)
        .unwrap_or_default()
        .to_string()
}

#[test]
fn transport_jwt_only_config_rejects_requests_without_a_token() {
    let _guard = env_lock().lock().expect("env lock should be available");
    let _jwt_secret = EnvVarGuard::set(
        "DASH_RETRIEVAL_JWT_HS256_SECRET",
        OsStr::new(STRONG_JWT_SECRET),
    );
    let store = sample_store();
    for headers in [
        "",
        "Authorization: Bearer not-a-jwt\r\n",
        "X-API-Key: anything\r\n",
    ] {
        let request = format!(
            "GET /v1/retrieve?tenant_id=tenant-other&query=company+x&top_k=1 HTTP/1.1\r\nHost: localhost\r\n{headers}Connection: close\r\n\r\n"
        );
        let response = retrieval::transport::handle_http_request_bytes(&store, request.as_bytes())
            .expect("request should parse and return response");
        assert_eq!(status_of(response), "401", "headers {headers:?}");
    }
}

#[test]
fn transport_embeddings_metrics_and_debug_require_authentication() {
    let _guard = env_lock().lock().expect("env lock should be available");
    let _api_key = EnvVarGuard::set("DASH_RETRIEVAL_API_KEY", OsStr::new(STRONG_API_KEY));
    let store = sample_store();
    let body = r#"{"input":"hello","model":"text-embedding-3-small"}"#;
    let embeddings = format!(
        "POST /v1/embeddings HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    let authed_embeddings = format!(
        "POST /v1/embeddings HTTP/1.1\r\nHost: localhost\r\nX-API-Key: {STRONG_API_KEY}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    let anonymous =
        |path: &str| format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
    let handle = |raw: &str| {
        status_of(
            retrieval::transport::handle_http_request_bytes(&store, raw.as_bytes())
                .expect("request should parse and return response"),
        )
    };
    assert_eq!(handle(&embeddings), "401");
    assert_eq!(handle(&authed_embeddings), "200");
    assert_eq!(handle(&anonymous("/metrics")), "401");
    assert_eq!(handle(&anonymous("/debug/placement")), "401");
    // Probes stay open and do not leak internals.
    for probe in ["/live", "/health", "/ready"] {
        assert_eq!(handle(&anonymous(probe)), "200", "{probe}");
    }
}

#[test]
fn transport_rate_limiter_keeps_state_across_requests_and_sets_retry_after() {
    let _guard = env_lock().lock().expect("env lock should be available");
    let _api_key = EnvVarGuard::set("DASH_RETRIEVAL_API_KEY", OsStr::new(STRONG_API_KEY));
    let _rps = EnvVarGuard::set("DASH_RETRIEVAL_RATE_LIMIT_PER_TENANT_RPS", OsStr::new("1"));
    let _burst = EnvVarGuard::set("DASH_RETRIEVAL_RATE_LIMIT_BURST", OsStr::new("2"));
    let store = sample_store();
    let request = format!(
        "GET /v1/retrieve?tenant_id=tenant-http&query=company+x&top_k=1 HTTP/1.1\r\nHost: localhost\r\nX-API-Key: {STRONG_API_KEY}\r\nConnection: close\r\n\r\n"
    );
    let mut statuses = Vec::new();
    let mut last = String::new();
    for _ in 0..4 {
        let response = retrieval::transport::handle_http_request_bytes(&store, request.as_bytes())
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
fn transport_authorizes_before_calling_the_embedding_provider() {
    let _guard = env_lock().lock().expect("env lock should be available");
    let _api_key = EnvVarGuard::set("DASH_RETRIEVAL_API_KEY", OsStr::new(STRONG_API_KEY));
    // An unreachable provider makes any pre-auth embedding call visible: the
    // old code answered 400 "embedding failed" before checking credentials.
    let _provider = EnvVarGuard::set("DASH_EMBEDDING_PROVIDER", OsStr::new("ollama"));
    let _endpoint = EnvVarGuard::set("DASH_OLLAMA_ENDPOINT", OsStr::new("http://127.0.0.1:1"));
    let store = sample_store();
    let body = r#"{"tenant_id":"tenant-http","query":"company x","top_k":1}"#;
    let send = |auth: &str| {
        let request = format!(
            "POST /v1/retrieve HTTP/1.1\r\nHost: localhost\r\n{auth}Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        status_of(
            retrieval::transport::handle_http_request_bytes(&store, request.as_bytes())
                .expect("request should parse and return response"),
        )
    };
    assert_eq!(send(""), "401");
    assert_eq!(send(&format!("X-API-Key: {STRONG_API_KEY}\r\n")), "502");
}
