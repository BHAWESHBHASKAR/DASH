use schema::{Claim, Evidence, Stance};
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
                event_time_unix: None,
                entities: vec![],
                embedding_ids: vec![],
                claim_type: None,
                valid_from: None,
                valid_to: None,
                created_at: None,
                updated_at: None,
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
                doc_id: None,
                extraction_model: None,
                ingested_at: None,
            }],
            vec![],
        )
        .expect("sample ingest should succeed");
    store
}

#[test]
fn transport_get_request_parses_and_returns_json_response() {
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
}

#[test]
fn transport_post_request_parses_json_body_and_returns_json_response() {
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
}

#[test]
fn transport_metrics_endpoint_returns_prometheus_payload() {
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
    let store = sample_store();
    let request = b"POST /v1/retrieve HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: 20000000\r\nConnection: close\r\n\r\n";
    let err = retrieval::transport::handle_http_request_bytes(&store, request)
        .expect_err("oversized payload should be rejected");
    assert!(err.contains("exceeds max body size"));
}
