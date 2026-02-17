use std::sync::{Arc, Mutex};

use ingestion::transport::{IngestionRuntime, handle_http_request_bytes};
use store::InMemoryStore;

fn sample_runtime() -> Arc<Mutex<IngestionRuntime>> {
    Arc::new(Mutex::new(
        IngestionRuntime::in_memory(InMemoryStore::new()),
    ))
}

#[test]
fn transport_post_ingest_parses_json_and_returns_response() {
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
    let runtime = sample_runtime();
    let request = b"POST /v1/ingest HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: 20000000\r\nConnection: close\r\n\r\n";
    let err = handle_http_request_bytes(&runtime, request)
        .expect_err("oversized payload should be rejected");
    assert!(err.contains("exceeds max body size"));
}
