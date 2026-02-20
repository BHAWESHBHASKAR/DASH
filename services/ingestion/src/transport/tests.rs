use super::*;
use indexer::{CompactionSchedulerConfig, Segment, Tier, persist_segments_atomic};
use metadata_router::{ReplicaHealth, ReplicaPlacement, ReplicaRole, promote_replica_to_leader};
use std::io::Read;
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

fn sample_runtime() -> SharedRuntime {
    Arc::new(Mutex::new(
        IngestionRuntime::in_memory(InMemoryStore::new()),
    ))
}

fn temp_wal_path() -> PathBuf {
    let mut wal_path = std::env::temp_dir();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be monotonic")
        .as_nanos();
    wal_path.push(format!(
        "dash-ingest-runtime-wal-{}-{}.log",
        std::process::id(),
        nanos
    ));
    wal_path
}

fn env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

#[allow(unused_unsafe)]
fn set_env_var_for_tests(key: &str, value: &str) {
    unsafe {
        std::env::set_var(key, value);
    }
}

#[allow(unused_unsafe)]
fn restore_env_var_for_tests(key: &str, value: Option<&std::ffi::OsStr>) {
    match value {
        Some(value) => unsafe {
            std::env::set_var(key, value);
        },
        None => unsafe {
            std::env::remove_var(key);
        },
    }
}

#[test]
fn build_ingest_request_from_json_accepts_contract_payload() {
    let body = r#"{
            "claim": {
                "claim_id": "c1",
                "tenant_id": "tenant-a",
                "canonical_text": "Company X acquired Company Y",
                "confidence": 0.9,
                "event_time_unix": 100
            },
            "evidence": [
                {
                    "evidence_id": "e1",
                    "claim_id": "c1",
                    "source_id": "source://doc",
                    "stance": "supports",
                    "source_quality": 0.95
                }
            ],
            "edges": [
                {
                    "edge_id": "g1",
                    "from_claim_id": "c1",
                    "to_claim_id": "c2",
                    "relation": "supports",
                    "strength": 0.8
                }
            ]
        }"#;

    let req = build_ingest_request_from_json(body).unwrap();
    assert_eq!(req.claim.claim_id, "c1");
    assert!(req.claim.entities.is_empty());
    assert!(req.claim.embedding_ids.is_empty());
    assert_eq!(req.evidence.len(), 1);
    assert_eq!(req.evidence[0].chunk_id, None);
    assert_eq!(req.evidence[0].span_start, None);
    assert_eq!(req.evidence[0].span_end, None);
    assert_eq!(req.edges.len(), 1);
}

#[test]
fn build_ingest_request_from_json_accepts_metadata_fields() {
    let body = r#"{
            "claim": {
                "claim_id": "c1",
                "tenant_id": "tenant-a",
                "canonical_text": "Company X acquired Company Y",
                "confidence": 0.9,
                "entities": ["Company X", "Company Y"],
                "embedding_ids": ["emb://1"]
            },
            "evidence": [
                {
                    "evidence_id": "e1",
                    "claim_id": "c1",
                    "source_id": "source://doc",
                    "stance": "supports",
                    "source_quality": 0.95,
                    "chunk_id": "chunk-10",
                    "span_start": 12,
                    "span_end": 48
                }
            ]
        }"#;

    let req = build_ingest_request_from_json(body).unwrap();
    assert_eq!(req.claim.entities, vec!["Company X", "Company Y"]);
    assert_eq!(req.claim.embedding_ids, vec!["emb://1"]);
    assert_eq!(req.evidence[0].chunk_id.as_deref(), Some("chunk-10"));
    assert_eq!(req.evidence[0].span_start, Some(12));
    assert_eq!(req.evidence[0].span_end, Some(48));
}

#[test]
fn build_ingest_request_from_json_accepts_temporal_claim_fields() {
    let body = r#"{
            "claim": {
                "claim_id": "c1",
                "tenant_id": "tenant-a",
                "canonical_text": "Temporal claim payload",
                "confidence": 0.9,
                "claim_type": "temporal",
                "valid_from": 120,
                "valid_to": 240,
                "created_at": 1771620000000,
                "updated_at": 1771620001000
            }
        }"#;

    let req = build_ingest_request_from_json(body).unwrap();
    assert_eq!(req.claim.claim_type, Some(schema::ClaimType::Temporal));
    assert_eq!(req.claim.valid_from, Some(120));
    assert_eq!(req.claim.valid_to, Some(240));
    assert_eq!(req.claim.created_at, Some(1_771_620_000_000));
    assert_eq!(req.claim.updated_at, Some(1_771_620_001_000));
}

#[test]
fn build_ingest_request_from_json_rejects_invalid_claim_type() {
    let body = r#"{
            "claim": {
                "claim_id": "c1",
                "tenant_id": "tenant-a",
                "canonical_text": "Temporal claim payload",
                "confidence": 0.9,
                "claim_type": "invalid"
            }
        }"#;

    let err = build_ingest_request_from_json(body).unwrap_err();
    assert!(err.contains("claim.claim_type"));
}

#[test]
fn build_ingest_request_from_json_rejects_invalid_relation() {
    let body = r#"{
            "claim": {
                "claim_id": "c1",
                "tenant_id": "tenant-a",
                "canonical_text": "Company X acquired Company Y",
                "confidence": 0.9
            },
            "edges": [
                {
                    "edge_id": "g1",
                    "from_claim_id": "c1",
                    "to_claim_id": "c2",
                    "relation": "invalid",
                    "strength": 0.8
                }
            ]
        }"#;

    let err = build_ingest_request_from_json(body).unwrap_err();
    assert!(err.contains("edge.relation"));
}

#[test]
fn build_ingest_batch_request_from_json_accepts_items_and_commit_id() {
    let body = r#"{
            "commit_id": "commit-test-1",
            "items": [
                {
                    "claim": {
                        "claim_id": "c1",
                        "tenant_id": "tenant-a",
                        "canonical_text": "Company X acquired Company Y",
                        "confidence": 0.9
                    }
                },
                {
                    "claim": {
                        "claim_id": "c2",
                        "tenant_id": "tenant-a",
                        "canonical_text": "Company X expanded into region Z",
                        "confidence": 0.88
                    }
                }
            ]
        }"#;

    let req = build_ingest_batch_request_from_json(body, 8).expect("batch parse should work");
    assert_eq!(req.commit_id.as_deref(), Some("commit-test-1"));
    assert_eq!(req.items.len(), 2);
    assert_eq!(req.items[0].claim.claim_id, "c1");
    assert_eq!(req.items[1].claim.claim_id, "c2");
}

#[test]
fn build_ingest_batch_request_from_json_rejects_cross_tenant_batch() {
    let body = r#"{
            "items": [
                {
                    "claim": {
                        "claim_id": "c1",
                        "tenant_id": "tenant-a",
                        "canonical_text": "A",
                        "confidence": 0.9
                    }
                },
                {
                    "claim": {
                        "claim_id": "c2",
                        "tenant_id": "tenant-b",
                        "canonical_text": "B",
                        "confidence": 0.8
                    }
                }
            ]
        }"#;
    let err = build_ingest_batch_request_from_json(body, 16).expect_err("batch should fail");
    assert!(err.contains("same claim.tenant_id"));
}

#[test]
fn handle_request_post_ingests_claim() {
    let runtime = sample_runtime();
    let request = HttpRequest {
            method: "POST".to_string(),
            target: "/v1/ingest".to_string(),
            headers: HashMap::from([("content-type".to_string(), "application/json".to_string())]),
            body: br#"{"claim":{"claim_id":"c1","tenant_id":"tenant-a","canonical_text":"Company X acquired Company Y","confidence":0.9},"evidence":[{"evidence_id":"e1","claim_id":"c1","source_id":"source://doc","stance":"supports","source_quality":0.95}]}"#.to_vec(),
        };

    let response = handle_request(&runtime, &request);
    assert_eq!(response.status, 200);
    assert!(response.body.contains("\"ingested_claim_id\":\"c1\""));
    assert!(response.body.contains("\"claims_total\":1"));
}

#[test]
fn handle_request_post_batch_ingests_claims_and_returns_commit_metadata() {
    let runtime = sample_runtime();
    let request = HttpRequest {
        method: "POST".to_string(),
        target: "/v1/ingest/batch".to_string(),
        headers: HashMap::from([("content-type".to_string(), "application/json".to_string())]),
        body: br#"{
                "items": [
                    {
                        "claim": {
                            "claim_id": "c-b1",
                            "tenant_id": "tenant-a",
                            "canonical_text": "Batch item one",
                            "confidence": 0.91
                        }
                    },
                    {
                        "claim": {
                            "claim_id": "c-b2",
                            "tenant_id": "tenant-a",
                            "canonical_text": "Batch item two",
                            "confidence": 0.89
                        }
                    }
                ]
            }"#
        .to_vec(),
    };

    let response = handle_request(&runtime, &request);
    assert_eq!(response.status, 200);
    assert!(response.body.contains("\"commit_id\":\"commit-"));
    assert!(response.body.contains("\"idempotent_replay\":false"));
    assert!(response.body.contains("\"batch_size\":2"));
    assert!(
        response
            .body
            .contains("\"ingested_claim_ids\":[\"c-b1\",\"c-b2\"]")
    );
    assert!(response.body.contains("\"claims_total\":2"));

    let metrics_request = HttpRequest {
        method: "GET".to_string(),
        target: "/metrics".to_string(),
        headers: HashMap::new(),
        body: Vec::new(),
    };
    let metrics_response = handle_request(&runtime, &metrics_request);
    assert_eq!(metrics_response.status, 200);
    assert!(
        metrics_response
            .body
            .contains("dash_ingest_batch_success_total 1")
    );
    assert!(
        metrics_response
            .body
            .contains("dash_ingest_batch_last_size 2")
    );
    assert!(
        metrics_response
            .body
            .contains("dash_ingest_batch_idempotent_hit_total 0")
    );
}

#[test]
fn handle_request_post_batch_replays_idempotently_for_same_commit_id() {
    let runtime = sample_runtime();
    let body = br#"{
            "commit_id": "commit-idem-1",
            "items": [
                {
                    "claim": {
                        "claim_id": "c-idem-1",
                        "tenant_id": "tenant-a",
                        "canonical_text": "Idempotent batch one",
                        "confidence": 0.91
                    }
                },
                {
                    "claim": {
                        "claim_id": "c-idem-2",
                        "tenant_id": "tenant-a",
                        "canonical_text": "Idempotent batch two",
                        "confidence": 0.89
                    }
                }
            ]
        }"#
    .to_vec();
    let request = HttpRequest {
        method: "POST".to_string(),
        target: "/v1/ingest/batch".to_string(),
        headers: HashMap::from([("content-type".to_string(), "application/json".to_string())]),
        body: body.clone(),
    };
    let first = handle_request(&runtime, &request);
    assert_eq!(first.status, 200);
    assert!(first.body.contains("\"idempotent_replay\":false"));
    assert!(first.body.contains("\"claims_total\":2"));

    let second = handle_request(&runtime, &HttpRequest { body, ..request });
    assert_eq!(second.status, 200);
    assert!(second.body.contains("\"idempotent_replay\":true"));
    assert!(second.body.contains("\"claims_total\":2"));

    let metrics_request = HttpRequest {
        method: "GET".to_string(),
        target: "/metrics".to_string(),
        headers: HashMap::new(),
        body: Vec::new(),
    };
    let metrics_response = handle_request(&runtime, &metrics_request);
    assert_eq!(metrics_response.status, 200);
    assert!(
        metrics_response
            .body
            .contains("dash_ingest_batch_success_total 2")
    );
    assert!(
        metrics_response
            .body
            .contains("dash_ingest_batch_idempotent_hit_total 1")
    );
    assert!(
        metrics_response
            .body
            .contains("dash_ingest_success_total 2")
    );
}

#[test]
fn handle_request_post_batch_rejects_commit_id_reuse_with_different_payload() {
    let runtime = sample_runtime();
    let first_request = HttpRequest {
        method: "POST".to_string(),
        target: "/v1/ingest/batch".to_string(),
        headers: HashMap::from([("content-type".to_string(), "application/json".to_string())]),
        body: br#"{
                "commit_id": "commit-conflict-1",
                "items": [
                    {
                        "claim": {
                            "claim_id": "c-conflict-1",
                            "tenant_id": "tenant-a",
                            "canonical_text": "Commit conflict one",
                            "confidence": 0.91
                        }
                    }
                ]
            }"#
        .to_vec(),
    };
    let first = handle_request(&runtime, &first_request);
    assert_eq!(first.status, 200);
    assert!(first.body.contains("\"idempotent_replay\":false"));

    let second_request = HttpRequest {
        method: "POST".to_string(),
        target: "/v1/ingest/batch".to_string(),
        headers: HashMap::from([("content-type".to_string(), "application/json".to_string())]),
        body: br#"{
                "commit_id": "commit-conflict-1",
                "items": [
                    {
                        "claim": {
                            "claim_id": "c-conflict-2",
                            "tenant_id": "tenant-a",
                            "canonical_text": "Commit conflict two",
                            "confidence": 0.89
                        }
                    }
                ]
            }"#
        .to_vec(),
    };
    let second = handle_request(&runtime, &second_request);
    assert_eq!(second.status, 409);
    assert!(second.body.contains("state conflict"));

    let metrics_request = HttpRequest {
        method: "GET".to_string(),
        target: "/metrics".to_string(),
        headers: HashMap::new(),
        body: Vec::new(),
    };
    let metrics_response = handle_request(&runtime, &metrics_request);
    assert_eq!(metrics_response.status, 200);
    assert!(
        metrics_response
            .body
            .contains("dash_ingest_batch_failed_total 1")
    );
    assert!(
        metrics_response
            .body
            .contains("dash_ingest_batch_idempotent_hit_total 0")
    );
}

#[test]
fn handle_request_post_batch_is_atomic_on_validation_failure() {
    let wal_path = temp_wal_path();
    let wal = FileWal::open(&wal_path).expect("wal should open");
    let runtime = Arc::new(Mutex::new(IngestionRuntime::persistent(
        InMemoryStore::new(),
        wal,
        CheckpointPolicy::default(),
    )));

    let request = HttpRequest {
        method: "POST".to_string(),
        target: "/v1/ingest/batch".to_string(),
        headers: HashMap::from([("content-type".to_string(), "application/json".to_string())]),
        body: br#"{
                "items": [
                    {
                        "claim": {
                            "claim_id": "c-a1",
                            "tenant_id": "tenant-a",
                            "canonical_text": "Batch atomic item one",
                            "confidence": 0.91,
                            "embedding_vector": [0.1, 0.2, 0.3, 0.4]
                        }
                    },
                    {
                        "claim": {
                            "claim_id": "c-a2",
                            "tenant_id": "tenant-a",
                            "canonical_text": "Batch atomic item two",
                            "confidence": 0.89,
                            "embedding_vector": [0.1, 0.2]
                        }
                    }
                ]
            }"#
        .to_vec(),
    };

    let response = handle_request(&runtime, &request);
    assert_eq!(response.status, 400);
    assert!(response.body.contains("invalid vector"));

    let guard = runtime.lock().expect("runtime lock should be available");
    assert_eq!(guard.claims_len(), 0);
    let wal_records = guard
        .wal
        .as_ref()
        .expect("persistent runtime should have wal")
        .wal_record_count()
        .expect("wal record count should be readable");
    assert_eq!(wal_records, 0);
    drop(guard);

    let _ = std::fs::remove_file(&wal_path);
    let mut snapshot_path = wal_path.clone().into_os_string();
    snapshot_path.push(".snapshot");
    let _ = std::fs::remove_file(PathBuf::from(snapshot_path));
}

#[test]
fn handle_request_internal_replication_wal_returns_delta_payload() {
    let _guard = env_lock().lock().expect("env lock should be available");
    let previous_dash_token = std::env::var_os("DASH_INGEST_REPLICATION_TOKEN");
    let previous_eme_token = std::env::var_os("EME_INGEST_REPLICATION_TOKEN");
    restore_env_var_for_tests("DASH_INGEST_REPLICATION_TOKEN", None);
    restore_env_var_for_tests("EME_INGEST_REPLICATION_TOKEN", None);

    let wal_path = temp_wal_path();
    let wal = FileWal::open(&wal_path).expect("wal should open");
    let runtime = Arc::new(Mutex::new(IngestionRuntime::persistent(
        InMemoryStore::new(),
        wal,
        CheckpointPolicy::default(),
    )));
    let ingest_request = HttpRequest {
            method: "POST".to_string(),
            target: "/v1/ingest".to_string(),
            headers: HashMap::from([("content-type".to_string(), "application/json".to_string())]),
            body: br#"{"claim":{"claim_id":"c-repl-1","tenant_id":"tenant-a","canonical_text":"replication source claim","confidence":0.9}}"#.to_vec(),
        };
    let ingest_response = handle_request(&runtime, &ingest_request);
    assert_eq!(ingest_response.status, 200);

    let pull_request = HttpRequest {
        method: "GET".to_string(),
        target: "/internal/replication/wal?from_offset=0&max_records=10".to_string(),
        headers: HashMap::new(),
        body: Vec::new(),
    };
    let pull_response = handle_request(&runtime, &pull_request);
    assert_eq!(pull_response.status, 200);
    assert!(pull_response.body.contains("status=ok"));
    assert!(pull_response.body.contains("records=1"));
    assert!(pull_response.body.contains("next_offset=1"));

    let guard = runtime.lock().expect("runtime lock should be available");
    let _ = std::fs::remove_file(
        guard
            .wal
            .as_ref()
            .expect("persistent runtime should have wal")
            .path(),
    );
    let _ = std::fs::remove_file(
        guard
            .wal
            .as_ref()
            .expect("persistent runtime should have wal")
            .snapshot_path(),
    );
    restore_env_var_for_tests(
        "DASH_INGEST_REPLICATION_TOKEN",
        previous_dash_token.as_deref(),
    );
    restore_env_var_for_tests(
        "EME_INGEST_REPLICATION_TOKEN",
        previous_eme_token.as_deref(),
    );
}

#[test]
fn handle_request_internal_replication_endpoints_require_token_when_configured() {
    let _guard = env_lock().lock().expect("env lock should be available");
    let previous = std::env::var_os("DASH_INGEST_REPLICATION_TOKEN");
    set_env_var_for_tests("DASH_INGEST_REPLICATION_TOKEN", "token-a");

    let wal_path = temp_wal_path();
    let wal = FileWal::open(&wal_path).expect("wal should open");
    let runtime = Arc::new(Mutex::new(IngestionRuntime::persistent(
        InMemoryStore::new(),
        wal,
        CheckpointPolicy::default(),
    )));
    let unauthorized = HttpRequest {
        method: "GET".to_string(),
        target: "/internal/replication/wal?from_offset=0".to_string(),
        headers: HashMap::new(),
        body: Vec::new(),
    };
    let unauthorized_response = handle_request(&runtime, &unauthorized);
    assert_eq!(unauthorized_response.status, 403);

    let authorized = HttpRequest {
        method: "GET".to_string(),
        target: "/internal/replication/export".to_string(),
        headers: HashMap::from([("x-replication-token".to_string(), "token-a".to_string())]),
        body: Vec::new(),
    };
    let authorized_response = handle_request(&runtime, &authorized);
    assert_eq!(authorized_response.status, 200);
    assert!(authorized_response.body.contains("status=ok"));

    restore_env_var_for_tests("DASH_INGEST_REPLICATION_TOKEN", previous.as_deref());
    let guard = runtime.lock().expect("runtime lock should be available");
    let _ = std::fs::remove_file(
        guard
            .wal
            .as_ref()
            .expect("persistent runtime should have wal")
            .path(),
    );
    let _ = std::fs::remove_file(
        guard
            .wal
            .as_ref()
            .expect("persistent runtime should have wal")
            .snapshot_path(),
    );
}

#[test]
fn handle_request_post_rejects_invalid_content_type() {
    let runtime = sample_runtime();
    let request = HttpRequest {
        method: "POST".to_string(),
        target: "/v1/ingest".to_string(),
        headers: HashMap::from([("content-type".to_string(), "text/plain".to_string())]),
        body: b"{}".to_vec(),
    };

    let response = handle_request(&runtime, &request);
    assert_eq!(response.status, 400);
    assert!(response.body.contains("application/json"));
}

#[test]
fn auth_policy_scoped_key_allows_configured_tenant() {
    let request = HttpRequest {
        method: "POST".to_string(),
        target: "/v1/ingest".to_string(),
        headers: HashMap::from([("x-api-key".to_string(), "scope-a".to_string())]),
        body: Vec::new(),
    };
    let policy = AuthPolicy::from_env(
        None,
        None,
        None,
        None,
        Some("scope-a:tenant-a,tenant-b".to_string()),
    );
    assert_eq!(
        authorize_request_for_tenant(&request, "tenant-b", &policy),
        AuthDecision::Allowed
    );
}

#[test]
fn auth_policy_scoped_key_rejects_other_tenants() {
    let request = HttpRequest {
        method: "POST".to_string(),
        target: "/v1/ingest".to_string(),
        headers: HashMap::from([("authorization".to_string(), "Bearer scope-a".to_string())]),
        body: Vec::new(),
    };
    let policy = AuthPolicy::from_env(None, None, None, None, Some("scope-a:tenant-a".to_string()));
    assert_eq!(
        authorize_request_for_tenant(&request, "tenant-z", &policy),
        AuthDecision::Forbidden("tenant is not allowed for this API key")
    );
}

#[test]
fn auth_policy_required_key_rejects_missing_key() {
    let request = HttpRequest {
        method: "POST".to_string(),
        target: "/v1/ingest".to_string(),
        headers: HashMap::new(),
        body: Vec::new(),
    };
    let policy = AuthPolicy::from_env(Some("secret".to_string()), None, None, None, None);
    assert_eq!(
        authorize_request_for_tenant(&request, "tenant-a", &policy),
        AuthDecision::Unauthorized("missing or invalid API key")
    );
}

#[test]
fn auth_policy_required_key_set_supports_rotation() {
    let request = HttpRequest {
        method: "POST".to_string(),
        target: "/v1/ingest".to_string(),
        headers: HashMap::from([("x-api-key".to_string(), "new-key".to_string())]),
        body: Vec::new(),
    };
    let policy = AuthPolicy::from_env(
        Some("old-key".to_string()),
        Some("new-key,old-key-2".to_string()),
        None,
        None,
        None,
    );
    assert_eq!(
        authorize_request_for_tenant(&request, "tenant-a", &policy),
        AuthDecision::Allowed
    );
}

#[test]
fn auth_policy_revoked_key_is_denied() {
    let request = HttpRequest {
        method: "POST".to_string(),
        target: "/v1/ingest".to_string(),
        headers: HashMap::from([("authorization".to_string(), "Bearer scope-a".to_string())]),
        body: Vec::new(),
    };
    let policy = AuthPolicy::from_env(
        None,
        None,
        Some("scope-a".to_string()),
        None,
        Some("scope-a:tenant-a".to_string()),
    );
    assert_eq!(
        authorize_request_for_tenant(&request, "tenant-a", &policy),
        AuthDecision::Unauthorized("API key revoked")
    );
}

#[test]
fn metrics_endpoint_reports_counters() {
    let runtime = sample_runtime();

    let request = HttpRequest {
            method: "POST".to_string(),
            target: "/v1/ingest".to_string(),
            headers: HashMap::from([("content-type".to_string(), "application/json".to_string())]),
            body: br#"{"claim":{"claim_id":"c1","tenant_id":"tenant-a","canonical_text":"Company X acquired Company Y","confidence":0.9}}"#.to_vec(),
        };
    let response = handle_request(&runtime, &request);
    assert_eq!(response.status, 200);

    let metrics_request = HttpRequest {
        method: "GET".to_string(),
        target: "/metrics".to_string(),
        headers: HashMap::new(),
        body: Vec::new(),
    };
    let metrics_response = handle_request(&runtime, &metrics_request);
    assert_eq!(metrics_response.status, 200);
    assert!(
        metrics_response
            .body
            .contains("dash_ingest_success_total 1")
    );
    assert!(
        metrics_response
            .body
            .contains("dash_ingest_auth_success_total 1")
    );
    assert!(
        metrics_response
            .body
            .contains("dash_ingest_placement_enabled 0")
    );
    assert!(
        metrics_response
            .body
            .contains("dash_ingest_wal_async_flush_enabled 0")
    );
    assert!(
        metrics_response
            .body
            .contains("dash_ingest_wal_background_flush_only 0")
    );
    assert!(
        metrics_response
            .body
            .contains("dash_ingest_transport_queue_capacity 0")
    );
    assert!(
        metrics_response
            .body
            .contains("dash_ingest_transport_queue_depth 0")
    );
    assert!(
        metrics_response
            .body
            .contains("dash_ingest_transport_queue_full_reject_total 0")
    );
}

#[test]
fn metrics_endpoint_reports_backpressure_queue_values() {
    let runtime = sample_runtime();
    let queue_metrics = Arc::new(TransportBackpressureMetrics {
        queue_depth: AtomicUsize::new(3),
        queue_capacity: 8,
        queue_full_reject_total: AtomicU64::new(11),
    });
    {
        let mut guard = runtime.lock().expect("runtime lock should be available");
        guard.set_transport_backpressure_metrics(queue_metrics);
    }

    let metrics_request = HttpRequest {
        method: "GET".to_string(),
        target: "/metrics".to_string(),
        headers: HashMap::new(),
        body: Vec::new(),
    };
    let metrics_response = handle_request(&runtime, &metrics_request);
    assert_eq!(metrics_response.status, 200);
    assert!(
        metrics_response
            .body
            .contains("dash_ingest_transport_queue_capacity 8")
    );
    assert!(
        metrics_response
            .body
            .contains("dash_ingest_transport_queue_depth 3")
    );
    assert!(
        metrics_response
            .body
            .contains("dash_ingest_transport_queue_full_reject_total 11")
    );
}

#[test]
fn resolve_http_queue_capacity_defaults_to_workers_times_constant() {
    let _guard = env_lock().lock().expect("env lock should be available");
    let key = "DASH_INGEST_HTTP_QUEUE_CAPACITY";
    let previous = std::env::var_os(key);
    restore_env_var_for_tests(key, None);
    let capacity = resolve_http_queue_capacity(3);
    assert_eq!(capacity, 3 * DEFAULT_HTTP_QUEUE_CAPACITY_PER_WORKER);
    restore_env_var_for_tests(key, previous.as_deref());
}

#[test]
fn resolve_http_queue_capacity_prefers_env_override() {
    let _guard = env_lock().lock().expect("env lock should be available");
    let key = "DASH_INGEST_HTTP_QUEUE_CAPACITY";
    let previous = std::env::var_os(key);
    set_env_var_for_tests(key, "7");
    let capacity = resolve_http_queue_capacity(3);
    assert_eq!(capacity, 7);
    restore_env_var_for_tests(key, previous.as_deref());
}

#[test]
fn write_backpressure_response_returns_http_503_payload() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("listener bind should succeed");
    let addr = listener.local_addr().expect("local addr should resolve");
    let client = std::thread::spawn(move || {
        let mut stream = TcpStream::connect(addr).expect("client connect should succeed");
        let mut response = String::new();
        stream
            .read_to_string(&mut response)
            .expect("client should read response");
        response
    });

    let (server_stream, _) = listener.accept().expect("accept should succeed");
    write_backpressure_response(server_stream, SOCKET_TIMEOUT_SECS)
        .expect("response write should succeed");
    let response = client.join().expect("client thread should join");
    assert!(response.starts_with("HTTP/1.1 503 Service Unavailable"));
    assert!(response.contains("content-type: application/json"));
    assert!(response.contains("ingestion worker queue full"));
}

#[test]
fn persistent_runtime_enables_async_flush_for_batched_wal_by_default() {
    let wal_path = temp_wal_path();
    let wal = FileWal::open_with_policy(
        &wal_path,
        store::WalWritePolicy {
            sync_every_records: 64,
            append_buffer_max_records: 64,
            sync_interval: None,
            background_flush_only: false,
        },
    )
    .expect("wal should open");
    let runtime =
        IngestionRuntime::persistent(InMemoryStore::new(), wal, CheckpointPolicy::default());
    assert_eq!(
        runtime.wal_async_flush_interval(),
        Some(Duration::from_millis(DEFAULT_ASYNC_WAL_FLUSH_INTERVAL_MS))
    );
    drop(runtime);
    let _ = std::fs::remove_file(&wal_path);
    let mut snapshot = wal_path.into_os_string();
    snapshot.push(".snapshot");
    let _ = std::fs::remove_file(PathBuf::from(snapshot));
}

#[test]
fn persistent_runtime_enables_async_flush_for_background_only_wal() {
    let wal_path = temp_wal_path();
    let wal = FileWal::open_with_policy(
        &wal_path,
        store::WalWritePolicy {
            sync_every_records: 1,
            append_buffer_max_records: 1,
            sync_interval: None,
            background_flush_only: true,
        },
    )
    .expect("wal should open");
    let runtime =
        IngestionRuntime::persistent(InMemoryStore::new(), wal, CheckpointPolicy::default());
    assert_eq!(
        runtime.wal_async_flush_interval(),
        Some(Duration::from_millis(DEFAULT_ASYNC_WAL_FLUSH_INTERVAL_MS))
    );
    drop(runtime);
    let _ = std::fs::remove_file(&wal_path);
    let mut snapshot = wal_path.into_os_string();
    snapshot.push(".snapshot");
    let _ = std::fs::remove_file(PathBuf::from(snapshot));
}

#[test]
fn background_only_mode_skips_request_thread_interval_flush() {
    let wal_path = temp_wal_path();
    let wal = FileWal::open_with_policy(
        &wal_path,
        store::WalWritePolicy {
            sync_every_records: 1,
            append_buffer_max_records: 1,
            sync_interval: Some(Duration::from_millis(1)),
            background_flush_only: true,
        },
    )
    .expect("wal should open");
    let mut runtime =
        IngestionRuntime::persistent(InMemoryStore::new(), wal, CheckpointPolicy::default());
    let request = build_ingest_request_from_json(
            r#"{"claim":{"claim_id":"c-bg","tenant_id":"tenant-a","canonical_text":"Background mode claim","confidence":0.9}}"#,
        )
        .expect("request should parse");
    runtime.ingest(request).expect("ingest should succeed");
    std::thread::sleep(Duration::from_millis(2));
    runtime.flush_wal_if_due();
    let metrics = runtime.metrics_text();
    assert!(metrics.contains("dash_ingest_wal_unsynced_records 1"));
    assert!(metrics.contains("dash_ingest_wal_background_flush_only 1"));

    runtime.flush_wal_for_async_tick();
    let flushed = runtime.metrics_text();
    assert!(flushed.contains("dash_ingest_wal_unsynced_records 0"));

    drop(runtime);
    let _ = std::fs::remove_file(&wal_path);
    let mut snapshot = wal_path.into_os_string();
    snapshot.push(".snapshot");
    let _ = std::fs::remove_file(PathBuf::from(snapshot));
}

#[test]
fn async_flush_tick_forces_sync_of_unsynced_wal_records() {
    let wal_path = temp_wal_path();
    let wal = FileWal::open_with_policy(
        &wal_path,
        store::WalWritePolicy {
            sync_every_records: 64,
            append_buffer_max_records: 64,
            sync_interval: None,
            background_flush_only: false,
        },
    )
    .expect("wal should open");
    let mut runtime =
        IngestionRuntime::persistent(InMemoryStore::new(), wal, CheckpointPolicy::default());
    let request = build_ingest_request_from_json(
            r#"{"claim":{"claim_id":"c1","tenant_id":"tenant-a","canonical_text":"Company X acquired Company Y","confidence":0.9}}"#,
        )
        .expect("request should parse");
    runtime.ingest(request).expect("ingest should succeed");
    let before = runtime.metrics_text();
    assert!(before.contains("dash_ingest_wal_unsynced_records 1"));
    assert!(before.contains("dash_ingest_wal_buffered_records 1"));

    runtime.flush_wal_for_async_tick();
    let after = runtime.metrics_text();
    assert!(after.contains("dash_ingest_wal_unsynced_records 0"));
    assert!(after.contains("dash_ingest_wal_buffered_records 0"));
    assert!(after.contains("dash_ingest_wal_async_flush_tick_total 1"));

    drop(runtime);
    let _ = std::fs::remove_file(&wal_path);
    let mut snapshot = wal_path.into_os_string();
    snapshot.push(".snapshot");
    let _ = std::fs::remove_file(PathBuf::from(snapshot));
}

#[test]
fn handle_request_post_rejects_when_local_node_is_not_write_leader() {
    let placement = ShardPlacement {
        tenant_id: "tenant-a".to_string(),
        shard_id: 0,
        epoch: 9,
        replicas: vec![
            ReplicaPlacement {
                node_id: "node-a".to_string(),
                role: ReplicaRole::Leader,
                health: ReplicaHealth::Healthy,
            },
            ReplicaPlacement {
                node_id: "node-b".to_string(),
                role: ReplicaRole::Follower,
                health: ReplicaHealth::Healthy,
            },
        ],
    };
    let runtime = Arc::new(Mutex::new(
        IngestionRuntime::in_memory(InMemoryStore::new()).with_placement_runtime_for_tests(Ok(
            Some(PlacementRoutingRuntime {
                local_node_id: "node-b".to_string(),
                router_config: RouterConfig {
                    shard_ids: vec![0],
                    virtual_nodes_per_shard: 16,
                    replica_count: 2,
                },
                placements: vec![placement],
            }),
        )),
    ));
    let request = HttpRequest {
            method: "POST".to_string(),
            target: "/v1/ingest".to_string(),
            headers: HashMap::from([("content-type".to_string(), "application/json".to_string())]),
            body: br#"{"claim":{"claim_id":"c-route","tenant_id":"tenant-a","canonical_text":"placement route test","confidence":0.9}}"#.to_vec(),
        };
    let response = handle_request(&runtime, &request);
    assert_eq!(
        response.status, 503,
        "unexpected response body: {}",
        response.body
    );
    assert!(response.body.contains("local node 'node-b'"));

    let claims_len = runtime
        .lock()
        .expect("runtime lock should hold")
        .claims_len();
    assert_eq!(claims_len, 0);

    let metrics_request = HttpRequest {
        method: "GET".to_string(),
        target: "/metrics".to_string(),
        headers: HashMap::new(),
        body: Vec::new(),
    };
    let metrics_response = handle_request(&runtime, &metrics_request);
    assert!(
        metrics_response
            .body
            .contains("dash_ingest_placement_route_reject_total 1")
    );
    assert!(
        metrics_response
            .body
            .contains("dash_ingest_placement_last_epoch 9")
    );
}

#[test]
fn handle_request_write_route_reresolves_after_leader_promotion() {
    let placement = ShardPlacement {
        tenant_id: "tenant-a".to_string(),
        shard_id: 0,
        epoch: 9,
        replicas: vec![
            ReplicaPlacement {
                node_id: "node-a".to_string(),
                role: ReplicaRole::Leader,
                health: ReplicaHealth::Healthy,
            },
            ReplicaPlacement {
                node_id: "node-b".to_string(),
                role: ReplicaRole::Follower,
                health: ReplicaHealth::Healthy,
            },
        ],
    };
    let runtime = Arc::new(Mutex::new(
        IngestionRuntime::in_memory(InMemoryStore::new()).with_placement_runtime_for_tests(Ok(
            Some(PlacementRoutingRuntime {
                local_node_id: "node-b".to_string(),
                router_config: RouterConfig {
                    shard_ids: vec![0],
                    virtual_nodes_per_shard: 16,
                    replica_count: 2,
                },
                placements: vec![placement],
            }),
        )),
    ));
    let denied_request = HttpRequest {
            method: "POST".to_string(),
            target: "/v1/ingest".to_string(),
            headers: HashMap::from([("content-type".to_string(), "application/json".to_string())]),
            body: br#"{"claim":{"claim_id":"c-failover-a","tenant_id":"tenant-a","canonical_text":"placement route failover test","confidence":0.9}}"#.to_vec(),
        };
    let denied_response = handle_request(&runtime, &denied_request);
    assert_eq!(denied_response.status, 503);

    {
        let mut guard = runtime.lock().expect("runtime lock should hold");
        let routing = guard
            .placement_routing
            .as_mut()
            .expect("placement config should be present")
            .as_mut()
            .expect("placement routing should be enabled");
        let epoch = promote_replica_to_leader(&mut routing.runtime.placements[0], "node-b")
            .expect("promotion should succeed");
        assert_eq!(epoch, 10);
    }

    let accepted_request = HttpRequest {
            method: "POST".to_string(),
            target: "/v1/ingest".to_string(),
            headers: HashMap::from([("content-type".to_string(), "application/json".to_string())]),
            body: br#"{"claim":{"claim_id":"c-failover-b","tenant_id":"tenant-a","canonical_text":"placement route failover test","confidence":0.9}}"#.to_vec(),
        };
    let accepted_response = handle_request(&runtime, &accepted_request);
    assert_eq!(accepted_response.status, 200);

    let metrics_request = HttpRequest {
        method: "GET".to_string(),
        target: "/metrics".to_string(),
        headers: HashMap::new(),
        body: Vec::new(),
    };
    let metrics_response = handle_request(&runtime, &metrics_request);
    assert_eq!(metrics_response.status, 200);
    assert!(
        metrics_response
            .body
            .contains("dash_ingest_placement_route_reject_total 1")
    );
    assert!(
        metrics_response
            .body
            .contains("dash_ingest_placement_last_epoch 10")
    );
    assert!(
        metrics_response
            .body
            .contains("dash_ingest_placement_last_role 1")
    );
    assert!(
        metrics_response
            .body
            .contains("dash_ingest_placement_replicas_healthy 2")
    );
}

#[test]
fn debug_placement_endpoint_returns_structured_route_probe() {
    let placement = ShardPlacement {
        tenant_id: "tenant-a".to_string(),
        shard_id: 0,
        epoch: 4,
        replicas: vec![
            ReplicaPlacement {
                node_id: "node-a".to_string(),
                role: ReplicaRole::Leader,
                health: ReplicaHealth::Healthy,
            },
            ReplicaPlacement {
                node_id: "node-b".to_string(),
                role: ReplicaRole::Follower,
                health: ReplicaHealth::Degraded,
            },
        ],
    };
    let runtime = Arc::new(Mutex::new(
        IngestionRuntime::in_memory(InMemoryStore::new()).with_placement_runtime_for_tests(Ok(
            Some(PlacementRoutingRuntime {
                local_node_id: "node-b".to_string(),
                router_config: RouterConfig {
                    shard_ids: vec![0],
                    virtual_nodes_per_shard: 16,
                    replica_count: 2,
                },
                placements: vec![placement],
            }),
        )),
    ));
    let request = HttpRequest {
        method: "GET".to_string(),
        target: "/debug/placement?tenant_id=tenant-a&entity_key=company-x".to_string(),
        headers: HashMap::new(),
        body: Vec::new(),
    };
    let response = handle_request(&runtime, &request);
    assert_eq!(response.status, 200);
    assert!(response.body.contains("\"enabled\":true"));
    assert!(response.body.contains("\"placements\""));
    assert!(response.body.contains("\"route_probe\""));
    assert!(response.body.contains("\"status\":\"rejected\""));
    assert!(response.body.contains("\"local_node_id\":\"node-b\""));
    assert!(response.body.contains("\"target_node_id\":\"node-a\""));
}

#[test]
fn segment_publish_writes_manifest_and_metrics() {
    let mut root_dir = std::env::temp_dir();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be monotonic")
        .as_nanos();
    root_dir.push(format!(
        "dash-ingest-segment-test-{}-{}",
        std::process::id(),
        nanos
    ));

    let runtime = Arc::new(Mutex::new(
        IngestionRuntime::in_memory(InMemoryStore::new()).with_segment_runtime_for_tests(Some(
            SegmentRuntime {
                root_dir: root_dir.clone(),
                max_segment_size: 1,
                scheduler: CompactionSchedulerConfig {
                    max_segments_per_tier: 2,
                    max_compaction_input_segments: 2,
                },
                maintenance_interval: None,
                maintenance_min_stale_age: Duration::from_millis(0),
            },
        )),
    ));

    let request = HttpRequest {
            method: "POST".to_string(),
            target: "/v1/ingest".to_string(),
            headers: HashMap::from([("content-type".to_string(), "application/json".to_string())]),
            body: br#"{"claim":{"claim_id":"c1","tenant_id":"tenant-a","canonical_text":"Company X acquired Company Y","confidence":0.9}}"#.to_vec(),
        };
    let response = handle_request(&runtime, &request);
    assert_eq!(response.status, 200);

    let manifest_path = root_dir.join("tenant-a").join("segments.manifest");
    assert!(manifest_path.exists());

    let metrics_request = HttpRequest {
        method: "GET".to_string(),
        target: "/metrics".to_string(),
        headers: HashMap::new(),
        body: Vec::new(),
    };
    let metrics_response = handle_request(&runtime, &metrics_request);
    assert_eq!(metrics_response.status, 200);
    assert!(
        metrics_response
            .body
            .contains("dash_ingest_segment_publish_success_total 1")
    );
    assert!(
        metrics_response
            .body
            .contains("dash_ingest_segment_last_claim_count 1")
    );

    let _ = std::fs::remove_dir_all(root_dir);
}

#[test]
fn segment_maintenance_prunes_orphan_segment_files() {
    let mut root_dir = std::env::temp_dir();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be monotonic")
        .as_nanos();
    root_dir.push(format!(
        "dash-ingest-segment-maintenance-test-{}-{}",
        std::process::id(),
        nanos
    ));

    let tenant_dir = root_dir.join("tenant-a");
    persist_segments_atomic(
        &tenant_dir,
        &[Segment {
            segment_id: "hot-0".to_string(),
            tier: Tier::Hot,
            claim_ids: vec!["c1".to_string()],
        }],
    )
    .expect("segment persist should succeed");
    let orphan_segment = tenant_dir.join("orphan.seg");
    std::fs::write(&orphan_segment, "orphan-segment").expect("orphan segment write should succeed");
    assert!(orphan_segment.exists());

    let mut runtime = IngestionRuntime::in_memory(InMemoryStore::new())
        .with_segment_runtime_for_tests(Some(SegmentRuntime {
            root_dir: root_dir.clone(),
            max_segment_size: 1,
            scheduler: CompactionSchedulerConfig {
                max_segments_per_tier: 2,
                max_compaction_input_segments: 2,
            },
            maintenance_interval: Some(Duration::from_millis(1)),
            maintenance_min_stale_age: Duration::from_millis(0),
        }));
    runtime.run_segment_maintenance_tick();

    assert!(!orphan_segment.exists());
    let metrics = runtime.metrics_text();
    assert!(metrics.contains("dash_ingest_segment_maintenance_tick_total 1"));
    assert!(metrics.contains("dash_ingest_segment_maintenance_success_total 1"));
    assert!(metrics.contains("dash_ingest_segment_maintenance_failure_total 0"));
    assert!(metrics.contains("dash_ingest_segment_maintenance_last_pruned_count 1"));
    assert!(metrics.contains("dash_ingest_segment_maintenance_last_tenant_dirs 1"));
    assert!(metrics.contains("dash_ingest_segment_maintenance_last_tenant_manifests 1"));

    let _ = std::fs::remove_dir_all(root_dir);
}

#[test]
fn append_audit_record_writes_chained_hash_and_seq() {
    let mut audit_path = std::env::temp_dir();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be monotonic")
        .as_nanos();
    audit_path.push(format!(
        "dash-ingest-audit-chain-{}-{}.jsonl",
        std::process::id(),
        nanos
    ));
    let audit_path_str = audit_path.to_string_lossy().to_string();
    clear_cached_audit_chain_state(&audit_path_str);

    let first = AuditEvent {
        action: "ingest",
        tenant_id: Some("tenant-a"),
        claim_id: Some("c-1"),
        status: 200,
        outcome: "success",
        reason: "ok",
    };
    let second = AuditEvent {
        action: "ingest",
        tenant_id: Some("tenant-a"),
        claim_id: Some("c-2"),
        status: 200,
        outcome: "success",
        reason: "ok",
    };

    append_audit_record(&audit_path_str, &first, 1_700_000_000_001)
        .expect("first audit append should succeed");
    append_audit_record(&audit_path_str, &second, 1_700_000_000_002)
        .expect("second audit append should succeed");

    let payload = std::fs::read_to_string(&audit_path).expect("audit file should be readable");
    let lines: Vec<&str> = payload
        .lines()
        .filter(|line| !line.trim().is_empty())
        .collect();
    assert_eq!(lines.len(), 2);

    let first_obj = match parse_json(lines[0]).expect("first line JSON should parse") {
        JsonValue::Object(object) => object,
        _ => panic!("first line should be object"),
    };
    let second_obj = match parse_json(lines[1]).expect("second line JSON should parse") {
        JsonValue::Object(object) => object,
        _ => panic!("second line should be object"),
    };

    assert!(matches!(first_obj.get("seq"), Some(JsonValue::Number(raw)) if raw == "1"));
    assert!(matches!(second_obj.get("seq"), Some(JsonValue::Number(raw)) if raw == "2"));
    let first_hash = match first_obj.get("hash") {
        Some(JsonValue::String(raw)) => raw.clone(),
        _ => panic!("first hash should exist"),
    };
    let second_prev = match second_obj.get("prev_hash") {
        Some(JsonValue::String(raw)) => raw.clone(),
        _ => panic!("second prev_hash should exist"),
    };
    assert!(is_sha256_hex(&first_hash));
    assert_eq!(second_prev, first_hash);

    let _ = std::fs::remove_file(audit_path);
}
