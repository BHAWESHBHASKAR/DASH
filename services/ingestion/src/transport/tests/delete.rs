//! Delete routes end to end through the HTTP handler: WAL durability, the
//! redb mirror, segments, idempotency, validation, audit records and the
//! group-commit ordering rule.

use std::path::Path;

use indexer::{CompactionSchedulerConfig, load_current_segments, resolve_tenant_dir};
use store::AnnTuningConfig;

use super::super::segment_runtime::SegmentRuntime;
use super::super::*;
use super::env_lock;

fn persistent_runtime(dir: &Path) -> SharedRuntime {
    let store = InMemoryStore::new().attach_disk(dir.join("store.redb"));
    assert_eq!(store.disk_status(), &DiskStatus::Available);
    let wal = FileWal::open(dir.join("wal.log")).expect("wal should open");
    let runtime = IngestionRuntime::persistent(store, wal, CheckpointPolicy::default())
        .with_segment_runtime_for_tests(Some(SegmentRuntime {
            root_dir: dir.join("segments"),
            max_segment_size: 2,
            scheduler: CompactionSchedulerConfig::default(),
            maintenance_interval: None,
            maintenance_min_stale_age: Duration::ZERO,
        }));
    assert!(runtime.group_commit_active());
    Arc::new(Mutex::new(runtime))
}

fn post(target: &str, body: String) -> HttpRequest {
    HttpRequest {
        method: "POST".to_string(),
        target: target.to_string(),
        headers: HashMap::from([("content-type".to_string(), "application/json".to_string())]),
        body: body.into_bytes(),
    }
}

fn delete(target: &str) -> HttpRequest {
    HttpRequest {
        method: "DELETE".to_string(),
        target: target.to_string(),
        headers: HashMap::new(),
        body: Vec::new(),
    }
}

fn ingest(runtime: &SharedRuntime, tenant: &str, claim_id: &str, extra: &str) {
    let body = format!(
        r#"{{"claim":{{"claim_id":"{claim_id}","tenant_id":"{tenant}","canonical_text":"Statement {claim_id} about Acme","confidence":0.9,"embedding_vector":[0.1,0.2,0.3]}},"evidence":[{{"evidence_id":"ev-{claim_id}","claim_id":"{claim_id}","source_id":"src://{claim_id}","stance":"supports","source_quality":0.8}}]{extra}}}"#
    );
    let response = handle_request(runtime, &post("/v1/ingest", body));
    assert_eq!(response.status, 200, "{}", response.body);
}

fn edge_to(from: &str, to: &str) -> String {
    format!(
        r#","edges":[{{"edge_id":"{from}-{to}","from_claim_id":"{from}","to_claim_id":"{to}","relation":"supports","strength":0.6}}]"#
    )
}

fn json(response: &HttpResponse) -> serde_json::Value {
    serde_json::from_str(&response.body).expect("JSON body")
}

fn segment_claim_ids(dir: &Path, tenant: &str) -> Vec<String> {
    let tenant_dir = resolve_tenant_dir(&dir.join("segments"), tenant);
    let mut ids: Vec<String> = load_current_segments(&tenant_dir)
        .expect("segments load")
        .map(|(_, segments)| {
            segments
                .into_iter()
                .flat_map(|segment| segment.claim_ids)
                .collect()
        })
        .unwrap_or_default();
    ids.sort();
    ids
}

/// Claims (with evidence and edge ids) of every tenant, for comparisons.
fn dump(store: &InMemoryStore) -> Vec<String> {
    let mut out = Vec::new();
    for tenant in store.tenant_ids() {
        let mut claims = store.claims_for_tenant(&tenant);
        claims.sort_by(|a, b| a.claim_id.cmp(&b.claim_id));
        for claim in claims {
            let mut evidence: Vec<String> = store
                .evidence_for_claim(&claim.claim_id)
                .into_iter()
                .map(|e| e.evidence_id)
                .collect();
            evidence.sort();
            let mut edges: Vec<String> = store
                .edges_for_claim(&claim.claim_id)
                .into_iter()
                .map(|e| e.edge_id)
                .collect();
            edges.sort();
            out.push(format!(
                "{tenant}/{} {evidence:?} {edges:?} vec={}",
                claim.claim_id,
                store
                    .ann_vector_top_candidates(&tenant, &[0.1, 0.2, 0.3], 1_000)
                    .contains(&claim.claim_id)
            ));
        }
    }
    out
}

fn reload_from_wal(dir: &Path) -> InMemoryStore {
    let wal = FileWal::open(dir.join("wal.log")).unwrap();
    InMemoryStore::load_from_wal(&wal).expect("WAL replays cleanly")
}

#[test]
fn claim_delete_is_durable_idempotent_and_refreshes_segments() {
    let _guard = env_lock().lock().expect("env lock");
    let dir = tempfile::tempdir().unwrap();
    let runtime = persistent_runtime(dir.path());
    ingest(&runtime, "tenant-d", "a", "");
    ingest(&runtime, "tenant-d", "b", &edge_to("b", "a"));
    ingest(&runtime, "tenant-d", "c", "");
    assert_eq!(segment_claim_ids(dir.path(), "tenant-d"), ["a", "b", "c"]);

    let response = handle_request(&runtime, &delete("/v1/claims/a?tenant_id=tenant-d"));
    assert_eq!(response.status, 200, "{}", response.body);
    let body = json(&response);
    assert_eq!(body["deleted"], true);
    assert_eq!(body["scope"], "claim");
    assert_eq!(body["tenant_id"], "tenant-d");
    assert_eq!(body["claim_id"], "a");
    assert_eq!(body["claims_deleted"], 1);
    assert_eq!(body["evidence_deleted"], 1);
    assert_eq!(body["edges_deleted"], 1);
    assert_eq!(body["vectors_deleted"], 1);
    assert_eq!(body["claims_total"], 2);
    assert_eq!(segment_claim_ids(dir.path(), "tenant-d"), ["b", "c"]);

    // Repeating it, or deleting something absent, is a 200 with deleted=false
    // and writes nothing.
    let wal_len = std::fs::metadata(dir.path().join("wal.log")).unwrap().len();
    for target in [
        "/v1/claims/a?tenant_id=tenant-d",
        "/v1/claims/never?tenant_id=tenant-d",
        // A claim of this tenant addressed with another tenant id.
        "/v1/claims/b?tenant_id=tenant-other",
        "/v1/evidence/ev-missing?tenant_id=tenant-d",
        "/v1/tenants/tenant-never",
    ] {
        let response = handle_request(&runtime, &delete(target));
        assert_eq!(response.status, 200, "{target}: {}", response.body);
        assert_eq!(json(&response)["deleted"], false, "{target}");
    }
    assert_eq!(
        std::fs::metadata(dir.path().join("wal.log")).unwrap().len(),
        wal_len
    );

    let live = dump(&runtime.lock().unwrap().store);
    assert!(!live.iter().any(|line| line.starts_with("tenant-d/a ")));
    assert!(
        live.iter()
            .any(|line| line.contains("tenant-d/b [\"ev-b\"] []"))
    );
    let metrics = runtime.lock().unwrap().metrics_text();
    assert!(metrics.contains("dash_ingest_delete_total{scope=\"claim\"} 1"));
    assert!(metrics.contains("dash_ingest_delete_noop_total 5"));
    drop(runtime);

    // Restart from the WAL and from redb alone.
    assert_eq!(dump(&reload_from_wal(dir.path())), live);
    let mut empty = FileWal::open(dir.path().join("empty.wal")).unwrap();
    let (from_redb, _) = InMemoryStore::load_from_disk_and_wal(
        dir.path().join("store.redb"),
        &mut empty,
        AnnTuningConfig::default(),
    )
    .unwrap();
    assert_eq!(dump(&from_redb), live);
}

#[test]
fn evidence_and_tenant_deletes_through_http() {
    let _guard = env_lock().lock().expect("env lock");
    let dir = tempfile::tempdir().unwrap();
    let runtime = persistent_runtime(dir.path());
    ingest(&runtime, "tenant-e", "e1", "");
    ingest(&runtime, "tenant-e", "e2", "");
    ingest(&runtime, "tenant-keep", "k1", "");

    let response = handle_request(&runtime, &delete("/v1/evidence/ev-e1?tenant_id=tenant-e"));
    assert_eq!(response.status, 200, "{}", response.body);
    let body = json(&response);
    assert_eq!(
        (
            &body["deleted"],
            &body["evidence_deleted"],
            &body["claims_deleted"]
        ),
        (
            &serde_json::json!(true),
            &serde_json::json!(1),
            &serde_json::json!(0)
        )
    );
    assert_eq!(body["evidence_id"], "ev-e1");

    let response = handle_request(&runtime, &delete("/v1/tenants/tenant-e"));
    assert_eq!(response.status, 200, "{}", response.body);
    let body = json(&response);
    assert_eq!(body["deleted"], true);
    assert_eq!(body["scope"], "tenant");
    assert_eq!(body["claims_deleted"], 2);
    assert_eq!(body["claims_total"], 1);
    assert!(segment_claim_ids(dir.path(), "tenant-e").is_empty());
    assert_eq!(segment_claim_ids(dir.path(), "tenant-keep"), ["k1"]);

    let live = dump(&runtime.lock().unwrap().store);
    assert_eq!(live.len(), 1, "{live:?}");
    drop(runtime);
    assert_eq!(dump(&reload_from_wal(dir.path())), live);
}

#[test]
fn malformed_delete_requests_are_rejected_before_anything_changes() {
    let _guard = env_lock().lock().expect("env lock");
    let runtime = sample_runtime_with_claim();
    for (method, target, status) in [
        ("DELETE", "/v1/claims/c1", 400),
        ("DELETE", "/v1/claims/c1?tenant_id=", 400),
        ("DELETE", "/v1/claims/%ZZ?tenant_id=tenant-a", 400),
        (
            "DELETE",
            "/v1/claims/c1?tenant_id=tenant-a&write_consistency=most",
            400,
        ),
        ("DELETE", "/v1/tenants/tenant-a?tenant_id=tenant-a", 400),
        ("DELETE", "/v1/claims/c1/extra?tenant_id=tenant-a", 404),
        ("DELETE", "/v1/claims/?tenant_id=tenant-a", 404),
        ("POST", "/v1/claims/c1?tenant_id=tenant-a", 405),
        ("PUT", "/v1/tenants/tenant-a", 405),
    ] {
        let request = HttpRequest {
            method: method.to_string(),
            target: target.to_string(),
            headers: HashMap::new(),
            body: Vec::new(),
        };
        let response = handle_request(&runtime, &request);
        assert_eq!(
            response.status, status,
            "{method} {target}: {}",
            response.body
        );
    }
    let long = "x".repeat(dash_common::audit::MAX_AUDIT_FIELD_BYTES + 1);
    let response = handle_request(
        &runtime,
        &delete(&format!("/v1/claims/{long}?tenant_id=tenant-a")),
    );
    assert_eq!(response.status, 400);
    assert!(runtime.lock().unwrap().store.claim_by_id("c1").is_some());
}

fn sample_runtime_with_claim() -> SharedRuntime {
    let runtime = super::sample_runtime();
    let response = handle_request(
        &runtime,
        &post(
            "/v1/ingest",
            r#"{"claim":{"claim_id":"c1","tenant_id":"tenant-a","canonical_text":"Company X acquired Company Y","confidence":0.9}}"#
                .to_string(),
        ),
    );
    assert_eq!(response.status, 200, "{}", response.body);
    runtime
}

#[test]
fn deletes_are_audit_logged_like_other_mutations() {
    let _guard = env_lock().lock().expect("env lock");
    let dir = tempfile::tempdir().unwrap();
    let audit_path = dir.path().join("audit.jsonl");
    let previous = std::env::var_os("DASH_INGEST_AUDIT_LOG_PATH");
    super::set_env_var_for_tests("DASH_INGEST_AUDIT_LOG_PATH", audit_path.to_str().unwrap());

    let runtime = sample_runtime_with_claim();
    let first = handle_request(&runtime, &delete("/v1/claims/c1?tenant_id=tenant-a"));
    let second = handle_request(&runtime, &delete("/v1/claims/c1?tenant_id=tenant-a"));
    let tenant = handle_request(&runtime, &delete("/v1/tenants/tenant-a"));
    super::restore_env_var_for_tests("DASH_INGEST_AUDIT_LOG_PATH", previous.as_deref());
    assert_eq!(
        (first.status, second.status, tenant.status),
        (200, 200, 200)
    );

    let records: Vec<serde_json::Value> = std::fs::read_to_string(&audit_path)
        .unwrap()
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let deletes: Vec<&serde_json::Value> = records
        .iter()
        .filter(|r| {
            r["action"]
                .as_str()
                .is_some_and(|a| a.starts_with("delete_"))
        })
        .collect();
    assert_eq!(deletes.len(), 3, "{records:?}");
    assert_eq!(deletes[0]["action"], "delete_claim");
    assert_eq!(deletes[0]["tenant_id"], "tenant-a");
    assert_eq!(deletes[0]["claim_id"], "c1");
    assert_eq!(deletes[0]["status"], 200);
    assert_eq!(deletes[0]["outcome"], "success");
    assert!(
        deletes[0]["reason"]
            .as_str()
            .unwrap()
            .contains("deleted=true")
    );
    assert!(
        deletes[1]["reason"]
            .as_str()
            .unwrap()
            .contains("deleted=false")
    );
    assert_eq!(deletes[2]["action"], "delete_tenant");
    let report = dash_common::audit::verify_file(
        audit_path.to_str().unwrap(),
        &dash_common::audit::VerifyOptions::default(),
    )
    .expect("audit chain verifies");
    assert!(report.chained_records >= 4);
}

/// Deletes drain the group-commit pipeline: run them concurrently with
/// pipelined ingests (same and other tenants, re-creating deleted ids) and
/// the live state must equal a replay of the WAL.
#[test]
fn concurrent_deletes_and_pipelined_ingests_match_a_wal_replay() {
    let _guard = env_lock().lock().expect("env lock");
    super::set_env_var_for_tests("DASH_INGEST_RATE_LIMIT_PER_TENANT_RPS", "0");
    let dir = tempfile::tempdir().unwrap();
    let runtime = persistent_runtime(dir.path());
    let rounds = 30;
    std::thread::scope(|scope| {
        for t in 0..4 {
            let runtime = &runtime;
            scope.spawn(move || {
                for i in 0..rounds {
                    let tenant = format!("cc-{}", t % 2);
                    let id = format!("cc-{t}-{}", i % 7);
                    // Edges may name a target that was just deleted (or not
                    // written yet): such an ingest is rejected, as it would
                    // be in serial order.
                    let extra = if i % 3 == 0 {
                        edge_to(&id, &format!("cc-{t}-{}", (i + 1) % 7))
                    } else {
                        String::new()
                    };
                    let body = format!(
                        r#"{{"claim":{{"claim_id":"{id}","tenant_id":"{tenant}","canonical_text":"Statement {id}","confidence":0.9,"embedding_vector":[0.1,0.2,0.3]}}{extra}}}"#
                    );
                    let response = handle_request(runtime, &post("/v1/ingest", body));
                    assert!(
                        matches!(response.status, 200 | 400),
                        "{}: {}",
                        response.status,
                        response.body
                    );
                }
            });
        }
        let runtime = &runtime;
        scope.spawn(move || {
            for i in 0..rounds {
                let target = match i % 5 {
                    4 => format!("/v1/tenants/cc-{}", i % 2),
                    _ => format!(
                        "/v1/claims/cc-{}-{}?tenant_id=cc-{}",
                        i % 4,
                        i % 7,
                        (i % 4) % 2
                    ),
                };
                let response = handle_request(runtime, &delete(&target));
                assert_eq!(response.status, 200, "{target}: {}", response.body);
            }
        });
    });
    super::restore_env_var_for_tests("DASH_INGEST_RATE_LIMIT_PER_TENANT_RPS", None);
    let live = dump(&runtime.lock().unwrap().store);
    drop(runtime);
    assert_eq!(dump(&reload_from_wal(dir.path())), live);
}
