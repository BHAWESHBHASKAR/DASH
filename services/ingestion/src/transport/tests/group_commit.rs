//! Group commit on the single-ingest path: concurrent requests share fsyncs,
//! acknowledged writes survive a restart, visibility follows WAL order,
//! failed or poisoned WAL writes never reach memory or redb, and conflicting
//! concurrent requests end up exactly as a serial replay of the WAL.

use std::path::Path;

use store::AnnTuningConfig;

use super::super::*;
use super::env_lock;

fn persistent_runtime(dir: &Path) -> SharedRuntime {
    let store = InMemoryStore::new().attach_disk(dir.join("store.redb"));
    assert_eq!(store.disk_status(), &DiskStatus::Available);
    let wal = FileWal::open(dir.join("wal.log")).expect("wal should open");
    let runtime = IngestionRuntime::persistent(store, wal, CheckpointPolicy::default());
    assert!(
        runtime.group_commit_active(),
        "group commit is on by default"
    );
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

fn ingest_body(claim_id: &str, tenant: &str, text: &str, extra: &str) -> String {
    format!(
        r#"{{"claim":{{"claim_id":"{claim_id}","tenant_id":"{tenant}","canonical_text":"{text}","confidence":0.9}}{extra}}}"#
    )
}

fn field_u64(body: &str, name: &str) -> u64 {
    let needle = format!("\"{name}\":");
    let start = body.find(&needle).expect("field present") + needle.len();
    body[start..]
        .chars()
        .take_while(char::is_ascii_digit)
        .collect::<String>()
        .parse()
        .expect("numeric field")
}

/// Claim ids in the order their claim records appear in the WAL file.
fn wal_claim_order(path: &Path) -> Vec<String> {
    std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .filter(|line| line.starts_with("C2\t") || line.starts_with("C\t"))
        .map(|line| line.split('\t').nth(1).unwrap().to_string())
        .collect()
}

fn metric(runtime: &SharedRuntime, name: &str) -> f64 {
    let text = runtime.lock().unwrap().metrics_text();
    text.lines()
        .find_map(|line| line.strip_prefix(&format!("{name} ")))
        .unwrap_or_else(|| panic!("metric {name} missing"))
        .parse()
        .unwrap()
}

/// The dev-mode credential shares one rate-limit bucket across tenants;
/// the concurrency tests send more requests than its burst. Lifts the limit
/// while alive (callers hold `env_lock`).
struct NoRateLimit;

impl NoRateLimit {
    const VAR: &'static str = "DASH_INGEST_RATE_LIMIT_PER_TENANT_RPS";

    fn new() -> Self {
        super::set_env_var_for_tests(Self::VAR, "0");
        Self
    }
}

impl Drop for NoRateLimit {
    fn drop(&mut self) {
        super::restore_env_var_for_tests(Self::VAR, None);
    }
}

fn reload(dir: &Path) -> InMemoryStore {
    let wal = FileWal::open(dir.join("wal.log")).unwrap();
    InMemoryStore::load_from_wal(&wal).expect("WAL replays cleanly")
}

#[test]
fn concurrent_ingests_are_all_durable_and_visible_in_wal_order() {
    let _guard = env_lock().lock().expect("env lock");
    let _limits = NoRateLimit::new();
    let dir = tempfile::tempdir().unwrap();
    let runtime = persistent_runtime(dir.path());

    let threads = 16;
    let per_thread = 20;
    // (claim id, claims_total seen right after this ingest was applied)
    let acknowledged: Vec<(String, u64)> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..threads)
            .map(|t| {
                let runtime = &runtime;
                scope.spawn(move || {
                    let mut acked = Vec::new();
                    for i in 0..per_thread {
                        let id = format!("gc-t{t}-c{i}");
                        let response = handle_request(
                            runtime,
                            &post(
                                "/v1/ingest",
                                ingest_body(
                                    &id,
                                    &format!("gc-order-{t}"),
                                    &format!("Claim text {id}"),
                                    "",
                                ),
                            ),
                        );
                        assert_eq!(response.status, 200, "{}", response.body);
                        acked.push((id, field_u64(&response.body, "claims_total")));
                    }
                    acked
                })
            })
            .collect();
        handles
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect()
    });
    let total = (threads * per_thread) as u64;
    assert_eq!(acknowledged.len() as u64, total);
    assert_eq!(
        metric(&runtime, "dash_ingest_wal_group_commit_entries_total"),
        total as f64
    );
    assert_eq!(
        metric(&runtime, "dash_ingest_wal_group_commit_in_flight"),
        0.0
    );
    drop(runtime);

    // Every acknowledged claim is in the WAL after a restart.
    let reloaded = reload(dir.path());
    for (id, _) in &acknowledged {
        assert!(reloaded.claim_by_id(id).is_some(), "acknowledged {id} lost");
    }
    assert_eq!(reloaded.claims_len() as u64, total);

    // Visibility follows WAL order: the k-th claim in the WAL was applied
    // when the store held exactly k claims (every claim is new).
    let order = wal_claim_order(&dir.path().join("wal.log"));
    assert_eq!(order.len() as u64, total);
    let seen: HashMap<&str, u64> = acknowledged
        .iter()
        .map(|(id, total)| (id.as_str(), *total))
        .collect();
    for (position, id) in order.iter().enumerate() {
        assert_eq!(
            seen[id.as_str()],
            position as u64 + 1,
            "{id} became visible out of WAL order"
        );
    }

    // redb mirrored every applied record too.
    let mut empty = FileWal::open(dir.path().join("empty.log")).unwrap();
    let (from_redb, _) = InMemoryStore::load_from_disk_and_wal(
        dir.path().join("store.redb"),
        &mut empty,
        AnnTuningConfig::default(),
    )
    .unwrap();
    assert_eq!(from_redb.claims_len() as u64, total);
}

#[test]
fn batches_interleaved_with_pipelined_ingests_drain_and_survive_restart() {
    let _guard = env_lock().lock().expect("env lock");
    let _limits = NoRateLimit::new();
    let dir = tempfile::tempdir().unwrap();
    let runtime = persistent_runtime(dir.path());

    let acknowledged: Vec<String> = std::thread::scope(|scope| {
        let mut handles = Vec::new();
        for t in 0..6 {
            let runtime = &runtime;
            handles.push(scope.spawn(move || {
                let mut acked = Vec::new();
                for i in 0..15 {
                    if t % 3 == 0 {
                        let a = format!("mix-b{t}-{i}-a");
                        let b = format!("mix-b{t}-{i}-b");
                        let body = format!(
                            r#"{{"commit_id":"batch-{t}-{i}","items":[{},{}]}}"#,
                            ingest_body(&a, &format!("gc-mix-{t}"), "Batched claim alpha", ""),
                            ingest_body(&b, &format!("gc-mix-{t}"), "Batched claim beta", ""),
                        );
                        let response = handle_request(runtime, &post("/v1/ingest/batch", body));
                        assert_eq!(response.status, 200, "{}", response.body);
                        acked.extend([a, b]);
                    } else {
                        let id = format!("mix-s{t}-{i}");
                        let response = handle_request(
                            runtime,
                            &post(
                                "/v1/ingest",
                                ingest_body(&id, &format!("gc-mix-{t}"), "Single claim", ""),
                            ),
                        );
                        assert_eq!(response.status, 200, "{}", response.body);
                        acked.push(id);
                    }
                }
                acked
            }));
        }
        handles
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect()
    });
    let live_claims = runtime.lock().unwrap().claims_len();
    drop(runtime);
    let reloaded = reload(dir.path());
    for id in &acknowledged {
        assert!(reloaded.claim_by_id(id).is_some(), "acknowledged {id} lost");
    }
    assert_eq!(reloaded.claims_len(), acknowledged.len());
    assert_eq!(live_claims, acknowledged.len());
}

#[test]
fn conflicting_concurrent_ingests_match_a_serial_replay_of_the_wal() {
    let _guard = env_lock().lock().expect("env lock");
    let _limits = NoRateLimit::new();
    let dir = tempfile::tempdir().unwrap();
    let runtime = persistent_runtime(dir.path());

    // Same claim ids rewritten from many threads, edges to claims created
    // concurrently, and a fresh tenant whose first vectors disagree on the
    // dimension: every interaction the conflict keys must serialize.
    let statuses: Vec<u16> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..12)
            .map(|t| {
                let runtime = &runtime;
                scope.spawn(move || {
                    let mut statuses = Vec::new();
                    for i in 0..10 {
                        let shared = format!("shared-{}", i % 3);
                        let edge = format!(
                            r#","edges":[{{"edge_id":"e-{t}-{i}","from_claim_id":"{shared}","to_claim_id":"shared-{}","relation":"supports","strength":0.5}}]"#,
                            (i + 1) % 3
                        );
                        let body = ingest_body(
                            &shared,
                            "gc-conflict",
                            &format!("Version {t}-{i}"),
                            &edge,
                        );
                        statuses.push(handle_request(runtime, &post("/v1/ingest", body)).status);

                        let dim = if t % 2 == 0 { "[0.1,0.2,0.3]" } else { "[0.1,0.2,0.3,0.4]" };
                        let body = ingest_body(
                            &format!("dim-{t}-{i}"),
                            "gc-dims",
                            "Vector claim",
                            &format!(r#","claim_embedding":{dim}"#),
                        );
                        statuses.push(handle_request(runtime, &post("/v1/ingest", body)).status);
                    }
                    statuses
                })
            })
            .collect();
        handles
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect()
    });
    assert!(
        statuses.iter().all(|s| *s == 200 || *s == 400),
        "{statuses:?}"
    );
    assert!(statuses.contains(&400), "one dimension must lose");

    let guard = runtime.lock().unwrap();
    let live_dim = guard.store.tenant_vector_dim("gc-dims");
    let mut live_claims = guard.store.claims_for_tenant("gc-conflict");
    live_claims.extend(guard.store.claims_for_tenant("gc-dims"));
    live_claims.sort_by(|a, b| a.claim_id.cmp(&b.claim_id));
    let live_edges: Vec<_> = (0..3)
        .map(|i| guard.store.edges_for_claim(&format!("shared-{i}")))
        .collect();
    drop(guard);
    drop(runtime);

    let wal = FileWal::open(dir.path().join("wal.log")).unwrap();
    let (replayed, stats) = InMemoryStore::load_from_wal_with_policy(
        &wal,
        AnnTuningConfig::default(),
        store::ReplayPolicy::Strict,
    )
    .expect("strict replay accepts every record that was acknowledged");
    assert_eq!(stats.replay.quarantined_records, 0);
    assert_eq!(replayed.tenant_vector_dim("gc-dims"), live_dim);
    let mut replayed_claims = replayed.claims_for_tenant("gc-conflict");
    replayed_claims.extend(replayed.claims_for_tenant("gc-dims"));
    replayed_claims.sort_by(|a, b| a.claim_id.cmp(&b.claim_id));
    assert_eq!(replayed_claims, live_claims);
    let replayed_edges: Vec<_> = (0..3)
        .map(|i| replayed.edges_for_claim(&format!("shared-{i}")))
        .collect();
    assert_eq!(replayed_edges, live_edges);
}

#[test]
fn failed_group_append_leaves_memory_and_redb_untouched() {
    let _guard = env_lock().lock().expect("env lock");
    let dir = tempfile::tempdir().unwrap();
    let runtime = persistent_runtime(dir.path());
    let ok = handle_request(
        &runtime,
        &post(
            "/v1/ingest",
            ingest_body("a1", "gc-misc", "First claim", ""),
        ),
    );
    assert_eq!(ok.status, 200, "{}", ok.body);

    // Make every WAL append fail: replace the log file with a directory.
    let wal_path = dir.path().join("wal.log");
    std::fs::remove_file(&wal_path).unwrap();
    std::fs::create_dir(&wal_path).unwrap();

    let failed = handle_request(
        &runtime,
        &post(
            "/v1/ingest",
            ingest_body("b1", "gc-misc", "Must not land", ""),
        ),
    );
    assert_eq!(failed.status, 500, "{}", failed.body);
    {
        let guard = runtime.lock().unwrap();
        assert!(guard.store.claim_by_id("b1").is_none(), "memory leaked");
        assert_eq!(guard.claims_len(), 1);
        assert!(guard.wal_poisoned_reason().is_none());
    }
    assert_eq!(
        metric(
            &runtime,
            "dash_ingest_wal_group_commit_failed_batches_total"
        ),
        1.0
    );
    drop(runtime);

    let mut empty = FileWal::open(dir.path().join("empty.log")).unwrap();
    let (from_redb, _) = InMemoryStore::load_from_disk_and_wal(
        dir.path().join("store.redb"),
        &mut empty,
        AnnTuningConfig::default(),
    )
    .unwrap();
    assert!(from_redb.claim_by_id("a1").is_some());
    assert!(from_redb.claim_by_id("b1").is_none(), "redb leaked");
}

#[test]
fn poisoned_wal_fails_writes_closed_and_reports_not_ready() {
    let _guard = env_lock().lock().expect("env lock");
    let dir = tempfile::tempdir().unwrap();
    let runtime = persistent_runtime(dir.path());
    let ok = handle_request(
        &runtime,
        &post(
            "/v1/ingest",
            ingest_body("a1", "gc-misc", "First claim", ""),
        ),
    );
    assert_eq!(ok.status, 200, "{}", ok.body);
    {
        let guard = runtime.lock().unwrap();
        lock_wal(guard.wal.as_ref().unwrap()).poison_for_testing("injected fsync failure");
    }

    for target in ["/v1/ingest", "/v1/ingest", "/v1/ingest/batch"] {
        let body = if target.ends_with("batch") {
            format!(
                r#"{{"commit_id":"after-poison","items":[{}]}}"#,
                ingest_body("c1", "gc-misc", "Batched after poison", "")
            )
        } else {
            ingest_body("b1", "gc-misc", "Must not land", "")
        };
        let response = handle_request(&runtime, &post(target, body));
        assert_eq!(response.status, 503, "{target}: {}", response.body);
        assert!(response.body.contains("wal_poisoned"), "{}", response.body);
    }
    {
        let guard = runtime.lock().unwrap();
        assert!(guard.store.claim_by_id("b1").is_none());
        assert!(guard.store.claim_by_id("c1").is_none());
        assert_eq!(guard.claims_len(), 1);
    }
    let ready = handle_request(
        &runtime,
        &HttpRequest {
            method: "GET".to_string(),
            target: "/ready".to_string(),
            headers: HashMap::new(),
            body: Vec::new(),
        },
    );
    assert_eq!(ready.status, 503);
    assert!(ready.body.contains("wal_poisoned"), "{}", ready.body);
    assert_eq!(metric(&runtime, "dash_ingest_wal_poisoned"), 1.0);
}

#[test]
fn group_commit_can_be_disabled() {
    let _guard = env_lock().lock().expect("env lock");
    let dir = tempfile::tempdir().unwrap();
    super::set_env_var_for_tests("DASH_INGEST_WAL_GROUP_COMMIT", "false");
    let wal = FileWal::open(dir.path().join("wal.log")).unwrap();
    let runtime =
        IngestionRuntime::persistent(InMemoryStore::new(), wal, CheckpointPolicy::default());
    super::restore_env_var_for_tests("DASH_INGEST_WAL_GROUP_COMMIT", None);
    assert!(!runtime.group_commit_active());
    assert!(runtime.group_commit_summary().is_none());
    let runtime = Arc::new(Mutex::new(runtime));
    let response = handle_request(
        &runtime,
        &post(
            "/v1/ingest",
            ingest_body("a1", "gc-misc", "Direct path claim", ""),
        ),
    );
    assert_eq!(response.status, 200, "{}", response.body);
    assert_eq!(
        metric(&runtime, "dash_ingest_wal_group_commit_enabled"),
        0.0
    );
    drop(runtime);
    assert!(reload(dir.path()).claim_by_id("a1").is_some());
}

#[test]
fn group_commit_settings_are_read_and_clamped() {
    let _guard = env_lock().lock().expect("env lock");
    super::set_env_var_for_tests("DASH_INGEST_WAL_GROUP_COMMIT_MAX_WAIT_US", "999999");
    super::set_env_var_for_tests("DASH_INGEST_WAL_GROUP_COMMIT_MAX_BATCH_BYTES", "4096");
    super::set_env_var_for_tests("DASH_INGEST_WAL_GROUP_COMMIT_QUEUE_CAPACITY", "8");
    let config = super::super::group_commit::resolve_group_commit_config();
    for key in [
        "DASH_INGEST_WAL_GROUP_COMMIT_MAX_WAIT_US",
        "DASH_INGEST_WAL_GROUP_COMMIT_MAX_BATCH_BYTES",
        "DASH_INGEST_WAL_GROUP_COMMIT_QUEUE_CAPACITY",
    ] {
        super::restore_env_var_for_tests(key, None);
    }
    let config = config.expect("enabled by default");
    assert_eq!(config.max_wait, store::GROUP_COMMIT_MAX_WAIT_LIMIT);
    assert_eq!(config.max_batch_bytes, 4096);
    assert_eq!(config.queue_capacity, 8);
    let defaults = super::super::group_commit::resolve_group_commit_config().unwrap();
    assert_eq!(defaults, store::GroupCommitConfig::default());
}
