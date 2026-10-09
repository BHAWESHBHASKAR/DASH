//! ROB-12: `/ready` is a single JSON document and never carries raw error
//! strings (hostnames, filesystem paths, upstream bodies), driven through the
//! real request handler.

use dash_common::RawAuthConfig;

use super::super::authz::policy_from_raw;
use super::super::routes::handle_request_with_policy;
use super::super::*;
use super::{env_lock, restore_env_var_for_tests, sample_runtime, set_env_var_for_tests};

fn ready(runtime: &SharedRuntime) -> (u16, String) {
    let policy = policy_from_raw(RawAuthConfig {
        insecure_dev: true,
        ..Default::default()
    })
    .expect("policy");
    let request = HttpRequest {
        method: "GET".to_string(),
        target: "/ready".to_string(),
        headers: HashMap::new(),
        body: Vec::new(),
    };
    let response = handle_request_with_policy(runtime, &request, &policy);
    (response.status, response.body)
}

fn follower_runtime() -> SharedRuntime {
    let runtime = sample_runtime();
    {
        let mut rt = runtime.lock().expect("lock");
        rt.replication_follower.enabled = true;
    }
    runtime
}

fn assert_clean(body: &str) -> serde_json::Value {
    let value: serde_json::Value = serde_json::from_str(body)
        .unwrap_or_else(|err| panic!("/ready must be valid JSON ({err}): {body}"));
    for leaked in [
        "10.9.8.7",
        "http://",
        "refused",
        "os error",
        "/var/lib",
        "byte limit",
        "secret-body",
        "\\\"",
    ] {
        assert!(!body.contains(leaked), "/ready leaked '{leaked}': {body}");
    }
    value
}

#[test]
fn ready_reports_an_unreachable_leader_by_code_not_by_raw_error() {
    let _env = env_lock().lock().unwrap_or_else(|p| p.into_inner());
    let runtime = follower_runtime();
    {
        let mut rt = runtime.lock().expect("lock");
        rt.observe_replication_pull_failure(
            "failed requesting replication source 'http://10.9.8.7:8081': \
             Connection refused (os error 111) while writing /var/lib/dash/state"
                .to_string(),
        );
        rt.replication_follower.consecutive_failures = 3;
    }
    let (status, body) = ready(&runtime);
    assert_eq!(status, 503, "{body}");
    let value = assert_clean(&body);
    assert_eq!(value["reason"], "replication_initial_sync_pending");
    assert!(value["replication"].is_object(), "single-encoded: {body}");
    assert_eq!(value["replication"]["last_error"], "source_unreachable");
    assert_eq!(value["replication"]["consecutive_failures"], 3);
}

#[test]
fn ready_reports_an_oversized_leader_response_by_code() {
    let _env = env_lock().lock().unwrap_or_else(|p| p.into_inner());
    let runtime = follower_runtime();
    {
        let mut rt = runtime.lock().expect("lock");
        rt.observe_replication_pull_failure(
            "replication response exceeds 1048576 byte limit".to_string(),
        );
        rt.replication_follower.blocked_reason = Some("replication_response_too_large");
    }
    let (status, body) = ready(&runtime);
    assert_eq!(status, 503, "{body}");
    let value = assert_clean(&body);
    assert_eq!(value["reason"], "replication_response_too_large");
    assert_eq!(
        value["replication"]["blocked_reason"],
        "replication_response_too_large"
    );
    assert_eq!(value["replication"]["last_error"], "response_too_large");
}

#[test]
fn ready_never_echoes_an_upstream_error_body() {
    let _env = env_lock().lock().unwrap_or_else(|p| p.into_inner());
    let runtime = follower_runtime();
    {
        let mut rt = runtime.lock().expect("lock");
        rt.observe_replication_pull_failure(
            "replication source returned status 500 (secret-body \"quoted\" \\ backslash)"
                .to_string(),
        );
    }
    let (_, body) = ready(&runtime);
    let value = assert_clean(&body);
    assert_eq!(value["replication"]["last_error"], "source_error_status");
}

#[test]
fn ready_reports_quarantine_counts_as_a_number() {
    let _env = env_lock().lock().unwrap_or_else(|p| p.into_inner());
    let runtime = follower_runtime();
    {
        let mut rt = runtime.lock().expect("lock");
        rt.replication_follower.skipped_records_total = 7;
        rt.replication_follower.synced_once = true;
        rt.replication_follower.last_success = Some(Instant::now());
    }
    let (status, body) = ready(&runtime);
    assert_eq!(status, 200, "{body}");
    let value = assert_clean(&body);
    assert_eq!(value["status"], "ready");
    assert_eq!(value["replication"]["skipped_records_total"], 7);
    assert!(value["replication"]["last_error"].is_null());
}

#[test]
fn ready_reports_disk_unavailable_without_the_failure_reason() {
    let _env = env_lock().lock().unwrap_or_else(|p| p.into_inner());
    let dir = tempfile::tempdir().expect("tempdir");
    let blocker = dir.path().join("not-a-directory");
    std::fs::write(&blocker, b"file").expect("blocker file");
    let store = InMemoryStore::new().attach_disk(blocker.join("store.redb"));
    assert!(matches!(
        store.disk_status(),
        DiskStatus::Unavailable { .. }
    ));
    let runtime: SharedRuntime = Arc::new(Mutex::new(IngestionRuntime::in_memory(store)));

    let previous = std::env::var_os("DASH_INGEST_PERSISTENCE_PATH");
    set_env_var_for_tests(
        "DASH_INGEST_PERSISTENCE_PATH",
        &blocker.join("store.redb").to_string_lossy(),
    );
    let (status, body) = ready(&runtime);
    restore_env_var_for_tests("DASH_INGEST_PERSISTENCE_PATH", previous.as_deref());

    assert_eq!(status, 503, "{body}");
    let value: serde_json::Value = serde_json::from_str(&body).expect("valid JSON");
    assert_eq!(value["reason"], "disk_unavailable");
    assert!(
        !body.contains(&dir.path().to_string_lossy().to_string()),
        "no filesystem path in /ready: {body}"
    );
}

fn post_ingest(runtime: &SharedRuntime, claim_id: &str) -> u16 {
    let request = HttpRequest {
        method: "POST".to_string(),
        target: "/v1/ingest".to_string(),
        headers: HashMap::from([("content-type".to_string(), "application/json".to_string())]),
        body: format!(
            r#"{{"claim":{{"claim_id":"{claim_id}","tenant_id":"tenant-a","canonical_text":"volume test {claim_id}","confidence":0.9}}}}"#
        )
        .into_bytes(),
    };
    handle_request(runtime, &request).status
}

#[test]
fn ready_reports_failed_wal_writes_until_the_volume_is_writable_again() {
    let _env = env_lock().lock().unwrap_or_else(|p| p.into_inner());
    let dir = tempfile::tempdir().expect("tempdir");
    let volume = dir.path().join("volume");
    let wal = FileWal::open(volume.join("ingest.wal")).expect("wal");
    let runtime: SharedRuntime = Arc::new(Mutex::new(IngestionRuntime::persistent(
        InMemoryStore::new(),
        wal,
        CheckpointPolicy::default(),
    )));
    assert_eq!(post_ingest(&runtime, "before"), 200);
    assert_eq!(ready(&runtime).0, 200);

    // The volume goes away (unmounted): appends fail with an I/O error.
    let away = dir.path().join("volume-away");
    std::fs::rename(&volume, &away).expect("unmount volume");
    assert_eq!(post_ingest(&runtime, "lost"), 500);
    let (status, body) = ready(&runtime);
    assert_eq!(status, 503, "{body}");
    let value = assert_clean(&body);
    assert_eq!(value["reason"], "wal_write_failed");
    let metrics = runtime.lock().expect("lock").metrics_text();
    assert!(
        metrics.contains("dash_ingest_wal_write_failing 1"),
        "{metrics}"
    );
    assert!(
        metrics.contains("dash_ingest_wal_write_failure_total 1"),
        "{metrics}"
    );

    // The volume is back: the readiness probe succeeds and writes go through.
    std::fs::rename(&away, &volume).expect("remount volume");
    assert_eq!(ready(&runtime).0, 200);
    assert_eq!(post_ingest(&runtime, "after"), 200);
    let metrics = runtime.lock().expect("lock").metrics_text();
    assert!(
        metrics.contains("dash_ingest_wal_write_failing 0"),
        "{metrics}"
    );
    assert!(
        metrics.contains("dash_ingest_wal_write_recovered_total 1"),
        "{metrics}"
    );
    assert!(
        !volume.join("ingest.wal.space-probe").exists(),
        "the probe file must be removed"
    );
}
