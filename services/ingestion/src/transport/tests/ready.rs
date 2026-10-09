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
