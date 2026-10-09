//! Regression tests for the independent security review findings that touch
//! the ingestion HTTP handler.

use std::ffi::OsString;

use dash_common::{AuthPolicy, RawAuthConfig};

use super::super::authz::policy_from_raw;
use super::super::routes::handle_request_with_policy;
use super::super::*;
use super::{env_lock, restore_env_var_for_tests, sample_runtime, set_env_var_for_tests};

const KEY: &str = "review-fixes-key-3f9a1c7e5d2b8046";

/// Sets an environment variable for the duration of a test and restores the
/// previous value on drop (also when the test panics).
struct ScopedEnv {
    key: &'static str,
    previous: Option<OsString>,
}

impl ScopedEnv {
    fn set(key: &'static str, value: &str) -> Self {
        let previous = std::env::var_os(key);
        set_env_var_for_tests(key, value);
        Self { key, previous }
    }
}

impl Drop for ScopedEnv {
    fn drop(&mut self) {
        restore_env_var_for_tests(self.key, self.previous.as_deref());
    }
}

fn keyed_policy() -> AuthPolicy {
    policy_from_raw(RawAuthConfig {
        api_key: Some(KEY.to_string()),
        strict_secrets: true,
        ..Default::default()
    })
    .expect("policy")
}

fn audit_path(tag: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    std::env::temp_dir()
        .join(format!(
            "dash-ingest-review-{tag}-{}-{nanos}.jsonl",
            std::process::id()
        ))
        .to_string_lossy()
        .to_string()
}

fn post(target: &str, body: &str, headers: &[(&str, &str)]) -> HttpRequest {
    let mut map: HashMap<String, String> = headers
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    map.insert("content-type".to_string(), "application/json".to_string());
    HttpRequest {
        method: "POST".to_string(),
        target: target.to_string(),
        headers: map,
        body: body.as_bytes().to_vec(),
    }
}

fn bodies(tenant: &str, id: &str) -> [(&'static str, String); 4] {
    [
        (
            "/v1/ingest",
            format!(
                "{{\"claim\":{{\"claim_id\":\"{id}\",\"tenant_id\":\"{tenant}\",\
                 \"canonical_text\":\"Company X acquired Company Y\",\"confidence\":0.9}}}}"
            ),
        ),
        (
            "/v1/ingest/raw",
            format!(
                "{{\"tenant_id\":\"{tenant}\",\"document_id\":\"{id}\",\
                 \"source_id\":\"source://doc\",\"text\":\"Company X acquired Company Y in 2024.\",\
                 \"min_sentence_chars\":10,\"max_claims\":4}}"
            ),
        ),
        (
            "/v1/ingest/document",
            format!(
                "{{\"tenant_id\":\"{tenant}\",\"document_id\":\"{id}\",\
                 \"source_id\":\"source://doc\",\"mime_type\":\"text/plain\",\
                 \"text\":\"Company X acquired Company Y in 2024.\"}}"
            ),
        ),
        (
            "/v1/ingest/batch",
            format!(
                "{{\"items\":[{{\"claim\":{{\"claim_id\":\"{id}\",\"tenant_id\":\"{tenant}\",\
                 \"canonical_text\":\"Batch item\",\"confidence\":0.9}}}}]}}"
            ),
        ),
    ]
}

/// Audit lines that mention `needle`. Other tests in this process may run
/// handlers while the audit path environment variable is set, so assertions
/// look only at the records this test caused.
fn audit_lines_with(path: &str, needle: &str) -> Vec<String> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .filter(|line| line.contains(needle))
        .map(str::to_string)
        .collect()
}

/// Review finding 1: an unauthenticated request with a megabyte-sized
/// identifier used to be answered 401 *and* appended in full to the audit log.
#[test]
fn unauthenticated_oversized_identifiers_are_rejected_before_any_audit_record() {
    let _env = env_lock().lock().unwrap_or_else(|p| p.into_inner());
    let path = audit_path("oversized");
    let _audit = ScopedEnv::set("DASH_INGEST_AUDIT_LOG_PATH", &path);
    let policy = keyed_policy();
    let runtime = sample_runtime();
    let huge = "T".repeat(1024 * 1024);
    for (route, body) in bodies(&huge, "c-1") {
        let response = handle_request_with_policy(&runtime, &post(route, &body, &[]), &policy);
        assert_eq!(response.status, 400, "{route} oversized tenant_id");
    }
    for (route, body) in bodies("tenant-a", &huge) {
        let response = handle_request_with_policy(&runtime, &post(route, &body, &[]), &policy);
        assert_eq!(response.status, 400, "{route} oversized claim/document id");
    }
    assert!(
        audit_lines_with(&path, "TTTTTTTT").is_empty(),
        "oversized values reached the audit log"
    );
    let _ = std::fs::remove_file(&path);
}

#[test]
fn identifiers_at_the_limit_are_still_audited_and_bounded() {
    let _env = env_lock().lock().unwrap_or_else(|p| p.into_inner());
    let path = audit_path("boundary");
    let _audit = ScopedEnv::set("DASH_INGEST_AUDIT_LOG_PATH", &path);
    let policy = keyed_policy();
    let runtime = sample_runtime();
    let at_limit = "t".repeat(256);
    let (route, body) = &bodies(&at_limit, "c-1")[0];
    let response = handle_request_with_policy(&runtime, &post(route, body, &[]), &policy);
    assert_eq!(response.status, 401);
    let records = audit_lines_with(&path, "tttttttt");
    assert_eq!(records.len(), 1, "{records:?}");
    assert!(records[0].len() < 2048, "{} bytes", records[0].len());

    let over = "t".repeat(257);
    let (route, body) = &bodies(&over, "c-1")[0];
    let response = handle_request_with_policy(&runtime, &post(route, body, &[]), &policy);
    assert_eq!(response.status, 400);
    assert_eq!(
        audit_lines_with(&path, "tttttttt").len(),
        1,
        "no new record for the 400"
    );
    let _ = std::fs::remove_file(&path);
}

/// An authenticated caller gets the same 400 for oversized identifiers.
#[test]
fn authenticated_oversized_identifiers_are_rejected_too() {
    let _env = env_lock().lock().unwrap_or_else(|p| p.into_inner());
    let path = audit_path("auth-oversized");
    let _audit = ScopedEnv::set("DASH_INGEST_AUDIT_LOG_PATH", &path);
    let policy = keyed_policy();
    let runtime = sample_runtime();
    let (route, body) = &bodies("tenant-a", &"c".repeat(300))[0];
    let response =
        handle_request_with_policy(&runtime, &post(route, body, &[("x-api-key", KEY)]), &policy);
    assert_eq!(response.status, 400);
    let _ = std::fs::remove_file(&path);
}

impl ScopedEnv {
    fn unset(key: &'static str) -> Self {
        let previous = std::env::var_os(key);
        restore_env_var_for_tests(key, None);
        Self { key, previous }
    }
}

fn get(target: &str) -> HttpRequest {
    HttpRequest {
        method: "GET".to_string(),
        target: target.to_string(),
        headers: HashMap::new(),
        body: Vec::new(),
    }
}

/// Review finding 7: dev mode must not open replication on a service that has
/// authentication configured; only a completely unconfigured dev-mode service
/// may serve it without a token.
#[test]
fn dev_mode_does_not_open_replication_when_auth_is_configured() {
    let _env = env_lock().lock().unwrap_or_else(|p| p.into_inner());
    let _token = ScopedEnv::unset("DASH_INGEST_REPLICATION_TOKEN");
    let _legacy = ScopedEnv::unset("EME_INGEST_REPLICATION_TOKEN");
    let runtime = sample_runtime();
    let routes = [
        "/internal/replication/export",
        "/internal/replication/wal?from_offset=0",
        "/internal/replication/commit-status?commit_id=c1",
    ];

    // Dev mode plus an API key: replication stays closed without a token.
    let configured = policy_from_raw(RawAuthConfig {
        api_key: Some(KEY.to_string()),
        insecure_dev: true,
        ..Default::default()
    })
    .expect("policy");
    assert!(!configured.is_open_dev_mode());
    for route in routes {
        let response = handle_request_with_policy(&runtime, &get(route), &configured);
        assert_eq!(response.status, 403, "{route} with auth configured");
    }
    let ack = post(
        "/internal/replication/ack?commit_id=c1&replica_id=r&epoch=1",
        "",
        &[],
    );
    assert_eq!(
        handle_request_with_policy(&runtime, &ack, &configured).status,
        403
    );

    // A completely unconfigured dev-mode service still serves replication.
    let open = policy_from_raw(RawAuthConfig {
        insecure_dev: true,
        ..Default::default()
    })
    .expect("policy");
    assert!(open.is_open_dev_mode());
    let response = handle_request_with_policy(&runtime, &get(routes[0]), &open);
    assert_ne!(response.status, 403, "{}", response.body);
}
