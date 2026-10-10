//! PERF-07: the embedding provider used on the ingest path is constructed
//! once per environment signature, not once per request.

use dash_common::RawAuthConfig;

use super::super::authz::policy_from_raw;
use super::super::routes::handle_request_with_policy;
use super::super::*;
use super::{env_lock, restore_env_var_for_tests, sample_runtime, set_env_var_for_tests};
use crate::api::embedding_provider_build_count;

const SIGNATURE_VAR: &str = "DASH_OLLAMA_MODEL";

fn ingest(runtime: &SharedRuntime, id: &str) -> u16 {
    let policy = policy_from_raw(RawAuthConfig {
        insecure_dev: true,
        ..Default::default()
    })
    .expect("policy");
    let body = format!(
        "{{\"claim\":{{\"claim_id\":\"{id}\",\"tenant_id\":\"tenant-a\",\
         \"canonical_text\":\"Company X acquired Company Y\",\"confidence\":0.9}}}}"
    );
    let request = HttpRequest {
        method: "POST".to_string(),
        target: "/v1/ingest".to_string(),
        headers: HashMap::from([("content-type".to_string(), "application/json".to_string())]),
        body: body.into_bytes(),
    };
    handle_request_with_policy(runtime, &request, &policy).status
}

#[test]
fn ingest_requests_reuse_one_provider_until_the_environment_signature_changes() {
    let _env = env_lock().lock().unwrap_or_else(|p| p.into_inner());
    let previous = std::env::var_os(SIGNATURE_VAR);
    let runtime = sample_runtime();

    set_env_var_for_tests(SIGNATURE_VAR, "model-a");
    assert_eq!(ingest(&runtime, "warm-0"), 200);
    let after_warmup = embedding_provider_build_count();

    for i in 0..50 {
        assert_eq!(ingest(&runtime, &format!("same-{i}")), 200);
    }
    assert_eq!(
        embedding_provider_build_count(),
        after_warmup,
        "50 requests with an unchanged environment must not build a provider"
    );

    set_env_var_for_tests(SIGNATURE_VAR, "model-b");
    for i in 0..50 {
        assert_eq!(ingest(&runtime, &format!("changed-{i}")), 200);
    }
    assert_eq!(
        embedding_provider_build_count(),
        after_warmup + 1,
        "a signature change must cause exactly one rebuild"
    );

    restore_env_var_for_tests(SIGNATURE_VAR, previous.as_deref());
}

/// The batch size limit is read on every batch request; it is resolved once.
#[test]
fn batch_item_limit_env_is_resolved_once_per_process() {
    use super::super::config::resolve_ingest_batch_max_items;
    let _env = env_lock().lock().unwrap_or_else(|p| p.into_inner());
    let previous = std::env::var_os("DASH_INGEST_BATCH_MAX_ITEMS");
    let first = resolve_ingest_batch_max_items(128);
    set_env_var_for_tests("DASH_INGEST_BATCH_MAX_ITEMS", &(first + 7).to_string());
    let second = resolve_ingest_batch_max_items(128);
    restore_env_var_for_tests("DASH_INGEST_BATCH_MAX_ITEMS", previous.as_deref());
    assert_eq!(first, second, "the limit must not be re-read per request");
}
