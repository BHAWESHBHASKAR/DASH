//! Helpers shared by the compat test binaries that need the service crates.

#![allow(dead_code)]

use std::path::Path;

use dash_compat::{
    Fixture, RETRIEVE_KEY, ScratchState, env_lock, parse_response, raw_request, remove_env,
    retrieve_requests, set_env,
};
use serde_json::Value;
use store::{AnnTuningConfig, FileWal, InMemoryStore, ReplayPolicy, StoreLoadStats};

/// Replays the scratch state with the strict policy: every record of the
/// old release must parse and apply (nothing quarantined).
pub fn load_strict(state: &ScratchState) -> (InMemoryStore, StoreLoadStats, FileWal) {
    let wal = FileWal::open(state.wal()).expect("open fixture WAL");
    let (store, stats) =
        InMemoryStore::load_from_wal_with_policy(&wal, AnnTuningConfig::default(), ReplayPolicy::Strict)
            .expect("strict replay of the fixture");
    (store, stats, wal)
}

/// Sends every dataset retrieve request through the current retrieval HTTP
/// handler, authenticated with the API key the old build was configured
/// with. `segment_dir` sets `DASH_RETRIEVAL_SEGMENT_DIR` like the scenario
/// did.
pub fn run_retrieves(store: &InMemoryStore, segment_dir: Option<&Path>) -> Vec<(u16, Value)> {
    let _env = env_lock();
    set_env("DASH_RETRIEVAL_API_KEY", RETRIEVE_KEY);
    match segment_dir {
        Some(dir) => set_env("DASH_RETRIEVAL_SEGMENT_DIR", &dir.display().to_string()),
        None => remove_env("DASH_RETRIEVAL_SEGMENT_DIR"),
    }
    let out = retrieve_requests()
        .iter()
        .map(|row| {
            let raw = raw_request(
                row["method"].as_str().expect("method"),
                row["path"].as_str().expect("path"),
                Some(&row["body"]),
                &[("x-api-key", RETRIEVE_KEY)],
            );
            let response = retrieval::transport::handle_http_request_bytes(store, &raw)
                .expect("request parses");
            parse_response(&response)
        })
        .collect();
    remove_env("DASH_RETRIEVAL_SEGMENT_DIR");
    out
}

/// Score tolerance: the old build printed scores with six decimals.
const SCORE_EPSILON: f64 = 2e-6;

/// The parts of one retrieve answer that must not change across an upgrade:
/// which claims, in which order, with which score, stance counts and
/// citations.
fn result_summary(result: &Value) -> Value {
    let mut citations: Vec<String> = result["citations"]
        .as_array()
        .map(|items| {
            items
                .iter()
                .map(|c| c["evidence_id"].as_str().unwrap_or_default().to_string())
                .collect()
        })
        .unwrap_or_default();
    citations.sort();
    serde_json::json!({
        "claim_id": result["claim_id"],
        "canonical_text": result["canonical_text"],
        "supports": result["supports"],
        "contradicts": result["contradicts"],
        "dominant_stance": result["dominant_stance"],
        "citations": citations,
    })
}

/// Compares the current answers with the ones the old build recorded
/// (`http/retrieve-responses.jsonl`). Returns a description of every
/// difference (empty when they match).
pub fn diff_against_recorded(fixture: &Fixture, actual: &[(u16, Value)]) -> Vec<String> {
    let recorded = fixture.read_jsonl("http/retrieve-responses.jsonl");
    assert_eq!(recorded.len(), actual.len(), "one answer per dataset request");
    let mut diffs = Vec::new();
    for (index, (expected, (status, body))) in recorded.iter().zip(actual).enumerate() {
        let expected_status = expected["status"].as_u64().unwrap_or(0);
        if u64::from(*status) != expected_status {
            diffs.push(format!(
                "request {index}: status {status}, old build answered {expected_status}: {body}"
            ));
            continue;
        }
        if expected_status != 200 {
            continue;
        }
        let old = expected["body"]["results"].as_array().cloned().unwrap_or_default();
        let new = body["results"].as_array().cloned().unwrap_or_default();
        let old_ids: Vec<&str> = old.iter().map(|r| r["claim_id"].as_str().unwrap_or("")).collect();
        let new_ids: Vec<&str> = new.iter().map(|r| r["claim_id"].as_str().unwrap_or("")).collect();
        if old_ids != new_ids {
            diffs.push(format!("request {index}: claims {new_ids:?}, old build {old_ids:?}"));
            continue;
        }
        for (o, n) in old.iter().zip(&new) {
            let (os, ns) = (result_summary(o), result_summary(n));
            if os != ns {
                diffs.push(format!("request {index}: result {ns}, old build {os}"));
            }
            let (Some(old_score), Some(new_score)) = (o["score"].as_f64(), n["score"].as_f64())
            else {
                diffs.push(format!("request {index}: missing score"));
                continue;
            };
            if (old_score - new_score).abs() > SCORE_EPSILON {
                diffs.push(format!(
                    "request {index}: {} score {new_score}, old build {old_score}",
                    n["claim_id"]
                ));
            }
        }
        // Every field the old build answered with is still present.
        if let (Some(old_obj), Some(new_obj)) = (expected["body"].as_object(), body.as_object()) {
            for key in old_obj.keys() {
                if !new_obj.contains_key(key) {
                    diffs.push(format!("request {index}: response lost field '{key}'"));
                }
            }
        }
        for (o, n) in old.iter().zip(&new) {
            if let (Some(old_obj), Some(new_obj)) = (o.as_object(), n.as_object()) {
                for key in old_obj.keys() {
                    if !new_obj.contains_key(key) {
                        diffs.push(format!("request {index}: result lost field '{key}'"));
                    }
                }
            }
        }
    }
    diffs
}
