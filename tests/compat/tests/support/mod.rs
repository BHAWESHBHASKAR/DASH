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
    let (store, stats) = InMemoryStore::load_from_wal_with_policy(
        &wal,
        AnnTuningConfig::default(),
        ReplayPolicy::Strict,
    )
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

/// By-design differences between the current answers and the old build's
/// for one fixture (`tests/compat/expected/<label>.json`, optional).
struct ExpectedOverrides {
    scores_comparable: bool,
    claims: std::collections::BTreeMap<usize, Vec<String>>,
}

fn expected_overrides(fixture: &Fixture) -> ExpectedOverrides {
    let path = dash_compat::compat_root()
        .join("expected")
        .join(format!("{}.json", fixture.label));
    let Ok(text) = std::fs::read_to_string(&path) else {
        return ExpectedOverrides {
            scores_comparable: true,
            claims: Default::default(),
        };
    };
    let value: Value = serde_json::from_str(&text).expect("valid expected file");
    let mut claims = std::collections::BTreeMap::new();
    for (index, entry) in value["requests"].as_object().into_iter().flatten() {
        assert!(
            entry["why"].as_str().is_some_and(|why| !why.is_empty()),
            "{}: every by-design difference says why",
            path.display()
        );
        let ids = entry["claims"]
            .as_array()
            .expect("claims")
            .iter()
            .map(|id| id.as_str().expect("claim id").to_string())
            .collect();
        claims.insert(index.parse().expect("request index"), ids);
    }
    ExpectedOverrides {
        scores_comparable: value["scores_comparable"].as_bool().unwrap_or(true),
        claims,
    }
}

/// Compares the current answers with the ones the old build recorded
/// (`http/retrieve-responses.jsonl`), allowing only the by-design
/// differences listed in `tests/compat/expected/<label>.json`. Returns a
/// description of every other difference (empty when there is none).
pub fn diff_against_recorded(fixture: &Fixture, actual: &[(u16, Value)]) -> Vec<String> {
    let recorded = fixture.read_jsonl("http/retrieve-responses.jsonl");
    assert_eq!(
        recorded.len(),
        actual.len(),
        "one answer per dataset request"
    );
    let overrides = expected_overrides(fixture);
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
        let old = expected["body"]["results"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        let new = body["results"].as_array().cloned().unwrap_or_default();
        let old_ids: Vec<String> = old
            .iter()
            .map(|r| r["claim_id"].as_str().unwrap_or("").to_string())
            .collect();
        let new_ids: Vec<String> = new
            .iter()
            .map(|r| r["claim_id"].as_str().unwrap_or("").to_string())
            .collect();
        match overrides.claims.get(&index) {
            Some(by_design) => {
                if by_design == &old_ids {
                    diffs.push(format!(
                        "request {index}: stale entry in expected/{}.json (same as the old build)",
                        fixture.label
                    ));
                }
                if &new_ids != by_design {
                    diffs.push(format!(
                        "request {index}: claims {new_ids:?}, expected (by design) {by_design:?}"
                    ));
                }
            }
            None => {
                if old_ids != new_ids {
                    diffs.push(format!(
                        "request {index}: claims {new_ids:?}, old build {old_ids:?}"
                    ));
                    continue;
                }
                if overrides.scores_comparable {
                    for (o, n) in old.iter().zip(&new) {
                        let (os, ns) = (result_summary(o), result_summary(n));
                        if os != ns {
                            diffs.push(format!("request {index}: result {ns}, old build {os}"));
                        }
                        let (Some(old_score), Some(new_score)) =
                            (o["score"].as_f64(), n["score"].as_f64())
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
                }
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
        if let (Some(o), Some(n)) = (old.first(), new.first())
            && let (Some(old_obj), Some(new_obj)) = (o.as_object(), n.as_object())
        {
            for key in old_obj.keys() {
                if !new_obj.contains_key(key) {
                    diffs.push(format!("request {index}: result lost field '{key}'"));
                }
            }
        }
    }
    diffs
}
