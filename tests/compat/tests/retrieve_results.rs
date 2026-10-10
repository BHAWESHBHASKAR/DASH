//! The current code serves the same retrieve answers as the release that
//! wrote each fixture: straight after the upgrade (replaying the old WAL and
//! snapshot, with the old segment directory configured as the scenario
//! did), and after a checkpoint in the current format followed by a
//! restart. By-design differences are listed per fixture in
//! `tests/compat/expected/<label>.json`.

mod support;

use dash_compat::FIXTURES;
use store::{FileWal, InMemoryStore};
use support::{diff_against_recorded, load_strict, run_retrieves};

#[test]
fn upgraded_node_serves_the_answers_the_old_release_recorded() {
    for fixture in FIXTURES {
        let state = fixture.scratch_state();
        let (store, _, _) = load_strict(&state);
        let answers = run_retrieves(&store, Some(&state.segments()));
        let diffs = diff_against_recorded(fixture, &answers);
        assert!(diffs.is_empty(), "{}:\n{}", fixture.label, diffs.join("\n"));
    }
}

/// The segment directory the old release published is still usable as the
/// retrieval prefilter: vector queries answer exactly as without it.
/// Text-only queries keep the lexical matches first, in the same order; the
/// segment candidate path then fills `top_k` with low-scored claims of the
/// tenant, which the index path does not (current behaviour on any data,
/// not an upgrade effect).
#[test]
fn answers_are_unchanged_with_the_old_segment_directory_configured() {
    let requests = dash_compat::retrieve_requests();
    for fixture in FIXTURES {
        let state = fixture.scratch_state();
        let (store, _, _) = load_strict(&state);
        let segments = state.segments();
        let with = run_retrieves(&store, Some(&segments));
        let without = run_retrieves(&store, None);
        for (index, request) in requests.iter().enumerate() {
            let (status, body) = &with[index];
            assert_eq!(*status, without[index].0, "{} request {index}", fixture.label);
            if request["body"].get("query_embedding").is_some() {
                assert_eq!(
                    body["results"], without[index].1["results"],
                    "{} request {index}",
                    fixture.label
                );
                // The graph lists the same edges (its order is not defined).
                let edges = |v: &serde_json::Value| -> Vec<String> {
                    let mut out: Vec<String> = v["graph"]["edges"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .map(|e| e.to_string())
                        .collect();
                    out.sort();
                    out
                };
                assert_eq!(edges(body), edges(&without[index].1), "{}", fixture.label);
            } else {
                let ids = |v: &serde_json::Value| -> Vec<String> {
                    v["results"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .map(|r| r["claim_id"].as_str().unwrap_or("").to_string())
                        .collect()
                };
                let (seg, idx) = (ids(body), ids(&without[index].1));
                assert!(
                    seg.starts_with(&idx),
                    "{} request {index}: {seg:?} does not start with {idx:?}",
                    fixture.label
                );
            }
        }
    }
}

#[test]
fn answers_survive_a_checkpoint_in_the_current_format_and_a_restart() {
    for fixture in FIXTURES {
        let state = fixture.scratch_state();
        {
            let (store, _, mut wal) = load_strict(&state);
            store.checkpoint_and_compact(&mut wal).expect("checkpoint");
        }
        let wal = FileWal::open(state.wal()).expect("reopen");
        let store = InMemoryStore::load_from_wal(&wal).expect("reload after checkpoint");
        let answers = run_retrieves(&store, Some(&state.segments()));
        let diffs = diff_against_recorded(fixture, &answers);
        assert!(diffs.is_empty(), "{}:\n{}", fixture.label, diffs.join("\n"));
    }
}
