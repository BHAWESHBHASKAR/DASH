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
/// retrieval prefilter: every query, vector or text-only, answers exactly as
/// without it (same claims, same order, same scores, same graph edges). A
/// text-only query in particular returns only claims that match a query term
/// on both paths, never claims of the tenant that only fill `top_k`.
#[test]
fn answers_are_unchanged_with_the_old_segment_directory_configured() {
    let requests = dash_compat::retrieve_requests();
    for fixture in FIXTURES {
        let state = fixture.scratch_state();
        let (store, _, _) = load_strict(&state);
        let segments = state.segments();
        let with = run_retrieves(&store, Some(&segments));
        let without = run_retrieves(&store, None);
        for index in 0..requests.len() {
            let (status, body) = &with[index];
            assert_eq!(
                *status, without[index].0,
                "{} request {index}",
                fixture.label
            );
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
            assert_eq!(
                edges(body),
                edges(&without[index].1),
                "{} request {index}",
                fixture.label
            );
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
