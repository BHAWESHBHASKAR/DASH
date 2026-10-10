//! The current code serves the same retrieve answers as the release that
//! wrote each fixture: straight after the upgrade (replaying the old WAL and
//! snapshot), with the old segment directory configured, and after a
//! checkpoint in the current format followed by a restart.

mod support;

use dash_compat::FIXTURES;
use store::{FileWal, InMemoryStore};
use support::{diff_against_recorded, load_strict, run_retrieves};

#[test]
fn upgraded_node_serves_the_answers_the_old_release_recorded() {
    for fixture in FIXTURES {
        let state = fixture.scratch_state();
        let (store, _, _) = load_strict(&state);
        let diffs = diff_against_recorded(fixture, &run_retrieves(&store, None));
        assert!(diffs.is_empty(), "{}:\n{}", fixture.label, diffs.join("\n"));
    }
}

#[test]
fn answers_are_unchanged_with_the_old_segment_directory_configured() {
    for fixture in FIXTURES {
        let state = fixture.scratch_state();
        let (store, _, _) = load_strict(&state);
        let segments = state.segments();
        let diffs = diff_against_recorded(fixture, &run_retrieves(&store, Some(&segments)));
        assert!(diffs.is_empty(), "{}:\n{}", fixture.label, diffs.join("\n"));
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
        let diffs = diff_against_recorded(fixture, &run_retrieves(&store, None));
        assert!(diffs.is_empty(), "{}:\n{}", fixture.label, diffs.join("\n"));
    }
}
