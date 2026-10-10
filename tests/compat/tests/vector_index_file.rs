//! Persisted vector index (`<wal>.vindex`, format version 1) compatibility.
//! The file is derived from the WAL: a file from this format version loads,
//! a missing, stale (other WAL generation) or unknown-version file is
//! rebuilt from the WAL and never served, and deleting it is always safe.

mod support;

use std::fs;

use dash_compat::FIXTURES;
use store::{
    AnnTuningConfig, FileWal, InMemoryStore, ReplayPolicy, VECTOR_INDEX_FORMAT_VERSION,
    VectorIndexRestore,
};
use support::{diff_against_recorded, run_retrieves};

fn load_with_index(state: &dash_compat::ScratchState) -> (InMemoryStore, VectorIndexRestore) {
    let wal = FileWal::open(state.wal()).expect("open WAL");
    let (store, stats) = InMemoryStore::load_from_wal_with_vector_index(
        &wal,
        AnnTuningConfig::default(),
        ReplayPolicy::Strict,
        Some(&state.vindex()),
    )
    .expect("load");
    (store, stats.vector_index)
}

fn assert_recorded_answers(fixture: &dash_compat::Fixture, state: &dash_compat::ScratchState, store: &InMemoryStore) {
    let answers = run_retrieves(store, Some(&state.segments()));
    let diffs = diff_against_recorded(fixture, &answers);
    assert!(diffs.is_empty(), "{}:\n{}", fixture.label, diffs.join("\n"));
}

#[test]
fn a_saved_index_of_the_current_format_loads_and_serves_the_same_answers() {
    let mut checked = 0;
    for fixture in FIXTURES {
        let state = fixture.scratch_state();
        if !state.vindex().exists() {
            continue;
        }
        let bytes = fs::read(state.vindex()).expect("read vindex");
        assert_eq!(&bytes[..8], b"DASHVIDX", "{}", fixture.label);
        let version = u32::from_le_bytes(bytes[8..12].try_into().expect("4 bytes"));
        assert_eq!(version, VECTOR_INDEX_FORMAT_VERSION, "{}", fixture.label);
        let (store, restore) = load_with_index(&state);
        assert!(restore.is_loaded(), "{}: {}", fixture.label, restore.describe());
        assert_recorded_answers(fixture, &state, &store);
        checked += 1;
    }
    assert!(checked > 0, "at least one fixture carries a persisted index");
}

#[test]
fn a_release_without_the_file_starts_by_building_and_then_saves_it() {
    for fixture in FIXTURES {
        let state = fixture.scratch_state();
        let _ = fs::remove_file(state.vindex());
        let (store, restore) = load_with_index(&state);
        assert_eq!(restore, VectorIndexRestore::Missing, "{}", fixture.label);
        assert_recorded_answers(fixture, &state, &store);
        let position = store.wal_position().expect("position after replay");
        store
            .vector_index_snapshot(position)
            .save(&state.vindex())
            .expect("save");
        let (store, restore) = load_with_index(&state);
        assert!(restore.is_loaded(), "{}: {}", fixture.label, restore.describe());
        assert_recorded_answers(fixture, &state, &store);
    }
}

/// A file written by a newer release (another format version), or one left
/// behind after a downgrade and re-upgrade, is never misread.
#[test]
fn an_index_file_of_another_format_version_is_rebuilt_not_served() {
    for fixture in FIXTURES {
        let state = fixture.scratch_state();
        if !state.vindex().exists() {
            continue;
        }
        let mut bytes = fs::read(state.vindex()).expect("read");
        bytes[8..12].copy_from_slice(&(VECTOR_INDEX_FORMAT_VERSION + 1).to_le_bytes());
        fs::write(state.vindex(), bytes).expect("write");
        let (store, restore) = load_with_index(&state);
        assert!(
            matches!(restore, VectorIndexRestore::Rebuilt { .. }),
            "{}: {}",
            fixture.label,
            restore.describe()
        );
        assert_recorded_answers(fixture, &state, &store);
    }
}

/// After a checkpoint (new WAL generation) the saved index no longer matches
/// and is rebuilt, e.g. when the WAL was compacted by another release.
#[test]
fn an_index_saved_for_another_wal_generation_is_rebuilt() {
    for fixture in FIXTURES {
        let state = fixture.scratch_state();
        if !state.vindex().exists() {
            continue;
        }
        {
            let wal = FileWal::open(state.wal()).expect("open");
            let mut wal = wal;
            let store = InMemoryStore::load_from_wal(&wal).expect("load");
            store.checkpoint_and_compact(&mut wal).expect("checkpoint");
        }
        let (store, restore) = load_with_index(&state);
        assert!(
            matches!(restore, VectorIndexRestore::Rebuilt { .. }),
            "{}: {}",
            fixture.label,
            restore.describe()
        );
        assert_recorded_answers(fixture, &state, &store);
    }
}
