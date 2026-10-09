//! Replication over a leader WAL that holds legacy lines the lenient replay
//! quarantines: the leader must not serve them, and a follower must tolerate
//! them (skip + count) instead of wedging.

use std::collections::BTreeSet;
use std::path::Path;

use store::{AnnTuningConfig, FileWal, InMemoryStore, ReplayPolicy};
use tempfile::TempDir;

const TAIL: &str = "null\tnull\tnull\tnull\tnull";

fn legacy_claim(id: &str, entities: &str) -> String {
    format!("C\t{id}\ttenant-a\ttext of {id}\t0.9\tnull\t{entities}\t\t{TAIL}")
}

fn legacy_evidence(id: &str, claim: &str) -> String {
    format!("E\t{id}\t{claim}\tsource-1\tsupports\t0.8")
}

fn legacy_edge(id: &str, from: &str, to: &str) -> String {
    format!("G\t{id}\t{from}\t{to}\tsupports\t0.5")
}

fn legacy_vector(claim: &str, values: &str) -> String {
    format!("V\t{claim}\t{values}")
}

/// Healthy records mixed with lines an old release could have left behind:
/// a control character in an id, an unparseable entity list, dependents of
/// those claims and poisoned vectors.
fn poisoned_legacy_lines() -> Vec<String> {
    vec![
        legacy_claim("c-ok", "3:foo"),
        legacy_claim("c\\tbad", ""),
        legacy_claim("c-ok2", "3:bar"),
        legacy_claim("c-ent", "5:a\tb c"),
        legacy_evidence("e-ok", "c-ok"),
        legacy_evidence("e-dep-a", "c\\tbad"),
        legacy_evidence("e-dep-b", "c-ent"),
        legacy_edge("g-dep", "c-ok", "c\\tbad"),
        legacy_edge("g-ok", "c-ok", "c-ok2"),
        legacy_vector("c-ok", "1,2,3"),
        legacy_vector("c-ok2", "1,2"),
        legacy_vector("c-ok", "1,NaN,3"),
        legacy_vector("ghost", "1,2,3"),
        "B\tcommit-1\t2\t1700000000000\t4:c-ok5:c-ok2".to_string(),
    ]
}

fn write_leader_wal(path: &Path) -> FileWal {
    std::fs::write(path, poisoned_legacy_lines().join("\n") + "\n").unwrap();
    FileWal::open(path).unwrap()
}

fn state_of(store: &InMemoryStore) -> (BTreeSet<String>, usize) {
    let claims = store
        .claims_for_tenant("tenant-a")
        .into_iter()
        .map(|c| c.claim_id)
        .collect();
    let edges: usize = ["c-ok", "c-ok2", "c-ent", "c\tbad"]
        .iter()
        .map(|id| store.edges_for_claim(id).len())
        .sum();
    (claims, edges)
}

#[test]
fn leader_frames_and_exports_skip_lines_that_lenient_replay_quarantines() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("leader.wal");
    let mut wal = write_leader_wal(&path);

    let frame = wal.replication_frame_from(None, 0, 1000).unwrap();
    let export = wal.replication_export().unwrap();
    for lines in [&frame.wal_lines, &export.wal_lines] {
        // The unparseable entity list and everything depending on it are gone.
        assert!(!lines.iter().any(|l| l.contains("c-ent")), "{lines:?}");
        assert!(!lines.iter().any(|l| l.contains("e-dep-b")), "{lines:?}");
        // Every served line is readable by the strict parser.
        let mut probe = InMemoryStore::new();
        for line in lines.iter() {
            if line.starts_with("V\tc-ok2") || line.contains("c\\tbad") || line.contains("ghost")
            {
                continue; // validation-level poison is the follower's job
            }
            probe
                .apply_persisted_record_line(line)
                .unwrap_or_else(|e| panic!("{line:?}: {e:?}"));
        }
    }
    assert_eq!(frame.total_records, frame.wal_lines.len());
    assert_eq!(frame.next_offset, frame.wal_lines.len());
    assert!(wal.replication_skipped_lines() > 0);
}

#[test]
fn follower_converges_to_the_leader_lenient_state_over_raw_poisoned_lines() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("leader.wal");
    let wal = write_leader_wal(&path);
    let (leader, _) =
        InMemoryStore::load_from_wal_with_policy(&wal, AnnTuningConfig::default(), ReplayPolicy::Lenient)
            .unwrap();
    let expected = state_of(&leader);
    assert_eq!(expected.0.len(), 2);

    // Strict apply wedges on the raw lines ...
    let mut strict = InMemoryStore::new();
    assert!(
        poisoned_legacy_lines()
            .iter()
            .any(|line| strict.apply_persisted_record_line(line).is_err())
    );

    // ... the lenient follower apply skips and counts them.
    let mut follower = InMemoryStore::new();
    let mut skipped = 0;
    for line in poisoned_legacy_lines() {
        if !follower.apply_persisted_record_line_lenient(&line).unwrap() {
            skipped += 1;
        }
    }
    assert!(skipped >= 6, "skipped {skipped}");
    assert_eq!(state_of(&follower), expected);
}

#[test]
fn follower_applies_leader_filtered_lines_and_matches_the_leader() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("leader.wal");
    let mut wal = write_leader_wal(&path);
    let (leader, _) =
        InMemoryStore::load_from_wal_with_policy(&wal, AnnTuningConfig::default(), ReplayPolicy::Lenient)
            .unwrap();
    let frame = wal.replication_frame_from(None, 0, 1000).unwrap();
    let mut follower = InMemoryStore::new();
    for line in &frame.wal_lines {
        follower.apply_persisted_record_line_lenient(line).unwrap();
    }
    assert_eq!(state_of(&follower), state_of(&leader));
}

#[test]
fn follower_wal_mirrors_poisoned_lines_without_wedging() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("follower.wal");
    let mut wal = FileWal::open(&path).unwrap();
    for line in poisoned_legacy_lines() {
        wal.append_raw_record_line(&line)
            .unwrap_or_else(|e| panic!("{line:?}: {e:?}"));
    }
    assert_eq!(wal.wal_record_count().unwrap(), poisoned_legacy_lines().len());
}
