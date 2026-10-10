//! REP-01 (WAL side): a persistent WAL generation lets followers detect
//! compaction instead of silently skipping records. Also DATA-08 checks.

use schema::claim_builder;
use store::{FileWal, InMemoryStore};
use tempfile::TempDir;

fn append_claims(wal: &mut FileWal, prefix: &str, n: usize) {
    for i in 0..n {
        wal.append_claim(&claim_builder(
            &format!("{prefix}-{i}"),
            "tenant-a",
            &format!("claim text {prefix} {i}"),
            0.9,
        ))
        .unwrap();
    }
}

#[test]
fn follower_is_told_to_resync_after_leader_checkpoint() {
    let dir = TempDir::new().unwrap();
    let mut leader = FileWal::open(dir.path().join("leader.wal")).unwrap();
    let n = 4;
    append_claims(&mut leader, "old", n);

    let frame = leader.replication_frame_from(None, 0, 100).unwrap();
    assert!(!frame.needs_resync);
    assert_eq!(frame.next_offset, n);
    let generation = frame.generation;
    assert_eq!(generation, leader.generation());

    // Follower is caught up at offset N: nothing to send, no resync.
    let caught_up = leader
        .replication_frame_from(Some(generation), n, 100)
        .unwrap();
    assert!(!caught_up.needs_resync);
    assert!(caught_up.wal_lines.is_empty());

    // Leader compacts (WAL resets) and then writes M >= N new records.
    let store = InMemoryStore::load_from_wal(&leader).unwrap();
    store.checkpoint_and_compact(&mut leader).unwrap();
    assert_ne!(
        leader.generation(),
        generation,
        "checkpoint must change generation"
    );
    append_claims(&mut leader, "new", n + 2);

    let frame = leader
        .replication_frame_from(Some(generation), n, 100)
        .unwrap();
    assert!(frame.needs_resync, "must not silently skip records");
    assert!(frame.wal_lines.is_empty());
    assert_eq!(frame.generation, leader.generation());

    // The legacy offset-only API cannot see it; that is the bug being fixed.
    let legacy = leader.replication_delta_from(n, 100).unwrap();
    assert!(!legacy.needs_resync);

    // After resync the follower adopts the new generation and proceeds.
    let ok = leader
        .replication_frame_from(Some(leader.generation()), 0, 100)
        .unwrap();
    assert!(!ok.needs_resync);
    assert_eq!(ok.wal_lines.len(), n + 2);
}

#[test]
fn offset_beyond_total_or_unknown_generation_requires_resync() {
    let dir = TempDir::new().unwrap();
    let mut wal = FileWal::open(dir.path().join("dash.wal")).unwrap();
    append_claims(&mut wal, "c", 3);
    let generation = wal.generation();

    let beyond = wal.replication_frame_from(Some(generation), 10, 5).unwrap();
    assert!(beyond.needs_resync);
    assert_eq!(beyond.next_offset, 3);

    let unknown_midstream = wal.replication_frame_from(None, 2, 5).unwrap();
    assert!(unknown_midstream.needs_resync);
    let fresh = wal.replication_frame_from(None, 0, 5).unwrap();
    assert!(!fresh.needs_resync);
    assert_eq!(fresh.wal_lines.len(), 3);

    let limited = wal.replication_frame_from(Some(generation), 1, 1).unwrap();
    assert_eq!(limited.next_offset, 2);
    assert_eq!(limited.wal_lines.len(), 1);
}

#[test]
fn generation_is_persistent_and_stable_until_compaction() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("dash.wal");
    let g1 = {
        let mut wal = FileWal::open(&path).unwrap();
        append_claims(&mut wal, "c", 2);
        assert!(wal.generation_path().exists());
        wal.generation()
    };
    let mut wal = FileWal::open(&path).unwrap();
    assert_eq!(wal.generation(), g1, "generation survives restart");
    assert_eq!(wal.replay_boundary().unwrap().wal_generation, g1);

    let store = InMemoryStore::load_from_wal(&wal).unwrap();
    store.checkpoint_and_compact(&mut wal).unwrap();
    let g2 = wal.generation();
    assert_ne!(g1, g2);
    drop(wal);
    let wal = FileWal::open(&path).unwrap();
    assert_eq!(wal.generation(), g2);
}

#[test]
fn checkpoint_leaves_durable_snapshot_and_empty_wal() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("dash.wal");
    let mut wal = FileWal::open(&path).unwrap();
    append_claims(&mut wal, "c", 3);
    let store = InMemoryStore::load_from_wal(&wal).unwrap();
    let stats = store.checkpoint_and_compact(&mut wal).unwrap();
    assert_eq!(stats.truncated_wal_records, 3);
    assert_eq!(std::fs::metadata(&path).unwrap().len(), 0);
    assert!(wal.snapshot_path().exists());
    assert!(!wal.snapshot_path().with_extension("snapshot.tmp").exists());
    drop(wal);

    let wal = FileWal::open(&path).unwrap();
    let restored = InMemoryStore::load_from_wal(&wal).unwrap();
    assert_eq!(restored.claims_len(), 3);
}
