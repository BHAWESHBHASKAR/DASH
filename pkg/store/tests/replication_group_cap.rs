//! A commit group the leader cannot ship whole must be an explicit error, not
//! a frame the follower can never make progress on.

use schema::claim_builder;
use store::{FileWal, StoreError};
use tempfile::TempDir;

fn group_wal(path: &std::path::Path, members: usize) -> FileWal {
    let mut wal = FileWal::open(path).unwrap();
    wal.begin_group("big", 1).unwrap();
    for i in 0..members {
        wal.append_claim(&claim_builder(&format!("c{i}"), "t", "text", 0.5))
            .unwrap();
    }
    wal.append_batch_commit("big", members, 2, &[]).unwrap();
    wal.flush_pending_sync().unwrap();
    wal
}

#[test]
fn frame_is_extended_to_the_end_of_a_group_within_the_cap() {
    let dir = TempDir::new().unwrap();
    let mut wal = group_wal(&dir.path().join("w"), 30);
    wal.set_replication_group_cap(100);
    let frame = wal.replication_frame_from(None, 0, 5).unwrap();
    assert_eq!(frame.wal_lines.len(), 32, "begin + 30 claims + end");
    assert_eq!(frame.next_offset, 32);
}

#[test]
fn group_larger_than_the_cap_is_an_explicit_error_and_counted() {
    let dir = TempDir::new().unwrap();
    let mut wal = group_wal(&dir.path().join("w"), 200);
    wal.set_replication_group_cap(50);
    let err = wal.replication_frame_from(None, 0, 5).unwrap_err();
    match err {
        StoreError::Io(msg) => assert!(msg.contains("replication_group_too_large"), "{msg}"),
        other => panic!("unexpected error {other:?}"),
    }
    assert_eq!(wal.replication_group_too_large_total(), 1);
}

#[test]
fn unterminated_group_at_the_tail_is_not_an_error() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("w");
    let mut wal = FileWal::open(&path).unwrap();
    wal.begin_group("open", 1).unwrap();
    for i in 0..20 {
        wal.append_claim(&claim_builder(&format!("c{i}"), "t", "text", 0.5))
            .unwrap();
    }
    wal.flush_pending_sync().unwrap();
    wal.set_replication_group_cap(10);
    let frame = wal.replication_frame_from(None, 0, 5).unwrap();
    assert!(!frame.needs_resync);
    assert_eq!(wal.replication_group_too_large_total(), 0);
}
