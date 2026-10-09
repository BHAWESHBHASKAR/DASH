//! `commit_staged` must append only the events produced on the staged clone
//! to the live event ring (no duplication of events the ring already holds).

use schema::claim_builder;
use store::InMemoryStore;

#[test]
fn commit_staged_appends_only_new_events() {
    let mut live = InMemoryStore::new();
    for i in 0..3 {
        live.ingest_bundle(
            claim_builder(&format!("c{i}"), "t", "text", 0.5),
            vec![],
            vec![],
        )
        .unwrap();
    }
    let before_len = live.wal_len();
    let before_total = live.wal_events_total();
    assert!(before_len >= 3);

    let mut staged = live.clone_detached();
    staged
        .ingest_bundle(claim_builder("c-new", "t", "more", 0.5), vec![], vec![])
        .unwrap();
    let staged_new = staged.wal_events_total() - before_total;
    assert!(staged_new >= 1);

    live.commit_staged(staged).unwrap();
    assert_eq!(live.wal_len(), before_len + staged_new as usize);
    assert_eq!(live.wal_events_total(), before_total + staged_new);
    assert!(live.claim_by_id("c-new").is_some());

    // A second round trip with a clone that applied nothing adds nothing.
    let staged = live.clone_detached();
    let len = live.wal_len();
    live.commit_staged(staged).unwrap();
    assert_eq!(live.wal_len(), len);
}
