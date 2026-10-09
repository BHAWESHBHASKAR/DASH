//! Single-bundle ingest is one WAL commit group (DATA-10): a crash inside the
//! group replays as nothing, and a retry of an applied bundle is a no-op.

use schema::{Claim, ClaimEdge, Evidence, Relation, Stance};
use store::{FileWal, InMemoryStore};
use tempfile::TempDir;

fn claim(id: &str, text: &str) -> Claim {
    Claim {
        claim_id: id.to_string(),
        tenant_id: "tenant-a".to_string(),
        canonical_text: text.to_string(),
        confidence: 0.9,
        event_time_unix: None,
        entities: vec![],
        embedding_ids: vec![],
        claim_type: None,
        valid_from: None,
        valid_to: None,
        created_at: None,
        updated_at: None,
    }
}

fn evidence(id: &str, claim_id: &str) -> Evidence {
    Evidence {
        evidence_id: id.to_string(),
        claim_id: claim_id.to_string(),
        source_id: "src".to_string(),
        stance: Stance::Supports,
        source_quality: 0.9,
        chunk_id: None,
        span_start: None,
        span_end: None,
        doc_id: None,
        extraction_model: None,
        ingested_at: None,
    }
}

fn edge(id: &str, from: &str, to: &str) -> ClaimEdge {
    ClaimEdge {
        edge_id: id.to_string(),
        from_claim_id: from.to_string(),
        to_claim_id: to.to_string(),
        relation: Relation::Supports,
        strength: 0.5,
        reason_codes: vec![],
        created_at: None,
    }
}

fn ingest(store: &mut InMemoryStore, wal: &mut FileWal, id: &str) -> bool {
    store
        .ingest_atomic_persistent(
            wal,
            claim(id, "Atomic bundle text for the claim"),
            vec![
                evidence(&format!("e-{id}"), id),
                evidence(&format!("e2-{id}"), id),
            ],
            vec![edge(&format!("g-{id}"), id, "other")],
            Some(vec![0.1, 0.2, 0.3]),
            1,
        )
        .expect("ingest")
        .applied
}

#[test]
fn committed_group_replays_completely() {
    let dir = TempDir::new().unwrap();
    let wal_path = dir.path().join("wal.log");
    let mut wal = FileWal::open(&wal_path).unwrap();
    let mut store = InMemoryStore::new();
    assert!(ingest(&mut store, &mut wal, "c1"));
    drop(wal);

    let wal = FileWal::open(&wal_path).unwrap();
    let replayed = InMemoryStore::load_from_wal(&wal).unwrap();
    assert_eq!(replayed.claim_count_for_tenant("tenant-a"), 1);
    assert_eq!(replayed.edges_for_claim("c1").len(), 1);
    // Markers are not registered as batch commits.
    assert!(replayed.batch_commit_metadata("~tx:c1").is_none());
}

#[test]
fn crash_inside_group_replays_zero_partial_bundle() {
    // Cut the log at every byte boundary inside the group: whatever prefix
    // survives, replay must show either nothing or the complete bundle.
    let dir = TempDir::new().unwrap();
    let wal_path = dir.path().join("wal.log");
    {
        let mut wal = FileWal::open(&wal_path).unwrap();
        let mut store = InMemoryStore::new();
        assert!(ingest(&mut store, &mut wal, "c1"));
    }
    let full = std::fs::read(&wal_path).unwrap();
    let mut saw_empty = false;
    for cut in 1..full.len() {
        let case = dir.path().join(format!("cut-{cut}.log"));
        std::fs::write(&case, &full[..cut]).unwrap();
        let wal = FileWal::open(&case).unwrap();
        let replayed = InMemoryStore::load_from_wal(&wal).unwrap();
        let claims = replayed.claim_count_for_tenant("tenant-a");
        let edges = replayed.edges_for_claim("c1").len();
        assert_eq!(claims, edges, "partial bundle visible at cut {cut}");
        saw_empty |= claims == 0;
    }
    assert!(saw_empty);
}

#[test]
fn torn_group_is_truncated_so_later_appends_survive() {
    let dir = TempDir::new().unwrap();
    let wal_path = dir.path().join("wal.log");
    {
        let mut wal = FileWal::open(&wal_path).unwrap();
        let mut store = InMemoryStore::new();
        assert!(ingest(&mut store, &mut wal, "c1"));
    }
    // Drop the closing commit marker line.
    let text = std::fs::read_to_string(&wal_path).unwrap();
    let lines: Vec<&str> = text.lines().collect();
    let without_commit = lines[..lines.len() - 1].join("\n") + "\n";
    std::fs::write(&wal_path, without_commit).unwrap();

    let mut wal = FileWal::open(&wal_path).unwrap();
    let mut store = InMemoryStore::new();
    assert!(ingest(&mut store, &mut wal, "c2"));
    drop(wal);

    let wal = FileWal::open(&wal_path).unwrap();
    let replayed = InMemoryStore::load_from_wal(&wal).unwrap();
    assert!(replayed.claim_by_id("c1").is_none());
    assert!(replayed.claim_by_id("c2").is_some());
    assert_eq!(replayed.edges_for_claim("c2").len(), 1);
}

#[test]
fn retry_of_applied_bundle_is_a_noop() {
    let dir = TempDir::new().unwrap();
    let wal_path = dir.path().join("wal.log");
    let mut wal = FileWal::open(&wal_path).unwrap();
    let mut store = InMemoryStore::new();
    assert!(ingest(&mut store, &mut wal, "c1"));
    let records = wal.wal_record_count().unwrap();
    assert!(!ingest(&mut store, &mut wal, "c1"), "retry must be a no-op");
    assert_eq!(wal.wal_record_count().unwrap(), records);
    assert_eq!(store.claim_count_for_tenant("tenant-a"), 1);
}

#[test]
fn invalid_bundle_leaves_wal_and_store_untouched() {
    let dir = TempDir::new().unwrap();
    let wal_path = dir.path().join("wal.log");
    let mut wal = FileWal::open(&wal_path).unwrap();
    let mut store = InMemoryStore::new();
    let result = store.ingest_atomic_persistent(
        &mut wal,
        claim("c1", "Atomic bundle text for the claim"),
        vec![evidence("e1", "c1")],
        vec![],
        Some(vec![f32::NAN]),
        1,
    );
    assert!(result.is_err());
    assert_eq!(wal.wal_record_count().unwrap(), 0);
    assert_eq!(store.claim_count_for_tenant("tenant-a"), 0);
}
