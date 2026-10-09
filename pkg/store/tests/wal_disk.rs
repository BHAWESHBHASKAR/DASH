//! redb layer: single-transaction claim writes and idempotent evidence/edge
//! storage.

use schema::{ClaimEdge, Evidence, Relation, Stance, claim_builder};
use store::DiskBackedStore;
use tempfile::TempDir;

fn evidence(id: &str, claim: &str, quality: f32) -> Evidence {
    Evidence {
        evidence_id: id.to_string(),
        claim_id: claim.to_string(),
        source_id: "source://x".to_string(),
        stance: Stance::Supports,
        source_quality: quality,
        chunk_id: None,
        span_start: None,
        span_end: None,
        doc_id: None,
        extraction_model: None,
        ingested_at: None,
    }
}

fn edge(id: &str, from: &str, to: &str, strength: f32) -> ClaimEdge {
    ClaimEdge {
        edge_id: id.to_string(),
        from_claim_id: from.to_string(),
        to_claim_id: to.to_string(),
        relation: Relation::Supports,
        strength,
        reason_codes: vec![],
        created_at: None,
    }
}

#[test]
fn put_claim_records_tenant_membership_in_the_same_transaction() {
    let dir = TempDir::new().unwrap();
    let disk = DiskBackedStore::new(dir.path().join("dash.redb")).unwrap();
    disk.put_claim(&claim_builder("c1", "tenant-a", "text", 0.9))
        .unwrap();
    // No separate add_claim_to_tenant call is needed.
    assert!(disk.claim_in_tenant("tenant-a", "c1").unwrap());
    // Explicit call stays idempotent.
    disk.add_claim_to_tenant("tenant-a", "c1").unwrap();
    let mut seen = Vec::new();
    disk.for_each_claim_in_tenant("tenant-a", &mut |c| seen.push(c.to_string()))
        .unwrap();
    assert_eq!(seen, vec!["c1".to_string()]);
}

#[test]
fn evidence_upsert_is_idempotent_per_evidence_id() {
    let dir = TempDir::new().unwrap();
    let disk = DiskBackedStore::new(dir.path().join("dash.redb")).unwrap();
    disk.upsert_evidence(&evidence("e1", "c1", 0.5)).unwrap();
    disk.upsert_evidence(&evidence("e1", "c1", 0.9)).unwrap();
    disk.upsert_evidence(&evidence("e2", "c1", 0.4)).unwrap();
    let blob = disk.get_evidence_blob("c1").unwrap().unwrap();
    assert_eq!(blob.len(), 2);
    assert_eq!(blob[0].evidence_id, "e1");
    assert_eq!(blob[0].source_quality, 0.9);
}

#[test]
fn blob_writes_collapse_duplicates_so_reload_cannot_duplicate() {
    let dir = TempDir::new().unwrap();
    let disk = DiskBackedStore::new(dir.path().join("dash.redb")).unwrap();
    // The legacy read-append-write pattern used to grow without bound.
    for _ in 0..3 {
        let mut current = disk.get_evidence_blob("c1").unwrap().unwrap_or_default();
        current.push(evidence("e1", "c1", 0.5));
        disk.put_evidence_blob("c1", &current).unwrap();
        let mut edges = disk.get_edge_blob("c1").unwrap().unwrap_or_default();
        edges.push(edge("g-anything", "c1", "c2", 0.5));
        disk.put_edge_blob("c1", &edges).unwrap();
    }
    assert_eq!(disk.get_evidence_blob("c1").unwrap().unwrap().len(), 1);
    assert_eq!(disk.get_edge_blob("c1").unwrap().unwrap().len(), 1);
}

#[test]
fn edge_upsert_is_keyed_by_from_to_relation() {
    let dir = TempDir::new().unwrap();
    let disk = DiskBackedStore::new(dir.path().join("dash.redb")).unwrap();
    disk.upsert_edge(&edge("g1", "a", "b", 0.5)).unwrap();
    disk.upsert_edge(&edge("g2", "a", "b", 0.8)).unwrap();
    disk.upsert_edge(&edge("g3", "a", "c", 0.1)).unwrap();
    let blob = disk.get_edge_blob("a").unwrap().unwrap();
    assert_eq!(blob.len(), 2);
    assert_eq!(blob[0].strength, 0.8);
}
