//! Store apply-side semantics: idempotent re-apply (DATA-01), validation
//! before the WAL append (DATA-03), vector survival on claim re-upsert
//! (DATA-04), detached staged clones (DATA-09) and the bounded in-memory
//! event ring (PERF-05).

use schema::{Claim, ClaimEdge, Evidence, Relation, RetrievalRequest, Stance, StanceMode};
use store::{AnnTuningConfig, FileWal, InMemoryStore, StoreError};
use tempfile::TempDir;

fn claim(id: &str, tenant: &str, text: &str) -> Claim {
    Claim {
        claim_id: id.to_string(),
        tenant_id: tenant.to_string(),
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

fn evidence(id: &str, claim_id: &str, source: &str, stance: Stance) -> Evidence {
    Evidence {
        evidence_id: id.to_string(),
        claim_id: claim_id.to_string(),
        source_id: source.to_string(),
        stance,
        source_quality: 0.9,
        chunk_id: None,
        span_start: None,
        span_end: None,
        doc_id: None,
        extraction_model: None,
        ingested_at: None,
    }
}

fn edge(id: &str, from: &str, to: &str, relation: Relation) -> ClaimEdge {
    ClaimEdge {
        edge_id: id.to_string(),
        from_claim_id: from.to_string(),
        to_claim_id: to.to_string(),
        relation,
        strength: 0.7,
        reason_codes: vec![],
        created_at: None,
    }
}

fn request(tenant: &str, query: &str) -> RetrievalRequest {
    RetrievalRequest {
        tenant_id: tenant.to_string(),
        query: query.to_string(),
        top_k: 50,
        stance_mode: StanceMode::Balanced,
    }
}

fn citation_count(store: &InMemoryStore, tenant: &str, query: &str, claim_id: &str) -> usize {
    store
        .retrieve(&request(tenant, query))
        .iter()
        .find(|r| r.claim_id == claim_id)
        .unwrap_or_else(|| panic!("claim {claim_id} should be retrievable"))
        .citations
        .len()
}

fn bundle_parts() -> (Claim, Vec<Evidence>, Vec<ClaimEdge>) {
    (
        claim("c1", "t1", "alpha reactor shutdown"),
        vec![
            evidence("e1", "c1", "src-a", Stance::Supports),
            evidence("e2", "c1", "src-b", Stance::Supports),
        ],
        vec![edge("g1", "c1", "c2", Relation::Supports)],
    )
}

// ---------------------------------------------------------------------------
// DATA-01
// ---------------------------------------------------------------------------

#[test]
fn applying_the_same_bundle_many_times_keeps_exactly_one_copy() {
    let mut store = InMemoryStore::new();
    for _ in 0..5 {
        let (c, e, g) = bundle_parts();
        store.ingest_bundle(c, e, g).unwrap();
    }
    assert_eq!(citation_count(&store, "t1", "alpha", "c1"), 2);
    assert_eq!(store.edges_for_claim("c1").len(), 1);
    assert_eq!(store.claims_len(), 1);
}

#[test]
fn evidence_is_upserted_by_evidence_id() {
    let mut store = InMemoryStore::new();
    store
        .ingest_bundle(
            claim("c1", "t1", "alpha"),
            vec![evidence("e1", "c1", "src", Stance::Supports)],
            vec![],
        )
        .unwrap();
    store
        .ingest_bundle(
            claim("c1", "t1", "alpha"),
            vec![evidence("e1", "c1", "src", Stance::Contradicts)],
            vec![],
        )
        .unwrap();
    let result = &store.retrieve(&request("t1", "alpha"))[0];
    assert_eq!(result.citations.len(), 1, "same evidence_id must replace");
    assert!(matches!(result.citations[0].stance, Stance::Contradicts));
}

#[test]
fn edges_are_upserted_by_from_to_relation() {
    let mut store = InMemoryStore::new();
    store
        .ingest_bundle(claim("c1", "t1", "alpha"), vec![], vec![])
        .unwrap();
    for id in ["g1", "g2", "g3"] {
        store
            .ingest_bundle(
                claim("c1", "t1", "alpha"),
                vec![],
                vec![edge(id, "c1", "c2", Relation::Supports)],
            )
            .unwrap();
    }
    assert_eq!(store.edges_for_claim("c1").len(), 1);
    store
        .ingest_bundle(
            claim("c1", "t1", "alpha"),
            vec![],
            vec![edge("g4", "c1", "c2", Relation::Contradicts)],
        )
        .unwrap();
    assert_eq!(
        store.edges_for_claim("c1").len(),
        2,
        "a different relation is a different edge"
    );
}

#[test]
fn wal_reload_cycles_and_duplicate_wal_appends_never_duplicate() {
    let tmp = TempDir::new().unwrap();
    let wal_path = tmp.path().join("a.wal");
    let mut wal = FileWal::open(&wal_path).unwrap();
    let mut store = InMemoryStore::new();
    // The same bundle appended to the WAL three times (client retry).
    for _ in 0..3 {
        let (c, e, g) = bundle_parts();
        store.ingest_bundle_persistent(&mut wal, c, e, g).unwrap();
    }
    drop(wal);

    for _ in 0..5 {
        let wal = FileWal::open(&wal_path).unwrap();
        let reloaded = InMemoryStore::load_from_wal(&wal).unwrap();
        assert_eq!(citation_count(&reloaded, "t1", "alpha", "c1"), 2);
        assert_eq!(reloaded.edges_for_claim("c1").len(), 1);
    }
}

#[test]
fn disk_plus_wal_reload_does_not_grow_evidence_across_five_cycles() {
    // Reproduces the reported 2 -> 3 -> 4 citation growth: the redb
    // snapshot already holds the evidence and the WAL replays it again.
    let tmp = TempDir::new().unwrap();
    let wal_path = tmp.path().join("b.wal");
    let disk_path = tmp.path().join("b.redb");

    {
        let mut wal = FileWal::open(&wal_path).unwrap();
        let mut store = InMemoryStore::new().with_disk(&disk_path).unwrap();
        let (c, e, g) = bundle_parts();
        store.ingest_bundle_persistent(&mut wal, c, e, g).unwrap();
        assert_eq!(citation_count(&store, "t1", "alpha", "c1"), 2);
    }

    for cycle in 0..5 {
        let mut wal = FileWal::open(&wal_path).unwrap();
        let (store, _stats) =
            InMemoryStore::load_from_disk_and_wal(&disk_path, &mut wal, AnnTuningConfig::default())
                .unwrap();
        assert_eq!(
            citation_count(&store, "t1", "alpha", "c1"),
            2,
            "evidence grew on reload cycle {cycle}"
        );
        assert_eq!(
            store.edges_for_claim("c1").len(),
            1,
            "edges grew on reload cycle {cycle}"
        );
        assert_eq!(store.claims_len(), 1);
    }
}

#[test]
fn replication_reapply_is_idempotent() {
    let tmp = TempDir::new().unwrap();
    let mut leader_wal = FileWal::open(tmp.path().join("leader.wal")).unwrap();
    let mut leader = InMemoryStore::new();
    let (c, e, g) = bundle_parts();
    leader
        .ingest_bundle_persistent(&mut leader_wal, c, e, g)
        .unwrap();
    leader
        .upsert_claim_vector_persistent(&mut leader_wal, "c1", vec![1.0, 0.0])
        .unwrap();
    let export = leader_wal.replication_export().unwrap();
    let lines: Vec<String> = export
        .snapshot_lines
        .into_iter()
        .chain(export.wal_lines)
        .collect();

    let mut follower = InMemoryStore::new();
    for _ in 0..4 {
        for line in &lines {
            follower.apply_persisted_record_line(line).unwrap();
        }
    }
    assert_eq!(citation_count(&follower, "t1", "alpha", "c1"), 2);
    assert_eq!(follower.edges_for_claim("c1").len(), 1);
    assert_eq!(follower.index_stats().vector_count, 1);
}

// ---------------------------------------------------------------------------
// DATA-03
// ---------------------------------------------------------------------------

#[test]
fn rejected_vector_upserts_never_reach_the_wal() {
    let tmp = TempDir::new().unwrap();
    let wal_path = tmp.path().join("c.wal");
    let mut wal = FileWal::open(&wal_path).unwrap();
    let mut store = InMemoryStore::new();
    store
        .ingest_bundle_persistent(&mut wal, claim("c1", "t1", "alpha"), vec![], vec![])
        .unwrap();
    store
        .upsert_claim_vector_persistent(&mut wal, "c1", vec![1.0, 2.0, 3.0])
        .unwrap();
    store
        .ingest_bundle_persistent(&mut wal, claim("c2", "t1", "beta"), vec![], vec![])
        .unwrap();
    let before = wal.wal_record_count().unwrap();

    let missing = store.upsert_claim_vector_persistent(&mut wal, "ghost", vec![1.0, 2.0, 3.0]);
    assert!(matches!(missing, Err(StoreError::MissingClaim(_))));
    let wrong_dim = store.upsert_claim_vector_persistent(&mut wal, "c2", vec![1.0, 2.0]);
    assert!(matches!(wrong_dim, Err(StoreError::InvalidVector(_))));
    let non_finite = store.upsert_claim_vector_persistent(&mut wal, "c2", vec![f32::NAN, 1.0, 1.0]);
    assert!(matches!(non_finite, Err(StoreError::InvalidVector(_))));
    let infinite =
        store.upsert_claim_vector_persistent(&mut wal, "c2", vec![f32::INFINITY, 1.0, 1.0]);
    assert!(matches!(infinite, Err(StoreError::InvalidVector(_))));

    assert_eq!(
        wal.wal_record_count().unwrap(),
        before,
        "rejected requests must not be appended"
    );
    // Replay must still succeed (old code poisoned it with InvalidVector).
    let reloaded = InMemoryStore::load_from_wal(&wal).expect("replay must not be poisoned");
    assert_eq!(reloaded.claims_len(), 2);
}

#[test]
fn rejected_bundles_never_reach_the_wal() {
    let tmp = TempDir::new().unwrap();
    let mut wal = FileWal::open(tmp.path().join("d.wal")).unwrap();
    let mut store = InMemoryStore::new();
    store
        .ingest_bundle_persistent(&mut wal, claim("c1", "t1", "alpha"), vec![], vec![])
        .unwrap();
    let before = wal.wal_record_count().unwrap();

    // Evidence for a different claim than the bundle's.
    let err = store.ingest_bundle_persistent(
        &mut wal,
        claim("c9", "t1", "gamma"),
        vec![evidence("e1", "other", "src", Stance::Supports)],
        vec![],
    );
    assert!(matches!(err, Err(StoreError::MissingClaim(_))));
    // Cross-tenant claim_id conflict.
    let err = store.ingest_bundle_persistent(&mut wal, claim("c1", "t2", "alpha"), vec![], vec![]);
    assert!(matches!(err, Err(StoreError::Conflict(_))));
    assert_eq!(wal.wal_record_count().unwrap(), before);
}

// ---------------------------------------------------------------------------
// DATA-04
// ---------------------------------------------------------------------------

#[test]
fn claim_reupsert_keeps_vector_and_ann_entry() {
    let mut store = InMemoryStore::new();
    store
        .ingest_bundle(claim("c1", "t1", "alpha original"), vec![], vec![])
        .unwrap();
    store
        .ingest_bundle(claim("c2", "t1", "beta other"), vec![], vec![])
        .unwrap();
    store.upsert_claim_vector("c1", vec![1.0, 0.0]).unwrap();
    store.upsert_claim_vector("c2", vec![0.0, 1.0]).unwrap();

    store
        .ingest_bundle(claim("c1", "t1", "alpha rewritten"), vec![], vec![])
        .unwrap();

    assert_eq!(store.index_stats().vector_count, 2);
    assert_eq!(
        store.exact_vector_top_candidates("t1", &[1.0, 0.0], 1),
        vec!["c1".to_string()]
    );
    assert_eq!(
        store.ann_vector_top_candidates("t1", &[1.0, 0.0], 2)[0],
        "c1".to_string()
    );
    // A later vector for the same claim (different dimension) is still
    // checked against the tenant dimension, which was not reset.
    assert!(
        store
            .upsert_claim_vector("c1", vec![1.0, 0.0, 0.0])
            .is_err()
    );
}

#[test]
fn claim_reupsert_keeps_memory_and_disk_in_agreement() {
    let tmp = TempDir::new().unwrap();
    let disk_path = tmp.path().join("e.redb");
    let wal_path = tmp.path().join("e.wal");
    {
        let mut store = InMemoryStore::new().with_disk(&disk_path).unwrap();
        store
            .ingest_bundle(claim("c1", "t1", "alpha original"), vec![], vec![])
            .unwrap();
        store.upsert_claim_vector("c1", vec![0.5, 0.5]).unwrap();
        store
            .ingest_bundle(claim("c1", "t1", "alpha rewritten"), vec![], vec![])
            .unwrap();
        assert_eq!(store.index_stats().vector_count, 1, "memory kept it");
    }
    let mut wal = FileWal::open(&wal_path).unwrap();
    let (reloaded, _) =
        InMemoryStore::load_from_disk_and_wal(&disk_path, &mut wal, AnnTuningConfig::default())
            .unwrap();
    assert_eq!(reloaded.index_stats().vector_count, 1, "disk kept it too");
    assert_eq!(
        reloaded.claim_by_id("c1").unwrap().canonical_text,
        "alpha rewritten"
    );
}

// ---------------------------------------------------------------------------
// DATA-09
// ---------------------------------------------------------------------------

fn reload_from_disk(disk_path: &std::path::Path, wal_path: &std::path::Path) -> InMemoryStore {
    let mut wal = FileWal::open(wal_path).unwrap();
    InMemoryStore::load_from_disk_and_wal(disk_path, &mut wal, AnnTuningConfig::default())
        .unwrap()
        .0
}

#[test]
fn dropped_staged_clone_never_touches_redb() {
    let tmp = TempDir::new().unwrap();
    let disk_path = tmp.path().join("f.redb");
    let wal_path = tmp.path().join("f.wal");
    {
        let mut live = InMemoryStore::new().with_disk(&disk_path).unwrap();
        live.ingest_bundle(claim("kept", "t1", "kept claim"), vec![], vec![])
            .unwrap();

        // Plain `clone()` is detached: a rolled-back batch must not leak.
        let mut staged = live.clone();
        staged
            .ingest_bundle(
                claim("rolled-back", "t1", "rolled back claim"),
                vec![evidence("e1", "rolled-back", "src", Stance::Supports)],
                vec![],
            )
            .unwrap();
        drop(staged);
        assert_eq!(live.claims_len(), 1);
    }
    let reloaded = reload_from_disk(&disk_path, &wal_path);
    assert!(reloaded.claim_by_id("kept").is_some());
    assert!(
        reloaded.claim_by_id("rolled-back").is_none(),
        "staged writes leaked into redb before the WAL commit"
    );
}

#[test]
fn commit_staged_writes_redb_and_keeps_the_disk_handle() {
    let tmp = TempDir::new().unwrap();
    let disk_path = tmp.path().join("g.redb");
    let wal_path = tmp.path().join("g.wal");
    {
        let mut live = InMemoryStore::new().with_disk(&disk_path).unwrap();
        live.ingest_bundle(claim("c0", "t1", "zero"), vec![], vec![])
            .unwrap();

        let mut staged = live.clone_detached();
        staged
            .ingest_bundle(
                claim("c1", "t1", "one"),
                vec![evidence("e1", "c1", "src", Stance::Supports)],
                vec![edge("g1", "c1", "c0", Relation::Supports)],
            )
            .unwrap();
        staged.upsert_claim_vector("c1", vec![1.0, 2.0]).unwrap();
        // (the caller's WAL append would happen here)
        live.commit_staged(staged).unwrap();
        assert!(matches!(live.disk_status(), store::DiskStatus::Available));
        assert_eq!(live.claims_len(), 2);

        // The handle survived the swap: later direct writes still hit redb.
        live.ingest_bundle(claim("c2", "t1", "two"), vec![], vec![])
            .unwrap();
    }
    let reloaded = reload_from_disk(&disk_path, &wal_path);
    assert_eq!(reloaded.claims_len(), 3);
    assert_eq!(citation_count(&reloaded, "t1", "one", "c1"), 1);
    assert_eq!(reloaded.edges_for_claim("c1").len(), 1);
    assert_eq!(reloaded.index_stats().vector_count, 1);
}

#[test]
fn commit_staged_on_a_diskless_store_just_swaps_state() {
    let mut live = InMemoryStore::new();
    let mut staged = live.clone_detached();
    staged
        .ingest_bundle(claim("c1", "t1", "one"), vec![], vec![])
        .unwrap();
    assert_eq!(
        live.claims_len(),
        0,
        "staging must not affect the live store"
    );
    live.commit_staged(staged).unwrap();
    assert_eq!(live.claims_len(), 1);
    assert!(live.wal_len() >= 1);
}

// ---------------------------------------------------------------------------
// PERF-05
// ---------------------------------------------------------------------------

#[test]
fn in_memory_wal_event_buffer_is_bounded() {
    let mut store = InMemoryStore::new();
    store.set_wal_event_capacity(16);
    for i in 0..200 {
        store
            .ingest_bundle(claim(&format!("c{i}"), "t1", "alpha"), vec![], vec![])
            .unwrap();
    }
    assert!(store.wal_len() <= 16, "buffer grew to {}", store.wal_len());
    assert_eq!(store.wal_events_total(), 200);
    assert_eq!(store.claims_len(), 200);

    store.set_wal_event_capacity(0);
    assert_eq!(store.wal_len(), 0);
    store
        .ingest_bundle(claim("extra", "t1", "alpha"), vec![], vec![])
        .unwrap();
    assert_eq!(store.wal_len(), 0);
    assert_eq!(store.wal_events_total(), 201);
}

#[test]
fn default_wal_event_buffer_has_a_finite_capacity() {
    let store = InMemoryStore::new();
    assert!(store.wal_event_capacity() > 0);
    assert!(store.wal_event_capacity() <= 1_000_000);
}
