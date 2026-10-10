//! The per-tenant full-text index seen through the store: candidates are
//! the top N by BM25, updates and tombstones change what matches and the
//! BM25 statistics, tenants do not see each other's statistics, and a
//! replication follower, a restarted node and a resynced node answer every
//! query with bit-identical scores.

use schema::{Claim, RetrievalRequest, RetrievalResult, StanceMode, claim_builder};
use store::{FileWal, InMemoryStore, Tombstone, candidate_depth};
use tempfile::TempDir;

const TS: u64 = 1_700_000_000_000;

fn request(tenant: &str, query: &str, top_k: usize) -> RetrievalRequest {
    RetrievalRequest {
        tenant_id: tenant.to_string(),
        query: query.to_string(),
        top_k,
        stance_mode: StanceMode::Balanced,
    }
}

fn ids(results: &[RetrievalResult]) -> Vec<&str> {
    results.iter().map(|r| r.claim_id.as_str()).collect()
}

/// Claim ids with exact score bits, for comparing two stores.
fn answer(results: Vec<RetrievalResult>) -> Vec<(String, u32)> {
    results
        .into_iter()
        .map(|r| (r.claim_id, r.score.to_bits()))
        .collect()
}

fn ingest(store: &mut InMemoryStore, claim: Claim) {
    store.ingest_bundle(claim, vec![], vec![]).unwrap();
}

#[test]
fn updates_and_tombstones_change_matches_and_statistics() {
    let mut store = InMemoryStore::new();
    ingest(
        &mut store,
        claim_builder("a", "t1", "Coolant pump failure at reactor two", 0.8),
    );
    ingest(
        &mut store,
        claim_builder("b", "t1", "Reactor inspection passed", 0.8),
    );
    ingest(
        &mut store,
        claim_builder("c", "t1", "Pumps were replaced at the reactor", 0.8),
    );
    assert_eq!(
        ids(&store.retrieve(&request("t1", "pump", 10))),
        vec!["a", "c"]
    );

    // Re-upsert with new text: the old terms stop matching, the new match.
    ingest(
        &mut store,
        claim_builder("a", "t1", "Turbine vibration alarm", 0.8),
    );
    assert_eq!(ids(&store.retrieve(&request("t1", "pump", 10))), vec!["c"]);
    assert_eq!(
        ids(&store.retrieve(&request("t1", "turbines", 10))),
        vec!["a"]
    );

    // A claim tombstone removes the claim from the index.
    store
        .delete(Tombstone::Claim {
            tenant_id: "t1".into(),
            claim_id: "c".into(),
        })
        .unwrap();
    assert!(store.retrieve(&request("t1", "pump", 10)).is_empty());

    // Statistics (document count, document frequency, average length)
    // describe the live claims only: scores equal those of a store that
    // never held the deleted or replaced texts.
    let mut fresh = InMemoryStore::new();
    ingest(
        &mut fresh,
        claim_builder("a", "t1", "Turbine vibration alarm", 0.8),
    );
    ingest(
        &mut fresh,
        claim_builder("b", "t1", "Reactor inspection passed", 0.8),
    );
    for query in ["reactor", "turbine alarm", "inspection reactor vibration"] {
        assert_eq!(
            answer(store.retrieve(&request("t1", query, 10))),
            answer(fresh.retrieve(&request("t1", query, 10))),
            "{query}"
        );
    }

    // A tenant tombstone drops the tenant's index.
    store
        .delete(Tombstone::Tenant {
            tenant_id: "t1".into(),
        })
        .unwrap();
    assert!(store.retrieve(&request("t1", "reactor", 10)).is_empty());
    assert_eq!(store.index_stats().inverted_terms, 0);
}

#[test]
fn lexical_candidates_are_the_top_n_by_bm25_after_filters() {
    let mut store = InMemoryStore::new();
    // 300 claims share "report"; only some also mention "tariff".
    for i in 0..300 {
        let text = if i % 50 == 0 {
            format!("report {i} on the steel tariff")
        } else {
            format!("report {i} on unrelated matters")
        };
        ingest(
            &mut store,
            claim_builder(&format!("c{i:03}"), "t1", &text, 0.7),
        );
    }
    let depth = candidate_depth(5);
    assert_eq!(depth, 100);
    // Every claim shares "report": the old rule scored all 300.
    assert_eq!(
        store.candidate_count_for_retrieval_request(&request("t1", "report", 5)),
        depth
    );
    // The best BM25 matches are always in the candidate set.
    let results = store.retrieve(&request("t1", "steel tariff report", 6));
    assert_eq!(
        ids(&results),
        vec!["c000", "c050", "c100", "c150", "c200", "c250"]
    );
    // Filters apply before the top N is taken: a claim ranked far below the
    // depth is still found when the allowed set selects it.
    let allowed: std::collections::HashSet<String> = ["c299".to_string()].into();
    let (results, count) = store.retrieve_with_candidate_count_query_vector_and_allowed_claim_ids(
        &request("t1", "report", 5),
        None,
        None,
        None,
        Some(&allowed),
    );
    assert_eq!(count, 1);
    assert_eq!(ids(&results), vec!["c299"]);
}

#[test]
fn tenants_have_their_own_bm25_statistics() {
    let mut store = InMemoryStore::new();
    ingest(
        &mut store,
        claim_builder("a1", "ta", "bridge toll raised", 0.8),
    );
    ingest(
        &mut store,
        claim_builder("a2", "ta", "harbour ferry schedule", 0.8),
    );
    let before = answer(store.retrieve(&request("ta", "bridge toll", 10)));
    for i in 0..200 {
        ingest(
            &mut store,
            claim_builder(
                &format!("b{i}"),
                "tb",
                "bridge toll bridge toll bridge",
                0.8,
            ),
        );
    }
    assert_eq!(
        answer(store.retrieve(&request("ta", "bridge toll", 10))),
        before
    );
    let tb = store.retrieve(&request("tb", "bridge", 300));
    assert_eq!(tb.len(), 200);
    assert!(tb.iter().all(|r| r.claim_id.starts_with('b')));
}

#[test]
fn unicode_text_is_searchable() {
    let mut store = InMemoryStore::new();
    ingest(
        &mut store,
        claim_builder("u1", "t1", "Café RÉSUMÉ naïve", 0.8),
    );
    ingest(
        &mut store,
        claim_builder("u2", "t1", "会議は東京で開かれた", 0.8),
    );
    ingest(
        &mut store,
        claim_builder("u3", "t1", "Встреча в Москве", 0.8),
    );
    ingest(
        &mut store,
        claim_builder("u4", "t1", "plain ascii text", 0.8),
    );
    assert_eq!(ids(&store.retrieve(&request("t1", "café", 10))), vec!["u1"]);
    assert_eq!(
        ids(&store.retrieve(&request("t1", "résumé", 10))),
        vec!["u1"]
    );
    assert_eq!(ids(&store.retrieve(&request("t1", "東京", 10))), vec!["u2"]);
    assert_eq!(
        ids(&store.retrieve(&request("t1", "МОСКВЕ", 10))),
        vec!["u3"]
    );
    // The old ASCII-only tokenizer reduced "café" to "caf" and dropped
    // non-Latin words entirely.
    assert!(store.retrieve(&request("t1", "caf", 10)).is_empty());
}

const QUERIES: &[&str] = &[
    "coolant pump",
    "reactor inspection report",
    "turbine",
    "what did the board approve?",
    "東京",
    "the",
];

fn seed(store: &mut InMemoryStore, wal: &mut FileWal) {
    let texts = [
        ("c01", "Coolant pump failure at reactor two"),
        ("c02", "Reactor inspection report published"),
        ("c03", "The board approved the turbine upgrade"),
        ("c04", "Pumps replaced; the inspection passed"),
        ("c05", "会議は東京で開かれた"),
        ("c06", "Quarterly report: turbine output rose"),
    ];
    for (i, (id, text)) in texts.iter().enumerate() {
        let angle = i as f32 * 0.5;
        store
            .ingest_atomic_persistent(
                wal,
                claim_builder(id, "t1", text, 0.6 + i as f32 * 0.05),
                vec![],
                vec![],
                Some(vec![angle.cos(), angle.sin(), 0.1]),
                TS + i as u64,
            )
            .unwrap();
    }
    // Update one text, delete another.
    store
        .ingest_atomic_persistent(
            wal,
            claim_builder("c02", "t1", "Reactor inspection delayed by storm", 0.7),
            vec![],
            vec![],
            None,
            TS + 10,
        )
        .unwrap();
    store
        .delete_persistent(
            wal,
            Tombstone::Claim {
                tenant_id: "t1".into(),
                claim_id: "c04".into(),
            },
            TS + 11,
        )
        .unwrap();
}

fn answers(store: &InMemoryStore) -> Vec<Vec<(String, u32)>> {
    let mut out = Vec::new();
    for query in QUERIES {
        out.push(answer(store.retrieve(&request("t1", query, 10))));
        out.push(answer(
            store.retrieve_semantic(&request("t1", query, 10), &[0.9, 0.3, 0.1]),
        ));
    }
    out
}

#[test]
fn follower_restart_and_resync_answer_like_the_leader() {
    let dir = TempDir::new().unwrap();
    let wal_path = dir.path().join("leader.wal");
    let mut wal = FileWal::open(&wal_path).unwrap();
    let mut leader = InMemoryStore::new();
    seed(&mut leader, &mut wal);
    let expected = answers(&leader);
    assert!(!expected[0].is_empty(), "the seed matches the queries");
    assert!(
        expected[2].iter().all(|(id, _)| id != "c04"),
        "deleted claim"
    );

    // Replication follower applying the leader's WAL lines.
    let mut follower = InMemoryStore::new();
    let delta = wal.replication_delta_from(0, 10_000).unwrap();
    for line in &delta.wal_lines {
        assert!(follower.apply_persisted_record_line_lenient(line).unwrap());
    }
    assert_eq!(answers(&follower), expected, "follower");

    // Restart: the index is rebuilt by the WAL replay.
    let restarted = InMemoryStore::load_from_wal(&wal).unwrap();
    assert_eq!(answers(&restarted), expected, "restart");

    // Checkpoint (snapshot + empty delta), restart and resync from export.
    leader.checkpoint_and_compact(&mut wal).unwrap();
    drop(wal);
    let mut wal = FileWal::open(&wal_path).unwrap();
    let restarted = InMemoryStore::load_from_wal(&wal).unwrap();
    assert_eq!(answers(&restarted), expected, "restart after checkpoint");
    let export = wal.replication_export().unwrap();
    let mut resynced = InMemoryStore::new();
    for line in export.snapshot_lines.iter().chain(export.wal_lines.iter()) {
        resynced.apply_persisted_record_line(line).unwrap();
    }
    assert_eq!(answers(&resynced), expected, "resync");
}
