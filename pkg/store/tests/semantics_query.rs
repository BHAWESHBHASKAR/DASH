//! Query-path semantics: query-vector validation and NaN-free scoring
//! (DATA-05), tenant-scoped fallbacks (IDX-03), edge direction / dangling
//! edges / saturating support (IDX-05), the per-tenant claim index
//! (PERF-02) and non-leaky conflict errors (SEC-18).

use schema::{Claim, ClaimEdge, Evidence, Relation, RetrievalRequest, Stance, StanceMode};
use store::{InMemoryStore, StoreError};

fn claim(id: &str, tenant: &str, text: &str) -> Claim {
    Claim {
        claim_id: id.to_string(),
        tenant_id: tenant.to_string(),
        canonical_text: text.to_string(),
        confidence: 0.8,
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
        source_quality: 0.8,
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
        strength: 0.9,
        reason_codes: vec![],
        created_at: None,
    }
}

fn request(tenant: &str, query: &str) -> RetrievalRequest {
    RetrievalRequest {
        tenant_id: tenant.to_string(),
        query: query.to_string(),
        top_k: 1000,
        stance_mode: StanceMode::Balanced,
    }
}

fn result_for(
    store: &InMemoryStore,
    tenant: &str,
    query: &str,
    claim_id: &str,
) -> schema::RetrievalResult {
    store
        .retrieve(&request(tenant, query))
        .into_iter()
        .find(|r| r.claim_id == claim_id)
        .unwrap_or_else(|| panic!("{claim_id} should be retrievable"))
}

// ---------------------------------------------------------------------------
// DATA-05
// ---------------------------------------------------------------------------

fn store_with_vectors() -> InMemoryStore {
    let mut store = InMemoryStore::new();
    for (id, text, vector) in [
        ("c1", "alpha first", vec![1.0, 0.0, 0.0]),
        ("c2", "alpha second", vec![0.0, 1.0, 0.0]),
    ] {
        store
            .ingest_bundle(claim(id, "t1", text), vec![], vec![])
            .unwrap();
        store.upsert_claim_vector(id, vector).unwrap();
    }
    store
}

#[test]
fn invalid_query_vectors_are_rejected_with_errors() {
    let store = store_with_vectors();
    for bad in [
        vec![],
        vec![f32::NAN, 0.0, 0.0],
        vec![f32::INFINITY, 0.0, 0.0],
        vec![f32::NEG_INFINITY, 1.0, 0.0],
        vec![0.0, 0.0, 0.0],
        vec![1.0, 0.0],
        vec![1.0, 0.0, 0.0, 0.0],
    ] {
        assert!(
            matches!(
                store.validate_query_vector("t1", &bad),
                Err(StoreError::InvalidVector(_))
            ),
            "{bad:?} should be rejected"
        );
    }
    assert!(store.validate_query_vector("t1", &[0.5, 0.5, 0.0]).is_ok());
}

#[test]
fn invalid_query_vectors_yield_no_results_instead_of_nan_scores() {
    let store = store_with_vectors();
    for bad in [
        vec![f32::NAN, 0.0, 0.0],
        vec![0.0, 0.0, 0.0],
        vec![1.0, 0.0],
    ] {
        let results = store.retrieve_semantic(&request("t1", "alpha"), &bad);
        assert!(results.is_empty(), "{bad:?} returned {results:?}");
    }
}

#[test]
fn huge_finite_stored_vectors_never_produce_nan_scores() {
    let mut store = InMemoryStore::new();
    for (id, sign) in [("pos", 1.0f32), ("neg", -1.0f32)] {
        store
            .ingest_bundle(claim(id, "t1", "alpha"), vec![], vec![])
            .unwrap();
        // Squares and dot products overflow f32 (3e38^2 = inf).
        store
            .upsert_claim_vector(id, vec![sign * 3.0e38, sign * 3.0e38, sign * 3.0e38])
            .unwrap();
    }
    let results = store.retrieve_semantic(&request("t1", "alpha"), &[1.0, 1.0, 1.0]);
    assert_eq!(results.len(), 2);
    assert!(results.iter().all(|r| r.score.is_finite()), "{results:?}");
    assert_eq!(results[0].claim_id, "pos", "aligned vector must rank first");
    assert!(results[0].score > results[1].score);

    let exact = store.exact_vector_top_candidates("t1", &[1.0, 1.0, 1.0], 2);
    assert_eq!(exact, vec!["pos".to_string(), "neg".to_string()]);
}

// ---------------------------------------------------------------------------
// IDX-03
// ---------------------------------------------------------------------------

#[test]
fn wrong_dimension_query_returns_empty_and_never_scans_other_tenants() {
    let mut store = store_with_vectors();
    store
        .ingest_bundle(claim("b1", "t2", "alpha other tenant"), vec![], vec![])
        .unwrap();
    store.upsert_claim_vector("b1", vec![1.0, 0.0]).unwrap();

    // t2 has 2-dim vectors; a 3-dim query matches t1's dimension but not
    // t2's, so it must yield nothing for t2 (and never t1's claims).
    assert!(
        store
            .exact_vector_top_candidates("t2", &[1.0, 0.0, 0.0], 10)
            .is_empty()
    );
    assert!(
        store
            .ann_vector_top_candidates("t2", &[1.0, 0.0, 0.0], 10)
            .is_empty()
    );
    assert!(
        store
            .retrieve_semantic(&request("t2", "alpha"), &[1.0, 0.0, 0.0])
            .is_empty()
    );
    // Tenant without vectors: same-dimension query finds nothing either.
    assert!(
        store
            .exact_vector_top_candidates("t3", &[1.0, 0.0, 0.0], 10)
            .is_empty()
    );
    // And a correct-dimension query for t2 only sees t2.
    let ids = store.exact_vector_top_candidates("t2", &[1.0, 0.0], 10);
    assert_eq!(ids, vec!["b1".to_string()]);
}

#[test]
fn lexical_fallback_with_no_token_hits_stays_inside_the_tenant() {
    let mut store = InMemoryStore::new();
    store
        .ingest_bundle(claim("a1", "ta", "alpha"), vec![], vec![])
        .unwrap();
    store
        .ingest_bundle(claim("b1", "tb", "beta"), vec![], vec![])
        .unwrap();
    let results = store.retrieve(&request("ta", "zzz-no-such-token"));
    assert!(results.iter().all(|r| r.claim_id == "a1"), "{results:?}");
}

#[test]
fn claim_index_is_per_tenant() {
    let mut store = InMemoryStore::new();
    for i in 0..5 {
        store
            .ingest_bundle(claim(&format!("a{i}"), "ta", "alpha"), vec![], vec![])
            .unwrap();
    }
    store
        .ingest_bundle(claim("b0", "tb", "beta"), vec![], vec![])
        .unwrap();
    let ta = store.claims_for_tenant("ta");
    assert_eq!(ta.len(), 5);
    assert!(ta.iter().all(|c| c.tenant_id == "ta"));
    assert!(ta.windows(2).all(|w| w[0].claim_id < w[1].claim_id));
    assert_eq!(store.claim_count_for_tenant("ta"), 5);
    assert_eq!(store.claim_count_for_tenant("tb"), 1);
    assert_eq!(store.claim_count_for_tenant("nobody"), 0);
    assert!(store.claims_for_tenant("nobody").is_empty());
}

// ---------------------------------------------------------------------------
// IDX-05
// ---------------------------------------------------------------------------

#[test]
fn support_edge_supports_the_target_not_the_author() {
    let mut store = InMemoryStore::new();
    store
        .ingest_bundle(
            claim("target", "t1", "reactor shutdown confirmed"),
            vec![],
            vec![],
        )
        .unwrap();
    store
        .ingest_bundle(
            claim("author", "t1", "reactor shutdown confirmed"),
            vec![],
            vec![edge("g1", "author", "target", Relation::Supports)],
        )
        .unwrap();

    let target = result_for(&store, "t1", "reactor shutdown", "target");
    let author = result_for(&store, "t1", "reactor shutdown", "author");
    assert_eq!(target.supports, 1, "target is the supported claim");
    assert_eq!(author.supports, 0, "author must not gain support");
    assert!(target.score > author.score);
}

#[test]
fn contradiction_edge_penalizes_the_target_not_the_author() {
    let mut store = InMemoryStore::new();
    store
        .ingest_bundle(
            claim("target", "t1", "reactor shutdown confirmed"),
            vec![],
            vec![],
        )
        .unwrap();
    store
        .ingest_bundle(
            claim("author", "t1", "reactor shutdown confirmed"),
            vec![],
            vec![edge("g1", "author", "target", Relation::Contradicts)],
        )
        .unwrap();
    let target = result_for(&store, "t1", "reactor shutdown", "target");
    let author = result_for(&store, "t1", "reactor shutdown", "author");
    assert_eq!(target.contradicts, 1);
    assert_eq!(author.contradicts, 0);
    assert!(target.score < author.score);
}

#[test]
fn dangling_and_cross_tenant_edges_are_ignored() {
    let mut store = InMemoryStore::new();
    store
        .ingest_bundle(claim("a1", "ta", "alpha topic"), vec![], vec![])
        .unwrap();
    // Edge to a claim that does not exist (yet): affects nobody.
    store
        .ingest_bundle(
            claim("a2", "ta", "alpha topic"),
            vec![],
            vec![edge("g1", "a2", "missing", Relation::Supports)],
        )
        .unwrap();
    // Tenant tb's claim "b1" points a support edge at tenant ta's "a1".
    store
        .ingest_bundle(
            claim("b1", "tb", "alpha topic"),
            vec![],
            vec![edge("g2", "b1", "a1", Relation::Supports)],
        )
        .unwrap();
    // A claim cannot endorse itself.
    store
        .ingest_bundle(
            claim("a3", "ta", "alpha topic"),
            vec![],
            vec![edge("g3", "a3", "a3", Relation::Supports)],
        )
        .unwrap();

    for id in ["a1", "a2", "a3"] {
        let r = result_for(&store, "ta", "alpha", id);
        assert_eq!((r.supports, r.contradicts), (0, 0), "{id}: {r:?}");
    }
    let b1 = result_for(&store, "tb", "alpha", "b1");
    assert_eq!((b1.supports, b1.contradicts), (0, 0));

    // Once the endpoint exists in the same tenant the edge counts.
    store
        .ingest_bundle(claim("missing", "ta", "alpha topic"), vec![], vec![])
        .unwrap();
    assert_eq!(result_for(&store, "ta", "alpha", "missing").supports, 1);
}

#[test]
fn support_bonus_saturates_for_many_supporting_edges() {
    let mut store = InMemoryStore::new();
    store
        .ingest_bundle(claim("many", "t1", "reactor shutdown"), vec![], vec![])
        .unwrap();
    store
        .ingest_bundle(claim("some", "t1", "reactor shutdown"), vec![], vec![])
        .unwrap();
    for i in 0..150 {
        let edges = vec![edge(
            &format!("m{i}"),
            &format!("s{i}"),
            "many",
            Relation::Supports,
        )];
        store
            .ingest_bundle(
                claim(&format!("s{i}"), "t1", &format!("zz{i}")),
                vec![],
                edges,
            )
            .unwrap();
    }
    for i in 0..15 {
        let edges = vec![edge(
            &format!("n{i}"),
            &format!("s{i}"),
            "some",
            Relation::Supports,
        )];
        store
            .ingest_bundle(
                claim(&format!("s{i}"), "t1", &format!("zz{i}")),
                vec![],
                edges,
            )
            .unwrap();
    }

    let many = result_for(&store, "t1", "reactor shutdown", "many");
    let some = result_for(&store, "t1", "reactor shutdown", "some");
    assert_eq!(many.supports, 150);
    assert_eq!(some.supports, 15);
    // Linear scoring would put these 135 * 0.08 * 0.72 apart.
    assert!(
        (many.score - some.score).abs() < 0.02,
        "support bonus must saturate: {} vs {}",
        many.score,
        some.score
    );
}

#[test]
fn repeated_evidence_from_one_source_counts_once_for_ranking() {
    let mut store = InMemoryStore::new();
    let repeated: Vec<Evidence> = (0..10)
        .map(|i| evidence(&format!("r{i}"), "spam", "same-src", Stance::Supports))
        .collect();
    store
        .ingest_bundle(claim("spam", "t1", "reactor shutdown"), repeated, vec![])
        .unwrap();
    store
        .ingest_bundle(
            claim("single", "t1", "reactor shutdown"),
            vec![evidence("s0", "single", "same-src", Stance::Supports)],
            vec![],
        )
        .unwrap();
    let spam = result_for(&store, "t1", "reactor shutdown", "spam");
    let single = result_for(&store, "t1", "reactor shutdown", "single");
    assert_eq!(spam.citations.len(), 10, "all citations remain visible");
    assert!(
        (spam.score - single.score).abs() < 1e-5,
        "one source must be worth one vote: {} vs {}",
        spam.score,
        single.score
    );
}

// ---------------------------------------------------------------------------
// SEC-18
// ---------------------------------------------------------------------------

#[test]
fn cross_tenant_conflict_error_does_not_name_the_other_tenant() {
    let mut store = InMemoryStore::new();
    store
        .ingest_bundle(claim("shared", "secret-tenant-a", "x"), vec![], vec![])
        .unwrap();
    let err = store
        .ingest_bundle(claim("shared", "tenant-b", "y"), vec![], vec![])
        .unwrap_err();
    let rendered = format!("{err:?}");
    assert!(matches!(err, StoreError::Conflict(_)));
    assert!(!rendered.contains("secret-tenant-a"), "{rendered}");
    assert!(rendered.contains("claim_id already exists"));

    // Same on the vector-less apply path used by WAL replay/replication.
    let line_err = store
        .ingest_bundle(claim("shared", "tenant-c", "z"), vec![], vec![])
        .unwrap_err();
    assert!(!format!("{line_err:?}").contains("secret-tenant-a"));
}
