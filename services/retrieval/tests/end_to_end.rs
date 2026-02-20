use retrieval::retrieve_for_rag;
use schema::{Claim, ClaimEdge, Relation, RetrievalRequest, Stance, StanceMode};
use store::InMemoryStore;

#[test]
fn end_to_end_retrieval_returns_citations_and_support_counts() {
    let mut store = InMemoryStore::new();

    store
        .ingest_bundle(
            Claim {
                claim_id: "claim-acq".into(),
                tenant_id: "tenant-a".into(),
                canonical_text: "Company X acquired Company Y in 2025".into(),
                confidence: 0.93,
                event_time_unix: Some(1736035200),
                entities: vec![],
                embedding_ids: vec![],
                claim_type: None,
                valid_from: None,
                valid_to: None,
                created_at: None,
                updated_at: None,
            },
            vec![schema::Evidence {
                evidence_id: "ev1".into(),
                claim_id: "claim-acq".into(),
                source_id: "source://press-release".into(),
                stance: Stance::Supports,
                source_quality: 0.95,
                chunk_id: None,
                span_start: None,
                span_end: None,
                doc_id: Some("doc://press-release".into()),
                extraction_model: Some("extractor-v5".into()),
                ingested_at: Some(1_736_035_200_000),
            }],
            vec![ClaimEdge {
                edge_id: "edge1".into(),
                from_claim_id: "claim-acq".into(),
                to_claim_id: "claim-related".into(),
                relation: Relation::Supports,
                strength: 0.8,
                reason_codes: vec![],
                created_at: None,
            }],
        )
        .unwrap();

    let results = retrieve_for_rag(
        &store,
        RetrievalRequest {
            tenant_id: "tenant-a".into(),
            query: "Did company x acquire company y in 2025?".into(),
            top_k: 3,
            stance_mode: StanceMode::Balanced,
        },
    );

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].claim_id, "claim-acq");
    assert_eq!(results[0].supports, 2);
    assert_eq!(results[0].citations.len(), 1);
    assert_eq!(results[0].citations[0].source_id, "source://press-release");
    assert_eq!(
        results[0].citations[0].doc_id.as_deref(),
        Some("doc://press-release")
    );
    assert_eq!(
        results[0].citations[0].extraction_model.as_deref(),
        Some("extractor-v5")
    );
    assert_eq!(results[0].citations[0].ingested_at, Some(1_736_035_200_000));
}
