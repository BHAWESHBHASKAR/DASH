pub mod api;
pub mod transport;
#[cfg(feature = "async-transport")]
pub mod transport_axum;

use schema::{RetrievalRequest, RetrievalResult};
use store::InMemoryStore;

pub fn retrieve_for_rag(store: &InMemoryStore, req: RetrievalRequest) -> Vec<RetrievalResult> {
    store.retrieve(&req)
}

pub fn retrieve_for_rag_with_time_range(
    store: &InMemoryStore,
    req: RetrievalRequest,
    from_unix: Option<i64>,
    to_unix: Option<i64>,
    query_embedding: Option<&[f32]>,
) -> Vec<RetrievalResult> {
    store.retrieve_with_time_range_and_query_vector(&req, from_unix, to_unix, query_embedding)
}

#[cfg(test)]
mod tests {
    use super::*;
    use schema::{Claim, Evidence, Stance, StanceMode};

    #[test]
    fn retrieve_for_rag_returns_ranked_results_with_citations() {
        let mut store = InMemoryStore::new();
        store
            .ingest_bundle(
                Claim {
                    claim_id: "c1".into(),
                    tenant_id: "tenant-a".into(),
                    canonical_text: "Company X acquired Company Y".into(),
                    confidence: 0.9,
                    event_time_unix: None,
                    entities: vec![],
                    embedding_ids: vec![],
                    claim_type: None,
                    valid_from: None,
                    valid_to: None,
                    created_at: None,
                    updated_at: None,
                },
                vec![Evidence {
                    evidence_id: "e1".into(),
                    claim_id: "c1".into(),
                    source_id: "source://doc-1".into(),
                    stance: Stance::Supports,
                    source_quality: 0.8,
                    chunk_id: None,
                    span_start: None,
                    span_end: None,
                    doc_id: None,
                    extraction_model: None,
                    ingested_at: None,
                }],
                vec![],
            )
            .unwrap();

        let results = retrieve_for_rag(
            &store,
            RetrievalRequest {
                tenant_id: "tenant-a".into(),
                query: "Did company x acquire company y?".into(),
                top_k: 1,
                stance_mode: StanceMode::Balanced,
            },
        );
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].claim_id, "c1");
        assert_eq!(results[0].citations.len(), 1);
        assert_eq!(results[0].citations[0].source_id, "source://doc-1");
        assert_eq!(results[0].citations[0].stance, Stance::Supports);
    }
}
