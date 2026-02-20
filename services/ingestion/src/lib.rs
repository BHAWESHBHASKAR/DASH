pub mod api;
pub mod transport;
#[cfg(feature = "async-transport")]
pub mod transport_axum;

use schema::{Claim, ClaimEdge, Evidence};
use store::{CheckpointPolicy, FileWal, InMemoryStore, StoreError, WalCheckpointStats};

#[derive(Debug, Clone, PartialEq)]
pub struct IngestInput {
    pub claim: Claim,
    pub claim_embedding: Option<Vec<f32>>,
    pub evidence: Vec<Evidence>,
    pub edges: Vec<ClaimEdge>,
}

pub fn ingest_document(store: &mut InMemoryStore, input: IngestInput) -> Result<(), StoreError> {
    let claim_id = input.claim.claim_id.clone();
    store.ingest_bundle(input.claim, input.evidence, input.edges)?;
    if let Some(vector) = input.claim_embedding {
        store.upsert_claim_vector(&claim_id, vector)?;
    }
    Ok(())
}

pub fn ingest_document_persistent(
    store: &mut InMemoryStore,
    wal: &mut FileWal,
    input: IngestInput,
) -> Result<(), StoreError> {
    let claim_id = input.claim.claim_id.clone();
    store.ingest_bundle_persistent(wal, input.claim, input.evidence, input.edges)?;
    if let Some(vector) = input.claim_embedding {
        store.upsert_claim_vector_persistent(wal, &claim_id, vector)?;
    }
    Ok(())
}

pub fn ingest_document_persistent_with_policy(
    store: &mut InMemoryStore,
    wal: &mut FileWal,
    policy: &CheckpointPolicy,
    input: IngestInput,
) -> Result<Option<WalCheckpointStats>, StoreError> {
    let claim_id = input.claim.claim_id.clone();
    let stats = store.ingest_bundle_persistent_with_policy(
        wal,
        policy,
        input.claim,
        input.evidence,
        input.edges,
    )?;
    if let Some(vector) = input.claim_embedding {
        store.upsert_claim_vector_persistent(wal, &claim_id, vector)?;
    }
    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;
    use schema::{Stance, ValidationError};
    use store::StoreError;

    #[test]
    fn ingest_document_persists_claim_and_evidence() {
        let mut store = InMemoryStore::new();
        let input = IngestInput {
            claim: Claim {
                claim_id: "c1".into(),
                tenant_id: "tenant-a".into(),
                canonical_text: "Company X acquired Company Y".into(),
                confidence: 0.85,
                event_time_unix: None,
                entities: vec![],
                embedding_ids: vec![],
                claim_type: None,
                valid_from: None,
                valid_to: None,
                created_at: None,
                updated_at: None,
            },
            claim_embedding: None,
            evidence: vec![Evidence {
                evidence_id: "e1".into(),
                claim_id: "c1".into(),
                source_id: "doc-1".into(),
                stance: Stance::Supports,
                source_quality: 0.9,
                chunk_id: None,
                span_start: None,
                span_end: None,
                doc_id: None,
                extraction_model: None,
                ingested_at: None,
            }],
            edges: vec![],
        };

        ingest_document(&mut store, input).unwrap();
        assert_eq!(store.claims_len(), 1);
        assert_eq!(store.wal_len(), 2);
    }

    #[test]
    fn ingest_document_rejects_invalid_claim() {
        let mut store = InMemoryStore::new();
        let input = IngestInput {
            claim: Claim {
                claim_id: "c1".into(),
                tenant_id: "tenant-a".into(),
                canonical_text: "bad confidence".into(),
                confidence: 2.0,
                event_time_unix: None,
                entities: vec![],
                embedding_ids: vec![],
                claim_type: None,
                valid_from: None,
                valid_to: None,
                created_at: None,
                updated_at: None,
            },
            claim_embedding: None,
            evidence: vec![],
            edges: vec![],
        };

        let err = ingest_document(&mut store, input).unwrap_err();
        assert_eq!(
            err,
            StoreError::Validation(ValidationError::InvalidRange("confidence"))
        );
    }

    #[test]
    fn ingest_document_persistent_replays_from_disk_wal() {
        let mut wal_path = std::env::temp_dir();
        wal_path.push(format!("eme-ingest-{}.jsonl", std::process::id()));
        let mut wal = FileWal::open(&wal_path).unwrap();

        let mut store = InMemoryStore::new();
        let input = IngestInput {
            claim: Claim {
                claim_id: "c10".into(),
                tenant_id: "tenant-a".into(),
                canonical_text: "Persistent ingest path".into(),
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
            claim_embedding: None,
            evidence: vec![Evidence {
                evidence_id: "e10".into(),
                claim_id: "c10".into(),
                source_id: "doc-10".into(),
                stance: Stance::Supports,
                source_quality: 0.92,
                chunk_id: None,
                span_start: None,
                span_end: None,
                doc_id: None,
                extraction_model: None,
                ingested_at: None,
            }],
            edges: vec![],
        };
        ingest_document_persistent(&mut store, &mut wal, input).unwrap();

        let replayed = InMemoryStore::load_from_wal(&wal).unwrap();
        assert_eq!(replayed.claims_len(), 1);
        let _ = std::fs::remove_file(wal.path());
    }

    #[test]
    fn ingest_document_persistent_with_policy_triggers_checkpoint() {
        let mut wal_path = std::env::temp_dir();
        wal_path.push(format!("eme-ingest-policy-{}.jsonl", std::process::id()));
        let mut wal = FileWal::open(&wal_path).unwrap();
        let policy = CheckpointPolicy {
            max_wal_records: Some(2),
            max_wal_bytes: None,
        };

        let mut store = InMemoryStore::new();
        let input = IngestInput {
            claim: Claim {
                claim_id: "c-policy".into(),
                tenant_id: "tenant-a".into(),
                canonical_text: "Policy-triggered checkpoint".into(),
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
            claim_embedding: None,
            evidence: vec![Evidence {
                evidence_id: "e-policy".into(),
                claim_id: "c-policy".into(),
                source_id: "doc-policy".into(),
                stance: Stance::Supports,
                source_quality: 0.92,
                chunk_id: None,
                span_start: None,
                span_end: None,
                doc_id: None,
                extraction_model: None,
                ingested_at: None,
            }],
            edges: vec![],
        };

        let stats =
            ingest_document_persistent_with_policy(&mut store, &mut wal, &policy, input).unwrap();
        assert!(stats.is_some());
        assert_eq!(wal.wal_record_count().unwrap(), 0);
        assert!(wal.snapshot_path().exists());

        let _ = std::fs::remove_file(wal.path());
        let _ = std::fs::remove_file(wal.snapshot_path());
    }

    #[test]
    fn ingest_document_persists_claim_embedding_vector() {
        let mut store = InMemoryStore::new();
        let input = IngestInput {
            claim: Claim {
                claim_id: "c-vec".into(),
                tenant_id: "tenant-a".into(),
                canonical_text: "Vectorized claim".into(),
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
            claim_embedding: Some(vec![0.1, 0.2, 0.3, 0.4]),
            evidence: vec![],
            edges: vec![],
        };

        ingest_document(&mut store, input).unwrap();
        let results = store.retrieve_with_time_range_and_query_vector(
            &schema::RetrievalRequest {
                tenant_id: "tenant-a".into(),
                query: "vectorized claim".into(),
                top_k: 1,
                stance_mode: schema::StanceMode::Balanced,
            },
            None,
            None,
            Some(&[0.1, 0.2, 0.3, 0.4]),
        );
        assert_eq!(results.first().map(|r| r.claim_id.as_str()), Some("c-vec"));
    }

    #[test]
    fn ingest_document_preserves_temporal_claim_metadata() {
        let mut store = InMemoryStore::new();
        let input = IngestInput {
            claim: Claim {
                claim_id: "c-temporal".into(),
                tenant_id: "tenant-a".into(),
                canonical_text: "Temporal metadata ingestion".into(),
                confidence: 0.9,
                event_time_unix: Some(200),
                entities: vec![],
                embedding_ids: vec![],
                claim_type: Some(schema::ClaimType::Temporal),
                valid_from: Some(120),
                valid_to: Some(260),
                created_at: Some(1_771_620_000_000),
                updated_at: Some(1_771_620_100_000),
            },
            claim_embedding: None,
            evidence: vec![],
            edges: vec![],
        };

        ingest_document(&mut store, input).unwrap();
        let claim = store
            .claim_by_id("c-temporal")
            .expect("claim should be persisted");
        assert_eq!(claim.claim_type, Some(schema::ClaimType::Temporal));
        assert_eq!(claim.valid_from, Some(120));
        assert_eq!(claim.valid_to, Some(260));
        assert_eq!(claim.created_at, Some(1_771_620_000_000));
        assert_eq!(claim.updated_at, Some(1_771_620_100_000));
    }
}
