use schema::{Claim, ClaimEdge, Evidence};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteConsistencyPolicy {
    One,
    Quorum,
    All,
}

impl WriteConsistencyPolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::One => "one",
            Self::Quorum => "quorum",
            Self::All => "all",
        }
    }

    pub fn required_acks(self, replica_count: usize) -> usize {
        let replica_count = replica_count.max(1);
        match self {
            Self::One => 1,
            Self::Quorum => replica_count / 2 + 1,
            Self::All => replica_count,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct IngestApiRequest {
    pub claim: Claim,
    pub claim_embedding: Option<Vec<f32>>,
    pub evidence: Vec<Evidence>,
    pub edges: Vec<ClaimEdge>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct IngestBatchApiRequest {
    pub commit_id: Option<String>,
    pub items: Vec<IngestApiRequest>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct IngestRawApiRequest {
    pub tenant_id: String,
    pub document_id: String,
    pub source_id: String,
    pub text: String,
    pub extraction_model: Option<String>,
    pub claim_confidence: Option<f32>,
    pub source_quality: Option<f32>,
    pub min_sentence_chars: Option<usize>,
    pub max_claims: Option<usize>,
    pub generate_embeddings: Option<bool>,
    pub embedding_model: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct IngestDocumentApiRequest {
    pub tenant_id: String,
    pub document_id: String,
    pub source_id: String,
    pub mime_type: String,
    pub text: Option<String>,
    pub content_base64: Option<String>,
    pub extraction_model: Option<String>,
    pub claim_confidence: Option<f32>,
    pub source_quality: Option<f32>,
    pub min_sentence_chars: Option<usize>,
    pub max_claims: Option<usize>,
    pub generate_embeddings: Option<bool>,
    pub embedding_model: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngestApiResponse {
    pub ingested_claim_id: String,
    pub claims_total: usize,
    pub commit_epoch: Option<u64>,
    pub ack_count: usize,
    pub required_acks: usize,
    pub commit_status: String,
    pub checkpoint_triggered: bool,
    pub checkpoint_snapshot_records: Option<usize>,
    pub checkpoint_truncated_wal_records: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngestBatchApiResponse {
    pub commit_id: String,
    pub idempotent_replay: bool,
    pub ingested_claim_ids: Vec<String>,
    pub batch_size: usize,
    pub claims_total: usize,
    pub commit_epoch: Option<u64>,
    pub ack_count: usize,
    pub required_acks: usize,
    pub commit_status: String,
    pub checkpoint_triggered: bool,
    pub checkpoint_snapshot_records: Option<usize>,
    pub checkpoint_truncated_wal_records: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngestRawApiResponse {
    pub document_id: String,
    pub commit_id: String,
    pub idempotent_replay: bool,
    pub extracted_count: usize,
    pub embedding_provider: String,
    pub embeddings_generated: usize,
    pub embedding_dimensions: Option<usize>,
    pub ingested_claim_ids: Vec<String>,
    pub claims_total: usize,
    pub commit_epoch: Option<u64>,
    pub ack_count: usize,
    pub required_acks: usize,
    pub commit_status: String,
    pub checkpoint_triggered: bool,
    pub checkpoint_snapshot_records: Option<usize>,
    pub checkpoint_truncated_wal_records: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngestDocumentApiResponse {
    pub document_id: String,
    pub mime_type: String,
    pub parser_provider: String,
    pub commit_id: String,
    pub idempotent_replay: bool,
    pub extracted_count: usize,
    pub embedding_provider: String,
    pub embeddings_generated: usize,
    pub embedding_dimensions: Option<usize>,
    pub ingested_claim_ids: Vec<String>,
    pub claims_total: usize,
    pub commit_epoch: Option<u64>,
    pub ack_count: usize,
    pub required_acks: usize,
    pub commit_status: String,
    pub checkpoint_triggered: bool,
    pub checkpoint_snapshot_records: Option<usize>,
    pub checkpoint_truncated_wal_records: Option<usize>,
}
