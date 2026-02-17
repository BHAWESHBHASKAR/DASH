use schema::{Claim, ClaimEdge, Evidence};

#[derive(Debug, Clone, PartialEq)]
pub struct IngestApiRequest {
    pub claim: Claim,
    pub claim_embedding: Option<Vec<f32>>,
    pub evidence: Vec<Evidence>,
    pub edges: Vec<ClaimEdge>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngestApiResponse {
    pub ingested_claim_id: String,
    pub claims_total: usize,
    pub checkpoint_triggered: bool,
    pub checkpoint_snapshot_records: Option<usize>,
    pub checkpoint_truncated_wal_records: Option<usize>,
}
