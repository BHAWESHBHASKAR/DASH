use schema::{Claim, ClaimEdge, ClaimType, Evidence, Relation, Stance};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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

// ---------------------------------------------------------------------------
// Wire-format types — serde-native JSON shapes for the HTTP request bodies.
//
// The runtime `IngestApiRequest` has `claim_embedding` as a top-level field,
// but the public wire format places the embedding inside the `claim` object
// under the name `embedding_vector`. We model that quirk with a separate
// `IngestApiRequestWire` and convert via `into_runtime()`.
// ---------------------------------------------------------------------------

/// Wire-format body for `POST /v1/ingest`.
///
/// The runtime `IngestApiRequest` expects `claim_embedding` at the top level,
/// but the historical wire format embeds the vector as `embedding_vector` on
/// the `claim` object. Both shapes are accepted; the claim-side value is
/// extracted into the runtime's `claim_embedding` field during conversion.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct IngestApiRequestWire {
    pub claim: ClaimWire,
    #[serde(default)]
    pub claim_embedding: Option<Vec<f32>>,
    #[serde(default)]
    pub evidence: Vec<EvidenceWire>,
    #[serde(default)]
    pub edges: Vec<ClaimEdgeWire>,
}

impl IngestApiRequestWire {
    pub fn into_runtime(self) -> Result<IngestApiRequest, String> {
        let (claim, claim_embedding) = self.claim.into_runtime()?;
        // Top-level `claim_embedding` takes precedence (legacy compat) over
        // the in-claim `embedding_vector` field.
        let claim_embedding = self.claim_embedding.or(claim_embedding);
        let mut evidence = Vec::with_capacity(self.evidence.len());
        for item in self.evidence {
            evidence.push(item.into_runtime()?);
        }
        let mut edges = Vec::with_capacity(self.edges.len());
        for item in self.edges {
            edges.push(item.into_runtime()?);
        }
        Ok(IngestApiRequest {
            claim,
            claim_embedding,
            evidence,
            edges,
        })
    }
}

/// Wire-format body for `POST /v1/ingest/batch`.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct IngestBatchApiRequestWire {
    #[serde(default)]
    pub commit_id: Option<String>,
    pub items: Vec<IngestApiRequestWire>,
}

impl IngestBatchApiRequestWire {
    pub fn into_runtime(self, max_items: usize) -> Result<IngestBatchApiRequest, String> {
        if self.items.is_empty() {
            return Err("items must not be empty".to_string());
        }
        if self.items.len() > max_items {
            return Err(format!(
                "items length {} exceeds max batch size {}",
                self.items.len(),
                max_items
            ));
        }

        let commit_id = self
            .commit_id
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());

        let mut items = Vec::with_capacity(self.items.len());
        let mut expected_tenant: Option<String> = None;
        for item in self.items {
            let req = item.into_runtime()?;
            if let Some(tenant_id) = expected_tenant.as_deref() {
                if tenant_id != req.claim.tenant_id {
                    return Err("all batch items must share the same claim.tenant_id".to_string());
                }
            } else {
                expected_tenant = Some(req.claim.tenant_id.clone());
            }
            items.push(req);
        }

        Ok(IngestBatchApiRequest { commit_id, items })
    }
}

/// Wire-format `claim` object. Mirrors `schema::Claim` plus the legacy
/// `embedding_vector` field that lives on the claim in the public API.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ClaimWire {
    pub claim_id: String,
    pub tenant_id: String,
    pub canonical_text: String,
    pub confidence: f32,
    #[serde(default)]
    pub event_time_unix: Option<i64>,
    #[serde(default)]
    pub entities: Vec<String>,
    #[serde(default)]
    pub embedding_ids: Vec<String>,
    #[serde(default)]
    pub claim_type: Option<String>,
    #[serde(default)]
    pub valid_from: Option<i64>,
    #[serde(default)]
    pub valid_to: Option<i64>,
    #[serde(default)]
    pub created_at: Option<i64>,
    #[serde(default)]
    pub updated_at: Option<i64>,
    #[serde(default)]
    pub embedding_vector: Option<Vec<f32>>,
}

impl ClaimWire {
    /// Convert to a runtime `Claim` plus the optional extracted embedding.
    /// Performs the claim_type and embedding_vector validation that used to
    /// happen in the hand-rolled parser, preserving the legacy error messages
    /// the test suite expects.
    pub fn into_runtime(self) -> Result<(Claim, Option<Vec<f32>>), String> {
        let claim_type = match self.claim_type {
            None => None,
            Some(raw) => {
                let normalized = raw.trim().to_ascii_lowercase();
                Some(match normalized.as_str() {
                    "factual" => ClaimType::Factual,
                    "opinion" => ClaimType::Opinion,
                    "prediction" => ClaimType::Prediction,
                    "temporal" => ClaimType::Temporal,
                    "causal" => ClaimType::Causal,
                    _ => {
                        return Err(
                            "claim.claim_type must be one of: factual, opinion, prediction, temporal, causal"
                                .to_string(),
                        );
                    }
                })
            }
        };

        if let Some(vector) = &self.embedding_vector {
            if vector.is_empty() {
                return Err("claim.embedding_vector must not be empty when provided".to_string());
            }
            if !vector.iter().all(|v| v.is_finite()) {
                return Err("claim.embedding_vector values must be finite numbers".to_string());
            }
        }

        Ok((
            Claim {
                claim_id: self.claim_id,
                tenant_id: self.tenant_id,
                canonical_text: self.canonical_text,
                confidence: self.confidence,
                event_time_unix: self.event_time_unix,
                entities: self.entities,
                embedding_ids: self.embedding_ids,
                claim_type,
                valid_from: self.valid_from,
                valid_to: self.valid_to,
                created_at: self.created_at,
                updated_at: self.updated_at,
            },
            self.embedding_vector,
        ))
    }
}

/// Wire-format `evidence` object. Mirrors `schema::Evidence` but accepts
/// `stance` as a free-form string so the deserializer does not reject
/// unknown values; the conversion validates the stance label.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct EvidenceWire {
    pub evidence_id: String,
    pub claim_id: String,
    pub source_id: String,
    pub stance: String,
    pub source_quality: f32,
    #[serde(default)]
    pub chunk_id: Option<String>,
    #[serde(default)]
    pub span_start: Option<u32>,
    #[serde(default)]
    pub span_end: Option<u32>,
    #[serde(default)]
    pub doc_id: Option<String>,
    #[serde(default)]
    pub extraction_model: Option<String>,
    #[serde(default)]
    pub ingested_at: Option<i64>,
}

impl EvidenceWire {
    pub fn into_runtime(self) -> Result<Evidence, String> {
        let stance = match self.stance.as_str() {
            "supports" => Stance::Supports,
            "contradicts" => Stance::Contradicts,
            "neutral" => Stance::Neutral,
            _ => {
                return Err("evidence.stance must be supports, contradicts, or neutral".to_string());
            }
        };
        Ok(Evidence {
            evidence_id: self.evidence_id,
            claim_id: self.claim_id,
            source_id: self.source_id,
            stance,
            source_quality: self.source_quality,
            chunk_id: self.chunk_id,
            span_start: self.span_start,
            span_end: self.span_end,
            doc_id: self.doc_id,
            extraction_model: self.extraction_model,
            ingested_at: self.ingested_at,
        })
    }
}

/// Wire-format `edges` object. Mirrors `schema::ClaimEdge` but accepts
/// `relation` as a free-form string so the deserializer does not reject
/// unknown values; the conversion validates the relation label.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ClaimEdgeWire {
    pub edge_id: String,
    pub from_claim_id: String,
    pub to_claim_id: String,
    pub relation: String,
    pub strength: f32,
    #[serde(default)]
    pub reason_codes: Vec<String>,
    #[serde(default)]
    pub created_at: Option<i64>,
}

impl ClaimEdgeWire {
    pub fn into_runtime(self) -> Result<ClaimEdge, String> {
        let relation = match self.relation.as_str() {
            "supports" => Relation::Supports,
            "contradicts" => Relation::Contradicts,
            "refines" => Relation::Refines,
            "duplicates" => Relation::Duplicates,
            "depends_on" => Relation::DependsOn,
            _ => {
                return Err(
                    "edge.relation must be supports, contradicts, refines, duplicates, or depends_on"
                        .to_string(),
                );
            }
        };
        Ok(ClaimEdge {
            edge_id: self.edge_id,
            from_claim_id: self.from_claim_id,
            to_claim_id: self.to_claim_id,
            relation,
            strength: self.strength,
            reason_codes: self.reason_codes,
            created_at: self.created_at,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct IngestRawApiRequest {
    pub tenant_id: String,
    pub document_id: String,
    pub source_id: String,
    pub text: String,
    #[serde(default)]
    pub extraction_model: Option<String>,
    #[serde(default)]
    pub claim_confidence: Option<f32>,
    #[serde(default)]
    pub source_quality: Option<f32>,
    #[serde(default)]
    pub min_sentence_chars: Option<usize>,
    #[serde(default)]
    pub max_claims: Option<usize>,
    #[serde(default)]
    pub generate_embeddings: Option<bool>,
    #[serde(default)]
    pub embedding_model: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct IngestDocumentApiRequest {
    pub tenant_id: String,
    pub document_id: String,
    pub source_id: String,
    pub mime_type: String,
    #[serde(default)]
    pub text: Option<String>,
    #[serde(default)]
    pub content_base64: Option<String>,
    #[serde(default)]
    pub extraction_model: Option<String>,
    #[serde(default)]
    pub claim_confidence: Option<f32>,
    #[serde(default)]
    pub source_quality: Option<f32>,
    #[serde(default)]
    pub min_sentence_chars: Option<usize>,
    #[serde(default)]
    pub max_claims: Option<usize>,
    #[serde(default)]
    pub generate_embeddings: Option<bool>,
    #[serde(default)]
    pub embedding_model: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct IngestApiResponse {
    pub ingested_claim_id: String,
    pub claims_total: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_epoch: Option<u64>,
    pub ack_count: usize,
    pub required_acks: usize,
    pub commit_status: String,
    pub checkpoint_triggered: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint_snapshot_records: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint_truncated_wal_records: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct IngestBatchApiResponse {
    pub commit_id: String,
    pub idempotent_replay: bool,
    pub ingested_claim_ids: Vec<String>,
    pub batch_size: usize,
    pub claims_total: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_epoch: Option<u64>,
    pub ack_count: usize,
    pub required_acks: usize,
    pub commit_status: String,
    pub checkpoint_triggered: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint_snapshot_records: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint_truncated_wal_records: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct IngestRawApiResponse {
    pub document_id: String,
    pub commit_id: String,
    pub idempotent_replay: bool,
    pub extracted_count: usize,
    pub embedding_provider: String,
    pub embeddings_generated: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub embedding_dimensions: Option<usize>,
    pub ingested_claim_ids: Vec<String>,
    pub claims_total: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_epoch: Option<u64>,
    pub ack_count: usize,
    pub required_acks: usize,
    pub commit_status: String,
    pub checkpoint_triggered: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint_snapshot_records: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint_truncated_wal_records: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct IngestDocumentApiResponse {
    pub document_id: String,
    pub mime_type: String,
    pub parser_provider: String,
    pub commit_id: String,
    pub idempotent_replay: bool,
    pub extracted_count: usize,
    pub embedding_provider: String,
    pub embeddings_generated: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub embedding_dimensions: Option<usize>,
    pub ingested_claim_ids: Vec<String>,
    pub claims_total: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_epoch: Option<u64>,
    pub ack_count: usize,
    pub required_acks: usize,
    pub commit_status: String,
    pub checkpoint_triggered: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint_snapshot_records: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint_truncated_wal_records: Option<usize>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use schema::{ClaimType, Relation, Stance};

    #[test]
    fn ingest_raw_request_serde_roundtrip() {
        let req = IngestRawApiRequest {
            tenant_id: "t1".into(),
            document_id: "d1".into(),
            source_id: "s1".into(),
            text: "raw text".into(),
            extraction_model: Some("v3".into()),
            claim_confidence: Some(0.85),
            source_quality: Some(0.9),
            min_sentence_chars: Some(8),
            max_claims: Some(50),
            generate_embeddings: Some(true),
            embedding_model: Some("text-embedding-3-small".into()),
        };
        let json = serde_json::to_string(&req).unwrap();
        let decoded: IngestRawApiRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(req, decoded);
    }

    #[test]
    fn ingest_document_request_serde_roundtrip_with_only_text() {
        let req = IngestDocumentApiRequest {
            tenant_id: "t1".into(),
            document_id: "d1".into(),
            source_id: "s1".into(),
            mime_type: "text/plain".into(),
            text: Some("hello world".into()),
            content_base64: None,
            extraction_model: None,
            claim_confidence: None,
            source_quality: None,
            min_sentence_chars: None,
            max_claims: None,
            generate_embeddings: None,
            embedding_model: None,
        };
        let json = serde_json::to_string(&req).unwrap();
        let decoded: IngestDocumentApiRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(req, decoded);
    }

    #[test]
    fn ingest_api_response_serde_roundtrip() {
        let resp = IngestApiResponse {
            ingested_claim_id: "c1".into(),
            claims_total: 42,
            commit_epoch: Some(7),
            ack_count: 1,
            required_acks: 1,
            commit_status: "accepted".into(),
            checkpoint_triggered: true,
            checkpoint_snapshot_records: Some(10),
            checkpoint_truncated_wal_records: Some(5),
        };
        let json = serde_json::to_string(&resp).unwrap();
        let decoded: IngestApiResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(resp, decoded);
    }

    #[test]
    fn ingest_response_omits_optional_fields_when_none() {
        let resp = IngestApiResponse {
            ingested_claim_id: "c1".into(),
            claims_total: 1,
            commit_epoch: None,
            ack_count: 1,
            required_acks: 1,
            commit_status: "accepted".into(),
            checkpoint_triggered: false,
            checkpoint_snapshot_records: None,
            checkpoint_truncated_wal_records: None,
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(!json.contains("commit_epoch"));
        assert!(!json.contains("checkpoint_snapshot_records"));
        let decoded: IngestApiResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(resp, decoded);
    }

    #[test]
    fn write_consistency_policy_serializes_lowercase() {
        assert_eq!(
            serde_json::to_string(&WriteConsistencyPolicy::One).unwrap(),
            "\"one\""
        );
        assert_eq!(
            serde_json::to_string(&WriteConsistencyPolicy::Quorum).unwrap(),
            "\"quorum\""
        );
        assert_eq!(
            serde_json::to_string(&WriteConsistencyPolicy::All).unwrap(),
            "\"all\""
        );
    }

    // Reference imports to ensure these types stay re-exported at the module
    // boundary even if a refactor removes direct field access.
    #[allow(dead_code)]
    fn _types_in_use() {
        let _ = ClaimType::Temporal;
        let _ = Relation::DependsOn;
        let _ = Stance::Supports;
    }
}
