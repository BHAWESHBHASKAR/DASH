// ---------------------------------------------------------------------------
// Core domain enums
// ---------------------------------------------------------------------------

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Stance {
    Supports,
    Contradicts,
    Neutral,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Relation {
    Supports,
    Contradicts,
    Refines,
    Duplicates,
    DependsOn,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StanceMode {
    Balanced,
    SupportOnly,
}

/// The kind of claim: factual assertion, opinion, prediction, etc.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClaimType {
    Factual,
    Opinion,
    Prediction,
    Temporal,
    Causal,
}

// ---------------------------------------------------------------------------
// Core domain types — architecture §6
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct Claim {
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
    /// Architecture §6.1 — optional claim classification.
    #[serde(default)]
    pub claim_type: Option<ClaimType>,
    /// Architecture §6.1 — temporal validity window start (unix seconds).
    #[serde(default)]
    pub valid_from: Option<i64>,
    /// Architecture §6.1 — temporal validity window end (unix seconds).
    #[serde(default)]
    pub valid_to: Option<i64>,
    /// Epoch‐millis when this claim was first ingested.
    #[serde(default)]
    pub created_at: Option<i64>,
    /// Epoch‐millis of the most recent update.
    #[serde(default)]
    pub updated_at: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct Evidence {
    pub evidence_id: String,
    pub claim_id: String,
    pub source_id: String,
    pub stance: Stance,
    pub source_quality: f32,
    #[serde(default)]
    pub chunk_id: Option<String>,
    #[serde(default)]
    pub span_start: Option<u32>,
    #[serde(default)]
    pub span_end: Option<u32>,
    /// Architecture §6.2 — the document that produced this evidence.
    #[serde(default)]
    pub doc_id: Option<String>,
    /// Architecture §6.2 — which extraction model produced it.
    #[serde(default)]
    pub extraction_model: Option<String>,
    /// Epoch‐millis when this evidence was first ingested.
    #[serde(default)]
    pub ingested_at: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ClaimEdge {
    pub edge_id: String,
    pub from_claim_id: String,
    pub to_claim_id: String,
    pub relation: Relation,
    pub strength: f32,
    /// Architecture §6.3 — human‐readable reason codes for the edge.
    #[serde(default)]
    pub reason_codes: Vec<String>,
    /// Epoch‐millis when this edge was created.
    #[serde(default)]
    pub created_at: Option<i64>,
}

/// Named entity extracted from claims — architecture §3.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct Entity {
    pub name: String,
    pub entity_type: String,
    #[serde(default)]
    pub canonical_name: Option<String>,
}

// ---------------------------------------------------------------------------
// Retrieval request/response types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct RetrievalRequest {
    pub tenant_id: String,
    pub query: String,
    pub top_k: usize,
    pub stance_mode: StanceMode,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct Citation {
    pub evidence_id: String,
    pub source_id: String,
    pub stance: Stance,
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

/// Per-result breakdown of how the final score was produced. Surfaces the
/// dense + lexical signals and the fusion strategy so callers can audit and
/// explain ranking decisions (an evidence-grade requirement, not just a
/// black-box similarity).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ScoreExplanation {
    /// Fusion strategy applied: `semantic_primary`, `rrf`, or `lexical_only`.
    pub fusion: String,
    /// Dense (vector) similarity in the configured metric. 0.0 when the query
    /// carried no vector.
    pub dense_similarity: f32,
    /// Lexical/BM25 relevance signal.
    pub lexical_score: f32,
    /// Number of supporting edges considered.
    pub supports: usize,
    /// Number of contradicting edges considered.
    pub contradicts: usize,
    /// The final fused score (same value as `RetrievalResult::score`).
    pub final_score: f32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct RetrievalResult {
    pub claim_id: String,
    pub canonical_text: String,
    pub score: f32,
    pub supports: usize,
    pub contradicts: usize,
    pub citations: Vec<Citation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub score_explanation: Option<ScoreExplanation>,
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValidationError {
    MissingField(&'static str),
    InvalidRange(&'static str),
}

// ---------------------------------------------------------------------------
// Token utilities — shared across ranking, store, etc.
// ---------------------------------------------------------------------------

/// Normalize a token to lowercase ASCII alphanumeric for lexical matching.
pub fn normalize_token(token: &str) -> String {
    token
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .collect::<String>()
        .to_ascii_lowercase()
}

/// Tokenize text into normalized tokens, ready for indexing or matching.
pub fn tokenize(text: &str) -> Vec<String> {
    text.split_whitespace()
        .map(normalize_token)
        .filter(|t| !t.is_empty())
        .collect()
}

pub fn validate_claim(claim: &Claim) -> Result<(), ValidationError> {
    if claim.claim_id.trim().is_empty() {
        return Err(ValidationError::MissingField("claim_id"));
    }
    if claim.tenant_id.trim().is_empty() {
        return Err(ValidationError::MissingField("tenant_id"));
    }
    if claim.canonical_text.trim().is_empty() {
        return Err(ValidationError::MissingField("canonical_text"));
    }
    if !(0.0..=1.0).contains(&claim.confidence) {
        return Err(ValidationError::InvalidRange("confidence"));
    }
    for entity in &claim.entities {
        if entity.trim().is_empty() {
            return Err(ValidationError::MissingField("entities[]"));
        }
    }
    for embedding_id in &claim.embedding_ids {
        if embedding_id.trim().is_empty() {
            return Err(ValidationError::MissingField("embedding_ids[]"));
        }
    }
    // Validate temporal validity window
    if let (Some(from), Some(to)) = (claim.valid_from, claim.valid_to)
        && from > to
    {
        return Err(ValidationError::InvalidRange("valid_from/valid_to"));
    }
    Ok(())
}

pub fn validate_evidence(evidence: &Evidence) -> Result<(), ValidationError> {
    if evidence.evidence_id.trim().is_empty() {
        return Err(ValidationError::MissingField("evidence_id"));
    }
    if evidence.claim_id.trim().is_empty() {
        return Err(ValidationError::MissingField("claim_id"));
    }
    if evidence.source_id.trim().is_empty() {
        return Err(ValidationError::MissingField("source_id"));
    }
    if !(0.0..=1.0).contains(&evidence.source_quality) {
        return Err(ValidationError::InvalidRange("source_quality"));
    }
    if let Some(chunk_id) = &evidence.chunk_id
        && chunk_id.trim().is_empty()
    {
        return Err(ValidationError::MissingField("chunk_id"));
    }
    match (evidence.span_start, evidence.span_end) {
        (Some(start), Some(end)) => {
            if start > end {
                return Err(ValidationError::InvalidRange("span_range"));
            }
        }
        (None, None) => {}
        _ => return Err(ValidationError::InvalidRange("span_range")),
    }
    Ok(())
}

pub fn validate_edge(edge: &ClaimEdge) -> Result<(), ValidationError> {
    if edge.edge_id.trim().is_empty() {
        return Err(ValidationError::MissingField("edge_id"));
    }
    if edge.from_claim_id.trim().is_empty() {
        return Err(ValidationError::MissingField("from_claim_id"));
    }
    if edge.to_claim_id.trim().is_empty() {
        return Err(ValidationError::MissingField("to_claim_id"));
    }
    if !(0.0..=1.0).contains(&edge.strength) {
        return Err(ValidationError::InvalidRange("strength"));
    }
    for code in &edge.reason_codes {
        if code.trim().is_empty() {
            return Err(ValidationError::MissingField("reason_codes[]"));
        }
    }
    Ok(())
}

/// Helper to create a `Claim` with default optional fields.
/// Used throughout tests to avoid repetitive struct construction.
pub fn claim_builder(claim_id: &str, tenant_id: &str, text: &str, confidence: f32) -> Claim {
    Claim {
        claim_id: claim_id.to_string(),
        tenant_id: tenant_id.to_string(),
        canonical_text: text.to_string(),
        confidence,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn test_claim(id: &str, text: &str) -> Claim {
        claim_builder(id, "t1", text, 0.9)
    }

    #[test]
    fn validates_claim_successfully() {
        let claim = test_claim("c1", "A acquired B");
        assert_eq!(validate_claim(&claim), Ok(()));
    }

    #[test]
    fn rejects_claim_with_invalid_confidence() {
        let mut claim = test_claim("c1", "A acquired B");
        claim.confidence = 1.5;
        assert_eq!(
            validate_claim(&claim),
            Err(ValidationError::InvalidRange("confidence"))
        );
    }

    #[test]
    fn rejects_evidence_with_empty_source() {
        let ev = Evidence {
            evidence_id: "e1".to_string(),
            claim_id: "c1".to_string(),
            source_id: "  ".to_string(),
            stance: Stance::Supports,
            source_quality: 0.5,
            chunk_id: None,
            span_start: None,
            span_end: None,
            doc_id: None,
            extraction_model: None,
            ingested_at: None,
        };
        assert_eq!(
            validate_evidence(&ev),
            Err(ValidationError::MissingField("source_id"))
        );
    }

    #[test]
    fn rejects_edge_with_invalid_strength() {
        let edge = ClaimEdge {
            edge_id: "g1".to_string(),
            from_claim_id: "c1".to_string(),
            to_claim_id: "c2".to_string(),
            relation: Relation::Supports,
            strength: -0.1,
            reason_codes: vec![],
            created_at: None,
        };
        assert_eq!(
            validate_edge(&edge),
            Err(ValidationError::InvalidRange("strength"))
        );
    }

    #[test]
    fn rejects_evidence_with_partial_span() {
        let ev = Evidence {
            evidence_id: "e1".to_string(),
            claim_id: "c1".to_string(),
            source_id: "src".to_string(),
            stance: Stance::Supports,
            source_quality: 0.5,
            chunk_id: None,
            span_start: Some(10),
            span_end: None,
            doc_id: None,
            extraction_model: None,
            ingested_at: None,
        };
        assert_eq!(
            validate_evidence(&ev),
            Err(ValidationError::InvalidRange("span_range"))
        );
    }

    #[test]
    fn rejects_evidence_with_inverted_span() {
        let ev = Evidence {
            evidence_id: "e1".to_string(),
            claim_id: "c1".to_string(),
            source_id: "src".to_string(),
            stance: Stance::Supports,
            source_quality: 0.5,
            chunk_id: None,
            span_start: Some(20),
            span_end: Some(10),
            doc_id: None,
            extraction_model: None,
            ingested_at: None,
        };
        assert_eq!(
            validate_evidence(&ev),
            Err(ValidationError::InvalidRange("span_range"))
        );
    }

    #[test]
    fn rejects_claim_with_inverted_validity_window() {
        let mut claim = test_claim("c1", "A acquired B");
        claim.valid_from = Some(200);
        claim.valid_to = Some(100);
        assert_eq!(
            validate_claim(&claim),
            Err(ValidationError::InvalidRange("valid_from/valid_to"))
        );
    }

    #[test]
    fn tokenize_normalizes_and_splits() {
        let tokens = tokenize("Company X acquired Company-Y");
        assert_eq!(tokens, vec!["company", "x", "acquired", "companyy"]);
    }

    #[test]
    fn claim_builder_creates_valid_claim() {
        let claim = claim_builder("c1", "t1", "text", 0.5);
        assert_eq!(validate_claim(&claim), Ok(()));
    }

    // -----------------------------------------------------------------------
    // serde round-trip tests
    // -----------------------------------------------------------------------

    #[test]
    fn claim_serde_roundtrip_preserves_all_fields() {
        let original = Claim {
            claim_id: "c1".into(),
            tenant_id: "t1".into(),
            canonical_text: "text".into(),
            confidence: 0.85,
            event_time_unix: Some(1_700_000_000),
            entities: vec!["X".into(), "Y".into()],
            embedding_ids: vec!["emb://1".into()],
            claim_type: Some(ClaimType::Temporal),
            valid_from: Some(100),
            valid_to: Some(200),
            created_at: Some(1_700_000_000_000),
            updated_at: Some(1_700_000_001_000),
        };
        let json = serde_json::to_string(&original).unwrap();
        let decoded: Claim = serde_json::from_str(&json).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn claim_deserialize_omits_optional_fields() {
        let json = r#"{
            "claim_id": "c1",
            "tenant_id": "t1",
            "canonical_text": "text",
            "confidence": 0.9
        }"#;
        let claim: Claim = serde_json::from_str(json).unwrap();
        assert_eq!(claim.entities, Vec::<String>::new());
        assert_eq!(claim.embedding_ids, Vec::<String>::new());
        assert_eq!(claim.event_time_unix, None);
        assert_eq!(claim.claim_type, None);
    }

    #[test]
    fn stance_enum_serializes_lowercase() {
        assert_eq!(
            serde_json::to_string(&Stance::Supports).unwrap(),
            "\"supports\""
        );
        assert_eq!(
            serde_json::to_string(&Stance::Contradicts).unwrap(),
            "\"contradicts\""
        );
        assert_eq!(
            serde_json::to_string(&Stance::Neutral).unwrap(),
            "\"neutral\""
        );
    }

    #[test]
    fn relation_enum_serializes_snake_case() {
        assert_eq!(
            serde_json::to_string(&Relation::DependsOn).unwrap(),
            "\"depends_on\""
        );
        assert_eq!(
            serde_json::to_string(&Relation::Duplicates).unwrap(),
            "\"duplicates\""
        );
    }

    #[test]
    fn stance_mode_enum_serializes_snake_case() {
        assert_eq!(
            serde_json::to_string(&StanceMode::SupportOnly).unwrap(),
            "\"support_only\""
        );
        assert_eq!(
            serde_json::to_string(&StanceMode::Balanced).unwrap(),
            "\"balanced\""
        );
    }

    #[test]
    fn evidence_serde_roundtrip_preserves_all_fields() {
        let original = Evidence {
            evidence_id: "e1".into(),
            claim_id: "c1".into(),
            source_id: "src".into(),
            stance: Stance::Contradicts,
            source_quality: 0.7,
            chunk_id: Some("chunk-1".into()),
            span_start: Some(0),
            span_end: Some(100),
            doc_id: Some("doc".into()),
            extraction_model: Some("v3".into()),
            ingested_at: Some(1_700_000_000_000),
        };
        let json = serde_json::to_string(&original).unwrap();
        let decoded: Evidence = serde_json::from_str(&json).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn claim_edge_serde_roundtrip_preserves_all_fields() {
        let original = ClaimEdge {
            edge_id: "g1".into(),
            from_claim_id: "c1".into(),
            to_claim_id: "c2".into(),
            relation: Relation::Refines,
            strength: 0.5,
            reason_codes: vec!["contextual".into()],
            created_at: Some(1_700_000_000_000),
        };
        let json = serde_json::to_string(&original).unwrap();
        let decoded: ClaimEdge = serde_json::from_str(&json).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn retrieval_request_serde_uses_snake_case_fields() {
        let req = RetrievalRequest {
            tenant_id: "t1".into(),
            query: "what is X".into(),
            top_k: 10,
            stance_mode: StanceMode::SupportOnly,
        };
        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("\"top_k\""));
        assert!(json.contains("\"stance_mode\":\"support_only\""));
    }
}
