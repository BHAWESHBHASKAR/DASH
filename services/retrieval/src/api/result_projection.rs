use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct TemporalAnnotation {
    pub(super) match_mode: Option<&'static str>,
    pub(super) in_range: Option<bool>,
}

pub(super) fn evidence_node_from_parts(
    claim_id: String,
    canonical_text: String,
    signals: EvidenceNodeSignals,
    claim: Option<&Claim>,
    query_from_unix: Option<i64>,
    query_to_unix: Option<i64>,
) -> EvidenceNode {
    let temporal_annotation = temporal_annotation_for_claim(claim, query_from_unix, query_to_unix);
    EvidenceNode {
        claim_id,
        canonical_text,
        score: signals.score,
        claim_confidence: claim.map(|value| value.confidence),
        confidence_band: claim
            .map(|value| confidence_band_for_claim_confidence(value.confidence).to_string()),
        dominant_stance: dominant_stance_for_counts(signals.supports, signals.contradicts)
            .map(str::to_string),
        contradiction_risk: contradiction_risk_for_counts(signals.supports, signals.contradicts),
        graph_score: None,
        support_path_count: None,
        contradiction_chain_depth: None,
        supports: signals.supports,
        contradicts: signals.contradicts,
        citations: signals.citations,
        event_time_unix: claim.and_then(|value| value.event_time_unix),
        temporal_match_mode: temporal_annotation.match_mode.map(str::to_string),
        temporal_in_range: temporal_annotation.in_range,
        claim_type: claim
            .and_then(|value| value.claim_type.as_ref())
            .map(claim_type_to_str)
            .map(str::to_string),
        valid_from: claim.and_then(|value| value.valid_from),
        valid_to: claim.and_then(|value| value.valid_to),
        created_at: claim.and_then(|value| value.created_at),
        updated_at: claim.and_then(|value| value.updated_at),
    }
}

pub(super) fn temporal_annotation_for_claim(
    claim: Option<&Claim>,
    query_from_unix: Option<i64>,
    query_to_unix: Option<i64>,
) -> TemporalAnnotation {
    if query_from_unix.is_none() && query_to_unix.is_none() {
        return TemporalAnnotation {
            match_mode: None,
            in_range: None,
        };
    }

    let Some(claim) = claim else {
        return TemporalAnnotation {
            match_mode: Some("missing_claim"),
            in_range: Some(false),
        };
    };

    let has_event = claim.event_time_unix.is_some();
    let has_validity = claim.valid_from.is_some() || claim.valid_to.is_some();
    let event_match = claim
        .event_time_unix
        .is_some_and(|value| value_in_time_range(value, query_from_unix, query_to_unix));
    let validity_match = has_validity
        && time_windows_overlap(
            claim.valid_from,
            claim.valid_to,
            query_from_unix,
            query_to_unix,
        );

    let (mode, in_range) = match (has_event, has_validity) {
        (true, true) => ("event_and_validity_window", event_match && validity_match),
        (true, false) => ("event_time", event_match),
        (false, true) => ("validity_window", validity_match),
        (false, false) => ("no_temporal_data", false),
    };
    TemporalAnnotation {
        match_mode: Some(mode),
        in_range: Some(in_range),
    }
}

pub(super) fn claim_type_to_str(value: &ClaimType) -> &'static str {
    match value {
        ClaimType::Factual => "factual",
        ClaimType::Opinion => "opinion",
        ClaimType::Prediction => "prediction",
        ClaimType::Temporal => "temporal",
        ClaimType::Causal => "causal",
    }
}

pub(super) fn confidence_band_for_claim_confidence(value: f32) -> &'static str {
    if value >= 0.8 {
        "high"
    } else if value >= 0.5 {
        "medium"
    } else {
        "low"
    }
}

pub(super) fn dominant_stance_for_counts(
    supports: usize,
    contradicts: usize,
) -> Option<&'static str> {
    if supports == 0 && contradicts == 0 {
        None
    } else if supports > contradicts {
        Some("supports")
    } else if contradicts > supports {
        Some("contradicts")
    } else {
        Some("balanced")
    }
}

pub(super) fn contradiction_risk_for_counts(supports: usize, contradicts: usize) -> Option<f32> {
    let total = supports + contradicts;
    if total == 0 {
        None
    } else {
        Some(contradicts as f32 / total as f32)
    }
}

fn value_in_time_range(value: i64, from_unix: Option<i64>, to_unix: Option<i64>) -> bool {
    if let Some(from) = from_unix
        && value < from
    {
        return false;
    }
    if let Some(to) = to_unix
        && value > to
    {
        return false;
    }
    true
}

fn time_windows_overlap(
    window_start: Option<i64>,
    window_end: Option<i64>,
    query_from: Option<i64>,
    query_to: Option<i64>,
) -> bool {
    let start = window_start.unwrap_or(i64::MIN);
    let end = window_end.unwrap_or(i64::MAX);
    let query_start = query_from.unwrap_or(i64::MIN);
    let query_end = query_to.unwrap_or(i64::MAX);
    start <= query_end && end >= query_start
}
