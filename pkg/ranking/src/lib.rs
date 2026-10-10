use std::collections::HashMap;

use schema::{Claim, tokenize};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RankSignals {
    pub supports: usize,
    pub contradicts: usize,
}

pub fn lexical_overlap_score(query: &str, text: &str) -> f32 {
    let query_tokens: Vec<String> = tokenize(query);
    if query_tokens.is_empty() {
        return 0.0;
    }

    let text_tokens: Vec<String> = tokenize(text);

    let mut hits = 0usize;
    for token in &query_tokens {
        if text_tokens.iter().any(|candidate| candidate == token) {
            hits += 1;
        }
    }
    hits as f32 / query_tokens.len() as f32
}

pub fn bm25_score(
    query: &str,
    doc_tokens: &[String],
    doc_freq: &HashMap<String, usize>,
    total_docs: usize,
    avg_doc_len: f32,
) -> f32 {
    if total_docs == 0 || doc_tokens.is_empty() || avg_doc_len <= f32::EPSILON {
        return 0.0;
    }

    let query_tokens = tokenize(query);
    if query_tokens.is_empty() {
        return 0.0;
    }

    let mut tf: HashMap<&str, usize> = HashMap::new();
    for token in doc_tokens {
        *tf.entry(token.as_str()).or_insert(0) += 1;
    }

    let k1 = 1.2_f32;
    let b = 0.75_f32;
    let doc_len = doc_tokens.len() as f32;

    let mut score = 0.0_f32;
    for token in query_tokens {
        let term_tf = tf.get(token.as_str()).copied().unwrap_or(0) as f32;
        if term_tf <= 0.0 {
            continue;
        }

        let df = doc_freq.get(&token).copied().unwrap_or(0) as f32;
        let idf = (((total_docs as f32 - df + 0.5) / (df + 0.5)) + 1.0).ln();
        let denom = term_tf + k1 * (1.0 - b + b * (doc_len / avg_doc_len));
        score += idf * ((term_tf * (k1 + 1.0)) / denom.max(f32::EPSILON));
    }
    score.max(0.0)
}

/// Marginal support bonus of the first distinct source (same slope as the
/// historical linear `0.08 * n` near zero).
pub const SUPPORT_PER_SOURCE: f32 = 0.08;
/// Marginal contradiction penalty of the first distinct source.
pub const CONTRADICTION_PER_SOURCE: f32 = 0.1;
/// Hard ceiling on the support bonus, however many sources there are.
pub const MAX_SUPPORT_BONUS: f32 = 0.4;
/// Hard ceiling on the contradiction penalty.
pub const MAX_CONTRADICTION_PENALTY: f32 = 0.5;

/// Saturating contribution of `count` distinct sources: roughly linear
/// (`count * per_source`) for small counts and bounded by `cap`
/// (IDX-05), so an author cannot buy rank by stacking supporting
/// edges or evidence.
pub fn saturating_signal(count: usize, per_source: f32, cap: f32) -> f32 {
    if count == 0 || cap <= 0.0 || per_source <= 0.0 {
        return 0.0;
    }
    cap * ((count as f32 * per_source) / cap).tanh()
}

/// The ranking signals that are not query relevance: saturated support
/// and contradiction, average source quality and claim confidence. Same
/// weights as the non-lexical part of [`score_claim`]. Range
/// `[-MAX_CONTRADICTION_PENALTY, MAX_SUPPORT_BONUS + 0.4]`.
pub fn prior_score(claim: &Claim, avg_source_quality: f32, signals: RankSignals) -> f32 {
    let support_score = saturating_signal(signals.supports, SUPPORT_PER_SOURCE, MAX_SUPPORT_BONUS);
    let contradiction_penalty = saturating_signal(
        signals.contradicts,
        CONTRADICTION_PER_SOURCE,
        MAX_CONTRADICTION_PENALTY,
    );
    support_score - contradiction_penalty + avg_source_quality * 0.15 + claim.confidence * 0.25
}

/// Weight of the prior signals ([`prior_score`]) next to query relevance
/// (in `[0, 1]`) in the final score.
pub const PRIOR_WEIGHT: f32 = 0.5;

/// Relevance of a lexical-only retrieve: the claim's BM25 divided by the
/// query's BM25 upper bound (`sum(idf * (k1 + 1))`), so in `[0, 1)`.
pub fn lexical_relevance(bm25_fraction: f32) -> f32 {
    if bm25_fraction.is_finite() {
        bm25_fraction.clamp(0.0, 1.0)
    } else {
        0.0
    }
}

/// Weight of vector similarity (mapped to `[0, 1]`) in hybrid relevance.
/// Twice [`HYBRID_TEXT_WEIGHT`] because the mapping halves the cosine
/// range: one unit of cosine and one unit of normalised BM25 weigh the
/// same, and a claim with cosine 1 and no query term still outranks a claim
/// with cosine 0 and any BM25 (normalised BM25 is always below 1).
pub const HYBRID_DENSE_WEIGHT: f32 = 2.0 / 3.0;
/// Weight of normalised BM25 in hybrid relevance.
pub const HYBRID_TEXT_WEIGHT: f32 = 1.0 / 3.0;

/// Relevance of a hybrid retrieve (a query vector is present): a
/// calibrated blend of vector similarity (cosine in `[-1, 1]` mapped to
/// `[0, 1]`; a claim without a vector counts as orthogonal, 0.5) and
/// normalised BM25 ([`lexical_relevance`]). In `[0, 1]`. See
/// [`HYBRID_DENSE_WEIGHT`] for the calibration.
pub fn hybrid_relevance(cosine: Option<f32>, bm25_fraction: f32) -> f32 {
    let cosine = cosine.filter(|c| c.is_finite()).unwrap_or(0.0).clamp(-1.0, 1.0);
    let dense = (cosine + 1.0) * 0.5;
    HYBRID_DENSE_WEIGHT * dense + HYBRID_TEXT_WEIGHT * lexical_relevance(bm25_fraction)
}

/// Final retrieve score: relevance plus weighted priors.
pub fn ranked_score(relevance: f32, priors: f32) -> f32 {
    relevance + PRIOR_WEIGHT * priors
}

pub fn score_claim_with_bm25(
    query: &str,
    claim: &Claim,
    avg_source_quality: f32,
    signals: RankSignals,
    bm25: f32,
) -> f32 {
    let base = score_claim(query, claim, avg_source_quality, signals);
    (base * 0.72) + (bm25 * 0.28)
}

pub fn score_claim(
    query: &str,
    claim: &Claim,
    avg_source_quality: f32,
    signals: RankSignals,
) -> f32 {
    let semantic = lexical_overlap_score(query, &claim.canonical_text);
    let support_score = saturating_signal(signals.supports, SUPPORT_PER_SOURCE, MAX_SUPPORT_BONUS);
    let contradiction_penalty = saturating_signal(
        signals.contradicts,
        CONTRADICTION_PER_SOURCE,
        MAX_CONTRADICTION_PENALTY,
    );
    let quality = avg_source_quality * 0.15;
    let confidence = claim.confidence * 0.25;

    (semantic * 0.6) + support_score - contradiction_penalty + quality + confidence
}

#[cfg(test)]
mod tests {
    use super::*;
    use schema::Claim;

    #[test]
    fn overlap_score_is_higher_for_more_matching_terms() {
        let strong = lexical_overlap_score("company x acquired y", "Company X acquired Company Y");
        let weak = lexical_overlap_score("company x acquired y", "Company Z opened a store");
        assert!(strong > weak);
    }

    #[test]
    fn scoring_penalizes_contradictions() {
        let claim = Claim {
            claim_id: "c1".into(),
            tenant_id: "t1".into(),
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
        };

        let with_support = score_claim(
            "did company x acquire company y",
            &claim,
            0.9,
            RankSignals {
                supports: 2,
                contradicts: 0,
            },
        );
        let with_contradiction = score_claim(
            "did company x acquire company y",
            &claim,
            0.9,
            RankSignals {
                supports: 2,
                contradicts: 2,
            },
        );
        assert!(with_support > with_contradiction);
    }

    #[test]
    fn support_bonus_saturates_instead_of_growing_linearly() {
        // Old formula: 0.08 * n, i.e. 80.0 for n = 1000.
        let huge = saturating_signal(1000, SUPPORT_PER_SOURCE, MAX_SUPPORT_BONUS);
        assert!(huge <= MAX_SUPPORT_BONUS + 1e-6, "bonus {huge} exceeds cap");
        let many = saturating_signal(50, SUPPORT_PER_SOURCE, MAX_SUPPORT_BONUS);
        assert!((huge - many).abs() < 0.01, "bonus should plateau");
    }

    #[test]
    fn saturating_signal_is_monotonic_and_near_linear_for_small_counts() {
        let mut previous = 0.0;
        for n in 0..20 {
            let value = saturating_signal(n, SUPPORT_PER_SOURCE, MAX_SUPPORT_BONUS);
            assert!(value >= previous);
            previous = value;
        }
        let one = saturating_signal(1, SUPPORT_PER_SOURCE, MAX_SUPPORT_BONUS);
        assert!((one - 0.08).abs() < 0.005);
        assert_eq!(saturating_signal(0, 0.08, 0.4), 0.0);
    }

    #[test]
    fn contradiction_penalty_is_capped() {
        let claim = Claim {
            claim_id: "c1".into(),
            tenant_id: "t1".into(),
            canonical_text: "alpha beta".into(),
            confidence: 0.5,
            event_time_unix: None,
            entities: vec![],
            embedding_ids: vec![],
            claim_type: None,
            valid_from: None,
            valid_to: None,
            created_at: None,
            updated_at: None,
        };
        let signals = |contradicts| RankSignals {
            supports: 0,
            contradicts,
        };
        let a = score_claim("alpha", &claim, 0.5, signals(100));
        let b = score_claim("alpha", &claim, 0.5, signals(100_000));
        assert!((a - b).abs() < 1e-3);
        let none = score_claim("alpha", &claim, 0.5, signals(0));
        assert!(none - b <= MAX_CONTRADICTION_PENALTY + 1e-4);
    }

    #[test]
    fn bm25_scores_relevant_doc_higher() {
        let doc_a = tokenize("company x acquired company y");
        let doc_b = tokenize("weather forecast for tomorrow");
        let mut df = HashMap::new();
        df.insert("company".to_string(), 1);
        df.insert("acquired".to_string(), 1);
        df.insert("y".to_string(), 1);
        let query = "did company acquire y";

        let a = bm25_score(query, &doc_a, &df, 2, 4.5);
        let b = bm25_score(query, &doc_b, &df, 2, 4.5);
        assert!(a > b);
    }
}
