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
    let support_score = signals.supports as f32 * 0.08;
    let contradiction_penalty = signals.contradicts as f32 * 0.1;
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
