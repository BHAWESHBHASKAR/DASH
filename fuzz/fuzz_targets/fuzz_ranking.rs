#![no_main]
use libfuzzer_sys::fuzz_target;

use std::collections::HashMap;

use arbitrary::Arbitrary;
use ranking::{bm25_score, score_claim, RankSignals};
use schema::Claim;

#[derive(Arbitrary, Debug)]
struct FuzzInput {
    query: String,
    claim_text: String,
    confidence: f32,
    source_quality: f32,
    supports: usize,
    contradicts: usize,
}

fn sanitize_f32(value: f32) -> f32 {
    if value.is_finite() {
        value
    } else {
        0.0
    }
}

fuzz_target!(|input: FuzzInput| {
    let confidence = sanitize_f32(input.confidence).clamp(0.0, 1.0);
    let claim = Claim {
        claim_id: "c1".into(),
        tenant_id: "t1".into(),
        canonical_text: input.claim_text,
        confidence,
        event_time_unix: None,
        entities: vec![],
        embedding_ids: vec![],
        claim_type: None,
        valid_from: None,
        valid_to: None,
        created_at: None,
        updated_at: None,
    };
    let signals = RankSignals {
        supports: input.supports,
        contradicts: input.contradicts,
    };
    let source_quality = sanitize_f32(input.source_quality);
    let score = score_claim(&input.query, &claim, source_quality, signals);
    assert!(
        score.is_finite(),
        "score_claim produced non-finite score: {score}"
    );

    let bm = bm25_score(&input.query, &[], &HashMap::new(), 0, 1.0);
    assert!(
        bm.is_finite(),
        "bm25_score produced non-finite score: {bm}"
    );
});
