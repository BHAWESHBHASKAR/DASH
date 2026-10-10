//! Retrieval quality on the labelled relevance set (`relevance/mod.rs`):
//! nDCG@10 and recall@10 of
//!
//! - `old_shared_word`: the previous lexical rule (every claim sharing a
//!   lowercased ASCII token is a candidate; score = overlap fraction, raw
//!   BM25 over those tokens and the prior signals, `ranking::score_claim_with_bm25`);
//! - `old_hybrid`: the previous hybrid scoring (shared-word candidates plus
//!   the 200 nearest vectors, `(cos + 1) / 2 + 0.1 * old lexical score`);
//! - `bm25`: the full-text index alone (`store::text_index`);
//! - `vector`: exact cosine over 384-d hash embeddings (`embeddings::HashEmbeddingProvider`);
//! - `store_lexical`: `InMemoryStore::retrieve` (BM25 candidates, BM25 + priors);
//! - `store_hybrid`: `InMemoryStore::retrieve_semantic` with the hash
//!   embedding of the query (BM25 and vector candidates, blended).
//!
//! The test prints the table (`cargo test -p store --test relevance_eval -- --nocapture`)
//! and fails when a system falls below its recorded floor: the numbers of
//! the commit that introduced the index minus a small margin. Raise the
//! floors when quality improves; never lower them to make a change pass.

mod relevance;

use std::collections::{HashMap, HashSet};

use embeddings::{EmbeddingProvider, HashEmbeddingProvider};
use ranking::{RankSignals, bm25_score, score_claim_with_bm25};
use relevance::{RelevanceSet, ndcg_at, recall_at, relevance_set};
use schema::{RetrievalRequest, StanceMode, claim_builder, tokenize};
use store::InMemoryStore;
use store::text_index::{TenantTextIndex, analyze_query};

const TENANT: &str = "eval";
const K: usize = 10;

/// (system, nDCG@10 floor, recall@10 floor). Measured values are in
/// docs/benchmarks/retrieval-quality.md; floors sit 0.02 below them.
const FLOORS: &[(&str, f64, f64)] = &[
    ("bm25", 0.854, 0.817),
    ("store_lexical", 0.843, 0.806),
    ("store_hybrid", 0.775, 0.661),
];

/// The previous lexical scoring of every claim: (shares a token with the
/// query, `score_claim_with_bm25`), as `InMemoryStore` computed it before
/// the full-text index (no evidence, no edges in this set).
fn old_lexical_scores(set: &RelevanceSet, query: &str) -> Vec<(bool, f32)> {
    let tokens: Vec<Vec<String>> = set.claims.iter().map(|c| tokenize(&c.text)).collect();
    let mut doc_freq: HashMap<String, usize> = HashMap::new();
    for doc in &tokens {
        for token in doc.iter().collect::<HashSet<_>>() {
            *doc_freq.entry(token.clone()).or_insert(0) += 1;
        }
    }
    let total: usize = tokens.iter().map(Vec::len).sum();
    let avg = (total as f32 / tokens.len() as f32).max(1.0);
    let query_tokens: HashSet<String> = tokenize(query).into_iter().collect();
    set.claims
        .iter()
        .zip(&tokens)
        .map(|(claim, doc)| {
            let bm25 = bm25_score(query, doc, &doc_freq, tokens.len(), avg);
            let full = claim_builder(&claim.id, TENANT, &claim.text, claim.confidence);
            let score = score_claim_with_bm25(
                query,
                &full,
                0.0,
                RankSignals {
                    supports: 0,
                    contradicts: 0,
                },
                bm25,
            );
            (doc.iter().any(|t| query_tokens.contains(t)), score)
        })
        .collect()
}

/// Claim ids by descending score; equal scores keep claim-id order (the
/// old store sorted a claim-id-ordered candidate list with a stable sort).
fn rank(mut scored: Vec<(String, f32)>) -> Vec<String> {
    scored.sort_by(|a, b| a.0.cmp(&b.0));
    scored.sort_by(|a, b| b.1.total_cmp(&a.1));
    scored.into_iter().map(|(id, _)| id).collect()
}

struct Systems {
    set: RelevanceSet,
    index: TenantTextIndex,
    vectors: Vec<Vec<f32>>,
    store: InMemoryStore,
    provider: HashEmbeddingProvider,
}

impl Systems {
    fn new() -> Self {
        let set = relevance_set();
        let provider = HashEmbeddingProvider::new(384);
        let texts: Vec<String> = set.claims.iter().map(|c| c.text.clone()).collect();
        let vectors = provider.embed(&texts).expect("hash embeddings");
        let mut index = TenantTextIndex::new();
        let mut store = InMemoryStore::new();
        for (claim, vector) in set.claims.iter().zip(&vectors) {
            index.insert(&claim.id, &claim.text);
            store
                .ingest_bundle(
                    claim_builder(&claim.id, TENANT, &claim.text, claim.confidence),
                    vec![],
                    vec![],
                )
                .expect("ingest");
            store
                .upsert_claim_vector(&claim.id, vector.clone())
                .expect("vector");
        }
        Self {
            set,
            index,
            vectors,
            store,
            provider,
        }
    }

    fn query_vector(&self, query: &str) -> Vec<f32> {
        self.provider
            .embed(&[query.to_string()])
            .expect("hash embedding")
            .remove(0)
    }

    /// Cosine of the query's hash embedding with every claim's (both are
    /// unit vectors).
    fn cosines(&self, query: &str) -> Vec<f32> {
        let q = self.query_vector(query);
        self.vectors
            .iter()
            .map(|v| v.iter().zip(&q).map(|(a, b)| a * b).sum())
            .collect()
    }

    fn run(&self, system: &str, query: &str) -> Vec<String> {
        let request = RetrievalRequest {
            tenant_id: TENANT.into(),
            query: query.into(),
            top_k: K,
            stance_mode: StanceMode::Balanced,
        };
        match system {
            "old_shared_word" => rank(
                self.set
                    .claims
                    .iter()
                    .zip(old_lexical_scores(&self.set, query))
                    .filter(|(_, (candidate, _))| *candidate)
                    .map(|(c, (_, score))| (c.id.clone(), score))
                    .collect(),
            ),
            "old_hybrid" => {
                // Old hybrid: shared-word candidates plus the 200 nearest
                // vectors, scored (cos + 1) / 2 + 0.1 * old lexical score.
                let cosines = self.cosines(query);
                let mut by_cosine: Vec<(usize, f32)> =
                    cosines.iter().copied().enumerate().collect();
                by_cosine.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
                let nearest: HashSet<usize> = by_cosine.iter().take(200).map(|(i, _)| *i).collect();
                rank(
                    old_lexical_scores(&self.set, query)
                        .into_iter()
                        .enumerate()
                        .filter(|(i, (candidate, _))| *candidate || nearest.contains(i))
                        .map(|(i, (_, lexical))| {
                            let dense = (cosines[i] + 1.0) * 0.5;
                            (self.set.claims[i].id.clone(), dense + lexical * 0.1)
                        })
                        .collect(),
                )
            }
            "bm25" => self
                .index
                .search(&analyze_query(query), K, None)
                .into_iter()
                .map(|(id, _)| id)
                .collect(),
            "vector" => rank(
                self.set
                    .claims
                    .iter()
                    .zip(self.cosines(query))
                    .map(|(c, cosine)| (c.id.clone(), cosine))
                    .collect(),
            ),
            "store_lexical" => self
                .store
                .retrieve(&request)
                .into_iter()
                .map(|r| r.claim_id)
                .collect(),
            "store_hybrid" => self
                .store
                .retrieve_semantic(&request, &self.query_vector(query))
                .into_iter()
                .map(|r| r.claim_id)
                .collect(),
            other => panic!("unknown system {other}"),
        }
    }

    /// Mean nDCG@10 and recall@10 over every query.
    fn evaluate(&self, system: &str) -> (f64, f64) {
        let (mut ndcg, mut recall) = (0.0, 0.0);
        for query in &self.set.queries {
            let ranked = self.run(system, &query.text);
            ndcg += ndcg_at(K, &ranked, &query.relevant);
            recall += recall_at(K, &ranked, &query.relevant);
        }
        let n = self.set.queries.len() as f64;
        (ndcg / n, recall / n)
    }
}

#[test]
fn relevance_set_has_the_documented_shape() {
    let set = relevance_set();
    assert_eq!(set.claims.len(), 448);
    assert_eq!(set.queries.len(), 72);
    let ids: HashSet<&str> = set.claims.iter().map(|c| c.id.as_str()).collect();
    assert_eq!(ids.len(), set.claims.len());
    for query in &set.queries {
        assert_eq!(query.relevant.len(), 16, "{}", query.text);
        assert!(query.relevant.keys().all(|id| ids.contains(id.as_str())));
    }
    // Deterministic: a second generation is identical.
    let again = relevance_set();
    assert!(
        set.claims
            .iter()
            .zip(&again.claims)
            .all(|(a, b)| a.text == b.text)
    );
}

#[test]
fn retrieval_quality_does_not_regress() {
    let systems = Systems::new();
    let names = [
        "old_shared_word",
        "old_hybrid",
        "bm25",
        "vector",
        "store_lexical",
        "store_hybrid",
    ];
    let mut measured = HashMap::new();
    println!("system            nDCG@10  recall@10");
    for name in names {
        let (ndcg, recall) = systems.evaluate(name);
        println!("{name:<17} {ndcg:.4}   {recall:.4}");
        measured.insert(name, (ndcg, recall));
    }
    for (name, ndcg_floor, recall_floor) in FLOORS {
        let (ndcg, recall) = measured[name];
        assert!(ndcg >= *ndcg_floor, "{name}: nDCG@10 {ndcg:.4} < floor {ndcg_floor}");
        assert!(
            recall >= *recall_floor,
            "{name}: recall@10 {recall:.4} < floor {recall_floor}"
        );
    }
    // The index must beat the rule it replaced.
    assert!(measured["bm25"].0 > measured["old_shared_word"].0);
    assert!(measured["store_lexical"].0 > measured["old_shared_word"].0);
    assert!(measured["store_hybrid"].0 > measured["old_hybrid"].0);
}

