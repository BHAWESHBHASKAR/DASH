//! Full-text index cost at scale: build time, ingest overhead, memory per
//! claim, query latency (index alone, store text-only retrieve, store hybrid
//! retrieve), delete cost, and the previous shared-word rule on the same
//! queries for comparison.
//!
//! Corpus (same shape as the ADR 0003 text spike): Zipf (s = 1.0) over a
//! 50k-word vocabulary, 30..=60 words per claim, one tenant. Queries are OR
//! queries of 1, 3 or 6 distinct words taken from a random claim, either
//! Zipf-weighted ("natural", head words dominate) or restricted to words of
//! rank >= 100 ("midtail").
//!
//! Run in release mode:
//!
//! ```bash
//! cargo run --release -p benchmark-smoke --bin fulltext_bench -- [N] [QUERIES] [DIM]
//! ```
//!
//! Defaults: N=100000, QUERIES=1000, DIM=64 (vectors for the hybrid cell;
//! 0 skips it). Prints one line per measurement and a final
//! `FULLTEXT_BENCH_JSON:` line.

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use ranking::{RankSignals, bm25_score, score_claim_with_bm25};
use schema::{RetrievalRequest, StanceMode, claim_builder, tokenize};
use store::InMemoryStore;
use store::text_index::{TenantTextIndex, analyze_query};

const TENANT: &str = "bench";
const VOCAB: usize = 50_000;

struct Rng(u64);

impl Rng {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn unit(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next_u64() % n as u64) as usize
    }
}

fn corpus(n: usize) -> Vec<Vec<u32>> {
    let mut cdf = Vec::with_capacity(VOCAB);
    let mut acc = 0.0f64;
    for r in 0..VOCAB {
        acc += 1.0 / (r as f64 + 1.0);
        cdf.push(acc);
    }
    let mut rng = Rng(42);
    (0..n)
        .map(|_| {
            let len = 30 + rng.below(31);
            (0..len)
                .map(|_| {
                    let u = rng.unit() * acc;
                    cdf.partition_point(|&c| c < u).min(VOCAB - 1) as u32
                })
                .collect()
        })
        .collect()
}

fn text(doc: &[u32]) -> String {
    doc.iter()
        .map(|t| format!("w{t}"))
        .collect::<Vec<_>>()
        .join(" ")
}

fn queries(docs: &[Vec<u32>], terms: usize, midtail: bool, n: usize, seed: u64) -> Vec<String> {
    let mut rng = Rng(seed);
    let mut out = Vec::with_capacity(n);
    while out.len() < n {
        let doc = &docs[rng.below(docs.len())];
        let mut chosen: Vec<u32> = Vec::new();
        for _ in 0..50 {
            let t = doc[rng.below(doc.len())];
            if (!midtail || t >= 100) && !chosen.contains(&t) {
                chosen.push(t);
            }
            if chosen.len() == terms {
                break;
            }
        }
        if chosen.len() == terms {
            out.push(text(&chosen));
        }
    }
    out
}

fn percentile(sorted: &[Duration], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let at = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[at].as_secs_f64() * 1e6
}

/// (p50, p95, p99) in microseconds.
fn latencies(mut samples: Vec<Duration>) -> (f64, f64, f64) {
    samples.sort();
    (
        percentile(&samples, 0.50),
        percentile(&samples, 0.95),
        percentile(&samples, 0.99),
    )
}

fn rss_kb() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("VmRSS:"))
                .and_then(|l| l.split_whitespace().nth(1))
                .and_then(|v| v.parse().ok())
        })
        .unwrap_or(0)
}

/// The rule the index replaced: every claim sharing a token is a candidate,
/// each scored with the composite lexical score (document frequencies and
/// the average length recomputed per query, as the store did).
struct OldRule {
    postings: HashMap<String, HashSet<usize>>,
    tokens: Vec<Vec<String>>,
}

impl OldRule {
    fn new(texts: &[String]) -> Self {
        let tokens: Vec<Vec<String>> = texts.iter().map(|t| tokenize(t)).collect();
        let mut postings: HashMap<String, HashSet<usize>> = HashMap::new();
        for (i, doc) in tokens.iter().enumerate() {
            for token in doc {
                postings.entry(token.clone()).or_default().insert(i);
            }
        }
        Self { postings, tokens }
    }

    fn top10(&self, texts: &[String], query: &str) -> usize {
        let query_tokens = tokenize(query);
        let mut candidates: HashSet<usize> = HashSet::new();
        for token in &query_tokens {
            if let Some(ids) = self.postings.get(token) {
                candidates.extend(ids.iter().copied());
            }
        }
        let total: usize = self.tokens.iter().map(Vec::len).sum();
        let avg = (total as f32 / self.tokens.len() as f32).max(1.0);
        let doc_freq: HashMap<String, usize> = query_tokens
            .iter()
            .map(|t| (t.clone(), self.postings.get(t).map_or(0, HashSet::len)))
            .collect();
        let mut scored: Vec<(usize, f32)> = candidates
            .iter()
            .map(|&i| {
                let bm25 = bm25_score(query, &self.tokens[i], &doc_freq, self.tokens.len(), avg);
                let claim = claim_builder("c", TENANT, &texts[i], 0.8);
                let score = score_claim_with_bm25(
                    query,
                    &claim,
                    0.0,
                    RankSignals {
                        supports: 0,
                        contradicts: 0,
                    },
                    bm25,
                );
                (i, score)
            })
            .collect();
        scored.sort_by(|a, b| b.1.total_cmp(&a.1));
        scored.truncate(10);
        candidates.len()
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let n: usize = args.get(1).and_then(|v| v.parse().ok()).unwrap_or(100_000);
    let nq: usize = args.get(2).and_then(|v| v.parse().ok()).unwrap_or(1000);
    let dim: usize = args.get(3).and_then(|v| v.parse().ok()).unwrap_or(64);
    let mut json = serde_json::Map::new();
    json.insert("claims".into(), n.into());

    let docs = corpus(n);
    let texts: Vec<String> = docs.iter().map(|d| text(d)).collect();
    let words: usize = docs.iter().map(Vec::len).sum();
    println!("corpus: {n} claims, {words} words");

    // 1. Index alone.
    let started = Instant::now();
    let mut index = TenantTextIndex::new();
    for (i, t) in texts.iter().enumerate() {
        index.insert(&format!("c{i:07}"), t);
    }
    let build = started.elapsed();
    let heap = index.heap_bytes();
    println!(
        "index build: {:.2} s ({:.1} us/claim), {} terms, heap {:.1} MB ({} B/claim)",
        build.as_secs_f64(),
        build.as_secs_f64() * 1e6 / n as f64,
        index.term_count(),
        heap as f64 / 1e6,
        heap / n
    );
    json.insert("index_build_s".into(), build.as_secs_f64().into());
    json.insert("index_heap_bytes".into(), heap.into());
    json.insert("index_heap_bytes_per_claim".into(), (heap / n).into());

    // 2. Store ingest (claims only), RSS growth per claim.
    let rss_before = rss_kb();
    let started = Instant::now();
    let mut store = InMemoryStore::new();
    for (i, t) in texts.iter().enumerate() {
        store
            .ingest_bundle(
                claim_builder(&format!("c{i:07}"), TENANT, t, 0.8),
                vec![],
                vec![],
            )
            .expect("ingest");
    }
    let ingest = started.elapsed();
    let rss_after = rss_kb();
    println!(
        "store ingest: {:.2} s ({:.1} us/claim; the index insert is {:.0}% of it), RSS +{:.1} MB ({} B/claim)",
        ingest.as_secs_f64(),
        ingest.as_secs_f64() * 1e6 / n as f64,
        100.0 * build.as_secs_f64() / ingest.as_secs_f64(),
        (rss_after.saturating_sub(rss_before)) as f64 / 1e3,
        rss_after.saturating_sub(rss_before) * 1024 / n as u64
    );
    json.insert("store_ingest_s".into(), ingest.as_secs_f64().into());
    json.insert(
        "store_rss_bytes_per_claim".into(),
        (rss_after.saturating_sub(rss_before) * 1024 / n as u64).into(),
    );

    // 3. Query latency.
    let old = OldRule::new(&texts);
    let mut cells = Vec::new();
    for midtail in [false, true] {
        for terms in [1usize, 3, 6] {
            let mix = if midtail { "midtail" } else { "natural" };
            let qs = queries(
                &docs,
                terms,
                midtail,
                nq,
                7 + terms as u64 + midtail as u64 * 100,
            );
            // Warm-up.
            for q in qs.iter().take(20) {
                let _ = store.retrieve(&request(q));
            }
            let mut index_lat = Vec::with_capacity(nq);
            let mut store_lat = Vec::with_capacity(nq);
            let mut candidates = 0usize;
            for q in &qs {
                let parsed = analyze_query(q);
                let t = Instant::now();
                let hits = index.search(&parsed, 200, None);
                index_lat.push(t.elapsed());
                candidates += hits.len();
                let t = Instant::now();
                let results = store.retrieve(&request(q));
                store_lat.push(t.elapsed());
                assert!(!results.is_empty());
            }
            let old_n = nq.min(100);
            let mut old_lat = Vec::with_capacity(old_n);
            let mut old_candidates = 0usize;
            for q in qs.iter().take(old_n) {
                let t = Instant::now();
                old_candidates += old.top10(&texts, q);
                old_lat.push(t.elapsed());
            }
            let (i50, i95, i99) = latencies(index_lat);
            let (s50, s95, s99) = latencies(store_lat);
            let (o50, o95, o99) = latencies(old_lat);
            println!(
                "{mix:<7} {terms} terms: index top-200 {i50:.0}/{i95:.0}/{i99:.0} us, store retrieve {s50:.0}/{s95:.0}/{s99:.0} us, old rule {o50:.0}/{o95:.0}/{o99:.0} us (old candidates avg {})",
                old_candidates / old_n
            );
            cells.push(serde_json::json!({
                "mix": mix, "terms": terms,
                "index_top200_us": [i50, i95, i99],
                "store_retrieve_us": [s50, s95, s99],
                "old_rule_us": [o50, o95, o99],
                "old_rule_avg_candidates": old_candidates / old_n,
                "index_avg_hits": candidates / nq,
            }));
        }
    }
    json.insert("queries".into(), cells.into());
    drop(old);

    // 4. Hybrid retrieve with vectors.
    if dim > 0 {
        let mut rng = Rng(9);
        let started = Instant::now();
        for i in 0..n {
            let v: Vec<f32> = (0..dim).map(|_| rng.unit() as f32 - 0.5).collect();
            store
                .upsert_claim_vector(&format!("c{i:07}"), v)
                .expect("vector");
        }
        println!(
            "vectors: {n} x {dim} in {:.1} s",
            started.elapsed().as_secs_f64()
        );
        let qs = queries(&docs, 3, false, nq, 99);
        let mut lat = Vec::with_capacity(nq);
        for q in &qs {
            let v: Vec<f32> = (0..dim).map(|_| rng.unit() as f32 - 0.5).collect();
            let t = Instant::now();
            let _ = store.retrieve_semantic(&request(q), &v);
            lat.push(t.elapsed());
        }
        let (h50, h95, h99) = latencies(lat);
        println!("hybrid (natural, 3 terms, {dim}-d): {h50:.0}/{h95:.0}/{h99:.0} us");
        json.insert("hybrid_us".into(), serde_json::json!([h50, h95, h99]));
    }

    // 5. Deletes (index alone).
    let started = Instant::now();
    for (i, text) in texts.iter().enumerate().take(1000) {
        index.remove(&format!("c{i:07}"), text);
    }
    let deletes = started.elapsed();
    println!(
        "index delete: {:.1} us/claim",
        deletes.as_secs_f64() * 1e6 / 1000.0
    );
    json.insert(
        "index_delete_us".into(),
        (deletes.as_secs_f64() * 1e6 / 1000.0).into(),
    );

    println!("FULLTEXT_BENCH_JSON: {}", serde_json::Value::Object(json));
}

fn request(query: &str) -> RetrievalRequest {
    RetrievalRequest {
        tenant_id: TENANT.into(),
        query: query.into(),
        top_k: 10,
        stance_mode: StanceMode::Balanced,
    }
}
