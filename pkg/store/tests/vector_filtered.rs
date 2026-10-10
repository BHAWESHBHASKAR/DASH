//! Filtered vector search through the public store API (IDX-01, IDX-03).
//!
//! A time-range / entity / allowed-id filter must give the same candidates as
//! an exact search over the allowed set: exactly the same set when the
//! allowed set is small enough to be scanned exactly (at most the flat
//! threshold), and recall@10 >= 0.95 when it is large and the HNSW runs with
//! the filter as a predicate. Tests run with the default threshold (8192) on
//! a 12k-vector tenant and with a threshold of 200 on a 3k-vector tenant.

use schema::{Claim, RetrievalRequest, StanceMode};
use std::collections::HashSet;
use store::{AnnTuningConfig, InMemoryStore};

const DIM: usize = 48;
const TENANT: &str = "t0";
const BASE_TS: i64 = 1_700_000_000;
const ENTITIES: usize = 7;

struct Rng(u64);

impl Rng {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn gauss(&mut self) -> f32 {
        let u1 = ((self.next_u64() >> 11) as f64 + 1.0) / (1u64 << 53) as f64;
        let u2 = ((self.next_u64() >> 11) as f64 + 1.0) / (1u64 << 53) as f64;
        ((-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos()) as f32
    }
}

fn unit(mut v: Vec<f32>) -> Vec<f32> {
    let n = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    v.iter_mut().for_each(|x| *x /= n);
    v
}

/// Clustered points; point `i` has event time `BASE_TS + i` and entity
/// `entity-(i % 7)`, so time and entity filters are independent of the
/// clusters (the hard, uncorrelated case from ADR 0003).
fn dataset(n: usize, seed: u64) -> (Vec<Vec<f32>>, Vec<Vec<f32>>) {
    let mut rng = Rng(seed);
    let centers: Vec<Vec<f32>> = (0..24)
        .map(|_| unit((0..DIM).map(|_| rng.gauss()).collect()))
        .collect();
    let point = |rng: &mut Rng| {
        let c = &centers[(rng.next_u64() % 24) as usize];
        unit(c.iter().map(|x| x + 0.1 * rng.gauss()).collect())
    };
    let base = (0..n).map(|_| point(&mut rng)).collect();
    let mut qrng = Rng(seed ^ 0xABCD_EF01_2345_6789);
    let queries = (0..30).map(|_| point(&mut qrng)).collect();
    (base, queries)
}

fn claim(i: usize) -> Claim {
    Claim {
        claim_id: format!("c{i}"),
        tenant_id: TENANT.to_string(),
        canonical_text: format!("claim {i}"),
        confidence: 0.5,
        event_time_unix: Some(BASE_TS + i as i64),
        entities: vec![format!("entity-{}", i % ENTITIES)],
        embedding_ids: vec![],
        claim_type: None,
        valid_from: None,
        valid_to: None,
        created_at: None,
        updated_at: None,
    }
}

fn build(vectors: &[Vec<f32>], tuning: AnnTuningConfig) -> InMemoryStore {
    let mut store = InMemoryStore::new_with_ann_tuning(tuning);
    for (i, v) in vectors.iter().enumerate() {
        store.ingest_bundle(claim(i), vec![], vec![]).unwrap();
        store
            .upsert_claim_vector(&format!("c{i}"), v.clone())
            .unwrap();
    }
    store
}

fn dot(a: &[f32], b: &[f32]) -> f64 {
    a.iter()
        .zip(b)
        .map(|(x, y)| f64::from(*x) * f64::from(*y))
        .sum()
}

fn exact_over(
    vectors: &[Vec<f32>],
    allowed: impl Iterator<Item = usize>,
    q: &[f32],
    k: usize,
) -> Vec<String> {
    let mut scored: Vec<(f64, usize)> = allowed.map(|i| (dot(q, &vectors[i]), i)).collect();
    scored.sort_by(|a, b| b.0.total_cmp(&a.0).then(a.1.cmp(&b.1)));
    scored
        .into_iter()
        .take(k)
        .map(|(_, i)| format!("c{i}"))
        .collect()
}

fn recall(found: &[String], truth: &[String]) -> f64 {
    let t: HashSet<&String> = truth.iter().collect();
    found.iter().filter(|id| t.contains(id)).count() as f64 / truth.len() as f64
}

/// Compare the store's filtered candidates with exact search over the
/// allowed indices. `exact` demands set equality, otherwise mean recall >= 0.95.
struct Ctx<'a> {
    store: &'a InMemoryStore,
    vectors: &'a [Vec<f32>],
    queries: &'a [Vec<f32>],
}

fn check(
    label: &str,
    ctx: &Ctx,
    time_range: (Option<i64>, Option<i64>),
    allowed_ids: Option<&HashSet<String>>,
    allowed_idx: &[usize],
    exact: bool,
) {
    let Ctx {
        store,
        vectors,
        queries,
    } = ctx;
    let mut total = 0.0;
    for q in *queries {
        let truth = exact_over(vectors, allowed_idx.iter().copied(), q, 10);
        let found =
            store.ann_vector_top_candidates_filtered(TENANT, q, 10, time_range, allowed_ids);
        assert_eq!(
            found.len(),
            truth.len(),
            "{label}: filtered search must still return top-N of the allowed set"
        );
        let allowed_set: HashSet<usize> = allowed_idx.iter().copied().collect();
        for id in &found {
            let i: usize = id[1..].parse().unwrap();
            assert!(
                allowed_set.contains(&i),
                "{label}: {id} violates the filter"
            );
        }
        if exact {
            let f: HashSet<&String> = found.iter().collect();
            let t: HashSet<&String> = truth.iter().collect();
            assert_eq!(f, t, "{label}: small allowed set must be searched exactly");
        }
        total += recall(&found, &truth);
    }
    let r = total / queries.len() as f64;
    assert!(r >= 0.95, "{label}: recall@10 {r:.4} < 0.95");
}

fn entity_set(entities: &[usize], n: usize) -> (HashSet<String>, Vec<usize>) {
    let idx: Vec<usize> = (0..n)
        .filter(|i| entities.contains(&(i % ENTITIES)))
        .collect();
    (idx.iter().map(|i| format!("c{i}")).collect(), idx)
}

fn run_matrix(n: usize, tuning: AnnTuningConfig, small_cut: usize) {
    let (vectors, queries) = dataset(n, 41);
    let store = build(&vectors, tuning);
    let ctx = Ctx {
        store: &store,
        vectors: &vectors,
        queries: &queries,
    };
    let lo = BASE_TS + 100;

    // Time range only: a window below the threshold and one above it.
    let small_hi = BASE_TS + small_cut as i64 - 1;
    let small: Vec<usize> = (100..small_cut).collect();
    check(
        "time small",
        &ctx,
        (Some(lo), Some(small_hi)),
        None,
        &small,
        true,
    );
    let large: Vec<usize> = (0..n).filter(|i| *i >= 50).collect();
    check(
        "time large",
        &ctx,
        (Some(BASE_TS + 50), None),
        None,
        &large,
        false,
    );

    // Entity prefilter (an allowed claim-id set from the entity index).
    let (one_entity, one_idx) = entity_set(&[3], n);
    assert_eq!(store.claim_ids_for_entity(TENANT, "entity-3"), one_entity);
    check(
        "entity small",
        &ctx,
        (None, None),
        Some(&one_entity),
        &one_idx,
        true,
    );
    let (many_entities, many_idx) = entity_set(&[0, 1, 2, 3, 4, 5], n);
    check(
        "entity large",
        &ctx,
        (None, None),
        Some(&many_entities),
        &many_idx,
        false,
    );

    // Entity AND time window.
    let both: Vec<usize> = one_idx
        .iter()
        .copied()
        .filter(|i| (100..small_cut).contains(i))
        .collect();
    check(
        "entity+time",
        &ctx,
        (Some(lo), Some(small_hi)),
        Some(&one_entity),
        &both,
        true,
    );
    let both_large: Vec<usize> = many_idx.iter().copied().filter(|i| *i >= 50).collect();
    check(
        "entity+time large",
        &ctx,
        (Some(BASE_TS + 50), None),
        Some(&many_entities),
        &both_large,
        false,
    );

    // A filter that excludes everything yields nothing, never a fallback.
    let nothing = store.ann_vector_top_candidates_filtered(
        TENANT,
        &queries[0],
        10,
        (Some(BASE_TS + 10 * n as i64), None),
        None,
    );
    assert!(nothing.is_empty());
}

#[test]
fn filtered_search_matches_exact_with_default_threshold() {
    // 12k vectors: above the 8192 flat threshold, so the tenant is HNSW.
    // Allowed sets of 900 (below) and ~11,950 / 10,286 (above) are covered.
    run_matrix(12_000, AnnTuningConfig::default(), 1_000);
}

#[test]
fn filtered_search_matches_exact_with_low_threshold() {
    let tuning = AnnTuningConfig {
        flat_threshold: 200,
        ..AnnTuningConfig::default()
    };
    // 3k vectors, threshold 200: allowed sets of 150 (below) and ~430..2,950 (above).
    run_matrix(3_000, tuning, 250);
}

#[test]
fn filtered_search_on_a_flat_tenant_is_exact() {
    let n = 2_000;
    let (vectors, queries) = dataset(n, 43);
    let store = build(&vectors, AnnTuningConfig::default());
    let ctx = Ctx {
        store: &store,
        vectors: &vectors,
        queries: &queries,
    };
    let (ids, idx) = entity_set(&[1, 2], n);
    check("flat entity", &ctx, (None, None), Some(&ids), &idx, true);
    let window: Vec<usize> = (500..1_500).collect();
    check(
        "flat time",
        &ctx,
        (Some(BASE_TS + 500), Some(BASE_TS + 1_499)),
        None,
        &window,
        true,
    );
}

#[test]
fn retrieval_with_filters_only_returns_allowed_claims_and_finds_far_neighbours() {
    // The global nearest neighbours of the query all lie OUTSIDE the time
    // window; the old behaviour (global top-N, then filter) returned nothing
    // useful, the filtered search must still surface the window's best.
    let n = 3_000;
    let tuning = AnnTuningConfig {
        flat_threshold: 200,
        ..AnnTuningConfig::default()
    };
    let (vectors, _) = dataset(n, 47);
    let store = build(&vectors, tuning);
    let query = vectors[2_900].clone();
    let window = (Some(BASE_TS), Some(BASE_TS + 599));
    let req = RetrievalRequest {
        tenant_id: TENANT.to_string(),
        query: String::new(),
        top_k: 5,
        stance_mode: StanceMode::Balanced,
    };
    let results =
        store.retrieve_with_time_range_and_query_vector(&req, window.0, window.1, Some(&query));
    assert!(!results.is_empty());
    for r in &results {
        let i: usize = r.claim_id[1..].parse().unwrap();
        assert!(i < 600, "{} is outside the time window", r.claim_id);
    }
    let truth = exact_over(&vectors, 0..600, &query, 5);
    let top: Vec<String> = results.iter().take(5).map(|r| r.claim_id.clone()).collect();
    let found: HashSet<&String> = top.iter().collect();
    let hits = truth.iter().filter(|id| found.contains(id)).count();
    assert!(hits >= 4, "top-5 {top:?} vs exact {truth:?}");
}
