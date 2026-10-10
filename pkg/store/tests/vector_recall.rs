//! Vector candidate quality through the public store API (IDX-01, IDX-02).
//!
//! Data is a seeded Gaussian mixture (32 clusters, 64-d, unit-normalised):
//! the shape on which the previous in-repo graph was effectively
//! disconnected beyond a few thousand vectors (measured recall@10 of 0.15
//! from 5k vectors up, see `docs/adr/0003-storage-engine-indexes.md`).
//! Ground truth is an exact brute-force scan computed in this file, not by
//! the store, so a bug shared by the store's exact path cannot hide.
//!
//! Every test is deterministic (fixed seeds, no clocks, no sleeps) and
//! order-independent (each builds its own store).

use schema::Claim;
use std::collections::HashSet;
use store::{AnnTuningConfig, FileWal, InMemoryStore};
use tempfile::TempDir;

const DIM: usize = 64;
const CLUSTERS: usize = 32;
const TENANT: &str = "t0";
const BASE_TS: i64 = 1_700_000_000;

// ---------------------------------------------------------------------------
// Deterministic data generation (splitmix64 + Box-Muller; no external RNG so
// the stream can never change under a dependency upgrade).
// ---------------------------------------------------------------------------

struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn unit(&mut self) -> f64 {
        // (0, 1]
        ((self.next_u64() >> 11) as f64 + 1.0) / (1u64 << 53) as f64
    }

    fn gauss(&mut self) -> f64 {
        let (u1, u2) = (self.unit(), self.unit());
        (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos()
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next_u64() % n as u64) as usize
    }
}

fn normalize(v: &mut [f32]) {
    let norm = v
        .iter()
        .map(|x| f64::from(*x) * f64::from(*x))
        .sum::<f64>()
        .sqrt();
    for x in v.iter_mut() {
        *x = (f64::from(*x) / norm) as f32;
    }
}

struct Mixture {
    centers: Vec<Vec<f32>>,
}

impl Mixture {
    fn new(seed: u64) -> Self {
        let mut rng = Rng::new(seed);
        let centers = (0..CLUSTERS)
            .map(|_| {
                let mut c: Vec<f32> = (0..DIM).map(|_| rng.gauss() as f32).collect();
                normalize(&mut c);
                c
            })
            .collect();
        Self { centers }
    }

    /// One point of the mixture: a cluster centre plus isotropic noise
    /// (|noise| ~ 0.8, so clusters are well separated but not trivially so).
    fn sample(&self, rng: &mut Rng) -> Vec<f32> {
        let center = &self.centers[rng.below(CLUSTERS)];
        let mut v: Vec<f32> = center
            .iter()
            .map(|c| *c + (0.1 * rng.gauss()) as f32)
            .collect();
        normalize(&mut v);
        v
    }
}

fn claim(tenant: &str, id: &str, i: usize) -> Claim {
    Claim {
        claim_id: id.to_string(),
        tenant_id: tenant.to_string(),
        canonical_text: format!("claim number {i}"),
        confidence: 0.5,
        event_time_unix: Some(BASE_TS + i as i64),
        entities: vec![format!("entity-{}", i % 7)],
        embedding_ids: vec![],
        claim_type: None,
        valid_from: None,
        valid_to: None,
        created_at: None,
        updated_at: None,
    }
}

/// Default tuning (flat below 8192 vectors) and a low flat threshold that
/// puts even small test tenants on the HNSW path.
fn tunings() -> [(&'static str, AnnTuningConfig); 2] {
    [
        ("default", AnnTuningConfig::default()),
        (
            "hnsw",
            AnnTuningConfig {
                flat_threshold: 128,
                ..AnnTuningConfig::default()
            },
        ),
    ]
}

fn id_of(i: usize) -> String {
    format!("c{i}")
}

fn build_store(vectors: &[Vec<f32>], tuning: AnnTuningConfig) -> InMemoryStore {
    let mut store = InMemoryStore::new_with_ann_tuning(tuning);
    for (i, v) in vectors.iter().enumerate() {
        let id = id_of(i);
        store
            .ingest_bundle(claim(TENANT, &id, i), vec![], vec![])
            .unwrap();
        store.upsert_claim_vector(&id, v.clone()).unwrap();
    }
    store
}

fn dataset(n: usize, n_queries: usize, seed: u64) -> (Vec<Vec<f32>>, Vec<Vec<f32>>) {
    let mixture = Mixture::new(seed);
    let mut rng = Rng::new(seed ^ 0xD1B5_4A32_D192_ED03);
    let base = (0..n).map(|_| mixture.sample(&mut rng)).collect();
    // Held-out queries come from the same mixture but a disjoint stream.
    let mut qrng = Rng::new(seed ^ 0x5851_F42D_4C95_7F2D);
    let queries = (0..n_queries).map(|_| mixture.sample(&mut qrng)).collect();
    (base, queries)
}

fn dot_f64(a: &[f32], b: &[f32]) -> f64 {
    a.iter()
        .zip(b)
        .map(|(x, y)| f64::from(*x) * f64::from(*y))
        .sum()
}

/// Exact top-`k` ids by cosine over `ids` (all inputs are unit vectors, so
/// cosine equals the dot product).
fn brute_force(
    vectors: &[Vec<f32>],
    ids: impl Iterator<Item = usize>,
    query: &[f32],
    k: usize,
) -> Vec<String> {
    let mut scored: Vec<(f64, usize)> = ids.map(|i| (dot_f64(query, &vectors[i]), i)).collect();
    scored.sort_by(|a, b| b.0.total_cmp(&a.0).then(a.1.cmp(&b.1)));
    scored.into_iter().take(k).map(|(_, i)| id_of(i)).collect()
}

fn recall(found: &[String], truth: &[String]) -> f64 {
    let truth: HashSet<&String> = truth.iter().collect();
    let hit = found.iter().filter(|id| truth.contains(id)).count();
    hit as f64 / truth.len() as f64
}

fn mean_recall_at_10(store: &InMemoryStore, vectors: &[Vec<f32>], queries: &[Vec<f32>]) -> f64 {
    let mut total = 0.0;
    for q in queries {
        let truth = brute_force(vectors, 0..vectors.len(), q, 10);
        let found = store.ann_vector_top_candidates(TENANT, q, 10);
        total += recall(&found, &truth);
    }
    total / queries.len() as f64
}

// ---------------------------------------------------------------------------
// Recall on clustered data
// ---------------------------------------------------------------------------

#[test]
fn recall_at_10_on_clustered_data_2k() {
    let (vectors, queries) = dataset(2_000, 100, 7);
    let store = build_store(&vectors, AnnTuningConfig::default());
    let r = mean_recall_at_10(&store, &vectors, &queries);
    eprintln!("recall@10 n=2000 = {r:.4}");
    assert!(r >= 0.95, "recall@10 {r:.4} < 0.95 at n=2000");
}

#[test]
fn recall_at_10_on_clustered_data_5k() {
    let (vectors, queries) = dataset(5_000, 100, 11);
    let store = build_store(&vectors, AnnTuningConfig::default());
    let r = mean_recall_at_10(&store, &vectors, &queries);
    eprintln!("recall@10 n=5000 = {r:.4}");
    assert!(r >= 0.95, "recall@10 {r:.4} < 0.95 at n=5000");
}

#[test]
fn recall_at_10_on_clustered_data_20k() {
    let (vectors, queries) = dataset(20_000, 100, 13);
    let started = std::time::Instant::now();
    let store = build_store(&vectors, AnnTuningConfig::default());
    eprintln!("build n=20000 took {:?}", started.elapsed());
    let r = mean_recall_at_10(&store, &vectors, &queries);
    eprintln!("recall@10 n=20000 = {r:.4}");
    assert!(r >= 0.95, "recall@10 {r:.4} < 0.95 at n=20000");
}

#[test]
fn ann_top_candidates_are_sorted_by_similarity_and_sized() {
    let (vectors, queries) = dataset(3_000, 5, 17);
    for (name, tuning) in tunings() {
        let store = build_store(&vectors, tuning);
        for q in &queries {
            let found = store.ann_vector_top_candidates(TENANT, q, 25);
            assert_eq!(found.len(), 25, "{name}");
            let unique: HashSet<&String> = found.iter().collect();
            assert_eq!(unique.len(), found.len(), "{name}: no duplicate candidates");
            let scores: Vec<f64> = found
                .iter()
                .map(|id| {
                    let i: usize = id[1..].parse().unwrap();
                    dot_f64(q, &vectors[i])
                })
                .collect();
            assert!(
                scores.windows(2).all(|w| w[0] >= w[1] - 1e-6),
                "{name}: candidates must come back best first: {scores:?}"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Tenant isolation, dimension mismatch
// ---------------------------------------------------------------------------

#[test]
fn tenant_isolation_with_identical_vectors() {
    let (vectors, queries) = dataset(1_500, 20, 19);
    for (name, tuning) in tunings() {
        let mut store = InMemoryStore::new_with_ann_tuning(tuning);
        for (i, v) in vectors.iter().enumerate() {
            for tenant in ["tenant-a", "tenant-b"] {
                let id = format!("{tenant}/{i}");
                store
                    .ingest_bundle(claim(tenant, &id, i), vec![], vec![])
                    .unwrap();
                store.upsert_claim_vector(&id, v.clone()).unwrap();
            }
        }
        for q in &queries {
            for tenant in ["tenant-a", "tenant-b"] {
                let prefix = format!("{tenant}/");
                let ann = store.ann_vector_top_candidates(tenant, q, 50);
                assert_eq!(ann.len(), 50, "{name}");
                assert!(
                    ann.iter().all(|id| id.starts_with(&prefix)),
                    "{name}: {tenant} query leaked another tenant: {ann:?}"
                );
                // Filtered search stays inside the tenant even when the
                // allowed set names the other tenant's claims.
                let other = if tenant == "tenant-a" {
                    "tenant-b"
                } else {
                    "tenant-a"
                };
                let allowed: HashSet<String> = (0..1_500).map(|i| format!("{other}/{i}")).collect();
                let none = store.ann_vector_top_candidates_filtered(
                    tenant,
                    q,
                    10,
                    (None, None),
                    Some(&allowed),
                );
                assert!(none.is_empty(), "{name}: cross-tenant allowed set leaked");
                let exact = store.exact_vector_top_candidates(tenant, q, 50);
                assert!(exact.iter().all(|id| id.starts_with(&prefix)));
            }
        }
        assert!(
            store
                .ann_vector_top_candidates("tenant-missing", &queries[0], 10)
                .is_empty()
        );
    }
}

#[test]
fn wrong_dimension_query_returns_nothing_and_wrong_dimension_upsert_errors() {
    let (vectors, _) = dataset(400, 1, 23);
    for (name, tuning) in tunings() {
        let mut store = build_store(&vectors, tuning);
        let short = vec![0.5f32; DIM - 1];
        assert!(
            store
                .ann_vector_top_candidates(TENANT, &short, 10)
                .is_empty(),
            "{name}"
        );
        assert!(
            store
                .exact_vector_top_candidates(TENANT, &short, 10)
                .is_empty()
        );
        assert!(store.validate_query_vector(TENANT, &short).is_err());
        store
            .ingest_bundle(claim(TENANT, "extra", 999), vec![], vec![])
            .unwrap();
        assert!(store.upsert_claim_vector("extra", short).is_err());
        // A rejected upsert leaves the index untouched.
        assert_eq!(
            store
                .ann_vector_top_candidates(TENANT, &vectors[0], 5)
                .len(),
            5,
            "{name}"
        );
    }
}

// ---------------------------------------------------------------------------
// Re-upsert / replace consistency
// ---------------------------------------------------------------------------

#[test]
fn claim_reupsert_keeps_vector_and_vector_replace_updates_results() {
    let (vectors, _) = dataset(1_000, 1, 29);
    for (name, tuning) in tunings() {
        let mut store = build_store(&vectors, tuning);

        // Re-upserting the claim (new text) must not drop its vector (DATA-04).
        let mut changed = claim(TENANT, &id_of(5), 5);
        changed.canonical_text = "completely different text".to_string();
        store.ingest_bundle(changed, vec![], vec![]).unwrap();
        let top = store.ann_vector_top_candidates(TENANT, &vectors[5], 1);
        assert_eq!(
            top,
            vec![id_of(5)],
            "{name}: claim re-upsert must keep its vector"
        );

        // Replace c5's vector with a copy of c900's: c5 must now answer to
        // the new vector and no longer to the old one.
        let new_vector = vectors[900].clone();
        store
            .upsert_claim_vector(&id_of(5), new_vector.clone())
            .unwrap();
        let near_new = store.ann_vector_top_candidates(TENANT, &new_vector, 2);
        assert!(
            near_new.contains(&id_of(5)) && near_new.contains(&id_of(900)),
            "{name}: {near_new:?}"
        );
        let near_old = store.ann_vector_top_candidates(TENANT, &vectors[5], 10);
        assert!(
            !near_old.contains(&id_of(5)),
            "{name}: the replaced vector must not be reachable any more"
        );
    }
}

#[test]
fn many_replacements_keep_results_equal_to_exact_search() {
    let (vectors, queries) = dataset(1_200, 20, 31);
    for (name, tuning) in tunings() {
        let mut store = build_store(&vectors, tuning);
        let mut current = vectors.clone();
        let mut rng = Rng::new(99);
        // Replace 400 vectors (some more than once) with other points' vectors.
        for _ in 0..400 {
            let target = rng.below(current.len());
            let source = rng.below(vectors.len());
            current[target] = vectors[source].clone();
            store
                .upsert_claim_vector(&id_of(target), vectors[source].clone())
                .unwrap();
        }
        let mut total = 0.0;
        for q in &queries {
            let truth = brute_force(&current, 0..current.len(), q, 10);
            let found = store.ann_vector_top_candidates(TENANT, q, 10);
            total += recall(&found, &truth);
        }
        let r = total / queries.len() as f64;
        assert!(r >= 0.95, "{name}: recall after churn {r:.4} < 0.95");
    }
}

// ---------------------------------------------------------------------------
// Restart / replay
// ---------------------------------------------------------------------------

#[test]
fn wal_replay_returns_the_same_top_k() {
    let (vectors, queries) = dataset(2_500, 15, 37);
    for (name, tuning) in tunings() {
        let tmp = TempDir::new().unwrap();
        let wal_path = tmp.path().join("vectors.wal");
        let mut wal = FileWal::open(&wal_path).unwrap();
        let mut store = InMemoryStore::new_with_ann_tuning(tuning.clone());
        let mut current = vectors.clone();
        for (i, v) in vectors.iter().enumerate() {
            let id = id_of(i);
            store
                .ingest_bundle_persistent(&mut wal, claim(TENANT, &id, i), vec![], vec![])
                .unwrap();
            store
                .upsert_claim_vector_persistent(&mut wal, &id, v.clone())
                .unwrap();
        }
        // Replacements are part of the log too: replay must apply them.
        let mut rng = Rng::new(5);
        for _ in 0..150 {
            let target = rng.below(current.len());
            let source = rng.below(vectors.len());
            current[target] = vectors[source].clone();
            store
                .upsert_claim_vector_persistent(&mut wal, &id_of(target), vectors[source].clone())
                .unwrap();
        }
        let before: Vec<Vec<String>> = queries
            .iter()
            .map(|q| store.ann_vector_top_candidates(TENANT, q, 10))
            .collect();
        drop(wal);
        drop(store);

        let wal = FileWal::open(&wal_path).unwrap();
        let (replayed, stats) =
            InMemoryStore::load_from_wal_with_stats_and_ann_tuning(&wal, tuning).unwrap();
        assert_eq!(stats.claims_loaded, vectors.len(), "{name}");
        let mut total = 0.0;
        for (q, expected) in queries.iter().zip(&before) {
            let after = replayed.ann_vector_top_candidates(TENANT, q, 10);
            if name == "default" {
                // Exact (flat) tenants must answer identically after replay.
                assert_eq!(&after, expected, "{name}: replayed store differs");
            }
            total += recall(&after, &brute_force(&current, 0..current.len(), q, 10));
        }
        let r = total / queries.len() as f64;
        assert!(r >= 0.95, "{name}: recall after replay {r:.4}");
    }
}
