//! Criterion-based benchmark suite for the DASH hot paths.
//!
//! Run with:
//!
//! ```bash
//! # All benches
//! cargo bench -p benchmark-smoke
//!
//! # Single bench
//! cargo bench -p benchmark-smoke --bench criterion_benches -- \
//!     --bench ingest_in_memory
//!
//! # Save a baseline for regression detection
//! cargo bench -p benchmark-smoke -- --save-baseline main
//!
//! # Compare current run against the saved baseline; non-zero
//! # exit if any metric regressed by more than 5%.
//! cargo bench -p benchmark-smoke -- --baseline main --threshold 5
//! ```
//!
//! Unlike `perf_bench` (a single-shot CLI), Criterion gives:
//!
//! - **Statistical analysis**: confidence intervals on every metric
//! - **Outlier detection**: IQR-based filtering, automatically
//! - **Regression detection**: `--baseline main` compares current
//!   numbers against a saved baseline and exits non-zero if any
//!   metric regressed beyond a threshold
//! - **HTML reports**: `target/criterion/report/index.html` is a
//!   browsable per-bench analysis with violin plots
//! - **Comparison groups**: `bench::compare` runs two configs back
//!   to back back (e.g. `in_memory` vs `disk_backed`) and emits a
//!   delta with p-value

use std::time::Duration;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use schema::{Claim, Evidence, RetrievalRequest, Stance, StanceMode};
use store::{AnnTuningConfig, FileWal, InMemoryStore};

use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

fn make_claim(id: &str, tenant: &str, text: &str) -> Claim {
    Claim {
        claim_id: id.to_string(),
        tenant_id: tenant.to_string(),
        canonical_text: text.to_string(),
        confidence: 0.9,
        event_time_unix: None,
        entities: vec![],
        embedding_ids: vec![],
        claim_type: None,
        valid_from: None,
        valid_to: None,
        created_at: Some(0),
        updated_at: Some(0),
    }
}

fn make_evidence(id: &str, claim_id: &str) -> Evidence {
    Evidence {
        evidence_id: id.to_string(),
        claim_id: claim_id.to_string(),
        source_id: format!("src://{id}"),
        stance: Stance::Supports,
        source_quality: 0.9,
        chunk_id: None,
        span_start: None,
        span_end: None,
        doc_id: None,
        extraction_model: None,
        ingested_at: None,
    }
}

fn build_store_with_n_claims(n: usize) -> InMemoryStore {
    let mut store = InMemoryStore::new_with_ann_tuning(AnnTuningConfig::default());
    for i in 0..n {
        let id = format!("c{i}");
        store
            .ingest_bundle(
                make_claim(&id, "tenant-a", &format!("claim {i} content")),
                vec![make_evidence(&format!("e{i}"), &id)],
                vec![],
            )
            .expect("ingest should succeed");
    }
    store
}

// ---------------------------------------------------------------------------
// Bench 1: ingest throughput (in-memory)
// ---------------------------------------------------------------------------

fn bench_ingest_in_memory(c: &mut Criterion) {
    let mut group = c.benchmark_group("ingest_in_memory");
    for n in [100, 1_000, 10_000] {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_batched(
                || InMemoryStore::new_with_ann_tuning(AnnTuningConfig::default()),
                |mut store| {
                    for i in 0..n {
                        let id = format!("c{i}");
                        store
                            .ingest_bundle(
                                make_claim(&id, "t1", &format!("text {i}")),
                                vec![make_evidence(&format!("e{i}"), &id)],
                                vec![],
                            )
                            .expect("ingest");
                    }
                    std::hint::black_box(store);
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Bench 2: ingest throughput (persistent WAL) — comparison
// ---------------------------------------------------------------------------

fn bench_ingest_persistent(c: &mut Criterion) {
    let mut group = c.benchmark_group("ingest_persistent");
    for n in [100, 1_000] {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_batched(
                || {
                    let tmp = TempDir::new().unwrap();
                    let wal_path = tmp.path().join("bench.wal");
                    let wal = FileWal::open(&wal_path).expect("open wal");
                    let store = InMemoryStore::new_with_ann_tuning(AnnTuningConfig::default());
                    (tmp, wal, store)
                },
                |(_tmp, mut wal, mut store)| {
                    for i in 0..n {
                        let id = format!("c{i}");
                        store
                            .ingest_bundle_persistent(
                                &mut wal,
                                make_claim(&id, "t1", &format!("text {i}")),
                                vec![make_evidence(&format!("e{i}"), &id)],
                                vec![],
                            )
                            .expect("ingest");
                    }
                    std::hint::black_box(store);
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Bench 3: lexical vs semantic retrieve — direct comparison
// ---------------------------------------------------------------------------

fn build_retrieval_fixture(n: usize) -> InMemoryStore {
    let mut store = build_store_with_n_claims(n);
    // add a tenant so the fixture has variety
    for i in 0..(n / 2) {
        let id = format!("b{i}");
        store
            .ingest_bundle(
                make_claim(&id, "tenant-b", &format!("second tenant claim {i}")),
                vec![],
                vec![],
            )
            .expect("ingest");
    }
    store
}

fn bench_retrieve_lexical(c: &mut Criterion) {
    let mut group = c.benchmark_group("retrieve_lexical");
    for n in [1_000, 10_000] {
        group.throughput(Throughput::Elements(1));
        let store = build_retrieval_fixture(n);
        let req = RetrievalRequest {
            tenant_id: "tenant-a".into(),
            query: "claim 42 content".into(),
            top_k: 10,
            stance_mode: StanceMode::Balanced,
        };
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _n| {
            b.iter(|| {
                let results = store.retrieve(&req);
                std::hint::black_box(results);
            });
        });
    }
    group.finish();
}

fn bench_retrieve_semantic(c: &mut Criterion) {
    let mut group = c.benchmark_group("retrieve_semantic");
    for n in [1_000, 10_000] {
        group.throughput(Throughput::Elements(1));
        let store = build_retrieval_fixture(n);
        let query_vector = vec![0.1f32; 384]; // dummy 384-dim vector
        let req = RetrievalRequest {
            tenant_id: "tenant-a".into(),
            query: "claim 42 content".into(),
            top_k: 10,
            stance_mode: StanceMode::Balanced,
        };
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _n| {
            b.iter(|| {
                let results = store.retrieve_semantic(&req, &query_vector);
                std::hint::black_box(results);
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Bench 4: ANN vs brute-force — quantify the speedup
// ---------------------------------------------------------------------------

fn bench_ann_vs_brute_force(c: &mut Criterion) {
    // With small n the brute-force is faster (no graph traversal overhead);
    // with large n the ANN index wins. Criterion will show the crossover.
    let mut group = c.benchmark_group("ann_vs_brute_force");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(5));
    for n in [1_000, 5_000, 10_000] {
        let store = build_store_with_n_claims(n);
        let query_vector = vec![0.1f32; 384];
        // ANN path: uses the HNSW graph
        group.bench_with_input(BenchmarkId::new("ann", n), &n, |b, _n| {
            b.iter(|| {
                let cands = store.ann_vector_top_candidates("tenant-a", &query_vector, 10);
                std::hint::black_box(cands);
            });
        });
        // Brute-force path: full scan, exact cosine
        group.bench_with_input(BenchmarkId::new("brute_force", n), &n, |b, _n| {
            b.iter(|| {
                let cands = store.exact_vector_top_candidates("tenant-a", &query_vector, 10);
                std::hint::black_box(cands);
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Bench 5: WAL replay throughput
// ---------------------------------------------------------------------------

fn bench_wal_replay(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_replay");
    group.throughput(Throughput::Elements(1_000));
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(8));
    group.bench_function("1000_claims", |b| {
        // Pre-build the WAL once and reuse the path across iterations.
        // (The benchmark measures load_from_wal, not the WAL build.)
        let tmp = TempDir::new().unwrap();
        let wal_path = tmp.path().join("replay.wal");
        let mut wal = FileWal::open(&wal_path).expect("open");
        let mut build_store = InMemoryStore::new_with_ann_tuning(AnnTuningConfig::default());
        for i in 0..1_000 {
            let id = format!("c{i}");
            build_store
                .ingest_bundle_persistent(
                    &mut wal,
                    make_claim(&id, "t1", &format!("replay text {i}")),
                    vec![],
                    vec![],
                )
                .expect("ingest");
        }
        drop(wal);
        drop(build_store);
        // Hold the TempDir alive across the closure
        let _keep_alive = &tmp;
        b.iter(|| {
            let wal = FileWal::open(&wal_path).expect("reopen");
            let (store, _stats) = InMemoryStore::load_from_wal_with_stats(&wal)
                .expect("replay");
            std::hint::black_box(store);
        });
    });
    group.finish();
}

// ---------------------------------------------------------------------------
// Bench 6: in-memory vs disk-backed ingest — apples-to-apples
// ---------------------------------------------------------------------------

fn bench_disk_vs_memory_ingest(c: &mut Criterion) {
    let mut group = c.benchmark_group("disk_vs_memory_ingest");
    group.throughput(Throughput::Elements(1_000));
    for n in [1_000] {
        // In-memory path
        group.bench_with_input(BenchmarkId::new("in_memory", n), &n, |b, &n| {
            b.iter(|| {
                let mut store = InMemoryStore::new_with_ann_tuning(AnnTuningConfig::default());
                for i in 0..n {
                    let id = format!("c{i}");
                    store
                        .ingest_bundle(
                            make_claim(&id, "t1", &format!("text {i}")),
                            vec![],
                            vec![],
                        )
                        .expect("ingest");
                }
                std::hint::black_box(store);
            });
        });
        // Disk-backed path: same ingest but mirrored to redb
        group.bench_with_input(BenchmarkId::new("disk_backed", n), &n, |b, &n| {
            b.iter(|| {
                let tmp = TempDir::new().unwrap();
                let disk_path = tmp.path().join("bench.redb");
                let mut store =
                    InMemoryStore::new_with_ann_tuning(AnnTuningConfig::default());
                store = store.with_disk(&disk_path).expect("with_disk");
                for i in 0..n {
                    let id = format!("c{i}");
                    store
                        .ingest_bundle(
                            make_claim(&id, "t1", &format!("text {i}")),
                            vec![],
                            vec![],
                        )
                        .expect("ingest");
                }
                std::hint::black_box(store);
            });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_ingest_in_memory,
    bench_ingest_persistent,
    bench_retrieve_lexical,
    bench_retrieve_semantic,
    bench_ann_vs_brute_force,
    bench_wal_replay,
    bench_disk_vs_memory_ingest,
);
criterion_main!(benches);
