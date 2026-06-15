# Benchmarking & Profiling Guide

The DASH workspace ships with three independent benchmark layers,
each catching a different class of regression.

## At a glance

| Layer | Tool | What it catches | When to run |
|---|---|---|---|
| `perf_bench` (CLI) | hand-rolled | single-shot smoke check | ad-hoc, pre-commit |
| `cargo bench --bench criterion_benches` | Criterion | statistical regression, comparison groups | nightly, on every PR |
| `cargo run --bin load_test` | custom | network-attached qps, HTTP framing, serde | pre-release, perf-investigation |
| `cargo run --bin mem_profile` | dhat-rs | heap allocations, leak detection | pre-release, new feature |

Plus three production-readiness scripts:

| Script | What it does |
|---|---|
| `scripts/build-pgo.sh` | Builds the binaries with Profile-Guided Optimization (8-15% throughput win) |
| `scripts/check-bench-regression.sh` | Runs Criterion against a saved baseline; fails CI if any metric regresses by more than 5% |
| `scripts/cargo-audit.sh` | Runs `cargo audit` and denies warnings |

## `perf_bench` — single-shot CLI

The legacy CLI for ad-hoc smoke checks. Hand-rolled percentiles,
no statistical analysis, but no warmup cost.

```bash
cargo run --release -p benchmark-smoke --bin perf_bench -- \
    --scenario retrieve_throughput_semantic \
    --iterations 1000
```

Outputs a `BENCH_JSON:` line on stdout (one JSON object per scenario)
for CI ingestion.

## `cargo bench --bench criterion_benches` — statistical suite

The gold-standard Rust benchmarking tool. Provides confidence
intervals on every metric, IQR-based outlier filtering, and
regression detection against a saved baseline.

### What's measured

Six bench groups, 12 parameterized scenarios:

| Group | What | Sizes |
|---|---|---|
| `ingest_in_memory` | append-only ingest into a fresh `InMemoryStore` | 100, 1k, 10k claims |
| `ingest_persistent` | ingest mirrored to a `redb` disk store | 100, 1k claims |
| `retrieve_lexical` | BM25 + cosine over an inverted index | 1k, 10k claims |
| `retrieve_semantic` | dense similarity over the ANN graph | 1k, 10k claims |
| `ann_vs_brute_force` | side-by-side: ANN graph vs full scan | 1k, 5k, 10k vectors |
| `disk_vs_memory_ingest` | side-by-side: in-memory vs disk-backed | 1k claims |
| `wal_replay` | cold-start replay of a 1k-claim WAL | 1 scenario |

### Running

```bash
# All benches (slow: ~15 minutes for the full suite)
cargo bench -p benchmark-smoke

# A single group
cargo bench --bench criterion_benches -- "ingest_in_memory"

# A single parameterized scenario
cargo bench --bench criterion_benches -- "ingest_in_memory/1000"
```

### Saving a baseline

The first run on a branch should save a baseline:

```bash
cargo bench -p benchmark-smoke -- --save-baseline main
```

This writes per-bench measurements under
`target/criterion/criterion_benches/main/`.

### Regression detection

```bash
# Compare current run against the saved baseline
cargo bench -p benchmark-smoke -- --baseline main --threshold 5
```

`--threshold 5` means "fail if any metric regressed by more than 5%".
Criterion's algorithm accounts for noise (uses the Welch t-test on
the confidence intervals) so the comparison is robust to natural
benchmark variance.

### HTML reports

After any bench run, browse `target/criterion/report/index.html` —
Criterion generates a per-bench analysis with violin plots, slope
graphs, and the full IQR table.

## `load_test` — real HTTP path

Unlike the in-process benches, `load_test` spawns (or connects to) a
running DASH service and drives concurrent HTTP traffic at the real
network endpoints. This is the only bench that exercises the HTTP
framing, bearer-token middleware, JSON parser, and dispatcher in
series.

### Setup

```bash
# Terminal A: start the retrieval service
DASH_RETRIEVAL_PERSISTENCE_DISABLE=1 cargo run --release -p retrieval

# Terminal B: drive traffic
cargo run --release -p benchmark-smoke --bin load_test -- \
    --url http://127.0.0.1:8080 \
    --concurrency 32 \
    --duration 30 \
    --endpoint retrieve
```

The `--endpoint` flag selects between `retrieve`, `embeddings`, and
`health`. Use `--query` to vary the request body.

The output is a human-readable block followed by a `LOAD_JSON:`
line. The JSON shape is:

```json
[
  {
    "endpoint": "retrieve",
    "concurrency": 32,
    "duration_seconds": 30.0,
    "total_requests": 12345,
    "successful_requests": 12340,
    "failed_requests": 5,
    "qps": 411.5,
    "p50_ms": 24.1,
    "p95_ms": 38.2,
    "p99_ms": 52.7,
    "p999_ms": 78.4,
    "max_ms": 102.1
  }
]
```

## `mem_profile` — heap profiling with dhat

dhat is a heap profiler that records every allocation, the call
stack that produced it, and the live bytes attributable to each call
site. Unlike `heaptrack` or `valgrind --tool=massif`, dhat runs
in-process with minimal overhead (a few percent), so it can be used
on real workloads.

```bash
# Pick a scenario
cargo run --release -p benchmark-smoke --bin mem_profile -- ingest_10k
cargo run --release -p benchmark-smoke --bin mem_profile -- retrieve_10k
cargo run --release -p benchmark-smoke --bin mem_profile -- with_disk
```

The output is a `target/dhat/<scenario>.heap.json` file. Load it
into the dhat viewer (or convert to a flamegraph with
`dhat-to-flamegraph`) to see the top allocators.

## PGO — Profile-Guided Optimization

PGO gives a free 8-15% throughput improvement on hot paths because
the compiler can make better inlining and branch-prediction
decisions when it has observed the actual call graph from a
representative run.

```bash
./scripts/build-pgo.sh
```

The script:
1. Builds the binaries with `RUSTFLAGS="-Cprofile-generate=..."`
2. Runs `perf_bench` against the instrumented binaries to populate
   the `.profraw` files
3. Merges the `.profraw` files with `llvm-profdata merge` into a
   single `.profdata` file
4. Rebuilds with `RUSTFLAGS="-Cprofile-use=..."`

Output: `target-pgo/release/ingestion` and `target-pgo/release/retrieval`.

Compare against `target/release/ingestion` (non-PGO) to quantify
the win. Typical improvement: 8-15% on the ingest and retrieve
hot paths on Apple Silicon; 15-25% on x86_64 Linux with PGO+BOLT.

## CI regression check

The `scripts/check-bench-regression.sh` script wraps the
Criterion-based regression check for CI:

```bash
# First run on a new branch: save a baseline
./scripts/check-bench-regression.sh --save-baseline main

# Subsequent runs: compare against the baseline
./scripts/check-bench-regression.sh --baseline main --threshold 5
```

Exit codes:
- `0`: all metrics within threshold
- `1`: at least one metric regressed by more than the threshold
- `2`: usage error (no baseline, missing tool, etc.)

This is meant to be wired into `.github/workflows/rust.yml` on a
nightly schedule and on every PR that touches a hot-path file.

## Latest measurements (2026-06-15)

### `disk_vs_memory_ingest` — apples-to-apples ingest

| Variant | Throughput (Criterion mean) | Notes |
|---|---:|---|
| `in_memory` | **1,067,000 elements/s** | no persistence, no fsync |
| `disk_backed` | 125 elements/s | redb fsync per claim (default policy) |

Disk-backed ingest is **~8,500x slower** than in-memory. The
bottleneck is `redb::Database::commit()` which fsyncs the WAL on
every transaction. To trade durability for throughput, see
`WalWritePolicy::sync_every_records` (raise it from 1 to 100 for
~100x throughput at the cost of losing the last 99 records on a
crash).

### `ann_vs_brute_force` — when does the index help?

| n (vectors) | ANN (ns) | Brute-force (ns) | Verdict |
|---:|---:|---:|---|
| 1,000 | 6.83 | 6.83 | tie (the bench is too small to differentiate; both paths fit in L1) |
| 5,000 | 17.34 | 7.23 | **brute force is 2.4x faster** |
| 10,000 | 17.51 | 6.91 | **brute force is 2.5x faster** |

The HNSW graph in this build has a 4-level hierarchy with a
sparse top level, so the constant overhead per query is high
(allocations, the BinaryHeap push/pop, the cosine calls at each
hop). For n in the 1k-10k range, the scan cost is dominated by
L1/L2 cache hits and SIMD-friendly loops, so brute force wins.

**Implication for production**: the ANN index only beats brute
force when the corpus is large enough that scan cost exceeds
graph traversal cost. In practice, that crossover is around
50k-100k vectors. Below that, `exact_vector_top_candidates` is
the right call; above it, `ann_vector_top_candidates` wins.

### `ingest_in_memory`

| n (claims) | Throughput |
|---:|---:|
| 100 | 876 Kelem/s |
| 1,000 | 821 Kelem/s |
| 10,000 | 705 Kelem/s |

Throughput is roughly constant per-claim (the in-memory map
insertions don't degrade with size), so the total time scales
linearly.

## Adding a new bench

1. Add a `fn bench_<thing>(c: &mut Criterion)` to
   `tests/benchmarks/benches/criterion_benches.rs`.
2. Use `c.benchmark_group("<name>")` for grouping and
   `c.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| { ... })`
   for parameterized scenarios.
3. Add the new function to the `criterion_group!` macro at the
   bottom of the file.
4. Run `cargo bench --bench criterion_benches -- <group_name>` to
   verify.
5. Save a fresh baseline: `cargo bench -- --save-baseline main`.
6. Update this doc with the new measurement.

## When to update the perf doc

Update `docs/benchmarks/performance.md` (the original `perf_bench`
documentation) only when the per-bench contract changes (new
metric, new scenario, methodology change). For day-to-day
regressions, the Criterion reports under `target/criterion/`
are the source of truth.
