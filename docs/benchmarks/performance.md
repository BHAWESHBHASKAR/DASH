# DASH Performance Benchmark Suite

This document describes the `perf_bench` micro-benchmark binary that
ships in `tests/benchmarks/src/perf_bench.rs`. It measures the latency
and throughput of the six hot paths in the DASH retrieval pipeline:

| Scenario | Path measured | Fixture size | Default iterations |
|---|---|---:|---:|
| `ingest_throughput_sequential.in_memory` | `InMemoryStore::ingest_bundle` (no WAL) | empty → 110 | 100 |
| `ingest_throughput_sequential.persistent_wal` | `InMemoryStore::ingest_bundle_persistent` + `FileWal::append_*` + `sync_data` per record | empty → 110 | 100 |
| `retrieve_throughput_lexical` | `InMemoryStore::retrieve` (no query vector) | 10 000 claims | 1 000 |
| `retrieve_throughput_semantic` | `InMemoryStore::retrieve_semantic` (with 768-dim query vector) | 10 000 claims + vectors | 1 000 |
| `ann_search_throughput_at_scale` | `InMemoryStore::ann_vector_top_candidates` (top-10) | 10 000 vectors (384-dim) | 500 |
| `wal_replay_throughput` | `FileWal::open` + `InMemoryStore::load_from_wal_with_stats_and_ann_tuning` (1 000 claims) | 1 000 claims in WAL | 100 |

All scenarios are timed with `std::time::Instant` per iteration. The
distribution is summarized as p50 / p95 / p99 / min / max / mean
microseconds, plus an aggregate throughput in operations per second.

## Running the suite

```bash
# All scenarios with default iteration counts (release build recommended)
cargo run -p benchmark-smoke --bin perf_bench --release -- --all

# Single scenario, custom iteration count
cargo run -p benchmark-smoke --bin perf_bench --release -- --scenario retrieve_throughput_semantic --iterations 500

# Adjust warm-up
cargo run -p benchmark-smoke --bin perf_bench --release -- --all --warmup 25
```

The output is two-part:

1. **Human-readable per-scenario block** printed to stdout (also to
   stderr are per-scenario section headers and pre-load progress).
2. **A single `BENCH_JSON:` line** at the very end of the run, with
   one JSON object per scenario. Stable schema:

   ```json
   BENCH_JSON: [
     {
       "name": "ingest_throughput_sequential.in_memory",
       "iterations": 100,
       "p50_us": 1, "p95_us": 8, "p99_us": 14,
       "max_us": 15, "min_us": 1, "mean_us": 1.99,
       "throughput_ops_per_sec": 502512.56,
       "extras": { "wal_enabled": "false", "fixture_size_target": "10000", "claims_in_window": "110" }
     },
     ...
   ]
   ```

The `extras` map records per-scenario context (fixture size, vector
dimension, top-k, WAL sync policy, etc.) so the JSON line is
self-describing.

## Methodology

- **Build mode**: `--release`. Debug builds are not representative for
  numeric kernels (cosine, BM25) and inflate the ANN graph build by
  ~10×.
- **Warm-up**: 10 iterations per scenario by default, not included in
  the measurement. Warm-up evens out the OS page cache and stabilizes
  the in-memory maps (e.g. `HashMap` capacity growth).
- **Measurement**: per-scenario default iteration count, recorded as a
  `Vec<u64>` of per-iteration microsecond latencies. Default counts
  follow the spec for each path (1 000 retrieves, 500 ANN searches,
  100 replays); override with `--iterations N`.
- **Percentile calculation**: `latencies_us.sort_unstable()` once, then
  `idx = ((len-1) * quantile).round() as usize`, clamped to
  `len - 1`. Returns the value at that index.
- **Throughput**: `iterations / total_seconds`, where `total_seconds`
  is the sum of the per-iteration latencies. This is the inverse of
  `mean_us` expressed in seconds, so a 1 ms mean ≈ 1 000 ops/sec.
- **Single-process, single-tenant per scenario** to avoid cross-scenario
  interference. WAL and ANN graph state is built fresh inside each
  scenario. Where the scenario writes a WAL, the temp directory is
  cleaned up via `fs::remove_dir_all` before the scenario returns.

## First-run baseline numbers (2026-06-15)

Hardware: Apple M-series (release build, single-threaded, default features).

### ingest_throughput_sequential

| Variant | p50 (us) | p95 (us) | p99 (us) | Throughput (ops/sec) |
|---|---:|---:|---:|---:|
| in_memory | 2 | 5 | 18 | 297,974 |
| persistent_wal | 7,941 | 8,287 | 9,926 | (fsync-bound) |

The ~4,000x gap between in-memory and persistent WAL is dominated
by `fsync`: the default WAL policy flushes every record to disk
for crash durability. A relaxed `sync_every_records` (e.g. 100)
trades durability for ~100x throughput; see `WalWritePolicy`.

### retrieve_throughput_lexical (top_k=10, no query vector)

| p50 (us) | p95 (us) | p99 (us) | Throughput (ops/sec) |
|---:|---:|---:|---:|
| 161 | 184 | 267 | ~5,800 |

### retrieve_throughput_semantic (top_k=10, query vector, 768-dim, 10k fixture)

| p50 (ms) | p95 (ms) | p99 (ms) | Throughput (ops/sec) |
|---:|---:|---:|---:|
| 10.9 | 12.4 | 14.1 | 90 |

Bottleneck: BM25 + dense similarity scoring + graph traversal over
the ANN index for every candidate. Acceptable for interactive RAG
(latency budget: ~100ms p99).

### ann_search_throughput_at_scale (top_n=10, 384-dim, 10k fixture)

| p50 (us) | p95 (us) | p99 (us) | Throughput (ops/sec) |
|---:|---:|---:|---:|
| 645 | 848 | 945 | 1,479 |

### wal_replay_throughput

| p50 (ms) | p95 (ms) | p99 (ms) |
|---:|---:|---:|
| 2.1 | 2.8 | 4.2 |

## Security posture (cargo audit, 2026-06-15)

`cargo audit` reports **0 vulnerabilities** and 3 warnings:

- **bincode 1.3.3 (RUSTSEC-2025-0141)**: unmaintained. Transitive
  dep of redb. Mitigation: pin to 1.3.3 (last 1.x release) until
  redb migrates to bincode 2.x; track upstream.
- **js-sys 0.3.88**: yanked. Transitive dep of the gpu-backend
  feature's wgpu dependency tree. Not in the default build.
- **wasm-bindgen 0.2.111**: yanked. Same provenance as js-sys.

Run `./scripts/cargo-audit.sh` in CI to detect new findings.

## Comparison to the existing `tests/benchmarks/src/main.rs` scenarios

The existing binary (`tests/benchmarks/src/main.rs`) runs the DASH
end-to-end benchmark suite — `phase4_*`, `phase11_*`, plus the
quality probes. It is a *correctness + candidate-reduction* suite
(does DASH return the right top-1, and what is the candidate-set
shrinkage?). It is built around a single `seed_fixture(...)` followed
by `measure_eme_latency_ms(...)` which times the whole loop
end-to-end and reports a single mean.

`perf_bench` is complementary: it does not check correctness or
candidate reduction, it only measures the per-operation latency
distribution. The per-scenario extras make it easy to attribute a
regression to a specific code path. Concrete differences:

| Property | `main.rs` | `perf_bench` |
|---|---|---|
| Granularity | Single mean over an entire loop | Per-iteration p50/p95/p99/min/max/mean |
| Output | Human-readable scorecard + CSV/JSONL history | Per-scenario block + single `BENCH_JSON:` line |
| WAL replay slice | One-shot at the end of `xlarge` / `xxlarge` profiles | Standalone scenario with 100 replays for distribution |
| ANN scale | 100 000 claims with stride 4 (= 25 000 vectors) | 10 000 claims with all 10 000 vectors |
| Scope | One fixture per profile (`smoke`/`standard`/`large`/`xlarge`/`xxlarge`/`hybrid`) | One fixture per scenario, sized for the path being measured |
| Quality gates | `evaluate_profile_gates`, `quality.all_passed` | None — pure performance |

In short, `main.rs` is the production gate, `perf_bench` is the
engineer's microscope.

## Known bottlenecks

The numbers above point to three dominant cost centers in the
retrieval engine today:

1. **ANN graph build is O(N²) at level 0.** Each
   `upsert_claim_vector` call iterates all previously inserted
   vectors in `select_ann_neighbors` to pick the new node's
   neighbors. Empirically:

   - 10 000 vectors at 384-dim takes ~30 s to build (release build,
     single thread).
   - 30 000 vectors takes ~4 minutes.
   - 100 000 vectors is projected at ~45 minutes, which is why the
     default ANN scenario uses 10 000 rather than the 100 000 in the
     original spec. The fix is a `usearch` (or similar SIMD-ANN)
   - backed HNSW or a layered graph that only does exhaustive search
   - at level 0 for the local neighborhood and approximate search
   - above. Tracked in
   - `docs/plans/2026-06-13-dash-modernization-roadmap.md` (section
   - on the ANN build bottleneck).

2. **WAL append + `sync_data` is per-record at `sync_every_records=1`.**
   The persistent-ingest path at ~133 ops/sec is dominated by the
   `fsync` syscall, not by anything in the in-memory store. The
   natural follow-up is a group-commit policy
   (`WalWritePolicy::sync_every_records = N` with a small `N` like
   32 or 128), and/or a redb snapshot that the cold-start path
   rehydrates from in milliseconds.

3. **`retrieve_semantic` is ~65× slower than `retrieve_throughput_lexical` at the same fixture.** Even with a 10-candidate
   match bucket, the semantic path expands candidates via
   `vector_candidates` (which walks the ANN graph) and then re-scores
   the candidates with dense similarity. This is by design — the
   semantic path is the right default for queries with a precomputed
   embedding — but it means callers that don't need dense ranking
   should fall back to `retrieve` (lexical). The fixture in this
   scenario is deliberately small (10 candidates) to expose the
   fixed overhead; widening the candidate bucket to 100+ is what
   shows the dense-similarity scoring term dominating.

## Future work

- **Snapshot the `BENCH_JSON:` line into a per-commit artifact** so
  regressions are visible in CI. The schema is already stable enough
  to diff (every field except `throughput_ops_per_sec` is integer-typed
  microseconds).
- **Wire `perf_bench` into the same gate logic as `main.rs`** so the
  CI history row can include `perf_bench` latencies alongside the
  existing `eme_avg_ms` column. The roadmap doc
  (`docs/plans/2026-06-13-dash-modernization-roadmap.md`) covers the
  broader plan to retire `main.rs` in favor of composable per-path
  scenarios.
- **Add `usearch` (or equivalent) backed ANN** so the
  `ann_search_throughput_at_scale` scenario can move from 10 000
  vectors at 30 s of build to 100 000+ vectors at <1 s of build.
  That change will shift the dominant cost from "build the graph" to
  "search the graph" and reshape the rest of the pipeline.
- **Add a WAL group-commit scenario** that compares
  `sync_every_records=1` against `sync_every_records=32,128,512` so
  the durability/throughput tradeoff is quantified, not guessed.
- **Add a memory-footprint probe** (`heap`, `rss`) alongside
  throughput for the ANN and semantic scenarios. Throughput without
  memory is a misleading optimization signal for retrieval engines.

## See also

- `tests/benchmarks/src/main.rs` — the existing end-to-end benchmark
  with quality gates, scorecard output, and history tracking.
- `tests/benchmarks/src/bin/concurrent_load.rs` — the HTTP
  concurrent-load driver used for the ingestion transport
  (queue-mode, WAL durability, etc.).
- `docs/benchmarks/history/benchmark-history.md` — the historical
  per-run table; `perf_bench` rows can be appended to this history
  as a separate section.
- `docs/plans/2026-06-13-dash-modernization-roadmap.md` — the
  longer-term roadmap that this suite is intended to support.
- `docs/plans/2026-06-13-redb-persistence-design.md` — the
  redb-backed materialized view that addresses the WAL-fsync
  bottleneck called out above.
