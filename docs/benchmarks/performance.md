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

The following numbers are the actual output of `cargo run -p
benchmark-smoke --bin perf_bench --release -- --all` on the reference
machine used to validate this suite. They are the "first-run baseline"
— re-run the suite to compare against these.

### `ingest_throughput_sequential.in_memory`
```
iterations:               100
p50_us:                   1
p95_us:                   8
p99_us:                   14
max_us:                   15
min_us:                   1
mean_us:                  2.0
throughput_ops_per_sec:   502513
```

### `ingest_throughput_sequential.persistent_wal`
```
iterations:               100
p50_us:                   7983
p95_us:                   8069
p99_us:                   8597
max_us:                   14311
min_us:                   5908
mean_us:                  7539.4
throughput_ops_per_sec:   133
```

The 4 000× gap between the in-memory path and the WAL-fsync-per-record
path isolates the cost of `sync_data` on the on-disk WAL. This is the
floor: any optimization to ingest throughput will be invisible until
the durability boundary is amortized (e.g. group-commit, or a
redb-backed materialized view + WAL-tail replay as in
`docs/plans/2026-06-13-redb-persistence-design.md`).

### `retrieve_throughput_lexical`
```
iterations:               1000
p50_us:                   163
p95_us:                   174
p99_us:                   347
max_us:                   611
min_us:                   154
mean_us:                  167.5
throughput_ops_per_sec:   5969
```

10 000-claim fixture with the query (`"alice operation status"`)
matching exactly 10 claims (the `QUERY_MATCH_BUCKET`). The candidate
set is small, so `score_and_rank_candidate_claim_ids` is the dominant
cost.

### `retrieve_throughput_semantic`
```
iterations:               1000
p50_us:                   10873
p95_us:                   12009
p99_us:                   13220
max_us:                   29847
min_us:                   10535
mean_us:                  11060.9
throughput_ops_per_sec:   90
```

Same fixture but with 768-dim vectors stored on every claim and the
query vector passed in. The semantic path expands the candidate set
via `vector_candidates` (which itself walks the ANN graph) before
scoring, so it is ~65× slower than lexical here. The candidate set
is still small in this fixture, so this is mostly ANN-graph traversal
plus dense-similarity scoring; widen the fixture to see the score
term dominate.

### `ann_search_throughput_at_scale`
```
iterations:               500
p50_us:                   655
p95_us:                   957
p99_us:                   1131
max_us:                   1312
min_us:                   593
mean_us:                  695.6
throughput_ops_per_sec:   1438
```

10 000 vectors at 384-dim, top-10 lookup. The pre-load takes
~30 seconds because `select_ann_neighbors` is O(N) at level 0; this
is the most expensive setup of the suite (see "Known bottlenecks"
below).

### `wal_replay_throughput`
```
iterations:               100
p50_us:                   2001
p95_us:                   2344
p99_us:                   2419
max_us:                   2444
min_us:                   1956
mean_us:                  2066.5
throughput_ops_per_sec:   484
```

1 000 claims in a WAL with `sync_every_records=1` (so 2 000 records:
1 000 claims + 1 000 evidence). Each measured iteration re-opens the
WAL and replays it into a fresh `InMemoryStore`. The throughput is
~2 ms per replay ≈ 484 replays/sec.

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
