# DASH Performance Benchmark Suite

> **Status note (2026-10-09).** The numbers in this document come from a single local run on an unspecified
> "Apple M-series" machine on 2026-06-15. They were not produced by CI, are not tied to a commit, and have not
> been repeated, so treat them as illustrative only. They must not be quoted as DASH performance. The suite's
> methodology (scenarios, fixtures, percentile math) is accurate to the code in `tests/benchmarks`. The
> "Security posture" section below is a 2026-06-15 snapshot and is out of date; run `cargo audit` for current
> results. Reproducible, CI-produced numbers are planned (see `docs/benchmarks/history/README.md`).

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

These numbers were measured against the in-repo graph that P2 removed; see
"Vector index after P2" below for the replacement's numbers.

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

## Vector index after P2 (2026-10-09)

Measured with a throwaway driver (not committed) on the same 4 vCPU Xeon box as
ADR 0003, release build, seeded Gaussian mixture of 32 clusters with isotropic
noise (|noise| about 0.8, a harder shape than the ADR's low-dimensional
clusters), top-10, 200 timed queries, recall@10 against
`exact_vector_top_candidates` on the first 50. "Build" is claim ingest plus
vector upserts into an empty store, single-threaded. Single runs on a shared box.

| Vectors | Dim | Index | Build | p50 | p99 | Recall@10 |
|---:|---:|---|---:|---:|---:|---:|
| 5 000 | 384 | removed graph | 11.3 s | 463 us | 626 us | 0.280 |
| 10 000 | 384 | removed graph | 54.4 s | 631 us | 901 us | 0.518 |
| 20 000 | 384 | removed graph | 327.3 s | 1,121 us | 1,511 us | 0.580 |
| 20 000 | 384 | flat/HNSW | 4.3 s | 299 us | 976 us | 1.000 |
| 20 000 | 64 | flat/HNSW | 1.5 s | 119 us | 610 us | 1.000 |
| 100 000 | 384 | flat/HNSW | 74.7 s | 1,028 us | 1,905 us | 0.964 |

The `ann_search_throughput_at_scale` scenario of `perf_bench` (10 000 uniform random 384-d vectors, which are harder for HNSW than clustered data) now reports `build_ms` and
`vector_index_bytes`; in the unoptimised dev profile (C++ `usearch` is always built with -O3, the Rust loops are not) it measured build 4.8 s,
p50 1.29 ms, p99 3.5 ms and 18 MB of index on this box. It is not comparable with the 2026-06-15 release numbers above.

Cold start with 100 000 x 384-d vectors in a WAL (`load_from_wal`, 4 threads for
the index build): 24.0 s, of which 18.6 s is the HNSW build (a second run:
26.4 s and 24.8 s total). The same inserts done one by one take 74.7 s, and raw
`usearch` `i8` single-thread insertion of the same vectors takes 73.6 s, so the
wrapper adds no measurable overhead; the per-vector cost is the library's on this
data (the ADR's lower-dimensional clusters built about 3x faster). Since the
index is persisted (next section) a restart with a current index file skips this
build. Memory: the `i8` HNSW holds `dim` bytes plus the graph
per vector and no `f32` copy (the full-precision vectors stay in `claim_vectors`);
`StoreIndexStats::vector_index_bytes` reports it. Tenants at or below the flat
threshold (default 8192) keep one extra normalised `f32` copy.

### Cold start with the persisted vector index (2026-10-09)

`cargo run --release -p benchmark-smoke --bin cold_start -- N 384 1000 RUNS`
(`tests/benchmarks/src/bin/cold_start.rs`): one tenant, 64-cluster Gaussian
mixture, default tuning, 4 vCPUs on a shared VM (other jobs were running, so
the spread between runs is large; every run is listed).

| N x dim | WAL | index file | replay floor (no HNSW) | full rebuild (before) | load saved index (after) | load + catch-up of 1000 vectors | save |
|---|---|---|---|---|---|---|---|
| 50k x 384 | 215 MB | 28.2 MB | 4.0 s | 8.0, 8.2, 12.0 s | 3.9, 3.7, 3.9 s | 4.5, 4.6, 4.6 s | 0.16 to 0.23 s |
| 100k x 384 | 431 MB | 56.4 MB | 9.0 s | 23.1, 27.5 s | 8.8 s | not measured (the shared disk filled up) | 0.41 s |

"Replay floor" loads the same WAL with the tenant kept on the flat index, so it
is the WAL parsing and store rebuild that every start pays. With a current
index file the load equals that floor within noise: the HNSW build is gone and
cold start is now bounded by parsing the text WAL (about 2x faster at 50k,
about 3x at 100k). Catch-up re-inserts the vectors written after the last save
one by one, about 0.7 ms each at 384-d. ADR 0003 section 11 has the design.

## Full-text index (2026-10-10)

`pkg/store/src/text_index.rs` (ADR 0003 section 12). Two measurements: retrieval quality on a labelled set, and
cost at 100,000 claims.

### Quality: nDCG@10 and recall@10

`cargo test -p store --test relevance_eval -- --nocapture`. The set (`pkg/store/tests/relevance/mod.rs`) is generated
deterministically: 24 topics with their own entities, nouns, inflected verbs and three aspects each, 16 claims per topic,
64 distractor claims made of words every topic uses, about a third of the claims also naming a noun of another topic,
mixed case and punctuation; 72 queries (aspect, entity with stop words, upper-case keywords) using other surface forms
than the claims, with graded judgements (2 = topic and aspect or entity, 1 = topic). Gain `2^grade - 1`; recall@10 is
capped (`hits / min(10, relevant)`). Claims carry no evidence or edges; confidence varies from 0.5 to 1.0, so the prior
signals add noise that the judgements do not reward.

| System | nDCG@10 | recall@10 |
|---|---|---|
| Previous shared-word rule (candidates share an ASCII token; overlap + raw BM25 + priors) | 0.7686 | 0.6597 |
| Previous hybrid score (`(cos + 1) / 2 + 0.1 * old lexical`, 200 nearest vectors) | 0.7657 | 0.6417 |
| BM25 alone (`TenantTextIndex::search`) | **0.8744** | **0.8375** |
| Vector alone (exact cosine, 384-d hash embeddings) | 0.3106 | 0.2222 |
| `InMemoryStore::retrieve` (BM25 candidates, normalised BM25 + priors) | 0.8634 | 0.8264 |
| `InMemoryStore::retrieve_semantic` (hybrid blend, hash-embedding query vector) | 0.7950 | 0.6819 |

The test fails when BM25, the store's lexical or its hybrid retrieve drops 0.02 below these values, or when either new
path stops beating the path it replaced. Hybrid is below BM25 alone here because the hash embedder is a development
stand-in (vector alone 0.31) and the blend keeps the semantic-first guarantee (cosine weighs as much as normalised BM25);
with a real embedding model the vector part is expected to help, but that is not measured. The set is synthetic; numbers
on a real corpus are not measured either.

### Cost at 100,000 claims

`cargo run --release -p benchmark-smoke --bin fulltext_bench -- 100000 1000 64`. Corpus as in the ADR 0003 text spike:
Zipf (s = 1.0) over 50k words, 30 to 60 words per claim (4.5M words), one tenant; 1,000 OR queries per cell taken from a
random claim ("natural": Zipf-weighted, head words dominate; "midtail": words of rank >= 100); top_k 10. Release build,
4 vCPUs on a shared VM, one run. The previous rule is re-implemented in the binary (union of posting sets, composite
score of every candidate, statistics recomputed per query) and run on the first 100 queries of each cell.

| | |
|---|---|
| Index build (insert of 100k claims) | 1.26 s, 12.6 us per claim |
| Store ingest of the same claims (no vectors, includes the index) | 1.53 s, 15.3 us per claim (index insert is 82 % of it) |
| Index heap | 54.9 MB, 549 B per claim (12 B per posting: slot + term frequency, plus terms and the id table) |
| Store RSS growth for the claims (claim rows, all indexes) | 125.6 MB, 1,286 B per claim |
| Delete (index only) | 220 us per claim (posting lists of head terms are shifted); a tenant erasure drops the index at once |

Query latency p50 / p95 / p99 in microseconds:

| Mix, terms | Index top-200 | Store retrieve (text only) | Previous rule | Previous rule, avg candidates |
|---|---|---|---|---|
| natural, 1 | 190 / 1,230 / 1,471 | 444 / 1,505 / 1,833 | 8,946 / 701,081 / 758,385 | 25,909 |
| natural, 3 | 930 / 1,508 / 1,671 | 1,305 / 1,887 / 2,059 | 347,210 / 784,449 / 847,841 | 48,064 |
| natural, 6 | 1,260 / 1,868 / 2,338 | 1,687 / 2,330 / 2,669 | 791,880 / 921,301 / 943,413 | 73,049 |
| midtail, 1 | 90 / 192 / 236 | 273 / 433 / 484 | 1,938 / 22,919 / 28,785 | 709 |
| midtail, 3 | 148 / 246 / 295 | 408 / 537 / 650 | 13,676 / 33,435 / 46,414 | 1,750 |
| midtail, 6 | 193 / 305 / 381 | 501 / 637 / 741 | 26,380 / 59,032 / 82,546 | 3,356 |

Hybrid retrieve (natural, 3 terms, 64-d random vectors, HNSW): 2,881 / 3,841 / 4,610 us.

The index is not persisted: the WAL replay at startup rebuilds it, which adds the build time above (about 1.3 s per
100k claims of this length) to a replay that costs about 9 s per 100k claims with 384-d vectors. The index scores
term-at-a-time over whole posting lists (no block-max WAND), so queries dominated by head terms cost about 1-2 ms at
100k; the tantivy spike measured 0.07-0.3 ms p50 on the same corpus shape.

## Known bottlenecks

The numbers above point to three dominant cost centers in the
retrieval engine today:

1. **ANN graph build was O(N²) at level 0 (fixed in P2, IDX-01).** The
   in-repo graph scanned every previously inserted vector per insert:
   10 000 vectors at 384-dim took ~30 s, 20 000 took 321 s in the ADR 0003
   spike. It was replaced by a per-tenant flat/`usearch` HNSW index; see the
   next section for the measured build time, recall and startup cost.

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
- **Make WAL replay cheaper.** The vector index is persisted (see "Cold start
  with the persisted vector index"); cold start is now the text WAL and
  snapshot parse, about 9 s per 100k 384-d vectors. A binary snapshot, or
  loading vectors from redb, is the next step. Memory-mapping the index
  (`view` took 45 ms for 500k vectors in the ADR 0003 spike) would also save
  the copy into RAM, but a viewed index is read-only.
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
