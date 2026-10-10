# Benchmarks

DASH ships a `perf_bench` micro-benchmark binary at `tests/benchmarks/src/perf_bench.rs`. The full source-of-truth document is in the source tree at `docs/benchmarks/performance.md`; this page summarizes the methodology. It does not carry result numbers (see below).

## Methodology

The benchmark suite measures the six hot paths in the DASH retrieval pipeline:

| Scenario                              | Path measured                                                          | Fixture size   | Iterations |
| ------------------------------------- | ---------------------------------------------------------------------- | -------------: | ---------: |
| `ingest_throughput_sequential.in_memory` | `InMemoryStore::ingest_bundle` (no WAL)                                | empty → 110    |        100 |
| `ingest_throughput_sequential.persistent_wal` | `ingest_bundle_persistent` + `FileWal::append_*` + `sync_data` | empty → 110    |        100 |
| `retrieve_throughput_lexical`        | `InMemoryStore::retrieve` (no query vector)                            | 10 000 claims  |      1 000 |
| `retrieve_throughput_semantic`        | `InMemoryStore::retrieve_semantic` (with 768-dim query vector)         | 10 000 + vec   |      1 000 |
| `ann_search_throughput_at_scale`      | `InMemoryStore::ann_vector_top_candidates` (top-10; flat/`usearch` HNSW index, reports `build_ms`) | 10 000 × 384-d |        500 |
| `wal_replay_throughput`               | `FileWal::open` + `load_from_wal_with_stats_and_ann_tuning`            | 1 000 claims   |        100 |

All scenarios are timed with `std::time::Instant` per iteration. The distribution is summarized as p50 / p95 / p99 / min / max / mean microseconds, plus an aggregate throughput in operations per second.

### Build mode

`--release`. Debug builds inflate the numeric kernels (cosine, BM25) by 10× and the ANN graph build by 10×. Release numbers are the canonical reference.

### Warm-up

10 iterations per scenario by default, not included in the measurement. The warm-up evens out the OS page cache and stabilizes the in-memory maps (`HashMap` capacity growth).

### Percentile calculation

```text
latencies_us.sort_unstable();
idx = ((len-1) * quantile).round() as usize;
percentile = latencies_us[idx.clamp(0, len-1)];
```

### Throughput

```text
throughput_ops_per_sec = iterations / total_seconds
```

where `total_seconds` is the sum of the per-iteration latencies. A 1 ms mean ≈ 1 000 ops/sec.

### Single-process, single-tenant

Each scenario builds a fresh `InMemoryStore` and (where applicable) a fresh `FileWal`. Cross-scenario interference is avoided. WAL temp directories are cleaned up via `fs::remove_dir_all` before the scenario returns.

## Running the suite

```bash
# All scenarios with default iteration counts
cargo run -p benchmark-smoke --bin perf_bench --release -- --all

# Single scenario, custom iteration count
cargo run -p benchmark-smoke --bin perf_bench --release \
  -- --scenario retrieve_throughput_semantic --iterations 500

# Adjust warm-up
cargo run -p benchmark-smoke --bin perf_bench --release -- --all --warmup 25
```

The output is two-part:

1. A human-readable per-scenario block on stdout.
2. A single `BENCH_JSON:` line at the end of the run, with a stable JSON schema (one object per scenario).

## Published numbers

**There are no verified published numbers.** An earlier version of this page showed a table attributed to commit `b3a4f1e` (a commit that does not exist in this repository) and a `c6i.4xlarge` instance, with latencies that contradict the in-tree document. That table could not be reproduced or traced to a run, and it was removed on 2026-10-09 (register item DOC-05).

What exists:

- [`docs/benchmarks/performance.md`](https://github.com/BHAWESHBHASKAR/DASH/blob/main/docs/benchmarks/performance.md) records one first-run baseline dated 2026-06-15 on an unspecified "Apple M-series" machine. It is a single local run, not repeated, not produced by CI, and not tied to a commit. Use it only to see the shape of the results, not as a performance claim.
- [`docs/benchmarks/history/`](https://github.com/BHAWESHBHASKAR/DASH/tree/main/docs/benchmarks/history) holds drill and smoke outputs; see the README there.

No comparison against Pinecone, Weaviate, Milvus, Qdrant or Chroma exists. Reproducible benchmark numbers (command, commit, machine description, CI artifact) are planned work (P5/P7 in the master plan); until then do not quote a DASH latency or throughput figure.

## Where the suite does **not** cover

The current release does **not** benchmark:

- Cross-tenant isolation. The perf suite runs single-tenant (isolation is covered by unit and integration tests in `pkg/store` and the services, not by this suite).
- Multi-replica retrieval and replication lag under load.
- ANN sharding (not implemented).
- Embedding-provider latency. The `hash` provider is in-process; the `ollama`/`openai` paths add network latency that the suite does not measure.
- Competitor comparison. Internal baselines only.

ANN recall at scale is not measured by a reproducible job either (register IDX-01).

## For the source-of-truth doc

The in-tree doc at [`docs/benchmarks/performance.md`](https://github.com/BHAWESHBHASKAR/DASH/blob/main/docs/benchmarks/performance.md) has the full per-scenario fixtures, the percentile math, the throughput formula, and the changelog of baseline runs. The evaluation protocol (the test set, the ground truth, the scoring rubric) is at [`docs/benchmarks/evaluation-protocol.md`](https://github.com/BHAWESHBHASKAR/DASH/blob/main/docs/benchmarks/evaluation-protocol.md).
