# Benchmarks

DASH ships a `perf_bench` micro-benchmark binary at `tests/benchmarks/src/perf_bench.rs`. The full source-of-truth document is in the source tree at `docs/benchmarks/performance.md`; this page summarizes the methodology and the latest published numbers so the public docs site is self-contained.

## Methodology

The benchmark suite measures the six hot paths in the DASH retrieval pipeline:

| Scenario                              | Path measured                                                          | Fixture size   | Iterations |
| ------------------------------------- | ---------------------------------------------------------------------- | -------------: | ---------: |
| `ingest_throughput_sequential.in_memory` | `InMemoryStore::ingest_bundle` (no WAL)                                | empty → 110    |        100 |
| `ingest_throughput_sequential.persistent_wal` | `ingest_bundle_persistent` + `FileWal::append_*` + `sync_data` | empty → 110    |        100 |
| `retrieve_throughput_lexical`        | `InMemoryStore::retrieve` (no query vector)                            | 10 000 claims  |      1 000 |
| `retrieve_throughput_semantic`        | `InMemoryStore::retrieve_semantic` (with 768-dim query vector)         | 10 000 + vec   |      1 000 |
| `ann_search_throughput_at_scale`      | `InMemoryStore::ann_vector_top_candidates` (top-10)                    | 10 000 × 384-d |        500 |
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

## Latest published numbers

The latest baseline run was on commit `b3a4f1e` (the v0.1.0 release) on a `c6i.4xlarge` (16 vCPU, 32 GiB RAM, NVMe), single-tenant, single-process. The numbers are **DASH against itself** — there is no Pinecone / Weaviate / Milvus comparison in the current release. The competitor-comparison work is on the [roadmap](https://github.com/BHAWESHBHASKAR/DASH/issues?q=is%3Aopen+label%3Acompetitor-bench).

| Scenario                                  |     p50 |     p95 |     p99 |    mean | Throughput (ops/sec) |
| ----------------------------------------- | ------: | ------: | ------: | ------: | -------------------: |
| `ingest_throughput_sequential.in_memory`  |    1 µs |    8 µs |   14 µs |  1.99 µs |            502 512 |
| `ingest_throughput_sequential.persistent_wal` |  18 µs |   42 µs |   78 µs |  22.3 µs |             44 843 |
| `retrieve_throughput_lexical`             |  120 µs |  340 µs |  510 µs |  140 µs |              7 142 |
| `retrieve_throughput_semantic`            |  210 µs |  580 µs |  890 µs |  240 µs |              4 166 |
| `ann_search_throughput_at_scale`          |   85 µs |  220 µs |  340 µs |   98 µs |             10 204 |
| `wal_replay_throughput`                   | 1.2 ms | 1.9 ms | 2.4 ms | 1.3 ms |                769 |

!!! note "Reproducing these numbers"
    The numbers above are the canonical reference for the v0.1.0 release. Reproducing them requires a `c6i.4xlarge` (or equivalent) and `--release`. The `BENCH_JSON:` line in the run output is the machine-readable form; the table above is the human-readable summary.

## Where the suite does **not** cover

The current release does **not** benchmark:

- Cross-tenant isolation. The integration tests in `tests/store/` cover the isolation guarantees; the perf suite runs single-tenant.
- Multi-replica retrieval. The `redb` PR 3 replication path is on the [roadmap](https://github.com/BHAWESHBHASKAR/DASH/issues?q=is%3Aopen+label%3Aredb-pr3).
- ANN sharding. The sharding path is on the [roadmap](https://github.com/BHAWESHBHASKAR/DASH/issues?q=is%3Aopen+label%3Ashard-reshard).
- Embedding-provider latency. The `hash` provider is in-process; the `ollama`/`openai` paths add network latency that the suite does not measure.
- Competitor comparison. Internal baselines only.

These are tracked in the [issue tracker](https://github.com/BHAWESHBHASKAR/DASH/issues?q=is%3Aopen+label%3Abenchmark).

## For the source-of-truth doc

The in-tree doc at [`docs/benchmarks/performance.md`](https://github.com/BHAWESHBHASKAR/DASH/blob/main/docs/benchmarks/performance.md) has the full per-scenario fixtures, the percentile math, the throughput formula, and the changelog of baseline runs. The evaluation protocol (the test set, the ground truth, the scoring rubric) is at [`docs/benchmarks/evaluation-protocol.md`](https://github.com/BHAWESHBHASKAR/DASH/blob/main/docs/benchmarks/evaluation-protocol.md).
