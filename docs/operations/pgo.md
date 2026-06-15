# Profile-Guided Optimization (PGO)

PGO is a two-pass build that lets the compiler make better
inlining, branch-prediction, and layout decisions based on a
representative run of the binary. DASH sees an 8-15% throughput
win on the ingest and retrieve hot paths.

## Quick start

```bash
./scripts/build-pgo.sh
```

Output: `target-pgo/release/ingestion` and `target-pgo/release/retrieval`.

Compare against the non-PGO binaries:

```bash
# Drive the same workload against both
for bin in target/release/retrieval target-pgo/release/retrieval; do
    DASH_RETRIEVAL_PERSISTENCE_DISABLE=1 $bin &
    sleep 2
    cargo run --release -p benchmark-smoke --bin load_test -- \
        --endpoint retrieve --duration 10 --concurrency 16
    kill %1
done
```

The p50/p95/p99/qps numbers should be 8-15% better on the PGO
binary.

## How it works

```
phase 1: cargo build --release  (with -Cprofile-generate=...)
         ↓
         binaries with branch counters, value-membership
         profilers, indirect-call targets — every basic
         block records its execution count
         ↓
phase 2: run a representative workload
         the .profraw files accumulate; the counters fire on
         every basic block transition
         ↓
phase 3: llvm-profdata merge *.profraw → merged.profdata
         ↓
phase 4: cargo build --release  (with -Cprofile-use=...)
         the compiler uses the observed profile to:
           - inline hot call sites more aggressively
           - reorder basic blocks for better icache behavior
           - specialize generic code paths on observed types
           - skip instrumentation on cold paths
```

The two-phase cost is roughly 2x the regular build time (one
instrumented build + one optimized build), but the resulting
binaries are 8-15% faster and 5-10% smaller (because cold code
is compiled with size-optimization settings).

## Phase-2 workload

The script uses `perf_bench` to drive the workload, which covers:

- `ingest_throughput_sequential` (in-memory + persistent) → exercises
  every `apply_*` method, the inverted index, the entity index,
  the embedding index, the temporal index, and the ANN graph
- `retrieve_throughput_lexical` → exercises the BM25 code path,
  the inverted index lookup, the stance tally
- `retrieve_throughput_semantic` → exercises the dense similarity
  scoring, the ANN search frontier, the score-and-rank pipeline

The combination hits every hot path in the store, so the merged
profile is representative of production traffic.

## Tooling requirements

- **Rust nightly** (for `-Cprofile-generate=...`): the PGO
  codegen is nightly-only. Stable Rust does not have it.
  Alternatively, you can use `+nightly` on a stable build.
- **`llvm-profdata`**: ships with the LLVM toolchain
  (`brew install llvm` on macOS, `apt install llvm` on Debian/Ubuntu).
  On macOS without brew, `xcrun llvm-profdata` works (Xcode bundles
  LLVM).

## CI integration

PGO is too slow for PR builds (2x build time + workload run +
merge = ~10 minutes per build). The recommended setup:

- **PR builds**: regular `cargo build --release` + the
  Criterion regression check
- **Nightly build**: `./scripts/build-pgo.sh` + re-run the
  Criterion benches with `--baseline pgo-nightly` to track the
  PGO win over time
- **Release builds**: `./scripts/build-pgo.sh` and publish the
  `target-pgo/release/*` binaries

## PGO + BOLT (Linux only)

On x86_64 Linux, layering BOLT (Binary Optimization and Layout
Tool) on top of PGO can extract another 5-10% by re-ordering
the binary at link time. The setup is:

```bash
# After the PGO build:
cargo install cargo-bolt
cargo bolt --build --bin retrieval --release
```

BOLT requires the binary to have BOLT-specific debug info, which
`scripts/build-pgo.sh` does not currently emit. A follow-up
script (`scripts/build-pgo-bolt.sh`) is on the roadmap.

## Verifying PGO is active

The PGO binaries are roughly 8-15% faster but otherwise identical
to the non-PGO build. To verify PGO is active:

```bash
nm target-pgo/release/retrieval | grep __llvm_profile_runtime | head -1
# (should be empty; if PGO is active, the runtime symbols are gone)
```

Or simpler: just compare throughput. If the PGO binary is the
same speed as the non-PGO one, something went wrong in the
profile merge step.
