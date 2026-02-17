# Evaluation Protocol

Date: 2026-02-17
Status: active benchmark baseline

## 1. Objective

Measure whether DASH (Evidence Memory Engine) improves retrieval quality and evidence reliability under equivalent latency and infrastructure constraints.

## 2. Systems Under Test

- `B0 Dense-only`: ANN retrieval only
- `B1 Hybrid`: dense + lexical fusion without claim graph reasoning
- `B2 DASH`: dense + lexical + entity + temporal + graph-aware ranking

All systems must run with:

- identical query sets
- identical hardware budget class
- fixed index/model versions for a run

## 3. Dataset Strategy

### 3.1 Public comparability set

Purpose:

- cross-check retrieval behavior against widely used corpora

Requirements:

- stable snapshot hash
- clear licensing and redistribution policy

### 3.2 Internal domain set

Purpose:

- represent production document structures and language

Requirements:

- source quality labels
- tenant-safe anonymization rules

### 3.3 Adversarial set

Purpose:

- stress contradiction and temporal logic

Requirements:

- paired supporting/conflicting claims
- stale vs fresh timestamp variants
- multi-hop dependency cases

## 4. Query Buckets

- factual lookup
- temporal lookup
- contradiction-sensitive lookup
- multi-hop synthesis

Each bucket requires at least 200 labeled queries for stable trend tracking.

## 5. Metrics

### 5.1 Retrieval quality

- Recall@k
- nDCG@k
- MRR

### 5.2 Evidence quality

- citation coverage rate
- provenance precision
- contradiction detection F1

### 5.3 System performance

- p50/p95/p99 latency
- throughput at latency SLO
- memory footprint and storage growth

### 5.4 Cost

- infrastructure cost per 1k queries
- indexing cost per 1M claims

## 6. Gating Thresholds

Initial gates:

- p95 latency <= 350 ms on benchmark profile
- citation coverage >= 0.98 on labeled set
- contradiction F1 >= 0.80 on adversarial set
- no more than 5% regression on any primary metric release-over-release

## 7. Run Protocol

1. Freeze dataset snapshot IDs and model/index versions.
2. Build indexes with fixed resource budget and record build time.
3. Execute identical query suite with fixed random seed.
4. Collect quality, system, and cost metrics.
5. Compute confidence intervals for key quality metrics.
6. Publish scorecard and regression analysis.

## 8. Reproducibility Constraints

- pin software version and commit SHA
- pin hardware class and region
- store run manifest with configuration and seed
- retain raw run outputs for at least 30 days

## 9. Regression Triage Rules

- If quality regresses and latency improves, investigate ranking feature interactions.
- If latency regresses and quality is flat, inspect graph expansion and cache hit ratios.
- If citation coverage drops, block release until provenance integrity is restored.

## 10. Reporting Format

Use this template for each run:

```text
Run ID:
Date:
Commit:
Dataset snapshot:
Index/model versions:
Hardware profile:

Quality summary:
System summary:
Cost summary:

Regressions:
Root-cause hypotheses:
Action items:
```

## 11. CI/Staging Integration

- run smoke benchmark on each merge to main
- run benchmark history guard (`--guard-history`) on smoke profile in CI
- optionally run large-profile history guard in staged CI (`DASH_CI_INCLUDE_LARGE_GUARD=true`, `EME_CI_INCLUDE_LARGE_GUARD` still supported as fallback)
- large CI guard supports ANN preset overrides and hard gates via:
  - `DASH_CI_LARGE_ANN_*`
  - `DASH_CI_LARGE_MIN_CANDIDATE_REDUCTION_PCT`
  - `DASH_CI_LARGE_MAX_DASH_LATENCY_MS`
  - optional iteration override: `DASH_CI_LARGE_GUARD_ITERATIONS`
- optionally run xlarge-profile history guard in staged CI (`DASH_CI_INCLUDE_XLARGE_GUARD=true`, `EME_CI_INCLUDE_XLARGE_GUARD` fallback)
- xlarge CI guard supports ANN preset overrides and hard gates via:
  - `DASH_CI_XLARGE_ANN_*`
  - `DASH_CI_XLARGE_MIN_CANDIDATE_REDUCTION_PCT`
  - `DASH_CI_XLARGE_MAX_DASH_LATENCY_MS`
  - optional iteration override: `DASH_CI_XLARGE_GUARD_ITERATIONS`
- optionally run hybrid-profile history guard in staged CI (`DASH_CI_INCLUDE_HYBRID_GUARD=true`, `EME_CI_INCLUDE_HYBRID_GUARD` fallback)
- optionally run full trend automation in staged CI (`DASH_CI_RUN_BENCH_TREND=true`) with profile scope control via `DASH_CI_BENCH_TREND_INCLUDE_LARGE`, `DASH_CI_BENCH_TREND_INCLUDE_XLARGE`, and `DASH_CI_BENCH_TREND_INCLUDE_HYBRID`
- optionally run async transport feature checks and tests in staged CI (`DASH_CI_CHECK_ASYNC_TRANSPORT=true`)
- run full benchmark nightly
- block production promotion if any gate fails

## 12. Local Profile Matrix (Repository)

Current benchmark binary (`tests/benchmarks/src/main.rs`) exposes:

- `--profile smoke` -> 2,000 claims (CI smoke path)
- `--profile standard` -> 10,000 claims (default local baseline)
- `--profile large` -> 50,000 claims (scale sanity profile)
- `--profile xlarge` -> 100,000 claims (high-scale stress profile)
- `--profile hybrid` -> 20,000 claims with metadata-rich fixture for end-to-end `query_embedding + entity_filters + embedding_id_filters`

Optional overrides:

- `--iterations <N>` for explicit loop count
- `--history-out <PATH>` to append run metrics as a markdown row
- `--guard-history <PATH>` to compare current run against last history row for same profile
- `--max-dash-latency-regression-pct <N>` to cap allowed DASH avg-latency growth vs prior run (`--max-eme-latency-regression-pct` alias is still accepted)
- `--scorecard-out <PATH>` to emit a markdown benchmark scorecard (latency + quality probes)
- ANN tuning controls:
  - `--ann-max-neighbors-base <N>`
  - `--ann-max-neighbors-upper <N>`
  - `--ann-search-expansion-factor <N>`
  - `--ann-search-expansion-min <N>`
  - `--ann-search-expansion-max <N>`
- large-profile gates:
  - `--large-min-candidate-reduction-pct <N>` (default `95`)
  - `--large-max-dash-latency-ms <N>` (default `120`)
- xlarge-profile gates:
  - `--xlarge-min-candidate-reduction-pct <N>` (default `96`)
  - `--xlarge-max-dash-latency-ms <N>` (default `250`)
- env equivalents for benchmark runtime:
  - `DASH_BENCH_ANN_MAX_NEIGHBORS_BASE`
  - `DASH_BENCH_ANN_MAX_NEIGHBORS_UPPER`
  - `DASH_BENCH_ANN_SEARCH_EXPANSION_FACTOR`
  - `DASH_BENCH_ANN_SEARCH_EXPANSION_MIN`
  - `DASH_BENCH_ANN_SEARCH_EXPANSION_MAX`
  - `DASH_BENCH_LARGE_MIN_CANDIDATE_REDUCTION_PCT`
  - `DASH_BENCH_LARGE_MAX_DASH_LATENCY_MS`
  - `DASH_BENCH_XLARGE_MIN_CANDIDATE_REDUCTION_PCT`
  - `DASH_BENCH_XLARGE_MAX_DASH_LATENCY_MS`

## 13. Benchmark History Outputs

- Repository history path: `docs/benchmarks/history/benchmark-history.md`
- Each appended row must include profile, fixture size, iteration count, top1 hit status, and average latency metrics.
- History rows now also include:
  - `metadata_prefilter_count`
  - `ann_candidate_count`
  - `final_scored_candidate_count`
- Keep this artifact in version control so trend shifts are visible in review.

## 14. Trend Automation (Release Candidate)

- Script: `scripts/benchmark_trend.sh`
- Default behavior runs both:
  - `smoke` profile (`2,000` claims)
  - `large` profile (`50,000` claims)
- Optional scale profile:
  - `xlarge` profile (`100,000` claims) when `DASH_BENCH_INCLUDE_XLARGE=true` or `--include-xlarge true`
- Optional profile:
  - `hybrid` profile (`20,000` claims) when `DASH_BENCH_INCLUDE_HYBRID=true` or `--include-hybrid true`
- For each profile, the script enforces history guard, appends a new history row, and emits a timestamped scorecard under `docs/benchmarks/scorecards/`.
- The script also writes a markdown run summary under `docs/benchmarks/history/runs/` with command output and a per-profile metric table.

## 15. Scorecard Artifact

- Repository scorecard path (initial): `docs/benchmarks/scorecards/2026-02-17-initial-quality-scorecard.md`
- Current scorecard includes:
  - factual top1 hit and latency summary
  - candidate observability fields:
    - metadata prefilter candidate count
    - ANN candidate count
    - final scored candidate count
  - ANN tuning parameters used in the run
  - contradiction probe (`support_only`) pass/fail
  - temporal-window filtering probe pass/fail
  - exclusion of unknown event-time claims under temporal filters
  - hybrid retrieval probe for `query_embedding + entity_filters + embedding_id_filters`
  - all probes are enforced as benchmark pass/fail gates in the benchmark binary

## 16. Transport Concurrency Benchmark

- Load generator binary:
  - `cargo run -p benchmark-smoke --bin concurrent_load -- ...`
- Orchestration script:
  - `scripts/benchmark_transport_concurrency.sh`
- Purpose:
  - quantify throughput and latency shifts across HTTP worker-pool sizes
- Supported targets:
  - `--target retrieval` (default): benchmarks `GET /v1/retrieve?...`
  - `--target ingestion`: benchmarks `POST /v1/ingest` in persistent WAL mode
- Output artifact path:
  - `docs/benchmarks/history/concurrency/*.md`
- Ingestion WAL mode:
  - `--ingest-wal-path <PATH>` to control WAL location
  - default WAL path is a temporary `/tmp/dash-ingest-concurrency-<run_id>.wal`
- Baseline recommendation:
  - compare at least `workers=1` vs `workers=4` with fixed client/request settings for each target.
