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
- optionally run SLO/error-budget guard in staged CI (`DASH_CI_INCLUDE_SLO_GUARD=true`) with optional recovery-drill inclusion (`DASH_CI_SLO_INCLUDE_RECOVERY_DRILL=true`)
- run full benchmark nightly
- block production promotion if any gate fails

## 12. Local Profile Matrix (Repository)

Current benchmark binary (`tests/benchmarks/src/main.rs`) exposes:

- `--profile smoke` -> 2,000 claims (CI smoke path)
- `--profile standard` -> 10,000 claims (default local baseline)
- `--profile large` -> 50,000 claims (scale sanity profile)
- `--profile xlarge` -> 100,000 claims (high-scale stress profile; benchmark fixture keeps full claim cardinality but samples non-target vector upserts to keep ANN build time bounded)
- `--profile hybrid` -> 20,000 claims with metadata-rich fixture for end-to-end `query_embedding + entity_filters + embedding_id_filters`

Optional overrides:

- `--fixture-size <N>` to override profile fixture cardinality for scale-proof runs
- `--iterations <N>` for explicit loop count
- `--history-out <PATH>` to append run metrics as a markdown row
- `--history-csv-out <PATH>` to append the same metrics in CSV format
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
- segment-cache probe strictness gates:
  - `--min-segment-refresh-successes <N>` (default `0`)
  - `--min-segment-cache-hits <N>` (default `0`)
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
  - `DASH_BENCH_HISTORY_CSV_OUT`
  - `DASH_BENCH_MIN_SEGMENT_REFRESH_SUCCESSES`
  - `DASH_BENCH_MIN_SEGMENT_CACHE_HITS`
  - `DASH_BENCH_FIXTURE_SIZE` (same behavior as `--fixture-size`)
  - `DASH_BENCH_WAL_SCALE_CLAIMS` (override claim volume used by WAL checkpoint/replay scale slice)

## 13. Benchmark History Outputs

- Repository history path: `docs/benchmarks/history/benchmark-history.md`
- Optional CSV history path: configured via `--history-csv-out` or `DASH_BENCH_HISTORY_CSV_OUT`
- Each appended row must include profile, fixture size, iteration count, top1 hit status, and average latency metrics.
- History rows now also include:
  - `metadata_prefilter_count`
  - `ann_candidate_count`
  - `final_scored_candidate_count`
  - ANN recall metrics:
    - `ann_recall_at_10`
    - `ann_recall_at_100`
    - `ann_recall_curve` (`budget:recall` points)
  - segment-cache probe metrics:
    - `segment_cache_hits`
    - `segment_refresh_attempts`
    - `segment_refresh_successes`
    - `segment_refresh_failures`
    - `segment_refresh_avg_ms`
  - WAL scale slice metrics:
    - `wal_claims_seeded`
    - `wal_checkpoint_ms`
    - `wal_replay_ms`
    - `wal_snapshot_records`
    - `wal_truncated_wal_records`
    - `wal_replay_snapshot_records`
    - `wal_replay_wal_records`
    - `wal_replay_validation_hit`
    - `wal_replay_validation_top_claim`
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
  - ANN recall observability fields:
    - `ann_recall_at_10`
    - `ann_recall_at_100`
    - `ann_recall_curve`
  - ANN tuning parameters used in the run
  - segment prefilter cache observability:
    - refresh attempts/successes/failures
    - cache hits
    - refresh average load time
  - WAL checkpoint/snapshot scale slice:
    - profile-scoped claim volume used
    - checkpoint runtime
    - replay runtime
    - snapshot/wal replay record counts
    - post-checkpoint replay retrieval validation (`hit` + top claim id)
  - contradiction probe (`support_only`) pass/fail
  - contradiction-detection F1 probe on adversarial fixture:
    - `contradiction_detection_f1`
    - enforced gate: `>= 0.80`
    - `contradiction_detection_f1_pass`
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
  - `--ingest-wal-sync-every-records <N>` to configure WAL fsync batch threshold
  - `--ingest-wal-append-buffer-records <N>` to configure in-process append buffering
  - `--ingest-wal-sync-interval-ms <N|off>` to cap max unsynced duration when batching
  - `--ingest-allow-unsafe-wal-durability true|false` to explicitly bypass ingestion startup guardrails for stress runs
  - default WAL path is a temporary `/tmp/dash-ingest-concurrency-<run_id>.wal`
- Baseline recommendation:
  - compare at least `workers=1` vs `workers=4` with fixed client/request settings for each target.

## 17. Ingestion WAL Durability Benchmark

- Orchestration script:
  - `scripts/benchmark_ingest_wal_durability.sh`
- Purpose:
  - compare ingestion throughput/latency tradeoffs across WAL durability policies under a fixed load profile
- Default modes:
  - `strict_per_record`: `sync_every=1`, `append_buffer=1`, `sync_interval=off`
  - `grouped_sync`: `sync_every=32`, `append_buffer=1`, `sync_interval=off`
  - `buffered_interval`: `sync_every=64`, `append_buffer=32`, `sync_interval=250ms`
- Guardrail behavior:
  - ingestion startup enforces safe WAL durability defaults
  - script defaults to `allow_unsafe_wal_durability=true` to run intentional stress profiles
- Output artifact paths:
  - summary: `docs/benchmarks/history/concurrency/wal-durability/*.md`
  - per-mode raw runs: `docs/benchmarks/history/concurrency/*.md`
- Baseline recommendation:
  - run with fixed `workers`, `clients`, and request profile, then compare `throughput_vs_strict` deltas only across mode rows from the same run artifact.

## 18. Phase 4 Scale-Proof Runner

- Orchestration script:
  - `scripts/phase4_scale_proof.sh`
- Purpose:
  - run the Phase 4 kickoff evidence lane in one command with reproducible artifacts
- Script actions:
  - executes benchmark lane with profile + `--fixture-size` override and appends history + scorecard
  - executes retrieval transport concurrency benchmark (captures throughput + p95/p99)
  - executes ingestion transport concurrency benchmark (captures throughput + p95/p99 + freshness proxy guard)
  - executes failover drill (`restart`, `no-restart`, or `both`)
  - executes rebalance/split drill (placement shard-count increase + epoch transition + route movement evidence)
  - emits consolidated summary under `docs/benchmarks/history/runs/*.md`
- Key options:
  - `--profile <smoke|standard|large|xlarge|hybrid>`
  - `--run-id <ID>` (optional explicit run id; enables deterministic summary/scorecard artifact names)
  - `--fixture-size <N>`
  - `--iterations <N>`
  - `--wal-scale-claims <N>`
  - `--bench-release true|false` (default `true`; uses optimized benchmark binary for large fixtures)
  - `--retrieval-workers-list <csv>`
  - `--ingestion-workers-list <csv>`
  - `--ingest-freshness-p95-slo-ms <N>`
  - `--failover-mode restart|no-restart|both`
  - `--run-rebalance-drill true|false`
  - `--rebalance-artifact-dir <dir>`
  - `--rebalance-require-moved-keys <N>`
  - `--rebalance-probe-keys-count <N>`
  - `--rebalance-max-wait-seconds <N>`
  - `--rebalance-target-shards <N>` (default `8`)
  - `--rebalance-min-shards-gate <N>` (default `8`)

## 19. Phase 4 Distributed Protocol Gates (`10M` / `100M`)

### 19.1 Gate tiers

- Tier A (`1M` baseline)
  - retrieval latency: `p95 <= 350 ms`, `p99 <= 500 ms`
  - freshness proxy: ingestion transport benchmark `latency_p95_ms <= 5000`
  - quality: benchmark expected top1 hit `true`, quality probes pass, contradiction F1 gate satisfied
  - ANN recall: `recall@10 >= 0.95`, `recall@100 >= 0.98`
- Tier B (`10M` staged distributed)
  - routing: placement-enabled routed traffic across `>= 8` shards
  - rebalance: shard split/rebalance epoch transition drill passes with artifacts
  - retrieval latency: `p95 <= 350 ms`, `p99 <= 600 ms`
  - freshness: sustained ingest freshness `p95 <= 5000 ms`
  - security/isolation: scoped cross-tenant deny checks pass under routed workload
- Tier C (`100M` world-scale proof)
  - distributed staged topology: multi-shard groups with leader/follower placement
  - retrieval latency: `p95 <= 350 ms`, `p99 <= 700 ms`
  - freshness: sustained ingest freshness `p95 <= 5000 ms` during segment maintenance
  - durability/recovery: replay+restore drill meets agreed RTO/RPO envelope and post-recovery retrieval correctness checks
  - operational resilience: incident simulation gate passes with scale placement active

### 19.2 Required artifact bundle per gate run

- phase-4 run summary: `docs/benchmarks/history/runs/<run-id>.md`
- benchmark scorecard: `docs/benchmarks/scorecards/<run-id>-<profile>-fixture-<n>.md`
- benchmark history row in:
  - `docs/benchmarks/history/benchmark-history.md`
- retrieval concurrency artifact:
  - `docs/benchmarks/history/concurrency/<run-id>-*-retrieval.md`
- ingestion concurrency artifact:
  - `docs/benchmarks/history/concurrency/<run-id>-*-ingestion.md`
- failover/rebalance artifact references embedded in run summary
- rebalance drill summary:
  - `docs/benchmarks/history/rebalance/<run-id>-rebalance/summary.md`

### 19.3 Recommended execution protocol

1. Run baseline scale lane:
   - `scripts/phase4_scale_proof.sh --run-tag phase4-1m --fixture-size 1000000 --iterations 1`
2. Run staged distributed lane (`10M`) with routed placement metadata and rebalance drill attached:
   - use `scripts/phase4_scale_proof.sh` for benchmark+concurrency+failover+rebalance bundle
   - if needed, run standalone rebalance rehearsal:
     - `scripts/rebalance_drill.sh --run-id <run-id>-rebalance`
3. Run world-scale lane (`100M`) with distributed placement and recovery drills:
   - execute phase4 lane command in staged distributed environment
   - execute `scripts/recovery_drill.sh` and `scripts/incident_simulation_gate.sh`
   - include all outputs in progress log and phase closure checklist

### 19.4 Gate decision rule

- PASS only if all required metrics and artifact classes for the tier are present and within thresholds.
- Any missing artifact or threshold breach is a FAIL and blocks promotion to next tier.
- For Tier-B shard topology proof, `phase4_scale_proof` now enforces a rebalance shard gate:
  - `rebalance_shard_count_after >= rebalance_min_shards_gate`

## 20. Long-Soak Launcher (Deterministic Paths + Heartbeat)

- Script:
  - `scripts/phase4_long_soak.sh`
- Purpose:
  - run long Phase 4 scale jobs with deterministic artifact paths and a continuously refreshed heartbeat file.
- Required:
  - `--run-id <ID>`
- Deterministic artifacts for run id `R`:
  - summary: `docs/benchmarks/history/runs/R.md`
  - log: `docs/benchmarks/history/runs/R.log` (default)
  - heartbeat: `docs/benchmarks/history/runs/R.heartbeat` (default)
  - scorecard: `docs/benchmarks/scorecards/R-<profile>-fixture-<n>.md`
- Heartbeat payload fields:
  - `run_id`
  - `status` (`running|success|failed`)
  - `heartbeat_utc`
  - `elapsed_seconds`
  - `summary_path`
  - `scorecard_path`
  - `log_path`
  - final write includes `exit_code`
- Example:
  - `scripts/phase4_long_soak.sh --run-id phase4-1m-long --fixture-size 1000000 --iterations 1`

### 20.1 Long-Soak Control Script

- Script:
  - `scripts/phase4_long_soak_ctl.sh`
- Purpose:
  - lifecycle control for long-soak jobs (`start`, `status`, `tail`, `stop`, `run`) with stable pid/log/heartbeat file conventions.
- Actions:
  - `start`: launches `phase4_long_soak.sh` in background and writes `<run_id>.pid`
  - `status`: reports pid state and prints heartbeat payload if present
  - `tail`: tails `<run_id>.log` (`--follow true|false`)
  - `stop`: sends signal to pid from `<run_id>.pid`
  - `run`: foreground execution wrapper around `phase4_long_soak.sh`
- Example:
  - `scripts/phase4_long_soak_ctl.sh start --run-id phase4-1m-long -- --fixture-size 1000000 --iterations 1`
  - `scripts/phase4_long_soak_ctl.sh status --run-id phase4-1m-long`
  - `scripts/phase4_long_soak_ctl.sh tail --run-id phase4-1m-long --lines 200 --follow true`
  - `scripts/phase4_long_soak_ctl.sh stop --run-id phase4-1m-long`

## 21. Tier-B Rehearsal Wrapper (`>=8` shard gate preset)

- Script:
  - `scripts/phase4_tierb_rehearsal.sh`
- Purpose:
  - run a single command rehearsal for Tier-B defaults with strict `>=8` shard rebalance gate.
  - by default, also runs closure checklist generation via `scripts/phase4_closure_checklist.sh`.
- Preset modes:
  - `quick` (default):
    - profile: `smoke`
    - fixture size: `4000`
    - failover mode: `no-restart`
    - lightweight concurrency knobs
  - `staged`:
    - profile: `xlarge`
    - fixture size: `150000`
    - failover mode: `both`
    - higher concurrency knobs for closer Tier-B rehearsal
  - both modes enforce:
    - rebalance target shards: `8` (default)
    - rebalance min gate: `8` (default)
- Override options include:
  - `--mode quick|staged`
  - `--run-closure-checklist true|false`
  - `--profile`
  - `--fixture-size`
  - `--target-shards`
  - `--min-shards-gate`
  - standard concurrency/failover knobs
- Example:
  - `scripts/phase4_tierb_rehearsal.sh --run-id phase4-tierb-quick --mode quick`
  - `scripts/phase4_tierb_rehearsal.sh --run-id phase4-tierb-staged --mode staged`

## 22. Tier-C Rehearsal Wrapper (recovery + incident + closure)

- Script:
  - `scripts/phase4_tierc_rehearsal.sh`
- Purpose:
  - run a single command Tier-C rehearsal that orchestrates:
    - `phase4_scale_proof` execution with rebalance shard gate
    - recovery drill artifact generation
    - incident simulation gate artifact generation
    - `tier-c` closure checklist evaluation
- Preset modes:
  - `quick` (default):
    - profile: `smoke`
    - fixture size: `6000`
    - failover mode: `both`
    - lightweight concurrency knobs
  - `staged`:
    - profile: `xlarge`
    - fixture size: `150000`
    - failover mode: `both`
    - higher concurrency knobs for closer Tier-C rehearsal
  - both modes enforce:
    - rebalance target shards: `>=8` gate model
    - recovery + incident artifacts when closure checklist is enabled
- Artifact outputs:
  - recovery summary:
    - `docs/benchmarks/history/recovery/<run-id>-recovery-drill.md`
  - incident simulation summary:
    - `docs/benchmarks/history/incidents/<timestamp>-<run-id>-tierc-incident-gate.md`
  - closure checklist:
    - `docs/benchmarks/history/runs/<run-id>-closure-tier-c.md`
- Override options include:
  - `--mode quick|staged`
  - `--run-scale-proof true|false` (development mode: skip expensive scale-proof rerun)
  - `--summary-path <path>` (reuse an existing phase4 summary when `--run-scale-proof=false`)
  - `--run-recovery-drill true|false`
  - `--run-incident-gate true|false`
  - `--run-closure-checklist true|false`
  - `--recovery-artifact PATH` / `--incident-artifact PATH` (for externally produced artifacts)
  - standard profile/shard/concurrency/failover knobs
- Example:
  - `scripts/phase4_tierc_rehearsal.sh --run-id phase4-tierc-quick --mode quick`
  - `scripts/phase4_tierc_rehearsal.sh --run-id phase4-tierc-staged --mode staged`

## 23. Phase 4 Closure Checklist Gate

- Script:
  - `scripts/phase4_closure_checklist.sh`
- Purpose:
  - verify tier-specific artifact/metric gates from a `phase4_scale_proof` summary and emit a reproducible checklist markdown artifact.
- Supported tiers:
  - `tier-a|1m`
  - `tier-b|10m`
  - `tier-c|100m`
- Core checks:
  - summary overall status
  - required step statuses (`benchmark`, concurrency, failover, and rebalance for tier-b/c)
  - ANN recall thresholds (`ann_recall_at_10`, `ann_recall_at_100`)
  - ingestion freshness guard
  - required artifact file existence
  - rebalance shard gate checks for tier-b/c
  - optional recovery/incident artifact checks for tier-c
- Output:
  - default checklist path:
    - `docs/benchmarks/history/runs/<run-id>-closure-<tier>.md`
- Example:
  - `scripts/phase4_closure_checklist.sh --tier tier-b --run-id phase4-tierb-staged`
