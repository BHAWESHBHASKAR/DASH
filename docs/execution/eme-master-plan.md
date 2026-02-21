# DASH Master Plan (EME)

Date: 2026-02-17
Status: active execution baseline

## 1. Mission

Build and ship an evidence-first memory engine for RAG where claims, evidence, provenance, contradiction handling, and temporal validity are first-class retrieval primitives.

## 2. Current State

- Consolidated architecture-and-goals execution spec is available at `docs/execution/dash-full-architecture-and-goals.md`.
- `docs/architecture/eme-architecture.md` exists and defines target architecture.
- Repository scaffolding exists for core services and shared packages.
- Rust workspace implementation is active across schema, ranking, graph, store, ingestion, retrieval, indexer, and metadata-router crates.
- Persistent WAL replay is implemented with snapshot checkpoint compaction in `pkg/store`.
- Retrieval API contracts are implemented with optional graph payload and transport layer support.
- Retrieval transport now includes an authenticated planner-debug endpoint (`GET /debug/planner`) for stage-wise candidate observability.
- Retrieval transport now includes an authenticated storage-visibility endpoint (`GET /debug/storage-visibility`) with segment/WAL divergence warning evaluation and metrics.
- Ingestion API transport is implemented (`POST /v1/ingest`, `GET /health`, `GET /metrics`) with persistent-policy wiring.
- Ingestion batch API transport is implemented (`POST /v1/ingest/batch`) with durable WAL batch commit metadata records, strict atomic rollback semantics, and `commit_id` idempotent replay/conflict enforcement.
- Ingestion now supports leader/follower pull replication v1 via internal replication endpoints (`GET /internal/replication/wal`, `GET /internal/replication/export`) plus optional follower loop (`DASH_INGEST_REPLICATION_SOURCE_URL`).
- Retrieval transport exposes runtime metrics at `/metrics` including DASH latency percentiles and visibility-lag estimates.
- Schema layer includes claim/evidence metadata for citation-grade retrieval (`entities`, `embedding_ids`, `chunk_id`, `span_start`, `span_end`) with backward-compatible WAL replay.
- Ingestion/retrieval transports support configurable HTTP worker pools for concurrent request handling (`DASH_*_HTTP_WORKERS`).
- Ingestion and retrieval transport runtimes (`std` and `axum`) now enforce bounded admission with shared backpressure metrics (`DASH_*_HTTP_QUEUE_CAPACITY`, queue depth/full-reject counters).
- Ingestion/retrieval transports support runtime selection (`std` default, optional `axum`) via `DASH_*_TRANSPORT_RUNTIME` with `EME_*` fallback.
- Ingestion/retrieval transports include tenant-scoped authz policy controls (`DASH_*_ALLOWED_TENANTS`, `DASH_*_API_KEY_SCOPES`) and optional JSONL audit trails (`DASH_*_AUDIT_LOG_PATH`).
- Audit trails are now tamper-evident with chained hashes (`seq`, `prev_hash`, `hash`) and verifiable via `scripts/verify_audit_chain.sh`.
- Ingestion/retrieval auth now supports rotation overlap (`DASH_*_API_KEYS`) and hard revocation (`DASH_*_REVOKED_API_KEYS`) in addition to single-key compatibility envs.
- Ingestion/retrieval transport integration tests now include scoped cross-tenant deny and revoked-key deny coverage.
- Ingestion/retrieval now support JWT bearer auth mode (HS256) with issuer/audience/time-claim checks and tenant claim enforcement.
- JWT auth now supports rotation overlap (`active + fallback`) and optional `kid`-mapped secret selection.
- Ingestion/retrieval transport integration tests now include JWT cross-tenant deny and JWT expiry deny coverage.
- Store now fails closed on cross-tenant `claim_id` collisions to prevent tenant overwrite and replay divergence.
- Retrieval API tests now validate tenant isolation in segment-prefilter allowlists and cache scoping.
- Indexer includes immutable segment lifecycle primitives (atomic segment+manifest persistence, checksum-verified load, compaction scheduler hook).
- Metadata router now includes placement-aware routing primitives (leader/follower health policies, read preferences, and failover promotion with epoch bump).
- Ingestion/retrieval transports now support placement-aware request admission gates (write-leader enforcement on ingest and read-replica preference enforcement on retrieve) using shared placement CSV metadata.
- Ingestion/retrieval transports now expose placement-debug snapshots and route probe context at `GET /debug/placement`.
- Ingestion/retrieval transports support optional in-process placement live reload (`DASH_ROUTER_PLACEMENT_RELOAD_INTERVAL_MS`) with reload observability metrics.
- Ingestion can publish tenant-scoped segment snapshots (`DASH_INGEST_SEGMENT_DIR`) and retrieval can apply segment-backed prefiltering (`DASH_RETRIEVAL_SEGMENT_DIR`).
- Ingestion segment publish now enforces on-disk stale segment GC with one-generation safety retention (`active manifest + previous manifest`) to bound disk growth without breaking in-flight readers.
- Ingestion transport now includes a scheduled in-process segment lifecycle maintenance worker (`DASH_INGEST_SEGMENT_MAINTENANCE_INTERVAL_MS`) with stale-age policy control (`DASH_INGEST_SEGMENT_GC_MIN_STALE_AGE_MS`) and maintenance metrics.
- Standalone segment lifecycle daemon binary (`segment-maintenance-daemon`) is now available for out-of-process maintenance loops and one-shot maintenance verification.
- Retrieval segment read semantics explicitly merge `immutable segment base + mutable WAL delta` before applying metadata prefilters.
- Ingestion WAL durability now supports configurable batching controls with strict defaults:
  - sync batch threshold: `DASH_INGEST_WAL_SYNC_EVERY_RECORDS`
  - append buffer threshold: `DASH_INGEST_WAL_APPEND_BUFFER_RECORDS`
  - optional interval-triggered flush policy: `DASH_INGEST_WAL_SYNC_INTERVAL_MS`
  - async flush worker: `DASH_INGEST_WAL_ASYNC_FLUSH_INTERVAL_MS` (auto-enabled when batching is active)
  - optional queue-mode write path: `DASH_INGEST_WAL_BACKGROUND_FLUSH_ONLY=true` (flush/sync deferred to async worker)
  - startup safety guardrails reject unsafe durability windows unless explicitly overridden via `DASH_INGEST_ALLOW_UNSAFE_WAL_DURABILITY=true`
- Benchmark harness supports smoke, standard, and large fixture profiles with history output.
- Benchmark trend automation is available via `scripts/benchmark_trend.sh` (smoke + large guard/history/scorecard outputs).
- Phase 4 scale-proof automation lane is available via `scripts/phase4_scale_proof.sh` (scaled fixture benchmark + retrieval/ingestion concurrency + failover drill with consolidated summary artifact).
- Phase 4 long-soak launcher is available via `scripts/phase4_long_soak.sh` (deterministic run-id artifact paths + heartbeat telemetry for extended `1M+` runs).
- Phase 4 long-soak control wrapper is available via `scripts/phase4_long_soak_ctl.sh` (`start|status|tail|stop|run` workflow with pid/log/heartbeat management).
- Rebalance/split drill automation is available via `scripts/rebalance_drill.sh` (placement split + epoch transition verification with before/after route snapshots and probe-diff artifacts).
- Tier-B rehearsal wrapper is available via `scripts/phase4_tierb_rehearsal.sh` (one-command `>=8` shard rebalance gate preset over phase4 scale-proof runner).
- Tier-C rehearsal wrapper is available via `scripts/phase4_tierc_rehearsal.sh` (phase4 scale-proof + recovery drill + incident simulation + tier-c closure checklist orchestration, with optional dev-mode reuse of existing scale-proof summaries).
- Phase 4 closure checklist gate is available via `scripts/phase4_closure_checklist.sh` (tier-aware PASS/FAIL checklist from scale-proof summary artifacts).
- Placement failover validation script supports restart and no-restart drills via `scripts/failover_drill.sh --mode restart|no-restart|both`.
- Transport concurrency benchmark tooling is available via `scripts/benchmark_transport_concurrency.sh` and `concurrent_load`.
- Ingestion WAL durability benchmark comparison tooling is available via `scripts/benchmark_ingest_wal_durability.sh` with history artifacts under `docs/benchmarks/history/concurrency/wal-durability/`.
- CI supports optional staged trend execution via `DASH_CI_RUN_BENCH_TREND=true`.
- SLO/error-budget gate automation is available via `scripts/slo_guard.sh` (benchmark SLI checks + rolling failed-run budget + optional recovery drill signal).
- Consolidated release-candidate gate automation is available via `scripts/release_candidate_gate.sh` (fmt/clippy/tests/ci + SLO + recovery + ingestion throughput floor + optional audit-chain and benchmark trend gates).
- Incident simulation gate automation is available via `scripts/incident_simulation_gate.sh` (failover + auth revocation + recovery drills) with `scripts/auth_revocation_drill.sh` for focused auth-failure drills.
- CI pipeline (`scripts/ci.sh`) runs fmt, clippy, workspace tests, and smoke benchmark.
- Production packaging and operations artifacts are available:
  - `docs/execution/dash-production-runbook.md`
  - `docs/execution/dash-startup-env-matrix.md`
  - `docs/execution/dash-release-checklist.md`
  - `scripts/package_release.sh`
  - `scripts/benchmark_trend.sh`
  - `scripts/deploy_systemd.sh`
  - `scripts/deploy_container.sh`
  - `deploy/systemd/dash-segment-maintenance.service`
  - `deploy/systemd/segment-maintenance.env.example`
  - `scripts/backup_state_bundle.sh`
  - `scripts/restore_state_bundle.sh`
  - `scripts/recovery_drill.sh`
  - `scripts/auth_revocation_drill.sh`
  - `scripts/incident_simulation_gate.sh`
  - `scripts/security_signoff_gate.sh`
  - `scripts/slo_guard.sh`
  - `scripts/verify_audit_chain.sh`
  - `scripts/release_candidate_gate.sh`
  - `docs/execution/dash-security-threat-model-signoff.md`

### 2.1 Milestone Status (2026-02-19)

- Phase 0 (Vertical Slice): complete
- Phase 1 (Evidence Graph): complete
  - contradiction-aware retrieval behavior is enforced
  - graph expansion is bounded in retrieval graph payload assembly
  - contradiction detection F1 probe gate is enforced in benchmark quality probes (`>= 0.80`)
- Phase 2 (Scale Path): complete
  - shard routing/placement admission is active for ingest + retrieve with epoch-aware route probes
  - failover drill validates routing epoch switch in restart and no-restart modes (`scripts/failover_drill.sh --mode both`)
  - immutable segment publish + retrieval prefilter merge path is implemented with `segment base + WAL delta` semantics
  - xlarge benchmark guard path is validated (`100,000` claims profile) and recorded in benchmark history
  - sustained-load ingest freshness proxy is validated via concurrency benchmark (`latency_p95_ms` well below `5,000 ms`)
- Phase 3 (Production Hardening): complete
  - tenant isolation, authn/authz, JWT rotation/revocation, and tamper-evident audit trails are implemented with regression coverage
  - incident simulation gate and release-candidate sign-off wrapper are implemented and passing
- Phase 4 (Scale Proof): in progress
  - objective is to close remaining world-scale proof gap (`1M -> 10M -> 100M`) with reproducible performance and freshness evidence

### 2.2 Detailed Phase-Doc Closure Snapshot (2026-02-21)

- Detailed phase document `phase-04-retrieval-semantics-and-citations.md`: complete (exit-gate evidence mapped to existing retrieval tests plus Tier-B staged artifacts).
- Detailed phase document `phase-05-hybrid-candidate-planner.md`: complete (exit-gate evidence mapped to existing planner-stage metrics, recall gates, and Tier-B/Tier-C closure artifacts).
- Scope note:
  - this closure snapshot applies to the detailed retrieval/planner phase-doc set.
  - master-plan Phase 4 scale-proof (`1M -> 10M -> 100M`) remains in progress and is intentionally not promoted here.

### 2.3 Detailed Phase 06 Closure Snapshot (2026-02-21)

- Detailed phase document `phase-06-segment-lifecycle-and-merge-semantics.md`: complete.
- Closure evidence highlights:
  - merge semantics remain `segment base UNION WAL delta` in retrieval planner path.
  - replay-only and segment-assisted logical read-set equivalence is covered by retrieval tests (`execute_api_query_segment_assisted_matches_replay_only_logical_set`).
  - stale/missing/invalid segment states safely fall back without data loss, with explicit reason coverage in tests.
  - runtime observability now includes segment fallback activation counters and reason-classified counters in retrieval `/metrics`.
- Scope note:
  - this snapshot closes the detailed Phase 06 lifecycle hardening track.
  - master-plan Phase 4 scale-proof (`1M -> 10M -> 100M`) remains in progress.

## 3. Success Targets (v1)

- retrieval API returns claim-level results with machine-readable citations.
- contradiction-aware mode surfaces supporting and conflicting evidence.
- ingest-to-visible freshness <= 5 seconds in single-region deployment.
- p95 latency <= 350 ms for top-50 retrieval under benchmark profile.
- benchmark scorecard shows quality lift over dense-only baseline.

## 4. Scope and Non-Goals

In scope:

- single-region production candidate with shard-ready architecture
- claim/evidence/edge schema and index lifecycle
- retrieval planner with hybrid candidate generation and graph-aware ranking
- evaluation protocol and repeatable benchmark reporting

Out of scope in v1:

- cross-region active-active failover
- generalized OLAP analytics for arbitrary graph queries
- end-user document editing or source authoring features

## 5. Workstreams

- `WS1 Ingestion`: source intake, chunking, extraction orchestration, WAL append
- `WS2 Data and Index`: schemas, delta indexes, compaction, segment management
- `WS3 Retrieval`: query decomposition, candidate generation, graph assembly, ranking
- `WS4 Platform`: routing, tenancy, security, observability, deployment
- `WS5 Evaluation`: datasets, benchmark harness, scorecards, regression gates

## 6. Phased Delivery Plan

### Phase 0: Vertical Slice

Duration:

- 2 weeks

Objective:

- prove end-to-end feasibility on one node with a fixed dataset

Deliverables:

- source ingest endpoint
- claim/evidence persistence via WAL + mutable store
- retrieval endpoint returning top-k claims with provenance
- integration test: ingest -> retrieve -> citation verification
- base dashboard for latency, freshness lag, and ingest errors

Exit criteria:

- end-to-end test stable for 1M claims
- citation coverage >= 0.95 on seed validation queries
- p95 latency <= 450 ms on baseline hardware

### Phase 1: Evidence Graph

Duration:

- 3 weeks

Objective:

- add evidence reasoning signals beyond similarity-only retrieval

Deliverables:

- support/contradiction edge builder
- graph expansion with bounded depth
- composite ranking with temporal and source quality features
- contradiction-aware output mode (`support_only`, `balanced`)
- evaluation set focused on conflicting/stale claims

Exit criteria:

- contradiction detection F1 >= 0.80 on adversarial set
- quality lift over dense-only baseline at equal latency budget
- no regression in citation payload integrity

### Phase 2: Scale Path

Duration:

- 4 weeks

Objective:

- transition from single-node vertical slice to shard-capable deployment path

Deliverables:

- immutable segment compaction pipeline
- tiered read path (memory + local disk + object store)
- shard routing service and placement metadata
- replication support (leader + follower model)
- failover drill documentation

Exit criteria:

- xlarge profile (`100,000` claims) passes scale guardrails and history guard in staged runs
- ingest freshness <= 5 seconds under sustained load
- successful shard split and routing epoch switch in staging

### Phase 3: Production Hardening

Duration:

- 3 weeks

Objective:

- production readiness for first tenant cohort

Deliverables:

- authn/authz integration and per-tenant isolation tests
- audit trail for claim and evidence mutations
- on-call runbooks, dashboards, alert policy, rollback playbook
- backup/restore operational scripts and timed recovery drill procedure
- SLO policy and error budget tracking
- security review and threat model sign-off

Exit criteria:

- production readiness checklist complete
- incident response simulation completed
- zero critical gaps from security review

### Phase 4: Scale Proof

Duration:

- 4 weeks

Objective:

- prove world-scale performance and operational behavior beyond Phase 2 shard-ready capability

Deliverables:

- multi-volume benchmark tracks (`1M`, `10M`, `100M`) with repeatable run protocol
- explicit p95/p99 latency and ingest-freshness trend reporting at scale
- scale-path capacity model (resource envelope, throughput envelope, replay/recovery envelope)
- shard split/rebalance rehearsal with epoch transition audit artifacts

Exit criteria:

- load test with `100M` claims meets p95 <= `350 ms` for target retrieval query class
- ingest freshness <= `5 seconds` under sustained load at scale profile
- successful shard split/rebalance and routing epoch transition validated in staged environment

Distributed gate matrix (`1M` -> `10M` -> `100M`):

- `1M` gate (single-region scale baseline)
  - retrieval p95 <= `350 ms`, p99 <= `500 ms` on target query class
  - ingest freshness proxy p95 <= `5,000 ms`
  - benchmark top1 correctness expected-hit remains `true`
  - ANN recall@10 >= `0.95`, ANN recall@100 >= `0.98`
  - artifacts:
    - benchmark scorecard
    - phase4 run summary
    - benchmark-history row
- `10M` gate (multi-shard staged rehearsal)
  - placement with `>= 8` shards and replica-aware read/write routing
  - shard split/rebalance drill with epoch transition audit artifacts
  - retrieval p95 <= `350 ms`, p99 <= `600 ms` across routed shard traffic
  - ingest freshness p95 <= `5,000 ms` under sustained load
  - no cross-tenant leakage under routed retrieval and ingest tests
  - artifacts:
    - placement snapshot before/after rebalance
    - failover/rebalance drill summary
    - benchmark + concurrency summaries with routed workload tags
- `100M` gate (world-scale proof)
  - distributed staged run across multiple placement domains (shard groups + followers)
  - retrieval p95 <= `350 ms`, p99 <= `700 ms` for target query class
  - ingest freshness p95 <= `5,000 ms` during sustained ingest + background maintenance
  - WAL replay/recovery rehearsal:
    - replay + restore time objective evidence captured
    - post-recovery correctness probe must pass (expected top1 + citation integrity)
  - incident simulation bundle passes with scale placement active:
    - failover drill
    - auth revocation drill
    - recovery drill
  - artifacts:
    - full run summary bundle
    - scale capacity model revision
    - phase closure checklist signed off

## 7. Dependency Map

- Phase 1 depends on Phase 0 stable schema contracts.
- Phase 2 depends on Phase 1 ranking and edge schema stability.
- Phase 3 depends on Phase 2 deployment pipeline and reliability signals.
- Phase 4 depends on Phase 3 production-hardening controls and release gates.
- Benchmark quality gates run continuously from Phase 0 onward.

## 8. Risk Register

Risk:

- extraction quality drift

Impact:

- low-confidence claims degrade ranking quality

Likelihood:

- medium

Mitigation:

- model-version pinning, weekly calibration check, drift alarms

Trigger signal:

- provenance precision drops > 5% week-over-week

Risk:

- graph explosion from noisy edge creation

Impact:

- query latency and memory usage spikes

Likelihood:

- high

Mitigation:

- edge confidence thresholding, degree caps, compaction pruning

Trigger signal:

- average node degree exceeds policy threshold

Risk:

- latency regression during scale transition

Impact:

- SLO breaches and poor user experience

Likelihood:

- medium

Mitigation:

- bounded graph depth, adaptive top-k, cache tuning

Trigger signal:

- p95 latency exceeds 350 ms for two consecutive benchmark runs

Risk:

- tenant data leakage

Impact:

- severe security/compliance event

Likelihood:

- low

Mitigation:

- tenant predicate enforcement at API/index/cache layers, isolation tests

Trigger signal:

- any cross-tenant read in integration/staging tests

## 9. Governance Cadence

- daily implementation sync for active phase
- weekly architecture + risk review
- bi-weekly benchmark checkpoint with trend report
- release gate review before each phase transition

## 10. Definition of Done (Program Level)

- architecture, plan, and benchmark artifacts remain aligned
- each phase exit criteria is met with reproducible evidence
- benchmark report includes quality, latency, and cost dimensions
- operations and security controls are tested, not just documented

## 11. Immediate Next Sprint (10 Working Days)

Phase 4 kickoff scope:

1. Scale benchmark lane expansion (Days 1-4)
   - Add dedicated scale-proof run lane for `xlarge` + next-volume profile.
   - Emit p95/p99 latency and freshness-focused summary rows.
2. Freshness stress and ingest throughput (Days 4-7)
   - Standardize sustained-load ingest freshness checks with fixed workload profiles.
   - Record pass/fail thresholds and trend artifacts under benchmark history runs.
3. Rebalance and epoch transition evidence (Days 7-10)
   - Capture staged shard split/rebalance drill artifacts with explicit epoch-transition checkpoints.
   - Publish closure checklist for Phase 4 exit evidence collection.

Exit criteria for sprint:

- Phase 4 scale-proof lane is runnable in one command and emits reproducible artifacts.
- At least one post-xlarge scale profile has baseline evidence committed.
- Freshness and routing-transition evidence is attached to progress logs with exact command outputs.
