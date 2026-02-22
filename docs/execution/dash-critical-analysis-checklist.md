# DASH Critical Analysis Checklist

Date: 2026-02-22
Status: active
Source review: `/Users/bhaweshbhaskar/Downloads/dash_critical_analysis.md.resolved`

## 1. Purpose

Track which critical-analysis findings are already addressed in the current codebase, which are partially addressed, and which remain open work.

## 2. Resolved Findings

- [x] Planner and placement debug visibility
  - Implemented endpoints:
    - `GET /debug/placement`
    - `GET /debug/planner`
    - `GET /debug/storage-visibility`
  - Current location: `services/retrieval/src/transport.rs`

- [x] Segment + WAL merge semantics clarity
  - Read semantics are explicit: `immutable segment base + mutable WAL delta`.
  - Current references:
    - `docs/execution/eme-master-plan.md`
    - `services/retrieval/src/api.rs` tests for stale-manifest fallback and replay equivalence.

- [x] Hybrid planner reporting clarity in benchmark history
  - History includes:
    - `metadata_prefilter_count`
    - `ann_candidate_count`
    - `final_scored_candidate_count`
  - Current location: `tests/benchmarks/src/main.rs`

- [x] ANN recall quality gates
  - Benchmark now tracks and gates:
    - `ann_recall_at_10`
    - `ann_recall_at_100`
    - recall curve by budget
  - Current location: `tests/benchmarks/src/main.rs`

- [x] Security baseline beyond a single static API key
  - Added scoped keys, tenant allowlists, JWT mode, key rotation overlap, and audit chain verifier.
  - Current locations:
    - `pkg/auth/src/lib.rs`
    - `services/ingestion/src/transport/authz.rs`
    - `services/retrieval/src/transport.rs`
    - `scripts/verify_audit_chain.sh`

- [x] Raw-document extraction bootstrap path
  - Implemented transport endpoint:
    - `POST /v1/ingest/raw`
  - Deterministic sentence-based extraction now converts raw text into claim/evidence records with citation spans.
  - Current locations:
    - `services/ingestion/src/transport/ingest_routes.rs`
    - `services/ingestion/src/extraction.rs`

- [x] Extraction quality probes in benchmark quality gates
  - Quality probes now enforce:
    - extracted claim count floor
    - deterministic commit-id replay stability
    - citation span/doc coverage for extracted claims
  - Current location: `tests/benchmarks/src/main.rs`

## 3. Partially Addressed Findings

- [~] Async transport migration
  - `axum + tokio` runtime exists behind `async-transport` feature flag.
  - Default runtime is still std transport; parity is functional but not fully defaulted.
  - Next action: evaluate flipping default runtime for staged/prod profiles after canary evidence.

- [~] ANN maturity
  - Moved beyond bucket LSH toward a graph-style ANN path in store.
  - Still not external HNSW/DiskANN-class library and needs high-scale recall/latency evidence at higher fixtures.
  - Next action: complete larger recall/latency envelopes in phase-4/phase-11 staged runs.

- [~] Service modularity
  - Ingestion transport has been split into submodules.
  - Retrieval transport payload/parsing/json serialization moved into `services/retrieval/src/transport/payload.rs`.
  - Retrieval API result-projection logic moved into `services/retrieval/src/api/result_projection.rs`.
  - Next action: continue splitting retrieval routing/auth/audit into focused modules.

- [~] Model-extraction plugin path
  - Raw ingest extraction now supports provider selection via env:
    - `DASH_INGEST_RAW_EXTRACTION_PROVIDER=rule_sentence|adapter_command`
    - `DASH_INGEST_RAW_ADAPTER_CMD` for command-based model adapter.
  - Adapter path is feature-gated behind `model-extraction-adapter`.
  - Next action: ship provider contract docs and staged adapter harness.

- [~] Graph reasoning depth
  - Graph reasoning v2 is implemented with configurable weighting and depth controls in retrieval graph payloads:
    - node-level `graph_score`
    - `support_path_count`
    - `contradiction_chain_depth`
  - Graph quality probes now validate these signals in benchmark quality gates, and large+ profile gates enforce coverage/path/depth thresholds.
  - Current locations:
    - `pkg/graph/src/lib.rs`
    - `services/retrieval/src/api.rs`
    - `tests/benchmarks/src/main.rs`
  - Next action: complete staged evidence at deeper fixtures to harden depth/weight envelopes under phase-4/phase-11 scale runs.

## 4. Open Findings (Execution Priority)

- [~] Graph reasoning v2 at larger depth and scale
  - Gap: v2 weighting + depth controls and large-profile graph gates are implemented, but staged deep-scale evidence is still pending.
  - Next action: run and archive phase-4/phase-11 graph-depth evidence runs with explicit acceptance envelopes.

- [~] Disk-native serving path
  - Gap: retrieval can now execute from segment-disk-base candidate sets, but fallback remains memory-index based when segment base is inactive and deeper tiered disk-serving hardening remains incomplete.
  - Progress: retrieval now emits explicit execution-time merge accounting for `segment base + WAL delta` (result source attribution and out-of-visibility detection), records execution mode (`segment_disk_base_with_wal_overlay` vs `memory_index`) with per-mode counters, and now exposes explicit promotion-boundary state (`replay_only`, `segment_base_plus_wal_delta`, `segment_base_fully_promoted`) plus transition flagging in debug/metrics surfaces.
  - Progress: store now exposes explicit replay-compaction boundary counters (`snapshot_record_count` vs `wal_delta_record_count`) and regression tests cover checkpoint -> delta -> checkpoint transitions.
  - Progress: staged release gates now include an optional under-load promotion-boundary guard (`scripts/storage_promotion_boundary_guard.sh`) and phase-11 orchestration forwards this gate with staged defaults.
  - Progress: phase-4 scale proof now supports an integrated promotion-boundary guard plus rolling trend thresholds for state-counter regression, and tier-b/tier-c/tier-d wrappers now forward this gate with staged defaults.
  - Current locations:
    - `services/retrieval/src/api.rs`
    - `services/retrieval/src/transport.rs`
    - `pkg/store/src/lib.rs`
    - `scripts/storage_promotion_boundary_guard.sh`
    - `scripts/phase4_scale_proof.sh`
    - `scripts/phase4_tierb_rehearsal.sh`
    - `scripts/phase4_tierc_rehearsal.sh`
    - `scripts/phase4_tierd_rehearsal.sh`
    - `scripts/release_candidate_gate.sh`
    - `scripts/phase11_core_staging_gate.sh`
  - Next action: set staged minimum-sample policy for promotion-boundary trend windows and wire hard PASS/FAIL checks into closure checklist scoring.

- [ ] Automatic leader election/failover runtime
  - Gap: placement and failover drills exist, but automatic election runtime remains incomplete.
  - Next action: implement deterministic leader-election state transitions and control-plane ownership.

## 5. Current Execution Focus

1. Keep phase-11 signoff and evidence gates passing (`scripts/ci.sh` plus staged wrappers).
2. Continue retrieval modularization without behavior regression.
3. Deepen graph reasoning and disk-native serving path.
