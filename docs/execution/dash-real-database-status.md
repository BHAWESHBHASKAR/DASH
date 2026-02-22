# DASH Real Database Status

Date: 2026-02-22
Status: active

## 1. Objective

Define where DASH stands as a real production database platform, what is complete, what remains, and what is mandatory before production-grade rollout.

## 2. Readiness Snapshot

- Engine core readiness: 65%
- Distributed database readiness: 40%
- Production operations readiness: 70%

These percentages are directional and tied to implemented code paths, tests, and run artifacts in this repository.

## 3. Implemented Today

### 3.1 Data and durability

- Evidence-first schema and validation for claim/evidence/edge primitives.
- WAL append + replay durability path.
- Snapshot checkpoint compaction and snapshot+delta recovery path.
- Explicit replay-compaction boundary observability (`snapshot_record_count` vs `wal_delta_record_count`) with transition regression coverage.
- Backward-compatible WAL record evolution for metadata and vectors.
- Fail-closed tenant safety guard for cross-tenant `claim_id` collisions.

### 3.2 Retrieval engine

- Hybrid retrieval across lexical + vector + metadata + temporal constraints.
- Stance modes (`balanced`, `support_only`) and citation-grade outputs.
- Benchmark quality probes now gate both contradiction detection F1 and citation coverage (`>= 0.95`).
- Optional graph payload in retrieval contracts.
- Graph reasoning v2 node signals (`graph_score`, `support_path_count`, `contradiction_chain_depth`) with configurable depth/weight controls in retrieval graph responses.
- Retrieval planner debug endpoint (`GET /debug/planner`) exposes stage-wise candidate counts for metadata prefilter, segment base, WAL delta, ANN, and final planner candidate set.
- Retrieval storage-visibility endpoint (`GET /debug/storage-visibility`) exposes both planner-level segment/WAL merge counts and execution-time result-source attribution (`segment_base`, `wal_delta`, `unknown`, `outside_storage_visible`) plus divergence-warning state.
- Retrieval now records and exports execution mode (`segment_disk_base_with_wal_overlay` vs `memory_index`) and execution candidate counts, with disk-native segment-base execution active when segment prefilter state is available.
- Retrieval now exposes explicit promotion-boundary state (`replay_only`, `segment_base_plus_wal_delta`, `segment_base_fully_promoted`) with transition flagging and per-state counters.
- Placement-routed retrieval supports explicit read consistency admission (`read_consistency=one|quorum|all`).
- Segment prefilter cache with runtime refresh controls and observability metrics.
- Tenant-isolation coverage for retrieval segment prefilter/cache paths.

### 3.3 Services and transport

- Ingestion and retrieval HTTP transports with health and metrics endpoints.
- Raw-text ingestion bootstrap endpoint (`POST /v1/ingest/raw`) with deterministic sentence-based extraction into claim/evidence records.
- Raw extraction now supports env-selectable provider adapters (`rule_sentence` default, optional `adapter_command` via feature gate).
- Ingestion batch write API (`POST /v1/ingest/batch`) with durable WAL commit metadata records.
- Batch writes now enforce strict atomic rollback semantics (store state and WAL are rolled back on batch failure).
- Batch `commit_id` retry semantics are now explicit: same payload replays idempotently, mismatched payload returns conflict.
- Runtime policy controls for auth, worker pools, and transport mode selection.
- Startup replay and replay-stat logging for persistent mode.
- JWT bearer auth mode (HS256) with claim validation and tenant-scope enforcement.
- JWT key rotation support (`active + fallback` secrets and optional `kid` secret maps).
- Tamper-evident audit log chaining (`seq`, `prev_hash`, `hash`) with offline verifier script.
- Ingestion replication source/sink v1:
  - source endpoints: `GET /internal/replication/wal`, `GET /internal/replication/export`
  - follower pull loop with WAL-delta apply and full export resync fallback.

### 3.4 Index and scale path primitives

- Segment persistence + manifest checksum verification.
- Segment compaction scheduler primitives.
- Segment publish now prunes stale `.seg` files with one-generation safety retention (`active + previous` manifest files).
- Ingestion transport now runs scheduled in-process segment maintenance (manifest verification + stale-file GC by minimum age policy).
- Standalone segment lifecycle daemon binary (`segment-maintenance-daemon`) is available for out-of-process maintenance loops and one-shot verification ticks.
- Benchmark profiles through xxlarge (1M claims).
- Tier-D rehearsal wrapper (`scripts/phase4_tierd_rehearsal.sh`) adds post-xlarge orchestration defaults for repeatable `1M` phase-4 evidence runs.
- Tier-D quick closure evidence is captured via `phase4-next-tierd-quick` run artifacts (scale summary + tier-c closure checklist PASS).
- Tier-D staged-lite closure evidence is captured via `phase4-next-tierd-staged-lite` (`xxlarge`, `200k`, `>=12` shard gate, tier-c closure PASS).
- Benchmark history and scorecard outputs with regression guardrails.
- Backup/restore/recovery drill scripts for WAL+snapshot+segment operations.
- SLO/error-budget gate script with rolling pass/fail window and optional recovery drill signal.
- Follower replication lag guard script (`scripts/replication_lag_guard.sh`) checks claim-count lag and replication-error/failure SLOs from leader/follower metrics.
- Storage promotion-boundary guard script (`scripts/storage_promotion_boundary_guard.sh`) validates replay-only, segment+WAL-delta, and fully-promoted boundary states under retrieval load via debug/metrics counters.
- Phase-4 scale proof now embeds promotion-boundary gating with rolling regression thresholds (`scripts/phase4_scale_proof.sh` + `storage-promotion-boundary-history.csv`), and tier-b/tier-c/tier-d wrappers enable this by default in staged mode.
- Phase 11 distributed staging gate wrapper (`scripts/phase11_core_staging_gate.sh`) standardizes release-candidate gating for core-production rehearsals.
- Failover chaos-loop gate (`scripts/failover_chaos_loop.sh`) adds repeated failover convergence checks for route-stability evidence.
- Retrieval latency envelope guard (`scripts/retrieval_latency_guard.sh`) adds staged p95/p99/success-rate gating with markdown artifacts.
- Read-consistency policy guard (`scripts/read_consistency_policy_guard.sh`) adds policy-specific PASS/FAIL evidence artifacts.
- Capacity model report generator (`scripts/capacity_model_report.sh`) produces throughput/latency/replay envelopes and first-billion sizing guidance from staged evidence.
- Phase 11 production signoff bundle (`scripts/phase11_production_signoff_bundle.sh`) enforces required incident/recovery/lag/throughput/SLO artifact presence and links promotion+rollback rehearsal artifacts in one PASS/FAIL summary.

## 4. Gaps to Production Database

### 4.1 Core storage/runtime gaps

- No fully tier-complete disk-native serving path (segment-disk-base execution with WAL overlay is active, but fallback and lifecycle hardening are still required for full disk-first serving).
- Standalone segment maintenance now has basic systemd/container wiring; higher-level orchestration (k8s/operator automation and policy-driven lifecycle management) is still incomplete.
- Single-node batch idempotency/retry semantics are implemented; remaining gap is cross-replica idempotency orchestration in distributed deployments.

### 4.2 Distributed system gaps

- No dedicated dynamic placement control-plane service (current routing uses file-backed placement metadata with optional live reload).
- Pull-based replica replication stream exists for ingestion v1; remaining gap is multi-replica ack/quorum replication protocol (lag SLO guard now exists, but quorum protocol is still incomplete).
- No automatic leader election/failover runtime across shard replicas.

### 4.3 Operational and safety gaps

- Backup/restore workflow is codified in scripts and runbook, but scheduled recurring recovery drills are not yet automated in staging/prod.
- Security threat model document and incident simulation gate now exist, but formal sign-off and staged recurring execution are still pending.
- SLO/error-budget gate automation exists, but live-service telemetry and alert-driven budget burn integration are not yet complete.

## 5. Must-Have Before Production-Candidate DB

- Shard placement metadata service with routing-epoch semantics.
- Leader/follower replication path with bounded divergence policy.
- Failover orchestration with deterministic epoch bump and write re-targeting.
- Disk-first segment serving tier and lifecycle GC.
- Backup/restore with measured RTO/RPO and repeatable drill procedure.
- Tenant isolation verification suite across all cache/index/transport layers.
- Release gates that enforce latency, evidence quality, and durability invariants.

## 6. Immediate Implementation Priority

P0 should focus on shard/replica control-plane maturity:

1. Placement metadata model and health-aware routing primitives.
2. Failover promotion path with epoch increments.
3. Integration wiring into ingestion/retrieval service startup and route resolution.
4. Benchmark/CI probes that fail on missing placement health invariants.

## 7. Acceptance Criteria for P0

- Placement table supports leader/follower state for each shard.
- Write route fails closed when no healthy leader is present.
- Read route supports explicit preference policy (leader-only, follower-preferred, any-healthy).
- Failover promotion updates leadership and increments epoch deterministically.
- Unit tests cover routing and failover edge cases.
