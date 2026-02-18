# DASH Real Database Status

Date: 2026-02-18
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
- Backward-compatible WAL record evolution for metadata and vectors.

### 3.2 Retrieval engine

- Hybrid retrieval across lexical + vector + metadata + temporal constraints.
- Stance modes (`balanced`, `support_only`) and citation-grade outputs.
- Optional graph payload in retrieval contracts.
- Segment prefilter cache with runtime refresh controls and observability metrics.

### 3.3 Services and transport

- Ingestion and retrieval HTTP transports with health and metrics endpoints.
- Runtime policy controls for auth, worker pools, and transport mode selection.
- Startup replay and replay-stat logging for persistent mode.
- JWT bearer auth mode (HS256) with claim validation and tenant-scope enforcement.

### 3.4 Index and scale path primitives

- Segment persistence + manifest checksum verification.
- Segment compaction scheduler primitives.
- Benchmark profiles through xlarge (100k claims).
- Benchmark history and scorecard outputs with regression guardrails.
- Backup/restore/recovery drill scripts for WAL+snapshot+segment operations.
- SLO/error-budget gate script with rolling pass/fail window and optional recovery drill signal.

## 4. Gaps to Production Database

### 4.1 Core storage/runtime gaps

- No fully disk-native query serving path (current serving remains memory-centric with WAL/snapshot recovery).
- No background segment lifecycle daemon with retention and garbage collection policy enforcement.
- No transactional write batch API with durable commit metadata.

### 4.2 Distributed system gaps

- No dedicated dynamic placement control-plane service (current routing uses file-backed placement metadata with optional live reload).
- No replica replication stream and follower catch-up protocol.
- No automatic leader election/failover runtime across shard replicas.

### 4.3 Operational and safety gaps

- Backup/restore workflow is codified in scripts and runbook, but scheduled recurring recovery drills are not yet automated in staging/prod.
- Security threat model execution and hardening validation remain incomplete.
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
