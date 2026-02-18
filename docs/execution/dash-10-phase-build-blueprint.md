# DASH 10-Phase Build Blueprint (Detailed)

Date: 2026-02-18  
Status: active execution blueprint  
Owner: DASH core engineering

## 1. Full Build Idea (End-State Vision)

DASH is not a generic vector database.  
DASH is an evidence memory engine for RAG where the system of record is:

- claims
- evidence
- claim-to-claim edges
- provenance spans/citations
- contradiction signals
- temporal validity windows

Vectors are important, but only one ranking signal in a multi-signal retrieval planner.

### 1.1 Product promise

For every retrieval result, DASH must provide:

- why this claim was selected
- what source evidence supports or contradicts it
- where the source text came from (citation metadata)
- whether it is temporally valid for the query context

### 1.2 End-state architecture summary

1. Ingestion extracts/normalizes claim/evidence objects.
2. Durable write path appends to WAL with replayable schema evolution.
3. Mutable delta indexes expose near-real-time query visibility.
4. Background compaction publishes immutable segments with integrity checks.
5. Retrieval planner merges segment base + WAL delta and produces candidates from:
   - ANN dense signal
   - sparse lexical signal
   - entity/metadata filters
   - temporal filters
6. Graph-aware ranking scores support + contradiction context.
7. API returns citation-grade, machine-readable outputs with stance controls (`balanced`, `support_only`).
8. Router enforces shard placement, read/write roles, and failover behavior.
9. Security/tenant policy, auditability, and operations gates harden deployment.
10. Benchmarks + CI + release gates keep quality/performance/regression risk controlled.

### 1.3 What “production-ready” means for DASH

- correctness:
  - no retrieval response without citations
  - contradiction + temporal behavior validated by tests/benchmarks
- reliability:
  - WAL replay and checkpoint/snapshot recovery proven under failure drills
- scale:
  - query and ingest throughput can be tuned with explicit durability/latency tradeoffs
- operability:
  - health/metrics/debug endpoints and reproducible runbooks
- security:
  - tenant isolation, scoped auth, auditable access/mutation trails

## 2. Current Completion Snapshot (as of 2026-02-18)

### 2.1 Already implemented

- Rust workspace + service/package boundaries are active.
- WAL durability + replay + checkpoint compaction are implemented.
- Ingestion and retrieval HTTP transports are implemented.
- Retrieval API contracts include citation fields and optional graph payload.
- Segment publish/read prefilter path exists with manifest + checksum validation.
- Placement-aware routing primitives and failover drill tooling exist.
- Benchmark harness, history guard, trend tooling, and transport concurrency benchmarking exist.
- New WAL durability benchmarking exists (strict vs grouped vs buffered modes).

### 2.2 Still needed for full end-state

- stronger sparse retrieval quality path and planner transparency at scale
- full live segment lifecycle maturity (promotion cadence, object-store tier operations)
- no-restart placement live reload path
- security hardening beyond baseline API keys (rotation/federation/more tests)
- stronger ingestion throughput path with controlled durability policy guardrails
- production SLO and incident automation depth (beyond current baseline runbooks)

## 3. 10-Phase Detailed Plan

## Phase 1: Domain Contract Lock and Governance

### Objective

Freeze stable semantics for `Claim`, `Evidence`, `ClaimEdge`, citations, stance behavior, and temporal filtering so all later scale work preserves evidence correctness.

### Build scope

- formal contract docs for:
  - claim/evidence/edge canonical fields
  - citation payload requirements
  - contradiction semantics
  - temporal inclusion/exclusion rules
- versioned schema evolution policy (backward/forward compatibility)

### Deliverables

- contract spec doc with examples and invalid-case matrix
- compatibility policy for WAL/snapshot record versions
- required invariants checklist integrated into PR review

### Exit gates

- invariant tests pass for schema validation and retrieval behaviors
- every retrieval mode has golden input/output tests

### Status

- mostly complete; continue tightening contract tests as new features are added.

## Phase 2: Ingestion Pipeline Reliability

### Objective

Ensure source intake -> normalization -> durable append remains deterministic, replay-safe, and tenant-safe under load.

### Build scope

- ingestion payload validation and strict error surfaces
- optional extraction hooks and normalization controls
- write admission controls (tenant policy + placement leader checks)

### Deliverables

- robust `POST /v1/ingest` behavior for success/retryable/fatal categories
- ingestion metrics with reason-coded failures
- integration tests for malformed payloads and policy denial paths

### Exit gates

- zero silent partial writes
- deterministic replay for accepted writes
- ingest failure reason visibility in metrics/logs

### Status

- implemented baseline transport and policy flow; continue expanding extraction + ingestion semantics depth.

## Phase 3: Storage Core (WAL, Replay, Checkpoint, Compaction)

### Objective

Maintain crash-safe durability while enabling scalable replay and bounded recovery times.

### Build scope

- WAL append + fsync policy controls
- replay from snapshot + WAL delta
- checkpoint compaction triggers (record count/bytes)
- schema evolution compatibility in replay path

### Deliverables

- `FileWal` with configurable durability policies
- snapshot/truncate compaction path
- replay stats visibility and startup instrumentation

### Exit gates

- replay correctness tests for legacy + current record shapes
- compaction tests prove no data loss before/after snapshot
- benchmarked startup replay time bounded by policy target

### Status

- strong baseline complete; now optimize safety guardrails and throughput profile defaults.

## Phase 4: Retrieval Semantics and Citation Integrity

### Objective

Guarantee that retrieval responses are explainable and evidence-first, not embedding-only.

### Build scope

- stance modes:
  - `balanced`
  - `support_only`
- temporal filtering behavior
- citation payload completeness and provenance integrity

### Deliverables

- stable GET/POST retrieval contract
- retrieval responses with support/contradiction counts + evidence citations
- temporal/stance quality probes in benchmark scorecard

### Exit gates

- no missing citation fields in normal responses
- contradiction and temporal probe pass gates in CI benchmark path

### Status

- complete baseline; continue broadening adversarial query coverage.

## Phase 5: Hybrid Candidate Planner (Dense + Sparse + Metadata)

### Objective

Scale candidate generation beyond naive full scan while preserving evidence quality.

### Build scope

- ANN dense candidates
- lexical candidates
- metadata/entity/embedding prefilters
- explicit observability of:
  - metadata prefilter count
  - ANN candidate count
  - final scored candidate count

### Deliverables

- hybrid planner with deterministic merge strategy
- benchmark scorecard fields for candidate pipeline transparency
- ANN recall observability (`recall@10`, `recall@100`, curve)

### Exit gates

- candidate reduction and latency gates hold at large profiles
- ANN recall gates maintained across benchmark history

### Status

- active and functional; continue refinement for larger/cross-tenant distributions.

## Phase 6: Segment Lifecycle and Read Semantics

### Objective

Make immutable segments first-class while preserving correctness between fresh mutable writes and compacted data.

### Build scope

- segment builder/publisher integrity
- segment manifest and checksum verification
- explicit read semantics:
  - immutable segment base
  - mutable WAL delta
  - merge boundary definition

### Deliverables

- documented + tested source-of-truth model for base/delta merge
- retrieval prefilter integration with segment data
- compaction/snapshot lifecycle docs and operational knobs

### Exit gates

- no divergence between replay state and segment-served read state
- manifest drift recovery behavior tested

### Status

- strong prototype complete; needs continued hardening and object-store tier rollout.

## Phase 7: Distributed Routing, Placement, and Failover

### Objective

Ensure requests are admitted only on valid nodes and routing can evolve safely during failover events.

### Build scope

- placement CSV/metadata ingestion
- leader/follower routing rules
- admission checks in ingestion/retrieval transports
- debug introspection endpoints for routing decisions

### Deliverables

- `GET /debug/placement` with shard/replica/epoch/health context
- failover drill scripts for role switch validation
- route probe metrics/logging for rejection diagnosis

### Exit gates

- deterministic route behavior under epoch changes
- failover drill passes repeatedly in CI/staging scripts

### Status

- implemented with restart-based drill; next milestone is live placement reload for no-restart drills.

## Phase 8: Security, Tenancy, and Compliance Layer

### Objective

Move from baseline API key auth to production-grade tenant-safe access control and audit guarantees.

### Build scope

- scoped API key policy + tenant allowlists
- full audit event streams for access/mutations
- key lifecycle controls (rotation/revocation policy)
- optional federation path (JWT/OIDC)

### Deliverables

- tenant-isolation integration test suite
- audited security runbook and threat model
- operational guidance for key rotation and incident response

### Exit gates

- no cross-tenant leakage in tests/fuzz scenarios
- all privileged actions recorded with auditable identifiers

### Status

- baseline implemented; hardening depth still required for full enterprise posture.

## Phase 9: Performance Engineering and Scale Operations

### Objective

Close throughput and latency gaps while preserving durability correctness and evidence quality.

### Build scope

- ingestion throughput optimization:
  - grouped commit
  - append buffering
  - bounded sync interval policies
- retrieval latency optimization:
  - planner efficiency
  - segment cache effectiveness
- benchmark automation and historical trend analysis

### Deliverables

- WAL durability benchmark profiles and history artifacts
- tuning guardrails for production-safe configs
- scale testing matrix (smoke/large/xlarge/hybrid/concurrency)

### Exit gates

- latency and quality gates remain green under tuned configs
- durability tradeoffs are explicit and documented

### Status

- active; newly added WAL durability benchmark is a major step forward.

## Phase 10: Production Package, Release Discipline, and Developer Experience

### Objective

Turn DASH into a repeatable, operable product package with stable release rhythm and onboarding clarity.

### Build scope

- release packaging and deployment automation
- CI/CD release gates and staged benchmark enforcement
- full documentation set:
  - architecture
  - runbooks
  - env matrix
  - release checklist
  - incident playbook
- local developer bootstrap and validation workflow

### Deliverables

- release artifact pipeline
- versioned release notes + benchmark evidence
- production readiness checklist with sign-off gates

### Exit gates

- release can be executed by on-call/operator playbook without tribal knowledge
- rollback drill and incident drill both pass

### Status

- strong baseline exists; continue tightening release governance and staged promotion logic.

## 4. Cross-Phase Guardrails (Always On)

- rust-first implementation for core engine paths
- no breaking schema changes without replay compatibility plan
- all retrieval changes preserve citation-grade output
- all durability/performance tradeoff changes require benchmark evidence
- `progress.md` updated after each milestone and meaningful validation run
- CI green before promotion

## 5. Definition of Done (DASH Full Build)

DASH is considered fully built for production candidate readiness when all of the following are true:

1. Claim/evidence/edge contracts are stable and versioned.
2. WAL+snapshot durability, replay, and compaction are proven under failure simulations.
3. Retrieval responses are citation-complete with stance + temporal correctness.
4. Hybrid planner quality and ANN recall metrics are tracked and gated.
5. Segment base + WAL delta read semantics are documented, tested, and operationally stable.
6. Placement routing + failover drills are repeatable with high observability.
7. Tenant isolation and auth/audit controls pass hardening tests.
8. Throughput/latency benchmarks remain within target envelopes at scale profiles.
9. Release/deployment/runbook workflows are reproducible and tested.
10. Ongoing benchmark history and CI guardrails prevent silent regressions.

## 6. Immediate Next Sequence (Practical Execution Order)

1. Add ingestion config guardrails for unsafe WAL durability windows (with explicit override escape hatch).
2. Implement placement live-reload and extend failover drill to no-restart mode.
3. Add durability trend rollups into committed benchmark history/reporting.
4. Expand tenant isolation + auth hardening integration tests.
5. Continue large/xlarge/hybrid benchmark trend cadence with explicit release gates.

