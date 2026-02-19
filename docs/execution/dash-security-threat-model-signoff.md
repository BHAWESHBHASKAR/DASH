# DASH Security Threat Model and Sign-Off

Date: 2026-02-19  
Status: Phase 3 hardening sign-off recorded (local pre-staging)

## 1. Scope

This document defines the production-facing security model for DASH transport, storage, and operational workflows in this repository.

In scope:
- ingestion transport (`/v1/ingest`, `/health`, `/metrics`, `/debug/placement`)
- retrieval transport (`/v1/retrieve`, `/health`, `/metrics`, `/debug/placement`)
- tenant isolation and placement-aware routing controls
- WAL + snapshot persistence and recovery tooling
- authN/authZ, audit chain integrity, and release-gate security checks

Out of scope:
- external identity provider integration beyond current JWT HS256 mode
- KMS/HSM-backed secret lifecycle orchestration
- host-level kernel/network hardening and cloud IAM policy enforcement

## 2. Security Objectives

- Prevent cross-tenant data disclosure or mutation.
- Ensure unauthorized clients cannot ingest/retrieve data.
- Preserve evidence provenance and audit tamper detection.
- Provide deterministic fail-closed behavior under routing/auth config errors.
- Ensure recovery workflows do not violate RPO/RTO policy targets.

## 3. Assets and Trust Boundaries

Primary assets:
- Claim, Evidence, ClaimEdge records and metadata
- Tenant-scoped vector indexes and segment manifests
- WAL and snapshot files
- API keys/JWT secrets and rotation sets
- Audit logs and audit-chain verifier output

Trust boundaries:
- External client -> transport API boundary
- Service process -> filesystem (WAL/snapshots/audit logs)
- Placement metadata source -> in-process routing decisions
- Operational scripts -> production state artifacts

## 4. Threat Model (STRIDE)

### 4.1 Spoofing

Threats:
- forged API key or JWT bearer tokens
- stale but leaked key remains valid after incident

Mitigations implemented:
- API key enforcement (`DASH_*_API_KEY`, `DASH_*_API_KEYS`)
- explicit revoked-key deny list (`DASH_*_REVOKED_API_KEYS`)
- JWT HS256 validation with issuer/audience/time claim checks and tenant claim enforcement
- JWT rotation via fallback secrets and `kid`-mapped secret selection

### 4.2 Tampering

Threats:
- mutation of audit records post-write
- WAL/snapshot corruption influencing replay

Mitigations implemented:
- tamper-evident chained audit logs (`seq`, `prev_hash`, `hash`)
- `scripts/verify_audit_chain.sh` verification gate
- segment checksum validation during load in indexer
- WAL replay validation and startup replay observability

### 4.3 Repudiation

Threats:
- inability to trace denied or accepted writes/reads

Mitigations implemented:
- auth success/failure/authz-deny counters on `/metrics`
- optional JSONL audit events for ingest/retrieve operations
- release checklist and gate hooks for audit-chain verification

### 4.4 Information Disclosure

Threats:
- cross-tenant claim_id collision or index leakage
- wrong-node routing serving unauthorized shard data

Mitigations implemented:
- store-level fail-closed on cross-tenant `claim_id` collision
- retrieval segment prefilter tenant scoping and foreign-id exclusion tests
- placement-aware read/write admission checks with fail-closed behavior
- tenant allowlists and per-key tenant scopes

### 4.5 Denial of Service

Threats:
- unbounded worker queue growth and request exhaustion
- oversized request bodies and socket starvation

Mitigations implemented:
- bounded backpressure admission in ingestion/retrieval runtimes (`std` + `axum`)
- queue-full `503` reject behavior and queue depth/reject metrics
- max body size guard (`16 MiB`) and socket timeout guards

### 4.6 Elevation of Privilege

Threats:
- weak scope enforcement allows unauthorized tenant actions
- routing metadata misconfiguration bypasses leader/replica role constraints

Mitigations implemented:
- per-key tenant scope enforcement + deny tests
- JWT tenant-claim validation + deny tests
- write-leader/read-replica routing enforcement
- placement configuration error fails startup

## 5. Required Security Controls and Current State

- AuthN/AuthZ controls: implemented and tested.
- Tenant isolation controls: implemented and tested (including collision fail-closed).
- Audit tamper-evidence: implemented; verifier script integrated in release gate.
- Routing safety controls: implemented with failover drill coverage.
- Recovery controls: backup/restore and recovery drill scripted with RTO/RPO checks.
- DoS backpressure controls: implemented in both transports and both runtimes.

## 6. Validation Evidence

Core validation commands:
- `cargo test --workspace`
- `scripts/ci.sh`
- `scripts/failover_drill.sh --mode no-restart`
- `scripts/auth_revocation_drill.sh`
- `scripts/recovery_drill.sh --max-rto-seconds 60`
- `scripts/incident_simulation_gate.sh --failover-mode no-restart --recovery-max-rto-seconds 60`
- `scripts/release_candidate_gate.sh --run-incident-simulation-guard true ...`
- `scripts/security_signoff_gate.sh --run-tag <tag> --ingestion-audit <path> --retrieval-audit <path>`

Security-relevant expected outcomes:
- revoked key requests return `401` in both services
- cross-tenant scope violations return deny status (`401`/`403` depending on decision type)
- failover drill phase-1 rejects wrong-node routing (`503`), phase-2 accepts after promotion (`200`)
- recovery drill reports `rpo_claim_gap=0` and `rto_seconds <= threshold`

## 7. Residual Risks

- JWT mode currently supports HS256 only; OIDC/JWKS-based asymmetric verification is pending.
- Secret distribution/rotation orchestration is env-driven and not centrally managed.
- At-rest encryption controls for WAL/segments are deployment-environment responsibilities and not enforced in-app.
- Full adversarial load testing for auth endpoints under DDoS scenarios remains limited.

## 8. Phase 3 Sign-Off Checklist

- [x] AuthN/AuthZ controls verified in local pre-staging gate run.
- [x] Tenant isolation regression suite green on release commit.
- [x] Audit chain verification run on ingestion + retrieval logs.
- [x] Incident simulation gate passes (`failover + auth revocation + recovery`).
- [x] Recovery drill SLO pass recorded (RPO gap `0`, RTO <= policy).
- [x] Residual-risk acceptance documented by owner.

### 8.1 Executed Sign-Off Evidence (2026-02-19 IST)

- Audit log generation artifact root:
  - `/var/folders/md/cj3zgrq94_d04dl1yrh1c00m0000gn/T//dash-signoff-audit-b5qneh`
- Audit verification commands:
  - `scripts/verify_audit_chain.sh --path /var/folders/md/cj3zgrq94_d04dl1yrh1c00m0000gn/T//dash-signoff-audit-b5qneh/ingestion-audit.jsonl --service ingestion`
  - `scripts/verify_audit_chain.sh --path /var/folders/md/cj3zgrq94_d04dl1yrh1c00m0000gn/T//dash-signoff-audit-b5qneh/retrieval-audit.jsonl --service retrieval`
- Incident-enabled release gate command:
  - `scripts/release_candidate_gate.sh --run-tag phase3-signoff-local --run-benchmark-trend false --run-ingest-throughput-guard false --slo-include-recovery-drill false --run-incident-simulation-guard true --incident-failover-mode no-restart --incident-failover-max-wait-seconds 30 --incident-auth-max-wait-seconds 30 --incident-recovery-max-rto-seconds 60 --verify-ingestion-audit /var/folders/md/cj3zgrq94_d04dl1yrh1c00m0000gn/T//dash-signoff-audit-b5qneh/ingestion-audit.jsonl --verify-retrieval-audit /var/folders/md/cj3zgrq94_d04dl1yrh1c00m0000gn/T//dash-signoff-audit-b5qneh/retrieval-audit.jsonl`
- Release gate summary artifact:
  - `/Users/bhaweshbhaskar/Desktop/vector/docs/benchmarks/history/runs/20260218-215058-phase3-signoff-local-release-gate.md`
- Incident simulation summary artifact:
  - `/Users/bhaweshbhaskar/Desktop/vector/docs/benchmarks/history/runs/20260218-215124-phase3-signoff-local-incident-incident-gate.md`

## 9. Sign-Off Record

- Security reviewer: Codex automated security gate runner (local pre-staging)
- Platform owner: DASH engineering workspace owner
- Date: 2026-02-19 03:21:34 IST
- Decision: `approved-with-risks`
- Notes:
  - Approval is based on local pre-staging execution evidence and full release-gate PASS with incident simulation + audit verification enabled.
  - Residual risks from Section 7 are accepted for current phase; production cutover still requires a final environment-specific rerun in staging/prod change window.
