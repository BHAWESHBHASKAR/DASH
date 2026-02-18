# DASH

DASH is an evidence-first retrieval engine for RAG built in Rust.

Instead of treating retrieval as only vector similarity, DASH models:
- claims
- evidence
- claim edges (support/contradiction/refinement)
- citation provenance
- temporal validity

The goal is citation-grade, contradiction-aware, temporally-aware retrieval that is production-oriented.

## Current Capabilities

- Rust workspace with modular crates (`schema`, `store`, `ranking`, `graph`, services)
- Durable WAL with replay, checkpoints, and compaction in `pkg/store`
- Ingestion and retrieval services with HTTP transport (std runtime, optional axum)
- Retrieval API with:
  - `Balanced` and `SupportOnly` stance behavior
  - optional graph payload
  - metadata filters
  - temporal range filtering
  - optional query embedding path
- ANN-style candidate generation controls and benchmark gates
- Segment lifecycle prototype:
  - immutable segment writer/manifest/checksum verification (`services/indexer`)
  - ingestion runtime segment publish (env-gated)
  - retrieval segment-backed prefilter (env-gated)
- CI pipeline + benchmark smoke/history guard

## Repository Layout

- `pkg/schema` - core domain models and validation
- `pkg/store` - WAL, replay, indexes, retrieval core
- `pkg/ranking` - scoring and ranking logic
- `pkg/graph` - graph traversal/summarization
- `services/ingestion` - ingestion service + transport
- `services/retrieval` - retrieval service + transport
- `services/indexer` - segment lifecycle and compaction planning
- `tests/benchmarks` - benchmark binaries and profiles
- `docs/architecture` - architecture docs
- `docs/execution` - runbooks, env matrix, plans
- `docs/benchmarks` - benchmark protocol/history/scorecards

## Quick Start

Prerequisite: Rust stable toolchain.

```bash
cargo test --workspace
scripts/ci.sh
```

Run ingestion service:

```bash
cargo run -p ingestion -- --serve
```

Run retrieval service:

```bash
cargo run -p retrieval -- --serve
```

Default endpoints:
- ingestion: `http://127.0.0.1:8081`
- retrieval: `http://127.0.0.1:8080`

## Operational Environment

See full matrix:
- `docs/execution/dash-startup-env-matrix.md`

Key env groups:
- transport/runtime: `DASH_*_BIND`, `DASH_*_HTTP_WORKERS`, `DASH_*_TRANSPORT_RUNTIME`
- durability: `DASH_INGEST_WAL_PATH`, checkpoint limits
- security: API keys, tenant allowlists, scoped keys, audit log paths
- JWT auth (HS256): `DASH_*_JWT_HS256_SECRET`, optional rotation via `DASH_*_JWT_HS256_SECRETS` / `DASH_*_JWT_HS256_SECRETS_BY_KID`, plus optional `DASH_*_JWT_ISSUER` / `DASH_*_JWT_AUDIENCE`
- ANN tuning: `DASH_*_ANN_*`
- segment lifecycle:
  - ingestion publish: `DASH_INGEST_SEGMENT_DIR` (+ sizing/compaction knobs)
  - retrieval prefilter: `DASH_RETRIEVAL_SEGMENT_DIR`

Operational scripts:
- backup: `scripts/backup_state_bundle.sh`
- restore: `scripts/restore_state_bundle.sh`
- recovery drill: `scripts/recovery_drill.sh`
- failover drill: `scripts/failover_drill.sh`
- slo/error-budget guard: `scripts/slo_guard.sh`
- audit chain verifier: `scripts/verify_audit_chain.sh`

## Benchmarks

Smoke / hybrid / large benchmark profiles are available in `tests/benchmarks`.

Typical commands:

```bash
cargo run -p benchmark-smoke --bin benchmark-smoke -- --profile smoke
cargo run -p benchmark-smoke --bin benchmark-smoke -- --profile hybrid
cargo run -p benchmark-smoke --bin benchmark-smoke -- --profile large
```

## Documentation

- Architecture: `docs/architecture/eme-architecture.md`
- Master plan: `docs/execution/eme-master-plan.md`
- Production runbook: `docs/execution/dash-production-runbook.md`
- Benchmark protocol: `docs/benchmarks/evaluation-protocol.md`

## Project Status

DASH has a strong production-ready foundation (durability, retrieval semantics, transport, CI, benchmark guardrails).

Primary remaining hardening areas:
- deeper ANN/quality tuning at larger scale
- segment runtime integration in full serving pipeline
- auth federation/key rotation/extended security posture
- distributed shard/replication path
