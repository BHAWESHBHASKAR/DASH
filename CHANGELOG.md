# Changelog

All notable changes to DASH are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/) and the project adheres
to [Semantic Versioning](https://semver.org/).

## [Unreleased]

This section is the planned content of **0.3.0**. Nothing below has shipped
yet; items move to a dated release section only when the fix is merged with
a regression test that fails on the old code (see `CONTRIBUTING.md`). The
authoritative scope, owners and exit criteria are in
[`docs/plans/2026-10-09-production-readiness-master-plan.md`](docs/plans/2026-10-09-production-readiness-master-plan.md)
and the defect IDs below refer to
[`docs/plans/2026-10-09-issue-register.md`](docs/plans/2026-10-09-issue-register.md).

## 0.3.0 (planned) - P0 production-readiness hardening

### Security
- Services refuse to start without credentials unless
  `DASH_INSECURE_DEV_MODE=1`; dev mode binds localhost only (SEC-01).
- Strict secret validation is on by default: at least 32 characters,
  placeholders rejected (SEC-04, SEC-05).
- `/v1/embeddings`, `/debug/*` and `/metrics` require authentication;
  authentication runs before any embedding provider call (SEC-09,
  SEC-10).
- Replication endpoints require `DASH_INGEST_REPLICATION_TOKEN` (SEC-08).
- The control plane requires `DASH_CONTROL_PLANE_TOKEN` (SEC-07).
- Per-tenant rate limiting is enforced and rejects with HTTP 429
  (SEC-06).
- The OpenAI embedding provider speaks TLS (SEC-23).
- The Ollama endpoint variable is `DASH_OLLAMA_ENDPOINT`.

### Data integrity and recovery
- Evidence and edges are idempotent upserts instead of appends (DATA-01).
- A torn WAL tail is truncated on recovery instead of failing startup.
- WAL generation ids force follower resync after compaction.

### SDKs
- Java, Kotlin and C# SDKs are fixed and unified at version 0.2.0.

### Documentation
- README, configuration reference, HTTP API reference, deploy guide,
  threat model and SOC 2 readiness mapping rewritten to describe only
  what the code does (DOC-01 to DOC-07). Every README capability claim is
  tracked in `docs/claims-ledger.md` and checked by
  `scripts/check_claims_ledger.sh`.

## M11 - Enterprise identity, RBAC, encryption library, SOC 2 package (in tree, untagged; 2026-08-10)

Delivered in commits `c55e8cb` (M11a/M11b) and `e38cd0b` (M11c/M11d). What
exists and what does not:

### Added
- **OIDC/JWKS token validation** (`pkg/auth/src/oidc.rs`): JWKS fetch with
  a refresh interval, issuer/audience/expiry checks, tenant claim
  extraction. Enabled per service with `DASH_*_JWT_PROVIDER=oidc`. Unit
  tests use a symmetric (`oct`/HS256) JWK; there is no end-to-end test
  against an identity provider and no RSA/EC key test.
- **Role-based access control**: roles `admin`, `ingest`, `retrieve`,
  `read_only` parsed from JWT claims and scoped API keys
  (`key:tenants:roles`) and checked per route in ingestion and retrieval.
  Known gaps: roles have no hierarchy, a JWT without a roles claim gets
  all roles, unscoped keys skip role checks (SEC-11). The planned
  control-plane `admin` enforcement was not implemented (SEC-07).
- **`pkg/encryption`**: an AES-256-GCM `EncryptionProvider` trait with an
  environment master-key provider. This is a library only: no storage
  code calls it, so no data is encrypted at rest by DASH (SEC-16).
- **SOC 2 readiness package**: `docs/compliance/soc2-readiness.md`,
  policy templates, and `scripts/soc2_evidence_collector.sh`. These
  describe a target state; see the "Current status" column added in the
  2026-10-09 documentation correction.

## 0.2.x line - 2026-06-13 modernization (in tree, untagged)

Everything below this heading was previously listed under `[Unreleased]`.
Corrections made on 2026-10-09 are marked *Correction*.


### Added
- **OpenAI-compatible `/v1/embeddings` endpoint** on the retrieval
  service. Any OpenAI client (langchain, llama-index, semantic-kernel,
  the `openai` Python SDK) can point at DASH with one environment
  variable (`OPENAI_API_BASE=http://localhost:8080/v1`). 17 tests
  cover the wire-format compatibility, error envelope, and HTTP-level
  integration.
- **Python SDK** `dash-py` — idiomatic Python client with sync + async,
  typed dataclasses, OpenAI drop-in examples, RAG example showing
  the Claim + Evidence + Contradiction differentiator. 59 tests pass.
- **Go SDK** `dash-go` — Go 1.21+ module, idiomatic functional options
  + service pattern, full OpenAI drop-in compatibility, errors.Is/As
  support. 86 tests pass.
- **TypeScript SDK** `dash-ts` — ESM-first, Node 18+, zero runtime deps,
  full type safety with discriminated unions, drop-in OpenAI
  compatibility. 65 tests pass.
- **Docker + docker-compose** — multi-stage, multi-arch (linux/amd64,
  linux/arm64), non-root runtime, healthcheck, dependency ordering,
  dev overlay with hot-reload. `docker compose up -d` brings the
  ingestion + retrieval services online.
- **Quickstart + README + comparison docs** — `docs/quickstart.md`
  (5-minute path from clone to first query), `README.md` (rewrite
  with hero section and the differentiator showcase), `docs/comparison.md`
  (DASH vs Pinecone/Weaviate/Milvus/Qdrant/Chroma).
- **Real embedding integration** — env-driven provider selection
  (`DASH_EMBEDDING_PROVIDER=hash|ollama|openai`) so deployments
  can wire up real semantic embeddings instead of the
  hash-based default.
- **Semantic-first retrieval** — new `InMemoryStore::retrieve_semantic`
  method. When the caller passes a pre-computed query vector, the
  dense-similarity score becomes the primary ranking signal (cosine
  in `[-1, 1]`, mapped to `[0, 1]`); the lexical/BM25 score becomes a
  small tie-breaker. 3 new integration tests cover the semantic-first
  guarantee.
- **redb persistence (PR 1, additive)** (*Correction:* PR 1 shipped default-off, but commit `6a242d8` (PR 2) made persistence default-on when a WAL path is set; disable with `DASH_*_PERSISTENCE_DISABLE=1`. The text below describes PR 1.) — `DiskBackedStore`
  struct in `pkg/store/src/disk.rs` provides on-disk durability for
  claims, evidence, edges, vectors, and the tenant→claim set. Enabled
  via `DASH_INGEST_PERSISTENCE_PATH` / `DASH_RETRIEVAL_PERSISTENCE_PATH`
  env vars. If unset (the default), DASH runs in WAL-only mode — the
  pre-redb behavior is preserved bit-for-bit. 5 new integration
  tests cover round-trip, fallback, tenant consistency, ANN rebuild,
  and open-failure paths. The design doc at
  `docs/plans/2026-06-13-redb-persistence-design.md` describes the
  full 3-PR phasing.
- **cargo-fuzz harnesses** — `fuzz/` directory with 4 focused targets
  (`fuzz_jwt`, `fuzz_openai_embeddings`, `fuzz_ranking`, `fuzz_wal_parse`)
  that exercise the JWT verifier, OpenAI request parser, ranking
  function, and WAL record parser against arbitrary input. Run via
  `cargo +nightly fuzz run <target>`. See `fuzz/README.md`.
- **Performance benchmark suite** — `tests/benchmarks/src/perf_bench.rs`
  with 5 scenarios: `ingest_throughput_sequential` (in-memory and
  persistent), `retrieve_throughput_lexical`, `retrieve_throughput_semantic`,
  `ann_search_throughput_at_scale`, `wal_replay_throughput`. CLI
  flags `--scenario`, `--iterations`, `--warmup`. Baseline numbers
  published in `docs/benchmarks/performance.md`.
- **Public `evidence_for_claim` accessor** on `InMemoryStore` for
  test introspection of ingested evidence.
- **`OpenAIErrorResponse`** with `DashError` interface and
  `DashAPIError`/`DashConnectionError` concrete types; `from_response`
  factory tolerates both OpenAI and ad-hoc error shapes.

### Changed
- **JSON parsing in services/ingestion** — replaced 633 lines of
  hand-rolled JSON parsing in `transport/payload.rs` with serde-based
  deserialization. `transport/json.rs` is now a thin compat shim for
  the test suite.
- **Hand-rolled crypto in pkg/auth** — replaced SHA-256, HMAC-SHA256,
  base64url, and the recursive-descent JSON parser with
  `jsonwebtoken`, `serde_json`, `base64`, `sha2`, and `hex`. Public
  API preserved. 16 tests pass (was 7 in the hand-rolled impl —
  added 9 new edge-case tests including a FIPS SHA-256 known vector).
- **Hand-rolled HNSW in pkg/store** — *Correction (2026-10-09):* this
  entry originally claimed the in-repo HNSW scaffolding was replaced by
  the `usearch` crate and was "substantially faster". That did not
  happen. `usearch` is declared in `Cargo.toml` but no source file uses
  it; the ANN index is the in-repo HNSW-style graph in
  `pkg/store/src/ann.rs` (register IDX-01). No benchmark in the
  repository supports a speedup claim.
- **Schema types** in `pkg/schema` — added `Serialize`/`Deserialize`
  derives to all public domain types with `#[serde(default)]` on
  optional fields.
- **API request/response types** in `services/ingestion/src/api.rs` —
  added `Serialize`/`Deserialize` derives. 5 new serde round-trip tests.
- **Half-built axum runtime paths removed** from both services. The
  `TransportRuntime::Axum` branch that exited with an error when the
  `async-transport` feature was absent is gone; the runtime path is
  now a single direct call to `serve_http_with_workers`.
- **`.gitignore`** updated to ignore SDK build artifacts
  (`node_modules/`, `dist/`, `__pycache__/`, `*.pyc`, etc.).

### Fixed
- All 24 prior build warnings (cfg conditions, dead code, lazy
  doc-continuation) resolved.
- 4 pre-existing test bugs (one in `wal_persistence_and_replay_round_trip`
  where the support_only test split `Vec`s and passed only one to
  `ingest_bundle`; fixed by merging into a single evidence vec).

### Test counts
*Correction (2026-10-09):* the counts below are the figures recorded on
2026-06-13 and were not re-verified. On 2026-10-09 the repository
contains 420 Rust `#[test]`/`#[tokio::test]` declarations (static
count). SDK test declarations: Python 64, Go 89, TypeScript 69, Java 21,
Kotlin 12, C# 41. Pass/fail status is the CI result, not these numbers.

- **Rust unit + integration tests:** 379 passing (was 333 at the start
  of this modernization campaign; +46 new tests across schema,
  auth, store (unit), store (integration_retrieval), retrieval,
  embeddings, retrieval HTTP integration, and disk persistence).
- **Python SDK:** 59 tests passing.
- **Go SDK:** 86 tests passing.
- **TypeScript SDK:** 65 tests passing.
- **Total across all stacks:** **589 tests passing.**

### Known limitations
- *Superseded:* the `InMemoryStore::Clone` limitation (disk handle dropped
  on clone) was addressed by redb PR 2 (`Arc<DiskBackedStore>`, commit
  `6a242d8`).
- The `embeddings_for_claim` returns the full evidence vec; for tenants
  with thousands of evidence per claim, this is unbounded. A future
  PR will add pagination.
- The performance benchmark suite does not include comparison
  against Pinecone/Weaviate/Milvus. Baseline numbers are internal
  (DASH against itself). See `docs/benchmarks/performance.md` for the
  methodology and the roadmap for the competitor-comparison work.

## Earlier releases

### Pre-modernization
(Note: the architecture document referred to here is
`docs/architecture/eme-architecture.md`; the root `EME_ARCHITECTURE.md`
is now a pointer to it.)
The original EME/DASH architecture is documented in
`docs/architecture/eme-architecture.md` (the "Evidence Memory Engine"
phase 0 design). The 11-phase production rollout plan lives in
`docs/execution/phases/`. The 2026-06-13 modernization roadmap at
`docs/plans/2026-06-13-dash-modernization-roadmap.md` records the
session-by-session deltas in detail.
