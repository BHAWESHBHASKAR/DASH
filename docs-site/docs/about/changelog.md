# Changelog

All notable changes to DASH are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/) and the project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- **OpenAI-compatible `/v1/embeddings` endpoint** on the retrieval service. Any OpenAI client (langchain, llama-index, semantic-kernel, the `openai` Python SDK) can point at DASH with one environment variable (`OPENAI_API_BASE=http://localhost:8080/v1`). 17 tests cover the wire-format compatibility, error envelope, and HTTP-level integration.
- **Python SDK** `dash-py` — idiomatic Python client with sync + async, typed dataclasses, OpenAI drop-in examples, RAG example showing the Claim + Evidence + Contradiction differentiator. 59 tests pass.
- **Go SDK** `dash-go` — Go 1.21+ module, idiomatic functional options + service pattern, full OpenAI drop-in compatibility, `errors.Is`/`errors.As` support. 86 tests pass.
- **TypeScript SDK** `dash-ts` — ESM-first, Node 18+, zero runtime deps, full type safety with discriminated unions, drop-in OpenAI compatibility. 65 tests pass.
- **Docker + docker-compose** — multi-stage, multi-arch (`linux/amd64`, `linux/arm64`), non-root runtime, healthcheck, dependency ordering, dev overlay with hot-reload. `docker compose up -d` brings the ingestion + retrieval services online.
- **Quickstart + README + comparison docs** — `docs/quickstart.md` (5-minute path from clone to first query), `README.md` (rewrite with hero section and the differentiator showcase), `docs/comparison.md` (DASH vs Pinecone/Weaviate/Milvus/Qdrant/Chroma).
- **Real embedding integration** — env-driven provider selection (`DASH_EMBEDDING_PROVIDER=hash|ollama|openai`) so deployments can wire up real semantic embeddings instead of the hash-based default.
- **Semantic-first retrieval** — new `InMemoryStore::retrieve_semantic` method. When the caller passes a pre-computed query vector, the dense-similarity score becomes the primary ranking signal (cosine in `[-1, 1]`, mapped to `[0, 1]`); the lexical/BM25 score becomes a small tie-breaker. 3 new integration tests cover the semantic-first guarantee.
- **redb persistence (PR 1, additive, default off)** — `DiskBackedStore` struct in `pkg/store/src/disk.rs` provides on-disk durability for claims, evidence, edges, vectors, and the tenant→claim set. Enabled via `DASH_INGEST_PERSISTENCE_PATH` / `DASH_RETRIEVAL_PERSISTENCE_PATH` env vars. If unset (the default), DASH runs in WAL-only mode — the pre-redb behavior is preserved bit-for-bit. 5 new integration tests cover round-trip, fallback, tenant consistency, ANN rebuild, and open-failure paths. The design doc at `docs/plans/2026-06-13-redb-persistence-design.md` describes the full 3-PR phasing.
- **cargo-fuzz harnesses** — `fuzz/` directory with 4 focused targets (`fuzz_jwt`, `fuzz_openai_embeddings`, `fuzz_ranking`, `fuzz_wal_parse`) that exercise the JWT verifier, OpenAI request parser, ranking function, and WAL record parser against arbitrary input. Run via `cargo +nightly fuzz run <target>`. See `fuzz/README.md`.
- **Performance benchmark suite** — `tests/benchmarks/src/perf_bench.rs` with 5 scenarios: `ingest_throughput_sequential` (in-memory and persistent), `retrieve_throughput_lexical`, `retrieve_throughput_semantic`, `ann_search_throughput_at_scale`, `wal_replay_throughput`. CLI flags `--scenario`, `--iterations`, `--warmup`. Baseline numbers published in `docs/benchmarks/performance.md`.
- **Public `evidence_for_claim` accessor** on `InMemoryStore` for test introspection of ingested evidence.
- **`OpenAIErrorResponse`** with `DashError` interface and `DashAPIError`/`DashConnectionError` concrete types; `from_response` factory tolerates both OpenAI and ad-hoc error shapes.

### Changed

- **JSON parsing in services/ingestion** — replaced 633 lines of hand-rolled JSON parsing in `transport/payload.rs` with serde-based deserialization. `transport/json.rs` is now a thin compat shim for the test suite.
- **Hand-rolled crypto in pkg/auth** — replaced SHA-256, HMAC-SHA256, base64url, and the recursive-descent JSON parser with `jsonwebtoken`, `serde_json`, `base64`, `sha2`, and `hex`. Public API preserved. 16 tests pass (was 7 in the hand-rolled impl — added 9 new edge-case tests including a FIPS SHA-256 known vector).
- **Hand-rolled HNSW in pkg/store** — replaced the 4-level O(N²)-per-insert HNSW scaffolding with the `usearch` crate. Per-tenant ANN graphs are still isolated and dimension-pinned; the new index is substantially faster on the benchmark suite.
- **Schema types** in `pkg/schema` — added `Serialize`/`Deserialize` derives to all public domain types with `#[serde(default)]` on optional fields.
- **API request/response types** in `services/ingestion/src/api.rs` — added `Serialize`/`Deserialize` derives. 5 new serde round-trip tests.
- **Half-built axum runtime paths removed** from both services. The `TransportRuntime::Axum` branch that exited with an error when the `async-transport` feature was absent is gone; the runtime path is now a single direct call to `serve_http_with_workers`.
- **`.gitignore`** updated to ignore SDK build artifacts (`node_modules/`, `dist/`, `__pycache__/`, `*.pyc`, etc.).

### Fixed

- All 24 prior build warnings (cfg conditions, dead code, lazy doc-continuation) resolved.
- 4 pre-existing test bugs (one in `wal_persistence_and_replay_round_trip` where the `support_only` test split `Vec`s and passed only one to `ingest_bundle`; fixed by merging into a single evidence vec).

### Test counts

- **Rust unit + integration tests:** 379 passing (was 333 at the start of this modernization campaign; +46 new tests across schema, auth, store (unit), store (integration_retrieval), retrieval, embeddings, retrieval HTTP integration, and disk persistence).
- **Python SDK:** 59 tests passing.
- **Go SDK:** 86 tests passing.
- **TypeScript SDK:** 65 tests passing.
- **Total across all stacks:** **589 tests passing.**

### Known limitations

- The `InMemoryStore::Clone` derive was replaced with a manual impl that drops the disk handle on clone. This is a known limitation of the redb PR 1 design; the next PR will switch to `Arc<DiskBackedStore>` to share the handle cheaply. Cloned stores used for batched ingest staging will lose their disk attachment — this affects `IngestionRuntime::ingest_batch` which does `self.store.clone()` internally.
- The `embeddings_for_claim` returns the full evidence vec; for tenants with thousands of evidence per claim, this is unbounded. A future PR will add pagination.
- The performance benchmark suite does not include comparison against Pinecone/Weaviate/Milvus. Baseline numbers are internal (DASH against itself). See `docs/benchmarks/performance.md` for the methodology and the roadmap for the competitor-comparison work.

## Earlier releases

### Pre-modernization

The original EME/DASH architecture is documented in `docs/architecture/eme-architecture.md` (the "Evidence Memory Engine" phase 0 design). The 11-phase production rollout plan lives in `docs/execution/phases/`. The 2026-06-13 modernization roadmap at `docs/plans/2026-06-13-dash-modernization-roadmap.md` records the session-by-session deltas in detail.
