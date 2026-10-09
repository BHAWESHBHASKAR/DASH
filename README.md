# DASH

> Evidence-first vector database for citation-grade RAG.

DASH stores atomic claims with provenance, retrieves them with citation-bearing rankings, and exposes an OpenAI-compatible embeddings endpoint so existing clients can call it without code changes.

**Status: pre-1.0, not yet production-ready.** The current line is v0.2.x; v0.3.0 is the security and correctness hardening release described in [`docs/plans/2026-10-09-production-readiness-master-plan.md`](docs/plans/2026-10-09-production-readiness-master-plan.md). Read the [Status](#status) section before deploying anything that holds real data. Every capability claim in this file is tracked in [`docs/claims-ledger.md`](docs/claims-ledger.md) with the test or document that backs it.

## Quickstart

Requires Docker with Compose v2, `git`, and `curl`. This builds the images from source (no release images have been published yet), so the first run takes several minutes.

```bash
git clone https://github.com/BHAWESHBHASKAR/DASH.git
cd DASH

# 1. Generate strong random credentials into deploy/container/.env (git-ignored).
./scripts/generate-secrets.sh
set -a; source deploy/container/.env; set +a

# 2. Build and start ingestion (:8081), retrieval (:8080), control-plane (:8090).
docker compose -f deploy/container/docker-compose.yml up -d --build
curl -fsS http://localhost:8081/health
curl -fsS http://localhost:8080/health
```

The services refuse to be left open: v0.3.0 will not start without credentials unless `DASH_INSECURE_DEV_MODE=1` is set (dev mode binds localhost only). The compose file requires the keys generated above. Send them in the `x-api-key` header (or as `Authorization: Bearer <key>`). Ingestion and retrieval use separate keys.

Ingest a claim with supporting evidence:

```bash
curl -fsS -X POST http://localhost:8081/v1/ingest \
  -H "Content-Type: application/json" \
  -H "x-api-key: $DASH_INGEST_API_KEY" \
  -d '{
    "claim": {
      "claim_id": "c1",
      "tenant_id": "t1",
      "canonical_text": "Company X acquired Company Y",
      "confidence": 0.95
    },
    "evidence": [{
      "evidence_id": "e1",
      "claim_id": "c1",
      "source_id": "news://nyt",
      "stance": "supports",
      "source_quality": 0.95
    }],
    "edges": []
  }'
```

The retrieval service is a read replica that follows the ingestion WAL by polling (250 ms in the compose file), so wait a moment, then retrieve with citations and drop any contradicted claim:

```bash
sleep 2
curl -fsS -X POST http://localhost:8080/v1/retrieve \
  -H "Content-Type: application/json" \
  -H "x-api-key: $DASH_RETRIEVAL_API_KEY" \
  -d '{
    "tenant_id": "t1",
    "query": "Company X acquired Company Y",
    "top_k": 5,
    "stance_mode": "support_only"
  }'
```

The OpenAI-compatible embeddings endpoint is on the retrieval service. It is authenticated as of v0.3.0 (before that it was open, which was a defect). The default provider is a deterministic hash embedder with no network access; it is for development and tests, not semantic quality. Use `DASH_EMBEDDING_PROVIDER=ollama` for real vectors (see [Configuration](docs-site/docs/reference/configuration.md)).

```bash
curl -fsS -X POST http://localhost:8080/v1/embeddings \
  -H "Content-Type: application/json" \
  -H "x-api-key: $DASH_RETRIEVAL_API_KEY" \
  -d '{"input": "Company X acquired Company Y", "model": "text-embedding-3-small"}'
```

```python
import openai
client = openai.OpenAI(base_url="http://localhost:8080/v1", api_key=RETRIEVAL_API_KEY)
resp = client.embeddings.create(input="hello world", model="text-embedding-3-small")
```

To run from source instead of Docker, see [`docs/quickstart.md`](docs/quickstart.md). To stop and wipe the stack: `docker compose -f deploy/container/docker-compose.yml down -v`.

## Why DASH

Naive RAG ranks documents by vector similarity and returns the top *k* chunks. That fails in three common enterprise cases: (1) two sources say opposite things and nothing demotes the contradicted one, (2) a fact has a temporal window and the retrieved version is stale, (3) an auditor asks "why did the model say that" and the answer is "a vector was close." DASH treats the **claim**, an atomic source-bound assertion, as the primary data primitive, with **evidence** and **citation** as first-class fields on every result.

A retrieval result carries the claim text, a score, `supports` / `contradicts` counts, and a `citations[]` array. Each citation has `evidence_id`, `source_id`, `stance` (supports / contradicts / neutral), `source_quality`, and optional `chunk_id`, `span_start`, `span_end`, `doc_id`, `extraction_model`. The retrieval API accepts `stance_mode: support_only` to drop claims with more contradictions than supports, and `time_range: {from_unix, to_unix}` to constrain results to a validity window. DASH does not aim to be the fastest pure vector index; the goal is defensible retrieval.

## Status

This is the honest state as of 2026-10-09. The authoritative plan, with owners, phases and exit criteria, is [`docs/plans/2026-10-09-production-readiness-master-plan.md`](docs/plans/2026-10-09-production-readiness-master-plan.md); the defect list is [`docs/plans/2026-10-09-issue-register.md`](docs/plans/2026-10-09-issue-register.md).

**Stable (behavior covered by tests, unlikely to change shape):**
- Claim, Evidence and Edge schema and validation (`pkg/schema`).
- Retrieval semantics: `balanced` and `support_only` stance modes, contradiction demotion, temporal `time_range` filtering, optional graph payload.
- WAL write, replay, checkpoint and snapshot compaction in a single process (`pkg/store`).
- HS256 JWT validation (kid rotation, `iss`/`aud`, tenant claims), scoped API keys, key revocation lists, role checks (`admin`, `ingest`, `retrieve`, `read_only`).

**Beta (works, with known defects listed in the register; v0.3.0 addresses the P0 items):**
- HTTP services on a hand-written thread-per-connection transport (ingestion, retrieval, control-plane). v0.3.0: services refuse to start without credentials unless `DASH_INSECURE_DEV_MODE=1`; strict secret validation (>= 32 chars, placeholders rejected) is on by default; `/v1/embeddings`, `/debug/*` and `/metrics` require auth; replication requires `DASH_INGEST_REPLICATION_TOKEN`; the control plane requires `DASH_CONTROL_PLANE_TOKEN`.
- Per-tenant rate limiting. Configurable today, but the limiter state is rebuilt per request and rejections surface as 401, so it does not throttle. v0.3.0 enforces it with HTTP 429.
- Evidence and edge writes are not idempotent today: evidence is duplicated on restart, retry and replication re-apply (register DATA-01), which inflates citation counts and ranking. v0.3.0 makes them idempotent upserts.
- WAL recovery: v0.3.0 truncates a torn tail on recovery and adds WAL generation ids so followers resync after compaction.
- Single-writer ingestion plus polling read replicas, a file-lease control plane and CSV placement files. This is not consensus replication.
- `redb` on-disk persistence is **on by default** when a WAL path is configured (default `./data/dash-ingestion.redb` and `./data/dash-retrieval.redb`; opt out with `DASH_INGEST_PERSISTENCE_DISABLE=1` / `DASH_RETRIEVAL_PERSISTENCE_DISABLE=1`). If the file cannot be opened the service logs an error and continues in memory.
- Embedding providers: `hash` (default, dev only), `ollama`, `openai`. The OpenAI provider currently speaks plain HTTP only and cannot reach `api.openai.com`; v0.3.0 adds TLS. The Ollama endpoint variable is `DASH_OLLAMA_ENDPOINT`.
- SHA-256 hash-chained audit log (`DASH_*_AUDIT_LOG_PATH`, verifier `scripts/verify_audit_chain.sh`). The chain is unkeyed, so it detects accidental edits but not an attacker who can rewrite the file; there is no HMAC.
- OIDC/JWKS validation and RBAC roles (library and transport wiring exist; no end-to-end IdP test in CI).
- Segment build, manifest, compaction and maintenance daemon (`services/indexer`). Retrieval's use of segments is partial.
- ANN: an in-repo, multi-level HNSW-style graph in `pkg/store/src/ann.rs`. It is **not** `usearch`: the `usearch` crate is declared in `Cargo.toml` but no source file uses it. Tuning via `DASH_*_ANN_*`. Recall at scale has not been measured in a reproducible CI job.
- Benchmark and drill scripts. Numbers in `docs/benchmarks/` are drill output, not a published performance claim.

**Planned (do not rely on):**
- Encryption at rest / CMEK. `pkg/encryption` is a standalone AES-256-GCM library with an env-key provider; no storage code calls it, so nothing on disk is encrypted by DASH.
- GPU vector backend. `pkg/store/src/gpu.rs` is a stub that always returns "no GPU"; scoring runs on CPU.
- Consensus replication, automatic failover, sharded cluster mode.
- Delete, tenant management and reindex APIs (`/v1/delete`, `/v1/tenants`, `/v1/admin/reindex` do not exist; see [Planned API](docs-site/docs/reference/planned-api.md)).
- Generated config/API reference, signed release images, published release artifacts.

## Architecture

DASH is a Rust workspace (edition 2024).

| Crate | Role |
|---|---|
| `pkg/schema` | Claim, Evidence, ClaimEdge types and validation |
| `pkg/store` | In-memory store, WAL, redb disk layer, in-repo HNSW-style ANN, GPU stub |
| `pkg/ranking` | Scoring (confidence, stance, source quality, contradiction penalty) |
| `pkg/graph` | Claim graph expansion and support/contradiction path reasoning |
| `pkg/auth` | HS256 JWT, OIDC/JWKS, roles, SHA-256 helper |
| `pkg/embeddings` | `EmbeddingProvider` trait; hash, Ollama, OpenAI providers; circuit breaker |
| `pkg/encryption` | AES-256-GCM envelope library with env-key provider (not wired into storage) |
| `services/ingestion` | Write API (`/v1/ingest*`), WAL owner, replication source |
| `services/retrieval` | Read API (`/v1/retrieve`, `/v1/embeddings`), WAL/replication follower |
| `services/control-plane` | Placement state, file-lease leader election, failover promotion |
| `services/metadata-router` | Shard placement and read/write routing library used by the services |
| `services/indexer` | Segment builder, compaction planner, `segment-maintenance-daemon` |
| `services/common` | Shutdown signaling, secret validation, logging |
| `tests/benchmarks` | Benchmark and load-test binaries |

Ingested claims are written to a WAL, replayed into an in-memory Claim + Evidence + Edge store, and indexed for ANN candidate generation. The retrieval path combines ANN and lexical candidates with tenant, time-range and stance filters and optional graph expansion, then projects citation-bearing results. Design detail: [`docs/architecture/eme-architecture.md`](docs/architecture/eme-architecture.md) (the design document predates several renames; where it disagrees with the code, the code and this README win).

## SDKs

Six client SDKs live in `sdks/`. The Python, Go, TypeScript, Java and C# SDKs send the API key as `Authorization: Bearer <api_key>`, which the servers accept (the Kotlin SDK was not checked). Test counts are static counts of test declarations and include live-integration tests that are skipped without a running server.

| SDK | Path | Version | Coverage | Tests |
|---|---|---|---|---|
| Python (`dash-py`) | `sdks/python` | 0.1.0 | embeddings, retrieve; sync and async; OpenAI-compat helper. Ingest types only, no ingest method. | 64 |
| Go | `sdks/go` | untagged | embeddings, retrieve, OpenAI-compat. No ingest method. | 89 |
| TypeScript (`dash-ts`) | `sdks/typescript` | 0.1.0 | embeddings, retrieve, OpenAI-compat. No ingest method. | 69 |
| Java | `sdks/java` | 0.2.0 | embeddings, ingest, retrieve. | 21 |
| Kotlin | `sdks/kotlin` | 0.2.0 | embeddings, ingest, retrieve (suspend API). | 12 |
| C# | `sdks/csharp` | 0.2.0 | embeddings, ingest, retrieve. | 41 |

The Java, Kotlin and C# SDKs also expose a `delete` call for `POST /v1/delete`, which the server does not implement. Do not use it. v0.3.0 fixes the three JVM/.NET SDKs and unifies them at 0.2.0. See [`sdks/LIVE_INTEGRATION_TESTS.md`](sdks/LIVE_INTEGRATION_TESTS.md) for running SDK tests against a live stack.

## Tests

Counts are static (computed 2026-10-09 with `grep -rE '#\[(tokio::)?test\]' --include=*.rs . | wc -l`); they are not a pass/fail report. CI is the source of truth for what passes.

| Suite | Declared tests |
|---|---|
| Rust workspace (`#[test]` and `#[tokio::test]`) | 420 |
| Python SDK | 64 |
| Go SDK | 89 |
| TypeScript SDK | 69 |
| Java SDK | 21 |
| Kotlin SDK | 12 |
| C# SDK | 41 |

There are also four `cargo-fuzz` targets in `fuzz/` (JWT, OpenAI embeddings parser, ranking, WAL parser) and a benchmark suite in `tests/benchmarks`.

## Comparison

See [`docs/comparison.md`](docs/comparison.md) for a feature comparison with other vector databases and guidance on when not to use DASH. DASH's differentiators are the claim/evidence/contradiction/temporal data model and per-result citations. It does not match mature vector databases on scale, operational tooling, or ANN performance, and none of those comparisons are backed by head-to-head benchmarks yet.

## Roadmap

Shipped in this tree: claim/evidence/edge model, WAL with checkpoints, retrieval with stance and time filters, scoped keys and JWT auth, OIDC/JWKS and RBAC, OpenAI-compatible embeddings endpoint, hash-chained audit log, segment indexer and maintenance daemon, control-plane with file-lease leadership, redb persistence layer, six SDKs, Docker/Helm/systemd packaging, benchmark suite.

Next (P0, release v0.3.0): the hardening items listed under [Status](#status), tracked in the issue register.

After that, in plan order: container/Helm/CI hardening (P1), storage engine and index rework (P2), consensus replication (P3), encryption at rest and tenant lifecycle (P4), retrieval quality evaluation (P5), API and SDK regeneration (P6), GA hardening (P7). See the master plan for scope and exit criteria.

## Contributing

See [`CONTRIBUTING.md`](CONTRIBUTING.md). New code must pass `cargo fmt --check`, `cargo clippy --workspace --all-targets -- -D warnings` and `cargo test --workspace`. An issue is closed only when the fix is merged with a regression test that fails on the old code; plan status lines must link to evidence.

## License

Apache License 2.0. See [`LICENSE`](LICENSE).
