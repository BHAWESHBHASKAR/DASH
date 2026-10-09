# DASH

> Evidence-first vector database for citation-grade RAG.

DASH stores atomic claims with provenance, retrieves them with citation-bearing rankings, and exposes an OpenAI-compatible embeddings endpoint so existing clients can call it without code changes.

**Status: pre-1.0, not yet production-ready.** This tree is **0.3.0 (unreleased)**, the security and correctness hardening release described in [`docs/plans/2026-10-09-production-readiness-master-plan.md`](docs/plans/2026-10-09-production-readiness-master-plan.md); nothing is tagged or published yet. Upgrading from 0.2.x needs configuration changes: see [`CHANGELOG.md`](CHANGELOG.md#upgrading-to-030). Read the [Status](#status) section before deploying anything that holds real data. Every capability claim in this file is tracked in [`docs/claims-ledger.md`](docs/claims-ledger.md) with the test or document that backs it.

## Quickstart

Requires Docker with Compose v2, `git`, and `curl`. This builds the images from source (no release images have been published yet), so the first run takes several minutes.

```bash
git clone https://github.com/BHAWESHBHASKAR/DASH.git
cd DASH

# 1. Generate strong random credentials into deploy/container/.env (git-ignored).
./scripts/generate-secrets.sh
set -a; source deploy/container/.env; set +a

# 2. Build and start ingestion (:8081), retrieval (:8080) and the segment maintenance
#    daemon. The control plane (:8090) is optional: add `--profile control-plane`.
docker compose -f deploy/container/docker-compose.yml up -d --build
curl -fsS http://localhost:8081/health
curl -fsS http://localhost:8080/health
```

The services refuse to be left open: since 0.3.0 they will not start without credentials unless `DASH_INSECURE_DEV_MODE=1` is set (dev mode binds localhost only). The compose file requires the keys generated above. Send them in the `x-api-key` header (or as `Authorization: Bearer <key>`). Ingestion and retrieval use separate keys.

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

The retrieval service is a read replica that follows the ingestion WAL by polling (250 ms in the compose file), so wait a moment, then retrieve with citations, dropping claims that have more contradicting than supporting evidence:

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

The OpenAI-compatible embeddings endpoint is on the retrieval service. It requires a credential with the `retrieve` role (it was open before 0.3.0, which was a defect). The default provider is a deterministic hash embedder with no network access; it is for development and tests, not semantic quality. Use `DASH_EMBEDDING_PROVIDER=ollama` for real vectors (see [Configuration](docs-site/docs/reference/configuration.md)).

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

This is the honest state of the 0.3.0 (unreleased) tree. The authoritative plan, with owners, phases and exit criteria, is [`docs/plans/2026-10-09-production-readiness-master-plan.md`](docs/plans/2026-10-09-production-readiness-master-plan.md); the defect list is [`docs/plans/2026-10-09-issue-register.md`](docs/plans/2026-10-09-issue-register.md); what the P0 code changes cover, with evidence and what is still open, is [`docs/plans/2026-10-09-p0-status.md`](docs/plans/2026-10-09-p0-status.md).

**Stable (behavior covered by tests, unlikely to change shape):**
- Claim, Evidence and Edge schema and validation (`pkg/schema`).
- Retrieval semantics: `balanced` and `support_only` stance modes, contradiction demotion, temporal `time_range` filtering, optional graph payload.
- WAL write, replay, checkpoint and snapshot compaction in a single process (`pkg/store`): checksummed records, torn-tail truncation, one commit group per single ingest, quarantine of unreadable legacy records (`tools/wal-inspect` to inspect and repair).
- Authentication and authorization (`services/common`, `pkg/auth`): deny by default, HS256 JWT validation (kid rotation, `iss`/`aud`, tenant claims, `exp` required, lifetime cap, `jti` denylist), scoped API keys, key revocation lists, a role hierarchy (`admin`, `ingest`, `retrieve`, `read_only`), per-tenant token-bucket rate limiting (429) and SIGHUP reload. Details: [`docs/operations/auth.md`](docs/operations/auth.md).

**Beta (works, with known gaps listed in the register or below):**
- HTTP services on a hand-written thread-per-connection transport (ingestion, retrieval, control-plane) with bounded headers, bodies and whole-request deadlines. Since 0.3.0 the services refuse to start without credentials unless `DASH_INSECURE_DEV_MODE=1`; strict secret validation (>= 16 characters for keys and tokens, >= 32 for JWT secrets, placeholders rejected) is on by default; `/v1/embeddings`, `/debug/*` and `/metrics` require auth; replication requires `DASH_INGEST_REPLICATION_TOKEN`; the control plane requires `DASH_CONTROL_PLANE_TOKEN`. TLS is not terminated by DASH; put a proxy in front.
- Evidence and edge writes are idempotent upserts (evidence by `evidence_id`, edges by endpoints and relation), so retries, restarts and replication re-apply do not duplicate citations.
- Single-writer ingestion plus polling read replicas that follow the WAL generation-aware (resync on checkpoint, `/ready` reflects lag), a file-lease control plane with fenced leases and lag-guarded promotion, and CSV placement files. This is not consensus replication.
- `redb` on-disk persistence is **on by default** when a WAL path is configured (default `./data/dash-ingestion.redb` and `./data/dash-retrieval.redb`; opt out with `DASH_INGEST_PERSISTENCE_DISABLE=1` / `DASH_RETRIEVAL_PERSISTENCE_DISABLE=1`). If the file cannot be opened the service logs an error and continues in memory.
- Embedding providers: `hash` (default, dev only), `ollama`, `openai` (HTTPS). `/v1/embeddings` rejects token-id array inputs unless `DASH_EMBEDDING_ALLOW_TOKEN_IDS=1`, and `usage.prompt_tokens` is an estimate. The Ollama endpoint variable is `DASH_OLLAMA_ENDPOINT`. Live Ollama and OpenAI calls are not exercised by CI.
- SHA-256 hash-chained audit log (`DASH_*_AUDIT_LOG_PATH`) with one canonical record encoding, file locking, torn-tail recovery and the `tools/audit-verify` verifier (`scripts/verify_audit_chain.sh` wraps it). The chain is **unkeyed**, so it detects accidental edits but not an attacker who can rewrite the file; there is no HMAC. Details: [`docs/operations/audit-chain.md`](docs/operations/audit-chain.md).
- OIDC/JWKS validation: asymmetric algorithms only, `iss`/`aud`/`exp` required, HTTPS JWKS, a hardened JWKS cache. Tests use RS256 keys against an in-process stub IdP; there is no test against a real identity provider, and the ES256/EdDSA/PS* algorithms are accepted but untested.
- Segment build, manifest, compaction and maintenance daemon (`services/indexer`), with collision-free tenant directories. Retrieval's use of segments is partial.
- ANN: an in-repo, multi-level HNSW-style graph in `pkg/store/src/ann.rs`. It is **not** `usearch`: the `usearch` crate is declared in `Cargo.toml` but no source file uses it. Tuning via `DASH_*_ANN_*`. Recall at scale has not been measured in a reproducible CI job.
- Ranking: support and contradiction contributions saturate (support bonus capped at 0.4, contradiction penalty at 0.5), and an edge `from supports to` credits the target claim.
- Benchmark and drill scripts. Numbers in `docs/benchmarks/` are drill output, not a published performance claim.

**Planned (do not rely on):**
- Keyed (HMAC) audit chain and external anchoring; the current chain is unkeyed.
- Encryption at rest / CMEK. `pkg/encryption` is a standalone AES-256-GCM library with an env-key provider; no storage code calls it, so nothing on disk is encrypted by DASH.
- mTLS between services (tokens are shared secrets over plain HTTP), an external penetration test, and published SOC 2 evidence.
- GPU vector backend. `pkg/store/src/gpu.rs` is a placeholder that never returns a GPU engine; scoring runs on CPU.
- Consensus replication (Raft), automatic failover, sharded cluster mode.
- Delete, tenant management and reindex APIs (`/v1/delete`, `/v1/tenants`, `/v1/admin/reindex` do not exist; see [Planned API](docs-site/docs/reference/planned-api.md)).
- Signed release images and published release artifacts.

## Architecture

DASH is a Rust workspace (edition 2024).

| Crate | Role |
|---|---|
| `pkg/schema` | Claim, Evidence, ClaimEdge types and validation |
| `pkg/store` | In-memory store, WAL (checksummed records, commit groups, generations), redb disk layer, in-repo HNSW-style ANN, GPU placeholder |
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
| `services/common` | Deny-by-default auth policy and rate limiter, secret validation, audit chain writer and verifier, shutdown signaling, logging |
| `tools/wal-inspect` | Inspect, verify and repair WAL and snapshot files |
| `tools/audit-verify` | Verify audit-chain files (v2 and legacy records) |
| `tests/benchmarks` | Benchmark and load-test binaries |

Ingested claims are written to a WAL, replayed into an in-memory Claim + Evidence + Edge store, and indexed for ANN candidate generation. The retrieval path combines ANN and lexical candidates with tenant, time-range and stance filters and optional graph expansion, then projects citation-bearing results. Design detail: [`docs/architecture/eme-architecture.md`](docs/architecture/eme-architecture.md) (the design document predates several renames; where it disagrees with the code, the code and this README win).

## SDKs

Six client SDKs live in `sdks/`, all at version 0.2.0 (the Go module is untagged). They send the API key as `Authorization: Bearer <api_key>`, which the servers accept. Test counts are static counts of test declarations and include live-integration tests that are skipped without a running server.

| SDK | Path | Version | Coverage | Tests |
|---|---|---|---|---|
| Python (`dash-py`) | `sdks/python` | 0.2.0 | embeddings, retrieve (full request and response contract); sync and async; OpenAI-compat helper. No ingest method. | 69 |
| Go | `sdks/go` (module `github.com/BHAWESHBHASKAR/DASH/sdks/go`) | untagged | embeddings, retrieve, OpenAI-compat. No ingest method. | 92 |
| TypeScript (`dash-ts`) | `sdks/typescript` | 0.2.0 | embeddings, retrieve, OpenAI-compat. No ingest method. | 71 |
| Java | `sdks/java` | 0.2.0 | embeddings, ingest (separate ingestion base URL), retrieve. | 32 |
| Kotlin | `sdks/kotlin` | 0.2.0 | embeddings, ingest, retrieve (suspend API, wraps the Java client). | 12 |
| C# | `sdks/csharp` | 0.2.0 | embeddings, ingest (`IngestionBaseUrl` option), retrieve. | 50 |

The `delete` call that the Java, Kotlin and C# SDKs used to expose (it targeted a `POST /v1/delete` route that does not exist) was removed in 0.3.0. The Go module path changed from `github.com/anomalyco/dash-go` to `github.com/BHAWESHBHASKAR/DASH/sdks/go`; update your imports. The Java, Kotlin and C# SDKs retry only idempotent requests (or requests with an `Idempotency-Key`). See [`sdks/LIVE_INTEGRATION_TESTS.md`](sdks/LIVE_INTEGRATION_TESTS.md) for running SDK tests against a live stack.

## Tests

Counts are static (computed 2026-10-09: the Rust figure with `cargo test --workspace -- --list 2>/dev/null | grep -c ': test$'`, the SDK figures by counting test declarations); they are not a pass/fail report. CI is the source of truth for what passes.

| Suite | Declared tests |
|---|---|
| Rust workspace (`#[test]` and `#[tokio::test]`) | 956 |
| Python SDK | 69 |
| Go SDK | 92 |
| TypeScript SDK | 71 |
| Java SDK | 32 |
| Kotlin SDK | 12 |
| C# SDK | 50 |

There are also four `cargo-fuzz` targets in `fuzz/` (JWT, OpenAI embeddings parser, ranking, WAL parser) and a benchmark suite in `tests/benchmarks`.

## Comparison

See [`docs/comparison.md`](docs/comparison.md) for a feature comparison with other vector databases and guidance on when not to use DASH. DASH's differentiators are the claim/evidence/contradiction/temporal data model and per-result citations. It does not match mature vector databases on scale, operational tooling, or ANN performance, and none of those comparisons are backed by head-to-head benchmarks yet.

## Roadmap

Shipped in this tree (0.3.0, unreleased): claim/evidence/edge model, WAL with checkpoints, checksums, commit groups and generations, retrieval with stance and time filters, deny-by-default scoped-key, JWT and OIDC auth with role hierarchy and rate limiting, OpenAI-compatible authenticated embeddings endpoint, hash-chained audit log with verifier, segment indexer and maintenance daemon, control plane with authenticated, fenced file-lease leadership, redb persistence layer, WAL and audit tools, six SDKs, Docker/Helm/systemd packaging, benchmark suite.

Remaining P0 and later work is tracked in [`docs/plans/2026-10-09-p0-status.md`](docs/plans/2026-10-09-p0-status.md) and the issue register.

After that, in plan order: container/Helm/CI hardening (P1), storage engine and index rework (P2), consensus replication (P3), encryption at rest and tenant lifecycle (P4), retrieval quality evaluation (P5), API and SDK regeneration (P6), GA hardening (P7). See the master plan for scope and exit criteria.

## Contributing

See [`CONTRIBUTING.md`](CONTRIBUTING.md). New code must pass `cargo fmt --check`, `cargo clippy --workspace --all-targets -- -D warnings` and `cargo test --workspace`. An issue is closed only when the fix is merged with a regression test that fails on the old code; plan status lines must link to evidence.

## License

Apache License 2.0. See [`LICENSE`](LICENSE).
