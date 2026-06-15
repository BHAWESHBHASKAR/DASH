# DASH

> Evidence-first vector database for citation-grade RAG.

DASH stores atomic claims with provenance, retrieves them with citation-grade rankings, and ships an OpenAI-compatible embeddings endpoint so any client can adopt it without changing call sites.

```bash
# Drop-in OpenAI-compatible embeddings
curl -X POST http://localhost:8080/v1/embeddings \
  -H "Content-Type: application/json" \
  -d '{"input": "Company X acquired Company Y", "model": "text-embedding-3-small"}'
```

```python
import openai
client = openai.OpenAI(base_url="http://localhost:8080/v1", api_key="not_needed")
resp = client.embeddings.create(input="hello world", model="text-embedding-3-small")
print(resp.data[0].embedding[:5])
```

## Current state (2026-06-13)

**Production-ready in this release:**
- OpenAI-compatible `/v1/embeddings` endpoint (wire-byte compatible with the OpenAI spec)
- Semantic-first retrieval (`InMemoryStore::retrieve_semantic`) with dense-similarity as the primary ranking signal
- Env-driven real embedding providers: `DASH_EMBEDDING_PROVIDER=hash|ollama|openai`
- `redb` persistence (PR 1, additive, default off; enable with `DASH_*_PERSISTENCE_PATH`)
- SDKs: Python (`dash-py`), Go (`dash-go`), TypeScript (`dash-ts`) — all OpenAI-drop-in compatible
- `cargo-fuzz` harnesses for JWT, OpenAI parser, ranking, and WAL parser
- Performance benchmark suite (`perf_bench`): ingest, retrieve-lexical, retrieve-semantic, ANN-at-scale, WAL-replay
- Docker + docker-compose (multi-arch, non-root, healthcheck)
- Hash-chained audit log, per-tenant rate limiting, JWT auth + scoped API keys, OpenAI drop-in

**Test counts:** 379 Rust unit/integration tests passing, plus 86 Go + 65 TypeScript + 59 Python = **589 tests total** across the workspace. `cargo clippy --workspace --all-targets` is clean. `cargo build --workspace` is clean.

See [`CHANGELOG.md`](./CHANGELOG.md) for the full deltas and [`docs/quickstart.md`](./docs/quickstart.md) for the 5-minute path.

## Why DASH

Naive RAG ranks documents by vector similarity and returns the top *k* chunks. That works for "summarize this article" but fails in three common enterprise cases: (1) two sources say opposite things and you have no way to demote the contradicted one, (2) a fact has a temporal window and the version you retrieved is stale, (3) your auditor asks "why did the model say that" and the answer is "because a 768-dimensional number was close to a query." DASH treats the **claim** — an atomic, source-bound assertion — as the primary data primitive, with **evidence** and **citation** as first-class fields on every result.

Concretely, every retrieval response in DASH is `{ claim, score, supports, contradicts, citations[] }`. Each `citation` carries its `source_id`, `stance` (supports/contradicts/neutral), `source_quality`, and an optional `chunk_id` plus `span_start`/`span_end` for character-level traceability. The retrieval API exposes `stance_mode: support_only` to filter out claims that have been contradicted, and `time_range: {from_unix, to_unix}` to constrain results to a validity window. This makes DASH a different kind of vector database: not the fastest pure vector index, but the most defensible one for RAG that has to ship to legal, medical, financial, and enterprise knowledge workflows.

## Quickstart

The five-minute path from clone to retrieval query. Requires Docker.

```bash
git clone https://github.com/anomalyco/dash.git
cd dash
docker compose -f deploy/container/docker-compose.yml up -d
```

Ingest a claim with its supporting evidence:

```bash
curl -X POST http://localhost:8081/v1/ingest \
  -H "Content-Type: application/json" \
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

Retrieve with citations, dropping any claim that has been contradicted:

```bash
curl -X POST http://localhost:8080/v1/retrieve \
  -H "Content-Type: application/json" \
  -d '{
    "tenant_id": "t1",
    "query": "Company X acquired Company Y",
    "top_k": 5,
    "stance_mode": "support_only"
  }'
```

Use the OpenAI-compatible `/v1/embeddings` endpoint from any OpenAI SDK — `langchain`, `llama-index`, the `openai` CLI — by setting `OPENAI_API_BASE=http://localhost:8080/v1`. See [`docs/quickstart.md`](docs/quickstart.md) for the full path including building from source, Python examples, and the contradiction-handling walkthrough.

## What's production-ready

- **Claim + Evidence + Edge data model** with first-class citation provenance (`source_id`, `stance`, `source_quality`, `chunk_id`, `span_start`, `span_end`, `doc_id`, `extraction_model`).
- **Contradiction handling**: `Stance::Contradicts` on evidence and `ClaimEdge { relation: Contradicts }` demote results; `stance_mode: support_only` filters them out.
- **Temporal validity windows**: `event_time_unix`, `valid_from`, `valid_to` on every claim, with `time_range` filtering on the retrieval API.
- **OpenAI-compatible `/v1/embeddings`**: byte-compatible request/response with the OpenAI v1 embeddings API. Default provider is `HashEmbeddingProvider` (deterministic, no network); swap for Ollama, OpenAI, or any custom backend by implementing the `EmbeddingProvider` trait.
- **HNSW ANN** via `usearch` for vector candidate generation, with `DASH_*_ANN_*` tuning knobs and a graph-backed recall layer on top.
- **Durable WAL** with replay, checkpoints, and compaction in `pkg/store`. WAL durability guardrails reject unsafe flush policies by default; an explicit `DASH_INGEST_ALLOW_UNSAFE_WAL_DURABILITY=true` override is required for stress testing.
- **Hash-chained audit log**: every authenticated state change is recorded as a SHA-256-chained JSON line; verify with `scripts/verify_audit_chain.sh`.
- **Per-tenant rate limits, scoped API keys, and key revocation** with `DASH_*_RATE_LIMIT_*`, `DASH_*_SCOPED_API_KEYS`, and `DASH_*_REVOKED_API_KEYS`. Multi-tenant tenant allowlist enforced in the authz layer.
- **JWT auth (HS256)** with key rotation by `kid`, optional `iss`/`aud` checks, fallback secrets list, and per-tenant claim enforcement.
- **Open source** — see the `LICENSE` file (the intended license is Apache-2.0, pending confirmation before the first tagged release). Fully auditable core, no vendor lock-in, no telemetry.
- **Operational scripts**: backup, restore, recovery drill, failover drill, SLO guard, release-candidate gate, audit chain verifier — see `scripts/`.
- **Benchmark suite** with `smoke`, `hybrid`, and `large` profiles and a CI-enforced regression guard against prior scorecards.

## Comparison

DASH vs other vector databases on the dimensions that matter to RAG users. See [`docs/comparison.md`](docs/comparison.md) for the full table and "when to use / when not to use" guidance.

| Dimension | DASH | Pinecone | Weaviate | Milvus | Qdrant | Chroma |
|---|---|---|---|---|---|---|
| Open source | yes | no | yes (BSD-3) | yes (Apache-2.0) | yes (Apache-2.0) | yes (Apache-2.0) |
| Claim + Evidence model | first-class | no | no | no | no | no |
| Contradiction handling | first-class | no | manual | no | no | no |
| Temporal validity windows | first-class | metadata | manual | manual | manual | manual |
| OpenAI-compatible `/v1/embeddings` | yes | limited | yes | proxy | proxy | yes |
| Hash-chained audit log | yes | no | no | no | no | no |
| Tenant rate limits | yes | yes | yes | yes | partial | no |
| JWT + scoped API keys + revocation | yes | JWT only | OIDC | yes | partial | no |
| RAG-specific primitives | yes | no | modules | no | no | no |

## Architecture

DASH is a Rust workspace organized into library crates (`pkg/schema`, `pkg/store`, `pkg/ranking`, `pkg/graph`, `pkg/auth`, `pkg/embeddings`) and four service binaries (`services/ingestion`, `services/retrieval`, `services/indexer`, `services/control-plane`). Ingested claims are durably written to a write-ahead log, replayed into an in-memory `Claim + Evidence + Edge` store, and indexed for HNSW ANN candidate generation. The retrieval path runs a planner that combines ANN candidates with metadata filters, time-range filters, stance demotion/filtering, and optional graph expansion, then projects results into a citation-bearing response. JWT and scoped-API-key authz is enforced in the transport layer; per-tenant rate limits and a hash-chained audit log are emitted alongside every state change. The full design — including the data model, WAL/snapshot protocol, retrieval planner, and operational model — lives in [`docs/architecture/eme-architecture.md`](docs/architecture/eme-architecture.md).

## Roadmap

Done (in this tree):
- Claim + Evidence + Edge schema, validation, and serde round-trips
- WAL with replay, checkpoints, compaction, and durability guardrails
- HNSW ANN via `usearch` with `DASH_*_ANN_*` tuning
- Retrieval API with `Balanced` and `SupportOnly` stance modes, time-range filtering, optional graph payload
- OpenAI-compatible `/v1/embeddings`
- Per-tenant authz, scoped keys, revocation, rate limits
- Hash-chained audit log with chain verifier
- HS256 JWT with kid rotation, `iss`/`aud`, fallback secrets
- Benchmark suite with CI regression guard
- Docker Compose and systemd unit files

Next (active development):
- Larger-scale ANN recall/quality tuning and benchmarking at 10M+ claim corpora
- Full segment lifecycle integration in the retrieval hot path
- Distributed shard + replication protocol (placement router is wired; production-ready replication path is not)
- Auth federation (OIDC), broader key-rotation story
- Pluggable embedding backends (Ollama, OpenAI passthrough) as a first-class `EmbeddingProvider` example set
- JavaScript/TypeScript and Go SDKs

## Contributing

Contributions are welcome. See [`CONTRIBUTING.md`](CONTRIBUTING.md) for the workflow, code standards (clippy enforced at `-D warnings`, fmt enforced in CI), RFC process, and "good first issue" list. All new code must pass `cargo test --workspace` and `./scripts/ci.sh`.

## License

DASH is source-available software. The intended release license is Apache-2.0, pending confirmation before the first tagged release. Until a `LICENSE` file is added at the repository root, the actual terms are those stated in the repository's `README.md` and `CONTRIBUTING.md`.
