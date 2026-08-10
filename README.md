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

## Current state (2026-08-09)

**Production-ready in this release:**
- OpenAI-compatible `/v1/embeddings` endpoint (wire-byte compatible with the OpenAI spec)
- Semantic-first retrieval with dense similarity as the primary ranking signal
- Env-driven real embedding providers: `DASH_EMBEDDING_PROVIDER=hash|ollama|openai` (Ollama overlay in `deploy/container/docker-compose.ollama.yml`)
- `redb` persistence with WAL replay, checkpoints, compaction, and durability guardrails
- Customer-managed encryption keys (`pkg/encryption`) with `env` and AWS KMS (`aws-kms` feature) providers, wired into WAL/snapshot lines
- OIDC/JWKS authentication and RBAC (`admin`, `ingest`, `retrieve`, `read_only`) with scoped API keys
- Control-plane leader election, shard placement, and failover promotion
- Quorum replication with follower pull and persistent replication offset
- Disk-first segment serving tier for large tenants
- Object-storage (S3) backup/restore
- Helm chart with managed-cloud scaffolding
- OpenAPI 3.0 spec and SDK quick-start docs in `docs/api/`
- SDKs: Python (`dash-py`), Go (`dash-go`), TypeScript (`dash-ts`), C# (`Dash`), Java, Kotlin — all OpenAI-drop-in compatible
- `cargo-fuzz` harnesses for JWT, OpenAI parser, ranking, and WAL parser
- Performance benchmark suite with CI regression guard
- Docker + docker-compose (multi-arch, non-root, healthcheck, Prometheus alert rules)
- Hash-chained audit log, per-tenant rate limiting, key revocation

**Test counts:** Rust unit/integration tests, Go/TypeScript/Python/C#/Java/Kotlin SDK tests, and `./scripts/ci.sh` all pass. `cargo clippy --workspace --all-features` is clean. `cargo build --workspace --all-features` is clean.

See [`CHANGELOG.md`](./CHANGELOG.md) for the full deltas and [`docs/api/README.md`](./docs/api/README.md) for the API reference and quick-start.

## Why DASH

Naive RAG ranks documents by vector similarity and returns the top *k* chunks. That works for "summarize this article" but fails in three common enterprise cases: (1) two sources say opposite things and you have no way to demote the contradicted one, (2) a fact has a temporal window and the version you retrieved is stale, (3) your auditor asks "why did the model say that" and the answer is "because a 768-dimensional number was close to a query." DASH treats the **claim** — an atomic, source-bound assertion — as the primary data primitive, with **evidence** and **citation** as first-class fields on every result.

Concretely, every retrieval response in DASH is `{ claim, score, supports, contradicts, citations[] }`. Each `citation` carries its `source_id`, `stance` (supports/contradicts/neutral), `source_quality`, and an optional `chunk_id` plus `span_start`/`span_end` for character-level traceability. The retrieval API exposes `stance_mode: support_only` to filter out claims that have been contradicted, and `time_range: {from_unix, to_unix}` to constrain results to a validity window. This makes DASH a different kind of vector database: not the fastest pure vector index, but the most defensible one for RAG that has to ship to legal, medical, financial, and enterprise knowledge workflows.

## Quickstart

The five-minute path from clone to retrieval query. Requires Docker.

```bash
git clone https://github.com/BHAWESHBHASKAR/DASH.git
cd DASH
make docker
```

Or step-by-step:

```bash
git clone https://github.com/BHAWESHBHASKAR/DASH.git
cd DASH
./scripts/generate-secrets.sh
docker compose -f deploy/container/docker-compose.yml up -d
```

Ingest a claim with its supporting evidence (after `source deploy/container/.env`):

```bash
curl -X POST http://localhost:8081/v1/ingest \
  -H "x-api-key: $DASH_INGEST_API_KEY" \
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
  -H "x-api-key: $DASH_RETRIEVAL_API_KEY" \
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
- **JWT auth (HS256 + OIDC/JWKS)** with key rotation by `kid`, optional `iss`/`aud` checks, fallback secrets list, per-tenant claim enforcement, and RBAC on every route.
- **Customer-managed encryption keys** for WAL and snapshot at-rest encryption; self-hosted `env` master key or AWS KMS envelope encryption.
- **Open source** — see the `LICENSE` file (the intended license is Apache-2.0, pending confirmation before the first tagged release). Fully auditable core, no vendor lock-in, no telemetry.
- **Operational scripts**: backup, restore, S3 upload/download, recovery drill, failover drill, SLO guard, release-candidate gate, audit chain verifier, SOC 2 evidence collector — see `scripts/`.
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
| RBAC (admin/ingest/retrieve/read_only) | yes | no | limited | yes | partial | no |
| CMEK / at-rest encryption | yes | no | no | no | no | no |
| RAG-specific primitives | yes | no | modules | no | no | no |

## Architecture

DASH is a Rust workspace organized into library crates (`pkg/schema`, `pkg/store`, `pkg/ranking`, `pkg/graph`, `pkg/auth`, `pkg/embeddings`, `pkg/encryption`) and service binaries (`services/ingestion`, `services/retrieval`, `services/indexer`, `services/control-plane`, `services/metadata-router`). Ingested claims are durably written to a write-ahead log, replayed into an in-memory `Claim + Evidence + Edge` store, and indexed for HNSW ANN candidate generation. The retrieval path runs a planner that combines ANN candidates with metadata filters, time-range filters, stance demotion/filtering, and optional graph expansion, then projects results into a citation-bearing response. JWT and scoped-API-key authz is enforced in the transport layer; per-tenant rate limits and a hash-chained audit log are emitted alongside every state change. The full design — including the data model, WAL/snapshot protocol, retrieval planner, and operational model — lives in [`docs/architecture/eme-architecture.md`](docs/architecture/eme-architecture.md).

## Roadmap

Done (in this tree):
- Claim + Evidence + Edge schema, validation, and serde round-trips
- WAL with replay, checkpoints, compaction, and durability guardrails
- HNSW ANN via `usearch` with `DASH_*_ANN_*` tuning
- Retrieval API with `Balanced` and `SupportOnly` stance modes, time-range filtering, optional graph payload
- OpenAI-compatible `/v1/embeddings` with `hash`, `ollama`, and `openai` providers
- Per-tenant authz, scoped keys, revocation, rate limits
- OIDC/JWKS authentication and RBAC (`admin`, `ingest`, `retrieve`, `read_only`)
- Hash-chained audit log with chain verifier
- HS256 JWT with kid rotation, `iss`/`aud`, fallback secrets
- Customer-managed encryption keys (`env` and AWS KMS) for WAL/snapshot at-rest encryption
- Control-plane leader election, shard placement, and failover promotion
- Quorum replication with follower pull and persistent replication offset
- Disk-first segment serving tier for large tenants
- Object-storage (S3) backup/restore
- Helm chart with managed-cloud scaffolding
- OpenAPI 3.0 spec and SDK quick-start docs
- SDKs: Python, Go, TypeScript, C#, Java, Kotlin
- Benchmark suite with CI regression guard
- Docker Compose, systemd unit files, and Ollama overlay

Next (active development):
- Larger-scale ANN recall/quality tuning and benchmarking at 10M+ claim corpora
- Replicator promotion to synchronous multi-region quorum
- Fully managed cloud control plane with usage-based metering
- Web dashboard / cloud console
- gRPC API surface alongside REST

## Contributing

Contributions are welcome. See [`CONTRIBUTING.md`](CONTRIBUTING.md) for the workflow, code standards (clippy enforced at `-D warnings`, fmt enforced in CI), RFC process, and "good first issue" list. All new code must pass `cargo test --workspace` and `./scripts/ci.sh`.

## License

DASH is source-available software. The intended release license is Apache-2.0, pending confirmation before the first tagged release. Until a `LICENSE` file is added at the repository root, the actual terms are those stated in the repository's `README.md` and `CONTRIBUTING.md`.
