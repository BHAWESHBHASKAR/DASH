# Architecture

DASH is two HTTP services backed by a shared library crate (`pkg/store`) and a shared type crate (`pkg/schema`). This page describes the topology, the responsibilities of each component, the data flow, and the persistence stack.

## The two services

```text
                        ┌───────────────────────┐
                        │      Client(s)        │
                        │  SDKs / curl / OpenAI │
                        └───────────┬───────────┘
                                    │
                ┌───────────────────┴───────────────────┐
                │                                       │
        POST /v1/ingest                        POST /v1/retrieve
        POST /v1/embeddings                    POST /v1/embeddings
                │                                       │
        ┌───────▼────────┐                    ┌────────▼────────┐
        │   ingestion    │                    │    retrieval    │
        │  (port 8081)   │                    │   (port 8080)   │
        │                │                    │                 │
        │ • validate     │                    │ • embed query   │
        │ • write WAL    │                    │ • ANN search    │
        │ • append redb  │                    │ • lexical rerank│
        │ • audit log    │                    │ • stance filter │
        │ • emit metrics │                    │ • emit metrics  │
        └───────┬────────┘                    └────────┬────────┘
                │                                      │
                │         ┌────────────────┐           │
                └────────►│   pkg/store    │◄──────────┘
                          │                │
                          │ • InMemoryStore│
                          │ • DiskBackedStore
                          │ • ANN (usearch)│
                          │ • redb tables  │
                          │ • WAL          │
                          └────────┬───────┘
                                   │
                          ┌────────▼───────┐
                          │   Persistence  │
                          │  redb + WAL    │
                          │  (per-tenant   │
                          │   key spaces)  │
                          └────────────────┘
```

The two services are intentionally independent. They share the type system (`pkg/schema`) and the store implementation (`pkg/store`), but they do not share a process or a network port. A deployment can scale ingestion replicas separately from retrieval replicas; the only thing they have to agree on is the shape of the `redb` files and the wire format on disk.

## Component responsibilities

### `services/ingestion`

- Accepts `POST /v1/ingest` with a `Claim`, an evidence `Vec`, and an edges `Vec`.
- Validates the bundle against the schema (`claim_id` uniqueness, `tenant_id` consistency, `confidence ∈ [0, 1]`, etc.).
- Appends a record to the **write-ahead log** (WAL).
- If `DASH_INGEST_PERSISTENCE_PATH` is set, appends the bundle to the **redb** file via `DiskBackedStore::insert`.
- Emits an `AuditEvent` (hash-chained) recording the operator, the tenant, the bundle IDs, and the SHA-256 of the canonical form.
- Emits Prometheus metrics: `dash_ingest_requests_total`, `dash_ingest_latency_seconds`, `dash_ingest_bundle_size`.

### `services/retrieval`

- Accepts `POST /v1/retrieve` with a `tenant_id`, a `query`, an optional `query_vector`, a `top_k`, an optional `time_range`, and a `stance_mode`.
- Computes the query embedding via the configured `EmbeddingProvider` (default: `HashEmbeddingProvider`; alternatives: `ollama`, `openai`, custom).
- Performs a per-tenant **ANN search** (`usearch`) to get the top-*N* candidate claims.
- **Reranks** the candidates with the lexical/BM25 score, applies the `time_range` filter, and the `stance_mode` filter.
- Returns `{ claim, score, supports, contradicts, citations[] }` for each surviving result.

The retrieval service also serves:

- `POST /v1/embeddings` — OpenAI v1 wire-compatible.
- `GET /v1/health` — liveness/readiness.
- `GET /metrics` — Prometheus exposition.

### `pkg/store`

The shared store. Exposes:

- `InMemoryStore` — `HashMap`-backed, in-process. Fast, volatile, the default for tests and small deployments.
- `DiskBackedStore` — `redb`-backed, durable, opt-in via `DASH_*_PERSISTENCE_PATH`. Wraps `InMemoryStore`; the in-memory layer is the source of truth during a process's lifetime, and redb is the snapshot.
- `IndexBuilder` — pluggable ANN index. The default implementation is `usearch` (HNSW).
- `FileWal` — the append-only write-ahead log with replay.
- `audit::hash_chain` — the SHA-256-chained `AuditEvent` recorder.

### `pkg/schema`

The wire-level types. Every HTTP request body, every redb record, and every JSON line in the audit log deserializes to a type defined here. The types are `Serialize`/`Deserialize`-derived; the optional fields are `#[serde(default)]` so old payloads stay forward-compatible.

## Data flow: ingest → store → retrieve

```text
client
  │
  │  POST /v1/ingest
  ▼
ingestion service
  │
  │ 1. validate
  │ 2. write WAL (fsync, durability guard on)
  │ 3. if redb path set: DiskBackedStore::insert
  │ 4. AuditEvent::record (hash chain)
  │ 5. respond 202 Accepted
  ▼
redb + WAL on disk
  │
  │  (on retrieval, or on restart during WAL replay)
  ▼
InMemoryStore rebuilt in the retrieval service
  │
  │  POST /v1/retrieve
  │      → EmbeddingProvider::embed(query)
  │      → ANN top-N candidates
  │      → lexical rerank
  │      → stance + time_range filter
  │      → response
  ▼
client
```

## Persistence architecture (redb + WAL)

DASH has a **two-layer persistence story**:

1. **WAL** — the append-only write-ahead log. Every mutation is recorded as a length-prefixed, CRC-32c-checked record. The WAL is the *first* thing that gets written; the in-memory state and the redb snapshot are *consequences* of the WAL. Replay reconstructs the in-memory state on restart.

2. **redb** — the on-disk snapshot. `redb` is a pure-Rust, ACID, single-file embedded database built on top of B-trees. DASH uses redb PR 1: a single `redb` file per service (one for ingestion, one for retrieval), with per-tenant key spaces. The `DiskBackedStore` is additive; if `DASH_*_PERSISTENCE_PATH` is unset, DASH runs in WAL-only mode and the pre-redb behavior is preserved bit-for-bit.

The reasons for choosing redb are documented in [ADR-001: Why redb over sled/rocksdb](../reference/architecture-decisions.md#adr-001-why-redb-over-sledrocksdb). The full 3-PR persistence plan is in `docs/plans/2026-06-13-redb-persistence-design.md` in the source tree.

## Concurrency model

Both services are single-threaded at the request-handling level. They use `std::net::TcpListener` and a thread-per-connection model with a configurable worker count (`DASH_*_WORKERS`). The reasoning for staying on `std::net::TcpServer` rather than reaching for `axum` / `tokio` is in [ADR-003](../reference/architecture-decisions.md#adr-003-why-stdnettcpstream-over-axumreqwest).

This is a deliberate constraint, not an oversight: DASH is built so that horizontal scale comes from **more processes**, not more concurrency inside a process. The redb file lock guarantees that only one writer is ever active per file, and the WAL replay is single-pass, so a single process can saturate a single NVMe drive. To go faster, run more processes (with a load balancer in front), and let redb's cross-process primitives coordinate.
