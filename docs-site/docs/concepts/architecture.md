# Architecture

DASH is a Rust workspace of shared library crates and four service binaries. This page describes the topology as the code implements it today, the data flow, and the concurrency model. Planned designs (consensus replication, sharded clusters) are described in the [master plan](https://github.com/BHAWESHBHASKAR/DASH/blob/main/docs/plans/2026-10-09-production-readiness-master-plan.md) and are not available yet.

## Topology

```text
                  clients (SDKs, curl, OpenAI clients)
                       │                     │
            POST /v1/ingest*          POST /v1/retrieve
                       │              POST /v1/embeddings
                       ▼                     ▼
              ┌────────────────┐     ┌────────────────┐
              │   ingestion    │     │   retrieval    │
              │  :8081         │     │  :8080         │
              │  owns the WAL  │────►│  read replica  │
              │  redb mirror   │ HTTP│  in-memory     │
              │  /internal/    │ poll│  store + ANN   │
              │   replication  │     │  redb mirror   │
              └───────┬────────┘     └────────────────┘
                      │ segments
              ┌───────▼────────┐     ┌────────────────┐
              │ segment-       │     │ control-plane  │
              │ maintenance    │     │ :8090          │
              │ daemon         │     │ placement,     │
              └────────────────┘     │ file lease     │
                                     └────────────────┘
```

- **ingestion** (`services/ingestion`) validates writes, appends them to the WAL, updates its in-memory store, mirrors to redb, optionally publishes index segments, and serves the WAL to followers over `/internal/replication/*`.
- **retrieval** (`services/retrieval`) answers `/v1/retrieve` and `/v1/embeddings`. It does not accept writes; it follows the ingestion service by polling its replication endpoint (`DASH_RETRIEVAL_REPLICATION_SOURCE_URL`) and applying WAL deltas into its own in-memory store.
- **control-plane** (`services/control-plane`) holds shard placement state (CSV, optionally persisted with a SHA-256 checksum), runs a file-lease leader election, and exposes placement and failover-promotion endpoints. Ingestion and retrieval can consult it via `DASH_ROUTER_CONTROL_PLANE_URL` or read a placement file directly.
- **metadata-router** (`services/metadata-router`) is a library, not a process: shard placement types, consistent-hash routing, and read/write route resolution used by the services.
- **indexer** (`services/indexer`) is a library plus the `segment-maintenance-daemon` binary: it builds immutable segments with manifests and checksums, plans and applies compactions, and garbage-collects unreferenced files.

There is no consensus protocol. Writes go to a single ingestion process per WAL; failover is an operator-driven placement change.

## Library crates

| Crate | Responsibility |
|---|---|
| `pkg/schema` | Claim, Evidence, ClaimEdge, Citation types; validation; tokenization helpers |
| `pkg/store` | `InMemoryStore`, `FileWal`, `DiskBackedStore` (redb), the ANN graph, metrics, GPU stub |
| `pkg/ranking` | Score computation: confidence, stance, source quality, contradiction penalty |
| `pkg/graph` | Graph expansion over claim edges; support-path and contradiction-depth reasoning |
| `pkg/auth` | HS256 JWT, OIDC/JWKS validation, roles, SHA-256 helper |
| `pkg/embeddings` | `EmbeddingProvider` trait; hash, Ollama, OpenAI providers; circuit breaker |
| `pkg/encryption` | AES-256-GCM envelope library; **not used by any service** |
| `services/common` | Shutdown signaling, secret validation, logging init |

### Storage inside `pkg/store`

- `InMemoryStore`: hash-map based, holds claims, evidence, edges, vectors and lexical, entity, embedding-id and temporal indexes. It is the source of truth while a process runs.
- `FileWal`: the append-only line-oriented log with snapshot checkpoints. See [Persistence](persistence.md).
- `DiskBackedStore`: a redb mirror, on by default when a WAL path is set.
- ANN: a per-tenant multi-level HNSW-style graph implemented in `pkg/store/src/ann.rs`. The `usearch` crate is declared in `Cargo.toml` but no code uses it. The `gpu-backend` feature is a stub; scoring runs on the CPU. Recall and scale characteristics have not been measured in a reproducible CI job; the insert path is known to scan all stored vectors per level (register IDX-01).

## Data flow

```text
client ── POST /v1/ingest ──► ingestion
                               1. parse JSON, check credential (embeds a vector if none given)
                               2. validate bundle (pkg/schema, store)
                               3. append WAL records (claim, evidence, edges, vector)
                               4. mirror to redb, update in-memory indexes and ANN
                               5. optional checkpoint; optional audit line
                               6. respond 200 with ingested_claim_id and commit fields
retrieval ◄── poll /internal/replication/wal ── ingestion
   │  apply delta (or full export when behind)
   ▼
client ── POST /v1/retrieve ─► retrieval
                               embed query (unless query_embedding given)
                               candidates: lexical + entity + temporal + ANN
                               filter by tenant, time_range, stance_mode
                               rank, attach citations, optional graph expansion
                               respond 200
```

Because retrieval is a polling follower, a write is visible on retrieval only after the next poll (250 ms in the compose file, 1000 ms default). Note that step 1 currently computes embeddings before authentication (register SEC-09); v0.3.0 moves authentication first.

## Concurrency model

Each service accepts connections on a `std::net::TcpListener` and hands them to a bounded queue served by a fixed pool of worker threads (`DASH_INGEST_HTTP_WORKERS` / `DASH_RETRIEVAL_HTTP_WORKERS`, queue `workers * 64` by default; a full queue answers 503). Requests are HTTP/1.1 with `Connection: close`. The ingestion runtime is protected by one mutex, so writes are serialized. The retrieval store is behind a read-write lock. See [ADR-003](../reference/architecture-decisions.md#adr-003-why-stdnettcpstream-over-axumreqwest) for the rationale and the [issue register](https://github.com/BHAWESHBHASKAR/DASH/blob/main/docs/plans/2026-10-09-issue-register.md) (ROB-* items) for known robustness gaps in this hand-written transport, such as per-read rather than per-request timeouts and an unbounded JSON nesting depth on the retrieval parser.

Horizontal scaling today means adding retrieval followers that poll one ingestion process. Multiple writers over the same WAL or redb file are not supported (redb holds a file lock).
