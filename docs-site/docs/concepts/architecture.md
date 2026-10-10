# Architecture

DASH is a Rust workspace of shared library crates, four service binaries and two offline tools (`wal-inspect`, `audit-verify`). This page describes the topology as the code implements it in 0.3.0 (unreleased), the data flow, and the concurrency model. Planned designs (consensus replication, sharded clusters) are described in the [master plan](https://github.com/BHAWESHBHASKAR/DASH/blob/main/docs/plans/2026-10-09-production-readiness-master-plan.md) and are not available yet.

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
- **control-plane** (`services/control-plane`) holds shard placement state (CSV, optionally persisted with a SHA-256 checksum), runs a durable, fenced file-lease leader election, and exposes token-authenticated placement and failover-promotion endpoints (promotion is refused unless the replica reported zero lag). Ingestion and retrieval can consult it via `DASH_ROUTER_CONTROL_PLANE_URL` or read a placement file directly.
- **metadata-router** (`services/metadata-router`) is a library, not a process: shard placement types, consistent-hash routing, and read/write route resolution used by the services.
- **indexer** (`services/indexer`) is a library plus the `segment-maintenance-daemon` binary: it builds immutable segments with manifests and checksums, plans and applies compactions, and garbage-collects unreferenced files.

There is no consensus protocol. Writes go to a single ingestion process per WAL; failover is an operator-driven placement change.

## Library crates

| Crate | Responsibility |
|---|---|
| `pkg/schema` | Claim, Evidence, ClaimEdge, Citation types; validation; tokenization helpers |
| `pkg/store` | `InMemoryStore`, `FileWal` (checksummed records, commit groups, generations), `DiskBackedStore` (redb), the ANN graph, metrics, GPU placeholder |
| `pkg/ranking` | Score computation: confidence, stance, source quality, contradiction penalty |
| `pkg/graph` | Graph expansion over claim edges; support-path and contradiction-depth reasoning |
| `pkg/auth` | HS256 JWT, OIDC/JWKS validation, roles, SHA-256 helper |
| `pkg/embeddings` | `EmbeddingProvider` trait; hash, Ollama, OpenAI providers; circuit breaker |
| `pkg/encryption` | AES-256-GCM envelope library; **not used by any service** |
| `services/common` | Deny-by-default auth policy and rate limiter (`policy`), audit chain writer and verifier (`audit`), secret validation, shutdown signaling, logging init |

### Storage inside `pkg/store`

- `InMemoryStore`: hash-map based, holds claims, evidence, edges, vectors and lexical, entity, embedding-id and temporal indexes. It is the source of truth while a process runs.
- `FileWal`: the append-only line-oriented log with snapshot checkpoints. See [Persistence](persistence.md).
- `DiskBackedStore`: a redb mirror, on by default when a WAL path is set.
- Vector index: one `TenantVectorIndex` per tenant (`pkg/store/src/vector_index.rs`). It starts as an exact **flat** index (contiguous normalised `f32` rows, SIMD-friendly dot product) and converts itself to a [`usearch`](https://github.com/unum-cloud/usearch) **HNSW** (cosine metric, `i8` scalar quantisation) once the tenant holds more than `DASH_*_VECTOR_FLAT_THRESHOLD` vectors (default 8192). The HNSW returns about 50 candidates (`DASH_*_VECTOR_RERANK`) that are re-scored with exact `f32` cosine against the full-precision vectors in `claim_vectors`, so the quantised index costs roughly a third of an `f32` HNSW and recall does not pay for it. Claim ids are interned to dense `u64` keys per tenant; removing or replacing a vector is a usearch soft delete whose slot is reused (memory does not shrink after deletes).
- Filtered vector search: time-range and allowed-claim-id filters are passed to the index as a predicate. When the allowed set is no larger than the flat threshold it is scanned exactly instead (measured faster and exact, ADR 0003); a larger set goes through the HNSW with the predicate. Every search stays inside the tenant's own index.
- Cold start: the vector index is **not persisted**. WAL replay and the redb bulk load collect the vectors and then build each tenant's index once, multi-threaded. The cost grows with the vector count (see [Persistence](persistence.md) for measured numbers); persisting or memory-mapping the index is a follow-up.
- The `gpu-backend` feature is a placeholder that never produces a GPU engine; scoring runs on the CPU.

## Data flow

```text
client ── POST /v1/ingest ──► ingestion
                               1. read request (bounded), parse JSON, authorize (tenant + role, rate limit)
                               2. embed the claim if no vector was given
                               3. validate bundle (pkg/schema, store)
                               4. append one WAL commit group (claim, evidence, edges, vector)
                               5. mirror to redb, update in-memory indexes and ANN
                               6. optional checkpoint; optional audit line
                               7. respond 200 with ingested_claim_id and commit fields
retrieval ◄── poll /internal/replication/wal ── ingestion
   │  apply delta (or full export when behind)
   ▼
client ── POST /v1/retrieve ─► retrieval
                               authorize, then embed query (unless query_embedding given)
                               candidates: lexical + entity + temporal + ANN
                               filter by tenant, time_range, stance_mode
                               rank, attach citations, optional graph expansion
                               respond 200
```

Because retrieval is a polling follower, a write is visible on retrieval only after the next poll (250 ms in the compose file, 1000 ms default). Authentication and authorization run before any embedding provider call (register SEC-09, fixed in 0.3.0). When the leader checkpoints, its WAL generation changes and followers resync from a full export.

## Concurrency model

All three services use one shared server implementation, the `dash-http` crate (`pkg/http`): a strict request parser, response renderer and `std::thread` server. Each service passes it a handler closure; the server accepts connections on a `std::net::TcpListener` and hands them to a bounded queue served by a fixed pool of worker threads (`DASH_INGEST_HTTP_WORKERS` / `DASH_RETRIEVAL_HTTP_WORKERS`, queue `workers * 64` by default; a full queue answers 503). Requests are HTTP/1.1 with `Connection: close`. The ingestion runtime is protected by one mutex, so writes are serialized. The retrieval store is behind a read-write lock. See [ADR-003](../reference/architecture-decisions.md#adr-003-why-stdnettcpstream-over-axumreqwest) for the rationale and the [issue register](https://github.com/BHAWESHBHASKAR/DASH/blob/main/docs/plans/2026-10-09-issue-register.md) (ROB-* items) for remaining robustness gaps in this hand-written transport (now a single module, so a later move to hyper/axum replaces one crate). Since 0.3.0 requests have a whole-request deadline (`DASH_HTTP_REQUEST_TIMEOUT_MS`), bounded headers and bodies, and a depth-limited JSON parser.

Horizontal scaling today means adding retrieval followers that poll one ingestion process. Multiple writers over the same WAL or redb file are not supported (redb holds a file lock).
