# Architecture decisions

DASH is shaped by a small set of explicit, dated decisions. Each is recorded as an Architecture Decision Record (ADR) — a one-page document that captures the context, the options considered, the decision, and the consequences. The current ADRs are summarized below; the full source-of-truth ADRs live in `docs/rfcs/` in the source tree.

## ADR-001: Why redb over sled/rocksdb

**Status:** Accepted. **Date:** 2026-05-14.

### Context

DASH needs an embedded, single-file, ACID key-value store. The options considered:

- **`sled`** — pure Rust, async, lock-free. Active development, but the on-disk format has had breaking changes between minor versions, and the project has historically struggled with stability.
- **`rocksdb`** — battle-tested, the de-facto standard for embedded KV. C++ core, vendored as a static library. The CGo boundary is a known pain point in Rust FFI; the build time is a multi-minute hit.
- **`redb`** — pure Rust, ACID, single-file, B-tree-based. Smaller community, but a clean API surface and a forward-compatible on-disk format.

### Decision

Use `redb`. The decision is justified by:

1. **Pure Rust** — no FFI boundary, no C++ to vendor, no CGo in the build.
2. **Single-file** — backups are a `cp`, replication is a length-prefixed log, deploys are a single artifact.
3. **Forward-compatible format** — `redb`'s on-disk format has been stable across versions; the migration story is well-documented.
4. **ACID** — the on-disk representation is recoverable after a crash without manual WAL replay.

### Consequences

- The single-process write lock is a constraint. We accept it because DASH is designed to scale horizontally with more processes, not more concurrency inside a process. See [Scaling → Horizontal scaling](../operations/scaling.md#horizontal-scaling).
- The PR 1 (additive, default off) and PR 3 (replication) plan is built around redb's lock semantics. See [Persistence](../concepts/persistence.md) and [Scaling → redb PR 3 replication](../operations/scaling.md#redb-pr-3-replication).
- A migration off redb is **not** in the roadmap. The commitment is durable.

## ADR-002: Why semantic-first over lexical-first

**Status:** Accepted. **Date:** 2026-05-21.

### Context

The retrieval ranking can be lexical-first (BM25 dominant), semantic-first (cosine dominant), or a weighted hybrid. The three modes have different cost / quality trade-offs.

### Decision

Default to **semantic-first** (`hybrid_alpha = 0.7`). The lexical/BM25 score is a small tie-breaker.

The decision is justified by:

1. **The use cases are semantic.** The legal, medical, financial, and enterprise RAG workloads that DASH targets are dominated by paraphrastic queries — the user does not know the exact term of art, and the corpus contains it.
2. **The differentiator is dense.** DASH's claim is *citation-grade retrieval*. A semantic-first ranking is the one that surfaces the right *claim* most often; the citations are a property of the claim, not a property of the query.
3. **Lexical-first is available as an option.** A query for "Section 230" is dominated by exact-term recall. `hybrid_alpha = 0.0` switches to lexical-first. The default is a default, not a constraint.

### Consequences

- The `retrieve_semantic` method is the canonical read path. `retrieve` (lexical-only) remains for callers who want it.
- The performance cost is real. The ANN search is a CPU-bound kernel; the reranker adds lexical scoring on top. The `perf_bench` suite measures the two paths separately. See [Benchmarks](benchmarks.md).
- The hybrid weight is a per-request knob (`hybrid_alpha`). There is no global "lexical-first" deployment mode in the current release.

## ADR-003: Why `std::net::TcpStream` over axum/reqwest

**Status:** Accepted. **Date:** 2026-05-28.

### Context

DASH's HTTP services could be built on top of `axum` (the de-facto Rust web framework) running on `tokio`, or on the standard library's `std::net::TcpStream` with a thread-per-connection model.

### Decision

Use `std::net::TcpStream` with a thread-per-connection model. Configure the worker count with `DASH_INGEST_WORKERS` / `DASH_RETRIEVAL_WORKERS`.

The decision is justified by:

1. **The runtime is small.** DASH's HTTP surface is a handful of routes, all synchronous, all fast. The benefit of `axum` (declarative routing, middleware composition, async handlers) is real for a large API surface; it is overkill for ours.
2. **The dependency surface is small.** `axum` pulls in `tokio`, `hyper`, `tower`, and a long transitive tail. `std::net::TcpStream` adds zero dependencies. The build time matters for CI.
3. **The concurrency model is explicit.** A thread-per-connection model with a known worker count is easy to reason about. The alternative — a tokio runtime with N worker threads and M concurrent connections — is harder to model under load.
4. **The throughput is sufficient.** A 4-vCPU host can saturate a 10 Gbps NIC with a thread-per-connection model, given the right worker count. We do not need the throughput ceiling that `axum`/`tokio` provides.

### Consequences

- The transport is single-threaded at the request level. The worker count is a config knob, not a runtime decision.
- There is no async/await in the request path. The `pkg/store` APIs are sync.
- A migration to `axum`/`tokio` is **not** on the roadmap. If a future feature requires async I/O (e.g. an outbound `reqwest` call to an embedding provider), the migration will be additive and scoped to the specific feature.
- The half-built axum runtime paths in both services were removed in the [Unreleased] changelog cycle. The `TransportRuntime::Axum` branch that exited with an error when the `async-transport` feature was absent is gone; the runtime path is now a single direct call to `serve_http_with_workers`.

## ADR-004: Why we don't ship a gRPC API

**Status:** Accepted. **Date:** 2026-06-04.

### Context

DASH could expose a gRPC API alongside the HTTP API, or as a replacement.

### Decision

HTTP only. No gRPC.

The decision is justified by:

1. **The OpenAI drop-in is the entry point.** gRPC is not OpenAI-compatible. Adding gRPC as a parallel API would split the audience between two protocols and require maintaining two wire-level definitions.
2. **The wire format is simple.** The `IngestRequest` and `RetrieveRequest` shapes are small. A gRPC `protobuf` would not be significantly more efficient than the JSON over HTTP we have today.
3. **The ecosystem is HTTP-first.** The five SDKs (`dash-py`, `dash-go`, `dash-ts`, `dash-java`, `dash.NET`) are all built on the HTTP surface. A gRPC SDK would be a parallel investment with no clear ROI.
4. **The internal services are HTTP.** The retrieval service already calls the embeddings service over HTTP. The pattern is consistent.

### Consequences

- A future gRPC API is **not** on the roadmap. The commitment is HTTP.
- Streaming responses (e.g. for very large result sets) are **not** supported. The `top_k` cap of 100 bounds the response size.
- WebSocket and Server-Sent Events are also **not** supported. The retrieval path is request/response.

## ADR-005: OpenAI drop-in over a custom protocol

**Status:** Accepted. **Date:** 2026-06-08.

### Context

DASH could have shipped a custom wire protocol for `/v1/embeddings`, or it could be a drop-in for the OpenAI v1 API.

### Decision

OpenAI drop-in. The `/v1/embeddings` endpoint is byte-compatible with the OpenAI v1 spec, including the error envelope, the `usage` field, and the `encoding_format` switch.

The decision is justified by:

1. **The audience is already using OpenAI.** The vast majority of RAG frameworks (`langchain`, `llama-index`, `semantic-kernel`, the official `openai` SDKs) have an OpenAI client. A drop-in means a one-line change at the call site (`OPENAI_API_BASE`).
2. **The compatibility is exercised.** 17 tests in `tests/retrieval/integration_embeddings.rs` cover wire-format compatibility, error envelope, and HTTP-level integration with the `openai` Python SDK. The compatibility is not aspirational — it is tested on every PR.
3. **The cost is bounded.** The OpenAI spec is small (request, response, error envelope). Implementing it is a few hundred lines. The maintenance cost is the wire-format compatibility test, which is already in CI.
4. **The future is migration-friendly.** A user can run DASH with `DASH_EMBEDDING_PROVIDER=openai` to proxy through DASH to OpenAI, then flip to `=ollama` to go on-prem. The drop-in makes the migration a config change, not a code change.

### Consequences

- The wire format is locked to the OpenAI v1 spec. DASH-specific extensions go in HTTP headers (e.g. `dash_request_id`), not in the JSON body.
- A divergence from the OpenAI spec is a bug. The compatibility tests are a hard gate on PRs.
- The OpenAI-specific fields (`user`, `encoding_format`) are honored even if they are not used by the configured provider.
- The `model` field is a free-form string. DASH advertises the models it can serve; an unrecognized model returns `dash_invalid_model`.
