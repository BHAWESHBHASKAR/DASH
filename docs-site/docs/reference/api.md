# HTTP API

DASH exposes plain HTTP/1.1 with JSON bodies (one request per connection; the server closes the connection after each response). There is no gRPC, GraphQL or WebSocket interface.

This page documents the routes that exist in the code (0.3.0, unreleased). Anything not listed here is not implemented; see [Planned API](planned-api.md) for the ideas that earlier documentation described as if they were available. Behavior that changed in 0.3.0 is marked **0.3.0**; see the [changelog](../about/changelog.md) and the upgrade steps.

## Services and base URLs

| Service | Default bind | Purpose |
|---|---|---|
| ingestion | `127.0.0.1:8081` | Writes. Owns the WAL. |
| retrieval | `127.0.0.1:8080` | Reads, and the embeddings endpoint. Follows ingestion by replication. |
| control-plane | `127.0.0.1:8090` | Placement and leader state. |

The compose file publishes `8081`, `8080` and `8090` on the host. There are no hosted staging or production base URLs.

## Authentication

Each service has its own credentials; an ingestion key does not work on retrieval. Every route except the health probes (`/health`, `/live`, `/ready` and their `/v1/` forms) requires a credential.

```text
x-api-key: <api key>
Authorization: Bearer <api key>        # equivalent
Authorization: Bearer <JWT>            # when a JWT secret or OIDC is configured
```

There is no `DashKey` scheme. A bearer token with three dot-separated parts is judged by the JWT verifier (when one is configured) and never falls through to API-key matching. A JWT must carry the tenant in `tenant_id` (or a `tenants` / `tenant_ids` array; `*` only when wildcard tenants are enabled), an `exp`, and, for role checks, the roles in `dash_roles` (the claim name is configurable). See [Configuration](configuration.md#authentication-and-authorization) for keys, scoped keys, roles, revocation and JWT settings.

**Deny by default (0.3.0).** A service started with no API key, scoped key, JWT secret or OIDC provider refuses to start unless `DASH_INSECURE_DEV_MODE=1`; in dev mode it accepts every request and binds loopback only. A configuration that only sets a JWT secret rejects requests without a valid token (there is no open API-key branch).

Authorization is per tenant. The tenant is taken from the **request** (`claim.tenant_id` on ingest, `tenant_id` on retrieve) and then checked against the credential's tenant scope and the service allowlist. Routes that are not tenant-scoped (`/metrics`, `/debug/placement`, `/v1/embeddings`) still need a valid credential with the right role.

| Role | Allows |
|---|---|
| `admin` | every route |
| `ingest` | `/v1/ingest*`, `DELETE /v1/claims/{claim_id}`, `DELETE /v1/evidence/{evidence_id}` |
| `retrieve` | `/v1/retrieve`, `/v1/embeddings` |
| `read_only` | everything `retrieve` allows, plus `/metrics` and `/debug/*` (which additionally need `admin` or an unscoped credential, see below) |

`ingest` and `retrieve` are independent; neither implies `read_only`. Legacy unscoped API keys get `DASH_*_API_KEY_DEFAULT_ROLES` (default: `ingest` on ingestion, `retrieve` on retrieval). A JWT **without** a roles claim gets no roles (403 on every role-checked route) unless `DASH_*_JWT_DEFAULT_ROLES` is set. `/metrics` can be exempted from authentication with `DASH_METRICS_PUBLIC=1`.

| Route | Required role |
|---|---|
| retrieval `POST`/`GET /v1/retrieve`, `POST /v1/embeddings` | `retrieve` |
| retrieval `/metrics`, `/debug/*`; ingestion `/metrics`, `/debug/*` | `read_only` and (`admin` or an unscoped credential); tenant-scoped keys get 403 |
| ingestion `POST /v1/ingest`, `/v1/ingest/raw`, `/v1/ingest/document`, `/v1/ingest/batch` | `ingest` |
| ingestion `DELETE /v1/claims/{claim_id}`, `DELETE /v1/evidence/{evidence_id}` | `ingest` on the `tenant_id` query parameter |
| ingestion `DELETE /v1/tenants/{tenant_id}` | `admin` for that tenant (an admin of another tenant gets 403) |
| ingestion `/internal/replication/*` | not a role: `x-replication-token` header |
| control plane `/v1/control-plane/*` except health and ready | not a role: `Authorization: Bearer <control-plane token>` |

| Condition | Status | Body |
|---|---:|---|
| Missing or unknown key, invalid or expired JWT, revoked key or `jti`, unreachable IdP with no cached keys | 401 | `{"error":"missing or invalid API key"}` (or `invalid JWT`, `JWT expired`, `API key revoked`, `JWT revoked`, `invalid OIDC token`, `OIDC provider unreachable`) |
| Valid credential, tenant or role not permitted | 403 | `{"error":"tenant is not allowed for this API key"}` (or `... for this JWT`, `tenant is not allowed by service policy`, `role is not allowed for this API key`, `role is not allowed for this JWT`) |
| Per-tenant rate limit exceeded | 429 | `{"error":"rate limit exceeded"}` with a `Retry-After` header (seconds) |

Error responses use fixed messages and never repeat claim values or key material.

## Errors

Errors from the DASH routes are a JSON object with one field, and there is no error-code enum or request id (the OpenAI-compatible `/v1/embeddings` route uses OpenAI's error shape, below):

```json
{ "error": "validation error: ..." }
```

| Status | Meaning in practice |
|---:|---|
| 400 | Malformed JSON (nesting is depth-limited), failed validation, bad query parameter, wrong `Content-Type` (POST bodies must include `application/json`), invalid vector, unknown stance or relation, `top_k` or other request bound exceeded. Unknown claim references are 400 (`missing claim`). |
| 401 / 403 | See [Authentication](#authentication). |
| 404 | Unknown path, or an unknown `commit_id` on the replication endpoints. |
| 405 | Wrong method for a known path. |
| 409 | State conflict from the store (for example a `claim_id` that already belongs to another tenant; the message does not name that tenant), or a stale `expected_epoch` on the control plane. |
| 408 | The whole request was not received within `DASH_HTTP_REQUEST_TIMEOUT_MS` (default 10 s). |
| 413 | **0.3.0.** `Content-Length` over 16 MiB (ingestion and retrieval); rejected before the body is read. |
| 429 | **0.3.0.** Per-tenant rate limit exceeded; carries `Retry-After`. |
| 431 | **0.3.0.** Request line or a header line over 8 KiB, header block over 32 KiB, or more than 100 headers. |
| 500 | Persistence or internal error. |
| 501 | **0.3.0.** `Transfer-Encoding` (chunked bodies) is not supported; send a `Content-Length` body. |
| 502 | Embedding provider returned an error. Ingest: `{"error":"embedding_upstream_error"}`; retrieve: `{"error":"embedding_provider_error"}`; `/v1/embeddings`: an OpenAI-shaped error with code `embedding_provider_error`. Details are logged, not returned. |
| 503 | Worker queue full, placement rejected the request (retrieval answers with the short codes `placement_unavailable`, `wrong_node` or `read_consistency_unavailable`; ingestion explains: node is not the shard leader, consistency unavailable, **placement is stale**), audit fail-closed gate refused the request (`audit log unavailable`), embedding provider unavailable (`embedding_unavailable`), `/ready` not ready (disk unavailable, replication lagging, stale or not yet synced), or the control plane is not the leader. |

## Retrieval service

### `POST /v1/retrieve` and `GET /v1/retrieve`

Requires role `retrieve` on the request's tenant. Authorization runs before any embedding provider call. `POST` takes JSON; `GET` takes the same fields as query parameters (lists as comma-separated values, time range as `from_unix` and `to_unix`).

| Field | Type | Default | Notes |
|---|---|---|---|
| `tenant_id` | string | required | Must be non-empty. |
| `query` | string | required | Must be non-empty, at most 8 KiB. Embedded with the configured provider unless `query_embedding` is given. |
| `top_k` | positive integer | `5` | Upper bound `DASH_RETRIEVAL_MAX_TOP_K` (default 1000); larger values get 400. |
| `stance_mode` | `balanced` \| `support_only` | `balanced` | `support_only` drops claims with more contradictions than supports. |
| `time_range` | `{ "from_unix": i64?, "to_unix": i64? }` | none | Filters on event time and validity window. |
| `query_embedding` | float array | none | Pre-computed query vector; finite values, at most 8192 entries. A vector that does not match the tenant's dimension returns no results, never an error or NaN scores. |
| `entity_filters` | string array | `[]` | At most 256 values. |
| `embedding_id_filters` | string array | `[]` | At most 256 values. |
| `return_graph` | boolean | `false` | Include the expanded claim graph. |
| `read_consistency` | `one` \| `quorum` \| `all` | `one` | Only meaningful with placement routing configured. |

```json
{
  "tenant_id": "t1",
  "query": "Company X acquired Company Y",
  "top_k": 5,
  "stance_mode": "support_only",
  "time_range": { "from_unix": 1718300000, "to_unix": 1735609600 }
}
```

Response `200`:

```json
{
  "results": [
    {
      "claim_id": "c1",
      "canonical_text": "Company X acquired Company Y",
      "score": 0.91,
      "claim_confidence": 0.95,
      "confidence_band": "high",
      "dominant_stance": "supports",
      "contradiction_risk": 0.0,
      "supports": 1,
      "contradicts": 0,
      "citations": [
        {
          "evidence_id": "e1",
          "source_id": "news://nyt",
          "stance": "supports",
          "source_quality": 0.95,
          "chunk_id": null,
          "span_start": null,
          "span_end": null,
          "doc_id": null,
          "extraction_model": null,
          "ingested_at": 1718300050
        }
      ],
      "event_time_unix": null,
      "claim_type": null,
      "valid_from": null,
      "valid_to": null,
      "created_at": null,
      "updated_at": null
    }
  ],
  "graph": null,
  "read_policy": "one",
  "read_quorum_met": true,
  "serving_replica": null
}
```

Each result also carries `graph_score`, `support_path_count`, `contradiction_chain_depth`, `temporal_match_mode` and `temporal_in_range` (null when not applicable). Numbers shown are illustrative; scores are formatted with six decimals. With `return_graph: true`, `graph` is `{ "nodes": [...], "edges": [{ "from_claim_id", "to_claim_id", "relation", "strength" }] }`.

### `POST /v1/embeddings`

OpenAI-compatible embeddings. **0.3.0:** requires a credential with the `retrieve` role (or `admin`), and authentication runs before any embedding provider call. See the [Embeddings guide](../guides/embeddings.md).

Request: `{"input": "text" | ["text", ...], "model": "...", "encoding_format": "float" | "base64", "dimensions": N, "user": "..."}`.

- `model` is required and echoed back as a hint; the vectors come from the provider selected by `DASH_EMBEDDING_PROVIDER` (default: deterministic hash embedder).
- `encoding_format` defaults to `float`; `base64` returns little-endian float32 bytes.
- `dimensions` is accepted only if it equals the provider's dimension; otherwise 400 `unsupported_dimensions`. `user` is accepted and ignored.
- At most 2048 inputs, each non-empty, and at most `DASH_EMBEDDING_MAX_TOTAL_CHARS` (default 524288) characters in total.
- **Token-id inputs** (an array of integers or an array of integer arrays) are **rejected by default** with 400 `unsupported_input_type`, because token ids cannot be decoded without the client's tokenizer. With `DASH_EMBEDDING_ALLOW_TOKEN_IDS=1` each id array is embedded as its decimal ids joined by spaces, which is deterministic but not equivalent to embedding the decoded text.

Response `200`: `{"object":"list","data":[{"object":"embedding","index":0,"embedding":[...]}],"model":"...","usage":{"prompt_tokens":N,"total_tokens":N}}`. `usage.prompt_tokens` is an **estimate** (`ceil(characters / 4)` per input, at least 1; the id count for token inputs), not a tokenizer count, and `total_tokens` equals it.

Errors use OpenAI's shape, `{"error":{"message":"...","type":"invalid_request_error","param":"input","code":"empty_input"}}`. Client errors are 400 (`invalid_request_body`, `invalid_input_type`, `empty_input`, `too_many_inputs`, `input_too_long`, `invalid_encoding_format`, `unsupported_dimensions`, `unsupported_input_type`). Provider failures are 503 (`embedding_unavailable`: timeout, open circuit breaker or invalid provider configuration) or 502 (`embedding_provider_error`); the message is the short code, details are logged.

### Health, metrics, debug (retrieval)

| Route | Auth | Notes |
|---|---|---|
| `GET /health`, `/v1/health` | none | `{"status":"ok"}` |
| `GET /live`, `/v1/live` | none | `{"status":"alive"}` |
| `GET /ready`, `/v1/ready` | none | `{"status":"ready"}`, with a `replication` object when a follower is attached. 503 `not_ready` when a configured redb path is unavailable (`disk_unavailable`) or the follower is not ready (`replication_initial_sync_pending`, `replication_lag_exceeded`, `replication_stale`). |
| `GET /metrics` | `read_only` (or none with `DASH_METRICS_PUBLIC=1`) | Prometheus text, including per-route latency histograms, auth and rate-limit counters, replication follower gauges and audit counters. |
| `GET /debug/placement` | `read_only` | Placement route probe (`tenant_id`, `entity_key`). |
| `GET /debug/planner` | `read_only` on the query's tenant | Planner snapshot for a retrieve-style query. |
| `GET /debug/storage-visibility` | `read_only` on the query's tenant | Segment vs WAL visibility for a query. |

## Ingestion service

All writes accept an optional `write_consistency` query parameter (`one` default, `quorum`, `all`) that matters only with placement routing configured. Successful writes return **200** (not 202).

### `POST /v1/ingest`

Requires role `ingest` on the claim's tenant. One claim with its evidence and edges. Authorization runs before any embedding provider call.

```json
{
  "claim": {
    "claim_id": "c1",
    "tenant_id": "t1",
    "canonical_text": "Company X acquired Company Y",
    "confidence": 0.95,
    "event_time_unix": 1718300000,
    "valid_from": 1718300000,
    "valid_to": null,
    "claim_type": "factual",
    "entities": ["Company X", "Company Y"],
    "embedding_vector": null
  },
  "claim_embedding": null,
  "evidence": [
    {
      "evidence_id": "e1",
      "claim_id": "c1",
      "source_id": "news://nyt/2024/05/12/acme-x",
      "stance": "supports",
      "source_quality": 0.95,
      "chunk_id": "chunk-7",
      "span_start": 1024,
      "span_end": 1280,
      "doc_id": "doc-1",
      "extraction_model": "llm-extractor-v1"
    }
  ],
  "edges": [
    {
      "edge_id": "edge-1",
      "from_claim_id": "c1",
      "to_claim_id": "c0",
      "relation": "supports",
      "strength": 0.8
    }
  ]
}
```

- Required: `claim.claim_id`, `claim.tenant_id`, `claim.canonical_text`, `claim.confidence` (0 to 1). Evidence requires `evidence_id`, `claim_id`, `source_id`, `stance`, `source_quality`; edges require `edge_id`, `from_claim_id`, `to_claim_id`, `relation`, `strength`.
- `stance`: `supports`, `contradicts`, `neutral`. `relation`: `supports`, `contradicts`, `refines`, `duplicates`, `depends_on`. `claim_type`: `factual`, `opinion`, `prediction`, `temporal`, `causal`.
- Evidence has no `tenant_id`; it inherits the claim's tenant. The claim has no `extraction_model` field; `extraction_model` is on evidence. There is no `idempotency_key` field or `Idempotency-Key` header on this route. If no embedding is given, one is generated with the configured provider.
- Identifier-like fields (ids, tenant, entity names, source ids) must not contain ASCII control characters (400).
- **Idempotent writes (0.3.0).** Evidence is upserted by `evidence_id`; edges are upserted by `(from_claim_id, to_claim_id, relation)`; re-sending a claim for the same tenant updates it and keeps its vector. Re-sending a bundle, restarting the service, reloading the WAL and replication re-apply all leave exactly one copy, so a retry does not inflate citation counts. A `claim_id` that already belongs to **another** tenant is a 409 whose message does not name that tenant.
- **Atomic bundle (0.3.0).** One single ingest is written to the WAL as one commit group: after a crash the whole bundle is replayed or none of it is.
- **Edge direction (0.3.0).** An edge `from supports to` is evidence for the **target** claim (`to_claim_id`); it no longer credits the author. `from_claim_id` must be the claim in the request. Edges whose endpoints are missing, in another tenant, or equal (self edges) are ignored when ranking, and each distinct source counts once.

Response `200` (`checkpoint_snapshot_records` and `checkpoint_truncated_wal_records` appear when a checkpoint ran; `checkpoint_deferred: true` means the write is durable but a post-commit checkpoint failed and will be retried):

```json
{
  "ingested_claim_id": "c1",
  "claims_total": 1,
  "commit_epoch": null,
  "ack_count": 1,
  "required_acks": 1,
  "commit_status": "replication_quorum_met",
  "checkpoint_triggered": false
}
```

### `POST /v1/ingest/batch`

Requires role `ingest`. `{"commit_id": "optional-string", "items": [ <ingest body>, ... ]}`. Up to `DASH_INGEST_BATCH_MAX_ITEMS` (default 128) items, all for the same tenant. The batch is staged on a detached copy of the store and committed as a unit, so a validation failure in any item leaves nothing applied. Replaying the same `commit_id` with the **same content** returns `idempotent_replay: true` without writing. Reusing a `commit_id` with **different content** is treated as an update: the new content is applied as an upsert over the previous version and the response carries `updated: true` (with `idempotent_replay: false`). Response fields: `commit_id`, `idempotent_replay`, `updated` (only when true), `ingested_claim_ids`, `batch_size`, `claims_total`, plus the commit fields above.

### `POST /v1/ingest/raw`

Requires role `ingest`. Extracts sentence claims from text: `{ "tenant_id", "document_id", "source_id", "text", "extraction_model"?, "claim_confidence"?, "source_quality"?, "min_sentence_chars"?, "max_claims"?, "generate_embeddings"?, "embedding_model"? }`. Ids derived from the tenant and document id carry a hash of the full, untruncated ids, so different (tenant, document) pairs cannot collide, and evidence spans (`span_start`, `span_end`) refer to offsets in the original text. Response includes `document_id`, `commit_id`, `idempotent_replay`, `updated` (only when true), `extracted_count`, `embedding_provider`, `embeddings_generated`, `embedding_dimensions` (when embeddings were generated), `ingested_claim_ids`, `claims_total`.

### `POST /v1/ingest/document`

Requires role `ingest`. Like `/raw` but takes `mime_type` and either `text` or `content_base64`. Only UTF-8 text is parsed by the built-in parser; other MIME types need `DASH_INGEST_DOCUMENT_PARSER_PROVIDER=adapter_command`. Response adds `mime_type` and `parser_provider`.

### Deletes

| Route | Role | Removes |
|---|---|---|
| `DELETE /v1/claims/{claim_id}?tenant_id=...` | `ingest` | The claim, its vector, its evidence and every edge from or to it. |
| `DELETE /v1/evidence/{evidence_id}?tenant_id=...` | `ingest` | Every evidence row with this id on the tenant's claims. The claims stay. |
| `DELETE /v1/tenants/{tenant_id}` | `admin` | All of the tenant's claims with their vectors, evidence and edges, the tenant's vector dimension and index, and batch-commit metadata naming its claims. |

- Path ids are percent-decoded (`+` is a literal plus in the path). `tenant_id` is required in the query for claim and evidence deletes and must not be sent for a tenant delete (400). An empty id is a 400, an extra path segment a 404, any other method on these paths a 405. `write_consistency` is accepted as on every write.
- **Idempotent.** Every delete answers `200`. `deleted` is `false` when there was nothing to remove: an unknown id, an id already deleted, or a claim that belongs to another tenant (the answer does not reveal that it exists). A delete of nothing writes nothing to the WAL.
- **Durable before visible.** A delete that removes something is one checksummed WAL tombstone record (`T2`), framed as a commit group, appended with the same write policy as an ingest before memory, redb, the vector index and the tenant's segments change. Deletes wait for in-flight pipelined ingests to be applied first, so the result equals serial execution in WAL order. Tombstones replicate to followers like any other record; a checkpoint drops the deleted rows for good. Backups and WAL archives taken before the delete still contain the data; see `docs/operations/data-deletion.md`.
- Deleting a tenant's last vector releases its vector dimension, so the tenant may later write vectors of another size.
- A delete does not block later writes: re-sending an ingest (or a batch with the same `commit_id`) after a delete writes the data again, as an update. Deleted ids can be reused.
- With placement routing, a claim delete is routed like an ingest of the stored claim; an evidence or tenant delete must be sent to the leader of every shard of the tenant (a follower answers with the usual wrong-node error).
- Every delete is audit-logged (`delete_claim`, `delete_evidence`, `delete_tenant`) with the counts, including denied requests, and is refused with 503 by the audit fail-closed gate like an ingest.

Response `200`:

```json
{
  "deleted": true,
  "scope": "claim",
  "tenant_id": "t1",
  "claim_id": "c1",
  "claims_deleted": 1,
  "evidence_deleted": 2,
  "edges_deleted": 1,
  "vectors_deleted": 1,
  "claims_total": 41,
  "checkpoint_triggered": false,
  "checkpoint_deferred": false
}
```

`claim_id` appears only for a claim delete and `evidence_id` only for an evidence delete. `claims_total` counts the claims left on the node.

### Health, metrics, debug (ingestion)

Same health, readiness and liveness routes as retrieval (`/ready` also reports ingestion-to-ingestion follower state and a disk problem), plus `GET /metrics` (Prometheus text), `GET /debug/placement` and `GET /debug/document-parser` (parser/extraction provider config). `/metrics` and `/debug/*` need a credential with the `read_only` (or `admin`) role that is also `admin` or unscoped (tenant-scoped keys get 403, see `docs/operations/auth.md`); `/metrics` can be made public with `DASH_METRICS_PUBLIC=1`.

### Replication endpoints (internal)

Used by followers; not for clients.

| Route | Purpose |
|---|---|
| `GET /internal/replication/wal?from_offset=N&max_records=M[&from_generation=G]` | WAL delta frame (plain text). The frame carries the WAL generation; a follower with a different generation, or an offset beyond the leader's total, is told to resync. Frames never end inside a commit group. `max_records` is capped at 10000. |
| `GET /internal/replication/export` | Full state export frame, tagged with the generation. |
| `GET /internal/replication/commit-status?commit_id=...` | JSON commit progress; 404 for unknown id. |
| `POST /internal/replication/ack?commit_id=...&replica_id=...&ack_epoch=N` | Record a replica acknowledgement. |

**0.3.0:** every replication route requires the `x-replication-token` header to equal `DASH_INGEST_REPLICATION_TOKEN` (constant-time comparison); a missing or wrong token, or no token configured (outside dev mode), returns 403. These endpoints export all tenants' data, so never expose them outside the cluster network. A follower persists `(generation, offset)`, resyncs from the export when the generation changes (for example after a leader checkpoint), applies each frame atomically, bounds response sizes, backs off on failure, and reports its state in `/ready` and `/metrics`.

## Control-plane service

**0.3.0:** every `/v1/control-plane/*` route except health and ready requires `Authorization: Bearer <token>` matching `DASH_CONTROL_PLANE_TOKEN` (constant-time comparison; 401 with `WWW-Authenticate: Bearer` on a missing or wrong token). The service refuses to start without a token unless `DASH_INSECURE_DEV_MODE=1` (then it is unauthenticated and binds loopback only). Services that read placement from it send the same token (`DASH_ROUTER_CONTROL_PLANE_TOKEN`, falling back to `DASH_CONTROL_PLANE_TOKEN`). Do not expose this service beyond the cluster network.

| Route | Purpose |
|---|---|
| `GET /health`, `/v1/control-plane/health` | `{"status":"ok"}`, no auth |
| `GET /ready`, `/v1/control-plane/ready` | 200 only when this node is the leader; otherwise 503. No auth. |
| `GET /v1/control-plane/leader` | Current lease holder (`is_leader`, `leader_node_id`, `epoch`, `fencing_token`, `expires_at_ms`, `local_node_id`); 503 if none. |
| `POST /v1/control-plane/leader/acquire` | Try to take the lease. |
| `GET /v1/control-plane/placement[?format=csv]` | Current shard placements (JSON, or CSV). Leader only: a follower refuses, with an `X-Dash-Leader-Node` header when it knows the leader. |
| `PUT /v1/control-plane/placement[?expected_epoch=N]` | Replace placements with a CSV body. Leader only; 409 on stale epoch or epoch regression. |
| `POST /v1/control-plane/failover/promote?tenant_id=&shard_id=&node_id=[&expected_epoch=N][&force=1]` | Promote a replica to shard leader. Leader only. Refused (409) unless the replica has reported a replication lag of exactly zero; `force=1` overrides the guard. |
| `POST /v1/control-plane/replica-lag?tenant_id=&shard_id=&node_id=&lag=N` | Report a replica's replication lag in records. Leader only; 404 for an unknown replica. |

The lease is durable and fenced: leader responses carry a fencing token, and the leader renews its lease in a background thread.

## Versioning

Routes are under `/v1`. The project is pre-1.0 and compatibility between v0.x releases is not guaranteed; breaking changes are recorded in the [Changelog](../about/changelog.md). Earlier text promising six months of `/v1` support after a `/v2` was aspirational and is withdrawn.
