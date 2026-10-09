# HTTP API

DASH exposes plain HTTP/1.1 with JSON bodies (one request per connection; the server closes the connection after each response). There is no gRPC, GraphQL or WebSocket interface.

This page documents the routes that exist in the code today. Anything not listed here is not implemented; see [Planned API](planned-api.md) for the ideas that earlier documentation described as if they were available. Behavior that changes in **v0.3.0** is marked.

## Services and base URLs

| Service | Default bind | Purpose |
|---|---|---|
| ingestion | `127.0.0.1:8081` | Writes. Owns the WAL. |
| retrieval | `127.0.0.1:8080` | Reads, and the embeddings endpoint. Follows ingestion by replication. |
| control-plane | `127.0.0.1:8090` | Placement and leader state. |

The compose file publishes `8081`, `8080` and `8090` on the host. There are no hosted staging or production base URLs.

## Authentication

Each service has its own credentials; an ingestion key does not work on retrieval.

```text
x-api-key: <api key>
Authorization: Bearer <api key>        # equivalent
Authorization: Bearer <HS256 JWT>      # when a JWT secret or OIDC is configured
```

There is no `DashKey` scheme. A JWT must carry the tenant in `tenant_id` (or a `tenants` / `tenant_ids` array, `*` for all) and, if roles are used, the roles in `dash_roles`. See [Configuration](configuration.md#authentication-and-authorization) for keys, scoped keys, roles, revocation and JWT settings.

Authorization is per tenant. The tenant is taken from the **request** (`claim.tenant_id` on ingest, `tenant_id` on retrieve) and then checked against the credential's tenant scope and the service allowlist. Required roles: `ingest` for `/v1/ingest*`, `retrieve` for `/v1/retrieve`. Roles have no hierarchy (`admin` does not imply the others), a JWT without a roles claim is granted all roles, and unscoped API keys are not role-checked (register SEC-11, planned for P1).

| Condition | Status | Body |
|---|---:|---|
| Missing or unknown key, invalid or expired JWT, revoked key | 401 | `{"error":"missing or invalid API key"}` (or `invalid JWT`, `JWT expired`, `API key revoked`) |
| Valid credential, tenant or role not permitted | 403 | `{"error":"tenant is not allowed for this API key"}` (or `role is not allowed ...`) |

**Today (v0.2.x):** if the service has no API key, scoped key or JWT secret configured, requests are accepted without credentials (SEC-01); if only a JWT secret is configured, a request with no `Authorization` header is also accepted (SEC-02); and `/v1/embeddings`, `/metrics` and `/debug/*` need no credentials in any configuration. **v0.3.0:** services refuse to start without credentials unless `DASH_INSECURE_DEV_MODE=1`, and `/v1/embeddings`, `/debug/*` and `/metrics` require authentication.

## Errors

Errors are a JSON object with one field, and there is no error-code enum or request id:

```json
{ "error": "validation error: ..." }
```

| Status | Meaning in practice |
|---:|---|
| 400 | Malformed JSON, failed validation, bad query parameter, wrong `Content-Type` (POST bodies must include `application/json`), invalid vector, unknown stance or relation. |
| 401 / 403 | See [Authentication](#authentication). |
| 404 | Unknown path, or an unknown `commit_id` on the replication endpoints. |
| 405 | Wrong method for a known path. |
| 409 | State conflict from the store, or a stale `expected_epoch` on the control plane. |
| 413 | Not produced. A body over 16 MiB (ingestion and retrieval) fails request parsing and is returned as 400 `content-length exceeds max body size`. |
| 429 | **v0.3.0.** Per-tenant rate limit exceeded. Today the limiter does not throttle, and the code path that exists answers 401 `rate limit exceeded`. |
| 500 | Persistence or internal error. |
| 503 | Worker queue full (`service unavailable: ...worker queue full`), placement rejected the write (node is not the shard leader, consistency unavailable), disk unavailable on `/ready`, or the control plane is not the leader. |

## Retrieval service

### `POST /v1/retrieve` and `GET /v1/retrieve`

Requires role `retrieve`. `POST` takes JSON; `GET` takes the same fields as query parameters (lists as comma-separated values, time range as `from_unix` and `to_unix`).

| Field | Type | Default | Notes |
|---|---|---|---|
| `tenant_id` | string | required | Must be non-empty. |
| `query` | string | required | Must be non-empty. Embedded with the configured provider unless `query_embedding` is given. |
| `top_k` | positive integer | `5` | |
| `stance_mode` | `balanced` \| `support_only` | `balanced` | `support_only` drops claims with more contradictions than supports. |
| `time_range` | `{ "from_unix": i64?, "to_unix": i64? }` | none | Filters on event time and validity window. |
| `query_embedding` | float array | none | Pre-computed query vector. |
| `entity_filters` | string array | `[]` | |
| `embedding_id_filters` | string array | `[]` | |
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

OpenAI-compatible embeddings. **v0.3.0 requires authentication**; today it is open. See the [Embeddings guide](../guides/embeddings.md).

Request: `{"input": "text" | ["text", ...], "model": "...", "encoding_format": "float" | "base64", "user": "..."}`. `model` is echoed back as a hint; the vectors come from the provider selected by `DASH_EMBEDDING_PROVIDER` (default: deterministic hash embedder). `encoding_format` defaults to `float`.

Response `200`: `{"object":"list","data":[{"object":"embedding","index":0,"embedding":[...]}],"model":"...","usage":{"prompt_tokens":N,"total_tokens":N}}`. Errors are returned with status 400 in an OpenAI-style error object, including provider failures.

### Health, metrics, debug (retrieval)

| Route | Auth today | Auth in v0.3.0 | Notes |
|---|---|---|---|
| `GET /health`, `/v1/health` | none | none | `{"status":"ok"}` |
| `GET /live`, `/v1/live` | none | none | `{"status":"alive"}` |
| `GET /ready`, `/v1/ready` | none | none | `{"status":"ready"}`, or 503 `not_ready` if a configured redb path is unavailable. |
| `GET /metrics` | none | required | Prometheus text. |
| `GET /debug/placement` | none | required | Placement route probe (`tenant_id`, `entity_key`). |
| `GET /debug/planner` | none | required | Planner snapshot for a retrieve-style query. |
| `GET /debug/storage-visibility` | none | required | Segment vs WAL visibility for a query. |

## Ingestion service

All writes accept an optional `write_consistency` query parameter (`one` default, `quorum`, `all`) that matters only with placement routing configured. Successful writes return **200** (not 202).

### `POST /v1/ingest`

Requires role `ingest`. One claim with its evidence and edges.

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
- Re-sending the same evidence appends duplicates today. **v0.3.0:** evidence and edges become idempotent upserts keyed by `evidence_id` / `edge_id`.

Response `200`:

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

Requires role `ingest`. `{"commit_id": "optional-string", "items": [ <ingest body>, ... ]}`. Up to `DASH_INGEST_BATCH_MAX_ITEMS` (default 128) items, all for the same tenant. Replaying a `commit_id` returns `idempotent_replay: true`. Response fields: `commit_id`, `idempotent_replay`, `ingested_claim_ids`, `batch_size`, `claims_total`, plus the commit fields above.

### `POST /v1/ingest/raw`

Requires role `ingest`. Extracts sentence claims from text: `{ "tenant_id", "document_id", "source_id", "text", "extraction_model"?, "claim_confidence"?, "source_quality"?, "min_sentence_chars"?, "max_claims"?, "generate_embeddings"?, "embedding_model"? }`. Response includes `document_id`, `commit_id`, `idempotent_replay`, `extracted_count`, `embedding_provider`, `embeddings_generated`, `ingested_claim_ids`, `claims_total`.

### `POST /v1/ingest/document`

Requires role `ingest`. Like `/raw` but takes `mime_type` and either `text` or `content_base64`. Only UTF-8 text is parsed by the built-in parser; other MIME types need `DASH_INGEST_DOCUMENT_PARSER_PROVIDER=adapter_command`. Response adds `mime_type` and `parser_provider`.

### Health, metrics, debug (ingestion)

Same health, readiness and liveness routes as retrieval, plus `GET /metrics` (Prometheus text), `GET /debug/placement` and `GET /debug/document-parser` (parser/extraction provider config). The auth column for `/metrics` and `/debug/*` is the same as the retrieval table above: open today, authenticated in v0.3.0.

### Replication endpoints (internal)

Used by followers; not for clients.

| Route | Purpose |
|---|---|
| `GET /internal/replication/wal?from_offset=N&max_records=M` | WAL delta frame (plain text). |
| `GET /internal/replication/export` | Full state export frame. |
| `GET /internal/replication/commit-status?commit_id=...` | JSON commit progress; 404 for unknown id. |
| `POST /internal/replication/ack?commit_id=...&replica_id=...&ack_epoch=N` | Record a replica acknowledgement. |

Today these are guarded only if `DASH_INGEST_REPLICATION_TOKEN` is set (header `x-replication-token`; mismatch returns 403). **v0.3.0 requires the token.** These endpoints export all tenants' data, so never expose them outside the cluster network.

## Control-plane service

No credentials are checked today. **v0.3.0 requires `DASH_CONTROL_PLANE_TOKEN`.** Do not expose this service beyond the cluster network.

| Route | Purpose |
|---|---|
| `GET /health`, `/v1/control-plane/health` | `{"status":"ok"}` |
| `GET /ready`, `/v1/control-plane/ready` | 200 only when this node is the leader; otherwise 503. |
| `GET /v1/control-plane/leader` | Current lease holder (`is_leader`, `leader_node_id`, `epoch`, `expires_at_ms`); 503 if none. |
| `POST /v1/control-plane/leader/acquire` | Try to take the lease. |
| `GET /v1/control-plane/placement[?format=csv]` | Current shard placements (JSON, or CSV). |
| `PUT /v1/control-plane/placement[?expected_epoch=N]` | Replace placements with a CSV body. Leader only; 409 on stale epoch or epoch regression. |
| `POST /v1/control-plane/failover/promote?tenant_id=&shard_id=&node_id=[&expected_epoch=N]` | Promote a replica to shard leader. Leader only. |

## Versioning

Routes are under `/v1`. The project is pre-1.0 and compatibility between v0.x releases is not guaranteed; breaking changes are recorded in the [Changelog](../about/changelog.md). Earlier text promising six months of `/v1` support after a `/v2` was aspirational and is withdrawn.
