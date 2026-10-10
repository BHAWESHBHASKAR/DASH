# Ingest

The ingestion service accepts a `Claim`, its supporting `Evidence` records and the `ClaimEdge`s that connect it to other claims. This page describes the write routes that exist in 0.3.0, the validation rules, and what is and is not idempotent. The exact field list is in the [HTTP API reference](../reference/api.md#ingestion-service).

## `POST /v1/ingest`

```text
POST /v1/ingest
Content-Type: application/json
x-api-key: <ingestion api key>       (or Authorization: Bearer <key or JWT>)
```

Send the ingestion key, not the retrieval key. The request is one claim plus its evidence and edges:

```json
{
  "claim": {
    "claim_id": "c1",
    "tenant_id": "t1",
    "canonical_text": "Company X acquired Company Y",
    "confidence": 0.95,
    "event_time_unix": 1718300000,
    "valid_from": 1718300000,
    "valid_to": null
  },
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
      "extraction_model": "llm-extractor-v1"
    }
  ],
  "edges": []
}
```

A successful write returns **HTTP 200** with:

```json
{
  "ingested_claim_id": "c1",
  "claims_total": 1,
  "ack_count": 1,
  "required_acks": 1,
  "commit_status": "replication_quorum_met",
  "checkpoint_triggered": false
}
```

### Field rules (enforced by `pkg/schema` and the store)

- `claim.claim_id`, `claim.tenant_id`, `claim.canonical_text`: required, non-blank.
- `claim.confidence`: required, in `[0, 1]`.
- `claim.valid_from` / `claim.valid_to`: optional; if both are set, `valid_from <= valid_to`. These (with `event_time_unix`) drive `time_range` filtering at retrieval. The field names are `valid_from` / `valid_to`, not `valid_from_unix`.
- `claim.claim_type`: optional, one of `factual`, `opinion`, `prediction`, `temporal`, `causal`.
- `claim.embedding_vector` or top-level `claim_embedding`: optional pre-computed vector. If absent, the service computes one with the configured provider.
- `evidence[].claim_id` must equal `claim.claim_id`, otherwise the request is rejected with 400 (`missing claim`). Evidence has no `tenant_id` field; it inherits the claim's tenant.
- `evidence[].stance`: `supports`, `contradicts` or `neutral`. `source_quality` in `[0, 1]`. `span_start` and `span_end` must be given together, with `span_start <= span_end`.
- `extraction_model` is an optional field **on evidence** (it records which model produced the evidence), not on the claim.
- `edges[].from_claim_id` must equal `claim.claim_id`. `to_claim_id` may name any claim id; it is not checked for existence or tenant at write time, and ranking ignores edges whose endpoints are missing or in another tenant (see [Multi-tenancy](../concepts/multi-tenancy.md#known-gaps)). An edge `from supports to` is evidence for the **target** claim. `relation`: `supports`, `contradicts`, `refines`, `duplicates` or `depends_on`. `strength` in `[0, 1]`.
- A `claim_id` already used by a *different* tenant is rejected with 409.

## Idempotency

There is no `idempotency_key` field and no `Idempotency-Key` header on `POST /v1/ingest`, but since 0.3.0 the write itself is idempotent (register DATA-01):

- Re-sending a claim with the same `claim_id` and tenant updates the claim and keeps its vector.
- Evidence is upserted by `evidence_id`; edges are upserted by `(from_claim_id, to_claim_id, relation)`. Re-sending the same bundle, restarting the service, reloading the WAL and replication re-apply all leave one copy, so a retry does not inflate citation counts.
- One ingest is atomic: it is written to the WAL as a single commit group, so after a crash the whole bundle is replayed or none of it is. A retry of an already-applied bundle is a no-op.

For many items, the batch route additionally de-duplicates on `commit_id`.

## Batch: `POST /v1/ingest/batch`

```json
{
  "commit_id": "load-2026-10-09-001",
  "items": [ { "claim": {}, "evidence": [], "edges": [] } ]
}
```

`items` holds ingest bodies as above, all for the same tenant; at most `DASH_INGEST_BATCH_MAX_ITEMS` (default 128). The batch is staged on a detached copy of the store and committed as a unit: if any item fails validation, nothing is applied. Replaying a `commit_id` with the **same content** returns `idempotent_replay: true` and writes nothing. Reusing a `commit_id` with **different content** is applied as an update (upsert over the previous version) and the response carries `updated: true` (register DATA-12; before 0.3.0 the check compared ids only). The field `bundles` and the setting `DASH_INGEST_MAX_BUNDLES_PER_REQUEST` do not exist.

## Raw text and documents

- `POST /v1/ingest/raw` takes `{tenant_id, document_id, source_id, text, ...}` and extracts sentence-level claims (extraction provider `rule_sentence` by default).
- `POST /v1/ingest/document` takes `{tenant_id, document_id, source_id, mime_type, text | content_base64, ...}`. The built-in parser accepts UTF-8 text MIME types (`text/*`, JSON, XML, YAML, CSV, Markdown); other types need `DASH_INGEST_DOCUMENT_PARSER_PROVIDER=adapter_command` and an adapter command.

Both responses include `commit_id`, `idempotent_replay`, `updated` (only when true), `extracted_count`, `ingested_claim_ids`, and embedding counts. Claim ids derived from the document carry a hash of the full tenant and document ids (no collisions between different documents), and evidence spans are offsets into the original text. Editing a document and re-sending it with the same `document_id` updates it. The `adapter_command` raw extraction provider needs the `model-extraction-adapter` cargo feature, which is off by default.

## Failure modes

| Condition | HTTP | Body |
|---|---:|---|
| Missing or invalid API key / JWT | 401 | `{"error":"missing or invalid API key"}` |
| Tenant or role not allowed for the credential | 403 | `{"error":"tenant is not allowed for this API key"}` |
| Validation failure, bad JSON, wrong `Content-Type`, mismatched `claim_id`, invalid vector | 400 | `{"error":"validation error: ..."}` |
| `claim_id` belongs to another tenant, or other state conflict | 409 | `{"error":"state conflict: ..."}` |
| Request body over 16 MiB | 413 | `content-length exceeds max body size` |
| Headers too large or too many, chunked body, slow request | 431, 501, 408 | `{"error":"..."}` |
| Worker queue full, stale placement, audit gate refused, embedding provider unavailable, or this node is not the shard leader | 503 | `{"error":"..."}` |
| Embedding provider error | 502 | `{"error":"embedding_upstream_error"}` |
| Persistence error | 500 | `{"error":"internal persistence error: ..."}` |
| Rate limit exceeded | 429 | `{"error":"rate limit exceeded"}` plus `Retry-After` |

See [HTTP API](../reference/api.md#errors) for the complete status table.
