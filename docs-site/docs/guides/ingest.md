# Ingest

The ingestion service accepts a `Claim`, its supporting `Evidence` records and the `ClaimEdge`s that connect it to other claims. This page describes the write routes that exist today, the validation rules, and what is and is not idempotent. The exact field list is in the [HTTP API reference](../reference/api.md#ingestion-service).

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
- `edges[].from_claim_id` must equal `claim.claim_id`. `to_claim_id` may name any claim id; it is not checked for existence or tenant today (see [Multi-tenancy](../concepts/multi-tenancy.md#known-gaps)). `relation`: `supports`, `contradicts`, `refines`, `duplicates` or `depends_on`. `strength` in `[0, 1]`.
- A `claim_id` already used by a *different* tenant is rejected with 409.

## Idempotency

There is no `idempotency_key` field and no `Idempotency-Key` header on `POST /v1/ingest`. Be precise about what happens on a retry in v0.2.x:

- Re-sending a claim with the same `claim_id` and tenant updates the claim.
- Re-sending the same **evidence** or **edges** appends duplicates, and evidence is also duplicated on restart and on replication re-apply (register DATA-01). Duplicated evidence inflates citation counts and ranking.
- **v0.3.0** makes evidence and edge writes idempotent upserts keyed by `evidence_id` and `edge_id`. Until you run v0.3.0, avoid blind retries of ingest calls.

The only replay-safe write today is the batch route, which de-duplicates on `commit_id`.

## Batch: `POST /v1/ingest/batch`

```json
{
  "commit_id": "load-2026-10-09-001",
  "items": [ { "claim": {}, "evidence": [], "edges": [] } ]
}
```

`items` holds ingest bodies as above, all for the same tenant; at most `DASH_INGEST_BATCH_MAX_ITEMS` (default 128). Replaying a `commit_id` returns the earlier result with `idempotent_replay: true`. The replay check compares ids, not content (register DATA-12), so do not reuse a `commit_id` for edited content. The field `bundles` and the setting `DASH_INGEST_MAX_BUNDLES_PER_REQUEST` do not exist.

## Raw text and documents

- `POST /v1/ingest/raw` takes `{tenant_id, document_id, source_id, text, ...}` and extracts sentence-level claims (extraction provider `rule_sentence` by default).
- `POST /v1/ingest/document` takes `{tenant_id, document_id, source_id, mime_type, text | content_base64, ...}`. The built-in parser accepts UTF-8 text MIME types (`text/*`, JSON, XML, YAML, CSV, Markdown); other types need `DASH_INGEST_DOCUMENT_PARSER_PROVIDER=adapter_command` and an adapter command.

Both responses include `commit_id`, `idempotent_replay`, `extracted_count`, `ingested_claim_ids`, and embedding counts.

## Failure modes

| Condition | HTTP | Body |
|---|---:|---|
| Missing or invalid API key / JWT | 401 | `{"error":"missing or invalid API key"}` |
| Tenant or role not allowed for the credential | 403 | `{"error":"tenant is not allowed for this API key"}` |
| Validation failure, bad JSON, wrong `Content-Type`, mismatched `claim_id`, invalid vector | 400 | `{"error":"validation error: ..."}` |
| `claim_id` belongs to another tenant, or other state conflict | 409 | `{"error":"state conflict: ..."}` |
| Request body over 16 MiB | 400 | `content-length exceeds max body size` |
| Worker queue full, or this node is not the shard leader | 503 | `{"error":"..."}` |
| Persistence error | 500 | `{"error":"internal persistence error: ..."}` |
| Rate limit exceeded | 429 in v0.3.0; not enforced today | |

See [HTTP API](../reference/api.md#errors) for the complete status table.
