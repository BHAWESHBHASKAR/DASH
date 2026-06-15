# Ingest

The ingestion endpoint accepts a `Claim`, its supporting `Evidence` records, and the `ClaimEdge`s that connect it to other claims. This page describes the request shape, the idempotency contract, and the bulk-ingest path.

## `POST /v1/ingest`

```text
POST /v1/ingest
Content-Type: application/json
Authorization: Bearer <jwt>   (or DashKey <ak_...>)

{
  "claim": { ... },
  "evidence": [ ... ],
  "edges":   [ ... ]
}
```

The response is HTTP 202 Accepted with the IDs of the records that were created:

```json
{
  "claim_id": "c1",
  "evidence_ids": ["e1", "e2"],
  "edge_ids": ["g1"],
  "ingest_seq": 17,
  "idempotent_replay": false
}
```

If the same `idempotency_key` is replayed, the response is identical and `idempotent_replay: true` is set. See [Idempotency](#idempotency).

## `IngestRequest`

```json
{
  "claim": {
    "claim_id": "c1",
    "tenant_id": "t1",
    "canonical_text": "Company X acquired Company Y",
    "confidence": 0.95,
    "event_time_unix": 1718300000,
    "valid_from_unix": 1718300000,
    "valid_to_unix": null,
    "extraction_model": "claude-opus-4-7"
  },
  "evidence": [
    {
      "evidence_id": "e1",
      "claim_id": "c1",
      "tenant_id": "t1",
      "source_id": "news://nyt/2024/05/12/acme-x",
      "stance": "supports",
      "source_quality": 0.95,
      "chunk_id": "chunk-7",
      "span_start": 1024,
      "span_end": 1280
    }
  ],
  "edges": [],
  "idempotency_key": "ingest-2024-05-12-c1"
}
```

### Field rules

- `claim.claim_id` — required, unique within the tenant.
- `claim.tenant_id` — required, must match the JWT's tenant scope.
- `claim.canonical_text` — required, ≤ 4 KiB.
- `claim.confidence` — required, in `[0, 1]`.
- `claim.event_time_unix` — optional, defaults to "now".
- `claim.valid_from_unix` / `valid_to_unix` — optional, used for time-range filtering at retrieval.
- `claim.extraction_model` — optional, recorded in the audit log.
- `evidence[].claim_id` — must equal the outer `claim.claim_id`. Mismatches are rejected with HTTP 400.
- `evidence[].tenant_id` — must equal the outer `claim.tenant_id`.
- `evidence[].stance` — one of `supports`, `contradicts`, `neutral`.
- `edges[].src_claim_id` — must equal the outer `claim.claim_id`.
- `edges[].dst_claim_id` — must name an existing claim in the same tenant (a forward-reference to a not-yet-ingested claim is allowed; the edge is stored but not traversable until both endpoints exist).
- `idempotency_key` — optional but strongly recommended. See below.

## Idempotency

The ingestion endpoint is **idempotent on `(tenant_id, idempotency_key)`**. The recommended pattern:

```bash
IDEM="ingest-$(date -u +%Y%m%dT%H%M%S)-$(uuidgen)"

curl -X POST http://localhost:8081/v1/ingest \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: $IDEM" \
  -d @request.json
```

The header form is equivalent to the body field. A replay with the same `Idempotency-Key` returns the original result with `idempotent_replay: true` and HTTP 200 (not 202) so the caller can distinguish a fresh ingest from a replay.

The `(tenant_id, idempotency_key) → result` mapping is stored in redb. Keys are retained for `DASH_IDEMPOTENCY_RETENTION_DAYS` (default: 30) and then garbage-collected.

If a replay uses the same `idempotency_key` but a **different** request body, the response is HTTP 409 Conflict with a `dash_idempotency_mismatch` error code. The caller is expected to use a new key for a new request.

## Bulk ingest

For workloads that move many claims at once, the ingestion endpoint accepts a `bundles: [...]` field on the request body. The single-bundle shape is the same; the response is an array:

```json
{
  "results": [
    { "claim_id": "c1", "evidence_ids": ["e1"], "edge_ids": [], "ingest_seq": 17 },
    { "claim_id": "c2", "evidence_ids": ["e2"], "edge_ids": [], "ingest_seq": 18 }
  ]
}
```

The bulk path is **all-or-nothing per bundle** but **not** all-or-nothing across the array. A validation failure on bundle #3 returns an error pointing at bundle #3 and the prior bundles are already committed. The caller is expected to retry the failed bundle alone with its own `idempotency_key`.

A bulk request is limited to `DASH_INGEST_MAX_BUNDLES_PER_REQUEST` (default: 1 000) bundles.

## Failure modes

| Condition                                          | HTTP | `error.code`                |
| -------------------------------------------------- | ---: | --------------------------- |
| Missing or invalid JWT                             |  401 | `dash_unauthenticated`      |
| JWT scope does not include `claims:write`          |  403 | `dash_forbidden_scope`      |
| `tenant_id` mismatch with JWT                      |  403 | `dash_tenant_mismatch`      |
| Validation failure (out-of-range confidence, etc.) |  400 | `dash_validation_error`     |
| `claim_id` already exists with different content   |  409 | `dash_claim_conflict`       |
| Idempotency key replay with a different body       |  409 | `dash_idempotency_mismatch` |
| Request body too large                             |  413 | `dash_payload_too_large`    |
| Rate limit exceeded                                |  429 | `dash_rate_limited`         |
| Internal error                                     |  500 | `dash_internal_error`       |

For the full error envelope and the wire-level error codes, see [HTTP API → Error codes](../reference/api.md#error-codes).
