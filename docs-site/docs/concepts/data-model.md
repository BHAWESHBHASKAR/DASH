# Data model

DASH's data model is small and explicit. Three stored record types (`Claim`, `Evidence`, `ClaimEdge`), a derived contradiction signal, an optional vector per claim, and an append-only audit record. The authoritative definitions are in `pkg/schema/src/lib.rs`; this page mirrors them.

| Type | Stored in | Cardinality |
|---|---|---|
| `Claim` | WAL, redb, in-memory store | unbounded per tenant |
| `Evidence` | WAL, redb, in-memory store | zero or more per claim |
| `ClaimEdge` | WAL, redb, in-memory store | zero or more per claim |
| Vector | WAL, redb, ANN graph | at most one per claim |
| Contradiction | derived at query time | derived counters on each result |
| Audit record | JSON-lines file, if enabled | one per audited request |

There is no `Tenant` record. A tenant is just the `tenant_id` string on claims; see [Multi-tenancy](multi-tenancy.md).

## `Claim`

A claim is an atomic, source-bound assertion and the primary data primitive.

```json
{
  "claim_id": "c1",
  "tenant_id": "t1",
  "canonical_text": "Company X acquired Company Y",
  "confidence": 0.95,
  "event_time_unix": 1718300000,
  "entities": ["Company X", "Company Y"],
  "embedding_ids": [],
  "claim_type": "factual",
  "valid_from": 1718300000,
  "valid_to": null,
  "created_at": null,
  "updated_at": null
}
```

Field rules (from `validate_claim`):

- `claim_id`, `tenant_id`, `canonical_text` are required and non-blank. `claim_id` is unique across the deployment's store (a different tenant reusing it gets a 409 whose message does not name the other tenant). Re-ingesting a `claim_id` for the same tenant updates the claim.
- Identifier-like fields (ids, tenant, entities, embedding ids, source ids) must not contain ASCII control characters.
- `confidence` is in `[0, 1]`; out-of-range values are rejected.
- `event_time_unix` is the time of the event the claim is about. Optional.
- `valid_from` / `valid_to` define the temporal validity window (unix seconds). If both are set, `valid_from <= valid_to`. A `null` bound is open. A retrieval `time_range` filters on the event time and the window; see the tests `retrieve_with_time_range_*` in `pkg/store`.
- `claim_type` is optional: `factual`, `opinion`, `prediction`, `temporal`, `causal`.
- `entities` and `embedding_ids` are non-blank string lists used for filtering.
- There is no `extraction_model` on a claim and no `*_unix` suffix on the validity fields; `extraction_model` belongs to evidence.

## `Evidence`

Evidence ties a claim to a source. It is what makes the citation defensible.

```json
{
  "evidence_id": "e1",
  "claim_id": "c1",
  "source_id": "news://nyt/2024/05/12/acme-x",
  "stance": "supports",
  "source_quality": 0.95,
  "chunk_id": "chunk-7",
  "span_start": 1024,
  "span_end": 1280,
  "doc_id": "nyt-2024-05-12-acme-x",
  "extraction_model": "llm-extractor-v1",
  "ingested_at": 1718300051
}
```

- `stance`: `supports`, `contradicts` or `neutral`.
- `source_quality` is required and in `[0, 1]`; there is no default.
- `chunk_id` must be non-blank if present. `span_start` and `span_end` must both be present or both absent, with `span_start <= span_end`.
- `source_id` is an opaque string. DASH does not parse it.
- Evidence has no `tenant_id`; it belongs to the claim named by `claim_id`.
- Evidence is upserted by `evidence_id` (0.3.0, register DATA-01): re-ingesting, restarting, reloading the WAL and replication re-apply leave one copy. For ranking, evidence from the same `source_id` counts once; the `supports`/`contradicts` counters on a result stay raw.

## Vector

An optional embedding per claim, supplied as `claim.embedding_vector` (or top-level `claim_embedding`) on ingest or computed by the configured provider. Vectors are stored in the WAL and redb as raw `f32` data and fed to the per-tenant ANN graph. The dimension is pinned per tenant when its first vector is stored; a later vector of a different dimension is rejected as an invalid vector (400). The default hash provider produces 384-dimension vectors; the ingestion-side `hash_vector` provider used by `/v1/ingest/raw` and `/v1/ingest/document` defaults to 64 dimensions (mixing the two for one tenant trips the dimension check). There is no 768-dimension default. Re-ingesting a claim keeps its vector and ANN entry (0.3.0, register DATA-04). Vectors are validated (finite values, tenant dimension, claim exists) before they are written to the WAL.

## Contradiction (derived)

A contradiction is not a stored type. A result's `contradicts` counter counts evidence with `stance: contradicts` and incoming `contradicts` edges; `supports` counts the supportive counterparts. `stance_mode: support_only` removes claims with more contradictions than supports; `balanced` keeps them and lowers their score. Tests: `support_only_drops_claim_with_more_contradictions_than_supports`, `balanced_mode_keeps_contradicted_claims_with_neutral_score`, `edge_contradicts_evidence_increments_contradict_count` in `pkg/store/tests/integration_retrieval.rs`.

## `ClaimEdge`

A typed, weighted relationship from one claim to another.

```json
{
  "edge_id": "g1",
  "from_claim_id": "c1",
  "to_claim_id": "c2",
  "relation": "supports",
  "strength": 0.8,
  "reason_codes": [],
  "created_at": null
}
```

`relation` is one of `supports`, `contradicts`, `refines`, `duplicates`, `depends_on` (there is no `supersedes`). `strength` is in `[0, 1]`. On ingest, `from_claim_id` must equal the claim in the request. `to_claim_id` is not required to exist at write time. Edges are upserted by `(from_claim_id, to_claim_id, relation)` (0.3.0). **Direction:** an edge `from supports to` is evidence for the **target** claim (`to`), and a `contradicts` edge counts against the target; edges whose endpoints are missing, belong to another tenant, or are self-edges are ignored for ranking, and each distinct source counts once (0.3.0, register IDX-05; 0.2.x credited the author).

## Audit record

When `DASH_INGEST_AUDIT_LOG_PATH` or `DASH_RETRIEVAL_AUDIT_LOG_PATH` is set, the service appends one JSON line per audited request:

```json
{
  "seq": 17,
  "ts_unix_ms": 1718300060000,
  "service": "ingestion",
  "action": "ingest",
  "tenant_id": "t1",
  "claim_id": "c1",
  "status": 200,
  "outcome": "success",
  "reason": "ingest accepted",
  "prev_hash": "3a2b...9f",
  "hash": "9c0d...71"
}
```

The record also carries `v` (encoding version 2), `actor` (`{kind, id}`: the credential kind and the first 8 hex characters of SHA-256 of the presented credential, never the credential), `request_id`, `client_ip` (not populated today) and, after a recovery, a `restart` object. `hash` is SHA-256 over a fixed canonical encoding of the record (including `prev_hash`), shared by both services and checked by `tools/audit-verify` (`scripts/verify_audit_chain.sh` wraps it). The chain is **unkeyed**: it detects accidental edits but anyone who can rewrite the file can recompute it. There is no request or response hash and no JWT `jti`. Audit is off unless the path variable is set. Details: the audit-chain operations guide (`docs/operations/audit-chain.md`).

## Relationships

```text
Claim 1──* Evidence
Claim 1──0..1 Vector
Claim 1──* ClaimEdge ──► Claim
```

## Not in the data model

- No tenant record, display name or per-tenant settings.
- No free-form metadata map on claims.
- No claim versioning or history: re-ingesting a `claim_id` for the same tenant updates the claim.
- No delete or tombstone. There is no way to remove a claim, evidence or edge (register DATA-14, planned P2).
