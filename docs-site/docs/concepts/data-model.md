# Data model

DASH's data model is small and explicit. Six types cover everything that lives in the database, the audit log, or the response payloads. This page describes each one with the wire-level JSON shape, the relationship to the other types, and the cardinality rules.

## The six types

| Type            | Lives in               | Cardinality per tenant           |
| --------------- | ---------------------- | -------------------------------- |
| `Tenant`        | redb, control plane    | one per deployment customer      |
| `Vector`        | redb, ANN index        | one per claim (per tenant)       |
| `Claim`         | redb, WAL, ANN         | unbounded                        |
| `Evidence`      | redb, WAL              | zero-to-many per claim           |
| `Contradiction` | derived, in response   | zero-to-many per claim (derived) |
| `AuditEvent`    | hash-chained log file  | one per state-changing request   |

## `Tenant`

A tenant is the unit of isolation. Every `Claim`, `Evidence`, `Vector`, and `AuditEvent` carries a `tenant_id`, and the retrieval API scopes its query to a single tenant.

```json
{
  "tenant_id": "t1",
  "display_name": "Acme Corp",
  "rate_limit_rps": 100,
  "created_at_unix": 1718300000
}
```

There is no cross-tenant query path. A retrieval request that names tenant `t1` will never return a claim from tenant `t2`, even if the query text is identical and the vector is closer. The enforcement is at the storage layer (per-tenant key spaces in redb) and at the API layer (a request without a `tenant_id` is rejected with `400 Bad Request`).

## `Claim`

A claim is an atomic, source-bound assertion. It is the primary data primitive.

```json
{
  "claim_id": "c1",
  "tenant_id": "t1",
  "canonical_text": "Company X acquired Company Y",
  "confidence": 0.95,
  "event_time_unix": 1718300000,
  "valid_from_unix": 1718300000,
  "valid_to_unix": null,
  "extraction_model": "claude-opus-4-7",
  "created_at_unix": 1718300050
}
```

Field rules:

- `claim_id` is unique within a tenant. Re-ingesting the same `claim_id` updates the existing record (idempotent, see [Ingest](../guides/ingest.md#idempotency)).
- `confidence` is in `[0, 1]`. Out-of-range values are rejected.
- `event_time_unix` is the wall-clock time of the *event* the claim is about (not the time the claim was extracted).
- `valid_from_unix` / `valid_to_unix` define the temporal validity window. A `null` upper bound means "valid indefinitely". A retrieval with `time_range: { from_unix, to_unix }` filters out claims whose `[valid_from, valid_to]` does not intersect the requested range.
- `extraction_model` records which model produced the claim. It is part of the audit trail.

## `Evidence`

Evidence is the record that ties a claim to a source. It is the field that makes the citation *defensible*.

```json
{
  "evidence_id": "e1",
  "claim_id": "c1",
  "tenant_id": "t1",
  "source_id": "news://nyt/2024/05/12/acme-x",
  "stance": "supports",
  "source_quality": 0.95,
  "chunk_id": "chunk-7",
  "span_start": 1024,
  "span_end": 1280,
  "doc_id": "nyt-2024-05-12-acme-x",
  "extraction_model": "claude-opus-4-7",
  "created_at_unix": 1718300051
}
```

Field rules:

- `stance` is one of `supports`, `contradicts`, `neutral`.
- `source_quality` is in `[0, 1]`. The default is `0.5` if the ingestion request omits it.
- `chunk_id`, `span_start`, `span_end` are optional. When present, they let a downstream UI deep-link to the exact characters in the source.
- `source_id` is an opaque URI. DASH does not parse it; the convention is `<scheme>://<authority>/<path>`.

## `Vector`

A vector is a fixed-dimensional embedding associated with a claim.

```json
{
  "claim_id": "c1",
  "tenant_id": "t1",
  "dim": 768,
  "values_b64": "AaBbCcDdEe..."
}
```

The vector is stored **base64-encoded** in the wire format and the on-disk format, decoded into `&[f32]` for ANN search. The default dimension is 768 (matching `nomic-embed-text`); the dimension is pinned per tenant at first ingest. Mismatched dimensions on subsequent ingests are rejected with `409 Conflict`.

## `Contradiction` (derived)

A contradiction is **not** a stored type. It is a *derived* field on a retrieval response. A `Contradiction` exists when a claim has at least one `Evidence` with `stance: contradicts`, or at least one `ClaimEdge` with `relation: contradicts` from another claim. The retrieval response exposes this as the `contradicts: N` counter on each result.

## `ClaimEdge` (graph)

A `ClaimEdge` is a typed relationship between two claims in the same tenant.

```json
{
  "edge_id": "g1",
  "tenant_id": "t1",
  "src_claim_id": "c1",
  "dst_claim_id": "c2",
  "relation": "supports",
  "weight": 0.8
}
```

`relation` is one of `supports`, `contradicts`, `refines`, `supersedes`. The `supersedes` relation is used to mark a claim as the temporal successor of another (e.g. "the 2024-10-K supersedes the 2023-10-K for this disclosure").

## `AuditEvent`

An `AuditEvent` is the hash-chained record of every authenticated state change.

```json
{
  "seq": 17,
  "ts_unix": 1718300060,
  "actor": "jwt:sub=alice@acme",
  "tenant_id": "t1",
  "action": "claim.ingest",
  "subject_ids": ["c1", "e1"],
  "payload_sha256": "5b8f...c1",
  "prev_hash": "3a2b...9f",
  "hash": "9c0d...71"
}
```

Field rules:

- `seq` is a monotonically increasing per-tenant sequence number.
- `hash = sha256( seq || ts_unix || actor || action || subject_ids || payload_sha256 || prev_hash )`.
- The chain is **tamper-evident**: any modification to an earlier event invalidates every subsequent `hash`. Verification is a linear scan with `scripts/verify_audit_chain.sh`.

## Relationships and cardinality

```text
Tenant 1──* Claim 1──* Evidence
                │
                ├──1 Vector
                │
                └──* ClaimEdge ──► Claim (same tenant)
```

- One `Tenant` has many `Claim`s.
- One `Claim` has many `Evidence` records.
- One `Claim` has exactly one `Vector` (the embedding). Re-ingest replaces the vector.
- One `Claim` has many outgoing `ClaimEdge`s, many incoming `ClaimEdge`s. Edges are between claims in the *same* tenant.

## What's not in the data model

- **No cross-tenant graph.** Edges are within a tenant. Cross-tenant retrieval is not a feature.
- **No free-form metadata on claims.** The fields above are exhaustive. A future PR may add a `metadata: Map<String, Value>` field, gated by a feature flag.
- **No versioning of claims.** Re-ingesting a `claim_id` overwrites. A `history` table is a known gap; see the [Changelog](../about/changelog.md#known-limitations) for the current limitations.
