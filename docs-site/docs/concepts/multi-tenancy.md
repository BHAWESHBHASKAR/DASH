# Multi-tenancy

DASH is multi-tenant by design. Every `Claim`, `Evidence`, `Vector`, `AuditEvent`, and `ClaimEdge` carries a `tenant_id`, and the API rejects any request that would cross tenant boundaries. This page describes the isolation guarantees, the per-tenant rate limiting, and the per-tenant key spaces inside `redb`.

## Isolation guarantees

DASH provides **hard tenant isolation**. The guarantees are:

1. **No cross-tenant reads.** A `POST /v1/retrieve` request that names tenant `t1` will return only claims from `t1`. This is enforced at two layers:
   - The retrieval API rejects requests without a `tenant_id` (HTTP 400).
   - The `InMemoryStore` and `DiskBackedStore` look up by `(tenant_id, claim_id)`; a missing `tenant_id` cannot match.

2. **No cross-tenant writes.** A `POST /v1/ingest` request with `tenant_id: t1` cannot write a claim that names `tenant_id: t2` in any nested field (evidence, edges, vectors). The validation step rejects mismatches with HTTP 400.

3. **No cross-tenant graph edges.** `ClaimEdge` endpoints must belong to the same tenant. An edge from `t1/c1` to `t2/c2` is rejected at validation time.

4. **No cross-tenant audit visibility.** A JWT with `tenant: ["t1"]` cannot read audit events for `t2`. The audit log is partitioned by tenant, and the reader path is gated by the same JWT scope check as the write path.

5. **No shared ANN index.** Each tenant has its own `usearch` index, dimension-pinned at first ingest. The ANN graphs do not see each other's vectors.

## Per-tenant rate limiting

DASH enforces a per-tenant token-bucket rate limit on the **retrieval service**. The defaults:

| Tenant tier   | `RATE_LIMIT_RPS` | `RATE_LIMIT_BURST` |
| ------------- | ---------------: | -----------------: |
| `free`        |               10 |                 20 |
| `pro`         |              100 |                200 |
| `enterprise`  | 1 000 (or higher)| 2 000 (or higher) |

The values are env-driven per service (`DASH_RETRIEVAL_RATE_LIMIT_RPS`, `DASH_RETRIEVAL_RATE_LIMIT_BURST`). A request that exceeds the bucket returns HTTP 429 with a `Retry-After` header.

The bucket is **per-tenant**, not per-IP and not per-token. Two clients within the same tenant share the bucket; a single client across two tenants gets two independent buckets. This is deliberate — the threat model treats the tenant as the principal, not the caller.

## Per-tenant key spaces in redb

`redb` is a key-value store with typed tables. DASH uses a table-per-type layout, and the keys are `(tenant_id, claim_id)` pairs (or the analogous pair for the other types).

```text
Table "claims"      : key   = (tenant_id: String, claim_id: String)
                    : value = Claim (bincode)

Table "evidence"    : key   = (tenant_id: String, claim_id: String, evidence_id: String)
                    : value = Evidence

Table "vectors"     : key   = (tenant_id: String, claim_id: String)
                    : value = Vector (base64-encoded values)

Table "ann_index"   : key   = (tenant_id: String)
                    : value = bytes of the usearch HNSW index

Table "audit_log"   : key   = (tenant_id: String, seq: u64)
                    : value = AuditEvent
```

The `(tenant_id, claim_id)` prefix is the **isolation boundary**. The redb API does not support prefix scans, so the isolation is enforced at the application layer — the store never exposes a "list all claims" operation that omits the tenant prefix.

The ANN index is one HNSW graph per tenant. The cost is **O(tenants × dim × 2 bytes)** of RAM for vectors, plus the graph overhead. For 1 000 tenants at 768-dim with 10 000 vectors each, that is ~30 GB of vector RAM. The path for tenants that exceed a single host is **sharding** — see [Scaling](../operations/scaling.md#ann-index-sharding).

## Tenant lifecycle

A tenant is created out-of-band — by an admin in the control plane, by a `POST /v1/tenants` request from an operator-scoped JWT, or by the bootstrap process. The retrieval service does not auto-provision tenants on first request; a request to an unknown tenant returns HTTP 404.

When a tenant is deleted:

1. All `Claim` rows in the `claims` table with `tenant_id == deleted` are removed.
2. All `Evidence` rows are removed.
3. All `Vector` rows are removed; the ANN graph is dropped.
4. The `audit_log` table is **not** removed — the audit history is retained for the regulatory retention window (default: 7 years, configurable via `DASH_AUDIT_RETENTION_DAYS`). An audit event of kind `tenant.deleted` is appended to the chain.

Tenant deletion is **irreversible** from the storage layer's perspective. Operators are expected to take a `redb` snapshot before deletion if they want a recovery path.
