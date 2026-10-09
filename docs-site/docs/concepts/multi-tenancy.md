# Multi-tenancy

DASH is multi-tenant by design: every `Claim` carries a `tenant_id`, evidence and edges hang off claims, and the retrieval path filters on the tenant. This page describes what is enforced today, what is not, and what is planned. Isolation is enforced by application code; there is no separate storage per tenant.

!!! warning "Isolation still has a known gap in 0.3.0"
    The isolation guarantees below describe the intended model. Several 2026-10 findings are fixed (conflict errors no longer name the owning tenant, vector fallbacks are tenant-scoped, segment directories are collision-free), but the `claim_id` namespace is still global, which lets a caller probe whether another tenant has a given id (register SEC-18, see [Known gaps](#known-gaps)). Do not host mutually untrusted tenants on one deployment until it is closed.

## What is enforced

1. **Retrieval is tenant-filtered.** `POST /v1/retrieve` requires `tenant_id`; candidate generation (lexical, entity, temporal and ANN) and the final claim filter compare against it. Evidence for each result comes from the claim's own record.
2. **Credentials are tenant-scoped.** A scoped API key (`DASH_*_API_KEY_SCOPES`, entries `key:tenantA,tenantB[:roles]`) or a JWT tenant claim limits which tenants a caller may act on; a mismatch returns 403. A service-wide allowlist (`DASH_*_ALLOWED_TENANTS`) applies on top. Tests: `transport_denies_cross_tenant_retrieval_for_scoped_key` (retrieval) and `transport_denies_cross_tenant_ingest_for_scoped_key` (ingestion).
3. **Vector indexes are per tenant.** The in-memory store keeps one vector index per tenant (`TenantVectorIndex`: flat, then `usearch` HNSW), with the vector dimension pinned at that tenant's first vector. A query only ever searches its own tenant's index, including filtered and exact scans (test: `tenant_isolation_with_identical_vectors` in `pkg/store/tests/vector_recall.rs`).
4. **Claim ids are validated across tenants on write.** Reusing a `claim_id` that belongs to another tenant is rejected by the store (unit tests exercise this in `pkg/store`).

## Where the tenant comes from

The tenant is read from the **request** (`claim.tenant_id` for ingest, `tenant_id` for retrieve) and then checked against the credential. It is not taken solely from the token. A caller whose credential allows tenant `t1` and who names `t2` gets 403; a credential with a wildcard tenant (`*` in a scoped key) allows any tenant. A `*` tenant inside a JWT is honored only with `DASH_*_JWT_ALLOW_WILDCARD_TENANT=1`. A service started in dev mode (`DASH_INSECURE_DEV_MODE=1`) with no credentials allows any tenant.

## Tenant lifecycle

There is no tenant registry and no tenant API. A tenant comes into existence when the first claim with that `tenant_id` is ingested, if the credential and allowlist permit it. A retrieve for a tenant with no data returns no results; there is no "unknown tenant" error. There is no tenant deletion: no claim, evidence, edge or tenant delete path exists yet (register DATA-14, planned for P2). See [Planned API](../reference/planned-api.md).

## Rate limiting

Per-tenant rate limits are configured with `DASH_INGEST_RATE_LIMIT_PER_TENANT_RPS` / `DASH_INGEST_RATE_LIMIT_BURST` and the `DASH_RETRIEVAL_` equivalents (defaults: ingestion 100 rps and burst 200, retrieval 500 rps and burst 1000; `0` disables). Since 0.3.0 the limit is enforced for API keys, JWTs and OIDC alike with a per-process token bucket, and an excess request gets HTTP 429 with `Retry-After` (register SEC-06). The bucket is keyed by the tenant in the request; it is not per IP and not shared between replicas. There are no tenant tiers.

## Storage layout

- **WAL:** one append-only file per service, shared by all tenants. Records carry the tenant id inside the claim.
- **redb:** `dash_claims`, `dash_evidence`, `dash_edges`, `dash_claim_vectors`, `dash_tenant_dims`, `dash_tenant_claims_set`, plus batch-commit records. Keys are strings; the tenant is part of the stored value and of the `(tenant, claim)` membership table, not a per-tenant database.
- **Segments:** published per tenant under the segment directory, in a collision-free directory derived from the tenant id (bytes outside `a-z0-9-` are escaped as `_xx`; long ids get a hash suffix). Directories created by the older lossy sanitizer are renamed automatically on first use.

## Known gaps

- The claim-id namespace is global: writing a `claim_id` that another tenant owns is a 409, so a caller can learn that the id exists (SEC-18, planned P4). The conflict message no longer names the other tenant.
- An edge's `to_claim_id` is not checked for existence or tenant at write time. Ranking ignores edges whose endpoints are missing or belong to another tenant. Whether graph output (`return_graph`) can still include another tenant's ids was not re-verified in the 0.3.0 documentation pass.
- There is no dedicated multi-tenant isolation test suite. The credential check, the store-level isolation (per-tenant indexes, tenant-scoped vector fallbacks, cross-tenant edge handling) and the segment directory mapping are covered by unit and integration tests, not by a single end-to-end isolation suite.

The full list, with severities and target phases, is in [`docs/plans/2026-10-09-issue-register.md`](https://github.com/BHAWESHBHASKAR/DASH/blob/main/docs/plans/2026-10-09-issue-register.md).
