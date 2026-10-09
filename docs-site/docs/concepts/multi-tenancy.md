# Multi-tenancy

DASH is multi-tenant by design: every `Claim` carries a `tenant_id`, evidence and edges hang off claims, and the retrieval path filters on the tenant. This page describes what is enforced today, what is not, and what is planned. Isolation is enforced by application code; there is no separate storage per tenant.

!!! warning "Isolation has known gaps in v0.2.x"
    The isolation guarantees below describe the intended model. The 2026-10 review found cases that weaken them (register items SEC-18, SEC-19 and others, listed under [Known gaps](#known-gaps)). Do not host mutually untrusted tenants on one deployment until they are closed.

## What is enforced

1. **Retrieval is tenant-filtered.** `POST /v1/retrieve` requires `tenant_id`; candidate generation (lexical, entity, temporal and ANN) and the final claim filter compare against it. Evidence for each result comes from the claim's own record.
2. **Credentials are tenant-scoped.** A scoped API key (`DASH_*_API_KEY_SCOPES`, entries `key:tenantA,tenantB[:roles]`) or a JWT tenant claim limits which tenants a caller may act on; a mismatch returns 403. A service-wide allowlist (`DASH_*_ALLOWED_TENANTS`) applies on top. Tests: `transport_denies_cross_tenant_retrieval_for_scoped_key` (retrieval) and `transport_denies_cross_tenant_ingest_for_scoped_key` (ingestion).
3. **ANN graphs are per tenant.** The in-memory store keeps one HNSW-style graph per tenant (`TenantAnnGraph`), with the vector dimension pinned at that tenant's first vector. The index is in-repo code, not `usearch`.
4. **Claim ids are validated across tenants on write.** Reusing a `claim_id` that belongs to another tenant is rejected by the store (unit tests exercise this in `pkg/store`).

## Where the tenant comes from

The tenant is read from the **request** (`claim.tenant_id` for ingest, `tenant_id` for retrieve) and then checked against the credential. It is not taken solely from the token. A caller whose credential allows tenant `t1` and who names `t2` gets 403; a credential with a wildcard (`*`) or, today, an unconfigured service allows any tenant.

## Tenant lifecycle

There is no tenant registry and no tenant API. A tenant comes into existence when the first claim with that `tenant_id` is ingested, if the credential and allowlist permit it. A retrieve for a tenant with no data returns no results; there is no "unknown tenant" error. There is no tenant deletion: no claim, evidence, edge or tenant delete path exists yet (register DATA-14, planned for P2). See [Planned API](../reference/planned-api.md).

## Rate limiting

Per-tenant rate limits are configured with `DASH_INGEST_RATE_LIMIT_PER_TENANT_RPS` / `DASH_INGEST_RATE_LIMIT_BURST` and the `DASH_RETRIEVAL_` equivalents (defaults 100 and 200). In v0.2.x these settings do **not** throttle: the limiter is rebuilt on every request, and a rejection would surface as 401. v0.3.0 enforces the limit and returns 429 (register SEC-06). There are no tenant tiers.

## Storage layout

- **WAL:** one append-only file per service, shared by all tenants. Records carry the tenant id inside the claim.
- **redb:** `dash_claims`, `dash_evidence`, `dash_edges`, `dash_claim_vectors`, `dash_tenant_dims`, `dash_tenant_claims_set`, plus batch-commit records. Keys are strings; the tenant is part of the stored value and of the `(tenant, claim)` membership table, not a per-tenant database.
- **Segments:** published per tenant under the segment directory, in a directory derived from the tenant id.

## Known gaps

- Conflict errors name the owning tenant and the claim-id namespace is global, so a caller can learn whether another tenant has a given `claim_id` (SEC-18).
- An edge's `to_claim_id` is not tenant-checked, and graph output can emit another tenant's ids (SEC-18).
- Tenant ids are sanitized into directory names with collisions (`a.b` and `a_b` map to the same directory), so two tenants can share segment files (SEC-19, P0).
- Roles have no hierarchy and a JWT without a roles claim is granted all roles (SEC-11).
- There is no dedicated multi-tenant isolation test suite. The tests listed above cover the credential check; the store-level isolation is covered by unit tests only.

The full list, with severities and target phases, is in [`docs/plans/2026-10-09-issue-register.md`](https://github.com/BHAWESHBHASKAR/DASH/blob/main/docs/plans/2026-10-09-issue-register.md).
