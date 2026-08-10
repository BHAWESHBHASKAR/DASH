# Access Control Policy

## Purpose

Define how access to DASH infrastructure and customer data is granted, reviewed, and revoked.

## Scope

Applies to all production DASH deployments and all personnel with administrative access.

## Policy

1. **Authentication**: All administrative and tenant-level access must use API keys or signed JWTs. OIDC is required for production multi-tenant environments.
2. **Authorization**: Access is enforced by role (admin, ingest, retrieve, read_only) and tenant scope. Principle of least privilege applies.
3. **Revocation**: API keys can be revoked via `DASH_*_REVOKED_KEYS_PATH` files. Revoked keys are rejected immediately after reload.
4. **Reviews**: Access grants are reviewed quarterly. Dormant keys older than 90 days are removed.
5. **Audit**: Authentication decisions are logged via the audit chain and Prometheus counters.
