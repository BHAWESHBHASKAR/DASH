# Access Control Policy

> **Implementation status (0.3.0 unreleased tree):** this is a target policy. Implemented: API keys, HS256 JWTs and (optionally) OIDC; role and tenant scoping with a role hierarchy (`admin`, `ingest`, `retrieve`, `read_only`); deny-by-default startup; key revocation via `DASH_*_REVOKED_API_KEYS` / `DASH_*_REVOKED_KEYS_PATH` (file re-read when its mtime or size changes) and JWT `jti` revocation via `DASH_*_JWT_REVOKED_JTIS` / `DASH_*_JWT_REVOKED_JTIS_PATH`; authentication settings reload on SIGHUP without a restart. Partial: OIDC is optional, not required (item 1 is not enforced); audit records carry a credential fingerprint as actor, not a named principal. **NOT IMPLEMENTED:** quarterly access reviews and dormant-key removal (no key creation or last-used metadata exists), key expiry, hashed key storage (SEC-15). See [soc2-readiness.md](../soc2-readiness.md) and `docs/operations/auth.md`.

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
