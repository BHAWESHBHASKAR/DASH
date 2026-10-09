# Security

This page describes the security controls that exist in DASH today, the ones that change in v0.3.0, and the ones that are only planned. DASH is pre-1.0; read the [threat model](https://github.com/BHAWESHBHASKAR/DASH/blob/main/docs/threat-model.md) and the [issue register](https://github.com/BHAWESHBHASKAR/DASH/blob/main/docs/plans/2026-10-09-issue-register.md) before exposing a deployment to untrusted networks.

## Authentication

Each service (ingestion, retrieval) authenticates requests independently. Accepted credentials:

1. **API keys**: `x-api-key: <key>` or `Authorization: Bearer <key>`. Configured with `DASH_INGEST_API_KEY(S)` / `DASH_RETRIEVAL_API_KEY(S)`. Keys are compared as plain strings (no hashing at rest).
2. **Scoped API keys**: `DASH_*_API_KEY_SCOPES`, entries `key:tenantA,tenantB[:role,...]` separated by `;`. A scoped key is limited to the listed tenants and roles.
3. **HS256 JWTs**: validated with `DASH_*_JWT_HS256_SECRET` (plus rotation secrets and a `kid` to secret map). Optional `iss`, `aud`, leeway and required `exp`. The tenant list is read from `tenant_id`, `tenants` or `tenant_ids`; roles from `dash_roles`.
4. **OIDC / JWKS** (`DASH_*_JWT_PROVIDER=oidc`): validates tokens against an issuer's JWKS, using the algorithm named in the token header and the matching JWK (via the `jsonwebtoken` crate). The unit tests only exercise a symmetric (`oct`/HS256) JWK; RSA/EC JWKs are not covered by tests in this repository. There is no PEM public-key configuration (`DASH_*_JWT_PUBLIC_KEY` does not exist) and no required scope names such as `claims:read`.

Authorization checks, in order: credential validity, revocation, tenant scope (credential and the `DASH_*_ALLOWED_TENANTS` allowlist), and role (`admin`, `ingest`, `retrieve`, `read_only`). Roles do not imply each other.

Revoke an API key with `DASH_*_REVOKED_API_KEYS` or by adding it to the file at `DASH_*_REVOKED_KEYS_PATH` (one key per line; re-read on each request). There is no key expiry and no overlap/rotation setting; rotate by adding the new key to `DASH_*_API_KEYS`, moving clients, then removing the old one.

### Known gaps (v0.2.x), fixed or changing in v0.3.0

| Gap | Register | v0.3.0 |
|---|---|---|
| Auth is fail-open when no credentials are configured | SEC-01 | Services refuse to start without credentials unless `DASH_INSECURE_DEV_MODE=1` (localhost only). |
| With only a JWT secret set, a request with no `Authorization` header is accepted | SEC-02 | P0 fix. |
| `/v1/embeddings`, `/metrics`, `/debug/*` unauthenticated | SEC-09, SEC-10 | Require auth. |
| Embeddings are computed before authentication on retrieve/ingest | SEC-09 | P0 fix. |
| Replication endpoints open unless a token is set | SEC-08 | `DASH_INGEST_REPLICATION_TOKEN` required. |
| Control plane has no authentication | SEC-07 | `DASH_CONTROL_PLANE_TOKEN` required. |
| Rate limiter never throttles | SEC-06 | Enforced; HTTP 429. |
| Secret validation is opt-in, minimum 16 characters | SEC-04/05 | On by default, minimum 32 characters, placeholders rejected. |
| JWT without a roles claim gets all roles; roles have no hierarchy | SEC-11 | P1. |
| OpenAI embedding provider has no TLS | SEC-23 | TLS added. |

## Audit log

When `DASH_INGEST_AUDIT_LOG_PATH` / `DASH_RETRIEVAL_AUDIT_LOG_PATH` is set, each service appends a JSON line per audited request with a SHA-256 hash chain (`seq`, `prev_hash`, `hash`). The format is in the [data model](data-model.md#audit-record). Limits to understand:

- The chain is unkeyed. It catches accidental damage, not a deliberate rewrite by someone with write access to the file. There is no HMAC and no external anchoring.
- Records carry the tenant, action, status and reason, but no principal, JWT `jti`, or request/response hashes.
- Ingestion's chain does not verify with `scripts/verify_audit_chain.sh` today (SEC-17).
- Audit is off by default and is not enabled in any shipped deployment manifest.
- There is no retention setting, compaction or per-tenant chain; one chain per service log file.

## Encryption

DASH does not encrypt data at rest and does not terminate TLS. Use an encrypted volume and a TLS-terminating proxy. `pkg/encryption` is an AES-256-GCM library with an environment-key provider; no service calls it, so `DASH_ENCRYPTION_*` settings have no effect (SEC-16, planned P4). The OpenAI embedding provider needs v0.3.0 for TLS.

## What was documented before and is not true

Earlier versions of this page described RS256/EdDSA-only JWTs, `claims:read` / `claims:write` scopes, `DashKey` API keys stored in redb with expiry, constant-time comparison, `DASH_API_KEY_OVERLAP_SECONDS`, per-tenant audit chains, and `DASH_AUDIT_RETENTION_DAYS` compaction. None of these exist. See [Planned API](../reference/planned-api.md).

## Reporting a vulnerability

See [`SECURITY.md`](https://github.com/BHAWESHBHASKAR/DASH/blob/main/SECURITY.md) and [About → Security](../about/security.md).
