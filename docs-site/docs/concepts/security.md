# Security

This page describes the security controls that exist in DASH 0.3.0 (unreleased), the gaps that remain, and the ones that are only planned. DASH is pre-1.0 and has had no external security review; read the [threat model](https://github.com/BHAWESHBHASKAR/DASH/blob/main/docs/threat-model.md) and the [issue register](https://github.com/BHAWESHBHASKAR/DASH/blob/main/docs/plans/2026-10-09-issue-register.md) before exposing a deployment to untrusted networks. The authentication details are in the [authentication guide](../operations/auth.md).

## Authentication

Each service (ingestion, retrieval) authenticates requests independently, and **denies by default**: a service started with no credentials exits (code 2) unless `DASH_INSECURE_DEV_MODE=1` is set, which is for local development only and binds loopback. Accepted credentials:

1. **API keys**: `x-api-key: <key>` or `Authorization: Bearer <key>`. Configured with `DASH_INGEST_API_KEY(S)` / `DASH_RETRIEVAL_API_KEY(S)`. Keys are compared in constant time but stored as plain strings (no hashing at rest, SEC-15). A legacy unscoped key carries the roles in `DASH_*_API_KEY_DEFAULT_ROLES` (default: `ingest` on ingestion, `retrieve` on retrieval).
2. **Scoped API keys**: `DASH_*_API_KEY_SCOPES`, entries `key:tenantA,tenantB[:role,...]` separated by `;`. A scoped key is limited to the listed tenants and roles.
3. **HS256 JWTs**: validated with `DASH_*_JWT_HS256_SECRET` (plus rotation secrets and a `kid` to secret map). `exp` is always required; there is a maximum lifetime (default 24 h), a leeway cap (60 s), optional `iss` and `aud`, and a `jti` denylist. The tenant list is read from `tenant_id`, `tenants` or `tenant_ids` (a `*` tenant only with `DASH_*_JWT_ALLOW_WILDCARD_TENANT=1`); roles from `dash_roles`. A token without a roles claim has no roles unless `DASH_*_JWT_DEFAULT_ROLES` is set.
4. **OIDC / JWKS** (`DASH_*_JWT_PROVIDER=oidc`): requires `iss`, `aud`, `exp` and a `kid`, accepts only asymmetric algorithms from an allow-list (RS256/384/512, PS256/384/512, ES256/384, EdDSA) and an `https://` JWKS URL. The JWKS cache is single-flight, serves stale keys for up to 24 h, caches failures and never fetches for malformed tokens. Tests use generated RS256 keys against an in-process stub IdP; the other algorithms and real identity providers are untested. There is no PEM public-key configuration (`DASH_*_JWT_PUBLIC_KEY` does not exist).

Authorization checks, in order: credential validity, revocation, role (`admin` implies every role; `read_only` implies `retrieve`; `ingest` and `retrieve` are independent), tenant scope (credential and the `DASH_*_ALLOWED_TENANTS` allowlist), and a per-tenant rate limit (429). `/v1/embeddings` needs `retrieve`; `/metrics` and `/debug/*` need `read_only`.

Revoke an API key with `DASH_*_REVOKED_API_KEYS` or by adding it to the file at `DASH_*_REVOKED_KEYS_PATH` (one key per line; re-read when the file changes), and a token with `DASH_*_JWT_REVOKED_JTIS` / `DASH_*_JWT_REVOKED_JTIS_PATH`. Authentication settings can be reloaded without a restart: set `DASH_CONFIG_RELOAD_FILE` and send SIGHUP. There is no key expiry and no overlap setting; rotate by adding the new key, moving clients, then removing the old one.

Between services:

- **Replication** (`/internal/replication/*`): `x-replication-token` must equal `DASH_INGEST_REPLICATION_TOKEN`; without a token the endpoints answer 403 and an ingestion follower refuses to start (outside dev mode).
- **Control plane**: `Authorization: Bearer <DASH_CONTROL_PLANE_TOKEN>` on every route except health and ready; the service refuses to start without a token outside dev mode.

Both are shared secrets. Every listener can serve HTTPS itself and verify client certificates (off by default; see [TLS and mutual TLS](https://github.com/BHAWESHBHASKAR/DASH/blob/main/docs/operations/tls.md)): ingestion can require a verified follower certificate on the replication routes, on top of the token, and pin follower certificates by SHA-256 fingerprint, and followers always verify the leader certificate. Without TLS the tokens travel over plain HTTP. Secret strength is validated at startup by default (`DASH_STRICT_SECRETS`): at least 16 characters for keys and tokens, 32 for HS256 secrets, no placeholders. The control-plane token has no length check.

### What changed in 0.3.0

| 0.2.x gap | Register | Status in 0.3.0 |
|---|---|---|
| Auth is fail-open when no credentials are configured | SEC-01 | Fixed: services refuse to start without credentials unless `DASH_INSECURE_DEV_MODE=1` (loopback only). |
| With only a JWT secret set, a request with no `Authorization` header is accepted | SEC-02 | Fixed. |
| `/v1/embeddings`, `/metrics`, `/debug/*` unauthenticated | SEC-09, SEC-10 | Fixed: they require a credential (`retrieve` / `read_only`). |
| Embeddings are computed before authentication on retrieve/ingest | SEC-09 | Fixed. |
| Replication endpoints open unless a token is set | SEC-08 | Fixed: `DASH_INGEST_REPLICATION_TOKEN` required. |
| Control plane has no authentication | SEC-07 | Fixed: `DASH_CONTROL_PLANE_TOKEN` required. |
| Rate limiter never throttles | SEC-06 | Fixed: enforced per tenant, HTTP 429 with `Retry-After`. |
| Secret validation is opt-in, minimum 16 characters | SEC-04/05 | Fixed: on by default, placeholders rejected, 32 characters for JWT secrets (16 for keys and tokens). |
| JWT without a roles claim gets all roles; roles have no hierarchy | SEC-11 | Fixed: role hierarchy, no roles without a claim or default. |
| OpenAI embedding provider has no TLS | SEC-23 | Fixed: HTTPS. |
| Keys stored and compared as plain strings | SEC-15 | Constant-time comparison added; keys still held in plaintext (P1). |

## Audit log

When `DASH_INGEST_AUDIT_LOG_PATH` / `DASH_RETRIEVAL_AUDIT_LOG_PATH` is set, each service appends a JSON line per audited request with a SHA-256 hash chain (`seq`, `prev_hash`, `hash`). Both services use one canonical encoding, a file lock and `fdatasync`, truncate a torn tail and record explicit `chain_restart` entries; `tools/audit-verify` (wrapped by `scripts/verify_audit_chain.sh`) verifies the files. The format is in the [data model](data-model.md#audit-record). Limits to understand:

- The chain is unkeyed. It catches accidental damage, not a deliberate rewrite by someone with write access to the file. There is no HMAC and no external anchoring; truncating the tail is caught only if you record the last `seq` and hash elsewhere.
- Records carry the tenant, action, status, reason, request id and a credential fingerprint (not the credential), but no JWT `jti`, client IP, or request/response hashes.
- `DASH_*_AUDIT_FAIL_CLOSED=1` refuses a request when the log cannot be opened, locked or its tail recovered; the append itself happens after the mutation, so this is a pre-check, not a write-ahead guarantee.
- Audit is off unless a path is set; the shipped Compose, Kubernetes, Helm and systemd examples set one.
- There is no retention setting, compaction or per-tenant chain; one chain per service log file.

## Encryption

With `DASH_ENCRYPTION_KEY_FILE` set, DASH encrypts its data files at rest (WAL, snapshot, replication exports, persisted vector index, segment files, redb values) with AES-256-GCM under per-file data keys wrapped by your key; it is off by default, audit logs and identifiers stay plaintext, and a node that finds encrypted files without its key refuses to start ([Encryption at rest](https://github.com/BHAWESHBHASKAR/DASH/blob/main/docs/operations/encryption.md)). In transit, each service can terminate TLS itself (TLS 1.2/1.3 via rustls, optional mutual TLS, certificate reload without restart; [TLS and mutual TLS](https://github.com/BHAWESHBHASKAR/DASH/blob/main/docs/operations/tls.md)), or sit behind a TLS-terminating proxy. The WAL carries per-record CRC-32 checksums for corruption detection; they are not a security control. The OpenAI embedding provider uses HTTPS, and refuses to send its key over plaintext HTTP to a non-loopback host.

## What was documented before and is not true

Earlier versions of this page described RS256/EdDSA-only JWTs, `claims:read` / `claims:write` scopes, `DashKey` API keys stored in redb with expiry, `DASH_API_KEY_OVERLAP_SECONDS`, per-tenant audit chains, and `DASH_AUDIT_RETENTION_DAYS` compaction. None of these exist. See [Planned API](../reference/planned-api.md).

## Reporting a vulnerability

See [`SECURITY.md`](https://github.com/BHAWESHBHASKAR/DASH/blob/main/SECURITY.md) and [About → Security](../about/security.md).
