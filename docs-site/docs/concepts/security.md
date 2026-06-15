# Security

DASH is built to be deployed in environments that are subject to audit — legal-tech, med-tech, fintech, enterprise. The security model is small, explicit, and conservative. This page describes the auth model, the API-key scoping, the hash-chained audit log, and the threat model.

## JWT auth model

DASH uses **JWT bearer tokens** for all authenticated endpoints. The token is verified against an **asymmetric public key** (`RS256` or `EdDSA`); the private key never lives in the DASH process.

```text
Authorization: Bearer eyJhbGciOiJSUzI1NiIs...
```

The JWT is verified by the `pkg/auth` crate (which uses `jsonwebtoken`). The required claims are:

| Claim      | Required | Type             | Meaning                                          |
| ---------- | -------- | ---------------- | ------------------------------------------------ |
| `sub`      | yes      | string           | The principal (a user ID, service-account ID).   |
| `tenants`  | yes      | array of strings | The tenants the principal is authorized for.     |
| `scopes`   | yes      | array of strings | The capabilities (see below).                    |
| `exp`      | yes      | integer (unix)   | Expiration.                                      |
| `iat`      | yes      | integer (unix)   | Issued-at.                                       |
| `aud`      | yes      | string           | Must equal `dash`.                               |

The signing key is configured by `DASH_INGEST_JWT_PUBLIC_KEY` / `DASH_RETRIEVAL_JWT_PUBLIC_KEY` (PEM, env var) or by a path (`DASH_*_JWT_PUBLIC_KEY_PATH`). A token with an unknown `kid` is rejected.

### Scopes

| Scope             | Grants                                              |
| ----------------- | --------------------------------------------------- |
| `claims:read`     | `POST /v1/retrieve`, `POST /v1/embeddings`          |
| `claims:write`    | `POST /v1/ingest`                                   |
| `audit:read`      | The audit-log read endpoint (if enabled).           |
| `admin:tenants`   | Tenant create / delete in the control plane.        |

A token without `claims:write` cannot call `POST /v1/ingest`. A token without `claims:read` cannot call `POST /v1/retrieve`. The check is in the route handler; a scope mismatch returns HTTP 403.

### Token lifetime

DASH does not enforce a maximum token lifetime — the issuer is responsible. Recommended ceiling: **1 hour** for user tokens, **15 minutes** for service-account tokens. A refresh-token flow is the issuer's responsibility (DASH does not issue tokens).

## Scoped API keys

For service-to-service callers that cannot easily mint JWTs (legacy backends, cron jobs, embedded agents), DASH supports **scoped API keys**. A scoped API key is a 256-bit random value, base64url-encoded, with a record in redb:

```json
{
  "key_id": "ak_01HMRX...",
  "tenant_id": "t1",
  "scopes": ["claims:read", "claims:write"],
  "created_at_unix": 1718300000,
  "expires_at_unix": 1720976000,
  "last_used_at_unix": 1718300050
}
```

The caller passes the key in the `Authorization` header with a `DashKey` scheme:

```text
Authorization: DashKey ak_01HMRX...
```

The lookup is constant-time on the key prefix; the full key is compared with a `subtle::ConstantTimeEq`. A key with `expires_at_unix` in the past returns HTTP 401.

API keys are **tenant-scoped**: a key for `t1` cannot be used to read or write claims in `t2`. The check is in the same code path as the JWT tenant check.

### Rotation

A new key can be issued with the same `key_id` and a new secret. The old key remains valid for the duration of `DASH_API_KEY_OVERLAP_SECONDS` (default: 60 seconds) so that callers can roll without downtime.

## Hash-chained audit log

Every authenticated state change is recorded in a **hash-chained audit log**. The format is described in [Data model → AuditEvent](data-model.md#auditevent). The chain is verified by `scripts/verify_audit_chain.sh`:

```bash
./scripts/verify_audit_chain.sh /var/lib/dash/audit.log
# [OK] chain verifies, 17842 events
```

A modified event invalidates every subsequent `hash`. The chain is the **primary tamper-evidence mechanism** in DASH — it is the artifact the auditor inspects when a question of the form "did anyone modify the data?" comes up.

The chain is **per-tenant**. A break in tenant `t1`'s chain does not affect tenant `t2`'s chain.

### Retention

`DASH_AUDIT_RETENTION_DAYS` (default: 2555 — seven years) controls how long audit events are retained. Events older than the retention window are compacted into a daily summary record (event count, hash of the day's chain) and the individual records are deleted.

## Threat model

The full threat model is in [Security → Threat model](../about/security.md#threat-model). The short version:

| Threat                                            | Mitigation                                            |
| ------------------------------------------------- | ----------------------------------------------------- |
| Stolen JWT                                         | Short token lifetime; audience claim; `aud = dash`.   |
| Stolen API key                                     | Per-tenant scoping; rotation overlap; constant-time compare. |
| Cross-tenant data leak                             | Hard isolation in the store + the API layer.           |
| Tampering with audit log                           | SHA-256 hash chain; verify script in CI.               |
| Replay of ingest                                   | Idempotency keys; redb dedup.                          |
| DoS via large payload                              | `Content-Length` cap; per-tenant rate limit.           |
| Slow-loris DoS                                     | TCP read timeout in the transport.                     |
| SQL injection                                      | N/A — DASH is not SQL.                                 |
| Dependency CVE                                     | `cargo audit` in CI; `trivy` on the image.             |
| Compromised redb file                              | `redb` is ACID; replay from WAL reconstructs.          |
| Compromised process memory                        | JWT public-key only; private key never enters DASH.   |
| Compromised operator workstation                  | Out of scope; see the operator-host-hardening guide.   |

For the full threat model, including the residual risks, see [Security policy](../about/security.md).
