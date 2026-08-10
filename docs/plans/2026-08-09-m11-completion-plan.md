# M11 Completion Plan — RBAC, OIDC, CMEK, SOC 2 Readiness

## Goal

Close the remaining production-adoption blockers that mature vector databases ship as table-stakes: enterprise identity (OIDC), coarse-grained authorization (RBAC), customer-managed encryption keys (CMEK / BYOK), and the evidence package needed for SOC 2 Type II readiness.

## Current baseline

DASH already has:
- Shared-secret HS256 JWT verification (`pkg/auth`).
- Tenant-scoped API keys and per-key tenant allowlists (`services/*/transport/authz.rs`).
- Per-tenant rate limits and revocation lists.
- Hash-chained audit log + `verify_audit_chain.sh`.
- `DASH_STRICT_SECRETS=1` startup guard and `scripts/generate-secrets.sh`.
- TLS is assumed to be terminated by the ingress / load balancer.

What is **not** present:
- OIDC / RS256 / JWKS-based authentication.
- Role-based access control beyond per-tenant scope (no read-only, admin, or ingest-only roles).
- Application-level encryption or CMEK integration; at-rest encryption is currently delegated to the block device.
- SOC 2 control evidence collection (policy templates, evidence scripts, runbooks).

## M11 sub-milestones

### M11a — OIDC / RS256 / JWKS authentication (1.5 sessions)

1. Extend `pkg/auth` with an `OidcJwtConfig`:
   - `DASH_*_JWT_MODE=hs256|oidc`.
   - `DASH_*_JWT_ISSUER`, `DASH_*_JWT_AUDIENCE`, `DASH_*_JWT_JWKS_URL`, `DASH_*_JWT_JWKS_REFRESH_MINUTES`.
   - Add `ureq` or `reqwest` as a minimal blocking HTTPS client for JWKS fetch.
2. Add `verify_oidc_token_for_tenant` that:
   - Decodes the JWT header to get `kid`.
   - Caches JWKS keys in memory with TTL.
   - Verifies signature with `jsonwebtoken::DecodingKey::from_rsa_components` (or ECDSA/EdDSA keys as a follow-up).
   - Enforces `iss`/`aud`/`exp`/`nbf` and extracts the tenant claim.
3. Keep the existing HS256 path 100% backward-compatible.
4. Add tests: expired token, wrong issuer, tampered signature, missing `kid`, key rotation.
5. Update Helm `values.yaml` / `config.yaml` and `deploy/container/.env.example` with OIDC env vars.

### M11b — Role-based access control (1 session)

1. Introduce roles and permissions:
   - `admin:*` — control-plane placement/failover, audit export, key revocation.
   - `ingest:<tenant>` — write claims/evidence/edges for `<tenant>`.
   - `retrieve:<tenant>` — read/retrieve for `<tenant>`.
   - `read_only:<tenant>` — retrieve only, no graph mutations.
2. Extend the existing `scoped_api_keys` / `API_KEY_SCOPES` parser to accept a 3rd field:
   - `key:tenant:role` or `key:tenant:action1,action2`.
3. Extend `AuthDecision` to return `Forbidden` with a role-aware reason.
4. Plumb role checks into HTTP route dispatch in `services/ingestion/src/transport/routes.rs` and `services/retrieval/src/transport/routes.rs`:
   - `POST /v1/ingest*` requires `ingest:<tenant>`.
   - `POST /v1/ingest/batch` same.
   - `POST /v1/retrieve` requires `retrieve:<tenant>` or `read_only:<tenant>`.
   - `PUT|POST /v1/control-plane/*` requires `admin:*`.
   - `GET /v1/control-plane/*` can be `admin:*` or read-only for observers.
5. Add env `DASH_*_REQUIRE_ROLES=true` to fail closed when role config is present.
6. Add tests for cross-role denials and admin-only endpoints.

### M11c — Customer-managed encryption keys / envelope encryption (1.5 sessions)

**Status: implemented.** `pkg/encryption` provides:

1. `EncryptionProvider` trait with `encrypt`, `decrypt`, `name`.
2. `DASH_ENCRYPTION_PROVIDER=none|env|aws-kms`.
3. `env` provider: 256-bit master key from `DASH_ENCRYPTION_MASTER_KEY` (hex or base64), AES-256-GCM with a random nonce.
4. `aws-kms` provider (feature `aws-kms`): uses AWS KMS `GenerateDataKey`/`Decrypt` to unwrap a data key, then AES-256-GCM locally. Supports `DASH_AWS_KMS_KEY_ID`, `DASH_AWS_KMS_REGION`, and `DASH_ENCRYPTION_WRAPPED_KEY_FILE`.
5. `FileWal` and `InMemoryStore` now use the configured provider to encrypt WAL/snapshot lines and decrypt on replay; plaintext `enc:` prefix makes encrypted lines self-describing and backward-compatible with unencrypted WALs.
6. Tests: `encrypted_wal_round_trip_replays_claim`, `encrypted_wal_file_is_not_plaintext`, `encrypted_wal_without_provider_fails_to_replay`.

Remaining follow-up:
- Per-tenant DEK rotation.
- Segment manifest encryption and wrapped-DEK storage for immutable segments.

### M11d — SOC 2 Type II readiness package (0.75 session)

1. Create `docs/compliance/soc2-readiness.md`:
   - Control mapping to SOC 2 trust principles (Security, Availability, Processing Integrity, Confidentiality, Privacy as applicable).
   - Owner and evidence location for each control.
   - Runbook links.
2. Add `scripts/soc2_evidence_collector.sh` that exports:
   - `audit.jsonl` for a date range.
   - Current RBAC/API key snapshot (sanitized).
   - `cargo audit`, `trivy`, `gitleaks` results.
   - `/version` and `/metrics` snapshots.
   - `control-plane` placement state checksum.
3. Add `docs/compliance/policies/` templates:
   - Access Control Policy.
   - Encryption & Key Management Policy.
   - Audit Logging & Monitoring Policy.
   - Incident Response Runbook.
4. Update `docs/security-checklist.md` to require:
   - OIDC/RS256 enabled in staging/production.
   - Encryption provider configured.
   - SOC 2 evidence exported and signed before release.

### M11e — Tests, CI, and hardening (0.75 session)

1. Add `tests/authz_integration.rs` covering OIDC + RBAC + tenant isolation.
2. Add `tests/encryption_integration.rs` round-trip for each provider.
3. Update `.github/workflows/rust.yml` to run an OIDC mock (e.g., `mock-oauth2-server` container) and encryption tests.
4. Add `scripts/security_signoff_gate.sh` improvements:
   - JWT HS256 → RS256 rotation drill.
   - Encryption provider switch drill.
5. Update `deploy/container/docker-compose.yml` with an optional `oidc-provider` service for local testing.

## Implementation sequence

1. **M11a** (OIDC) first — it is the identity foundation for RBAC.
2. **M11b** (RBAC) second — authorization depends on authenticated identity.
3. **M11c** (CMEK) third — encryption is independent but easier to test once auth is solid.
4. **M11d** (SOC 2) last — it documents and collects evidence from the above.
5. **M11e** runs in parallel as each sub-milestone lands.

Estimated total: **4–5 sessions** (OIDC 1.5 + RBAC 1 + CMEK 1.5 + SOC2 0.75 + CI 0.75, with overlap).

## Decisions needed before implementation

1. **Identity provider / OIDC**
   - Generic OIDC (any provider with a JWKS endpoint) is recommended.
   - But if you have a specific vendor, tell me now: Auth0, Okta, Keycloak, WorkOS, Azure Entra, AWS Cognito, Google, etc. — the JWKS/key format and claim name for tenant/role may differ.
2. **KMS / key management**
   - Which cloud or KMS should be the first-class CMEK provider? AWS KMS, GCP Cloud KMS, Azure Key Vault, HashiCorp Vault, or a local HSM?
   - Should the self-hosted `env` provider be the default (operators supply `DASH_ENCRYPTION_MASTER_KEY`), or should the default remain `none` for backward compatibility?
3. **SOC 2 scope**
   - Type I (point-in-time) or Type II (operational effectiveness over time)?
   - Which trust principles are in scope initially? At minimum Security (CC) and Availability; add Confidentiality if encryption is required.
   - Do you have an auditor already, or should the evidence package be generic and auditor-agnostic?

## Proposed defaults if you want to move fast

- **OIDC**: generic OpenID Connect with `email` and `dash_tenant` / `dash_role` claims, JWKS auto-refresh.
- **KMS**: implement `env` provider first, then AWS KMS, then Vault.
- **SOC 2**: target a generic Type II readiness package for the **Security** and **Availability** trust principles.

## Risks

- OIDC adds an external network dependency; we need a startup mode that fails closed if `DASH_STRICT_SECRETS=1` and JWKS is unreachable.
- CMEK changes the WAL/segment format; encryption must be opt-in per tenant to avoid breaking existing data.
- KMS credentials must not be logged or embedded in containers; we will rely on cloud IAM / Vault auth, never static keys in env.
- SOC 2 evidence scripts must not export plaintext secrets or unredacted keys.
