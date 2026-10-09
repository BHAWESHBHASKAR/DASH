# SOC 2 Readiness Mapping: DASH

> **Status (2026-10-09): this is a readiness plan, not an attestation.** DASH has not been audited, has no SOC 2 report, and several controls below are partial or not implemented. The earlier version of this table stated controls as if present; the "Current status" column below replaces those claims with what the code and CI actually show (register item DOC-06). Remediation is tracked in the [master plan](../plans/2026-10-09-production-readiness-master-plan.md) and [issue register](../plans/2026-10-09-issue-register.md). Status words: **implemented**, **partial** (works with a known defect), **library only** (code exists, not used), **NOT IMPLEMENTED**, **planned (phase)**.

This document maps DASH controls to the AICPA Trust Services Criteria for **Security** and **Availability** for a self-hosted vector database.

## Scope

- **System**: DASH ingestion, retrieval, control-plane, indexer and metadata-router components.
- **Environment**: self-hosted deployments via Docker Compose, Helm or systemd.
- **Readiness target**: three months of evidence collection before an audit, after the P0 and P1 phases close the gaps below.

## Control mapping

| TSC | Control ID | Control statement (target) | Current status | Evidence or plan |
|---|---|---|---|---|
| CC6.1 | SEC-LOG-01 | Logical access to ingestion and retrieval requires an API key or signed JWT. | **Partial.** Fail-open when nothing is configured (SEC-01); a JWT-only configuration accepts requests with no `Authorization` header (SEC-02); `/v1/embeddings`, `/metrics`, `/debug/*` are unauthenticated (SEC-09, SEC-10). **v0.3.0** fixes these. | Tests: `transport_denies_revoked_ingest_key`, `transport_allows_ingest_jwt_signed_with_kid_secret`, `auth_policy_scoped_key_rejects_unknown_key_when_required_keys_are_unset`; code `services/*/src/transport/authz.rs`, `pkg/auth/src/lib.rs`. Plan: P0. |
| CC6.1 | SEC-RBAC-01 | Route-level roles (`admin`, `ingest`, `retrieve`, `read_only`) are enforced. | **Partial.** Roles checked per route for scoped keys and JWTs with a roles claim; no hierarchy, a JWT without a roles claim gets all roles, unscoped keys skip role checks (SEC-11). The control plane has no role or credential check (SEC-07). | Code `pkg/auth/src/lib.rs` (`RoleSet`), `authz.rs`. Plan: P0 (control plane), P1 (roles). |
| CC6.1 | SEC-OIDC-01 | OIDC / JWKS authentication can be enabled per service. | **Partial.** Implemented and unit-tested with a symmetric JWK; no RSA/EC test and no end-to-end IdP test; JWKS fetch has availability and caching defects (SEC-12, SEC-14). | Tests: `oidc_accepts_valid_hs256_jwk`, `oidc_rejects_wrong_tenant`, `oidc_rejects_issuer_mismatch`, `oidc_rejects_tampered_signature` in `pkg/auth/src/oidc.rs`. Plan: P1. |
| CC6.1 | SEC-KEY-01 | API keys are scoped to tenants and can be revoked. | **Partial.** Scoped keys and revocation (env list and file) work. **NOT IMPLEMENTED:** key expiry, key creation or last-used metadata, periodic review records, hashed storage (SEC-15). | Tests: `auth_policy_scoped_key_rejects_other_tenants`, `auth_policy_revoked_key_is_denied` (both services). Plan: P1. |
| CC6.6 | SEC-ENC-01 | Customer-managed encryption keys are supported via `pkg/encryption` (`env` provider). | **Library only.** The crate provides an `EncryptionProvider` trait and an environment master-key provider. No service or storage code depends on it, so setting `DASH_ENCRYPTION_*` has no effect (SEC-16). | Tests: `env_provider_round_trip`, `env_provider_rejects_wrong_aad` in `pkg/encryption/src/lib.rs`. Plan: **planned (P4)**. |
| CC6.6 | SEC-ENC-02 | Data at rest is encrypted with AES-256-GCM using a per-record nonce. | **NOT IMPLEMENTED.** The library can do AES-256-GCM with a random nonce, but nothing DASH writes (WAL, snapshot, redb, segments, audit log) is encrypted. Rely on volume encryption. | Plan: **planned (P4)**. |
| CC6.7 | SEC-TLS-01 | Data in transit is protected. | **NOT IMPLEMENTED in DASH.** Services speak plain HTTP and expect a TLS-terminating proxy. The OpenAI embedding client has no TLS in v0.2.x (SEC-23; **v0.3.0**). Replication and control-plane traffic are plaintext. | Plan: P0 (OpenAI TLS), P1 and P3 (service-to-service TLS). |
| CC7.1 | AVL-MON-01 | `/ready` and `/live` probes expose health and disk status. | **Implemented, untested.** Routes exist in both services and the control plane; no automated test was found for `/ready` behavior. | Code `services/ingestion/src/transport/read_routes.rs`, `services/retrieval/src/transport.rs`, `services/control-plane/src/lib.rs`. Plan: P1 (add tests). |
| CC7.2 | AVL-BKP-01 | Backup and restore scripts produce and validate state bundles. | **Partial.** Scripts create checksummed bundles and a CI job runs a backup/restore drill in Docker Compose. Bundles can be taken from a live WAL with no consistency guarantee (DATA-10). No encryption of bundles. | `scripts/backup_state_bundle.sh`, `scripts/restore_state_bundle.sh`, `scripts/backup_restore_drill.sh`; CI job "Backup/Restore Drill" in `.github/workflows/rust.yml`. |
| CC7.2 | AVL-HA-01 | Control-plane leader election and quorum read consistency are implemented. | **Partial.** A file-lease leader election exists, with unit tests; `read_consistency` and `write_consistency` are routing policies over a placement file. There is **no consensus replication and no automatic failover**; replication is single-writer polling; the control plane is unauthenticated (SEC-07). | Tests: `leader_lease_acquire_and_renew`, `leader_lease_transfers_after_expiry` (`services/control-plane/src/leader.rs`); `handle_request_read_route_reresolves_after_leader_promotion` (retrieval). Plan: **planned (P3)**. |
| CC7.3 | AVL-RCV-01 | WAL replay and snapshot compaction support recovery. | **Partial.** WAL replay and checkpoint compaction work in a single process. **NOT IMPLEMENTED:** point-in-time recovery. A torn WAL tail can fail startup (**v0.3.0** truncates it); no record checksums; multi-record writes are not atomic (DATA-10). Recovery-time objectives have not been measured. | Tests: `persistent_wal_replay_restores_claims_and_retrieval`, `checkpoint_compacts_wal_and_replays_snapshot_plus_delta` (`pkg/store/src/lib.rs`), `wal_persistence_and_replay_round_trip` (`pkg/store/tests/integration_retrieval.rs`). Plan: P0 (tail), P2 (atomicity). |
| CC7.2 | AVL-DOS-01 | Rate limits and request bounds protect availability. | **Partial.** Body cap (16 MiB) and a bounded worker queue exist. The per-tenant rate limiter does not throttle (SEC-06); no per-IP caps; JSON nesting is unbounded on retrieval (ROB-01). | **v0.3.0** enforces rate limits (429). Plan: P0/P1. |
| CC2.1 | GOV-DOC-01 | Runbooks and environment plans are under version control. | **Partial.** Documents are versioned, but earlier docs contained false claims. From 2026-10-09 each README claim is listed in `docs/claims-ledger.md` and checked by `scripts/check_claims_ledger.sh`. | `docs/plans/`, `docs/compliance/`, `docs/claims-ledger.md`. |
| CC4.1 | GOV-MON-01 | Prometheus metrics and alert rules are shipped. | **Partial.** `/metrics` and an alert rules file exist. Metrics are unauthenticated today (SEC-10; **v0.3.0** requires auth). The rules are not validated by any test. | `deploy/container/monitoring/prometheus-alert-rules.yml`. |
| CC4.2 | GOV-CHG-01 | CI validates formatting, clippy and tests, with a backup drill. | **Implemented.** `.github/workflows/rust.yml` runs `cargo fmt --check`, clippy with `-D warnings`, `cargo test --workspace --all-features`, a release build and the backup/restore drill. It does **not** run a benchmark smoke job (an earlier version claimed it did). Branch-protection and required-review settings are repository settings, not verified here. | `.github/workflows/rust.yml`. |
| CC7.1 | GOV-VULN-01 | Dependencies and code are scanned. | **Partial.** `.github/workflows/security.yml` runs `cargo audit`, a Trivy filesystem scan, CodeQL and Gitleaks. Actions are tag-pinned and Trivy is `@master` (SEC-21). | `.github/workflows/security.yml`. |
| CC8.1 | GOV-SUP-01 | Release artifacts are signed with provenance. | **NOT IMPLEMENTED.** The release workflow builds binaries and images and generates an SBOM; there is no signing or provenance attestation. | `.github/workflows/release.yml`. Plan: **planned (P7)**. |
| CC7.2 | GOV-AUD-01 | Audit log is tamper-evident and attributable. | **Partial.** SHA-256 hash chain, unkeyed; no principal in records; ingestion chain does not verify with the bundled verifier (SEC-17); off by default. **NOT IMPLEMENTED:** HMAC, external anchoring, retention compaction. | `services/*/src/transport/audit.rs`, `scripts/verify_audit_chain.sh`. Plan: **planned (P4)**. |

## Evidence collection

`scripts/soc2_evidence_collector.sh <output-dir>` gathers repository metadata, CI workflow definitions (not run logs), the result of `scripts/ci.sh`, backup/restore drill logs when available, Prometheus rule files, the encryption provider name (not the key), and `cargo audit` output when available. It is a convenience for assembling evidence; it does not make a control operate. No collected evidence set exists in this repository.

## Known gaps and next steps

1. **P0 (v0.3.0):** close SEC-01 to SEC-10 and SEC-23; make rate limiting real; fix evidence duplication.
2. **P1:** role hierarchy and key lifecycle (expiry, review metadata, hashed storage); OIDC hardening and tests; deployment manifests and CI hardening.
3. **P3:** consensus replication and automatic failover; service-to-service TLS.
4. **P4:** wire encryption into WAL, snapshot, redb and segment write paths; key management (KMS provider, rotation, key ids); keyed audit chain with anchoring and retention.
5. **P7:** signed images, provenance, digest pinning, external penetration test.

The AWS KMS provider and external uptime probe mentioned in earlier versions are not implemented and are not currently scheduled in a specific phase.

## Roles and responsibilities

- **Engineering**: maintain controls, fix findings, and keep evidence current.
- **Security/Compliance**: review collector output, track exceptions, and coordinate auditor access.
- **Operations**: run backups, monitor alerts, and own incident response playbooks.

The policy templates in [`policies/`](policies/) state target policy; each carries a status note.
