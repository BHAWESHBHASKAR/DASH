# Encryption Policy

> **Implementation status (2026-10-09): NOT IMPLEMENTED in the services.** `pkg/encryption` is a library (AES-256-GCM, random 96-bit nonce, caller-supplied AAD, environment master-key provider); no WAL, snapshot, redb, segment, backup bundle or audit-log code calls it, so nothing DASH writes is encrypted by DASH. There is no tenant-AAD wiring, no re-encryption during backups, no KMS provider and no key rotation tooling. Until the work in phase P4 lands, protect data with volume-level encryption and treat items 2, 3 and 5 below as goals. See [soc2-readiness.md](../soc2-readiness.md) (SEC-ENC-01, SEC-ENC-02).

## Purpose

Protect customer data at rest using customer-managed encryption keys.

## Scope

Applies to all DASH state bundles, WAL files, snapshots, and backups.

## Policy

1. **Key ownership**: Production deployments must use a customer-managed master key; the default `env` provider is acceptable only in single-node self-hosted deployments with restricted access.
2. **Algorithm**: Data is encrypted with AES-256-GCM using a 96-bit nonce and 128-bit tag per record.
3. **Key rotation**: Master keys are rotated at least annually. New data is encrypted with the new key; old data is re-encrypted during the next scheduled backup window.
4. **Key storage**: Keys are never committed to version control. Use a secrets manager (Vault, AWS Secrets Manager, etc.) and inject via environment variables or a KMS provider.
5. **AAD**: Tenant identifier is used as additional authenticated data to prevent cross-tenant ciphertext replay.
