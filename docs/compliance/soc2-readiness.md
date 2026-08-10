# SOC 2 Type II Readiness — DASH

This document maps DASH controls to the AICPA Trust Services Criteria for **Security** and **Availability**, the two categories most relevant to a self-hosted vector database handling customer data.

## Scope

- **System**: DASH vector database (ingestion, retrieval, control-plane, indexer, metadata-router services).
- **Environment**: self-hosted deployments via Docker Compose, Helm, or systemd.
- **Period readiness target**: 3 months of evidence collection before an audit.

## Control mapping

| TSC | Control ID | Control statement | Evidence location |
|---|---|---|---|
| CC6.1 | SEC-LOG-01 | Logical access to ingestion/retrieval requires API key or signed JWT. | `pkg/auth`, `services/*/transport/authz.rs` |
| CC6.1 | SEC-RBAC-01 | Route-level roles (admin, ingest, retrieve, read_only) are enforced. | `services/ingestion/src/transport/authz.rs`, `services/retrieval/src/transport/authz.rs` |
| CC6.1 | SEC-OIDC-01 | Generic OIDC / JWKS authentication can be enabled per service. | `pkg/auth/src/oidc.rs` |
| CC6.1 | SEC-KEY-01 | API keys are scoped to tenants and can be revoked. | `services/*/transport/authz.rs`, revocation list files |
| CC6.6 | SEC-ENC-01 | Customer-managed encryption keys are supported via `pkg/encryption` (`env` provider). | `pkg/encryption` |
| CC6.6 | SEC-ENC-02 | Encryption at rest uses AES-256-GCM with per-record nonce. | `pkg/encryption/src/lib.rs` |
| CC7.1 | AVL-MON-01 | `/ready` and `/live` probes expose health and disk status. | `services/*/src/transport.rs` |
| CC7.2 | AVL-BKP-01 | Backup/restore scripts produce and validate state bundles. | `scripts/backup_state_bundle.sh`, `scripts/restore_state_bundle.sh` |
| CC7.2 | AVL-HA-01 | Control-plane leader election and quorum read consistency are implemented. | `services/control-plane/src/leader.rs`, `services/retrieval/src/transport.rs` |
| CC7.3 | AVL-RCV-01 | WAL replay and snapshot compaction support point-in-time recovery. | `pkg/store/src/wal.rs` |
| CC2.1 | GOV-DOC-01 | Runbooks and environment plans are under version control. | `docs/plans/`, `docs/compliance/` |
| CC4.1 | GOV-MON-01 | Prometheus metrics and alert rules are shipped. | `deploy/container/monitoring/prometheus-alert-rules.yml` |
| CC4.2 | GOV-CHG-01 | CI validates formatting, clippy, tests, backup drill, and benchmark smoke. | `.github/workflows/rust.yml` |

## Evidence collection

Run the evidence collector monthly (or before an audit):

```bash
./scripts/soc2_evidence_collector.sh /path/to/evidence/output
```

It gathers:

- `git log` / `git diff` for the period
- CI workflow results from `.github/workflows/`
- Backup/restore drill logs
- Prometheus alert rules
- Encryption provider configuration (non-sensitive, name only)
- Open dependency vulnerabilities (`cargo audit` if available)

## Known gaps and next steps

1. **CMEK integration**: `pkg/encryption` provides the trait and an `env` master-key provider. The next step is to wire it into the WAL/snapshot write path so data is encrypted before hitting disk.
2. **AWS KMS provider**: implement behind a feature flag once the trait is proven in the WAL path.
3. **SOC 2 Availability monitoring**: add an external uptime probe to the Helm chart.
4. **Penetration test**: schedule a third-party pen test before audit.

## Roles and responsibilities

- **Engineering**: maintain controls, fix findings, and keep evidence current.
- **Security/Compliance**: review collector output, track exceptions, and coordinate auditor access.
- **Operations**: run backups, monitor alerts, and own incident response playbooks.
