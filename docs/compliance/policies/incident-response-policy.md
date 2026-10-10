# Incident Response Policy

> **Implementation status (0.3.0 unreleased tree):** this is a process template, not an implemented control. Prometheus alert rules exist in `deploy/observability/prometheus/dash-alerts.rules.yml` (21 alerts, unit-tested with `promtool test rules` in CI), each linked to a runbook in `docs/operations/runbooks/`; routing alerts to a pager is left to the operator. Credentials can be rotated without a restart only if the service was started with `DASH_CONFIG_RELOAD_FILE`: change the key or secret in that overlay file and send SIGHUP (a change to the process environment itself needs a restart); individual keys can be revoked through the revocation files and individual JWTs through the `jti` denylist (`DASH_*_JWT_REVOKED_JTIS_PATH`). Audit records carry a credential fingerprint (not the credential) and can be verified with `tools/audit-verify`. SEV definitions, notification windows and postmortem deadlines have no tooling behind them. See [soc2-readiness.md](../soc2-readiness.md).

## Purpose

Establish a consistent process for identifying, containing, and remediating security and availability incidents.

## Scope

Applies to all DASH production deployments and on-call personnel.

## Policy

1. **Detection**: Incidents are detected via Prometheus alerts (`/ready` failures, replication lag, disk fallback, auth failures) and external monitoring.
2. **Classification**: Incidents are classified by severity (SEV1 = data loss or unavailability, SEV2 = degraded performance, SEV3 = non-critical findings).
3. **Containment**: For suspected compromise, rotate `DASH_*_API_KEY` values and secrets and revoke affected keys and JWT `jti` values (see the status note above).
4. **Eradication**: Apply patches, restore from verified backups, and re-encrypt data if key exposure is suspected.
5. **Communication**: Internal stakeholders are notified within 1 hour for SEV1, customers within 24 hours where contractually required.
6. **Post-incident**: A blameless postmortem is completed within 5 business days and tracked until closure.
