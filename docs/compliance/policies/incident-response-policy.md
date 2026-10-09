# Incident Response Policy

> **Implementation status (2026-10-09):** this is a process template, not an implemented control. Prometheus alert rules exist in `deploy/container/monitoring/prometheus-alert-rules.yml` (disk unavailable, ingest-to-visible lag, storage divergence, server error rate, readiness failing) but are not validated by tests. Rotating `DASH_*_API_KEY` values requires a restart of the affected service; there is no JWT revocation (`jti`) mechanism, so "revoke affected JWTs" means rotating the signing secret. SEV definitions, notification windows and postmortem deadlines have no tooling behind them. See [soc2-readiness.md](../soc2-readiness.md).

## Purpose

Establish a consistent process for identifying, containing, and remediating security and availability incidents.

## Scope

Applies to all DASH production deployments and on-call personnel.

## Policy

1. **Detection**: Incidents are detected via Prometheus alerts (`/ready` failures, replication lag, disk fallback, auth failures) and external monitoring.
2. **Classification**: Incidents are classified by severity (SEV1 = data loss or unavailability, SEV2 = degraded performance, SEV3 = non-critical findings).
3. **Containment**: For suspected compromise, rotate `DASH_*_API_KEY` values and revoke affected JWTs.
4. **Eradication**: Apply patches, restore from verified backups, and re-encrypt data if key exposure is suspected.
5. **Communication**: Internal stakeholders are notified within 1 hour for SEV1, customers within 24 hours where contractually required.
6. **Post-incident**: A blameless postmortem is completed within 5 business days and tracked until closure.
