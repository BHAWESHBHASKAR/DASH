# DashAuditDenialsDropped

| | |
|---|---|
| Alert | `DashAuditDenialsDropped` |
| Severity | warning |
| Rules | [`deploy/observability/prometheus/dash-alerts.rules.yml`](../../../deploy/observability/prometheus/dash-alerts.rules.yml) |
| Dashboards | DASH / Overview, DASH / Ingestion and WAL, DASH / Retrieval, embeddings and replication |

## Meaning

Authentication or authorization denials arrive faster than `DASH_AUDIT_DENIAL_MAX_PER_SEC`; the throttle dropped individual denial records (`dash_audit_denials_dropped_total` increased) and wrote summary records instead.

## Impact

The audit log stays bounded, but individual denied requests (actor fingerprint, request id) are not recorded. Often a credential-guessing attempt, or a client with a revoked or expired key in a retry loop.

## Diagnosis

```promql
rate(dash_ingest_auth_failure_total[5m])
rate(dash_transport_auth_failure_total[5m])
sum by (component, route, code) (rate(dash_http_server_requests_total{code=~"401|403|429"}[5m]))
```

The summary records in the audit log name the window and the count. The ingress access log shows
the source addresses.

Key queries:

```promql
increase(dash_audit_denials_dropped_total[15m])
```

## Mitigation

* An attack: block the sources at the ingress or firewall; rotate any credential that
  might be exposed.
* A misbehaving client: contact the owner (the actor fingerprint in the remaining audit records
  identifies the key), revoke or fix its credential.

## Escalation

If the mitigation does not resolve the alert within 30 minutes, or data loss is suspected, escalate to the storage on-call (WAL, replication) or the platform on-call (deployment, network), and open an incident following [the incident response policy](../../compliance/policies/incident-response-policy.md).
