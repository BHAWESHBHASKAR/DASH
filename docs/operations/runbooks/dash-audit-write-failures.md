# DashAuditWriteFailures

| | |
|---|---|
| Alert | `DashAuditWriteFailures` |
| Severity | critical |
| Rules | [`deploy/observability/prometheus/dash-alerts.rules.yml`](../../../deploy/observability/prometheus/dash-alerts.rules.yml) |
| Dashboards | DASH / Overview, DASH / Ingestion and WAL, DASH / Retrieval, embeddings and replication |

## Meaning

Appending to the hash-chained audit log failed (`dash_audit_write_failures_total` increased in the last 5 minutes).

## Impact

With `DASH_<SVC>_AUDIT_FAIL_CLOSED=1` mutations are refused with `503 audit log unavailable` (an outage). Otherwise requests proceed and the events are **missing from the audit trail**, which is a compliance incident.

## Diagnosis

```sh
kubectl -n dash-system exec <pod> -- df -h /var/lib/dash
kubectl -n dash-system exec <pod> -- ls -la /var/lib/dash/audit
kubectl -n dash-system logs <pod> --since=30m | grep -i audit
```

Verify the chain afterwards with `tools/audit-verify` ([audit chain](../audit-chain.md#verifier)).

Key queries:

```promql
increase(dash_audit_write_failures_total[5m])
rate(dash_audit_records_total[5m])
```

## Mitigation

Free space or fix permissions on the audit volume. A damaged tail is handled by the
writer (a `chain_restart` record); keep the old file for evidence. Record the window of missing
events in the incident.

## Escalation

If the mitigation does not resolve the alert within 30 minutes, or data loss is suspected, escalate to the storage on-call (WAL, replication) or the platform on-call (deployment, network), and open an incident following [the incident response policy](../../compliance/policies/incident-response-policy.md).
