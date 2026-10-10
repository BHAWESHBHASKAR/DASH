# DashErrorBudgetBurn

| | |
|---|---|
| Alert | `DashErrorBudgetBurn` |
| Severity | critical (window=fast), warning (window=slow) |
| Rules | [`deploy/observability/prometheus/dash-alerts.rules.yml`](../../../deploy/observability/prometheus/dash-alerts.rules.yml) |
| Dashboards | DASH / Overview, DASH / Ingestion and WAL, DASH / Retrieval, embeddings and replication |

## Meaning

The availability SLO (99.9% of API requests without a 5xx over 30 days, see [slos.md](../slos.md)) is being consumed too fast. `window=fast`: burn rate above 14.4 over both 1 hour and 5 minutes (2% of the monthly budget per hour; the budget is gone in about 2 days). `window=slow`: burn rate above 6 over 6 hours and 30 minutes (gone in about 5 days).

## Impact

Users see errors now (fast) or a sustained low-grade failure (slow). If it continues, the SLO is missed for the month and feature work should yield to reliability work.

## Diagnosis

1. Current burn rate per component (1 = on budget):

   ```promql
   dash:slo_availability_errors:ratio_rate1h / 0.001
   ```

2. Which routes and codes produce the errors: follow the diagnosis in
   [DashHighErrorRate](dash-high-error-rate.md); the burn-rate alert fires earlier for lower error
   ratios (above 1.44% for the fast window, 0.6% for the slow one).
3. The recording rules come from `deploy/observability/prometheus/dash-recording.rules.yml`; if
   they are not loaded the alert never fires and the SLO panels are empty.

Key queries:

```promql
dash:slo_availability_errors:ratio_rate1h / 0.001
dash:slo_availability_errors:ratio_rate6h / 0.001
```

## Mitigation

Mitigate the underlying error source (see DashHighErrorRate). For a planned, accepted
budget spend (for example a migration), silence the `window=slow` alert for the maintenance window
only, never the fast one.

## Escalation

If the mitigation does not resolve the alert within 30 minutes, or data loss is suspected, escalate to the storage on-call (WAL, replication) or the platform on-call (deployment, network), and open an incident following [the incident response policy](../../compliance/policies/incident-response-policy.md).
