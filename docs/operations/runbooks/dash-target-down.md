# DashTargetDown

| | |
|---|---|
| Alert | `DashTargetDown` |
| Severity | critical |
| Rules | [`deploy/observability/prometheus/dash-alerts.rules.yml`](../../../deploy/observability/prometheus/dash-alerts.rules.yml) |
| Dashboards | DASH / Overview, DASH / Ingestion and WAL, DASH / Retrieval, embeddings and replication |

## Meaning

Prometheus could not scrape `/metrics` of a DASH target (`up == 0`) for 2 minutes.

## Impact

The process may be down (no traffic served by that instance), or only monitoring is blind: a rejected scrape credential or a NetworkPolicy change also produces `up == 0`. Every other DASH alert for that instance is unreliable while this fires.

## Diagnosis

1. Check the scrape error on the Prometheus *Targets* page (`connection refused`,
   `401 Unauthorized`, `403 Forbidden`, timeout).
2. Check the process:

```sh
# Kubernetes (adjust release and namespace):
kubectl -n dash-system get pods -l app.kubernetes.io/name=dash -o wide
kubectl -n dash-system logs <pod> --since=30m | grep -iE 'error|warn|poison|refus'
# Readiness reason (no credential needed):
kubectl -n dash-system exec <pod> -- wget -qO- http://127.0.0.1:8081/ready   # ingestion
kubectl -n dash-system exec <pod> -- wget -qO- http://127.0.0.1:8080/ready   # retrieval
# systemd:
journalctl -u dash-ingestion --since '-30 min'
```

3. `401`/`403`: `/metrics` needs a credential with the `read_only` (or `admin`) role on ingestion
   and retrieval, or the control-plane token on the control plane, unless `DASH_METRICS_PUBLIC=1`.
   A rotated key or a revoked `jti` breaks the scrape.
4. Restarts: `kubectl get pod <pod> -o jsonpath='{.status.containerStatuses[0].restartCount}'`;
   `dash_process_uptime_seconds` resets on restart. Exit code 2 at startup is a configuration
   refusal (secrets, TLS, bind); the log names the setting.

Key queries:

```promql
up{job=~".*dash.*"}
changes(dash_process_uptime_seconds[1h])
```

## Mitigation

* Process down or crash-looping: fix the reported configuration error, or roll back the
  last change (`helm rollback <release>`). An OOM kill shows `OOMKilled` in the pod status; raise
  `resources.<component>.limits.memory`.
* Credential rejected: update the Secret referenced by
  `metrics.serviceMonitor.authorization.<component>` (Helm) or the `authorization` block of the
  scrape config.
* Network: check that `metrics.networkPolicy.from` matches the Prometheus namespace.

## Escalation

If the mitigation does not resolve the alert within 30 minutes, or data loss is suspected, escalate to the storage on-call (WAL, replication) or the platform on-call (deployment, network), and open an incident following [the incident response policy](../../compliance/policies/incident-response-policy.md).
