# DashFileDescriptorsExhausted

| | |
|---|---|
| Alert | `DashFileDescriptorsExhausted` |
| Severity | warning |
| Rules | [`deploy/observability/prometheus/dash-alerts.rules.yml`](../../../deploy/observability/prometheus/dash-alerts.rules.yml) |
| Dashboards | DASH / Overview, DASH / Ingestion and WAL, DASH / Retrieval, embeddings and replication |

## Meaning

A DASH process uses more than 90% of its open-file limit (`process_open_fds / process_max_fds`) for 10 minutes.

## Impact

At the limit, `accept()` fails (the server backs off and keeps running, but new connections are not served) and WAL, segment and audit file opens fail.

## Diagnosis

```sh
kubectl -n dash-system exec <pod> -- sh -c 'ls -l /proc/1/fd | wc -l; cat /proc/1/limits | grep "open files"'
kubectl -n dash-system exec <pod> -- sh -c 'ls -l /proc/1/fd | awk "{print \$NF}" | sort | uniq -c | sort -rn | head'
```

Many sockets: idle client connections or a connection flood (`DASH_HTTP_MAX_CONNS_PER_IP` caps them per
peer). Many files: segment files or retained replication exports.

Key queries:

```promql
process_open_fds / process_max_fds
```

## Mitigation

Raise the limit (`LimitNOFILE=` in the systemd unit, or the container runtime's ulimit), cap connections per peer, and fix the leaking client.

## Escalation

If the mitigation does not resolve the alert within 30 minutes, or data loss is suspected, escalate to the storage on-call (WAL, replication) or the platform on-call (deployment, network), and open an incident following [the incident response policy](../../compliance/policies/incident-response-policy.md).
