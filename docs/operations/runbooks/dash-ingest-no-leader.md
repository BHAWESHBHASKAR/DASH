# DashIngestNoLeader

| | |
|---|---|
| Alert | `DashIngestNoLeader` |
| Severity | critical |
| Rules | [`deploy/observability/prometheus/dash-alerts.rules.yml`](../../../deploy/observability/prometheus/dash-alerts.rules.yml) |
| Dashboards | DASH / Overview, DASH / Ingestion and WAL |
| Background | [failover.md](../failover.md), [ADR 0006](../../adr/0006-leader-failover.md) |

## Meaning

Leader failover is on (`DASH_INGEST_FAILOVER_CONTROL_PLANE_URL`) and no ingestion node of the job holds a valid leader lease (`max(dash_ingest_failover_accepts_writes) == 0`) for one minute.

## Impact

Every write (`POST /v1/ingest*`, deletes) is refused with `503 not_leader`. Reads and replication continue with the data written so far.

## Diagnosis

```sh
curl -s -H "Authorization: Bearer $DASH_CONTROL_PLANE_TOKEN" http://<control-plane>:8090/v1/control-plane/ingest
curl -s http://<ingestion-node>:8081/v1/ready | jq .failover
kubectl -n dash-system logs <ingestion-pod> --since=15m | grep 'ingestion failover'
```

```promql
increase(dash_ingest_failover_heartbeat_failures_total[5m])
dash_control_plane_ingest_leader_known
dash_control_plane_ingest_failover_blocked
up{job=~"dash-control-plane|dash-ingestion"}
```

* Heartbeat failures on every node: the control plane is down, unreachable or rejecting the token (`last_heartbeat_error` in `/v1/ready`). The leader cannot renew its lease without it.
* The control plane is up and `blocked` is set: see [dash-ingest-failover-blocked.md](dash-ingest-failover-blocked.md).
* A leader is named but `promotion_failures_total` grows on it: it refused the promotion (its WAL does not match its cursor); the control plane replaces it after one lease.

## Mitigation

Restore the control plane (and its state volume) first; writes resume within one heartbeat once a leader can renew. Never configure a node as a writer by hand while failover is on: that is how split brain starts.

## Escalation

If the mitigation does not resolve the alert within 30 minutes, or data loss is suspected, escalate to the storage on-call (WAL, replication) or the platform on-call (deployment, network), and open an incident following [the incident response policy](../../compliance/policies/incident-response-policy.md).
