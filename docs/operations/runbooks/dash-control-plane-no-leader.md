# DashControlPlaneNoLeader

| | |
|---|---|
| Alert | `DashControlPlaneNoLeader` |
| Severity | warning |
| Rules | [`deploy/observability/prometheus/dash-alerts.rules.yml`](../../../deploy/observability/prometheus/dash-alerts.rules.yml) |
| Dashboards | DASH / Overview, DASH / Ingestion and WAL, DASH / Retrieval, embeddings and replication |

## Meaning

No control-plane node of a job holds the leader lease (`max(dash_control_plane_is_leader) == 0`) for 2 minutes.

## Impact

Placement reads (`GET /v1/control-plane/placement`), placement changes, replica-lag reports and failover promotion are refused with `503`. Ingestion and retrieval keep serving with the placement they already loaded; placement reloads fail.

## Diagnosis

```sh
kubectl -n dash-system logs <control-plane-pod> --since=30m | grep -iE 'lease|leader'
curl -s -H "Authorization: Bearer $DASH_CONTROL_PLANE_TOKEN" http://<control-plane>:8090/v1/control-plane/leader
```

```promql
dash_control_plane_leader_lease_remaining_seconds
dash_control_plane_state_error
```

The lease is a file (`DASH_CONTROL_PLANE_LEASE_PATH`); an unwritable or full state volume stops
renewal (`dash_control_plane_state_error == 1` when it cannot be read).

Key queries:

```promql
max by (job) (dash_control_plane_is_leader)
dash_control_plane_leader_lease_remaining_seconds
```

## Mitigation

Fix the state volume, then restart the control plane; the first node to start acquires the lease. `POST /v1/control-plane/leader/acquire` forces an acquisition attempt.

## Escalation

If the mitigation does not resolve the alert within 30 minutes, or data loss is suspected, escalate to the storage on-call (WAL, replication) or the platform on-call (deployment, network), and open an incident following [the incident response policy](../../compliance/policies/incident-response-policy.md).
