# DashReplicationFollowerNotReady

| | |
|---|---|
| Alert | `DashReplicationFollowerNotReady` |
| Severity | critical |
| Rules | [`deploy/observability/prometheus/dash-alerts.rules.yml`](../../../deploy/observability/prometheus/dash-alerts.rules.yml) |
| Dashboards | DASH / Overview, DASH / Ingestion and WAL, DASH / Retrieval, embeddings and replication |

## Meaning

A retrieval follower has reported `dash_retrieval_replication_ready == 0` for 10 minutes.

## Impact

The follower is out of the load balancer: less read capacity, and if every follower is affected, retrieval is down.

## Diagnosis

```sh
kubectl -n dash-system exec <retrieval-pod> -- wget -qO- http://127.0.0.1:8080/ready
```

`reason` / `replication` in the answer:

* `replication_initial_sync_pending`: the first sync has not completed (large export, unreachable
  leader).
* `replication_lag_exceeded` / `replication_stale`: see
  [DashReplicationFollowerLagging](dash-replication-follower-lagging.md).
* `replication_response_too_large`, `replication_group_too_large`, `replication_leader_too_old`:
  blocked; retrying cannot fix it (`dash_retrieval_replication_blocked_*` gauges).

Key queries:

```promql
dash_retrieval_replication_ready
dash_retrieval_replication_consecutive_failures
```

## Mitigation

Fix the cause above. For a blocked follower raise
`DASH_RETRIEVAL_REPLICATION_MAX_RESPONSE_BYTES`, or upgrade the leader. A follower whose local state
is suspect can be reset by deleting its PVC (StatefulSet) so it resyncs from scratch; do this one
replica at a time.

## Escalation

If the mitigation does not resolve the alert within 30 minutes, or data loss is suspected, escalate to the storage on-call (WAL, replication) or the platform on-call (deployment, network), and open an incident following [the incident response policy](../../compliance/policies/incident-response-policy.md).
