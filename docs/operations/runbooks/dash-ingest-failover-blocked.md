# DashIngestFailoverBlocked

| | |
|---|---|
| Alert | `DashIngestFailoverBlocked` |
| Severity | critical |
| Rules | [`deploy/observability/prometheus/dash-alerts.rules.yml`](../../../deploy/observability/prometheus/dash-alerts.rules.yml) |
| Dashboards | DASH / Overview, DASH / Ingestion and WAL |
| Background | [failover.md](../failover.md), [ADR 0006](../../adr/0006-leader-failover.md) |

## Meaning

The ingestion leader's lease lapsed and the control plane cannot promote anyone (`dash_control_plane_ingest_failover_blocked == 1`) for one minute.

## Impact

Writes are unavailable until a member can be promoted.

## Diagnosis

```sh
curl -s -H "Authorization: Bearer $DASH_CONTROL_PLANE_TOKEN" http://<control-plane>:8090/v1/control-plane/ingest | jq '{term, leader_node_id, blocked, members}'
curl -s http://<ingestion-node>:8081/v1/ready | jq '{replication, failover}'
```

* `blocked: "waiting_for_reports"`: a member that was live has not heartbeated since the lease lapsed. It counts as dead after one more lease; if the alert fires, a node keeps reporting irregularly (CPU starvation, network flaps).
* `blocked: "no_eligible_candidate"`: no live member is synced, in the current term and in a WAL generation the control plane can order. Typical causes: every follower is resyncing (`replication.force_resync`, `resyncs_total` growing), or all followers were lagging behind a checkpoint the leader never reported and no follower crossed it.

## Mitigation

Get one follower to a synced state (fix its disk or network, let its resync finish); it is promoted on its next heartbeat. If the old leader's disk survived, starting it again also works: a restarted leader that still holds the most data is re-elected. Do not promote by hand.

## Escalation

If the mitigation does not resolve the alert within 30 minutes, or data loss is suspected, escalate to the storage on-call (WAL, replication) or the platform on-call (deployment, network), and open an incident following [the incident response policy](../../compliance/policies/incident-response-policy.md).
