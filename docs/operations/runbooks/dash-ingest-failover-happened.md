# DashIngestFailoverHappened

| | |
|---|---|
| Alert | `DashIngestFailoverHappened` |
| Severity | info |
| Rules | [`deploy/observability/prometheus/dash-alerts.rules.yml`](../../../deploy/observability/prometheus/dash-alerts.rules.yml) |
| Dashboards | DASH / Overview, DASH / Ingestion and WAL |
| Background | [failover.md](../failover.md), [ADR 0006](../../adr/0006-leader-failover.md) |

## Meaning

The control plane promoted an ingestion leader in the last 10 minutes (`increase(dash_control_plane_ingest_promotions_total[10m]) > 0`). The first election of a new cluster, a planned step-down and a real failover all count.

## Impact

Writes were unavailable for roughly `lease + grace + one heartbeat` around the change. With asynchronous replication (`DASH_INGEST_MIN_SYNC_REPLICAS=0`) writes the old leader acknowledged but had not replicated are not in the new leader's history; a copy is kept in `<wal>.deposed-*` on the old node when it rejoins.

## Diagnosis

```sh
kubectl -n dash-system logs <control-plane-pod> --since=15m | grep 'ingestion leader'
curl -s -H "Authorization: Bearer $DASH_CONTROL_PLANE_TOKEN" http://<control-plane>:8090/v1/control-plane/ingest
```

```promql
dash_control_plane_ingest_last_failover_seconds
dash_control_plane_ingest_term
dash_ingest_failover_demotions_total
```

## Mitigation

Find out why the old leader was lost (node failure, OOM kill, network) and bring it back; it rejoins as a follower and resyncs. In asynchronous mode, check for `<wal>.deposed-*` files on it and decide whether to re-ingest anything from them (`wal-inspect`), then delete them.

## Escalation

If the mitigation does not resolve the alert within 30 minutes, or data loss is suspected, escalate to the storage on-call (WAL, replication) or the platform on-call (deployment, network), and open an incident following [the incident response policy](../../compliance/policies/incident-response-policy.md).
