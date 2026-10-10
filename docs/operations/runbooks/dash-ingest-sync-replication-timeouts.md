# DashIngestSyncReplicationTimeouts

| | |
|---|---|
| Alert | `DashIngestSyncReplicationTimeouts` |
| Severity | warning |
| Rules | [`deploy/observability/prometheus/dash-alerts.rules.yml`](../../../deploy/observability/prometheus/dash-alerts.rules.yml) |
| Dashboards | DASH / Overview, DASH / Ingestion and WAL |
| Background | [failover.md](../failover.md), [ADR 0006](../../adr/0006-leader-failover.md) |

## Meaning

Synchronous writes (`DASH_INGEST_MIN_SYNC_REPLICAS >= 1`) waited longer than `DASH_INGEST_SYNC_REPLICATION_TIMEOUT_MS` for follower confirmations in the last 5 minutes.

## Impact

With `DASH_INGEST_SYNC_REPLICATION_ON_TIMEOUT=fail` those writes were answered `503 sync_replication_timeout` (stored on the leader, outcome unknown to the client, which retries). With `degrade` they were acknowledged without the durability guarantee (`dash_ingest_sync_replication_degraded_total`).

## Diagnosis

```promql
increase(dash_ingest_sync_replication_timeouts_total[5m])
dash_ingest_sync_replication_min_replicas
rate(dash_ingest_sync_replication_wait_seconds_total[5m]) / rate(dash_ingest_sync_replication_waits_total[5m])
dash_ingest_replication_lag_records
```

```sh
curl -s http://<ingestion-follower>:8081/v1/ready | jq .replication
```

* Fewer promotable followers than `DASH_INGEST_MIN_SYNC_REPLICAS` are running or caught up (down, resyncing, blocked).
* Followers are slow to apply (disk fsync latency on the follower, see [dash-wal-fsync-slow.md](dash-wal-fsync-slow.md)).
* All HTTP workers of the leader are busy holding synchronous writes: raise `DASH_INGEST_HTTP_WORKERS`.

## Mitigation

Restore the missing follower or lower `DASH_INGEST_MIN_SYNC_REPLICAS` to the number of followers you actually run. Switching to `degrade` keeps writes available at the cost of the guarantee for the writes that time out.

## Escalation

If the mitigation does not resolve the alert within 30 minutes, or data loss is suspected, escalate to the storage on-call (WAL, replication) or the platform on-call (deployment, network), and open an incident following [the incident response policy](../../compliance/policies/incident-response-policy.md).
