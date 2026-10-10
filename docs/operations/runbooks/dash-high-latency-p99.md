# DashHighLatencyP99

| | |
|---|---|
| Alert | `DashHighLatencyP99` |
| Severity | warning |
| Rules | [`deploy/observability/prometheus/dash-alerts.rules.yml`](../../../deploy/observability/prometheus/dash-alerts.rules.yml) |
| Dashboards | DASH / Overview, DASH / Ingestion and WAL, DASH / Retrieval, embeddings and replication |

## Meaning

The 99th percentile latency of a client route (retrieve, embeddings, ingest, batch, raw, deletes) has been above 1 second for 10 minutes, measured by `dash_http_server_request_duration_seconds` from the parsed request to the rendered response.

## Impact

Slow answers for 1% or more of requests; clients with short timeouts see failures. Latency counts against the latency SLOs in [slos.md](../slos.md).

## Diagnosis

1. Which route, and is it all instances or one:

   ```promql
   histogram_quantile(0.99, sum by (instance, route, le) (rate(dash_http_server_request_duration_seconds_bucket[5m])))
   ```

2. Ingest routes: compare with WAL fsync latency (`dash_wal_fsync_duration_seconds`, see
   [DashWalFsyncSlow](dash-wal-fsync-slow.md)) and group-commit queue depth
   (`dash_ingest_wal_group_commit_queue_depth`).
3. Retrieve: embedding provider latency
   (`histogram_quantile(0.99, sum by (le) (rate(dash_embedding_request_duration_seconds_bucket[5m])))`)
   when queries carry text that must be embedded; result sizes (`top_k`) and the storage execution
   mode (`dash_retrieve_storage_execution_mode_disk_native_total`).
4. CPU saturation: `rate(process_cpu_seconds_total[5m])` against the pod CPU limit (throttling
   shows as `container_cpu_cfs_throttled_periods_total`).
5. A checkpoint or vector index save running (`dash_wal_checkpoint_duration_seconds`,
   `dash_vector_index_save_duration_seconds`) can stall writes briefly.

Key queries:

```promql
histogram_quantile(0.99, sum by (component, route, le) (rate(dash_http_server_request_duration_seconds_bucket[5m])))
```

## Mitigation

* CPU bound: raise the CPU limit or add retrieval replicas.
* Disk bound: move the volume to faster storage (fsync latency dominates ingest latency).
* Embedding provider slow: send precomputed embeddings (`query_embedding`), or move to a faster
  provider or model; a lower `DASH_EMBEDDING_BREAKER_THRESHOLD` makes the breaker trip sooner on
  timeouts.

## Escalation

If the mitigation does not resolve the alert within 30 minutes, or data loss is suspected, escalate to the storage on-call (WAL, replication) or the platform on-call (deployment, network), and open an incident following [the incident response policy](../../compliance/policies/incident-response-policy.md).
