# Scaling

DASH 0.3.0 (unreleased) scales **reads** by adding polling followers and has **no built-in write sharding, consensus or automatic failover**. This page describes what exists and what does not. An earlier version of this page contained a replica-sizing table, QPS and latency figures, a description of a log-shipping "redb PR 3" protocol, a retrieval service sharing a read-only `redb` file, and ANN and ingest routers. None of those exist in the code, and no throughput or latency figure has been measured in a reproducible CI job; all were removed. For benchmark methodology see [Benchmarks](../reference/benchmarks.md).

## What exists: one writer, polling read followers

```text
client ── writes ──► ingestion (single writer, owns the WAL and a redb mirror)
                          │  GET /internal/replication/wal  (token, generation-aware)
                          ▼
         retrieval-1   retrieval-2   retrieval-N   (each has its own in-memory store,
                                                    optional local WAL and redb file)
client ── reads ──► load balancer ──► any retrieval replica
```

- **Writes** go to one ingestion process per WAL. Two writers on the same WAL or redb file are not supported (redb holds a file lock).
- **Reads** can be spread over any number of retrieval replicas. Each replica follows the ingestion service by polling (`DASH_RETRIEVAL_REPLICATION_SOURCE_URL`, `DASH_RETRIEVAL_REPLICATION_TOKEN`), keeps its own copy of the data in memory, and, if it has a local WAL (`DASH_RETRIEVAL_WAL_PATH`), resumes from its saved `(generation, offset)` after a restart. A replica that is new, or whose state no longer matches the leader's WAL generation, resyncs from a full export. Memory use per replica grows with the data set: every replica holds every tenant's claims, evidence, vectors and ANN graph.
- **Freshness.** A write is visible on a replica after the next poll (`DASH_RETRIEVAL_REPLICATION_POLL_INTERVAL_MS`, default 1000 ms) plus apply time.
- **Load balancing.** Use `/ready`: a follower that has not finished its first sync, is further behind than `DASH_RETRIEVAL_REPLICATION_MAX_LAG_RECORDS` (default 100000) or has not polled successfully within `DASH_RETRIEVAL_REPLICATION_MAX_STALENESS_MS` (default 300000) answers 503 with a reason, so a load balancer can take it out of rotation. Watch `dash_retrieval_replication_lag_records` and `_last_success_age_ms` (see [Observability](observability.md)).
- **Kubernetes and Helm.** Retrieval is a StatefulSet with one PVC per pod; scale it manually (`kubectl scale statefulset ... --replicas=N`). There is no autoscaler, because a new replica starts empty and must catch up first. Never scale ingestion or the control plane above 1. A single ingestion node bounds ingest throughput, and the ingestion runtime serializes writes.

## What exists: placement routing (manual, limited)

`DASH_ROUTER_PLACEMENT_FILE` (a CSV of shard placements) or `DASH_ROUTER_CONTROL_PLANE_URL` turns on placement routing in ingestion and retrieval. Each node then checks that it is the leader (writes) or an eligible replica (reads) for the tenant's shard, and refuses the request with 503 otherwise (`write_consistency` / `read_consistency` of `one`, `quorum` or `all` are checked against replica health). The control plane stores the placement, holds a file-lease leader election and can promote a replica to shard leader, but only if that replica reported zero replication lag (or the operator passes `force=1`). Placements reload on an interval (`DASH_ROUTER_PLACEMENT_RELOAD_INTERVAL_MS`); if the control plane becomes unreachable, ingestion keeps accepting writes on the last known placement only for `DASH_INGEST_PLACEMENT_STALE_GRACE_MS` (default 30 s) and then refuses them.

What this is **not**: DASH ships no router or proxy that forwards a request to the right node. The metadata router is a library used inside the services to validate routes. Splitting tenants across several ingestion nodes means running one ingestion per placement leader and sending each tenant's writes to its node yourself. Failover is operator-driven: promote a caught-up replica through the control plane, then repoint clients. There is no consensus protocol; one lease file decides control-plane leadership.

## Not implemented

- Cross-host ANN sharding or query fan-out; each retrieval process holds every tenant's ANN graph in memory.
- Consensus replication (Raft) and automatic failover (planned P3).
- A write-sharding router and tenant resharding.
- Rate-limit state shared across replicas: limits are per process.
