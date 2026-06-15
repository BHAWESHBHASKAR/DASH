# Scaling

DASH is designed to scale **horizontally** — by adding more processes, not by adding concurrency inside a process. This page describes the three scaling axes: process replication, `redb` PR 3 replication (for read-after-write across data centers), and ANN index sharding (for tenants that exceed a single host).

## Horizontal scaling

The retrieval service is **stateless** above the persistence layer. Two replicas of the retrieval service can serve the same `redb` file (in read-only mode) and produce identical results. The recommended pattern:

```text
       ┌──────────────────────────┐
       │  Load balancer (L7)      │
       └────────────┬─────────────┘
                    │
        ┌───────────┼───────────┐
        │           │           │
   ┌────▼────┐ ┌────▼────┐ ┌────▼────┐
   │ ret-1   │ │ ret-2   │ │ ret-3   │
   └────┬────┘ └────┬────┘ └────┬────┘
        │           │           │
        └───────────┼───────────┘
                    │
              redb file (read-only mount)
```

The ingestion service is **stateful** — it is the writer of the `redb` file. A single ingestion replica is the default; for higher ingest throughput, see the [ingest-side sharding](#ingest-side-sharding) section below.

### Recommended replica counts

| Workload                              | Ingestion replicas | Retrieval replicas |
| ------------------------------------- | ------------------: | -----------------: |
| Dev / staging                         |                 1   |                 1   |
| Single-tenant small (< 1 M claims)    |                 1   |                 2   |
| Single-tenant medium (< 100 M claims) |                 2   |                 4   |
| Multi-tenant large (≥ 100 M claims)   |                 4   |                 8+  |
| Multi-region                          |                 4/region | 8/region         |

The retrieval replicas are CPU-bound. A 4-vCPU host can serve ~1 000 QPS at p99 < 5 ms; a 16-vCPU host can serve ~4 000 QPS. The bottleneck is the lexical reranker for large `top_k` and the ANN search for large `ann_top_n`.

## redb PR 3 replication

The current `redb` integration is **PR 1** — single-process write, single-host read. **PR 3** (on the roadmap) adds a log-based replication protocol that allows a follower `redb` to tail a leader `redb` over the network.

The shape:

```text
  ┌─────────────┐
  │  leader     │  (ingestion replica; writes to redb)
  │  ingest-1   │
  └──────┬──────┘
         │  redb log (length-prefixed, CRC-32c)
         │
   ┌─────┴──────┬──────────────┐
   │            │              │
┌──▼──┐      ┌──▼──┐        ┌──▼──┐
│ret-1│      │ret-2│        │ret-3│   (read-only followers)
└─────┘      └─────┘        └─────┘
```

The follower's redb is read-only; it is updated by applying the leader's log records. The protocol is **at-least-once** with idempotent applies; a follower can lose its position and re-tail from the leader's last checkpoint without diverging.

PR 3 is not in the current release. The design doc is at `docs/plans/2026-06-13-redb-persistence-design.md` (PR 3 section) in the source tree. Subscribe to the [issue tracker](https://github.com/BHAWESHBHASKAR/DASH/issues?q=is%3Aopen+label%3Aredb-pr3) for the rollout date.

## ANN index sharding

For tenants with more vectors than fit on a single host, the ANN index is **sharded** by claim-ID range. Each shard is a separate `usearch` HNSW graph on a separate host; the retrieval service fans out the query to all shards and merges the top-*N* results.

```text
                 ┌──────────────────────┐
                 │  ret-router          │
                 │  (per-tenant routing)│
                 └──────────┬───────────┘
                            │
        ┌───────────────────┼───────────────────┐
        │                   │                   │
   ┌────▼─────┐        ┌────▼─────┐        ┌────▼─────┐
   │ shard-A  │        │ shard-B  │        │ shard-C  │
   │ c0..cN/3 │        │ cN/3..2N │        │ 2N..N    │
   └──────────┘        └──────────┘        └──────────┘
```

The routing key is the `claim_id` modulo the shard count. The router is a thin process that holds the routing table and forwards requests; it can be co-located with any retrieval replica.

The sharding is **per-tenant**. A tenant with 1 M vectors does not need sharding (it fits on one host). A tenant with 100 M vectors is sharded 4×; a tenant with 1 B vectors is sharded 16×. The shard count is fixed at tenant-provisioning time; reshard is a known gap and is on the [roadmap](https://github.com/BHAWESHBHASKAR/DASH/issues?q=is%3Aopen+label%3Ashard-reshard).

## Ingest-side sharding

The ingestion service is single-writer per `redb` file. For higher ingest throughput, the supported pattern is **shard by tenant**:

```text
  ┌──────────────────────┐
  │  ingest-router       │
  └──────────┬───────────┘
             │
   ┌─────────┼─────────┐
   │         │         │
┌──▼──┐   ┌──▼──┐   ┌──▼──┐
│ i-1 │   │ i-2 │   │ i-3 │   (each owns a slice of tenants)
│ t1..│   │ t5..│   │ t9..│
└──┬──┘   └──┬──┘   └──┬──┘
   │         │         │
   ▼         ▼         ▼
 ingest    ingest    ingest
 .redb-1  .redb-2   .redb-3
```

The router is a stateless process that hashes `tenant_id` to a shard. Each ingestion replica owns a non-overlapping set of tenants and writes to its own `redb` file. The retrieval side reads from all `redb` files (mount them all, or fan out via a thin aggregator).

This is a manual setup in the current release; a control-plane-managed sharding is on the [roadmap](https://github.com/BHAWESHBHASKAR/DASH/issues?q=is%3Aopen+label%3Acontrol-plane).
