# DASH Rebalance Drill Summary

- run_id: phase4-rebalance-smoke
- run_utc: 2026-02-20T15:25:09Z
- tenant_id: sample-tenant
- local_node_id: node-a
- ingest_bind: 127.0.0.1:19181
- retrieve_bind: 127.0.0.1:19180
- placement_reload_interval_ms: 200
- max_wait_seconds: 60

## Status

| check | result |
|---|---|
| shard_count increase (>=2) | PASS |
| shard-0 epoch transition | PASS |
| moved key threshold (1) | PASS |

## Metrics

| metric | value |
|---|---:|
| probe_keys_count | 32 |
| moved_keys_count | 32 |
| moved_to_new_shard_count | 32 |
| shard_count_before | 1 |
| shard_count_after | 2 |
| shard0_epoch_before | 1 |
| shard0_epoch_after | 2 |
| reload_success_before | 0 |
| reload_success_after | 5 |

## Moved Keys (first 20)

| key | before_shard | after_shard | moved |
|---|---:|---:|---|
| probe-key-000 | 0 | 1 | true |
| probe-key-001 | 0 | 1 | true |
| probe-key-002 | 0 | 1 | true |
| probe-key-003 | 0 | 1 | true |
| probe-key-004 | 0 | 1 | true |
| probe-key-005 | 0 | 1 | true |
| probe-key-006 | 0 | 1 | true |
| probe-key-007 | 0 | 1 | true |
| probe-key-008 | 0 | 1 | true |
| probe-key-009 | 0 | 1 | true |
| probe-key-010 | 0 | 1 | true |
| probe-key-011 | 0 | 1 | true |
| probe-key-012 | 0 | 1 | true |
| probe-key-013 | 0 | 1 | true |
| probe-key-014 | 0 | 1 | true |
| probe-key-015 | 0 | 1 | true |
| probe-key-016 | 0 | 1 | true |
| probe-key-017 | 0 | 1 | true |
| probe-key-018 | 0 | 1 | true |
| probe-key-019 | 0 | 1 | true |

## Artifacts

- placement_before: docs/benchmarks/history/rebalance/phase4-rebalance-smoke/placement-before.csv
- placement_after: docs/benchmarks/history/rebalance/phase4-rebalance-smoke/placement-after.csv
- retrieval_debug_before: docs/benchmarks/history/rebalance/phase4-rebalance-smoke/retrieval-debug-before.json
- retrieval_debug_after: docs/benchmarks/history/rebalance/phase4-rebalance-smoke/retrieval-debug-after.json
- ingestion_debug_before: docs/benchmarks/history/rebalance/phase4-rebalance-smoke/ingestion-debug-before.json
- ingestion_debug_after: docs/benchmarks/history/rebalance/phase4-rebalance-smoke/ingestion-debug-after.json
- probe_before: docs/benchmarks/history/rebalance/phase4-rebalance-smoke/probe-before.csv
- probe_after: docs/benchmarks/history/rebalance/phase4-rebalance-smoke/probe-after.csv
- probe_diff: docs/benchmarks/history/rebalance/phase4-rebalance-smoke/probe-diff.csv
- ingestion_log: docs/benchmarks/history/rebalance/phase4-rebalance-smoke/ingestion.log
- retrieval_log: docs/benchmarks/history/rebalance/phase4-rebalance-smoke/retrieval.log
