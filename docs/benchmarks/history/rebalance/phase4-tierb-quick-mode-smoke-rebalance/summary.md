# DASH Rebalance Drill Summary

- run_id: phase4-tierb-quick-mode-smoke-rebalance
- run_utc: 2026-02-20T16:09:31Z
- tenant_id: sample-tenant
- local_node_id: node-a
- ingest_bind: 127.0.0.1:19181
- retrieve_bind: 127.0.0.1:19180
- placement_reload_interval_ms: 200
- max_wait_seconds: 60
- target_shards: 8

## Status

| check | result |
|---|---|
| shard_count increase (>=8) | PASS |
| shard-0 epoch transition | PASS |
| moved key threshold (1) | PASS |

## Metrics

| metric | value |
|---|---:|
| target_shards | 8 |
| probe_keys_count | 64 |
| moved_keys_count | 64 |
| moved_to_new_shard_count | 64 |
| shard_count_before | 1 |
| shard_count_after | 8 |
| shard0_epoch_before | 1 |
| shard0_epoch_after | 2 |
| reload_success_before | 1 |
| reload_success_after | 9 |

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

- placement_before: docs/benchmarks/history/rebalance/phase4-tierb-quick-mode-smoke-rebalance/placement-before.csv
- placement_after: docs/benchmarks/history/rebalance/phase4-tierb-quick-mode-smoke-rebalance/placement-after.csv
- retrieval_debug_before: docs/benchmarks/history/rebalance/phase4-tierb-quick-mode-smoke-rebalance/retrieval-debug-before.json
- retrieval_debug_after: docs/benchmarks/history/rebalance/phase4-tierb-quick-mode-smoke-rebalance/retrieval-debug-after.json
- ingestion_debug_before: docs/benchmarks/history/rebalance/phase4-tierb-quick-mode-smoke-rebalance/ingestion-debug-before.json
- ingestion_debug_after: docs/benchmarks/history/rebalance/phase4-tierb-quick-mode-smoke-rebalance/ingestion-debug-after.json
- probe_before: docs/benchmarks/history/rebalance/phase4-tierb-quick-mode-smoke-rebalance/probe-before.csv
- probe_after: docs/benchmarks/history/rebalance/phase4-tierb-quick-mode-smoke-rebalance/probe-after.csv
- probe_diff: docs/benchmarks/history/rebalance/phase4-tierb-quick-mode-smoke-rebalance/probe-diff.csv
- ingestion_log: docs/benchmarks/history/rebalance/phase4-tierb-quick-mode-smoke-rebalance/ingestion.log
- retrieval_log: docs/benchmarks/history/rebalance/phase4-tierb-quick-mode-smoke-rebalance/retrieval.log
