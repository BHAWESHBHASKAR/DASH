# Benchmark History

| run_epoch_secs | profile | fixture_size | iterations | baseline_top1 | eme_top1 | baseline_hit | eme_hit | baseline_avg_ms | eme_avg_ms | baseline_scan_count | dash_candidate_count | metadata_prefilter_count | ann_candidate_count | final_scored_candidate_count | ann_recall_at_10 | ann_recall_at_100 | ann_recall_curve | segment_cache_hits | segment_refresh_attempts | segment_refresh_successes | segment_refresh_failures | segment_refresh_avg_ms | wal_claims_seeded | wal_checkpoint_ms | wal_replay_ms | wal_snapshot_records | wal_truncated_wal_records | wal_replay_snapshot_records | wal_replay_wal_records | wal_replay_validation_hit | wal_replay_validation_top_claim |
|---|---|---:|---:|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|
| 1771324191 | smoke | 2000 | 100 | claim-target | claim-target | true | true | 12.5631 | 14.3213 |
| 1771324277 | large | 50000 | 120 | claim-target | claim-target | true | true | 329.4547 | 380.9968 |
| 1771325896 | smoke | 2000 | 100 | claim-target | claim-target | true | true | 13.1804 | 14.6673 |
| 1771326894 | smoke | 2000 | 100 | claim-target | claim-target | true | true | 13.5680 | 14.5680 |
| 1771331301 | smoke | 2000 | 100 | claim-target | claim-target | true | true | 12.5247 | 13.5857 |
| 1771331386 | large | 50000 | 120 | claim-target | claim-target | true | true | 321.1573 | 376.0963 |
| 1771331522 | smoke | 2000 | 100 | claim-target | claim-target | true | true | 12.3652 | 14.5355 |
| 1771331606 | large | 50000 | 120 | claim-target | claim-target | true | true | 321.4981 | 373.3454 |
| 1771331670 | smoke | 2000 | 100 | claim-target | claim-target | true | true | 12.4935 | 14.7038 |
| 1771331973 | smoke | 2000 | 100 | claim-target | claim-target | true | true | 12.6217 | 13.6454 |
| 1771334782 | smoke | 2000 | 100 | claim-target | claim-target | true | true | 12.3604 | 13.5771 |
| 1771353985 | hybrid | 20000 | 180 | claim-hybrid-target | claim-hybrid-target | true | true | 21.8142 | 100.4537 | 20000 | 3846 |
| 1771355569 | hybrid | 20000 | 180 | claim-hybrid-target | claim-hybrid-target | true | true | 21.4730 | 23.1846 | 20000 | 3846 |
| 1771363764 | xlarge | 100000 | 1 | claim-target | claim-target | true | true | 684.6643 | 94.5811 | 100000 | 2096 | 0 | 100 | 2096 | 1 | 1 | 1 | 0 | 74.0700 | 1000 | 23.4797 | 738.5493 | 3000 | 3000 | 3000 | 3 | true | claim-wal-delta |
| 1771403814 | smoke | 2000 | 100 | claim-target | claim-target | true | true | 12.8650 | 9.5385 | 2000 | 140 | 0 | 100 | 140 | 1.0000 | 1.0000 | 10:1.0000;25:1.0000;50:1.0000;100:1.0000;200:1.0000 | 1 | 1 | 1 | 0 | 1.4660 | 0 | n/a | n/a | 0 | 0 | 0 | 0 | false | none |
| 1771448524 | large | 50000 | 1 | claim-target | claim-target | true | true | 48.0826 | 8.8813 | 50000 | 1099 | 0 | 100 | 1099 | 1.0000 | 1.0000 | 10:1.0000;25:1.0000;50:1.0000;100:1.0000;200:1.0000 | 1 | 1 | 1 | 0 | 5.2520 | 10000 | 120.6174 | 5280.3495 | 30000 | 30000 | 30000 | 3 | true | claim-wal-delta |

## Ingestion WAL Durability History

| run_utc | run_id | workers | clients | requests_per_worker | strict_throughput_rps | grouped_throughput_rps | buffered_throughput_rps | grouped_vs_strict_pct | buffered_vs_strict_pct | summary_artifact |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 2026-02-18T10:03:12Z | 20260218-100306-ingest-wal-durability | 4 | 16 | 25 | 128.12 | 2461.20 | 4381.95 | 1821.01 | 3320.19 | `docs/benchmarks/history/concurrency/wal-durability/20260218-100306-ingest-wal-durability.md` |

## Ingestion Queue-Mode Concurrency History

| run_utc | run_id | workers | clients | requests_per_worker | strict_throughput_rps | grouped_throughput_rps | background_only_throughput_rps | grouped_vs_strict_pct | background_only_vs_strict_pct | strict_artifact | grouped_artifact | background_only_artifact |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---|
| 2026-02-18T20:23:39Z | 20260218-202317-ingest-queue-modes | 4 | 16 | 25 | 132.33 | 3059.81 | 12616.14 | 2212.26 | 9433.85 | `docs/benchmarks/history/concurrency/20260218-202317-ingest-strict.md` | `docs/benchmarks/history/concurrency/20260218-202330-ingest-grouped.md` | `docs/benchmarks/history/concurrency/20260218-202339-ingest-background-only.md` |
