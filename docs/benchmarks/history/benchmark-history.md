# Benchmark History

| run_epoch_secs | profile | fixture_size | iterations | baseline_top1 | eme_top1 | baseline_hit | eme_hit | baseline_avg_ms | eme_avg_ms | baseline_scan_count | dash_candidate_count | metadata_prefilter_count | ann_candidate_count | final_scored_candidate_count | segment_cache_hits | segment_refresh_attempts | segment_refresh_successes | segment_refresh_failures | segment_refresh_avg_ms | wal_claims_seeded | wal_checkpoint_ms | wal_replay_ms | wal_snapshot_records | wal_truncated_wal_records | wal_replay_snapshot_records | wal_replay_wal_records |
|---|---|---:|---:|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
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
| 1771363764 | xlarge | 100000 | 1 | claim-target | claim-target | true | true | 684.6643 | 94.5811 | 100000 | 2096 | 0 | 100 | 2096 | 1 | 1 | 1 | 0 | 74.0700 | 1000 | 23.4797 | 738.5493 | 3000 | 3000 | 3000 | 3 |
