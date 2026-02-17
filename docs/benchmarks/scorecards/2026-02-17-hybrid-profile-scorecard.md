# DASH Benchmark Scorecard

- run_epoch_secs: 1771353985
- profile: hybrid
- fixture_size: 20000
- iterations: 180
- baseline_top1: claim-hybrid-target
- dash_top1: claim-hybrid-target
- baseline_avg_ms: 21.8142
- dash_avg_ms: 100.4537
- baseline_scan_count: 20000
- dash_candidate_count: 3846
- candidate_reduction_pct: 80.77
- index_tenant_count: 1
- index_claim_count: 20000
- index_inverted_terms: 20014
- index_entity_terms: 3
- index_temporal_buckets: 20000

## Quality Probes

- passed: 4/4
- contradiction_support_only_pass: true
- temporal_window_pass: true
- temporal_unknown_excluded_pass: true
- hybrid_filter_with_embedding_pass: true
