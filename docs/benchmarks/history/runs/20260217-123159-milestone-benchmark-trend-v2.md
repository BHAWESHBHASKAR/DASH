# DASH Benchmark Trend Run

- run_id: 20260217-123159-milestone-benchmark-trend-v2
- run_utc: 2026-02-17T12:33:26Z
- history_path: docs/benchmarks/history/benchmark-history.md
- max_regression_pct: 50

| profile | baseline_avg_ms | dash_avg_ms | quality_probes | scorecard |
|---|---:|---:|---|---|
| smoke | 12.3652 | 14.5355 | 3/3 | docs/benchmarks/scorecards/20260217-123159-milestone-benchmark-trend-v2-smoke.md |
| large | 321.4981 | 373.3454 | 3/3 | docs/benchmarks/scorecards/20260217-123159-milestone-benchmark-trend-v2-large.md |


## smoke

status: PASS
scorecard: docs/benchmarks/scorecards/20260217-123159-milestone-benchmark-trend-v2-smoke.md

```text
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 0.05s
     Running `target/debug/benchmark-smoke --profile smoke --guard-history docs/benchmarks/history/benchmark-history.md --max-dash-latency-regression-pct 50 --history-out docs/benchmarks/history/benchmark-history.md --scorecard-out docs/benchmarks/scorecards/20260217-123159-milestone-benchmark-trend-v2-smoke.md`
Benchmark profile: smoke
Benchmark fixture size: 2000
Iterations: 100
Baseline top1: claim-target
DASH top1: claim-target
Baseline hit expected: true
DASH hit expected: true
Baseline avg latency (ms): 12.3652
DASH avg latency (ms): 14.5355
Quality probes passed: 3/3
Quality probe contradiction_support_only: true
Quality probe temporal_window: true
Quality probe temporal_unknown_excluded: true
History guard check passed: profile=smoke, prev_dash_avg_ms=13.5857, current_dash_avg_ms=14.5355, max_regression_pct=50
Benchmark history output updated: docs/benchmarks/history/benchmark-history.md
Benchmark scorecard output updated: docs/benchmarks/scorecards/20260217-123159-milestone-benchmark-trend-v2-smoke.md
```

## large

status: PASS
scorecard: docs/benchmarks/scorecards/20260217-123159-milestone-benchmark-trend-v2-large.md

```text
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 0.00s
     Running `target/debug/benchmark-smoke --profile large --guard-history docs/benchmarks/history/benchmark-history.md --max-dash-latency-regression-pct 50 --history-out docs/benchmarks/history/benchmark-history.md --scorecard-out docs/benchmarks/scorecards/20260217-123159-milestone-benchmark-trend-v2-large.md`
Benchmark profile: large
Benchmark fixture size: 50000
Iterations: 120
Baseline top1: claim-target
DASH top1: claim-target
Baseline hit expected: true
DASH hit expected: true
Baseline avg latency (ms): 321.4981
DASH avg latency (ms): 373.3454
Quality probes passed: 3/3
Quality probe contradiction_support_only: true
Quality probe temporal_window: true
Quality probe temporal_unknown_excluded: true
History guard check passed: profile=large, prev_dash_avg_ms=376.0963, current_dash_avg_ms=373.3454, max_regression_pct=50
Benchmark history output updated: docs/benchmarks/history/benchmark-history.md
Benchmark scorecard output updated: docs/benchmarks/scorecards/20260217-123159-milestone-benchmark-trend-v2-large.md
```
