# DASH Benchmark Trend Run

- run_id: 20260217-122818-milestone-benchmark-trend
- run_utc: 2026-02-17T12:28:18Z
- history_path: docs/benchmarks/history/benchmark-history.md
- max_regression_pct: 50

| profile | baseline_avg_ms | dash_avg_ms | quality_probes | scorecard |
|---|---:|---:|---|---|
| smoke | 12.5247 | 13.5857 | 3/3 | docs/benchmarks/scorecards/20260217-122818-milestone-benchmark-trend-smoke.md |

## smoke

status: PASS
scorecard: docs/benchmarks/scorecards/20260217-122818-milestone-benchmark-trend-smoke.md

```text
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 0.05s
     Running `target/debug/benchmark-smoke --profile smoke --guard-history docs/benchmarks/history/benchmark-history.md --max-dash-latency-regression-pct 50 --history-out docs/benchmarks/history/benchmark-history.md --scorecard-out docs/benchmarks/scorecards/20260217-122818-milestone-benchmark-trend-smoke.md`
Benchmark profile: smoke
Benchmark fixture size: 2000
Iterations: 100
Baseline top1: claim-target
DASH top1: claim-target
Baseline hit expected: true
DASH hit expected: true
Baseline avg latency (ms): 12.5247
DASH avg latency (ms): 13.5857
Quality probes passed: 3/3
Quality probe contradiction_support_only: true
Quality probe temporal_window: true
Quality probe temporal_unknown_excluded: true
History guard check passed: profile=smoke, prev_dash_avg_ms=14.5680, current_dash_avg_ms=13.5857, max_regression_pct=50
Benchmark history output updated: docs/benchmarks/history/benchmark-history.md
Benchmark scorecard output updated: docs/benchmarks/scorecards/20260217-122818-milestone-benchmark-trend-smoke.md
```
| large | 321.1573 | 376.0963 | 3/3 | docs/benchmarks/scorecards/20260217-122818-milestone-benchmark-trend-large.md |

## large

status: PASS
scorecard: docs/benchmarks/scorecards/20260217-122818-milestone-benchmark-trend-large.md

```text
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 0.00s
     Running `target/debug/benchmark-smoke --profile large --guard-history docs/benchmarks/history/benchmark-history.md --max-dash-latency-regression-pct 50 --history-out docs/benchmarks/history/benchmark-history.md --scorecard-out docs/benchmarks/scorecards/20260217-122818-milestone-benchmark-trend-large.md`
Benchmark profile: large
Benchmark fixture size: 50000
Iterations: 120
Baseline top1: claim-target
DASH top1: claim-target
Baseline hit expected: true
DASH hit expected: true
Baseline avg latency (ms): 321.1573
DASH avg latency (ms): 376.0963
Quality probes passed: 3/3
Quality probe contradiction_support_only: true
Quality probe temporal_window: true
Quality probe temporal_unknown_excluded: true
History guard check passed: profile=large, prev_dash_avg_ms=380.9968, current_dash_avg_ms=376.0963, max_regression_pct=50
Benchmark history output updated: docs/benchmarks/history/benchmark-history.md
Benchmark scorecard output updated: docs/benchmarks/scorecards/20260217-122818-milestone-benchmark-trend-large.md
```
