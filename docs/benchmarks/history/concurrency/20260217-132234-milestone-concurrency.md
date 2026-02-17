# DASH Transport Concurrency Benchmark

- run_id: 20260217-132234-milestone-concurrency
- run_utc: 2026-02-17T13:22:34Z
- target: retrieval HTTP transport
- bind_addr: 127.0.0.1:18080
- clients: 16
- requests_per_worker: 30
- warmup_requests: 5
- workers_list: 1,4

| transport_workers | total_requests | throughput_rps | latency_avg_ms | latency_p95_ms | latency_p99_ms | success_rate_pct |
|---:|---:|---:|---:|---:|---:|---:|
