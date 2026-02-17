# DASH Transport Concurrency Benchmark

- run_id: 20260217-132412-milestone-concurrency-v2
- run_utc: 2026-02-17T13:24:14Z
- target: retrieval HTTP transport
- bind_addr: 127.0.0.1:18080
- clients: 16
- requests_per_worker: 30
- warmup_requests: 5
- workers_list: 1,4

| transport_workers | total_requests | throughput_rps | latency_avg_ms | latency_p95_ms | latency_p99_ms | success_rate_pct |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 480 | 16589.00 | 0.9468 | 1.2832 | 1.3075 | 100.00 |
| 4 | 480 | 19428.94 | 0.8088 | 0.9440 | 3.4971 | 100.00 |


## workers=1

status: PASS

```text
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 0.00s
     Running `target/debug/concurrent_load --addr '127.0.0.1:18080' --path '/v1/retrieve?tenant_id=sample-tenant&query=retrieval+initialized&top_k=5&stance_mode=balanced' --concurrency 16 --requests-per-worker 30 --warmup-requests 5 --connect-timeout-ms 2000 --read-timeout-ms 5000`
Concurrent load benchmark
addr: 127.0.0.1:18080
path: /v1/retrieve?tenant_id=sample-tenant&query=retrieval+initialized&top_k=5&stance_mode=balanced
concurrency: 16
requests_per_worker: 30
total_requests: 480
successful_requests: 480
failed_requests: 0
success_rate_pct: 100.00
elapsed_seconds: 0.0289
throughput_rps: 16589.00
latency_avg_ms: 0.9468
latency_p50_ms: 0.9392
latency_p95_ms: 1.2832
latency_p99_ms: 1.3075
```

## workers=4

status: PASS

```text
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 0.00s
     Running `target/debug/concurrent_load --addr '127.0.0.1:18080' --path '/v1/retrieve?tenant_id=sample-tenant&query=retrieval+initialized&top_k=5&stance_mode=balanced' --concurrency 16 --requests-per-worker 30 --warmup-requests 5 --connect-timeout-ms 2000 --read-timeout-ms 5000`
Concurrent load benchmark
addr: 127.0.0.1:18080
path: /v1/retrieve?tenant_id=sample-tenant&query=retrieval+initialized&top_k=5&stance_mode=balanced
concurrency: 16
requests_per_worker: 30
total_requests: 480
successful_requests: 480
failed_requests: 0
success_rate_pct: 100.00
elapsed_seconds: 0.0247
throughput_rps: 19428.94
latency_avg_ms: 0.8088
latency_p50_ms: 0.6984
latency_p95_ms: 0.9440
latency_p99_ms: 3.4971
```
