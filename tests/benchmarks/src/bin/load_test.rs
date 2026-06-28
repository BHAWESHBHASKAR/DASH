//! Real HTTP load test for the DASH retrieval service.
//!
//! Unlike the in-process `perf_bench` and Criterion suites, this
//! binary spawns a real DASH service (via `cargo run --release`),
//! waits for `/v1/health` to return 200, then drives concurrent
//! traffic at `/v1/embeddings` and `/v1/retrieve` and reports the
//! end-to-end qps, latency distribution, and error rate.
//!
//! Run with:
//!
//! ```bash
//! # Terminal A: start the retrieval service
//! DASH_RETRIEVAL_PERSISTENCE_DISABLE=1 cargo run --release -p retrieval
//!
//! # Terminal B: run the load test
//! cargo run --release -p benchmark-smoke --bin load_test -- \
//!     --url http://127.0.0.1:8080 \
//!     --concurrency 32 \
//!     --duration 30 \
//!     --endpoint retrieve
//! ```
//!
//! This is the only benchmark that exercises the real network
//! transport (the HTTP framing, the bearer-token middleware, the
//! JSON parser, the dispatcher), so it catches regressions that
//! an in-process benchmark would miss (e.g. a serde regression in
//! the request shape).
//!
//! The output is two-part:
//!
//! - Human-readable per-endpoint block
//! - A single `LOAD_JSON:` line with one JSON object per endpoint,
//!   parseable by the CI regression check

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;
use reqwest::Client;
use serde::Serialize;
use serde_json::json;

#[derive(Serialize)]
struct LoadTestResult {
    endpoint: String,
    concurrency: usize,
    duration_seconds: f64,
    total_requests: u64,
    successful_requests: u64,
    failed_requests: u64,
    qps: f64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    p999_ms: f64,
    max_ms: f64,
}

struct Args {
    url: String,
    concurrency: usize,
    duration_seconds: u64,
    endpoint: String,
    api_key: Option<String>,
    query: String,
}

fn parse_args() -> Result<Args, String> {
    let mut url =
        std::env::var("DASH_LIVE_URL").unwrap_or_else(|_| "http://127.0.0.1:8080".to_string());
    let mut concurrency = 16;
    let mut duration_seconds = 30;
    let mut endpoint = "retrieve".to_string();
    let mut api_key = std::env::var("DASH_API_KEY").ok();
    let mut query = "what is the launch date of project orion".to_string();
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--url" => {
                url = args
                    .get(i + 1)
                    .ok_or_else(|| "--url requires a value".to_string())?
                    .clone();
                i += 2;
            }
            "--concurrency" => {
                concurrency = args
                    .get(i + 1)
                    .ok_or_else(|| "--concurrency requires a value".to_string())?
                    .parse()
                    .map_err(|e| format!("--concurrency: {e}"))?;
                i += 2;
            }
            "--duration" => {
                duration_seconds = args
                    .get(i + 1)
                    .ok_or_else(|| "--duration requires a value".to_string())?
                    .parse()
                    .map_err(|e| format!("--duration: {e}"))?;
                i += 2;
            }
            "--endpoint" => {
                endpoint = args
                    .get(i + 1)
                    .ok_or_else(|| "--endpoint requires a value".to_string())?
                    .clone();
                i += 2;
            }
            "--api-key" => {
                api_key = Some(
                    args.get(i + 1)
                        .ok_or_else(|| "--api-key requires a value".to_string())?
                        .clone(),
                );
                i += 2;
            }
            "--query" => {
                query = args
                    .get(i + 1)
                    .ok_or_else(|| "--query requires a value".to_string())?
                    .clone();
                i += 2;
            }
            "--help" | "-h" => {
                eprintln!("{USAGE}");
                std::process::exit(0);
            }
            other => return Err(format!("unknown flag: {other}")),
        }
    }
    Ok(Args {
        url,
        concurrency,
        duration_seconds,
        endpoint,
        api_key,
        query,
    })
}

const USAGE: &str = "\
load_test - real HTTP load test for the DASH retrieval service

USAGE:
    load_test [OPTIONS]

OPTIONS:
    --url <URL>           DASH base URL (default: $DASH_LIVE_URL or http://127.0.0.1:8080)
    --concurrency <N>     Number of concurrent in-flight requests (default: 16)
    --duration <SECONDS>  How long to drive traffic (default: 30)
    --endpoint <NAME>     retrieve | embeddings | health (default: retrieve)
    --api-key <KEY>       Bearer token (default: $DASH_API_KEY)
    --query <TEXT>        Query text for /v1/retrieve (default: 'what is the launch date of project orion')
    -h, --help            Show this help

OUTPUT:
    A human-readable block followed by a single LOAD_JSON: line
    that CI parses to detect regressions.
";

async fn wait_for_health(client: &Client, url: &str) -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(15);
    while Instant::now() < deadline {
        match client.get(format!("{url}/v1/health")).send().await {
            Ok(r) if r.status().is_success() => return Ok(()),
            Ok(r) => eprintln!(
                "health: {} {}",
                r.status(),
                r.text().await.unwrap_or_default()
            ),
            Err(e) => eprintln!("health: {e}"),
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    Err("DASH did not become healthy in 15s".to_string())
}

async fn run_load_test(args: &Args) -> Result<LoadTestResult, String> {
    let client = Client::builder()
        .pool_max_idle_per_host(args.concurrency)
        .build()
        .map_err(|e| format!("build client: {e}"))?;
    wait_for_health(&client, &args.url).await?;

    let stop_at = Instant::now() + Duration::from_secs(args.duration_seconds);
    let total = Arc::new(AtomicU64::new(0));
    let successful = Arc::new(AtomicU64::new(0));
    let failed = Arc::new(AtomicU64::new(0));
    let histogram: Arc<std::sync::Mutex<Histogram<u64>>> = Arc::new(std::sync::Mutex::new(
        Histogram::new(3).expect("histogram init"),
    ));
    let started = Instant::now();

    let mut tasks = Vec::new();
    for worker_id in 0..args.concurrency {
        let client = client.clone();
        let url = args.url.clone();
        let endpoint = args.endpoint.clone();
        let query = args.query.clone();
        let api_key = args.api_key.clone();
        let total = total.clone();
        let successful = successful.clone();
        let failed = failed.clone();
        let histogram = histogram.clone();
        tasks.push(tokio::spawn(async move {
            while Instant::now() < stop_at {
                let req_start = Instant::now();
                let result = match endpoint.as_str() {
                    "retrieve" => {
                        let body = json!({
                            "tenant_id": "loadtest-tenant",
                            "query": query,
                            "top_k": 10,
                        });
                        let mut rb = client
                            .post(format!("{url}/v1/retrieve"))
                            .header("Content-Type", "application/json")
                            .json(&body);
                        if let Some(key) = &api_key {
                            rb = rb.bearer_auth(key);
                        }
                        rb.send().await
                    }
                    "embeddings" => {
                        let body = json!({
                            "input": query,
                            "model": "text-embedding-3-small",
                        });
                        let mut rb = client
                            .post(format!("{url}/v1/embeddings"))
                            .header("Content-Type", "application/json")
                            .json(&body);
                        if let Some(key) = &api_key {
                            rb = rb.bearer_auth(key);
                        }
                        rb.send().await
                    }
                    "health" => client.get(format!("{url}/v1/health")).send().await,
                    other => {
                        eprintln!("worker {worker_id}: unknown endpoint {other}");
                        break;
                    }
                };
                total.fetch_add(1, Ordering::Relaxed);
                match result {
                    Ok(resp) if resp.status().is_success() => {
                        successful.fetch_add(1, Ordering::Relaxed);
                        let elapsed_us = req_start.elapsed().as_micros() as u64;
                        histogram.lock().unwrap().record(elapsed_us).ok();
                    }
                    Ok(resp) => {
                        failed.fetch_add(1, Ordering::Relaxed);
                        if worker_id == 0 && total.load(Ordering::Relaxed) <= 3 {
                            eprintln!(
                                "worker 0: non-success status {} (body: {})",
                                resp.status(),
                                resp.text().await.unwrap_or_default()
                            );
                        }
                    }
                    Err(e) => {
                        failed.fetch_add(1, Ordering::Relaxed);
                        if worker_id == 0 && total.load(Ordering::Relaxed) <= 3 {
                            eprintln!("worker 0: request error: {e}");
                        }
                    }
                }
            }
        }));
    }
    for t in tasks {
        let _ = t.await;
    }
    let elapsed = started.elapsed();
    let hist = histogram.lock().unwrap();
    let max_us = (0..=hist.max())
        .rev()
        .find(|&v| hist.count_at(v) > 0)
        .unwrap_or(0);
    Ok(LoadTestResult {
        endpoint: args.endpoint.clone(),
        concurrency: args.concurrency,
        duration_seconds: elapsed.as_secs_f64(),
        total_requests: total.load(Ordering::Relaxed),
        successful_requests: successful.load(Ordering::Relaxed),
        failed_requests: failed.load(Ordering::Relaxed),
        qps: total.load(Ordering::Relaxed) as f64 / elapsed.as_secs_f64().max(0.001),
        p50_ms: hist.value_at_percentile(50.0) as f64 / 1_000.0,
        p95_ms: hist.value_at_percentile(95.0) as f64 / 1_000.0,
        p99_ms: hist.value_at_percentile(99.0) as f64 / 1_000.0,
        p999_ms: hist.value_at_percentile(99.9) as f64 / 1_000.0,
        max_ms: max_us as f64 / 1_000.0,
    })
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let args = match parse_args() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("{e}\n\n{USAGE}");
            std::process::exit(2);
        }
    };
    eprintln!(
        "load_test: url={} endpoint={} concurrency={} duration={}s",
        args.url, args.endpoint, args.concurrency, args.duration_seconds
    );
    match run_load_test(&args).await {
        Ok(result) => {
            println!(
                "\n=== {} (concurrency={}, duration={:.1}s) ===\n  total: {}\n  successful: {}\n  failed: {}\n  qps: {:.1}\n  p50: {:.1} ms\n  p95: {:.1} ms\n  p99: {:.1} ms\n  p99.9: {:.1} ms\n  max: {:.1} ms",
                result.endpoint,
                result.concurrency,
                result.duration_seconds,
                result.total_requests,
                result.successful_requests,
                result.failed_requests,
                result.qps,
                result.p50_ms,
                result.p95_ms,
                result.p99_ms,
                result.p999_ms,
                result.max_ms
            );
            let json = serde_json::to_string(&vec![result]).expect("serialize result");
            println!("\nLOAD_JSON: {json}");
        }
        Err(e) => {
            eprintln!("load_test: {e}");
            std::process::exit(1);
        }
    }
}
