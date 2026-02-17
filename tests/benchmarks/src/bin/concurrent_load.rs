use std::{
    io::{Read, Write},
    net::{TcpStream, ToSocketAddrs},
    sync::mpsc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

#[derive(Debug, Clone)]
struct Config {
    addr: String,
    path: String,
    method: String,
    body_template: Option<String>,
    content_type: String,
    concurrency: usize,
    requests_per_worker: usize,
    warmup_requests: usize,
    connect_timeout_ms: u64,
    read_timeout_ms: u64,
}

#[derive(Debug, Default)]
struct WorkerStats {
    success: usize,
    failed: usize,
    latencies_ms: Vec<f64>,
    sample_errors: Vec<String>,
}

fn main() {
    let config = match parse_args(std::env::args().skip(1)) {
        Ok(config) => config,
        Err(message) => {
            eprintln!("{message}");
            std::process::exit(2);
        }
    };

    if let Err(err) = run(config) {
        eprintln!("concurrent-load failed: {err}");
        std::process::exit(1);
    }
}

fn run(config: Config) -> Result<(), String> {
    if config.warmup_requests > 0 {
        for warmup_idx in 0..config.warmup_requests {
            let _ = run_single_request(&config.addr, &config.path, &config, 0, warmup_idx)?;
        }
    }

    let total_requests = config.concurrency * config.requests_per_worker;
    let started_at = Instant::now();
    let (tx, rx) = mpsc::channel::<WorkerStats>();

    std::thread::scope(|scope| {
        for worker_idx in 0..config.concurrency {
            let tx = tx.clone();
            let config = config.clone();
            scope.spawn(move || {
                let mut stats = WorkerStats::default();
                for request_idx in 0..config.requests_per_worker {
                    let req_started = Instant::now();
                    match run_single_request(
                        &config.addr,
                        &config.path,
                        &config,
                        worker_idx,
                        request_idx,
                    ) {
                        Ok(200) => {
                            stats.success += 1;
                            stats
                                .latencies_ms
                                .push(req_started.elapsed().as_secs_f64() * 1000.0);
                        }
                        Ok(status) => {
                            stats.failed += 1;
                            if stats.sample_errors.len() < 4 {
                                stats
                                    .sample_errors
                                    .push(format!("unexpected HTTP status code: {status}"));
                            }
                        }
                        Err(err) => {
                            stats.failed += 1;
                            if stats.sample_errors.len() < 4 {
                                stats.sample_errors.push(err);
                            }
                        }
                    }
                }
                let _ = tx.send(stats);
            });
        }
    });
    drop(tx);

    let elapsed_seconds = started_at.elapsed().as_secs_f64();
    let mut success = 0usize;
    let mut failed = 0usize;
    let mut latencies_ms = Vec::with_capacity(total_requests);
    let mut errors = Vec::new();

    for stats in rx {
        success += stats.success;
        failed += stats.failed;
        latencies_ms.extend(stats.latencies_ms);
        for err in stats.sample_errors {
            if errors.len() >= 10 {
                break;
            }
            errors.push(err);
        }
    }

    if success == 0 {
        return Err("no successful requests".to_string());
    }

    latencies_ms.sort_by(|a, b| a.total_cmp(b));
    let avg_ms = latencies_ms.iter().sum::<f64>() / latencies_ms.len() as f64;
    let p50_ms = percentile(&latencies_ms, 0.50);
    let p95_ms = percentile(&latencies_ms, 0.95);
    let p99_ms = percentile(&latencies_ms, 0.99);

    println!("Concurrent load benchmark");
    println!("addr: {}", config.addr);
    println!("path: {}", config.path);
    println!("method: {}", config.method);
    println!("concurrency: {}", config.concurrency);
    println!("requests_per_worker: {}", config.requests_per_worker);
    println!("total_requests: {total_requests}");
    println!("successful_requests: {success}");
    println!("failed_requests: {failed}");
    println!(
        "success_rate_pct: {:.2}",
        (success as f64 / total_requests as f64) * 100.0
    );
    println!("elapsed_seconds: {:.4}", elapsed_seconds);
    println!(
        "throughput_rps: {:.2}",
        success as f64 / elapsed_seconds.max(0.0001)
    );
    println!("latency_avg_ms: {:.4}", avg_ms);
    println!("latency_p50_ms: {:.4}", p50_ms);
    println!("latency_p95_ms: {:.4}", p95_ms);
    println!("latency_p99_ms: {:.4}", p99_ms);

    if failed > 0 {
        for err in errors {
            println!("error_sample: {err}");
        }
        return Err(format!("{failed} requests failed"));
    }

    Ok(())
}

fn percentile(sorted: &[f64], quantile: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = (((sorted.len() - 1) as f64) * quantile).round() as usize;
    sorted[idx]
}

fn run_single_request(
    addr: &str,
    path: &str,
    config: &Config,
    worker_idx: usize,
    request_idx: usize,
) -> Result<u16, String> {
    let mut addrs = addr
        .to_socket_addrs()
        .map_err(|e| format!("unable to resolve addr '{addr}': {e}"))?;
    let socket_addr = addrs
        .next()
        .ok_or_else(|| format!("unable to resolve addr '{addr}'"))?;

    let mut stream = TcpStream::connect_timeout(
        &socket_addr,
        Duration::from_millis(config.connect_timeout_ms),
    )
    .map_err(|e| format!("connect failed: {e}"))?;
    stream
        .set_read_timeout(Some(Duration::from_millis(config.read_timeout_ms)))
        .map_err(|e| format!("set_read_timeout failed: {e}"))?;
    stream
        .set_write_timeout(Some(Duration::from_millis(config.read_timeout_ms)))
        .map_err(|e| format!("set_write_timeout failed: {e}"))?;

    let request_body = config
        .body_template
        .as_ref()
        .map(|template| render_body_template(template, worker_idx, request_idx))
        .unwrap_or_default();
    let request = build_http_request(
        &config.method,
        addr,
        path,
        &request_body,
        &config.content_type,
    );
    stream
        .write_all(request.as_bytes())
        .map_err(|e| format!("write failed: {e}"))?;

    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .map_err(|e| format!("read failed: {e}"))?;
    parse_status_code(&response)
}

fn build_http_request(
    method: &str,
    addr: &str,
    path: &str,
    request_body: &str,
    content_type: &str,
) -> String {
    if request_body.is_empty() {
        return format!(
            "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\nAccept: application/json\r\n\r\n"
        );
    }

    format!(
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\nAccept: application/json\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\n\r\n{}",
        request_body.len(),
        request_body
    )
}

fn render_body_template(template: &str, worker_idx: usize, request_idx: usize) -> String {
    let now_epoch_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis().to_string())
        .unwrap_or_else(|_| "0".to_string());
    template
        .replace("%WORKER%", &worker_idx.to_string())
        .replace("%REQUEST%", &request_idx.to_string())
        .replace("%EPOCH_MS%", &now_epoch_ms)
}

fn parse_status_code(response: &[u8]) -> Result<u16, String> {
    let line_end = response
        .windows(2)
        .position(|w| w == b"\r\n")
        .ok_or_else(|| "invalid HTTP response: missing status line terminator".to_string())?;
    let status_line = std::str::from_utf8(&response[..line_end])
        .map_err(|_| "invalid HTTP response: status line not UTF-8".to_string())?;
    let status = status_line
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| "invalid HTTP response: missing status code".to_string())?;
    status
        .parse::<u16>()
        .map_err(|_| "invalid HTTP response: status code parse failed".to_string())
}

fn parse_args<I>(args: I) -> Result<Config, String>
where
    I: Iterator<Item = String>,
{
    let mut config = Config {
        addr: "127.0.0.1:8080".to_string(),
        path: "/v1/retrieve?tenant_id=sample-tenant&query=retrieval+initialized&top_k=5&stance_mode=balanced".to_string(),
        method: "GET".to_string(),
        body_template: None,
        content_type: "application/json".to_string(),
        concurrency: 16,
        requests_per_worker: 100,
        warmup_requests: 10,
        connect_timeout_ms: 2_000,
        read_timeout_ms: 5_000,
    };

    let mut args = args.peekable();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--addr" => {
                config.addr = args
                    .next()
                    .ok_or_else(|| "Missing value for --addr".to_string())?;
            }
            "--path" => {
                config.path = args
                    .next()
                    .ok_or_else(|| "Missing value for --path".to_string())?;
            }
            "--method" => {
                let method = args
                    .next()
                    .ok_or_else(|| "Missing value for --method".to_string())?;
                config.method = parse_method(&method)?;
            }
            "--body" => {
                config.body_template = Some(
                    args.next()
                        .ok_or_else(|| "Missing value for --body".to_string())?,
                );
            }
            "--content-type" => {
                config.content_type = args
                    .next()
                    .ok_or_else(|| "Missing value for --content-type".to_string())?;
            }
            "--concurrency" => {
                config.concurrency = parse_usize_arg(&mut args, "--concurrency")?;
            }
            "--requests-per-worker" => {
                config.requests_per_worker = parse_usize_arg(&mut args, "--requests-per-worker")?;
            }
            "--warmup-requests" => {
                config.warmup_requests =
                    parse_usize_allow_zero_arg(&mut args, "--warmup-requests")?;
            }
            "--connect-timeout-ms" => {
                config.connect_timeout_ms = parse_u64_arg(&mut args, "--connect-timeout-ms")?;
            }
            "--read-timeout-ms" => {
                config.read_timeout_ms = parse_u64_arg(&mut args, "--read-timeout-ms")?;
            }
            "--help" | "-h" => return Err(usage_text().to_string()),
            _ => return Err(format!("Unknown argument '{arg}'.\n\n{}", usage_text())),
        }
    }

    if !config.path.starts_with('/') {
        return Err("--path must start with '/'".to_string());
    }
    if config.concurrency == 0 {
        return Err("--concurrency must be > 0".to_string());
    }
    if config.requests_per_worker == 0 {
        return Err("--requests-per-worker must be > 0".to_string());
    }
    if config.method == "POST" && config.body_template.is_none() {
        return Err("--method POST requires --body payload".to_string());
    }

    Ok(config)
}

fn parse_method(raw: &str) -> Result<String, String> {
    let normalized = raw.trim().to_ascii_uppercase();
    match normalized.as_str() {
        "GET" | "POST" => Ok(normalized),
        _ => Err(format!("Unsupported --method '{raw}' (allowed: GET, POST)")),
    }
}

fn parse_usize_arg<I>(args: &mut I, flag: &str) -> Result<usize, String>
where
    I: Iterator<Item = String>,
{
    let raw = args
        .next()
        .ok_or_else(|| format!("Missing value for {flag}"))?;
    let value = raw
        .parse::<usize>()
        .map_err(|_| format!("Invalid value for {flag}: {raw}"))?;
    if value == 0 {
        return Err(format!("{flag} must be > 0"));
    }
    Ok(value)
}

fn parse_usize_allow_zero_arg<I>(args: &mut I, flag: &str) -> Result<usize, String>
where
    I: Iterator<Item = String>,
{
    let raw = args
        .next()
        .ok_or_else(|| format!("Missing value for {flag}"))?;
    raw.parse::<usize>()
        .map_err(|_| format!("Invalid value for {flag}: {raw}"))
}

fn parse_u64_arg<I>(args: &mut I, flag: &str) -> Result<u64, String>
where
    I: Iterator<Item = String>,
{
    let raw = args
        .next()
        .ok_or_else(|| format!("Missing value for {flag}"))?;
    let value = raw
        .parse::<u64>()
        .map_err(|_| format!("Invalid value for {flag}: {raw}"))?;
    if value == 0 {
        return Err(format!("{flag} must be > 0"));
    }
    Ok(value)
}

fn usage_text() -> &'static str {
    "Usage: cargo run -p benchmark-smoke --bin concurrent_load -- [--addr HOST:PORT] [--path /v1/retrieve?... ] [--method GET|POST] [--body '{...}'] [--content-type MIME] [--concurrency N] [--requests-per-worker N] [--warmup-requests N] [--connect-timeout-ms N] [--read-timeout-ms N]"
}
