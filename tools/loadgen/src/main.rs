//! `loadgen`: closed-loop load and soak generator for the DASH services.
//!
//! `--concurrency` worker threads each send one request at a time (ingest to
//! the ingestion service, retrieve to the retrieval service, mixed by
//! `--ingest-percent`) for `--duration-secs`. Latency is recorded per
//! operation in HDR histograms; the report gives throughput, p50/p95/p99/max
//! latency and errors by kind, as text and (with `--json-out`) as JSON.
//!
//! By default it starts its own ingestion + retrieval pair (the real
//! binaries, through the e2e harness) on loopback with generated keys;
//! `--ingest-url`/`--retrieve-url` with keys target a running deployment.
//! Soak use: `--report-every-secs` prints interval stats and samples the
//! servers' resident memory (`VmRSS` in `/proc/<pid>/status`);
//! `--max-rss-growth-mib` turns unbounded growth into a failure, and
//! `--id-space` keeps the data set bounded so memory should plateau.
//! Every interval (and at the end) it also samples the retrieval follower's
//! replication lag in WAL records, and after the load stops it measures how
//! long the follower takes to catch up (`--catch-up-timeout-secs`).
//!
//! Exit status: 0 when the error rate and RSS growth stay within the given
//! limits, 1 otherwise, 2 for usage errors. See
//! `docs/operations/testing-durability.md`.

use std::collections::BTreeMap;
use std::net::{SocketAddr, ToSocketAddrs};
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use dash_e2e::{Client, Stack, StackOpts, rss_kib};
use hdrhistogram::Histogram;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde_json::{Value, json};

const USAGE: &str = "usage: loadgen [options]
  --concurrency N          worker threads, one request in flight each (default 16)
  --duration-secs N        measured run time (default 30)
  --warmup-secs N          unmeasured warm-up before the run (default 5)
  --ingest-percent N       share of ingest requests, the rest retrieve (default 50)
  --dim N                  vector dimension sent with claims and queries;
                           0 lets the server embed text (default 384)
  --evidence N             evidence items per ingested claim (default 2)
  --batch-size N           claims per ingest request; above 1 the request
                           goes to /v1/ingest/batch (default 1)
  --top-k N                retrieve top_k (default 10)
  --preload N              claims ingested before the run (default 1000)
  --id-space N             ingest claim ids drawn from N ids (updates once
                           all exist); 0 = always new ids (default 0)
  --tenant T               tenant id (default tenant-a)
  --seed N                 RNG seed (default random)
  --report-every-secs N    print interval stats and sample server RSS (default 0: off)
  --max-error-rate F       fail when errors/requests exceeds F (default 0)
  --max-rss-growth-mib N   fail when any server's RSS grows more than N MiB
                           between the first and last sample
  --json-out PATH          write the report as JSON
  --catch-up-timeout-secs N after the run, wait up to N s for the retrieval
                           follower to reach the leader's WAL position and
                           report the time it took (default 120; 0: skip)
 spawned servers (default):
  --server-workers N       HTTP workers per service (default 4)
  --checkpoint-every N     DASH_CHECKPOINT_MAX_WAL_RECORDS for ingestion
  --server-env KEY=VALUE   extra environment for both spawned services
                           (repeatable; e.g. DASH_ENCRYPTION_KEY_FILE=...)
 external servers:
  --ingest-url URL --retrieve-url URL
  --ingest-key KEY --retrieve-key KEY
  --server-pid NAME=PID    sample RSS of a local server process (repeatable)

Spawned binaries come from DASH_E2E_BIN_DIR, or are built with cargo
(DASH_E2E_PROFILE=release for release builds).";

#[derive(Debug, Clone)]
struct Config {
    concurrency: usize,
    duration: Duration,
    warmup: Duration,
    ingest_percent: u32,
    dim: usize,
    evidence: usize,
    batch_size: usize,
    top_k: usize,
    preload: usize,
    id_space: u64,
    tenant: String,
    seed: u64,
    report_every: Option<Duration>,
    max_error_rate: f64,
    max_rss_growth_mib: Option<f64>,
    json_out: Option<PathBuf>,
    catch_up_timeout: Duration,
    server_workers: usize,
    checkpoint_every: Option<usize>,
    server_envs: Vec<(String, String)>,
    ingest_url: Option<String>,
    retrieve_url: Option<String>,
    ingest_key: Option<String>,
    retrieve_key: Option<String>,
    server_pids: Vec<(String, u32)>,
}

fn parse_args(args: &[String]) -> Result<Config, String> {
    let mut cfg = Config {
        concurrency: 16,
        duration: Duration::from_secs(30),
        warmup: Duration::from_secs(5),
        ingest_percent: 50,
        dim: 384,
        evidence: 2,
        batch_size: 1,
        top_k: 10,
        preload: 1000,
        id_space: 0,
        tenant: "tenant-a".into(),
        seed: rand::thread_rng().r#gen(),
        report_every: None,
        max_error_rate: 0.0,
        max_rss_growth_mib: None,
        json_out: None,
        catch_up_timeout: Duration::from_secs(120),
        server_workers: 4,
        checkpoint_every: None,
        server_envs: vec![],
        ingest_url: None,
        retrieve_url: None,
        ingest_key: None,
        retrieve_key: None,
        server_pids: vec![],
    };
    let mut it = args.iter();
    while let Some(flag) = it.next() {
        let mut value = || {
            it.next()
                .cloned()
                .ok_or_else(|| format!("{flag} needs a value"))
        };
        fn num<T: std::str::FromStr>(flag: &str, raw: String) -> Result<T, String> {
            raw.parse()
                .map_err(|_| format!("{flag}: not a number: {raw}"))
        }
        let secs = |raw: String| -> Result<Duration, String> {
            Ok(Duration::from_secs_f64(num::<f64>(flag, raw)?.max(0.0)))
        };
        match flag.as_str() {
            "--concurrency" => cfg.concurrency = num::<usize>(flag, value()?)?.max(1),
            "--duration-secs" => cfg.duration = secs(value()?)?,
            "--warmup-secs" => cfg.warmup = secs(value()?)?,
            "--ingest-percent" => cfg.ingest_percent = num::<u32>(flag, value()?)?.min(100),
            "--dim" => cfg.dim = num(flag, value()?)?,
            "--evidence" => cfg.evidence = num(flag, value()?)?,
            "--batch-size" => cfg.batch_size = num::<usize>(flag, value()?)?.max(1),
            "--top-k" => cfg.top_k = num::<usize>(flag, value()?)?.max(1),
            "--preload" => cfg.preload = num(flag, value()?)?,
            "--id-space" => cfg.id_space = num(flag, value()?)?,
            "--tenant" => cfg.tenant = value()?,
            "--seed" => cfg.seed = num(flag, value()?)?,
            "--report-every-secs" => {
                let d = secs(value()?)?;
                cfg.report_every = (!d.is_zero()).then_some(d);
            }
            "--max-error-rate" => cfg.max_error_rate = num(flag, value()?)?,
            "--max-rss-growth-mib" => cfg.max_rss_growth_mib = Some(num(flag, value()?)?),
            "--json-out" => cfg.json_out = Some(PathBuf::from(value()?)),
            "--catch-up-timeout-secs" => cfg.catch_up_timeout = secs(value()?)?,
            "--server-workers" => cfg.server_workers = num::<usize>(flag, value()?)?.max(1),
            "--checkpoint-every" => cfg.checkpoint_every = Some(num(flag, value()?)?),
            "--server-env" => {
                let raw = value()?;
                let (k, v) = raw
                    .split_once('=')
                    .ok_or_else(|| format!("--server-env expects KEY=VALUE, got {raw}"))?;
                cfg.server_envs.push((k.to_string(), v.to_string()));
            }
            "--ingest-url" => cfg.ingest_url = Some(value()?),
            "--retrieve-url" => cfg.retrieve_url = Some(value()?),
            "--ingest-key" => cfg.ingest_key = Some(value()?),
            "--retrieve-key" => cfg.retrieve_key = Some(value()?),
            "--server-pid" => {
                let raw = value()?;
                let (name, pid) = raw
                    .split_once('=')
                    .ok_or_else(|| format!("--server-pid expects NAME=PID, got {raw}"))?;
                cfg.server_pids
                    .push((name.to_string(), num(flag, pid.to_string())?));
            }
            "-h" | "--help" => return Err(String::new()),
            other => return Err(format!("unknown argument {other}")),
        }
    }
    let external = [
        &cfg.ingest_url,
        &cfg.retrieve_url,
        &cfg.ingest_key,
        &cfg.retrieve_key,
    ];
    let given = external.iter().filter(|v| v.is_some()).count();
    if given != 0 && given != external.len() {
        return Err(
            "external mode needs all of --ingest-url, --retrieve-url, --ingest-key, --retrieve-key"
                .into(),
        );
    }
    Ok(cfg)
}

/// `http://host:port[/]` or `host:port` to a socket address.
fn parse_addr(raw: &str) -> Result<SocketAddr, String> {
    let rest = raw
        .strip_prefix("http://")
        .unwrap_or(raw)
        .trim_end_matches('/');
    if raw.starts_with("https://") {
        return Err(format!(
            "{raw}: TLS endpoints are not supported; use plain http"
        ));
    }
    rest.to_socket_addrs()
        .map_err(|e| format!("{raw}: {e}"))?
        .next()
        .ok_or_else(|| format!("{raw}: no address"))
}

/// Where the load goes.
struct Target {
    ingest: SocketAddr,
    retrieve: SocketAddr,
    ingest_key: String,
    retrieve_key: String,
    /// Kept alive for the run when the servers were spawned here.
    stack: Option<Stack>,
    pids: Vec<(String, u32)>,
}

fn spawn_target(cfg: &Config) -> Target {
    let workers = cfg.server_workers.to_string();
    let mut s = Stack::new(StackOpts {
        tenants: vec![cfg.tenant.clone()],
        checkpoint_every: cfg.checkpoint_every,
        extra_ingest_env: [("DASH_INGEST_HTTP_WORKERS".to_string(), workers.clone())]
            .into_iter()
            .chain(cfg.server_envs.iter().cloned())
            .collect(),
        extra_retrieval_env: [("DASH_RETRIEVAL_HTTP_WORKERS".to_string(), workers)]
            .into_iter()
            .chain(cfg.server_envs.iter().cloned())
            .collect(),
    });
    s.start_all();
    s.wait_retrieval_ready(Duration::from_secs(60));
    let pids = vec![
        ("ingestion".to_string(), s.ingest.as_ref().unwrap().pid()),
        ("retrieval".to_string(), s.retrieval.as_ref().unwrap().pid()),
    ];
    Target {
        ingest: s.ingest_addr(),
        retrieve: s.retrieval_addr(),
        ingest_key: s.ik(&cfg.tenant).1,
        retrieve_key: s.rk(&cfg.tenant).1,
        stack: Some(s),
        pids,
    }
}

const WORDS: &[&str] = &[
    "turbine",
    "bearing",
    "vibration",
    "coolant",
    "pressure",
    "valve",
    "rotor",
    "blade",
    "sensor",
    "anomaly",
    "inspection",
    "corrosion",
    "fatigue",
    "thermal",
    "gearbox",
    "lubricant",
    "seal",
    "torque",
    "alignment",
    "spectrum",
    "harmonic",
    "failure",
    "maintenance",
    "outage",
    "grid",
    "voltage",
    "insulation",
    "transformer",
    "relay",
    "breaker",
    "fault",
    "cable",
];

fn words(rng: &mut StdRng, n: usize) -> String {
    (0..n)
        .map(|_| WORDS[rng.gen_range(0..WORDS.len())])
        .collect::<Vec<_>>()
        .join(" ")
}

fn vector(rng: &mut StdRng, dim: usize) -> Vec<f32> {
    let mut v: Vec<f32> = (0..dim).map(|_| rng.gen_range(-1.0f32..1.0)).collect();
    let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt().max(1e-6);
    v.iter_mut().for_each(|x| *x /= norm);
    v
}

fn ingest_body(cfg: &Config, rng: &mut StdRng, claim_id: &str) -> Value {
    let evidence: Vec<Value> = (0..cfg.evidence)
        .map(|i| {
            json!({
                "evidence_id": format!("{claim_id}-e{i}"),
                "claim_id": claim_id,
                "source_id": format!("src://loadgen/{claim_id}/{i}"),
                "stance": "supports",
                "source_quality": 0.8,
            })
        })
        .collect();
    let claim = json!({
        "claim_id": claim_id,
        "tenant_id": cfg.tenant,
        "canonical_text": format!("{} {}", words(rng, 8), rng.gen_range(0..1_000_000)),
        "confidence": rng.gen_range(0.5..1.0),
    });
    let mut body = json!({"claim": claim, "evidence": evidence, "edges": []});
    if cfg.dim > 0 {
        body["claim_embedding"] = json!(vector(rng, cfg.dim));
    }
    body
}

fn retrieve_body(cfg: &Config, rng: &mut StdRng) -> Value {
    let mut body = json!({
        "tenant_id": cfg.tenant,
        "query": words(rng, 3),
        "top_k": cfg.top_k,
    });
    if cfg.dim > 0 {
        body["query_embedding"] = json!(vector(rng, cfg.dim));
    }
    body
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Op {
    Ingest = 0,
    Retrieve = 1,
}

impl Op {
    fn name(self) -> &'static str {
        match self {
            Op::Ingest => "ingest",
            Op::Retrieve => "retrieve",
        }
    }
}

fn new_hist() -> Histogram<u64> {
    Histogram::new_with_bounds(1, 120_000_000, 3).expect("histogram bounds")
}

/// Latency in microseconds plus outcome counters for one operation.
struct OpStats {
    total: Histogram<u64>,
    window: Histogram<u64>,
    ok: u64,
    window_ok: u64,
    window_err: u64,
    errors: BTreeMap<String, u64>,
}

impl OpStats {
    fn new() -> Self {
        Self {
            total: new_hist(),
            window: new_hist(),
            ok: 0,
            window_ok: 0,
            window_err: 0,
            errors: BTreeMap::new(),
        }
    }

    fn record(&mut self, micros: u64, outcome: Result<(), String>) {
        let micros = micros.clamp(1, 120_000_000);
        match outcome {
            Ok(()) => {
                self.total.record(micros).ok();
                self.window.record(micros).ok();
                self.ok += 1;
                self.window_ok += 1;
            }
            Err(kind) => {
                *self.errors.entry(kind).or_default() += 1;
                self.window_err += 1;
            }
        }
    }

    fn error_count(&self) -> u64 {
        self.errors.values().sum()
    }
}

struct Shared {
    ops: [Mutex<OpStats>; 2],
    recording: AtomicBool,
    stop: AtomicBool,
}

fn send(client: &Client, path: &str, key: &str, body: &Value) -> Result<(), String> {
    match client.try_post_json(path, &[("x-api-key", key)], body) {
        Ok(r) if (200..300).contains(&r.status) => Ok(()),
        Ok(r) => Err(format!("status_{}", r.status)),
        Err(e) => Err(format!("transport_{:?}", e.kind()).to_lowercase()),
    }
}

fn worker(
    cfg: Arc<Config>,
    target: Arc<(SocketAddr, SocketAddr, String, String)>,
    idx: usize,
    shared: Arc<Shared>,
) {
    let (ingest_addr, retrieve_addr, ingest_key, retrieve_key) = &*target;
    let mut ic = Client::new(*ingest_addr);
    let mut rc = Client::new(*retrieve_addr);
    ic.timeout = Duration::from_secs(30);
    rc.timeout = Duration::from_secs(30);
    let mut rng = StdRng::seed_from_u64(cfg.seed.wrapping_add(idx as u64 + 1));
    let mut seq = 0u64;
    while !shared.stop.load(Ordering::Relaxed) {
        let op = if rng.gen_range(0..100) < cfg.ingest_percent {
            Op::Ingest
        } else {
            Op::Retrieve
        };
        let started = Instant::now();
        let outcome = match op {
            Op::Ingest => {
                let mut items = Vec::with_capacity(cfg.batch_size);
                for _ in 0..cfg.batch_size {
                    seq += 1;
                    let id = if cfg.id_space > 0 {
                        format!("lg-{}", rng.gen_range(0..cfg.id_space))
                    } else {
                        format!("lg-w{idx}-{seq}")
                    };
                    items.push(ingest_body(&cfg, &mut rng, &id));
                }
                if items.len() == 1 {
                    send(&ic, "/v1/ingest", ingest_key, &items[0])
                } else {
                    send(
                        &ic,
                        "/v1/ingest/batch",
                        ingest_key,
                        &json!({ "items": items }),
                    )
                }
            }
            Op::Retrieve => send(
                &rc,
                "/v1/retrieve",
                retrieve_key,
                &retrieve_body(&cfg, &mut rng),
            ),
        };
        let micros = started.elapsed().as_micros() as u64;
        if shared.recording.load(Ordering::Relaxed) {
            shared.ops[op as usize]
                .lock()
                .unwrap()
                .record(micros, outcome);
        }
    }
}

/// Ingest `cfg.preload` claims (8 threads) so retrieval has data to search.
fn preload(cfg: &Config, target: &Target) -> Result<(), String> {
    if cfg.preload == 0 {
        return Ok(());
    }
    let threads = 8.min(cfg.preload);
    let per = cfg.preload.div_ceil(threads);
    let handles: Vec<_> = (0..threads)
        .map(|t| {
            let cfg = cfg.clone();
            let (addr, key) = (target.ingest, target.ingest_key.clone());
            thread::spawn(move || -> Result<(), String> {
                let client = Client::new(addr);
                let mut rng = StdRng::seed_from_u64(cfg.seed ^ (0xA5A5 + t as u64));
                for i in 0..per {
                    let n = t * per + i;
                    if n >= cfg.preload {
                        break;
                    }
                    let id = if cfg.id_space > 0 {
                        format!("lg-{}", n as u64 % cfg.id_space)
                    } else {
                        format!("lg-pre-{n}")
                    };
                    send(
                        &client,
                        "/v1/ingest",
                        &key,
                        &ingest_body(&cfg, &mut rng, &id),
                    )
                    .map_err(|e| format!("preload ingest {id}: {e}"))?;
                }
                Ok(())
            })
        })
        .collect();
    for h in handles {
        h.join()
            .map_err(|_| "preload thread panicked".to_string())??;
    }
    Ok(())
}

fn ms(micros: u64) -> f64 {
    micros as f64 / 1000.0
}

fn op_json(s: &OpStats, secs: f64) -> Value {
    let h = &s.total;
    json!({
        "ok": s.ok,
        "errors": s.error_count(),
        "errors_by_kind": s.errors,
        "throughput_rps": s.ok as f64 / secs.max(1e-9),
        "latency_ms": {
            "p50": ms(h.value_at_quantile(0.50)),
            "p95": ms(h.value_at_quantile(0.95)),
            "p99": ms(h.value_at_quantile(0.99)),
            "max": ms(h.max()),
            "mean": h.mean() / 1000.0,
        },
    })
}

#[derive(Debug, Clone)]
struct RssSample {
    t_secs: f64,
    name: String,
    kib: u64,
}

fn sample_rss(pids: &[(String, u32)], t0: Instant, out: &mut Vec<RssSample>) {
    for (name, pid) in pids {
        if let Some(kib) = rss_kib(*pid) {
            out.push(RssSample {
                t_secs: t0.elapsed().as_secs_f64(),
                name: name.clone(),
                kib,
            });
        }
    }
}

/// Per server: (first KiB, last KiB, peak KiB).
fn rss_summary(samples: &[RssSample]) -> BTreeMap<String, (u64, u64, u64)> {
    let mut out: BTreeMap<String, (u64, u64, u64)> = BTreeMap::new();
    for s in samples {
        out.entry(s.name.clone())
            .and_modify(|(_, last, peak)| {
                *last = s.kib;
                *peak = (*peak).max(s.kib);
            })
            .or_insert((s.kib, s.kib, s.kib));
    }
    out
}

/// Replication lag of the retrieval follower in WAL records. With spawned
/// servers it is measured against the leader's live WAL position (the
/// follower's own `lag_records` only knows the total it saw on its last
/// poll); a follower in another generation (resync in progress) lags by the
/// leader's whole WAL. Against external servers it is the follower's
/// reported `lag_records`. `None` when it cannot be read.
fn follower_lag(target: &Target) -> Option<u64> {
    match &target.stack {
        Some(stack) => {
            let (generation, total) = stack.try_leader_position()?;
            let rep = stack.try_retrieval_replication()?;
            if rep["generation"].as_u64() != Some(generation) {
                return Some(total as u64);
            }
            Some((total as u64).saturating_sub(rep["offset"].as_u64()?))
        }
        None => {
            let mut client = Client::new(target.retrieve);
            client.timeout = Duration::from_secs(5);
            let r = client.request("GET", "/ready", &[], None).ok()?;
            r.json()["replication"]["lag_records"].as_u64()
        }
    }
}

#[derive(Debug, Clone, Default)]
struct LagStats {
    samples: Vec<(f64, u64)>,
}

impl LagStats {
    fn record(&mut self, t_secs: f64, lag: Option<u64>) {
        if let Some(lag) = lag {
            self.samples.push((t_secs, lag));
        }
    }

    fn max(&self) -> Option<u64> {
        self.samples.iter().map(|(_, lag)| *lag).max()
    }

    fn mean(&self) -> Option<f64> {
        if self.samples.is_empty() {
            return None;
        }
        Some(
            self.samples.iter().map(|(_, lag)| *lag as f64).sum::<f64>()
                / self.samples.len() as f64,
        )
    }

    fn last(&self) -> Option<u64> {
        self.samples.last().map(|(_, lag)| *lag)
    }
}

/// Time until the follower's lag reaches zero, polled every 50 ms, or
/// `None` when it does not within `timeout`.
fn wait_follower_caught_up(target: &Target, timeout: Duration) -> Option<Duration> {
    let started = Instant::now();
    loop {
        if follower_lag(target) == Some(0) {
            return Some(started.elapsed());
        }
        if started.elapsed() >= timeout {
            return None;
        }
        thread::sleep(Duration::from_millis(50));
    }
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let cfg = match parse_args(&args) {
        Ok(c) => c,
        Err(e) => {
            if !e.is_empty() {
                eprintln!("loadgen: {e}");
            }
            eprintln!("{USAGE}");
            return ExitCode::from(2);
        }
    };
    let target = if let (Some(iu), Some(ru), Some(ik), Some(rk)) = (
        &cfg.ingest_url,
        &cfg.retrieve_url,
        &cfg.ingest_key,
        &cfg.retrieve_key,
    ) {
        let (ingest, retrieve) = match (parse_addr(iu), parse_addr(ru)) {
            (Ok(i), Ok(r)) => (i, r),
            (Err(e), _) | (_, Err(e)) => {
                eprintln!("loadgen: {e}");
                return ExitCode::from(2);
            }
        };
        Target {
            ingest,
            retrieve,
            ingest_key: ik.clone(),
            retrieve_key: rk.clone(),
            stack: None,
            pids: cfg.server_pids.clone(),
        }
    } else {
        spawn_target(&cfg)
    };
    let mode = if target.stack.is_some() {
        "spawned"
    } else {
        "external"
    };
    println!(
        "loadgen seed={} mode={mode} concurrency={} duration={:?} warmup={:?} ingest_percent={} dim={} evidence={} batch_size={} top_k={} preload={} id_space={}",
        cfg.seed,
        cfg.concurrency,
        cfg.duration,
        cfg.warmup,
        cfg.ingest_percent,
        cfg.dim,
        cfg.evidence,
        cfg.batch_size,
        cfg.top_k,
        cfg.preload,
        cfg.id_space
    );
    let t_preload = Instant::now();
    if let Err(e) = preload(&cfg, &target) {
        eprintln!("loadgen: {e}");
        return ExitCode::from(1);
    }
    if let Some(s) = &target.stack {
        s.wait_caught_up(Duration::from_secs(120));
    }
    if cfg.preload > 0 {
        println!(
            "preloaded {} claims in {:.1}s",
            cfg.preload,
            t_preload.elapsed().as_secs_f64()
        );
    }

    let shared = Arc::new(Shared {
        ops: [Mutex::new(OpStats::new()), Mutex::new(OpStats::new())],
        recording: AtomicBool::new(false),
        stop: AtomicBool::new(false),
    });
    let cfg = Arc::new(cfg);
    let addrs = Arc::new((
        target.ingest,
        target.retrieve,
        target.ingest_key.clone(),
        target.retrieve_key.clone(),
    ));
    let handles: Vec<_> = (0..cfg.concurrency)
        .map(|i| {
            let (cfg, addrs, shared) = (cfg.clone(), addrs.clone(), shared.clone());
            thread::spawn(move || worker(cfg, addrs, i, shared))
        })
        .collect();

    let t_start = Instant::now();
    thread::sleep(cfg.warmup);
    let mut rss: Vec<RssSample> = vec![];
    sample_rss(&target.pids, t_start, &mut rss);
    shared.recording.store(true, Ordering::Relaxed);
    let t_run = Instant::now();
    let end = t_run + cfg.duration;
    let mut last_report = Instant::now();
    let mut lag = LagStats::default();
    while Instant::now() < end {
        let step = cfg
            .report_every
            .map(|r| (last_report + r).min(end))
            .unwrap_or(end);
        thread::sleep(step.saturating_duration_since(Instant::now()));
        if let Some(every) = cfg.report_every
            && last_report.elapsed() >= every
        {
            let window = last_report.elapsed().as_secs_f64();
            last_report = Instant::now();
            sample_rss(&target.pids, t_start, &mut rss);
            let lag_now = follower_lag(&target);
            lag.record(t_run.elapsed().as_secs_f64(), lag_now);
            let mut line = format!("[{:>7.1}s]", t_run.elapsed().as_secs_f64());
            for op in [Op::Ingest, Op::Retrieve] {
                let mut s = shared.ops[op as usize].lock().unwrap();
                line.push_str(&format!(
                    " {}: {:.0} rps p50={:.2}ms p99={:.2}ms err={}",
                    op.name(),
                    s.window_ok as f64 / window,
                    ms(s.window.value_at_quantile(0.5)),
                    ms(s.window.value_at_quantile(0.99)),
                    s.window_err
                ));
                s.window.reset();
                s.window_ok = 0;
                s.window_err = 0;
            }
            for (name, pid) in &target.pids {
                if let Some(kib) = rss_kib(*pid) {
                    line.push_str(&format!(" {name}_rss={:.1}MiB", kib as f64 / 1024.0));
                }
            }
            if let Some(records) = lag_now {
                line.push_str(&format!(" follower_lag={records}"));
            }
            println!("{line}");
        }
    }
    shared.recording.store(false, Ordering::Relaxed);
    let measured = t_run.elapsed().as_secs_f64();
    lag.record(measured, follower_lag(&target));
    shared.stop.store(true, Ordering::Relaxed);
    for h in handles {
        let _ = h.join();
    }
    sample_rss(&target.pids, t_start, &mut rss);
    let lag_at_stop = follower_lag(&target);
    let catch_up = (!cfg.catch_up_timeout.is_zero())
        .then(|| wait_follower_caught_up(&target, cfg.catch_up_timeout));

    let ingest = shared.ops[0].lock().unwrap();
    let retrieve = shared.ops[1].lock().unwrap();
    let requests = ingest.ok + retrieve.ok + ingest.error_count() + retrieve.error_count();
    let errors = ingest.error_count() + retrieve.error_count();
    let error_rate = if requests > 0 {
        errors as f64 / requests as f64
    } else {
        0.0
    };
    let rss_by_server = rss_summary(&rss);
    let mut failures: Vec<String> = vec![];
    if requests == 0 {
        failures.push("no request completed".into());
    }
    if error_rate > cfg.max_error_rate {
        failures.push(format!(
            "error rate {error_rate:.6} above the limit {}",
            cfg.max_error_rate
        ));
    }
    if let Some(None) = catch_up {
        failures.push(format!(
            "retrieval follower did not catch up within {:?} after the load stopped",
            cfg.catch_up_timeout
        ));
    }
    if let Some(limit) = cfg.max_rss_growth_mib {
        for (name, (first, last, _)) in &rss_by_server {
            let growth = (*last as f64 - *first as f64) / 1024.0;
            if growth > limit {
                failures.push(format!(
                    "{name} RSS grew {growth:.1} MiB (limit {limit} MiB)"
                ));
            }
        }
    }

    println!();
    println!(
        "loadgen report: measured {measured:.1}s, concurrency {}, ingest {}% / retrieve {}%, dim {}, batch size {}",
        cfg.concurrency,
        cfg.ingest_percent,
        100 - cfg.ingest_percent,
        cfg.dim,
        cfg.batch_size
    );
    println!(
        "{:<9} {:>9} {:>7} {:>10} {:>9} {:>9} {:>9} {:>9}",
        "op", "ok", "errors", "rps", "p50 ms", "p95 ms", "p99 ms", "max ms"
    );
    for (op, s) in [(Op::Ingest, &*ingest), (Op::Retrieve, &*retrieve)] {
        let h = &s.total;
        println!(
            "{:<9} {:>9} {:>7} {:>10.1} {:>9.2} {:>9.2} {:>9.2} {:>9.2}",
            op.name(),
            s.ok,
            s.error_count(),
            s.ok as f64 / measured.max(1e-9),
            ms(h.value_at_quantile(0.50)),
            ms(h.value_at_quantile(0.95)),
            ms(h.value_at_quantile(0.99)),
            ms(h.max())
        );
        for (kind, n) in &s.errors {
            println!("          error {kind}: {n}");
        }
    }
    for (name, (first, last, peak)) in &rss_by_server {
        println!(
            "{name} rss: first {:.1} MiB, last {:.1} MiB, peak {:.1} MiB, growth {:+.1} MiB",
            *first as f64 / 1024.0,
            *last as f64 / 1024.0,
            *peak as f64 / 1024.0,
            (*last as f64 - *first as f64) / 1024.0
        );
    }
    let fmt_opt = |v: Option<u64>| v.map_or_else(|| "n/a".to_string(), |v| v.to_string());
    println!(
        "follower lag (WAL records): max {}, mean {}, at end of run {}, after workers stopped {}",
        fmt_opt(lag.max()),
        lag.mean()
            .map_or_else(|| "n/a".to_string(), |v| format!("{v:.0}")),
        fmt_opt(lag.last()),
        fmt_opt(lag_at_stop),
    );
    match catch_up {
        Some(Some(d)) => println!(
            "follower caught up {:.2}s after the load stopped",
            d.as_secs_f64()
        ),
        Some(None) => println!(
            "follower did not catch up within {:?}",
            cfg.catch_up_timeout
        ),
        None => {}
    }
    let report = json!({
        "ok": failures.is_empty(),
        "failures": failures,
        "seed": cfg.seed,
        "mode": mode,
        "config": {
            "concurrency": cfg.concurrency,
            "duration_secs": cfg.duration.as_secs_f64(),
            "warmup_secs": cfg.warmup.as_secs_f64(),
            "ingest_percent": cfg.ingest_percent,
            "dim": cfg.dim,
            "evidence": cfg.evidence,
            "batch_size": cfg.batch_size,
            "top_k": cfg.top_k,
            "preload": cfg.preload,
            "id_space": cfg.id_space,
            "server_workers": cfg.server_workers,
        },
        "measured_secs": measured,
        "requests": requests,
        "errors": errors,
        "error_rate": error_rate,
        "total_throughput_rps": (ingest.ok + retrieve.ok) as f64 / measured.max(1e-9),
        "ingest": op_json(&ingest, measured),
        "ingested_claims_per_sec": (ingest.ok * cfg.batch_size as u64) as f64 / measured.max(1e-9),
        "retrieve": op_json(&retrieve, measured),
        "rss": rss_by_server.iter().map(|(name, (first, last, peak))| {
            (name.clone(), json!({"first_kib": first, "last_kib": last, "peak_kib": peak}))
        }).collect::<serde_json::Map<String, Value>>(),
        "follower_lag_records": {
            "max": lag.max(),
            "mean": lag.mean(),
            "end_of_run": lag.last(),
            "after_stop": lag_at_stop,
            "samples": lag.samples.iter().map(|(t, l)| json!({"t_secs": t, "records": l})).collect::<Vec<_>>(),
        },
        "follower_catch_up_secs": catch_up.flatten().map(|d| d.as_secs_f64()),
        "rss_samples": rss.iter().map(|s| json!({"t_secs": s.t_secs, "server": s.name, "kib": s.kib})).collect::<Vec<_>>(),
    });
    if let Some(path) = &cfg.json_out
        && let Err(e) = std::fs::write(path, format!("{report:#}\n"))
    {
        eprintln!("loadgen: cannot write {}: {e}", path.display());
        return ExitCode::from(1);
    }
    drop(target);
    if failures.is_empty() {
        println!("loadgen PASSED");
        ExitCode::SUCCESS
    } else {
        for f in &failures {
            eprintln!("loadgen FAILED: {f}");
        }
        ExitCode::from(1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(raw: &[&str]) -> Vec<String> {
        raw.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn defaults_and_overrides_parse() {
        let cfg = parse_args(&[]).unwrap();
        assert_eq!(cfg.concurrency, 16);
        assert_eq!(cfg.dim, 384);
        assert!(cfg.report_every.is_none());
        let cfg = parse_args(&args(&[
            "--concurrency",
            "4",
            "--duration-secs",
            "1.5",
            "--ingest-percent",
            "150",
            "--report-every-secs",
            "10",
            "--server-pid",
            "ingestion=42",
        ]))
        .unwrap();
        assert_eq!(cfg.concurrency, 4);
        assert_eq!(cfg.duration, Duration::from_millis(1500));
        assert_eq!(cfg.ingest_percent, 100, "clamped");
        assert_eq!(cfg.report_every, Some(Duration::from_secs(10)));
        assert_eq!(cfg.server_pids, vec![("ingestion".to_string(), 42)]);
    }

    #[test]
    fn external_mode_needs_every_endpoint_and_key() {
        let err = parse_args(&args(&["--ingest-url", "http://127.0.0.1:1"])).unwrap_err();
        assert!(err.contains("external mode"), "{err}");
        assert!(parse_args(&args(&["--nope"])).is_err());
        assert!(parse_args(&args(&["--dim", "x"])).is_err());
    }

    #[test]
    fn addresses_parse_with_or_without_scheme() {
        assert_eq!(
            parse_addr("http://127.0.0.1:8080/").unwrap(),
            "127.0.0.1:8080".parse().unwrap()
        );
        assert_eq!(
            parse_addr("127.0.0.1:9").unwrap(),
            "127.0.0.1:9".parse().unwrap()
        );
        assert!(parse_addr("https://127.0.0.1:443").is_err());
    }

    #[test]
    fn op_stats_track_latency_and_errors() {
        let mut s = OpStats::new();
        for us in [1000, 2000, 3000, 4000] {
            s.record(us, Ok(()));
        }
        s.record(10, Err("status_503".into()));
        assert_eq!(s.ok, 4);
        assert_eq!(s.error_count(), 1);
        assert_eq!(s.errors["status_503"], 1);
        let j = op_json(&s, 2.0);
        assert_eq!(j["throughput_rps"], 2.0);
        assert!((j["latency_ms"]["max"].as_f64().unwrap() - 4.0).abs() < 0.01);
    }

    #[test]
    fn lag_stats_summarise_samples() {
        let mut lag = LagStats::default();
        assert_eq!(lag.max(), None);
        lag.record(1.0, Some(10));
        lag.record(2.0, None);
        lag.record(3.0, Some(30));
        lag.record(4.0, Some(5));
        assert_eq!(lag.max(), Some(30));
        assert_eq!(lag.last(), Some(5));
        assert_eq!(lag.mean(), Some(15.0));
        let cfg = parse_args(&args(&["--catch-up-timeout-secs", "0"])).unwrap();
        assert!(cfg.catch_up_timeout.is_zero());
    }

    #[test]
    fn rss_summary_reports_first_last_and_peak() {
        let mk = |t, kib| RssSample {
            t_secs: t,
            name: "ingestion".into(),
            kib,
        };
        let s = rss_summary(&[mk(0.0, 100), mk(1.0, 300), mk(2.0, 200)]);
        assert_eq!(s["ingestion"], (100, 200, 300));
    }

    #[test]
    fn ingest_body_carries_the_requested_shape() {
        let cfg = parse_args(&args(&["--dim", "8", "--evidence", "3"])).unwrap();
        let mut rng = StdRng::seed_from_u64(1);
        let b = ingest_body(&cfg, &mut rng, "c1");
        assert_eq!(b["claim"]["claim_id"], "c1");
        assert_eq!(b["evidence"].as_array().unwrap().len(), 3);
        assert_eq!(b["claim_embedding"].as_array().unwrap().len(), 8);
        let r = retrieve_body(&cfg, &mut rng);
        assert_eq!(r["query_embedding"].as_array().unwrap().len(), 8);
    }
}
