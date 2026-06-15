//! Performance benchmark suite for the DASH retrieval engine.
//!
//! Measures the latency and throughput of the six measurable hot paths
//! in the retrieval pipeline: in-memory ingest, persistent (WAL) ingest,
//! lexical retrieve, semantic retrieve (with a query vector), ANN
//! top-k search, and WAL replay.
//!
//! Each scenario runs a fixed warm-up loop (results discarded) and a
//! timed measurement loop. Per-iteration latencies are recorded with
//! [`std::time::Instant`], then sorted and summarized as p50/p95/p99/
//! max/min/mean microseconds plus overall throughput in ops/sec.
//!
//! Human-readable output is written to stdout. A single
//! machine-parseable JSON line is appended at the end of the run,
//! prefixed with `BENCH_JSON:`.
//!
//! Usage:
//!   cargo run -p benchmark-smoke --bin perf_bench -- --all
//!   cargo run -p benchmark-smoke --bin perf_bench -- --scenario retrieve_throughput_semantic --iterations 500

use std::{
    collections::BTreeMap,
    env, fmt, fs,
    path::PathBuf,
    process, time::{Instant, SystemTime, UNIX_EPOCH},
};

use schema::{Claim, Evidence, RetrievalRequest, Stance, StanceMode};
use serde::Serialize;
use store::{AnnTuningConfig, FileWal, InMemoryStore};

const DEFAULT_ITERATIONS: usize = 100;
const DEFAULT_WARMUP: usize = 10;

const INGEST_FIXTURE_TARGET: usize = 10_000;
const RETRIEVE_FIXTURE_SIZE: usize = 10_000;
const QUERY_MATCH_BUCKET: usize = 10;

const SEMANTIC_DIM: usize = 768;
const SEMANTIC_TOP_K: usize = 10;

const ANN_VECTORS: usize = 10_000;
const ANN_DIM: usize = 384;
const ANN_TOP_N: usize = 10;

const WAL_REPLAY_FIXTURE_CLAIMS: usize = 1_000;
const WAL_TENANT: &str = "tenant-perf-wal-replay";

const INGEST_TENANT: &str = "tenant-perf-ingest";
const LEX_TENANT: &str = "tenant-perf-retrieve-lex";
const SEM_TENANT: &str = "tenant-perf-retrieve-sem";
const ANN_TENANT: &str = "tenant-perf-ann";

const SCENARIO_INGEST: &str = "ingest_throughput_sequential";
const SCENARIO_LEX: &str = "retrieve_throughput_lexical";
const SCENARIO_SEM: &str = "retrieve_throughput_semantic";
const SCENARIO_ANN: &str = "ann_search_throughput_at_scale";
const SCENARIO_WAL: &str = "wal_replay_throughput";

const ALL_SCENARIOS: &[&str] = &[
    SCENARIO_INGEST,
    SCENARIO_LEX,
    SCENARIO_SEM,
    SCENARIO_ANN,
    SCENARIO_WAL,
];

// ---------------------------------------------------------------------------
// CLI argument parsing
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
struct BenchArgs {
    scenario: Option<String>,
    iterations: Option<usize>,
    warmup: Option<usize>,
    all: bool,
    help: bool,
}

fn parse_args<I>(args: I) -> Result<BenchArgs, String>
where
    I: Iterator<Item = String>,
{
    let mut out = BenchArgs::default();
    let mut args = args.peekable();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--scenario" => {
                let value = args
                    .next()
                    .ok_or_else(|| "Missing value for --scenario".to_string())?;
                out.scenario = Some(value);
            }
            "--iterations" => {
                let value = args
                    .next()
                    .ok_or_else(|| "Missing value for --iterations".to_string())?;
                let parsed = value
                    .parse::<usize>()
                    .map_err(|_| format!("Invalid --iterations value '{value}'"))?;
                if parsed == 0 {
                    return Err("--iterations must be > 0".to_string());
                }
                out.iterations = Some(parsed);
            }
            "--warmup" => {
                let value = args
                    .next()
                    .ok_or_else(|| "Missing value for --warmup".to_string())?;
                let parsed = value
                    .parse::<usize>()
                    .map_err(|_| format!("Invalid --warmup value '{value}'"))?;
                out.warmup = Some(parsed);
            }
            "--all" => out.all = true,
            "--help" | "-h" => out.help = true,
            other => {
                return Err(format!(
                    "Unknown argument '{other}'.\n\n{}",
                    usage_text()
                ));
            }
        }
    }
    if !(out.all || out.scenario.is_some() || out.help) {
        out.all = true;
    }
    Ok(out)
}

fn usage_text() -> &'static str {
    "Usage: cargo run -p benchmark-smoke --bin perf_bench -- [--all] [--scenario NAME] [--iterations N] [--warmup N]\n\
     Scenarios: ingest_throughput_sequential, retrieve_throughput_lexical, retrieve_throughput_semantic, ann_search_throughput_at_scale, wal_replay_throughput"
}

fn default_iterations_for(scenario: &str) -> usize {
    match scenario {
        SCENARIO_INGEST => 100,
        SCENARIO_LEX => 1_000,
        SCENARIO_SEM => 1_000,
        SCENARIO_ANN => 500,
        SCENARIO_WAL => 100,
        _ => DEFAULT_ITERATIONS,
    }
}

// ---------------------------------------------------------------------------
// BenchResult + serialization
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct BenchResult {
    name: String,
    iterations: usize,
    latencies_us: Vec<u64>,
    extras: BTreeMap<String, String>,
}

impl BenchResult {
    fn from_latencies(
        name: impl Into<String>,
        iterations: usize,
        mut latencies_us: Vec<u64>,
    ) -> Self {
        latencies_us.sort_unstable();
        Self {
            name: name.into(),
            iterations,
            latencies_us,
            extras: BTreeMap::new(),
        }
    }

    fn with_extra(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.extras.insert(key.into(), value.into());
        self
    }

    fn quantile(&self, q: f64) -> u64 {
        if self.latencies_us.is_empty() {
            return 0;
        }
        let len = self.latencies_us.len();
        let raw = ((len - 1) as f64 * q).round() as usize;
        let idx = raw.min(len - 1);
        self.latencies_us[idx]
    }

    fn min_us(&self) -> u64 {
        self.latencies_us.first().copied().unwrap_or(0)
    }

    fn max_us(&self) -> u64 {
        self.latencies_us.last().copied().unwrap_or(0)
    }

    fn mean_us(&self) -> f64 {
        if self.latencies_us.is_empty() {
            return 0.0;
        }
        self.latencies_us.iter().sum::<u64>() as f64 / self.latencies_us.len() as f64
    }

    fn throughput_ops_per_sec(&self) -> f64 {
        if self.latencies_us.is_empty() {
            return 0.0;
        }
        let total_seconds: f64 = self.latencies_us.iter().sum::<u64>() as f64 / 1_000_000.0;
        if total_seconds <= 0.0 {
            return 0.0;
        }
        self.latencies_us.len() as f64 / total_seconds
    }
}

impl fmt::Display for BenchResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "[{}]", self.name)?;
        writeln!(f, "  iterations:               {}", self.iterations)?;
        writeln!(f, "  p50_us:                   {}", self.quantile(0.50))?;
        writeln!(f, "  p95_us:                   {}", self.quantile(0.95))?;
        writeln!(f, "  p99_us:                   {}", self.quantile(0.99))?;
        writeln!(f, "  max_us:                   {}", self.max_us())?;
        writeln!(f, "  min_us:                   {}", self.min_us())?;
        writeln!(f, "  mean_us:                  {:.1}", self.mean_us())?;
        writeln!(
            f,
            "  throughput_ops_per_sec:   {:.0}",
            self.throughput_ops_per_sec()
        )?;
        for (key, value) in &self.extras {
            writeln!(f, "  {:<25} {}", key, value)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize)]
struct BenchResultJson {
    name: String,
    iterations: usize,
    p50_us: u64,
    p95_us: u64,
    p99_us: u64,
    max_us: u64,
    min_us: u64,
    mean_us: f64,
    throughput_ops_per_sec: f64,
    extras: BTreeMap<String, String>,
}

impl From<&BenchResult> for BenchResultJson {
    fn from(r: &BenchResult) -> Self {
        Self {
            name: r.name.clone(),
            iterations: r.iterations,
            p50_us: r.quantile(0.50),
            p95_us: r.quantile(0.95),
            p99_us: r.quantile(0.99),
            max_us: r.max_us(),
            min_us: r.min_us(),
            mean_us: r.mean_us(),
            throughput_ops_per_sec: r.throughput_ops_per_sec(),
            extras: r.extras.clone(),
        }
    }
}

// ---------------------------------------------------------------------------
// Fixture builders
// ---------------------------------------------------------------------------

fn make_claim(claim_id: &str, tenant_id: &str, text: &str, confidence: f32) -> Claim {
    Claim {
        claim_id: claim_id.to_string(),
        tenant_id: tenant_id.to_string(),
        canonical_text: text.to_string(),
        confidence,
        event_time_unix: None,
        entities: vec![],
        embedding_ids: vec![],
        claim_type: None,
        valid_from: None,
        valid_to: None,
        created_at: None,
        updated_at: None,
    }
}

fn make_evidence(evidence_id: &str, claim_id: &str) -> Evidence {
    Evidence {
        evidence_id: evidence_id.to_string(),
        claim_id: claim_id.to_string(),
        source_id: format!("source://perf/{evidence_id}"),
        stance: Stance::Supports,
        source_quality: 0.9,
        chunk_id: None,
        span_start: None,
        span_end: None,
        doc_id: None,
        extraction_model: None,
        ingested_at: None,
    }
}

fn fixture_vector(seed: usize, dim: usize) -> Vec<f32> {
    let mut out = Vec::with_capacity(dim);
    let mut state = (seed as u64).wrapping_mul(6364136223846793005).wrapping_add(1);
    for _ in 0..dim {
        state = state
            .wrapping_mul(2862933555777941757)
            .wrapping_add(3037000493);
        let value = ((state >> 33) as f32) / (u32::MAX as f32);
        out.push(value);
    }
    out
}

fn temp_dir(tag: &str) -> PathBuf {
    let mut out = env::temp_dir();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    out.push(format!(
        "dash-perf-bench-{}-{}-{}",
        tag,
        process::id(),
        nanos
    ));
    out
}

// ---------------------------------------------------------------------------
// Ingest helpers
// ---------------------------------------------------------------------------

fn ingest_one_in_memory(store: &mut InMemoryStore, tenant: &str, index: usize) {
    let id = format!("claim-perf-im-{index}");
    let claim = make_claim(&id, tenant, "perf in-memory ingest claim", 0.9);
    let evidence = make_evidence(&format!("evd-perf-im-{index}"), &id);
    store
        .ingest_bundle(claim, vec![evidence], vec![])
        .expect("in-memory ingest should succeed");
}

fn ingest_one_persistent(
    store: &mut InMemoryStore,
    wal: &mut FileWal,
    tenant: &str,
    index: usize,
) {
    let id = format!("claim-perf-pw-{index}");
    let claim = make_claim(&id, tenant, "perf persistent ingest claim", 0.9);
    let evidence = make_evidence(&format!("evd-perf-pw-{index}"), &id);
    store
        .ingest_bundle_persistent(wal, claim, vec![evidence], vec![])
        .expect("persistent ingest should succeed");
}

// ---------------------------------------------------------------------------
// Scenario runners
// ---------------------------------------------------------------------------

fn scenario_ingest_throughput_sequential(
    iterations: usize,
    warmup: usize,
) -> Result<Vec<BenchResult>, String> {
    let mut out = Vec::new();

    // (a) In-memory ingest (no WAL).
    {
        let mut store = InMemoryStore::new();
        let mut counter: usize = 0;
        for _ in 0..warmup {
            ingest_one_in_memory(&mut store, INGEST_TENANT, counter);
            counter += 1;
        }
        let mut latencies = Vec::with_capacity(iterations);
        for _ in 0..iterations {
            let start = Instant::now();
            ingest_one_in_memory(&mut store, INGEST_TENANT, counter);
            latencies.push(start.elapsed().as_micros() as u64);
            counter += 1;
        }
        out.push(
            BenchResult::from_latencies(
                "ingest_throughput_sequential.in_memory",
                iterations,
                latencies,
            )
            .with_extra("wal_enabled", "false")
            .with_extra("fixture_size_target", INGEST_FIXTURE_TARGET.to_string())
            .with_extra("claims_in_window", counter.to_string()),
        );
    }

    // (b) Persistent ingest with sync_every_records=1 (WAL fsync on every claim).
    {
        let tmp = temp_dir("ingest-wal");
        fs::create_dir_all(&tmp).map_err(|e| format!("mkdir temp: {e}"))?;
        let wal_path = tmp.join("bench.wal");
        let mut wal = FileWal::open(&wal_path).map_err(|e| format!("open wal: {e:?}"))?;
        let mut store = InMemoryStore::new();
        let mut counter: usize = 0;
        for _ in 0..warmup {
            ingest_one_persistent(&mut store, &mut wal, INGEST_TENANT, counter);
            counter += 1;
        }
        let mut latencies = Vec::with_capacity(iterations);
        for _ in 0..iterations {
            let start = Instant::now();
            ingest_one_persistent(&mut store, &mut wal, INGEST_TENANT, counter);
            latencies.push(start.elapsed().as_micros() as u64);
            counter += 1;
        }
        drop(wal);
        let _ = fs::remove_dir_all(&tmp);
        out.push(
            BenchResult::from_latencies(
                "ingest_throughput_sequential.persistent_wal",
                iterations,
                latencies,
            )
            .with_extra("wal_enabled", "true")
            .with_extra("wal_sync_every_records", "1")
            .with_extra("fixture_size_target", INGEST_FIXTURE_TARGET.to_string())
            .with_extra("claims_in_window", counter.to_string()),
        );
    }

    Ok(out)
}

fn scenario_retrieve_throughput_lexical(
    iterations: usize,
    warmup: usize,
) -> Result<Vec<BenchResult>, String> {
    let mut store = InMemoryStore::new();
    for i in 0..RETRIEVE_FIXTURE_SIZE {
        let id = format!("claim-rl-{i}");
        let text = if i < QUERY_MATCH_BUCKET {
            format!("alice operational update phase {i}")
        } else {
            format!("Unrelated operational update {i}")
        };
        let claim = make_claim(&id, LEX_TENANT, &text, 0.9);
        let evidence = make_evidence(&format!("evd-rl-{i}"), &id);
        store
            .ingest_bundle(claim, vec![evidence], vec![])
            .map_err(|e| format!("fixture ingest at {i}: {e:?}"))?;
    }

    let req = RetrievalRequest {
        tenant_id: LEX_TENANT.to_string(),
        query: "alice operation status".to_string(),
        top_k: 10,
        stance_mode: StanceMode::Balanced,
    };

    for _ in 0..warmup {
        let _ = store.retrieve(&req);
    }

    let mut latencies = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let start = Instant::now();
        let _ = store.retrieve(&req);
        latencies.push(start.elapsed().as_micros() as u64);
    }

    Ok(vec![
        BenchResult::from_latencies("retrieve_throughput_lexical", iterations, latencies)
            .with_extra("fixture_size", RETRIEVE_FIXTURE_SIZE.to_string())
            .with_extra("expected_candidates", QUERY_MATCH_BUCKET.to_string())
            .with_extra("query_vector", "none")
            .with_extra("top_k", "10")
            .with_extra("mode", "lexical"),
    ])
}

fn scenario_retrieve_throughput_semantic(
    iterations: usize,
    warmup: usize,
) -> Result<Vec<BenchResult>, String> {
    let mut store = InMemoryStore::new();
    for i in 0..RETRIEVE_FIXTURE_SIZE {
        let id = format!("claim-rs-{i}");
        let text = if i < QUERY_MATCH_BUCKET {
            format!("alice operational update phase {i}")
        } else {
            format!("Unrelated operational update {i}")
        };
        let claim = make_claim(&id, SEM_TENANT, &text, 0.9);
        let evidence = make_evidence(&format!("evd-rs-{i}"), &id);
        store
            .ingest_bundle(claim, vec![evidence], vec![])
            .map_err(|e| format!("fixture ingest at {i}: {e:?}"))?;
        store
            .upsert_claim_vector(&id, fixture_vector(i, SEMANTIC_DIM))
            .map_err(|e| format!("vector upsert at {i}: {e:?}"))?;
    }

    let query_vector = fixture_vector(0, SEMANTIC_DIM);
    let req = RetrievalRequest {
        tenant_id: SEM_TENANT.to_string(),
        query: "alice operation status".to_string(),
        top_k: SEMANTIC_TOP_K,
        stance_mode: StanceMode::Balanced,
    };

    for _ in 0..warmup {
        let _ = store.retrieve_semantic(&req, &query_vector);
    }

    let mut latencies = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let start = Instant::now();
        let _ = store.retrieve_semantic(&req, &query_vector);
        latencies.push(start.elapsed().as_micros() as u64);
    }

    Ok(vec![
        BenchResult::from_latencies("retrieve_throughput_semantic", iterations, latencies)
            .with_extra("fixture_size", RETRIEVE_FIXTURE_SIZE.to_string())
            .with_extra("expected_candidates", QUERY_MATCH_BUCKET.to_string())
            .with_extra("vector_dim", SEMANTIC_DIM.to_string())
            .with_extra("top_k", SEMANTIC_TOP_K.to_string())
            .with_extra("mode", "semantic"),
    ])
}

fn scenario_ann_search_throughput_at_scale(
    iterations: usize,
    warmup: usize,
) -> Result<Vec<BenchResult>, String> {
    eprintln!(
        "  (pre-loading {ANN_VECTORS} vectors at {ANN_DIM} dim — this is the O(N^2) ANN graph build and may take ~30s)"
    );
    let mut store = InMemoryStore::new();
    for i in 0..ANN_VECTORS {
        let id = format!("claim-ann-{i}");
        let claim = make_claim(&id, ANN_TENANT, &format!("ann fixture text {i}"), 0.9);
        store
            .ingest_bundle(claim, vec![], vec![])
            .map_err(|e| format!("ann fixture ingest at {i}: {e:?}"))?;
        store
            .upsert_claim_vector(&id, fixture_vector(i + 17, ANN_DIM))
            .map_err(|e| format!("ann vector upsert at {i}: {e:?}"))?;
    }

    let warmup_queries: Vec<Vec<f32>> = (0..warmup)
        .map(|i| fixture_vector(2_000_000 + i, ANN_DIM))
        .collect();
    for q in &warmup_queries {
        let _ = store.ann_vector_top_candidates(ANN_TENANT, q, ANN_TOP_N);
    }

    let queries: Vec<Vec<f32>> = (0..iterations)
        .map(|i| fixture_vector(3_000_000 + i, ANN_DIM))
        .collect();
    let mut latencies = Vec::with_capacity(iterations);
    for q in &queries {
        let start = Instant::now();
        let _ = store.ann_vector_top_candidates(ANN_TENANT, q, ANN_TOP_N);
        latencies.push(start.elapsed().as_micros() as u64);
    }

    Ok(vec![
        BenchResult::from_latencies("ann_search_throughput_at_scale", iterations, latencies)
            .with_extra("fixture_size", ANN_VECTORS.to_string())
            .with_extra("vector_dim", ANN_DIM.to_string())
            .with_extra("top_n", ANN_TOP_N.to_string())
            .with_extra("note", "ANN graph build is O(N^2); scale reduced from 100k spec for build feasibility"),
    ])
}

fn scenario_wal_replay_throughput(
    iterations: usize,
    warmup: usize,
) -> Result<Vec<BenchResult>, String> {
    let tmp = temp_dir("wal-replay");
    fs::create_dir_all(&tmp).map_err(|e| format!("mkdir temp: {e}"))?;
    let wal_path = tmp.join("bench.wal");

    // Setup: ingest WAL_REPLAY_FIXTURE_CLAIMS claims into a FileWal with
    // sync_every_records=1 (the default). Then drop everything.
    {
        let mut wal = FileWal::open(&wal_path).map_err(|e| format!("open wal: {e:?}"))?;
        let mut store = InMemoryStore::new();
        for i in 0..WAL_REPLAY_FIXTURE_CLAIMS {
            let id = format!("claim-wr-{i}");
            let claim = make_claim(&id, WAL_TENANT, &format!("wal replay claim {i}"), 0.9);
            let evidence = make_evidence(&format!("evd-wr-{i}"), &id);
            store
                .ingest_bundle_persistent(&mut wal, claim, vec![evidence], vec![])
                .map_err(|e| format!("wal fixture ingest at {i}: {e:?}"))?;
        }
        // Drop store and wal so the file is closed and the OS page cache
        // doesn't pre-warm the replay read path.
    }

    // Warmup replays.
    for _ in 0..warmup {
        let wal = FileWal::open(&wal_path).map_err(|e| format!("open wal: {e:?}"))?;
        let _ = InMemoryStore::load_from_wal_with_stats_and_ann_tuning(
            &wal,
            AnnTuningConfig::default(),
        )
        .map_err(|e| format!("warmup replay: {e:?}"))?;
    }

    // Measured replays.
    let mut latencies = Vec::with_capacity(iterations);
    let mut last_record_count: usize = 0;
    for _ in 0..iterations {
        let wal = FileWal::open(&wal_path).map_err(|e| format!("open wal: {e:?}"))?;
        let start = Instant::now();
        let (_, stats) = InMemoryStore::load_from_wal_with_stats_and_ann_tuning(
            &wal,
            AnnTuningConfig::default(),
        )
        .map_err(|e| format!("replay: {e:?}"))?;
        latencies.push(start.elapsed().as_micros() as u64);
        last_record_count = stats.replay.wal_records + stats.replay.snapshot_records;
    }

    let _ = fs::remove_dir_all(&tmp);

    Ok(vec![
        BenchResult::from_latencies("wal_replay_throughput", iterations, latencies)
            .with_extra("wal_claims_seeded", WAL_REPLAY_FIXTURE_CLAIMS.to_string())
            .with_extra("wal_records_replayed", last_record_count.to_string())
            .with_extra("wal_sync_every_records", "1"),
    ])
}

// ---------------------------------------------------------------------------
// Scenario dispatch
// ---------------------------------------------------------------------------

fn run_scenario(
    scenario: &str,
    iterations: usize,
    warmup: usize,
) -> Result<Vec<BenchResult>, String> {
    match scenario {
        SCENARIO_INGEST => scenario_ingest_throughput_sequential(iterations, warmup),
        SCENARIO_LEX => scenario_retrieve_throughput_lexical(iterations, warmup),
        SCENARIO_SEM => scenario_retrieve_throughput_semantic(iterations, warmup),
        SCENARIO_ANN => scenario_ann_search_throughput_at_scale(iterations, warmup),
        SCENARIO_WAL => scenario_wal_replay_throughput(iterations, warmup),
        other => Err(format!("Unknown scenario '{other}'")),
    }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn main() {
    let args = match parse_args(env::args().skip(1)) {
        Ok(args) => args,
        Err(message) => {
            eprintln!("{message}");
            process::exit(2);
        }
    };

    if args.help {
        println!("{}", usage_text());
        return;
    }

    let selected: Vec<String> = if let Some(scenario) = args.scenario.as_deref() {
        if !ALL_SCENARIOS.contains(&scenario) {
            eprintln!(
                "Unknown scenario '{scenario}'. Valid: {:?}",
                ALL_SCENARIOS
            );
            process::exit(2);
        }
        vec![scenario.to_string()]
    } else {
        ALL_SCENARIOS.iter().map(|s| s.to_string()).collect()
    };

    let warmup = args.warmup.unwrap_or(DEFAULT_WARMUP);

    let mut all_results: Vec<BenchResult> = Vec::new();
    for scenario in &selected {
        let iterations = args
            .iterations
            .unwrap_or_else(|| default_iterations_for(scenario));
        eprintln!("=== {scenario} (iterations={iterations}, warmup={warmup}) ===");
        match run_scenario(scenario, iterations, warmup) {
            Ok(results) => {
                for r in &results {
                    print!("{r}");
                }
                all_results.extend(results);
            }
            Err(err) => {
                eprintln!("  scenario '{scenario}' failed: {err}");
                process::exit(1);
            }
        }
    }

    // Machine-parseable single-line JSON summary, prefixed for easy grep.
    let json_payload: Vec<BenchResultJson> =
        all_results.iter().map(BenchResultJson::from).collect();
    let json = serde_json::to_string(&json_payload).unwrap_or_else(|_| "[]".to_string());
    println!("BENCH_JSON: {json}");
}
