use std::{
    collections::{HashSet, VecDeque},
    ffi::{OsStr, OsString},
    fs::{OpenOptions, create_dir_all},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use indexer::{Segment, Tier, persist_segments_atomic};
use ranking::lexical_overlap_score;
use retrieval::api::{
    RetrieveApiRequest, execute_api_query, reset_segment_prefilter_cache_metrics,
    segment_prefilter_cache_metrics_snapshot,
};
use schema::{Claim, Evidence, RetrievalRequest, Stance, StanceMode};
use store::{AnnTuningConfig, FileWal, InMemoryStore, StoreIndexStats, WalCheckpointStats};

const CONTRADICTION_DETECTION_F1_GATE: f64 = 0.80;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BenchmarkProfile {
    Smoke,
    Standard,
    Large,
    XLarge,
    Hybrid,
}

impl BenchmarkProfile {
    fn from_arg(raw: &str) -> Option<Self> {
        match raw {
            "smoke" => Some(Self::Smoke),
            "standard" | "default" => Some(Self::Standard),
            "large" => Some(Self::Large),
            "xlarge" => Some(Self::XLarge),
            "hybrid" => Some(Self::Hybrid),
            _ => None,
        }
    }

    fn fixture_size(self) -> usize {
        match self {
            Self::Smoke => 2_000,
            Self::Standard => 10_000,
            Self::Large => 50_000,
            Self::XLarge => 100_000,
            Self::Hybrid => 20_000,
        }
    }

    fn default_iterations(self) -> usize {
        match self {
            Self::Smoke => 100,
            Self::Standard => 300,
            Self::Large => 120,
            Self::XLarge => 80,
            Self::Hybrid => 180,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Smoke => "smoke",
            Self::Standard => "standard",
            Self::Large => "large",
            Self::XLarge => "xlarge",
            Self::Hybrid => "hybrid",
        }
    }
}

#[derive(Debug, Clone)]
struct BenchmarkConfig {
    profile: BenchmarkProfile,
    iterations: Option<usize>,
    history_out: Option<String>,
    history_csv_out: Option<String>,
    guard_history: Option<String>,
    max_dash_latency_regression_pct: Option<f64>,
    scorecard_out: Option<String>,
    ann_tuning: AnnTuningConfig,
    large_min_candidate_reduction_pct: f64,
    large_max_dash_latency_ms: f64,
    xlarge_min_candidate_reduction_pct: f64,
    xlarge_max_dash_latency_ms: f64,
    min_segment_refresh_successes: usize,
    min_segment_cache_hits: usize,
}

#[derive(Debug, Clone)]
struct BenchmarkSummary {
    run_epoch_secs: u64,
    profile: BenchmarkProfile,
    fixture_size: usize,
    iterations: usize,
    baseline_top: Option<String>,
    eme_top: Option<String>,
    baseline_hit: bool,
    eme_hit: bool,
    baseline_latency: f64,
    eme_latency: f64,
    baseline_scan_count: usize,
    dash_candidate_count: usize,
    metadata_prefilter_count: usize,
    ann_candidate_count: usize,
    final_scored_candidate_count: usize,
    ann_recall: AnnRecallSummary,
    index_stats: StoreIndexStats,
    ann_tuning: AnnTuningConfig,
    segment_cache_probe: SegmentCacheProbeSummary,
    wal_scale: Option<WalScaleSummary>,
}

#[derive(Debug, Clone)]
struct HistoryRow {
    profile: BenchmarkProfile,
    eme_avg_ms: f64,
}

#[derive(Debug, Clone)]
struct QualityProbeSummary {
    contradiction_support_only_pass: bool,
    contradiction_detection_f1: f64,
    contradiction_detection_f1_pass: bool,
    temporal_window_pass: bool,
    temporal_unknown_excluded_pass: bool,
    hybrid_filter_with_embedding_pass: bool,
}

#[derive(Debug, Clone, Default)]
struct SegmentCacheProbeSummary {
    cache_hits: u64,
    refresh_attempts: u64,
    refresh_successes: u64,
    refresh_failures: u64,
    refresh_load_micros: u64,
}

#[derive(Debug, Clone)]
struct WalScaleSummary {
    claims_seeded: usize,
    checkpoint_stats: WalCheckpointStats,
    checkpoint_ms: f64,
    replay_ms: f64,
    replay_snapshot_records: usize,
    replay_wal_records: usize,
    replay_validation_hit: bool,
    replay_validation_top_claim: Option<String>,
}

#[derive(Debug, Clone, Default)]
struct AnnRecallSummary {
    recall_at_10: f64,
    recall_at_100: f64,
    curve: Vec<AnnRecallPoint>,
}

#[derive(Debug, Clone)]
struct AnnRecallPoint {
    budget: usize,
    recall: f64,
}

impl QualityProbeSummary {
    fn passed_count(&self) -> usize {
        [
            self.contradiction_support_only_pass,
            self.contradiction_detection_f1_pass,
            self.temporal_window_pass,
            self.temporal_unknown_excluded_pass,
            self.hybrid_filter_with_embedding_pass,
        ]
        .into_iter()
        .filter(|v| *v)
        .count()
    }

    fn total_count(&self) -> usize {
        5
    }

    fn all_passed(&self) -> bool {
        self.passed_count() == self.total_count()
    }
}

fn main() {
    let config = match parse_args(std::env::args().skip(1)) {
        Ok(config) => config,
        Err(message) => {
            eprintln!("{message}");
            std::process::exit(2);
        }
    };

    let fixture_size = config.profile.fixture_size();
    let iterations = config
        .iterations
        .unwrap_or_else(|| config.profile.default_iterations());
    let (tenant, query, expected_top) = if config.profile == BenchmarkProfile::Hybrid {
        (
            "tenant-benchmark-hybrid",
            "Did project helios acquire startup nova in 2026?",
            "claim-hybrid-target",
        )
    } else {
        (
            "tenant-benchmark",
            "Did company x acquire company y in 2025?",
            "claim-target",
        )
    };
    let hybrid_entity_filters = vec!["project helios".to_string()];
    let hybrid_embedding_filters = vec!["emb://hybrid-target".to_string()];
    let hybrid_query_embedding = benchmark_query_embedding();

    let mut store = InMemoryStore::new_with_ann_tuning(config.ann_tuning.clone());
    if config.profile == BenchmarkProfile::Hybrid {
        seed_hybrid_fixture(&mut store, tenant, fixture_size);
    } else {
        seed_fixture(&mut store, tenant, fixture_size);
    }
    let empty_filters: Vec<String> = Vec::new();
    let (probe_entity_filters, probe_embedding_filters) =
        if config.profile == BenchmarkProfile::Hybrid {
            (&hybrid_entity_filters, &hybrid_embedding_filters)
        } else {
            (&empty_filters, &empty_filters)
        };
    let segment_cache_probe = match run_segment_cache_probe(
        &store,
        tenant,
        query,
        &hybrid_query_embedding,
        probe_entity_filters,
        probe_embedding_filters,
    ) {
        Ok(summary) => summary,
        Err(err) => {
            eprintln!("Benchmark failed: unable to run segment cache probe ({err}).");
            std::process::exit(1);
        }
    };
    let wal_scale = match maybe_run_wal_scale_slice(config.profile, fixture_size) {
        Ok(summary) => summary,
        Err(err) => {
            eprintln!("Benchmark failed: unable to run WAL scale slice ({err}).");
            std::process::exit(1);
        }
    };

    let baseline_top = if config.profile == BenchmarkProfile::Hybrid {
        baseline_hybrid_retrieve_top1(
            &store,
            tenant,
            query,
            &hybrid_entity_filters,
            &hybrid_embedding_filters,
        )
    } else {
        baseline_retrieve_top1(&store, tenant, query)
    };
    let baseline_hit = baseline_top.as_deref() == Some(expected_top);

    let eme_top = if config.profile == BenchmarkProfile::Hybrid {
        eme_hybrid_retrieve_top1(
            &store,
            tenant,
            query,
            &hybrid_query_embedding,
            &hybrid_entity_filters,
            &hybrid_embedding_filters,
        )
    } else {
        eme_retrieve_top1(&store, tenant, query)
    };
    let eme_hit = eme_top.as_deref() == Some(expected_top);

    let baseline_scan_count = store.claims_for_tenant(tenant).len();
    let diagnostics_req = RetrievalRequest {
        tenant_id: tenant.to_string(),
        query: query.to_string(),
        top_k: 1,
        stance_mode: StanceMode::Balanced,
    };
    let metadata_prefilter_claim_ids = if config.profile == BenchmarkProfile::Hybrid {
        build_metadata_prefilter_claim_ids(
            &store,
            tenant,
            &hybrid_entity_filters,
            &hybrid_embedding_filters,
        )
    } else {
        None
    };
    let metadata_prefilter_count = metadata_prefilter_claim_ids
        .as_ref()
        .map(|ids| ids.len())
        .unwrap_or(0);
    let ann_candidate_count =
        store.ann_candidate_count_for_query_vector(tenant, &hybrid_query_embedding, 1);
    let final_scored_candidate_count = store
        .candidate_count_with_query_vector_and_allowed_claim_ids(
            &diagnostics_req,
            Some(&hybrid_query_embedding),
            (None, None),
            metadata_prefilter_claim_ids.as_ref(),
        );
    let dash_candidate_count = final_scored_candidate_count;
    let ann_recall = measure_ann_recall(&store, tenant, &hybrid_query_embedding);
    let index_stats = store.index_stats();

    let baseline_latency = if config.profile == BenchmarkProfile::Hybrid {
        measure_baseline_hybrid_latency_ms(
            &store,
            tenant,
            query,
            iterations,
            &hybrid_entity_filters,
            &hybrid_embedding_filters,
        )
    } else {
        measure_baseline_latency_ms(&store, tenant, query, iterations)
    };
    let eme_latency = if config.profile == BenchmarkProfile::Hybrid {
        measure_eme_hybrid_latency_ms(
            &store,
            tenant,
            query,
            iterations,
            &hybrid_query_embedding,
            &hybrid_entity_filters,
            &hybrid_embedding_filters,
        )
    } else {
        measure_eme_latency_ms(&store, tenant, query, iterations)
    };

    let summary = BenchmarkSummary {
        run_epoch_secs: now_epoch_secs(),
        profile: config.profile,
        fixture_size,
        iterations,
        baseline_top,
        eme_top,
        baseline_hit,
        eme_hit,
        baseline_latency,
        eme_latency,
        baseline_scan_count,
        dash_candidate_count,
        metadata_prefilter_count,
        ann_candidate_count,
        final_scored_candidate_count,
        ann_recall,
        index_stats,
        ann_tuning: config.ann_tuning.clone(),
        segment_cache_probe,
        wal_scale,
    };
    let quality = run_quality_probes();

    print_summary(&summary);
    print_quality_summary(&quality);
    print_ann_tuning(&summary.ann_tuning);

    if let Some(history_path) = config.guard_history.as_deref() {
        let max_regression_pct = config.max_dash_latency_regression_pct.unwrap_or(20.0);
        if let Err(err) = enforce_history_guard(
            history_path,
            summary.profile,
            summary.eme_latency,
            max_regression_pct,
        ) {
            eprintln!("Benchmark failed: {err}");
            std::process::exit(1);
        }
    }

    if let Some(path) = config.history_out.as_deref() {
        if let Err(err) = append_history(path, &summary) {
            eprintln!("Benchmark failed: unable to write history output ({err}).");
            std::process::exit(1);
        }
        println!("Benchmark history output updated: {path}");
    }
    if let Some(path) = config.history_csv_out.as_deref() {
        if let Err(err) = append_history_csv(path, &summary) {
            eprintln!("Benchmark failed: unable to write history CSV output ({err}).");
            std::process::exit(1);
        }
        println!("Benchmark history CSV output updated: {path}");
    }
    if let Some(path) = config.scorecard_out.as_deref() {
        if let Err(err) = write_scorecard(path, &summary, &quality) {
            eprintln!("Benchmark failed: unable to write scorecard output ({err}).");
            std::process::exit(1);
        }
        println!("Benchmark scorecard output updated: {path}");
    }

    if !summary.eme_hit {
        eprintln!("Benchmark failed: DASH retrieval missed expected top1 result.");
        std::process::exit(1);
    }
    if summary.fixture_size >= 10_000 && summary.dash_candidate_count >= summary.baseline_scan_count
    {
        eprintln!(
            "Benchmark failed: indexed retrieval did not reduce candidate set (baseline_scan_count={}, dash_candidate_count={}).",
            summary.baseline_scan_count, summary.dash_candidate_count
        );
        std::process::exit(1);
    }
    if summary.baseline_hit && summary.eme_latency > summary.baseline_latency * 15.0 {
        eprintln!("Benchmark failed: DASH latency regression too high (>15x baseline).");
        std::process::exit(1);
    }
    if let Err(err) = evaluate_profile_gates(&summary, &config) {
        eprintln!("Benchmark failed: {err}");
        std::process::exit(1);
    }
    if !quality.all_passed() {
        eprintln!("Benchmark failed: quality probes did not pass.");
        std::process::exit(1);
    }
}

fn evaluate_profile_gates(
    summary: &BenchmarkSummary,
    config: &BenchmarkConfig,
) -> Result<(), String> {
    let reduction_pct = if summary.baseline_scan_count == 0 {
        0.0
    } else {
        100.0 * (1.0 - summary.dash_candidate_count as f64 / summary.baseline_scan_count as f64)
    }
    .max(0.0);

    if summary.profile == BenchmarkProfile::Large
        && reduction_pct < config.large_min_candidate_reduction_pct
    {
        return Err(format!(
            "large profile candidate reduction {:.2}% is below gate {:.2}%",
            reduction_pct, config.large_min_candidate_reduction_pct
        ));
    }
    if summary.profile == BenchmarkProfile::Large
        && summary.eme_latency > config.large_max_dash_latency_ms
    {
        return Err(format!(
            "large profile DASH avg latency {:.4} ms exceeds gate {:.4} ms",
            summary.eme_latency, config.large_max_dash_latency_ms
        ));
    }
    if summary.profile == BenchmarkProfile::XLarge
        && reduction_pct < config.xlarge_min_candidate_reduction_pct
    {
        return Err(format!(
            "xlarge profile candidate reduction {:.2}% is below gate {:.2}%",
            reduction_pct, config.xlarge_min_candidate_reduction_pct
        ));
    }
    if summary.profile == BenchmarkProfile::XLarge
        && summary.eme_latency > config.xlarge_max_dash_latency_ms
    {
        return Err(format!(
            "xlarge profile DASH avg latency {:.4} ms exceeds gate {:.4} ms",
            summary.eme_latency, config.xlarge_max_dash_latency_ms
        ));
    }
    if summary.segment_cache_probe.refresh_successes < config.min_segment_refresh_successes as u64 {
        return Err(format!(
            "segment cache refresh successes {} is below gate {}",
            summary.segment_cache_probe.refresh_successes, config.min_segment_refresh_successes
        ));
    }
    if summary.segment_cache_probe.cache_hits < config.min_segment_cache_hits as u64 {
        return Err(format!(
            "segment cache hits {} is below gate {}",
            summary.segment_cache_probe.cache_hits, config.min_segment_cache_hits
        ));
    }
    Ok(())
}

fn build_metadata_prefilter_claim_ids(
    store: &InMemoryStore,
    tenant_id: &str,
    entity_filters: &[String],
    embedding_filters: &[String],
) -> Option<std::collections::HashSet<String>> {
    let entity_candidates = if entity_filters.is_empty() {
        None
    } else {
        let mut ids = std::collections::HashSet::new();
        for filter in entity_filters {
            ids.extend(store.claim_ids_for_entity(tenant_id, filter));
        }
        Some(ids)
    };
    let embedding_candidates = if embedding_filters.is_empty() {
        None
    } else {
        let mut ids = std::collections::HashSet::new();
        for filter in embedding_filters {
            ids.extend(store.claim_ids_for_embedding_id(tenant_id, filter));
        }
        Some(ids)
    };

    match (entity_candidates, embedding_candidates) {
        (None, None) => None,
        (Some(entity), None) => Some(entity),
        (None, Some(embedding)) => Some(embedding),
        (Some(entity), Some(embedding)) => Some(entity.intersection(&embedding).cloned().collect()),
    }
}

fn parse_args<I>(args: I) -> Result<BenchmarkConfig, String>
where
    I: Iterator<Item = String>,
{
    let mut profile = BenchmarkProfile::Standard;
    let mut iterations = None;
    let mut history_out = None;
    let mut history_csv_out = std::env::var("DASH_BENCH_HISTORY_CSV_OUT").ok();
    let mut guard_history = None;
    let mut max_dash_latency_regression_pct = None;
    let mut scorecard_out = None;
    let defaults = AnnTuningConfig::default();
    let mut ann_tuning = AnnTuningConfig {
        max_neighbors_base: env_or_default_usize(
            "DASH_BENCH_ANN_MAX_NEIGHBORS_BASE",
            defaults.max_neighbors_base,
        ),
        max_neighbors_upper: env_or_default_usize(
            "DASH_BENCH_ANN_MAX_NEIGHBORS_UPPER",
            defaults.max_neighbors_upper,
        ),
        search_expansion_factor: env_or_default_usize(
            "DASH_BENCH_ANN_SEARCH_EXPANSION_FACTOR",
            defaults.search_expansion_factor,
        ),
        search_expansion_min: env_or_default_usize(
            "DASH_BENCH_ANN_SEARCH_EXPANSION_MIN",
            defaults.search_expansion_min,
        ),
        search_expansion_max: env_or_default_usize(
            "DASH_BENCH_ANN_SEARCH_EXPANSION_MAX",
            defaults.search_expansion_max,
        ),
    };
    let mut large_min_candidate_reduction_pct =
        env_or_default_f64("DASH_BENCH_LARGE_MIN_CANDIDATE_REDUCTION_PCT", 95.0);
    let mut large_max_dash_latency_ms =
        env_or_default_f64("DASH_BENCH_LARGE_MAX_DASH_LATENCY_MS", 120.0);
    let mut xlarge_min_candidate_reduction_pct =
        env_or_default_f64("DASH_BENCH_XLARGE_MIN_CANDIDATE_REDUCTION_PCT", 96.0);
    let mut xlarge_max_dash_latency_ms =
        env_or_default_f64("DASH_BENCH_XLARGE_MAX_DASH_LATENCY_MS", 250.0);
    let mut min_segment_refresh_successes =
        env_or_default_usize("DASH_BENCH_MIN_SEGMENT_REFRESH_SUCCESSES", 0);
    let mut min_segment_cache_hits = env_or_default_usize("DASH_BENCH_MIN_SEGMENT_CACHE_HITS", 0);

    let mut args = args.peekable();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--smoke" => profile = BenchmarkProfile::Smoke,
            "--profile" => {
                let value = args
                    .next()
                    .ok_or_else(|| "Missing value for --profile".to_string())?;
                profile = BenchmarkProfile::from_arg(&value).ok_or_else(|| {
                    format!(
                        "Invalid profile '{value}'. Valid values: smoke, standard, large, xlarge, hybrid."
                    )
                })?;
            }
            "--iterations" => {
                let value = args
                    .next()
                    .ok_or_else(|| "Missing value for --iterations".to_string())?;
                let parsed = value
                    .parse::<usize>()
                    .map_err(|_| format!("Invalid iterations value '{value}'"))?;
                if parsed == 0 {
                    return Err("Iterations must be > 0".to_string());
                }
                iterations = Some(parsed);
            }
            "--history-out" => {
                let value = args
                    .next()
                    .ok_or_else(|| "Missing value for --history-out".to_string())?;
                history_out = Some(value);
            }
            "--history-csv-out" => {
                let value = args
                    .next()
                    .ok_or_else(|| "Missing value for --history-csv-out".to_string())?;
                history_csv_out = Some(value);
            }
            "--guard-history" => {
                let value = args
                    .next()
                    .ok_or_else(|| "Missing value for --guard-history".to_string())?;
                guard_history = Some(value);
            }
            "--max-dash-latency-regression-pct" | "--max-eme-latency-regression-pct" => {
                let value = args.next().ok_or_else(|| {
                    "Missing value for --max-dash-latency-regression-pct".to_string()
                })?;
                let parsed = value.parse::<f64>().map_err(|_| {
                    format!("Invalid max-dash-latency-regression-pct value '{value}'")
                })?;
                if parsed < 0.0 {
                    return Err("max-dash-latency-regression-pct must be >= 0".to_string());
                }
                max_dash_latency_regression_pct = Some(parsed);
            }
            "--scorecard-out" => {
                let value = args
                    .next()
                    .ok_or_else(|| "Missing value for --scorecard-out".to_string())?;
                scorecard_out = Some(value);
            }
            "--ann-max-neighbors-base" => {
                ann_tuning.max_neighbors_base =
                    parse_positive_usize_arg(args.next(), "--ann-max-neighbors-base")?;
            }
            "--ann-max-neighbors-upper" => {
                ann_tuning.max_neighbors_upper =
                    parse_positive_usize_arg(args.next(), "--ann-max-neighbors-upper")?;
            }
            "--ann-search-expansion-factor" => {
                ann_tuning.search_expansion_factor =
                    parse_positive_usize_arg(args.next(), "--ann-search-expansion-factor")?;
            }
            "--ann-search-expansion-min" => {
                ann_tuning.search_expansion_min =
                    parse_positive_usize_arg(args.next(), "--ann-search-expansion-min")?;
            }
            "--ann-search-expansion-max" => {
                ann_tuning.search_expansion_max =
                    parse_positive_usize_arg(args.next(), "--ann-search-expansion-max")?;
            }
            "--large-min-candidate-reduction-pct" => {
                large_min_candidate_reduction_pct =
                    parse_non_negative_f64_arg(args.next(), "--large-min-candidate-reduction-pct")?;
            }
            "--large-max-dash-latency-ms" => {
                large_max_dash_latency_ms =
                    parse_non_negative_f64_arg(args.next(), "--large-max-dash-latency-ms")?;
            }
            "--xlarge-min-candidate-reduction-pct" => {
                xlarge_min_candidate_reduction_pct = parse_non_negative_f64_arg(
                    args.next(),
                    "--xlarge-min-candidate-reduction-pct",
                )?;
            }
            "--xlarge-max-dash-latency-ms" => {
                xlarge_max_dash_latency_ms =
                    parse_non_negative_f64_arg(args.next(), "--xlarge-max-dash-latency-ms")?;
            }
            "--min-segment-refresh-successes" => {
                min_segment_refresh_successes =
                    parse_non_negative_usize_arg(args.next(), "--min-segment-refresh-successes")?;
            }
            "--min-segment-cache-hits" => {
                min_segment_cache_hits =
                    parse_non_negative_usize_arg(args.next(), "--min-segment-cache-hits")?;
            }
            "--help" | "-h" => return Err(usage_text().to_string()),
            _ => {
                return Err(format!("Unknown argument '{arg}'.\n\n{}", usage_text()));
            }
        }
    }

    if ann_tuning.search_expansion_max < ann_tuning.search_expansion_min {
        return Err("--ann-search-expansion-max must be >= --ann-search-expansion-min".to_string());
    }

    Ok(BenchmarkConfig {
        profile,
        iterations,
        history_out,
        history_csv_out,
        guard_history,
        max_dash_latency_regression_pct,
        scorecard_out,
        ann_tuning,
        large_min_candidate_reduction_pct,
        large_max_dash_latency_ms,
        xlarge_min_candidate_reduction_pct,
        xlarge_max_dash_latency_ms,
        min_segment_refresh_successes,
        min_segment_cache_hits,
    })
}

fn env_or_default_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn env_or_default_f64(key: &str, default: f64) -> f64 {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse::<f64>().ok())
        .filter(|value| *value >= 0.0)
        .unwrap_or(default)
}

fn parse_positive_usize_arg(value: Option<String>, flag: &str) -> Result<usize, String> {
    let raw = value.ok_or_else(|| format!("Missing value for {flag}"))?;
    let parsed = raw
        .parse::<usize>()
        .map_err(|_| format!("Invalid value '{raw}' for {flag}"))?;
    if parsed == 0 {
        return Err(format!("{flag} must be > 0"));
    }
    Ok(parsed)
}

fn parse_non_negative_f64_arg(value: Option<String>, flag: &str) -> Result<f64, String> {
    let raw = value.ok_or_else(|| format!("Missing value for {flag}"))?;
    let parsed = raw
        .parse::<f64>()
        .map_err(|_| format!("Invalid value '{raw}' for {flag}"))?;
    if parsed < 0.0 {
        return Err(format!("{flag} must be >= 0"));
    }
    Ok(parsed)
}

fn parse_non_negative_usize_arg(value: Option<String>, flag: &str) -> Result<usize, String> {
    let raw = value.ok_or_else(|| format!("Missing value for {flag}"))?;
    raw.parse::<usize>()
        .map_err(|_| format!("Invalid value '{raw}' for {flag}"))
}

fn usage_text() -> &'static str {
    "Usage: cargo run -p benchmark-smoke --bin benchmark-smoke -- [--smoke] [--profile smoke|standard|large|xlarge|hybrid] [--iterations N] [--history-out PATH] [--history-csv-out PATH] [--guard-history PATH] [--max-dash-latency-regression-pct N] [--scorecard-out PATH] [--ann-max-neighbors-base N] [--ann-max-neighbors-upper N] [--ann-search-expansion-factor N] [--ann-search-expansion-min N] [--ann-search-expansion-max N] [--large-min-candidate-reduction-pct N] [--large-max-dash-latency-ms N] [--xlarge-min-candidate-reduction-pct N] [--xlarge-max-dash-latency-ms N] [--min-segment-refresh-successes N] [--min-segment-cache-hits N] [quality probe enforces contradiction_detection_f1 >= 0.80]"
}

#[allow(unused_unsafe)]
fn set_env_var(key: &str, value: &OsStr) {
    unsafe {
        std::env::set_var(key, value);
    }
}

#[allow(unused_unsafe)]
fn restore_env_var(key: &str, value: Option<&OsStr>) {
    match value {
        Some(value) => unsafe {
            std::env::set_var(key, value);
        },
        None => unsafe {
            std::env::remove_var(key);
        },
    }
}

struct EnvVarGuard {
    key: &'static str,
    previous: Option<OsString>,
}

impl EnvVarGuard {
    fn set(key: &'static str, value: &OsStr) -> Self {
        let previous = std::env::var_os(key);
        set_env_var(key, value);
        Self { key, previous }
    }
}

impl Drop for EnvVarGuard {
    fn drop(&mut self) {
        restore_env_var(self.key, self.previous.as_deref());
    }
}

fn temp_dir_for(tag: &str) -> PathBuf {
    let mut out = std::env::temp_dir();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be monotonic")
        .as_nanos();
    out.push(format!(
        "dash-bench-{}-{}-{}",
        tag,
        std::process::id(),
        nanos
    ));
    out
}

fn run_segment_cache_probe(
    store: &InMemoryStore,
    tenant: &str,
    query: &str,
    query_embedding: &[f32],
    entity_filters: &[String],
    embedding_filters: &[String],
) -> Result<SegmentCacheProbeSummary, String> {
    let root = temp_dir_for("segment-cache");
    let tenant_segment_root = root.join(tenant);
    let claim_ids: Vec<String> = store
        .claims_for_tenant(tenant)
        .into_iter()
        .map(|claim| claim.claim_id)
        .collect();
    persist_segments_atomic(
        &tenant_segment_root,
        &[Segment {
            segment_id: "bench-segment-hot-0".to_string(),
            tier: Tier::Hot,
            claim_ids,
        }],
    )
    .map_err(|err| format!("segment manifest persist failed: {err:?}"))?;

    let _segment_dir_env = EnvVarGuard::set("DASH_RETRIEVAL_SEGMENT_DIR", root.as_os_str());
    let _segment_refresh_env = EnvVarGuard::set(
        "DASH_RETRIEVAL_SEGMENT_CACHE_REFRESH_MS",
        OsStr::new("600000"),
    );
    reset_segment_prefilter_cache_metrics();

    let request = RetrieveApiRequest {
        tenant_id: tenant.to_string(),
        query: query.to_string(),
        query_embedding: Some(query_embedding.to_vec()),
        entity_filters: entity_filters.to_vec(),
        embedding_id_filters: embedding_filters.to_vec(),
        top_k: 1,
        stance_mode: StanceMode::Balanced,
        return_graph: false,
        time_range: None,
    };
    let _ = execute_api_query(store, request.clone());
    let _ = execute_api_query(store, request);

    let metrics = segment_prefilter_cache_metrics_snapshot();
    let _ = std::fs::remove_dir_all(&root);
    Ok(SegmentCacheProbeSummary {
        cache_hits: metrics.cache_hits,
        refresh_attempts: metrics.refresh_attempts,
        refresh_successes: metrics.refresh_successes,
        refresh_failures: metrics.refresh_failures,
        refresh_load_micros: metrics.refresh_load_micros,
    })
}

fn maybe_run_wal_scale_slice(
    profile: BenchmarkProfile,
    fixture_size: usize,
) -> Result<Option<WalScaleSummary>, String> {
    if !matches!(profile, BenchmarkProfile::Large | BenchmarkProfile::XLarge) {
        return Ok(None);
    }

    let root = temp_dir_for("wal-scale");
    let wal_path = root.join("bench.wal");
    let mut wal = FileWal::open(&wal_path).map_err(|err| format!("open WAL failed: {err:?}"))?;
    let mut store = InMemoryStore::new();
    let default_claims = match profile {
        BenchmarkProfile::Large => 10_000,
        BenchmarkProfile::XLarge => 20_000,
        _ => 5_000,
    };
    let claims_seeded = env_or_default_usize("DASH_BENCH_WAL_SCALE_CLAIMS", default_claims)
        .min(fixture_size)
        .max(1);

    seed_fixture_persistent(
        &mut store,
        &mut wal,
        "tenant-benchmark-wal-scale",
        claims_seeded,
    )?;

    let checkpoint_start = Instant::now();
    let checkpoint_stats = store
        .checkpoint_and_compact(&mut wal)
        .map_err(|err| format!("checkpoint failed: {err:?}"))?;
    let checkpoint_ms = checkpoint_start.elapsed().as_secs_f64() * 1000.0;

    let delta_claim_id = "claim-wal-delta";
    store
        .ingest_bundle_persistent(
            &mut wal,
            Claim {
                claim_id: delta_claim_id.to_string(),
                tenant_id: "tenant-benchmark-wal-scale".to_string(),
                canonical_text: "post checkpoint replay delta".to_string(),
                confidence: 0.9,
                event_time_unix: Some(1_775_000_000),
                entities: vec![],
                embedding_ids: vec![],
                claim_type: None,
                valid_from: None,
                valid_to: None,
                created_at: None,
                updated_at: None,
            },
            vec![Evidence {
                evidence_id: "evidence-wal-delta".to_string(),
                claim_id: delta_claim_id.to_string(),
                source_id: "source://wal/delta".to_string(),
                stance: Stance::Supports,
                source_quality: 0.9,
                chunk_id: None,
                span_start: None,
                span_end: None,
                doc_id: None,
                extraction_model: None,
                ingested_at: None,
            }],
            vec![],
        )
        .map_err(|err| format!("checkpoint delta ingest failed: {err:?}"))?;
    store
        .upsert_claim_vector_persistent(&mut wal, delta_claim_id, benchmark_query_embedding())
        .map_err(|err| format!("checkpoint delta vector upsert failed: {err:?}"))?;

    let replay_start = Instant::now();
    let (replayed, load_stats) = InMemoryStore::load_from_wal_with_stats(&wal)
        .map_err(|err| format!("replay from WAL failed: {err:?}"))?;
    let replay_ms = replay_start.elapsed().as_secs_f64() * 1000.0;
    let replay_validation_top_claim = replayed
        .retrieve(&RetrievalRequest {
            tenant_id: "tenant-benchmark-wal-scale".to_string(),
            query: "post checkpoint replay delta".to_string(),
            top_k: 1,
            stance_mode: StanceMode::Balanced,
        })
        .first()
        .map(|result| result.claim_id.clone());
    let replay_validation_hit = replay_validation_top_claim.as_deref() == Some(delta_claim_id);

    let _ = std::fs::remove_dir_all(&root);
    Ok(Some(WalScaleSummary {
        claims_seeded,
        checkpoint_stats,
        checkpoint_ms,
        replay_ms,
        replay_snapshot_records: load_stats.replay.snapshot_records,
        replay_wal_records: load_stats.replay.wal_records,
        replay_validation_hit,
        replay_validation_top_claim,
    }))
}

fn now_epoch_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be valid")
        .as_secs()
}

fn measure_ann_recall(
    store: &InMemoryStore,
    tenant: &str,
    query_embedding: &[f32],
) -> AnnRecallSummary {
    let budgets = [10usize, 25, 50, 100, 200];
    let curve: Vec<AnnRecallPoint> = budgets
        .into_iter()
        .map(|budget| AnnRecallPoint {
            budget,
            recall: compute_ann_recall_at_budget(store, tenant, query_embedding, budget),
        })
        .collect();

    AnnRecallSummary {
        recall_at_10: compute_ann_recall_at_budget(store, tenant, query_embedding, 10),
        recall_at_100: compute_ann_recall_at_budget(store, tenant, query_embedding, 100),
        curve,
    }
}

fn compute_ann_recall_at_budget(
    store: &InMemoryStore,
    tenant: &str,
    query_embedding: &[f32],
    budget: usize,
) -> f64 {
    if budget == 0 {
        return 1.0;
    }

    let ann = store.ann_vector_top_candidates(tenant, query_embedding, budget);
    let exact = store.exact_vector_top_candidates(tenant, query_embedding, budget);
    if exact.is_empty() {
        return 1.0;
    }

    let exact_set: HashSet<&str> = exact.iter().map(String::as_str).collect();
    let hits = ann
        .iter()
        .filter(|claim_id| exact_set.contains(claim_id.as_str()))
        .count();
    hits as f64 / exact_set.len() as f64
}

fn format_ann_recall_curve(curve: &[AnnRecallPoint]) -> String {
    if curve.is_empty() {
        return "none".to_string();
    }
    curve
        .iter()
        .map(|point| format!("{}:{:.4}", point.budget, point.recall))
        .collect::<Vec<_>>()
        .join(";")
}

fn print_summary(summary: &BenchmarkSummary) {
    println!("Benchmark profile: {}", summary.profile.as_str());
    println!("Benchmark fixture size: {}", summary.fixture_size);
    println!("Iterations: {}", summary.iterations);
    println!(
        "Baseline top1: {}",
        summary.baseline_top.as_deref().unwrap_or("none")
    );
    println!(
        "DASH top1: {}",
        summary.eme_top.as_deref().unwrap_or("none")
    );
    println!("Baseline hit expected: {}", summary.baseline_hit);
    println!("DASH hit expected: {}", summary.eme_hit);
    println!("Baseline avg latency (ms): {:.4}", summary.baseline_latency);
    println!("DASH avg latency (ms): {:.4}", summary.eme_latency);
    println!("Baseline scan count: {}", summary.baseline_scan_count);
    println!(
        "Metadata prefilter candidate count: {}",
        summary.metadata_prefilter_count
    );
    println!("ANN candidate count: {}", summary.ann_candidate_count);
    println!(
        "Final scored candidate count: {}",
        summary.final_scored_candidate_count
    );
    println!("ANN recall@10: {:.4}", summary.ann_recall.recall_at_10);
    println!("ANN recall@100: {:.4}", summary.ann_recall.recall_at_100);
    println!(
        "ANN recall curve (budget:recall): {}",
        format_ann_recall_curve(&summary.ann_recall.curve)
    );
    println!("DASH candidate count: {}", summary.dash_candidate_count);
    let reduction_pct = if summary.baseline_scan_count == 0 {
        0.0
    } else {
        100.0 * (1.0 - summary.dash_candidate_count as f64 / summary.baseline_scan_count as f64)
    };
    println!("Candidate reduction (%): {:.2}", reduction_pct.max(0.0));
    println!(
        "Index stats: tenants={}, claims={}, vectors={}, inverted_terms={}, entity_terms={}, temporal_buckets={}, ann_vector_buckets={}",
        summary.index_stats.tenant_count,
        summary.index_stats.claim_count,
        summary.index_stats.vector_count,
        summary.index_stats.inverted_terms,
        summary.index_stats.entity_terms,
        summary.index_stats.temporal_buckets,
        summary.index_stats.ann_vector_buckets,
    );
    println!(
        "Segment cache probe: hits={}, refresh_attempts={}, refresh_successes={}, refresh_failures={}, refresh_load_micros={}",
        summary.segment_cache_probe.cache_hits,
        summary.segment_cache_probe.refresh_attempts,
        summary.segment_cache_probe.refresh_successes,
        summary.segment_cache_probe.refresh_failures,
        summary.segment_cache_probe.refresh_load_micros
    );
    if let Some(wal_scale) = summary.wal_scale.as_ref() {
        println!(
            "WAL scale slice: claims_seeded={}, checkpoint_ms={:.4}, replay_ms={:.4}, snapshot_records={}, truncated_wal_records={}, replay_snapshot_records={}, replay_wal_records={}, replay_validation_hit={}, replay_validation_top_claim={}",
            wal_scale.claims_seeded,
            wal_scale.checkpoint_ms,
            wal_scale.replay_ms,
            wal_scale.checkpoint_stats.snapshot_records,
            wal_scale.checkpoint_stats.truncated_wal_records,
            wal_scale.replay_snapshot_records,
            wal_scale.replay_wal_records,
            wal_scale.replay_validation_hit,
            wal_scale
                .replay_validation_top_claim
                .as_deref()
                .unwrap_or("none")
        );
    } else {
        println!("WAL scale slice: skipped");
    }
}

fn print_quality_summary(summary: &QualityProbeSummary) {
    println!(
        "Quality probes passed: {}/{}",
        summary.passed_count(),
        summary.total_count()
    );
    println!(
        "Quality probe contradiction_support_only: {}",
        summary.contradiction_support_only_pass
    );
    println!(
        "Quality probe contradiction_detection_f1: {:.4} (gate >= {:.2})",
        summary.contradiction_detection_f1, CONTRADICTION_DETECTION_F1_GATE
    );
    println!(
        "Quality probe contradiction_detection_f1_pass: {}",
        summary.contradiction_detection_f1_pass
    );
    println!(
        "Quality probe temporal_window: {}",
        summary.temporal_window_pass
    );
    println!(
        "Quality probe temporal_unknown_excluded: {}",
        summary.temporal_unknown_excluded_pass
    );
    println!(
        "Quality probe hybrid_filter_with_embedding: {}",
        summary.hybrid_filter_with_embedding_pass
    );
}

fn print_ann_tuning(ann_tuning: &AnnTuningConfig) {
    println!(
        "ANN tuning: base_neighbors={}, upper_neighbors={}, search_factor={}, search_min={}, search_max={}",
        ann_tuning.max_neighbors_base,
        ann_tuning.max_neighbors_upper,
        ann_tuning.search_expansion_factor,
        ann_tuning.search_expansion_min,
        ann_tuning.search_expansion_max
    );
}

fn append_history(path: &str, summary: &BenchmarkSummary) -> Result<(), std::io::Error> {
    let history_path = Path::new(path);
    if let Some(parent) = history_path.parent()
        && !parent.as_os_str().is_empty()
    {
        create_dir_all(parent)?;
    }

    let needs_header = !history_path.exists() || std::fs::metadata(history_path)?.len() == 0;
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(history_path)?;

    if needs_header {
        writeln!(file, "# Benchmark History")?;
        writeln!(file)?;
        writeln!(
            file,
            "| run_epoch_secs | profile | fixture_size | iterations | baseline_top1 | eme_top1 | baseline_hit | eme_hit | baseline_avg_ms | eme_avg_ms | baseline_scan_count | dash_candidate_count | metadata_prefilter_count | ann_candidate_count | final_scored_candidate_count | ann_recall_at_10 | ann_recall_at_100 | ann_recall_curve | segment_cache_hits | segment_refresh_attempts | segment_refresh_successes | segment_refresh_failures | segment_refresh_avg_ms | wal_claims_seeded | wal_checkpoint_ms | wal_replay_ms | wal_snapshot_records | wal_truncated_wal_records | wal_replay_snapshot_records | wal_replay_wal_records | wal_replay_validation_hit | wal_replay_validation_top_claim |"
        )?;
        writeln!(
            file,
            "|---|---|---:|---:|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|"
        )?;
    }
    let segment_refresh_avg_ms = if summary.segment_cache_probe.refresh_attempts == 0 {
        0.0
    } else {
        (summary.segment_cache_probe.refresh_load_micros as f64 / 1000.0)
            / summary.segment_cache_probe.refresh_attempts as f64
    };
    let wal_claims_seeded = summary
        .wal_scale
        .as_ref()
        .map(|metrics| metrics.claims_seeded.to_string())
        .unwrap_or_else(|| "0".to_string());
    let wal_checkpoint_ms = summary
        .wal_scale
        .as_ref()
        .map(|metrics| format!("{:.4}", metrics.checkpoint_ms))
        .unwrap_or_else(|| "n/a".to_string());
    let wal_replay_ms = summary
        .wal_scale
        .as_ref()
        .map(|metrics| format!("{:.4}", metrics.replay_ms))
        .unwrap_or_else(|| "n/a".to_string());
    let wal_snapshot_records = summary
        .wal_scale
        .as_ref()
        .map(|metrics| metrics.checkpoint_stats.snapshot_records.to_string())
        .unwrap_or_else(|| "0".to_string());
    let wal_truncated_wal_records = summary
        .wal_scale
        .as_ref()
        .map(|metrics| metrics.checkpoint_stats.truncated_wal_records.to_string())
        .unwrap_or_else(|| "0".to_string());
    let wal_replay_snapshot_records = summary
        .wal_scale
        .as_ref()
        .map(|metrics| metrics.replay_snapshot_records.to_string())
        .unwrap_or_else(|| "0".to_string());
    let wal_replay_wal_records = summary
        .wal_scale
        .as_ref()
        .map(|metrics| metrics.replay_wal_records.to_string())
        .unwrap_or_else(|| "0".to_string());
    let wal_replay_validation_hit = summary
        .wal_scale
        .as_ref()
        .map(|metrics| metrics.replay_validation_hit.to_string())
        .unwrap_or_else(|| "false".to_string());
    let wal_replay_validation_top_claim = summary
        .wal_scale
        .as_ref()
        .and_then(|metrics| metrics.replay_validation_top_claim.clone())
        .unwrap_or_else(|| "none".to_string());
    let ann_recall_curve = format_ann_recall_curve(&summary.ann_recall.curve);

    writeln!(
        file,
        "| {} | {} | {} | {} | {} | {} | {} | {} | {:.4} | {:.4} | {} | {} | {} | {} | {} | {:.4} | {:.4} | {} | {} | {} | {} | {} | {:.4} | {} | {} | {} | {} | {} | {} | {} | {} | {} |",
        summary.run_epoch_secs,
        summary.profile.as_str(),
        summary.fixture_size,
        summary.iterations,
        summary.baseline_top.as_deref().unwrap_or("none"),
        summary.eme_top.as_deref().unwrap_or("none"),
        summary.baseline_hit,
        summary.eme_hit,
        summary.baseline_latency,
        summary.eme_latency,
        summary.baseline_scan_count,
        summary.dash_candidate_count,
        summary.metadata_prefilter_count,
        summary.ann_candidate_count,
        summary.final_scored_candidate_count,
        summary.ann_recall.recall_at_10,
        summary.ann_recall.recall_at_100,
        ann_recall_curve,
        summary.segment_cache_probe.cache_hits,
        summary.segment_cache_probe.refresh_attempts,
        summary.segment_cache_probe.refresh_successes,
        summary.segment_cache_probe.refresh_failures,
        segment_refresh_avg_ms,
        wal_claims_seeded,
        wal_checkpoint_ms,
        wal_replay_ms,
        wal_snapshot_records,
        wal_truncated_wal_records,
        wal_replay_snapshot_records,
        wal_replay_wal_records,
        wal_replay_validation_hit,
        wal_replay_validation_top_claim
    )?;
    Ok(())
}

fn append_history_csv(path: &str, summary: &BenchmarkSummary) -> Result<(), std::io::Error> {
    let csv_path = Path::new(path);
    if let Some(parent) = csv_path.parent()
        && !parent.as_os_str().is_empty()
    {
        create_dir_all(parent)?;
    }

    let needs_header = !csv_path.exists() || std::fs::metadata(csv_path)?.len() == 0;
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(csv_path)?;
    if needs_header {
        writeln!(
            file,
            "run_epoch_secs,profile,fixture_size,iterations,baseline_top1,eme_top1,baseline_hit,eme_hit,baseline_avg_ms,eme_avg_ms,baseline_scan_count,dash_candidate_count,metadata_prefilter_count,ann_candidate_count,final_scored_candidate_count,ann_recall_at_10,ann_recall_at_100,ann_recall_curve,segment_cache_hits,segment_refresh_attempts,segment_refresh_successes,segment_refresh_failures,segment_refresh_avg_ms,wal_claims_seeded,wal_checkpoint_ms,wal_replay_ms,wal_snapshot_records,wal_truncated_wal_records,wal_replay_snapshot_records,wal_replay_wal_records,wal_replay_validation_hit,wal_replay_validation_top_claim"
        )?;
    }

    let segment_refresh_avg_ms = if summary.segment_cache_probe.refresh_attempts == 0 {
        0.0
    } else {
        (summary.segment_cache_probe.refresh_load_micros as f64 / 1000.0)
            / summary.segment_cache_probe.refresh_attempts as f64
    };
    let wal_claims_seeded = summary
        .wal_scale
        .as_ref()
        .map(|metrics| metrics.claims_seeded.to_string())
        .unwrap_or_else(|| "0".to_string());
    let wal_checkpoint_ms = summary
        .wal_scale
        .as_ref()
        .map(|metrics| format!("{:.4}", metrics.checkpoint_ms))
        .unwrap_or_else(|| "n/a".to_string());
    let wal_replay_ms = summary
        .wal_scale
        .as_ref()
        .map(|metrics| format!("{:.4}", metrics.replay_ms))
        .unwrap_or_else(|| "n/a".to_string());
    let wal_snapshot_records = summary
        .wal_scale
        .as_ref()
        .map(|metrics| metrics.checkpoint_stats.snapshot_records.to_string())
        .unwrap_or_else(|| "0".to_string());
    let wal_truncated_wal_records = summary
        .wal_scale
        .as_ref()
        .map(|metrics| metrics.checkpoint_stats.truncated_wal_records.to_string())
        .unwrap_or_else(|| "0".to_string());
    let wal_replay_snapshot_records = summary
        .wal_scale
        .as_ref()
        .map(|metrics| metrics.replay_snapshot_records.to_string())
        .unwrap_or_else(|| "0".to_string());
    let wal_replay_wal_records = summary
        .wal_scale
        .as_ref()
        .map(|metrics| metrics.replay_wal_records.to_string())
        .unwrap_or_else(|| "0".to_string());
    let wal_replay_validation_hit = summary
        .wal_scale
        .as_ref()
        .map(|metrics| metrics.replay_validation_hit.to_string())
        .unwrap_or_else(|| "false".to_string());
    let wal_replay_validation_top_claim = summary
        .wal_scale
        .as_ref()
        .and_then(|metrics| metrics.replay_validation_top_claim.clone())
        .unwrap_or_else(|| "none".to_string());
    let ann_recall_curve = format_ann_recall_curve(&summary.ann_recall.curve);

    let row = vec![
        summary.run_epoch_secs.to_string(),
        summary.profile.as_str().to_string(),
        summary.fixture_size.to_string(),
        summary.iterations.to_string(),
        summary
            .baseline_top
            .as_deref()
            .unwrap_or("none")
            .to_string(),
        summary.eme_top.as_deref().unwrap_or("none").to_string(),
        summary.baseline_hit.to_string(),
        summary.eme_hit.to_string(),
        format!("{:.4}", summary.baseline_latency),
        format!("{:.4}", summary.eme_latency),
        summary.baseline_scan_count.to_string(),
        summary.dash_candidate_count.to_string(),
        summary.metadata_prefilter_count.to_string(),
        summary.ann_candidate_count.to_string(),
        summary.final_scored_candidate_count.to_string(),
        format!("{:.4}", summary.ann_recall.recall_at_10),
        format!("{:.4}", summary.ann_recall.recall_at_100),
        ann_recall_curve,
        summary.segment_cache_probe.cache_hits.to_string(),
        summary.segment_cache_probe.refresh_attempts.to_string(),
        summary.segment_cache_probe.refresh_successes.to_string(),
        summary.segment_cache_probe.refresh_failures.to_string(),
        format!("{:.4}", segment_refresh_avg_ms),
        wal_claims_seeded,
        wal_checkpoint_ms,
        wal_replay_ms,
        wal_snapshot_records,
        wal_truncated_wal_records,
        wal_replay_snapshot_records,
        wal_replay_wal_records,
        wal_replay_validation_hit,
        wal_replay_validation_top_claim,
    ];
    writeln!(file, "{}", row.join(","))?;
    Ok(())
}

fn write_scorecard(
    path: &str,
    summary: &BenchmarkSummary,
    quality: &QualityProbeSummary,
) -> Result<(), std::io::Error> {
    let scorecard_path = Path::new(path);
    if let Some(parent) = scorecard_path.parent()
        && !parent.as_os_str().is_empty()
    {
        create_dir_all(parent)?;
    }

    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(scorecard_path)?;

    writeln!(file, "# DASH Benchmark Scorecard")?;
    writeln!(file)?;
    writeln!(file, "- run_epoch_secs: {}", summary.run_epoch_secs)?;
    writeln!(file, "- profile: {}", summary.profile.as_str())?;
    writeln!(file, "- fixture_size: {}", summary.fixture_size)?;
    writeln!(file, "- iterations: {}", summary.iterations)?;
    writeln!(
        file,
        "- baseline_top1: {}",
        summary.baseline_top.as_deref().unwrap_or("none")
    )?;
    writeln!(
        file,
        "- dash_top1: {}",
        summary.eme_top.as_deref().unwrap_or("none")
    )?;
    writeln!(file, "- baseline_avg_ms: {:.4}", summary.baseline_latency)?;
    writeln!(file, "- dash_avg_ms: {:.4}", summary.eme_latency)?;
    writeln!(
        file,
        "- baseline_scan_count: {}",
        summary.baseline_scan_count
    )?;
    writeln!(
        file,
        "- dash_candidate_count: {}",
        summary.dash_candidate_count
    )?;
    writeln!(
        file,
        "- metadata_prefilter_count: {}",
        summary.metadata_prefilter_count
    )?;
    writeln!(
        file,
        "- ann_candidate_count: {}",
        summary.ann_candidate_count
    )?;
    writeln!(
        file,
        "- final_scored_candidate_count: {}",
        summary.final_scored_candidate_count
    )?;
    writeln!(
        file,
        "- ann_recall_at_10: {:.4}",
        summary.ann_recall.recall_at_10
    )?;
    writeln!(
        file,
        "- ann_recall_at_100: {:.4}",
        summary.ann_recall.recall_at_100
    )?;
    writeln!(
        file,
        "- ann_recall_curve: {}",
        format_ann_recall_curve(&summary.ann_recall.curve)
    )?;
    let reduction_pct = if summary.baseline_scan_count == 0 {
        0.0
    } else {
        100.0 * (1.0 - summary.dash_candidate_count as f64 / summary.baseline_scan_count as f64)
    };
    writeln!(
        file,
        "- candidate_reduction_pct: {:.2}",
        reduction_pct.max(0.0)
    )?;
    writeln!(
        file,
        "- index_tenant_count: {}",
        summary.index_stats.tenant_count
    )?;
    writeln!(
        file,
        "- index_claim_count: {}",
        summary.index_stats.claim_count
    )?;
    writeln!(
        file,
        "- index_inverted_terms: {}",
        summary.index_stats.inverted_terms
    )?;
    writeln!(
        file,
        "- index_entity_terms: {}",
        summary.index_stats.entity_terms
    )?;
    writeln!(
        file,
        "- index_temporal_buckets: {}",
        summary.index_stats.temporal_buckets
    )?;
    writeln!(
        file,
        "- ann_max_neighbors_base: {}",
        summary.ann_tuning.max_neighbors_base
    )?;
    writeln!(
        file,
        "- ann_max_neighbors_upper: {}",
        summary.ann_tuning.max_neighbors_upper
    )?;
    writeln!(
        file,
        "- ann_search_expansion_factor: {}",
        summary.ann_tuning.search_expansion_factor
    )?;
    writeln!(
        file,
        "- ann_search_expansion_min: {}",
        summary.ann_tuning.search_expansion_min
    )?;
    writeln!(
        file,
        "- ann_search_expansion_max: {}",
        summary.ann_tuning.search_expansion_max
    )?;
    let segment_refresh_avg_ms = if summary.segment_cache_probe.refresh_attempts == 0 {
        0.0
    } else {
        (summary.segment_cache_probe.refresh_load_micros as f64 / 1000.0)
            / summary.segment_cache_probe.refresh_attempts as f64
    };
    writeln!(file)?;
    writeln!(file, "## Segment Cache Probe")?;
    writeln!(file)?;
    writeln!(
        file,
        "- segment_cache_hits: {}",
        summary.segment_cache_probe.cache_hits
    )?;
    writeln!(
        file,
        "- segment_refresh_attempts: {}",
        summary.segment_cache_probe.refresh_attempts
    )?;
    writeln!(
        file,
        "- segment_refresh_successes: {}",
        summary.segment_cache_probe.refresh_successes
    )?;
    writeln!(
        file,
        "- segment_refresh_failures: {}",
        summary.segment_cache_probe.refresh_failures
    )?;
    writeln!(
        file,
        "- segment_refresh_avg_ms: {:.4}",
        segment_refresh_avg_ms
    )?;
    if let Some(wal_scale) = summary.wal_scale.as_ref() {
        writeln!(file)?;
        writeln!(file, "## WAL Scale Slice")?;
        writeln!(file)?;
        writeln!(file, "- claims_seeded: {}", wal_scale.claims_seeded)?;
        writeln!(file, "- checkpoint_ms: {:.4}", wal_scale.checkpoint_ms)?;
        writeln!(file, "- replay_ms: {:.4}", wal_scale.replay_ms)?;
        writeln!(
            file,
            "- checkpoint_snapshot_records: {}",
            wal_scale.checkpoint_stats.snapshot_records
        )?;
        writeln!(
            file,
            "- checkpoint_truncated_wal_records: {}",
            wal_scale.checkpoint_stats.truncated_wal_records
        )?;
        writeln!(
            file,
            "- replay_snapshot_records: {}",
            wal_scale.replay_snapshot_records
        )?;
        writeln!(
            file,
            "- replay_wal_records: {}",
            wal_scale.replay_wal_records
        )?;
        writeln!(
            file,
            "- replay_validation_hit: {}",
            wal_scale.replay_validation_hit
        )?;
        writeln!(
            file,
            "- replay_validation_top_claim: {}",
            wal_scale
                .replay_validation_top_claim
                .as_deref()
                .unwrap_or("none")
        )?;
    } else {
        writeln!(file)?;
        writeln!(file, "## WAL Scale Slice")?;
        writeln!(file)?;
        writeln!(file, "- skipped: true")?;
    }
    writeln!(file)?;
    writeln!(file, "## Quality Probes")?;
    writeln!(file)?;
    writeln!(
        file,
        "- passed: {}/{}",
        quality.passed_count(),
        quality.total_count()
    )?;
    writeln!(
        file,
        "- contradiction_support_only_pass: {}",
        quality.contradiction_support_only_pass
    )?;
    writeln!(
        file,
        "- contradiction_detection_f1: {:.4}",
        quality.contradiction_detection_f1
    )?;
    writeln!(
        file,
        "- contradiction_detection_f1_gate: {:.2}",
        CONTRADICTION_DETECTION_F1_GATE
    )?;
    writeln!(
        file,
        "- contradiction_detection_f1_pass: {}",
        quality.contradiction_detection_f1_pass
    )?;
    writeln!(
        file,
        "- temporal_window_pass: {}",
        quality.temporal_window_pass
    )?;
    writeln!(
        file,
        "- temporal_unknown_excluded_pass: {}",
        quality.temporal_unknown_excluded_pass
    )?;
    writeln!(
        file,
        "- hybrid_filter_with_embedding_pass: {}",
        quality.hybrid_filter_with_embedding_pass
    )?;
    Ok(())
}

fn enforce_history_guard(
    path: &str,
    profile: BenchmarkProfile,
    current_eme_avg_ms: f64,
    max_regression_pct: f64,
) -> Result<(), String> {
    let previous = read_latest_history_row(path, profile)?.map(|row| row.eme_avg_ms);
    match previous {
        Some(previous_ms) if previous_ms > 0.0 => {
            let allowed = previous_ms * (1.0 + max_regression_pct / 100.0);
            if current_eme_avg_ms > allowed {
                return Err(format!(
                    "history guard violated for profile '{}': current DASH avg {:.4} ms exceeds allowed {:.4} ms (prev {:.4} ms, max regression {}%)",
                    profile.as_str(),
                    current_eme_avg_ms,
                    allowed,
                    previous_ms,
                    max_regression_pct
                ));
            }
            println!(
                "History guard check passed: profile={}, prev_dash_avg_ms={:.4}, current_dash_avg_ms={:.4}, max_regression_pct={}",
                profile.as_str(),
                previous_ms,
                current_eme_avg_ms,
                max_regression_pct
            );
            Ok(())
        }
        _ => {
            println!(
                "History guard skipped: no prior row found for profile '{}' in {}",
                profile.as_str(),
                path
            );
            Ok(())
        }
    }
}

fn read_latest_history_row(
    path: &str,
    profile: BenchmarkProfile,
) -> Result<Option<HistoryRow>, String> {
    let history_path = Path::new(path);
    if !history_path.exists() {
        return Ok(None);
    }

    let file = std::fs::File::open(history_path).map_err(|e| e.to_string())?;
    let reader = BufReader::new(file);
    let mut matching_rows: VecDeque<HistoryRow> = VecDeque::new();

    for line in reader.lines() {
        let line = line.map_err(|e| e.to_string())?;
        let trimmed = line.trim();
        if !trimmed.starts_with('|') {
            continue;
        }
        if trimmed.contains("---") || trimmed.contains("run_epoch_secs") {
            continue;
        }

        if let Some(row) = parse_history_row(trimmed)?
            && row.profile == profile
        {
            matching_rows.push_back(row);
        }
    }

    Ok(matching_rows.pop_back())
}

fn parse_history_row(line: &str) -> Result<Option<HistoryRow>, String> {
    let cols: Vec<&str> = line.split('|').map(|c| c.trim()).collect();
    if cols.len() < 11 {
        return Ok(None);
    }

    let profile = match cols[2] {
        "smoke" => BenchmarkProfile::Smoke,
        "standard" => BenchmarkProfile::Standard,
        "large" => BenchmarkProfile::Large,
        "xlarge" => BenchmarkProfile::XLarge,
        "hybrid" => BenchmarkProfile::Hybrid,
        _ => return Ok(None),
    };
    let eme_avg_ms = cols[10]
        .parse::<f64>()
        .map_err(|_| "invalid eme_avg_ms in benchmark history row".to_string())?;

    Ok(Some(HistoryRow {
        profile,
        eme_avg_ms,
    }))
}

fn run_quality_probes() -> QualityProbeSummary {
    let tenant = "tenant-quality-probe";
    let mut store = InMemoryStore::new();
    seed_quality_probe_fixture(&mut store, tenant);

    let contradiction_top = store
        .retrieve(&RetrievalRequest {
            tenant_id: tenant.to_string(),
            query: "project orion launched".to_string(),
            top_k: 1,
            stance_mode: StanceMode::SupportOnly,
        })
        .first()
        .map(|r| r.claim_id.clone());
    let contradiction_support_only_pass =
        contradiction_top.as_deref() == Some("probe-contradiction-supported");
    let contradiction_detection_f1 = measure_contradiction_detection_f1(&store, tenant);
    let contradiction_detection_f1_pass =
        contradiction_detection_f1 >= CONTRADICTION_DETECTION_F1_GATE;

    let temporal_results = store.retrieve_with_time_range(
        &RetrievalRequest {
            tenant_id: tenant.to_string(),
            query: "mars mission status update".to_string(),
            top_k: 10,
            stance_mode: StanceMode::Balanced,
        },
        Some(2_000),
        Some(3_000),
    );
    let temporal_window_pass =
        temporal_results.first().map(|r| r.claim_id.as_str()) == Some("probe-temporal-new");
    let temporal_unknown_excluded_pass = !temporal_results
        .iter()
        .any(|r| r.claim_id == "probe-temporal-unknown");

    let hybrid_filtered = execute_api_query(
        &store,
        RetrieveApiRequest {
            tenant_id: tenant.to_string(),
            query: "Did project helios acquire startup nova?".to_string(),
            query_embedding: Some(benchmark_query_embedding()),
            entity_filters: vec!["project helios".to_string()],
            embedding_id_filters: vec!["emb://probe-filter-match".to_string()],
            top_k: 1,
            stance_mode: StanceMode::Balanced,
            return_graph: false,
            time_range: None,
        },
    );
    let hybrid_filter_with_embedding_pass =
        hybrid_filtered.results.first().map(|r| r.claim_id.as_str()) == Some("probe-filter-match");

    QualityProbeSummary {
        contradiction_support_only_pass,
        contradiction_detection_f1,
        contradiction_detection_f1_pass,
        temporal_window_pass,
        temporal_unknown_excluded_pass,
        hybrid_filter_with_embedding_pass,
    }
}

fn seed_quality_probe_fixture(store: &mut InMemoryStore, tenant: &str) {
    store
        .ingest_bundle(
            Claim {
                claim_id: "probe-contradiction-heavy".to_string(),
                tenant_id: tenant.to_string(),
                canonical_text: "Project Orion launched in 2024".to_string(),
                confidence: 0.85,
                event_time_unix: Some(2_024),
                entities: vec![],
                embedding_ids: vec![],
                claim_type: None,
                valid_from: None,
                valid_to: None,
                created_at: None,
                updated_at: None,
            },
            vec![
                Evidence {
                    evidence_id: "probe-contradiction-heavy-s1".to_string(),
                    claim_id: "probe-contradiction-heavy".to_string(),
                    source_id: "source://probe/heavy/s1".to_string(),
                    stance: Stance::Supports,
                    source_quality: 0.9,
                    chunk_id: None,
                    span_start: None,
                    span_end: None,
                    doc_id: None,
                    extraction_model: None,
                    ingested_at: None,
                },
                Evidence {
                    evidence_id: "probe-contradiction-heavy-c1".to_string(),
                    claim_id: "probe-contradiction-heavy".to_string(),
                    source_id: "source://probe/heavy/c1".to_string(),
                    stance: Stance::Contradicts,
                    source_quality: 0.9,
                    chunk_id: None,
                    span_start: None,
                    span_end: None,
                    doc_id: None,
                    extraction_model: None,
                    ingested_at: None,
                },
                Evidence {
                    evidence_id: "probe-contradiction-heavy-c2".to_string(),
                    claim_id: "probe-contradiction-heavy".to_string(),
                    source_id: "source://probe/heavy/c2".to_string(),
                    stance: Stance::Contradicts,
                    source_quality: 0.9,
                    chunk_id: None,
                    span_start: None,
                    span_end: None,
                    doc_id: None,
                    extraction_model: None,
                    ingested_at: None,
                },
            ],
            vec![],
        )
        .expect("quality probe ingest should succeed");

    store
        .ingest_bundle(
            Claim {
                claim_id: "probe-contradiction-supported".to_string(),
                tenant_id: tenant.to_string(),
                canonical_text: "Project Orion launched in 2023".to_string(),
                confidence: 0.9,
                event_time_unix: Some(2_023),
                entities: vec![],
                embedding_ids: vec![],
                claim_type: None,
                valid_from: None,
                valid_to: None,
                created_at: None,
                updated_at: None,
            },
            vec![
                Evidence {
                    evidence_id: "probe-contradiction-supported-s1".to_string(),
                    claim_id: "probe-contradiction-supported".to_string(),
                    source_id: "source://probe/supported/s1".to_string(),
                    stance: Stance::Supports,
                    source_quality: 0.92,
                    chunk_id: None,
                    span_start: None,
                    span_end: None,
                    doc_id: None,
                    extraction_model: None,
                    ingested_at: None,
                },
                Evidence {
                    evidence_id: "probe-contradiction-supported-s2".to_string(),
                    claim_id: "probe-contradiction-supported".to_string(),
                    source_id: "source://probe/supported/s2".to_string(),
                    stance: Stance::Supports,
                    source_quality: 0.9,
                    chunk_id: None,
                    span_start: None,
                    span_end: None,
                    doc_id: None,
                    extraction_model: None,
                    ingested_at: None,
                },
            ],
            vec![],
        )
        .expect("quality probe ingest should succeed");

    store
        .ingest_bundle(
            Claim {
                claim_id: "probe-temporal-old".to_string(),
                tenant_id: tenant.to_string(),
                canonical_text: "Mars mission status update".to_string(),
                confidence: 0.9,
                event_time_unix: Some(1_500),
                entities: vec![],
                embedding_ids: vec![],
                claim_type: None,
                valid_from: None,
                valid_to: None,
                created_at: None,
                updated_at: None,
            },
            vec![Evidence {
                evidence_id: "probe-temporal-old-s1".to_string(),
                claim_id: "probe-temporal-old".to_string(),
                source_id: "source://probe/temporal/old".to_string(),
                stance: Stance::Supports,
                source_quality: 0.9,
                chunk_id: None,
                span_start: None,
                span_end: None,
                doc_id: None,
                extraction_model: None,
                ingested_at: None,
            }],
            vec![],
        )
        .expect("quality probe ingest should succeed");

    store
        .ingest_bundle(
            Claim {
                claim_id: "probe-temporal-new".to_string(),
                tenant_id: tenant.to_string(),
                canonical_text: "Mars mission status update".to_string(),
                confidence: 0.9,
                event_time_unix: Some(2_500),
                entities: vec![],
                embedding_ids: vec![],
                claim_type: None,
                valid_from: None,
                valid_to: None,
                created_at: None,
                updated_at: None,
            },
            vec![Evidence {
                evidence_id: "probe-temporal-new-s1".to_string(),
                claim_id: "probe-temporal-new".to_string(),
                source_id: "source://probe/temporal/new".to_string(),
                stance: Stance::Supports,
                source_quality: 0.9,
                chunk_id: None,
                span_start: None,
                span_end: None,
                doc_id: None,
                extraction_model: None,
                ingested_at: None,
            }],
            vec![],
        )
        .expect("quality probe ingest should succeed");

    store
        .ingest_bundle(
            Claim {
                claim_id: "probe-temporal-unknown".to_string(),
                tenant_id: tenant.to_string(),
                canonical_text: "Mars mission status update".to_string(),
                confidence: 0.95,
                event_time_unix: None,
                entities: vec![],
                embedding_ids: vec![],
                claim_type: None,
                valid_from: None,
                valid_to: None,
                created_at: None,
                updated_at: None,
            },
            vec![Evidence {
                evidence_id: "probe-temporal-unknown-s1".to_string(),
                claim_id: "probe-temporal-unknown".to_string(),
                source_id: "source://probe/temporal/unknown".to_string(),
                stance: Stance::Supports,
                source_quality: 0.95,
                chunk_id: None,
                span_start: None,
                span_end: None,
                doc_id: None,
                extraction_model: None,
                ingested_at: None,
            }],
            vec![],
        )
        .expect("quality probe ingest should succeed");

    store
        .ingest_bundle(
            Claim {
                claim_id: "probe-filter-match".to_string(),
                tenant_id: tenant.to_string(),
                canonical_text: "Project Helios acquired Startup Nova".to_string(),
                confidence: 0.96,
                event_time_unix: Some(2_026),
                entities: vec!["Project Helios".to_string(), "Startup Nova".to_string()],
                embedding_ids: vec!["emb://probe-filter-match".to_string()],
                claim_type: None,
                valid_from: None,
                valid_to: None,
                created_at: None,
                updated_at: None,
            },
            vec![Evidence {
                evidence_id: "probe-filter-match-s1".to_string(),
                claim_id: "probe-filter-match".to_string(),
                source_id: "source://probe/filter/match".to_string(),
                stance: Stance::Supports,
                source_quality: 0.95,
                chunk_id: None,
                span_start: None,
                span_end: None,
                doc_id: None,
                extraction_model: None,
                ingested_at: None,
            }],
            vec![],
        )
        .expect("quality probe ingest should succeed");
    store
        .upsert_claim_vector("probe-filter-match", benchmark_query_embedding())
        .expect("quality probe vector upsert should succeed");

    store
        .ingest_bundle(
            Claim {
                claim_id: "probe-filter-other".to_string(),
                tenant_id: tenant.to_string(),
                canonical_text: "Project Helios announced startup program".to_string(),
                confidence: 0.91,
                event_time_unix: Some(2_026),
                entities: vec!["Project Helios".to_string()],
                embedding_ids: vec!["emb://probe-filter-other".to_string()],
                claim_type: None,
                valid_from: None,
                valid_to: None,
                created_at: None,
                updated_at: None,
            },
            vec![Evidence {
                evidence_id: "probe-filter-other-s1".to_string(),
                claim_id: "probe-filter-other".to_string(),
                source_id: "source://probe/filter/other".to_string(),
                stance: Stance::Supports,
                source_quality: 0.9,
                chunk_id: None,
                span_start: None,
                span_end: None,
                doc_id: None,
                extraction_model: None,
                ingested_at: None,
            }],
            vec![],
        )
        .expect("quality probe ingest should succeed");
    store
        .upsert_claim_vector("probe-filter-other", fixture_vector_for_index(999_991))
        .expect("quality probe vector upsert should succeed");

    seed_contradiction_f1_fixture(store, tenant);
}

fn seed_contradiction_f1_fixture(store: &mut InMemoryStore, tenant: &str) {
    let mut seed_case = |claim_id: &str, supports: usize, contradicts: usize| {
        let mut evidence = Vec::with_capacity(supports + contradicts);
        for idx in 0..supports {
            evidence.push(Evidence {
                evidence_id: format!("{claim_id}-s{}", idx + 1),
                claim_id: claim_id.to_string(),
                source_id: format!("source://probe/f1/{claim_id}/supports/{}", idx + 1),
                stance: Stance::Supports,
                source_quality: 0.91,
                chunk_id: None,
                span_start: None,
                span_end: None,
                doc_id: None,
                extraction_model: None,
                ingested_at: None,
            });
        }
        for idx in 0..contradicts {
            evidence.push(Evidence {
                evidence_id: format!("{claim_id}-c{}", idx + 1),
                claim_id: claim_id.to_string(),
                source_id: format!("source://probe/f1/{claim_id}/contradicts/{}", idx + 1),
                stance: Stance::Contradicts,
                source_quality: 0.91,
                chunk_id: None,
                span_start: None,
                span_end: None,
                doc_id: None,
                extraction_model: None,
                ingested_at: None,
            });
        }

        store
            .ingest_bundle(
                Claim {
                    claim_id: claim_id.to_string(),
                    tenant_id: tenant.to_string(),
                    canonical_text: format!(
                        "Adversarial contradiction probe statement for {claim_id}"
                    ),
                    confidence: 0.9,
                    event_time_unix: Some(2_026),
                    entities: vec![],
                    embedding_ids: vec![],
                    claim_type: None,
                    valid_from: None,
                    valid_to: None,
                    created_at: None,
                    updated_at: None,
                },
                evidence,
                vec![],
            )
            .expect("contradiction F1 probe ingest should succeed");
    };

    for idx in 1..=5 {
        seed_case(&format!("probe-f1-support-{idx}"), 2, 0);
    }
    for idx in 1..=5 {
        seed_case(&format!("probe-f1-contradict-{idx}"), 1, 2);
    }
}

fn measure_contradiction_detection_f1(store: &InMemoryStore, tenant: &str) -> f64 {
    let results = store.retrieve(&RetrievalRequest {
        tenant_id: tenant.to_string(),
        query: "adversarial contradiction probe".to_string(),
        top_k: 32,
        stance_mode: StanceMode::Balanced,
    });

    let expected_contradiction_ids: HashSet<String> = (1..=5)
        .map(|idx| format!("probe-f1-contradict-{idx}"))
        .collect();
    let predicted_contradiction_ids: HashSet<String> = results
        .into_iter()
        .filter(|result| result.claim_id.starts_with("probe-f1-"))
        .filter(|result| result.contradicts > result.supports)
        .map(|result| result.claim_id)
        .collect();

    let true_positive = predicted_contradiction_ids
        .intersection(&expected_contradiction_ids)
        .count();
    let false_positive = predicted_contradiction_ids
        .difference(&expected_contradiction_ids)
        .count();
    let false_negative = expected_contradiction_ids
        .difference(&predicted_contradiction_ids)
        .count();

    f1_score(true_positive, false_positive, false_negative)
}

fn f1_score(true_positive: usize, false_positive: usize, false_negative: usize) -> f64 {
    if true_positive == 0 {
        return 0.0;
    }
    let precision = true_positive as f64 / (true_positive + false_positive) as f64;
    let recall = true_positive as f64 / (true_positive + false_negative) as f64;
    if (precision + recall).abs() <= f64::EPSILON {
        0.0
    } else {
        (2.0 * precision * recall) / (precision + recall)
    }
}

fn seed_fixture(store: &mut InMemoryStore, tenant: &str, count: usize) {
    let vector_upsert_stride = vector_upsert_stride_for_count(count);
    for i in 0..count {
        let claim_id = if i == count / 3 {
            "claim-target".to_string()
        } else {
            format!("claim-{i}")
        };
        let claim_text = if i == count / 3 {
            "Company X acquired Company Y in 2025".to_string()
        } else if i % 50 == 0 {
            format!("Company X announced partnership program {i}")
        } else {
            format!("Unrelated operational update {i}")
        };
        store
            .ingest_bundle(
                Claim {
                    claim_id: claim_id.clone(),
                    tenant_id: tenant.to_string(),
                    canonical_text: claim_text,
                    confidence: if i == count / 3 { 0.95 } else { 0.7 },
                    event_time_unix: Some(1735689600 + i as i64),
                    entities: vec![],
                    embedding_ids: vec![],
                    claim_type: None,
                    valid_from: None,
                    valid_to: None,
                    created_at: None,
                    updated_at: None,
                },
                vec![Evidence {
                    evidence_id: format!("evidence-{i}"),
                    claim_id: claim_id.clone(),
                    source_id: format!("source://doc-{i}"),
                    stance: Stance::Supports,
                    source_quality: if i == count / 3 { 0.95 } else { 0.75 },
                    chunk_id: None,
                    span_start: None,
                    span_end: None,
                    doc_id: None,
                    extraction_model: None,
                    ingested_at: None,
                }],
                vec![],
            )
            .expect("fixture ingest should succeed");

        let should_upsert_vector = i == count / 3 || i % vector_upsert_stride == 0;
        if should_upsert_vector {
            let vector = if i == count / 3 {
                benchmark_query_embedding()
            } else {
                fixture_vector_for_index(i)
            };
            store
                .upsert_claim_vector(&claim_id, vector)
                .expect("fixture vector upsert should succeed");
        }
    }
}

fn seed_fixture_persistent(
    store: &mut InMemoryStore,
    wal: &mut FileWal,
    tenant: &str,
    count: usize,
) -> Result<(), String> {
    let vector_upsert_stride = vector_upsert_stride_for_count(count);
    for i in 0..count {
        let claim_id = if i == count / 3 {
            "claim-target".to_string()
        } else {
            format!("claim-{i}")
        };
        let claim_text = if i == count / 3 {
            "Company X acquired Company Y in 2025".to_string()
        } else if i % 50 == 0 {
            format!("Company X announced partnership program {i}")
        } else {
            format!("Unrelated operational update {i}")
        };
        store
            .ingest_bundle_persistent(
                wal,
                Claim {
                    claim_id: claim_id.clone(),
                    tenant_id: tenant.to_string(),
                    canonical_text: claim_text,
                    confidence: if i == count / 3 { 0.95 } else { 0.7 },
                    event_time_unix: Some(1735689600 + i as i64),
                    entities: vec![],
                    embedding_ids: vec![],
                    claim_type: None,
                    valid_from: None,
                    valid_to: None,
                    created_at: None,
                    updated_at: None,
                },
                vec![Evidence {
                    evidence_id: format!("evidence-{i}"),
                    claim_id: claim_id.clone(),
                    source_id: format!("source://doc-{i}"),
                    stance: Stance::Supports,
                    source_quality: if i == count / 3 { 0.95 } else { 0.75 },
                    chunk_id: None,
                    span_start: None,
                    span_end: None,
                    doc_id: None,
                    extraction_model: None,
                    ingested_at: None,
                }],
                vec![],
            )
            .map_err(|err| format!("persistent fixture ingest failed at index {i}: {err:?}"))?;

        let should_upsert_vector = i == count / 3 || i % vector_upsert_stride == 0;
        if should_upsert_vector {
            let vector = if i == count / 3 {
                benchmark_query_embedding()
            } else {
                fixture_vector_for_index(i)
            };
            store
                .upsert_claim_vector_persistent(wal, &claim_id, vector)
                .map_err(|err| {
                    format!("persistent fixture vector upsert failed at index {i}: {err:?}")
                })?;
        }
    }
    Ok(())
}

fn vector_upsert_stride_for_count(count: usize) -> usize {
    if count >= 100_000 { 4 } else { 1 }
}

fn seed_hybrid_fixture(store: &mut InMemoryStore, tenant: &str, count: usize) {
    let target_index = count / 4;
    for i in 0..count {
        let is_target = i == target_index;
        let claim_id = if is_target {
            "claim-hybrid-target".to_string()
        } else {
            format!("claim-hybrid-{i}")
        };

        let entities = if is_target || i % 19 == 0 {
            vec!["Project Helios".to_string(), "Startup Nova".to_string()]
        } else if i % 7 == 0 {
            vec!["Project Helios".to_string()]
        } else {
            vec!["Project Atlas".to_string()]
        };
        let embedding_ids = if is_target {
            vec!["emb://hybrid-target".to_string()]
        } else if i % 19 == 0 {
            vec!["emb://hybrid-decoy".to_string()]
        } else {
            vec![format!("emb://hybrid/{i}")]
        };
        let canonical_text = if is_target {
            "Project Helios acquired Startup Nova in 2026".to_string()
        } else if i % 19 == 0 {
            format!("Project Helios expanded Startup Nova partnership phase {i}")
        } else if i % 7 == 0 {
            format!("Project Helios operational update {i}")
        } else {
            format!("Unrelated system status report {i}")
        };

        store
            .ingest_bundle(
                Claim {
                    claim_id: claim_id.clone(),
                    tenant_id: tenant.to_string(),
                    canonical_text,
                    confidence: if is_target { 0.97 } else { 0.72 },
                    event_time_unix: Some(1767225600 + i as i64),
                    entities,
                    embedding_ids,
                    claim_type: None,
                    valid_from: None,
                    valid_to: None,
                    created_at: None,
                    updated_at: None,
                },
                vec![Evidence {
                    evidence_id: format!("evidence-hybrid-{i}"),
                    claim_id: claim_id.clone(),
                    source_id: format!("source://hybrid/doc-{i}"),
                    stance: Stance::Supports,
                    source_quality: if is_target { 0.97 } else { 0.78 },
                    chunk_id: None,
                    span_start: None,
                    span_end: None,
                    doc_id: None,
                    extraction_model: None,
                    ingested_at: None,
                }],
                vec![],
            )
            .expect("hybrid fixture ingest should succeed");

        let vector = if is_target {
            benchmark_query_embedding()
        } else if i % 19 == 0 {
            fixture_vector_for_index(i + 900_000)
        } else {
            fixture_vector_for_index(i + 200_000)
        };
        store
            .upsert_claim_vector(&claim_id, vector)
            .expect("hybrid fixture vector upsert should succeed");
    }
}

fn benchmark_query_embedding() -> Vec<f32> {
    vec![
        0.92, 0.11, 0.07, 0.66, 0.13, 0.81, 0.21, 0.35, 0.77, 0.09, 0.58, 0.44, 0.18, 0.72, 0.30,
        0.63,
    ]
}

fn fixture_vector_for_index(i: usize) -> Vec<f32> {
    let mut out = Vec::with_capacity(16);
    let mut state = (i as u64).wrapping_mul(6364136223846793005).wrapping_add(1);
    for _ in 0..16 {
        state = state
            .wrapping_mul(2862933555777941757)
            .wrapping_add(3037000493);
        let value = ((state >> 33) as f32) / (u32::MAX as f32);
        out.push(value);
    }
    out
}

fn baseline_retrieve_top1(store: &InMemoryStore, tenant: &str, query: &str) -> Option<String> {
    let claims = store.claims_for_tenant(tenant);
    claims
        .iter()
        .map(|claim| {
            (
                claim.claim_id.clone(),
                lexical_overlap_score(query, &claim.canonical_text),
            )
        })
        .max_by(|a, b| a.1.total_cmp(&b.1))
        .map(|(claim_id, _)| claim_id)
}

fn baseline_hybrid_retrieve_top1(
    store: &InMemoryStore,
    tenant: &str,
    query: &str,
    entity_filters: &[String],
    embedding_filters: &[String],
) -> Option<String> {
    let normalized_entities: Vec<String> = entity_filters
        .iter()
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty())
        .collect();
    let embedding_filter_set: std::collections::HashSet<String> = embedding_filters
        .iter()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .collect();

    store
        .claims_for_tenant(tenant)
        .into_iter()
        .filter(|claim| {
            let entity_match = if normalized_entities.is_empty() {
                true
            } else {
                claim
                    .entities
                    .iter()
                    .any(|entity| normalized_entities.contains(&entity.trim().to_ascii_lowercase()))
            };
            let embedding_match = if embedding_filter_set.is_empty() {
                true
            } else {
                claim
                    .embedding_ids
                    .iter()
                    .any(|embedding_id| embedding_filter_set.contains(embedding_id.trim()))
            };
            entity_match && embedding_match
        })
        .map(|claim| {
            (
                claim.claim_id,
                lexical_overlap_score(query, &claim.canonical_text),
            )
        })
        .max_by(|a, b| a.1.total_cmp(&b.1))
        .map(|(claim_id, _)| claim_id)
}

fn eme_retrieve_top1(store: &InMemoryStore, tenant: &str, query: &str) -> Option<String> {
    store
        .retrieve_with_time_range_and_query_vector(
            &RetrievalRequest {
                tenant_id: tenant.to_string(),
                query: query.to_string(),
                top_k: 1,
                stance_mode: StanceMode::Balanced,
            },
            None,
            None,
            Some(&benchmark_query_embedding()),
        )
        .first()
        .map(|r| r.claim_id.clone())
}

fn eme_hybrid_retrieve_top1(
    store: &InMemoryStore,
    tenant: &str,
    query: &str,
    query_embedding: &[f32],
    entity_filters: &[String],
    embedding_filters: &[String],
) -> Option<String> {
    execute_api_query(
        store,
        RetrieveApiRequest {
            tenant_id: tenant.to_string(),
            query: query.to_string(),
            query_embedding: Some(query_embedding.to_vec()),
            entity_filters: entity_filters.to_vec(),
            embedding_id_filters: embedding_filters.to_vec(),
            top_k: 1,
            stance_mode: StanceMode::Balanced,
            return_graph: false,
            time_range: None,
        },
    )
    .results
    .first()
    .map(|result| result.claim_id.clone())
}

fn measure_baseline_latency_ms(
    store: &InMemoryStore,
    tenant: &str,
    query: &str,
    iterations: usize,
) -> f64 {
    let start = Instant::now();
    for _ in 0..iterations {
        let _ = baseline_retrieve_top1(store, tenant, query);
    }
    let elapsed = start.elapsed().as_secs_f64() * 1000.0;
    elapsed / iterations as f64
}

fn measure_baseline_hybrid_latency_ms(
    store: &InMemoryStore,
    tenant: &str,
    query: &str,
    iterations: usize,
    entity_filters: &[String],
    embedding_filters: &[String],
) -> f64 {
    let start = Instant::now();
    for _ in 0..iterations {
        let _ =
            baseline_hybrid_retrieve_top1(store, tenant, query, entity_filters, embedding_filters);
    }
    let elapsed = start.elapsed().as_secs_f64() * 1000.0;
    elapsed / iterations as f64
}

fn measure_eme_latency_ms(
    store: &InMemoryStore,
    tenant: &str,
    query: &str,
    iterations: usize,
) -> f64 {
    let start = Instant::now();
    for _ in 0..iterations {
        let _ = eme_retrieve_top1(store, tenant, query);
    }
    let elapsed = start.elapsed().as_secs_f64() * 1000.0;
    elapsed / iterations as f64
}

fn measure_eme_hybrid_latency_ms(
    store: &InMemoryStore,
    tenant: &str,
    query: &str,
    iterations: usize,
    query_embedding: &[f32],
    entity_filters: &[String],
    embedding_filters: &[String],
) -> f64 {
    let start = Instant::now();
    for _ in 0..iterations {
        let _ = eme_hybrid_retrieve_top1(
            store,
            tenant,
            query,
            query_embedding,
            entity_filters,
            embedding_filters,
        );
    }
    let elapsed = start.elapsed().as_secs_f64() * 1000.0;
    elapsed / iterations as f64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config_with_large_gates(min_reduction: f64, max_latency: f64) -> BenchmarkConfig {
        BenchmarkConfig {
            profile: BenchmarkProfile::Large,
            iterations: Some(1),
            history_out: None,
            history_csv_out: None,
            guard_history: None,
            max_dash_latency_regression_pct: None,
            scorecard_out: None,
            ann_tuning: AnnTuningConfig::default(),
            large_min_candidate_reduction_pct: min_reduction,
            large_max_dash_latency_ms: max_latency,
            xlarge_min_candidate_reduction_pct: 96.0,
            xlarge_max_dash_latency_ms: 250.0,
            min_segment_refresh_successes: 0,
            min_segment_cache_hits: 0,
        }
    }

    fn summary_with_profile(
        profile: BenchmarkProfile,
        fixture_size: usize,
        dash_avg_ms: f64,
        baseline_scan_count: usize,
        dash_candidates: usize,
    ) -> BenchmarkSummary {
        BenchmarkSummary {
            run_epoch_secs: 0,
            profile,
            fixture_size,
            iterations: 1,
            baseline_top: Some("claim-target".to_string()),
            eme_top: Some("claim-target".to_string()),
            baseline_hit: true,
            eme_hit: true,
            baseline_latency: 100.0,
            eme_latency: dash_avg_ms,
            baseline_scan_count,
            dash_candidate_count: dash_candidates,
            metadata_prefilter_count: 0,
            ann_candidate_count: dash_candidates,
            final_scored_candidate_count: dash_candidates,
            ann_recall: AnnRecallSummary::default(),
            index_stats: StoreIndexStats::default(),
            ann_tuning: AnnTuningConfig::default(),
            segment_cache_probe: SegmentCacheProbeSummary::default(),
            wal_scale: None,
        }
    }

    #[test]
    fn large_gate_rejects_low_candidate_reduction() {
        let config = config_with_large_gates(95.0, 120.0);
        let summary = summary_with_profile(BenchmarkProfile::Large, 50_000, 30.0, 50_000, 10_000);
        let err = evaluate_profile_gates(&summary, &config).expect_err("gate should fail");
        assert!(err.contains("candidate reduction"));
    }

    #[test]
    fn large_gate_rejects_high_dash_latency() {
        let config = config_with_large_gates(90.0, 50.0);
        let summary = summary_with_profile(BenchmarkProfile::Large, 50_000, 80.0, 50_000, 2_000);
        let err = evaluate_profile_gates(&summary, &config).expect_err("gate should fail");
        assert!(err.contains("avg latency"));
    }

    #[test]
    fn large_gate_accepts_good_large_profile_metrics() {
        let config = config_with_large_gates(90.0, 80.0);
        let summary = summary_with_profile(BenchmarkProfile::Large, 50_000, 40.0, 50_000, 2_000);
        assert!(evaluate_profile_gates(&summary, &config).is_ok());
    }

    #[test]
    fn xlarge_gate_rejects_low_candidate_reduction() {
        let mut config = config_with_large_gates(90.0, 80.0);
        config.xlarge_min_candidate_reduction_pct = 98.0;
        let summary = summary_with_profile(BenchmarkProfile::XLarge, 100_000, 90.0, 100_000, 4_000);
        let err = evaluate_profile_gates(&summary, &config).expect_err("gate should fail");
        assert!(err.contains("xlarge profile candidate reduction"));
    }

    #[test]
    fn xlarge_gate_rejects_high_dash_latency() {
        let mut config = config_with_large_gates(90.0, 80.0);
        config.xlarge_max_dash_latency_ms = 120.0;
        let summary =
            summary_with_profile(BenchmarkProfile::XLarge, 100_000, 180.0, 100_000, 2_000);
        let err = evaluate_profile_gates(&summary, &config).expect_err("gate should fail");
        assert!(err.contains("xlarge profile DASH avg latency"));
    }

    #[test]
    fn xlarge_gate_accepts_good_metrics() {
        let mut config = config_with_large_gates(90.0, 80.0);
        config.xlarge_min_candidate_reduction_pct = 95.0;
        config.xlarge_max_dash_latency_ms = 220.0;
        let summary =
            summary_with_profile(BenchmarkProfile::XLarge, 100_000, 140.0, 100_000, 2_500);
        assert!(evaluate_profile_gates(&summary, &config).is_ok());
    }

    #[test]
    fn segment_refresh_success_gate_rejects_when_threshold_not_met() {
        let mut config = config_with_large_gates(90.0, 80.0);
        config.min_segment_refresh_successes = 1;
        let summary = summary_with_profile(BenchmarkProfile::Large, 50_000, 40.0, 50_000, 2_000);
        let err = evaluate_profile_gates(&summary, &config).expect_err("gate should fail");
        assert!(err.contains("segment cache refresh successes"));
    }

    #[test]
    fn segment_cache_hit_gate_rejects_when_threshold_not_met() {
        let mut config = config_with_large_gates(90.0, 80.0);
        config.min_segment_cache_hits = 2;
        let mut summary =
            summary_with_profile(BenchmarkProfile::Large, 50_000, 40.0, 50_000, 2_000);
        summary.segment_cache_probe.cache_hits = 1;
        summary.segment_cache_probe.refresh_successes = 1;
        let err = evaluate_profile_gates(&summary, &config).expect_err("gate should fail");
        assert!(err.contains("segment cache hits"));
    }

    #[test]
    fn segment_probe_gates_accept_when_thresholds_met() {
        let mut config = config_with_large_gates(90.0, 80.0);
        config.min_segment_refresh_successes = 1;
        config.min_segment_cache_hits = 1;
        let mut summary =
            summary_with_profile(BenchmarkProfile::Large, 50_000, 40.0, 50_000, 2_000);
        summary.segment_cache_probe.refresh_successes = 1;
        summary.segment_cache_probe.cache_hits = 1;
        assert!(evaluate_profile_gates(&summary, &config).is_ok());
    }

    #[test]
    fn f1_score_returns_expected_value() {
        let score = f1_score(4, 1, 1);
        assert!((score - 0.8).abs() < 0.0001);
    }
}
