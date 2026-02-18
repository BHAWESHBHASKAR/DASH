use ingestion::{
    IngestInput, ingest_document, ingest_document_persistent_with_policy,
    transport::IngestionRuntime, transport::serve_http_with_workers,
};
use schema::{Claim, Evidence, Stance};
use store::{AnnTuningConfig, CheckpointPolicy, FileWal, InMemoryStore, WalWritePolicy};

const SAFE_WAL_SYNC_EVERY_RECORDS_MAX: usize = 256;
const SAFE_WAL_APPEND_BUFFER_RECORDS_MAX: usize = 256;
const SAFE_WAL_SYNC_INTERVAL_MS_MAX: u64 = 5_000;
const DEFAULT_ASYNC_WAL_FLUSH_INTERVAL_MS: u64 = 250;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AsyncWalFlushSetting {
    Auto,
    Disabled,
    IntervalMs(u64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WalDurabilityConfig {
    sync_every_records: usize,
    append_buffer_records: usize,
    sync_interval_ms: Option<u64>,
    async_flush_interval_ms: Option<u64>,
    background_flush_only: bool,
    allow_unsafe_override: bool,
}

fn main() {
    let serve_mode = std::env::args().any(|arg| arg == "--serve");
    let bind_addr = env_with_fallback("DASH_INGEST_BIND", "EME_INGEST_BIND")
        .unwrap_or_else(|| "127.0.0.1:8081".to_string());
    let http_workers = parse_http_workers();
    let transport_runtime = parse_transport_runtime();
    let ann_tuning = parse_ann_tuning_config();
    let segment_dir = env_with_fallback("DASH_INGEST_SEGMENT_DIR", "EME_INGEST_SEGMENT_DIR");
    let wal_sync_every_records = parse_env_with_fallback::<usize>(
        "DASH_INGEST_WAL_SYNC_EVERY_RECORDS",
        "EME_INGEST_WAL_SYNC_EVERY_RECORDS",
    )
    .filter(|value| *value > 0)
    .unwrap_or(1);
    let wal_append_buffer_records = parse_env_with_fallback::<usize>(
        "DASH_INGEST_WAL_APPEND_BUFFER_RECORDS",
        "EME_INGEST_WAL_APPEND_BUFFER_RECORDS",
    )
    .filter(|value| *value > 0)
    .unwrap_or(1);
    let wal_sync_interval_ms = parse_env_with_fallback::<u64>(
        "DASH_INGEST_WAL_SYNC_INTERVAL_MS",
        "EME_INGEST_WAL_SYNC_INTERVAL_MS",
    )
    .filter(|value| *value > 0);
    let allow_unsafe_wal_durability = match parse_bool_env_with_fallback(
        "DASH_INGEST_ALLOW_UNSAFE_WAL_DURABILITY",
        "EME_INGEST_ALLOW_UNSAFE_WAL_DURABILITY",
    ) {
        Ok(value) => value.unwrap_or(false),
        Err(reason) => {
            eprintln!("ingestion invalid WAL durability override env: {reason}");
            std::process::exit(2);
        }
    };
    let wal_background_flush_only = match parse_bool_env_with_fallback(
        "DASH_INGEST_WAL_BACKGROUND_FLUSH_ONLY",
        "EME_INGEST_WAL_BACKGROUND_FLUSH_ONLY",
    ) {
        Ok(value) => value.unwrap_or(false),
        Err(reason) => {
            eprintln!("ingestion invalid WAL background flush env: {reason}");
            std::process::exit(2);
        }
    };
    let wal_async_flush_setting = match parse_wal_async_flush_setting() {
        Ok(value) => value,
        Err(reason) => {
            eprintln!("ingestion invalid WAL async flush env: {reason}");
            std::process::exit(2);
        }
    };

    let input = IngestInput {
        claim: Claim {
            claim_id: "sample-claim".into(),
            tenant_id: "sample-tenant".into(),
            canonical_text: "DASH ingestion service initialized".into(),
            confidence: 0.99,
            event_time_unix: None,
            entities: vec![],
            embedding_ids: vec![],
            claim_type: None,
            valid_from: None,
            valid_to: None,
            created_at: None,
            updated_at: None,
        },
        claim_embedding: None,
        evidence: vec![Evidence {
            evidence_id: "sample-evidence".into(),
            claim_id: "sample-claim".into(),
            source_id: "bootstrap".into(),
            stance: Stance::Supports,
            source_quality: 1.0,
            chunk_id: None,
            span_start: None,
            span_end: None,
            doc_id: None,
            extraction_model: None,
            ingested_at: None,
        }],
        edges: vec![],
    };

    if let Some(wal_path) = env_with_fallback("DASH_INGEST_WAL_PATH", "EME_INGEST_WAL_PATH") {
        let wal_async_flush_interval_ms = resolve_wal_async_flush_interval_ms(
            wal_sync_every_records,
            wal_append_buffer_records,
            wal_sync_interval_ms,
            wal_background_flush_only,
            wal_async_flush_setting,
        );
        let wal_durability_config = WalDurabilityConfig {
            sync_every_records: wal_sync_every_records,
            append_buffer_records: wal_append_buffer_records,
            sync_interval_ms: wal_sync_interval_ms,
            async_flush_interval_ms: wal_async_flush_interval_ms,
            background_flush_only: wal_background_flush_only,
            allow_unsafe_override: allow_unsafe_wal_durability,
        };
        if let Err(reason) = validate_wal_durability_guardrails(&wal_durability_config) {
            eprintln!("ingestion WAL durability config rejected: {reason}");
            std::process::exit(2);
        }
        let guardrail_violations = wal_durability_guardrail_violations(
            wal_durability_config.sync_every_records,
            wal_durability_config.append_buffer_records,
            wal_durability_config.sync_interval_ms,
            wal_durability_config.async_flush_interval_ms,
            wal_durability_config.background_flush_only,
        );
        if allow_unsafe_wal_durability && !guardrail_violations.is_empty() {
            eprintln!(
                "ingestion WAL durability guardrails overridden via DASH_INGEST_ALLOW_UNSAFE_WAL_DURABILITY=true: {}",
                guardrail_violations.join("; ")
            );
        }

        let mut wal = match FileWal::open_with_policy(
            &wal_path,
            WalWritePolicy {
                sync_every_records: wal_sync_every_records,
                append_buffer_max_records: wal_append_buffer_records,
                sync_interval: wal_sync_interval_ms.map(std::time::Duration::from_millis),
                background_flush_only: wal_background_flush_only,
            },
        ) {
            Ok(wal) => wal,
            Err(err) => {
                eprintln!("ingestion failed opening WAL '{wal_path}': {err:?}");
                std::process::exit(1);
            }
        };
        let (mut store, load_stats) = match InMemoryStore::load_from_wal_with_stats_and_ann_tuning(
            &wal,
            ann_tuning.clone(),
        ) {
            Ok(result) => result,
            Err(err) => {
                eprintln!("ingestion failed replaying WAL '{wal_path}': {err:?}");
                std::process::exit(1);
            }
        };
        println!(
            "ingestion startup replay: claims_loaded={}, evidence_loaded={}, edges_loaded={}, vectors_loaded={}, snapshot_records={}, wal_delta_records={}",
            load_stats.claims_loaded,
            load_stats.evidence_loaded,
            load_stats.edges_loaded,
            load_stats.vectors_loaded,
            load_stats.replay.snapshot_records,
            load_stats.replay.wal_records
        );
        println!(
            "ingestion wal durability: sync_every_records={}, append_buffer_records={}, sync_interval_ms={}, async_flush_interval_ms={}, background_flush_only={}, unsafe_override={}",
            wal.sync_every_records(),
            wal.append_buffer_max_records(),
            wal.sync_interval()
                .map(|value| value.as_millis())
                .unwrap_or(0),
            wal_async_flush_interval_ms.unwrap_or(0),
            wal.background_flush_only(),
            allow_unsafe_wal_durability
        );
        let policy = CheckpointPolicy {
            max_wal_records: parse_env_with_fallback::<usize>(
                "DASH_CHECKPOINT_MAX_WAL_RECORDS",
                "EME_CHECKPOINT_MAX_WAL_RECORDS",
            ),
            max_wal_bytes: parse_env_with_fallback::<u64>(
                "DASH_CHECKPOINT_MAX_WAL_BYTES",
                "EME_CHECKPOINT_MAX_WAL_BYTES",
            ),
        };

        if serve_mode {
            println!("ingestion transport listening on http://{bind_addr}");
            println!("ingestion transport workers: {http_workers}");
            println!(
                "ingestion transport runtime: {}",
                transport_runtime.as_str()
            );
            println!(
                "ingestion ann tuning: base_neighbors={}, upper_neighbors={}, search_factor={}, search_min={}, search_max={}",
                store.ann_tuning().max_neighbors_base,
                store.ann_tuning().max_neighbors_upper,
                store.ann_tuning().search_expansion_factor,
                store.ann_tuning().search_expansion_min,
                store.ann_tuning().search_expansion_max
            );
            println!("ingestion health endpoint: http://{bind_addr}/health");
            println!("ingestion metrics endpoint: http://{bind_addr}/metrics");
            println!("ingestion placement debug endpoint: http://{bind_addr}/debug/placement");
            println!("ingestion API endpoint: http://{bind_addr}/v1/ingest");
            if let Some(segment_dir) = segment_dir.as_deref() {
                println!("ingestion segment publish dir: {segment_dir}");
            }
            let runtime = IngestionRuntime::persistent(store, wal, policy);
            if let Some(reason) = runtime.placement_routing_error() {
                eprintln!("ingestion placement routing configuration error: {reason}");
                std::process::exit(2);
            }
            if let Some(summary) = runtime.placement_routing_summary() {
                println!("ingestion placement routing: {summary}");
            }
            match transport_runtime {
                TransportRuntime::Std => {
                    if let Err(err) = serve_http_with_workers(runtime, &bind_addr, http_workers) {
                        eprintln!("ingestion transport failed: {err}");
                        std::process::exit(1);
                    }
                }
                TransportRuntime::Axum => {
                    #[cfg(feature = "async-transport")]
                    {
                        if let Err(err) = ingestion::transport_axum::serve_http_with_axum(
                            runtime,
                            &bind_addr,
                            http_workers,
                        ) {
                            eprintln!("ingestion transport failed: {err}");
                            std::process::exit(1);
                        }
                    }
                    #[cfg(not(feature = "async-transport"))]
                    {
                        eprintln!(
                            "ingestion transport runtime 'axum' requires build feature 'async-transport'"
                        );
                        std::process::exit(2);
                    }
                }
            }
        } else {
            match ingest_document_persistent_with_policy(&mut store, &mut wal, &policy, input) {
                Ok(Some(stats)) => println!(
                    "ingestion ready: claims={}, wal={}, snapshot={}, checkpoint_records={}, truncated_wal_records={}",
                    store.claims_len(),
                    wal.path().display(),
                    wal.snapshot_path().display(),
                    stats.snapshot_records,
                    stats.truncated_wal_records
                ),
                Ok(None) => println!(
                    "ingestion ready: claims={}, wal={}, checkpoint_triggered=false",
                    store.claims_len(),
                    wal.path().display()
                ),
                Err(err) => eprintln!("ingestion failed: {err:?}"),
            }
        }
    } else {
        let store = InMemoryStore::new_with_ann_tuning(ann_tuning);
        if serve_mode {
            println!("ingestion transport listening on http://{bind_addr}");
            println!("ingestion transport workers: {http_workers}");
            println!(
                "ingestion transport runtime: {}",
                transport_runtime.as_str()
            );
            println!(
                "ingestion ann tuning: base_neighbors={}, upper_neighbors={}, search_factor={}, search_min={}, search_max={}",
                store.ann_tuning().max_neighbors_base,
                store.ann_tuning().max_neighbors_upper,
                store.ann_tuning().search_expansion_factor,
                store.ann_tuning().search_expansion_min,
                store.ann_tuning().search_expansion_max
            );
            println!("ingestion health endpoint: http://{bind_addr}/health");
            println!("ingestion metrics endpoint: http://{bind_addr}/metrics");
            println!("ingestion placement debug endpoint: http://{bind_addr}/debug/placement");
            println!("ingestion API endpoint: http://{bind_addr}/v1/ingest");
            if let Some(segment_dir) = segment_dir.as_deref() {
                println!("ingestion segment publish dir: {segment_dir}");
            }
            let runtime = IngestionRuntime::in_memory(store);
            if let Some(reason) = runtime.placement_routing_error() {
                eprintln!("ingestion placement routing configuration error: {reason}");
                std::process::exit(2);
            }
            if let Some(summary) = runtime.placement_routing_summary() {
                println!("ingestion placement routing: {summary}");
            }
            match transport_runtime {
                TransportRuntime::Std => {
                    if let Err(err) = serve_http_with_workers(runtime, &bind_addr, http_workers) {
                        eprintln!("ingestion transport failed: {err}");
                        std::process::exit(1);
                    }
                }
                TransportRuntime::Axum => {
                    #[cfg(feature = "async-transport")]
                    {
                        if let Err(err) = ingestion::transport_axum::serve_http_with_axum(
                            runtime,
                            &bind_addr,
                            http_workers,
                        ) {
                            eprintln!("ingestion transport failed: {err}");
                            std::process::exit(1);
                        }
                    }
                    #[cfg(not(feature = "async-transport"))]
                    {
                        eprintln!(
                            "ingestion transport runtime 'axum' requires build feature 'async-transport'"
                        );
                        std::process::exit(2);
                    }
                }
            }
        } else {
            let mut store = store;
            match ingest_document(&mut store, input) {
                Ok(()) => println!(
                    "ingestion ready: claims={} (set DASH_INGEST_WAL_PATH for persistent mode)",
                    store.claims_len()
                ),
                Err(err) => eprintln!("ingestion failed: {err:?}"),
            }
        }
    }
}

fn env_with_fallback(primary: &str, fallback: &str) -> Option<String> {
    std::env::var(primary)
        .ok()
        .or_else(|| std::env::var(fallback).ok())
}

fn parse_env_with_fallback<T>(primary: &str, fallback: &str) -> Option<T>
where
    T: std::str::FromStr,
{
    env_with_fallback(primary, fallback).and_then(|value| value.parse::<T>().ok())
}

fn parse_bool_env_with_fallback(primary: &str, fallback: &str) -> Result<Option<bool>, String> {
    let Some(raw) = env_with_fallback(primary, fallback) else {
        return Ok(None);
    };
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(Some(true)),
        "0" | "false" | "no" | "off" => Ok(Some(false)),
        _ => Err(format!(
            "{primary}/{fallback} must be one of: true,false,1,0,yes,no,on,off (got '{raw}')"
        )),
    }
}

fn parse_wal_async_flush_setting() -> Result<AsyncWalFlushSetting, String> {
    let Some(raw) = env_with_fallback(
        "DASH_INGEST_WAL_ASYNC_FLUSH_INTERVAL_MS",
        "EME_INGEST_WAL_ASYNC_FLUSH_INTERVAL_MS",
    ) else {
        return Ok(AsyncWalFlushSetting::Auto);
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(AsyncWalFlushSetting::Disabled);
    }
    let normalized = trimmed.to_ascii_lowercase();
    if normalized == "auto" {
        return Ok(AsyncWalFlushSetting::Auto);
    }
    if matches!(
        normalized.as_str(),
        "off" | "none" | "false" | "disabled" | "0"
    ) {
        return Ok(AsyncWalFlushSetting::Disabled);
    }
    match trimmed.parse::<u64>() {
        Ok(value) if value > 0 => Ok(AsyncWalFlushSetting::IntervalMs(value)),
        _ => Err(format!(
            "DASH_INGEST_WAL_ASYNC_FLUSH_INTERVAL_MS/EME_INGEST_WAL_ASYNC_FLUSH_INTERVAL_MS must be positive integer milliseconds, auto, or off (got '{raw}')"
        )),
    }
}

fn resolve_wal_async_flush_interval_ms(
    sync_every_records: usize,
    append_buffer_records: usize,
    sync_interval_ms: Option<u64>,
    background_flush_only: bool,
    setting: AsyncWalFlushSetting,
) -> Option<u64> {
    match setting {
        AsyncWalFlushSetting::Disabled => None,
        AsyncWalFlushSetting::IntervalMs(value) => Some(value),
        AsyncWalFlushSetting::Auto => {
            let batching_enabled = background_flush_only
                || sync_every_records > 1
                || append_buffer_records > 1
                || sync_interval_ms.is_some();
            if !batching_enabled {
                return None;
            }
            Some(
                sync_interval_ms
                    .map(|value| value.min(DEFAULT_ASYNC_WAL_FLUSH_INTERVAL_MS))
                    .unwrap_or(DEFAULT_ASYNC_WAL_FLUSH_INTERVAL_MS),
            )
        }
    }
}

fn wal_durability_guardrail_violations(
    sync_every_records: usize,
    append_buffer_records: usize,
    sync_interval_ms: Option<u64>,
    async_flush_interval_ms: Option<u64>,
    background_flush_only: bool,
) -> Vec<String> {
    let mut violations = Vec::new();
    if sync_every_records > SAFE_WAL_SYNC_EVERY_RECORDS_MAX {
        violations.push(format!(
            "DASH_INGEST_WAL_SYNC_EVERY_RECORDS={} exceeds safe max {}",
            sync_every_records, SAFE_WAL_SYNC_EVERY_RECORDS_MAX
        ));
    }
    if append_buffer_records > SAFE_WAL_APPEND_BUFFER_RECORDS_MAX {
        violations.push(format!(
            "DASH_INGEST_WAL_APPEND_BUFFER_RECORDS={} exceeds safe max {}",
            append_buffer_records, SAFE_WAL_APPEND_BUFFER_RECORDS_MAX
        ));
    }
    if let Some(interval_ms) = sync_interval_ms
        && interval_ms > SAFE_WAL_SYNC_INTERVAL_MS_MAX
    {
        violations.push(format!(
            "DASH_INGEST_WAL_SYNC_INTERVAL_MS={} exceeds safe max {}",
            interval_ms, SAFE_WAL_SYNC_INTERVAL_MS_MAX
        ));
    }
    if let Some(interval_ms) = async_flush_interval_ms
        && interval_ms > SAFE_WAL_SYNC_INTERVAL_MS_MAX
    {
        violations.push(format!(
            "DASH_INGEST_WAL_ASYNC_FLUSH_INTERVAL_MS={} exceeds safe max {}",
            interval_ms, SAFE_WAL_SYNC_INTERVAL_MS_MAX
        ));
    }
    let request_thread_batching_enabled =
        !background_flush_only && (sync_every_records > 1 || append_buffer_records > 1);
    if request_thread_batching_enabled && sync_interval_ms.is_none() {
        violations.push(
            "batched WAL durability requires DASH_INGEST_WAL_SYNC_INTERVAL_MS to cap unsynced window"
                .to_string(),
        );
    }
    if background_flush_only && async_flush_interval_ms.is_none() {
        violations.push(
            "DASH_INGEST_WAL_BACKGROUND_FLUSH_ONLY=true requires async flush worker (set DASH_INGEST_WAL_ASYNC_FLUSH_INTERVAL_MS to a positive value or keep auto)"
                .to_string(),
        );
    }
    violations
}

fn validate_wal_durability_guardrails(config: &WalDurabilityConfig) -> Result<(), String> {
    let violations = wal_durability_guardrail_violations(
        config.sync_every_records,
        config.append_buffer_records,
        config.sync_interval_ms,
        config.async_flush_interval_ms,
        config.background_flush_only,
    );
    if config.allow_unsafe_override || violations.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "{} (set DASH_INGEST_ALLOW_UNSAFE_WAL_DURABILITY=true only for controlled stress testing)",
            violations.join("; ")
        ))
    }
}

fn parse_ann_tuning_config() -> AnnTuningConfig {
    let defaults = AnnTuningConfig::default();
    AnnTuningConfig {
        max_neighbors_base: parse_env_first::<usize>(&[
            "DASH_INGEST_ANN_MAX_NEIGHBORS_BASE",
            "DASH_ANN_MAX_NEIGHBORS_BASE",
            "EME_INGEST_ANN_MAX_NEIGHBORS_BASE",
            "EME_ANN_MAX_NEIGHBORS_BASE",
        ])
        .filter(|value| *value > 0)
        .unwrap_or(defaults.max_neighbors_base),
        max_neighbors_upper: parse_env_first::<usize>(&[
            "DASH_INGEST_ANN_MAX_NEIGHBORS_UPPER",
            "DASH_ANN_MAX_NEIGHBORS_UPPER",
            "EME_INGEST_ANN_MAX_NEIGHBORS_UPPER",
            "EME_ANN_MAX_NEIGHBORS_UPPER",
        ])
        .filter(|value| *value > 0)
        .unwrap_or(defaults.max_neighbors_upper),
        search_expansion_factor: parse_env_first::<usize>(&[
            "DASH_INGEST_ANN_SEARCH_EXPANSION_FACTOR",
            "DASH_ANN_SEARCH_EXPANSION_FACTOR",
            "EME_INGEST_ANN_SEARCH_EXPANSION_FACTOR",
            "EME_ANN_SEARCH_EXPANSION_FACTOR",
        ])
        .filter(|value| *value > 0)
        .unwrap_or(defaults.search_expansion_factor),
        search_expansion_min: parse_env_first::<usize>(&[
            "DASH_INGEST_ANN_SEARCH_EXPANSION_MIN",
            "DASH_ANN_SEARCH_EXPANSION_MIN",
            "EME_INGEST_ANN_SEARCH_EXPANSION_MIN",
            "EME_ANN_SEARCH_EXPANSION_MIN",
        ])
        .filter(|value| *value > 0)
        .unwrap_or(defaults.search_expansion_min),
        search_expansion_max: parse_env_first::<usize>(&[
            "DASH_INGEST_ANN_SEARCH_EXPANSION_MAX",
            "DASH_ANN_SEARCH_EXPANSION_MAX",
            "EME_INGEST_ANN_SEARCH_EXPANSION_MAX",
            "EME_ANN_SEARCH_EXPANSION_MAX",
        ])
        .filter(|value| *value > 0)
        .unwrap_or(defaults.search_expansion_max),
    }
}

fn parse_env_first<T>(keys: &[&str]) -> Option<T>
where
    T: std::str::FromStr,
{
    for key in keys {
        if let Ok(value) = std::env::var(key)
            && let Ok(parsed) = value.parse::<T>()
        {
            return Some(parsed);
        }
    }
    None
}

fn parse_http_workers() -> usize {
    parse_env_with_fallback::<usize>("DASH_INGEST_HTTP_WORKERS", "EME_INGEST_HTTP_WORKERS")
        .filter(|workers| *workers > 0)
        .unwrap_or_else(default_http_workers)
}

fn default_http_workers() -> usize {
    std::thread::available_parallelism()
        .map(|parallelism| parallelism.get().clamp(1, 32))
        .unwrap_or(4)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TransportRuntime {
    Std,
    Axum,
}

impl TransportRuntime {
    fn as_str(self) -> &'static str {
        match self {
            Self::Std => "std",
            Self::Axum => "axum",
        }
    }
}

fn parse_transport_runtime() -> TransportRuntime {
    let runtime_raw = env_with_fallback(
        "DASH_INGEST_TRANSPORT_RUNTIME",
        "EME_INGEST_TRANSPORT_RUNTIME",
    );
    match runtime_raw.as_deref() {
        Some("axum") => TransportRuntime::Axum,
        _ => TransportRuntime::Std,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wal_durability_guardrails_accept_strict_defaults() {
        let config = WalDurabilityConfig {
            sync_every_records: 1,
            append_buffer_records: 1,
            sync_interval_ms: None,
            async_flush_interval_ms: None,
            background_flush_only: false,
            allow_unsafe_override: false,
        };
        assert!(validate_wal_durability_guardrails(&config).is_ok());
    }

    #[test]
    fn wal_durability_guardrails_reject_batching_without_interval() {
        let config = WalDurabilityConfig {
            sync_every_records: 32,
            append_buffer_records: 1,
            sync_interval_ms: None,
            async_flush_interval_ms: Some(250),
            background_flush_only: false,
            allow_unsafe_override: false,
        };
        let err = validate_wal_durability_guardrails(&config).expect_err("must reject");
        assert!(err.contains("batched WAL durability requires"));
    }

    #[test]
    fn wal_durability_guardrails_allow_unsafe_override() {
        let config = WalDurabilityConfig {
            sync_every_records: 512,
            append_buffer_records: 512,
            sync_interval_ms: None,
            async_flush_interval_ms: None,
            background_flush_only: false,
            allow_unsafe_override: true,
        };
        assert!(validate_wal_durability_guardrails(&config).is_ok());
    }

    #[test]
    fn wal_durability_guardrails_reject_interval_above_safe_max() {
        let config = WalDurabilityConfig {
            sync_every_records: 2,
            append_buffer_records: 2,
            sync_interval_ms: Some(SAFE_WAL_SYNC_INTERVAL_MS_MAX + 1),
            async_flush_interval_ms: Some(250),
            background_flush_only: false,
            allow_unsafe_override: false,
        };
        let err = validate_wal_durability_guardrails(&config).expect_err("must reject");
        assert!(err.contains("DASH_INGEST_WAL_SYNC_INTERVAL_MS"));
    }

    #[test]
    fn wal_durability_guardrails_reject_background_flush_without_async_worker() {
        let config = WalDurabilityConfig {
            sync_every_records: 1,
            append_buffer_records: 1,
            sync_interval_ms: None,
            async_flush_interval_ms: None,
            background_flush_only: true,
            allow_unsafe_override: false,
        };
        let err = validate_wal_durability_guardrails(&config).expect_err("must reject");
        assert!(err.contains("DASH_INGEST_WAL_BACKGROUND_FLUSH_ONLY"));
    }

    #[test]
    fn wal_durability_guardrails_allow_background_flush_with_async_worker() {
        let config = WalDurabilityConfig {
            sync_every_records: 1,
            append_buffer_records: 1,
            sync_interval_ms: None,
            async_flush_interval_ms: Some(250),
            background_flush_only: true,
            allow_unsafe_override: false,
        };
        assert!(validate_wal_durability_guardrails(&config).is_ok());
    }

    #[test]
    fn resolve_wal_async_flush_interval_auto_enables_for_background_mode() {
        let resolved =
            resolve_wal_async_flush_interval_ms(1, 1, None, true, AsyncWalFlushSetting::Auto);
        assert_eq!(resolved, Some(DEFAULT_ASYNC_WAL_FLUSH_INTERVAL_MS));
    }
}
