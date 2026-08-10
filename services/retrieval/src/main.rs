use std::sync::{Arc, RwLock};

use retrieval::{
    replication::spawn_replication_follower, retrieve_for_rag, transport::serve_http_with_workers,
};
use schema::{Claim, Evidence, RetrievalRequest, Stance, StanceMode};
use store::{AnnTuningConfig, FileWal, InMemoryStore};

fn main() {
    dash_common::init_logging();
    // Default to serve mode (this is a server binary; the CLI
    // mode is for smoke tests and one-shot benchmarks). Pass
    // `--cli` or `--no-serve` to run the one-shot path without
    // binding to a TCP port. The previous behavior (require
    // `--serve` to start the server) was a footgun: most users
    // assumed the default was to serve, and the service would
    // silently exit after printing the startup banner.
    let serve_mode = !std::env::args().any(|arg| arg == "--cli" || arg == "--no-serve");
    let bind_addr = env_with_fallback("DASH_RETRIEVAL_BIND", "EME_RETRIEVAL_BIND")
        .unwrap_or_else(|| "127.0.0.1:8080".to_string());
    let http_workers = parse_http_workers();
    let ann_tuning = parse_ann_tuning_config();
    let segment_dir = env_with_fallback("DASH_RETRIEVAL_SEGMENT_DIR", "EME_RETRIEVAL_SEGMENT_DIR");

    let encryption_provider = match encryption::provider_from_env() {
        Ok(provider) => provider,
        Err(err) => {
            tracing::error!("retrieval failed to load encryption provider: {err:?}");
            std::process::exit(1);
        }
    };

    if let Err(reason) = validate_startup_secrets() {
        if dash_common::strict_secrets_enabled() {
            tracing::error!("retrieval startup secret validation failed: {reason}");
            std::process::exit(2);
        } else {
            tracing::error!(
                "retrieval startup warning: {reason} (set DASH_STRICT_SECRETS=1 to fail)"
            );
        }
    }

    let disk_disabled = env_with_fallback(
        "DASH_RETRIEVAL_PERSISTENCE_DISABLE",
        "EME_RETRIEVAL_PERSISTENCE_DISABLE",
    )
    .as_deref()
        == Some("1");
    let disk_path = env_with_fallback(
        "DASH_RETRIEVAL_PERSISTENCE_PATH",
        "EME_RETRIEVAL_PERSISTENCE_PATH",
    )
    .unwrap_or_else(|| "./data/dash-retrieval.redb".to_string());

    let store = if let Some(wal_path) =
        env_with_fallback("DASH_RETRIEVAL_WAL_PATH", "EME_RETRIEVAL_WAL_PATH")
    {
        let wal = match FileWal::open(&wal_path) {
            Ok(wal) => wal.with_encryption(Arc::clone(&encryption_provider)),
            Err(err) => {
                tracing::error!("retrieval failed opening WAL '{wal_path}': {err:?}");
                std::process::exit(1);
            }
        };
        let (mut store, load_stats) = match InMemoryStore::load_from_wal_with_stats_and_ann_tuning(
            &wal,
            ann_tuning.clone(),
        ) {
            Ok(result) => result,
            Err(err) => {
                tracing::error!("retrieval failed replaying WAL '{wal_path}': {err:?}");
                std::process::exit(1);
            }
        };
        tracing::info!(
            "retrieval startup replay: claims_loaded={}, evidence_loaded={}, edges_loaded={}, vectors_loaded={}, snapshot_records={}, wal_delta_records={}",
            load_stats.claims_loaded,
            load_stats.evidence_loaded,
            load_stats.edges_loaded,
            load_stats.vectors_loaded,
            load_stats.replay.snapshot_records,
            load_stats.replay.wal_records
        );
        store = store.with_encryption(Arc::clone(&encryption_provider));
        if !disk_disabled {
            store = attach_disk(store, &disk_path);
        }
        tracing::info!("retrieval ready: claims={}", store.claims_len());
        store
    } else {
        let mut store = InMemoryStore::new_with_ann_tuning(ann_tuning)
            .with_encryption(Arc::clone(&encryption_provider));
        store
            .ingest_bundle(
                Claim {
                    claim_id: "sample-claim".into(),
                    tenant_id: "sample-tenant".into(),
                    canonical_text: "DASH retrieval service initialized".into(),
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
                vec![],
            )
            .expect("sample ingest should succeed");

        let results = retrieve_for_rag(
            &store,
            RetrievalRequest {
                tenant_id: "sample-tenant".into(),
                query: "retrieval initialized".into(),
                top_k: 5,
                stance_mode: StanceMode::Balanced,
            },
        );
        tracing::info!("retrieval ready: results={}", results.len());
        if !disk_disabled {
            store = attach_disk(store, &disk_path);
        }
        store
    };

    let shared_store = Arc::new(RwLock::new(store));
    spawn_replication_follower(Arc::clone(&shared_store));

    if serve_mode {
        {
            let store_guard = shared_store.read().unwrap_or_else(|p| p.into_inner());
            tracing::info!("retrieval transport listening on http://{bind_addr}");
            tracing::info!("retrieval transport workers: {http_workers}");
            tracing::info!(
                "retrieval ann tuning: base_neighbors={}, upper_neighbors={}, search_factor={}, search_min={}, search_max={}",
                store_guard.ann_tuning().max_neighbors_base,
                store_guard.ann_tuning().max_neighbors_upper,
                store_guard.ann_tuning().search_expansion_factor,
                store_guard.ann_tuning().search_expansion_min,
                store_guard.ann_tuning().search_expansion_max
            );
            tracing::info!(
                "retrieval vector backend: {}",
                store_guard.vector_backend_label()
            );
            if let Some(segment_dir) = segment_dir.as_deref() {
                tracing::info!("retrieval segment read dir: {segment_dir}");
            }
        }
        if let Some(placement_file) =
            env_with_fallback("DASH_ROUTER_PLACEMENT_FILE", "EME_ROUTER_PLACEMENT_FILE")
        {
            let local_node =
                env_with_fallback("DASH_ROUTER_LOCAL_NODE_ID", "EME_ROUTER_LOCAL_NODE_ID")
                    .or_else(|| env_with_fallback("DASH_NODE_ID", "EME_NODE_ID"))
                    .unwrap_or_else(|| "<unset>".to_string());
            let read_preference =
                env_with_fallback("DASH_ROUTER_READ_PREFERENCE", "EME_ROUTER_READ_PREFERENCE")
                    .unwrap_or_else(|| "any_healthy".to_string());
            let reload_interval_ms = env_with_fallback(
                "DASH_ROUTER_PLACEMENT_RELOAD_INTERVAL_MS",
                "EME_ROUTER_PLACEMENT_RELOAD_INTERVAL_MS",
            )
            .unwrap_or_else(|| "0".to_string());
            tracing::info!(
                "retrieval placement routing: file={}, local_node_id={}, read_preference={}, reload_interval_ms={}",
                placement_file,
                local_node,
                read_preference,
                reload_interval_ms
            );
        }
        tracing::info!("retrieval health endpoint: http://{bind_addr}/health");
        tracing::info!("retrieval metrics endpoint: http://{bind_addr}/metrics");
        tracing::info!("retrieval placement debug endpoint: http://{bind_addr}/debug/placement");
        // Install SIGTERM/SIGINT handlers that set a flag the
        // accept loop polls every 50ms. This gives us sub-second
        // graceful shutdown: in-flight requests drain, the worker
        // threads finish, then the process exits cleanly.
        let shutdown = dash_common::ShutdownSignal::install();
        tracing::error!("retrieval: serving on http://{bind_addr} (--cli to run without a port)");
        if let Err(err) = serve_http_with_workers(shared_store, &bind_addr, http_workers, shutdown)
        {
            tracing::error!("retrieval transport failed: {err}");
            std::process::exit(1);
        }
    }
}

fn env_with_fallback(primary: &str, fallback: &str) -> Option<String> {
    std::env::var(primary)
        .ok()
        .or_else(|| std::env::var(fallback).ok())
}

fn validate_startup_secrets() -> Result<(), String> {
    let api_key = env_with_fallback("DASH_RETRIEVAL_API_KEY", "EME_RETRIEVAL_API_KEY");
    let api_keys = env_with_fallback("DASH_RETRIEVAL_API_KEYS", "EME_RETRIEVAL_API_KEYS");
    let jwt_secret = env_with_fallback(
        "DASH_RETRIEVAL_JWT_HS256_SECRET",
        "EME_RETRIEVAL_JWT_HS256_SECRET",
    );
    let jwt_secrets = env_with_fallback(
        "DASH_RETRIEVAL_JWT_HS256_SECRETS",
        "EME_RETRIEVAL_JWT_HS256_SECRETS",
    );

    if let Some(value) = api_key.as_deref() {
        dash_common::validate_secret(value, "DASH_RETRIEVAL_API_KEY")?;
    }
    if let Some(value) = api_keys.as_deref() {
        dash_common::validate_secret_csv(Some(value), "DASH_RETRIEVAL_API_KEYS")?;
    }
    if let Some(value) = jwt_secret.as_deref() {
        dash_common::validate_secret(value, "DASH_RETRIEVAL_JWT_HS256_SECRET")?;
    }
    if let Some(value) = jwt_secrets.as_deref() {
        dash_common::validate_secret_csv(Some(value), "DASH_RETRIEVAL_JWT_HS256_SECRETS")?;
    }

    if dash_common::strict_secrets_enabled() && api_key.is_none() && api_keys.is_none() {
        return Err(
            "DASH_STRICT_SECRETS=1 requires at least one retrieval API key (DASH_RETRIEVAL_API_KEY or DASH_RETRIEVAL_API_KEYS)"
                .into(),
        );
    }

    Ok(())
}

fn parse_http_workers() -> usize {
    parse_env_with_fallback::<usize>("DASH_RETRIEVAL_HTTP_WORKERS", "EME_RETRIEVAL_HTTP_WORKERS")
        .filter(|workers| *workers > 0)
        .unwrap_or_else(default_http_workers)
}

fn default_http_workers() -> usize {
    std::thread::available_parallelism()
        .map(|parallelism| parallelism.get().clamp(1, 32))
        .unwrap_or(4)
}

fn parse_env_with_fallback<T>(primary: &str, fallback: &str) -> Option<T>
where
    T: std::str::FromStr,
{
    env_with_fallback(primary, fallback).and_then(|value| value.parse::<T>().ok())
}

fn parse_ann_tuning_config() -> AnnTuningConfig {
    let defaults = AnnTuningConfig::default();
    AnnTuningConfig {
        max_neighbors_base: parse_env_first::<usize>(&[
            "DASH_RETRIEVAL_ANN_MAX_NEIGHBORS_BASE",
            "DASH_ANN_MAX_NEIGHBORS_BASE",
            "EME_RETRIEVAL_ANN_MAX_NEIGHBORS_BASE",
            "EME_ANN_MAX_NEIGHBORS_BASE",
        ])
        .filter(|value| *value > 0)
        .unwrap_or(defaults.max_neighbors_base),
        max_neighbors_upper: parse_env_first::<usize>(&[
            "DASH_RETRIEVAL_ANN_MAX_NEIGHBORS_UPPER",
            "DASH_ANN_MAX_NEIGHBORS_UPPER",
            "EME_RETRIEVAL_ANN_MAX_NEIGHBORS_UPPER",
            "EME_ANN_MAX_NEIGHBORS_UPPER",
        ])
        .filter(|value| *value > 0)
        .unwrap_or(defaults.max_neighbors_upper),
        search_expansion_factor: parse_env_first::<usize>(&[
            "DASH_RETRIEVAL_ANN_SEARCH_EXPANSION_FACTOR",
            "DASH_ANN_SEARCH_EXPANSION_FACTOR",
            "EME_RETRIEVAL_ANN_SEARCH_EXPANSION_FACTOR",
            "EME_ANN_SEARCH_EXPANSION_FACTOR",
        ])
        .filter(|value| *value > 0)
        .unwrap_or(defaults.search_expansion_factor),
        search_expansion_min: parse_env_first::<usize>(&[
            "DASH_RETRIEVAL_ANN_SEARCH_EXPANSION_MIN",
            "DASH_ANN_SEARCH_EXPANSION_MIN",
            "EME_RETRIEVAL_ANN_SEARCH_EXPANSION_MIN",
            "EME_ANN_SEARCH_EXPANSION_MIN",
        ])
        .filter(|value| *value > 0)
        .unwrap_or(defaults.search_expansion_min),
        search_expansion_max: parse_env_first::<usize>(&[
            "DASH_RETRIEVAL_ANN_SEARCH_EXPANSION_MAX",
            "DASH_ANN_SEARCH_EXPANSION_MAX",
            "EME_RETRIEVAL_ANN_SEARCH_EXPANSION_MAX",
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

fn attach_disk(store: InMemoryStore, disk_path: &str) -> InMemoryStore {
    match store.with_disk(disk_path) {
        Ok(updated) => {
            match updated.disk_status() {
                store::DiskStatus::Unavailable { reason } => {
                    tracing::error!(
                        "retrieval redb open failed for '{disk_path}': {reason}; falling back to in-memory mode"
                    );
                }
                _ => {
                    tracing::info!("retrieval persistence: disk={disk_path}");
                }
            }
            updated
        }
        Err(err) => {
            tracing::error!(
                "retrieval redb open failed for '{disk_path}': {err}; falling back to in-memory mode"
            );
            unreachable!("with_disk always returns Ok")
        }
    }
}
