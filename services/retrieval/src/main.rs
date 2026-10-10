use std::sync::{Arc, RwLock};
use std::time::Duration;

use retrieval::{
    replication::spawn_replication_follower, retrieve_for_rag, transport::serve_http_with_workers,
};
use schema::{Claim, Evidence, RetrievalRequest, Stance, StanceMode};
use store::{
    AnnTuningConfig, FileWal, InMemoryStore, ReplayPolicy, VectorIndexPersistence,
    VectorIndexRestore,
};

/// Default `DASH_RETRIEVAL_VECTOR_INDEX_SAVE_INTERVAL_MS`.
const DEFAULT_VECTOR_INDEX_SAVE_INTERVAL_MS: u64 = 300_000;

fn main() {
    dash_common::init_logging();
    dash_observe::process::mark_start();
    dash_config::startup_check(dash_config::Service::Retrieval);
    // Default to serve mode (this is a server binary; the CLI
    // mode is for smoke tests and one-shot benchmarks). Pass
    // `--cli` or `--no-serve` to run the one-shot path without
    // binding to a TCP port. The previous behavior (require
    // `--serve` to start the server) was a footgun: most users
    // assumed the default was to serve, and the service would
    // silently exit after printing the startup banner.
    let serve_mode = !std::env::args().any(|arg| arg == "--cli" || arg == "--no-serve");
    let requested_bind = env_with_fallback("DASH_RETRIEVAL_BIND", "EME_RETRIEVAL_BIND")
        .unwrap_or_else(|| "127.0.0.1:8080".to_string());
    // Dev mode only ever listens on loopback (see dash_common::resolve_bind_addr).
    let bind_addr = dash_common::resolve_bind_addr(&requested_bind);
    let http_workers = parse_http_workers();
    let ann_tuning = parse_ann_tuning_config();
    let segment_dir = env_with_fallback("DASH_RETRIEVAL_SEGMENT_DIR", "EME_RETRIEVAL_SEGMENT_DIR");

    // Fail closed: refuse to start without usable authentication unless
    // DASH_INSECURE_DEV_MODE=1 is set explicitly. The validated policy is
    // built once here and shared by every request.
    if serve_mode && let Err(reason) = retrieval::transport::initialize_auth_policy() {
        tracing::error!("retrieval startup refused: {reason}");
        std::process::exit(2);
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

    {
        let wal_path = env_with_fallback("DASH_RETRIEVAL_WAL_PATH", "EME_RETRIEVAL_WAL_PATH");
        init_encryption(
            "retrieval",
            store::EncryptionStatePaths {
                vector_index: wal_path
                    .as_deref()
                    .and_then(parse_vector_index_persistence)
                    .map(|p| p.path().to_path_buf()),
                redb: (!disk_disabled && wal_path.is_some())
                    .then(|| std::path::PathBuf::from(&disk_path)),
                wal: wal_path.map(std::path::PathBuf::from),
                segment_dirs: segment_dir.iter().map(std::path::PathBuf::from).collect(),
            },
        );
    }

    let mut follower_wal: Option<FileWal> = None;
    let mut vector_index_persistence: Option<Arc<VectorIndexPersistence>> = None;
    let store = if let Some(wal_path) =
        env_with_fallback("DASH_RETRIEVAL_WAL_PATH", "EME_RETRIEVAL_WAL_PATH")
    {
        let wal = match FileWal::open(&wal_path) {
            Ok(wal) => wal,
            Err(err) => {
                tracing::error!("retrieval failed opening WAL '{wal_path}': {err:?}");
                std::process::exit(1);
            }
        };
        vector_index_persistence = parse_vector_index_persistence(&wal_path);
        let (mut store, load_stats) = match InMemoryStore::load_from_wal_with_vector_index(
            &wal,
            ann_tuning.clone(),
            ReplayPolicy::from_env(),
            vector_index_persistence.as_deref().map(|p| p.path()),
        ) {
            Ok(result) => result,
            Err(err) => {
                tracing::error!("retrieval failed replaying WAL '{wal_path}': {err:?}");
                std::process::exit(1);
            }
        };
        match &vector_index_persistence {
            Some(persistence) => {
                let restore = &load_stats.vector_index;
                persistence.note_restored(restore);
                let message = format!(
                    "retrieval vector index '{}': {}",
                    persistence.path().display(),
                    restore.describe()
                );
                if matches!(restore, VectorIndexRestore::Rebuilt { .. }) {
                    tracing::warn!("{message}");
                } else {
                    tracing::info!("{message}");
                }
            }
            None => tracing::info!("retrieval vector index persistence: off"),
        }
        tracing::info!(
            "retrieval startup replay: claims_loaded={}, evidence_loaded={}, edges_loaded={}, vectors_loaded={}, snapshot_records={}, wal_delta_records={}",
            load_stats.claims_loaded,
            load_stats.evidence_loaded,
            load_stats.edges_loaded,
            load_stats.vectors_loaded,
            load_stats.replay.snapshot_records,
            load_stats.replay.wal_records
        );
        if load_stats.replay.quarantined_records > 0 || load_stats.replay.dependent_skipped > 0 {
            tracing::warn!(
                "retrieval startup replay quarantined {} unreadable legacy record(s) and skipped {} dependent record(s); see '{}.quarantine' and docs/operations/wal-recovery.md",
                load_stats.replay.quarantined_records,
                load_stats.replay.dependent_skipped,
                wal_path
            );
        }
        if !disk_disabled {
            store = attach_disk(store, &disk_path);
        }
        tracing::info!("retrieval ready: claims={}", store.claims_len());
        follower_wal = Some(wal);
        store
    } else {
        let mut store = InMemoryStore::new_with_ann_tuning(ann_tuning);
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
    // The retrieval WAL (when configured) is handed to the follower so
    // replicated records are mirrored into it and survive a restart.
    let _replication_follower = spawn_replication_follower(Arc::clone(&shared_store), follower_wal);

    if serve_mode {
        {
            let store_guard = shared_store.read().unwrap_or_else(|p| p.into_inner());
            tracing::info!("retrieval transport listening on http://{bind_addr}");
            tracing::info!("retrieval transport workers: {http_workers}");
            tracing::info!(
                "retrieval vector index tuning: connectivity={}, expansion_add={}, expansion_search={}, flat_threshold={}, rerank={}",
                store_guard.ann_tuning().connectivity,
                store_guard.ann_tuning().expansion_add,
                store_guard.ann_tuning().expansion_search,
                store_guard.ann_tuning().flat_threshold,
                store_guard.ann_tuning().rerank
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
        let saver = vector_index_persistence.as_ref().map(|persistence| {
            retrieval::vector_index::spawn_saver(Arc::clone(&shared_store), Arc::clone(persistence))
        });
        tracing::error!("retrieval: serving on http://{bind_addr} (--cli to run without a port)");
        let served = serve_http_with_workers(
            Arc::clone(&shared_store),
            &bind_addr,
            http_workers,
            shutdown,
        );
        if let Some(persistence) = vector_index_persistence.as_ref() {
            retrieval::vector_index::save_on_shutdown(&shared_store, persistence, saver);
        }
        if let Err(err) = served {
            tracing::error!("retrieval transport failed: {err}");
            std::process::exit(1);
        }
    }
}

/// Vector index persistence settings (on by default, file next to the WAL).
/// Values were validated by `dash_config::startup_check`.
fn parse_vector_index_persistence(wal_path: &str) -> Option<Arc<VectorIndexPersistence>> {
    let enabled = parse_env_first::<String>(&[
        "DASH_RETRIEVAL_VECTOR_INDEX_PERSIST",
        "DASH_VECTOR_INDEX_PERSIST",
    ])
    .is_none_or(|raw| {
        !matches!(
            raw.trim().to_ascii_lowercase().as_str(),
            "0" | "false" | "no" | "off"
        )
    });
    if !enabled {
        return None;
    }
    let path = std::env::var("DASH_RETRIEVAL_VECTOR_INDEX_PATH")
        .ok()
        .filter(|path| !path.trim().is_empty())
        .unwrap_or_else(|| format!("{wal_path}.vindex"));
    let interval_ms = parse_env_first::<u64>(&[
        "DASH_RETRIEVAL_VECTOR_INDEX_SAVE_INTERVAL_MS",
        "DASH_VECTOR_INDEX_SAVE_INTERVAL_MS",
    ])
    .unwrap_or(DEFAULT_VECTOR_INDEX_SAVE_INTERVAL_MS);
    Some(Arc::new(VectorIndexPersistence::new(
        path,
        (interval_ms > 0).then(|| Duration::from_millis(interval_ms)),
    )))
}

/// Installs the encryption keyring from `DASH_ENCRYPTION_KEY_FILE` (ADR
/// 0005) and checks the data already on disk before anything is opened:
/// encrypted files without a usable key stop the service (fail closed).
fn init_encryption(service: &str, paths: store::EncryptionStatePaths) {
    match store::init_encryption_from_env(&paths) {
        Ok(Some(keyring)) => tracing::info!(
            "{service} encryption at rest: on (provider {}, active key id {}, {} key id(s) configured)",
            keyring.provider_name(),
            keyring.active_key_id(),
            keyring.key_ids().len()
        ),
        Ok(None) => tracing::info!(
            "{service} encryption at rest: off (set DASH_ENCRYPTION_KEY_FILE to enable it)"
        ),
        Err(reason) => {
            tracing::error!("{service} startup refused: encryption at rest: {reason}");
            std::process::exit(2);
        }
    }
}

fn env_with_fallback(primary: &str, fallback: &str) -> Option<String> {
    std::env::var(primary)
        .ok()
        .or_else(|| std::env::var(fallback).ok())
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
        connectivity: parse_env_first::<usize>(&[
            "DASH_RETRIEVAL_ANN_MAX_NEIGHBORS_BASE",
            "DASH_ANN_MAX_NEIGHBORS_BASE",
            "EME_RETRIEVAL_ANN_MAX_NEIGHBORS_BASE",
            "EME_ANN_MAX_NEIGHBORS_BASE",
        ])
        .filter(|value| *value > 0)
        .unwrap_or(defaults.connectivity),
        expansion_add: parse_env_first::<usize>(&[
            "DASH_RETRIEVAL_ANN_EXPANSION_ADD",
            "DASH_ANN_EXPANSION_ADD",
        ])
        .filter(|value| *value > 0)
        .unwrap_or(defaults.expansion_add),
        expansion_search: parse_env_first::<usize>(&[
            "DASH_RETRIEVAL_ANN_SEARCH_EXPANSION_MIN",
            "DASH_ANN_SEARCH_EXPANSION_MIN",
            "EME_RETRIEVAL_ANN_SEARCH_EXPANSION_MIN",
            "EME_ANN_SEARCH_EXPANSION_MIN",
        ])
        .filter(|value| *value > 0)
        .unwrap_or(defaults.expansion_search),
        flat_threshold: parse_env_first::<usize>(&[
            "DASH_RETRIEVAL_VECTOR_FLAT_THRESHOLD",
            "DASH_VECTOR_FLAT_THRESHOLD",
        ])
        .filter(|value| *value > 0)
        .unwrap_or(defaults.flat_threshold),
        rerank: parse_env_first::<usize>(&["DASH_RETRIEVAL_VECTOR_RERANK", "DASH_VECTOR_RERANK"])
            .unwrap_or(defaults.rerank),
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
    let updated = store.attach_disk(disk_path);
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
