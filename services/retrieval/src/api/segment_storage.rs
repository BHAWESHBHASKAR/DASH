use indexer::{load_manifest, load_segments_from_manifest};
use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    sync::{
        OnceLock, RwLock,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use store::InMemoryStore;

use super::{SegmentPrefilterCacheMetrics, env_with_fallback};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SegmentCacheKey {
    root_dir: String,
    tenant_id: String,
}

impl SegmentCacheKey {
    fn new(root_dir: &Path, tenant_id: &str) -> Self {
        Self {
            root_dir: root_dir.to_string_lossy().to_string(),
            tenant_id: tenant_id.to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SegmentCacheEntry {
    claim_ids: Option<HashSet<String>>,
    fallback_reason: Option<SegmentFallbackReason>,
    next_refresh_instant: std::time::Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SegmentFallbackReason {
    MissingManifest,
    ManifestError,
    SegmentError,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SegmentPrefilterLoadResult {
    claim_ids: Option<HashSet<String>>,
    fallback_reason: Option<SegmentFallbackReason>,
}

#[derive(Debug, Default)]
struct SegmentPrefilterCacheMetricAtoms {
    cache_hits: AtomicU64,
    refresh_attempts: AtomicU64,
    refresh_successes: AtomicU64,
    refresh_failures: AtomicU64,
    refresh_load_micros: AtomicU64,
    fallback_activations: AtomicU64,
    fallback_missing_manifest: AtomicU64,
    fallback_manifest_errors: AtomicU64,
    fallback_segment_errors: AtomicU64,
}

static SEGMENT_PREFILTER_CACHE: OnceLock<RwLock<HashMap<SegmentCacheKey, SegmentCacheEntry>>> =
    OnceLock::new();
static SEGMENT_PREFILTER_CACHE_METRICS: OnceLock<SegmentPrefilterCacheMetricAtoms> =
    OnceLock::new();

pub(super) fn build_segment_prefilter_claim_ids(tenant_id: &str) -> Option<HashSet<String>> {
    let segment_root =
        env_with_fallback("DASH_RETRIEVAL_SEGMENT_DIR", "EME_RETRIEVAL_SEGMENT_DIR")?;
    build_segment_prefilter_claim_ids_from_root(tenant_id, PathBuf::from(segment_root))
}

pub(super) fn build_segment_prefilter_claim_ids_from_root(
    tenant_id: &str,
    segment_root: PathBuf,
) -> Option<HashSet<String>> {
    let cache_key = SegmentCacheKey::new(&segment_root, tenant_id);
    let segment_tenant_path = segment_root.join(sanitize_path_component(tenant_id));
    let now = std::time::Instant::now();
    if let Ok(cache) = segment_prefilter_cache().read()
        && let Some(entry) = cache.get(&cache_key)
        && entry.next_refresh_instant > now
    {
        segment_prefilter_cache_metric_atoms()
            .cache_hits
            .fetch_add(1, Ordering::Relaxed);
        if entry.claim_ids.is_none() {
            observe_segment_fallback_activation(entry.fallback_reason);
        }
        return entry.claim_ids.clone();
    }

    segment_prefilter_cache_metric_atoms()
        .refresh_attempts
        .fetch_add(1, Ordering::Relaxed);
    let refresh_start = std::time::Instant::now();
    let load_result = load_segment_prefilter_claim_ids(&segment_tenant_path);
    let elapsed_micros = refresh_start.elapsed().as_micros();
    let elapsed_micros_u64 = if elapsed_micros > u64::MAX as u128 {
        u64::MAX
    } else {
        elapsed_micros as u64
    };
    segment_prefilter_cache_metric_atoms()
        .refresh_load_micros
        .fetch_add(elapsed_micros_u64, Ordering::Relaxed);
    if load_result.claim_ids.is_some() {
        segment_prefilter_cache_metric_atoms()
            .refresh_successes
            .fetch_add(1, Ordering::Relaxed);
    } else {
        segment_prefilter_cache_metric_atoms()
            .refresh_failures
            .fetch_add(1, Ordering::Relaxed);
        observe_segment_fallback_activation(load_result.fallback_reason);
    }
    let next_refresh_instant = now + segment_prefilter_refresh_interval();
    if let Ok(mut cache) = segment_prefilter_cache().write() {
        cache.insert(
            cache_key,
            SegmentCacheEntry {
                claim_ids: load_result.claim_ids.clone(),
                fallback_reason: load_result.fallback_reason,
                next_refresh_instant,
            },
        );
    }
    load_result.claim_ids
}

fn load_segment_prefilter_claim_ids(segment_tenant_path: &Path) -> SegmentPrefilterLoadResult {
    let manifest = match load_manifest(segment_tenant_path) {
        Ok(Some(value)) => value,
        Ok(None) => {
            return SegmentPrefilterLoadResult {
                claim_ids: None,
                fallback_reason: Some(SegmentFallbackReason::MissingManifest),
            };
        }
        Err(_) => {
            return SegmentPrefilterLoadResult {
                claim_ids: None,
                fallback_reason: Some(SegmentFallbackReason::ManifestError),
            };
        }
    };
    let segments = match load_segments_from_manifest(segment_tenant_path, &manifest) {
        Ok(value) => value,
        Err(_) => {
            return SegmentPrefilterLoadResult {
                claim_ids: None,
                fallback_reason: Some(SegmentFallbackReason::SegmentError),
            };
        }
    };
    let mut ids = HashSet::new();
    for segment in segments {
        ids.extend(segment.claim_ids);
    }
    SegmentPrefilterLoadResult {
        claim_ids: Some(ids),
        fallback_reason: None,
    }
}

fn observe_segment_fallback_activation(reason: Option<SegmentFallbackReason>) {
    let metrics = segment_prefilter_cache_metric_atoms();
    metrics.fallback_activations.fetch_add(1, Ordering::Relaxed);
    match reason {
        Some(SegmentFallbackReason::MissingManifest) => {
            metrics
                .fallback_missing_manifest
                .fetch_add(1, Ordering::Relaxed);
        }
        Some(SegmentFallbackReason::ManifestError) => {
            metrics
                .fallback_manifest_errors
                .fetch_add(1, Ordering::Relaxed);
        }
        Some(SegmentFallbackReason::SegmentError) => {
            metrics
                .fallback_segment_errors
                .fetch_add(1, Ordering::Relaxed);
        }
        None => {}
    }
}

pub(super) fn build_wal_delta_claim_ids(
    store: &InMemoryStore,
    tenant_id: &str,
    segment_base_claim_ids: Option<&HashSet<String>>,
) -> Option<HashSet<String>> {
    let segment_base_claim_ids = segment_base_claim_ids?;
    let tenant_claim_ids = store.claim_ids_for_tenant(tenant_id);
    Some(
        tenant_claim_ids
            .difference(segment_base_claim_ids)
            .cloned()
            .collect(),
    )
}

pub(super) fn merge_segment_base_with_wal_delta_claim_ids(
    segment_base: Option<&HashSet<String>>,
    wal_delta: Option<&HashSet<String>>,
) -> Option<HashSet<String>> {
    match (segment_base, wal_delta) {
        (None, None) => None,
        (Some(segment_base), None) => Some(segment_base.clone()),
        (None, Some(wal_delta)) => Some(wal_delta.clone()),
        (Some(segment_base), Some(wal_delta)) => {
            let mut merged = segment_base.clone();
            merged.extend(wal_delta.iter().cloned());
            Some(merged)
        }
    }
}

pub(super) fn merge_allowed_claim_ids(
    metadata: Option<&HashSet<String>>,
    segment: Option<&HashSet<String>>,
) -> Option<HashSet<String>> {
    match (metadata, segment) {
        (None, None) => None,
        (Some(metadata), None) => Some(metadata.clone()),
        (None, Some(segment)) => Some(segment.clone()),
        (Some(metadata), Some(segment)) => Some(metadata.intersection(segment).cloned().collect()),
    }
}

fn sanitize_path_component(raw: &str) -> String {
    let mut out: String = raw
        .chars()
        .map(|ch| match ch {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' => ch,
            _ => '_',
        })
        .collect();
    if out.is_empty() {
        out.push('_');
    }
    out
}

pub(super) fn segment_prefilter_refresh_interval() -> Duration {
    let refresh_ms = env_with_fallback(
        "DASH_RETRIEVAL_SEGMENT_CACHE_REFRESH_MS",
        "EME_RETRIEVAL_SEGMENT_CACHE_REFRESH_MS",
    )
    .and_then(|value| value.parse::<u64>().ok())
    .filter(|value| *value > 0)
    .unwrap_or(1_000);
    Duration::from_millis(refresh_ms)
}

fn segment_prefilter_cache() -> &'static RwLock<HashMap<SegmentCacheKey, SegmentCacheEntry>> {
    SEGMENT_PREFILTER_CACHE.get_or_init(|| RwLock::new(HashMap::new()))
}

fn segment_prefilter_cache_metric_atoms() -> &'static SegmentPrefilterCacheMetricAtoms {
    SEGMENT_PREFILTER_CACHE_METRICS.get_or_init(SegmentPrefilterCacheMetricAtoms::default)
}

pub(super) fn segment_prefilter_cache_metrics_snapshot() -> SegmentPrefilterCacheMetrics {
    let metrics = segment_prefilter_cache_metric_atoms();
    SegmentPrefilterCacheMetrics {
        cache_hits: metrics.cache_hits.load(Ordering::Relaxed),
        refresh_attempts: metrics.refresh_attempts.load(Ordering::Relaxed),
        refresh_successes: metrics.refresh_successes.load(Ordering::Relaxed),
        refresh_failures: metrics.refresh_failures.load(Ordering::Relaxed),
        refresh_load_micros: metrics.refresh_load_micros.load(Ordering::Relaxed),
        fallback_activations: metrics.fallback_activations.load(Ordering::Relaxed),
        fallback_missing_manifest: metrics.fallback_missing_manifest.load(Ordering::Relaxed),
        fallback_manifest_errors: metrics.fallback_manifest_errors.load(Ordering::Relaxed),
        fallback_segment_errors: metrics.fallback_segment_errors.load(Ordering::Relaxed),
    }
}

pub(super) fn reset_segment_prefilter_cache_metrics() {
    let metrics = segment_prefilter_cache_metric_atoms();
    metrics.cache_hits.store(0, Ordering::Relaxed);
    metrics.refresh_attempts.store(0, Ordering::Relaxed);
    metrics.refresh_successes.store(0, Ordering::Relaxed);
    metrics.refresh_failures.store(0, Ordering::Relaxed);
    metrics.refresh_load_micros.store(0, Ordering::Relaxed);
    metrics.fallback_activations.store(0, Ordering::Relaxed);
    metrics
        .fallback_missing_manifest
        .store(0, Ordering::Relaxed);
    metrics.fallback_manifest_errors.store(0, Ordering::Relaxed);
    metrics.fallback_segment_errors.store(0, Ordering::Relaxed);
}

#[cfg(test)]
pub(super) fn clear_segment_prefilter_cache_for_tests() {
    if let Some(cache) = SEGMENT_PREFILTER_CACHE.get()
        && let Ok(mut guard) = cache.write()
    {
        guard.clear();
    }
    reset_segment_prefilter_cache_metrics();
}
