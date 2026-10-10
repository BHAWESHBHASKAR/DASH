use indexer::{SegmentStoreError, load_manifest, load_segments_from_manifest, resolve_tenant_dir};
use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    sync::{
        Condvar, Mutex, OnceLock, RwLock,
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

/// Per-tenant segment prefilter cache with single-flight refresh: when an entry
/// is stale exactly one thread reloads it from disk; concurrent callers keep
/// serving the previous entry (or, on a cold miss, wait for the one loader).
/// No cache lock is held while the segment files are read.
#[derive(Default)]
struct SegmentPrefilterCache {
    entries: RwLock<HashMap<SegmentCacheKey, SegmentCacheEntry>>,
    in_flight: Mutex<HashSet<SegmentCacheKey>>,
    refresh_done: Condvar,
}

/// Releases the single-flight slot (and wakes waiters) even if the loader panics.
struct RefreshSlot<'a> {
    cache: &'a SegmentPrefilterCache,
    key: &'a SegmentCacheKey,
}

impl Drop for RefreshSlot<'_> {
    fn drop(&mut self) {
        let mut in_flight = self
            .cache
            .in_flight
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        in_flight.remove(self.key);
        self.cache.refresh_done.notify_all();
    }
}

static SEGMENT_PREFILTER_CACHE: OnceLock<SegmentPrefilterCache> = OnceLock::new();
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
    segment_prefilter_cache().get_or_refresh(
        tenant_id,
        segment_root,
        segment_prefilter_refresh_interval(),
    )
}

impl SegmentPrefilterCache {
    fn get_or_refresh(
        &self,
        tenant_id: &str,
        segment_root: PathBuf,
        refresh_interval: Duration,
    ) -> Option<HashSet<String>> {
        let cache_key = SegmentCacheKey::new(&segment_root, tenant_id);
        loop {
            let now = std::time::Instant::now();
            let stale_entry = {
                let entries = self
                    .entries
                    .read()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                match entries.get(&cache_key) {
                    Some(entry) if entry.next_refresh_instant > now => {
                        observe_cache_hit(entry);
                        return entry.claim_ids.clone();
                    }
                    other => other.cloned(),
                }
            };

            // Stale or missing: try to become the single loader for this key.
            let is_loader = {
                let mut in_flight = self
                    .in_flight
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                in_flight.insert(cache_key.clone())
            };
            if !is_loader {
                if let Some(entry) = stale_entry {
                    // Someone else is refreshing: serve the previous cache.
                    observe_cache_hit(&entry);
                    return entry.claim_ids;
                }
                // Cold miss: wait for the loader to publish, then re-check.
                let mut in_flight = self
                    .in_flight
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                while in_flight.contains(&cache_key) {
                    in_flight = self
                        .refresh_done
                        .wait(in_flight)
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                }
                continue;
            }

            let _slot = RefreshSlot {
                cache: self,
                key: &cache_key,
            };
            let segment_tenant_path = resolve_tenant_dir(&segment_root, tenant_id);
            let load_result = timed_segment_load(&segment_tenant_path);
            let next_refresh_instant = std::time::Instant::now() + refresh_interval;
            let claim_ids = load_result.claim_ids.clone();
            self.entries
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .insert(
                    cache_key.clone(),
                    SegmentCacheEntry {
                        claim_ids: load_result.claim_ids,
                        fallback_reason: load_result.fallback_reason,
                        next_refresh_instant,
                    },
                );
            return claim_ids;
        }
    }
}

fn observe_cache_hit(entry: &SegmentCacheEntry) {
    segment_prefilter_cache_metric_atoms()
        .cache_hits
        .fetch_add(1, Ordering::Relaxed);
    if entry.claim_ids.is_none() {
        observe_segment_fallback_activation(entry.fallback_reason);
    }
}

fn timed_segment_load(segment_tenant_path: &Path) -> SegmentPrefilterLoadResult {
    let metrics = segment_prefilter_cache_metric_atoms();
    metrics.refresh_attempts.fetch_add(1, Ordering::Relaxed);
    let refresh_start = std::time::Instant::now();
    #[cfg(test)]
    tests::on_segment_load(segment_tenant_path);
    #[cfg(test)]
    apply_test_load_delay(segment_tenant_path);
    let load_result = load_segment_prefilter_claim_ids(segment_tenant_path);
    let elapsed_micros = refresh_start.elapsed().as_micros();
    let elapsed_micros_u64 = u64::try_from(elapsed_micros).unwrap_or(u64::MAX);
    metrics
        .refresh_load_micros
        .fetch_add(elapsed_micros_u64, Ordering::Relaxed);
    if load_result.claim_ids.is_some() {
        metrics.refresh_successes.fetch_add(1, Ordering::Relaxed);
    } else {
        metrics.refresh_failures.fetch_add(1, Ordering::Relaxed);
        observe_segment_fallback_activation(load_result.fallback_reason);
    }
    load_result
}

#[cfg(test)]
static TEST_LOAD_DELAYS: Mutex<Vec<(PathBuf, Duration)>> = Mutex::new(Vec::new());

/// Test hook: make every refresh of `tenant_dir` sleep for `delay`.
#[cfg(test)]
pub(super) fn set_segment_load_delay_for_tests(tenant_dir: &Path, delay: Duration) {
    let mut delays = TEST_LOAD_DELAYS.lock().unwrap_or_else(|p| p.into_inner());
    delays.retain(|(path, _)| path != tenant_dir);
    delays.push((tenant_dir.to_path_buf(), delay));
}

#[cfg(test)]
fn apply_test_load_delay(tenant_dir: &Path) {
    let delay = TEST_LOAD_DELAYS
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .iter()
        .find(|(path, _)| path == tenant_dir)
        .map(|(_, delay)| *delay);
    if let Some(delay) = delay {
        std::thread::sleep(delay);
    }
}

/// How often a reader re-reads the manifest when a segment file vanishes
/// because a publisher pruned it between our manifest read and segment read.
const MANIFEST_RELOAD_ATTEMPTS: usize = 3;

fn load_segment_prefilter_claim_ids(segment_tenant_path: &Path) -> SegmentPrefilterLoadResult {
    let mut attempt = 0;
    let segments = loop {
        attempt += 1;
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
        match load_segments_from_manifest(segment_tenant_path, &manifest) {
            Ok(value) => break value,
            Err(SegmentStoreError::MissingFile(_)) if attempt < MANIFEST_RELOAD_ATTEMPTS => {
                continue;
            }
            Err(_) => {
                return SegmentPrefilterLoadResult {
                    claim_ids: None,
                    fallback_reason: Some(SegmentFallbackReason::SegmentError),
                };
            }
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

fn segment_prefilter_cache() -> &'static SegmentPrefilterCache {
    SEGMENT_PREFILTER_CACHE.get_or_init(SegmentPrefilterCache::default)
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
        && let Ok(mut guard) = cache.entries.write()
    {
        guard.clear();
    }
    reset_segment_prefilter_cache_metrics();
}

#[cfg(test)]
mod tests {
    use super::*;
    use indexer::{Segment, Tier, persist_segments_atomic, tenant_dir_name};
    use std::sync::{Arc, Barrier};
    use std::time::{Instant, SystemTime, UNIX_EPOCH};

    struct LoadHook {
        path: PathBuf,
        delay: Duration,
        count: usize,
    }

    static LOAD_HOOKS: Mutex<Vec<LoadHook>> = Mutex::new(Vec::new());

    /// Test hook invoked by the loader: counts loads per tenant path and
    /// optionally slows them down so concurrency can be observed.
    pub(super) fn on_segment_load(path: &Path) {
        let delay = {
            let mut hooks = LOAD_HOOKS.lock().unwrap();
            match hooks.iter_mut().find(|hook| hook.path == path) {
                Some(hook) => {
                    hook.count += 1;
                    hook.delay
                }
                None => return,
            }
        };
        std::thread::sleep(delay);
    }

    fn register_hook(path: &Path, delay: Duration) {
        LOAD_HOOKS.lock().unwrap().push(LoadHook {
            path: path.to_path_buf(),
            delay,
            count: 0,
        });
    }

    fn set_delay(path: &Path, delay: Duration) {
        let mut hooks = LOAD_HOOKS.lock().unwrap();
        hooks
            .iter_mut()
            .find(|hook| hook.path == path)
            .unwrap()
            .delay = delay;
    }

    fn load_count(path: &Path) -> usize {
        let hooks = LOAD_HOOKS.lock().unwrap();
        hooks.iter().find(|hook| hook.path == path).unwrap().count
    }

    fn temp_root(tag: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "dash-segstore-{tag}-{}-{nanos}",
            std::process::id()
        ))
    }

    fn publish(root: &Path, tenant: &str, ids: &[&str]) {
        persist_segments_atomic(
            &root.join(tenant_dir_name(tenant)),
            &[Segment {
                segment_id: "hot-0".into(),
                tier: Tier::Hot,
                claim_ids: ids.iter().map(|id| (*id).to_string()).collect(),
            }],
        )
        .unwrap();
    }

    #[test]
    fn cold_cache_refresh_is_single_flight_per_tenant() {
        let root = temp_root("cold");
        publish(&root, "t1", &["c1"]);
        let tenant_path = root.join(tenant_dir_name("t1"));
        register_hook(&tenant_path, Duration::from_millis(150));

        let cache = Arc::new(SegmentPrefilterCache::default());
        let barrier = Arc::new(Barrier::new(16));
        let handles: Vec<_> = (0..16)
            .map(|_| {
                let (cache, barrier, root) = (cache.clone(), barrier.clone(), root.clone());
                std::thread::spawn(move || {
                    barrier.wait();
                    cache.get_or_refresh("t1", root, Duration::from_secs(60))
                })
            })
            .collect();
        for handle in handles {
            let ids = handle.join().unwrap().expect("segment ids should load");
            assert!(ids.contains("c1"));
        }
        assert_eq!(load_count(&tenant_path), 1, "only one thread may load");
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn stale_entry_is_served_while_single_loader_refreshes() {
        let root = temp_root("stale");
        publish(&root, "t1", &["old"]);
        let tenant_path = root.join(tenant_dir_name("t1"));
        register_hook(&tenant_path, Duration::ZERO);

        let cache = Arc::new(SegmentPrefilterCache::default());
        let interval = Duration::from_millis(1);
        let first = cache.get_or_refresh("t1", root.clone(), interval).unwrap();
        assert!(first.contains("old"));
        assert_eq!(load_count(&tenant_path), 1);

        publish(&root, "t1", &["new"]);
        std::thread::sleep(Duration::from_millis(10));
        set_delay(&tenant_path, Duration::from_millis(500));

        let loader = {
            let (cache, root) = (cache.clone(), root.clone());
            std::thread::spawn(move || cache.get_or_refresh("t1", root, interval))
        };
        while load_count(&tenant_path) < 2 {
            std::thread::sleep(Duration::from_millis(1));
        }

        let started = Instant::now();
        let followers: Vec<_> = (0..8)
            .map(|_| {
                let (cache, root) = (cache.clone(), root.clone());
                std::thread::spawn(move || cache.get_or_refresh("t1", root, interval))
            })
            .collect();
        for follower in followers {
            let ids = follower.join().unwrap().unwrap();
            assert!(ids.contains("old") && !ids.contains("new"));
        }
        assert!(
            started.elapsed() < Duration::from_millis(300),
            "followers must not wait for the refresh"
        );
        assert_eq!(load_count(&tenant_path), 2, "followers must not reload");

        let refreshed = loader.join().unwrap().unwrap();
        assert!(refreshed.contains("new"));
        let _ = std::fs::remove_dir_all(root);
    }
}
