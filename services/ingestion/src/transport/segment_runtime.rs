use std::{path::PathBuf, time::Duration};

use indexer::{
    CompactionSchedulerConfig, SegmentMaintenanceStats, SegmentStoreError, apply_compaction_plan,
    build_segments, load_manifest, maintain_segment_root, persist_segments_atomic,
    plan_compaction_round, prune_unreferenced_segment_files,
};
use store::InMemoryStore;

use super::{
    DEFAULT_SEGMENT_GC_MIN_STALE_AGE_MS, DEFAULT_SEGMENT_MAINTENANCE_INTERVAL_MS,
    config::{
        env_with_fallback, parse_env_first_u64, parse_env_first_usize, sanitize_path_component,
    },
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SegmentRuntime {
    pub(super) root_dir: PathBuf,
    pub(super) max_segment_size: usize,
    pub(super) scheduler: CompactionSchedulerConfig,
    pub(super) maintenance_interval: Option<Duration>,
    pub(super) maintenance_min_stale_age: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct SegmentPublishStats {
    pub(super) claim_count: usize,
    pub(super) segment_count: usize,
    pub(super) compaction_plan_count: usize,
    pub(super) stale_file_pruned_count: usize,
}

impl SegmentRuntime {
    pub(super) fn from_env() -> Option<Self> {
        let root_dir = env_with_fallback("DASH_INGEST_SEGMENT_DIR", "EME_INGEST_SEGMENT_DIR")?;
        let max_segment_size = parse_env_first_usize(&[
            "DASH_INGEST_SEGMENT_MAX_SEGMENT_SIZE",
            "DASH_SEGMENT_MAX_SEGMENT_SIZE",
            "EME_INGEST_SEGMENT_MAX_SEGMENT_SIZE",
            "EME_SEGMENT_MAX_SEGMENT_SIZE",
        ])
        .filter(|value| *value > 0)
        .unwrap_or(10_000);
        let max_segments_per_tier = parse_env_first_usize(&[
            "DASH_INGEST_SEGMENT_MAX_SEGMENTS_PER_TIER",
            "DASH_SEGMENT_MAX_SEGMENTS_PER_TIER",
            "EME_INGEST_SEGMENT_MAX_SEGMENTS_PER_TIER",
            "EME_SEGMENT_MAX_SEGMENTS_PER_TIER",
        ])
        .filter(|value| *value > 0)
        .unwrap_or(8);
        let max_compaction_input_segments = parse_env_first_usize(&[
            "DASH_INGEST_SEGMENT_MAX_COMPACTION_INPUT_SEGMENTS",
            "DASH_SEGMENT_MAX_COMPACTION_INPUT_SEGMENTS",
            "EME_INGEST_SEGMENT_MAX_COMPACTION_INPUT_SEGMENTS",
            "EME_SEGMENT_MAX_COMPACTION_INPUT_SEGMENTS",
        ])
        .filter(|value| *value > 1)
        .unwrap_or(4);
        let maintenance_interval = parse_env_first_u64(&[
            "DASH_INGEST_SEGMENT_MAINTENANCE_INTERVAL_MS",
            "EME_INGEST_SEGMENT_MAINTENANCE_INTERVAL_MS",
        ])
        .or(Some(DEFAULT_SEGMENT_MAINTENANCE_INTERVAL_MS))
        .filter(|value| *value > 0)
        .map(Duration::from_millis);
        let maintenance_min_stale_age = Duration::from_millis(
            parse_env_first_u64(&[
                "DASH_INGEST_SEGMENT_GC_MIN_STALE_AGE_MS",
                "EME_INGEST_SEGMENT_GC_MIN_STALE_AGE_MS",
            ])
            .unwrap_or(DEFAULT_SEGMENT_GC_MIN_STALE_AGE_MS),
        );
        Some(Self {
            root_dir: PathBuf::from(root_dir),
            max_segment_size,
            scheduler: CompactionSchedulerConfig {
                max_segments_per_tier,
                max_compaction_input_segments,
            },
            maintenance_interval,
            maintenance_min_stale_age,
        })
    }

    pub(super) fn publish_for_tenant(
        &self,
        store: &InMemoryStore,
        tenant_id: &str,
    ) -> Result<SegmentPublishStats, SegmentStoreError> {
        let claims = store.claims_for_tenant(tenant_id);
        let claim_count = claims.len();
        let mut segments = build_segments(&claims, self.max_segment_size);
        let plans = plan_compaction_round(&segments, &self.scheduler);
        for plan in &plans {
            segments = apply_compaction_plan(&segments, plan);
        }
        let tenant_dir = self.tenant_segment_dir(tenant_id);
        let previous_manifest = load_manifest(&tenant_dir)?;
        let manifest = persist_segments_atomic(&tenant_dir, &segments)?;
        let stale_file_pruned_count =
            prune_unreferenced_segment_files(&tenant_dir, &manifest, previous_manifest.as_ref())?;
        Ok(SegmentPublishStats {
            claim_count,
            segment_count: manifest.entries.len(),
            compaction_plan_count: plans.len(),
            stale_file_pruned_count,
        })
    }

    fn tenant_segment_dir(&self, tenant_id: &str) -> PathBuf {
        self.root_dir.join(sanitize_path_component(tenant_id))
    }

    pub(super) fn maintain_all_tenants(
        &self,
    ) -> Result<SegmentMaintenanceStats, SegmentStoreError> {
        maintain_segment_root(&self.root_dir, self.maintenance_min_stale_age)
    }
}
