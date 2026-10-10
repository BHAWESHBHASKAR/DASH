use std::{path::PathBuf, time::Duration};

use indexer::{
    CompactionSchedulerConfig, SegmentMaintenanceStats, SegmentPublishOptions, SegmentStoreError,
    maintain_segment_root_report, publish_claims_to_dir, resolve_tenant_dir, write_tenant_marker,
};
use store::InMemoryStore;

use super::{
    DEFAULT_SEGMENT_GC_MIN_STALE_AGE_MS, DEFAULT_SEGMENT_MAINTENANCE_INTERVAL_MS,
    config::{env_with_fallback, parse_env_first_u64, parse_env_first_usize},
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
        let tenant_dir = self.tenant_segment_dir(tenant_id);
        // Record the owner so a directory is never ambiguous again.
        write_tenant_marker(&tenant_dir, tenant_id)?;
        let result = publish_claims_to_dir(
            &tenant_dir,
            &claims,
            &SegmentPublishOptions {
                max_segment_size: self.max_segment_size,
                scheduler: self.scheduler.clone(),
                prune_grace: self.maintenance_min_stale_age,
            },
        )?;
        Ok(SegmentPublishStats {
            claim_count: result.claim_count,
            segment_count: result.segment_count,
            compaction_plan_count: result.compaction_plan_count,
            stale_file_pruned_count: result.stale_file_pruned_count,
        })
    }

    fn tenant_segment_dir(&self, tenant_id: &str) -> PathBuf {
        resolve_tenant_dir(&self.root_dir, tenant_id)
    }

    pub(super) fn maintain_all_tenants(
        &self,
    ) -> Result<SegmentMaintenanceStats, SegmentStoreError> {
        let report = maintain_segment_root_report(&self.root_dir, self.maintenance_min_stale_age)?;
        for (tenant_dir, err) in &report.tenant_errors {
            eprintln!(
                "ingestion segment maintenance failed for tenant dir '{tenant_dir}': {err:?}"
            );
        }
        Ok(report.stats)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn runtime(root: PathBuf) -> SegmentRuntime {
        SegmentRuntime {
            root_dir: root,
            max_segment_size: 10,
            scheduler: CompactionSchedulerConfig::default(),
            maintenance_interval: None,
            maintenance_min_stale_age: Duration::ZERO,
        }
    }

    #[test]
    fn colliding_tenant_ids_use_distinct_segment_directories() {
        let rt = runtime(PathBuf::from("/nonexistent-segments-root"));
        assert_ne!(rt.tenant_segment_dir("a.b"), rt.tenant_segment_dir("a_b"));
        let traversal = rt.tenant_segment_dir("../x");
        assert_eq!(traversal.parent(), Some(rt.root_dir.as_path()));
    }

    #[test]
    fn publish_for_colliding_tenants_writes_separate_directories() {
        let root = std::env::temp_dir().join(format!("dash-ingest-seg-{}", std::process::id()));
        let rt = runtime(root.clone());
        let store = InMemoryStore::new();
        rt.publish_for_tenant(&store, "a.b").expect("publish a.b");
        rt.publish_for_tenant(&store, "a_b").expect("publish a_b");
        assert_eq!(std::fs::read_dir(&root).expect("root exists").count(), 2);
        // Each directory records its owner.
        for tenant in ["a.b", "a_b"] {
            let dir = rt.tenant_segment_dir(tenant);
            assert_eq!(
                indexer::read_tenant_marker(&dir)
                    .expect("marker")
                    .as_deref(),
                Some(tenant)
            );
        }
        let _ = std::fs::remove_dir_all(root);
    }
}
