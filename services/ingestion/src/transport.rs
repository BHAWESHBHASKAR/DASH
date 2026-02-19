use std::{
    collections::{HashMap, HashSet},
    io::{BufRead, BufReader, Read},
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
        mpsc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

mod audit;
mod authz;
mod http;
mod json;
mod replication;

use audit::{AuditEvent, emit_audit_event};
use authz::{AuthDecision, AuthPolicy, authorize_request_for_tenant};
use http::{
    HttpRequest, HttpResponse, render_response_text, write_backpressure_response, write_response,
};
use indexer::{
    CompactionSchedulerConfig, SegmentMaintenanceStats, SegmentStoreError, apply_compaction_plan,
    build_segments, load_manifest, maintain_segment_root, persist_segments_atomic,
    plan_compaction_round, prune_unreferenced_segment_files,
};
use json::{JsonValue, json_escape, parse_json};
use metadata_router::{
    PlacementRouteError, ReplicaHealth, ReplicaRole, RoutedReplica, RouterConfig, ShardPlacement,
    load_shard_placements_csv, route_write_with_placement, shard_ids_from_placements,
};
use replication::{
    ReplicationPullConfig, parse_replication_delta_frame, parse_replication_export_frame,
    render_replication_delta_frame, render_replication_export_frame, request_replication_source,
};
use schema::{Claim, ClaimEdge, Evidence, Relation, Stance};
use store::{
    CheckpointPolicy, FileWal, InMemoryStore, StoreError, WalReplicationDelta, WalReplicationExport,
};

use crate::{
    IngestInput,
    api::{IngestApiRequest, IngestApiResponse, IngestBatchApiRequest, IngestBatchApiResponse},
    ingest_document, ingest_document_persistent_with_policy,
};

#[cfg(test)]
use audit::{append_audit_record, clear_cached_audit_chain_state, is_sha256_hex};

pub struct IngestionRuntime {
    store: InMemoryStore,
    wal: Option<FileWal>,
    wal_async_flush_interval: Option<Duration>,
    checkpoint_policy: CheckpointPolicy,
    segment_runtime: Option<SegmentRuntime>,
    placement_routing: Result<Option<PlacementRoutingState>, String>,
    successful_ingests: u64,
    failed_ingests: u64,
    batch_success_total: u64,
    batch_failed_total: u64,
    batch_commit_total: u64,
    batch_last_size: usize,
    batch_idempotent_hit_total: u64,
    segment_publish_success_total: u64,
    segment_publish_failure_total: u64,
    segment_last_claim_count: usize,
    segment_last_segment_count: usize,
    segment_last_compaction_plans: usize,
    segment_last_stale_file_pruned_count: usize,
    segment_maintenance_tick_total: u64,
    segment_maintenance_success_total: u64,
    segment_maintenance_failure_total: u64,
    segment_maintenance_last_pruned_count: usize,
    segment_maintenance_last_tenant_dirs: usize,
    segment_maintenance_last_tenant_manifests: usize,
    auth_success_total: u64,
    auth_failure_total: u64,
    authz_denied_total: u64,
    audit_events_total: u64,
    audit_write_error_total: u64,
    placement_route_reject_total: u64,
    placement_last_shard_id: Option<u32>,
    placement_last_epoch: Option<u64>,
    placement_last_role: Option<ReplicaRole>,
    wal_flush_due_total: u64,
    wal_flush_success_total: u64,
    wal_flush_failure_total: u64,
    wal_async_flush_tick_total: u64,
    replication_pull_success_total: u64,
    replication_pull_failure_total: u64,
    replication_applied_records_total: u64,
    replication_resync_total: u64,
    replication_last_offset: usize,
    replication_last_error: Option<String>,
    transport_backpressure: Option<Arc<TransportBackpressureMetrics>>,
    started_at: Instant,
}

#[derive(Debug, Default)]
pub(crate) struct TransportBackpressureMetrics {
    pub(crate) queue_depth: AtomicUsize,
    pub(crate) queue_capacity: usize,
    pub(crate) queue_full_reject_total: AtomicU64,
}

impl TransportBackpressureMetrics {
    pub(crate) fn new(queue_capacity: usize) -> Self {
        Self {
            queue_depth: AtomicUsize::new(0),
            queue_capacity,
            queue_full_reject_total: AtomicU64::new(0),
        }
    }

    pub(crate) fn observe_enqueued(&self) {
        self.queue_depth.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn observe_dequeued(&self) {
        let _ = self
            .queue_depth
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                Some(value.saturating_sub(1))
            });
    }

    pub(crate) fn observe_rejected(&self) {
        self.queue_full_reject_total.fetch_add(1, Ordering::Relaxed);
    }
}

impl IngestionRuntime {
    pub fn in_memory(store: InMemoryStore) -> Self {
        Self {
            store,
            wal: None,
            wal_async_flush_interval: None,
            checkpoint_policy: CheckpointPolicy::default(),
            segment_runtime: SegmentRuntime::from_env(),
            placement_routing: PlacementRoutingState::from_env(),
            successful_ingests: 0,
            failed_ingests: 0,
            batch_success_total: 0,
            batch_failed_total: 0,
            batch_commit_total: 0,
            batch_last_size: 0,
            batch_idempotent_hit_total: 0,
            segment_publish_success_total: 0,
            segment_publish_failure_total: 0,
            segment_last_claim_count: 0,
            segment_last_segment_count: 0,
            segment_last_compaction_plans: 0,
            segment_last_stale_file_pruned_count: 0,
            segment_maintenance_tick_total: 0,
            segment_maintenance_success_total: 0,
            segment_maintenance_failure_total: 0,
            segment_maintenance_last_pruned_count: 0,
            segment_maintenance_last_tenant_dirs: 0,
            segment_maintenance_last_tenant_manifests: 0,
            auth_success_total: 0,
            auth_failure_total: 0,
            authz_denied_total: 0,
            audit_events_total: 0,
            audit_write_error_total: 0,
            placement_route_reject_total: 0,
            placement_last_shard_id: None,
            placement_last_epoch: None,
            placement_last_role: None,
            wal_flush_due_total: 0,
            wal_flush_success_total: 0,
            wal_flush_failure_total: 0,
            wal_async_flush_tick_total: 0,
            replication_pull_success_total: 0,
            replication_pull_failure_total: 0,
            replication_applied_records_total: 0,
            replication_resync_total: 0,
            replication_last_offset: 0,
            replication_last_error: None,
            transport_backpressure: None,
            started_at: Instant::now(),
        }
    }

    pub fn persistent(
        store: InMemoryStore,
        wal: FileWal,
        checkpoint_policy: CheckpointPolicy,
    ) -> Self {
        let wal_async_flush_interval = resolve_wal_async_flush_interval(Some(&wal));
        Self {
            store,
            wal: Some(wal),
            wal_async_flush_interval,
            checkpoint_policy,
            segment_runtime: SegmentRuntime::from_env(),
            placement_routing: PlacementRoutingState::from_env(),
            successful_ingests: 0,
            failed_ingests: 0,
            batch_success_total: 0,
            batch_failed_total: 0,
            batch_commit_total: 0,
            batch_last_size: 0,
            batch_idempotent_hit_total: 0,
            segment_publish_success_total: 0,
            segment_publish_failure_total: 0,
            segment_last_claim_count: 0,
            segment_last_segment_count: 0,
            segment_last_compaction_plans: 0,
            segment_last_stale_file_pruned_count: 0,
            segment_maintenance_tick_total: 0,
            segment_maintenance_success_total: 0,
            segment_maintenance_failure_total: 0,
            segment_maintenance_last_pruned_count: 0,
            segment_maintenance_last_tenant_dirs: 0,
            segment_maintenance_last_tenant_manifests: 0,
            auth_success_total: 0,
            auth_failure_total: 0,
            authz_denied_total: 0,
            audit_events_total: 0,
            audit_write_error_total: 0,
            placement_route_reject_total: 0,
            placement_last_shard_id: None,
            placement_last_epoch: None,
            placement_last_role: None,
            wal_flush_due_total: 0,
            wal_flush_success_total: 0,
            wal_flush_failure_total: 0,
            wal_async_flush_tick_total: 0,
            replication_pull_success_total: 0,
            replication_pull_failure_total: 0,
            replication_applied_records_total: 0,
            replication_resync_total: 0,
            replication_last_offset: 0,
            replication_last_error: None,
            transport_backpressure: None,
            started_at: Instant::now(),
        }
    }

    pub fn claims_len(&self) -> usize {
        self.store.claims_len()
    }

    pub fn placement_routing_error(&self) -> Option<&str> {
        self.placement_routing.as_ref().err().map(String::as_str)
    }

    pub fn placement_routing_summary(&self) -> Option<String> {
        let routing = self.placement_routing.as_ref().ok()?.as_ref()?;
        let reload_interval_ms = routing.reload_snapshot().interval_ms.unwrap_or_default();
        Some(format!(
            "local_node_id={}, shards={}, replicas_per_shard={}, reload_interval_ms={}",
            routing.runtime().local_node_id,
            routing.runtime().router_config.shard_ids.len(),
            routing.runtime().router_config.replica_count,
            reload_interval_ms
        ))
    }

    #[cfg(test)]
    fn with_segment_runtime_for_tests(mut self, runtime: Option<SegmentRuntime>) -> Self {
        self.segment_runtime = runtime;
        self
    }

    #[cfg(test)]
    fn with_placement_runtime_for_tests(
        mut self,
        runtime: Result<Option<PlacementRoutingRuntime>, String>,
    ) -> Self {
        self.placement_routing =
            runtime.map(|state| state.map(PlacementRoutingState::from_static_runtime));
        self
    }

    fn ensure_local_write_route_for_claim(&mut self, claim: &Claim) -> Result<(), WriteRouteError> {
        let Some(routing_state) = self
            .placement_routing
            .as_mut()
            .map_err(|reason| WriteRouteError::Config(reason.clone()))?
            .as_mut()
        else {
            return Ok(());
        };
        routing_state.maybe_refresh();
        let routing = routing_state.runtime();
        let entity_key = write_entity_key_for_claim(claim);
        let routed = route_write_with_placement(
            &claim.tenant_id,
            entity_key,
            &routing.router_config,
            &routing.placements,
        )
        .map_err(WriteRouteError::Placement)?;
        let local_node_id = routing.local_node_id.clone();
        self.observe_write_route_resolution(&routed);
        if routed.node_id != local_node_id {
            return Err(WriteRouteError::WrongNode {
                local_node_id,
                target_node_id: routed.node_id,
                shard_id: routed.shard_id,
                epoch: routed.epoch,
                role: routed.role,
            });
        }
        Ok(())
    }

    fn ingest(&mut self, request: IngestApiRequest) -> Result<IngestApiResponse, StoreError> {
        let tenant_id = request.claim.tenant_id.clone();
        let ingested_claim_id = request.claim.claim_id.clone();
        let input = IngestInput {
            claim: request.claim,
            claim_embedding: request.claim_embedding,
            evidence: request.evidence,
            edges: request.edges,
        };
        let checkpoint_stats = self.ingest_input_internal(input)?;

        self.successful_ingests += 1;
        self.publish_segments_for_tenant(&tenant_id);
        Ok(IngestApiResponse {
            ingested_claim_id,
            claims_total: self.store.claims_len(),
            checkpoint_triggered: checkpoint_stats.is_some(),
            checkpoint_snapshot_records: checkpoint_stats.as_ref().map(|s| s.snapshot_records),
            checkpoint_truncated_wal_records: checkpoint_stats
                .as_ref()
                .map(|s| s.truncated_wal_records),
        })
    }

    fn ingest_batch(
        &mut self,
        request: IngestBatchApiRequest,
    ) -> Result<IngestBatchApiResponse, StoreError> {
        let commit_id = request.commit_id.unwrap_or_else(generate_batch_commit_id);
        let mut inputs = Vec::with_capacity(request.items.len());
        let mut ingested_claim_ids = Vec::with_capacity(request.items.len());
        let mut touched_tenants = HashSet::new();

        for item in request.items {
            let tenant_id = item.claim.tenant_id.clone();
            let claim_id = item.claim.claim_id.clone();
            inputs.push(IngestInput {
                claim: item.claim,
                claim_embedding: item.claim_embedding,
                evidence: item.evidence,
                edges: item.edges,
            });
            touched_tenants.insert(tenant_id);
            ingested_claim_ids.push(claim_id);
        }

        if let Some(existing) = self.store.batch_commit_metadata(&commit_id) {
            if existing.batch_size != ingested_claim_ids.len()
                || existing.claim_ids != ingested_claim_ids
            {
                return Err(StoreError::Conflict(format!(
                    "batch commit_id '{}' already exists with different payload",
                    commit_id
                )));
            }

            self.batch_success_total = self.batch_success_total.saturating_add(1);
            self.batch_last_size = ingested_claim_ids.len();
            self.batch_idempotent_hit_total = self.batch_idempotent_hit_total.saturating_add(1);
            return Ok(IngestBatchApiResponse {
                commit_id,
                idempotent_replay: true,
                batch_size: ingested_claim_ids.len(),
                ingested_claim_ids,
                claims_total: self.store.claims_len(),
                checkpoint_triggered: false,
                checkpoint_snapshot_records: None,
                checkpoint_truncated_wal_records: None,
            });
        }

        let mut staged_store = self.store.clone();
        for input in &inputs {
            ingest_document(&mut staged_store, input.clone())?;
        }

        let commit_ts_unix_ms = unix_timestamp_millis();
        if let Some(wal) = self.wal.as_mut() {
            let rollback_point = wal.begin_rollback_point()?;
            let append_result = (|| {
                for input in &inputs {
                    append_input_to_wal(wal, input)?;
                }
                wal.append_batch_commit(
                    &commit_id,
                    ingested_claim_ids.len(),
                    commit_ts_unix_ms,
                    &ingested_claim_ids,
                )?;
                Ok::<(), StoreError>(())
            })();
            if let Err(err) = append_result {
                if let Err(rollback_err) = wal.rollback_to(rollback_point) {
                    eprintln!("batch rollback failed after WAL append error: {rollback_err:?}");
                }
                return Err(err);
            }
            self.batch_commit_total = self.batch_commit_total.saturating_add(1);
        }

        self.store = staged_store;
        self.successful_ingests = self
            .successful_ingests
            .saturating_add(ingested_claim_ids.len() as u64);

        let mut checkpoint_stats = None;
        self.store.observe_batch_commit(
            &commit_id,
            ingested_claim_ids.len(),
            commit_ts_unix_ms,
            &ingested_claim_ids,
        )?;
        if let Some(wal) = self.wal.as_mut()
            && should_checkpoint_now(&self.checkpoint_policy, wal)?
        {
            match self.store.checkpoint_and_compact(wal) {
                Ok(stats) => checkpoint_stats = Some(stats),
                Err(err) => {
                    eprintln!("ingestion batch checkpoint failed after commit: {err:?}");
                }
            }
        }

        for tenant_id in touched_tenants {
            self.publish_segments_for_tenant(&tenant_id);
        }

        self.batch_success_total = self.batch_success_total.saturating_add(1);
        self.batch_last_size = ingested_claim_ids.len();
        Ok(IngestBatchApiResponse {
            commit_id,
            idempotent_replay: false,
            batch_size: ingested_claim_ids.len(),
            ingested_claim_ids,
            claims_total: self.store.claims_len(),
            checkpoint_triggered: checkpoint_stats.is_some(),
            checkpoint_snapshot_records: checkpoint_stats.as_ref().map(|s| s.snapshot_records),
            checkpoint_truncated_wal_records: checkpoint_stats
                .as_ref()
                .map(|s| s.truncated_wal_records),
        })
    }

    fn ingest_input_internal(
        &mut self,
        input: IngestInput,
    ) -> Result<Option<store::WalCheckpointStats>, StoreError> {
        let checkpoint_stats = if let Some(wal) = self.wal.as_mut() {
            ingest_document_persistent_with_policy(
                &mut self.store,
                wal,
                &self.checkpoint_policy,
                input,
            )?
        } else {
            ingest_document(&mut self.store, input)?;
            None
        };
        Ok(checkpoint_stats)
    }

    fn publish_segments_for_tenant(&mut self, tenant_id: &str) {
        if let Some(segment_runtime) = self.segment_runtime.as_ref() {
            match segment_runtime.publish_for_tenant(&self.store, tenant_id) {
                Ok(stats) => {
                    self.segment_publish_success_total =
                        self.segment_publish_success_total.saturating_add(1);
                    self.segment_last_claim_count = stats.claim_count;
                    self.segment_last_segment_count = stats.segment_count;
                    self.segment_last_compaction_plans = stats.compaction_plan_count;
                    self.segment_last_stale_file_pruned_count = stats.stale_file_pruned_count;
                }
                Err(err) => {
                    self.segment_publish_failure_total =
                        self.segment_publish_failure_total.saturating_add(1);
                    eprintln!(
                        "ingestion segment publish failed for tenant '{}': {err:?}",
                        tenant_id
                    );
                }
            }
        }
    }

    fn observe_failure(&mut self) {
        self.failed_ingests += 1;
    }

    fn observe_batch_failure(&mut self) {
        self.batch_failed_total = self.batch_failed_total.saturating_add(1);
    }

    fn observe_auth_success(&mut self) {
        self.auth_success_total += 1;
    }

    fn observe_auth_failure(&mut self) {
        self.auth_failure_total += 1;
    }

    fn observe_authz_denied(&mut self) {
        self.authz_denied_total += 1;
    }

    fn observe_audit_event(&mut self) {
        self.audit_events_total += 1;
    }

    fn observe_audit_write_error(&mut self) {
        self.audit_write_error_total += 1;
    }

    fn observe_write_route_resolution(&mut self, routed: &RoutedReplica) {
        self.placement_last_shard_id = Some(routed.shard_id);
        self.placement_last_epoch = Some(routed.epoch);
        self.placement_last_role = Some(routed.role);
    }

    fn observe_write_route_rejection(&mut self, error: &WriteRouteError) {
        self.placement_route_reject_total += 1;
        if let WriteRouteError::WrongNode {
            shard_id,
            epoch,
            role,
            ..
        } = error
        {
            self.placement_last_shard_id = Some(*shard_id);
            self.placement_last_epoch = Some(*epoch);
            self.placement_last_role = Some(*role);
        }
    }

    fn flush_wal_if_due(&mut self) {
        let Some(wal) = self.wal.as_mut() else {
            return;
        };
        if wal.background_flush_only() {
            return;
        }
        match wal.flush_pending_sync_if_interval_elapsed() {
            Ok(true) => {
                self.wal_flush_due_total += 1;
                self.wal_flush_success_total += 1;
            }
            Ok(false) => {}
            Err(err) => {
                self.wal_flush_due_total += 1;
                self.wal_flush_failure_total += 1;
                eprintln!("ingestion WAL interval flush failed: {err:?}");
            }
        }
    }

    pub(crate) fn flush_wal_for_async_tick(&mut self) {
        let Some(wal) = self.wal.as_mut() else {
            return;
        };
        self.wal_async_flush_tick_total += 1;
        match wal.flush_pending_sync_if_unsynced() {
            Ok(true) => {
                self.wal_flush_due_total += 1;
                self.wal_flush_success_total += 1;
            }
            Ok(false) => {}
            Err(err) => {
                self.wal_flush_due_total += 1;
                self.wal_flush_failure_total += 1;
                eprintln!("ingestion WAL async flush failed: {err:?}");
            }
        }
    }

    pub(crate) fn wal_async_flush_interval(&self) -> Option<Duration> {
        self.wal_async_flush_interval
    }

    pub(crate) fn set_transport_backpressure_metrics(
        &mut self,
        metrics: Arc<TransportBackpressureMetrics>,
    ) {
        self.transport_backpressure = Some(metrics);
    }

    pub(crate) fn refresh_placement_if_due(&mut self) {
        if let Ok(Some(state)) = self.placement_routing.as_mut() {
            state.maybe_refresh();
        }
    }

    pub(crate) fn segment_maintenance_interval(&self) -> Option<Duration> {
        self.segment_runtime.as_ref()?.maintenance_interval
    }

    pub(crate) fn run_segment_maintenance_tick(&mut self) {
        let Some(segment_runtime) = self.segment_runtime.as_ref() else {
            return;
        };
        self.segment_maintenance_tick_total = self.segment_maintenance_tick_total.saturating_add(1);
        match segment_runtime.maintain_all_tenants() {
            Ok(stats) => {
                self.segment_maintenance_success_total =
                    self.segment_maintenance_success_total.saturating_add(1);
                self.segment_maintenance_last_pruned_count = stats.pruned_file_count;
                self.segment_maintenance_last_tenant_dirs = stats.tenant_dirs_scanned;
                self.segment_maintenance_last_tenant_manifests = stats.tenant_manifests_found;
            }
            Err(err) => {
                self.segment_maintenance_failure_total =
                    self.segment_maintenance_failure_total.saturating_add(1);
                eprintln!("ingestion segment maintenance tick failed: {err:?}");
            }
        }
    }

    fn replication_delta_for_followers(
        &mut self,
        from_offset: usize,
        max_records: usize,
    ) -> Result<WalReplicationDelta, StoreError> {
        let wal = self.wal.as_mut().ok_or_else(|| {
            StoreError::Io("replication source requires persistent WAL mode".to_string())
        })?;
        wal.replication_delta_from(from_offset, max_records)
    }

    fn replication_export_for_followers(&mut self) -> Result<WalReplicationExport, StoreError> {
        let wal = self.wal.as_mut().ok_or_else(|| {
            StoreError::Io("replication source requires persistent WAL mode".to_string())
        })?;
        wal.replication_export()
    }

    fn apply_replication_delta_lines(
        &mut self,
        wal_lines: &[String],
        next_offset: usize,
    ) -> Result<(), StoreError> {
        if wal_lines.is_empty() {
            self.replication_pull_success_total =
                self.replication_pull_success_total.saturating_add(1);
            self.replication_last_offset = next_offset;
            self.replication_last_error = None;
            return Ok(());
        }

        let mut staged_store = self.store.clone();
        for line in wal_lines {
            staged_store.apply_persisted_record_line(line)?;
        }

        if let Some(wal) = self.wal.as_mut() {
            let rollback_point = wal.begin_rollback_point()?;
            let append_result = (|| {
                for line in wal_lines {
                    wal.append_raw_record_line(line)?;
                }
                Ok::<(), StoreError>(())
            })();
            if let Err(err) = append_result {
                if let Err(rollback_err) = wal.rollback_to(rollback_point) {
                    eprintln!(
                        "replication rollback failed after WAL append error: {rollback_err:?}"
                    );
                }
                return Err(err);
            }
        }

        self.store = staged_store;
        for tenant_id in self.store.tenant_ids() {
            self.publish_segments_for_tenant(&tenant_id);
        }
        self.replication_pull_success_total = self.replication_pull_success_total.saturating_add(1);
        self.replication_applied_records_total = self
            .replication_applied_records_total
            .saturating_add(wal_lines.len() as u64);
        self.replication_last_offset = next_offset;
        self.replication_last_error = None;
        Ok(())
    }

    fn apply_replication_export(&mut self, export: WalReplicationExport) -> Result<(), StoreError> {
        let ann_tuning = self.store.ann_tuning().clone();
        let mut rebuilt_store = InMemoryStore::new_with_ann_tuning(ann_tuning);
        for line in &export.snapshot_lines {
            rebuilt_store.apply_persisted_record_line(line)?;
        }
        for line in &export.wal_lines {
            rebuilt_store.apply_persisted_record_line(line)?;
        }
        if let Some(wal) = self.wal.as_mut() {
            wal.replace_with_replication_export(&export)?;
        }
        self.store = rebuilt_store;
        for tenant_id in self.store.tenant_ids() {
            self.publish_segments_for_tenant(&tenant_id);
        }
        self.replication_pull_success_total = self.replication_pull_success_total.saturating_add(1);
        self.replication_applied_records_total = self
            .replication_applied_records_total
            .saturating_add(export.wal_lines.len() as u64);
        self.replication_resync_total = self.replication_resync_total.saturating_add(1);
        self.replication_last_offset = export.wal_lines.len();
        self.replication_last_error = None;
        Ok(())
    }

    fn observe_replication_pull_failure(&mut self, error: String) {
        self.replication_pull_failure_total = self.replication_pull_failure_total.saturating_add(1);
        self.replication_last_error = Some(error);
    }

    fn metrics_text(&self) -> String {
        let placement_enabled = self
            .placement_routing
            .as_ref()
            .ok()
            .and_then(|state| state.as_ref())
            .map(|_| 1)
            .unwrap_or(0);
        let placement_config_error = if self.placement_routing.is_err() {
            1
        } else {
            0
        };
        let placement_state = self
            .placement_routing
            .as_ref()
            .ok()
            .and_then(|state| state.as_ref());
        let placement_snapshot = placement_state
            .map(PlacementRoutingState::observability_snapshot)
            .unwrap_or_default();
        let placement_reload_snapshot = placement_state
            .map(PlacementRoutingState::reload_snapshot)
            .unwrap_or_default();
        let placement_last_shard_id = self
            .placement_last_shard_id
            .map(|value| value as i64)
            .unwrap_or(-1);
        let placement_last_epoch = self.placement_last_epoch.unwrap_or(0);
        let placement_last_role = match self.placement_last_role {
            Some(ReplicaRole::Leader) => 1,
            Some(ReplicaRole::Follower) => 2,
            None => 0,
        };
        let wal_unsynced_records = self
            .wal
            .as_ref()
            .map(FileWal::unsynced_record_count)
            .unwrap_or(0);
        let wal_buffered_records = self
            .wal
            .as_ref()
            .map(FileWal::buffered_record_count)
            .unwrap_or(0);
        let wal_async_flush_enabled = self.wal_async_flush_interval.is_some() as usize;
        let wal_async_flush_interval_ms = self
            .wal_async_flush_interval
            .map(|value| value.as_millis() as u64)
            .unwrap_or(0);
        let wal_background_flush_only = self
            .wal
            .as_ref()
            .map(FileWal::background_flush_only)
            .unwrap_or(false) as usize;
        let transport_queue_capacity = self
            .transport_backpressure
            .as_ref()
            .map(|metrics| metrics.queue_capacity)
            .unwrap_or(0);
        let transport_queue_depth = self
            .transport_backpressure
            .as_ref()
            .map(|metrics| metrics.queue_depth.load(Ordering::Relaxed))
            .unwrap_or(0);
        let transport_queue_full_reject_total = self
            .transport_backpressure
            .as_ref()
            .map(|metrics| metrics.queue_full_reject_total.load(Ordering::Relaxed))
            .unwrap_or(0);
        format!(
            "# TYPE dash_ingest_success_total counter\n\
dash_ingest_success_total {}\n\
# TYPE dash_ingest_failed_total counter\n\
dash_ingest_failed_total {}\n\
# TYPE dash_ingest_batch_success_total counter\n\
dash_ingest_batch_success_total {}\n\
# TYPE dash_ingest_batch_failed_total counter\n\
dash_ingest_batch_failed_total {}\n\
# TYPE dash_ingest_batch_commit_total counter\n\
dash_ingest_batch_commit_total {}\n\
# TYPE dash_ingest_batch_last_size gauge\n\
dash_ingest_batch_last_size {}\n\
# TYPE dash_ingest_batch_idempotent_hit_total counter\n\
dash_ingest_batch_idempotent_hit_total {}\n\
# TYPE dash_ingest_segment_publish_success_total counter\n\
dash_ingest_segment_publish_success_total {}\n\
# TYPE dash_ingest_segment_publish_failure_total counter\n\
dash_ingest_segment_publish_failure_total {}\n\
# TYPE dash_ingest_segment_last_claim_count gauge\n\
dash_ingest_segment_last_claim_count {}\n\
# TYPE dash_ingest_segment_last_segment_count gauge\n\
dash_ingest_segment_last_segment_count {}\n\
# TYPE dash_ingest_segment_last_compaction_plans gauge\n\
dash_ingest_segment_last_compaction_plans {}\n\
# TYPE dash_ingest_segment_last_stale_file_pruned_count gauge\n\
dash_ingest_segment_last_stale_file_pruned_count {}\n\
# TYPE dash_ingest_segment_maintenance_tick_total counter\n\
dash_ingest_segment_maintenance_tick_total {}\n\
# TYPE dash_ingest_segment_maintenance_success_total counter\n\
dash_ingest_segment_maintenance_success_total {}\n\
# TYPE dash_ingest_segment_maintenance_failure_total counter\n\
dash_ingest_segment_maintenance_failure_total {}\n\
# TYPE dash_ingest_segment_maintenance_last_pruned_count gauge\n\
dash_ingest_segment_maintenance_last_pruned_count {}\n\
# TYPE dash_ingest_segment_maintenance_last_tenant_dirs gauge\n\
dash_ingest_segment_maintenance_last_tenant_dirs {}\n\
# TYPE dash_ingest_segment_maintenance_last_tenant_manifests gauge\n\
dash_ingest_segment_maintenance_last_tenant_manifests {}\n\
# TYPE dash_ingest_auth_success_total counter\n\
dash_ingest_auth_success_total {}\n\
# TYPE dash_ingest_auth_failure_total counter\n\
dash_ingest_auth_failure_total {}\n\
# TYPE dash_ingest_authz_denied_total counter\n\
dash_ingest_authz_denied_total {}\n\
# TYPE dash_ingest_audit_events_total counter\n\
dash_ingest_audit_events_total {}\n\
# TYPE dash_ingest_audit_write_error_total counter\n\
dash_ingest_audit_write_error_total {}\n\
# TYPE dash_ingest_placement_enabled gauge\n\
dash_ingest_placement_enabled {}\n\
# TYPE dash_ingest_placement_config_error gauge\n\
dash_ingest_placement_config_error {}\n\
# TYPE dash_ingest_placement_route_reject_total counter\n\
dash_ingest_placement_route_reject_total {}\n\
# TYPE dash_ingest_placement_last_shard_id gauge\n\
dash_ingest_placement_last_shard_id {}\n\
# TYPE dash_ingest_placement_last_epoch gauge\n\
dash_ingest_placement_last_epoch {}\n\
# TYPE dash_ingest_placement_last_role gauge\n\
dash_ingest_placement_last_role {}\n\
# TYPE dash_ingest_placement_leaders_total gauge\n\
dash_ingest_placement_leaders_total {}\n\
# TYPE dash_ingest_placement_followers_total gauge\n\
dash_ingest_placement_followers_total {}\n\
# TYPE dash_ingest_placement_replicas_healthy gauge\n\
dash_ingest_placement_replicas_healthy {}\n\
# TYPE dash_ingest_placement_replicas_degraded gauge\n\
dash_ingest_placement_replicas_degraded {}\n\
# TYPE dash_ingest_placement_replicas_unavailable gauge\n\
dash_ingest_placement_replicas_unavailable {}\n\
# TYPE dash_ingest_placement_reload_enabled gauge\n\
dash_ingest_placement_reload_enabled {}\n\
# TYPE dash_ingest_placement_reload_attempt_total counter\n\
dash_ingest_placement_reload_attempt_total {}\n\
# TYPE dash_ingest_placement_reload_success_total counter\n\
dash_ingest_placement_reload_success_total {}\n\
# TYPE dash_ingest_placement_reload_failure_total counter\n\
dash_ingest_placement_reload_failure_total {}\n\
# TYPE dash_ingest_placement_reload_last_error gauge\n\
dash_ingest_placement_reload_last_error {}\n\
# TYPE dash_ingest_wal_unsynced_records gauge\n\
dash_ingest_wal_unsynced_records {}\n\
# TYPE dash_ingest_wal_buffered_records gauge\n\
dash_ingest_wal_buffered_records {}\n\
# TYPE dash_ingest_wal_flush_due_total counter\n\
dash_ingest_wal_flush_due_total {}\n\
# TYPE dash_ingest_wal_flush_success_total counter\n\
dash_ingest_wal_flush_success_total {}\n\
# TYPE dash_ingest_wal_flush_failure_total counter\n\
dash_ingest_wal_flush_failure_total {}\n\
# TYPE dash_ingest_wal_async_flush_enabled gauge\n\
dash_ingest_wal_async_flush_enabled {}\n\
# TYPE dash_ingest_wal_async_flush_interval_ms gauge\n\
dash_ingest_wal_async_flush_interval_ms {}\n\
# TYPE dash_ingest_wal_async_flush_tick_total counter\n\
dash_ingest_wal_async_flush_tick_total {}\n\
# TYPE dash_ingest_wal_background_flush_only gauge\n\
dash_ingest_wal_background_flush_only {}\n\
# TYPE dash_ingest_transport_queue_capacity gauge\n\
dash_ingest_transport_queue_capacity {}\n\
# TYPE dash_ingest_transport_queue_depth gauge\n\
dash_ingest_transport_queue_depth {}\n\
# TYPE dash_ingest_transport_queue_full_reject_total counter\n\
dash_ingest_transport_queue_full_reject_total {}\n\
# TYPE dash_ingest_replication_pull_success_total counter\n\
dash_ingest_replication_pull_success_total {}\n\
# TYPE dash_ingest_replication_pull_failure_total counter\n\
dash_ingest_replication_pull_failure_total {}\n\
# TYPE dash_ingest_replication_applied_records_total counter\n\
dash_ingest_replication_applied_records_total {}\n\
# TYPE dash_ingest_replication_resync_total counter\n\
dash_ingest_replication_resync_total {}\n\
# TYPE dash_ingest_replication_last_offset gauge\n\
dash_ingest_replication_last_offset {}\n\
# TYPE dash_ingest_replication_last_error gauge\n\
dash_ingest_replication_last_error {}\n\
# TYPE dash_ingest_claims_total gauge\n\
dash_ingest_claims_total {}\n\
# TYPE dash_ingest_uptime_seconds gauge\n\
dash_ingest_uptime_seconds {:.4}\n",
            self.successful_ingests,
            self.failed_ingests,
            self.batch_success_total,
            self.batch_failed_total,
            self.batch_commit_total,
            self.batch_last_size,
            self.batch_idempotent_hit_total,
            self.segment_publish_success_total,
            self.segment_publish_failure_total,
            self.segment_last_claim_count,
            self.segment_last_segment_count,
            self.segment_last_compaction_plans,
            self.segment_last_stale_file_pruned_count,
            self.segment_maintenance_tick_total,
            self.segment_maintenance_success_total,
            self.segment_maintenance_failure_total,
            self.segment_maintenance_last_pruned_count,
            self.segment_maintenance_last_tenant_dirs,
            self.segment_maintenance_last_tenant_manifests,
            self.auth_success_total,
            self.auth_failure_total,
            self.authz_denied_total,
            self.audit_events_total,
            self.audit_write_error_total,
            placement_enabled,
            placement_config_error,
            self.placement_route_reject_total,
            placement_last_shard_id,
            placement_last_epoch,
            placement_last_role,
            placement_snapshot.leaders_total,
            placement_snapshot.followers_total,
            placement_snapshot.replicas_healthy,
            placement_snapshot.replicas_degraded,
            placement_snapshot.replicas_unavailable,
            placement_reload_snapshot.enabled as usize,
            placement_reload_snapshot.attempt_total,
            placement_reload_snapshot.success_total,
            placement_reload_snapshot.failure_total,
            placement_reload_snapshot.last_error.is_some() as usize,
            wal_unsynced_records,
            wal_buffered_records,
            self.wal_flush_due_total,
            self.wal_flush_success_total,
            self.wal_flush_failure_total,
            wal_async_flush_enabled,
            wal_async_flush_interval_ms,
            self.wal_async_flush_tick_total,
            wal_background_flush_only,
            transport_queue_capacity,
            transport_queue_depth,
            transport_queue_full_reject_total,
            self.replication_pull_success_total,
            self.replication_pull_failure_total,
            self.replication_applied_records_total,
            self.replication_resync_total,
            self.replication_last_offset,
            self.replication_last_error.is_some() as usize,
            self.store.claims_len(),
            self.started_at.elapsed().as_secs_f64()
        )
    }
}

pub(crate) type SharedRuntime = Arc<Mutex<IngestionRuntime>>;
const MAX_HTTP_BODY_BYTES: usize = 16 * 1024 * 1024;
const SOCKET_TIMEOUT_SECS: u64 = 5;
const DEFAULT_HTTP_WORKERS: usize = 4;
const DEFAULT_HTTP_QUEUE_CAPACITY_PER_WORKER: usize = 64;
const DEFAULT_ASYNC_WAL_FLUSH_INTERVAL_MS: u64 = 250;
const DEFAULT_SEGMENT_MAINTENANCE_INTERVAL_MS: u64 = 30_000;
const DEFAULT_SEGMENT_GC_MIN_STALE_AGE_MS: u64 = 60_000;
const DEFAULT_INGEST_BATCH_MAX_ITEMS: usize = 128;
const DEFAULT_REPLICATION_PULL_MAX_RECORDS: usize = 512;
static BATCH_COMMIT_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, PartialEq, Eq)]
struct SegmentRuntime {
    root_dir: PathBuf,
    max_segment_size: usize,
    scheduler: CompactionSchedulerConfig,
    maintenance_interval: Option<Duration>,
    maintenance_min_stale_age: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SegmentPublishStats {
    claim_count: usize,
    segment_count: usize,
    compaction_plan_count: usize,
    stale_file_pruned_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PlacementRoutingRuntime {
    local_node_id: String,
    router_config: RouterConfig,
    placements: Vec<ShardPlacement>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PlacementRoutingState {
    runtime: PlacementRoutingRuntime,
    reload: Option<PlacementReloadRuntime>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PlacementReloadRuntime {
    config: PlacementReloadConfig,
    next_reload_at: Instant,
    attempt_total: u64,
    success_total: u64,
    failure_total: u64,
    last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PlacementReloadConfig {
    placement_file: PathBuf,
    shard_ids_override: Option<Vec<u32>>,
    replica_count_override: Option<usize>,
    virtual_nodes_per_shard: u32,
    reload_interval: Duration,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct PlacementObservabilitySnapshot {
    leaders_total: usize,
    followers_total: usize,
    replicas_healthy: usize,
    replicas_degraded: usize,
    replicas_unavailable: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct PlacementReloadSnapshot {
    enabled: bool,
    interval_ms: Option<u64>,
    attempt_total: u64,
    success_total: u64,
    failure_total: u64,
    last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum WriteRouteError {
    Config(String),
    Placement(PlacementRouteError),
    WrongNode {
        local_node_id: String,
        target_node_id: String,
        shard_id: u32,
        epoch: u64,
        role: ReplicaRole,
    },
}

impl PlacementRoutingRuntime {
    fn observability_snapshot(&self) -> PlacementObservabilitySnapshot {
        let mut snapshot = PlacementObservabilitySnapshot::default();
        for placement in &self.placements {
            for replica in &placement.replicas {
                match replica.role {
                    ReplicaRole::Leader => snapshot.leaders_total += 1,
                    ReplicaRole::Follower => snapshot.followers_total += 1,
                }
                match replica.health {
                    ReplicaHealth::Healthy => snapshot.replicas_healthy += 1,
                    ReplicaHealth::Degraded => snapshot.replicas_degraded += 1,
                    ReplicaHealth::Unavailable => snapshot.replicas_unavailable += 1,
                }
            }
        }
        snapshot
    }
}

impl SegmentRuntime {
    fn from_env() -> Option<Self> {
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

    fn publish_for_tenant(
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

    fn maintain_all_tenants(&self) -> Result<SegmentMaintenanceStats, SegmentStoreError> {
        maintain_segment_root(&self.root_dir, self.maintenance_min_stale_age)
    }
}

impl PlacementRoutingState {
    fn from_env() -> Result<Option<Self>, String> {
        let Some(placement_file) =
            env_with_fallback("DASH_ROUTER_PLACEMENT_FILE", "EME_ROUTER_PLACEMENT_FILE")
        else {
            return Ok(None);
        };
        let local_node_id = env_with_fallback(
            "DASH_ROUTER_LOCAL_NODE_ID",
            "EME_ROUTER_LOCAL_NODE_ID",
        )
        .or_else(|| env_with_fallback("DASH_NODE_ID", "EME_NODE_ID"))
        .ok_or_else(|| {
            "placement routing enabled, but DASH_ROUTER_LOCAL_NODE_ID (or DASH_NODE_ID) is unset"
                .to_string()
        })?;
        let local_node_id = local_node_id.trim();
        if local_node_id.is_empty() {
            return Err(
                "placement routing enabled, but local node id is empty after trimming".to_string(),
            );
        }

        let shard_ids_override = parse_csv_u32_env("DASH_ROUTER_SHARD_IDS", "EME_ROUTER_SHARD_IDS");
        let replica_count_override =
            parse_env_first_usize(&["DASH_ROUTER_REPLICA_COUNT", "EME_ROUTER_REPLICA_COUNT"])
                .filter(|value| *value > 0);
        let virtual_nodes_per_shard = parse_env_first_usize(&[
            "DASH_ROUTER_VIRTUAL_NODES_PER_SHARD",
            "EME_ROUTER_VIRTUAL_NODES_PER_SHARD",
        ])
        .filter(|value| *value > 0)
        .unwrap_or(64) as u32;
        let runtime = load_placement_routing_runtime(
            Path::new(&placement_file),
            local_node_id,
            shard_ids_override.as_deref(),
            replica_count_override,
            virtual_nodes_per_shard,
        )?;

        let reload_interval_ms = parse_env_first_u64(&[
            "DASH_ROUTER_PLACEMENT_RELOAD_INTERVAL_MS",
            "EME_ROUTER_PLACEMENT_RELOAD_INTERVAL_MS",
        ])
        .filter(|value| *value > 0);
        let reload = reload_interval_ms.map(|interval_ms| {
            let reload_interval = Duration::from_millis(interval_ms);
            PlacementReloadRuntime {
                config: PlacementReloadConfig {
                    placement_file: PathBuf::from(&placement_file),
                    shard_ids_override: shard_ids_override.clone(),
                    replica_count_override,
                    virtual_nodes_per_shard,
                    reload_interval,
                },
                next_reload_at: Instant::now() + reload_interval,
                attempt_total: 0,
                success_total: 0,
                failure_total: 0,
                last_error: None,
            }
        });

        Ok(Some(Self { runtime, reload }))
    }

    #[cfg(test)]
    fn from_static_runtime(runtime: PlacementRoutingRuntime) -> Self {
        Self {
            runtime,
            reload: None,
        }
    }

    fn runtime(&self) -> &PlacementRoutingRuntime {
        &self.runtime
    }

    fn observability_snapshot(&self) -> PlacementObservabilitySnapshot {
        self.runtime.observability_snapshot()
    }

    fn reload_snapshot(&self) -> PlacementReloadSnapshot {
        let Some(reload) = self.reload.as_ref() else {
            return PlacementReloadSnapshot::default();
        };
        PlacementReloadSnapshot {
            enabled: true,
            interval_ms: Some(reload.config.reload_interval.as_millis() as u64),
            attempt_total: reload.attempt_total,
            success_total: reload.success_total,
            failure_total: reload.failure_total,
            last_error: reload.last_error.clone(),
        }
    }

    fn maybe_refresh(&mut self) {
        let Some(reload) = self.reload.as_mut() else {
            return;
        };
        let now = Instant::now();
        if now < reload.next_reload_at {
            return;
        }
        reload.attempt_total = reload.attempt_total.saturating_add(1);
        match load_placement_routing_runtime(
            &reload.config.placement_file,
            &self.runtime.local_node_id,
            reload.config.shard_ids_override.as_deref(),
            reload.config.replica_count_override,
            reload.config.virtual_nodes_per_shard,
        ) {
            Ok(runtime) => {
                self.runtime = runtime;
                reload.success_total = reload.success_total.saturating_add(1);
                reload.last_error = None;
            }
            Err(reason) => {
                reload.failure_total = reload.failure_total.saturating_add(1);
                reload.last_error = Some(reason.clone());
                eprintln!("ingestion placement reload failed: {reason}");
            }
        }
        reload.next_reload_at = now + reload.config.reload_interval;
    }
}

fn load_placement_routing_runtime(
    placement_file: &Path,
    local_node_id: &str,
    shard_ids_override: Option<&[u32]>,
    replica_count_override: Option<usize>,
    virtual_nodes_per_shard: u32,
) -> Result<PlacementRoutingRuntime, String> {
    let placements = load_shard_placements_csv(placement_file)?;
    if placements.is_empty() {
        return Err(format!(
            "placement file '{}' has no placement records",
            placement_file.display()
        ));
    }

    let shard_ids = shard_ids_override
        .map(|ids| ids.to_vec())
        .unwrap_or_else(|| shard_ids_from_placements(&placements));
    if shard_ids.is_empty() {
        return Err("placement routing requires at least one shard id".to_string());
    }
    let replica_count = replica_count_override.unwrap_or_else(|| {
        placements
            .iter()
            .map(|placement| placement.replicas.len())
            .max()
            .unwrap_or(1)
    });

    Ok(PlacementRoutingRuntime {
        local_node_id: local_node_id.to_string(),
        router_config: RouterConfig {
            shard_ids,
            virtual_nodes_per_shard,
            replica_count,
        },
        placements,
    })
}

fn write_entity_key_for_claim(claim: &Claim) -> &str {
    claim
        .entities
        .iter()
        .find(|value| !value.trim().is_empty())
        .map(String::as_str)
        .unwrap_or(claim.claim_id.as_str())
}

fn parse_csv_u32_env(primary: &str, fallback: &str) -> Option<Vec<u32>> {
    let raw = env_with_fallback(primary, fallback)?;
    let mut values = Vec::new();
    for item in raw.split(',') {
        let item = item.trim();
        if item.is_empty() {
            continue;
        }
        if let Ok(parsed) = item.parse::<u32>() {
            values.push(parsed);
        }
    }
    values.sort_unstable();
    values.dedup();
    if values.is_empty() {
        None
    } else {
        Some(values)
    }
}

fn map_write_route_error(error: &WriteRouteError) -> (u16, String) {
    match error {
        WriteRouteError::Config(reason) => (
            500,
            format!("placement routing configuration error: {reason}"),
        ),
        WriteRouteError::Placement(reason) => (
            503,
            format!("placement route rejected write request: {reason:?}"),
        ),
        WriteRouteError::WrongNode {
            local_node_id,
            target_node_id,
            shard_id,
            epoch,
            role: _,
        } => (
            503,
            format!(
                "placement route rejected write request: local node '{local_node_id}' is not leader for shard {shard_id} at epoch {epoch} (target leader: '{target_node_id}')"
            ),
        ),
    }
}

pub(crate) fn resolve_http_queue_capacity(worker_count: usize) -> usize {
    let default_capacity = worker_count
        .saturating_mul(DEFAULT_HTTP_QUEUE_CAPACITY_PER_WORKER)
        .max(worker_count);
    parse_env_first_usize(&[
        "DASH_INGEST_HTTP_QUEUE_CAPACITY",
        "EME_INGEST_HTTP_QUEUE_CAPACITY",
    ])
    .filter(|value| *value > 0)
    .unwrap_or(default_capacity)
}

pub fn serve_http(runtime: IngestionRuntime, bind_addr: &str) -> std::io::Result<()> {
    serve_http_with_workers(runtime, bind_addr, DEFAULT_HTTP_WORKERS)
}

pub fn serve_http_with_workers(
    runtime: IngestionRuntime,
    bind_addr: &str,
    worker_count: usize,
) -> std::io::Result<()> {
    let listener = TcpListener::bind(bind_addr)?;
    let worker_count = worker_count.max(1);
    let queue_capacity = resolve_http_queue_capacity(worker_count);
    let wal_async_flush_interval = runtime.wal_async_flush_interval();
    let segment_maintenance_interval = runtime.segment_maintenance_interval();
    let replication_pull = ReplicationPullConfig::from_env();
    let runtime = Arc::new(Mutex::new(runtime));
    let backpressure_metrics = Arc::new(TransportBackpressureMetrics::new(queue_capacity));
    if let Ok(mut guard) = runtime.lock() {
        guard.set_transport_backpressure_metrics(Arc::clone(&backpressure_metrics));
    }
    let (tx, rx) = mpsc::sync_channel::<TcpStream>(queue_capacity);
    let rx = Arc::new(Mutex::new(rx));
    let (flush_shutdown_tx, flush_shutdown_rx) = mpsc::channel::<()>();
    let (segment_shutdown_tx, segment_shutdown_rx) = mpsc::channel::<()>();
    let (replication_shutdown_tx, replication_shutdown_rx) = mpsc::channel::<()>();

    std::thread::scope(|scope| {
        if let Some(async_interval) = wal_async_flush_interval {
            let runtime = Arc::clone(&runtime);
            scope.spawn(move || {
                loop {
                    match flush_shutdown_rx.recv_timeout(async_interval) {
                        Ok(_) | Err(mpsc::RecvTimeoutError::Disconnected) => break,
                        Err(mpsc::RecvTimeoutError::Timeout) => {}
                    }
                    let Ok(mut guard) = runtime.lock() else {
                        break;
                    };
                    guard.flush_wal_for_async_tick();
                    guard.refresh_placement_if_due();
                }
            });
        }
        if let Some(maintenance_interval) = segment_maintenance_interval {
            let runtime = Arc::clone(&runtime);
            scope.spawn(move || {
                loop {
                    match segment_shutdown_rx.recv_timeout(maintenance_interval) {
                        Ok(_) | Err(mpsc::RecvTimeoutError::Disconnected) => break,
                        Err(mpsc::RecvTimeoutError::Timeout) => {}
                    }
                    let Ok(mut guard) = runtime.lock() else {
                        break;
                    };
                    guard.run_segment_maintenance_tick();
                    guard.refresh_placement_if_due();
                }
            });
        }
        if let Some(replication_pull) = replication_pull.clone() {
            let runtime = Arc::clone(&runtime);
            scope.spawn(move || {
                loop {
                    match replication_shutdown_rx.recv_timeout(replication_pull.poll_interval) {
                        Ok(_) | Err(mpsc::RecvTimeoutError::Disconnected) => break,
                        Err(mpsc::RecvTimeoutError::Timeout) => {}
                    }
                    run_replication_pull_tick(&runtime, &replication_pull);
                }
            });
        }

        for _ in 0..worker_count {
            let runtime = Arc::clone(&runtime);
            let rx = Arc::clone(&rx);
            let backpressure_metrics = Arc::clone(&backpressure_metrics);
            scope.spawn(move || {
                loop {
                    let stream = {
                        let guard = match rx.lock() {
                            Ok(guard) => guard,
                            Err(_) => break,
                        };
                        match guard.recv() {
                            Ok(stream) => {
                                backpressure_metrics.observe_dequeued();
                                stream
                            }
                            Err(_) => break,
                        }
                    };
                    if let Err(err) = handle_connection(&runtime, stream) {
                        eprintln!("ingestion transport error: {err}");
                    }
                }
            });
        }

        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    backpressure_metrics.observe_enqueued();
                    match tx.try_send(stream) {
                        Ok(()) => {}
                        Err(mpsc::TrySendError::Full(stream)) => {
                            backpressure_metrics.observe_dequeued();
                            backpressure_metrics.observe_rejected();
                            if let Err(err) =
                                write_backpressure_response(stream, SOCKET_TIMEOUT_SECS)
                            {
                                eprintln!(
                                    "ingestion transport backpressure response failed: {err}"
                                );
                            }
                        }
                        Err(mpsc::TrySendError::Disconnected(_)) => {
                            backpressure_metrics.observe_dequeued();
                            eprintln!("ingestion transport worker queue closed");
                            break;
                        }
                    }
                }
                Err(err) => eprintln!("ingestion transport accept error: {err}"),
            }
        }
        let _ = flush_shutdown_tx.send(());
        let _ = segment_shutdown_tx.send(());
        let _ = replication_shutdown_tx.send(());
        drop(tx);
    });

    Ok(())
}

pub fn handle_http_request_bytes(
    runtime: &Arc<Mutex<IngestionRuntime>>,
    raw_request: &[u8],
) -> Result<Vec<u8>, String> {
    let request_text =
        std::str::from_utf8(raw_request).map_err(|_| "request must be valid UTF-8".to_string())?;
    let (header_block, body) = request_text
        .split_once("\r\n\r\n")
        .ok_or_else(|| "missing HTTP header terminator".to_string())?;

    let mut lines = header_block.split("\r\n");
    let request_line = lines
        .next()
        .ok_or_else(|| "missing request line".to_string())?;
    let (method, target) = parse_request_line(request_line)?;

    let mut headers = HashMap::new();
    for line in lines {
        if line.trim().is_empty() {
            continue;
        }
        let (name, value) = line
            .split_once(':')
            .ok_or_else(|| "invalid HTTP header".to_string())?;
        headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_string());
    }

    let content_length = match headers.get("content-length") {
        Some(raw) => raw
            .parse::<usize>()
            .map_err(|_| "invalid content-length header".to_string())?,
        None => 0,
    };
    if content_length > MAX_HTTP_BODY_BYTES {
        return Err(format!(
            "content-length exceeds max body size ({MAX_HTTP_BODY_BYTES} bytes)"
        ));
    }
    if content_length != body.len() {
        return Err("content-length does not match body size".to_string());
    }

    let request = HttpRequest {
        method,
        target,
        headers,
        body: body.as_bytes().to_vec(),
    };
    let response = handle_request(runtime, &request);
    Ok(render_response_text(&response).into_bytes())
}

fn handle_connection(runtime: &SharedRuntime, mut stream: TcpStream) -> std::io::Result<()> {
    stream.set_read_timeout(Some(Duration::from_secs(SOCKET_TIMEOUT_SECS)))?;
    stream.set_write_timeout(Some(Duration::from_secs(SOCKET_TIMEOUT_SECS)))?;

    let request = match read_http_request(&mut stream) {
        Ok(Some(request)) => request,
        Ok(None) => return Ok(()),
        Err(err) => return write_response(&mut stream, HttpResponse::bad_request(&err)),
    };

    let response = handle_request(runtime, &request);
    write_response(&mut stream, response)
}

fn read_http_request(stream: &mut TcpStream) -> Result<Option<HttpRequest>, String> {
    let mut reader = BufReader::new(stream);

    let mut request_line = String::new();
    let bytes = reader
        .read_line(&mut request_line)
        .map_err(|e| e.to_string())?;
    if bytes == 0 {
        return Ok(None);
    }

    let (method, target) = parse_request_line(&request_line)?;

    let mut headers = HashMap::new();
    loop {
        let mut header_line = String::new();
        let bytes = reader
            .read_line(&mut header_line)
            .map_err(|e| e.to_string())?;
        if bytes == 0 || header_line == "\r\n" {
            break;
        }
        let (name, value) = header_line
            .split_once(':')
            .ok_or_else(|| "invalid HTTP header".to_string())?;
        headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_string());
    }

    let content_length = match headers.get("content-length") {
        Some(raw) => raw
            .parse::<usize>()
            .map_err(|_| "invalid content-length header".to_string())?,
        None => 0,
    };
    if content_length > MAX_HTTP_BODY_BYTES {
        return Err(format!(
            "content-length exceeds max body size ({MAX_HTTP_BODY_BYTES} bytes)"
        ));
    }
    let mut body = vec![0u8; content_length];
    if content_length > 0 {
        reader.read_exact(&mut body).map_err(|e| e.to_string())?;
    }

    Ok(Some(HttpRequest {
        method,
        target,
        headers,
        body,
    }))
}

fn parse_request_line(line: &str) -> Result<(String, String), String> {
    let line = line.trim();
    let mut parts = line.split_whitespace();
    let method = parts
        .next()
        .ok_or_else(|| "missing HTTP method".to_string())?;
    let target = parts
        .next()
        .ok_or_else(|| "missing HTTP target".to_string())?;
    let version = parts
        .next()
        .ok_or_else(|| "missing HTTP version".to_string())?;
    if !version.starts_with("HTTP/1.") {
        return Err("unsupported HTTP version".to_string());
    }
    Ok((method.to_string(), target.to_string()))
}

pub(crate) fn handle_request(runtime: &SharedRuntime, request: &HttpRequest) -> HttpResponse {
    let (path, query) = split_target(&request.target);
    let auth_policy = AuthPolicy::from_env(
        env_with_fallback("DASH_INGEST_API_KEY", "EME_INGEST_API_KEY"),
        env_with_fallback("DASH_INGEST_API_KEYS", "EME_INGEST_API_KEYS"),
        env_with_fallback(
            "DASH_INGEST_REVOKED_API_KEYS",
            "EME_INGEST_REVOKED_API_KEYS",
        ),
        env_with_fallback("DASH_INGEST_ALLOWED_TENANTS", "EME_INGEST_ALLOWED_TENANTS"),
        env_with_fallback("DASH_INGEST_API_KEY_SCOPES", "EME_INGEST_API_KEY_SCOPES"),
    );
    let audit_log_path =
        env_with_fallback("DASH_INGEST_AUDIT_LOG_PATH", "EME_INGEST_AUDIT_LOG_PATH");
    match (request.method.as_str(), path.as_str()) {
        ("GET", "/health") => HttpResponse::ok_json("{\"status\":\"ok\"}".to_string()),
        ("GET", "/metrics") => {
            let body = match runtime.lock() {
                Ok(mut rt) => {
                    rt.flush_wal_if_due();
                    rt.refresh_placement_if_due();
                    rt.metrics_text()
                }
                Err(_) => "dash_ingest_metrics_unavailable 1\n".to_string(),
            };
            HttpResponse::ok_text(body)
        }
        ("GET", "/debug/placement") => match runtime.lock() {
            Ok(mut rt) => {
                rt.refresh_placement_if_due();
                HttpResponse::ok_json(render_placement_debug_json(&rt, &query))
            }
            Err(_) => {
                HttpResponse::internal_server_error("failed to acquire ingestion runtime lock")
            }
        },
        ("GET", "/internal/replication/wal") => {
            if !is_replication_request_authorized(request) {
                return HttpResponse::forbidden("replication request is not authorized");
            }
            let from_offset = match parse_query_usize(&query, "from_offset") {
                Ok(value) => value.unwrap_or(0),
                Err(err) => return HttpResponse::bad_request(&err),
            };
            let max_records = match parse_query_usize(&query, "max_records") {
                Ok(value) => value.unwrap_or(DEFAULT_REPLICATION_PULL_MAX_RECORDS),
                Err(err) => return HttpResponse::bad_request(&err),
            };
            match runtime.lock() {
                Ok(mut rt) => match rt.replication_delta_for_followers(from_offset, max_records) {
                    Ok(delta) => HttpResponse::ok_plain(render_replication_delta_frame(&delta)),
                    Err(err) => {
                        let (status, message) = map_store_error(&err);
                        HttpResponse::error_with_status(status, &message)
                    }
                },
                Err(_) => {
                    HttpResponse::internal_server_error("failed to acquire ingestion runtime lock")
                }
            }
        }
        ("GET", "/internal/replication/export") => {
            if !is_replication_request_authorized(request) {
                return HttpResponse::forbidden("replication request is not authorized");
            }
            match runtime.lock() {
                Ok(mut rt) => match rt.replication_export_for_followers() {
                    Ok(export) => HttpResponse::ok_plain(render_replication_export_frame(&export)),
                    Err(err) => {
                        let (status, message) = map_store_error(&err);
                        HttpResponse::error_with_status(status, &message)
                    }
                },
                Err(_) => {
                    HttpResponse::internal_server_error("failed to acquire ingestion runtime lock")
                }
            }
        }
        ("POST", "/v1/ingest") => {
            if let Some(content_type) = request.headers.get("content-type")
                && !content_type
                    .to_ascii_lowercase()
                    .contains("application/json")
            {
                return HttpResponse::bad_request(
                    "content-type must include application/json for POST /v1/ingest",
                );
            }
            let body = match std::str::from_utf8(&request.body) {
                Ok(body) => body,
                Err(_) => return HttpResponse::bad_request("request body must be valid UTF-8"),
            };
            match build_ingest_request_from_json(body) {
                Ok(api_req) => {
                    let tenant_id = api_req.claim.tenant_id.clone();
                    let claim_id = api_req.claim.claim_id.clone();
                    match authorize_request_for_tenant(request, &tenant_id, &auth_policy) {
                        AuthDecision::Unauthorized(reason) => {
                            observe_auth_failure(runtime);
                            emit_audit_event(
                                runtime,
                                audit_log_path.as_deref(),
                                AuditEvent {
                                    action: "ingest",
                                    tenant_id: Some(&tenant_id),
                                    claim_id: Some(&claim_id),
                                    status: 401,
                                    outcome: "denied",
                                    reason,
                                },
                            );
                            return HttpResponse::unauthorized(reason);
                        }
                        AuthDecision::Forbidden(reason) => {
                            observe_authz_denied(runtime);
                            emit_audit_event(
                                runtime,
                                audit_log_path.as_deref(),
                                AuditEvent {
                                    action: "ingest",
                                    tenant_id: Some(&tenant_id),
                                    claim_id: Some(&claim_id),
                                    status: 403,
                                    outcome: "denied",
                                    reason,
                                },
                            );
                            return HttpResponse::forbidden(reason);
                        }
                        AuthDecision::Allowed => {
                            observe_auth_success(runtime);
                        }
                    }

                    let mut audit_status = 500;
                    let mut audit_outcome = "error";
                    let mut audit_reason = "runtime lock unavailable".to_string();
                    let mut guard = match runtime.lock() {
                        Ok(guard) => guard,
                        Err(_) => {
                            emit_audit_event(
                                runtime,
                                audit_log_path.as_deref(),
                                AuditEvent {
                                    action: "ingest",
                                    tenant_id: Some(&tenant_id),
                                    claim_id: Some(&claim_id),
                                    status: audit_status,
                                    outcome: audit_outcome,
                                    reason: &audit_reason,
                                },
                            );
                            return HttpResponse::internal_server_error(
                                "failed to acquire ingestion runtime lock",
                            );
                        }
                    };
                    guard.flush_wal_if_due();
                    let response = match guard.ensure_local_write_route_for_claim(&api_req.claim) {
                        Ok(()) => match guard.ingest(api_req) {
                            Ok(resp) => {
                                audit_status = 200;
                                audit_outcome = "success";
                                audit_reason = "ingest accepted".to_string();
                                HttpResponse::ok_json(render_ingest_response_json(&resp))
                            }
                            Err(err) => {
                                guard.observe_failure();
                                let (status, message) = map_store_error(&err);
                                audit_status = status;
                                audit_reason = message.clone();
                                HttpResponse::error_with_status(status, &message)
                            }
                        },
                        Err(route_err) => {
                            guard.observe_failure();
                            guard.observe_write_route_rejection(&route_err);
                            audit_outcome = "denied";
                            let (status, message) = map_write_route_error(&route_err);
                            audit_status = status;
                            audit_reason = message.clone();
                            HttpResponse::error_with_status(status, &message)
                        }
                    };
                    drop(guard);
                    emit_audit_event(
                        runtime,
                        audit_log_path.as_deref(),
                        AuditEvent {
                            action: "ingest",
                            tenant_id: Some(&tenant_id),
                            claim_id: Some(&claim_id),
                            status: audit_status,
                            outcome: audit_outcome,
                            reason: &audit_reason,
                        },
                    );
                    response
                }
                Err(err) => HttpResponse::bad_request(&err),
            }
        }
        ("POST", "/v1/ingest/batch") => {
            if let Some(content_type) = request.headers.get("content-type")
                && !content_type
                    .to_ascii_lowercase()
                    .contains("application/json")
            {
                return HttpResponse::bad_request(
                    "content-type must include application/json for POST /v1/ingest/batch",
                );
            }
            let body = match std::str::from_utf8(&request.body) {
                Ok(body) => body,
                Err(_) => return HttpResponse::bad_request("request body must be valid UTF-8"),
            };
            let max_items = resolve_ingest_batch_max_items();
            match build_ingest_batch_request_from_json(body, max_items) {
                Ok(api_req) => {
                    let tenant_id = api_req.items[0].claim.tenant_id.clone();
                    match authorize_request_for_tenant(request, &tenant_id, &auth_policy) {
                        AuthDecision::Unauthorized(reason) => {
                            observe_auth_failure(runtime);
                            emit_audit_event(
                                runtime,
                                audit_log_path.as_deref(),
                                AuditEvent {
                                    action: "ingest_batch",
                                    tenant_id: Some(&tenant_id),
                                    claim_id: None,
                                    status: 401,
                                    outcome: "denied",
                                    reason,
                                },
                            );
                            return HttpResponse::unauthorized(reason);
                        }
                        AuthDecision::Forbidden(reason) => {
                            observe_authz_denied(runtime);
                            emit_audit_event(
                                runtime,
                                audit_log_path.as_deref(),
                                AuditEvent {
                                    action: "ingest_batch",
                                    tenant_id: Some(&tenant_id),
                                    claim_id: None,
                                    status: 403,
                                    outcome: "denied",
                                    reason,
                                },
                            );
                            return HttpResponse::forbidden(reason);
                        }
                        AuthDecision::Allowed => {
                            observe_auth_success(runtime);
                        }
                    }

                    let mut audit_status = 500;
                    let mut audit_outcome = "error";
                    let mut audit_reason = "runtime lock unavailable".to_string();
                    let mut guard = match runtime.lock() {
                        Ok(guard) => guard,
                        Err(_) => {
                            emit_audit_event(
                                runtime,
                                audit_log_path.as_deref(),
                                AuditEvent {
                                    action: "ingest_batch",
                                    tenant_id: Some(&tenant_id),
                                    claim_id: None,
                                    status: audit_status,
                                    outcome: audit_outcome,
                                    reason: &audit_reason,
                                },
                            );
                            return HttpResponse::internal_server_error(
                                "failed to acquire ingestion runtime lock",
                            );
                        }
                    };
                    guard.flush_wal_if_due();
                    for item in &api_req.items {
                        if let Err(route_err) =
                            guard.ensure_local_write_route_for_claim(&item.claim)
                        {
                            guard.observe_failure();
                            guard.observe_batch_failure();
                            guard.observe_write_route_rejection(&route_err);
                            audit_outcome = "denied";
                            let (status, message) = map_write_route_error(&route_err);
                            audit_status = status;
                            audit_reason = message.clone();
                            let response = HttpResponse::error_with_status(status, &message);
                            drop(guard);
                            emit_audit_event(
                                runtime,
                                audit_log_path.as_deref(),
                                AuditEvent {
                                    action: "ingest_batch",
                                    tenant_id: Some(&tenant_id),
                                    claim_id: None,
                                    status: audit_status,
                                    outcome: audit_outcome,
                                    reason: &audit_reason,
                                },
                            );
                            return response;
                        }
                    }
                    let response = match guard.ingest_batch(api_req) {
                        Ok(resp) => {
                            audit_status = 200;
                            audit_outcome = "success";
                            audit_reason =
                                format!("ingest batch accepted (commit_id={})", resp.commit_id);
                            HttpResponse::ok_json(render_ingest_batch_response_json(&resp))
                        }
                        Err(err) => {
                            guard.observe_failure();
                            guard.observe_batch_failure();
                            let (status, message) = map_store_error(&err);
                            audit_status = status;
                            audit_reason = message.clone();
                            HttpResponse::error_with_status(status, &message)
                        }
                    };
                    drop(guard);
                    emit_audit_event(
                        runtime,
                        audit_log_path.as_deref(),
                        AuditEvent {
                            action: "ingest_batch",
                            tenant_id: Some(&tenant_id),
                            claim_id: None,
                            status: audit_status,
                            outcome: audit_outcome,
                            reason: &audit_reason,
                        },
                    );
                    response
                }
                Err(err) => HttpResponse::bad_request(&err),
            }
        }
        (_, "/v1/ingest") => HttpResponse::method_not_allowed("only POST is supported"),
        (_, "/v1/ingest/batch") => HttpResponse::method_not_allowed("only POST is supported"),
        (_, "/health")
        | (_, "/metrics")
        | (_, "/debug/placement")
        | (_, "/internal/replication/wal")
        | (_, "/internal/replication/export") => {
            HttpResponse::method_not_allowed("only GET is supported")
        }
        _ => HttpResponse::not_found("unknown path"),
    }
}

fn render_placement_debug_json(
    runtime: &IngestionRuntime,
    query: &HashMap<String, String>,
) -> String {
    let (enabled, config_error, local_node_id, shard_count, placements, route_probe, reload) =
        match runtime.placement_routing.as_ref() {
            Ok(Some(routing)) => (
                true,
                None,
                Some(routing.runtime().local_node_id.clone()),
                routing.runtime().router_config.shard_ids.len(),
                routing.runtime().placements.as_slice(),
                build_write_route_probe_json(Some(routing.runtime()), query),
                render_placement_reload_json(Some(routing)),
            ),
            Ok(None) => (
                false,
                None,
                None,
                0,
                &[][..],
                "null".to_string(),
                render_placement_reload_json(None),
            ),
            Err(reason) => (
                false,
                Some(reason.as_str()),
                None,
                0,
                &[][..],
                "null".to_string(),
                render_placement_reload_json(None),
            ),
        };

    format!(
        "{{\"enabled\":{},\"config_error\":{},\"local_node_id\":{},\"shard_count\":{},\"placements\":{},\"route_probe\":{},\"reload\":{}}}",
        enabled,
        config_error
            .map(|value| format!("\"{}\"", json_escape(value)))
            .unwrap_or_else(|| "null".to_string()),
        local_node_id
            .as_ref()
            .map(|value| format!("\"{}\"", json_escape(value)))
            .unwrap_or_else(|| "null".to_string()),
        shard_count,
        render_placements_json(placements),
        route_probe,
        reload
    )
}

fn render_placement_reload_json(state: Option<&PlacementRoutingState>) -> String {
    let snapshot = state
        .map(PlacementRoutingState::reload_snapshot)
        .unwrap_or_default();
    format!(
        "{{\"enabled\":{},\"interval_ms\":{},\"attempt_total\":{},\"success_total\":{},\"failure_total\":{},\"last_error\":{}}}",
        snapshot.enabled,
        snapshot
            .interval_ms
            .map(|value| value.to_string())
            .unwrap_or_else(|| "null".to_string()),
        snapshot.attempt_total,
        snapshot.success_total,
        snapshot.failure_total,
        snapshot
            .last_error
            .as_ref()
            .map(|value| format!("\"{}\"", json_escape(value)))
            .unwrap_or_else(|| "null".to_string()),
    )
}

fn build_write_route_probe_json(
    routing: Option<&PlacementRoutingRuntime>,
    query: &HashMap<String, String>,
) -> String {
    let Some(tenant_id) = query.get("tenant_id").map(String::as_str) else {
        return "null".to_string();
    };
    let tenant_id = tenant_id.trim();
    if tenant_id.is_empty() {
        return "{\"status\":\"invalid\",\"reason\":\"tenant_id must not be empty\"}".to_string();
    }
    let entity_key = query
        .get("entity_key")
        .map(String::as_str)
        .unwrap_or_default()
        .trim();
    if entity_key.is_empty() {
        return "{\"status\":\"invalid\",\"reason\":\"entity_key is required for route probe\"}"
            .to_string();
    }
    let Some(routing) = routing else {
        return "{\"status\":\"unconfigured\",\"reason\":\"placement routing is disabled\"}"
            .to_string();
    };

    match route_write_with_placement(
        tenant_id,
        entity_key,
        &routing.router_config,
        &routing.placements,
    ) {
        Ok(routed) => {
            let local_admission = routed.node_id == routing.local_node_id;
            let reason = if local_admission {
                "local node is write leader"
            } else {
                "request would be rejected: local node is not write leader"
            };
            format!(
                "{{\"status\":\"{}\",\"tenant_id\":\"{}\",\"entity_key\":\"{}\",\"local_node_id\":\"{}\",\"target_node_id\":\"{}\",\"shard_id\":{},\"epoch\":{},\"role\":\"{}\",\"local_admission\":{},\"reason\":\"{}\"}}",
                if local_admission {
                    "routable"
                } else {
                    "rejected"
                },
                json_escape(tenant_id),
                json_escape(entity_key),
                json_escape(&routing.local_node_id),
                json_escape(&routed.node_id),
                routed.shard_id,
                routed.epoch,
                replica_role_str(routed.role),
                local_admission,
                json_escape(reason),
            )
        }
        Err(err) => format!(
            "{{\"status\":\"rejected\",\"tenant_id\":\"{}\",\"entity_key\":\"{}\",\"reason\":\"{}\"}}",
            json_escape(tenant_id),
            json_escape(entity_key),
            json_escape(&format!("placement route error: {err:?}"))
        ),
    }
}

fn render_placements_json(placements: &[ShardPlacement]) -> String {
    let body = placements
        .iter()
        .map(|placement| {
            let replicas = placement
                .replicas
                .iter()
                .map(|replica| {
                    format!(
                        "{{\"node_id\":\"{}\",\"role\":\"{}\",\"health\":\"{}\"}}",
                        json_escape(&replica.node_id),
                        replica_role_str(replica.role),
                        replica_health_str(replica.health),
                    )
                })
                .collect::<Vec<_>>()
                .join(",");
            format!(
                "{{\"tenant_id\":\"{}\",\"shard_id\":{},\"epoch\":{},\"replicas\":[{}]}}",
                json_escape(&placement.tenant_id),
                placement.shard_id,
                placement.epoch,
                replicas
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    format!("[{body}]")
}

fn replica_role_str(role: ReplicaRole) -> &'static str {
    match role {
        ReplicaRole::Leader => "leader",
        ReplicaRole::Follower => "follower",
    }
}

fn replica_health_str(health: ReplicaHealth) -> &'static str {
    match health {
        ReplicaHealth::Healthy => "healthy",
        ReplicaHealth::Degraded => "degraded",
        ReplicaHealth::Unavailable => "unavailable",
    }
}

fn map_store_error(error: &StoreError) -> (u16, String) {
    match error {
        StoreError::Validation(err) => (400, format!("validation error: {err:?}")),
        StoreError::MissingClaim(claim_id) => (400, format!("missing claim: {claim_id}")),
        StoreError::Conflict(message) => (409, format!("state conflict: {message}")),
        StoreError::InvalidVector(message) => (400, format!("invalid vector: {message}")),
        StoreError::Io(message) | StoreError::Parse(message) => {
            (500, format!("internal persistence error: {message}"))
        }
    }
}

fn append_input_to_wal(wal: &mut FileWal, input: &IngestInput) -> Result<(), StoreError> {
    wal.append_claim(&input.claim)?;
    for evidence in &input.evidence {
        wal.append_evidence(evidence)?;
    }
    for edge in &input.edges {
        wal.append_edge(edge)?;
    }
    if let Some(vector) = input.claim_embedding.as_deref() {
        wal.append_claim_vector(&input.claim.claim_id, vector)?;
    }
    Ok(())
}

fn should_checkpoint_now(policy: &CheckpointPolicy, wal: &FileWal) -> Result<bool, StoreError> {
    if let Some(max_wal_records) = policy.max_wal_records
        && wal.wal_record_count()? >= max_wal_records
    {
        return Ok(true);
    }
    if let Some(max_wal_bytes) = policy.max_wal_bytes
        && wal.wal_size_bytes()? >= max_wal_bytes
    {
        return Ok(true);
    }
    Ok(false)
}

fn replication_token() -> Option<String> {
    env_with_fallback(
        "DASH_INGEST_REPLICATION_TOKEN",
        "EME_INGEST_REPLICATION_TOKEN",
    )
    .map(|value| value.trim().to_string())
    .filter(|value| !value.is_empty())
}

fn is_replication_request_authorized(request: &HttpRequest) -> bool {
    let Some(expected_token) = replication_token() else {
        return true;
    };
    request
        .headers
        .get("x-replication-token")
        .is_some_and(|value| value == &expected_token)
}

fn run_replication_pull_tick(runtime: &SharedRuntime, config: &ReplicationPullConfig) {
    let from_offset = match runtime.lock() {
        Ok(guard) => guard.replication_last_offset,
        Err(_) => return,
    };
    let delta_response = match request_replication_source(
        &config.wal_pull_url(from_offset),
        config.token.as_deref(),
    ) {
        Ok(response) => response,
        Err(err) => {
            if let Ok(mut guard) = runtime.lock() {
                guard.observe_replication_pull_failure(err.clone());
            }
            eprintln!("replication pull tick failed requesting WAL delta: {err}");
            return;
        }
    };
    if delta_response.status != 200 {
        let message = format!(
            "replication source WAL delta returned status {}",
            delta_response.status
        );
        if let Ok(mut guard) = runtime.lock() {
            guard.observe_replication_pull_failure(message.clone());
        }
        eprintln!("{message}");
        return;
    }
    let delta_frame = match parse_replication_delta_frame(&delta_response.body) {
        Ok(frame) => frame,
        Err(err) => {
            if let Ok(mut guard) = runtime.lock() {
                guard.observe_replication_pull_failure(err.clone());
            }
            eprintln!("replication pull tick failed parsing WAL delta: {err}");
            return;
        }
    };
    if delta_frame.needs_resync {
        let export_response =
            match request_replication_source(&config.export_url(), config.token.as_deref()) {
                Ok(response) => response,
                Err(err) => {
                    if let Ok(mut guard) = runtime.lock() {
                        guard.observe_replication_pull_failure(err.clone());
                    }
                    eprintln!("replication resync failed requesting export: {err}");
                    return;
                }
            };
        if export_response.status != 200 {
            let message = format!(
                "replication export returned non-200 status: {}",
                export_response.status
            );
            if let Ok(mut guard) = runtime.lock() {
                guard.observe_replication_pull_failure(message.clone());
            }
            eprintln!("{message}");
            return;
        }
        let export_frame = match parse_replication_export_frame(&export_response.body) {
            Ok(frame) => frame,
            Err(err) => {
                if let Ok(mut guard) = runtime.lock() {
                    guard.observe_replication_pull_failure(err.clone());
                }
                eprintln!("replication resync failed parsing export: {err}");
                return;
            }
        };
        let result = runtime
            .lock()
            .map_err(|_| StoreError::Io("replication runtime lock unavailable".to_string()))
            .and_then(|mut guard| {
                guard.apply_replication_export(WalReplicationExport {
                    snapshot_lines: export_frame.snapshot_lines,
                    wal_lines: export_frame.wal_lines,
                })
            });
        if let Err(err) = result {
            let message = format!("replication resync apply failed: {err:?}");
            if let Ok(mut guard) = runtime.lock() {
                guard.observe_replication_pull_failure(message.clone());
            }
            eprintln!("{message}");
        }
        return;
    }
    let result = runtime
        .lock()
        .map_err(|_| StoreError::Io("replication runtime lock unavailable".to_string()))
        .and_then(|mut guard| {
            guard.apply_replication_delta_lines(&delta_frame.wal_lines, delta_frame.next_offset)
        });
    if let Err(err) = result {
        let message = format!("replication delta apply failed: {err:?}");
        if let Ok(mut guard) = runtime.lock() {
            guard.observe_replication_pull_failure(message.clone());
        }
        eprintln!("{message}");
    }
}

fn resolve_ingest_batch_max_items() -> usize {
    parse_env_first_usize(&["DASH_INGEST_BATCH_MAX_ITEMS", "EME_INGEST_BATCH_MAX_ITEMS"])
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_INGEST_BATCH_MAX_ITEMS)
}

fn unix_timestamp_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| value.as_millis() as u64)
        .unwrap_or(0)
}

fn generate_batch_commit_id() -> String {
    let nonce = BATCH_COMMIT_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!(
        "commit-{}-{}-{}",
        unix_timestamp_millis(),
        std::process::id(),
        nonce
    )
}

fn env_with_fallback(primary: &str, fallback: &str) -> Option<String> {
    std::env::var(primary)
        .ok()
        .or_else(|| std::env::var(fallback).ok())
}

fn parse_env_first_usize(keys: &[&str]) -> Option<usize> {
    for key in keys {
        if let Ok(value) = std::env::var(key)
            && let Ok(parsed) = value.parse::<usize>()
        {
            return Some(parsed);
        }
    }
    None
}

fn parse_env_first_u64(keys: &[&str]) -> Option<u64> {
    for key in keys {
        if let Ok(value) = std::env::var(key)
            && let Ok(parsed) = value.parse::<u64>()
        {
            return Some(parsed);
        }
    }
    None
}

fn parse_wal_async_flush_interval_override() -> Option<Option<Duration>> {
    for key in [
        "DASH_INGEST_WAL_ASYNC_FLUSH_INTERVAL_MS",
        "EME_INGEST_WAL_ASYNC_FLUSH_INTERVAL_MS",
    ] {
        let Ok(raw) = std::env::var(key) else {
            continue;
        };
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Some(None);
        }
        let normalized = trimmed.to_ascii_lowercase();
        if matches!(
            normalized.as_str(),
            "off" | "none" | "false" | "disabled" | "0"
        ) {
            return Some(None);
        }
        match trimmed.parse::<u64>() {
            Ok(value) if value > 0 => return Some(Some(Duration::from_millis(value))),
            Ok(_) => return Some(None),
            Err(_) => {
                eprintln!(
                    "ingestion ignoring invalid {}='{}' (expected positive integer ms or off)",
                    key, trimmed
                );
                return Some(None);
            }
        }
    }
    None
}

fn resolve_wal_async_flush_interval(wal: Option<&FileWal>) -> Option<Duration> {
    let wal = wal?;
    if let Some(override_value) = parse_wal_async_flush_interval_override() {
        return override_value;
    }

    let batching_enabled = wal.background_flush_only()
        || wal.sync_every_records() > 1
        || wal.append_buffer_max_records() > 1
        || wal.sync_interval().is_some();
    if !batching_enabled {
        return None;
    }

    let default_interval = Duration::from_millis(DEFAULT_ASYNC_WAL_FLUSH_INTERVAL_MS);
    Some(
        wal.sync_interval()
            .map(|value| value.min(default_interval))
            .unwrap_or(default_interval),
    )
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

fn observe_auth_success(runtime: &SharedRuntime) {
    if let Ok(mut guard) = runtime.lock() {
        guard.observe_auth_success();
    }
}

fn observe_auth_failure(runtime: &SharedRuntime) {
    if let Ok(mut guard) = runtime.lock() {
        guard.observe_auth_failure();
    }
}

fn observe_authz_denied(runtime: &SharedRuntime) {
    if let Ok(mut guard) = runtime.lock() {
        guard.observe_authz_denied();
    }
}

fn build_ingest_request_from_json(body: &str) -> Result<IngestApiRequest, String> {
    let value = parse_json(body)?;
    if !matches!(value, JsonValue::Object(_)) {
        return Err("request body must be a JSON object".to_string());
    }
    build_ingest_request_from_json_value(&value)
}

fn build_ingest_batch_request_from_json(
    body: &str,
    max_items: usize,
) -> Result<IngestBatchApiRequest, String> {
    let value = parse_json(body)?;
    let object = match value {
        JsonValue::Object(map) => map,
        _ => return Err("request body must be a JSON object".to_string()),
    };
    let items = match object.get("items") {
        Some(JsonValue::Array(items)) => items,
        Some(_) => return Err("items must be an array".to_string()),
        None => return Err("items is required".to_string()),
    };
    if items.is_empty() {
        return Err("items must not be empty".to_string());
    }
    if items.len() > max_items {
        return Err(format!(
            "items length {} exceeds max batch size {}",
            items.len(),
            max_items
        ));
    }
    let commit_id = parse_optional_string(object.get("commit_id"), "commit_id")?
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());

    let mut parsed = Vec::with_capacity(items.len());
    let mut expected_tenant: Option<String> = None;
    for item in items {
        let req = build_ingest_request_from_json_value(item)?;
        if let Some(tenant_id) = expected_tenant.as_deref() {
            if tenant_id != req.claim.tenant_id {
                return Err("all batch items must share the same claim.tenant_id".to_string());
            }
        } else {
            expected_tenant = Some(req.claim.tenant_id.clone());
        }
        parsed.push(req);
    }

    Ok(IngestBatchApiRequest {
        commit_id,
        items: parsed,
    })
}

fn build_ingest_request_from_json_value(value: &JsonValue) -> Result<IngestApiRequest, String> {
    let object = match value {
        JsonValue::Object(map) => map,
        _ => return Err("ingest item must be a JSON object".to_string()),
    };

    let claim_obj = require_object(object, "claim")?;
    let claim = Claim {
        claim_id: require_string(&claim_obj, "claim_id")?,
        tenant_id: require_string(&claim_obj, "tenant_id")?,
        canonical_text: require_string(&claim_obj, "canonical_text")?,
        confidence: parse_f32(
            match claim_obj.get("confidence") {
                Some(JsonValue::Number(raw)) => raw,
                _ => return Err("claim.confidence must be a number".to_string()),
            },
            "claim.confidence",
        )?,
        event_time_unix: parse_optional_i64(
            claim_obj.get("event_time_unix"),
            "claim.event_time_unix",
        )?,
        entities: parse_optional_string_array(claim_obj.get("entities"), "claim.entities")?,
        embedding_ids: parse_optional_string_array(
            claim_obj.get("embedding_ids"),
            "claim.embedding_ids",
        )?,
        claim_type: None,
        valid_from: parse_optional_i64(claim_obj.get("valid_from"), "claim.valid_from")?,
        valid_to: parse_optional_i64(claim_obj.get("valid_to"), "claim.valid_to")?,
        created_at: None,
        updated_at: None,
    };

    let evidence = match object.get("evidence") {
        Some(JsonValue::Array(items)) => parse_evidence_array(items)?,
        Some(_) => return Err("evidence must be an array".to_string()),
        None => Vec::new(),
    };
    let edges = match object.get("edges") {
        Some(JsonValue::Array(items)) => parse_edges_array(items)?,
        Some(_) => return Err("edges must be an array".to_string()),
        None => Vec::new(),
    };

    Ok(IngestApiRequest {
        claim,
        claim_embedding: parse_optional_f32_array(
            claim_obj.get("embedding_vector"),
            "claim.embedding_vector",
        )?,
        evidence,
        edges,
    })
}

fn parse_optional_f32_array(
    value: Option<&JsonValue>,
    field: &str,
) -> Result<Option<Vec<f32>>, String> {
    match value {
        None | Some(JsonValue::Null) => Ok(None),
        Some(JsonValue::Array(items)) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                let raw = match item {
                    JsonValue::Number(raw) => raw,
                    _ => return Err(format!("{field} must be an array of numbers")),
                };
                let parsed = parse_f32(raw, field)?;
                if !parsed.is_finite() {
                    return Err(format!("{field} values must be finite numbers"));
                }
                out.push(parsed);
            }
            if out.is_empty() {
                return Err(format!("{field} must not be empty when provided"));
            }
            Ok(Some(out))
        }
        Some(_) => Err(format!("{field} must be an array or null")),
    }
}

fn parse_evidence_array(items: &[JsonValue]) -> Result<Vec<Evidence>, String> {
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        let obj = match item {
            JsonValue::Object(map) => map,
            _ => return Err("evidence items must be objects".to_string()),
        };
        let stance = match require_string(obj, "stance")?.as_str() {
            "supports" => Stance::Supports,
            "contradicts" => Stance::Contradicts,
            "neutral" => Stance::Neutral,
            _ => {
                return Err("evidence.stance must be supports, contradicts, or neutral".to_string());
            }
        };

        out.push(Evidence {
            evidence_id: require_string(obj, "evidence_id")?,
            claim_id: require_string(obj, "claim_id")?,
            source_id: require_string(obj, "source_id")?,
            stance,
            source_quality: parse_f32(
                match obj.get("source_quality") {
                    Some(JsonValue::Number(raw)) => raw,
                    _ => return Err("evidence.source_quality must be a number".to_string()),
                },
                "evidence.source_quality",
            )?,
            chunk_id: parse_optional_string(obj.get("chunk_id"), "evidence.chunk_id")?,
            span_start: parse_optional_u32(obj.get("span_start"), "evidence.span_start")?,
            span_end: parse_optional_u32(obj.get("span_end"), "evidence.span_end")?,
            doc_id: parse_optional_string(obj.get("doc_id"), "evidence.doc_id")?,
            extraction_model: parse_optional_string(
                obj.get("extraction_model"),
                "evidence.extraction_model",
            )?,
            ingested_at: None,
        });
    }
    Ok(out)
}

fn parse_edges_array(items: &[JsonValue]) -> Result<Vec<ClaimEdge>, String> {
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        let obj = match item {
            JsonValue::Object(map) => map,
            _ => return Err("edges items must be objects".to_string()),
        };
        let relation = match require_string(obj, "relation")?.as_str() {
            "supports" => Relation::Supports,
            "contradicts" => Relation::Contradicts,
            "refines" => Relation::Refines,
            "duplicates" => Relation::Duplicates,
            "depends_on" => Relation::DependsOn,
            _ => return Err(
                "edge.relation must be supports, contradicts, refines, duplicates, or depends_on"
                    .to_string(),
            ),
        };

        out.push(ClaimEdge {
            edge_id: require_string(obj, "edge_id")?,
            from_claim_id: require_string(obj, "from_claim_id")?,
            to_claim_id: require_string(obj, "to_claim_id")?,
            relation,
            strength: parse_f32(
                match obj.get("strength") {
                    Some(JsonValue::Number(raw)) => raw,
                    _ => return Err("edge.strength must be a number".to_string()),
                },
                "edge.strength",
            )?,
            reason_codes: parse_optional_string_array(
                obj.get("reason_codes"),
                "edge.reason_codes",
            )?,
            created_at: None,
        });
    }
    Ok(out)
}

fn require_object(
    map: &HashMap<String, JsonValue>,
    key: &str,
) -> Result<HashMap<String, JsonValue>, String> {
    match map.get(key) {
        Some(JsonValue::Object(value)) => Ok(value.clone()),
        Some(_) => Err(format!("{key} must be an object")),
        None => Err(format!("{key} is required")),
    }
}

fn require_string(map: &HashMap<String, JsonValue>, key: &str) -> Result<String, String> {
    match map.get(key) {
        Some(JsonValue::String(value)) => Ok(value.clone()),
        Some(_) => Err(format!("{key} must be a string")),
        None => Err(format!("{key} is required")),
    }
}

fn parse_f32(raw: &str, field_name: &str) -> Result<f32, String> {
    raw.parse::<f32>()
        .map_err(|_| format!("{field_name} must be a valid number"))
}

fn parse_optional_i64(value: Option<&JsonValue>, field_name: &str) -> Result<Option<i64>, String> {
    match value {
        None | Some(JsonValue::Null) => Ok(None),
        Some(JsonValue::Number(raw)) => raw
            .parse::<i64>()
            .map(Some)
            .map_err(|_| format!("{field_name} must be an i64 timestamp")),
        _ => Err(format!("{field_name} must be an i64 timestamp or null")),
    }
}

fn parse_optional_u32(value: Option<&JsonValue>, field_name: &str) -> Result<Option<u32>, String> {
    match value {
        None | Some(JsonValue::Null) => Ok(None),
        Some(JsonValue::Number(raw)) => raw
            .parse::<u32>()
            .map(Some)
            .map_err(|_| format!("{field_name} must be a u32 offset or null")),
        _ => Err(format!("{field_name} must be a u32 offset or null")),
    }
}

fn parse_optional_string(
    value: Option<&JsonValue>,
    field_name: &str,
) -> Result<Option<String>, String> {
    match value {
        None | Some(JsonValue::Null) => Ok(None),
        Some(JsonValue::String(raw)) => Ok(Some(raw.clone())),
        _ => Err(format!("{field_name} must be a string or null")),
    }
}

fn parse_optional_string_array(
    value: Option<&JsonValue>,
    field_name: &str,
) -> Result<Vec<String>, String> {
    match value {
        None | Some(JsonValue::Null) => Ok(Vec::new()),
        Some(JsonValue::Array(items)) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                match item {
                    JsonValue::String(value) => out.push(value.clone()),
                    _ => return Err(format!("{field_name} items must be strings")),
                }
            }
            Ok(out)
        }
        _ => Err(format!("{field_name} must be an array of strings or null")),
    }
}

fn render_ingest_response_json(resp: &IngestApiResponse) -> String {
    let checkpoint_snapshot_records = resp
        .checkpoint_snapshot_records
        .map(|v| v.to_string())
        .unwrap_or_else(|| "null".to_string());
    let checkpoint_truncated_wal_records = resp
        .checkpoint_truncated_wal_records
        .map(|v| v.to_string())
        .unwrap_or_else(|| "null".to_string());
    format!(
        "{{\"ingested_claim_id\":\"{}\",\"claims_total\":{},\"checkpoint_triggered\":{},\"checkpoint_snapshot_records\":{},\"checkpoint_truncated_wal_records\":{}}}",
        json_escape(&resp.ingested_claim_id),
        resp.claims_total,
        resp.checkpoint_triggered,
        checkpoint_snapshot_records,
        checkpoint_truncated_wal_records
    )
}

fn render_ingest_batch_response_json(resp: &IngestBatchApiResponse) -> String {
    let checkpoint_snapshot_records = resp
        .checkpoint_snapshot_records
        .map(|v| v.to_string())
        .unwrap_or_else(|| "null".to_string());
    let checkpoint_truncated_wal_records = resp
        .checkpoint_truncated_wal_records
        .map(|v| v.to_string())
        .unwrap_or_else(|| "null".to_string());
    let ingested_claim_ids = resp
        .ingested_claim_ids
        .iter()
        .map(|value| format!("\"{}\"", json_escape(value)))
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "{{\"commit_id\":\"{}\",\"idempotent_replay\":{},\"batch_size\":{},\"ingested_claim_ids\":[{}],\"claims_total\":{},\"checkpoint_triggered\":{},\"checkpoint_snapshot_records\":{},\"checkpoint_truncated_wal_records\":{}}}",
        json_escape(&resp.commit_id),
        resp.idempotent_replay,
        resp.batch_size,
        ingested_claim_ids,
        resp.claims_total,
        resp.checkpoint_triggered,
        checkpoint_snapshot_records,
        checkpoint_truncated_wal_records
    )
}

fn split_target(target: &str) -> (String, HashMap<String, String>) {
    let (path, query_str) = target
        .split_once('?')
        .map(|(path, query)| (path, Some(query)))
        .unwrap_or((target, None));
    let mut query = HashMap::new();
    if let Some(query_str) = query_str {
        for pair in query_str.split('&') {
            if pair.is_empty() {
                continue;
            }
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            query.insert(k.to_string(), v.to_string());
        }
    }
    (path.to_string(), query)
}

fn parse_query_usize(query: &HashMap<String, String>, key: &str) -> Result<Option<usize>, String> {
    match query.get(key) {
        None => Ok(None),
        Some(value) => value
            .parse::<usize>()
            .map(Some)
            .map_err(|_| format!("query parameter '{key}' must be a positive integer")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use indexer::{Segment, Tier, persist_segments_atomic};
    use metadata_router::{
        ReplicaHealth, ReplicaPlacement, ReplicaRole, promote_replica_to_leader,
    };
    use std::io::Read;
    use std::net::{TcpListener, TcpStream};
    use std::path::PathBuf;
    use std::sync::OnceLock;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn sample_runtime() -> SharedRuntime {
        Arc::new(Mutex::new(
            IngestionRuntime::in_memory(InMemoryStore::new()),
        ))
    }

    fn temp_wal_path() -> PathBuf {
        let mut wal_path = std::env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock should be monotonic")
            .as_nanos();
        wal_path.push(format!(
            "dash-ingest-runtime-wal-{}-{}.log",
            std::process::id(),
            nanos
        ));
        wal_path
    }

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    #[allow(unused_unsafe)]
    fn set_env_var_for_tests(key: &str, value: &str) {
        unsafe {
            std::env::set_var(key, value);
        }
    }

    #[allow(unused_unsafe)]
    fn restore_env_var_for_tests(key: &str, value: Option<&std::ffi::OsStr>) {
        match value {
            Some(value) => unsafe {
                std::env::set_var(key, value);
            },
            None => unsafe {
                std::env::remove_var(key);
            },
        }
    }

    #[test]
    fn build_ingest_request_from_json_accepts_contract_payload() {
        let body = r#"{
            "claim": {
                "claim_id": "c1",
                "tenant_id": "tenant-a",
                "canonical_text": "Company X acquired Company Y",
                "confidence": 0.9,
                "event_time_unix": 100
            },
            "evidence": [
                {
                    "evidence_id": "e1",
                    "claim_id": "c1",
                    "source_id": "source://doc",
                    "stance": "supports",
                    "source_quality": 0.95
                }
            ],
            "edges": [
                {
                    "edge_id": "g1",
                    "from_claim_id": "c1",
                    "to_claim_id": "c2",
                    "relation": "supports",
                    "strength": 0.8
                }
            ]
        }"#;

        let req = build_ingest_request_from_json(body).unwrap();
        assert_eq!(req.claim.claim_id, "c1");
        assert!(req.claim.entities.is_empty());
        assert!(req.claim.embedding_ids.is_empty());
        assert_eq!(req.evidence.len(), 1);
        assert_eq!(req.evidence[0].chunk_id, None);
        assert_eq!(req.evidence[0].span_start, None);
        assert_eq!(req.evidence[0].span_end, None);
        assert_eq!(req.edges.len(), 1);
    }

    #[test]
    fn build_ingest_request_from_json_accepts_metadata_fields() {
        let body = r#"{
            "claim": {
                "claim_id": "c1",
                "tenant_id": "tenant-a",
                "canonical_text": "Company X acquired Company Y",
                "confidence": 0.9,
                "entities": ["Company X", "Company Y"],
                "embedding_ids": ["emb://1"]
            },
            "evidence": [
                {
                    "evidence_id": "e1",
                    "claim_id": "c1",
                    "source_id": "source://doc",
                    "stance": "supports",
                    "source_quality": 0.95,
                    "chunk_id": "chunk-10",
                    "span_start": 12,
                    "span_end": 48
                }
            ]
        }"#;

        let req = build_ingest_request_from_json(body).unwrap();
        assert_eq!(req.claim.entities, vec!["Company X", "Company Y"]);
        assert_eq!(req.claim.embedding_ids, vec!["emb://1"]);
        assert_eq!(req.evidence[0].chunk_id.as_deref(), Some("chunk-10"));
        assert_eq!(req.evidence[0].span_start, Some(12));
        assert_eq!(req.evidence[0].span_end, Some(48));
    }

    #[test]
    fn build_ingest_request_from_json_rejects_invalid_relation() {
        let body = r#"{
            "claim": {
                "claim_id": "c1",
                "tenant_id": "tenant-a",
                "canonical_text": "Company X acquired Company Y",
                "confidence": 0.9
            },
            "edges": [
                {
                    "edge_id": "g1",
                    "from_claim_id": "c1",
                    "to_claim_id": "c2",
                    "relation": "invalid",
                    "strength": 0.8
                }
            ]
        }"#;

        let err = build_ingest_request_from_json(body).unwrap_err();
        assert!(err.contains("edge.relation"));
    }

    #[test]
    fn build_ingest_batch_request_from_json_accepts_items_and_commit_id() {
        let body = r#"{
            "commit_id": "commit-test-1",
            "items": [
                {
                    "claim": {
                        "claim_id": "c1",
                        "tenant_id": "tenant-a",
                        "canonical_text": "Company X acquired Company Y",
                        "confidence": 0.9
                    }
                },
                {
                    "claim": {
                        "claim_id": "c2",
                        "tenant_id": "tenant-a",
                        "canonical_text": "Company X expanded into region Z",
                        "confidence": 0.88
                    }
                }
            ]
        }"#;

        let req = build_ingest_batch_request_from_json(body, 8).expect("batch parse should work");
        assert_eq!(req.commit_id.as_deref(), Some("commit-test-1"));
        assert_eq!(req.items.len(), 2);
        assert_eq!(req.items[0].claim.claim_id, "c1");
        assert_eq!(req.items[1].claim.claim_id, "c2");
    }

    #[test]
    fn build_ingest_batch_request_from_json_rejects_cross_tenant_batch() {
        let body = r#"{
            "items": [
                {
                    "claim": {
                        "claim_id": "c1",
                        "tenant_id": "tenant-a",
                        "canonical_text": "A",
                        "confidence": 0.9
                    }
                },
                {
                    "claim": {
                        "claim_id": "c2",
                        "tenant_id": "tenant-b",
                        "canonical_text": "B",
                        "confidence": 0.8
                    }
                }
            ]
        }"#;
        let err = build_ingest_batch_request_from_json(body, 16).expect_err("batch should fail");
        assert!(err.contains("same claim.tenant_id"));
    }

    #[test]
    fn handle_request_post_ingests_claim() {
        let runtime = sample_runtime();
        let request = HttpRequest {
            method: "POST".to_string(),
            target: "/v1/ingest".to_string(),
            headers: HashMap::from([("content-type".to_string(), "application/json".to_string())]),
            body: br#"{"claim":{"claim_id":"c1","tenant_id":"tenant-a","canonical_text":"Company X acquired Company Y","confidence":0.9},"evidence":[{"evidence_id":"e1","claim_id":"c1","source_id":"source://doc","stance":"supports","source_quality":0.95}]}"#.to_vec(),
        };

        let response = handle_request(&runtime, &request);
        assert_eq!(response.status, 200);
        assert!(response.body.contains("\"ingested_claim_id\":\"c1\""));
        assert!(response.body.contains("\"claims_total\":1"));
    }

    #[test]
    fn handle_request_post_batch_ingests_claims_and_returns_commit_metadata() {
        let runtime = sample_runtime();
        let request = HttpRequest {
            method: "POST".to_string(),
            target: "/v1/ingest/batch".to_string(),
            headers: HashMap::from([("content-type".to_string(), "application/json".to_string())]),
            body: br#"{
                "items": [
                    {
                        "claim": {
                            "claim_id": "c-b1",
                            "tenant_id": "tenant-a",
                            "canonical_text": "Batch item one",
                            "confidence": 0.91
                        }
                    },
                    {
                        "claim": {
                            "claim_id": "c-b2",
                            "tenant_id": "tenant-a",
                            "canonical_text": "Batch item two",
                            "confidence": 0.89
                        }
                    }
                ]
            }"#
            .to_vec(),
        };

        let response = handle_request(&runtime, &request);
        assert_eq!(response.status, 200);
        assert!(response.body.contains("\"commit_id\":\"commit-"));
        assert!(response.body.contains("\"idempotent_replay\":false"));
        assert!(response.body.contains("\"batch_size\":2"));
        assert!(
            response
                .body
                .contains("\"ingested_claim_ids\":[\"c-b1\",\"c-b2\"]")
        );
        assert!(response.body.contains("\"claims_total\":2"));

        let metrics_request = HttpRequest {
            method: "GET".to_string(),
            target: "/metrics".to_string(),
            headers: HashMap::new(),
            body: Vec::new(),
        };
        let metrics_response = handle_request(&runtime, &metrics_request);
        assert_eq!(metrics_response.status, 200);
        assert!(
            metrics_response
                .body
                .contains("dash_ingest_batch_success_total 1")
        );
        assert!(
            metrics_response
                .body
                .contains("dash_ingest_batch_last_size 2")
        );
        assert!(
            metrics_response
                .body
                .contains("dash_ingest_batch_idempotent_hit_total 0")
        );
    }

    #[test]
    fn handle_request_post_batch_replays_idempotently_for_same_commit_id() {
        let runtime = sample_runtime();
        let body = br#"{
            "commit_id": "commit-idem-1",
            "items": [
                {
                    "claim": {
                        "claim_id": "c-idem-1",
                        "tenant_id": "tenant-a",
                        "canonical_text": "Idempotent batch one",
                        "confidence": 0.91
                    }
                },
                {
                    "claim": {
                        "claim_id": "c-idem-2",
                        "tenant_id": "tenant-a",
                        "canonical_text": "Idempotent batch two",
                        "confidence": 0.89
                    }
                }
            ]
        }"#
        .to_vec();
        let request = HttpRequest {
            method: "POST".to_string(),
            target: "/v1/ingest/batch".to_string(),
            headers: HashMap::from([("content-type".to_string(), "application/json".to_string())]),
            body: body.clone(),
        };
        let first = handle_request(&runtime, &request);
        assert_eq!(first.status, 200);
        assert!(first.body.contains("\"idempotent_replay\":false"));
        assert!(first.body.contains("\"claims_total\":2"));

        let second = handle_request(&runtime, &HttpRequest { body, ..request });
        assert_eq!(second.status, 200);
        assert!(second.body.contains("\"idempotent_replay\":true"));
        assert!(second.body.contains("\"claims_total\":2"));

        let metrics_request = HttpRequest {
            method: "GET".to_string(),
            target: "/metrics".to_string(),
            headers: HashMap::new(),
            body: Vec::new(),
        };
        let metrics_response = handle_request(&runtime, &metrics_request);
        assert_eq!(metrics_response.status, 200);
        assert!(
            metrics_response
                .body
                .contains("dash_ingest_batch_success_total 2")
        );
        assert!(
            metrics_response
                .body
                .contains("dash_ingest_batch_idempotent_hit_total 1")
        );
        assert!(
            metrics_response
                .body
                .contains("dash_ingest_success_total 2")
        );
    }

    #[test]
    fn handle_request_post_batch_rejects_commit_id_reuse_with_different_payload() {
        let runtime = sample_runtime();
        let first_request = HttpRequest {
            method: "POST".to_string(),
            target: "/v1/ingest/batch".to_string(),
            headers: HashMap::from([("content-type".to_string(), "application/json".to_string())]),
            body: br#"{
                "commit_id": "commit-conflict-1",
                "items": [
                    {
                        "claim": {
                            "claim_id": "c-conflict-1",
                            "tenant_id": "tenant-a",
                            "canonical_text": "Commit conflict one",
                            "confidence": 0.91
                        }
                    }
                ]
            }"#
            .to_vec(),
        };
        let first = handle_request(&runtime, &first_request);
        assert_eq!(first.status, 200);
        assert!(first.body.contains("\"idempotent_replay\":false"));

        let second_request = HttpRequest {
            method: "POST".to_string(),
            target: "/v1/ingest/batch".to_string(),
            headers: HashMap::from([("content-type".to_string(), "application/json".to_string())]),
            body: br#"{
                "commit_id": "commit-conflict-1",
                "items": [
                    {
                        "claim": {
                            "claim_id": "c-conflict-2",
                            "tenant_id": "tenant-a",
                            "canonical_text": "Commit conflict two",
                            "confidence": 0.89
                        }
                    }
                ]
            }"#
            .to_vec(),
        };
        let second = handle_request(&runtime, &second_request);
        assert_eq!(second.status, 409);
        assert!(second.body.contains("state conflict"));

        let metrics_request = HttpRequest {
            method: "GET".to_string(),
            target: "/metrics".to_string(),
            headers: HashMap::new(),
            body: Vec::new(),
        };
        let metrics_response = handle_request(&runtime, &metrics_request);
        assert_eq!(metrics_response.status, 200);
        assert!(
            metrics_response
                .body
                .contains("dash_ingest_batch_failed_total 1")
        );
        assert!(
            metrics_response
                .body
                .contains("dash_ingest_batch_idempotent_hit_total 0")
        );
    }

    #[test]
    fn handle_request_post_batch_is_atomic_on_validation_failure() {
        let wal_path = temp_wal_path();
        let wal = FileWal::open(&wal_path).expect("wal should open");
        let runtime = Arc::new(Mutex::new(IngestionRuntime::persistent(
            InMemoryStore::new(),
            wal,
            CheckpointPolicy::default(),
        )));

        let request = HttpRequest {
            method: "POST".to_string(),
            target: "/v1/ingest/batch".to_string(),
            headers: HashMap::from([("content-type".to_string(), "application/json".to_string())]),
            body: br#"{
                "items": [
                    {
                        "claim": {
                            "claim_id": "c-a1",
                            "tenant_id": "tenant-a",
                            "canonical_text": "Batch atomic item one",
                            "confidence": 0.91,
                            "embedding_vector": [0.1, 0.2, 0.3, 0.4]
                        }
                    },
                    {
                        "claim": {
                            "claim_id": "c-a2",
                            "tenant_id": "tenant-a",
                            "canonical_text": "Batch atomic item two",
                            "confidence": 0.89,
                            "embedding_vector": [0.1, 0.2]
                        }
                    }
                ]
            }"#
            .to_vec(),
        };

        let response = handle_request(&runtime, &request);
        assert_eq!(response.status, 400);
        assert!(response.body.contains("invalid vector"));

        let guard = runtime.lock().expect("runtime lock should be available");
        assert_eq!(guard.claims_len(), 0);
        let wal_records = guard
            .wal
            .as_ref()
            .expect("persistent runtime should have wal")
            .wal_record_count()
            .expect("wal record count should be readable");
        assert_eq!(wal_records, 0);
        drop(guard);

        let _ = std::fs::remove_file(&wal_path);
        let mut snapshot_path = wal_path.clone().into_os_string();
        snapshot_path.push(".snapshot");
        let _ = std::fs::remove_file(PathBuf::from(snapshot_path));
    }

    #[test]
    fn handle_request_internal_replication_wal_returns_delta_payload() {
        let wal_path = temp_wal_path();
        let wal = FileWal::open(&wal_path).expect("wal should open");
        let runtime = Arc::new(Mutex::new(IngestionRuntime::persistent(
            InMemoryStore::new(),
            wal,
            CheckpointPolicy::default(),
        )));
        let ingest_request = HttpRequest {
            method: "POST".to_string(),
            target: "/v1/ingest".to_string(),
            headers: HashMap::from([("content-type".to_string(), "application/json".to_string())]),
            body: br#"{"claim":{"claim_id":"c-repl-1","tenant_id":"tenant-a","canonical_text":"replication source claim","confidence":0.9}}"#.to_vec(),
        };
        let ingest_response = handle_request(&runtime, &ingest_request);
        assert_eq!(ingest_response.status, 200);

        let pull_request = HttpRequest {
            method: "GET".to_string(),
            target: "/internal/replication/wal?from_offset=0&max_records=10".to_string(),
            headers: HashMap::new(),
            body: Vec::new(),
        };
        let pull_response = handle_request(&runtime, &pull_request);
        assert_eq!(pull_response.status, 200);
        assert!(pull_response.body.contains("status=ok"));
        assert!(pull_response.body.contains("records=1"));
        assert!(pull_response.body.contains("next_offset=1"));

        let guard = runtime.lock().expect("runtime lock should be available");
        let _ = std::fs::remove_file(
            guard
                .wal
                .as_ref()
                .expect("persistent runtime should have wal")
                .path(),
        );
        let _ = std::fs::remove_file(
            guard
                .wal
                .as_ref()
                .expect("persistent runtime should have wal")
                .snapshot_path(),
        );
    }

    #[test]
    fn handle_request_internal_replication_endpoints_require_token_when_configured() {
        let _guard = env_lock().lock().expect("env lock should be available");
        let previous = std::env::var_os("DASH_INGEST_REPLICATION_TOKEN");
        set_env_var_for_tests("DASH_INGEST_REPLICATION_TOKEN", "token-a");

        let wal_path = temp_wal_path();
        let wal = FileWal::open(&wal_path).expect("wal should open");
        let runtime = Arc::new(Mutex::new(IngestionRuntime::persistent(
            InMemoryStore::new(),
            wal,
            CheckpointPolicy::default(),
        )));
        let unauthorized = HttpRequest {
            method: "GET".to_string(),
            target: "/internal/replication/wal?from_offset=0".to_string(),
            headers: HashMap::new(),
            body: Vec::new(),
        };
        let unauthorized_response = handle_request(&runtime, &unauthorized);
        assert_eq!(unauthorized_response.status, 403);

        let authorized = HttpRequest {
            method: "GET".to_string(),
            target: "/internal/replication/export".to_string(),
            headers: HashMap::from([("x-replication-token".to_string(), "token-a".to_string())]),
            body: Vec::new(),
        };
        let authorized_response = handle_request(&runtime, &authorized);
        assert_eq!(authorized_response.status, 200);
        assert!(authorized_response.body.contains("status=ok"));

        restore_env_var_for_tests("DASH_INGEST_REPLICATION_TOKEN", previous.as_deref());
        let guard = runtime.lock().expect("runtime lock should be available");
        let _ = std::fs::remove_file(
            guard
                .wal
                .as_ref()
                .expect("persistent runtime should have wal")
                .path(),
        );
        let _ = std::fs::remove_file(
            guard
                .wal
                .as_ref()
                .expect("persistent runtime should have wal")
                .snapshot_path(),
        );
    }

    #[test]
    fn handle_request_post_rejects_invalid_content_type() {
        let runtime = sample_runtime();
        let request = HttpRequest {
            method: "POST".to_string(),
            target: "/v1/ingest".to_string(),
            headers: HashMap::from([("content-type".to_string(), "text/plain".to_string())]),
            body: b"{}".to_vec(),
        };

        let response = handle_request(&runtime, &request);
        assert_eq!(response.status, 400);
        assert!(response.body.contains("application/json"));
    }

    #[test]
    fn auth_policy_scoped_key_allows_configured_tenant() {
        let request = HttpRequest {
            method: "POST".to_string(),
            target: "/v1/ingest".to_string(),
            headers: HashMap::from([("x-api-key".to_string(), "scope-a".to_string())]),
            body: Vec::new(),
        };
        let policy = AuthPolicy::from_env(
            None,
            None,
            None,
            None,
            Some("scope-a:tenant-a,tenant-b".to_string()),
        );
        assert_eq!(
            authorize_request_for_tenant(&request, "tenant-b", &policy),
            AuthDecision::Allowed
        );
    }

    #[test]
    fn auth_policy_scoped_key_rejects_other_tenants() {
        let request = HttpRequest {
            method: "POST".to_string(),
            target: "/v1/ingest".to_string(),
            headers: HashMap::from([("authorization".to_string(), "Bearer scope-a".to_string())]),
            body: Vec::new(),
        };
        let policy =
            AuthPolicy::from_env(None, None, None, None, Some("scope-a:tenant-a".to_string()));
        assert_eq!(
            authorize_request_for_tenant(&request, "tenant-z", &policy),
            AuthDecision::Forbidden("tenant is not allowed for this API key")
        );
    }

    #[test]
    fn auth_policy_required_key_rejects_missing_key() {
        let request = HttpRequest {
            method: "POST".to_string(),
            target: "/v1/ingest".to_string(),
            headers: HashMap::new(),
            body: Vec::new(),
        };
        let policy = AuthPolicy::from_env(Some("secret".to_string()), None, None, None, None);
        assert_eq!(
            authorize_request_for_tenant(&request, "tenant-a", &policy),
            AuthDecision::Unauthorized("missing or invalid API key")
        );
    }

    #[test]
    fn auth_policy_required_key_set_supports_rotation() {
        let request = HttpRequest {
            method: "POST".to_string(),
            target: "/v1/ingest".to_string(),
            headers: HashMap::from([("x-api-key".to_string(), "new-key".to_string())]),
            body: Vec::new(),
        };
        let policy = AuthPolicy::from_env(
            Some("old-key".to_string()),
            Some("new-key,old-key-2".to_string()),
            None,
            None,
            None,
        );
        assert_eq!(
            authorize_request_for_tenant(&request, "tenant-a", &policy),
            AuthDecision::Allowed
        );
    }

    #[test]
    fn auth_policy_revoked_key_is_denied() {
        let request = HttpRequest {
            method: "POST".to_string(),
            target: "/v1/ingest".to_string(),
            headers: HashMap::from([("authorization".to_string(), "Bearer scope-a".to_string())]),
            body: Vec::new(),
        };
        let policy = AuthPolicy::from_env(
            None,
            None,
            Some("scope-a".to_string()),
            None,
            Some("scope-a:tenant-a".to_string()),
        );
        assert_eq!(
            authorize_request_for_tenant(&request, "tenant-a", &policy),
            AuthDecision::Unauthorized("API key revoked")
        );
    }

    #[test]
    fn metrics_endpoint_reports_counters() {
        let runtime = sample_runtime();

        let request = HttpRequest {
            method: "POST".to_string(),
            target: "/v1/ingest".to_string(),
            headers: HashMap::from([("content-type".to_string(), "application/json".to_string())]),
            body: br#"{"claim":{"claim_id":"c1","tenant_id":"tenant-a","canonical_text":"Company X acquired Company Y","confidence":0.9}}"#.to_vec(),
        };
        let response = handle_request(&runtime, &request);
        assert_eq!(response.status, 200);

        let metrics_request = HttpRequest {
            method: "GET".to_string(),
            target: "/metrics".to_string(),
            headers: HashMap::new(),
            body: Vec::new(),
        };
        let metrics_response = handle_request(&runtime, &metrics_request);
        assert_eq!(metrics_response.status, 200);
        assert!(
            metrics_response
                .body
                .contains("dash_ingest_success_total 1")
        );
        assert!(
            metrics_response
                .body
                .contains("dash_ingest_auth_success_total 1")
        );
        assert!(
            metrics_response
                .body
                .contains("dash_ingest_placement_enabled 0")
        );
        assert!(
            metrics_response
                .body
                .contains("dash_ingest_wal_async_flush_enabled 0")
        );
        assert!(
            metrics_response
                .body
                .contains("dash_ingest_wal_background_flush_only 0")
        );
        assert!(
            metrics_response
                .body
                .contains("dash_ingest_transport_queue_capacity 0")
        );
        assert!(
            metrics_response
                .body
                .contains("dash_ingest_transport_queue_depth 0")
        );
        assert!(
            metrics_response
                .body
                .contains("dash_ingest_transport_queue_full_reject_total 0")
        );
    }

    #[test]
    fn metrics_endpoint_reports_backpressure_queue_values() {
        let runtime = sample_runtime();
        let queue_metrics = Arc::new(TransportBackpressureMetrics {
            queue_depth: AtomicUsize::new(3),
            queue_capacity: 8,
            queue_full_reject_total: AtomicU64::new(11),
        });
        {
            let mut guard = runtime.lock().expect("runtime lock should be available");
            guard.set_transport_backpressure_metrics(queue_metrics);
        }

        let metrics_request = HttpRequest {
            method: "GET".to_string(),
            target: "/metrics".to_string(),
            headers: HashMap::new(),
            body: Vec::new(),
        };
        let metrics_response = handle_request(&runtime, &metrics_request);
        assert_eq!(metrics_response.status, 200);
        assert!(
            metrics_response
                .body
                .contains("dash_ingest_transport_queue_capacity 8")
        );
        assert!(
            metrics_response
                .body
                .contains("dash_ingest_transport_queue_depth 3")
        );
        assert!(
            metrics_response
                .body
                .contains("dash_ingest_transport_queue_full_reject_total 11")
        );
    }

    #[test]
    fn resolve_http_queue_capacity_defaults_to_workers_times_constant() {
        let _guard = env_lock().lock().expect("env lock should be available");
        let key = "DASH_INGEST_HTTP_QUEUE_CAPACITY";
        let previous = std::env::var_os(key);
        restore_env_var_for_tests(key, None);
        let capacity = resolve_http_queue_capacity(3);
        assert_eq!(capacity, 3 * DEFAULT_HTTP_QUEUE_CAPACITY_PER_WORKER);
        restore_env_var_for_tests(key, previous.as_deref());
    }

    #[test]
    fn resolve_http_queue_capacity_prefers_env_override() {
        let _guard = env_lock().lock().expect("env lock should be available");
        let key = "DASH_INGEST_HTTP_QUEUE_CAPACITY";
        let previous = std::env::var_os(key);
        set_env_var_for_tests(key, "7");
        let capacity = resolve_http_queue_capacity(3);
        assert_eq!(capacity, 7);
        restore_env_var_for_tests(key, previous.as_deref());
    }

    #[test]
    fn write_backpressure_response_returns_http_503_payload() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("listener bind should succeed");
        let addr = listener.local_addr().expect("local addr should resolve");
        let client = std::thread::spawn(move || {
            let mut stream = TcpStream::connect(addr).expect("client connect should succeed");
            let mut response = String::new();
            stream
                .read_to_string(&mut response)
                .expect("client should read response");
            response
        });

        let (server_stream, _) = listener.accept().expect("accept should succeed");
        write_backpressure_response(server_stream, SOCKET_TIMEOUT_SECS)
            .expect("response write should succeed");
        let response = client.join().expect("client thread should join");
        assert!(response.starts_with("HTTP/1.1 503 Service Unavailable"));
        assert!(response.contains("content-type: application/json"));
        assert!(response.contains("ingestion worker queue full"));
    }

    #[test]
    fn persistent_runtime_enables_async_flush_for_batched_wal_by_default() {
        let wal_path = temp_wal_path();
        let wal = FileWal::open_with_policy(
            &wal_path,
            store::WalWritePolicy {
                sync_every_records: 64,
                append_buffer_max_records: 64,
                sync_interval: None,
                background_flush_only: false,
            },
        )
        .expect("wal should open");
        let runtime =
            IngestionRuntime::persistent(InMemoryStore::new(), wal, CheckpointPolicy::default());
        assert_eq!(
            runtime.wal_async_flush_interval(),
            Some(Duration::from_millis(DEFAULT_ASYNC_WAL_FLUSH_INTERVAL_MS))
        );
        drop(runtime);
        let _ = std::fs::remove_file(&wal_path);
        let mut snapshot = wal_path.into_os_string();
        snapshot.push(".snapshot");
        let _ = std::fs::remove_file(PathBuf::from(snapshot));
    }

    #[test]
    fn persistent_runtime_enables_async_flush_for_background_only_wal() {
        let wal_path = temp_wal_path();
        let wal = FileWal::open_with_policy(
            &wal_path,
            store::WalWritePolicy {
                sync_every_records: 1,
                append_buffer_max_records: 1,
                sync_interval: None,
                background_flush_only: true,
            },
        )
        .expect("wal should open");
        let runtime =
            IngestionRuntime::persistent(InMemoryStore::new(), wal, CheckpointPolicy::default());
        assert_eq!(
            runtime.wal_async_flush_interval(),
            Some(Duration::from_millis(DEFAULT_ASYNC_WAL_FLUSH_INTERVAL_MS))
        );
        drop(runtime);
        let _ = std::fs::remove_file(&wal_path);
        let mut snapshot = wal_path.into_os_string();
        snapshot.push(".snapshot");
        let _ = std::fs::remove_file(PathBuf::from(snapshot));
    }

    #[test]
    fn background_only_mode_skips_request_thread_interval_flush() {
        let wal_path = temp_wal_path();
        let wal = FileWal::open_with_policy(
            &wal_path,
            store::WalWritePolicy {
                sync_every_records: 1,
                append_buffer_max_records: 1,
                sync_interval: Some(Duration::from_millis(1)),
                background_flush_only: true,
            },
        )
        .expect("wal should open");
        let mut runtime =
            IngestionRuntime::persistent(InMemoryStore::new(), wal, CheckpointPolicy::default());
        let request = build_ingest_request_from_json(
            r#"{"claim":{"claim_id":"c-bg","tenant_id":"tenant-a","canonical_text":"Background mode claim","confidence":0.9}}"#,
        )
        .expect("request should parse");
        runtime.ingest(request).expect("ingest should succeed");
        std::thread::sleep(Duration::from_millis(2));
        runtime.flush_wal_if_due();
        let metrics = runtime.metrics_text();
        assert!(metrics.contains("dash_ingest_wal_unsynced_records 1"));
        assert!(metrics.contains("dash_ingest_wal_background_flush_only 1"));

        runtime.flush_wal_for_async_tick();
        let flushed = runtime.metrics_text();
        assert!(flushed.contains("dash_ingest_wal_unsynced_records 0"));

        drop(runtime);
        let _ = std::fs::remove_file(&wal_path);
        let mut snapshot = wal_path.into_os_string();
        snapshot.push(".snapshot");
        let _ = std::fs::remove_file(PathBuf::from(snapshot));
    }

    #[test]
    fn async_flush_tick_forces_sync_of_unsynced_wal_records() {
        let wal_path = temp_wal_path();
        let wal = FileWal::open_with_policy(
            &wal_path,
            store::WalWritePolicy {
                sync_every_records: 64,
                append_buffer_max_records: 64,
                sync_interval: None,
                background_flush_only: false,
            },
        )
        .expect("wal should open");
        let mut runtime =
            IngestionRuntime::persistent(InMemoryStore::new(), wal, CheckpointPolicy::default());
        let request = build_ingest_request_from_json(
            r#"{"claim":{"claim_id":"c1","tenant_id":"tenant-a","canonical_text":"Company X acquired Company Y","confidence":0.9}}"#,
        )
        .expect("request should parse");
        runtime.ingest(request).expect("ingest should succeed");
        let before = runtime.metrics_text();
        assert!(before.contains("dash_ingest_wal_unsynced_records 1"));
        assert!(before.contains("dash_ingest_wal_buffered_records 1"));

        runtime.flush_wal_for_async_tick();
        let after = runtime.metrics_text();
        assert!(after.contains("dash_ingest_wal_unsynced_records 0"));
        assert!(after.contains("dash_ingest_wal_buffered_records 0"));
        assert!(after.contains("dash_ingest_wal_async_flush_tick_total 1"));

        drop(runtime);
        let _ = std::fs::remove_file(&wal_path);
        let mut snapshot = wal_path.into_os_string();
        snapshot.push(".snapshot");
        let _ = std::fs::remove_file(PathBuf::from(snapshot));
    }

    #[test]
    fn handle_request_post_rejects_when_local_node_is_not_write_leader() {
        let placement = ShardPlacement {
            tenant_id: "tenant-a".to_string(),
            shard_id: 0,
            epoch: 9,
            replicas: vec![
                ReplicaPlacement {
                    node_id: "node-a".to_string(),
                    role: ReplicaRole::Leader,
                    health: ReplicaHealth::Healthy,
                },
                ReplicaPlacement {
                    node_id: "node-b".to_string(),
                    role: ReplicaRole::Follower,
                    health: ReplicaHealth::Healthy,
                },
            ],
        };
        let runtime = Arc::new(Mutex::new(
            IngestionRuntime::in_memory(InMemoryStore::new()).with_placement_runtime_for_tests(Ok(
                Some(PlacementRoutingRuntime {
                    local_node_id: "node-b".to_string(),
                    router_config: RouterConfig {
                        shard_ids: vec![0],
                        virtual_nodes_per_shard: 16,
                        replica_count: 2,
                    },
                    placements: vec![placement],
                }),
            )),
        ));
        let request = HttpRequest {
            method: "POST".to_string(),
            target: "/v1/ingest".to_string(),
            headers: HashMap::from([("content-type".to_string(), "application/json".to_string())]),
            body: br#"{"claim":{"claim_id":"c-route","tenant_id":"tenant-a","canonical_text":"placement route test","confidence":0.9}}"#.to_vec(),
        };
        let response = handle_request(&runtime, &request);
        assert_eq!(
            response.status, 503,
            "unexpected response body: {}",
            response.body
        );
        assert!(response.body.contains("local node 'node-b'"));

        let claims_len = runtime
            .lock()
            .expect("runtime lock should hold")
            .claims_len();
        assert_eq!(claims_len, 0);

        let metrics_request = HttpRequest {
            method: "GET".to_string(),
            target: "/metrics".to_string(),
            headers: HashMap::new(),
            body: Vec::new(),
        };
        let metrics_response = handle_request(&runtime, &metrics_request);
        assert!(
            metrics_response
                .body
                .contains("dash_ingest_placement_route_reject_total 1")
        );
        assert!(
            metrics_response
                .body
                .contains("dash_ingest_placement_last_epoch 9")
        );
    }

    #[test]
    fn handle_request_write_route_reresolves_after_leader_promotion() {
        let placement = ShardPlacement {
            tenant_id: "tenant-a".to_string(),
            shard_id: 0,
            epoch: 9,
            replicas: vec![
                ReplicaPlacement {
                    node_id: "node-a".to_string(),
                    role: ReplicaRole::Leader,
                    health: ReplicaHealth::Healthy,
                },
                ReplicaPlacement {
                    node_id: "node-b".to_string(),
                    role: ReplicaRole::Follower,
                    health: ReplicaHealth::Healthy,
                },
            ],
        };
        let runtime = Arc::new(Mutex::new(
            IngestionRuntime::in_memory(InMemoryStore::new()).with_placement_runtime_for_tests(Ok(
                Some(PlacementRoutingRuntime {
                    local_node_id: "node-b".to_string(),
                    router_config: RouterConfig {
                        shard_ids: vec![0],
                        virtual_nodes_per_shard: 16,
                        replica_count: 2,
                    },
                    placements: vec![placement],
                }),
            )),
        ));
        let denied_request = HttpRequest {
            method: "POST".to_string(),
            target: "/v1/ingest".to_string(),
            headers: HashMap::from([("content-type".to_string(), "application/json".to_string())]),
            body: br#"{"claim":{"claim_id":"c-failover-a","tenant_id":"tenant-a","canonical_text":"placement route failover test","confidence":0.9}}"#.to_vec(),
        };
        let denied_response = handle_request(&runtime, &denied_request);
        assert_eq!(denied_response.status, 503);

        {
            let mut guard = runtime.lock().expect("runtime lock should hold");
            let routing = guard
                .placement_routing
                .as_mut()
                .expect("placement config should be present")
                .as_mut()
                .expect("placement routing should be enabled");
            let epoch = promote_replica_to_leader(&mut routing.runtime.placements[0], "node-b")
                .expect("promotion should succeed");
            assert_eq!(epoch, 10);
        }

        let accepted_request = HttpRequest {
            method: "POST".to_string(),
            target: "/v1/ingest".to_string(),
            headers: HashMap::from([("content-type".to_string(), "application/json".to_string())]),
            body: br#"{"claim":{"claim_id":"c-failover-b","tenant_id":"tenant-a","canonical_text":"placement route failover test","confidence":0.9}}"#.to_vec(),
        };
        let accepted_response = handle_request(&runtime, &accepted_request);
        assert_eq!(accepted_response.status, 200);

        let metrics_request = HttpRequest {
            method: "GET".to_string(),
            target: "/metrics".to_string(),
            headers: HashMap::new(),
            body: Vec::new(),
        };
        let metrics_response = handle_request(&runtime, &metrics_request);
        assert_eq!(metrics_response.status, 200);
        assert!(
            metrics_response
                .body
                .contains("dash_ingest_placement_route_reject_total 1")
        );
        assert!(
            metrics_response
                .body
                .contains("dash_ingest_placement_last_epoch 10")
        );
        assert!(
            metrics_response
                .body
                .contains("dash_ingest_placement_last_role 1")
        );
        assert!(
            metrics_response
                .body
                .contains("dash_ingest_placement_replicas_healthy 2")
        );
    }

    #[test]
    fn debug_placement_endpoint_returns_structured_route_probe() {
        let placement = ShardPlacement {
            tenant_id: "tenant-a".to_string(),
            shard_id: 0,
            epoch: 4,
            replicas: vec![
                ReplicaPlacement {
                    node_id: "node-a".to_string(),
                    role: ReplicaRole::Leader,
                    health: ReplicaHealth::Healthy,
                },
                ReplicaPlacement {
                    node_id: "node-b".to_string(),
                    role: ReplicaRole::Follower,
                    health: ReplicaHealth::Degraded,
                },
            ],
        };
        let runtime = Arc::new(Mutex::new(
            IngestionRuntime::in_memory(InMemoryStore::new()).with_placement_runtime_for_tests(Ok(
                Some(PlacementRoutingRuntime {
                    local_node_id: "node-b".to_string(),
                    router_config: RouterConfig {
                        shard_ids: vec![0],
                        virtual_nodes_per_shard: 16,
                        replica_count: 2,
                    },
                    placements: vec![placement],
                }),
            )),
        ));
        let request = HttpRequest {
            method: "GET".to_string(),
            target: "/debug/placement?tenant_id=tenant-a&entity_key=company-x".to_string(),
            headers: HashMap::new(),
            body: Vec::new(),
        };
        let response = handle_request(&runtime, &request);
        assert_eq!(response.status, 200);
        assert!(response.body.contains("\"enabled\":true"));
        assert!(response.body.contains("\"placements\""));
        assert!(response.body.contains("\"route_probe\""));
        assert!(response.body.contains("\"status\":\"rejected\""));
        assert!(response.body.contains("\"local_node_id\":\"node-b\""));
        assert!(response.body.contains("\"target_node_id\":\"node-a\""));
    }

    #[test]
    fn segment_publish_writes_manifest_and_metrics() {
        let mut root_dir = std::env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock should be monotonic")
            .as_nanos();
        root_dir.push(format!(
            "dash-ingest-segment-test-{}-{}",
            std::process::id(),
            nanos
        ));

        let runtime = Arc::new(Mutex::new(
            IngestionRuntime::in_memory(InMemoryStore::new()).with_segment_runtime_for_tests(Some(
                SegmentRuntime {
                    root_dir: root_dir.clone(),
                    max_segment_size: 1,
                    scheduler: CompactionSchedulerConfig {
                        max_segments_per_tier: 2,
                        max_compaction_input_segments: 2,
                    },
                    maintenance_interval: None,
                    maintenance_min_stale_age: Duration::from_millis(0),
                },
            )),
        ));

        let request = HttpRequest {
            method: "POST".to_string(),
            target: "/v1/ingest".to_string(),
            headers: HashMap::from([("content-type".to_string(), "application/json".to_string())]),
            body: br#"{"claim":{"claim_id":"c1","tenant_id":"tenant-a","canonical_text":"Company X acquired Company Y","confidence":0.9}}"#.to_vec(),
        };
        let response = handle_request(&runtime, &request);
        assert_eq!(response.status, 200);

        let manifest_path = root_dir.join("tenant-a").join("segments.manifest");
        assert!(manifest_path.exists());

        let metrics_request = HttpRequest {
            method: "GET".to_string(),
            target: "/metrics".to_string(),
            headers: HashMap::new(),
            body: Vec::new(),
        };
        let metrics_response = handle_request(&runtime, &metrics_request);
        assert_eq!(metrics_response.status, 200);
        assert!(
            metrics_response
                .body
                .contains("dash_ingest_segment_publish_success_total 1")
        );
        assert!(
            metrics_response
                .body
                .contains("dash_ingest_segment_last_claim_count 1")
        );

        let _ = std::fs::remove_dir_all(root_dir);
    }

    #[test]
    fn segment_maintenance_prunes_orphan_segment_files() {
        let mut root_dir = std::env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock should be monotonic")
            .as_nanos();
        root_dir.push(format!(
            "dash-ingest-segment-maintenance-test-{}-{}",
            std::process::id(),
            nanos
        ));

        let tenant_dir = root_dir.join("tenant-a");
        persist_segments_atomic(
            &tenant_dir,
            &[Segment {
                segment_id: "hot-0".to_string(),
                tier: Tier::Hot,
                claim_ids: vec!["c1".to_string()],
            }],
        )
        .expect("segment persist should succeed");
        let orphan_segment = tenant_dir.join("orphan.seg");
        std::fs::write(&orphan_segment, "orphan-segment")
            .expect("orphan segment write should succeed");
        assert!(orphan_segment.exists());

        let mut runtime = IngestionRuntime::in_memory(InMemoryStore::new())
            .with_segment_runtime_for_tests(Some(SegmentRuntime {
                root_dir: root_dir.clone(),
                max_segment_size: 1,
                scheduler: CompactionSchedulerConfig {
                    max_segments_per_tier: 2,
                    max_compaction_input_segments: 2,
                },
                maintenance_interval: Some(Duration::from_millis(1)),
                maintenance_min_stale_age: Duration::from_millis(0),
            }));
        runtime.run_segment_maintenance_tick();

        assert!(!orphan_segment.exists());
        let metrics = runtime.metrics_text();
        assert!(metrics.contains("dash_ingest_segment_maintenance_tick_total 1"));
        assert!(metrics.contains("dash_ingest_segment_maintenance_success_total 1"));
        assert!(metrics.contains("dash_ingest_segment_maintenance_failure_total 0"));
        assert!(metrics.contains("dash_ingest_segment_maintenance_last_pruned_count 1"));
        assert!(metrics.contains("dash_ingest_segment_maintenance_last_tenant_dirs 1"));
        assert!(metrics.contains("dash_ingest_segment_maintenance_last_tenant_manifests 1"));

        let _ = std::fs::remove_dir_all(root_dir);
    }

    #[test]
    fn append_audit_record_writes_chained_hash_and_seq() {
        let mut audit_path = std::env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock should be monotonic")
            .as_nanos();
        audit_path.push(format!(
            "dash-ingest-audit-chain-{}-{}.jsonl",
            std::process::id(),
            nanos
        ));
        let audit_path_str = audit_path.to_string_lossy().to_string();
        clear_cached_audit_chain_state(&audit_path_str);

        let first = AuditEvent {
            action: "ingest",
            tenant_id: Some("tenant-a"),
            claim_id: Some("c-1"),
            status: 200,
            outcome: "success",
            reason: "ok",
        };
        let second = AuditEvent {
            action: "ingest",
            tenant_id: Some("tenant-a"),
            claim_id: Some("c-2"),
            status: 200,
            outcome: "success",
            reason: "ok",
        };

        append_audit_record(&audit_path_str, &first, 1_700_000_000_001)
            .expect("first audit append should succeed");
        append_audit_record(&audit_path_str, &second, 1_700_000_000_002)
            .expect("second audit append should succeed");

        let payload = std::fs::read_to_string(&audit_path).expect("audit file should be readable");
        let lines: Vec<&str> = payload
            .lines()
            .filter(|line| !line.trim().is_empty())
            .collect();
        assert_eq!(lines.len(), 2);

        let first_obj = match parse_json(lines[0]).expect("first line JSON should parse") {
            JsonValue::Object(object) => object,
            _ => panic!("first line should be object"),
        };
        let second_obj = match parse_json(lines[1]).expect("second line JSON should parse") {
            JsonValue::Object(object) => object,
            _ => panic!("second line should be object"),
        };

        assert!(matches!(first_obj.get("seq"), Some(JsonValue::Number(raw)) if raw == "1"));
        assert!(matches!(second_obj.get("seq"), Some(JsonValue::Number(raw)) if raw == "2"));
        let first_hash = match first_obj.get("hash") {
            Some(JsonValue::String(raw)) => raw.clone(),
            _ => panic!("first hash should exist"),
        };
        let second_prev = match second_obj.get("prev_hash") {
            Some(JsonValue::String(raw)) => raw.clone(),
            _ => panic!("second prev_hash should exist"),
        };
        assert!(is_sha256_hex(&first_hash));
        assert_eq!(second_prev, first_hash);

        let _ = std::fs::remove_file(audit_path);
    }
}
