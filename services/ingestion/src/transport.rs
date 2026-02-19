use std::{
    collections::{HashMap, HashSet},
    net::{TcpListener, TcpStream},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
        mpsc,
    },
    time::{Duration, Instant},
};

mod audit;
mod authz;
mod config;
mod http;
mod json;
mod payload;
mod persistence;
mod placement_debug;
mod placement_routing;
mod replication;
mod request;
mod segment_runtime;

use audit::{AuditEvent, emit_audit_event};
use authz::{AuthDecision, AuthPolicy, authorize_request_for_tenant};
use config::{
    env_with_fallback, generate_batch_commit_id, parse_env_first_usize,
    resolve_ingest_batch_max_items, resolve_wal_async_flush_interval, unix_timestamp_millis,
};
use http::{
    HttpRequest, HttpResponse, render_response_text, write_backpressure_response, write_response,
};
use metadata_router::{ReplicaRole, RoutedReplica, route_write_with_placement};
use payload::{
    build_ingest_batch_request_from_json, build_ingest_request_from_json,
    render_ingest_batch_response_json, render_ingest_response_json,
};
use persistence::{append_input_to_wal, map_store_error, should_checkpoint_now};
use placement_debug::render_placement_debug_json;
use placement_routing::{
    PlacementRoutingState, WriteRouteError, map_write_route_error, write_entity_key_for_claim,
};
use replication::{
    ReplicationPullConfig, parse_replication_delta_frame, parse_replication_export_frame,
    render_replication_delta_frame, render_replication_export_frame, request_replication_source,
};
use request::{parse_query_usize, parse_request_line, read_http_request, split_target};
use schema::Claim;
use segment_runtime::SegmentRuntime;
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
#[cfg(test)]
use json::{JsonValue, parse_json};
#[cfg(test)]
use metadata_router::{RouterConfig, ShardPlacement};
#[cfg(test)]
use placement_routing::PlacementRoutingRuntime;

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
        let wal_async_flush_interval =
            resolve_wal_async_flush_interval(Some(&wal), DEFAULT_ASYNC_WAL_FLUSH_INTERVAL_MS);
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
            let max_items = resolve_ingest_batch_max_items(DEFAULT_INGEST_BATCH_MAX_ITEMS);
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

#[cfg(test)]
mod tests;
