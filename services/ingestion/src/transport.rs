use std::{
    collections::{HashMap, HashSet},
    net::TcpListener,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
        mpsc,
    },
    time::{Duration, Instant},
};

mod audit;
mod authz;
mod commit_status;
mod config;
mod delete_routes;
mod document_parser_debug;
mod group_commit;
mod http;
mod ingest_routes;
mod json;
mod payload;
mod persistence;
mod placement_debug;
mod placement_routing;
mod read_routes;
mod replication;
mod request;
mod routes;
mod segment_runtime;
mod server_runtime;

use audit::{AuditEvent, emit_audit_event};
pub(crate) use authz::{
    AuthDecision, Role, authorize_request_for_tenant, authorize_request_ops, shared_auth_policy,
};
pub use authz::{initialize_auth_policy, warn_replication_transport};
use config::{
    env_with_fallback, generate_batch_commit_id, parse_env_first_u64, parse_env_first_usize,
    resolve_ingest_batch_max_items, resolve_wal_async_flush_interval, unix_timestamp_millis,
};
use dash_common::AuthPolicy;
use document_parser_debug::render_document_parser_debug_json;
use http::{HttpRequest, HttpResponse, render_response_text, server_config};
use metadata_router::{
    PlacementRouteError, ReplicaHealth, ReplicaRole, RoutedReplica, route_write_with_placement,
};
use payload::{
    build_ingest_batch_request_from_json, build_ingest_document_request_from_json,
    build_ingest_raw_request_from_json, build_ingest_request_from_json,
    render_ingest_batch_response_json, render_ingest_document_response_json,
    render_ingest_raw_response_json, render_ingest_response_json,
};
pub use persistence::{DEFAULT_CHECKPOINT_MAX_WAL_BYTES, checkpoint_policy_from_values};
use persistence::{append_input_to_wal, map_store_error, should_checkpoint_now};
use placement_debug::render_placement_debug_json;
use placement_routing::{
    PlacementRoutingState, WriteRouteError, WriteRouteResolution, map_write_route_error,
    refresh_placement, write_entity_key_for_claim,
};
use replication::{
    ReplicationPullConfig, is_replication_request_authorized, render_replication_delta_frame,
    render_replication_export_frame, run_replication_pull_tick,
};
use request::{parse_query_usize, query_encoding_is_invalid, split_target};
use schema::Claim;
use segment_runtime::SegmentRuntime;
use store::{
    CheckpointPolicy, DiskStatus, FileWal, InMemoryStore, StoreError, VectorIndexPersistence,
    VectorIndexSaveStats, VectorIndexSnapshot, WalReplicationExport, WalReplicationFrame,
};

use crate::{
    IngestInput,
    api::{
        IngestApiRequest, IngestApiResponse, IngestBatchApiRequest, IngestBatchApiResponse,
        IngestDocumentApiResponse, IngestRawApiResponse, WriteConsistencyPolicy,
    },
    extraction::{build_ingest_batch_from_document_request, build_ingest_raw_output_from_request},
    ingest_document,
};

#[cfg(test)]
use audit::{append_audit_record, clear_cached_audit_chain_state, is_sha256_hex};
#[cfg(test)]
use json::{JsonValue, parse_json};
#[cfg(test)]
use metadata_router::{RouterConfig, ShardPlacement};
#[cfg(test)]
use placement_routing::PlacementRoutingRuntime;

/// The WAL, shared between the runtime and the group committer thread. Its
/// mutex serializes every write; the runtime lock is always taken first.
pub(crate) type SharedWal = Arc<Mutex<FileWal>>;

/// Locks the WAL. A panic while holding it cannot leave the file half
/// written in a way the WAL does not already handle, so poisoning is ignored.
pub(crate) fn lock_wal(wal: &SharedWal) -> std::sync::MutexGuard<'_, FileWal> {
    wal.lock().unwrap_or_else(|e| e.into_inner())
}

pub struct IngestionRuntime {
    store: InMemoryStore,
    wal: Option<SharedWal>,
    group_commit: Option<group_commit::GroupCommitPipeline>,
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
    wal_flush_synced_records_total: u64,
    wal_flush_sync_latency_micros_total: u64,
    wal_flush_last_synced_records: u64,
    wal_flush_last_sync_latency_micros: u64,
    wal_async_flush_tick_total: u64,
    /// Set when a write could not be persisted (WAL append, fsync or
    /// interval flush failed with an I/O error, for example a full disk).
    /// While set, `/ready` reports `wal_write_failed`; it clears after a
    /// persisted write or a successful space probe.
    wal_write_error: Option<String>,
    wal_write_failure_total: u64,
    wal_write_recovered_total: u64,
    replication_pull_success_total: u64,
    replication_pull_failure_total: u64,
    replication_applied_records_total: u64,
    replication_resync_total: u64,
    replication_last_offset: usize,
    replication_last_error: Option<String>,
    replication_follower: replication::ReplicationFollowerState,
    replication_commit_status: commit_status::CommitStatusTable,
    transport_backpressure: Option<Arc<TransportBackpressureMetrics>>,
    started_at: Instant,
    /// Saves the vector indexes (persistent mode only); see
    /// `with_vector_index_persistence`.
    vector_index_persistence: Option<Arc<VectorIndexPersistence>>,
    /// Chunked exports served to followers that resync (`<wal>.exports`).
    replication_exports: Option<Arc<store::ReplicationExportStore>>,
    /// Counters of the delete routes (`delete_routes`).
    delete_metrics: delete_routes::DeleteMetrics,
}

#[derive(Debug, Default)]
pub(crate) struct TransportBackpressureMetrics {
    pub(crate) queue_depth: AtomicUsize,
    pub(crate) queue_capacity: usize,
    pub(crate) queue_full_reject_total: AtomicU64,
    /// Requests that failed while being read (408/413/431/400/...), by status class.
    pub(crate) read_error_4xx_total: AtomicU64,
    pub(crate) read_error_5xx_total: AtomicU64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReplicationCommitStatusSnapshot {
    commit_id: String,
    commit_epoch: Option<u64>,
    ack_count: usize,
    required_acks: usize,
    commit_status: String,
}

impl TransportBackpressureMetrics {
    pub(crate) fn new(queue_capacity: usize) -> Self {
        Self {
            queue_depth: AtomicUsize::new(0),
            queue_capacity,
            queue_full_reject_total: AtomicU64::new(0),
            read_error_4xx_total: AtomicU64::new(0),
            read_error_5xx_total: AtomicU64::new(0),
        }
    }

    pub(crate) fn observe_enqueued(&self) {
        self.queue_depth.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn observe_dequeued(&self) {
        let _ = self
            .queue_depth
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                Some(value.saturating_sub(1))
            });
    }

    pub(crate) fn observe_rejected(&self) {
        self.queue_full_reject_total.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn observe_read_error(&self, status: u16) {
        let counter = if status >= 500 {
            &self.read_error_5xx_total
        } else {
            &self.read_error_4xx_total
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }
}

impl IngestionRuntime {
    pub fn in_memory(store: InMemoryStore) -> Self {
        Self {
            store,
            wal: None,
            group_commit: None,
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
            wal_flush_synced_records_total: 0,
            wal_flush_sync_latency_micros_total: 0,
            wal_flush_last_synced_records: 0,
            wal_flush_last_sync_latency_micros: 0,
            wal_async_flush_tick_total: 0,
            wal_write_error: None,
            wal_write_failure_total: 0,
            wal_write_recovered_total: 0,
            replication_pull_success_total: 0,
            replication_pull_failure_total: 0,
            replication_applied_records_total: 0,
            replication_resync_total: 0,
            replication_last_offset: 0,
            replication_last_error: None,
            replication_follower: replication::ReplicationFollowerState::default(),
            replication_commit_status: commit_status::CommitStatusTable::from_env(),
            transport_backpressure: None,
            started_at: Instant::now(),
            vector_index_persistence: None,
            replication_exports: None,
            delete_metrics: delete_routes::DeleteMetrics::default(),
        }
    }

    pub fn persistent(
        store: InMemoryStore,
        wal: FileWal,
        checkpoint_policy: CheckpointPolicy,
    ) -> Self {
        let wal_async_flush_interval =
            resolve_wal_async_flush_interval(Some(&wal), DEFAULT_ASYNC_WAL_FLUSH_INTERVAL_MS);
        let replication_exports =
            Some(Arc::new(store::ReplicationExportStore::for_wal(wal.path())));
        let wal = Arc::new(Mutex::new(wal));
        let group_commit = group_commit::resolve_group_commit_config().and_then(|config| {
            match store::GroupCommitter::start(Arc::clone(&wal), config) {
                Ok(committer) => Some(group_commit::GroupCommitPipeline::new(committer)),
                Err(err) => {
                    eprintln!(
                        "ingestion could not start the WAL group committer ({err}); single ingests fsync one by one"
                    );
                    None
                }
            }
        });
        Self {
            store,
            wal: Some(wal),
            group_commit,
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
            wal_flush_synced_records_total: 0,
            wal_flush_sync_latency_micros_total: 0,
            wal_flush_last_synced_records: 0,
            wal_flush_last_sync_latency_micros: 0,
            wal_async_flush_tick_total: 0,
            wal_write_error: None,
            wal_write_failure_total: 0,
            wal_write_recovered_total: 0,
            replication_pull_success_total: 0,
            replication_pull_failure_total: 0,
            replication_applied_records_total: 0,
            replication_resync_total: 0,
            replication_last_offset: 0,
            replication_last_error: None,
            replication_follower: replication::ReplicationFollowerState::default(),
            replication_commit_status: commit_status::CommitStatusTable::from_env(),
            transport_backpressure: None,
            started_at: Instant::now(),
            vector_index_persistence: None,
            replication_exports,
            delete_metrics: delete_routes::DeleteMetrics::default(),
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

    fn ensure_local_write_route_for_claim(
        &mut self,
        claim: &Claim,
        write_consistency: WriteConsistencyPolicy,
    ) -> Result<WriteRouteResolution, WriteRouteError> {
        let Some(routing_state) = self
            .placement_routing
            .as_mut()
            .map_err(|reason| WriteRouteError::Config(reason.clone()))?
            .as_mut()
        else {
            let required_acks = write_consistency.required_acks(1);
            return Ok(WriteRouteResolution {
                shard_id: 0,
                epoch: 0,
                ack_count: required_acks,
                required_acks,
                total_replicas: 1,
            });
        };
        routing_state.check_fresh()?;
        let routing = routing_state.runtime();
        let entity_key = write_entity_key_for_claim(claim);
        let tenant_router_config = routing.router_config_for_tenant(&claim.tenant_id);
        let routed = route_write_with_placement(
            &claim.tenant_id,
            entity_key,
            &tenant_router_config,
            &routing.placements,
        )
        .map_err(WriteRouteError::Placement)?;
        let placement = routing
            .placements
            .iter()
            .find(|placement| {
                placement.tenant_id == claim.tenant_id && placement.shard_id == routed.shard_id
            })
            .ok_or_else(|| {
                WriteRouteError::Placement(PlacementRouteError::PlacementNotFound {
                    tenant_id: claim.tenant_id.clone(),
                    shard_id: routed.shard_id,
                })
            })?;
        let total_replicas = placement.replicas.len().max(1);
        let required_acks = write_consistency.required_acks(total_replicas);
        let healthy_replicas = placement
            .replicas
            .iter()
            .filter(|replica| replica.health == ReplicaHealth::Healthy)
            .count();
        if healthy_replicas < required_acks {
            return Err(WriteRouteError::ConsistencyUnavailable {
                policy: write_consistency,
                shard_id: routed.shard_id,
                healthy_replicas,
                required_replicas: required_acks,
                total_replicas,
                epoch: routed.epoch,
            });
        }
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
        Ok(WriteRouteResolution {
            shard_id: routed.shard_id,
            epoch: routed.epoch,
            ack_count: 1,
            required_acks,
            total_replicas,
        })
    }

    fn ingest(&mut self, request: IngestApiRequest) -> Result<IngestApiResponse, StoreError> {
        let result = self.ingest_unobserved(request);
        // An unchanged replay writes nothing, so it says nothing about the disk.
        let wrote = matches!(&result, Ok((_, true)));
        self.observe_write_outcome(result.as_ref().map(|_| wrote));
        result.map(|(resp, _)| resp)
    }

    /// The write and whether it appended to the WAL.
    fn ingest_unobserved(
        &mut self,
        request: IngestApiRequest,
    ) -> Result<(IngestApiResponse, bool), StoreError> {
        let tenant_id = request.claim.tenant_id.clone();
        let ingested_claim_id = request.claim.claim_id.clone();
        let input = IngestInput {
            claim: request.claim,
            claim_embedding: request.claim_embedding,
            evidence: request.evidence,
            edges: request.edges,
        };
        crate::api::validate_ingest_bundles(
            &self.store,
            &[(
                &input.claim,
                input.claim_embedding.as_deref(),
                input.edges.as_slice(),
            )],
        )?;
        let (checkpoint_stats, checkpoint_deferred, applied) = self.ingest_input_internal(input)?;

        self.successful_ingests += 1;
        // A retry that changed nothing must not re-scan the tenant's claims
        // to rebuild segments.
        if applied {
            self.publish_segments_for_tenant(&tenant_id);
        }
        let response = self.ingest_response(
            ingested_claim_id,
            Some((checkpoint_stats, checkpoint_deferred)),
        );
        Ok((response, applied))
    }

    /// Response of a committed single ingest. `checkpoint` is the outcome of
    /// [`IngestionRuntime::checkpoint_after_commit`], `None` when no
    /// checkpoint was considered.
    fn ingest_response(
        &self,
        ingested_claim_id: String,
        checkpoint: Option<(Option<store::WalCheckpointStats>, bool)>,
    ) -> IngestApiResponse {
        let (checkpoint_stats, checkpoint_deferred) = checkpoint.unwrap_or((None, false));
        IngestApiResponse {
            ingested_claim_id,
            claims_total: self.store.claims_len(),
            commit_epoch: None,
            ack_count: 1,
            required_acks: 1,
            commit_status: "accepted".to_string(),
            checkpoint_triggered: checkpoint_stats.is_some(),
            checkpoint_snapshot_records: checkpoint_stats.as_ref().map(|s| s.snapshot_records),
            checkpoint_truncated_wal_records: checkpoint_stats
                .as_ref()
                .map(|s| s.truncated_wal_records),
            checkpoint_deferred,
        }
    }

    /// `true` when single ingests go through the group committer.
    pub(crate) fn group_commit_active(&self) -> bool {
        self.group_commit.is_some()
    }

    /// One-line description of the group-commit settings for the startup
    /// log, or `None` when group commit is off.
    pub fn group_commit_summary(&self) -> Option<String> {
        let config = self.group_commit.as_ref()?.committer().config();
        Some(format!(
            "max_wait_us={}, max_batch_bytes={}, queue_capacity={}",
            config.max_wait.as_micros(),
            config.max_batch_bytes,
            config.queue_capacity
        ))
    }

    /// Why the WAL refuses writes (after an fsync failure), if it does.
    pub fn wal_poisoned_reason(&self) -> Option<String> {
        let wal = self.wal.as_ref()?;
        lock_wal(wal).poisoned_reason().map(str::to_string)
    }

    fn ingest_batch(
        &mut self,
        request: IngestBatchApiRequest,
    ) -> Result<IngestBatchApiResponse, StoreError> {
        let result = self.ingest_batch_unobserved(request);
        let wrote = matches!(&result, Ok(resp) if !resp.idempotent_replay);
        self.observe_write_outcome(result.as_ref().map(|_| wrote));
        result
    }

    fn ingest_batch_unobserved(
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

        let bundles: Vec<_> = inputs
            .iter()
            .map(|input| {
                (
                    &input.claim,
                    input.claim_embedding.as_deref(),
                    input.edges.as_slice(),
                )
            })
            .collect();
        crate::api::validate_ingest_bundles(&self.store, &bundles)?;

        // Idempotency is decided on CONTENT, not on claim ids (DATA-12): a
        // known commit id whose every bundle is already stored verbatim is a
        // replay; a known commit id with different content is an update that
        // upserts over the previous version.
        let existing = self.store.batch_commit_metadata(&commit_id).cloned();
        let content_unchanged = inputs.iter().all(|input| {
            self.store.bundle_already_applied(
                &input.claim,
                &input.evidence,
                &input.edges,
                input.claim_embedding.as_deref(),
            )
        });
        if existing.is_some() && content_unchanged {
            self.batch_success_total = self.batch_success_total.saturating_add(1);
            self.batch_last_size = ingested_claim_ids.len();
            self.batch_idempotent_hit_total = self.batch_idempotent_hit_total.saturating_add(1);
            return Ok(IngestBatchApiResponse {
                commit_id,
                idempotent_replay: true,
                updated: false,
                batch_size: ingested_claim_ids.len(),
                ingested_claim_ids,
                claims_total: self.store.claims_len(),
                commit_epoch: None,
                ack_count: 1,
                required_acks: 1,
                commit_status: "accepted".to_string(),
                checkpoint_triggered: false,
                checkpoint_snapshot_records: None,
                checkpoint_truncated_wal_records: None,
                checkpoint_deferred: false,
            });
        }
        let updated = existing.is_some();
        // The commit metadata is keyed by claim-id set; an update that
        // changes the set is recorded under a versioned id so the original
        // record is never rewritten (and replay never sees a conflict).
        let wal_commit_id = match existing.as_ref() {
            Some(meta) if meta.claim_ids != ingested_claim_ids => format!(
                "{commit_id}@{}",
                store::batch_commit_payload_fingerprint(
                    ingested_claim_ids.len(),
                    &ingested_claim_ids
                )
            ),
            _ => commit_id.clone(),
        };

        // Stage on a detached clone: nothing reaches redb until the WAL
        // append succeeded (DATA-09).
        let commit_ts_unix_ms = unix_timestamp_millis();
        let mut staged_store = self.store.clone_detached();
        for input in &inputs {
            ingest_document(&mut staged_store, input.clone())?;
        }
        staged_store.observe_batch_commit(
            &wal_commit_id,
            ingested_claim_ids.len(),
            commit_ts_unix_ms,
            &ingested_claim_ids,
        )?;

        if let Some(wal) = self.wal.as_ref() {
            let mut wal = lock_wal(wal);
            let wal = &mut *wal;
            let rollback_point = wal.begin_rollback_point()?;
            let append_result = (|| {
                wal.begin_group(&wal_commit_id, commit_ts_unix_ms)?;
                for input in &inputs {
                    append_input_to_wal(wal, input)?;
                }
                wal.append_batch_commit(
                    &wal_commit_id,
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

        // The WAL is durable: the commit stands even if redb mirroring
        // fails (the store detaches redb and reports `Unavailable`).
        if let Err(err) = self.store.commit_staged(staged_store) {
            eprintln!("ingestion batch commit: redb mirror failed after WAL commit: {err:?}");
        }
        self.successful_ingests = self
            .successful_ingests
            .saturating_add(ingested_claim_ids.len() as u64);

        let (checkpoint_stats, checkpoint_deferred) = self.checkpoint_after_commit("batch");

        for tenant_id in touched_tenants {
            self.publish_segments_for_tenant(&tenant_id);
        }

        self.batch_success_total = self.batch_success_total.saturating_add(1);
        self.batch_last_size = ingested_claim_ids.len();
        Ok(IngestBatchApiResponse {
            commit_id,
            idempotent_replay: false,
            updated,
            batch_size: ingested_claim_ids.len(),
            ingested_claim_ids,
            claims_total: self.store.claims_len(),
            commit_epoch: None,
            ack_count: 1,
            required_acks: 1,
            commit_status: "accepted".to_string(),
            checkpoint_triggered: checkpoint_stats.is_some(),
            checkpoint_snapshot_records: checkpoint_stats.as_ref().map(|s| s.snapshot_records),
            checkpoint_truncated_wal_records: checkpoint_stats
                .as_ref()
                .map(|s| s.truncated_wal_records),
            checkpoint_deferred,
        })
    }

    /// Checkpoint after a committed write. A failure never invalidates the
    /// commit: the write is durable in the WAL, so the caller is told the
    /// checkpoint was deferred and a later write retries it (DATA-10).
    fn checkpoint_after_commit(
        &mut self,
        label: &str,
    ) -> (Option<store::WalCheckpointStats>, bool) {
        let Some(wal) = self.wal.as_ref() else {
            return (None, false);
        };
        let mut wal = lock_wal(wal);
        match should_checkpoint_now(&self.checkpoint_policy, &wal) {
            Ok(false) => (None, false),
            Ok(true) => match self.store.checkpoint_and_compact(&mut wal) {
                Ok(stats) => {
                    // The checkpoint started a new WAL generation, which the
                    // saved vector index no longer matches: save it again.
                    if let Some(persistence) = self.vector_index_persistence.as_ref() {
                        persistence.request_save();
                    }
                    (Some(stats), false)
                }
                Err(err) => {
                    eprintln!("ingestion {label} checkpoint failed after commit: {err:?}");
                    (None, true)
                }
            },
            Err(err) => {
                eprintln!("ingestion {label} checkpoint check failed after commit: {err:?}");
                (None, true)
            }
        }
    }

    fn ingest_input_internal(
        &mut self,
        input: IngestInput,
    ) -> Result<(Option<store::WalCheckpointStats>, bool, bool), StoreError> {
        let Some(wal) = self.wal.as_ref() else {
            ingest_document(&mut self.store, input)?;
            return Ok((None, false, true));
        };
        let outcome = self.store.ingest_atomic_persistent(
            &mut lock_wal(wal),
            input.claim,
            input.evidence,
            input.edges,
            input.claim_embedding,
            unix_timestamp_millis(),
        )?;
        if let Some(reason) = outcome.disk_error {
            eprintln!("ingestion redb mirror failed after WAL commit: {reason}");
        }
        let (stats, deferred) = self.checkpoint_after_commit("ingest");
        Ok((stats, deferred, outcome.applied))
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

    fn record_commit_status(
        &mut self,
        commit_id: &str,
        commit_epoch: Option<u64>,
        ack_count: usize,
        required_acks: usize,
    ) {
        let required_acks = required_acks.max(1);
        let ack_count = ack_count.max(1);
        let mut acknowledged_replicas = HashSet::new();
        if let Some(local_replica_id) = self.local_replica_ack_seed() {
            acknowledged_replicas.insert(local_replica_id);
        }
        self.replication_commit_status.insert(
            commit_id.to_string(),
            commit_status::ReplicationCommitStatus::new(
                commit_epoch,
                ack_count,
                required_acks,
                acknowledged_replicas,
            ),
            Instant::now(),
        );
    }

    fn local_replica_ack_seed(&self) -> Option<String> {
        self.placement_routing
            .as_ref()
            .ok()
            .and_then(|state| state.as_ref())
            .map(|state| state.runtime().local_node_id.clone())
            .filter(|value| !value.trim().is_empty())
    }

    fn replication_commit_status_snapshot(
        &self,
        commit_id: &str,
    ) -> Option<ReplicationCommitStatusSnapshot> {
        let status = self.replication_commit_status.get(commit_id)?;
        Some(ReplicationCommitStatusSnapshot {
            commit_id: commit_id.to_string(),
            commit_epoch: status.commit_epoch,
            ack_count: status.ack_count,
            required_acks: status.required_acks,
            commit_status: status.commit_status.clone(),
        })
    }

    fn apply_replication_ack(
        &mut self,
        commit_id: &str,
        replica_id: &str,
        ack_epoch: Option<u64>,
    ) -> Result<ReplicationCommitStatusSnapshot, String> {
        let status = self
            .replication_commit_status
            .ack(commit_id, replica_id, ack_epoch, Instant::now())
            .ok_or_else(|| format!("unknown commit_id '{}'", commit_id))?;
        Ok(ReplicationCommitStatusSnapshot {
            commit_id: commit_id.to_string(),
            commit_epoch: status.commit_epoch,
            ack_count: status.ack_count,
            required_acks: status.required_acks,
            commit_status: status.commit_status.clone(),
        })
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

    /// Track whether writes can be persisted. `Ok(true)` is a write that
    /// reached the WAL; `Ok(false)` wrote nothing (an idempotent replay); an
    /// I/O error means the WAL could not be appended or synced.
    pub(super) fn observe_write_outcome(&mut self, outcome: Result<bool, &StoreError>) {
        match outcome {
            Ok(true) => {
                if self.wal.is_some() && self.wal_write_error.take().is_some() {
                    self.wal_write_recovered_total += 1;
                    eprintln!("ingestion: WAL writes succeed again; ready");
                }
            }
            Ok(false) => {}
            Err(StoreError::Io(reason)) => self.observe_wal_write_failure(reason),
            Err(_) => {}
        }
    }

    fn observe_wal_write_failure(&mut self, reason: &str) {
        if self.wal.is_none() {
            return;
        }
        self.wal_write_failure_total += 1;
        if self.wal_write_error.is_none() {
            eprintln!("ingestion: write could not be persisted, not ready: {reason}");
        }
        self.wal_write_error = Some(reason.to_string());
    }

    /// Readiness of the write path. After a failed write the WAL file must
    /// exist and the WAL directory is probed with a scratch file of
    /// [`WAL_SPACE_PROBE_BYTES`] (written, synced and removed); the service
    /// is ready again once that succeeds.
    pub(crate) fn wal_write_readiness(&mut self) -> Result<(), &'static str> {
        if self.wal_write_error.is_none() {
            return Ok(());
        }
        let Some(wal) = self.wal.as_ref() else {
            return Ok(());
        };
        let path = lock_wal(wal).path().to_path_buf();
        let probe =
            std::fs::metadata(&path).and_then(|_| probe_wal_space(&path, WAL_SPACE_PROBE_BYTES));
        match probe {
            Ok(()) => {
                self.wal_write_error = None;
                self.wal_write_recovered_total += 1;
                eprintln!("ingestion: WAL space probe succeeded; ready");
                Ok(())
            }
            Err(_) => Err("wal_write_failed"),
        }
    }

    fn flush_wal_if_due(&mut self) {
        let Some(wal) = self.wal.as_ref() else {
            return;
        };
        let mut wal = lock_wal(wal);
        if wal.background_flush_only() {
            return;
        }
        let unsynced_before = wal.unsynced_record_count() as u64;
        let started = Instant::now();
        let result = wal.flush_pending_sync_if_interval_elapsed();
        drop(wal);
        match result {
            Ok(true) => {
                self.wal_flush_due_total += 1;
                self.wal_flush_success_total += 1;
                self.observe_wal_flush_success(unsynced_before, started.elapsed());
            }
            Ok(false) => {}
            Err(err) => {
                self.wal_flush_due_total += 1;
                self.wal_flush_failure_total += 1;
                eprintln!("ingestion WAL interval flush failed: {err:?}");
                if let StoreError::Io(reason) = &err {
                    self.observe_wal_write_failure(reason);
                }
            }
        }
    }

    pub(crate) fn flush_wal_for_async_tick(&mut self) {
        let Some(wal) = self.wal.as_ref() else {
            return;
        };
        let mut wal = lock_wal(wal);
        let unsynced_before = wal.unsynced_record_count() as u64;
        let started = Instant::now();
        let result = wal.flush_pending_sync_if_unsynced();
        drop(wal);
        self.wal_async_flush_tick_total += 1;
        match result {
            Ok(true) => {
                self.wal_flush_due_total += 1;
                self.wal_flush_success_total += 1;
                self.observe_wal_flush_success(unsynced_before, started.elapsed());
            }
            Ok(false) => {}
            Err(err) => {
                self.wal_flush_due_total += 1;
                self.wal_flush_failure_total += 1;
                eprintln!("ingestion WAL async flush failed: {err:?}");
                if let StoreError::Io(reason) = &err {
                    self.observe_wal_write_failure(reason);
                }
            }
        }
    }

    pub(crate) fn wal_async_flush_interval(&self) -> Option<Duration> {
        self.wal_async_flush_interval
    }

    /// Save the vector indexes through `persistence`: periodically, after
    /// every WAL checkpoint and once more at a clean shutdown (see
    /// `serve_http_with_workers`). Ignored without a WAL.
    pub fn with_vector_index_persistence(
        mut self,
        persistence: Arc<VectorIndexPersistence>,
    ) -> Self {
        if self.wal.is_some() {
            self.vector_index_persistence = Some(persistence);
        }
        self
    }

    pub(crate) fn vector_index_persistence(&self) -> Option<Arc<VectorIndexPersistence>> {
        self.vector_index_persistence.clone()
    }

    /// A copy of the vector indexes stamped with the WAL position they
    /// reflect. The WAL is flushed first so that position is durable; on a
    /// flush failure there is nothing safe to save and `None` is returned.
    /// Cheap (copy-on-write); the caller saves it after releasing the lock.
    pub fn vector_index_snapshot(&mut self) -> Option<VectorIndexSnapshot> {
        // A group-committed record that is durable but not yet applied would
        // be inside the stamped WAL position yet missing from the snapshot.
        // Skip; the next interval, checkpoint or shutdown saves instead.
        if group_commit::has_unapplied(self) {
            return None;
        }
        let wal = self.wal.as_ref()?;
        let mut wal = lock_wal(wal);
        if let Err(err) = wal.flush_pending_sync() {
            tracing::warn!("ingestion vector index save skipped: WAL flush failed: {err:?}");
            return None;
        }
        Some(self.store.vector_index_snapshot(wal.position()))
    }

    fn observe_wal_flush_success(&mut self, synced_records: u64, latency: Duration) {
        let latency_micros = latency.as_micros().min(u64::MAX as u128) as u64;
        self.wal_flush_synced_records_total = self
            .wal_flush_synced_records_total
            .saturating_add(synced_records);
        self.wal_flush_sync_latency_micros_total = self
            .wal_flush_sync_latency_micros_total
            .saturating_add(latency_micros);
        self.wal_flush_last_synced_records = synced_records;
        self.wal_flush_last_sync_latency_micros = latency_micros;
    }

    pub(crate) fn set_transport_backpressure_metrics(
        &mut self,
        metrics: Arc<TransportBackpressureMetrics>,
    ) {
        self.transport_backpressure = Some(metrics);
    }

    fn begin_placement_refresh(&mut self) -> Option<placement_routing::PlacementRefreshJob> {
        match self.placement_routing.as_mut() {
            Ok(Some(state)) => state.begin_refresh(),
            _ => None,
        }
    }

    fn finish_placement_refresh(
        &mut self,
        result: Result<placement_routing::PlacementRoutingRuntime, String>,
    ) {
        if let Ok(Some(state)) = self.placement_routing.as_mut() {
            state.finish_refresh(result);
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

    /// A delta frame for a follower. With `allow_switch` (the follower sent
    /// `gen_switch=1`) a follower at the exact end of a generation a
    /// checkpoint closed is moved to offset 0 of the current generation
    /// instead of being sent to a resync.
    fn replication_delta_for_followers(
        &mut self,
        from_generation: Option<u64>,
        from_offset: usize,
        max_records: usize,
        allow_switch: bool,
    ) -> Result<WalReplicationFrame, StoreError> {
        let wal = self.wal.as_ref().ok_or_else(|| {
            StoreError::Io("replication source requires persistent WAL mode".to_string())
        })?;
        let mut wal = lock_wal(wal);
        if allow_switch {
            wal.replication_frame_with_switch(from_generation, from_offset, max_records)
        } else {
            wal.replication_frame_from(from_generation, from_offset, max_records)
        }
    }

    /// The export store and the WAL it exports, for serving a chunked
    /// export without holding the runtime lock.
    fn replication_export_handles(
        &self,
    ) -> Result<(Arc<store::ReplicationExportStore>, SharedWal), StoreError> {
        match (self.replication_exports.as_ref(), self.wal.as_ref()) {
            (Some(exports), Some(wal)) => Ok((Arc::clone(exports), Arc::clone(wal))),
            _ => Err(StoreError::Io(
                "replication source requires persistent WAL mode".to_string(),
            )),
        }
    }

    /// Full export plus the WAL generation it was taken at (read under the
    /// same runtime lock, so the pair is consistent).
    fn replication_export_for_followers(
        &mut self,
    ) -> Result<(WalReplicationExport, u64), StoreError> {
        let wal = self.wal.as_ref().ok_or_else(|| {
            StoreError::Io("replication source requires persistent WAL mode".to_string())
        })?;
        let mut wal = lock_wal(wal);
        let export = wal.replication_export()?;
        Ok((export, wal.generation()))
    }

    fn observe_replication_pull_failure(&mut self, error: String) {
        self.replication_pull_failure_total = self.replication_pull_failure_total.saturating_add(1);
        self.replication_last_error = Some(error);
    }

    pub fn disk_status(&self) -> &DiskStatus {
        self.store.disk_status()
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
        let (wal_unsynced_records, wal_buffered_records, wal_background_flush_only, wal_poisoned) =
            self.wal
                .as_ref()
                .map(|wal| {
                    let wal = lock_wal(wal);
                    (
                        wal.unsynced_record_count(),
                        wal.buffered_record_count(),
                        wal.background_flush_only(),
                        wal.poisoned_reason().is_some(),
                    )
                })
                .unwrap_or((0, 0, false, false));
        let wal_background_flush_only = wal_background_flush_only as usize;
        let wal_poisoned = wal_poisoned as usize;
        let wal_async_flush_enabled = self.wal_async_flush_interval.is_some() as usize;
        let wal_async_flush_interval_ms = self
            .wal_async_flush_interval
            .map(|value| value.as_millis() as u64)
            .unwrap_or(0);
        let wal_flush_avg_synced_records = if self.wal_flush_success_total > 0 {
            self.wal_flush_synced_records_total as f64 / self.wal_flush_success_total as f64
        } else {
            0.0
        };
        let wal_flush_avg_sync_latency_micros = if self.wal_flush_success_total > 0 {
            self.wal_flush_sync_latency_micros_total as f64 / self.wal_flush_success_total as f64
        } else {
            0.0
        };
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
        let (read_error_4xx, read_error_5xx) = self
            .transport_backpressure
            .as_ref()
            .map(|metrics| {
                (
                    metrics.read_error_4xx_total.load(Ordering::Relaxed),
                    metrics.read_error_5xx_total.load(Ordering::Relaxed),
                )
            })
            .unwrap_or((0, 0));
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
# TYPE dash_ingest_wal_flush_synced_records_total counter\n\
dash_ingest_wal_flush_synced_records_total {}\n\
# TYPE dash_ingest_wal_flush_sync_latency_micros_total counter\n\
dash_ingest_wal_flush_sync_latency_micros_total {}\n\
# TYPE dash_ingest_wal_flush_last_synced_records gauge\n\
dash_ingest_wal_flush_last_synced_records {}\n\
# TYPE dash_ingest_wal_flush_last_sync_latency_micros gauge\n\
dash_ingest_wal_flush_last_sync_latency_micros {}\n\
# TYPE dash_ingest_wal_flush_avg_synced_records gauge\n\
dash_ingest_wal_flush_avg_synced_records {:.4}\n\
# TYPE dash_ingest_wal_flush_avg_sync_latency_micros gauge\n\
dash_ingest_wal_flush_avg_sync_latency_micros {:.4}\n\
# TYPE dash_ingest_wal_async_flush_enabled gauge\n\
dash_ingest_wal_async_flush_enabled {}\n\
# TYPE dash_ingest_wal_async_flush_interval_ms gauge\n\
dash_ingest_wal_async_flush_interval_ms {}\n\
# TYPE dash_ingest_wal_async_flush_tick_total counter\n\
dash_ingest_wal_async_flush_tick_total {}\n\
# TYPE dash_ingest_wal_background_flush_only gauge\n\
dash_ingest_wal_background_flush_only {}\n\
# TYPE dash_ingest_wal_poisoned gauge\n\
dash_ingest_wal_poisoned {}\n\
# TYPE dash_ingest_transport_queue_capacity gauge\n\
dash_ingest_transport_queue_capacity {}\n\
# TYPE dash_ingest_transport_queue_depth gauge\n\
dash_ingest_transport_queue_depth {}\n\
# TYPE dash_ingest_transport_queue_full_reject_total counter\n\
dash_ingest_transport_queue_full_reject_total {}\n\
# TYPE dash_ingest_transport_read_error_total counter\n\
dash_ingest_transport_read_error_total{{status_class=\"4xx\"}} {}\n\
dash_ingest_transport_read_error_total{{status_class=\"5xx\"}} {}\n\
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
            self.wal_flush_synced_records_total,
            self.wal_flush_sync_latency_micros_total,
            self.wal_flush_last_synced_records,
            self.wal_flush_last_sync_latency_micros,
            wal_flush_avg_synced_records,
            wal_flush_avg_sync_latency_micros,
            wal_async_flush_enabled,
            wal_async_flush_interval_ms,
            self.wal_async_flush_tick_total,
            wal_background_flush_only,
            wal_poisoned,
            transport_queue_capacity,
            transport_queue_depth,
            transport_queue_full_reject_total,
            read_error_4xx,
            read_error_5xx,
            self.replication_pull_success_total,
            self.replication_pull_failure_total,
            self.replication_applied_records_total,
            self.replication_resync_total,
            self.replication_last_offset,
            self.replication_last_error.is_some() as usize,
            self.store.claims_len(),
            self.started_at.elapsed().as_secs_f64()
        ) + &self.group_commit_metrics_text()
            + &self.wal_write_metrics_text()
            + &self.delete_metrics.render()
    }

    fn group_commit_metrics_text(&self) -> String {
        match self.group_commit.as_ref() {
            Some(pipeline) => pipeline.metrics_text(),
            None => "# TYPE dash_ingest_wal_group_commit_enabled gauge\n\
dash_ingest_wal_group_commit_enabled 0\n"
                .to_string(),
        }
    }

    fn wal_write_metrics_text(&self) -> String {
        format!(
            "# TYPE dash_ingest_wal_write_failure_total counter\n\
dash_ingest_wal_write_failure_total {}\n\
# TYPE dash_ingest_wal_write_recovered_total counter\n\
dash_ingest_wal_write_recovered_total {}\n\
# TYPE dash_ingest_wal_write_failing gauge\n\
dash_ingest_wal_write_failing {}\n",
            self.wal_write_failure_total,
            self.wal_write_recovered_total,
            self.wal_write_error.is_some() as u8
        )
    }
}

/// Size of the scratch file written to decide that a full WAL volume has
/// space again (see [`IngestionRuntime::wal_write_readiness`]).
pub(crate) const WAL_SPACE_PROBE_BYTES: usize = 1024 * 1024;

/// Write `bytes` zeros to `<wal>.space-probe`, sync it and remove it.
pub(crate) fn probe_wal_space(wal_path: &std::path::Path, bytes: usize) -> std::io::Result<()> {
    use std::io::Write;
    let mut probe = wal_path.as_os_str().to_owned();
    probe.push(".space-probe");
    let probe = std::path::PathBuf::from(probe);
    let result = (|| {
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&probe)?;
        let chunk = [0u8; 64 * 1024];
        let mut left = bytes;
        while left > 0 {
            let n = left.min(chunk.len());
            file.write_all(&chunk[..n])?;
            left -= n;
        }
        file.sync_data()
    })();
    let _ = std::fs::remove_file(&probe);
    result
}

pub(crate) type SharedRuntime = Arc<Mutex<IngestionRuntime>>;
const DEFAULT_HTTP_WORKERS: usize = 4;
const DEFAULT_HTTP_QUEUE_CAPACITY_PER_WORKER: usize = 64;
const DEFAULT_ASYNC_WAL_FLUSH_INTERVAL_MS: u64 = 250;
const DEFAULT_SEGMENT_MAINTENANCE_INTERVAL_MS: u64 = 30_000;
const DEFAULT_SEGMENT_GC_MIN_STALE_AGE_MS: u64 = 60_000;
const DEFAULT_INGEST_BATCH_MAX_ITEMS: usize = 128;
const DEFAULT_REPLICATION_PULL_MAX_RECORDS: usize = 512;
const MAX_REPLICATION_PULL_MAX_RECORDS: usize = 10_000;

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
    let shutdown = dash_common::ShutdownSignal::install();
    serve_http_with_workers(runtime, bind_addr, DEFAULT_HTTP_WORKERS, shutdown)
}

pub(crate) fn log_vector_index_save(outcome: Result<Option<VectorIndexSaveStats>, StoreError>) {
    match outcome {
        Ok(Some(stats)) => tracing::info!(
            "ingestion vector index saved: vectors={}, tenants={}, bytes={}, wal_generation={:016x}, wal_records={}, elapsed_ms={}",
            stats.vectors,
            stats.tenants,
            stats.bytes,
            stats.position.generation,
            stats.position.records,
            stats.elapsed.as_millis()
        ),
        Ok(None) => {}
        Err(err) => tracing::warn!("ingestion vector index save failed: {err:?}"),
    }
}

pub fn serve_http_with_workers(
    runtime: IngestionRuntime,
    bind_addr: &str,
    worker_count: usize,
    shutdown: std::sync::Arc<dash_common::ShutdownSignal>,
) -> std::io::Result<()> {
    server_runtime::serve_http_with_workers(runtime, bind_addr, worker_count, shutdown)
}

pub fn handle_http_request_bytes(
    runtime: &Arc<Mutex<IngestionRuntime>>,
    raw_request: &[u8],
) -> Result<Vec<u8>, String> {
    let request = dash_http::parse_request_bytes(raw_request, &server_config(1, 1))
        .map_err(|err| err.message)?;
    let response = handle_request(runtime, &HttpRequest::from(request));
    Ok(render_response_text(&response).into_bytes())
}

pub(crate) fn handle_request(runtime: &SharedRuntime, request: &HttpRequest) -> HttpResponse {
    routes::handle_request(runtime, request)
}

#[cfg(test)]
mod tests;
