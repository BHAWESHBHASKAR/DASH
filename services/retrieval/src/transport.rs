use std::{
    collections::{HashMap, VecDeque},
    io::Write,
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
        mpsc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use metadata_router::{
    PlacementRouteError, ReadPreference, ReplicaHealth, ReplicaRole, RoutedReplica, RouterConfig,
    ShardPlacement, load_shard_placements_csv, route_read_with_placement,
    shard_ids_from_placements,
};
use schema::StanceMode;
use store::InMemoryStore;

#[cfg(test)]
use crate::api::STORAGE_MERGE_MODEL;
#[cfg(test)]
use crate::api::STORAGE_SOURCE_OF_TRUTH_MODEL;
use crate::api::{
    CitationNode, EvidenceNode, RetrieveApiRequest, RetrievePlannerDebugSnapshot,
    RetrieveStorageMergeSnapshot, STORAGE_EXECUTION_MODE_SEGMENT_DISK_BASE,
    STORAGE_PROMOTION_BOUNDARY_REPLAY_ONLY, STORAGE_PROMOTION_BOUNDARY_SEGMENT_FULLY_PROMOTED,
    STORAGE_PROMOTION_BOUNDARY_SEGMENT_PLUS_WAL_DELTA, TimeRange,
    build_retrieve_planner_debug_snapshot, execute_api_query_with_storage_snapshot,
    segment_prefilter_cache_metrics_snapshot,
};
mod audit;
mod authz;
mod debug_render;
mod http;
mod payload;
use audit::{AuditEvent, append_audit_record};
#[cfg(test)]
use audit::{audit_chain_states, is_sha256_hex};
use authz::{AuthDecision, AuthPolicy, authorize_request_for_tenant};
use debug_render::{
    evaluate_storage_divergence_warning, promotion_boundary_state_metric_value,
    render_placement_debug_json, render_planner_debug_json, render_storage_visibility_debug_json,
    resolve_storage_divergence_warn_delta_count, resolve_storage_divergence_warn_ratio,
};
use http::{
    parse_request_line, read_http_request, render_response_text, split_target, write_response,
};
#[cfg(test)]
use payload::build_retrieve_request_from_json;
#[cfg(test)]
use payload::{JsonValue, parse_json};
use payload::{
    build_retrieve_request_from_query, build_retrieve_transport_request_from_json,
    build_retrieve_transport_request_from_query, json_escape, render_retrieve_response_json,
};

const METRICS_WINDOW_SIZE: usize = 2048;
const MAX_HTTP_BODY_BYTES: usize = 16 * 1024 * 1024;
const SOCKET_TIMEOUT_SECS: u64 = 5;
const DEFAULT_HTTP_WORKERS: usize = 4;
const DEFAULT_HTTP_QUEUE_CAPACITY_PER_WORKER: usize = 64;
const DEFAULT_STORAGE_DIVERGENCE_WARN_DELTA_COUNT: usize = 1_000;
const DEFAULT_STORAGE_DIVERGENCE_WARN_RATIO: f64 = 0.25;
type SharedPlacementRouting = Arc<Mutex<Option<PlacementRoutingState>>>;

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

#[derive(Debug, Clone, PartialEq, Eq)]
struct PlacementRoutingRuntime {
    local_node_id: String,
    router_config: RouterConfig,
    placements: Vec<ShardPlacement>,
    read_preference: ReadPreference,
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
    read_preference: ReadPreference,
    reload_interval: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ReadRouteError {
    Placement(PlacementRouteError),
    WrongNode {
        local_node_id: String,
        target_node_id: String,
        shard_id: u32,
        epoch: u64,
        role: ReplicaRole,
    },
    ConsistencyUnavailable {
        policy: ReadConsistencyPolicy,
        shard_id: u32,
        readable_replicas: usize,
        required_replicas: usize,
        total_replicas: usize,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReadConsistencyPolicy {
    One,
    Quorum,
    All,
}

impl ReadConsistencyPolicy {
    fn from_raw(raw: Option<&str>) -> Result<Self, String> {
        match raw.map(|value| value.trim().to_ascii_lowercase()) {
            None => Ok(Self::One),
            Some(value) if value.is_empty() || value == "one" => Ok(Self::One),
            Some(value) if value == "quorum" => Ok(Self::Quorum),
            Some(value) if value == "all" => Ok(Self::All),
            Some(_) => Err("read_consistency must be one, quorum, or all".to_string()),
        }
    }

    fn required_replicas(self, total_replicas: usize) -> usize {
        let total_replicas = total_replicas.max(1);
        match self {
            Self::One => 1,
            Self::Quorum => total_replicas / 2 + 1,
            Self::All => total_replicas,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::One => "one",
            Self::Quorum => "quorum",
            Self::All => "all",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct RetrieveTransportRequest {
    request: RetrieveApiRequest,
    read_consistency: ReadConsistencyPolicy,
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

#[derive(Debug, Clone)]
pub(crate) struct TransportMetrics {
    started_at: Instant,
    http_requests_total: u64,
    health_requests_total: u64,
    metrics_requests_total: u64,
    retrieve_requests_total: u64,
    retrieve_success_total: u64,
    retrieve_client_error_total: u64,
    retrieve_server_error_total: u64,
    auth_success_total: u64,
    auth_failure_total: u64,
    authz_denied_total: u64,
    audit_events_total: u64,
    audit_write_error_total: u64,
    placement_route_reject_total: u64,
    placement_last_shard_id: Option<u32>,
    placement_last_epoch: Option<u64>,
    placement_last_role: Option<ReplicaRole>,
    placement_reload_enabled: bool,
    placement_reload_interval_ms: Option<u64>,
    placement_reload_attempt_total: u64,
    placement_reload_success_total: u64,
    placement_reload_failure_total: u64,
    placement_reload_last_error: bool,
    retrieve_last_result_count: usize,
    retrieve_latency_ms_window: VecDeque<f64>,
    ingest_to_visible_lag_ms_window: VecDeque<f64>,
    storage_last_segment_base_count: usize,
    storage_last_wal_delta_count: usize,
    storage_last_storage_visible_count: usize,
    storage_last_allowed_claim_ids_count: usize,
    storage_last_result_from_segment_base_count: usize,
    storage_last_result_from_wal_delta_count: usize,
    storage_last_result_source_unknown_count: usize,
    storage_last_result_outside_storage_visible_count: usize,
    storage_last_execution_candidate_count: usize,
    storage_last_execution_mode_disk_native: bool,
    storage_last_promotion_boundary_state: usize,
    storage_last_promotion_boundary_in_transition: bool,
    storage_last_divergence_ratio: f64,
    storage_last_divergence_warn: bool,
    storage_divergence_warn_total: u64,
    storage_result_from_segment_base_total: u64,
    storage_result_from_wal_delta_total: u64,
    storage_result_source_unknown_total: u64,
    storage_result_outside_storage_visible_total: u64,
    storage_execution_mode_disk_native_total: u64,
    storage_execution_mode_memory_index_total: u64,
    storage_promotion_boundary_replay_only_total: u64,
    storage_promotion_boundary_segment_plus_wal_delta_total: u64,
    storage_promotion_boundary_segment_fully_promoted_total: u64,
    transport_backpressure: Option<Arc<TransportBackpressureMetrics>>,
}

impl Default for TransportMetrics {
    fn default() -> Self {
        Self {
            started_at: Instant::now(),
            http_requests_total: 0,
            health_requests_total: 0,
            metrics_requests_total: 0,
            retrieve_requests_total: 0,
            retrieve_success_total: 0,
            retrieve_client_error_total: 0,
            retrieve_server_error_total: 0,
            auth_success_total: 0,
            auth_failure_total: 0,
            authz_denied_total: 0,
            audit_events_total: 0,
            audit_write_error_total: 0,
            placement_route_reject_total: 0,
            placement_last_shard_id: None,
            placement_last_epoch: None,
            placement_last_role: None,
            placement_reload_enabled: false,
            placement_reload_interval_ms: None,
            placement_reload_attempt_total: 0,
            placement_reload_success_total: 0,
            placement_reload_failure_total: 0,
            placement_reload_last_error: false,
            retrieve_last_result_count: 0,
            retrieve_latency_ms_window: VecDeque::with_capacity(METRICS_WINDOW_SIZE),
            ingest_to_visible_lag_ms_window: VecDeque::with_capacity(METRICS_WINDOW_SIZE),
            storage_last_segment_base_count: 0,
            storage_last_wal_delta_count: 0,
            storage_last_storage_visible_count: 0,
            storage_last_allowed_claim_ids_count: 0,
            storage_last_result_from_segment_base_count: 0,
            storage_last_result_from_wal_delta_count: 0,
            storage_last_result_source_unknown_count: 0,
            storage_last_result_outside_storage_visible_count: 0,
            storage_last_execution_candidate_count: 0,
            storage_last_execution_mode_disk_native: false,
            storage_last_promotion_boundary_state: 0,
            storage_last_promotion_boundary_in_transition: false,
            storage_last_divergence_ratio: 0.0,
            storage_last_divergence_warn: false,
            storage_divergence_warn_total: 0,
            storage_result_from_segment_base_total: 0,
            storage_result_from_wal_delta_total: 0,
            storage_result_source_unknown_total: 0,
            storage_result_outside_storage_visible_total: 0,
            storage_execution_mode_disk_native_total: 0,
            storage_execution_mode_memory_index_total: 0,
            storage_promotion_boundary_replay_only_total: 0,
            storage_promotion_boundary_segment_plus_wal_delta_total: 0,
            storage_promotion_boundary_segment_fully_promoted_total: 0,
            transport_backpressure: None,
        }
    }
}

impl TransportMetrics {
    pub(crate) fn set_transport_backpressure_metrics(
        &mut self,
        metrics: Arc<TransportBackpressureMetrics>,
    ) {
        self.transport_backpressure = Some(metrics);
    }

    fn push_window(window: &mut VecDeque<f64>, value: f64) {
        if window.len() >= METRICS_WINDOW_SIZE {
            let _ = window.pop_front();
        }
        window.push_back(value);
    }

    fn observe_http(&mut self, path: &str) {
        self.http_requests_total += 1;
        match path {
            "/health" => self.health_requests_total += 1,
            "/metrics" => self.metrics_requests_total += 1,
            _ => {}
        }
    }

    fn observe_retrieve(
        &mut self,
        status: u16,
        latency_ms: f64,
        result_count: usize,
        ingest_to_visible_lag_ms: Option<f64>,
    ) {
        self.retrieve_requests_total += 1;
        self.retrieve_last_result_count = result_count;
        Self::push_window(&mut self.retrieve_latency_ms_window, latency_ms);
        if let Some(value) = ingest_to_visible_lag_ms {
            Self::push_window(&mut self.ingest_to_visible_lag_ms_window, value);
        }

        match status {
            200..=299 => self.retrieve_success_total += 1,
            400..=499 => self.retrieve_client_error_total += 1,
            _ => self.retrieve_server_error_total += 1,
        }
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

    fn observe_audit_event(&mut self, write_error: bool) {
        self.audit_events_total += 1;
        if write_error {
            self.audit_write_error_total += 1;
        }
    }

    fn observe_read_route_resolution(&mut self, routed: &RoutedReplica) {
        self.placement_last_shard_id = Some(routed.shard_id);
        self.placement_last_epoch = Some(routed.epoch);
        self.placement_last_role = Some(routed.role);
    }

    fn observe_read_route_rejection(&mut self, error: &ReadRouteError) {
        self.placement_route_reject_total += 1;
        match error {
            ReadRouteError::WrongNode {
                shard_id,
                epoch,
                role,
                ..
            } => {
                self.placement_last_shard_id = Some(*shard_id);
                self.placement_last_epoch = Some(*epoch);
                self.placement_last_role = Some(*role);
            }
            ReadRouteError::ConsistencyUnavailable { shard_id, .. } => {
                self.placement_last_shard_id = Some(*shard_id);
            }
            ReadRouteError::Placement(_) => {}
        }
    }

    fn observe_placement_reload_snapshot(&mut self, snapshot: &PlacementReloadSnapshot) {
        self.placement_reload_enabled = snapshot.enabled;
        self.placement_reload_interval_ms = snapshot.interval_ms;
        self.placement_reload_attempt_total = snapshot.attempt_total;
        self.placement_reload_success_total = snapshot.success_total;
        self.placement_reload_failure_total = snapshot.failure_total;
        self.placement_reload_last_error = snapshot.last_error.is_some();
    }

    fn observe_storage_visibility_debug(
        &mut self,
        snapshot: &RetrievePlannerDebugSnapshot,
        divergence_ratio: f64,
        divergence_warn: bool,
    ) {
        self.storage_last_segment_base_count = snapshot.segment_base_count;
        self.storage_last_wal_delta_count = snapshot.wal_delta_count;
        self.storage_last_storage_visible_count = snapshot.storage_visible_count;
        self.storage_last_allowed_claim_ids_count = snapshot.allowed_claim_ids_count;
        self.storage_last_divergence_ratio = divergence_ratio;
        self.storage_last_divergence_warn = divergence_warn;
        if divergence_warn {
            self.storage_divergence_warn_total =
                self.storage_divergence_warn_total.saturating_add(1);
        }
    }

    fn observe_storage_merge_execution(&mut self, snapshot: &RetrieveStorageMergeSnapshot) {
        self.storage_last_segment_base_count = snapshot.segment_base_count;
        self.storage_last_wal_delta_count = snapshot.wal_delta_count;
        self.storage_last_storage_visible_count = snapshot.storage_visible_count;
        self.storage_last_allowed_claim_ids_count = snapshot.allowed_claim_ids_count;
        self.storage_last_result_from_segment_base_count = snapshot.result_from_segment_base_count;
        self.storage_last_result_from_wal_delta_count = snapshot.result_from_wal_delta_count;
        self.storage_last_result_source_unknown_count = snapshot.result_source_unknown_count;
        self.storage_last_result_outside_storage_visible_count =
            snapshot.result_outside_storage_visible_count;
        self.storage_last_execution_candidate_count = snapshot.execution_candidate_count;
        self.storage_last_execution_mode_disk_native =
            snapshot.execution_mode == STORAGE_EXECUTION_MODE_SEGMENT_DISK_BASE;
        self.storage_last_promotion_boundary_state =
            promotion_boundary_state_metric_value(&snapshot.promotion_boundary_state);
        self.storage_last_promotion_boundary_in_transition =
            snapshot.promotion_boundary_in_transition;
        self.storage_result_from_segment_base_total = self
            .storage_result_from_segment_base_total
            .saturating_add(snapshot.result_from_segment_base_count as u64);
        self.storage_result_from_wal_delta_total = self
            .storage_result_from_wal_delta_total
            .saturating_add(snapshot.result_from_wal_delta_count as u64);
        self.storage_result_source_unknown_total = self
            .storage_result_source_unknown_total
            .saturating_add(snapshot.result_source_unknown_count as u64);
        self.storage_result_outside_storage_visible_total = self
            .storage_result_outside_storage_visible_total
            .saturating_add(snapshot.result_outside_storage_visible_count as u64);
        if self.storage_last_execution_mode_disk_native {
            self.storage_execution_mode_disk_native_total = self
                .storage_execution_mode_disk_native_total
                .saturating_add(1);
        } else {
            self.storage_execution_mode_memory_index_total = self
                .storage_execution_mode_memory_index_total
                .saturating_add(1);
        }
        match snapshot.promotion_boundary_state.as_str() {
            STORAGE_PROMOTION_BOUNDARY_REPLAY_ONLY => {
                self.storage_promotion_boundary_replay_only_total = self
                    .storage_promotion_boundary_replay_only_total
                    .saturating_add(1);
            }
            STORAGE_PROMOTION_BOUNDARY_SEGMENT_PLUS_WAL_DELTA => {
                self.storage_promotion_boundary_segment_plus_wal_delta_total = self
                    .storage_promotion_boundary_segment_plus_wal_delta_total
                    .saturating_add(1);
            }
            STORAGE_PROMOTION_BOUNDARY_SEGMENT_FULLY_PROMOTED => {
                self.storage_promotion_boundary_segment_fully_promoted_total = self
                    .storage_promotion_boundary_segment_fully_promoted_total
                    .saturating_add(1);
            }
            _ => {}
        }
    }

    fn quantile(window: &VecDeque<f64>, quantile: f64) -> f64 {
        if window.is_empty() {
            return 0.0;
        }
        let mut values: Vec<f64> = window.iter().copied().collect();
        values.sort_by(|a, b| a.total_cmp(b));
        let idx = (((values.len() - 1) as f64) * quantile).round() as usize;
        values[idx]
    }

    fn render_prometheus(&self, placement_routing: Option<&PlacementRoutingRuntime>) -> String {
        let retrieve_latency_p50 = Self::quantile(&self.retrieve_latency_ms_window, 0.50);
        let retrieve_latency_p95 = Self::quantile(&self.retrieve_latency_ms_window, 0.95);
        let retrieve_latency_p99 = Self::quantile(&self.retrieve_latency_ms_window, 0.99);
        let visibility_lag_p50 = Self::quantile(&self.ingest_to_visible_lag_ms_window, 0.50);
        let visibility_lag_p95 = Self::quantile(&self.ingest_to_visible_lag_ms_window, 0.95);
        let segment_cache_metrics = segment_prefilter_cache_metrics_snapshot();
        let uptime_seconds = self.started_at.elapsed().as_secs_f64();
        let placement_enabled = placement_routing.map(|_| 1).unwrap_or(0);
        let placement_snapshot = placement_routing
            .map(PlacementRoutingRuntime::observability_snapshot)
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
            "# TYPE dash_http_requests_total counter\n\
dash_http_requests_total {}\n\
# TYPE dash_health_requests_total counter\n\
dash_health_requests_total {}\n\
# TYPE dash_metrics_requests_total counter\n\
dash_metrics_requests_total {}\n\
# TYPE dash_retrieve_requests_total counter\n\
dash_retrieve_requests_total {}\n\
# TYPE dash_retrieve_success_total counter\n\
dash_retrieve_success_total {}\n\
# TYPE dash_retrieve_client_error_total counter\n\
dash_retrieve_client_error_total {}\n\
# TYPE dash_retrieve_server_error_total counter\n\
dash_retrieve_server_error_total {}\n\
# TYPE dash_transport_auth_success_total counter\n\
dash_transport_auth_success_total {}\n\
# TYPE dash_transport_auth_failure_total counter\n\
dash_transport_auth_failure_total {}\n\
# TYPE dash_transport_authz_denied_total counter\n\
dash_transport_authz_denied_total {}\n\
# TYPE dash_transport_audit_events_total counter\n\
dash_transport_audit_events_total {}\n\
# TYPE dash_transport_audit_write_error_total counter\n\
dash_transport_audit_write_error_total {}\n\
# TYPE dash_retrieve_transport_queue_capacity gauge\n\
dash_retrieve_transport_queue_capacity {}\n\
# TYPE dash_retrieve_transport_queue_depth gauge\n\
dash_retrieve_transport_queue_depth {}\n\
# TYPE dash_retrieve_transport_queue_full_reject_total counter\n\
dash_retrieve_transport_queue_full_reject_total {}\n\
# TYPE dash_retrieve_placement_enabled gauge\n\
dash_retrieve_placement_enabled {}\n\
# TYPE dash_retrieve_placement_route_reject_total counter\n\
dash_retrieve_placement_route_reject_total {}\n\
# TYPE dash_retrieve_placement_last_shard_id gauge\n\
dash_retrieve_placement_last_shard_id {}\n\
# TYPE dash_retrieve_placement_last_epoch gauge\n\
dash_retrieve_placement_last_epoch {}\n\
# TYPE dash_retrieve_placement_last_role gauge\n\
dash_retrieve_placement_last_role {}\n\
# TYPE dash_retrieve_placement_leaders_total gauge\n\
dash_retrieve_placement_leaders_total {}\n\
# TYPE dash_retrieve_placement_followers_total gauge\n\
dash_retrieve_placement_followers_total {}\n\
# TYPE dash_retrieve_placement_replicas_healthy gauge\n\
dash_retrieve_placement_replicas_healthy {}\n\
# TYPE dash_retrieve_placement_replicas_degraded gauge\n\
dash_retrieve_placement_replicas_degraded {}\n\
# TYPE dash_retrieve_placement_replicas_unavailable gauge\n\
dash_retrieve_placement_replicas_unavailable {}\n\
# TYPE dash_retrieve_placement_reload_enabled gauge\n\
dash_retrieve_placement_reload_enabled {}\n\
# TYPE dash_retrieve_placement_reload_interval_ms gauge\n\
dash_retrieve_placement_reload_interval_ms {}\n\
# TYPE dash_retrieve_placement_reload_attempt_total counter\n\
dash_retrieve_placement_reload_attempt_total {}\n\
# TYPE dash_retrieve_placement_reload_success_total counter\n\
dash_retrieve_placement_reload_success_total {}\n\
# TYPE dash_retrieve_placement_reload_failure_total counter\n\
dash_retrieve_placement_reload_failure_total {}\n\
# TYPE dash_retrieve_placement_reload_last_error gauge\n\
dash_retrieve_placement_reload_last_error {}\n\
# TYPE dash_retrieve_last_result_count gauge\n\
dash_retrieve_last_result_count {}\n\
# TYPE dash_retrieve_latency_ms_p50 gauge\n\
dash_retrieve_latency_ms_p50 {:.4}\n\
# TYPE dash_retrieve_latency_ms_p95 gauge\n\
dash_retrieve_latency_ms_p95 {:.4}\n\
# TYPE dash_retrieve_latency_ms_p99 gauge\n\
dash_retrieve_latency_ms_p99 {:.4}\n\
# TYPE dash_ingest_to_visible_lag_ms_p50 gauge\n\
dash_ingest_to_visible_lag_ms_p50 {:.4}\n\
# TYPE dash_ingest_to_visible_lag_ms_p95 gauge\n\
dash_ingest_to_visible_lag_ms_p95 {:.4}\n\
# TYPE dash_retrieve_storage_last_segment_base_count gauge\n\
dash_retrieve_storage_last_segment_base_count {}\n\
# TYPE dash_retrieve_storage_last_wal_delta_count gauge\n\
dash_retrieve_storage_last_wal_delta_count {}\n\
# TYPE dash_retrieve_storage_last_storage_visible_count gauge\n\
dash_retrieve_storage_last_storage_visible_count {}\n\
# TYPE dash_retrieve_storage_last_allowed_claim_ids_count gauge\n\
dash_retrieve_storage_last_allowed_claim_ids_count {}\n\
# TYPE dash_retrieve_storage_last_result_from_segment_base_count gauge\n\
dash_retrieve_storage_last_result_from_segment_base_count {}\n\
# TYPE dash_retrieve_storage_last_result_from_wal_delta_count gauge\n\
dash_retrieve_storage_last_result_from_wal_delta_count {}\n\
# TYPE dash_retrieve_storage_last_result_source_unknown_count gauge\n\
dash_retrieve_storage_last_result_source_unknown_count {}\n\
# TYPE dash_retrieve_storage_last_result_outside_storage_visible_count gauge\n\
dash_retrieve_storage_last_result_outside_storage_visible_count {}\n\
# TYPE dash_retrieve_storage_last_execution_candidate_count gauge\n\
dash_retrieve_storage_last_execution_candidate_count {}\n\
# TYPE dash_retrieve_storage_last_execution_mode_disk_native gauge\n\
dash_retrieve_storage_last_execution_mode_disk_native {}\n\
# TYPE dash_retrieve_storage_last_promotion_boundary_state gauge\n\
dash_retrieve_storage_last_promotion_boundary_state {}\n\
# TYPE dash_retrieve_storage_last_promotion_boundary_in_transition gauge\n\
dash_retrieve_storage_last_promotion_boundary_in_transition {}\n\
# TYPE dash_retrieve_storage_last_divergence_ratio gauge\n\
dash_retrieve_storage_last_divergence_ratio {:.6}\n\
# TYPE dash_retrieve_storage_last_divergence_warn gauge\n\
dash_retrieve_storage_last_divergence_warn {}\n\
# TYPE dash_retrieve_storage_divergence_warn_total counter\n\
dash_retrieve_storage_divergence_warn_total {}\n\
# TYPE dash_retrieve_storage_result_from_segment_base_total counter\n\
dash_retrieve_storage_result_from_segment_base_total {}\n\
# TYPE dash_retrieve_storage_result_from_wal_delta_total counter\n\
dash_retrieve_storage_result_from_wal_delta_total {}\n\
# TYPE dash_retrieve_storage_result_source_unknown_total counter\n\
dash_retrieve_storage_result_source_unknown_total {}\n\
# TYPE dash_retrieve_storage_result_outside_storage_visible_total counter\n\
dash_retrieve_storage_result_outside_storage_visible_total {}\n\
# TYPE dash_retrieve_storage_execution_mode_disk_native_total counter\n\
dash_retrieve_storage_execution_mode_disk_native_total {}\n\
# TYPE dash_retrieve_storage_execution_mode_memory_index_total counter\n\
dash_retrieve_storage_execution_mode_memory_index_total {}\n\
# TYPE dash_retrieve_storage_promotion_boundary_replay_only_total counter\n\
dash_retrieve_storage_promotion_boundary_replay_only_total {}\n\
# TYPE dash_retrieve_storage_promotion_boundary_segment_plus_wal_delta_total counter\n\
dash_retrieve_storage_promotion_boundary_segment_plus_wal_delta_total {}\n\
# TYPE dash_retrieve_storage_promotion_boundary_segment_fully_promoted_total counter\n\
dash_retrieve_storage_promotion_boundary_segment_fully_promoted_total {}\n\
# TYPE dash_retrieve_segment_cache_hits_total counter\n\
dash_retrieve_segment_cache_hits_total {}\n\
# TYPE dash_retrieve_segment_refresh_attempt_total counter\n\
dash_retrieve_segment_refresh_attempt_total {}\n\
# TYPE dash_retrieve_segment_refresh_success_total counter\n\
dash_retrieve_segment_refresh_success_total {}\n\
# TYPE dash_retrieve_segment_refresh_failure_total counter\n\
dash_retrieve_segment_refresh_failure_total {}\n\
# TYPE dash_retrieve_segment_refresh_load_micros_total counter\n\
dash_retrieve_segment_refresh_load_micros_total {}\n\
# TYPE dash_retrieve_segment_fallback_activation_total counter\n\
dash_retrieve_segment_fallback_activation_total {}\n\
# TYPE dash_retrieve_segment_fallback_missing_manifest_total counter\n\
dash_retrieve_segment_fallback_missing_manifest_total {}\n\
# TYPE dash_retrieve_segment_fallback_manifest_error_total counter\n\
dash_retrieve_segment_fallback_manifest_error_total {}\n\
# TYPE dash_retrieve_segment_fallback_segment_error_total counter\n\
dash_retrieve_segment_fallback_segment_error_total {}\n\
# TYPE dash_transport_uptime_seconds gauge\n\
dash_transport_uptime_seconds {:.4}\n",
            self.http_requests_total,
            self.health_requests_total,
            self.metrics_requests_total,
            self.retrieve_requests_total,
            self.retrieve_success_total,
            self.retrieve_client_error_total,
            self.retrieve_server_error_total,
            self.auth_success_total,
            self.auth_failure_total,
            self.authz_denied_total,
            self.audit_events_total,
            self.audit_write_error_total,
            transport_queue_capacity,
            transport_queue_depth,
            transport_queue_full_reject_total,
            placement_enabled,
            self.placement_route_reject_total,
            placement_last_shard_id,
            placement_last_epoch,
            placement_last_role,
            placement_snapshot.leaders_total,
            placement_snapshot.followers_total,
            placement_snapshot.replicas_healthy,
            placement_snapshot.replicas_degraded,
            placement_snapshot.replicas_unavailable,
            self.placement_reload_enabled as usize,
            self.placement_reload_interval_ms.unwrap_or(0),
            self.placement_reload_attempt_total,
            self.placement_reload_success_total,
            self.placement_reload_failure_total,
            self.placement_reload_last_error as usize,
            self.retrieve_last_result_count,
            retrieve_latency_p50,
            retrieve_latency_p95,
            retrieve_latency_p99,
            visibility_lag_p50,
            visibility_lag_p95,
            self.storage_last_segment_base_count,
            self.storage_last_wal_delta_count,
            self.storage_last_storage_visible_count,
            self.storage_last_allowed_claim_ids_count,
            self.storage_last_result_from_segment_base_count,
            self.storage_last_result_from_wal_delta_count,
            self.storage_last_result_source_unknown_count,
            self.storage_last_result_outside_storage_visible_count,
            self.storage_last_execution_candidate_count,
            self.storage_last_execution_mode_disk_native as usize,
            self.storage_last_promotion_boundary_state,
            self.storage_last_promotion_boundary_in_transition as usize,
            self.storage_last_divergence_ratio,
            self.storage_last_divergence_warn as usize,
            self.storage_divergence_warn_total,
            self.storage_result_from_segment_base_total,
            self.storage_result_from_wal_delta_total,
            self.storage_result_source_unknown_total,
            self.storage_result_outside_storage_visible_total,
            self.storage_execution_mode_disk_native_total,
            self.storage_execution_mode_memory_index_total,
            self.storage_promotion_boundary_replay_only_total,
            self.storage_promotion_boundary_segment_plus_wal_delta_total,
            self.storage_promotion_boundary_segment_fully_promoted_total,
            segment_cache_metrics.cache_hits,
            segment_cache_metrics.refresh_attempts,
            segment_cache_metrics.refresh_successes,
            segment_cache_metrics.refresh_failures,
            segment_cache_metrics.refresh_load_micros,
            segment_cache_metrics.fallback_activations,
            segment_cache_metrics.fallback_missing_manifest,
            segment_cache_metrics.fallback_manifest_errors,
            segment_cache_metrics.fallback_segment_errors,
            uptime_seconds
        )
    }
}

pub(crate) fn resolve_http_queue_capacity(worker_count: usize) -> usize {
    let default_capacity = worker_count
        .saturating_mul(DEFAULT_HTTP_QUEUE_CAPACITY_PER_WORKER)
        .max(worker_count);
    parse_env_first_usize(&[
        "DASH_RETRIEVAL_HTTP_QUEUE_CAPACITY",
        "EME_RETRIEVAL_HTTP_QUEUE_CAPACITY",
    ])
    .filter(|value| *value > 0)
    .unwrap_or(default_capacity)
}

const BACKPRESSURE_QUEUE_FULL_MESSAGE: &str = "service unavailable: retrieval worker queue full";

pub(crate) fn backpressure_rejection_response() -> HttpResponse {
    HttpResponse::service_unavailable(BACKPRESSURE_QUEUE_FULL_MESSAGE)
}

fn write_backpressure_response(mut stream: TcpStream) -> std::io::Result<()> {
    stream.set_write_timeout(Some(Duration::from_secs(SOCKET_TIMEOUT_SECS)))?;
    let response = backpressure_rejection_response();
    let response = format!(
        "HTTP/1.1 503 Service Unavailable\r\ncontent-type: {}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
        response.content_type,
        response.body.len(),
        response.body
    );
    stream.write_all(response.as_bytes())
}

pub fn serve_http(store: &InMemoryStore, bind_addr: &str) -> std::io::Result<()> {
    serve_http_with_workers(store, bind_addr, DEFAULT_HTTP_WORKERS)
}

pub fn serve_http_with_workers(
    store: &InMemoryStore,
    bind_addr: &str,
    worker_count: usize,
) -> std::io::Result<()> {
    let listener = TcpListener::bind(bind_addr)?;
    let worker_count = worker_count.max(1);
    let metrics = Arc::new(Mutex::new(TransportMetrics::default()));
    let queue_capacity = resolve_http_queue_capacity(worker_count);
    let backpressure_metrics = Arc::new(TransportBackpressureMetrics::new(queue_capacity));
    if let Ok(mut guard) = metrics.lock() {
        guard.set_transport_backpressure_metrics(Arc::clone(&backpressure_metrics));
    }
    let placement_routing = Arc::new(Mutex::new(PlacementRoutingState::from_env().map_err(
        |reason| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("invalid placement routing configuration: {reason}"),
            )
        },
    )?));
    let (tx, rx) = mpsc::sync_channel::<TcpStream>(queue_capacity);
    let rx = Arc::new(Mutex::new(rx));

    std::thread::scope(|scope| {
        for _ in 0..worker_count {
            let metrics = Arc::clone(&metrics);
            let rx = Arc::clone(&rx);
            let placement_routing = Arc::clone(&placement_routing);
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
                    if let Err(err) = handle_connection(store, stream, &metrics, &placement_routing)
                    {
                        eprintln!("retrieval transport error: {err}");
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
                            if let Err(err) = write_backpressure_response(stream) {
                                eprintln!(
                                    "retrieval transport backpressure response failed: {err}"
                                );
                            }
                        }
                        Err(mpsc::TrySendError::Disconnected(_)) => {
                            backpressure_metrics.observe_dequeued();
                            eprintln!("retrieval transport worker queue closed");
                            break;
                        }
                    }
                }
                Err(err) => eprintln!("retrieval transport accept error: {err}"),
            }
        }
        drop(tx);
    });

    Ok(())
}

pub fn serve_http_once(store: &InMemoryStore, bind_addr: &str) -> std::io::Result<()> {
    let listener = TcpListener::bind(bind_addr)?;
    serve_http_once_with_listener(store, listener)
}

pub fn serve_http_once_with_listener(
    store: &InMemoryStore,
    listener: TcpListener,
) -> std::io::Result<()> {
    let metrics = Arc::new(Mutex::new(TransportMetrics::default()));
    let placement_routing = Arc::new(Mutex::new(PlacementRoutingState::from_env().map_err(
        |reason| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("invalid placement routing configuration: {reason}"),
            )
        },
    )?));
    let (stream, _) = listener.accept()?;
    handle_connection(store, stream, &metrics, &placement_routing)
}

pub fn handle_http_request_bytes(
    store: &InMemoryStore,
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
    let metrics = Arc::new(Mutex::new(TransportMetrics::default()));
    let mut placement_routing = PlacementRoutingState::from_env()?;
    let (routing_snapshot, reload_snapshot) = if let Some(state) = placement_routing.as_mut() {
        state.maybe_refresh();
        (Some(state.runtime().clone()), state.reload_snapshot())
    } else {
        (None, PlacementReloadSnapshot::default())
    };
    if let Ok(mut guard) = metrics.lock() {
        guard.observe_placement_reload_snapshot(&reload_snapshot);
    }
    let response = handle_request_with_metrics_and_reload(
        store,
        &request,
        &metrics,
        routing_snapshot.as_ref(),
        Some(&reload_snapshot),
    );
    Ok(render_response_text(&response).into_bytes())
}

fn handle_connection(
    store: &InMemoryStore,
    mut stream: TcpStream,
    metrics: &Arc<Mutex<TransportMetrics>>,
    placement_routing: &SharedPlacementRouting,
) -> std::io::Result<()> {
    stream.set_read_timeout(Some(Duration::from_secs(SOCKET_TIMEOUT_SECS)))?;
    stream.set_write_timeout(Some(Duration::from_secs(SOCKET_TIMEOUT_SECS)))?;

    let request = match read_http_request(&mut stream) {
        Ok(Some(request)) => request,
        Ok(None) => return Ok(()),
        Err(err) => {
            return write_response(&mut stream, HttpResponse::bad_request(&err));
        }
    };

    let (routing_snapshot, reload_snapshot) = match placement_routing.lock() {
        Ok(mut guard) => {
            if let Some(state) = guard.as_mut() {
                state.maybe_refresh();
                (Some(state.runtime().clone()), state.reload_snapshot())
            } else {
                (None, PlacementReloadSnapshot::default())
            }
        }
        Err(_) => {
            return write_response(
                &mut stream,
                HttpResponse::internal_server_error(
                    "failed to acquire retrieval placement routing lock",
                ),
            );
        }
    };
    if let Ok(mut guard) = metrics.lock() {
        guard.observe_placement_reload_snapshot(&reload_snapshot);
    }
    let response = handle_request_with_metrics_and_reload(
        store,
        &request,
        metrics,
        routing_snapshot.as_ref(),
        Some(&reload_snapshot),
    );
    write_response(&mut stream, response)
}

#[cfg(test)]
fn handle_request(store: &InMemoryStore, request: &HttpRequest) -> HttpResponse {
    let metrics = Arc::new(Mutex::new(TransportMetrics::default()));
    handle_request_with_metrics(store, request, &metrics)
}

#[cfg(any(test, feature = "async-transport"))]
pub(crate) fn handle_request_with_metrics(
    store: &InMemoryStore,
    request: &HttpRequest,
    metrics: &Arc<Mutex<TransportMetrics>>,
) -> HttpResponse {
    let mut placement_routing = match PlacementRoutingState::from_env() {
        Ok(runtime) => runtime,
        Err(reason) => {
            return HttpResponse::internal_server_error(&format!(
                "placement routing configuration error: {reason}"
            ));
        }
    };
    let (routing_snapshot, reload_snapshot) = if let Some(state) = placement_routing.as_mut() {
        state.maybe_refresh();
        (Some(state.runtime().clone()), state.reload_snapshot())
    } else {
        (None, PlacementReloadSnapshot::default())
    };
    if let Ok(mut guard) = metrics.lock() {
        guard.observe_placement_reload_snapshot(&reload_snapshot);
    }
    handle_request_with_metrics_and_reload(
        store,
        request,
        metrics,
        routing_snapshot.as_ref(),
        Some(&reload_snapshot),
    )
}

#[cfg(test)]
fn handle_request_with_metrics_and_routing(
    store: &InMemoryStore,
    request: &HttpRequest,
    metrics: &Arc<Mutex<TransportMetrics>>,
    placement_routing: Option<&PlacementRoutingRuntime>,
) -> HttpResponse {
    handle_request_with_metrics_and_reload(store, request, metrics, placement_routing, None)
}

fn handle_request_with_metrics_and_reload(
    store: &InMemoryStore,
    request: &HttpRequest,
    metrics: &Arc<Mutex<TransportMetrics>>,
    placement_routing: Option<&PlacementRoutingRuntime>,
    placement_reload: Option<&PlacementReloadSnapshot>,
) -> HttpResponse {
    let (path, query) = split_target(&request.target);
    let auth_policy = AuthPolicy::from_env(
        env_with_fallback("DASH_RETRIEVAL_API_KEY", "EME_RETRIEVAL_API_KEY"),
        env_with_fallback("DASH_RETRIEVAL_API_KEYS", "EME_RETRIEVAL_API_KEYS"),
        env_with_fallback(
            "DASH_RETRIEVAL_REVOKED_API_KEYS",
            "EME_RETRIEVAL_REVOKED_API_KEYS",
        ),
        env_with_fallback(
            "DASH_RETRIEVAL_ALLOWED_TENANTS",
            "EME_RETRIEVAL_ALLOWED_TENANTS",
        ),
        env_with_fallback(
            "DASH_RETRIEVAL_API_KEY_SCOPES",
            "EME_RETRIEVAL_API_KEY_SCOPES",
        ),
    );
    let audit_log_path = env_with_fallback(
        "DASH_RETRIEVAL_AUDIT_LOG_PATH",
        "EME_RETRIEVAL_AUDIT_LOG_PATH",
    );
    if let Ok(mut guard) = metrics.lock() {
        guard.observe_http(&path);
    }

    match (request.method.as_str(), path.as_str()) {
        ("GET", "/health") => HttpResponse::ok_json("{\"status\":\"ok\"}".to_string()),
        ("GET", "/metrics") => {
            let body = if let Ok(guard) = metrics.lock() {
                guard.render_prometheus(placement_routing)
            } else {
                "dash_transport_metrics_unavailable 1\n".to_string()
            };
            HttpResponse::ok_text(body)
        }
        ("GET", "/debug/placement") => HttpResponse::ok_json(render_placement_debug_json(
            placement_routing,
            placement_reload,
            &query,
        )),
        ("GET", "/debug/planner") => match build_retrieve_request_from_query(&query) {
            Ok(req) => {
                let tenant_id = req.tenant_id.clone();
                match authorize_request_for_tenant(request, &tenant_id, &auth_policy) {
                    AuthDecision::Unauthorized(reason) => {
                        observe_auth_failure(metrics);
                        emit_audit_event(
                            metrics,
                            audit_log_path.as_deref(),
                            "debug_planner",
                            Some(&tenant_id),
                            401,
                            "denied",
                            reason,
                        );
                        HttpResponse::unauthorized(reason)
                    }
                    AuthDecision::Forbidden(reason) => {
                        observe_authz_denied(metrics);
                        emit_audit_event(
                            metrics,
                            audit_log_path.as_deref(),
                            "debug_planner",
                            Some(&tenant_id),
                            403,
                            "denied",
                            reason,
                        );
                        HttpResponse::forbidden(reason)
                    }
                    AuthDecision::Allowed => {
                        observe_auth_success(metrics);
                        let snapshot = build_retrieve_planner_debug_snapshot(store, &req);
                        emit_audit_event(
                            metrics,
                            audit_log_path.as_deref(),
                            "debug_planner",
                            Some(&tenant_id),
                            200,
                            "success",
                            "planner debug snapshot generated",
                        );
                        HttpResponse::ok_json(render_planner_debug_json(&snapshot))
                    }
                }
            }
            Err(err) => HttpResponse::bad_request(&err),
        },
        ("GET", "/debug/storage-visibility") => match build_retrieve_request_from_query(&query) {
            Ok(req) => {
                let tenant_id = req.tenant_id.clone();
                match authorize_request_for_tenant(request, &tenant_id, &auth_policy) {
                    AuthDecision::Unauthorized(reason) => {
                        observe_auth_failure(metrics);
                        emit_audit_event(
                            metrics,
                            audit_log_path.as_deref(),
                            "debug_storage_visibility",
                            Some(&tenant_id),
                            401,
                            "denied",
                            reason,
                        );
                        HttpResponse::unauthorized(reason)
                    }
                    AuthDecision::Forbidden(reason) => {
                        observe_authz_denied(metrics);
                        emit_audit_event(
                            metrics,
                            audit_log_path.as_deref(),
                            "debug_storage_visibility",
                            Some(&tenant_id),
                            403,
                            "denied",
                            reason,
                        );
                        HttpResponse::forbidden(reason)
                    }
                    AuthDecision::Allowed => {
                        observe_auth_success(metrics);
                        let snapshot = build_retrieve_planner_debug_snapshot(store, &req);
                        let (_, merge_snapshot) =
                            execute_api_query_with_storage_snapshot(store, req.clone());
                        let warn_delta_count = resolve_storage_divergence_warn_delta_count();
                        let warn_ratio = resolve_storage_divergence_warn_ratio();
                        let (warn, reason, ratio) = evaluate_storage_divergence_warning(
                            &snapshot,
                            warn_delta_count,
                            warn_ratio,
                        );
                        if let Ok(mut guard) = metrics.lock() {
                            guard.observe_storage_visibility_debug(&snapshot, ratio, warn);
                            guard.observe_storage_merge_execution(&merge_snapshot);
                        }
                        emit_audit_event(
                            metrics,
                            audit_log_path.as_deref(),
                            "debug_storage_visibility",
                            Some(&tenant_id),
                            200,
                            if warn { "warning" } else { "success" },
                            reason
                                .as_deref()
                                .unwrap_or("storage visibility snapshot generated"),
                        );
                        HttpResponse::ok_json(render_storage_visibility_debug_json(
                            &snapshot,
                            &merge_snapshot,
                            warn_delta_count,
                            warn_ratio,
                            warn,
                            reason.as_deref(),
                            ratio,
                        ))
                    }
                }
            }
            Err(err) => HttpResponse::bad_request(&err),
        },
        ("GET", "/v1/retrieve") => match build_retrieve_transport_request_from_query(&query) {
            Ok(transport_req) => {
                let req = transport_req.request;
                let tenant_id = req.tenant_id.clone();
                match authorize_request_for_tenant(request, &tenant_id, &auth_policy) {
                    AuthDecision::Unauthorized(reason) => {
                        observe_auth_failure(metrics);
                        if let Ok(mut guard) = metrics.lock() {
                            guard.observe_retrieve(401, 0.0, 0, None);
                        }
                        emit_audit_event(
                            metrics,
                            audit_log_path.as_deref(),
                            "retrieve",
                            Some(&tenant_id),
                            401,
                            "denied",
                            reason,
                        );
                        HttpResponse::unauthorized(reason)
                    }
                    AuthDecision::Forbidden(reason) => {
                        observe_authz_denied(metrics);
                        if let Ok(mut guard) = metrics.lock() {
                            guard.observe_retrieve(403, 0.0, 0, None);
                        }
                        emit_audit_event(
                            metrics,
                            audit_log_path.as_deref(),
                            "retrieve",
                            Some(&tenant_id),
                            403,
                            "denied",
                            reason,
                        );
                        HttpResponse::forbidden(reason)
                    }
                    AuthDecision::Allowed => {
                        observe_auth_success(metrics);
                        let response = execute_retrieve_and_observe(
                            store,
                            req,
                            transport_req.read_consistency,
                            metrics,
                            placement_routing,
                        );
                        let (outcome, reason) = if response.status < 400 {
                            ("success", "retrieve accepted")
                        } else {
                            ("error", "retrieve rejected")
                        };
                        emit_audit_event(
                            metrics,
                            audit_log_path.as_deref(),
                            "retrieve",
                            Some(&tenant_id),
                            response.status,
                            outcome,
                            reason,
                        );
                        response
                    }
                }
            }
            Err(err) => {
                if let Ok(mut guard) = metrics.lock() {
                    guard.observe_retrieve(400, 0.0, 0, None);
                }
                HttpResponse::bad_request(&err)
            }
        },
        ("POST", "/v1/retrieve") => {
            if let Some(content_type) = request.headers.get("content-type")
                && !content_type
                    .to_ascii_lowercase()
                    .contains("application/json")
            {
                if let Ok(mut guard) = metrics.lock() {
                    guard.observe_retrieve(400, 0.0, 0, None);
                }
                return HttpResponse::bad_request(
                    "content-type must include application/json for POST /v1/retrieve",
                );
            }

            let body = match std::str::from_utf8(&request.body) {
                Ok(body) => body,
                Err(_) => {
                    if let Ok(mut guard) = metrics.lock() {
                        guard.observe_retrieve(400, 0.0, 0, None);
                    }
                    return HttpResponse::bad_request("request body must be valid UTF-8");
                }
            };
            match build_retrieve_transport_request_from_json(body) {
                Ok(transport_req) => {
                    let req = transport_req.request;
                    let tenant_id = req.tenant_id.clone();
                    match authorize_request_for_tenant(request, &tenant_id, &auth_policy) {
                        AuthDecision::Unauthorized(reason) => {
                            observe_auth_failure(metrics);
                            if let Ok(mut guard) = metrics.lock() {
                                guard.observe_retrieve(401, 0.0, 0, None);
                            }
                            emit_audit_event(
                                metrics,
                                audit_log_path.as_deref(),
                                "retrieve",
                                Some(&tenant_id),
                                401,
                                "denied",
                                reason,
                            );
                            HttpResponse::unauthorized(reason)
                        }
                        AuthDecision::Forbidden(reason) => {
                            observe_authz_denied(metrics);
                            if let Ok(mut guard) = metrics.lock() {
                                guard.observe_retrieve(403, 0.0, 0, None);
                            }
                            emit_audit_event(
                                metrics,
                                audit_log_path.as_deref(),
                                "retrieve",
                                Some(&tenant_id),
                                403,
                                "denied",
                                reason,
                            );
                            HttpResponse::forbidden(reason)
                        }
                        AuthDecision::Allowed => {
                            observe_auth_success(metrics);
                            let response = execute_retrieve_and_observe(
                                store,
                                req,
                                transport_req.read_consistency,
                                metrics,
                                placement_routing,
                            );
                            let (outcome, reason) = if response.status < 400 {
                                ("success", "retrieve accepted")
                            } else {
                                ("error", "retrieve rejected")
                            };
                            emit_audit_event(
                                metrics,
                                audit_log_path.as_deref(),
                                "retrieve",
                                Some(&tenant_id),
                                response.status,
                                outcome,
                                reason,
                            );
                            response
                        }
                    }
                }
                Err(err) => {
                    if let Ok(mut guard) = metrics.lock() {
                        guard.observe_retrieve(400, 0.0, 0, None);
                    }
                    HttpResponse::bad_request(&err)
                }
            }
        }
        (_, "/v1/retrieve") => HttpResponse::method_not_allowed("only GET and POST are supported"),
        (_, "/health")
        | (_, "/metrics")
        | (_, "/debug/placement")
        | (_, "/debug/planner")
        | (_, "/debug/storage-visibility") => {
            HttpResponse::method_not_allowed("only GET is supported")
        }
        _ => HttpResponse::not_found("unknown path"),
    }
}

fn env_with_fallback(primary: &str, fallback: &str) -> Option<String> {
    std::env::var(primary)
        .ok()
        .or_else(|| std::env::var(fallback).ok())
}

fn observe_auth_success(metrics: &Arc<Mutex<TransportMetrics>>) {
    if let Ok(mut guard) = metrics.lock() {
        guard.observe_auth_success();
    }
}

fn observe_auth_failure(metrics: &Arc<Mutex<TransportMetrics>>) {
    if let Ok(mut guard) = metrics.lock() {
        guard.observe_auth_failure();
    }
}

fn observe_authz_denied(metrics: &Arc<Mutex<TransportMetrics>>) {
    if let Ok(mut guard) = metrics.lock() {
        guard.observe_authz_denied();
    }
}

fn emit_audit_event(
    metrics: &Arc<Mutex<TransportMetrics>>,
    audit_log_path: Option<&str>,
    action: &str,
    tenant_id: Option<&str>,
    status: u16,
    outcome: &str,
    reason: &str,
) {
    let timestamp_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or_default();

    let mut write_error = false;
    if let Some(path) = audit_log_path
        && let Err(err) = append_audit_record(
            path,
            timestamp_ms,
            AuditEvent {
                action,
                tenant_id,
                status,
                outcome,
                reason,
            },
        )
    {
        write_error = true;
        eprintln!("retrieval audit write failed: {err}");
    }

    if let Ok(mut guard) = metrics.lock() {
        guard.observe_audit_event(write_error);
    }
}

fn execute_retrieve_and_observe(
    store: &InMemoryStore,
    req: RetrieveApiRequest,
    read_consistency: ReadConsistencyPolicy,
    metrics: &Arc<Mutex<TransportMetrics>>,
    placement_routing: Option<&PlacementRoutingRuntime>,
) -> HttpResponse {
    if let Some(routing) = placement_routing {
        match ensure_local_read_route(routing, &req, read_consistency) {
            Ok(routed) => {
                if let Ok(mut guard) = metrics.lock() {
                    guard.observe_read_route_resolution(&routed);
                }
            }
            Err(route_error) => {
                let (status, message) = map_read_route_error(&route_error);
                if let Ok(mut guard) = metrics.lock() {
                    guard.observe_retrieve(status, 0.0, 0, None);
                    guard.observe_read_route_rejection(&route_error);
                }
                return HttpResponse::error_with_status(status, &message);
            }
        }
    }

    let started_at = Instant::now();
    let tenant_id = req.tenant_id.clone();
    let (response, merge_snapshot) = execute_api_query_with_storage_snapshot(store, req);
    let latency_ms = started_at.elapsed().as_secs_f64() * 1000.0;
    let result_count = response.results.len();
    let ingest_to_visible_lag_ms =
        estimate_ingest_to_visible_lag_ms(store, &tenant_id, &response.results);

    if let Ok(mut guard) = metrics.lock() {
        guard.observe_retrieve(200, latency_ms, result_count, ingest_to_visible_lag_ms);
        guard.observe_storage_merge_execution(&merge_snapshot);
    }

    HttpResponse::ok_json(render_retrieve_response_json(&response))
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
        let read_preference =
            parse_read_preference_env("DASH_ROUTER_READ_PREFERENCE", "EME_ROUTER_READ_PREFERENCE")?;
        let runtime = load_placement_routing_runtime(
            Path::new(&placement_file),
            local_node_id,
            shard_ids_override.as_deref(),
            replica_count_override,
            virtual_nodes_per_shard,
            read_preference,
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
                    read_preference,
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

    fn runtime(&self) -> &PlacementRoutingRuntime {
        &self.runtime
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
            reload.config.read_preference,
        ) {
            Ok(runtime) => {
                self.runtime = runtime;
                reload.success_total = reload.success_total.saturating_add(1);
                reload.last_error = None;
            }
            Err(reason) => {
                reload.failure_total = reload.failure_total.saturating_add(1);
                reload.last_error = Some(reason.clone());
                eprintln!("retrieval placement reload failed: {reason}");
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
    read_preference: ReadPreference,
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
        read_preference,
    })
}

fn parse_read_preference_env(primary: &str, fallback: &str) -> Result<ReadPreference, String> {
    let Some(raw) = env_with_fallback(primary, fallback) else {
        return Ok(ReadPreference::AnyHealthy);
    };
    match raw.trim().to_ascii_lowercase().as_str() {
        "" | "any_healthy" => Ok(ReadPreference::AnyHealthy),
        "leader_only" => Ok(ReadPreference::LeaderOnly),
        "prefer_follower" => Ok(ReadPreference::PreferFollower),
        _ => Err(format!(
            "{primary} must be one of: any_healthy, leader_only, prefer_follower"
        )),
    }
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

fn parse_env_first_f64(keys: &[&str]) -> Option<f64> {
    for key in keys {
        if let Ok(value) = std::env::var(key)
            && let Ok(parsed) = value.parse::<f64>()
        {
            return Some(parsed);
        }
    }
    None
}

fn read_entity_key_for_request(req: &RetrieveApiRequest) -> &str {
    req.entity_filters
        .iter()
        .find(|value| !value.trim().is_empty())
        .map(String::as_str)
        .unwrap_or(req.query.as_str())
}

fn ensure_local_read_route(
    routing: &PlacementRoutingRuntime,
    req: &RetrieveApiRequest,
    read_consistency: ReadConsistencyPolicy,
) -> Result<RoutedReplica, ReadRouteError> {
    let entity_key = read_entity_key_for_request(req);
    let routed = route_read_with_placement(
        &req.tenant_id,
        entity_key,
        &routing.router_config,
        &routing.placements,
        routing.read_preference,
    )
    .map_err(ReadRouteError::Placement)?;
    if routed.node_id != routing.local_node_id {
        return Err(ReadRouteError::WrongNode {
            local_node_id: routing.local_node_id.clone(),
            target_node_id: routed.node_id,
            shard_id: routed.shard_id,
            epoch: routed.epoch,
            role: routed.role,
        });
    }

    if let Some(placement) = routing.placements.iter().find(|placement| {
        placement.tenant_id == req.tenant_id && placement.shard_id == routed.shard_id
    }) {
        let total_replicas = placement.replicas.len();
        let readable_replicas = placement
            .replicas
            .iter()
            .filter(|replica| {
                matches!(
                    replica.health,
                    ReplicaHealth::Healthy | ReplicaHealth::Degraded
                )
            })
            .count();
        let required_replicas = read_consistency.required_replicas(total_replicas);
        if readable_replicas < required_replicas {
            return Err(ReadRouteError::ConsistencyUnavailable {
                policy: read_consistency,
                shard_id: routed.shard_id,
                readable_replicas,
                required_replicas,
                total_replicas,
            });
        }
    }
    Ok(routed)
}

fn map_read_route_error(error: &ReadRouteError) -> (u16, String) {
    match error {
        ReadRouteError::Placement(reason) => (
            503,
            format!("placement route rejected read request: {reason:?}"),
        ),
        ReadRouteError::WrongNode {
            local_node_id,
            target_node_id,
            shard_id,
            epoch,
            role: _,
        } => (
            503,
            format!(
                "placement route rejected read request: local node '{local_node_id}' is not selected for shard {shard_id} at epoch {epoch} (target replica: '{target_node_id}')"
            ),
        ),
        ReadRouteError::ConsistencyUnavailable {
            policy,
            shard_id,
            readable_replicas,
            required_replicas,
            total_replicas,
        } => (
            503,
            format!(
                "placement route rejected read request: read_consistency={} requires {required_replicas} readable replicas for shard {shard_id}, but observed {readable_replicas}/{total_replicas}",
                policy.as_str()
            ),
        ),
    }
}

fn estimate_ingest_to_visible_lag_ms(
    store: &InMemoryStore,
    tenant_id: &str,
    results: &[EvidenceNode],
) -> Option<f64> {
    if results.is_empty() {
        return None;
    }
    let now_unix = SystemTime::now().duration_since(UNIX_EPOCH).ok()?.as_secs() as i64;
    let claims = store.claims_for_tenant(tenant_id);
    let event_time_by_claim_id: HashMap<String, i64> = claims
        .into_iter()
        .filter_map(|claim| claim.event_time_unix.map(|ts| (claim.claim_id, ts)))
        .collect();

    let mut lag_values: Vec<f64> = Vec::new();
    for node in results {
        if let Some(event_unix) = event_time_by_claim_id.get(&node.claim_id)
            && now_unix >= *event_unix
        {
            lag_values.push((now_unix - *event_unix) as f64 * 1000.0);
        }
    }
    if lag_values.is_empty() {
        return None;
    }
    Some(lag_values.iter().sum::<f64>() / lag_values.len() as f64)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HttpRequest {
    pub(crate) method: String,
    pub(crate) target: String,
    pub(crate) headers: HashMap<String, String>,
    pub(crate) body: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HttpResponse {
    pub(crate) status: u16,
    pub(crate) content_type: &'static str,
    pub(crate) body: String,
}

impl HttpResponse {
    fn ok_json(body: String) -> Self {
        Self {
            status: 200,
            content_type: "application/json",
            body,
        }
    }

    fn ok_text(body: String) -> Self {
        Self {
            status: 200,
            content_type: "text/plain; version=0.0.4; charset=utf-8",
            body,
        }
    }

    fn bad_request(message: &str) -> Self {
        Self {
            status: 400,
            content_type: "application/json",
            body: format!("{{\"error\":\"{}\"}}", json_escape(message)),
        }
    }

    fn unauthorized(message: &str) -> Self {
        Self {
            status: 401,
            content_type: "application/json",
            body: format!("{{\"error\":\"{}\"}}", json_escape(message)),
        }
    }

    fn forbidden(message: &str) -> Self {
        Self {
            status: 403,
            content_type: "application/json",
            body: format!("{{\"error\":\"{}\"}}", json_escape(message)),
        }
    }

    fn method_not_allowed(message: &str) -> Self {
        Self {
            status: 405,
            content_type: "application/json",
            body: format!("{{\"error\":\"{}\"}}", json_escape(message)),
        }
    }

    fn not_found(message: &str) -> Self {
        Self {
            status: 404,
            content_type: "application/json",
            body: format!("{{\"error\":\"{}\"}}", json_escape(message)),
        }
    }

    fn internal_server_error(message: &str) -> Self {
        Self {
            status: 500,
            content_type: "application/json",
            body: format!("{{\"error\":\"{}\"}}", json_escape(message)),
        }
    }

    fn service_unavailable(message: &str) -> Self {
        Self {
            status: 503,
            content_type: "application/json",
            body: format!("{{\"error\":\"{}\"}}", json_escape(message)),
        }
    }

    fn error_with_status(status: u16, message: &str) -> Self {
        match status {
            400 => Self::bad_request(message),
            401 => Self::unauthorized(message),
            403 => Self::forbidden(message),
            404 => Self::not_found(message),
            405 => Self::method_not_allowed(message),
            503 => Self::service_unavailable(message),
            _ => Self::internal_server_error(message),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use indexer::{Segment, Tier, persist_segments_atomic};
    use metadata_router::{
        ReplicaHealth, ReplicaPlacement, ReplicaRole, promote_replica_to_leader,
    };
    use schema::{Claim, Evidence, Stance};
    use std::{
        ffi::OsStr,
        fs::{File, OpenOptions},
        io::Write,
        path::PathBuf,
        sync::{Mutex, OnceLock},
        thread,
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    fn sample_store() -> InMemoryStore {
        let mut store = InMemoryStore::new();
        store
            .ingest_bundle(
                Claim {
                    claim_id: "c1".into(),
                    tenant_id: "tenant-a".into(),
                    canonical_text: "Company X acquired Company Y".into(),
                    confidence: 0.9,
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
                    evidence_id: "e1".into(),
                    claim_id: "c1".into(),
                    source_id: "source://doc-1".into(),
                    stance: Stance::Supports,
                    source_quality: 0.9,
                    chunk_id: None,
                    span_start: None,
                    span_end: None,
                    doc_id: None,
                    extraction_model: None,
                    ingested_at: None,
                }],
                vec![],
            )
            .unwrap();
        store
    }

    fn temp_placement_csv(contents: &str) -> PathBuf {
        let mut out = std::env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock should be monotonic")
            .as_nanos();
        out.push(format!(
            "dash-retrieve-placement-runtime-test-{}-{}.csv",
            std::process::id(),
            nanos
        ));
        let mut file = File::create(&out).expect("placement file should be created");
        file.write_all(contents.as_bytes())
            .expect("placement file should be writable");
        out
    }

    fn overwrite_placement_csv(path: &PathBuf, contents: &str) {
        let mut file = OpenOptions::new()
            .truncate(true)
            .write(true)
            .open(path)
            .expect("placement file should be writable");
        file.write_all(contents.as_bytes())
            .expect("placement file should be writable");
        file.flush().expect("placement file should flush");
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
    fn restore_env_var_for_tests(key: &str, value: Option<&OsStr>) {
        unsafe {
            if let Some(value) = value {
                std::env::set_var(key, value);
            } else {
                std::env::remove_var(key);
            }
        }
    }

    #[test]
    fn build_retrieve_request_from_query_accepts_balanced_defaults() {
        let mut params = HashMap::new();
        params.insert("tenant_id".into(), "tenant-a".into());
        params.insert("query".into(), "company x".into());

        let req = build_retrieve_request_from_query(&params).unwrap();
        assert_eq!(req.top_k, 5);
        assert_eq!(req.stance_mode, StanceMode::Balanced);
        assert!(!req.return_graph);
        assert!(req.query_embedding.is_none());
        assert!(req.entity_filters.is_empty());
        assert!(req.embedding_id_filters.is_empty());
    }

    #[test]
    fn build_retrieve_request_from_json_accepts_contract_payload() {
        let body = r#"{
            "tenant_id": "tenant-a",
            "query": "company x",
            "query_embedding": [0.1, 0.2, 0.3],
            "entity_filters": ["company x"],
            "embedding_id_filters": ["emb://1"],
            "top_k": 3,
            "stance_mode": "support_only",
            "return_graph": true,
            "time_range": {"from_unix": 10, "to_unix": 20}
        }"#;

        let req = build_retrieve_request_from_json(body).unwrap();
        assert_eq!(req.top_k, 3);
        assert_eq!(req.stance_mode, StanceMode::SupportOnly);
        assert!(req.return_graph);
        assert_eq!(req.time_range.unwrap().from_unix, Some(10));
        assert_eq!(req.query_embedding, Some(vec![0.1, 0.2, 0.3]));
        assert_eq!(req.entity_filters, vec!["company x"]);
        assert_eq!(req.embedding_id_filters, vec!["emb://1"]);
    }

    #[test]
    fn build_retrieve_request_from_query_accepts_embedding_and_filters() {
        let mut params = HashMap::new();
        params.insert("tenant_id".into(), "tenant-a".into());
        params.insert("query".into(), "company x".into());
        params.insert("query_embedding".into(), "0.1,0.2,0.3".into());
        params.insert("entity_filters".into(), "company x,company y".into());
        params.insert("embedding_id_filters".into(), "emb://1,emb://2".into());

        let req = build_retrieve_request_from_query(&params).unwrap();
        assert_eq!(req.query_embedding, Some(vec![0.1, 0.2, 0.3]));
        assert_eq!(req.entity_filters, vec!["company x", "company y"]);
        assert_eq!(req.embedding_id_filters, vec!["emb://1", "emb://2"]);
    }

    #[test]
    fn build_retrieve_request_from_query_rejects_invalid_time_range() {
        let mut params = HashMap::new();
        params.insert("tenant_id".into(), "tenant-a".into());
        params.insert("query".into(), "company x".into());
        params.insert("from_unix".into(), "20".into());
        params.insert("to_unix".into(), "10".into());

        let err = build_retrieve_request_from_query(&params).unwrap_err();
        assert!(err.contains("from_unix must be <= to_unix"));
    }

    #[test]
    fn build_retrieve_transport_request_from_query_accepts_read_consistency() {
        let mut params = HashMap::new();
        params.insert("tenant_id".into(), "tenant-a".into());
        params.insert("query".into(), "company x".into());
        params.insert("read_consistency".into(), "quorum".into());

        let req = build_retrieve_transport_request_from_query(&params).unwrap();
        assert_eq!(req.read_consistency, ReadConsistencyPolicy::Quorum);
    }

    #[test]
    fn build_retrieve_transport_request_from_json_accepts_read_consistency() {
        let body = r#"{
            "tenant_id": "tenant-a",
            "query": "company x",
            "read_consistency": "all"
        }"#;

        let req = build_retrieve_transport_request_from_json(body).unwrap();
        assert_eq!(req.read_consistency, ReadConsistencyPolicy::All);
    }

    #[test]
    fn build_retrieve_transport_request_rejects_invalid_read_consistency() {
        let mut params = HashMap::new();
        params.insert("tenant_id".into(), "tenant-a".into());
        params.insert("query".into(), "company x".into());
        params.insert("read_consistency".into(), "strong".into());

        let err = build_retrieve_transport_request_from_query(&params).unwrap_err();
        assert!(err.contains("read_consistency must be one, quorum, or all"));
    }

    #[test]
    fn split_target_decodes_query_parameters() {
        let (path, query) =
            split_target("/v1/retrieve?tenant_id=tenant-a&query=company+x&return_graph=true");
        assert_eq!(path, "/v1/retrieve");
        assert_eq!(query.get("query").map(String::as_str), Some("company x"));
        assert_eq!(query.get("return_graph").map(String::as_str), Some("true"));
    }

    #[test]
    fn handle_request_get_returns_json_payload() {
        let store = sample_store();
        let request = HttpRequest {
            method: "GET".to_string(),
            target: "/v1/retrieve?tenant_id=tenant-a&query=company+x&top_k=1".to_string(),
            headers: HashMap::new(),
            body: Vec::new(),
        };

        let response = handle_request(&store, &request);
        assert_eq!(response.status, 200);
        assert!(response.body.contains("\"results\""));
        assert!(response.body.contains("\"claim_id\":\"c1\""));
        assert!(response.body.contains("\"evidence_id\":\"e1\""));
        assert!(response.body.contains("\"stance\":\"supports\""));
    }

    #[test]
    fn handle_request_post_returns_json_payload() {
        let store = sample_store();
        let request = HttpRequest {
            method: "POST".to_string(),
            target: "/v1/retrieve".to_string(),
            headers: HashMap::from([("content-type".to_string(), "application/json".to_string())]),
            body: br#"{"tenant_id":"tenant-a","query":"company x","top_k":1}"#.to_vec(),
        };

        let response = handle_request(&store, &request);
        assert_eq!(response.status, 200);
        assert!(response.body.contains("\"results\""));
        assert!(response.body.contains("\"claim_id\":\"c1\""));
        assert!(response.body.contains("\"evidence_id\":\"e1\""));
    }

    #[test]
    fn handle_request_rejects_when_local_node_is_not_selected_replica() {
        let store = sample_store();
        let metrics = Arc::new(Mutex::new(TransportMetrics::default()));
        let routing = PlacementRoutingRuntime {
            local_node_id: "node-b".to_string(),
            router_config: RouterConfig {
                shard_ids: vec![0],
                virtual_nodes_per_shard: 16,
                replica_count: 2,
            },
            placements: vec![ShardPlacement {
                tenant_id: "tenant-a".to_string(),
                shard_id: 0,
                epoch: 11,
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
            }],
            read_preference: ReadPreference::LeaderOnly,
        };
        let request = HttpRequest {
            method: "GET".to_string(),
            target: "/v1/retrieve?tenant_id=tenant-a&query=company+x&top_k=1".to_string(),
            headers: HashMap::new(),
            body: Vec::new(),
        };
        let response =
            handle_request_with_metrics_and_routing(&store, &request, &metrics, Some(&routing));
        assert_eq!(response.status, 503);
        assert!(response.body.contains("local node 'node-b'"));

        let metrics_request = HttpRequest {
            method: "GET".to_string(),
            target: "/metrics".to_string(),
            headers: HashMap::new(),
            body: Vec::new(),
        };
        let metrics_response = handle_request_with_metrics_and_routing(
            &store,
            &metrics_request,
            &metrics,
            Some(&routing),
        );
        assert!(
            metrics_response
                .body
                .contains("dash_retrieve_server_error_total 1")
        );
        assert!(
            metrics_response
                .body
                .contains("dash_retrieve_placement_route_reject_total 1")
        );
        assert!(
            metrics_response
                .body
                .contains("dash_retrieve_placement_last_epoch 11")
        );
    }

    #[test]
    fn handle_request_rejects_when_quorum_read_consistency_is_unavailable() {
        let store = sample_store();
        let metrics = Arc::new(Mutex::new(TransportMetrics::default()));
        let routing = PlacementRoutingRuntime {
            local_node_id: "node-a".to_string(),
            router_config: RouterConfig {
                shard_ids: vec![0],
                virtual_nodes_per_shard: 16,
                replica_count: 3,
            },
            placements: vec![ShardPlacement {
                tenant_id: "tenant-a".to_string(),
                shard_id: 0,
                epoch: 11,
                replicas: vec![
                    ReplicaPlacement {
                        node_id: "node-a".to_string(),
                        role: ReplicaRole::Leader,
                        health: ReplicaHealth::Healthy,
                    },
                    ReplicaPlacement {
                        node_id: "node-b".to_string(),
                        role: ReplicaRole::Follower,
                        health: ReplicaHealth::Unavailable,
                    },
                    ReplicaPlacement {
                        node_id: "node-c".to_string(),
                        role: ReplicaRole::Follower,
                        health: ReplicaHealth::Unavailable,
                    },
                ],
            }],
            read_preference: ReadPreference::LeaderOnly,
        };
        let request = HttpRequest {
            method: "GET".to_string(),
            target:
                "/v1/retrieve?tenant_id=tenant-a&query=company+x&top_k=1&read_consistency=quorum"
                    .to_string(),
            headers: HashMap::new(),
            body: Vec::new(),
        };

        let response =
            handle_request_with_metrics_and_routing(&store, &request, &metrics, Some(&routing));
        assert_eq!(response.status, 503);
        assert!(response.body.contains("read_consistency=quorum"));
    }

    #[test]
    fn handle_request_rejects_when_all_read_consistency_is_unavailable() {
        let store = sample_store();
        let metrics = Arc::new(Mutex::new(TransportMetrics::default()));
        let routing = PlacementRoutingRuntime {
            local_node_id: "node-a".to_string(),
            router_config: RouterConfig {
                shard_ids: vec![0],
                virtual_nodes_per_shard: 16,
                replica_count: 2,
            },
            placements: vec![ShardPlacement {
                tenant_id: "tenant-a".to_string(),
                shard_id: 0,
                epoch: 11,
                replicas: vec![
                    ReplicaPlacement {
                        node_id: "node-a".to_string(),
                        role: ReplicaRole::Leader,
                        health: ReplicaHealth::Healthy,
                    },
                    ReplicaPlacement {
                        node_id: "node-b".to_string(),
                        role: ReplicaRole::Follower,
                        health: ReplicaHealth::Unavailable,
                    },
                ],
            }],
            read_preference: ReadPreference::LeaderOnly,
        };
        let request = HttpRequest {
            method: "GET".to_string(),
            target: "/v1/retrieve?tenant_id=tenant-a&query=company+x&top_k=1&read_consistency=all"
                .to_string(),
            headers: HashMap::new(),
            body: Vec::new(),
        };

        let response =
            handle_request_with_metrics_and_routing(&store, &request, &metrics, Some(&routing));
        assert_eq!(response.status, 503);
        assert!(response.body.contains("read_consistency=all"));
    }

    #[test]
    fn handle_request_accepts_when_one_read_consistency_is_met() {
        let store = sample_store();
        let metrics = Arc::new(Mutex::new(TransportMetrics::default()));
        let routing = PlacementRoutingRuntime {
            local_node_id: "node-a".to_string(),
            router_config: RouterConfig {
                shard_ids: vec![0],
                virtual_nodes_per_shard: 16,
                replica_count: 2,
            },
            placements: vec![ShardPlacement {
                tenant_id: "tenant-a".to_string(),
                shard_id: 0,
                epoch: 11,
                replicas: vec![
                    ReplicaPlacement {
                        node_id: "node-a".to_string(),
                        role: ReplicaRole::Leader,
                        health: ReplicaHealth::Healthy,
                    },
                    ReplicaPlacement {
                        node_id: "node-b".to_string(),
                        role: ReplicaRole::Follower,
                        health: ReplicaHealth::Unavailable,
                    },
                ],
            }],
            read_preference: ReadPreference::LeaderOnly,
        };
        let request = HttpRequest {
            method: "GET".to_string(),
            target: "/v1/retrieve?tenant_id=tenant-a&query=company+x&top_k=1&read_consistency=one"
                .to_string(),
            headers: HashMap::new(),
            body: Vec::new(),
        };

        let response =
            handle_request_with_metrics_and_routing(&store, &request, &metrics, Some(&routing));
        assert_eq!(response.status, 200);
        assert!(response.body.contains("\"results\""));
    }

    #[test]
    fn handle_request_read_route_reresolves_after_leader_promotion() {
        let store = sample_store();
        let metrics = Arc::new(Mutex::new(TransportMetrics::default()));
        let mut routing = PlacementRoutingRuntime {
            local_node_id: "node-b".to_string(),
            router_config: RouterConfig {
                shard_ids: vec![0],
                virtual_nodes_per_shard: 16,
                replica_count: 2,
            },
            placements: vec![ShardPlacement {
                tenant_id: "tenant-a".to_string(),
                shard_id: 0,
                epoch: 11,
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
            }],
            read_preference: ReadPreference::LeaderOnly,
        };
        let retrieve_request = HttpRequest {
            method: "GET".to_string(),
            target: "/v1/retrieve?tenant_id=tenant-a&query=company+x&top_k=1".to_string(),
            headers: HashMap::new(),
            body: Vec::new(),
        };
        let denied_response = handle_request_with_metrics_and_routing(
            &store,
            &retrieve_request,
            &metrics,
            Some(&routing),
        );
        assert_eq!(denied_response.status, 503);

        let new_epoch = promote_replica_to_leader(&mut routing.placements[0], "node-b")
            .expect("promotion should succeed");
        assert_eq!(new_epoch, 12);

        let accepted_response = handle_request_with_metrics_and_routing(
            &store,
            &retrieve_request,
            &metrics,
            Some(&routing),
        );
        assert_eq!(accepted_response.status, 200);

        let metrics_request = HttpRequest {
            method: "GET".to_string(),
            target: "/metrics".to_string(),
            headers: HashMap::new(),
            body: Vec::new(),
        };
        let metrics_response = handle_request_with_metrics_and_routing(
            &store,
            &metrics_request,
            &metrics,
            Some(&routing),
        );
        assert!(
            metrics_response
                .body
                .contains("dash_retrieve_placement_enabled 1")
        );
        assert!(
            metrics_response
                .body
                .contains("dash_retrieve_placement_route_reject_total 1")
        );
        assert!(
            metrics_response
                .body
                .contains("dash_retrieve_placement_last_epoch 12")
        );
        assert!(
            metrics_response
                .body
                .contains("dash_retrieve_placement_last_role 1")
        );
        assert!(
            metrics_response
                .body
                .contains("dash_retrieve_placement_replicas_healthy 2")
        );
    }

    #[test]
    fn placement_state_reloads_from_file_without_restart() {
        let placement_file = temp_placement_csv(
            "tenant-a,0,11,node-a,leader,healthy\n\
tenant-a,0,11,node-b,follower,healthy\n",
        );
        let mut state = PlacementRoutingState {
            runtime: load_placement_routing_runtime(
                &placement_file,
                "node-b",
                Some(&[0]),
                Some(2),
                16,
                ReadPreference::LeaderOnly,
            )
            .expect("initial placement should load"),
            reload: Some(PlacementReloadRuntime {
                config: PlacementReloadConfig {
                    placement_file: placement_file.clone(),
                    shard_ids_override: Some(vec![0]),
                    replica_count_override: Some(2),
                    virtual_nodes_per_shard: 16,
                    read_preference: ReadPreference::LeaderOnly,
                    reload_interval: Duration::from_millis(1),
                },
                next_reload_at: Instant::now(),
                attempt_total: 0,
                success_total: 0,
                failure_total: 0,
                last_error: None,
            }),
        };

        let before = route_read_with_placement(
            "tenant-a",
            "company-x",
            &state.runtime().router_config,
            &state.runtime().placements,
            state.runtime().read_preference,
        )
        .expect("route should resolve");
        assert_eq!(before.node_id, "node-a");
        assert_eq!(before.epoch, 11);

        overwrite_placement_csv(
            &placement_file,
            "tenant-a,0,12,node-b,leader,healthy\n\
tenant-a,0,12,node-a,follower,healthy\n",
        );
        thread::sleep(Duration::from_millis(5));
        state.maybe_refresh();

        let after = route_read_with_placement(
            "tenant-a",
            "company-x",
            &state.runtime().router_config,
            &state.runtime().placements,
            state.runtime().read_preference,
        )
        .expect("route should resolve");
        assert_eq!(after.node_id, "node-b");
        assert_eq!(after.epoch, 12);

        let reload = state.reload_snapshot();
        assert_eq!(reload.attempt_total, 1);
        assert_eq!(reload.success_total, 1);
        assert_eq!(reload.failure_total, 0);
        assert!(reload.last_error.is_none());

        let _ = std::fs::remove_file(placement_file);
    }

    #[test]
    fn debug_placement_endpoint_returns_structured_route_probe() {
        let store = sample_store();
        let metrics = Arc::new(Mutex::new(TransportMetrics::default()));
        let routing = PlacementRoutingRuntime {
            local_node_id: "node-b".to_string(),
            router_config: RouterConfig {
                shard_ids: vec![0],
                virtual_nodes_per_shard: 16,
                replica_count: 2,
            },
            placements: vec![ShardPlacement {
                tenant_id: "tenant-a".to_string(),
                shard_id: 0,
                epoch: 11,
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
            }],
            read_preference: ReadPreference::LeaderOnly,
        };
        let request = HttpRequest {
            method: "GET".to_string(),
            target: "/debug/placement?tenant_id=tenant-a&entity_key=company-x".to_string(),
            headers: HashMap::new(),
            body: Vec::new(),
        };
        let response =
            handle_request_with_metrics_and_routing(&store, &request, &metrics, Some(&routing));
        assert_eq!(response.status, 200);
        assert!(response.body.contains("\"enabled\":true"));
        assert!(response.body.contains("\"placements\""));
        assert!(response.body.contains("\"route_probe\""));
        assert!(response.body.contains("\"status\":\"rejected\""));
        assert!(
            response
                .body
                .contains("\"read_preference\":\"leader_only\"")
        );
        assert!(response.body.contains("\"target_node_id\":\"node-a\""));
    }

    #[test]
    fn debug_planner_endpoint_returns_stage_counts() {
        let mut store = sample_store();
        store
            .upsert_claim_vector("c1", vec![0.8, 0.2, 0.1, 0.9])
            .expect("vector upsert should succeed");
        let request = HttpRequest {
            method: "GET".to_string(),
            target: "/debug/planner?tenant_id=tenant-a&query=company+x&query_embedding=0.8,0.2,0.1,0.9&top_k=3".to_string(),
            headers: HashMap::new(),
            body: Vec::new(),
        };

        let response = handle_request(&store, &request);
        assert_eq!(response.status, 200);
        assert!(response.body.contains("\"tenant_id\":\"tenant-a\""));
        assert!(response.body.contains("\"ann_candidate_count\":1"));
        assert!(response.body.contains("\"planner_candidate_count\":1"));
        assert!(response.body.contains("\"short_circuit_empty\":false"));
    }

    #[test]
    fn debug_planner_endpoint_rejects_invalid_query_shape() {
        let store = sample_store();
        let request = HttpRequest {
            method: "GET".to_string(),
            target: "/debug/planner?tenant_id=tenant-a".to_string(),
            headers: HashMap::new(),
            body: Vec::new(),
        };

        let response = handle_request(&store, &request);
        assert_eq!(response.status, 400);
        assert!(response.body.contains("query is required"));
    }

    #[test]
    fn debug_storage_visibility_endpoint_reports_divergence_and_updates_metrics() {
        let _guard = env_lock().lock().expect("env lock should be available");
        let prev_segment_dir = std::env::var_os("DASH_RETRIEVAL_SEGMENT_DIR");
        let prev_warn_delta =
            std::env::var_os("DASH_RETRIEVAL_STORAGE_DIVERGENCE_WARN_DELTA_COUNT");
        let prev_warn_ratio = std::env::var_os("DASH_RETRIEVAL_STORAGE_DIVERGENCE_WARN_RATIO");
        let prev_refresh_ms = std::env::var_os("DASH_RETRIEVAL_SEGMENT_CACHE_REFRESH_MS");

        let mut segment_root = std::env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock should be monotonic")
            .as_nanos();
        segment_root.push(format!(
            "dash-retrieve-storage-visibility-test-{}-{}",
            std::process::id(),
            nanos
        ));
        let tenant_dir = segment_root.join("tenant-a");
        persist_segments_atomic(
            &tenant_dir,
            &[Segment {
                segment_id: "hot-0".to_string(),
                tier: Tier::Hot,
                claim_ids: vec!["c1".to_string()],
            }],
        )
        .expect("segment persist should succeed");

        set_env_var_for_tests(
            "DASH_RETRIEVAL_SEGMENT_DIR",
            segment_root.to_string_lossy().as_ref(),
        );
        set_env_var_for_tests("DASH_RETRIEVAL_SEGMENT_CACHE_REFRESH_MS", "1");
        set_env_var_for_tests("DASH_RETRIEVAL_STORAGE_DIVERGENCE_WARN_DELTA_COUNT", "1");
        set_env_var_for_tests("DASH_RETRIEVAL_STORAGE_DIVERGENCE_WARN_RATIO", "0.20");

        let mut store = sample_store();
        store
            .ingest_bundle(
                Claim {
                    claim_id: "c2".into(),
                    tenant_id: "tenant-a".into(),
                    canonical_text: "Company X expanded acquisition program".into(),
                    confidence: 0.88,
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
                    evidence_id: "e2".into(),
                    claim_id: "c2".into(),
                    source_id: "source://doc-2".into(),
                    stance: Stance::Supports,
                    source_quality: 0.86,
                    chunk_id: None,
                    span_start: None,
                    span_end: None,
                    doc_id: None,
                    extraction_model: None,
                    ingested_at: None,
                }],
                vec![],
            )
            .expect("ingest c2 should succeed");

        let metrics = Arc::new(Mutex::new(TransportMetrics::default()));
        let debug_request = HttpRequest {
            method: "GET".to_string(),
            target: "/debug/storage-visibility?tenant_id=tenant-a&query=company+x&top_k=2"
                .to_string(),
            headers: HashMap::new(),
            body: Vec::new(),
        };
        let debug_response = handle_request_with_metrics(&store, &debug_request, &metrics);
        assert_eq!(debug_response.status, 200);
        assert!(debug_response.body.contains("\"tenant_id\":\"tenant-a\""));
        assert!(debug_response.body.contains(&format!(
            "\"storage_merge_model\":\"{}\"",
            STORAGE_MERGE_MODEL
        )));
        assert!(debug_response.body.contains(&format!(
            "\"source_of_truth_model\":\"{}\"",
            STORAGE_SOURCE_OF_TRUTH_MODEL
        )));
        assert!(debug_response.body.contains(&format!(
            "\"execution_mode\":\"{}\"",
            STORAGE_EXECUTION_MODE_SEGMENT_DISK_BASE
        )));
        assert!(
            debug_response
                .body
                .contains("\"disk_native_segment_execution_active\":true")
        );
        assert!(
            debug_response
                .body
                .contains("\"execution_candidate_count\":2")
        );
        assert!(debug_response.body.contains(&format!(
            "\"promotion_boundary_state\":\"{}\"",
            STORAGE_PROMOTION_BOUNDARY_SEGMENT_PLUS_WAL_DELTA
        )));
        assert!(
            debug_response
                .body
                .contains("\"promotion_boundary_in_transition\":true")
        );
        assert!(debug_response.body.contains("\"segment_base_count\":1"));
        assert!(debug_response.body.contains("\"wal_delta_count\":1"));
        assert!(debug_response.body.contains("\"storage_visible_count\":2"));
        assert!(debug_response.body.contains("\"result_count\":2"));
        assert!(
            debug_response
                .body
                .contains("\"result_from_segment_base_count\":1")
        );
        assert!(
            debug_response
                .body
                .contains("\"result_from_wal_delta_count\":1")
        );
        assert!(
            debug_response
                .body
                .contains("\"result_source_unknown_count\":0")
        );
        assert!(
            debug_response
                .body
                .contains("\"result_outside_storage_visible_count\":0")
        );
        assert!(debug_response.body.contains("\"divergence_warn\":true"));

        let metrics_request = HttpRequest {
            method: "GET".to_string(),
            target: "/metrics".to_string(),
            headers: HashMap::new(),
            body: Vec::new(),
        };
        let metrics_response = handle_request_with_metrics(&store, &metrics_request, &metrics);
        assert_eq!(metrics_response.status, 200);
        assert!(
            metrics_response
                .body
                .contains("dash_retrieve_storage_last_wal_delta_count 1")
        );
        assert!(
            metrics_response
                .body
                .contains("dash_retrieve_storage_last_result_from_segment_base_count 1")
        );
        assert!(
            metrics_response
                .body
                .contains("dash_retrieve_storage_last_result_from_wal_delta_count 1")
        );
        assert!(
            metrics_response
                .body
                .contains("dash_retrieve_storage_result_from_segment_base_total 1")
        );
        assert!(
            metrics_response
                .body
                .contains("dash_retrieve_storage_result_from_wal_delta_total 1")
        );
        assert!(
            metrics_response
                .body
                .contains("dash_retrieve_storage_last_divergence_warn 1")
        );
        assert!(
            metrics_response
                .body
                .contains("dash_retrieve_storage_divergence_warn_total 1")
        );
        assert!(
            metrics_response
                .body
                .contains("dash_retrieve_storage_last_execution_candidate_count 2")
        );
        assert!(
            metrics_response
                .body
                .contains("dash_retrieve_storage_last_execution_mode_disk_native 1")
        );
        assert!(
            metrics_response
                .body
                .contains("dash_retrieve_storage_execution_mode_disk_native_total 1")
        );
        assert!(
            metrics_response
                .body
                .contains("dash_retrieve_storage_execution_mode_memory_index_total 0")
        );
        assert!(
            metrics_response
                .body
                .contains("dash_retrieve_storage_last_promotion_boundary_state 1")
        );
        assert!(
            metrics_response
                .body
                .contains("dash_retrieve_storage_last_promotion_boundary_in_transition 1")
        );
        assert!(
            metrics_response
                .body
                .contains("dash_retrieve_storage_promotion_boundary_replay_only_total 0")
        );
        assert!(
            metrics_response.body.contains(
                "dash_retrieve_storage_promotion_boundary_segment_plus_wal_delta_total 1"
            )
        );
        assert!(
            metrics_response.body.contains(
                "dash_retrieve_storage_promotion_boundary_segment_fully_promoted_total 0"
            )
        );

        restore_env_var_for_tests("DASH_RETRIEVAL_SEGMENT_DIR", prev_segment_dir.as_deref());
        restore_env_var_for_tests(
            "DASH_RETRIEVAL_STORAGE_DIVERGENCE_WARN_DELTA_COUNT",
            prev_warn_delta.as_deref(),
        );
        restore_env_var_for_tests(
            "DASH_RETRIEVAL_STORAGE_DIVERGENCE_WARN_RATIO",
            prev_warn_ratio.as_deref(),
        );
        restore_env_var_for_tests(
            "DASH_RETRIEVAL_SEGMENT_CACHE_REFRESH_MS",
            prev_refresh_ms.as_deref(),
        );
        let _ = std::fs::remove_dir_all(segment_root);
    }

    #[test]
    fn debug_storage_visibility_endpoint_rejects_invalid_query_shape() {
        let store = sample_store();
        let request = HttpRequest {
            method: "GET".to_string(),
            target: "/debug/storage-visibility?tenant_id=tenant-a".to_string(),
            headers: HashMap::new(),
            body: Vec::new(),
        };

        let response = handle_request(&store, &request);
        assert_eq!(response.status, 400);
        assert!(response.body.contains("query is required"));
    }

    #[test]
    fn metrics_endpoint_reports_retrieve_counters() {
        let _guard = env_lock().lock().expect("env lock should be available");
        let prev_segment_dir = std::env::var_os("DASH_RETRIEVAL_SEGMENT_DIR");
        let prev_refresh_ms = std::env::var_os("DASH_RETRIEVAL_SEGMENT_CACHE_REFRESH_MS");
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock should be monotonic")
            .as_nanos();
        let isolated_segment_root =
            std::env::temp_dir().join(format!("dash-retrieve-metrics-empty-segments-{nanos}"));
        set_env_var_for_tests(
            "DASH_RETRIEVAL_SEGMENT_DIR",
            isolated_segment_root.to_string_lossy().as_ref(),
        );
        set_env_var_for_tests("DASH_RETRIEVAL_SEGMENT_CACHE_REFRESH_MS", "600000");

        let store = sample_store();
        let metrics = Arc::new(Mutex::new(TransportMetrics::default()));

        let retrieve_request = HttpRequest {
            method: "GET".to_string(),
            target: "/v1/retrieve?tenant_id=tenant-a&query=company+x&top_k=1".to_string(),
            headers: HashMap::new(),
            body: Vec::new(),
        };
        let retrieve_response = handle_request_with_metrics(&store, &retrieve_request, &metrics);
        assert_eq!(retrieve_response.status, 200);

        let metrics_request = HttpRequest {
            method: "GET".to_string(),
            target: "/metrics".to_string(),
            headers: HashMap::new(),
            body: Vec::new(),
        };
        let metrics_response = handle_request_with_metrics(&store, &metrics_request, &metrics);
        assert_eq!(metrics_response.status, 200);
        assert!(
            metrics_response
                .content_type
                .starts_with("text/plain; version=0.0.4")
        );
        assert!(metrics_response.body.contains("dash_http_requests_total"));
        assert!(
            metrics_response
                .body
                .contains("dash_retrieve_requests_total 1")
        );
        assert!(
            metrics_response
                .body
                .contains("dash_transport_auth_success_total 1")
        );
        assert!(
            metrics_response
                .body
                .contains("dash_retrieve_storage_last_result_source_unknown_count 1")
        );
        assert!(
            metrics_response
                .body
                .contains("dash_retrieve_storage_result_source_unknown_total 1")
        );
        assert!(
            metrics_response
                .body
                .contains("dash_retrieve_storage_last_execution_mode_disk_native 0")
        );
        assert!(
            metrics_response
                .body
                .contains("dash_retrieve_storage_execution_mode_disk_native_total 0")
        );
        assert!(
            metrics_response
                .body
                .contains("dash_retrieve_storage_execution_mode_memory_index_total 1")
        );
        assert!(
            metrics_response
                .body
                .contains("dash_retrieve_storage_last_promotion_boundary_state 0")
        );
        assert!(
            metrics_response
                .body
                .contains("dash_retrieve_storage_last_promotion_boundary_in_transition 0")
        );
        assert!(
            metrics_response
                .body
                .contains("dash_retrieve_storage_promotion_boundary_replay_only_total 1")
        );
        assert!(
            metrics_response.body.contains(
                "dash_retrieve_storage_promotion_boundary_segment_plus_wal_delta_total 0"
            )
        );
        assert!(
            metrics_response.body.contains(
                "dash_retrieve_storage_promotion_boundary_segment_fully_promoted_total 0"
            )
        );
        assert!(
            metrics_response
                .body
                .contains("dash_retrieve_placement_enabled 0")
        );
        assert!(
            metrics_response
                .body
                .contains("dash_retrieve_placement_reload_enabled 0")
        );
        assert!(
            metrics_response
                .body
                .contains("dash_retrieve_transport_queue_capacity 0")
        );
        assert!(
            metrics_response
                .body
                .contains("dash_retrieve_transport_queue_depth 0")
        );
        assert!(
            metrics_response
                .body
                .contains("dash_retrieve_transport_queue_full_reject_total 0")
        );
        assert!(
            metrics_response
                .body
                .contains("dash_retrieve_latency_ms_p95")
        );

        restore_env_var_for_tests("DASH_RETRIEVAL_SEGMENT_DIR", prev_segment_dir.as_deref());
        restore_env_var_for_tests(
            "DASH_RETRIEVAL_SEGMENT_CACHE_REFRESH_MS",
            prev_refresh_ms.as_deref(),
        );
        let _ = std::fs::remove_dir_all(isolated_segment_root);
    }

    #[test]
    fn metrics_endpoint_reports_backpressure_queue_values() {
        let store = sample_store();
        let metrics = Arc::new(Mutex::new(TransportMetrics::default()));
        let queue_metrics = Arc::new(TransportBackpressureMetrics {
            queue_depth: AtomicUsize::new(3),
            queue_capacity: 8,
            queue_full_reject_total: AtomicU64::new(11),
        });
        {
            let mut guard = metrics.lock().expect("metrics lock should be available");
            guard.set_transport_backpressure_metrics(queue_metrics);
        }

        let metrics_request = HttpRequest {
            method: "GET".to_string(),
            target: "/metrics".to_string(),
            headers: HashMap::new(),
            body: Vec::new(),
        };
        let metrics_response = handle_request_with_metrics(&store, &metrics_request, &metrics);
        assert_eq!(metrics_response.status, 200);
        assert!(
            metrics_response
                .body
                .contains("dash_retrieve_transport_queue_capacity 8")
        );
        assert!(
            metrics_response
                .body
                .contains("dash_retrieve_transport_queue_depth 3")
        );
        assert!(
            metrics_response
                .body
                .contains("dash_retrieve_transport_queue_full_reject_total 11")
        );
    }

    #[test]
    fn resolve_http_queue_capacity_defaults_to_workers_times_constant() {
        let _guard = env_lock().lock().expect("env lock should be available");
        let key = "DASH_RETRIEVAL_HTTP_QUEUE_CAPACITY";
        let previous = std::env::var_os(key);
        restore_env_var_for_tests(key, None);
        let capacity = resolve_http_queue_capacity(3);
        assert_eq!(capacity, 3 * DEFAULT_HTTP_QUEUE_CAPACITY_PER_WORKER);
        restore_env_var_for_tests(key, previous.as_deref());
    }

    #[test]
    fn resolve_http_queue_capacity_prefers_env_override() {
        let _guard = env_lock().lock().expect("env lock should be available");
        let key = "DASH_RETRIEVAL_HTTP_QUEUE_CAPACITY";
        let previous = std::env::var_os(key);
        set_env_var_for_tests(key, "7");
        let capacity = resolve_http_queue_capacity(3);
        assert_eq!(capacity, 7);
        restore_env_var_for_tests(key, previous.as_deref());
    }

    #[test]
    fn metrics_endpoint_tracks_retrieve_client_errors() {
        let store = sample_store();
        let metrics = Arc::new(Mutex::new(TransportMetrics::default()));

        let bad_retrieve_request = HttpRequest {
            method: "GET".to_string(),
            target: "/v1/retrieve?tenant_id=tenant-a".to_string(),
            headers: HashMap::new(),
            body: Vec::new(),
        };
        let bad_response = handle_request_with_metrics(&store, &bad_retrieve_request, &metrics);
        assert_eq!(bad_response.status, 400);

        let metrics_request = HttpRequest {
            method: "GET".to_string(),
            target: "/metrics".to_string(),
            headers: HashMap::new(),
            body: Vec::new(),
        };
        let metrics_response = handle_request_with_metrics(&store, &metrics_request, &metrics);
        assert!(
            metrics_response
                .body
                .contains("dash_retrieve_client_error_total 1")
        );
        assert!(
            metrics_response
                .body
                .contains("dash_retrieve_segment_fallback_activation_total")
        );
        assert!(
            metrics_response
                .body
                .contains("dash_retrieve_segment_fallback_manifest_error_total")
        );
    }

    #[test]
    fn auth_policy_scoped_key_allows_configured_tenant() {
        let request = HttpRequest {
            method: "GET".to_string(),
            target: "/v1/retrieve?tenant_id=tenant-a&query=company+x".to_string(),
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
            method: "GET".to_string(),
            target: "/v1/retrieve?tenant_id=tenant-a&query=company+x".to_string(),
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
    fn auth_policy_scoped_key_rejects_unknown_key_when_required_keys_are_unset() {
        let request = HttpRequest {
            method: "GET".to_string(),
            target: "/v1/retrieve?tenant_id=tenant-a&query=company+x".to_string(),
            headers: HashMap::from([("x-api-key".to_string(), "unknown-key".to_string())]),
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
            authorize_request_for_tenant(&request, "tenant-a", &policy),
            AuthDecision::Unauthorized("missing or invalid API key")
        );
    }

    #[test]
    fn auth_policy_required_key_rejects_missing_key() {
        let request = HttpRequest {
            method: "GET".to_string(),
            target: "/v1/retrieve?tenant_id=tenant-a&query=company+x".to_string(),
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
            method: "GET".to_string(),
            target: "/v1/retrieve?tenant_id=tenant-a&query=company+x".to_string(),
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
            method: "GET".to_string(),
            target: "/v1/retrieve?tenant_id=tenant-a&query=company+x".to_string(),
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
    fn append_audit_record_writes_chained_hash_and_seq() {
        let mut audit_path = std::env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock should be monotonic")
            .as_nanos();
        audit_path.push(format!(
            "dash-retrieve-audit-chain-{}-{}.jsonl",
            std::process::id(),
            nanos
        ));
        let audit_path_str = audit_path.to_string_lossy().to_string();
        if let Ok(mut states) = audit_chain_states().lock() {
            states.remove(&audit_path_str);
        }

        append_audit_record(
            &audit_path_str,
            1_700_000_000_001,
            AuditEvent {
                action: "retrieve",
                tenant_id: Some("tenant-a"),
                status: 200,
                outcome: "success",
                reason: "ok",
            },
        )
        .expect("first audit append should succeed");
        append_audit_record(
            &audit_path_str,
            1_700_000_000_002,
            AuditEvent {
                action: "retrieve",
                tenant_id: Some("tenant-a"),
                status: 200,
                outcome: "success",
                reason: "ok",
            },
        )
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
