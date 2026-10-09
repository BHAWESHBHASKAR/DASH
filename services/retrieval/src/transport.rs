use std::{
    collections::{HashMap, VecDeque},
    net::TcpListener,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use metadata_router::{
    PlacementRouteError, ReadPreference, ReplicaHealth, ReplicaRole, RoutedReplica, RouterConfig,
    ShardPlacement, load_shard_placements_from_source, route_read_with_placement,
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
    build_retrieve_planner_debug_snapshot, execute_api_query_with_segment_prefilter,
    execute_api_query_with_storage_snapshot, resolve_segment_prefilter,
    segment_prefilter_cache_metrics_snapshot,
};
mod audit;
mod authz;
#[cfg(test)]
mod authz_matrix_tests;
mod debug_render;
mod http;
mod payload;
#[cfg(test)]
use audit::is_sha256_hex;
use audit::{AuditEvent, append_audit_record, audit_gate};
pub use authz::initialize_auth_policy;
pub(crate) use authz::{
    AuthDecision, Role, authorize_request_any_tenant, authorize_request_for_tenant,
    authorize_request_ops, shared_auth_policy,
};
use dash_common::AuthPolicy;
use debug_render::{
    evaluate_storage_divergence_warning, promotion_boundary_state_metric_value,
    render_placement_debug_json, render_planner_debug_json, render_storage_visibility_debug_json,
    resolve_storage_divergence_warn_delta_count, resolve_storage_divergence_warn_ratio,
};
use http::{query_encoding_is_invalid, render_response_text, server_config, split_target};
#[cfg(test)]
use payload::build_retrieve_request_from_json;
#[cfg(test)]
use payload::{JsonValue, parse_json};
use payload::{
    build_retrieve_request_from_query, build_retrieve_transport_request_from_json,
    build_retrieve_transport_request_from_query, json_escape, render_retrieve_response_json,
};

const METRICS_WINDOW_SIZE: usize = 2048;
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
    /// Requests that failed while being read (408/413/431/400/...), by status class.
    pub(crate) read_error_4xx_total: AtomicU64,
    pub(crate) read_error_5xx_total: AtomicU64,
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

impl dash_http::ServerHooks for TransportBackpressureMetrics {
    fn on_enqueued(&self) {
        self.observe_enqueued();
    }

    fn on_dequeued(&self) {
        self.observe_dequeued();
    }

    fn on_reject(&self, _reason: dash_http::RejectReason) {
        self.observe_rejected();
    }

    fn on_read_error(&self, status: u16) {
        self.observe_read_error(status);
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
    placement_file: Option<PathBuf>,
    control_plane_base_url: Option<String>,
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

/// Upper bounds (ms) of the per-route latency histogram buckets.
const LATENCY_BUCKETS_MS: [f64; 12] = [
    1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0,
];

/// Fixed-bucket latency histogram (non-cumulative counts per bucket; the
/// last slot is the overflow / `+Inf` bucket).
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct RouteLatencyHistogram {
    buckets: [u64; LATENCY_BUCKETS_MS.len() + 1],
    sum_ms: f64,
    count: u64,
}

impl RouteLatencyHistogram {
    fn observe(&mut self, latency_ms: f64) {
        let idx = LATENCY_BUCKETS_MS
            .iter()
            .position(|bound| latency_ms <= *bound)
            .unwrap_or(LATENCY_BUCKETS_MS.len());
        self.buckets[idx] += 1;
        self.sum_ms += latency_ms;
        self.count += 1;
    }
}

/// Stable route label for the latency histograms.
fn route_metric_label(path: &str) -> &'static str {
    match path {
        "/v1/retrieve" => "retrieve",
        "/v1/embeddings" => "embeddings",
        "/ready" | "/v1/ready" => "ready",
        "/metrics" => "metrics",
        "/debug/planner" | "/debug/storage-visibility" | "/debug/placement" => "debug",
        _ => "other",
    }
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
    /// Subsets of the 4xx total, so auth/rate-limit rejections are visible
    /// separately from malformed requests.
    retrieve_auth_denied_total: u64,
    retrieve_rate_limited_total: u64,
    route_latency: std::collections::BTreeMap<&'static str, RouteLatencyHistogram>,
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
            retrieve_auth_denied_total: 0,
            retrieve_rate_limited_total: 0,
            route_latency: std::collections::BTreeMap::new(),
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
            "/health" | "/v1/health" | "/live" | "/v1/live" | "/ready" | "/v1/ready" => {
                self.health_requests_total += 1;
            }
            "/metrics" => self.metrics_requests_total += 1,
            _ => {}
        }
    }

    /// Record one retrieve request. Latency and visibility-lag windows only
    /// receive samples from requests that actually executed (2xx): auth,
    /// validation and routing failures finish in microseconds and would
    /// drag the percentiles toward zero. They are counted per status class
    /// instead.
    fn observe_retrieve(
        &mut self,
        status: u16,
        latency_ms: f64,
        result_count: usize,
        ingest_to_visible_lag_ms: Option<f64>,
    ) {
        self.retrieve_requests_total += 1;
        match status {
            200..=299 => {
                self.retrieve_last_result_count = result_count;
                Self::push_window(&mut self.retrieve_latency_ms_window, latency_ms);
                if let Some(value) = ingest_to_visible_lag_ms {
                    Self::push_window(&mut self.ingest_to_visible_lag_ms_window, value);
                }
                self.retrieve_success_total += 1;
            }
            400..=499 => {
                self.retrieve_client_error_total += 1;
                match status {
                    401 | 403 => self.retrieve_auth_denied_total += 1,
                    429 => self.retrieve_rate_limited_total += 1,
                    _ => {}
                }
            }
            _ => self.retrieve_server_error_total += 1,
        }
    }

    fn observe_route_latency(&mut self, path: &str, latency_ms: f64) {
        self.route_latency
            .entry(route_metric_label(path))
            .or_default()
            .observe(latency_ms);
    }

    fn render_route_latency_histograms(&self) -> String {
        let mut out = String::from("# TYPE dash_http_request_duration_ms histogram\n");
        for (route, hist) in &self.route_latency {
            let mut cumulative = 0u64;
            for (idx, bound) in LATENCY_BUCKETS_MS.iter().enumerate() {
                cumulative += hist.buckets[idx];
                out.push_str(&format!(
                    "dash_http_request_duration_ms_bucket{{route=\"{route}\",le=\"{bound}\"}} {cumulative}\n"
                ));
            }
            out.push_str(&format!(
                "dash_http_request_duration_ms_bucket{{route=\"{route}\",le=\"+Inf\"}} {}\n",
                hist.count
            ));
            out.push_str(&format!(
                "dash_http_request_duration_ms_sum{{route=\"{route}\"}} {:.4}\n",
                hist.sum_ms
            ));
            out.push_str(&format!(
                "dash_http_request_duration_ms_count{{route=\"{route}\"}} {}\n",
                hist.count
            ));
        }
        out
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

    fn render_prometheus(
        &self,
        placement_routing: Option<&PlacementRoutingRuntime>,
        disk_status: &store::DiskStatus,
    ) -> String {
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
        let (disk_unavailable, disk_recovering) = match disk_status {
            store::DiskStatus::Unavailable { .. } => (1, 0),
            store::DiskStatus::Recovering => (0, 1),
            store::DiskStatus::Available => (0, 0),
        };

        let mut rendered = format!(
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
# TYPE dash_retrieve_transport_read_error_total counter\n\
dash_retrieve_transport_read_error_total{{status_class=\"4xx\"}} {}\n\
dash_retrieve_transport_read_error_total{{status_class=\"5xx\"}} {}\n\
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
# TYPE dash_disk_unavailable gauge\n\
dash_disk_unavailable {}\n\
# TYPE dash_disk_recovering gauge\n\
dash_disk_recovering {}\n\
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
            read_error_4xx,
            read_error_5xx,
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
            disk_unavailable,
            disk_recovering,
            uptime_seconds
        );
        rendered.push_str(&format!(
            "# TYPE dash_retrieve_auth_denied_total counter\n\
dash_retrieve_auth_denied_total {}\n\
# TYPE dash_retrieve_rate_limited_total counter\n\
dash_retrieve_rate_limited_total {}\n",
            self.retrieve_auth_denied_total, self.retrieve_rate_limited_total
        ));
        rendered.push_str(&self.render_route_latency_histograms());
        rendered
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

pub fn serve_http(store: Arc<RwLock<InMemoryStore>>, bind_addr: &str) -> std::io::Result<()> {
    let shutdown = dash_common::ShutdownSignal::install();
    serve_http_with_workers(store, bind_addr, DEFAULT_HTTP_WORKERS, shutdown)
}

pub fn serve_http_with_workers(
    store: Arc<RwLock<InMemoryStore>>,
    bind_addr: &str,
    worker_count: usize,
    shutdown: std::sync::Arc<dash_common::ShutdownSignal>,
) -> std::io::Result<()> {
    let listener = TcpListener::bind(bind_addr)?;
    let worker_count = worker_count.max(1);
    let metrics = Arc::new(Mutex::new(TransportMetrics::default()));
    let queue_capacity = resolve_http_queue_capacity(worker_count);
    let backpressure_metrics = Arc::new(TransportBackpressureMetrics::new(queue_capacity));
    if let Ok(mut guard) = metrics.lock() {
        guard.set_transport_backpressure_metrics(Arc::clone(&backpressure_metrics));
    }
    let placement_routing = new_placement_routing()?;
    let handler: dash_http::Handler = Arc::new(move |request| {
        handle_connection_request(
            &store,
            &HttpRequest::from(request),
            &metrics,
            &placement_routing,
        )
        .into()
    });
    dash_http::serve(
        listener,
        server_config(worker_count, queue_capacity),
        handler,
        dash_http::default_health_classifier,
        &|| shutdown.is_triggered(),
        backpressure_metrics,
    )
}

fn new_placement_routing() -> std::io::Result<SharedPlacementRouting> {
    let state = PlacementRoutingState::from_env().map_err(|reason| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("invalid placement routing configuration: {reason}"),
        )
    })?;
    Ok(Arc::new(Mutex::new(state)))
}

pub fn serve_http_once(store: Arc<RwLock<InMemoryStore>>, bind_addr: &str) -> std::io::Result<()> {
    let listener = TcpListener::bind(bind_addr)?;
    serve_http_once_with_listener(store, listener)
}

pub fn serve_http_once_with_listener(
    store: Arc<RwLock<InMemoryStore>>,
    listener: TcpListener,
) -> std::io::Result<()> {
    let metrics = Arc::new(Mutex::new(TransportMetrics::default()));
    let placement_routing = new_placement_routing()?;
    let handler: dash_http::Handler = Arc::new(move |request| {
        handle_connection_request(
            &store,
            &HttpRequest::from(request),
            &metrics,
            &placement_routing,
        )
        .into()
    });
    dash_http::serve_once(
        &listener,
        &server_config(1, 1),
        &handler,
        &TransportBackpressureMetrics::default(),
    )
}

pub fn handle_http_request_bytes(
    store: &InMemoryStore,
    raw_request: &[u8],
) -> Result<Vec<u8>, String> {
    let request = dash_http::parse_request_bytes(raw_request, &server_config(1, 1))
        .map_err(|err| err.message)?;
    let request = HttpRequest::from(request);
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

fn handle_connection_request(
    store: &Arc<RwLock<InMemoryStore>>,
    request: &HttpRequest,
    metrics: &Arc<Mutex<TransportMetrics>>,
    placement_routing: &SharedPlacementRouting,
) -> HttpResponse {
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
            return HttpResponse::internal_server_error(
                "failed to acquire retrieval placement routing lock",
            );
        }
    };
    if let Ok(mut guard) = metrics.lock() {
        guard.observe_placement_reload_snapshot(&reload_snapshot);
    }
    // The store lock is taken only for the short in-memory read sections inside
    // the handler (never across embedding calls) and is released before the
    // response is written to the socket.
    handle_request_with_metrics_and_reload(
        &**store,
        request,
        metrics,
        routing_snapshot.as_ref(),
        Some(&reload_snapshot),
    )
}

#[cfg(test)]
fn handle_request(store: &InMemoryStore, request: &HttpRequest) -> HttpResponse {
    let metrics = Arc::new(Mutex::new(TransportMetrics::default()));
    handle_request_with_metrics(store, request, &metrics)
}

#[cfg(test)]
pub(crate) fn handle_request_with_metrics(
    store: &InMemoryStore,
    request: &HttpRequest,
    metrics: &Arc<Mutex<TransportMetrics>>,
) -> HttpResponse {
    let mut placement_routing = match PlacementRoutingState::from_env() {
        Ok(runtime) => runtime,
        Err(reason) => {
            eprintln!("retrieval placement routing configuration error: {reason}");
            return HttpResponse::internal_server_error("placement_config_invalid");
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

fn handle_request_with_metrics_and_reload<S: StoreAccess + ?Sized>(
    store: &S,
    request: &HttpRequest,
    metrics: &Arc<Mutex<TransportMetrics>>,
    placement_routing: Option<&PlacementRoutingRuntime>,
    placement_reload: Option<&PlacementReloadSnapshot>,
) -> HttpResponse {
    let auth_policy = shared_auth_policy();
    // Audit context (actor fingerprint, request id) for every event emitted
    // while this request is handled on this thread.
    let _audit_ctx = dash_common::audit::enter_context(dash_common::audit::context_from_headers(
        &request.headers,
    ));
    let path_only = request.target.split('?').next().unwrap_or_default();
    if path_only == "/v1/retrieve" {
        let audit_log_path = env_with_fallback(
            "DASH_RETRIEVAL_AUDIT_LOG_PATH",
            "EME_RETRIEVAL_AUDIT_LOG_PATH",
        );
        if let Err(err) = audit_gate(audit_log_path.as_deref()) {
            eprintln!("retrieval audit fail-closed gate rejected request: {err}");
            return HttpResponse {
                status: 503,
                content_type: "application/json",
                body: "{\"error\":\"audit log unavailable\"}".to_string(),
                retry_after_secs: None,
            };
        }
    }
    let mut response = handle_request_with_policy(
        store,
        request,
        metrics,
        placement_routing,
        placement_reload,
        &auth_policy,
    );
    if request.method == "GET" && path_only == "/metrics" && response.status == 200 {
        response
            .body
            .push_str(&dash_common::audit::render_prometheus_counters());
    }
    response
}

fn handle_request_with_policy<S: StoreAccess + ?Sized>(
    store: &S,
    request: &HttpRequest,
    metrics: &Arc<Mutex<TransportMetrics>>,
    placement_routing: Option<&PlacementRoutingRuntime>,
    placement_reload: Option<&PlacementReloadSnapshot>,
    auth_policy: &AuthPolicy,
) -> HttpResponse {
    let started_at = Instant::now();
    let response = route_request(
        store,
        request,
        metrics,
        placement_routing,
        placement_reload,
        auth_policy,
    );
    let (path, _) = split_target(&request.target);
    if let Ok(mut guard) = metrics.lock() {
        guard.observe_route_latency(&path, started_at.elapsed().as_secs_f64() * 1000.0);
    }
    response
}

fn route_request<S: StoreAccess + ?Sized>(
    store: &S,
    request: &HttpRequest,
    metrics: &Arc<Mutex<TransportMetrics>>,
    placement_routing: Option<&PlacementRoutingRuntime>,
    placement_reload: Option<&PlacementReloadSnapshot>,
    auth_policy: &AuthPolicy,
) -> HttpResponse {
    let (path, query) = split_target(&request.target);
    if query_encoding_is_invalid(&request.target) {
        return HttpResponse::bad_request("invalid percent-encoding in query");
    }
    let audit_log_path = env_with_fallback(
        "DASH_RETRIEVAL_AUDIT_LOG_PATH",
        "EME_RETRIEVAL_AUDIT_LOG_PATH",
    );
    if let Ok(mut guard) = metrics.lock() {
        guard.observe_http(&path);
    }

    match (request.method.as_str(), path.as_str()) {
        // Versioned + unversioned health endpoints. The unversioned
        // `/health` is kept for backward compat with the existing
        // k8s probe config; `/v1/health` is the new canonical
        // versioned path that matches every other `/v1/*` endpoint.
        ("GET", "/health") | ("GET", "/v1/health") => {
            HttpResponse::ok_json("{\"status\":\"ok\"}".to_string())
        }
        // Liveness probe: the process is alive and not deadlocked.
        // Kubernetes restarts the pod if this fails. No disk /
        // network checks here — they belong on /ready.
        ("GET", "/live") | ("GET", "/v1/live") => {
            HttpResponse::ok_json("{\"status\":\"alive\"}".to_string())
        }
        // Readiness probe: the process is up AND can serve traffic.
        // Kubernetes removes the pod from the service if this fails.
        // We check that the in-memory store can be reached and that
        // disk persistence is healthy when a persistence path was
        // configured.
        ("GET", "/ready") | ("GET", "/v1/ready") => {
            if let Err(poisoned) = metrics.lock() {
                eprintln!("retrieval /ready: metrics mutex poisoned: {poisoned}");
                return HttpResponse::internal_server_error("metrics_unavailable");
            }
            // Replication follower health: a dead, stale or lagging follower
            // means this node serves outdated data, so it must leave the
            // load balancer. Only applies when a follower is attached.
            let replication_status = store.replication_status();
            if let Some(follower) = replication_status.as_ref()
                && let Err(reason) = follower.readiness()
            {
                return HttpResponse {
                    status: 503,
                    content_type: "application/json",
                    body: format!(
                        "{{\"status\":\"not_ready\",\"reason\":\"{reason}\",\"replication\":{}}}",
                        follower.to_json()
                    ),
                    retry_after_secs: None,
                };
            }
            let ready_body = match replication_status.as_ref() {
                Some(follower) => format!(
                    "{{\"status\":\"ready\",\"replication\":{}}}",
                    follower.to_json()
                ),
                None => "{\"status\":\"ready\"}".to_string(),
            };
            let store_view = store.read_store();
            let disk_status = store_view.disk_status();
            match disk_status {
                store::DiskStatus::Available | store::DiskStatus::Recovering => {
                    HttpResponse::ok_json(ready_body)
                }
                store::DiskStatus::Unavailable { reason } => {
                    if persistence_path_configured() {
                        eprintln!("retrieval /ready: disk unavailable: {reason}");
                        HttpResponse {
                            status: 503,
                            content_type: "application/json",
                            body: "{\"status\":\"not_ready\",\"reason\":\"disk_unavailable\"}"
                                .to_string(),
                            retry_after_secs: None,
                        }
                    } else {
                        HttpResponse::ok_json(ready_body)
                    }
                }
            }
        }
        ("GET", "/metrics") => {
            if !auth_policy.metrics_public()
                && let Some(denied) = deny_unless_allowed(
                    authorize_request_ops(request, auth_policy, Role::ReadOnly),
                    metrics,
                    audit_log_path.as_deref(),
                    "metrics",
                    None,
                    false,
                )
            {
                return denied;
            }
            let mut body = if let Ok(guard) = metrics.lock() {
                guard.render_prometheus(placement_routing, store.read_store().disk_status())
            } else {
                "dash_transport_metrics_unavailable 1\n".to_string()
            };
            if let Some(follower) = store.replication_status() {
                body.push_str(&follower.render_prometheus());
            }
            HttpResponse::ok_text(body)
        }
        ("GET", "/debug/placement") => {
            if let Some(denied) = deny_unless_allowed(
                authorize_request_ops(request, auth_policy, Role::ReadOnly),
                metrics,
                audit_log_path.as_deref(),
                "debug_placement",
                None,
                false,
            ) {
                return denied;
            }
            HttpResponse::ok_json(render_placement_debug_json(
                placement_routing,
                placement_reload,
                &query,
            ))
        }
        ("GET", "/debug/planner") => match build_retrieve_request_from_query(&query) {
            Ok(req) => {
                let tenant_id = req.tenant_id.clone();
                if let Some(denied) = deny_unless_allowed(
                    authorize_request_for_tenant(request, &tenant_id, auth_policy, Role::ReadOnly),
                    metrics,
                    audit_log_path.as_deref(),
                    "debug_planner",
                    Some(&tenant_id),
                    false,
                ) {
                    return denied;
                }
                let snapshot = build_retrieve_planner_debug_snapshot(&store.read_store(), &req);
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
            Err(err) => HttpResponse::bad_request(&err),
        },
        ("GET", "/debug/storage-visibility") => match build_retrieve_request_from_query(&query) {
            Ok(req) => {
                let tenant_id = req.tenant_id.clone();
                if let Some(denied) = deny_unless_allowed(
                    authorize_request_for_tenant(request, &tenant_id, auth_policy, Role::ReadOnly),
                    metrics,
                    audit_log_path.as_deref(),
                    "debug_storage_visibility",
                    Some(&tenant_id),
                    false,
                ) {
                    return denied;
                }
                let snapshot = build_retrieve_planner_debug_snapshot(&store.read_store(), &req);
                let (_, merge_snapshot) =
                    execute_api_query_with_storage_snapshot(&store.read_store(), req.clone());
                let warn_delta_count = resolve_storage_divergence_warn_delta_count();
                let warn_ratio = resolve_storage_divergence_warn_ratio();
                let (warn, reason, ratio) =
                    evaluate_storage_divergence_warning(&snapshot, warn_delta_count, warn_ratio);
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
            Err(err) => HttpResponse::bad_request(&err),
        },
        ("GET", "/v1/retrieve") => match build_retrieve_transport_request_from_query(&query) {
            Ok(transport_req) => handle_authorized_retrieve(
                store,
                request,
                transport_req,
                auth_policy,
                metrics,
                placement_routing,
                audit_log_path.as_deref(),
            ),
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
                Ok(transport_req) => handle_authorized_retrieve(
                    store,
                    request,
                    transport_req,
                    auth_policy,
                    metrics,
                    placement_routing,
                    audit_log_path.as_deref(),
                ),
                Err(err) => {
                    if let Ok(mut guard) = metrics.lock() {
                        guard.observe_retrieve(400, 0.0, 0, None);
                    }
                    HttpResponse::bad_request(&err)
                }
            }
        }
        ("POST", "/v1/embeddings") => {
            // OpenAI-compatible embeddings endpoint. Requires valid
            // credentials (and the retrieve role) before any provider call so
            // anonymous callers cannot spend provider quota.
            //
            // The embedding backend is selected from the
            // DASH_EMBEDDING_PROVIDER env var. The default is `hash`
            // (deterministic, no network) which is suitable for
            // testing and for environments that have not yet wired up
            // a real embedding model. Production deployments should
            // set `DASH_EMBEDDING_PROVIDER=ollama` or `=openai` so the
            // vectors are semantically meaningful.
            if let Some(denied) = deny_unless_allowed(
                authorize_request_any_tenant(request, auth_policy, Role::Retrieve),
                metrics,
                audit_log_path.as_deref(),
                "embeddings",
                None,
                false,
            ) {
                return denied;
            }
            let body = match std::str::from_utf8(&request.body) {
                Ok(text) => text,
                Err(_) => {
                    let err = crate::openai_embeddings::OpenAIErrorResponse::invalid_param(
                        "request body must be valid UTF-8",
                        "body",
                        "invalid_request_body",
                    );
                    return HttpResponse::json_with_status(
                        400,
                        serde_json::to_string(&err).unwrap_or_default(),
                    );
                }
            };
            match crate::openai_embeddings::handle_openai_embeddings_with_provider(
                body,
                embedding_provider().as_ref(),
            ) {
                Ok(resp) => {
                    let body = serde_json::to_string(&resp).unwrap_or_else(|_| "{}".to_string());
                    HttpResponse::ok_json(body)
                }
                Err(err) => {
                    let (status, err) = classify_openai_embeddings_error(err);
                    let body = serde_json::to_string(&err)
                        .unwrap_or_else(|_| "{\"error\":\"internal\"}".to_string());
                    let mut response = HttpResponse::json_with_status(status, body);
                    response.retry_after_secs = err.retry_after_secs;
                    response
                }
            }
        }
        (_, "/v1/retrieve") => HttpResponse::method_not_allowed("only GET and POST are supported"),
        (_, "/v1/embeddings") => HttpResponse::method_not_allowed("only POST is supported"),
        (_, "/health")
        | (_, "/v1/health")
        | (_, "/live")
        | (_, "/v1/live")
        | (_, "/ready")
        | (_, "/v1/ready")
        | (_, "/metrics")
        | (_, "/debug/placement")
        | (_, "/debug/planner")
        | (_, "/debug/storage-visibility") => {
            HttpResponse::method_not_allowed("only GET is supported")
        }
        _ => HttpResponse::not_found("unknown path"),
    }
}

/// Maps an embeddings-endpoint error to an HTTP status. Client errors stay
/// 400; provider failures carry a short code (details were logged where the
/// failure happened) and become 503/502.
fn classify_openai_embeddings_error(
    err: crate::openai_embeddings::OpenAIErrorResponse,
) -> (u16, crate::openai_embeddings::OpenAIErrorResponse) {
    if err.error.kind != "server_error" {
        return (400, err);
    }
    match err.error.code.as_deref() {
        Some("embedding_unavailable") => {
            let retry = err.retry_after_secs.unwrap_or(1);
            let mut err = err;
            err.retry_after_secs = Some(retry);
            (503, err)
        }
        _ => (
            502,
            crate::openai_embeddings::OpenAIErrorResponse::server_error("embedding_provider_error"),
        ),
    }
}

fn env_with_fallback(primary: &str, fallback: &str) -> Option<String> {
    std::env::var(primary)
        .ok()
        .or_else(|| std::env::var(fallback).ok())
}

/// True when a persistence path was explicitly configured and disk
/// persistence was not disabled. Used by the /ready probe to decide
/// whether an `Unavailable` disk status should fail readiness.
fn persistence_path_configured() -> bool {
    let disabled = std::env::var("DASH_RETRIEVAL_PERSISTENCE_DISABLE")
        .ok()
        .or_else(|| std::env::var("EME_RETRIEVAL_PERSISTENCE_DISABLE").ok())
        .is_some_and(|value| matches!(value.trim().to_lowercase().as_str(), "1" | "true" | "yes"));
    let path_set = std::env::var("DASH_RETRIEVAL_PERSISTENCE_PATH")
        .or_else(|_| std::env::var("EME_RETRIEVAL_PERSISTENCE_PATH"))
        .is_ok_and(|value| !value.trim().is_empty());
    !disabled && path_set
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

/// Turn a non-`Allowed` decision into a response, recording auth metrics and
/// an audit event. Returns `None` (after counting the success) when allowed.
fn deny_unless_allowed(
    decision: AuthDecision,
    metrics: &Arc<Mutex<TransportMetrics>>,
    audit_log_path: Option<&str>,
    action: &str,
    tenant_id: Option<&str>,
    observe_retrieve: bool,
) -> Option<HttpResponse> {
    let (status, reason, response) = match decision {
        AuthDecision::Allowed => {
            // Only tenant-scoped requests count towards the auth success
            // counter; operational endpoints (metrics, debug) do not.
            if tenant_id.is_some() {
                observe_auth_success(metrics);
            }
            return None;
        }
        AuthDecision::Unauthorized(reason) => {
            observe_auth_failure(metrics);
            (401, reason, HttpResponse::unauthorized(reason))
        }
        AuthDecision::Forbidden(reason) => {
            observe_authz_denied(metrics);
            (403, reason, HttpResponse::forbidden(reason))
        }
        AuthDecision::RateLimited { retry_after_secs } => {
            observe_authz_denied(metrics);
            (
                429,
                "rate limit exceeded",
                HttpResponse::too_many_requests("rate limit exceeded", retry_after_secs),
            )
        }
    };
    if observe_retrieve && let Ok(mut guard) = metrics.lock() {
        guard.observe_retrieve(status, 0.0, 0, None);
    }
    emit_audit_event(
        metrics,
        audit_log_path,
        action,
        tenant_id,
        status,
        "denied",
        reason,
    );
    Some(response)
}

/// Shared tail of `GET`/`POST /v1/retrieve`: authorize first, and only then
/// spend an embedding provider call on the query.
fn handle_authorized_retrieve<S: StoreAccess + ?Sized>(
    store: &S,
    request: &HttpRequest,
    transport_req: RetrieveTransportRequest,
    auth_policy: &AuthPolicy,
    metrics: &Arc<Mutex<TransportMetrics>>,
    placement_routing: Option<&PlacementRoutingRuntime>,
    audit_log_path: Option<&str>,
) -> HttpResponse {
    let mut req = transport_req.request;
    let tenant_id = req.tenant_id.clone();
    if let Some(denied) = deny_unless_allowed(
        authorize_request_for_tenant(request, &tenant_id, auth_policy, Role::Retrieve),
        metrics,
        audit_log_path,
        "retrieve",
        Some(&tenant_id),
        true,
    ) {
        return denied;
    }
    // Embedding happens only after authorization and before any store lock.
    if let Err(failure) = embed_query_if_missing(&mut req) {
        return failure.into_response();
    }
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
        audit_log_path,
        "retrieve",
        Some(&tenant_id),
        response.status,
        outcome,
        reason,
    );
    response
}

type SharedEmbeddingProvider = Arc<dyn embeddings::EmbeddingProvider + Send + Sync>;

#[cfg(test)]
thread_local! {
    /// Per-thread provider injected by tests so they never touch the
    /// process-wide environment.
    static PROVIDER_OVERRIDE: std::cell::RefCell<Option<SharedEmbeddingProvider>> =
        const { std::cell::RefCell::new(None) };
}

/// Embedding provider shared by requests. It is rebuilt only when the
/// provider-related environment changes, instead of on every request.
fn embedding_provider() -> SharedEmbeddingProvider {
    #[cfg(test)]
    if let Some(provider) = PROVIDER_OVERRIDE.with(|slot| slot.borrow().clone()) {
        return provider;
    }
    PROVIDER_CACHE.get()
}

static PROVIDER_CACHE: embeddings::SharedProviderCache = embeddings::SharedProviderCache::new();

/// How many times the request path has constructed an embedding provider.
#[cfg(test)]
fn embedding_provider_build_count() -> u64 {
    PROVIDER_CACHE.build_count()
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

/// Embedding failure for the retrieve path: HTTP status, short machine code
/// and (for 503) the `Retry-After` seconds.
struct QueryEmbedFailure {
    status: u16,
    code: &'static str,
    retry_after_secs: Option<u64>,
}

impl QueryEmbedFailure {
    fn into_response(self) -> HttpResponse {
        let mut response = HttpResponse::error_with_status(self.status, self.code);
        response.retry_after_secs = self.retry_after_secs.or(response.retry_after_secs);
        response
    }
}

/// Embed the retrieve query text using the configured `DASH_EMBEDDING_PROVIDER`
/// when the caller did not supply an explicit `query_embedding`. This makes
/// semantic retrieval work out of the box for SDKs and curl clients.
fn embed_query_if_missing(req: &mut RetrieveApiRequest) -> Result<(), QueryEmbedFailure> {
    if req.query_embedding.is_some() {
        return Ok(());
    }
    let provider = embedding_provider();
    let vectors = crate::openai_embeddings::embed_texts_checked(
        provider.as_ref(),
        std::slice::from_ref(&req.query),
    )
    .map_err(|err| {
        let (status, err) = classify_openai_embeddings_error(err);
        let code = match err.error.code.as_deref() {
            Some("embedding_unavailable") => "embedding_unavailable",
            _ => "embedding_provider_error",
        };
        QueryEmbedFailure {
            status,
            code,
            retry_after_secs: err.retry_after_secs,
        }
    })?;
    req.query_embedding = vectors.into_iter().next();
    Ok(())
}

fn execute_retrieve_and_observe<S: StoreAccess + ?Sized>(
    store: &S,
    req: RetrieveApiRequest,
    read_consistency: ReadConsistencyPolicy,
    metrics: &Arc<Mutex<TransportMetrics>>,
    placement_routing: Option<&PlacementRoutingRuntime>,
) -> HttpResponse {
    let mut serving_replica: Option<String> = None;
    if let Some(routing) = placement_routing {
        match ensure_local_read_route(routing, &req, read_consistency) {
            Ok(routed) => {
                serving_replica = Some(routed.node_id.clone());
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
    // Resolve the segment prefilter (may read segment files) BEFORE taking
    // the store read lock, so a slow refresh never stalls a store writer.
    let segment_base = resolve_segment_prefilter(&tenant_id);
    let store_view = store.read_store();
    if let Some(vector) = req.query_embedding.as_deref()
        && let Err(err) = store_view.validate_query_vector(&tenant_id, vector)
    {
        drop(store_view);
        let message = match err {
            store::StoreError::InvalidVector(detail) => format!("query_embedding: {detail}"),
            _ => "query_embedding is invalid".to_string(),
        };
        if let Ok(mut guard) = metrics.lock() {
            guard.observe_retrieve(400, 0.0, 0, None);
        }
        return HttpResponse::bad_request(&message);
    }
    let (response, merge_snapshot) =
        execute_api_query_with_segment_prefilter(&store_view, req, segment_base);
    let latency_ms = started_at.elapsed().as_secs_f64() * 1000.0;
    let result_count = response.results.len();
    let ingest_to_visible_lag_ms = estimate_ingest_to_visible_lag_ms(&response.results);
    drop(store_view);

    if let Ok(mut guard) = metrics.lock() {
        guard.observe_retrieve(200, latency_ms, result_count, ingest_to_visible_lag_ms);
        guard.observe_storage_merge_execution(&merge_snapshot);
    }

    HttpResponse::ok_json(render_retrieve_response_json(
        &response,
        read_consistency.as_str(),
        true,
        serving_replica.as_deref(),
    ))
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
        let placement_file =
            env_with_fallback("DASH_ROUTER_PLACEMENT_FILE", "EME_ROUTER_PLACEMENT_FILE");
        let control_plane_base_url = env_with_fallback(
            "DASH_ROUTER_CONTROL_PLANE_URL",
            "EME_ROUTER_CONTROL_PLANE_URL",
        )
        .map(|value| value.trim().trim_end_matches('/').to_string())
        .filter(|value| !value.is_empty());
        if placement_file.is_none() && control_plane_base_url.is_none() {
            return Ok(None);
        }
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
            placement_file.as_deref().map(Path::new),
            control_plane_base_url.as_deref(),
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
                    placement_file: placement_file.as_deref().map(PathBuf::from),
                    control_plane_base_url: control_plane_base_url.clone(),
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
            reload.config.placement_file.as_deref(),
            reload.config.control_plane_base_url.as_deref(),
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
    placement_file: Option<&Path>,
    control_plane_base_url: Option<&str>,
    local_node_id: &str,
    shard_ids_override: Option<&[u32]>,
    replica_count_override: Option<usize>,
    virtual_nodes_per_shard: u32,
    read_preference: ReadPreference,
) -> Result<PlacementRoutingRuntime, String> {
    let placements = load_shard_placements_from_source(placement_file, control_plane_base_url)?;
    if placements.is_empty() {
        let source = control_plane_base_url
            .map(|value| format!("control-plane '{}'", value))
            .or_else(|| placement_file.map(|value| format!("placement file '{}'", value.display())))
            .unwrap_or_else(|| "placement source".to_string());
        return Err(format!("{source} has no placement records"));
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
    // Topology details (node ids, replica counts) stay in the log; clients
    // only get a short code.
    eprintln!("retrieval read route rejected: {error:?}");
    let code = match error {
        ReadRouteError::Placement(_) => "placement_unavailable",
        ReadRouteError::WrongNode { .. } => "wrong_node",
        ReadRouteError::ConsistencyUnavailable { .. } => "read_consistency_unavailable",
    };
    (503, code.to_string())
}

/// Freshness lag of the returned evidence: the mean of `now - ingested_at`
/// over results that carry an evidence ingest timestamp (epoch millis, set at
/// ingest time). Results without one are skipped; claim `event_time_unix` is
/// NOT used because it is when the fact happened, not when it was ingested.
fn estimate_ingest_to_visible_lag_ms(results: &[EvidenceNode]) -> Option<f64> {
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()?
        .as_millis() as i64;
    ingest_lag_ms_at(results, now_ms)
}

fn ingest_lag_ms_at(results: &[EvidenceNode], now_ms: i64) -> Option<f64> {
    let lag_values: Vec<f64> = results
        .iter()
        .filter_map(|node| {
            let newest = node
                .citations
                .iter()
                .filter_map(|citation| citation.ingested_at)
                .max()?;
            (now_ms >= newest).then(|| (now_ms - newest) as f64)
        })
        .collect();
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
    /// Emitted as a `Retry-After` header (429 responses).
    pub(crate) retry_after_secs: Option<u64>,
}

impl HttpResponse {
    fn ok_json(body: String) -> Self {
        Self {
            status: 200,
            content_type: "application/json",
            body,
            retry_after_secs: None,
        }
    }

    fn ok_text(body: String) -> Self {
        Self {
            status: 200,
            content_type: "text/plain; version=0.0.4; charset=utf-8",
            body,
            retry_after_secs: None,
        }
    }

    fn bad_request(message: &str) -> Self {
        Self {
            status: 400,
            content_type: "application/json",
            body: format!("{{\"error\":\"{}\"}}", json_escape(message)),
            retry_after_secs: None,
        }
    }

    fn unauthorized(message: &str) -> Self {
        Self {
            status: 401,
            content_type: "application/json",
            body: format!("{{\"error\":\"{}\"}}", json_escape(message)),
            retry_after_secs: None,
        }
    }

    fn forbidden(message: &str) -> Self {
        Self {
            status: 403,
            content_type: "application/json",
            body: format!("{{\"error\":\"{}\"}}", json_escape(message)),
            retry_after_secs: None,
        }
    }

    fn method_not_allowed(message: &str) -> Self {
        Self {
            status: 405,
            content_type: "application/json",
            body: format!("{{\"error\":\"{}\"}}", json_escape(message)),
            retry_after_secs: None,
        }
    }

    fn not_found(message: &str) -> Self {
        Self {
            status: 404,
            content_type: "application/json",
            body: format!("{{\"error\":\"{}\"}}", json_escape(message)),
            retry_after_secs: None,
        }
    }

    fn internal_server_error(message: &str) -> Self {
        Self {
            status: 500,
            content_type: "application/json",
            body: format!("{{\"error\":\"{}\"}}", json_escape(message)),
            retry_after_secs: None,
        }
    }

    fn too_many_requests(message: &str, retry_after_secs: u64) -> Self {
        Self {
            status: 429,
            content_type: "application/json",
            body: format!("{{\"error\":\"{}\"}}", json_escape(message)),
            retry_after_secs: Some(retry_after_secs),
        }
    }

    fn json_with_status(status: u16, body: String) -> Self {
        Self {
            status,
            content_type: "application/json",
            body,
            retry_after_secs: None,
        }
    }

    fn error_with_status(status: u16, message: &str) -> Self {
        if status == 429 {
            return Self::too_many_requests(message, 1);
        }
        Self {
            status,
            content_type: "application/json",
            body: format!("{{\"error\":\"{}\"}}", json_escape(message)),
            retry_after_secs: None,
        }
    }
}

/// Access to the shared store that lets the handler take the read lock only
/// for the sections that need it, instead of for the whole request.
trait StoreAccess {
    fn read_store(&self) -> StoreView<'_>;

    /// Replication follower attached to this store, if any.
    fn replication_status(&self) -> Option<Arc<crate::replication::FollowerStatus>> {
        None
    }
}

enum StoreView<'a> {
    Borrowed(&'a InMemoryStore),
    Guard(std::sync::RwLockReadGuard<'a, InMemoryStore>),
}

impl std::ops::Deref for StoreView<'_> {
    type Target = InMemoryStore;
    fn deref(&self) -> &InMemoryStore {
        match self {
            StoreView::Borrowed(store) => store,
            StoreView::Guard(guard) => guard,
        }
    }
}

impl StoreAccess for InMemoryStore {
    fn read_store(&self) -> StoreView<'_> {
        StoreView::Borrowed(self)
    }
}

impl StoreAccess for RwLock<InMemoryStore> {
    fn read_store(&self) -> StoreView<'_> {
        StoreView::Guard(self.read().unwrap_or_else(|p| p.into_inner()))
    }

    fn replication_status(&self) -> Option<Arc<crate::replication::FollowerStatus>> {
        crate::replication::status_for_store_ptr(self as *const Self as usize)
    }
}

#[cfg(test)]
mod tests {
    use super::authz::policy_from_parts as test_auth_policy;
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
        ensure_dev_mode_env();
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

    /// Tests that exercise handlers without configuring credentials run in
    /// explicit dev mode (the only way to get an unauthenticated service).
    #[allow(unused_unsafe)]
    pub(super) fn ensure_dev_mode_env() {
        static ONCE: std::sync::Once = std::sync::Once::new();
        ONCE.call_once(|| unsafe {
            std::env::set_var("DASH_INSECURE_DEV_MODE", "1");
            std::env::set_var("DASH_STRICT_SECRETS", "0");
        });
    }

    pub(super) fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| {
            ensure_dev_mode_env();
            Mutex::new(())
        })
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
    fn invalid_percent_encoding_in_query_is_a_400_not_a_dropped_parameter() {
        let store = sample_store();
        for bad in [
            "stance_mode=%FF",
            "entity_filters=%FF",
            "top_k=%zz",
            "query=%4",
            "%zz=1",
        ] {
            let request = HttpRequest {
                method: "GET".to_string(),
                target: format!("/v1/retrieve?tenant_id=tenant-a&query=company+x&{bad}"),
                headers: HashMap::new(),
                body: Vec::new(),
            };
            let response = handle_request(&store, &request);
            assert_eq!(response.status, 400, "{bad}: {}", response.body);
            assert!(
                response.body.contains("invalid percent-encoding in query"),
                "{bad}: {}",
                response.body
            );
        }
        // Valid encodings, `+` and bare flags are unchanged.
        for good in [
            "stance_mode=balanced",
            "query=company%20x",
            "query=company+x&flag",
            "entity_filters=a%2Cb",
        ] {
            let request = HttpRequest {
                method: "GET".to_string(),
                target: format!("/v1/retrieve?tenant_id=tenant-a&top_k=1&query=x&{good}"),
                headers: HashMap::new(),
                body: Vec::new(),
            };
            let response = handle_request(&store, &request);
            assert_ne!(response.status, 400, "{good}: {}", response.body);
        }
        assert!(!query_encoding_is_invalid("/x?a=b+c&d=%41&e"));
        assert!(query_encoding_is_invalid("/x?a=%"));
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
        assert!(response.body.contains("wrong_node"));
        assert!(!response.body.contains("node-b") && !response.body.contains("node-a"));

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
        assert!(response.body.contains("read_consistency_unavailable"));
        assert!(!response.body.contains("node-"));
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
        assert!(response.body.contains("read_consistency_unavailable"));
        assert!(!response.body.contains("node-"));
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
                Some(&placement_file),
                None,
                "node-b",
                Some(&[0]),
                Some(2),
                16,
                ReadPreference::LeaderOnly,
            )
            .expect("initial placement should load"),
            reload: Some(PlacementReloadRuntime {
                config: PlacementReloadConfig {
                    placement_file: Some(placement_file.clone()),
                    control_plane_base_url: None,
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
            ..TransportBackpressureMetrics::default()
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
        let policy = test_auth_policy(
            None,
            None,
            None,
            None,
            Some("scope-a:tenant-a,tenant-b".to_string()),
        );
        assert_eq!(
            authorize_request_for_tenant(&request, "tenant-b", &policy, Role::Retrieve),
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
        let policy = test_auth_policy(None, None, None, None, Some("scope-a:tenant-a".to_string()));
        assert_eq!(
            authorize_request_for_tenant(&request, "tenant-z", &policy, Role::Retrieve),
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
        let policy = test_auth_policy(
            None,
            None,
            None,
            None,
            Some("scope-a:tenant-a,tenant-b".to_string()),
        );
        assert_eq!(
            authorize_request_for_tenant(&request, "tenant-a", &policy, Role::Retrieve),
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
        let policy = test_auth_policy(Some("secret".to_string()), None, None, None, None);
        assert_eq!(
            authorize_request_for_tenant(&request, "tenant-a", &policy, Role::Retrieve),
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
        let policy = test_auth_policy(
            Some("old-key".to_string()),
            Some("new-key,old-key-2".to_string()),
            None,
            None,
            None,
        );
        assert_eq!(
            authorize_request_for_tenant(&request, "tenant-a", &policy, Role::Retrieve),
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
        let policy = test_auth_policy(
            None,
            None,
            Some("scope-a".to_string()),
            None,
            Some("scope-a:tenant-a".to_string()),
        );
        assert_eq!(
            authorize_request_for_tenant(&request, "tenant-a", &policy, Role::Retrieve),
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

    fn json_post(target: &str, body: &str) -> HttpRequest {
        HttpRequest {
            method: "POST".to_string(),
            target: target.to_string(),
            headers: HashMap::from([("content-type".to_string(), "application/json".to_string())]),
            body: body.as_bytes().to_vec(),
        }
    }

    fn sample_store_with_vector() -> InMemoryStore {
        let mut store = sample_store();
        store
            .upsert_claim_vector("c1", vec![1.0, 0.0, 0.0])
            .unwrap();
        store
    }

    #[test]
    fn retrieve_rejects_invalid_query_embedding_with_specific_400() {
        let store = sample_store_with_vector();
        let metrics = Arc::new(Mutex::new(TransportMetrics::default()));
        let cases = [
            ("[1.0, 0.0]", "dimension mismatch"),
            ("[0.0, 0.0, 0.0]", "non-zero norm"),
        ];
        for (vector, expected) in cases {
            let body = format!(
                r#"{{"tenant_id":"tenant-a","query":"company x","top_k":1,"query_embedding":{vector}}}"#
            );
            let response =
                handle_request_with_metrics(&store, &json_post("/v1/retrieve", &body), &metrics);
            assert_eq!(response.status, 400, "{vector}: {}", response.body);
            assert!(response.body.contains(expected), "{}", response.body);
            assert!(response.body.contains("query_embedding"));
        }
        let ok = handle_request_with_metrics(
            &store,
            &json_post(
                "/v1/retrieve",
                r#"{"tenant_id":"tenant-a","query":"company x","top_k":1,"query_embedding":[1.0,0.0,0.0]}"#,
            ),
            &metrics,
        );
        assert_eq!(ok.status, 200, "{}", ok.body);
        assert!(ok.body.contains("\"claim_id\":\"c1\""));
    }

    #[test]
    fn retrieve_top_k_bound_is_configurable() {
        let _guard = env_lock().lock().unwrap_or_else(|p| p.into_inner());
        let store = sample_store();
        let metrics = Arc::new(Mutex::new(TransportMetrics::default()));
        let prev = std::env::var_os("DASH_RETRIEVAL_MAX_TOP_K");
        set_env_var_for_tests("DASH_RETRIEVAL_MAX_TOP_K", "3");
        let over = handle_request_with_metrics(
            &store,
            &json_post(
                "/v1/retrieve",
                r#"{"tenant_id":"tenant-a","query":"company x","top_k":4}"#,
            ),
            &metrics,
        );
        let at = handle_request_with_metrics(
            &store,
            &json_post(
                "/v1/retrieve",
                r#"{"tenant_id":"tenant-a","query":"company x","top_k":3}"#,
            ),
            &metrics,
        );
        restore_env_var_for_tests("DASH_RETRIEVAL_MAX_TOP_K", prev.as_deref());
        assert_eq!(over.status, 400);
        assert!(over.body.contains("top_k must be <= 3"));
        assert_eq!(at.status, 200);
    }

    #[test]
    fn slow_segment_refresh_does_not_block_store_writer() {
        let _guard = env_lock().lock().unwrap_or_else(|p| p.into_inner());
        let root = std::env::temp_dir().join(format!(
            "dash-retrieve-lockfree-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let tenant_dir = indexer::resolve_tenant_dir(&root, "tenant-a");
        persist_segments_atomic(
            &tenant_dir,
            &[Segment {
                segment_id: "hot-0".into(),
                tier: Tier::Hot,
                claim_ids: vec!["c1".into()],
            }],
        )
        .unwrap();
        crate::api::set_segment_load_delay_for_tests(&tenant_dir, Duration::from_millis(800));
        let prev = std::env::var_os("DASH_RETRIEVAL_SEGMENT_DIR");
        set_env_var_for_tests("DASH_RETRIEVAL_SEGMENT_DIR", root.to_str().unwrap());

        let store = Arc::new(RwLock::new(sample_store()));
        let metrics = Arc::new(Mutex::new(TransportMetrics::default()));
        let reader = {
            let (store, metrics) = (Arc::clone(&store), Arc::clone(&metrics));
            thread::spawn(move || {
                let request = json_post(
                    "/v1/retrieve",
                    r#"{"tenant_id":"tenant-a","query":"company x","top_k":1}"#,
                );
                handle_request_with_metrics_and_reload(&*store, &request, &metrics, None, None)
            })
        };
        // Let the reader reach the (slow) segment refresh.
        thread::sleep(Duration::from_millis(200));
        let started = Instant::now();
        drop(store.write().unwrap());
        let writer_wait = started.elapsed();

        let response = reader.join().unwrap();
        restore_env_var_for_tests("DASH_RETRIEVAL_SEGMENT_DIR", prev.as_deref());
        let _ = std::fs::remove_dir_all(&root);
        assert_eq!(response.status, 200, "{}", response.body);
        assert!(
            writer_wait < Duration::from_millis(400),
            "store writer was blocked {writer_wait:?} behind a segment refresh"
        );
    }

    #[test]
    fn validation_and_auth_failures_do_not_pollute_latency_window() {
        let store = sample_store();
        let metrics = Arc::new(Mutex::new(TransportMetrics::default()));
        // Validation failure: counted, but no latency sample.
        let bad = handle_request_with_metrics(
            &store,
            &json_post("/v1/retrieve", r#"{"tenant_id":"tenant-a","query":""}"#),
            &metrics,
        );
        assert_eq!(bad.status, 400);
        {
            let guard = metrics.lock().unwrap();
            assert_eq!(guard.retrieve_requests_total, 1);
            assert_eq!(guard.retrieve_client_error_total, 1);
            assert!(guard.retrieve_latency_ms_window.is_empty());
        }
        let ok = handle_request_with_metrics(
            &store,
            &json_post(
                "/v1/retrieve",
                r#"{"tenant_id":"tenant-a","query":"company x","top_k":1}"#,
            ),
            &metrics,
        );
        assert_eq!(ok.status, 200);
        let mut guard = metrics.lock().unwrap();
        assert_eq!(guard.retrieve_latency_ms_window.len(), 1);
        // Auth / rate-limit rejections have their own counters.
        guard.observe_retrieve(401, 0.0, 0, None);
        guard.observe_retrieve(429, 0.0, 0, None);
        assert_eq!(guard.retrieve_latency_ms_window.len(), 1);
        assert_eq!(guard.retrieve_auth_denied_total, 1);
        assert_eq!(guard.retrieve_rate_limited_total, 1);
        let rendered = guard.render_prometheus(None, &store::DiskStatus::Available);
        assert!(rendered.contains("dash_retrieve_auth_denied_total 1"));
        assert!(rendered.contains("dash_retrieve_rate_limited_total 1"));
    }

    #[test]
    fn route_latency_histograms_render_cumulative_buckets() {
        let mut metrics = TransportMetrics::default();
        metrics.observe_route_latency("/v1/retrieve", 0.4);
        metrics.observe_route_latency("/v1/retrieve", 7.0);
        metrics.observe_route_latency("/v1/retrieve", 9_000.0);
        metrics.observe_route_latency("/v1/embeddings", 30.0);
        let rendered = metrics.render_prometheus(None, &store::DiskStatus::Available);
        assert!(rendered.contains("# TYPE dash_http_request_duration_ms histogram"));
        assert!(
            rendered
                .contains("dash_http_request_duration_ms_bucket{route=\"retrieve\",le=\"1\"} 1")
        );
        assert!(
            rendered
                .contains("dash_http_request_duration_ms_bucket{route=\"retrieve\",le=\"10\"} 2")
        );
        assert!(
            rendered
                .contains("dash_http_request_duration_ms_bucket{route=\"retrieve\",le=\"5000\"} 2")
        );
        assert!(
            rendered
                .contains("dash_http_request_duration_ms_bucket{route=\"retrieve\",le=\"+Inf\"} 3")
        );
        assert!(rendered.contains("dash_http_request_duration_ms_count{route=\"retrieve\"} 3"));
        assert!(
            rendered
                .contains("dash_http_request_duration_ms_bucket{route=\"embeddings\",le=\"50\"} 1")
        );
        assert!(
            rendered.contains("dash_http_request_duration_ms_sum{route=\"embeddings\"} 30.0000")
        );
    }

    #[test]
    fn routed_requests_populate_route_histograms() {
        let store = sample_store();
        let metrics = Arc::new(Mutex::new(TransportMetrics::default()));
        let _ = handle_request_with_metrics(
            &store,
            &json_post(
                "/v1/retrieve",
                r#"{"tenant_id":"tenant-a","query":"company x","top_k":1}"#,
            ),
            &metrics,
        );
        let guard = metrics.lock().unwrap();
        assert_eq!(
            guard.route_latency.get("retrieve").map(|h| h.count),
            Some(1)
        );
    }

    fn node_with_ingest(ingested_at: &[Option<i64>]) -> EvidenceNode {
        let store = sample_store();
        let response = execute_api_query_with_storage_snapshot(
            &store,
            RetrieveApiRequest {
                tenant_id: "tenant-a".into(),
                query: "company x".into(),
                query_embedding: None,
                entity_filters: vec![],
                embedding_id_filters: vec![],
                top_k: 1,
                stance_mode: StanceMode::Balanced,
                return_graph: false,
                time_range: None,
            },
        )
        .0;
        let mut node = response.results.into_iter().next().unwrap();
        let template = node.citations[0].clone();
        node.citations = ingested_at
            .iter()
            .map(|ts| CitationNode {
                ingested_at: *ts,
                ..template.clone()
            })
            .collect();
        node
    }

    #[test]
    fn ingest_lag_uses_evidence_ingest_time_not_event_time() {
        let now_ms = 1_700_000_100_000;
        // Newest evidence ingested 250 ms ago.
        let node = node_with_ingest(&[Some(now_ms - 5_000), Some(now_ms - 250)]);
        assert_eq!(ingest_lag_ms_at(&[node], now_ms), Some(250.0));
        // No ingest timestamp: no sample rather than a fabricated one.
        let node = node_with_ingest(&[None]);
        assert_eq!(ingest_lag_ms_at(&[node], now_ms), None);
        // Future timestamps (clock skew) are skipped.
        let node = node_with_ingest(&[Some(now_ms + 10_000)]);
        assert_eq!(ingest_lag_ms_at(&[node], now_ms), None);
    }

    fn embeddings_request(body: &str) -> HttpRequest {
        json_post("/v1/embeddings", body)
    }

    fn ready_request() -> HttpRequest {
        HttpRequest {
            method: "GET".to_string(),
            target: "/ready".to_string(),
            headers: HashMap::new(),
            body: Vec::new(),
        }
    }

    /// ROB-12: `/ready` is one JSON document and never carries the raw
    /// follower error; quarantine counts are numbers.
    #[test]
    fn ready_json_is_single_encoded_and_has_no_raw_errors() {
        let _env = env_lock().lock().unwrap_or_else(|p| p.into_inner());
        let store = Arc::new(RwLock::new(sample_store()));
        let config = crate::replication::ReplicationFollowerConfig::new("http://10.9.8.7:8081");
        let status = Arc::new(crate::replication::FollowerStatus::new(&config));
        crate::replication::attach_status_for_tests(&store, &status, 4);
        status.record_failure(
            "failed requesting replication source 'http://10.9.8.7:8081': refused /var/lib/x"
                .to_string(),
        );
        let metrics = Arc::new(Mutex::new(TransportMetrics::default()));
        let response =
            handle_request_with_metrics_and_reload(&*store, &ready_request(), &metrics, None, None);
        assert_eq!(response.status, 503, "{}", response.body);
        let value: serde_json::Value = serde_json::from_str(&response.body).expect("valid JSON");
        assert!(value["replication"].is_object(), "{}", response.body);
        assert_eq!(value["replication"]["last_error"], "source_unreachable");
        assert_eq!(value["replication"]["skipped_records_total"], 4);
        for leaked in ["10.9.8.7", "http://", "refused", "/var/lib", "\\\""] {
            assert!(
                !response.body.contains(leaked),
                "{leaked}: {}",
                response.body
            );
        }

        // Once the follower is healthy the quarantine count is still a number.
        status.record_success();
        let response =
            handle_request_with_metrics_and_reload(&*store, &ready_request(), &metrics, None, None);
        let value: serde_json::Value = serde_json::from_str(&response.body).expect("valid JSON");
        assert_eq!(response.status, 200, "{}", response.body);
        assert_eq!(value["status"], "ready");
        assert_eq!(value["replication"]["skipped_records_total"], 4);
        assert!(value["replication"]["last_error"].is_null());
    }

    #[test]
    fn ready_reports_disk_unavailable_without_the_failure_reason() {
        let _env = env_lock().lock().unwrap_or_else(|p| p.into_inner());
        let dir = std::env::temp_dir().join(format!("dash-ready-disk-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("dir");
        let blocker = dir.join("not-a-directory");
        std::fs::write(&blocker, b"file").expect("blocker file");
        let store = sample_store().attach_disk(blocker.join("store.redb"));
        assert!(matches!(
            store.disk_status(),
            store::DiskStatus::Unavailable { .. }
        ));
        let metrics = Arc::new(Mutex::new(TransportMetrics::default()));
        let previous = std::env::var_os("DASH_RETRIEVAL_PERSISTENCE_PATH");
        set_env_var_for_tests(
            "DASH_RETRIEVAL_PERSISTENCE_PATH",
            &blocker.join("store.redb").to_string_lossy(),
        );
        let response = handle_request_with_metrics(&store, &ready_request(), &metrics);
        restore_env_var_for_tests("DASH_RETRIEVAL_PERSISTENCE_PATH", previous.as_deref());
        let _ = std::fs::remove_dir_all(&dir);

        assert_eq!(response.status, 503, "{}", response.body);
        let value: serde_json::Value = serde_json::from_str(&response.body).expect("valid JSON");
        assert_eq!(value["reason"], "disk_unavailable");
        assert!(
            !response.body.contains("dash-ready-disk"),
            "{}",
            response.body
        );
    }

    /// PERF-07: requests share one provider; only a change of a variable in
    /// the provider environment signature builds a new one.
    #[test]
    fn requests_reuse_one_provider_until_the_environment_signature_changes() {
        let _env = env_lock().lock().unwrap_or_else(|p| p.into_inner());
        let previous = std::env::var_os("DASH_OLLAMA_MODEL");
        let store = sample_store();
        let metrics = Arc::new(Mutex::new(TransportMetrics::default()));
        let embed = || {
            handle_request_with_metrics(
                &store,
                &embeddings_request(r#"{"input":"hello","model":"m"}"#),
                &metrics,
            )
            .status
        };

        set_env_var_for_tests("DASH_OLLAMA_MODEL", "model-a");
        assert_eq!(embed(), 200);
        let after_warmup = embedding_provider_build_count();
        for _ in 0..50 {
            assert_eq!(embed(), 200);
        }
        assert_eq!(
            embedding_provider_build_count(),
            after_warmup,
            "unchanged environment must not rebuild the provider"
        );

        set_env_var_for_tests("DASH_OLLAMA_MODEL", "model-b");
        for _ in 0..50 {
            assert_eq!(embed(), 200);
        }
        assert_eq!(
            embedding_provider_build_count(),
            after_warmup + 1,
            "a signature change must cause exactly one rebuild"
        );
        restore_env_var_for_tests("DASH_OLLAMA_MODEL", previous.as_deref());
    }

    #[test]
    fn embeddings_route_rejects_empty_text_and_too_many_inputs() {
        ensure_dev_mode_env();
        let store = sample_store();
        let metrics = Arc::new(Mutex::new(TransportMetrics::default()));
        let empty = handle_request_with_metrics(
            &store,
            &embeddings_request(r#"{"input":["ok",""],"model":"m"}"#),
            &metrics,
        );
        assert_eq!(empty.status, 400);
        assert!(empty.body.contains("\"error\":{"));
        assert!(empty.body.contains("\"param\":\"input\""));
        assert!(empty.body.contains("\"code\":\"empty_input\""));

        let many = format!(
            r#"{{"input":[{}],"model":"m"}}"#,
            vec!["\"a\""; 2049].join(",")
        );
        let too_many = handle_request_with_metrics(&store, &embeddings_request(&many), &metrics);
        assert_eq!(too_many.status, 400);
        assert!(too_many.body.contains("\"code\":\"too_many_inputs\""));
    }

    #[test]
    fn embeddings_route_token_arrays_are_rejected_unless_enabled() {
        let _guard = crate::openai_embeddings::test_env_lock()
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        ensure_dev_mode_env();
        let store = sample_store();
        let metrics = Arc::new(Mutex::new(TransportMetrics::default()));
        let prev = std::env::var_os("DASH_EMBEDDING_ALLOW_TOKEN_IDS");
        restore_env_var_for_tests("DASH_EMBEDDING_ALLOW_TOKEN_IDS", None);
        let rejected = handle_request_with_metrics(
            &store,
            &embeddings_request(r#"{"input":[[1,2,3]],"model":"m"}"#),
            &metrics,
        );
        set_env_var_for_tests("DASH_EMBEDDING_ALLOW_TOKEN_IDS", "1");
        let accepted = handle_request_with_metrics(
            &store,
            &embeddings_request(r#"{"input":[[1,2,3],[4]],"model":"m"}"#),
            &metrics,
        );
        let flat = handle_request_with_metrics(
            &store,
            &embeddings_request(r#"{"input":[1,2,3],"model":"m"}"#),
            &metrics,
        );
        restore_env_var_for_tests("DASH_EMBEDDING_ALLOW_TOKEN_IDS", prev.as_deref());
        assert_eq!(rejected.status, 400);
        assert!(
            rejected
                .body
                .contains("\"code\":\"unsupported_input_type\"")
        );
        assert!(rejected.body.contains("tokenizer"));
        assert_eq!(accepted.status, 200, "{}", accepted.body);
        assert!(accepted.body.contains("\"index\":1"));
        assert!(accepted.body.contains("\"prompt_tokens\":4"));
        assert_eq!(flat.status, 200, "{}", flat.body);
    }

    struct FailingProvider(embeddings::EmbeddingError);
    impl embeddings::EmbeddingProvider for FailingProvider {
        fn name(&self) -> &str {
            "failing"
        }
        fn dimensions(&self) -> usize {
            4
        }
        fn embed(&self, _texts: &[String]) -> Result<Vec<Vec<f32>>, embeddings::EmbeddingError> {
            let leak = "boom at http://127.0.0.1:11434/api/embed /var/lib/secret".to_string();
            Err(match &self.0 {
                embeddings::EmbeddingError::Timeout(s) => embeddings::EmbeddingError::Timeout(*s),
                embeddings::EmbeddingError::Io(_) => embeddings::EmbeddingError::Io(leak),
                embeddings::EmbeddingError::Parse(_) => embeddings::EmbeddingError::Parse(leak),
                embeddings::EmbeddingError::Overloaded => embeddings::EmbeddingError::Overloaded,
                embeddings::EmbeddingError::Http {
                    status,
                    retry_after_secs,
                    ..
                } => embeddings::EmbeddingError::Http {
                    status: *status,
                    retry_after_secs: *retry_after_secs,
                    body: leak,
                },
                _ => embeddings::EmbeddingError::Http {
                    retry_after_secs: None,
                    status: 500,
                    body: leak,
                },
            })
        }
    }

    #[test]
    fn embeddings_and_retrieve_provider_failures_return_short_codes_without_internals() {
        let store = sample_store();
        let metrics = Arc::new(Mutex::new(TransportMetrics::default()));
        let cases = [
            (
                embeddings::EmbeddingError::Timeout(5),
                503,
                "embedding_unavailable",
            ),
            (
                embeddings::EmbeddingError::Parse(String::new()),
                502,
                "embedding_provider_error",
            ),
            (
                embeddings::EmbeddingError::Io("connection refused".into()),
                503,
                "embedding_unavailable",
            ),
            (
                embeddings::EmbeddingError::Http {
                    status: 429,
                    body: String::new(),
                    retry_after_secs: Some(4),
                },
                503,
                "embedding_unavailable",
            ),
            (
                embeddings::EmbeddingError::Http {
                    status: 500,
                    body: String::new(),
                    retry_after_secs: None,
                },
                503,
                "embedding_unavailable",
            ),
            (
                embeddings::EmbeddingError::Http {
                    status: 400,
                    body: String::new(),
                    retry_after_secs: None,
                },
                502,
                "embedding_provider_error",
            ),
            (
                embeddings::EmbeddingError::Overloaded,
                503,
                "embedding_unavailable",
            ),
        ];
        for (error, status, code) in cases {
            let provider: SharedEmbeddingProvider = Arc::new(FailingProvider(error));
            PROVIDER_OVERRIDE.with(|slot| *slot.borrow_mut() = Some(provider));
            let embeddings_response = handle_request_with_metrics(
                &store,
                &embeddings_request(r#"{"input":"hello","model":"m"}"#),
                &metrics,
            );
            let retrieve = handle_request_with_metrics(
                &store,
                &json_post(
                    "/v1/retrieve",
                    r#"{"tenant_id":"tenant-a","query":"company x","top_k":1}"#,
                ),
                &metrics,
            );
            PROVIDER_OVERRIDE.with(|slot| *slot.borrow_mut() = None);
            for r in [&embeddings_response, &retrieve] {
                assert_eq!(r.status, status, "{}", r.body);
                assert!(r.body.contains(code), "{}", r.body);
                assert_eq!(
                    r.retry_after_secs.is_some(),
                    status == 503,
                    "503 carries Retry-After, 502 does not"
                );
                for leaked in ["127.0.0.1", "http", "/var/lib", "boom"] {
                    assert!(!r.body.contains(leaked), "{}", r.body);
                }
            }
        }
    }

    #[test]
    fn placement_rejection_does_not_leak_topology() {
        let (status, message) = map_read_route_error(&ReadRouteError::WrongNode {
            local_node_id: "node-secret-a".to_string(),
            target_node_id: "node-secret-b".to_string(),
            shard_id: 7,
            epoch: 3,
            role: ReplicaRole::Follower,
        });
        assert_eq!(status, 503);
        assert_eq!(message, "wrong_node");
    }

    #[test]
    fn error_responses_never_contain_filesystem_paths() {
        let _guard = env_lock().lock().unwrap_or_else(|p| p.into_inner());
        let store = sample_store();
        let metrics = Arc::new(Mutex::new(TransportMetrics::default()));
        let root = std::env::temp_dir().join("dash-retrieve-corrupt-segments");
        let tenant_dir = indexer::resolve_tenant_dir(&root, "tenant-a");
        std::fs::create_dir_all(&tenant_dir).unwrap();
        std::fs::write(tenant_dir.join("manifest.json"), b"{not json").unwrap();
        let prev = std::env::var_os("DASH_RETRIEVAL_SEGMENT_DIR");
        set_env_var_for_tests("DASH_RETRIEVAL_SEGMENT_DIR", root.to_str().unwrap());
        let response = handle_request_with_metrics(
            &store,
            &json_post(
                "/v1/retrieve",
                r#"{"tenant_id":"tenant-a","query":"company x","top_k":1}"#,
            ),
            &metrics,
        );
        restore_env_var_for_tests("DASH_RETRIEVAL_SEGMENT_DIR", prev.as_deref());
        let _ = std::fs::remove_dir_all(&root);
        let tmp = std::env::temp_dir().to_string_lossy().to_string();
        assert!(!response.body.contains(&tmp), "{}", response.body);
        assert!(!response.body.contains("manifest"), "{}", response.body);
    }
}
