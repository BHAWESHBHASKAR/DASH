use std::{
    collections::{HashMap, HashSet, VecDeque},
    fs::{OpenOptions, create_dir_all},
    io::{BufRead, BufReader, Read, Write},
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicU64, AtomicUsize, Ordering},
        mpsc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use auth::{JwtValidationConfig, JwtValidationError, sha256_hex, verify_hs256_token_for_tenant};
use metadata_router::{
    PlacementRouteError, ReadPreference, ReplicaHealth, ReplicaRole, RoutedReplica, RouterConfig,
    ShardPlacement, load_shard_placements_csv, route_read_with_placement,
    shard_ids_from_placements,
};
use schema::StanceMode;
use store::InMemoryStore;

use crate::api::{
    CitationNode, EvidenceNode, RetrieveApiRequest, RetrievePlannerDebugSnapshot, TimeRange,
    build_retrieve_planner_debug_snapshot, execute_api_query,
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
    storage_last_divergence_ratio: f64,
    storage_last_divergence_warn: bool,
    storage_divergence_warn_total: u64,
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
            storage_last_divergence_ratio: 0.0,
            storage_last_divergence_warn: false,
            storage_divergence_warn_total: 0,
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
        if let ReadRouteError::WrongNode {
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
# TYPE dash_retrieve_storage_last_divergence_ratio gauge\n\
dash_retrieve_storage_last_divergence_ratio {:.6}\n\
# TYPE dash_retrieve_storage_last_divergence_warn gauge\n\
dash_retrieve_storage_last_divergence_warn {}\n\
# TYPE dash_retrieve_storage_divergence_warn_total counter\n\
dash_retrieve_storage_divergence_warn_total {}\n\
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
            self.storage_last_divergence_ratio,
            self.storage_last_divergence_warn as usize,
            self.storage_divergence_warn_total,
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
                        let warn_delta_count = resolve_storage_divergence_warn_delta_count();
                        let warn_ratio = resolve_storage_divergence_warn_ratio();
                        let (warn, reason, ratio) = evaluate_storage_divergence_warning(
                            &snapshot,
                            warn_delta_count,
                            warn_ratio,
                        );
                        if let Ok(mut guard) = metrics.lock() {
                            guard.observe_storage_visibility_debug(&snapshot, ratio, warn);
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
        ("GET", "/v1/retrieve") => match build_retrieve_request_from_query(&query) {
            Ok(req) => {
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
                        let response =
                            execute_retrieve_and_observe(store, req, metrics, placement_routing);
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
            match build_retrieve_request_from_json(body) {
                Ok(req) => {
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

fn render_planner_debug_json(snapshot: &RetrievePlannerDebugSnapshot) -> String {
    format!(
        "{{\"tenant_id\":\"{}\",\"top_k\":{},\"stance_mode\":\"{}\",\"has_query_embedding\":{},\"entity_filter_count\":{},\"embedding_filter_count\":{},\"has_filtering\":{},\"metadata_prefilter_count\":{},\"segment_base_count\":{},\"wal_delta_count\":{},\"storage_visible_count\":{},\"allowed_claim_ids_active\":{},\"allowed_claim_ids_count\":{},\"short_circuit_empty\":{},\"ann_candidate_count\":{},\"planner_candidate_count\":{}}}",
        json_escape(&snapshot.tenant_id),
        snapshot.top_k,
        snapshot.stance_mode,
        snapshot.has_query_embedding,
        snapshot.entity_filter_count,
        snapshot.embedding_filter_count,
        snapshot.has_filtering,
        snapshot.metadata_prefilter_count,
        snapshot.segment_base_count,
        snapshot.wal_delta_count,
        snapshot.storage_visible_count,
        snapshot.allowed_claim_ids_active,
        snapshot.allowed_claim_ids_count,
        snapshot.short_circuit_empty,
        snapshot.ann_candidate_count,
        snapshot.planner_candidate_count,
    )
}

fn storage_divergence_ratio(snapshot: &RetrievePlannerDebugSnapshot) -> f64 {
    if snapshot.storage_visible_count == 0 {
        0.0
    } else {
        snapshot.wal_delta_count as f64 / snapshot.storage_visible_count as f64
    }
}

fn resolve_storage_divergence_warn_delta_count() -> usize {
    parse_env_first_usize(&[
        "DASH_RETRIEVAL_STORAGE_DIVERGENCE_WARN_DELTA_COUNT",
        "EME_RETRIEVAL_STORAGE_DIVERGENCE_WARN_DELTA_COUNT",
    ])
    .filter(|value| *value > 0)
    .unwrap_or(DEFAULT_STORAGE_DIVERGENCE_WARN_DELTA_COUNT)
}

fn resolve_storage_divergence_warn_ratio() -> f64 {
    parse_env_first_f64(&[
        "DASH_RETRIEVAL_STORAGE_DIVERGENCE_WARN_RATIO",
        "EME_RETRIEVAL_STORAGE_DIVERGENCE_WARN_RATIO",
    ])
    .filter(|value| value.is_finite() && *value >= 0.0)
    .unwrap_or(DEFAULT_STORAGE_DIVERGENCE_WARN_RATIO)
}

fn evaluate_storage_divergence_warning(
    snapshot: &RetrievePlannerDebugSnapshot,
    warn_delta_count: usize,
    warn_ratio: f64,
) -> (bool, Option<String>, f64) {
    let ratio = storage_divergence_ratio(snapshot);
    let delta_exceeded = snapshot.wal_delta_count >= warn_delta_count;
    let ratio_exceeded = ratio >= warn_ratio;
    let warn = snapshot.wal_delta_count > 0 && (delta_exceeded || ratio_exceeded);
    let reason = if !warn {
        None
    } else if delta_exceeded && ratio_exceeded {
        Some(format!(
            "wal_delta_count={} exceeds {} and divergence_ratio={:.6} exceeds {:.6}",
            snapshot.wal_delta_count, warn_delta_count, ratio, warn_ratio
        ))
    } else if delta_exceeded {
        Some(format!(
            "wal_delta_count={} exceeds {}",
            snapshot.wal_delta_count, warn_delta_count
        ))
    } else {
        Some(format!(
            "divergence_ratio={:.6} exceeds {:.6}",
            ratio, warn_ratio
        ))
    };
    (warn, reason, ratio)
}

fn render_storage_visibility_debug_json(
    snapshot: &RetrievePlannerDebugSnapshot,
    warn_delta_count: usize,
    warn_ratio: f64,
    warn: bool,
    reason: Option<&str>,
    ratio: f64,
) -> String {
    let reason_json = reason
        .map(|value| format!("\"{}\"", json_escape(value)))
        .unwrap_or_else(|| "null".to_string());
    format!(
        "{{\"tenant_id\":\"{}\",\"segment_base_count\":{},\"wal_delta_count\":{},\"storage_visible_count\":{},\"metadata_prefilter_count\":{},\"allowed_claim_ids_count\":{},\"has_filtering\":{},\"short_circuit_empty\":{},\"divergence_ratio\":{:.6},\"divergence_active\":{},\"divergence_warn\":{},\"warn_delta_count\":{},\"warn_ratio\":{:.6},\"warn_reason\":{}}}",
        json_escape(&snapshot.tenant_id),
        snapshot.segment_base_count,
        snapshot.wal_delta_count,
        snapshot.storage_visible_count,
        snapshot.metadata_prefilter_count,
        snapshot.allowed_claim_ids_count,
        snapshot.has_filtering,
        snapshot.short_circuit_empty,
        ratio,
        snapshot.wal_delta_count > 0,
        warn,
        warn_delta_count,
        warn_ratio,
        reason_json
    )
}

fn render_placement_debug_json(
    placement_routing: Option<&PlacementRoutingRuntime>,
    placement_reload: Option<&PlacementReloadSnapshot>,
    query: &HashMap<String, String>,
) -> String {
    let route_probe = build_read_route_probe_json(placement_routing, query);
    let (enabled, local_node_id, read_preference, shard_count, placements) =
        if let Some(routing) = placement_routing {
            (
                true,
                Some(routing.local_node_id.as_str()),
                Some(read_preference_str(routing.read_preference)),
                routing.router_config.shard_ids.len(),
                routing.placements.as_slice(),
            )
        } else {
            (false, None, None, 0, &[][..])
        };
    format!(
        "{{\"enabled\":{},\"local_node_id\":{},\"read_preference\":{},\"shard_count\":{},\"placements\":{},\"route_probe\":{},\"reload\":{}}}",
        enabled,
        local_node_id
            .map(|value| format!("\"{}\"", json_escape(value)))
            .unwrap_or_else(|| "null".to_string()),
        read_preference
            .map(|value| format!("\"{}\"", value))
            .unwrap_or_else(|| "null".to_string()),
        shard_count,
        render_placements_json(placements),
        route_probe,
        render_placement_reload_json(placement_reload),
    )
}

fn render_placement_reload_json(snapshot: Option<&PlacementReloadSnapshot>) -> String {
    let snapshot = snapshot.cloned().unwrap_or_default();
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

fn build_read_route_probe_json(
    placement_routing: Option<&PlacementRoutingRuntime>,
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
        .or_else(|| query.get("query"))
        .map(String::as_str)
        .unwrap_or_default()
        .trim();
    if entity_key.is_empty() {
        return "{\"status\":\"invalid\",\"reason\":\"entity_key (or query) is required for route probe\"}".to_string();
    }
    let Some(routing) = placement_routing else {
        return "{\"status\":\"unconfigured\",\"reason\":\"placement routing is disabled\"}"
            .to_string();
    };

    match route_read_with_placement(
        tenant_id,
        entity_key,
        &routing.router_config,
        &routing.placements,
        routing.read_preference,
    ) {
        Ok(routed) => {
            let local_admission = routed.node_id == routing.local_node_id;
            let reason = if local_admission {
                "local node is selected replica"
            } else {
                "request would be rejected: local node is not selected replica"
            };
            format!(
                "{{\"status\":\"{}\",\"tenant_id\":\"{}\",\"entity_key\":\"{}\",\"local_node_id\":\"{}\",\"target_node_id\":\"{}\",\"shard_id\":{},\"epoch\":{},\"role\":\"{}\",\"read_preference\":\"{}\",\"local_admission\":{},\"reason\":\"{}\"}}",
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
                read_preference_str(routing.read_preference),
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

fn read_preference_str(value: ReadPreference) -> &'static str {
    match value {
        ReadPreference::LeaderOnly => "leader_only",
        ReadPreference::PreferFollower => "prefer_follower",
        ReadPreference::AnyHealthy => "any_healthy",
    }
}

fn env_with_fallback(primary: &str, fallback: &str) -> Option<String> {
    std::env::var(primary)
        .ok()
        .or_else(|| std::env::var(fallback).ok())
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum AuthDecision {
    Allowed,
    Unauthorized(&'static str),
    Forbidden(&'static str),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AuthPolicy {
    required_api_keys: HashSet<String>,
    revoked_api_keys: HashSet<String>,
    allowed_tenants: TenantScope,
    scoped_api_keys: HashMap<String, TenantScope>,
    jwt_validation: Option<JwtValidationConfig>,
}

impl AuthPolicy {
    fn from_env(
        required_api_key: Option<String>,
        required_api_keys_raw: Option<String>,
        revoked_api_keys_raw: Option<String>,
        allowed_tenants_raw: Option<String>,
        scoped_api_keys_raw: Option<String>,
    ) -> Self {
        Self {
            required_api_keys: parse_api_key_set(
                required_api_key.as_deref(),
                required_api_keys_raw.as_deref(),
            ),
            revoked_api_keys: parse_api_key_set(None, revoked_api_keys_raw.as_deref()),
            allowed_tenants: parse_tenant_scope(allowed_tenants_raw.as_deref(), true),
            scoped_api_keys: parse_scoped_api_keys(scoped_api_keys_raw.as_deref()),
            jwt_validation: parse_jwt_validation_config(
                env_with_fallback(
                    "DASH_RETRIEVAL_JWT_HS256_SECRET",
                    "EME_RETRIEVAL_JWT_HS256_SECRET",
                ),
                env_with_fallback(
                    "DASH_RETRIEVAL_JWT_HS256_SECRETS",
                    "EME_RETRIEVAL_JWT_HS256_SECRETS",
                ),
                env_with_fallback(
                    "DASH_RETRIEVAL_JWT_HS256_SECRETS_BY_KID",
                    "EME_RETRIEVAL_JWT_HS256_SECRETS_BY_KID",
                ),
                env_with_fallback("DASH_RETRIEVAL_JWT_ISSUER", "EME_RETRIEVAL_JWT_ISSUER"),
                env_with_fallback("DASH_RETRIEVAL_JWT_AUDIENCE", "EME_RETRIEVAL_JWT_AUDIENCE"),
                env_with_fallback(
                    "DASH_RETRIEVAL_JWT_LEEWAY_SECS",
                    "EME_RETRIEVAL_JWT_LEEWAY_SECS",
                ),
                env_with_fallback(
                    "DASH_RETRIEVAL_JWT_REQUIRE_EXP",
                    "EME_RETRIEVAL_JWT_REQUIRE_EXP",
                ),
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum TenantScope {
    Any,
    Set(HashSet<String>),
}

impl TenantScope {
    fn allows(&self, tenant_id: &str) -> bool {
        match self {
            Self::Any => true,
            Self::Set(tenants) => tenants.contains(tenant_id),
        }
    }
}

fn authorize_request_for_tenant(
    request: &HttpRequest,
    tenant_id: &str,
    policy: &AuthPolicy,
) -> AuthDecision {
    if let Some(jwt_config) = policy.jwt_validation.as_ref()
        && let Some(token) = presented_bearer_token(request)
        && bearer_looks_like_jwt(token)
    {
        return match verify_hs256_token_for_tenant(token, tenant_id, jwt_config, unix_now_secs()) {
            Ok(()) => {
                if !policy.allowed_tenants.allows(tenant_id) {
                    AuthDecision::Forbidden("tenant is not allowed by service policy")
                } else {
                    AuthDecision::Allowed
                }
            }
            Err(JwtValidationError::TenantNotAllowed) => {
                AuthDecision::Forbidden("tenant is not allowed for this JWT")
            }
            Err(JwtValidationError::Expired) => AuthDecision::Unauthorized("JWT expired"),
            Err(_) => AuthDecision::Unauthorized("invalid JWT"),
        };
    }

    let maybe_api_key = presented_api_key(request);
    if let Some(api_key) = maybe_api_key
        && policy.revoked_api_keys.contains(api_key)
    {
        return AuthDecision::Unauthorized("API key revoked");
    }

    if !policy.scoped_api_keys.is_empty() {
        let Some(api_key) = maybe_api_key else {
            return AuthDecision::Unauthorized("missing or invalid API key");
        };
        if let Some(scope) = policy.scoped_api_keys.get(api_key) {
            if !scope.allows(tenant_id) {
                return AuthDecision::Forbidden("tenant is not allowed for this API key");
            }
        } else if !policy.required_api_keys.is_empty()
            && !policy.required_api_keys.contains(api_key)
        {
            return AuthDecision::Unauthorized("missing or invalid API key");
        }
    } else if !policy.required_api_keys.is_empty()
        && !matches!(maybe_api_key, Some(key) if policy.required_api_keys.contains(key))
    {
        return AuthDecision::Unauthorized("missing or invalid API key");
    }

    if !policy.allowed_tenants.allows(tenant_id) {
        return AuthDecision::Forbidden("tenant is not allowed by service policy");
    }
    AuthDecision::Allowed
}

fn presented_api_key(request: &HttpRequest) -> Option<&str> {
    if let Some(value) = request.headers.get("x-api-key") {
        return Some(value.as_str());
    }
    presented_bearer_token(request)
}

fn presented_bearer_token(request: &HttpRequest) -> Option<&str> {
    let value = request.headers.get("authorization")?;
    value.strip_prefix("Bearer ").map(str::trim)
}

fn bearer_looks_like_jwt(token: &str) -> bool {
    let mut parts = token.split('.');
    let first = parts.next().unwrap_or_default();
    let second = parts.next().unwrap_or_default();
    let third = parts.next().unwrap_or_default();
    parts.next().is_none() && !first.is_empty() && !second.is_empty() && !third.is_empty()
}

fn unix_now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default()
}

fn parse_tenant_scope(raw: Option<&str>, empty_means_any: bool) -> TenantScope {
    let Some(raw) = raw else {
        return TenantScope::Any;
    };
    let mut tenants = HashSet::new();
    for value in raw.split(',') {
        let tenant = value.trim();
        if tenant.is_empty() {
            continue;
        }
        if tenant == "*" {
            return TenantScope::Any;
        }
        tenants.insert(tenant.to_string());
    }
    if tenants.is_empty() && empty_means_any {
        TenantScope::Any
    } else {
        TenantScope::Set(tenants)
    }
}

fn parse_scoped_api_keys(raw: Option<&str>) -> HashMap<String, TenantScope> {
    let mut scoped = HashMap::new();
    let Some(raw) = raw else {
        return scoped;
    };

    for entry in raw.split(';') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        let Some((raw_key, raw_scope)) = entry.split_once(':') else {
            continue;
        };
        let key = raw_key.trim();
        if key.is_empty() {
            continue;
        }
        scoped.insert(
            key.to_string(),
            parse_tenant_scope(Some(raw_scope.trim()), false),
        );
    }
    scoped
}

fn parse_api_key_set(single_key: Option<&str>, raw: Option<&str>) -> HashSet<String> {
    let mut keys = HashSet::new();
    if let Some(single_key) = single_key {
        let key = single_key.trim();
        if !key.is_empty() {
            keys.insert(key.to_string());
        }
    }
    if let Some(raw) = raw {
        for key in raw.split(',') {
            let key = key.trim();
            if key.is_empty() {
                continue;
            }
            keys.insert(key.to_string());
        }
    }
    keys
}

fn parse_jwt_validation_config(
    secret_raw: Option<String>,
    secret_set_raw: Option<String>,
    secrets_by_kid_raw: Option<String>,
    issuer_raw: Option<String>,
    audience_raw: Option<String>,
    leeway_secs_raw: Option<String>,
    require_exp_raw: Option<String>,
) -> Option<JwtValidationConfig> {
    let secret = secret_raw?.trim().to_string();
    if secret.is_empty() {
        return None;
    }
    let leeway_secs = leeway_secs_raw
        .as_deref()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(0);
    let require_exp = parse_bool_env_default(require_exp_raw.as_deref(), true);
    let mut fallback_secrets = parse_secret_list(secret_set_raw.as_deref());
    fallback_secrets.retain(|value| value != &secret);
    let mut seen = HashSet::new();
    fallback_secrets.retain(|value| seen.insert(value.clone()));
    Some(JwtValidationConfig {
        hs256_secret: secret,
        hs256_fallback_secrets: fallback_secrets,
        hs256_secrets_by_kid: parse_jwt_secrets_by_kid(secrets_by_kid_raw.as_deref()),
        issuer: issuer_raw.and_then(|value| {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        }),
        audience: audience_raw.and_then(|value| {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        }),
        leeway_secs,
        require_exp,
    })
}

fn parse_bool_env_default(raw: Option<&str>, default: bool) -> bool {
    let Some(raw) = raw else {
        return default;
    };
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => true,
        "0" | "false" | "no" | "off" => false,
        _ => default,
    }
}

fn parse_secret_list(raw: Option<&str>) -> Vec<String> {
    let Some(raw) = raw else {
        return Vec::new();
    };
    raw.split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn parse_jwt_secrets_by_kid(raw: Option<&str>) -> HashMap<String, String> {
    let mut out = HashMap::new();
    let Some(raw) = raw else {
        return out;
    };
    for entry in raw.split(';') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        let Some((kid_raw, secret_raw)) = entry.split_once(':') else {
            continue;
        };
        let kid = kid_raw.trim();
        let secret = secret_raw.trim();
        if kid.is_empty() || secret.is_empty() {
            continue;
        }
        out.insert(kid.to_string(), secret.to_string());
    }
    out
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

const AUDIT_CHAIN_GENESIS_HASH: &str =
    "0000000000000000000000000000000000000000000000000000000000000000";

#[derive(Debug, Clone, PartialEq, Eq)]
struct AuditChainState {
    next_seq: u64,
    last_hash: String,
}

#[derive(Debug, Clone, Copy)]
struct AuditEvent<'a> {
    action: &'a str,
    tenant_id: Option<&'a str>,
    status: u16,
    outcome: &'a str,
    reason: &'a str,
}

fn append_audit_record(path: &str, timestamp_ms: u64, event: AuditEvent<'_>) -> Result<(), String> {
    if let Some(parent) = Path::new(path).parent()
        && !parent.as_os_str().is_empty()
    {
        create_dir_all(parent).map_err(|e| format!("creating audit directory failed: {e}"))?;
    }
    let mut chain_states = audit_chain_states()
        .lock()
        .map_err(|_| "acquiring audit chain lock failed".to_string())?;
    let state = if let Some(existing) = chain_states.get(path).cloned() {
        existing
    } else {
        let loaded = load_audit_chain_state(path)?;
        chain_states.insert(path.to_string(), loaded.clone());
        loaded
    };
    let (payload, next_state) = render_chained_audit_payload(timestamp_ms, event, &state);
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|e| format!("opening audit file failed: {e}"))?;
    writeln!(file, "{payload}").map_err(|e| format!("appending audit file failed: {e}"))?;
    chain_states.insert(path.to_string(), next_state);
    Ok(())
}

fn render_chained_audit_payload(
    timestamp_ms: u64,
    event: AuditEvent<'_>,
    state: &AuditChainState,
) -> (String, AuditChainState) {
    let seq = state.next_seq;
    let prev_hash = state.last_hash.as_str();
    let canonical = canonical_audit_payload(seq, timestamp_ms, event, prev_hash);
    let hash = sha256_hex(canonical.as_bytes());
    let payload = format!(
        "{{\"seq\":{seq},\"ts_unix_ms\":{timestamp_ms},\"service\":\"retrieval\",\"action\":\"{}\",\"tenant_id\":{},\"claim_id\":null,\"status\":{},\"outcome\":\"{}\",\"reason\":\"{}\",\"prev_hash\":\"{}\",\"hash\":\"{}\"}}",
        json_escape(event.action),
        optional_json_string(event.tenant_id),
        event.status,
        json_escape(event.outcome),
        json_escape(event.reason),
        prev_hash,
        hash,
    );
    (
        payload,
        AuditChainState {
            next_seq: seq.saturating_add(1),
            last_hash: hash,
        },
    )
}

fn canonical_audit_payload(
    seq: u64,
    timestamp_ms: u64,
    event: AuditEvent<'_>,
    prev_hash: &str,
) -> String {
    format!(
        "{{\"seq\":{seq},\"ts_unix_ms\":{timestamp_ms},\"service\":\"retrieval\",\"action\":\"{}\",\"tenant_id\":{},\"claim_id\":null,\"status\":{},\"outcome\":\"{}\",\"reason\":\"{}\",\"prev_hash\":\"{}\"}}",
        json_escape(event.action),
        optional_json_string(event.tenant_id),
        event.status,
        json_escape(event.outcome),
        json_escape(event.reason),
        prev_hash,
    )
}

fn optional_json_string(value: Option<&str>) -> String {
    value
        .map(|raw| format!("\"{}\"", json_escape(raw)))
        .unwrap_or_else(|| "null".to_string())
}

fn audit_chain_states() -> &'static Mutex<HashMap<String, AuditChainState>> {
    static STATES: OnceLock<Mutex<HashMap<String, AuditChainState>>> = OnceLock::new();
    STATES.get_or_init(|| Mutex::new(HashMap::new()))
}

fn load_audit_chain_state(path: &str) -> Result<AuditChainState, String> {
    if !Path::new(path).exists() {
        return Ok(AuditChainState {
            next_seq: 1,
            last_hash: AUDIT_CHAIN_GENESIS_HASH.to_string(),
        });
    }
    let file = OpenOptions::new()
        .read(true)
        .open(path)
        .map_err(|e| format!("opening audit file failed: {e}"))?;
    let reader = BufReader::new(file);
    let mut last_line: Option<String> = None;
    for line in reader.lines() {
        let line = line.map_err(|e| format!("reading audit file failed: {e}"))?;
        if line.trim().is_empty() {
            continue;
        }
        last_line = Some(line);
    }
    let Some(last_line) = last_line else {
        return Ok(AuditChainState {
            next_seq: 1,
            last_hash: AUDIT_CHAIN_GENESIS_HASH.to_string(),
        });
    };
    match parse_audit_chain_state_from_line(&last_line)? {
        Some(state) => Ok(state),
        None => Ok(AuditChainState {
            next_seq: 1,
            last_hash: AUDIT_CHAIN_GENESIS_HASH.to_string(),
        }),
    }
}

fn parse_audit_chain_state_from_line(line: &str) -> Result<Option<AuditChainState>, String> {
    let value = parse_json(line)?;
    let object = match value {
        JsonValue::Object(object) => object,
        _ => return Ok(None),
    };

    let seq_value = object.get("seq");
    let hash_value = object.get("hash");
    if seq_value.is_none() && hash_value.is_none() {
        return Ok(None);
    }
    let seq = match seq_value {
        Some(JsonValue::Number(raw)) => raw
            .parse::<u64>()
            .map_err(|_| "audit seq must be u64".to_string())?,
        _ => return Err("audit seq is missing or invalid".to_string()),
    };
    let hash = match hash_value {
        Some(JsonValue::String(raw)) if is_sha256_hex(raw) => raw.clone(),
        _ => return Err("audit hash is missing or invalid".to_string()),
    };

    Ok(Some(AuditChainState {
        next_seq: seq.saturating_add(1),
        last_hash: hash,
    }))
}

fn is_sha256_hex(raw: &str) -> bool {
    raw.len() == 64 && raw.chars().all(|ch| ch.is_ascii_hexdigit())
}

fn execute_retrieve_and_observe(
    store: &InMemoryStore,
    req: RetrieveApiRequest,
    metrics: &Arc<Mutex<TransportMetrics>>,
    placement_routing: Option<&PlacementRoutingRuntime>,
) -> HttpResponse {
    if let Some(routing) = placement_routing {
        match ensure_local_read_route(routing, &req) {
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
    let response = execute_api_query(store, req);
    let latency_ms = started_at.elapsed().as_secs_f64() * 1000.0;
    let result_count = response.results.len();
    let ingest_to_visible_lag_ms =
        estimate_ingest_to_visible_lag_ms(store, &tenant_id, &response.results);

    if let Ok(mut guard) = metrics.lock() {
        guard.observe_retrieve(200, latency_ms, result_count, ingest_to_visible_lag_ms);
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
            let (raw_key, raw_value) = pair.split_once('=').unwrap_or((pair, ""));
            let key = match url_decode(raw_key) {
                Ok(value) => value,
                Err(_) => continue,
            };
            let value = match url_decode(raw_value) {
                Ok(value) => value,
                Err(_) => continue,
            };
            query.insert(key, value);
        }
    }
    (path.to_string(), query)
}

fn url_decode(raw: &str) -> Result<String, String> {
    let bytes = raw.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' => {
                if i + 2 >= bytes.len() {
                    return Err("incomplete percent escape".to_string());
                }
                let hi = decode_hex(bytes[i + 1])?;
                let lo = decode_hex(bytes[i + 2])?;
                out.push((hi << 4) | lo);
                i += 3;
            }
            other => {
                out.push(other);
                i += 1;
            }
        }
    }

    String::from_utf8(out).map_err(|_| "invalid UTF-8 in URL field".to_string())
}

fn decode_hex(byte: u8) -> Result<u8, String> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err("invalid hex digit".to_string()),
    }
}

fn build_retrieve_request_from_query(
    query: &HashMap<String, String>,
) -> Result<RetrieveApiRequest, String> {
    let tenant_id = query
        .get("tenant_id")
        .ok_or_else(|| "tenant_id is required".to_string())?
        .trim()
        .to_string();
    if tenant_id.is_empty() {
        return Err("tenant_id cannot be empty".to_string());
    }

    let request_query = query
        .get("query")
        .ok_or_else(|| "query is required".to_string())?
        .trim()
        .to_string();
    if request_query.is_empty() {
        return Err("query cannot be empty".to_string());
    }

    let query_embedding = query
        .get("query_embedding")
        .map(|value| parse_query_embedding_csv(value, "query_embedding"))
        .transpose()?;
    let entity_filters = query
        .get("entity_filters")
        .map(|value| parse_csv_string_list(value, "entity_filters"))
        .transpose()?
        .unwrap_or_default();
    let embedding_id_filters = query
        .get("embedding_id_filters")
        .map(|value| parse_csv_string_list(value, "embedding_id_filters"))
        .transpose()?
        .unwrap_or_default();

    let top_k = match query.get("top_k") {
        Some(value) => parse_positive_usize(value, "top_k")?,
        None => 5,
    };

    let stance_mode = match query.get("stance_mode").map(|s| s.as_str()) {
        Some("balanced") | None => StanceMode::Balanced,
        Some("support_only") => StanceMode::SupportOnly,
        Some(_) => return Err("stance_mode must be balanced or support_only".to_string()),
    };

    let return_graph = match query.get("return_graph").map(|s| s.as_str()) {
        Some("true") => true,
        Some("false") | None => false,
        Some(_) => return Err("return_graph must be true or false".to_string()),
    };

    let from_unix = query
        .get("from_unix")
        .map(|value| parse_i64(value, "from_unix"))
        .transpose()?;
    let to_unix = query
        .get("to_unix")
        .map(|value| parse_i64(value, "to_unix"))
        .transpose()?;
    let time_range = if from_unix.is_some() || to_unix.is_some() {
        Some(TimeRange { from_unix, to_unix })
    } else {
        None
    };
    if let Some(TimeRange {
        from_unix: Some(from),
        to_unix: Some(to),
    }) = &time_range
        && from > to
    {
        return Err("time range is invalid: from_unix must be <= to_unix".to_string());
    }

    Ok(RetrieveApiRequest {
        tenant_id,
        query: request_query,
        query_embedding,
        entity_filters,
        embedding_id_filters,
        top_k,
        stance_mode,
        return_graph,
        time_range,
    })
}

fn build_retrieve_request_from_json(body: &str) -> Result<RetrieveApiRequest, String> {
    let value = parse_json(body)?;
    let object = match value {
        JsonValue::Object(map) => map,
        _ => return Err("request body must be a JSON object".to_string()),
    };

    let tenant_id = require_string(&object, "tenant_id")?;
    if tenant_id.trim().is_empty() {
        return Err("tenant_id cannot be empty".to_string());
    }

    let query = require_string(&object, "query")?;
    if query.trim().is_empty() {
        return Err("query cannot be empty".to_string());
    }

    let query_embedding =
        parse_optional_f32_array(object.get("query_embedding"), "query_embedding")?;
    let entity_filters =
        parse_optional_string_array(object.get("entity_filters"), "entity_filters")?
            .unwrap_or_default();
    let embedding_id_filters =
        parse_optional_string_array(object.get("embedding_id_filters"), "embedding_id_filters")?
            .unwrap_or_default();

    let top_k = match object.get("top_k") {
        Some(JsonValue::Number(raw)) => parse_positive_usize(raw, "top_k")?,
        Some(_) => return Err("top_k must be a positive integer".to_string()),
        None => 5,
    };

    let stance_mode = match object.get("stance_mode") {
        Some(JsonValue::String(mode)) => parse_stance_mode(mode)?,
        Some(_) => return Err("stance_mode must be a string".to_string()),
        None => StanceMode::Balanced,
    };

    let return_graph = match object.get("return_graph") {
        Some(JsonValue::Bool(flag)) => *flag,
        Some(_) => return Err("return_graph must be a boolean".to_string()),
        None => false,
    };

    let time_range = match object.get("time_range") {
        Some(JsonValue::Object(range_obj)) => {
            let from_unix = match range_obj.get("from_unix") {
                Some(JsonValue::Number(raw)) => Some(parse_i64(raw, "time_range.from_unix")?),
                Some(JsonValue::Null) | None => None,
                Some(_) => {
                    return Err("time_range.from_unix must be an i64 timestamp".to_string());
                }
            };
            let to_unix = match range_obj.get("to_unix") {
                Some(JsonValue::Number(raw)) => Some(parse_i64(raw, "time_range.to_unix")?),
                Some(JsonValue::Null) | None => None,
                Some(_) => {
                    return Err("time_range.to_unix must be an i64 timestamp".to_string());
                }
            };

            if from_unix.is_some() || to_unix.is_some() {
                Some(TimeRange { from_unix, to_unix })
            } else {
                None
            }
        }
        Some(JsonValue::Null) | None => None,
        Some(_) => return Err("time_range must be an object or null".to_string()),
    };
    if let Some(TimeRange {
        from_unix: Some(from),
        to_unix: Some(to),
    }) = &time_range
        && from > to
    {
        return Err("time range is invalid: from_unix must be <= to_unix".to_string());
    }

    Ok(RetrieveApiRequest {
        tenant_id,
        query,
        query_embedding,
        entity_filters,
        embedding_id_filters,
        top_k,
        stance_mode,
        return_graph,
        time_range,
    })
}

fn parse_stance_mode(raw: &str) -> Result<StanceMode, String> {
    match raw {
        "balanced" => Ok(StanceMode::Balanced),
        "support_only" => Ok(StanceMode::SupportOnly),
        _ => Err("stance_mode must be balanced or support_only".to_string()),
    }
}

fn require_string(map: &HashMap<String, JsonValue>, key: &str) -> Result<String, String> {
    match map.get(key) {
        Some(JsonValue::String(value)) => Ok(value.clone()),
        Some(_) => Err(format!("{key} must be a string")),
        None => Err(format!("{key} is required")),
    }
}

fn parse_positive_usize(raw: &str, field_name: &str) -> Result<usize, String> {
    if raw.contains('.') || raw.contains('e') || raw.contains('E') || raw.starts_with('-') {
        return Err(format!("{field_name} must be a positive integer"));
    }
    let parsed = raw
        .parse::<usize>()
        .map_err(|_| format!("{field_name} must be a positive integer"))?;
    if parsed == 0 {
        return Err(format!("{field_name} must be > 0"));
    }
    Ok(parsed)
}

fn parse_i64(raw: &str, field_name: &str) -> Result<i64, String> {
    if raw.contains('.') || raw.contains('e') || raw.contains('E') {
        return Err(format!("{field_name} must be an i64 timestamp"));
    }
    raw.parse::<i64>()
        .map_err(|_| format!("{field_name} must be an i64 timestamp"))
}

fn parse_query_embedding_csv(raw: &str, field_name: &str) -> Result<Vec<f32>, String> {
    let values: Vec<&str> = raw
        .split(',')
        .map(|part| part.trim())
        .filter(|part| !part.is_empty())
        .collect();
    if values.is_empty() {
        return Err(format!("{field_name} must not be empty"));
    }
    let mut out = Vec::with_capacity(values.len());
    for part in values {
        let parsed = part
            .parse::<f32>()
            .map_err(|_| format!("{field_name} must contain only numbers"))?;
        if !parsed.is_finite() {
            return Err(format!("{field_name} values must be finite"));
        }
        out.push(parsed);
    }
    Ok(out)
}

fn parse_csv_string_list(raw: &str, field_name: &str) -> Result<Vec<String>, String> {
    let out: Vec<String> = raw
        .split(',')
        .map(|part| part.trim())
        .filter(|part| !part.is_empty())
        .map(ToOwned::to_owned)
        .collect();
    if out.is_empty() {
        return Err(format!("{field_name} must not be empty"));
    }
    Ok(out)
}

fn parse_optional_f32_array(
    value: Option<&JsonValue>,
    field_name: &str,
) -> Result<Option<Vec<f32>>, String> {
    match value {
        None | Some(JsonValue::Null) => Ok(None),
        Some(JsonValue::Array(items)) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                let raw = match item {
                    JsonValue::Number(raw) => raw,
                    _ => return Err(format!("{field_name} must be an array of numbers")),
                };
                let parsed = raw
                    .parse::<f32>()
                    .map_err(|_| format!("{field_name} must be an array of numbers"))?;
                if !parsed.is_finite() {
                    return Err(format!("{field_name} values must be finite"));
                }
                out.push(parsed);
            }
            if out.is_empty() {
                return Err(format!("{field_name} must not be empty when provided"));
            }
            Ok(Some(out))
        }
        Some(_) => Err(format!("{field_name} must be an array or null")),
    }
}

fn parse_optional_string_array(
    value: Option<&JsonValue>,
    field_name: &str,
) -> Result<Option<Vec<String>>, String> {
    match value {
        None | Some(JsonValue::Null) => Ok(None),
        Some(JsonValue::Array(items)) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                let value = match item {
                    JsonValue::String(value) => value,
                    _ => return Err(format!("{field_name} must be an array of strings")),
                };
                let trimmed = value.trim();
                if trimmed.is_empty() {
                    return Err(format!("{field_name} must not contain empty strings"));
                }
                out.push(trimmed.to_string());
            }
            Ok(Some(out))
        }
        Some(_) => Err(format!("{field_name} must be an array or null")),
    }
}

#[derive(Debug, Clone, PartialEq)]
enum JsonValue {
    Object(HashMap<String, JsonValue>),
    Array(Vec<JsonValue>),
    String(String),
    Number(String),
    Bool(bool),
    Null,
}

fn parse_json(input: &str) -> Result<JsonValue, String> {
    let mut parser = JsonParser::new(input);
    let value = parser.parse_value()?;
    parser.skip_whitespace();
    if !parser.is_eof() {
        return Err("unexpected trailing JSON content".to_string());
    }
    Ok(value)
}

struct JsonParser<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> JsonParser<'a> {
    fn new(input: &'a str) -> Self {
        Self {
            bytes: input.as_bytes(),
            pos: 0,
        }
    }

    fn parse_value(&mut self) -> Result<JsonValue, String> {
        self.skip_whitespace();
        match self.peek_byte() {
            Some(b'{') => self.parse_object(),
            Some(b'[') => self.parse_array(),
            Some(b'"') => self.parse_string().map(JsonValue::String),
            Some(b't') => {
                self.expect_literal("true")?;
                Ok(JsonValue::Bool(true))
            }
            Some(b'f') => {
                self.expect_literal("false")?;
                Ok(JsonValue::Bool(false))
            }
            Some(b'n') => {
                self.expect_literal("null")?;
                Ok(JsonValue::Null)
            }
            Some(b'-' | b'0'..=b'9') => self.parse_number().map(JsonValue::Number),
            Some(_) => Err("unsupported JSON token".to_string()),
            None => Err("empty JSON payload".to_string()),
        }
    }

    fn parse_object(&mut self) -> Result<JsonValue, String> {
        self.expect_byte(b'{')?;
        self.skip_whitespace();

        let mut map = HashMap::new();
        if self.peek_byte() == Some(b'}') {
            self.pos += 1;
            return Ok(JsonValue::Object(map));
        }

        loop {
            self.skip_whitespace();
            let key = self.parse_string()?;
            self.skip_whitespace();
            self.expect_byte(b':')?;
            let value = self.parse_value()?;
            map.insert(key, value);
            self.skip_whitespace();

            match self.peek_byte() {
                Some(b',') => {
                    self.pos += 1;
                }
                Some(b'}') => {
                    self.pos += 1;
                    break;
                }
                _ => return Err("invalid JSON object".to_string()),
            }
        }

        Ok(JsonValue::Object(map))
    }

    fn parse_array(&mut self) -> Result<JsonValue, String> {
        self.expect_byte(b'[')?;
        self.skip_whitespace();

        let mut items = Vec::new();
        if self.peek_byte() == Some(b']') {
            self.pos += 1;
            return Ok(JsonValue::Array(items));
        }

        loop {
            let value = self.parse_value()?;
            items.push(value);
            self.skip_whitespace();
            match self.peek_byte() {
                Some(b',') => {
                    self.pos += 1;
                }
                Some(b']') => {
                    self.pos += 1;
                    break;
                }
                _ => return Err("invalid JSON array".to_string()),
            }
        }

        Ok(JsonValue::Array(items))
    }

    fn parse_string(&mut self) -> Result<String, String> {
        self.expect_byte(b'"')?;
        let mut out = String::new();

        while let Some(byte) = self.next_byte() {
            match byte {
                b'"' => return Ok(out),
                b'\\' => {
                    let escaped = self
                        .next_byte()
                        .ok_or_else(|| "unterminated JSON escape".to_string())?;
                    match escaped {
                        b'"' => out.push('"'),
                        b'\\' => out.push('\\'),
                        b'/' => out.push('/'),
                        b'b' => out.push('\u{0008}'),
                        b'f' => out.push('\u{000C}'),
                        b'n' => out.push('\n'),
                        b'r' => out.push('\r'),
                        b't' => out.push('\t'),
                        b'u' => {
                            let code = self.parse_hex4()?;
                            let ch = char::from_u32(code)
                                .ok_or_else(|| "invalid unicode escape".to_string())?;
                            out.push(ch);
                        }
                        _ => return Err("invalid JSON escape sequence".to_string()),
                    }
                }
                b if b.is_ascii_control() => {
                    return Err("unescaped control character in JSON string".to_string());
                }
                b => out.push(b as char),
            }
        }

        Err("unterminated JSON string".to_string())
    }

    fn parse_number(&mut self) -> Result<String, String> {
        let start = self.pos;

        if self.peek_byte() == Some(b'-') {
            self.pos += 1;
        }

        match self.peek_byte() {
            Some(b'0') => {
                self.pos += 1;
            }
            Some(b'1'..=b'9') => {
                self.pos += 1;
                while matches!(self.peek_byte(), Some(b'0'..=b'9')) {
                    self.pos += 1;
                }
            }
            _ => return Err("invalid JSON number".to_string()),
        }

        if self.peek_byte() == Some(b'.') {
            self.pos += 1;
            if !matches!(self.peek_byte(), Some(b'0'..=b'9')) {
                return Err("invalid JSON number".to_string());
            }
            while matches!(self.peek_byte(), Some(b'0'..=b'9')) {
                self.pos += 1;
            }
        }

        if matches!(self.peek_byte(), Some(b'e' | b'E')) {
            self.pos += 1;
            if matches!(self.peek_byte(), Some(b'+' | b'-')) {
                self.pos += 1;
            }
            if !matches!(self.peek_byte(), Some(b'0'..=b'9')) {
                return Err("invalid JSON number".to_string());
            }
            while matches!(self.peek_byte(), Some(b'0'..=b'9')) {
                self.pos += 1;
            }
        }

        let raw = std::str::from_utf8(&self.bytes[start..self.pos])
            .map_err(|_| "invalid JSON number".to_string())?;
        Ok(raw.to_string())
    }

    fn parse_hex4(&mut self) -> Result<u32, String> {
        let mut value: u32 = 0;
        for _ in 0..4 {
            let byte = self
                .next_byte()
                .ok_or_else(|| "incomplete unicode escape".to_string())?;
            value = (value << 4)
                + match byte {
                    b'0'..=b'9' => (byte - b'0') as u32,
                    b'a'..=b'f' => (byte - b'a' + 10) as u32,
                    b'A'..=b'F' => (byte - b'A' + 10) as u32,
                    _ => return Err("invalid unicode escape".to_string()),
                };
        }
        Ok(value)
    }

    fn expect_literal(&mut self, literal: &str) -> Result<(), String> {
        let bytes = literal.as_bytes();
        if self.bytes.get(self.pos..self.pos + bytes.len()) == Some(bytes) {
            self.pos += bytes.len();
            Ok(())
        } else {
            Err("invalid JSON literal".to_string())
        }
    }

    fn expect_byte(&mut self, expected: u8) -> Result<(), String> {
        match self.next_byte() {
            Some(byte) if byte == expected => Ok(()),
            _ => Err("invalid JSON syntax".to_string()),
        }
    }

    fn skip_whitespace(&mut self) {
        while matches!(self.peek_byte(), Some(b' ' | b'\n' | b'\r' | b'\t')) {
            self.pos += 1;
        }
    }

    fn is_eof(&self) -> bool {
        self.pos >= self.bytes.len()
    }

    fn peek_byte(&self) -> Option<u8> {
        self.bytes.get(self.pos).copied()
    }

    fn next_byte(&mut self) -> Option<u8> {
        let out = self.peek_byte()?;
        self.pos += 1;
        Some(out)
    }
}

fn render_retrieve_response_json(resp: &crate::api::RetrieveApiResponse) -> String {
    let mut out = String::new();
    out.push_str("{\"results\":[");
    for (idx, node) in resp.results.iter().enumerate() {
        if idx > 0 {
            out.push(',');
        }
        render_evidence_node_json(&mut out, node);
    }
    out.push_str("],\"graph\":");

    if let Some(graph) = &resp.graph {
        out.push_str("{\"nodes\":[");
        for (idx, node) in graph.nodes.iter().enumerate() {
            if idx > 0 {
                out.push(',');
            }
            render_evidence_node_json(&mut out, node);
        }
        out.push_str("],\"edges\":[");
        for (idx, edge) in graph.edges.iter().enumerate() {
            if idx > 0 {
                out.push(',');
            }
            out.push('{');
            out.push_str("\"from_claim_id\":\"");
            out.push_str(&json_escape(&edge.from_claim_id));
            out.push_str("\",\"to_claim_id\":\"");
            out.push_str(&json_escape(&edge.to_claim_id));
            out.push_str("\",\"relation\":\"");
            out.push_str(&json_escape(&edge.relation));
            out.push_str("\",\"strength\":");
            out.push_str(&format!("{:.6}", edge.strength));
            out.push('}');
        }
        out.push_str("]}");
    } else {
        out.push_str("null");
    }

    out.push('}');
    out
}

fn render_evidence_node_json(out: &mut String, node: &crate::api::EvidenceNode) {
    out.push('{');
    out.push_str("\"claim_id\":\"");
    out.push_str(&json_escape(&node.claim_id));
    out.push_str("\",\"canonical_text\":\"");
    out.push_str(&json_escape(&node.canonical_text));
    out.push_str("\",\"score\":");
    out.push_str(&format!("{:.6}", node.score));
    out.push_str(",\"claim_confidence\":");
    render_optional_f32(out, node.claim_confidence);
    out.push_str(",\"confidence_band\":");
    render_optional_string(out, node.confidence_band.as_deref());
    out.push_str(",\"supports\":");
    out.push_str(&node.supports.to_string());
    out.push_str(",\"contradicts\":");
    out.push_str(&node.contradicts.to_string());
    out.push_str(",\"citations\":");
    out.push_str(&render_citations_json(&node.citations));
    out.push_str(",\"event_time_unix\":");
    render_optional_i64(out, node.event_time_unix);
    out.push_str(",\"claim_type\":");
    render_optional_string(out, node.claim_type.as_deref());
    out.push_str(",\"valid_from\":");
    render_optional_i64(out, node.valid_from);
    out.push_str(",\"valid_to\":");
    render_optional_i64(out, node.valid_to);
    out.push_str(",\"created_at\":");
    render_optional_i64(out, node.created_at);
    out.push_str(",\"updated_at\":");
    render_optional_i64(out, node.updated_at);
    out.push('}');
}

fn render_optional_i64(out: &mut String, value: Option<i64>) {
    if let Some(value) = value {
        out.push_str(&value.to_string());
    } else {
        out.push_str("null");
    }
}

fn render_optional_f32(out: &mut String, value: Option<f32>) {
    if let Some(value) = value {
        out.push_str(&format!("{:.6}", value));
    } else {
        out.push_str("null");
    }
}

fn render_optional_string(out: &mut String, value: Option<&str>) {
    if let Some(value) = value {
        out.push('"');
        out.push_str(&json_escape(value));
        out.push('"');
    } else {
        out.push_str("null");
    }
}

fn render_citations_json(citations: &[CitationNode]) -> String {
    let mut out = String::new();
    out.push('[');
    for (idx, citation) in citations.iter().enumerate() {
        if idx > 0 {
            out.push(',');
        }
        out.push('{');
        out.push_str("\"evidence_id\":\"");
        out.push_str(&json_escape(&citation.evidence_id));
        out.push_str("\",\"source_id\":\"");
        out.push_str(&json_escape(&citation.source_id));
        out.push_str("\",\"stance\":\"");
        out.push_str(&json_escape(&citation.stance));
        out.push_str("\",\"source_quality\":");
        out.push_str(&format!("{:.6}", citation.source_quality));
        out.push_str(",\"chunk_id\":");
        if let Some(chunk_id) = &citation.chunk_id {
            out.push('"');
            out.push_str(&json_escape(chunk_id));
            out.push('"');
        } else {
            out.push_str("null");
        }
        out.push_str(",\"span_start\":");
        if let Some(span_start) = citation.span_start {
            out.push_str(&span_start.to_string());
        } else {
            out.push_str("null");
        }
        out.push_str(",\"span_end\":");
        if let Some(span_end) = citation.span_end {
            out.push_str(&span_end.to_string());
        } else {
            out.push_str("null");
        }
        out.push_str(",\"doc_id\":");
        render_optional_string(&mut out, citation.doc_id.as_deref());
        out.push_str(",\"extraction_model\":");
        render_optional_string(&mut out, citation.extraction_model.as_deref());
        out.push_str(",\"ingested_at\":");
        render_optional_i64(&mut out, citation.ingested_at);
        out.push('}');
    }
    out.push(']');
    out
}

fn json_escape(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for ch in raw.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            _ => out.push(ch),
        }
    }
    out
}

fn write_response(stream: &mut TcpStream, response: HttpResponse) -> std::io::Result<()> {
    stream.write_all(render_response_text(&response).as_bytes())?;
    stream.flush()
}

fn render_response_text(response: &HttpResponse) -> String {
    let status_text = match response.status {
        200 => "200 OK",
        400 => "400 Bad Request",
        401 => "401 Unauthorized",
        403 => "403 Forbidden",
        404 => "404 Not Found",
        405 => "405 Method Not Allowed",
        503 => "503 Service Unavailable",
        _ => "500 Internal Server Error",
    };
    let body_len = response.body.len();
    format!(
        "HTTP/1.1 {status_text}\r\nContent-Type: {}\r\nContent-Length: {body_len}\r\nConnection: close\r\n\r\n{}",
        response.content_type, response.body
    )
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
        assert!(debug_response.body.contains("\"segment_base_count\":1"));
        assert!(debug_response.body.contains("\"wal_delta_count\":1"));
        assert!(debug_response.body.contains("\"storage_visible_count\":2"));
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
                .contains("dash_retrieve_storage_last_divergence_warn 1")
        );
        assert!(
            metrics_response
                .body
                .contains("dash_retrieve_storage_divergence_warn_total 1")
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
