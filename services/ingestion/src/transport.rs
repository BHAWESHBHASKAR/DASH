use std::{
    collections::{HashMap, HashSet},
    fs::{OpenOptions, create_dir_all},
    io::{BufRead, BufReader, Read, Write},
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, mpsc},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use indexer::{
    CompactionSchedulerConfig, SegmentStoreError, apply_compaction_plan, build_segments,
    persist_segments_atomic, plan_compaction_round,
};
use schema::{Claim, ClaimEdge, Evidence, Relation, Stance};
use store::{CheckpointPolicy, FileWal, InMemoryStore, StoreError};

use crate::{
    IngestInput,
    api::{IngestApiRequest, IngestApiResponse},
    ingest_document, ingest_document_persistent_with_policy,
};

pub struct IngestionRuntime {
    store: InMemoryStore,
    wal: Option<FileWal>,
    checkpoint_policy: CheckpointPolicy,
    segment_runtime: Option<SegmentRuntime>,
    successful_ingests: u64,
    failed_ingests: u64,
    segment_publish_success_total: u64,
    segment_publish_failure_total: u64,
    segment_last_claim_count: usize,
    segment_last_segment_count: usize,
    segment_last_compaction_plans: usize,
    auth_success_total: u64,
    auth_failure_total: u64,
    authz_denied_total: u64,
    audit_events_total: u64,
    audit_write_error_total: u64,
    started_at: Instant,
}

impl IngestionRuntime {
    pub fn in_memory(store: InMemoryStore) -> Self {
        Self {
            store,
            wal: None,
            checkpoint_policy: CheckpointPolicy::default(),
            segment_runtime: SegmentRuntime::from_env(),
            successful_ingests: 0,
            failed_ingests: 0,
            segment_publish_success_total: 0,
            segment_publish_failure_total: 0,
            segment_last_claim_count: 0,
            segment_last_segment_count: 0,
            segment_last_compaction_plans: 0,
            auth_success_total: 0,
            auth_failure_total: 0,
            authz_denied_total: 0,
            audit_events_total: 0,
            audit_write_error_total: 0,
            started_at: Instant::now(),
        }
    }

    pub fn persistent(
        store: InMemoryStore,
        wal: FileWal,
        checkpoint_policy: CheckpointPolicy,
    ) -> Self {
        Self {
            store,
            wal: Some(wal),
            checkpoint_policy,
            segment_runtime: SegmentRuntime::from_env(),
            successful_ingests: 0,
            failed_ingests: 0,
            segment_publish_success_total: 0,
            segment_publish_failure_total: 0,
            segment_last_claim_count: 0,
            segment_last_segment_count: 0,
            segment_last_compaction_plans: 0,
            auth_success_total: 0,
            auth_failure_total: 0,
            authz_denied_total: 0,
            audit_events_total: 0,
            audit_write_error_total: 0,
            started_at: Instant::now(),
        }
    }

    pub fn claims_len(&self) -> usize {
        self.store.claims_len()
    }

    #[cfg(test)]
    fn with_segment_runtime_for_tests(mut self, runtime: Option<SegmentRuntime>) -> Self {
        self.segment_runtime = runtime;
        self
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

        self.successful_ingests += 1;
        if let Some(segment_runtime) = self.segment_runtime.as_ref() {
            match segment_runtime.publish_for_tenant(&self.store, &tenant_id) {
                Ok(stats) => {
                    self.segment_publish_success_total += 1;
                    self.segment_last_claim_count = stats.claim_count;
                    self.segment_last_segment_count = stats.segment_count;
                    self.segment_last_compaction_plans = stats.compaction_plan_count;
                }
                Err(err) => {
                    self.segment_publish_failure_total += 1;
                    eprintln!(
                        "ingestion segment publish failed for tenant '{}': {err:?}",
                        tenant_id
                    );
                }
            }
        }
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

    fn observe_failure(&mut self) {
        self.failed_ingests += 1;
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

    fn metrics_text(&self) -> String {
        format!(
            "# TYPE dash_ingest_success_total counter\n\
dash_ingest_success_total {}\n\
# TYPE dash_ingest_failed_total counter\n\
dash_ingest_failed_total {}\n\
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
# TYPE dash_ingest_claims_total gauge\n\
dash_ingest_claims_total {}\n\
# TYPE dash_ingest_uptime_seconds gauge\n\
dash_ingest_uptime_seconds {:.4}\n",
            self.successful_ingests,
            self.failed_ingests,
            self.segment_publish_success_total,
            self.segment_publish_failure_total,
            self.segment_last_claim_count,
            self.segment_last_segment_count,
            self.segment_last_compaction_plans,
            self.auth_success_total,
            self.auth_failure_total,
            self.authz_denied_total,
            self.audit_events_total,
            self.audit_write_error_total,
            self.store.claims_len(),
            self.started_at.elapsed().as_secs_f64()
        )
    }
}

pub(crate) type SharedRuntime = Arc<Mutex<IngestionRuntime>>;
const MAX_HTTP_BODY_BYTES: usize = 16 * 1024 * 1024;
const SOCKET_TIMEOUT_SECS: u64 = 5;
const DEFAULT_HTTP_WORKERS: usize = 4;

#[derive(Debug, Clone, PartialEq, Eq)]
struct SegmentRuntime {
    root_dir: PathBuf,
    max_segment_size: usize,
    scheduler: CompactionSchedulerConfig,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SegmentPublishStats {
    claim_count: usize,
    segment_count: usize,
    compaction_plan_count: usize,
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
        Some(Self {
            root_dir: PathBuf::from(root_dir),
            max_segment_size,
            scheduler: CompactionSchedulerConfig {
                max_segments_per_tier,
                max_compaction_input_segments,
            },
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
        let manifest = persist_segments_atomic(&tenant_dir, &segments)?;
        Ok(SegmentPublishStats {
            claim_count,
            segment_count: manifest.entries.len(),
            compaction_plan_count: plans.len(),
        })
    }

    fn tenant_segment_dir(&self, tenant_id: &str) -> PathBuf {
        self.root_dir.join(sanitize_path_component(tenant_id))
    }
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
    let runtime = Arc::new(Mutex::new(runtime));
    let (tx, rx) = mpsc::channel::<TcpStream>();
    let rx = Arc::new(Mutex::new(rx));

    std::thread::scope(|scope| {
        for _ in 0..worker_count {
            let runtime = Arc::clone(&runtime);
            let rx = Arc::clone(&rx);
            scope.spawn(move || {
                loop {
                    let stream = {
                        let guard = match rx.lock() {
                            Ok(guard) => guard,
                            Err(_) => break,
                        };
                        match guard.recv() {
                            Ok(stream) => stream,
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
                    if tx.send(stream).is_err() {
                        eprintln!("ingestion transport worker queue closed");
                        break;
                    }
                }
                Err(err) => eprintln!("ingestion transport accept error: {err}"),
            }
        }
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
    let (path, _) = split_target(&request.target);
    let auth_policy = AuthPolicy::from_env(
        env_with_fallback("DASH_INGEST_API_KEY", "EME_INGEST_API_KEY"),
        env_with_fallback("DASH_INGEST_ALLOWED_TENANTS", "EME_INGEST_ALLOWED_TENANTS"),
        env_with_fallback("DASH_INGEST_API_KEY_SCOPES", "EME_INGEST_API_KEY_SCOPES"),
    );
    let audit_log_path =
        env_with_fallback("DASH_INGEST_AUDIT_LOG_PATH", "EME_INGEST_AUDIT_LOG_PATH");
    match (request.method.as_str(), path.as_str()) {
        ("GET", "/health") => HttpResponse::ok_json("{\"status\":\"ok\"}".to_string()),
        ("GET", "/metrics") => {
            let body = match runtime.lock() {
                Ok(rt) => rt.metrics_text(),
                Err(_) => "dash_ingest_metrics_unavailable 1\n".to_string(),
            };
            HttpResponse::ok_text(body)
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
                    let response = match guard.ingest(api_req) {
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
        (_, "/v1/ingest") => HttpResponse::method_not_allowed("only POST is supported"),
        (_, "/health") | (_, "/metrics") => {
            HttpResponse::method_not_allowed("only GET is supported")
        }
        _ => HttpResponse::not_found("unknown path"),
    }
}

fn map_store_error(error: &StoreError) -> (u16, String) {
    match error {
        StoreError::Validation(err) => (400, format!("validation error: {err:?}")),
        StoreError::MissingClaim(claim_id) => (400, format!("missing claim: {claim_id}")),
        StoreError::InvalidVector(message) => (400, format!("invalid vector: {message}")),
        StoreError::Io(message) | StoreError::Parse(message) => {
            (500, format!("internal persistence error: {message}"))
        }
    }
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

#[derive(Debug, Clone, PartialEq, Eq)]
enum AuthDecision {
    Allowed,
    Unauthorized(&'static str),
    Forbidden(&'static str),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AuthPolicy {
    required_api_key: Option<String>,
    allowed_tenants: TenantScope,
    scoped_api_keys: HashMap<String, TenantScope>,
}

impl AuthPolicy {
    fn from_env(
        required_api_key: Option<String>,
        allowed_tenants_raw: Option<String>,
        scoped_api_keys_raw: Option<String>,
    ) -> Self {
        Self {
            required_api_key,
            allowed_tenants: parse_tenant_scope(allowed_tenants_raw.as_deref(), true),
            scoped_api_keys: parse_scoped_api_keys(scoped_api_keys_raw.as_deref()),
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
    let presented_api_key = presented_api_key(request);
    if !policy.scoped_api_keys.is_empty() {
        let Some(api_key) = presented_api_key else {
            return AuthDecision::Unauthorized("missing or invalid API key");
        };
        let Some(scope) = policy.scoped_api_keys.get(api_key) else {
            return AuthDecision::Unauthorized("missing or invalid API key");
        };
        if !scope.allows(tenant_id) {
            return AuthDecision::Forbidden("tenant is not allowed for this API key");
        }
    } else if let Some(required_api_key) = policy.required_api_key.as_deref()
        && !required_api_key.trim().is_empty()
        && presented_api_key != Some(required_api_key)
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
    let value = request.headers.get("authorization")?;
    value.strip_prefix("Bearer ").map(str::trim)
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

#[derive(Debug, Clone, Copy)]
struct AuditEvent<'a> {
    action: &'a str,
    tenant_id: Option<&'a str>,
    claim_id: Option<&'a str>,
    status: u16,
    outcome: &'a str,
    reason: &'a str,
}

fn emit_audit_event(runtime: &SharedRuntime, audit_log_path: Option<&str>, event: AuditEvent<'_>) {
    let timestamp_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or_default();
    let payload = format!(
        "{{\"ts_unix_ms\":{timestamp_ms},\"service\":\"ingestion\",\"action\":\"{}\",\"tenant_id\":{},\"claim_id\":{},\"status\":{},\"outcome\":\"{}\",\"reason\":\"{}\"}}",
        json_escape(event.action),
        event
            .tenant_id
            .map(|value| format!("\"{}\"", json_escape(value)))
            .unwrap_or_else(|| "null".to_string()),
        event
            .claim_id
            .map(|value| format!("\"{}\"", json_escape(value)))
            .unwrap_or_else(|| "null".to_string()),
        event.status,
        json_escape(event.outcome),
        json_escape(event.reason),
    );

    let mut write_error = false;
    if let Some(path) = audit_log_path
        && let Err(err) = append_audit_record(path, &payload)
    {
        write_error = true;
        eprintln!("ingestion audit write failed: {err}");
    }

    if let Ok(mut guard) = runtime.lock() {
        guard.observe_audit_event();
        if write_error {
            guard.observe_audit_write_error();
        }
    }
}

fn append_audit_record(path: &str, payload: &str) -> Result<(), String> {
    if let Some(parent) = Path::new(path).parent()
        && !parent.as_os_str().is_empty()
    {
        create_dir_all(parent).map_err(|e| format!("creating audit directory failed: {e}"))?;
    }
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|e| format!("opening audit file failed: {e}"))?;
    writeln!(file, "{payload}").map_err(|e| format!("appending audit file failed: {e}"))
}

fn build_ingest_request_from_json(body: &str) -> Result<IngestApiRequest, String> {
    let value = parse_json(body)?;
    let object = match value {
        JsonValue::Object(map) => map,
        _ => return Err("request body must be a JSON object".to_string()),
    };

    let claim_obj = require_object(&object, "claim")?;
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
                Some(b',') => self.pos += 1,
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
                Some(b',') => self.pos += 1,
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
            Some(b'0') => self.pos += 1,
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
        500 => "500 Internal Server Error",
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

    fn not_found(message: &str) -> Self {
        Self {
            status: 404,
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

    fn unauthorized(message: &str) -> Self {
        Self {
            status: 401,
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

    fn error_with_status(status: u16, message: &str) -> Self {
        match status {
            400 => Self::bad_request(message),
            401 => Self::unauthorized(message),
            403 => Self::forbidden(message),
            404 => Self::not_found(message),
            405 => Self::method_not_allowed(message),
            _ => Self::internal_server_error(message),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn sample_runtime() -> SharedRuntime {
        Arc::new(Mutex::new(
            IngestionRuntime::in_memory(InMemoryStore::new()),
        ))
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
        let policy =
            AuthPolicy::from_env(None, None, Some("scope-a:tenant-a,tenant-b".to_string()));
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
        let policy = AuthPolicy::from_env(None, None, Some("scope-a:tenant-a".to_string()));
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
        let policy = AuthPolicy::from_env(Some("secret".to_string()), None, None);
        assert_eq!(
            authorize_request_for_tenant(&request, "tenant-a", &policy),
            AuthDecision::Unauthorized("missing or invalid API key")
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
}
