use std::{
    collections::HashMap,
    fs,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use auth::sha256_hex;
use metadata_router::{
    PlacementRouteError, ReplicaRole, ShardPlacement, parse_shard_placements_csv,
    promote_replica_to_leader, render_shard_placements_csv,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlPlanePersistence {
    state_path: PathBuf,
    checksum_path: Option<PathBuf>,
}

impl ControlPlanePersistence {
    pub fn new(state_path: PathBuf, checksum_path: Option<PathBuf>) -> Result<Self, String> {
        if state_path.as_os_str().is_empty() {
            return Err("control-plane persistence state path must not be empty".to_string());
        }
        Ok(Self {
            state_path,
            checksum_path,
        })
    }

    pub fn state_path(&self) -> &Path {
        &self.state_path
    }

    pub fn checksum_path(&self) -> Option<&Path> {
        self.checksum_path.as_deref()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ControlPlanePlacementState {
    placements: Vec<ShardPlacement>,
    persistence: Option<ControlPlanePersistence>,
}

impl ControlPlanePlacementState {
    pub fn new(placements: Vec<ShardPlacement>) -> Self {
        Self {
            placements,
            persistence: None,
        }
    }

    pub fn with_persistence(mut self, persistence: ControlPlanePersistence) -> Self {
        self.persistence = Some(persistence);
        self
    }

    pub fn load_persisted_csv(path: &Path) -> Result<Vec<ShardPlacement>, String> {
        let csv = fs::read_to_string(path).map_err(|err| {
            format!(
                "failed reading control-plane state '{}': {err}",
                path.display()
            )
        })?;
        parse_shard_placements_csv(&csv)
    }

    pub fn verify_checksum_if_configured(&self) -> Result<(), String> {
        let Some(persistence) = self.persistence.as_ref() else {
            return Ok(());
        };
        let Some(checksum_path) = persistence.checksum_path() else {
            return Ok(());
        };
        if !persistence.state_path().exists() {
            return Ok(());
        }
        if !checksum_path.exists() {
            return Err(format!(
                "control-plane checksum file '{}' is missing",
                checksum_path.display()
            ));
        }
        let state_bytes = fs::read(persistence.state_path()).map_err(|err| {
            format!(
                "failed reading control-plane state '{}' for checksum verification: {err}",
                persistence.state_path().display()
            )
        })?;
        let expected = fs::read_to_string(checksum_path).map_err(|err| {
            format!(
                "failed reading control-plane checksum '{}': {err}",
                checksum_path.display()
            )
        })?;
        let actual = sha256_hex(&state_bytes);
        if actual != expected.trim() {
            return Err(format!(
                "control-plane checksum mismatch for '{}' (expected {}, actual {})",
                persistence.state_path().display(),
                expected.trim(),
                actual
            ));
        }
        Ok(())
    }

    pub fn placements(&self) -> &[ShardPlacement] {
        &self.placements
    }

    pub fn highest_epoch(&self) -> u64 {
        self.placements
            .iter()
            .map(|placement| placement.epoch)
            .max()
            .unwrap_or(0)
    }

    pub fn persistence_state_path(&self) -> Option<&Path> {
        self.persistence.as_ref().map(|cfg| cfg.state_path())
    }

    pub fn replace_placements_monotonic(
        &mut self,
        candidate: Vec<ShardPlacement>,
    ) -> Result<(), String> {
        let mut current_epochs: HashMap<(String, u32), u64> = HashMap::new();
        for placement in &self.placements {
            current_epochs.insert(
                (placement.tenant_id.clone(), placement.shard_id),
                placement.epoch,
            );
        }

        for placement in &candidate {
            let key = (placement.tenant_id.clone(), placement.shard_id);
            if let Some(current_epoch) = current_epochs.get(&key)
                && placement.epoch < *current_epoch
            {
                return Err(format!(
                    "epoch regression for tenant '{}' shard {}: current={}, candidate={}",
                    placement.tenant_id, placement.shard_id, current_epoch, placement.epoch
                ));
            }
        }

        self.placements = candidate;
        Ok(())
    }

    pub fn replace_placements_from_csv_monotonic(&mut self, csv: &str) -> Result<(), String> {
        let candidate = parse_shard_placements_csv(csv)?;
        self.replace_placements_monotonic(candidate)
    }

    pub fn cas_matches(&self, expected_epoch: Option<u64>) -> Result<(), String> {
        if let Some(expected_epoch) = expected_epoch {
            let current_epoch = self.highest_epoch();
            if expected_epoch != current_epoch {
                return Err(format!(
                    "stale placement epoch: expected={}, current={}",
                    expected_epoch, current_epoch
                ));
            }
        }
        Ok(())
    }

    pub fn promote_replica(
        &mut self,
        tenant_id: &str,
        shard_id: u32,
        node_id: &str,
    ) -> Result<u64, String> {
        let placement = self
            .placements
            .iter_mut()
            .find(|placement| placement.tenant_id == tenant_id && placement.shard_id == shard_id)
            .ok_or_else(|| {
                format!(
                    "placement not found for tenant '{}' shard {}",
                    tenant_id, shard_id
                )
            })?;

        promote_replica_to_leader(placement, node_id).map_err(|err| match err {
            PlacementRouteError::ReplicaNotFound { .. } => {
                format!("replica '{}' not found in shard {}", node_id, shard_id)
            }
            PlacementRouteError::ReplicaUnhealthy { .. } => {
                format!("replica '{}' is not promotable due to health", node_id)
            }
            other => format!("promotion failed: {other:?}"),
        })
    }

    pub fn persist_if_configured(&self) -> Result<(), String> {
        let Some(persistence) = self.persistence.as_ref() else {
            return Ok(());
        };
        let csv = render_shard_placements_csv(&self.placements);
        persist_atomically(persistence.state_path(), csv.as_bytes())?;
        if let Some(checksum_path) = persistence.checksum_path() {
            let digest = sha256_hex(csv.as_bytes());
            persist_atomically(checksum_path, format!("{digest}\n").as_bytes())?;
        }
        Ok(())
    }

    fn render_placement_json(&self) -> String {
        let placements_json = self
            .placements
            .iter()
            .map(|placement| {
                let replicas_json = placement
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
                    replicas_json,
                )
            })
            .collect::<Vec<_>>()
            .join(",");

        format!(
            "{{\"placements\":[{}],\"placement_count\":{}}}",
            placements_json,
            self.placements.len()
        )
    }
}

pub fn serve_http(
    bind_addr: &str,
    state: Arc<Mutex<ControlPlanePlacementState>>,
) -> std::io::Result<()> {
    let listener = TcpListener::bind(bind_addr)?;
    for stream in listener.incoming() {
        let state = Arc::clone(&state);
        match stream {
            Ok(stream) => {
                std::thread::spawn(move || {
                    let _ = handle_connection(stream, state);
                });
            }
            Err(err) => eprintln!("control-plane accept error: {err}"),
        }
    }
    Ok(())
}

pub fn handle_http_request_bytes(
    state: &Arc<Mutex<ControlPlanePlacementState>>,
    raw_request: &[u8],
) -> Result<Vec<u8>, String> {
    let request_text =
        std::str::from_utf8(raw_request).map_err(|_| "request must be valid UTF-8".to_string())?;
    let request = parse_http_request_text(request_text)?;
    let response = handle_request(state, request);
    Ok(render_response_text(&response).into_bytes())
}

fn handle_connection(
    mut stream: TcpStream,
    state: Arc<Mutex<ControlPlanePlacementState>>,
) -> std::io::Result<()> {
    let mut request_bytes = Vec::new();
    stream.read_to_end(&mut request_bytes)?;
    let response = match handle_http_request_bytes(&state, &request_bytes) {
        Ok(bytes) => bytes,
        Err(reason) => render_response_text(&HttpResponse::bad_request(&reason)).into_bytes(),
    };
    stream.write_all(&response)?;
    stream.flush()
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct HttpRequest {
    method: String,
    target: String,
    body: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct HttpResponse {
    status: u16,
    content_type: &'static str,
    body: String,
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
            content_type: "text/plain; charset=utf-8",
            body,
        }
    }

    fn error(status: u16, reason: &str) -> Self {
        Self {
            status,
            content_type: "application/json",
            body: format!("{{\"error\":\"{}\"}}", json_escape(reason)),
        }
    }

    fn bad_request(reason: &str) -> Self {
        Self::error(400, reason)
    }

    fn not_found(reason: &str) -> Self {
        Self::error(404, reason)
    }

    fn method_not_allowed(reason: &str) -> Self {
        Self::error(405, reason)
    }
}

fn parse_http_request_text(text: &str) -> Result<HttpRequest, String> {
    let (header_block, body) = text
        .split_once("\r\n\r\n")
        .ok_or_else(|| "missing HTTP header terminator".to_string())?;
    let mut lines = header_block.lines();
    let request_line = lines
        .next()
        .ok_or_else(|| "missing request line".to_string())?;
    let mut request_parts = request_line.split_whitespace();
    let method = request_parts
        .next()
        .ok_or_else(|| "missing HTTP method".to_string())?;
    let target = request_parts
        .next()
        .ok_or_else(|| "missing request target".to_string())?;

    let mut content_length = 0usize;
    for line in lines {
        if let Some((name, value)) = line.split_once(':')
            && name.trim().eq_ignore_ascii_case("content-length")
        {
            content_length = value
                .trim()
                .parse::<usize>()
                .map_err(|_| "invalid content-length header".to_string())?;
        }
    }
    if content_length != body.len() {
        return Err("content-length does not match body size".to_string());
    }

    Ok(HttpRequest {
        method: method.to_string(),
        target: target.to_string(),
        body: body.as_bytes().to_vec(),
    })
}

fn handle_request(
    state: &Arc<Mutex<ControlPlanePlacementState>>,
    request: HttpRequest,
) -> HttpResponse {
    let (path, query) = split_target(&request.target);
    match (request.method.as_str(), path.as_str()) {
        ("GET", "/health") | ("GET", "/v1/control-plane/health") => {
            HttpResponse::ok_json("{\"status\":\"ok\"}".to_string())
        }
        ("GET", "/v1/control-plane/placement") => {
            let guard = match state.lock() {
                Ok(guard) => guard,
                Err(_) => return HttpResponse::error(500, "control-plane state lock unavailable"),
            };
            if query
                .get("format")
                .is_some_and(|value| value.eq_ignore_ascii_case("csv"))
            {
                HttpResponse::ok_text(render_shard_placements_csv(guard.placements()))
            } else {
                HttpResponse::ok_json(guard.render_placement_json())
            }
        }
        ("PUT", "/v1/control-plane/placement") => {
            let expected_epoch = match parse_optional_u64(query.get("expected_epoch")) {
                Ok(value) => value,
                Err(reason) => return HttpResponse::bad_request(&reason),
            };
            let csv = match std::str::from_utf8(&request.body) {
                Ok(value) => value,
                Err(_) => return HttpResponse::bad_request("placement body must be UTF-8 CSV"),
            };
            let mut guard = match state.lock() {
                Ok(guard) => guard,
                Err(_) => return HttpResponse::error(500, "control-plane state lock unavailable"),
            };
            if let Err(reason) = guard.cas_matches(expected_epoch) {
                return HttpResponse::error(409, &reason);
            }
            let previous = guard.placements.clone();
            match guard.replace_placements_from_csv_monotonic(csv) {
                Ok(()) => match guard.persist_if_configured() {
                    Ok(()) => HttpResponse::ok_json(format!(
                        "{{\"status\":\"ok\",\"placement_count\":{},\"commit_epoch\":{}}}",
                        guard.placements().len(),
                        guard.highest_epoch(),
                    )),
                    Err(reason) => {
                        guard.placements = previous;
                        let _ = guard.persist_if_configured();
                        HttpResponse::error(500, &reason)
                    }
                },
                Err(reason) => HttpResponse::error(409, &reason),
            }
        }
        ("POST", "/v1/control-plane/failover/promote") => {
            let expected_epoch = match parse_optional_u64(query.get("expected_epoch")) {
                Ok(value) => value,
                Err(reason) => return HttpResponse::bad_request(&reason),
            };
            let tenant_id = match query.get("tenant_id") {
                Some(value) if !value.trim().is_empty() => value.trim(),
                _ => {
                    return HttpResponse::bad_request(
                        "tenant_id query parameter is required for promotion",
                    );
                }
            };
            let shard_id = match query
                .get("shard_id")
                .and_then(|value| value.trim().parse::<u32>().ok())
            {
                Some(value) => value,
                None => {
                    return HttpResponse::bad_request(
                        "shard_id query parameter must be a valid u32",
                    );
                }
            };
            let node_id = match query.get("node_id") {
                Some(value) if !value.trim().is_empty() => value.trim(),
                _ => {
                    return HttpResponse::bad_request(
                        "node_id query parameter is required for promotion",
                    );
                }
            };
            let mut guard = match state.lock() {
                Ok(guard) => guard,
                Err(_) => return HttpResponse::error(500, "control-plane state lock unavailable"),
            };
            if let Err(reason) = guard.cas_matches(expected_epoch) {
                return HttpResponse::error(409, &reason);
            }
            let previous = guard.placements.clone();
            match guard.promote_replica(tenant_id, shard_id, node_id) {
                Ok(epoch) => match guard.persist_if_configured() {
                    Ok(()) => HttpResponse::ok_json(format!(
                        "{{\"status\":\"ok\",\"tenant_id\":\"{}\",\"shard_id\":{},\"leader_node_id\":\"{}\",\"epoch\":{}}}",
                        json_escape(tenant_id),
                        shard_id,
                        json_escape(node_id),
                        epoch
                    )),
                    Err(reason) => {
                        guard.placements = previous;
                        let _ = guard.persist_if_configured();
                        HttpResponse::error(500, &reason)
                    }
                },
                Err(reason) => HttpResponse::error(409, &reason),
            }
        }
        (_, "/v1/control-plane/placement") => {
            HttpResponse::method_not_allowed("only GET and PUT are supported")
        }
        (_, "/v1/control-plane/failover/promote") => {
            HttpResponse::method_not_allowed("only POST is supported")
        }
        _ => HttpResponse::not_found("unknown path"),
    }
}

fn split_target(target: &str) -> (String, HashMap<String, String>) {
    let (path, query) = target
        .split_once('?')
        .map(|(path, query)| (path.to_string(), query))
        .unwrap_or_else(|| (target.to_string(), ""));
    let mut map = HashMap::new();
    for item in query.split('&') {
        if item.trim().is_empty() {
            continue;
        }
        let (key, value) = item
            .split_once('=')
            .map(|(key, value)| (key.trim(), value.trim()))
            .unwrap_or_else(|| (item.trim(), ""));
        if key.is_empty() {
            continue;
        }
        map.insert(key.to_string(), value.to_string());
    }
    (path, map)
}

fn parse_optional_u64(value: Option<&String>) -> Result<Option<u64>, String> {
    match value {
        Some(raw) if raw.trim().is_empty() => Ok(None),
        Some(raw) => raw
            .trim()
            .parse::<u64>()
            .map(Some)
            .map_err(|_| format!("invalid u64 value '{}'", raw.trim())),
        None => Ok(None),
    }
}

fn persist_atomically(path: &Path, bytes: &[u8]) -> Result<(), String> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent).map_err(|err| {
            format!(
                "failed creating parent directory '{}': {err}",
                parent.display()
            )
        })?;
    }
    let tmp_path = path.with_extension(format!(
        "tmp-{}-{}",
        std::process::id(),
        std::thread::current().name().unwrap_or("control-plane")
    ));
    fs::write(&tmp_path, bytes).map_err(|err| {
        format!(
            "failed writing temp persistence file '{}': {err}",
            tmp_path.display()
        )
    })?;
    fs::rename(&tmp_path, path).map_err(|err| {
        format!(
            "failed renaming '{}' to '{}': {err}",
            tmp_path.display(),
            path.display()
        )
    })
}

fn render_response_text(response: &HttpResponse) -> String {
    let status_text = match response.status {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        405 => "Method Not Allowed",
        409 => "Conflict",
        _ => "Internal Server Error",
    };
    format!(
        "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        response.status,
        status_text,
        response.content_type,
        response.body.len(),
        response.body
    )
}

fn json_escape(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t")
}

fn replica_role_str(role: ReplicaRole) -> &'static str {
    match role {
        ReplicaRole::Leader => "leader",
        ReplicaRole::Follower => "follower",
    }
}

fn replica_health_str(health: metadata_router::ReplicaHealth) -> &'static str {
    match health {
        metadata_router::ReplicaHealth::Healthy => "healthy",
        metadata_router::ReplicaHealth::Degraded => "degraded",
        metadata_router::ReplicaHealth::Unavailable => "unavailable",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use metadata_router::{ReplicaHealth, ReplicaPlacement};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn sample_placements(epoch: u64) -> Vec<ShardPlacement> {
        vec![ShardPlacement {
            tenant_id: "tenant-a".to_string(),
            shard_id: 1,
            epoch,
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
        }]
    }

    #[test]
    fn replace_placements_monotonic_rejects_epoch_regression() {
        let mut state = ControlPlanePlacementState::new(sample_placements(7));
        let err = state
            .replace_placements_monotonic(sample_placements(6))
            .expect_err("epoch regression should fail");
        assert!(err.contains("epoch regression"));
    }

    #[test]
    fn promote_replica_increments_epoch_and_flips_leader() {
        let mut state = ControlPlanePlacementState::new(sample_placements(7));
        let epoch = state
            .promote_replica("tenant-a", 1, "node-b")
            .expect("promotion should succeed");
        assert_eq!(epoch, 8);
        let leader = state.placements()[0]
            .replicas
            .iter()
            .find(|replica| replica.role == ReplicaRole::Leader)
            .expect("leader should exist");
        assert_eq!(leader.node_id, "node-b");
    }

    #[test]
    fn http_get_placement_csv_returns_csv() {
        let state = Arc::new(Mutex::new(ControlPlanePlacementState::new(
            sample_placements(2),
        )));
        let request = b"GET /v1/control-plane/placement?format=csv HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n";
        let response = handle_http_request_bytes(&state, request).expect("request should parse");
        let text = String::from_utf8(response).expect("response should be utf8");
        assert!(text.contains("HTTP/1.1 200 OK"));
        assert!(text.contains("tenant-a,1,2,node-a,leader,healthy"));
    }

    #[test]
    fn put_placement_rejects_stale_expected_epoch() {
        let state = Arc::new(Mutex::new(ControlPlanePlacementState::new(
            sample_placements(7),
        )));
        let csv = "tenant-a,1,8,node-a,leader,healthy\ntenant-a,1,8,node-b,follower,healthy\n";
        let request = format!(
            "PUT /v1/control-plane/placement?expected_epoch=6 HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\n\r\n{}",
            csv.len(),
            csv
        );
        let response =
            handle_http_request_bytes(&state, request.as_bytes()).expect("request should parse");
        let text = String::from_utf8(response).expect("response should be utf8");
        assert!(text.contains("HTTP/1.1 409 Conflict"));
        assert!(text.contains("stale placement epoch"));
    }

    #[test]
    fn persistence_round_trip_replays_on_restart() {
        let temp_root = unique_temp_dir("control-plane-persist-replay");
        let state_path = temp_root.join("placement.csv");
        let checksum_path = temp_root.join("placement.csv.sha256");
        let persistence = ControlPlanePersistence::new(state_path.clone(), Some(checksum_path))
            .expect("persistence config should be valid");

        let state =
            ControlPlanePlacementState::new(sample_placements(9)).with_persistence(persistence);
        state
            .persist_if_configured()
            .expect("persistence should succeed");
        let loaded = ControlPlanePlacementState::load_persisted_csv(&state_path)
            .expect("persisted state should parse");
        assert_eq!(loaded, sample_placements(9));
        state
            .verify_checksum_if_configured()
            .expect("checksum should verify");
    }

    #[test]
    fn checksum_mismatch_is_rejected() {
        let temp_root = unique_temp_dir("control-plane-checksum-mismatch");
        let state_path = temp_root.join("placement.csv");
        let checksum_path = temp_root.join("placement.csv.sha256");
        let persistence = ControlPlanePersistence::new(state_path.clone(), Some(checksum_path))
            .expect("persistence config should be valid");
        let state =
            ControlPlanePlacementState::new(sample_placements(11)).with_persistence(persistence);
        state
            .persist_if_configured()
            .expect("persist should succeed");
        fs::write(&state_path, "tampered\n").expect("tamper write should succeed");
        let err = state
            .verify_checksum_if_configured()
            .expect_err("tamper should be detected");
        assert!(err.contains("checksum mismatch"));
    }

    fn unique_temp_dir(prefix: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be valid")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("dash-{prefix}-{nanos}"));
        fs::create_dir_all(&dir).expect("temp dir should be creatable");
        dir
    }
}
