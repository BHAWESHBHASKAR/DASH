use super::*;
use metadata_router::{ReplicaHealth, ReplicaPlacement};
use std::io::Read;
use std::net::{SocketAddr, TcpStream};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

const TOKEN: &str = "test-token";

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

fn csv_for_epoch(epoch: u64) -> String {
    render_shard_placements_csv(&sample_placements(epoch))
}

fn authed_state(epoch: u64) -> Arc<Mutex<ControlPlanePlacementState>> {
    Arc::new(Mutex::new(
        ControlPlanePlacementState::new(sample_placements(epoch)).with_auth_token(TOKEN),
    ))
}

fn raw_request(method: &str, target: &str, token: Option<&str>, body: &str) -> Vec<u8> {
    let auth = token
        .map(|token| format!("Authorization: Bearer {token}\r\n"))
        .unwrap_or_default();
    format!(
        "{method} {target} HTTP/1.1\r\nHost: localhost\r\n{auth}Content-Length: {}\r\n\r\n{body}",
        body.len()
    )
    .into_bytes()
}

fn call(
    state: &Arc<Mutex<ControlPlanePlacementState>>,
    method: &str,
    target: &str,
    token: Option<&str>,
    body: &str,
) -> String {
    let response = handle_http_request_bytes(state, &raw_request(method, target, token, body))
        .expect("request should parse");
    String::from_utf8(response).expect("response should be utf8")
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

// ---------------------------------------------------------------------------
// Pre-existing behaviour (now exercised with authentication)
// ---------------------------------------------------------------------------

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
        .promote_replica("tenant-a", 1, "node-b", true)
        .expect("forced promotion should succeed");
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
    let state = authed_state(2);
    let text = call(
        &state,
        "GET",
        "/v1/control-plane/placement?format=csv",
        Some(TOKEN),
        "",
    );
    assert!(text.contains("HTTP/1.1 200 OK"));
    assert!(text.contains("tenant-a,1,2,node-a,leader,healthy"));
}

#[test]
fn put_placement_rejects_stale_expected_epoch() {
    let state = authed_state(7);
    let csv = "tenant-a,1,8,node-a,leader,healthy\ntenant-a,1,8,node-b,follower,healthy\n";
    let text = call(
        &state,
        "PUT",
        "/v1/control-plane/placement?expected_epoch=6",
        Some(TOKEN),
        csv,
    );
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

    let state = ControlPlanePlacementState::new(sample_placements(9)).with_persistence(persistence);
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

// ---------------------------------------------------------------------------
// SEC-07: authentication
// ---------------------------------------------------------------------------

#[test]
fn protected_routes_require_bearer_token() {
    let state = authed_state(3);
    let csv = csv_for_epoch(4);
    let routes: Vec<(&str, &str, &str)> = vec![
        ("GET", "/v1/control-plane/placement", ""),
        ("GET", "/v1/control-plane/leader", ""),
        ("PUT", "/v1/control-plane/placement", csv.as_str()),
        (
            "POST",
            "/v1/control-plane/failover/promote?tenant_id=tenant-a&shard_id=1&node_id=node-b&force=true",
            "",
        ),
        ("POST", "/v1/control-plane/leader/acquire", ""),
        (
            "POST",
            "/v1/control-plane/replica-lag?tenant_id=tenant-a&shard_id=1&node_id=node-b&lag=0",
            "",
        ),
    ];
    for (method, target, body) in routes {
        let missing = call(&state, method, target, None, body);
        assert!(
            missing.starts_with("HTTP/1.1 401 Unauthorized"),
            "{method} {target} without token: {missing}"
        );
        assert!(missing.contains("WWW-Authenticate: Bearer"));
        let wrong = call(&state, method, target, Some("wrong"), body);
        assert!(
            wrong.starts_with("HTTP/1.1 401 Unauthorized"),
            "{method} {target} with wrong token: {wrong}"
        );
        let right = call(&state, method, target, Some(TOKEN), body);
        assert!(
            !right.starts_with("HTTP/1.1 401"),
            "{method} {target} with right token: {right}"
        );
    }
    // The unauthenticated attempts must not have mutated anything.
    assert_eq!(state.lock().unwrap().highest_epoch(), 5);
}

#[test]
fn unauthenticated_mutation_does_not_change_state() {
    let state = authed_state(3);
    let csv = csv_for_epoch(9);
    let text = call(&state, "PUT", "/v1/control-plane/placement", None, &csv);
    assert!(text.starts_with("HTTP/1.1 401"));
    assert_eq!(state.lock().unwrap().highest_epoch(), 3);
}

#[test]
fn health_and_ready_stay_open_for_probes() {
    let state = authed_state(3);
    assert!(call(&state, "GET", "/v1/control-plane/health", None, "").starts_with("HTTP/1.1 200"));
    assert!(call(&state, "GET", "/health", None, "").starts_with("HTTP/1.1 200"));
    assert!(call(&state, "GET", "/v1/control-plane/ready", None, "").starts_with("HTTP/1.1 200"));
}

#[test]
fn state_without_configured_auth_denies_protected_routes() {
    let state = Arc::new(Mutex::new(ControlPlanePlacementState::new(
        sample_placements(1),
    )));
    let text = call(
        &state,
        "GET",
        "/v1/control-plane/placement",
        Some("anything"),
        "",
    );
    assert!(text.starts_with("HTTP/1.1 403 Forbidden"), "{text}");
}

#[test]
fn insecure_dev_mode_state_allows_requests_without_token() {
    let state = Arc::new(Mutex::new(
        ControlPlanePlacementState::new(sample_placements(1)).with_auth(AuthMode::InsecureDev),
    ));
    let text = call(&state, "GET", "/v1/control-plane/placement", None, "");
    assert!(text.starts_with("HTTP/1.1 200"), "{text}");
}

#[test]
fn bearer_scheme_is_case_insensitive_and_token_exact() {
    let state = authed_state(1);
    let request = format!(
        "GET /v1/control-plane/placement HTTP/1.1\r\nauthorization: bearer {TOKEN}\r\nContent-Length: 0\r\n\r\n"
    );
    let ok =
        String::from_utf8(handle_http_request_bytes(&state, request.as_bytes()).unwrap()).unwrap();
    assert!(ok.starts_with("HTTP/1.1 200"));
    let near = call(
        &state,
        "GET",
        "/v1/control-plane/placement",
        Some("test-token2"),
        "",
    );
    assert!(near.starts_with("HTTP/1.1 401"));
}

#[test]
fn constant_time_eq_matches_plain_equality() {
    assert!(constant_time_eq(b"secret", b"secret"));
    assert!(!constant_time_eq(b"secret", b"secreT"));
    assert!(!constant_time_eq(b"secret", b"secret-longer"));
    assert!(!constant_time_eq(b"", b"x"));
    assert!(constant_time_eq(b"", b""));
}

#[test]
fn startup_refuses_without_token_unless_insecure_dev() {
    let err = resolve_security(None, false, "0.0.0.0:8090").expect_err("must refuse");
    assert!(err.contains("DASH_CONTROL_PLANE_TOKEN"));
    let err = resolve_security(Some("   "), false, "127.0.0.1:8090").expect_err("blank token");
    assert!(err.contains("DASH_CONTROL_PLANE_TOKEN"));
}

#[test]
fn startup_with_token_keeps_requested_bind() {
    let config = resolve_security(Some(STRONG_TOKEN), false, "0.0.0.0:8090").unwrap();
    assert_eq!(config.auth, AuthMode::Token(STRONG_TOKEN.to_string()));
    assert_eq!(config.bind_addr, "0.0.0.0:8090");
    assert!(config.warnings.is_empty());
}

#[test]
fn insecure_dev_mode_binds_loopback_only_and_warns() {
    let config = resolve_security(None, true, "0.0.0.0:9000").unwrap();
    assert_eq!(config.auth, AuthMode::InsecureDev);
    assert_eq!(config.bind_addr, "127.0.0.1:9000");
    assert!(config.warnings.iter().any(|w| w.contains("DISABLED")));
    let kept = resolve_security(None, true, "127.0.0.1:8090").unwrap();
    assert_eq!(kept.bind_addr, "127.0.0.1:8090");
    let localhost = resolve_security(None, true, "localhost:8090").unwrap();
    assert_eq!(localhost.bind_addr, "localhost:8090");
    let v6 = resolve_security(None, true, "[::]:8090").unwrap();
    assert_eq!(v6.bind_addr, "127.0.0.1:8090");
}

#[test]
fn auth_mode_debug_does_not_leak_the_token() {
    let rendered = format!("{:?}", AuthMode::Token("super-secret".to_string()));
    assert!(!rendered.contains("super-secret"));
}

// ---------------------------------------------------------------------------
// CP-01 / CP-05: real sockets, incremental parsing, timeouts, bounded pool
// ---------------------------------------------------------------------------

fn start_server(state: Arc<Mutex<ControlPlanePlacementState>>, config: ServerConfig) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("addr");
    std::thread::spawn(move || {
        let _ = serve_listener(listener, state, config);
    });
    addr
}

fn fast_config() -> ServerConfig {
    ServerConfig {
        read_timeout: Duration::from_millis(250),
        request_deadline: Duration::from_millis(600),
        write_timeout: Duration::from_millis(500),
        ..ServerConfig::default()
    }
}

/// Read one HTTP response, framed by Content-Length (never waiting for the
/// server to close, and failing if nothing arrives within 3 seconds).
fn read_response(stream: &mut TcpStream) -> String {
    stream
        .set_read_timeout(Some(Duration::from_secs(3)))
        .expect("client read timeout");
    let mut buf = Vec::new();
    let mut chunk = [0u8; 1024];
    loop {
        if let Some(end) = find_header_end(&buf) {
            let head = String::from_utf8_lossy(&buf[..end]).to_ascii_lowercase();
            let length = head
                .lines()
                .find_map(|line| line.strip_prefix("content-length:"))
                .and_then(|value| value.trim().parse::<usize>().ok())
                .unwrap_or(0);
            if buf.len() >= end + 4 + length {
                break;
            }
        }
        match stream.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
            Err(err) => panic!("no complete response within timeout: {err}"),
        }
    }
    String::from_utf8_lossy(&buf).to_string()
}

#[test]
fn server_answers_plain_client_that_never_half_closes() {
    let addr = start_server(authed_state(2), ServerConfig::default());
    let started = Instant::now();
    // A plain std client: write the request, keep the write side open, and
    // wait for the response. The old read_to_end server hung here forever.
    let mut stream = TcpStream::connect(addr).unwrap();
    stream
        .write_all(&raw_request(
            "GET",
            "/v1/control-plane/placement?format=csv",
            Some(TOKEN),
            "",
        ))
        .unwrap();
    let response = read_response(&mut stream);
    assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
    assert!(response.contains("tenant-a,1,2,node-a,leader,healthy"));
    assert!(started.elapsed() < Duration::from_secs(2));
}

#[test]
fn server_reads_exact_content_length_body_split_across_writes() {
    let state = authed_state(2);
    let addr = start_server(Arc::clone(&state), ServerConfig::default());
    let csv = csv_for_epoch(3);
    let head = format!(
        "PUT /v1/control-plane/placement HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer {TOKEN}\r\nContent-Length: {}\r\n\r\n",
        csv.len()
    );
    let mut stream = TcpStream::connect(addr).unwrap();
    stream.write_all(head.as_bytes()).unwrap();
    std::thread::sleep(Duration::from_millis(100));
    let (first, second) = csv.split_at(csv.len() / 2);
    stream.write_all(first.as_bytes()).unwrap();
    std::thread::sleep(Duration::from_millis(100));
    // Trailing bytes beyond Content-Length must not be required or consumed.
    stream.write_all(second.as_bytes()).unwrap();
    let response = read_response(&mut stream);
    assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
    assert_eq!(state.lock().unwrap().highest_epoch(), 3);
}

#[test]
fn server_times_out_idle_slowloris_client() {
    let addr = start_server(authed_state(1), fast_config());
    let mut stream = TcpStream::connect(addr).unwrap();
    stream.write_all(b"GET /v1/control-plane/pla").unwrap();
    let started = Instant::now();
    let response = read_response(&mut stream);
    assert!(response.starts_with("HTTP/1.1 408"), "{response}");
    assert!(started.elapsed() < Duration::from_secs(2));
}

#[test]
fn server_times_out_drip_feeding_slowloris_client() {
    let addr = start_server(authed_state(1), fast_config());
    let mut stream = TcpStream::connect(addr).unwrap();
    let mut writer = stream.try_clone().unwrap();
    // One byte every 100ms: each read succeeds inside the per-read timeout,
    // so only the overall request deadline can stop this client.
    std::thread::spawn(move || {
        for byte in b"GET /health HTTP/1.1\r\nX-Slow: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".iter() {
            if writer.write_all(&[*byte]).is_err() {
                return;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    });
    let started = Instant::now();
    let response = read_response(&mut stream);
    assert!(response.starts_with("HTTP/1.1 408"), "{response}");
    assert!(started.elapsed() < Duration::from_secs(2));
}

#[test]
fn server_rejects_oversized_body_before_reading_it() {
    let config = ServerConfig {
        max_body_bytes: 1024,
        ..ServerConfig::default()
    };
    let addr = start_server(authed_state(1), config);
    let mut stream = TcpStream::connect(addr).unwrap();
    stream
        .write_all(
            format!(
                "PUT /v1/control-plane/placement HTTP/1.1\r\nAuthorization: Bearer {TOKEN}\r\nContent-Length: 99999999\r\n\r\n"
            )
            .as_bytes(),
        )
        .unwrap();
    let response = read_response(&mut stream);
    assert!(response.starts_with("HTTP/1.1 413"), "{response}");
}

#[test]
fn server_rejects_oversized_headers() {
    let config = ServerConfig {
        max_header_bytes: 512,
        ..ServerConfig::default()
    };
    let addr = start_server(authed_state(1), config);
    let mut stream = TcpStream::connect(addr).unwrap();
    let padding = "a".repeat(2048);
    stream
        .write_all(format!("GET /health HTTP/1.1\r\nX-Pad: {padding}\r\n\r\n").as_bytes())
        .unwrap();
    let response = read_response(&mut stream);
    assert!(response.starts_with("HTTP/1.1 431"), "{response}");
}

#[test]
fn worker_pool_is_bounded_and_sheds_load_with_503() {
    let config = ServerConfig {
        workers: 2,
        queue_depth: 1,
        read_timeout: Duration::from_secs(5),
        request_deadline: Duration::from_secs(5),
        ..ServerConfig::default()
    };
    let addr = start_server(authed_state(1), config);
    // Two stalled clients occupy both workers; a third fills the queue.
    let mut stalled = Vec::new();
    for _ in 0..3 {
        let mut stream = TcpStream::connect(addr).unwrap();
        stream.write_all(b"GET /hea").unwrap();
        stalled.push(stream);
        std::thread::sleep(Duration::from_millis(150));
    }
    let mut overflow = TcpStream::connect(addr).unwrap();
    let response = read_response(&mut overflow);
    assert!(response.starts_with("HTTP/1.1 503"), "{response}");
    // Releasing the stalled connections lets the pool serve again.
    drop(stalled);
    std::thread::sleep(Duration::from_millis(300));
    let mut healthy = TcpStream::connect(addr).unwrap();
    healthy
        .write_all(&raw_request("GET", "/health", None, ""))
        .unwrap();
    assert!(read_response(&mut healthy).starts_with("HTTP/1.1 200"));
}

// ---------------------------------------------------------------------------
// CP-03 / CP-04: fencing token, follower behaviour, re-acquisition
// ---------------------------------------------------------------------------

struct SharedVolume {
    lease_path: PathBuf,
    state_path: PathBuf,
}

impl SharedVolume {
    fn new(tag: &str) -> Self {
        let dir = unique_temp_dir(tag);
        Self {
            lease_path: dir.join("lease.txt"),
            state_path: dir.join("placement.csv"),
        }
    }

    fn node(
        &self,
        node_id: &str,
        startup_epoch: u64,
        lease_ms: u64,
    ) -> Arc<Mutex<ControlPlanePlacementState>> {
        let persistence = ControlPlanePersistence::new(
            self.state_path.clone(),
            Some(self.state_path.with_extension("sha256")),
        )
        .unwrap();
        let lease = Arc::new(leader::LeaderLease::new(
            node_id,
            self.lease_path.clone(),
            lease_ms,
            lease_ms / 4,
        ));
        Arc::new(Mutex::new(
            ControlPlanePlacementState::new(sample_placements(startup_epoch))
                .with_persistence(persistence)
                .with_lease(lease)
                .with_auth_token(TOKEN),
        ))
    }
}

#[test]
fn follower_get_placement_is_refused_with_leader_false_header() {
    let volume = SharedVolume::new("cp-follower-get");
    let leader_node = volume.node("node-a", 5, 5_000);
    let follower_node = volume.node("node-b", 1, 5_000);
    let acquire = call(
        &leader_node,
        "POST",
        "/v1/control-plane/leader/acquire",
        Some(TOKEN),
        "",
    );
    assert!(acquire.starts_with("HTTP/1.1 200"), "{acquire}");
    assert!(acquire.contains("\"fencing_token\":1"), "{acquire}");

    let from_follower = call(
        &follower_node,
        "GET",
        "/v1/control-plane/placement?format=csv",
        Some(TOKEN),
        "",
    );
    assert!(from_follower.starts_with("HTTP/1.1 503"), "{from_follower}");
    assert!(from_follower.contains("X-Dash-Leader: false"));
    // The stale startup data must not appear in the body.
    assert!(!from_follower.contains("tenant-a,1,1,"));

    let from_leader = call(
        &leader_node,
        "GET",
        "/v1/control-plane/placement?format=csv",
        Some(TOKEN),
        "",
    );
    assert!(from_leader.starts_with("HTTP/1.1 200"), "{from_leader}");
    assert!(from_leader.contains("X-Dash-Leader: true"));
    assert!(from_leader.contains("X-Dash-Fencing-Token: 1"));
}

#[test]
fn leader_responses_expose_fencing_token() {
    let volume = SharedVolume::new("cp-fencing");
    let node = volume.node("node-a", 5, 5_000);
    call(
        &node,
        "POST",
        "/v1/control-plane/leader/acquire",
        Some(TOKEN),
        "",
    );
    let leader = call(&node, "GET", "/v1/control-plane/leader", Some(TOKEN), "");
    assert!(leader.contains("\"fencing_token\":1"), "{leader}");
    let put = call(
        &node,
        "PUT",
        "/v1/control-plane/placement",
        Some(TOKEN),
        &csv_for_epoch(6),
    );
    assert!(put.contains("\"fencing_token\":1"), "{put}");
    assert!(put.contains("X-Dash-Fencing-Token: 1"));
}

#[test]
fn node_that_acquires_reloads_persisted_placements_before_persisting() {
    let volume = SharedVolume::new("cp-reload");
    // Node A is leader and persists epoch 6.
    let a = volume.node("node-a", 5, 400);
    assert!(
        call(
            &a,
            "POST",
            "/v1/control-plane/leader/acquire",
            Some(TOKEN),
            ""
        )
        .starts_with("HTTP/1.1 200")
    );
    let put = call(
        &a,
        "PUT",
        "/v1/control-plane/placement",
        Some(TOKEN),
        &csv_for_epoch(6),
    );
    assert!(put.starts_with("HTTP/1.1 200"), "{put}");

    // Node B booted earlier with stale epoch-1 placements. Once A's lease
    // lapses B acquires; it must adopt epoch 6 from disk first.
    let b = volume.node("node-b", 1, 400);
    std::thread::sleep(Duration::from_millis(500));
    let mut maintainer = LeaseMaintainer::new(
        Arc::clone(&b),
        Duration::from_millis(100),
        Duration::from_millis(400),
    );
    maintainer.step();
    assert_eq!(b.lock().unwrap().highest_epoch(), 6);

    // A stale write based on B's startup view is now rejected instead of
    // clobbering the persisted epoch-6 state.
    let stale = call(
        &b,
        "PUT",
        "/v1/control-plane/placement",
        Some(TOKEN),
        &csv_for_epoch(3),
    );
    assert!(stale.starts_with("HTTP/1.1 409"), "{stale}");
    assert!(stale.contains("epoch regression"));
    let persisted = ControlPlanePlacementState::load_persisted_csv(&volume.state_path).unwrap();
    assert_eq!(persisted[0].epoch, 6);
}

#[test]
fn acquire_endpoint_also_reloads_persisted_placements() {
    let volume = SharedVolume::new("cp-reload-http");
    let a = volume.node("node-a", 5, 400);
    call(
        &a,
        "POST",
        "/v1/control-plane/leader/acquire",
        Some(TOKEN),
        "",
    );
    call(
        &a,
        "PUT",
        "/v1/control-plane/placement",
        Some(TOKEN),
        &csv_for_epoch(8),
    );
    let b = volume.node("node-b", 1, 400);
    std::thread::sleep(Duration::from_millis(500));
    let acquired = call(
        &b,
        "POST",
        "/v1/control-plane/leader/acquire",
        Some(TOKEN),
        "",
    );
    assert!(acquired.starts_with("HTTP/1.1 200"), "{acquired}");
    assert_eq!(b.lock().unwrap().highest_epoch(), 8);
}

#[test]
fn lease_maintainer_keeps_running_and_reacquires_after_losing_leadership() {
    let volume = SharedVolume::new("cp-reacquire");
    let a = volume.node("node-a", 5, 400);
    let mut maintainer_a = LeaseMaintainer::new(
        Arc::clone(&a),
        Duration::from_millis(100),
        Duration::from_millis(400),
    );
    maintainer_a.step();
    assert!(a.lock().unwrap().is_leader().unwrap());
    assert_eq!(a.lock().unwrap().fencing_token().unwrap(), Some(1));

    // A stalls past its lease; B takes over.
    std::thread::sleep(Duration::from_millis(500));
    let b = volume.node("node-b", 5, 400);
    assert!(b.lock().unwrap().try_acquire_leader(0).unwrap());

    // Old behaviour: A's renewal thread returned here and never came back.
    let delay = maintainer_a.step();
    assert!(!a.lock().unwrap().is_leader().unwrap());
    assert!(delay > Duration::ZERO);

    // Once B's lease lapses, A's still-running maintainer re-acquires with a
    // higher fencing token.
    std::thread::sleep(Duration::from_millis(500));
    maintainer_a.step();
    assert!(a.lock().unwrap().is_leader().unwrap());
    assert_eq!(a.lock().unwrap().fencing_token().unwrap(), Some(3));
}

#[test]
fn follower_mutations_are_refused() {
    let volume = SharedVolume::new("cp-follower-put");
    let a = volume.node("node-a", 5, 5_000);
    let b = volume.node("node-b", 5, 5_000);
    call(
        &a,
        "POST",
        "/v1/control-plane/leader/acquire",
        Some(TOKEN),
        "",
    );
    let put = call(
        &b,
        "PUT",
        "/v1/control-plane/placement",
        Some(TOKEN),
        &csv_for_epoch(6),
    );
    assert!(put.starts_with("HTTP/1.1 503"), "{put}");
    assert!(put.contains("X-Dash-Leader: false"));
    let promote = call(
        &b,
        "POST",
        "/v1/control-plane/failover/promote?tenant_id=tenant-a&shard_id=1&node_id=node-b&force=true",
        Some(TOKEN),
        "",
    );
    assert!(promote.starts_with("HTTP/1.1 503"), "{promote}");
}

// ---------------------------------------------------------------------------
// Failover promotion: replica lag guard
// ---------------------------------------------------------------------------

#[test]
fn promotion_refuses_unknown_or_behind_replica_unless_forced() {
    let mut state = ControlPlanePlacementState::new(sample_placements(7));
    let unknown = state
        .promote_replica("tenant-a", 1, "node-b", false)
        .expect_err("unknown lag must be refused");
    assert!(unknown.contains("no reported replication lag"), "{unknown}");

    state
        .report_replica_lag("tenant-a", 1, "node-b", 12)
        .unwrap();
    let behind = state
        .promote_replica("tenant-a", 1, "node-b", false)
        .expect_err("lagging replica must be refused");
    assert!(behind.contains("12 records behind"), "{behind}");
    assert_eq!(state.highest_epoch(), 7, "refusal must not change state");

    state
        .report_replica_lag("tenant-a", 1, "node-b", 0)
        .unwrap();
    assert_eq!(
        state
            .promote_replica("tenant-a", 1, "node-b", false)
            .unwrap(),
        8
    );
}

#[test]
fn forced_promotion_overrides_lag_guard_and_clears_lag_reports() {
    let mut state = ControlPlanePlacementState::new(sample_placements(7));
    state
        .report_replica_lag("tenant-a", 1, "node-b", 99)
        .unwrap();
    assert_eq!(
        state
            .promote_replica("tenant-a", 1, "node-b", true)
            .unwrap(),
        8
    );
    assert_eq!(state.replica_lag("tenant-a", 1, "node-b"), None);
}

#[test]
fn http_promote_requires_force_without_lag_report() {
    let state = authed_state(7);
    let target = "/v1/control-plane/failover/promote?tenant_id=tenant-a&shard_id=1&node_id=node-b";
    let refused = call(&state, "POST", target, Some(TOKEN), "");
    assert!(refused.starts_with("HTTP/1.1 409"), "{refused}");
    assert!(refused.contains("no reported replication lag"));

    let lag = call(
        &state,
        "POST",
        "/v1/control-plane/replica-lag?tenant_id=tenant-a&shard_id=1&node_id=node-b&lag=0",
        Some(TOKEN),
        "",
    );
    assert!(lag.starts_with("HTTP/1.1 200"), "{lag}");
    let promoted = call(&state, "POST", target, Some(TOKEN), "");
    assert!(promoted.starts_with("HTTP/1.1 200"), "{promoted}");
    assert!(promoted.contains("\"epoch\":8"));
    assert!(promoted.contains("\"forced\":false"));
}

#[test]
fn http_promote_with_force_flag_succeeds() {
    let state = authed_state(7);
    let promoted = call(
        &state,
        "POST",
        "/v1/control-plane/failover/promote?tenant_id=tenant-a&shard_id=1&node_id=node-b&force=true",
        Some(TOKEN),
        "",
    );
    assert!(promoted.starts_with("HTTP/1.1 200"), "{promoted}");
    assert!(promoted.contains("\"forced\":true"));
}

#[test]
fn placement_json_reports_replica_lag_only_when_known() {
    let state = authed_state(7);
    let before = call(
        &state,
        "GET",
        "/v1/control-plane/placement",
        Some(TOKEN),
        "",
    );
    assert!(!before.contains("replica_lag"));
    call(
        &state,
        "POST",
        "/v1/control-plane/replica-lag?tenant_id=tenant-a&shard_id=1&node_id=node-b&lag=4",
        Some(TOKEN),
        "",
    );
    let after = call(
        &state,
        "GET",
        "/v1/control-plane/placement",
        Some(TOKEN),
        "",
    );
    assert!(after.contains("\"replica_lag\":4"), "{after}");
}

#[test]
fn replica_lag_report_for_unknown_replica_is_not_found() {
    let state = authed_state(7);
    let text = call(
        &state,
        "POST",
        "/v1/control-plane/replica-lag?tenant_id=tenant-a&shard_id=1&node_id=ghost&lag=0",
        Some(TOKEN),
        "",
    );
    assert!(text.starts_with("HTTP/1.1 404"), "{text}");
}

// ---------------------------------------------------------------------------
// Router client against the real server
// ---------------------------------------------------------------------------

#[test]
fn router_client_fetches_placements_from_live_server_with_token() {
    let addr = start_server(authed_state(4), ServerConfig::default());
    let url = format!("http://{addr}");
    let mut options = metadata_router::PlacementSourceOptions {
        bearer_token: Some(TOKEN.to_string()),
        ..Default::default()
    };
    let placements =
        metadata_router::load_shard_placements_from_control_plane_with_options(&url, &options)
            .expect("authenticated fetch should succeed");
    assert_eq!(placements, sample_placements(4));

    options.bearer_token = None;
    let err =
        metadata_router::load_shard_placements_from_control_plane_with_options(&url, &options)
            .expect_err("missing token must be rejected");
    assert!(err.contains("401"), "{err}");
}

#[test]
fn router_client_refuses_follower_control_plane() {
    let volume = SharedVolume::new("cp-router-follower");
    let leader_node = volume.node("node-a", 5, 5_000);
    let follower_node = volume.node("node-b", 1, 5_000);
    call(
        &leader_node,
        "POST",
        "/v1/control-plane/leader/acquire",
        Some(TOKEN),
        "",
    );
    let addr = start_server(follower_node, ServerConfig::default());
    let options = metadata_router::PlacementSourceOptions {
        bearer_token: Some(TOKEN.to_string()),
        ..Default::default()
    };
    let err = metadata_router::load_shard_placements_from_source_with_options(
        None,
        Some(&format!("http://{addr}")),
        &options,
    )
    .expect_err("follower placement must not be served");
    assert!(err.contains("503"), "{err}");
}

// ---------------------------------------------------------------------------
// Review findings: token strength, node id, authentication-failure throttle
// ---------------------------------------------------------------------------

const STRONG_TOKEN: &str = "9f2b7c41d8e0a3566b1c4d7e8f90a2b3c4d5e6f7";

#[test]
fn strict_secrets_reject_short_and_placeholder_tokens() {
    for weak in [
        "abc",
        "short-token",
        "0123456789abcdef0123456789abcde", // 31 chars
        "change-me-change-me-change-me-change-me",
        "<generate-a-long-random-token-here-please>",
        "secret-secret-secret-secret-secret-secret",
    ] {
        let err = resolve_security(Some(weak), false, "0.0.0.0:8090").expect_err(weak);
        assert!(err.contains("DASH_CONTROL_PLANE_TOKEN"), "{err}");
        assert!(!err.contains(weak), "error leaked the token: {err}");
    }
    let ok = resolve_security(Some(STRONG_TOKEN), false, "0.0.0.0:8090").unwrap();
    assert_eq!(ok.auth, AuthMode::Token(STRONG_TOKEN.to_string()));
}

#[test]
fn weak_token_is_only_allowed_when_strict_secrets_are_relaxed() {
    // Dev mode alone does not relax the check; the explicit opt-out does.
    assert!(resolve_security_with(Some("abc"), true, true, "127.0.0.1:1").is_err());
    let relaxed = resolve_security_with(Some("abc"), true, false, "127.0.0.1:1").unwrap();
    assert_eq!(relaxed.auth, AuthMode::Token("abc".to_string()));
}

#[test]
fn node_id_is_required_outside_dev_mode() {
    let err = resolve_node_id(None, false).expect_err("no node id");
    assert!(err.contains("DASH_CONTROL_PLANE_NODE_ID"), "{err}");
    assert!(resolve_node_id(Some("   "), false).is_err());
    assert_eq!(resolve_node_id(Some(" cp-1 "), false).unwrap(), "cp-1");
    assert_eq!(resolve_node_id(Some("cp-1"), true).unwrap(), "cp-1");
    let dev = resolve_node_id(None, true).unwrap();
    assert_eq!(dev, format!("control-plane-{}", std::process::id()));
}

fn throttled_call(
    state: &Arc<Mutex<ControlPlanePlacementState>>,
    token: Option<&str>,
    peer: std::net::IpAddr,
) -> String {
    let raw = raw_request("GET", "/v1/control-plane/placement", token, "");
    String::from_utf8(handle_http_request_bytes_from_peer(state, &raw, peer).unwrap()).unwrap()
}

#[test]
fn repeated_bad_tokens_from_one_peer_get_429_with_retry_after() {
    let state = Arc::new(Mutex::new(
        ControlPlanePlacementState::new(sample_placements(1)).with_auth_token(STRONG_TOKEN),
    ));
    let attacker: std::net::IpAddr = "203.0.113.7".parse().unwrap();
    let other: std::net::IpAddr = "203.0.113.8".parse().unwrap();
    for attempt in 0..10 {
        let response = throttled_call(&state, Some("wrong-token"), attacker);
        assert!(
            response.starts_with("HTTP/1.1 401"),
            "{attempt}: {response}"
        );
    }
    let blocked = throttled_call(&state, Some("wrong-token"), attacker);
    assert!(blocked.starts_with("HTTP/1.1 429"), "{blocked}");
    assert!(blocked.contains("Retry-After: "), "{blocked}");
    // Even the right token is refused while the peer is throttled...
    let blocked = throttled_call(&state, Some(STRONG_TOKEN), attacker);
    assert!(blocked.starts_with("HTTP/1.1 429"), "{blocked}");
    // ...but other peers are unaffected.
    let fine = throttled_call(&state, Some(STRONG_TOKEN), other);
    assert!(fine.starts_with("HTTP/1.1 200"), "{fine}");
}

#[test]
fn successful_requests_and_open_routes_do_not_count_as_failures() {
    let state = Arc::new(Mutex::new(
        ControlPlanePlacementState::new(sample_placements(1)).with_auth_token(STRONG_TOKEN),
    ));
    let peer: std::net::IpAddr = "198.51.100.1".parse().unwrap();
    for _ in 0..30 {
        let ok = throttled_call(&state, Some(STRONG_TOKEN), peer);
        assert!(ok.starts_with("HTTP/1.1 200"), "{ok}");
    }
}

#[test]
fn failure_window_expires_and_peer_table_is_bounded() {
    let throttle = AuthFailureThrottle::default();
    let peer: std::net::IpAddr = "192.0.2.1".parse().unwrap();
    let t0 = Instant::now();
    assert!(throttle.blocked_for(peer, t0).is_none());
    for _ in 0..10 {
        throttle.record_failure(peer, t0);
    }
    let wait = throttle.blocked_for(peer, t0).expect("blocked");
    assert!((1..=60).contains(&wait));
    assert!(
        throttle
            .blocked_for(peer, t0 + Duration::from_secs(61))
            .is_none()
    );
    // A new failure after the window starts a fresh count.
    throttle.record_failure(peer, t0 + Duration::from_secs(61));
    assert!(
        throttle
            .blocked_for(peer, t0 + Duration::from_secs(62))
            .is_none()
    );
    // Many distinct peers cannot grow the table without bound.
    for i in 0..12_000u32 {
        let ip = std::net::IpAddr::from(i.to_be_bytes());
        throttle.record_failure(ip, t0);
    }
    assert!(throttle.peers.lock().unwrap().len() <= 10_000);
}

#[test]
fn duplicate_authorization_headers_are_rejected_with_400() {
    let state = Arc::new(Mutex::new(
        ControlPlanePlacementState::new(sample_placements(1)).with_auth_token(STRONG_TOKEN),
    ));
    let raw = format!(
        "GET /v1/control-plane/placement HTTP/1.1\r\nHost: t\r\nAuthorization: Bearer {STRONG_TOKEN}\r\nauthorization: Bearer other\r\nContent-Length: 0\r\n\r\n"
    );
    // The library entry point refuses to parse it ...
    assert!(handle_http_request_bytes(&state, raw.as_bytes()).is_err());
    // ... and the real socket server answers 400.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let server_state = Arc::clone(&state);
    std::thread::spawn(move || {
        let _ = serve_listener(listener, server_state, ServerConfig::default());
    });
    let mut stream = TcpStream::connect(addr).unwrap();
    stream.write_all(raw.as_bytes()).unwrap();
    let mut out = String::new();
    stream.read_to_string(&mut out).unwrap();
    // Report the status line only: the rest is unvalidated network data.
    let status_line = out.lines().next().unwrap_or("").escape_debug().to_string();
    assert!(
        out.starts_with("HTTP/1.1 400"),
        "unexpected status line: {status_line}"
    );
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

#[test]
fn metrics_require_the_token_and_render_valid_exposition() {
    let state = authed_state(7);
    let denied = call(&state, "GET", "/metrics", None, "");
    assert!(denied.starts_with("HTTP/1.1 401"), "{denied}");
    let wrong = call(&state, "GET", "/metrics", Some("wrong-token"), "");
    assert!(wrong.starts_with("HTTP/1.1 401"), "{wrong}");

    let response = call(&state, "GET", "/metrics", Some(TOKEN), "");
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    let body = response.split_once("\r\n\r\n").expect("body").1;
    let report = dash_observe::validate(body).unwrap_or_else(|e| panic!("{e}\n{body}"));
    assert_eq!(report.value("dash_control_plane_is_leader", &[]), Some(1.0));
    assert_eq!(
        report.value("dash_control_plane_state_error", &[]),
        Some(0.0)
    );
    assert_eq!(
        report.value("dash_control_plane_placement_epoch", &[]),
        Some(7.0)
    );
    assert_eq!(
        report.value("dash_control_plane_placements", &[]),
        Some(1.0)
    );
    assert!(report.has_family("dash_build_info"), "{body}");
    assert!(report.has_family("process_start_time_seconds"), "{body}");

    let post = call(&state, "POST", "/metrics", Some(TOKEN), "");
    assert!(post.starts_with("HTTP/1.1 405"), "{post}");
}

#[test]
fn route_labels_are_bounded() {
    assert_eq!(
        http_route_label("GET", "/v1/control-plane/placement"),
        "placement"
    );
    assert_eq!(http_route_label("GET", "/metrics"), "metrics");
    assert_eq!(http_route_label("GET", "/v1/control-plane/x/123"), "other");
    assert_eq!(http_route_label("GET", "/anything"), "other");
}
