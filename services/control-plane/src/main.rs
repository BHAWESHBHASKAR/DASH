use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use control_plane::{
    ControlPlanePersistence, ControlPlanePlacementState, LeaderStatus, LeaseMaintainer,
    leader::LeaderLease, resolve_node_id, resolve_security, serve_http,
};
use metadata_router::load_shard_placements_csv;

fn main() {
    let requested_bind = env_with_fallback("DASH_CONTROL_PLANE_BIND", "EME_CONTROL_PLANE_BIND")
        .unwrap_or_else(|| "127.0.0.1:8090".to_string());
    let insecure_dev = matches!(
        std::env::var("DASH_INSECURE_DEV_MODE").ok().as_deref(),
        Some("1")
    );
    let security = resolve_security(
        env_with_fallback("DASH_CONTROL_PLANE_TOKEN", "EME_CONTROL_PLANE_TOKEN").as_deref(),
        insecure_dev,
        &requested_bind,
    )
    .unwrap_or_else(|err| {
        eprintln!("control-plane refusing to start: {err}");
        std::process::exit(2);
    });
    for warning in &security.warnings {
        eprintln!("WARNING: {warning}");
    }
    let bind_addr = security.bind_addr.clone();
    let node_id = resolve_node_id(
        env_with_fallback("DASH_CONTROL_PLANE_NODE_ID", "EME_CONTROL_PLANE_NODE_ID").as_deref(),
        insecure_dev,
    )
    .unwrap_or_else(|err| {
        eprintln!("control-plane refusing to start: {err}");
        std::process::exit(2);
    });
    let state_path = env_with_fallback(
        "DASH_CONTROL_PLANE_STATE_PATH",
        "EME_CONTROL_PLANE_STATE_PATH",
    );
    let checksum_path = env_with_fallback(
        "DASH_CONTROL_PLANE_STATE_SHA256_PATH",
        "EME_CONTROL_PLANE_STATE_SHA256_PATH",
    );
    let lease_path = env_with_fallback(
        "DASH_CONTROL_PLANE_LEASE_PATH",
        "EME_CONTROL_PLANE_LEASE_PATH",
    );
    let lease_duration_ms = env_with_fallback(
        "DASH_CONTROL_PLANE_LEASE_DURATION_MS",
        "EME_CONTROL_PLANE_LEASE_DURATION_MS",
    )
    .and_then(|value| value.parse::<u64>().ok())
    .unwrap_or(30_000);
    let lease_renewal_ms = env_with_fallback(
        "DASH_CONTROL_PLANE_LEASE_RENEWAL_MS",
        "EME_CONTROL_PLANE_LEASE_RENEWAL_MS",
    )
    .and_then(|value| value.parse::<u64>().ok())
    .unwrap_or(10_000);
    let lease_safety_margin_ms = env_with_fallback(
        "DASH_CONTROL_PLANE_LEASE_SAFETY_MARGIN_MS",
        "EME_CONTROL_PLANE_LEASE_SAFETY_MARGIN_MS",
    )
    .and_then(|value| value.parse::<u64>().ok())
    .unwrap_or(1_000);

    let initial_placements =
        env_with_fallback("DASH_ROUTER_PLACEMENT_FILE", "EME_ROUTER_PLACEMENT_FILE")
            .as_deref()
            .map(std::path::Path::new)
            .map(load_shard_placements_csv)
            .transpose()
            .unwrap_or_else(|err| {
                eprintln!("control-plane failed loading initial placement file: {err}");
                std::process::exit(2);
            })
            .unwrap_or_default();

    let mut state = ControlPlanePlacementState::new(initial_placements).with_auth(security.auth);
    if let Some(state_path) = state_path {
        let persistence = ControlPlanePersistence::new(
            std::path::PathBuf::from(state_path),
            checksum_path.map(std::path::PathBuf::from),
        )
        .unwrap_or_else(|err| {
            eprintln!("control-plane invalid persistence config: {err}");
            std::process::exit(2);
        });
        if persistence.state_path().exists() {
            let replayed = ControlPlanePlacementState::load_persisted_csv(persistence.state_path())
                .unwrap_or_else(|err| {
                    eprintln!("control-plane failed replaying persisted placement state: {err}");
                    std::process::exit(2);
                });
            state = ControlPlanePlacementState::new(replayed)
                .with_auth(state.auth_mode().clone())
                .with_persistence(persistence);
        } else {
            state = state.with_persistence(persistence);
            if let Err(err) = state.persist_if_configured() {
                eprintln!("control-plane failed seeding persisted placement state: {err}");
                std::process::exit(2);
            }
        }
        if let Err(err) = state.verify_checksum_if_configured() {
            eprintln!("control-plane checksum verification failed: {err}");
            std::process::exit(2);
        }
    }

    // Configure leader election when a lease path is provided. Without a lease
    // path the control-plane is standalone and always behaves as leader.
    let state = if let Some(lease_path) = lease_path {
        let lease = Arc::new(
            LeaderLease::new(
                node_id.clone(),
                lease_path,
                lease_duration_ms,
                lease_renewal_ms,
            )
            .with_safety_margin_ms(lease_safety_margin_ms)
            // Admin recovery from a forged or corrupt lease record.
            .with_lease_reset(matches!(
                env_with_fallback(
                    "DASH_CONTROL_PLANE_LEASE_RESET",
                    "EME_CONTROL_PLANE_LEASE_RESET"
                )
                .as_deref()
                .map(str::trim),
                Some("1")
            )),
        );
        let mut state = state.with_lease(lease);
        // Acquire (and, if leader, reload persisted placements) before
        // serving anything.
        match state.try_acquire_and_sync() {
            Ok(LeaderStatus::Leader { fencing_token }) => eprintln!(
                "control-plane '{node_id}' acquired leader lease (fencing token {})",
                fencing_token.unwrap_or(0)
            ),
            Ok(LeaderStatus::Follower) => eprintln!(
                "control-plane '{node_id}' started as follower; another node holds the lease"
            ),
            Err(err) => {
                eprintln!("control-plane failed to acquire or sync leader lease: {err}");
                std::process::exit(2);
            }
        }

        // Keep the lease renewed while leader and keep trying (with backoff)
        // to acquire it while follower. This thread never exits.
        let state = Arc::new(Mutex::new(state));
        let maintainer = LeaseMaintainer::new(
            state.clone(),
            Duration::from_millis(lease_renewal_ms),
            Duration::from_millis(lease_duration_ms),
        );
        thread::Builder::new()
            .name("control-plane-lease".to_string())
            .spawn(move || maintainer.run())
            .expect("failed to spawn lease maintenance thread");
        state
    } else {
        Arc::new(Mutex::new(state))
    };

    println!("control-plane listening on http://{bind_addr} (node_id={node_id})");
    println!("control-plane health endpoint: http://{bind_addr}/v1/control-plane/health");
    println!("control-plane ready endpoint: http://{bind_addr}/v1/control-plane/ready");
    println!("control-plane leader endpoint: http://{bind_addr}/v1/control-plane/leader");
    println!("control-plane placement endpoint: http://{bind_addr}/v1/control-plane/placement");
    println!(
        "control-plane failover endpoint: http://{bind_addr}/v1/control-plane/failover/promote"
    );

    if let Err(err) = serve_http(&bind_addr, state) {
        eprintln!("control-plane server failed: {err}");
        std::process::exit(1);
    }
}

fn env_with_fallback(primary: &str, fallback: &str) -> Option<String> {
    std::env::var(primary)
        .ok()
        .or_else(|| std::env::var(fallback).ok())
}
