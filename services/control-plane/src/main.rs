use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use control_plane::{
    ControlPlanePersistence, ControlPlanePlacementState, leader::LeaderLease, serve_http,
};
use metadata_router::load_shard_placements_csv;

fn main() {
    let bind_addr = env_with_fallback("DASH_CONTROL_PLANE_BIND", "EME_CONTROL_PLANE_BIND")
        .unwrap_or_else(|| "127.0.0.1:8090".to_string());
    let node_id = env_with_fallback("DASH_CONTROL_PLANE_NODE_ID", "EME_CONTROL_PLANE_NODE_ID")
        .unwrap_or_else(|| format!("control-plane-{}", std::process::id()));
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

    let mut state = ControlPlanePlacementState::new(initial_placements);
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
            state = ControlPlanePlacementState::new(replayed).with_persistence(persistence);
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
        let lease = Arc::new(LeaderLease::new(
            node_id.clone(),
            lease_path,
            lease_duration_ms,
            lease_renewal_ms,
        ));
        match lease.try_acquire(state.highest_epoch()) {
            Ok(true) => eprintln!("control-plane '{node_id}' acquired leader lease"),
            Ok(false) => eprintln!(
                "control-plane '{node_id}' started as follower; another node holds the lease"
            ),
            Err(err) => {
                eprintln!("control-plane failed to read leader lease: {err}");
                std::process::exit(2);
            }
        }
        let state = state.with_lease(lease.clone());

        // Spawn a background thread to keep the lease renewed while this
        // process remains the leader. The thread exits if the lease is lost.
        let state = Arc::new(Mutex::new(state));
        let state_for_renewal = state.clone();
        let lease_for_renewal = lease;
        let node_id_for_renewal = node_id.clone();
        thread::spawn(move || {
            let renewal_interval = Duration::from_millis(lease_for_renewal.renewal_interval_ms());
            loop {
                thread::sleep(renewal_interval);
                let epoch = state_for_renewal
                    .lock()
                    .map(|guard| guard.highest_epoch())
                    .unwrap_or(0);
                match lease_for_renewal.renew(epoch) {
                    Ok(true) => {
                        // Still leader; continue.
                    }
                    Ok(false) => {
                        eprintln!("control-plane '{node_id_for_renewal}' lost leader lease");
                        return;
                    }
                    Err(err) => {
                        eprintln!(
                            "control-plane '{node_id_for_renewal}' leader renewal failed: {err}"
                        );
                        return;
                    }
                }
            }
        });
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
