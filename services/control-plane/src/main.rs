use std::sync::{Arc, Mutex};

use control_plane::{ControlPlanePersistence, ControlPlanePlacementState, serve_http};
use metadata_router::load_shard_placements_csv;

fn main() {
    let bind_addr = env_with_fallback("DASH_CONTROL_PLANE_BIND", "EME_CONTROL_PLANE_BIND")
        .unwrap_or_else(|| "127.0.0.1:8090".to_string());
    let state_path = env_with_fallback(
        "DASH_CONTROL_PLANE_STATE_PATH",
        "EME_CONTROL_PLANE_STATE_PATH",
    );
    let checksum_path = env_with_fallback(
        "DASH_CONTROL_PLANE_STATE_SHA256_PATH",
        "EME_CONTROL_PLANE_STATE_SHA256_PATH",
    );

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

    let state = Arc::new(Mutex::new(state));

    println!("control-plane listening on http://{bind_addr}");
    println!("control-plane health endpoint: http://{bind_addr}/v1/control-plane/health");
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
