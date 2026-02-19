use std::{
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use metadata_router::{
    PlacementRouteError, ReplicaHealth, ReplicaRole, RouterConfig, ShardPlacement,
    load_shard_placements_csv, shard_ids_from_placements,
};
use schema::Claim;

use super::config::{env_with_fallback, parse_env_first_u64, parse_env_first_usize};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PlacementRoutingRuntime {
    pub(super) local_node_id: String,
    pub(super) router_config: RouterConfig,
    pub(super) placements: Vec<ShardPlacement>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PlacementRoutingState {
    pub(super) runtime: PlacementRoutingRuntime,
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
    reload_interval: Duration,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct PlacementObservabilitySnapshot {
    pub(super) leaders_total: usize,
    pub(super) followers_total: usize,
    pub(super) replicas_healthy: usize,
    pub(super) replicas_degraded: usize,
    pub(super) replicas_unavailable: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct PlacementReloadSnapshot {
    pub(super) enabled: bool,
    pub(super) interval_ms: Option<u64>,
    pub(super) attempt_total: u64,
    pub(super) success_total: u64,
    pub(super) failure_total: u64,
    pub(super) last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum WriteRouteError {
    Config(String),
    Placement(PlacementRouteError),
    WrongNode {
        local_node_id: String,
        target_node_id: String,
        shard_id: u32,
        epoch: u64,
        role: ReplicaRole,
    },
}

impl PlacementRoutingRuntime {
    pub(super) fn observability_snapshot(&self) -> PlacementObservabilitySnapshot {
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
    pub(super) fn from_env() -> Result<Option<Self>, String> {
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
        let runtime = load_placement_routing_runtime(
            Path::new(&placement_file),
            local_node_id,
            shard_ids_override.as_deref(),
            replica_count_override,
            virtual_nodes_per_shard,
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

    #[cfg(test)]
    pub(super) fn from_static_runtime(runtime: PlacementRoutingRuntime) -> Self {
        Self {
            runtime,
            reload: None,
        }
    }

    pub(super) fn runtime(&self) -> &PlacementRoutingRuntime {
        &self.runtime
    }

    pub(super) fn observability_snapshot(&self) -> PlacementObservabilitySnapshot {
        self.runtime.observability_snapshot()
    }

    pub(super) fn reload_snapshot(&self) -> PlacementReloadSnapshot {
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

    pub(super) fn maybe_refresh(&mut self) {
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
        ) {
            Ok(runtime) => {
                self.runtime = runtime;
                reload.success_total = reload.success_total.saturating_add(1);
                reload.last_error = None;
            }
            Err(reason) => {
                reload.failure_total = reload.failure_total.saturating_add(1);
                reload.last_error = Some(reason.clone());
                eprintln!("ingestion placement reload failed: {reason}");
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
    })
}

pub(super) fn write_entity_key_for_claim(claim: &Claim) -> &str {
    claim
        .entities
        .iter()
        .find(|value| !value.trim().is_empty())
        .map(String::as_str)
        .unwrap_or(claim.claim_id.as_str())
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

pub(super) fn map_write_route_error(error: &WriteRouteError) -> (u16, String) {
    match error {
        WriteRouteError::Config(reason) => (
            500,
            format!("placement routing configuration error: {reason}"),
        ),
        WriteRouteError::Placement(reason) => (
            503,
            format!("placement route rejected write request: {reason:?}"),
        ),
        WriteRouteError::WrongNode {
            local_node_id,
            target_node_id,
            shard_id,
            epoch,
            role: _,
        } => (
            503,
            format!(
                "placement route rejected write request: local node '{local_node_id}' is not leader for shard {shard_id} at epoch {epoch} (target leader: '{target_node_id}')"
            ),
        ),
    }
}
