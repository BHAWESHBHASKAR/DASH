use std::{
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use metadata_router::{
    PlacementRouteError, ReplicaHealth, ReplicaRole, RouterConfig, ShardPlacement,
    ensure_no_epoch_regression, load_shard_placements_from_source, shard_ids_from_placements,
};
use schema::Claim;

use super::SharedRuntime;
use super::config::{env_with_fallback, parse_env_first_u64, parse_env_first_usize};
use crate::api::WriteConsistencyPolicy;

/// How long writes keep being accepted on the last known placement after
/// reloads started failing (`DASH_INGEST_PLACEMENT_STALE_GRACE_MS`).
const DEFAULT_PLACEMENT_STALE_GRACE_MS: u64 = 30_000;

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
    /// When the in-memory placement was last confirmed by its source.
    last_success_at: Instant,
    stale_grace: Duration,
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
    ConsistencyUnavailable {
        policy: WriteConsistencyPolicy,
        shard_id: u32,
        healthy_replicas: usize,
        required_replicas: usize,
        total_replicas: usize,
        epoch: u64,
    },
    WrongNode {
        local_node_id: String,
        target_node_id: String,
        shard_id: u32,
        epoch: u64,
        role: ReplicaRole,
    },
    /// Placement reloads have been failing for longer than the grace; this
    /// node can no longer prove it is still the leader (REP-08).
    PlacementStale {
        age_ms: u64,
        grace_ms: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct WriteRouteResolution {
    pub(super) shard_id: u32,
    pub(super) epoch: u64,
    pub(super) ack_count: usize,
    pub(super) required_acks: usize,
    pub(super) total_replicas: usize,
}

impl PlacementRoutingRuntime {
    /// Router config whose hash ring is built from the shards THIS tenant
    /// actually has placements for (REP-09). A ring over every tenant's
    /// shards would hash a key to a shard the tenant does not own and answer
    /// `PlacementNotFound` for it.
    pub(super) fn router_config_for_tenant(&self, tenant_id: &str) -> RouterConfig {
        let mut shard_ids: Vec<u32> = self
            .placements
            .iter()
            .filter(|placement| placement.tenant_id == tenant_id)
            .map(|placement| placement.shard_id)
            .collect();
        shard_ids.sort_unstable();
        shard_ids.dedup();
        if shard_ids.is_empty() {
            return self.router_config.clone();
        }
        RouterConfig {
            shard_ids,
            ..self.router_config.clone()
        }
    }

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
        let runtime = load_placement_routing_runtime(
            placement_file.as_deref().map(Path::new),
            control_plane_base_url.as_deref(),
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
        let stale_grace = Duration::from_millis(
            parse_env_first_u64(&["DASH_INGEST_PLACEMENT_STALE_GRACE_MS"])
                .unwrap_or(DEFAULT_PLACEMENT_STALE_GRACE_MS),
        );
        let reload = reload_interval_ms.map(|interval_ms| {
            let reload_interval = Duration::from_millis(interval_ms);
            PlacementReloadRuntime {
                config: PlacementReloadConfig {
                    placement_file: placement_file.as_deref().map(PathBuf::from),
                    control_plane_base_url: control_plane_base_url.clone(),
                    shard_ids_override: shard_ids_override.clone(),
                    replica_count_override,
                    virtual_nodes_per_shard,
                    reload_interval,
                },
                next_reload_at: Instant::now() + reload_interval,
                last_success_at: Instant::now(),
                stale_grace,
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

    /// First phase of a reload, done under the runtime lock: decide whether
    /// a reload is due and capture everything the fetch needs. The slow
    /// fetch itself runs WITHOUT the lock (see [`refresh_placement`]).
    pub(super) fn begin_refresh(&mut self) -> Option<PlacementRefreshJob> {
        let reload = self.reload.as_mut()?;
        let now = Instant::now();
        if now < reload.next_reload_at {
            return None;
        }
        reload.attempt_total = reload.attempt_total.saturating_add(1);
        // Claim the slot so concurrent requests do not start a second fetch.
        reload.next_reload_at = now + reload.config.reload_interval;
        Some(PlacementRefreshJob {
            config: reload.config.clone(),
            local_node_id: self.runtime.local_node_id.clone(),
            current: self.runtime.placements.clone(),
        })
    }

    /// Last phase: swap in the fetched placement (or record the failure).
    pub(super) fn finish_refresh(&mut self, result: Result<PlacementRoutingRuntime, String>) {
        let Some(reload) = self.reload.as_mut() else {
            return;
        };
        match result {
            Ok(runtime) => {
                self.runtime = runtime;
                reload.success_total = reload.success_total.saturating_add(1);
                reload.last_success_at = Instant::now();
                reload.last_error = None;
            }
            Err(reason) => {
                reload.failure_total = reload.failure_total.saturating_add(1);
                reload.last_error = Some(reason.clone());
                eprintln!("ingestion placement reload failed: {reason}");
            }
        }
    }

    /// Writes are refused once reloads have been failing for longer than the
    /// stale grace: a node that cannot reach the placement source can no
    /// longer prove it is still the leader (REP-08).
    pub(super) fn check_fresh(&self) -> Result<(), WriteRouteError> {
        let Some(reload) = self.reload.as_ref() else {
            return Ok(());
        };
        if reload.last_error.is_none() {
            return Ok(());
        }
        let age = reload.last_success_at.elapsed();
        if age > reload.stale_grace {
            return Err(WriteRouteError::PlacementStale {
                age_ms: age.as_millis() as u64,
                grace_ms: reload.stale_grace.as_millis() as u64,
            });
        }
        Ok(())
    }
}

/// A placement reload prepared under the lock and executed outside it.
pub(super) struct PlacementRefreshJob {
    config: PlacementReloadConfig,
    local_node_id: String,
    current: Vec<ShardPlacement>,
}

impl PlacementRefreshJob {
    /// Blocking fetch + validation. Rejects epoch regressions (REP-08).
    pub(super) fn run(self) -> Result<PlacementRoutingRuntime, String> {
        let runtime = load_placement_routing_runtime(
            self.config.placement_file.as_deref(),
            self.config.control_plane_base_url.as_deref(),
            &self.local_node_id,
            self.config.shard_ids_override.as_deref(),
            self.config.replica_count_override,
            self.config.virtual_nodes_per_shard,
        )?;
        ensure_no_epoch_regression(&self.current, &runtime.placements)?;
        Ok(runtime)
    }
}

/// Reload the placement if due WITHOUT holding the runtime mutex during the
/// network fetch: lock briefly to plan, fetch, then lock briefly to swap.
pub(super) fn refresh_placement(runtime: &SharedRuntime) {
    let job = match runtime.lock() {
        Ok(mut guard) => guard.begin_placement_refresh(),
        Err(_) => return,
    };
    let Some(job) = job else {
        return;
    };
    let result = job.run();
    if let Ok(mut guard) = runtime.lock() {
        guard.finish_placement_refresh(result);
    }
}

fn load_placement_routing_runtime(
    placement_file: Option<&Path>,
    control_plane_base_url: Option<&str>,
    local_node_id: &str,
    shard_ids_override: Option<&[u32]>,
    replica_count_override: Option<usize>,
    virtual_nodes_per_shard: u32,
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
        WriteRouteError::ConsistencyUnavailable {
            policy,
            shard_id,
            healthy_replicas,
            required_replicas,
            total_replicas,
            epoch,
        } => (
            503,
            format!(
                "placement route rejected write request: write_consistency={} is unavailable for shard {} at epoch {} (healthy_replicas={}, required_acks={}, total_replicas={})",
                policy.as_str(),
                shard_id,
                epoch,
                healthy_replicas,
                required_replicas,
                total_replicas
            ),
        ),
        WriteRouteError::PlacementStale { age_ms, grace_ms } => (
            503,
            format!(
                "placement is stale: the placement source has been unreachable or invalid for {age_ms}ms (grace {grace_ms}ms); refusing writes until it recovers"
            ),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::IngestionRuntime;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};
    use store::InMemoryStore;

    /// Stub control plane. `mode`: 0 = healthy (epoch 5), 1 = HTTP 500,
    /// 2 = healthy but with a regressed epoch (3). `delay_ms` delays replies.
    struct StubControlPlane {
        base_url: String,
        mode: Arc<AtomicU64>,
        delay_ms: Arc<AtomicU64>,
    }

    fn stub_control_plane() -> StubControlPlane {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind stub");
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let mode = Arc::new(AtomicU64::new(0));
        let delay_ms = Arc::new(AtomicU64::new(0));
        let (mode_t, delay_t) = (Arc::clone(&mode), Arc::clone(&delay_ms));
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { continue };
                let mut buf = [0u8; 2048];
                let _ = stream.read(&mut buf);
                std::thread::sleep(Duration::from_millis(delay_t.load(Ordering::SeqCst)));
                let (status, body) = match mode_t.load(Ordering::SeqCst) {
                    1 => ("500 Internal Server Error", String::new()),
                    2 => ("200 OK", "tenant-a,0,3,node-a,leader,healthy\n".to_string()),
                    _ => ("200 OK", "tenant-a,0,5,node-a,leader,healthy\n".to_string()),
                };
                let response = format!(
                    "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = stream.write_all(response.as_bytes());
            }
        });
        StubControlPlane {
            base_url,
            mode,
            delay_ms,
        }
    }

    fn state_for(
        stub: &StubControlPlane,
        interval_ms: u64,
        grace_ms: u64,
    ) -> PlacementRoutingState {
        let runtime =
            load_placement_routing_runtime(None, Some(&stub.base_url), "node-a", None, None, 16)
                .expect("initial load from stub");
        PlacementRoutingState {
            runtime,
            reload: Some(PlacementReloadRuntime {
                config: PlacementReloadConfig {
                    placement_file: None,
                    control_plane_base_url: Some(stub.base_url.clone()),
                    shard_ids_override: None,
                    replica_count_override: None,
                    virtual_nodes_per_shard: 16,
                    reload_interval: Duration::from_millis(interval_ms),
                },
                next_reload_at: Instant::now(),
                last_success_at: Instant::now(),
                stale_grace: Duration::from_millis(grace_ms),
                attempt_total: 0,
                success_total: 0,
                failure_total: 0,
                last_error: None,
            }),
        }
    }

    fn refresh_now(state: &mut PlacementRoutingState) {
        if let Some(reload) = state.reload.as_mut() {
            reload.next_reload_at = Instant::now();
        }
        if let Some(job) = state.begin_refresh() {
            let result = job.run();
            state.finish_refresh(result);
        }
    }

    #[test]
    fn writes_are_refused_after_the_stale_grace_and_recover_with_the_source() {
        let stub = stub_control_plane();
        let mut state = state_for(&stub, 1, 150);
        assert!(state.check_fresh().is_ok());

        stub.mode.store(1, Ordering::SeqCst);
        refresh_now(&mut state);
        assert!(state.reload_snapshot().last_error.is_some());
        // Inside the grace the last known placement is still served.
        assert!(state.check_fresh().is_ok());

        std::thread::sleep(Duration::from_millis(200));
        refresh_now(&mut state);
        let err = state.check_fresh().expect_err("grace exceeded");
        assert!(matches!(err, WriteRouteError::PlacementStale { .. }));
        let (status, message) = map_write_route_error(&err);
        assert_eq!(status, 503);
        assert!(message.contains("placement is stale"), "{message}");

        stub.mode.store(0, Ordering::SeqCst);
        refresh_now(&mut state);
        assert!(state.check_fresh().is_ok());
    }

    #[test]
    fn epoch_regressions_are_rejected_and_the_current_placement_is_kept() {
        let stub = stub_control_plane();
        let mut state = state_for(&stub, 1, 30_000);
        assert_eq!(state.runtime().placements[0].epoch, 5);

        stub.mode.store(2, Ordering::SeqCst);
        refresh_now(&mut state);
        assert_eq!(state.runtime().placements[0].epoch, 5);
        let error = state
            .reload_snapshot()
            .last_error
            .expect("regression recorded");
        assert!(error.contains("epoch regression"), "{error}");
    }

    #[test]
    fn placement_fetch_does_not_hold_the_runtime_mutex() {
        let stub = stub_control_plane();
        stub.delay_ms.store(400, Ordering::SeqCst);
        let state = state_for(&stub, 1, 30_000);
        let mut runtime = IngestionRuntime::in_memory(InMemoryStore::new());
        runtime.placement_routing = Ok(Some(state));
        let shared: SharedRuntime = Arc::new(Mutex::new(runtime));

        let refresher = {
            let shared = Arc::clone(&shared);
            std::thread::spawn(move || refresh_placement(&shared))
        };
        // Let the refresher get into the (slow) fetch.
        std::thread::sleep(Duration::from_millis(100));
        let started = Instant::now();
        drop(shared.lock().expect("runtime lock"));
        assert!(
            started.elapsed() < Duration::from_millis(200),
            "runtime mutex was held during the placement fetch ({:?})",
            started.elapsed()
        );
        refresher.join().unwrap();
        let guard = shared.lock().unwrap();
        let snapshot = guard
            .placement_routing
            .as_ref()
            .unwrap()
            .as_ref()
            .unwrap()
            .reload_snapshot();
        assert_eq!(snapshot.success_total, 1);
    }

    #[test]
    fn hash_ring_is_built_from_the_tenants_own_shards() {
        use metadata_router::{ReplicaPlacement, route_write_with_placement};
        let placement = |tenant: &str, shard: u32| ShardPlacement {
            tenant_id: tenant.to_string(),
            shard_id: shard,
            epoch: 1,
            replicas: vec![ReplicaPlacement {
                node_id: "node-a".to_string(),
                role: ReplicaRole::Leader,
                health: ReplicaHealth::Healthy,
            }],
        };
        let runtime = PlacementRoutingRuntime {
            local_node_id: "node-a".to_string(),
            router_config: RouterConfig {
                shard_ids: vec![0, 1, 7],
                virtual_nodes_per_shard: 16,
                replica_count: 1,
            },
            placements: vec![
                placement("tenant-a", 0),
                placement("tenant-a", 1),
                placement("tenant-b", 7),
            ],
        };
        let tenant_config = runtime.router_config_for_tenant("tenant-b");
        assert_eq!(tenant_config.shard_ids, vec![7]);
        let mut global_failures = 0;
        for key in 0..200 {
            let entity = format!("entity-{key}");
            route_write_with_placement("tenant-b", &entity, &tenant_config, &runtime.placements)
                .expect("tenant ring only contains tenant shards");
            if route_write_with_placement(
                "tenant-b",
                &entity,
                &runtime.router_config,
                &runtime.placements,
            )
            .is_err()
            {
                global_failures += 1;
            }
        }
        assert!(
            global_failures > 0,
            "the global ring used to answer PlacementNotFound for some keys"
        );
    }
}
