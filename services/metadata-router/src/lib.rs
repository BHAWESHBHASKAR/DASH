use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardAssignment {
    pub tenant_id: String,
    pub entity_key: String,
    pub shard_id: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutingPlan {
    pub primary: ShardAssignment,
    pub replicas: Vec<ShardAssignment>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouterConfig {
    pub shard_ids: Vec<u32>,
    pub virtual_nodes_per_shard: u32,
    pub replica_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaRole {
    Leader,
    Follower,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaHealth {
    Healthy,
    Degraded,
    Unavailable,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaPlacement {
    pub node_id: String,
    pub role: ReplicaRole,
    pub health: ReplicaHealth,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardPlacement {
    pub tenant_id: String,
    pub shard_id: u32,
    pub epoch: u64,
    pub replicas: Vec<ReplicaPlacement>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadPreference {
    LeaderOnly,
    PreferFollower,
    AnyHealthy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutedReplica {
    pub tenant_id: String,
    pub entity_key: String,
    pub shard_id: u32,
    pub epoch: u64,
    pub node_id: String,
    pub role: ReplicaRole,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlacementRouteError {
    PlacementNotFound { tenant_id: String, shard_id: u32 },
    NoWritableLeader { tenant_id: String, shard_id: u32 },
    NoReadableReplica { tenant_id: String, shard_id: u32 },
    ReplicaNotFound { node_id: String },
    ReplicaUnhealthy { node_id: String },
}

impl Default for RouterConfig {
    fn default() -> Self {
        Self {
            shard_ids: vec![0],
            virtual_nodes_per_shard: 64,
            replica_count: 1,
        }
    }
}

pub fn route_to_shard(tenant_id: &str, entity_key: &str, shard_count: u32) -> ShardAssignment {
    let shard_count = shard_count.max(1);
    let mut hash: u64 = 1469598103934665603;
    for b in tenant_id.as_bytes().iter().chain(entity_key.as_bytes()) {
        hash ^= *b as u64;
        hash = hash.wrapping_mul(1099511628211);
    }
    ShardAssignment {
        tenant_id: tenant_id.to_string(),
        entity_key: entity_key.to_string(),
        shard_id: (hash % shard_count as u64) as u32,
    }
}

pub fn route_with_replicas(
    tenant_id: &str,
    entity_key: &str,
    config: &RouterConfig,
) -> RoutingPlan {
    let ring = build_ring(config);
    if ring.is_empty() {
        return RoutingPlan {
            primary: route_to_shard(tenant_id, entity_key, 1),
            replicas: Vec::new(),
        };
    }

    let target = hash_key(&format!("{tenant_id}|{entity_key}"));
    let mut ordered: Vec<u32> = ring
        .range(target..)
        .chain(ring.range(..target))
        .map(|(_, shard_id)| *shard_id)
        .collect();
    ordered.dedup();

    let primary_shard = *ordered.first().unwrap_or(&config.shard_ids[0]);
    let primary = ShardAssignment {
        tenant_id: tenant_id.to_string(),
        entity_key: entity_key.to_string(),
        shard_id: primary_shard,
    };

    let replicas = ordered
        .into_iter()
        .filter(|shard_id| *shard_id != primary_shard)
        .take(config.replica_count.saturating_sub(1))
        .map(|shard_id| ShardAssignment {
            tenant_id: tenant_id.to_string(),
            entity_key: entity_key.to_string(),
            shard_id,
        })
        .collect();

    RoutingPlan { primary, replicas }
}

pub fn route_write_with_placement(
    tenant_id: &str,
    entity_key: &str,
    config: &RouterConfig,
    placements: &[ShardPlacement],
) -> Result<RoutedReplica, PlacementRouteError> {
    let shard_id = route_with_replicas(tenant_id, entity_key, config)
        .primary
        .shard_id;
    let placement = find_placement(placements, tenant_id, shard_id).ok_or_else(|| {
        PlacementRouteError::PlacementNotFound {
            tenant_id: tenant_id.to_string(),
            shard_id,
        }
    })?;
    let leader = placement
        .replicas
        .iter()
        .find(|replica| {
            replica.role == ReplicaRole::Leader && replica.health == ReplicaHealth::Healthy
        })
        .ok_or_else(|| PlacementRouteError::NoWritableLeader {
            tenant_id: tenant_id.to_string(),
            shard_id,
        })?;
    Ok(RoutedReplica {
        tenant_id: tenant_id.to_string(),
        entity_key: entity_key.to_string(),
        shard_id,
        epoch: placement.epoch,
        node_id: leader.node_id.clone(),
        role: ReplicaRole::Leader,
    })
}

pub fn route_read_with_placement(
    tenant_id: &str,
    entity_key: &str,
    config: &RouterConfig,
    placements: &[ShardPlacement],
    preference: ReadPreference,
) -> Result<RoutedReplica, PlacementRouteError> {
    let shard_id = route_with_replicas(tenant_id, entity_key, config)
        .primary
        .shard_id;
    let placement = find_placement(placements, tenant_id, shard_id).ok_or_else(|| {
        PlacementRouteError::PlacementNotFound {
            tenant_id: tenant_id.to_string(),
            shard_id,
        }
    })?;
    let chosen = match preference {
        ReadPreference::LeaderOnly => placement.replicas.iter().find(|replica| {
            replica.role == ReplicaRole::Leader && is_readable_replica_health(replica.health)
        }),
        ReadPreference::PreferFollower => placement
            .replicas
            .iter()
            .find(|replica| {
                replica.role == ReplicaRole::Follower && is_readable_replica_health(replica.health)
            })
            .or_else(|| {
                placement.replicas.iter().find(|replica| {
                    replica.role == ReplicaRole::Leader
                        && is_readable_replica_health(replica.health)
                })
            }),
        ReadPreference::AnyHealthy => placement
            .replicas
            .iter()
            .find(|replica| is_readable_replica_health(replica.health)),
    }
    .ok_or_else(|| PlacementRouteError::NoReadableReplica {
        tenant_id: tenant_id.to_string(),
        shard_id,
    })?;
    Ok(RoutedReplica {
        tenant_id: tenant_id.to_string(),
        entity_key: entity_key.to_string(),
        shard_id,
        epoch: placement.epoch,
        node_id: chosen.node_id.clone(),
        role: chosen.role,
    })
}

pub fn set_replica_health(
    placement: &mut ShardPlacement,
    node_id: &str,
    health: ReplicaHealth,
) -> Result<(), PlacementRouteError> {
    let replica = placement
        .replicas
        .iter_mut()
        .find(|replica| replica.node_id == node_id)
        .ok_or_else(|| PlacementRouteError::ReplicaNotFound {
            node_id: node_id.to_string(),
        })?;
    replica.health = health;
    Ok(())
}

pub fn promote_replica_to_leader(
    placement: &mut ShardPlacement,
    node_id: &str,
) -> Result<u64, PlacementRouteError> {
    let promoted_index = placement
        .replicas
        .iter()
        .position(|replica| replica.node_id == node_id)
        .ok_or_else(|| PlacementRouteError::ReplicaNotFound {
            node_id: node_id.to_string(),
        })?;
    if !is_readable_replica_health(placement.replicas[promoted_index].health) {
        return Err(PlacementRouteError::ReplicaUnhealthy {
            node_id: node_id.to_string(),
        });
    }
    if placement.replicas[promoted_index].role == ReplicaRole::Leader {
        return Ok(placement.epoch);
    }
    for replica in &mut placement.replicas {
        if replica.role == ReplicaRole::Leader {
            replica.role = ReplicaRole::Follower;
        }
    }
    placement.replicas[promoted_index].role = ReplicaRole::Leader;
    placement.epoch = placement.epoch.saturating_add(1);
    Ok(placement.epoch)
}

pub fn load_shard_placements_csv(path: &Path) -> Result<Vec<ShardPlacement>, String> {
    let raw = fs::read_to_string(path)
        .map_err(|err| format!("failed to read placement file '{}': {err}", path.display()))?;
    parse_shard_placements_csv(&raw)
}

pub fn parse_shard_placements_csv(input: &str) -> Result<Vec<ShardPlacement>, String> {
    let mut grouped: BTreeMap<(String, u32), ShardPlacement> = BTreeMap::new();
    for (line_index, line) in input.lines().enumerate() {
        let line_no = line_index + 1;
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let columns: Vec<&str> = line.split(',').map(str::trim).collect();
        if columns.len() != 6 {
            return Err(format!(
                "invalid placement CSV line {line_no}: expected 6 columns (tenant_id,shard_id,epoch,node_id,role,health)"
            ));
        }
        let tenant_id = columns[0];
        if tenant_id.is_empty() {
            return Err(format!(
                "invalid placement CSV line {line_no}: tenant_id must not be empty"
            ));
        }
        let shard_id = columns[1]
            .parse::<u32>()
            .map_err(|_| format!("invalid placement CSV line {line_no}: shard_id must be a u32"))?;
        let epoch = columns[2]
            .parse::<u64>()
            .map_err(|_| format!("invalid placement CSV line {line_no}: epoch must be a u64"))?;
        let node_id = columns[3];
        if node_id.is_empty() {
            return Err(format!(
                "invalid placement CSV line {line_no}: node_id must not be empty"
            ));
        }
        let role = parse_replica_role(columns[4])
            .map_err(|reason| format!("invalid placement CSV line {line_no}: role {reason}"))?;
        let health = parse_replica_health(columns[5])
            .map_err(|reason| format!("invalid placement CSV line {line_no}: health {reason}"))?;
        let key = (tenant_id.to_string(), shard_id);
        let entry = grouped.entry(key).or_insert_with(|| ShardPlacement {
            tenant_id: tenant_id.to_string(),
            shard_id,
            epoch,
            replicas: Vec::new(),
        });
        if entry.epoch != epoch {
            return Err(format!(
                "invalid placement CSV line {line_no}: epoch mismatch for tenant '{tenant_id}' shard {shard_id}"
            ));
        }
        if entry
            .replicas
            .iter()
            .any(|replica| replica.node_id == node_id)
        {
            return Err(format!(
                "invalid placement CSV line {line_no}: duplicate node_id '{node_id}' for tenant '{tenant_id}' shard {shard_id}"
            ));
        }
        entry.replicas.push(ReplicaPlacement {
            node_id: node_id.to_string(),
            role,
            health,
        });
    }

    let mut placements: Vec<ShardPlacement> = grouped.into_values().collect();
    placements.sort_by(|a, b| {
        a.tenant_id
            .cmp(&b.tenant_id)
            .then(a.shard_id.cmp(&b.shard_id))
    });
    Ok(placements)
}

pub fn shard_ids_from_placements(placements: &[ShardPlacement]) -> Vec<u32> {
    let mut shard_ids = BTreeSet::new();
    for placement in placements {
        shard_ids.insert(placement.shard_id);
    }
    shard_ids.into_iter().collect()
}

fn parse_replica_role(raw: &str) -> Result<ReplicaRole, &'static str> {
    match raw.to_ascii_lowercase().as_str() {
        "leader" => Ok(ReplicaRole::Leader),
        "follower" => Ok(ReplicaRole::Follower),
        _ => Err("must be one of: leader, follower"),
    }
}

fn parse_replica_health(raw: &str) -> Result<ReplicaHealth, &'static str> {
    match raw.to_ascii_lowercase().as_str() {
        "healthy" => Ok(ReplicaHealth::Healthy),
        "degraded" => Ok(ReplicaHealth::Degraded),
        "unavailable" => Ok(ReplicaHealth::Unavailable),
        _ => Err("must be one of: healthy, degraded, unavailable"),
    }
}

fn find_placement<'a>(
    placements: &'a [ShardPlacement],
    tenant_id: &str,
    shard_id: u32,
) -> Option<&'a ShardPlacement> {
    placements
        .iter()
        .find(|placement| placement.tenant_id == tenant_id && placement.shard_id == shard_id)
}

fn is_readable_replica_health(health: ReplicaHealth) -> bool {
    matches!(health, ReplicaHealth::Healthy | ReplicaHealth::Degraded)
}

fn build_ring(config: &RouterConfig) -> BTreeMap<u64, u32> {
    let mut ring = BTreeMap::new();
    let vnodes = config.virtual_nodes_per_shard.max(1);
    let shard_ids: Vec<u32> = if config.shard_ids.is_empty() {
        vec![0]
    } else {
        config.shard_ids.clone()
    };
    for shard_id in shard_ids {
        for vnode in 0..vnodes {
            let key = format!("shard:{shard_id}:vn:{vnode}");
            ring.insert(hash_key(&key), shard_id);
        }
    }
    ring
}

fn hash_key(value: &str) -> u64 {
    let mut hash: u64 = 1469598103934665603;
    for byte in value.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(1099511628211);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routing_is_deterministic_for_same_input() {
        let a = route_to_shard("tenant-a", "entity-x", 16);
        let b = route_to_shard("tenant-a", "entity-x", 16);
        assert_eq!(a, b);
    }

    #[test]
    fn routing_changes_with_entity_key() {
        let a = route_to_shard("tenant-a", "entity-x", 16);
        let b = route_to_shard("tenant-a", "entity-y", 16);
        assert_ne!(a.shard_id, b.shard_id);
    }

    #[test]
    fn routing_with_replicas_is_stable() {
        let config = RouterConfig {
            shard_ids: vec![0, 1, 2, 3],
            virtual_nodes_per_shard: 32,
            replica_count: 2,
        };
        let a = route_with_replicas("tenant-a", "entity-x", &config);
        let b = route_with_replicas("tenant-a", "entity-x", &config);
        assert_eq!(a, b);
        assert!(a.replicas.len() <= 1);
    }

    fn single_shard_config() -> RouterConfig {
        RouterConfig {
            shard_ids: vec![5],
            virtual_nodes_per_shard: 16,
            replica_count: 3,
        }
    }

    fn sample_placement() -> ShardPlacement {
        ShardPlacement {
            tenant_id: "tenant-a".to_string(),
            shard_id: 5,
            epoch: 7,
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
                ReplicaPlacement {
                    node_id: "node-c".to_string(),
                    role: ReplicaRole::Follower,
                    health: ReplicaHealth::Degraded,
                },
            ],
        }
    }

    #[test]
    fn route_write_with_placement_returns_healthy_leader() {
        let config = single_shard_config();
        let placement = sample_placement();
        let routed = route_write_with_placement("tenant-a", "entity-x", &config, &[placement])
            .expect("write route should resolve");
        assert_eq!(routed.node_id, "node-a");
        assert_eq!(routed.role, ReplicaRole::Leader);
        assert_eq!(routed.epoch, 7);
    }

    #[test]
    fn route_read_with_placement_prefers_followers_when_requested() {
        let config = single_shard_config();
        let placement = sample_placement();
        let routed = route_read_with_placement(
            "tenant-a",
            "entity-x",
            &config,
            &[placement],
            ReadPreference::PreferFollower,
        )
        .expect("read route should resolve");
        assert_eq!(routed.node_id, "node-b");
        assert_eq!(routed.role, ReplicaRole::Follower);
    }

    #[test]
    fn route_write_with_placement_fails_without_healthy_leader() {
        let config = single_shard_config();
        let mut placement = sample_placement();
        set_replica_health(&mut placement, "node-a", ReplicaHealth::Unavailable)
            .expect("leader health update should succeed");
        let err = route_write_with_placement("tenant-a", "entity-x", &config, &[placement])
            .expect_err("write route should fail");
        assert!(matches!(err, PlacementRouteError::NoWritableLeader { .. }));
    }

    #[test]
    fn promote_replica_to_leader_increments_epoch_and_flips_roles() {
        let mut placement = sample_placement();
        let new_epoch =
            promote_replica_to_leader(&mut placement, "node-b").expect("promotion should succeed");
        assert_eq!(new_epoch, 8);
        let leader = placement
            .replicas
            .iter()
            .find(|replica| replica.role == ReplicaRole::Leader)
            .expect("leader should exist");
        assert_eq!(leader.node_id, "node-b");
    }

    #[test]
    fn route_read_with_placement_fails_without_readable_replicas() {
        let config = single_shard_config();
        let mut placement = sample_placement();
        for node in ["node-a", "node-b", "node-c"] {
            set_replica_health(&mut placement, node, ReplicaHealth::Unavailable)
                .expect("health update should succeed");
        }
        let err = route_read_with_placement(
            "tenant-a",
            "entity-x",
            &config,
            &[placement],
            ReadPreference::AnyHealthy,
        )
        .expect_err("read route should fail");
        assert!(matches!(err, PlacementRouteError::NoReadableReplica { .. }));
    }

    #[test]
    fn parse_shard_placements_csv_loads_replicas_per_shard() {
        let csv = r#"
            # tenant_id,shard_id,epoch,node_id,role,health
            tenant-a,0,7,node-a,leader,healthy
            tenant-a,0,7,node-b,follower,degraded
            tenant-a,1,2,node-c,leader,healthy
        "#;
        let placements = parse_shard_placements_csv(csv).expect("csv should parse");
        assert_eq!(placements.len(), 2);
        assert_eq!(placements[0].tenant_id, "tenant-a");
        assert_eq!(placements[0].shard_id, 0);
        assert_eq!(placements[0].epoch, 7);
        assert_eq!(placements[0].replicas.len(), 2);
        assert_eq!(placements[1].shard_id, 1);
        assert_eq!(placements[1].replicas.len(), 1);
    }

    #[test]
    fn parse_shard_placements_csv_rejects_epoch_conflicts() {
        let csv = r#"
            tenant-a,0,7,node-a,leader,healthy
            tenant-a,0,8,node-b,follower,healthy
        "#;
        let err = parse_shard_placements_csv(csv).expect_err("csv should reject epoch conflicts");
        assert!(err.contains("epoch mismatch"));
    }

    #[test]
    fn shard_ids_from_placements_deduplicates_and_sorts() {
        let placements = vec![
            ShardPlacement {
                tenant_id: "tenant-a".to_string(),
                shard_id: 5,
                epoch: 1,
                replicas: vec![],
            },
            ShardPlacement {
                tenant_id: "tenant-b".to_string(),
                shard_id: 2,
                epoch: 1,
                replicas: vec![],
            },
            ShardPlacement {
                tenant_id: "tenant-c".to_string(),
                shard_id: 5,
                epoch: 1,
                replicas: vec![],
            },
        ];
        assert_eq!(shard_ids_from_placements(&placements), vec![2, 5]);
    }
}
