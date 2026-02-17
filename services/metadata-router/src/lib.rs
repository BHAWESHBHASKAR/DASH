use std::collections::BTreeMap;

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
}
