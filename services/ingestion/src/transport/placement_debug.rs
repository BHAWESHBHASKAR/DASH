use std::collections::HashMap;

use metadata_router::{ReplicaHealth, ReplicaRole, ShardPlacement, route_write_with_placement};

use super::{IngestionRuntime, PlacementRoutingRuntime, PlacementRoutingState, json_escape};

pub(super) fn render_placement_debug_json(
    runtime: &IngestionRuntime,
    query: &HashMap<String, String>,
) -> String {
    let (enabled, config_error, local_node_id, shard_count, placements, route_probe, reload) =
        match runtime.placement_routing.as_ref() {
            Ok(Some(routing)) => (
                true,
                None,
                Some(routing.runtime().local_node_id.clone()),
                routing.runtime().router_config.shard_ids.len(),
                routing.runtime().placements.as_slice(),
                build_write_route_probe_json(Some(routing.runtime()), query),
                render_placement_reload_json(Some(routing)),
            ),
            Ok(None) => (
                false,
                None,
                None,
                0,
                &[][..],
                "null".to_string(),
                render_placement_reload_json(None),
            ),
            Err(reason) => (
                false,
                Some(reason.as_str()),
                None,
                0,
                &[][..],
                "null".to_string(),
                render_placement_reload_json(None),
            ),
        };

    format!(
        "{{\"enabled\":{},\"config_error\":{},\"local_node_id\":{},\"shard_count\":{},\"placements\":{},\"route_probe\":{},\"reload\":{}}}",
        enabled,
        config_error
            .map(|value| format!("\"{}\"", json_escape(value)))
            .unwrap_or_else(|| "null".to_string()),
        local_node_id
            .as_ref()
            .map(|value| format!("\"{}\"", json_escape(value)))
            .unwrap_or_else(|| "null".to_string()),
        shard_count,
        render_placements_json(placements),
        route_probe,
        reload
    )
}

fn render_placement_reload_json(state: Option<&PlacementRoutingState>) -> String {
    let snapshot = state
        .map(PlacementRoutingState::reload_snapshot)
        .unwrap_or_default();
    format!(
        "{{\"enabled\":{},\"interval_ms\":{},\"attempt_total\":{},\"success_total\":{},\"failure_total\":{},\"last_error\":{}}}",
        snapshot.enabled,
        snapshot
            .interval_ms
            .map(|value| value.to_string())
            .unwrap_or_else(|| "null".to_string()),
        snapshot.attempt_total,
        snapshot.success_total,
        snapshot.failure_total,
        snapshot
            .last_error
            .as_ref()
            .map(|value| format!("\"{}\"", json_escape(value)))
            .unwrap_or_else(|| "null".to_string()),
    )
}

fn build_write_route_probe_json(
    routing: Option<&PlacementRoutingRuntime>,
    query: &HashMap<String, String>,
) -> String {
    let Some(tenant_id) = query.get("tenant_id").map(String::as_str) else {
        return "null".to_string();
    };
    let tenant_id = tenant_id.trim();
    if tenant_id.is_empty() {
        return "{\"status\":\"invalid\",\"reason\":\"tenant_id must not be empty\"}".to_string();
    }
    let entity_key = query
        .get("entity_key")
        .map(String::as_str)
        .unwrap_or_default()
        .trim();
    if entity_key.is_empty() {
        return "{\"status\":\"invalid\",\"reason\":\"entity_key is required for route probe\"}"
            .to_string();
    }
    let Some(routing) = routing else {
        return "{\"status\":\"unconfigured\",\"reason\":\"placement routing is disabled\"}"
            .to_string();
    };

    match route_write_with_placement(
        tenant_id,
        entity_key,
        &routing.router_config,
        &routing.placements,
    ) {
        Ok(routed) => {
            let local_admission = routed.node_id == routing.local_node_id;
            let reason = if local_admission {
                "local node is write leader"
            } else {
                "request would be rejected: local node is not write leader"
            };
            format!(
                "{{\"status\":\"{}\",\"tenant_id\":\"{}\",\"entity_key\":\"{}\",\"local_node_id\":\"{}\",\"target_node_id\":\"{}\",\"shard_id\":{},\"epoch\":{},\"role\":\"{}\",\"local_admission\":{},\"reason\":\"{}\"}}",
                if local_admission {
                    "routable"
                } else {
                    "rejected"
                },
                json_escape(tenant_id),
                json_escape(entity_key),
                json_escape(&routing.local_node_id),
                json_escape(&routed.node_id),
                routed.shard_id,
                routed.epoch,
                replica_role_str(routed.role),
                local_admission,
                json_escape(reason),
            )
        }
        Err(err) => format!(
            "{{\"status\":\"rejected\",\"tenant_id\":\"{}\",\"entity_key\":\"{}\",\"reason\":\"{}\"}}",
            json_escape(tenant_id),
            json_escape(entity_key),
            json_escape(&format!("placement route error: {err:?}"))
        ),
    }
}

fn render_placements_json(placements: &[ShardPlacement]) -> String {
    let body = placements
        .iter()
        .map(|placement| {
            let replicas = placement
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
                replicas
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    format!("[{body}]")
}

fn replica_role_str(role: ReplicaRole) -> &'static str {
    match role {
        ReplicaRole::Leader => "leader",
        ReplicaRole::Follower => "follower",
    }
}

fn replica_health_str(health: ReplicaHealth) -> &'static str {
    match health {
        ReplicaHealth::Healthy => "healthy",
        ReplicaHealth::Degraded => "degraded",
        ReplicaHealth::Unavailable => "unavailable",
    }
}
