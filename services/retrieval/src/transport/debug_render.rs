use std::collections::HashMap;

use metadata_router::{
    ReadPreference, ReplicaHealth, ReplicaRole, ShardPlacement, route_read_with_placement,
};

use super::{
    DEFAULT_STORAGE_DIVERGENCE_WARN_DELTA_COUNT, DEFAULT_STORAGE_DIVERGENCE_WARN_RATIO,
    PlacementReloadSnapshot, PlacementRoutingRuntime, json_escape, parse_env_first_f64,
    parse_env_first_usize,
};
use crate::api::{
    RetrievePlannerDebugSnapshot, RetrieveStorageMergeSnapshot, STORAGE_MERGE_MODEL,
    STORAGE_PROMOTION_BOUNDARY_REPLAY_ONLY, STORAGE_PROMOTION_BOUNDARY_SEGMENT_FULLY_PROMOTED,
    STORAGE_PROMOTION_BOUNDARY_SEGMENT_PLUS_WAL_DELTA,
};

pub(super) fn render_planner_debug_json(snapshot: &RetrievePlannerDebugSnapshot) -> String {
    format!(
        "{{\"tenant_id\":\"{}\",\"top_k\":{},\"stance_mode\":\"{}\",\"has_query_embedding\":{},\"entity_filter_count\":{},\"embedding_filter_count\":{},\"has_filtering\":{},\"metadata_prefilter_count\":{},\"segment_base_count\":{},\"wal_delta_count\":{},\"storage_visible_count\":{},\"allowed_claim_ids_active\":{},\"allowed_claim_ids_count\":{},\"short_circuit_empty\":{},\"ann_candidate_count\":{},\"planner_candidate_count\":{}}}",
        json_escape(&snapshot.tenant_id),
        snapshot.top_k,
        snapshot.stance_mode,
        snapshot.has_query_embedding,
        snapshot.entity_filter_count,
        snapshot.embedding_filter_count,
        snapshot.has_filtering,
        snapshot.metadata_prefilter_count,
        snapshot.segment_base_count,
        snapshot.wal_delta_count,
        snapshot.storage_visible_count,
        snapshot.allowed_claim_ids_active,
        snapshot.allowed_claim_ids_count,
        snapshot.short_circuit_empty,
        snapshot.ann_candidate_count,
        snapshot.planner_candidate_count,
    )
}

fn storage_divergence_ratio(snapshot: &RetrievePlannerDebugSnapshot) -> f64 {
    if snapshot.storage_visible_count == 0 {
        0.0
    } else {
        snapshot.wal_delta_count as f64 / snapshot.storage_visible_count as f64
    }
}

pub(super) fn resolve_storage_divergence_warn_delta_count() -> usize {
    parse_env_first_usize(&[
        "DASH_RETRIEVAL_STORAGE_DIVERGENCE_WARN_DELTA_COUNT",
        "EME_RETRIEVAL_STORAGE_DIVERGENCE_WARN_DELTA_COUNT",
    ])
    .filter(|value| *value > 0)
    .unwrap_or(DEFAULT_STORAGE_DIVERGENCE_WARN_DELTA_COUNT)
}

pub(super) fn resolve_storage_divergence_warn_ratio() -> f64 {
    parse_env_first_f64(&[
        "DASH_RETRIEVAL_STORAGE_DIVERGENCE_WARN_RATIO",
        "EME_RETRIEVAL_STORAGE_DIVERGENCE_WARN_RATIO",
    ])
    .filter(|value| value.is_finite() && *value >= 0.0)
    .unwrap_or(DEFAULT_STORAGE_DIVERGENCE_WARN_RATIO)
}

pub(super) fn evaluate_storage_divergence_warning(
    snapshot: &RetrievePlannerDebugSnapshot,
    warn_delta_count: usize,
    warn_ratio: f64,
) -> (bool, Option<String>, f64) {
    let ratio = storage_divergence_ratio(snapshot);
    let delta_exceeded = snapshot.wal_delta_count >= warn_delta_count;
    let ratio_exceeded = ratio >= warn_ratio;
    let warn = snapshot.wal_delta_count > 0 && (delta_exceeded || ratio_exceeded);
    let reason = if !warn {
        None
    } else if delta_exceeded && ratio_exceeded {
        Some(format!(
            "wal_delta_count={} exceeds {} and divergence_ratio={:.6} exceeds {:.6}",
            snapshot.wal_delta_count, warn_delta_count, ratio, warn_ratio
        ))
    } else if delta_exceeded {
        Some(format!(
            "wal_delta_count={} exceeds {}",
            snapshot.wal_delta_count, warn_delta_count
        ))
    } else {
        Some(format!(
            "divergence_ratio={:.6} exceeds {:.6}",
            ratio, warn_ratio
        ))
    };
    (warn, reason, ratio)
}

pub(super) fn promotion_boundary_state_metric_value(state: &str) -> usize {
    match state {
        STORAGE_PROMOTION_BOUNDARY_REPLAY_ONLY => 0,
        STORAGE_PROMOTION_BOUNDARY_SEGMENT_PLUS_WAL_DELTA => 1,
        STORAGE_PROMOTION_BOUNDARY_SEGMENT_FULLY_PROMOTED => 2,
        _ => 3,
    }
}

pub(super) fn render_storage_visibility_debug_json(
    snapshot: &RetrievePlannerDebugSnapshot,
    merge_snapshot: &RetrieveStorageMergeSnapshot,
    warn_delta_count: usize,
    warn_ratio: f64,
    warn: bool,
    reason: Option<&str>,
    ratio: f64,
) -> String {
    let reason_json = reason
        .map(|value| format!("\"{}\"", json_escape(value)))
        .unwrap_or_else(|| "null".to_string());
    format!(
        "{{\"tenant_id\":\"{}\",\"storage_merge_model\":\"{}\",\"source_of_truth_model\":\"{}\",\"execution_mode\":\"{}\",\"disk_native_segment_execution_active\":{},\"execution_candidate_count\":{},\"promotion_boundary_state\":\"{}\",\"promotion_boundary_in_transition\":{},\"segment_base_count\":{},\"wal_delta_count\":{},\"storage_visible_count\":{},\"metadata_prefilter_count\":{},\"allowed_claim_ids_count\":{},\"has_filtering\":{},\"short_circuit_empty\":{},\"segment_base_active\":{},\"wal_delta_active\":{},\"storage_visible_active\":{},\"result_count\":{},\"result_from_segment_base_count\":{},\"result_from_wal_delta_count\":{},\"result_source_unknown_count\":{},\"result_outside_storage_visible_count\":{},\"divergence_ratio\":{:.6},\"divergence_active\":{},\"divergence_warn\":{},\"warn_delta_count\":{},\"warn_ratio\":{:.6},\"warn_reason\":{}}}",
        json_escape(&snapshot.tenant_id),
        STORAGE_MERGE_MODEL,
        json_escape(&merge_snapshot.source_of_truth_model),
        json_escape(&merge_snapshot.execution_mode),
        merge_snapshot.disk_native_segment_execution_active,
        merge_snapshot.execution_candidate_count,
        json_escape(&merge_snapshot.promotion_boundary_state),
        merge_snapshot.promotion_boundary_in_transition,
        snapshot.segment_base_count,
        snapshot.wal_delta_count,
        snapshot.storage_visible_count,
        snapshot.metadata_prefilter_count,
        snapshot.allowed_claim_ids_count,
        snapshot.has_filtering,
        snapshot.short_circuit_empty,
        merge_snapshot.segment_base_active,
        merge_snapshot.wal_delta_active,
        merge_snapshot.storage_visible_active,
        merge_snapshot.result_count,
        merge_snapshot.result_from_segment_base_count,
        merge_snapshot.result_from_wal_delta_count,
        merge_snapshot.result_source_unknown_count,
        merge_snapshot.result_outside_storage_visible_count,
        ratio,
        snapshot.wal_delta_count > 0,
        warn,
        warn_delta_count,
        warn_ratio,
        reason_json
    )
}

pub(super) fn render_placement_debug_json(
    placement_routing: Option<&PlacementRoutingRuntime>,
    placement_reload: Option<&PlacementReloadSnapshot>,
    query: &HashMap<String, String>,
) -> String {
    let route_probe = build_read_route_probe_json(placement_routing, query);
    let (enabled, local_node_id, read_preference, shard_count, placements) =
        if let Some(routing) = placement_routing {
            (
                true,
                Some(routing.local_node_id.as_str()),
                Some(read_preference_str(routing.read_preference)),
                routing.router_config.shard_ids.len(),
                routing.placements.as_slice(),
            )
        } else {
            (false, None, None, 0, &[][..])
        };
    format!(
        "{{\"enabled\":{},\"local_node_id\":{},\"read_preference\":{},\"shard_count\":{},\"placements\":{},\"route_probe\":{},\"reload\":{}}}",
        enabled,
        local_node_id
            .map(|value| format!("\"{}\"", json_escape(value)))
            .unwrap_or_else(|| "null".to_string()),
        read_preference
            .map(|value| format!("\"{}\"", value))
            .unwrap_or_else(|| "null".to_string()),
        shard_count,
        render_placements_json(placements),
        route_probe,
        render_placement_reload_json(placement_reload),
    )
}

fn render_placement_reload_json(snapshot: Option<&PlacementReloadSnapshot>) -> String {
    let snapshot = snapshot.cloned().unwrap_or_default();
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

fn build_read_route_probe_json(
    placement_routing: Option<&PlacementRoutingRuntime>,
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
        .or_else(|| query.get("query"))
        .map(String::as_str)
        .unwrap_or_default()
        .trim();
    if entity_key.is_empty() {
        return "{\"status\":\"invalid\",\"reason\":\"entity_key (or query) is required for route probe\"}".to_string();
    }
    let Some(routing) = placement_routing else {
        return "{\"status\":\"unconfigured\",\"reason\":\"placement routing is disabled\"}"
            .to_string();
    };

    match route_read_with_placement(
        tenant_id,
        entity_key,
        &routing.router_config,
        &routing.placements,
        routing.read_preference,
    ) {
        Ok(routed) => {
            let local_admission = routed.node_id == routing.local_node_id;
            let reason = if local_admission {
                "local node is selected replica"
            } else {
                "request would be rejected: local node is not selected replica"
            };
            format!(
                "{{\"status\":\"{}\",\"tenant_id\":\"{}\",\"entity_key\":\"{}\",\"local_node_id\":\"{}\",\"target_node_id\":\"{}\",\"shard_id\":{},\"epoch\":{},\"role\":\"{}\",\"read_preference\":\"{}\",\"local_admission\":{},\"reason\":\"{}\"}}",
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
                read_preference_str(routing.read_preference),
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

fn read_preference_str(value: ReadPreference) -> &'static str {
    match value {
        ReadPreference::LeaderOnly => "leader_only",
        ReadPreference::PreferFollower => "prefer_follower",
        ReadPreference::AnyHealthy => "any_healthy",
    }
}
