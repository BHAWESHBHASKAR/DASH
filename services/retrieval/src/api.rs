use graph::{
    GraphReasoningConfig, NodeReasoningSignals, compute_node_reasoning_with_config,
    traverse_edges_multi_hop,
};
use schema::{Claim, ClaimType, RetrievalRequest, Stance, StanceMode};
mod result_projection;
mod segment_storage;
#[cfg(test)]
use result_projection::TemporalAnnotation;
use result_projection::evidence_node_from_parts;
use std::collections::{HashMap, HashSet};
#[cfg(test)]
use std::path::PathBuf;
#[cfg(test)]
use std::time::Duration;
use store::InMemoryStore;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimeRange {
    pub from_unix: Option<i64>,
    pub to_unix: Option<i64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RetrieveApiRequest {
    pub tenant_id: String,
    pub query: String,
    pub query_embedding: Option<Vec<f32>>,
    pub entity_filters: Vec<String>,
    pub embedding_id_filters: Vec<String>,
    pub top_k: usize,
    pub stance_mode: StanceMode,
    pub return_graph: bool,
    pub time_range: Option<TimeRange>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CitationNode {
    pub evidence_id: String,
    pub source_id: String,
    pub stance: String,
    pub source_quality: f32,
    pub chunk_id: Option<String>,
    pub span_start: Option<u32>,
    pub span_end: Option<u32>,
    pub doc_id: Option<String>,
    pub extraction_model: Option<String>,
    pub ingested_at: Option<i64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct EvidenceNode {
    pub claim_id: String,
    pub canonical_text: String,
    pub score: f32,
    pub claim_confidence: Option<f32>,
    pub confidence_band: Option<String>,
    pub dominant_stance: Option<String>,
    pub contradiction_risk: Option<f32>,
    pub graph_score: Option<f32>,
    pub support_path_count: Option<usize>,
    pub contradiction_chain_depth: Option<usize>,
    pub supports: usize,
    pub contradicts: usize,
    pub citations: Vec<CitationNode>,
    pub event_time_unix: Option<i64>,
    pub temporal_match_mode: Option<String>,
    pub temporal_in_range: Option<bool>,
    pub claim_type: Option<String>,
    pub valid_from: Option<i64>,
    pub valid_to: Option<i64>,
    pub created_at: Option<i64>,
    pub updated_at: Option<i64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct EvidenceGraphEdge {
    pub from_claim_id: String,
    pub to_claim_id: String,
    pub relation: String,
    pub strength: f32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct EvidenceGraph {
    pub nodes: Vec<EvidenceNode>,
    pub edges: Vec<EvidenceGraphEdge>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RetrieveApiResponse {
    pub results: Vec<EvidenceNode>,
    pub graph: Option<EvidenceGraph>,
}

pub const STORAGE_MERGE_MODEL: &str = "immutable_segment_base_plus_mutable_wal_delta";
pub const STORAGE_EXECUTION_MODE_MEMORY_INDEX: &str = "memory_index_candidates";
pub const STORAGE_EXECUTION_MODE_SEGMENT_DISK_BASE: &str = "segment_disk_base_with_wal_overlay";
pub const STORAGE_SOURCE_OF_TRUTH_MODEL: &str = "wal_replay_state_with_segment_projection";
pub const STORAGE_PROMOTION_BOUNDARY_REPLAY_ONLY: &str = "replay_only";
pub const STORAGE_PROMOTION_BOUNDARY_SEGMENT_PLUS_WAL_DELTA: &str = "segment_base_plus_wal_delta";
pub const STORAGE_PROMOTION_BOUNDARY_SEGMENT_FULLY_PROMOTED: &str = "segment_base_fully_promoted";

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RetrieveStorageMergeSnapshot {
    pub source_of_truth_model: String,
    pub execution_mode: String,
    pub disk_native_segment_execution_active: bool,
    pub execution_candidate_count: usize,
    pub promotion_boundary_state: String,
    pub promotion_boundary_in_transition: bool,
    pub segment_base_active: bool,
    pub wal_delta_active: bool,
    pub storage_visible_active: bool,
    pub metadata_prefilter_count: usize,
    pub segment_base_count: usize,
    pub wal_delta_count: usize,
    pub storage_visible_count: usize,
    pub allowed_claim_ids_count: usize,
    pub result_count: usize,
    pub result_from_segment_base_count: usize,
    pub result_from_wal_delta_count: usize,
    pub result_source_unknown_count: usize,
    pub result_outside_storage_visible_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetrievePlannerDebugSnapshot {
    pub tenant_id: String,
    pub top_k: usize,
    pub stance_mode: String,
    pub has_query_embedding: bool,
    pub entity_filter_count: usize,
    pub embedding_filter_count: usize,
    pub has_filtering: bool,
    pub metadata_prefilter_count: usize,
    pub segment_base_count: usize,
    pub wal_delta_count: usize,
    pub storage_visible_count: usize,
    pub allowed_claim_ids_active: bool,
    pub allowed_claim_ids_count: usize,
    pub short_circuit_empty: bool,
    pub ann_candidate_count: usize,
    pub planner_candidate_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PlannerContext {
    tenant_id: String,
    from_unix: Option<i64>,
    to_unix: Option<i64>,
    entity_filters: Vec<String>,
    embedding_filters: Vec<String>,
    metadata_allowed_claim_ids: Option<HashSet<String>>,
    segment_base_claim_ids: Option<HashSet<String>>,
    wal_delta_claim_ids: Option<HashSet<String>>,
    storage_visible_claim_ids: Option<HashSet<String>>,
    allowed_claim_ids: Option<HashSet<String>>,
    has_filtering: bool,
    short_circuit_empty: bool,
}

#[derive(Debug, Clone)]
struct EvidenceNodeSignals {
    score: f32,
    supports: usize,
    contradicts: usize,
    citations: Vec<CitationNode>,
}

const DEFAULT_GRAPH_REASONING_MAX_HOPS: usize = 3;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SegmentPrefilterCacheMetrics {
    pub cache_hits: u64,
    pub refresh_attempts: u64,
    pub refresh_successes: u64,
    pub refresh_failures: u64,
    pub refresh_load_micros: u64,
    pub fallback_activations: u64,
    pub fallback_missing_manifest: u64,
    pub fallback_manifest_errors: u64,
    pub fallback_segment_errors: u64,
}

pub fn execute_api_query(store: &InMemoryStore, req: RetrieveApiRequest) -> RetrieveApiResponse {
    execute_api_query_with_storage_snapshot(store, req).0
}

pub fn execute_api_query_with_storage_snapshot(
    store: &InMemoryStore,
    req: RetrieveApiRequest,
) -> (RetrieveApiResponse, RetrieveStorageMergeSnapshot) {
    let planner = build_planner_context(store, &req);
    let mut merge_snapshot =
        build_storage_merge_snapshot(&planner, &[], STORAGE_EXECUTION_MODE_MEMORY_INDEX, 0);
    if planner.short_circuit_empty {
        return (
            RetrieveApiResponse {
                results: Vec::new(),
                graph: if req.return_graph {
                    Some(EvidenceGraph {
                        nodes: Vec::new(),
                        edges: Vec::new(),
                    })
                } else {
                    None
                },
            },
            merge_snapshot,
        );
    }

    let retrieval_request = RetrievalRequest {
        tenant_id: planner.tenant_id.clone(),
        query: req.query,
        top_k: req.top_k,
        stance_mode: req.stance_mode,
    };
    let disk_native_segment_execution_active = resolve_disk_native_segment_execution_enabled()
        && planner.segment_base_claim_ids.is_some()
        && planner.storage_visible_claim_ids.is_some();
    let (results, execution_mode, execution_candidate_count) =
        if disk_native_segment_execution_active {
            let candidate_claim_ids = planner
                .storage_visible_claim_ids
                .clone()
                .unwrap_or_default();
            let candidate_count = candidate_claim_ids.len();
            (
                store.retrieve_with_time_range_query_vector_and_explicit_candidate_claim_ids(
                    &retrieval_request,
                    planner.from_unix,
                    planner.to_unix,
                    req.query_embedding.as_deref(),
                    &candidate_claim_ids,
                    planner.allowed_claim_ids.as_ref(),
                ),
                STORAGE_EXECUTION_MODE_SEGMENT_DISK_BASE,
                candidate_count,
            )
        } else {
            let candidate_count = store.candidate_count_with_query_vector_and_allowed_claim_ids(
                &retrieval_request,
                req.query_embedding.as_deref(),
                (planner.from_unix, planner.to_unix),
                planner.allowed_claim_ids.as_ref(),
            );
            (
                store.retrieve_with_time_range_query_vector_and_allowed_claim_ids(
                    &retrieval_request,
                    planner.from_unix,
                    planner.to_unix,
                    req.query_embedding.as_deref(),
                    planner.allowed_claim_ids.as_ref(),
                ),
                STORAGE_EXECUTION_MODE_MEMORY_INDEX,
                candidate_count,
            )
        };

    let tenant_claims = store.claims_for_tenant(&planner.tenant_id);
    let tenant_claim_by_id: HashMap<String, Claim> = tenant_claims
        .iter()
        .cloned()
        .map(|claim| (claim.claim_id.clone(), claim))
        .collect();

    let mut nodes: Vec<EvidenceNode> = results
        .iter()
        .map(|r| {
            evidence_node_from_parts(
                r.claim_id.clone(),
                r.canonical_text.clone(),
                EvidenceNodeSignals {
                    score: r.score,
                    supports: r.supports,
                    contradicts: r.contradicts,
                    citations: r
                        .citations
                        .iter()
                        .map(|citation| CitationNode {
                            evidence_id: citation.evidence_id.clone(),
                            source_id: citation.source_id.clone(),
                            stance: stance_to_str(&citation.stance).to_string(),
                            source_quality: citation.source_quality,
                            chunk_id: citation.chunk_id.clone(),
                            span_start: citation.span_start,
                            span_end: citation.span_end,
                            doc_id: citation.doc_id.clone(),
                            extraction_model: citation.extraction_model.clone(),
                            ingested_at: citation.ingested_at,
                        })
                        .collect(),
                },
                tenant_claim_by_id.get(&r.claim_id),
                planner.from_unix,
                planner.to_unix,
            )
        })
        .collect();

    let graph = if req.return_graph {
        let graph_reasoning_config = graph_reasoning_config_from_env();
        let selected: std::collections::HashSet<String> =
            nodes.iter().map(|n| n.claim_id.clone()).collect();
        let start_ids: Vec<String> = selected.iter().cloned().collect();

        let mut all_edges = Vec::new();
        for claim in &tenant_claims {
            all_edges.extend(store.edges_for_claim(&claim.claim_id));
        }

        let traversed =
            traverse_edges_multi_hop(&start_ids, &all_edges, graph_reasoning_config.max_hops);
        let reasoning_by_claim =
            compute_node_reasoning_with_config(&start_ids, &traversed, graph_reasoning_config);
        for node in &mut nodes {
            apply_graph_reasoning(node, reasoning_by_claim.get(&node.claim_id));
        }
        let mut node_map: std::collections::HashMap<String, EvidenceNode> = nodes
            .iter()
            .cloned()
            .map(|node| (node.claim_id.clone(), node))
            .collect();
        let mut edges = Vec::new();
        for edge in traversed {
            if let Some(claim) = tenant_claim_by_id.get(&edge.from_claim_id)
                && !node_map.contains_key(&edge.from_claim_id)
            {
                node_map.insert(
                    edge.from_claim_id.clone(),
                    evidence_node_from_parts(
                        edge.from_claim_id.clone(),
                        claim.canonical_text.clone(),
                        EvidenceNodeSignals {
                            score: 0.0,
                            supports: 0,
                            contradicts: 0,
                            citations: Vec::new(),
                        },
                        Some(claim),
                        planner.from_unix,
                        planner.to_unix,
                    ),
                );
            }
            if let Some(claim) = tenant_claim_by_id.get(&edge.to_claim_id)
                && !node_map.contains_key(&edge.to_claim_id)
            {
                node_map.insert(
                    edge.to_claim_id.clone(),
                    evidence_node_from_parts(
                        edge.to_claim_id.clone(),
                        claim.canonical_text.clone(),
                        EvidenceNodeSignals {
                            score: 0.0,
                            supports: 0,
                            contradicts: 0,
                            citations: Vec::new(),
                        },
                        Some(claim),
                        planner.from_unix,
                        planner.to_unix,
                    ),
                );
            }

            edges.push(EvidenceGraphEdge {
                from_claim_id: edge.from_claim_id,
                to_claim_id: edge.to_claim_id,
                relation: format!("{:?}", edge.relation).to_ascii_lowercase(),
                strength: edge.strength,
            });
        }
        for node in node_map.values_mut() {
            apply_graph_reasoning(node, reasoning_by_claim.get(&node.claim_id));
        }

        let mut graph_nodes: Vec<EvidenceNode> = node_map.into_values().collect();
        graph_nodes.sort_by(|a, b| a.claim_id.cmp(&b.claim_id));
        Some(EvidenceGraph {
            nodes: graph_nodes,
            edges,
        })
    } else {
        None
    };

    merge_snapshot =
        build_storage_merge_snapshot(&planner, &nodes, execution_mode, execution_candidate_count);
    (
        RetrieveApiResponse {
            results: nodes,
            graph,
        },
        merge_snapshot,
    )
}

fn apply_graph_reasoning(node: &mut EvidenceNode, reasoning: Option<&NodeReasoningSignals>) {
    if let Some(reasoning) = reasoning {
        node.graph_score = Some(reasoning.graph_score);
        node.support_path_count = Some(reasoning.support_path_count);
        node.contradiction_chain_depth = Some(reasoning.contradiction_chain_depth);
    }
}

pub fn build_retrieve_planner_debug_snapshot(
    store: &InMemoryStore,
    req: &RetrieveApiRequest,
) -> RetrievePlannerDebugSnapshot {
    let planner = build_planner_context(store, req);
    let diagnostics_req = RetrievalRequest {
        tenant_id: planner.tenant_id.clone(),
        query: req.query.clone(),
        top_k: req.top_k,
        stance_mode: req.stance_mode.clone(),
    };
    let ann_candidate_count = req
        .query_embedding
        .as_ref()
        .map(|embedding| {
            store.ann_candidate_count_for_query_vector(&planner.tenant_id, embedding, req.top_k)
        })
        .unwrap_or(0);
    let planner_candidate_count = if planner.short_circuit_empty {
        0
    } else {
        store.candidate_count_with_query_vector_and_allowed_claim_ids(
            &diagnostics_req,
            req.query_embedding.as_deref(),
            (planner.from_unix, planner.to_unix),
            planner.allowed_claim_ids.as_ref(),
        )
    };

    RetrievePlannerDebugSnapshot {
        tenant_id: planner.tenant_id,
        top_k: req.top_k,
        stance_mode: stance_mode_to_str(req.stance_mode.clone()).to_string(),
        has_query_embedding: req.query_embedding.as_ref().is_some_and(|v| !v.is_empty()),
        entity_filter_count: planner.entity_filters.len(),
        embedding_filter_count: planner.embedding_filters.len(),
        has_filtering: planner.has_filtering,
        metadata_prefilter_count: planner
            .metadata_allowed_claim_ids
            .as_ref()
            .map_or(0, HashSet::len),
        segment_base_count: planner
            .segment_base_claim_ids
            .as_ref()
            .map_or(0, HashSet::len),
        wal_delta_count: planner.wal_delta_claim_ids.as_ref().map_or(0, HashSet::len),
        storage_visible_count: planner
            .storage_visible_claim_ids
            .as_ref()
            .map_or(0, HashSet::len),
        allowed_claim_ids_active: planner.allowed_claim_ids.is_some(),
        allowed_claim_ids_count: planner.allowed_claim_ids.as_ref().map_or(0, HashSet::len),
        short_circuit_empty: planner.short_circuit_empty,
        ann_candidate_count,
        planner_candidate_count,
    }
}

#[cfg(test)]
fn temporal_annotation_for_claim(
    claim: Option<&Claim>,
    query_from_unix: Option<i64>,
    query_to_unix: Option<i64>,
) -> result_projection::TemporalAnnotation {
    result_projection::temporal_annotation_for_claim(claim, query_from_unix, query_to_unix)
}

#[cfg(test)]
fn confidence_band_for_claim_confidence(value: f32) -> &'static str {
    result_projection::confidence_band_for_claim_confidence(value)
}

#[cfg(test)]
fn dominant_stance_for_counts(supports: usize, contradicts: usize) -> Option<&'static str> {
    result_projection::dominant_stance_for_counts(supports, contradicts)
}

#[cfg(test)]
fn contradiction_risk_for_counts(supports: usize, contradicts: usize) -> Option<f32> {
    result_projection::contradiction_risk_for_counts(supports, contradicts)
}

fn build_planner_context(store: &InMemoryStore, req: &RetrieveApiRequest) -> PlannerContext {
    let tenant_id = req.tenant_id.clone();
    let (from_unix, to_unix) = req
        .time_range
        .as_ref()
        .map(|t| (t.from_unix, t.to_unix))
        .unwrap_or((None, None));
    let invalid_time_range = matches!(
        (from_unix, to_unix),
        (Some(from), Some(to)) if from > to
    );
    let entity_filters: Vec<String> = req
        .entity_filters
        .iter()
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty())
        .collect();
    let embedding_filters: Vec<String> = req
        .embedding_id_filters
        .iter()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .collect();
    let metadata_allowed_claim_ids =
        build_metadata_prefilter_claim_ids(store, &tenant_id, &entity_filters, &embedding_filters);
    let segment_base_claim_ids = build_segment_prefilter_claim_ids(&tenant_id);
    let wal_delta_claim_ids =
        build_wal_delta_claim_ids(store, &tenant_id, segment_base_claim_ids.as_ref());
    let storage_visible_claim_ids = merge_segment_base_with_wal_delta_claim_ids(
        segment_base_claim_ids.as_ref(),
        wal_delta_claim_ids.as_ref(),
    );
    let has_filtering = !entity_filters.is_empty()
        || !embedding_filters.is_empty()
        || storage_visible_claim_ids.is_some();
    let allowed_claim_ids = merge_allowed_claim_ids(
        metadata_allowed_claim_ids.as_ref(),
        storage_visible_claim_ids.as_ref(),
    );
    let short_circuit_empty = invalid_time_range
        || (has_filtering
            && allowed_claim_ids
                .as_ref()
                .is_some_and(|claim_ids| claim_ids.is_empty()));

    PlannerContext {
        tenant_id,
        from_unix,
        to_unix,
        entity_filters,
        embedding_filters,
        metadata_allowed_claim_ids,
        segment_base_claim_ids,
        wal_delta_claim_ids,
        storage_visible_claim_ids,
        allowed_claim_ids,
        has_filtering,
        short_circuit_empty,
    }
}

fn build_storage_merge_snapshot(
    planner: &PlannerContext,
    results: &[EvidenceNode],
    execution_mode: &str,
    execution_candidate_count: usize,
) -> RetrieveStorageMergeSnapshot {
    let (promotion_boundary_state, promotion_boundary_in_transition) =
        resolve_storage_promotion_boundary(planner);
    let segment_base_active = planner.segment_base_claim_ids.is_some();
    let wal_delta_active = planner.wal_delta_claim_ids.is_some();
    let storage_visible_active = planner.storage_visible_claim_ids.is_some();
    let segment_base = planner.segment_base_claim_ids.as_ref();
    let wal_delta = planner.wal_delta_claim_ids.as_ref();
    let storage_visible = planner.storage_visible_claim_ids.as_ref();

    let mut result_from_segment_base_count = 0usize;
    let mut result_from_wal_delta_count = 0usize;
    let mut result_source_unknown_count = 0usize;
    let mut result_outside_storage_visible_count = 0usize;

    for node in results {
        let mut classified = false;
        if segment_base.is_some_and(|ids| ids.contains(&node.claim_id)) {
            result_from_segment_base_count += 1;
            classified = true;
        }
        if wal_delta.is_some_and(|ids| ids.contains(&node.claim_id)) {
            result_from_wal_delta_count += 1;
            classified = true;
        }
        if !classified {
            result_source_unknown_count += 1;
        }
        if storage_visible.is_some_and(|ids| !ids.contains(&node.claim_id)) {
            result_outside_storage_visible_count += 1;
        }
    }

    RetrieveStorageMergeSnapshot {
        source_of_truth_model: STORAGE_SOURCE_OF_TRUTH_MODEL.to_string(),
        execution_mode: execution_mode.to_string(),
        disk_native_segment_execution_active: execution_mode
            == STORAGE_EXECUTION_MODE_SEGMENT_DISK_BASE,
        execution_candidate_count,
        promotion_boundary_state: promotion_boundary_state.to_string(),
        promotion_boundary_in_transition,
        segment_base_active,
        wal_delta_active,
        storage_visible_active,
        metadata_prefilter_count: planner
            .metadata_allowed_claim_ids
            .as_ref()
            .map_or(0, HashSet::len),
        segment_base_count: planner
            .segment_base_claim_ids
            .as_ref()
            .map_or(0, HashSet::len),
        wal_delta_count: planner.wal_delta_claim_ids.as_ref().map_or(0, HashSet::len),
        storage_visible_count: planner
            .storage_visible_claim_ids
            .as_ref()
            .map_or(0, HashSet::len),
        allowed_claim_ids_count: planner.allowed_claim_ids.as_ref().map_or(0, HashSet::len),
        result_count: results.len(),
        result_from_segment_base_count,
        result_from_wal_delta_count,
        result_source_unknown_count,
        result_outside_storage_visible_count,
    }
}

fn resolve_storage_promotion_boundary(planner: &PlannerContext) -> (&'static str, bool) {
    if planner.segment_base_claim_ids.is_none() {
        return (STORAGE_PROMOTION_BOUNDARY_REPLAY_ONLY, false);
    }
    let wal_delta_count = planner.wal_delta_claim_ids.as_ref().map_or(0, HashSet::len);
    if wal_delta_count == 0 {
        (STORAGE_PROMOTION_BOUNDARY_SEGMENT_FULLY_PROMOTED, false)
    } else {
        (STORAGE_PROMOTION_BOUNDARY_SEGMENT_PLUS_WAL_DELTA, true)
    }
}

fn build_metadata_prefilter_claim_ids(
    store: &InMemoryStore,
    tenant_id: &str,
    entity_filters: &[String],
    embedding_filters: &[String],
) -> Option<std::collections::HashSet<String>> {
    let entity_candidates = if entity_filters.is_empty() {
        None
    } else {
        let mut ids = std::collections::HashSet::new();
        for filter in entity_filters {
            ids.extend(store.claim_ids_for_entity(tenant_id, filter));
        }
        Some(ids)
    };
    let embedding_candidates = if embedding_filters.is_empty() {
        None
    } else {
        let mut ids = std::collections::HashSet::new();
        for filter in embedding_filters {
            ids.extend(store.claim_ids_for_embedding_id(tenant_id, filter));
        }
        Some(ids)
    };

    match (entity_candidates, embedding_candidates) {
        (None, None) => None,
        (Some(entity), None) => Some(entity),
        (None, Some(embedding)) => Some(embedding),
        (Some(entity), Some(embedding)) => Some(entity.intersection(&embedding).cloned().collect()),
    }
}

fn build_segment_prefilter_claim_ids(tenant_id: &str) -> Option<HashSet<String>> {
    segment_storage::build_segment_prefilter_claim_ids(tenant_id)
}

#[cfg(test)]
fn build_segment_prefilter_claim_ids_from_root(
    tenant_id: &str,
    segment_root: PathBuf,
) -> Option<HashSet<String>> {
    segment_storage::build_segment_prefilter_claim_ids_from_root(tenant_id, segment_root)
}

fn build_wal_delta_claim_ids(
    store: &InMemoryStore,
    tenant_id: &str,
    segment_base_claim_ids: Option<&HashSet<String>>,
) -> Option<HashSet<String>> {
    segment_storage::build_wal_delta_claim_ids(store, tenant_id, segment_base_claim_ids)
}

fn merge_segment_base_with_wal_delta_claim_ids(
    segment_base: Option<&HashSet<String>>,
    wal_delta: Option<&HashSet<String>>,
) -> Option<HashSet<String>> {
    segment_storage::merge_segment_base_with_wal_delta_claim_ids(segment_base, wal_delta)
}

fn merge_allowed_claim_ids(
    metadata: Option<&HashSet<String>>,
    segment: Option<&HashSet<String>>,
) -> Option<HashSet<String>> {
    segment_storage::merge_allowed_claim_ids(metadata, segment)
}

fn env_with_fallback(primary: &str, fallback: &str) -> Option<String> {
    std::env::var(primary)
        .ok()
        .or_else(|| std::env::var(fallback).ok())
}

fn env_bool_with_fallback(primary: &str, fallback: &str, default: bool) -> bool {
    let Some(raw) = env_with_fallback(primary, fallback) else {
        return default;
    };
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => true,
        "0" | "false" | "no" | "off" => false,
        _ => default,
    }
}

fn env_f32_with_fallback(primary: &str, fallback: &str) -> Option<f32> {
    env_with_fallback(primary, fallback).and_then(|value| value.parse::<f32>().ok())
}

fn resolve_disk_native_segment_execution_enabled() -> bool {
    env_bool_with_fallback(
        "DASH_RETRIEVAL_DISK_NATIVE_SEGMENT_EXECUTION",
        "EME_RETRIEVAL_DISK_NATIVE_SEGMENT_EXECUTION",
        true,
    )
}

fn graph_reasoning_config_from_env() -> GraphReasoningConfig {
    let mut config = GraphReasoningConfig::default();
    config.max_hops = env_with_fallback(
        "DASH_RETRIEVAL_GRAPH_MAX_HOPS",
        "EME_RETRIEVAL_GRAPH_MAX_HOPS",
    )
    .and_then(|value| value.parse::<usize>().ok())
    .filter(|value| *value > 0)
    .unwrap_or(DEFAULT_GRAPH_REASONING_MAX_HOPS);
    config.edge_depth_decay = env_f32_with_fallback(
        "DASH_RETRIEVAL_GRAPH_EDGE_DEPTH_DECAY",
        "EME_RETRIEVAL_GRAPH_EDGE_DEPTH_DECAY",
    )
    .unwrap_or(config.edge_depth_decay)
    .clamp(0.0, 1.0);
    config.support_path_bonus = env_f32_with_fallback(
        "DASH_RETRIEVAL_GRAPH_SUPPORT_PATH_BONUS",
        "EME_RETRIEVAL_GRAPH_SUPPORT_PATH_BONUS",
    )
    .unwrap_or(config.support_path_bonus)
    .max(0.0);
    config.contradiction_depth_penalty = env_f32_with_fallback(
        "DASH_RETRIEVAL_GRAPH_CONTRADICTION_DEPTH_PENALTY",
        "EME_RETRIEVAL_GRAPH_CONTRADICTION_DEPTH_PENALTY",
    )
    .unwrap_or(config.contradiction_depth_penalty)
    .max(0.0);
    config
}

#[cfg(test)]
fn segment_prefilter_refresh_interval() -> Duration {
    segment_storage::segment_prefilter_refresh_interval()
}

pub fn segment_prefilter_cache_metrics_snapshot() -> SegmentPrefilterCacheMetrics {
    segment_storage::segment_prefilter_cache_metrics_snapshot()
}

pub fn reset_segment_prefilter_cache_metrics() {
    segment_storage::reset_segment_prefilter_cache_metrics();
}

fn stance_to_str(stance: &Stance) -> &'static str {
    match stance {
        Stance::Supports => "supports",
        Stance::Contradicts => "contradicts",
        Stance::Neutral => "neutral",
    }
}

fn stance_mode_to_str(mode: StanceMode) -> &'static str {
    match mode {
        StanceMode::Balanced => "balanced",
        StanceMode::SupportOnly => "support_only",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use indexer::{Segment, Tier, persist_segments_atomic};
    use schema::{Claim, ClaimEdge, ClaimType, Evidence, Relation, Stance};
    use std::ffi::{OsStr, OsString};
    use std::sync::{Mutex, OnceLock};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir(tag: &str) -> PathBuf {
        let mut out = std::env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock should be monotonic")
            .as_nanos();
        out.push(format!(
            "dash-retrieval-segment-{}-{}-{}",
            tag,
            std::process::id(),
            nanos
        ));
        out
    }

    fn clear_segment_cache_for_tests() {
        segment_storage::clear_segment_prefilter_cache_for_tests();
    }

    fn segment_cache_test_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    #[allow(unused_unsafe)]
    fn set_env_var_for_tests(key: &str, value: &OsStr) {
        unsafe {
            std::env::set_var(key, value);
        }
    }

    #[allow(unused_unsafe)]
    fn restore_env_var_for_tests(key: &str, value: Option<&OsStr>) {
        match value {
            Some(value) => unsafe {
                std::env::set_var(key, value);
            },
            None => unsafe {
                std::env::remove_var(key);
            },
        }
    }

    struct EnvVarGuard {
        key: &'static str,
        previous: Option<OsString>,
    }

    impl EnvVarGuard {
        fn set(key: &'static str, value: &OsStr) -> Self {
            let previous = std::env::var_os(key);
            set_env_var_for_tests(key, value);
            Self { key, previous }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            restore_env_var_for_tests(self.key, self.previous.as_deref());
        }
    }

    #[test]
    fn graph_reasoning_config_reads_env_overrides() {
        let _max_hops = EnvVarGuard::set("DASH_RETRIEVAL_GRAPH_MAX_HOPS", OsStr::new("5"));
        let _depth_decay =
            EnvVarGuard::set("DASH_RETRIEVAL_GRAPH_EDGE_DEPTH_DECAY", OsStr::new("0.45"));
        let _support_bonus = EnvVarGuard::set(
            "DASH_RETRIEVAL_GRAPH_SUPPORT_PATH_BONUS",
            OsStr::new("0.25"),
        );
        let _contradiction_penalty = EnvVarGuard::set(
            "DASH_RETRIEVAL_GRAPH_CONTRADICTION_DEPTH_PENALTY",
            OsStr::new("0.30"),
        );

        let config = graph_reasoning_config_from_env();
        assert_eq!(config.max_hops, 5);
        assert!((config.edge_depth_decay - 0.45).abs() < 0.0001);
        assert!((config.support_path_bonus - 0.25).abs() < 0.0001);
        assert!((config.contradiction_depth_penalty - 0.30).abs() < 0.0001);
    }

    #[test]
    fn execute_api_query_returns_graph_when_requested() {
        let mut store = InMemoryStore::new();
        store
            .ingest_bundle(
                Claim {
                    claim_id: "c1".into(),
                    tenant_id: "tenant-a".into(),
                    canonical_text: "Company X acquired Company Y".into(),
                    confidence: 0.9,
                    event_time_unix: None,
                    entities: vec![],
                    embedding_ids: vec![],
                    claim_type: None,
                    valid_from: None,
                    valid_to: None,
                    created_at: None,
                    updated_at: None,
                },
                vec![Evidence {
                    evidence_id: "e1".into(),
                    claim_id: "c1".into(),
                    source_id: "source://doc-1".into(),
                    stance: Stance::Supports,
                    source_quality: 0.9,
                    chunk_id: None,
                    span_start: None,
                    span_end: None,
                    doc_id: None,
                    extraction_model: None,
                    ingested_at: None,
                }],
                vec![ClaimEdge {
                    edge_id: "edge1".into(),
                    from_claim_id: "c1".into(),
                    to_claim_id: "c2".into(),
                    relation: Relation::Supports,
                    strength: 0.8,
                    reason_codes: vec![],
                    created_at: None,
                }],
            )
            .unwrap();
        store
            .ingest_bundle(
                Claim {
                    claim_id: "c2".into(),
                    tenant_id: "tenant-a".into(),
                    canonical_text: "Company Y integration started".into(),
                    confidence: 0.85,
                    event_time_unix: None,
                    entities: vec![],
                    embedding_ids: vec![],
                    claim_type: None,
                    valid_from: None,
                    valid_to: None,
                    created_at: None,
                    updated_at: None,
                },
                vec![Evidence {
                    evidence_id: "e2".into(),
                    claim_id: "c2".into(),
                    source_id: "source://doc-2".into(),
                    stance: Stance::Supports,
                    source_quality: 0.85,
                    chunk_id: None,
                    span_start: None,
                    span_end: None,
                    doc_id: None,
                    extraction_model: None,
                    ingested_at: None,
                }],
                vec![],
            )
            .unwrap();

        let response = execute_api_query(
            &store,
            RetrieveApiRequest {
                tenant_id: "tenant-a".into(),
                query: "company x acquired company y".into(),
                query_embedding: None,
                entity_filters: vec![],
                embedding_id_filters: vec![],
                top_k: 2,
                stance_mode: StanceMode::Balanced,
                return_graph: true,
                time_range: None,
            },
        );

        assert_eq!(response.results.len(), 2);
        assert!(!response.results[0].citations.is_empty());
        assert_eq!(response.results[0].citations[0].stance, "supports");
        assert!(response.results[0].graph_score.is_some());
        assert!(response.results[0].support_path_count.is_some());
        assert!(response.results[0].contradiction_chain_depth.is_some());
        assert!(response.graph.is_some());
        let graph = response.graph.unwrap();
        assert_eq!(graph.nodes.len(), 2);
        assert_eq!(graph.edges.len(), 1);
        assert_eq!(graph.edges[0].relation, "supports");
        assert!(
            graph
                .nodes
                .iter()
                .all(|node| node.graph_score.is_some() && node.support_path_count.is_some())
        );
    }

    #[test]
    fn execute_api_query_surfaces_temporal_claim_metadata_on_results_and_graph_nodes() {
        let mut store = InMemoryStore::new();
        store
            .ingest_bundle(
                Claim {
                    claim_id: "c1".into(),
                    tenant_id: "tenant-a".into(),
                    canonical_text: "Launch window for Mission Aurora remains open".into(),
                    confidence: 0.91,
                    event_time_unix: Some(1_735_689_600),
                    entities: vec![],
                    embedding_ids: vec![],
                    claim_type: Some(ClaimType::Temporal),
                    valid_from: Some(1_735_603_200),
                    valid_to: Some(1_735_776_000),
                    created_at: Some(1_735_603_200_000),
                    updated_at: Some(1_735_689_600_000),
                },
                vec![Evidence {
                    evidence_id: "e1".into(),
                    claim_id: "c1".into(),
                    source_id: "source://doc-1".into(),
                    stance: Stance::Supports,
                    source_quality: 0.92,
                    chunk_id: None,
                    span_start: None,
                    span_end: None,
                    doc_id: None,
                    extraction_model: None,
                    ingested_at: None,
                }],
                vec![ClaimEdge {
                    edge_id: "edge1".into(),
                    from_claim_id: "c1".into(),
                    to_claim_id: "c2".into(),
                    relation: Relation::Supports,
                    strength: 0.8,
                    reason_codes: vec![],
                    created_at: None,
                }],
            )
            .expect("ingest c1 should succeed");
        store
            .ingest_bundle(
                Claim {
                    claim_id: "c2".into(),
                    tenant_id: "tenant-a".into(),
                    canonical_text: "Mission Aurora weather constraints are improving".into(),
                    confidence: 0.86,
                    event_time_unix: Some(1_735_692_000),
                    entities: vec![],
                    embedding_ids: vec![],
                    claim_type: Some(ClaimType::Factual),
                    valid_from: Some(1_735_603_200),
                    valid_to: Some(1_735_862_400),
                    created_at: Some(1_735_603_200_000),
                    updated_at: Some(1_735_692_000_000),
                },
                vec![],
                vec![],
            )
            .expect("ingest c2 should succeed");

        let response = execute_api_query(
            &store,
            RetrieveApiRequest {
                tenant_id: "tenant-a".into(),
                query: "mission aurora launch window".into(),
                query_embedding: None,
                entity_filters: vec![],
                embedding_id_filters: vec![],
                top_k: 1,
                stance_mode: StanceMode::Balanced,
                return_graph: true,
                time_range: None,
            },
        );

        assert_eq!(response.results.len(), 1);
        let node = &response.results[0];
        assert_eq!(node.claim_id, "c1");
        assert_eq!(node.claim_confidence, Some(0.91));
        assert_eq!(node.confidence_band.as_deref(), Some("high"));
        assert_eq!(node.dominant_stance.as_deref(), Some("supports"));
        assert_eq!(node.contradiction_risk, Some(0.0));
        assert!(node.graph_score.is_some());
        assert_eq!(node.support_path_count, Some(1));
        assert_eq!(node.contradiction_chain_depth, Some(0));
        assert_eq!(node.event_time_unix, Some(1_735_689_600));
        assert_eq!(node.temporal_match_mode, None);
        assert_eq!(node.temporal_in_range, None);
        assert_eq!(node.claim_type.as_deref(), Some("temporal"));
        assert_eq!(node.valid_from, Some(1_735_603_200));
        assert_eq!(node.valid_to, Some(1_735_776_000));
        assert_eq!(node.created_at, Some(1_735_603_200_000));
        assert_eq!(node.updated_at, Some(1_735_689_600_000));

        let graph = response.graph.expect("graph should be present");
        let c2_graph_node = graph
            .nodes
            .iter()
            .find(|entry| entry.claim_id == "c2")
            .expect("graph should include connected c2 node");
        assert_eq!(c2_graph_node.claim_confidence, Some(0.86));
        assert_eq!(c2_graph_node.confidence_band.as_deref(), Some("high"));
        assert_eq!(c2_graph_node.dominant_stance, None);
        assert_eq!(c2_graph_node.contradiction_risk, None);
        assert!(c2_graph_node.graph_score.is_some());
        assert_eq!(c2_graph_node.support_path_count, Some(1));
        assert_eq!(c2_graph_node.contradiction_chain_depth, Some(0));
        assert_eq!(c2_graph_node.temporal_match_mode, None);
        assert_eq!(c2_graph_node.temporal_in_range, None);
        assert_eq!(c2_graph_node.claim_type.as_deref(), Some("factual"));
        assert_eq!(c2_graph_node.valid_to, Some(1_735_862_400));
        assert_eq!(c2_graph_node.updated_at, Some(1_735_692_000_000));
    }

    #[test]
    fn confidence_band_for_claim_confidence_uses_expected_thresholds() {
        assert_eq!(confidence_band_for_claim_confidence(0.80), "high");
        assert_eq!(confidence_band_for_claim_confidence(0.79), "medium");
        assert_eq!(confidence_band_for_claim_confidence(0.50), "medium");
        assert_eq!(confidence_band_for_claim_confidence(0.49), "low");
    }

    #[test]
    fn stance_summary_for_counts_uses_expected_values() {
        assert_eq!(dominant_stance_for_counts(2, 0), Some("supports"));
        assert_eq!(dominant_stance_for_counts(0, 2), Some("contradicts"));
        assert_eq!(dominant_stance_for_counts(2, 2), Some("balanced"));
        assert_eq!(dominant_stance_for_counts(0, 0), None);

        assert_eq!(contradiction_risk_for_counts(2, 0), Some(0.0));
        assert_eq!(contradiction_risk_for_counts(0, 2), Some(1.0));
        assert_eq!(contradiction_risk_for_counts(2, 2), Some(0.5));
        assert_eq!(contradiction_risk_for_counts(0, 0), None);
    }

    #[test]
    fn temporal_annotation_for_claim_uses_expected_match_modes() {
        let claim_event_only = Claim {
            claim_id: "c-event".into(),
            tenant_id: "tenant-a".into(),
            canonical_text: "event only".into(),
            confidence: 0.8,
            event_time_unix: Some(100),
            entities: vec![],
            embedding_ids: vec![],
            claim_type: None,
            valid_from: None,
            valid_to: None,
            created_at: None,
            updated_at: None,
        };
        let event_annotation =
            temporal_annotation_for_claim(Some(&claim_event_only), Some(90), Some(110));
        assert_eq!(
            event_annotation,
            TemporalAnnotation {
                match_mode: Some("event_time"),
                in_range: Some(true)
            }
        );

        let claim_window_only = Claim {
            claim_id: "c-window".into(),
            tenant_id: "tenant-a".into(),
            canonical_text: "window only".into(),
            confidence: 0.8,
            event_time_unix: None,
            entities: vec![],
            embedding_ids: vec![],
            claim_type: None,
            valid_from: Some(95),
            valid_to: Some(120),
            created_at: None,
            updated_at: None,
        };
        let window_annotation =
            temporal_annotation_for_claim(Some(&claim_window_only), Some(90), Some(110));
        assert_eq!(
            window_annotation,
            TemporalAnnotation {
                match_mode: Some("validity_window"),
                in_range: Some(true)
            }
        );

        let claim_both = Claim {
            claim_id: "c-both".into(),
            tenant_id: "tenant-a".into(),
            canonical_text: "both".into(),
            confidence: 0.8,
            event_time_unix: Some(200),
            entities: vec![],
            embedding_ids: vec![],
            claim_type: None,
            valid_from: Some(50),
            valid_to: Some(80),
            created_at: None,
            updated_at: None,
        };
        let both_annotation = temporal_annotation_for_claim(Some(&claim_both), Some(90), Some(110));
        assert_eq!(
            both_annotation,
            TemporalAnnotation {
                match_mode: Some("event_and_validity_window"),
                in_range: Some(false)
            }
        );

        let missing_temporal = Claim {
            claim_id: "c-none".into(),
            tenant_id: "tenant-a".into(),
            canonical_text: "none".into(),
            confidence: 0.8,
            event_time_unix: None,
            entities: vec![],
            embedding_ids: vec![],
            claim_type: None,
            valid_from: None,
            valid_to: None,
            created_at: None,
            updated_at: None,
        };
        let none_annotation =
            temporal_annotation_for_claim(Some(&missing_temporal), Some(90), Some(110));
        assert_eq!(
            none_annotation,
            TemporalAnnotation {
                match_mode: Some("no_temporal_data"),
                in_range: Some(false)
            }
        );

        let no_filter_annotation =
            temporal_annotation_for_claim(Some(&claim_event_only), None, None);
        assert_eq!(
            no_filter_annotation,
            TemporalAnnotation {
                match_mode: None,
                in_range: None
            }
        );
    }

    #[test]
    fn execute_api_query_applies_time_range_filter() {
        let mut store = InMemoryStore::new();
        store
            .ingest_bundle(
                Claim {
                    claim_id: "c-old".into(),
                    tenant_id: "tenant-a".into(),
                    canonical_text: "Project Orion launch milestone".into(),
                    confidence: 0.9,
                    event_time_unix: Some(100),
                    entities: vec![],
                    embedding_ids: vec![],
                    claim_type: None,
                    valid_from: None,
                    valid_to: None,
                    created_at: None,
                    updated_at: None,
                },
                vec![Evidence {
                    evidence_id: "e-old".into(),
                    claim_id: "c-old".into(),
                    source_id: "source://old".into(),
                    stance: Stance::Supports,
                    source_quality: 0.9,
                    chunk_id: None,
                    span_start: None,
                    span_end: None,
                    doc_id: None,
                    extraction_model: None,
                    ingested_at: None,
                }],
                vec![],
            )
            .unwrap();
        store
            .ingest_bundle(
                Claim {
                    claim_id: "c-new".into(),
                    tenant_id: "tenant-a".into(),
                    canonical_text: "Project Orion launch milestone".into(),
                    confidence: 0.9,
                    event_time_unix: Some(200),
                    entities: vec![],
                    embedding_ids: vec![],
                    claim_type: None,
                    valid_from: None,
                    valid_to: None,
                    created_at: None,
                    updated_at: None,
                },
                vec![Evidence {
                    evidence_id: "e-new".into(),
                    claim_id: "c-new".into(),
                    source_id: "source://new".into(),
                    stance: Stance::Supports,
                    source_quality: 0.9,
                    chunk_id: None,
                    span_start: None,
                    span_end: None,
                    doc_id: None,
                    extraction_model: None,
                    ingested_at: None,
                }],
                vec![],
            )
            .unwrap();

        let response = execute_api_query(
            &store,
            RetrieveApiRequest {
                tenant_id: "tenant-a".into(),
                query: "project orion launch milestone".into(),
                query_embedding: None,
                entity_filters: vec![],
                embedding_id_filters: vec![],
                top_k: 5,
                stance_mode: StanceMode::Balanced,
                return_graph: false,
                time_range: Some(TimeRange {
                    from_unix: Some(150),
                    to_unix: Some(250),
                }),
            },
        );

        assert_eq!(response.results.len(), 1);
        assert_eq!(response.results[0].claim_id, "c-new");
        assert_eq!(
            response.results[0].temporal_match_mode.as_deref(),
            Some("event_time")
        );
        assert_eq!(response.results[0].temporal_in_range, Some(true));
    }

    #[test]
    fn execute_api_query_applies_entity_and_embedding_filters() {
        let mut store = InMemoryStore::new();
        store
            .ingest_bundle(
                Claim {
                    claim_id: "c1".into(),
                    tenant_id: "tenant-a".into(),
                    canonical_text: "Company X acquired Company Y".into(),
                    confidence: 0.9,
                    event_time_unix: None,
                    entities: vec!["Company X".into()],
                    embedding_ids: vec!["emb://x".into()],
                    claim_type: None,
                    valid_from: None,
                    valid_to: None,
                    created_at: None,
                    updated_at: None,
                },
                vec![],
                vec![],
            )
            .unwrap();
        store
            .ingest_bundle(
                Claim {
                    claim_id: "c2".into(),
                    tenant_id: "tenant-a".into(),
                    canonical_text: "Company Z acquired Company Q".into(),
                    confidence: 0.9,
                    event_time_unix: None,
                    entities: vec!["Company Z".into()],
                    embedding_ids: vec!["emb://z".into()],
                    claim_type: None,
                    valid_from: None,
                    valid_to: None,
                    created_at: None,
                    updated_at: None,
                },
                vec![],
                vec![],
            )
            .unwrap();

        let response = execute_api_query(
            &store,
            RetrieveApiRequest {
                tenant_id: "tenant-a".into(),
                query: "acquired company".into(),
                query_embedding: None,
                entity_filters: vec!["company x".into()],
                embedding_id_filters: vec!["emb://x".into()],
                top_k: 1,
                stance_mode: StanceMode::Balanced,
                return_graph: false,
                time_range: None,
            },
        );

        assert_eq!(response.results.len(), 1);
        assert_eq!(response.results[0].claim_id, "c1");
    }

    #[test]
    fn execute_api_query_returns_empty_when_metadata_prefilter_has_no_matches() {
        let mut store = InMemoryStore::new();
        store
            .ingest_bundle(
                Claim {
                    claim_id: "c1".into(),
                    tenant_id: "tenant-a".into(),
                    canonical_text: "Company X acquired Company Y".into(),
                    confidence: 0.9,
                    event_time_unix: None,
                    entities: vec!["Company X".into()],
                    embedding_ids: vec!["emb://x".into()],
                    claim_type: None,
                    valid_from: None,
                    valid_to: None,
                    created_at: None,
                    updated_at: None,
                },
                vec![],
                vec![],
            )
            .unwrap();

        let response = execute_api_query(
            &store,
            RetrieveApiRequest {
                tenant_id: "tenant-a".into(),
                query: "acquired company".into(),
                query_embedding: None,
                entity_filters: vec!["company z".into()],
                embedding_id_filters: vec!["emb://z".into()],
                top_k: 1,
                stance_mode: StanceMode::Balanced,
                return_graph: true,
                time_range: None,
            },
        );

        assert!(response.results.is_empty());
        let graph = response.graph.expect("graph payload should be present");
        assert!(graph.nodes.is_empty());
        assert!(graph.edges.is_empty());
    }

    #[test]
    fn merge_allowed_claim_ids_intersects_metadata_and_segment_sets() {
        let metadata: HashSet<String> = ["c1".to_string(), "c2".to_string()].into_iter().collect();
        let segment: HashSet<String> = ["c2".to_string(), "c3".to_string()].into_iter().collect();
        let merged = merge_allowed_claim_ids(Some(&metadata), Some(&segment))
            .expect("merged set should exist");
        assert_eq!(merged.len(), 1);
        assert!(merged.contains("c2"));
    }

    #[test]
    fn merge_segment_base_with_wal_delta_claim_ids_unions_sets() {
        let segment_base: HashSet<String> = ["c-segment".to_string(), "c-shared".to_string()]
            .into_iter()
            .collect();
        let wal_delta: HashSet<String> = ["c-delta".to_string(), "c-shared".to_string()]
            .into_iter()
            .collect();
        let merged =
            merge_segment_base_with_wal_delta_claim_ids(Some(&segment_base), Some(&wal_delta))
                .expect("merged set should exist");
        assert_eq!(merged.len(), 3);
        assert!(merged.contains("c-segment"));
        assert!(merged.contains("c-shared"));
        assert!(merged.contains("c-delta"));
    }

    #[test]
    fn planner_debug_snapshot_reports_stage_counts() {
        let mut store = InMemoryStore::new();
        store
            .ingest_bundle(
                Claim {
                    claim_id: "c1".into(),
                    tenant_id: "tenant-a".into(),
                    canonical_text: "Company X acquired Company Y".into(),
                    confidence: 0.9,
                    event_time_unix: None,
                    entities: vec!["Company X".into()],
                    embedding_ids: vec!["emb://x".into()],
                    claim_type: None,
                    valid_from: None,
                    valid_to: None,
                    created_at: None,
                    updated_at: None,
                },
                vec![],
                vec![],
            )
            .expect("ingest c1 should succeed");
        store
            .ingest_bundle(
                Claim {
                    claim_id: "c2".into(),
                    tenant_id: "tenant-a".into(),
                    canonical_text: "Company Z acquired Company Q".into(),
                    confidence: 0.9,
                    event_time_unix: None,
                    entities: vec!["Company Z".into()],
                    embedding_ids: vec!["emb://z".into()],
                    claim_type: None,
                    valid_from: None,
                    valid_to: None,
                    created_at: None,
                    updated_at: None,
                },
                vec![],
                vec![],
            )
            .expect("ingest c2 should succeed");
        store
            .upsert_claim_vector("c1", vec![0.8, 0.2, 0.1, 0.9])
            .expect("upsert vector c1 should work");
        store
            .upsert_claim_vector("c2", vec![0.1, 0.9, 0.8, 0.2])
            .expect("upsert vector c2 should work");

        let snapshot = build_retrieve_planner_debug_snapshot(
            &store,
            &RetrieveApiRequest {
                tenant_id: "tenant-a".into(),
                query: "acquired company".into(),
                query_embedding: Some(vec![0.8, 0.2, 0.1, 0.9]),
                entity_filters: vec!["company x".into()],
                embedding_id_filters: vec!["emb://x".into()],
                top_k: 5,
                stance_mode: StanceMode::Balanced,
                return_graph: false,
                time_range: None,
            },
        );

        assert_eq!(snapshot.stance_mode, "balanced");
        assert!(snapshot.has_query_embedding);
        assert!(snapshot.has_filtering);
        assert_eq!(snapshot.metadata_prefilter_count, 1);
        assert!(snapshot.allowed_claim_ids_active);
        assert_eq!(snapshot.allowed_claim_ids_count, 1);
        assert_eq!(snapshot.ann_candidate_count, 2);
        assert_eq!(snapshot.planner_candidate_count, 1);
        assert!(!snapshot.short_circuit_empty);
    }

    #[test]
    fn planner_debug_snapshot_short_circuits_when_prefilter_is_empty() {
        let mut store = InMemoryStore::new();
        store
            .ingest_bundle(
                Claim {
                    claim_id: "c1".into(),
                    tenant_id: "tenant-a".into(),
                    canonical_text: "Company X acquired Company Y".into(),
                    confidence: 0.9,
                    event_time_unix: None,
                    entities: vec!["Company X".into()],
                    embedding_ids: vec!["emb://x".into()],
                    claim_type: None,
                    valid_from: None,
                    valid_to: None,
                    created_at: None,
                    updated_at: None,
                },
                vec![],
                vec![],
            )
            .expect("ingest should succeed");

        let snapshot = build_retrieve_planner_debug_snapshot(
            &store,
            &RetrieveApiRequest {
                tenant_id: "tenant-a".into(),
                query: "acquired company".into(),
                query_embedding: None,
                entity_filters: vec!["company-z".into()],
                embedding_id_filters: vec!["emb://missing".into()],
                top_k: 5,
                stance_mode: StanceMode::Balanced,
                return_graph: false,
                time_range: None,
            },
        );

        assert!(snapshot.has_filtering);
        assert_eq!(snapshot.metadata_prefilter_count, 0);
        assert_eq!(snapshot.allowed_claim_ids_count, 0);
        assert!(snapshot.short_circuit_empty);
        assert_eq!(snapshot.planner_candidate_count, 0);
    }

    #[test]
    fn execute_api_query_includes_wal_delta_when_segment_manifest_is_stale() {
        let _env_lock = env_lock().lock().expect("env lock should be available");
        let _lock = segment_cache_test_lock()
            .lock()
            .expect("segment cache test lock should be available");
        clear_segment_cache_for_tests();
        let root = temp_dir("wal-delta-visibility");
        let tenant = "tenant-a";
        let tenant_root = root.join(tenant);
        persist_segments_atomic(
            &tenant_root,
            &[Segment {
                segment_id: "hot-0".into(),
                tier: Tier::Hot,
                claim_ids: vec!["claim-segment".into()],
            }],
        )
        .expect("segment persist should succeed");

        let mut store = InMemoryStore::new();
        store
            .ingest_bundle(
                Claim {
                    claim_id: "claim-segment".into(),
                    tenant_id: tenant.into(),
                    canonical_text: "Company X acquired Company Y".into(),
                    confidence: 0.9,
                    event_time_unix: None,
                    entities: vec!["Company X".into()],
                    embedding_ids: vec!["emb://segment".into()],
                    claim_type: None,
                    valid_from: None,
                    valid_to: None,
                    created_at: None,
                    updated_at: None,
                },
                vec![],
                vec![],
            )
            .expect("segment base ingest should succeed");
        store
            .ingest_bundle(
                Claim {
                    claim_id: "claim-wal-delta".into(),
                    tenant_id: tenant.into(),
                    canonical_text: "Company X acquired Startup Nova in 2026".into(),
                    confidence: 0.95,
                    event_time_unix: None,
                    entities: vec!["Company X".into()],
                    embedding_ids: vec!["emb://wal-delta".into()],
                    claim_type: None,
                    valid_from: None,
                    valid_to: None,
                    created_at: None,
                    updated_at: None,
                },
                vec![],
                vec![],
            )
            .expect("wal delta ingest should succeed");

        let _segment_dir_env = EnvVarGuard::set("DASH_RETRIEVAL_SEGMENT_DIR", root.as_os_str());
        let _segment_refresh_env = EnvVarGuard::set(
            "DASH_RETRIEVAL_SEGMENT_CACHE_REFRESH_MS",
            OsStr::new("600000"),
        );

        let response = execute_api_query(
            &store,
            RetrieveApiRequest {
                tenant_id: tenant.into(),
                query: "startup nova acquisition 2026".into(),
                query_embedding: None,
                entity_filters: vec!["company x".into()],
                embedding_id_filters: vec!["emb://wal-delta".into()],
                top_k: 3,
                stance_mode: StanceMode::Balanced,
                return_graph: false,
                time_range: None,
            },
        );

        assert_eq!(response.results.len(), 1);
        assert_eq!(response.results[0].claim_id, "claim-wal-delta");

        let _ = std::fs::remove_dir_all(root);
        clear_segment_cache_for_tests();
    }

    #[test]
    fn execute_api_query_storage_merge_snapshot_tracks_result_sources() {
        let _env_lock = env_lock().lock().expect("env lock should be available");
        let _lock = segment_cache_test_lock()
            .lock()
            .expect("segment cache test lock should be available");
        clear_segment_cache_for_tests();
        let root = temp_dir("storage-merge-snapshot");
        let tenant = "tenant-a";
        let tenant_root = root.join(tenant);
        persist_segments_atomic(
            &tenant_root,
            &[Segment {
                segment_id: "hot-0".into(),
                tier: Tier::Hot,
                claim_ids: vec!["claim-segment".into()],
            }],
        )
        .expect("segment persist should succeed");

        let mut store = InMemoryStore::new();
        store
            .ingest_bundle(
                Claim {
                    claim_id: "claim-segment".into(),
                    tenant_id: tenant.into(),
                    canonical_text: "Company X completed acquisition of Company Y".into(),
                    confidence: 0.9,
                    event_time_unix: None,
                    entities: vec!["Company X".into()],
                    embedding_ids: vec!["emb://segment".into()],
                    claim_type: None,
                    valid_from: None,
                    valid_to: None,
                    created_at: None,
                    updated_at: None,
                },
                vec![],
                vec![],
            )
            .expect("segment base ingest should succeed");
        store
            .ingest_bundle(
                Claim {
                    claim_id: "claim-wal-delta".into(),
                    tenant_id: tenant.into(),
                    canonical_text: "Company X completed acquisition of Startup Nova".into(),
                    confidence: 0.95,
                    event_time_unix: None,
                    entities: vec!["Company X".into()],
                    embedding_ids: vec!["emb://wal-delta".into()],
                    claim_type: None,
                    valid_from: None,
                    valid_to: None,
                    created_at: None,
                    updated_at: None,
                },
                vec![],
                vec![],
            )
            .expect("wal delta ingest should succeed");

        let _segment_dir_env = EnvVarGuard::set("DASH_RETRIEVAL_SEGMENT_DIR", root.as_os_str());
        let _segment_refresh_env = EnvVarGuard::set(
            "DASH_RETRIEVAL_SEGMENT_CACHE_REFRESH_MS",
            OsStr::new("600000"),
        );

        let (_response, snapshot) = execute_api_query_with_storage_snapshot(
            &store,
            RetrieveApiRequest {
                tenant_id: tenant.into(),
                query: "company x acquisition".into(),
                query_embedding: None,
                entity_filters: vec!["company x".into()],
                embedding_id_filters: vec![],
                top_k: 10,
                stance_mode: StanceMode::Balanced,
                return_graph: false,
                time_range: None,
            },
        );

        assert_eq!(
            snapshot.execution_mode,
            STORAGE_EXECUTION_MODE_SEGMENT_DISK_BASE
        );
        assert_eq!(
            snapshot.source_of_truth_model,
            STORAGE_SOURCE_OF_TRUTH_MODEL
        );
        assert!(snapshot.disk_native_segment_execution_active);
        assert_eq!(snapshot.execution_candidate_count, 2);
        assert_eq!(
            snapshot.promotion_boundary_state,
            STORAGE_PROMOTION_BOUNDARY_SEGMENT_PLUS_WAL_DELTA
        );
        assert!(snapshot.promotion_boundary_in_transition);
        assert!(snapshot.segment_base_active);
        assert!(snapshot.wal_delta_active);
        assert!(snapshot.storage_visible_active);
        assert_eq!(snapshot.segment_base_count, 1);
        assert_eq!(snapshot.wal_delta_count, 1);
        assert_eq!(snapshot.storage_visible_count, 2);
        assert_eq!(snapshot.result_count, 2);
        assert_eq!(snapshot.result_from_segment_base_count, 1);
        assert_eq!(snapshot.result_from_wal_delta_count, 1);
        assert_eq!(snapshot.result_source_unknown_count, 0);
        assert_eq!(snapshot.result_outside_storage_visible_count, 0);

        let _ = std::fs::remove_dir_all(root);
        clear_segment_cache_for_tests();
    }

    #[test]
    fn execute_api_query_storage_merge_snapshot_marks_unknown_when_segment_source_is_inactive() {
        let _env_lock = env_lock().lock().expect("env lock should be available");
        let _lock = segment_cache_test_lock()
            .lock()
            .expect("segment cache test lock should be available");
        clear_segment_cache_for_tests();
        let root = temp_dir("storage-merge-unknown");
        let mut store = InMemoryStore::new();
        store
            .ingest_bundle(
                Claim {
                    claim_id: "c1".into(),
                    tenant_id: "tenant-a".into(),
                    canonical_text: "Company X acquired Company Y".into(),
                    confidence: 0.9,
                    event_time_unix: None,
                    entities: vec!["Company X".into()],
                    embedding_ids: vec!["emb://x".into()],
                    claim_type: None,
                    valid_from: None,
                    valid_to: None,
                    created_at: None,
                    updated_at: None,
                },
                vec![],
                vec![],
            )
            .expect("ingest should succeed");
        let _segment_dir_env = EnvVarGuard::set("DASH_RETRIEVAL_SEGMENT_DIR", root.as_os_str());
        let _segment_refresh_env = EnvVarGuard::set(
            "DASH_RETRIEVAL_SEGMENT_CACHE_REFRESH_MS",
            OsStr::new("600000"),
        );

        let (_response, snapshot) = execute_api_query_with_storage_snapshot(
            &store,
            RetrieveApiRequest {
                tenant_id: "tenant-a".into(),
                query: "company x acquired".into(),
                query_embedding: None,
                entity_filters: vec![],
                embedding_id_filters: vec![],
                top_k: 1,
                stance_mode: StanceMode::Balanced,
                return_graph: false,
                time_range: None,
            },
        );

        assert_eq!(snapshot.execution_mode, STORAGE_EXECUTION_MODE_MEMORY_INDEX);
        assert_eq!(
            snapshot.source_of_truth_model,
            STORAGE_SOURCE_OF_TRUTH_MODEL
        );
        assert!(!snapshot.disk_native_segment_execution_active);
        assert_eq!(snapshot.execution_candidate_count, 1);
        assert_eq!(
            snapshot.promotion_boundary_state,
            STORAGE_PROMOTION_BOUNDARY_REPLAY_ONLY
        );
        assert!(!snapshot.promotion_boundary_in_transition);
        assert!(!snapshot.segment_base_active);
        assert!(!snapshot.wal_delta_active);
        assert!(!snapshot.storage_visible_active);
        assert_eq!(snapshot.result_count, 1);
        assert_eq!(snapshot.result_source_unknown_count, 1);
        assert_eq!(snapshot.result_from_segment_base_count, 0);
        assert_eq!(snapshot.result_from_wal_delta_count, 0);
        assert_eq!(snapshot.result_outside_storage_visible_count, 0);

        let _ = std::fs::remove_dir_all(root);
        clear_segment_cache_for_tests();
    }

    #[test]
    fn execute_api_query_storage_merge_snapshot_respects_disk_native_toggle() {
        let _env_lock = env_lock().lock().expect("env lock should be available");
        let _lock = segment_cache_test_lock()
            .lock()
            .expect("segment cache test lock should be available");
        clear_segment_cache_for_tests();
        let root = temp_dir("storage-merge-toggle");
        let tenant = "tenant-a";
        let tenant_root = root.join(tenant);
        persist_segments_atomic(
            &tenant_root,
            &[Segment {
                segment_id: "hot-0".into(),
                tier: Tier::Hot,
                claim_ids: vec!["claim-segment".into()],
            }],
        )
        .expect("segment persist should succeed");

        let mut store = InMemoryStore::new();
        store
            .ingest_bundle(
                Claim {
                    claim_id: "claim-segment".into(),
                    tenant_id: tenant.into(),
                    canonical_text: "Company X completed acquisition of Company Y".into(),
                    confidence: 0.9,
                    event_time_unix: None,
                    entities: vec!["Company X".into()],
                    embedding_ids: vec!["emb://segment".into()],
                    claim_type: None,
                    valid_from: None,
                    valid_to: None,
                    created_at: None,
                    updated_at: None,
                },
                vec![],
                vec![],
            )
            .expect("segment base ingest should succeed");
        let _segment_dir_env = EnvVarGuard::set("DASH_RETRIEVAL_SEGMENT_DIR", root.as_os_str());
        let _segment_refresh_env = EnvVarGuard::set(
            "DASH_RETRIEVAL_SEGMENT_CACHE_REFRESH_MS",
            OsStr::new("600000"),
        );
        let _disk_native_toggle = EnvVarGuard::set(
            "DASH_RETRIEVAL_DISK_NATIVE_SEGMENT_EXECUTION",
            OsStr::new("false"),
        );

        let (_response, snapshot) = execute_api_query_with_storage_snapshot(
            &store,
            RetrieveApiRequest {
                tenant_id: tenant.into(),
                query: "company x acquisition".into(),
                query_embedding: None,
                entity_filters: vec![],
                embedding_id_filters: vec![],
                top_k: 10,
                stance_mode: StanceMode::Balanced,
                return_graph: false,
                time_range: None,
            },
        );
        assert_eq!(snapshot.execution_mode, STORAGE_EXECUTION_MODE_MEMORY_INDEX);
        assert!(!snapshot.disk_native_segment_execution_active);
        assert_eq!(
            snapshot.promotion_boundary_state,
            STORAGE_PROMOTION_BOUNDARY_SEGMENT_FULLY_PROMOTED
        );
        assert!(!snapshot.promotion_boundary_in_transition);

        let _ = std::fs::remove_dir_all(root);
        clear_segment_cache_for_tests();
    }

    #[test]
    fn execute_api_query_storage_merge_snapshot_promotion_boundary_transitions_after_refresh() {
        let tenant = "tenant-a";

        let mut store = InMemoryStore::new();
        for (claim_id, canonical_text) in [
            (
                "claim-segment",
                "Company X completed acquisition of Company Y",
            ),
            (
                "claim-wal-delta",
                "Company X completed acquisition of Startup Nova",
            ),
        ] {
            store
                .ingest_bundle(
                    Claim {
                        claim_id: claim_id.into(),
                        tenant_id: tenant.into(),
                        canonical_text: canonical_text.into(),
                        confidence: 0.9,
                        event_time_unix: None,
                        entities: vec!["Company X".into()],
                        embedding_ids: vec![],
                        claim_type: None,
                        valid_from: None,
                        valid_to: None,
                        created_at: None,
                        updated_at: None,
                    },
                    vec![],
                    vec![],
                )
                .expect("ingest should succeed");
        }

        let segment_base_before: HashSet<String> =
            ["claim-segment".to_string()].into_iter().collect();
        let wal_delta_before =
            build_wal_delta_claim_ids(&store, tenant, Some(&segment_base_before)).unwrap();
        let storage_visible_before = merge_segment_base_with_wal_delta_claim_ids(
            Some(&segment_base_before),
            Some(&wal_delta_before),
        )
        .unwrap();
        let planner_before = PlannerContext {
            tenant_id: tenant.to_string(),
            from_unix: None,
            to_unix: None,
            entity_filters: vec![],
            embedding_filters: vec![],
            metadata_allowed_claim_ids: None,
            segment_base_claim_ids: Some(segment_base_before),
            wal_delta_claim_ids: Some(wal_delta_before),
            storage_visible_claim_ids: Some(storage_visible_before),
            allowed_claim_ids: None,
            has_filtering: true,
            short_circuit_empty: false,
        };
        let snapshot_before = build_storage_merge_snapshot(
            &planner_before,
            &[],
            STORAGE_EXECUTION_MODE_MEMORY_INDEX,
            0,
        );
        assert_eq!(
            snapshot_before.promotion_boundary_state,
            STORAGE_PROMOTION_BOUNDARY_SEGMENT_PLUS_WAL_DELTA
        );
        assert!(snapshot_before.promotion_boundary_in_transition);
        assert_eq!(snapshot_before.wal_delta_count, 1);

        let segment_base_after: HashSet<String> =
            ["claim-segment".to_string(), "claim-wal-delta".to_string()]
                .into_iter()
                .collect();
        let wal_delta_after =
            build_wal_delta_claim_ids(&store, tenant, Some(&segment_base_after)).unwrap();
        let storage_visible_after = merge_segment_base_with_wal_delta_claim_ids(
            Some(&segment_base_after),
            Some(&wal_delta_after),
        )
        .unwrap();
        let planner_after = PlannerContext {
            tenant_id: tenant.to_string(),
            from_unix: None,
            to_unix: None,
            entity_filters: vec![],
            embedding_filters: vec![],
            metadata_allowed_claim_ids: None,
            segment_base_claim_ids: Some(segment_base_after),
            wal_delta_claim_ids: Some(wal_delta_after),
            storage_visible_claim_ids: Some(storage_visible_after),
            allowed_claim_ids: None,
            has_filtering: true,
            short_circuit_empty: false,
        };
        let snapshot_after = build_storage_merge_snapshot(
            &planner_after,
            &[],
            STORAGE_EXECUTION_MODE_MEMORY_INDEX,
            0,
        );
        assert_eq!(
            snapshot_after.promotion_boundary_state,
            STORAGE_PROMOTION_BOUNDARY_SEGMENT_FULLY_PROMOTED
        );
        assert!(!snapshot_after.promotion_boundary_in_transition);
        assert_eq!(snapshot_after.wal_delta_count, 0);
    }

    #[test]
    fn execute_api_query_segment_assisted_matches_replay_only_logical_set() {
        let _env_lock = env_lock().lock().expect("env lock should be available");
        let _lock = segment_cache_test_lock()
            .lock()
            .expect("segment cache test lock should be available");
        clear_segment_cache_for_tests();
        let root = temp_dir("segment-assisted-equivalence");
        let tenant = "tenant-a";
        let tenant_root = root.join(tenant);
        persist_segments_atomic(
            &tenant_root,
            &[Segment {
                segment_id: "hot-0".into(),
                tier: Tier::Hot,
                claim_ids: vec!["claim-segment".into()],
            }],
        )
        .expect("segment persist should succeed");

        let mut store = InMemoryStore::new();
        for (claim_id, canonical_text, embedding_id) in [
            (
                "claim-segment",
                "Company X acquisition of Company Y was approved",
                "emb://segment",
            ),
            (
                "claim-wal-delta",
                "Company X acquisition of Startup Nova closed",
                "emb://wal-delta",
            ),
        ] {
            store
                .ingest_bundle(
                    Claim {
                        claim_id: claim_id.into(),
                        tenant_id: tenant.into(),
                        canonical_text: canonical_text.into(),
                        confidence: 0.9,
                        event_time_unix: None,
                        entities: vec!["Company X".into()],
                        embedding_ids: vec![embedding_id.into()],
                        claim_type: None,
                        valid_from: None,
                        valid_to: None,
                        created_at: None,
                        updated_at: None,
                    },
                    vec![Evidence {
                        evidence_id: format!("e-{claim_id}"),
                        claim_id: claim_id.into(),
                        source_id: format!("source://{claim_id}"),
                        stance: Stance::Supports,
                        source_quality: 0.9,
                        chunk_id: None,
                        span_start: None,
                        span_end: None,
                        doc_id: None,
                        extraction_model: None,
                        ingested_at: None,
                    }],
                    vec![],
                )
                .expect("ingest should succeed");
        }

        let request = RetrieveApiRequest {
            tenant_id: tenant.into(),
            query: "company x acquisition".into(),
            query_embedding: None,
            entity_filters: vec!["company x".into()],
            embedding_id_filters: vec![],
            top_k: 10,
            stance_mode: StanceMode::Balanced,
            return_graph: false,
            time_range: None,
        };

        let segment_assisted_response = {
            let _segment_dir_env = EnvVarGuard::set("DASH_RETRIEVAL_SEGMENT_DIR", root.as_os_str());
            let _segment_refresh_env = EnvVarGuard::set(
                "DASH_RETRIEVAL_SEGMENT_CACHE_REFRESH_MS",
                OsStr::new("600000"),
            );
            execute_api_query(&store, request.clone())
        };
        let replay_only_response = execute_api_query(&store, request);

        let segment_assisted_ids: std::collections::HashSet<String> = segment_assisted_response
            .results
            .iter()
            .map(|node| node.claim_id.clone())
            .collect();
        let replay_only_ids: std::collections::HashSet<String> = replay_only_response
            .results
            .iter()
            .map(|node| node.claim_id.clone())
            .collect();

        assert_eq!(segment_assisted_ids, replay_only_ids);
        assert!(segment_assisted_ids.contains("claim-segment"));
        assert!(segment_assisted_ids.contains("claim-wal-delta"));

        let _ = std::fs::remove_dir_all(root);
        clear_segment_cache_for_tests();
    }

    #[test]
    fn execute_api_query_ignores_foreign_claim_ids_in_segment_allowlist() {
        let _env_lock = env_lock().lock().expect("env lock should be available");
        let _lock = segment_cache_test_lock()
            .lock()
            .expect("segment cache test lock should be available");
        clear_segment_cache_for_tests();
        let root = temp_dir("tenant-allowlist-isolation");
        let tenant_a_root = root.join("tenant-a");
        persist_segments_atomic(
            &tenant_a_root,
            &[Segment {
                segment_id: "hot-0".into(),
                tier: Tier::Hot,
                claim_ids: vec!["claim-tenant-a".into(), "claim-tenant-b".into()],
            }],
        )
        .expect("segment persist should succeed");

        let mut store = InMemoryStore::new();
        store
            .ingest_bundle(
                Claim {
                    claim_id: "claim-tenant-a".into(),
                    tenant_id: "tenant-a".into(),
                    canonical_text: "Tenant A project update".into(),
                    confidence: 0.9,
                    event_time_unix: None,
                    entities: vec!["Project Alpha".into()],
                    embedding_ids: vec!["emb://tenant-a".into()],
                    claim_type: None,
                    valid_from: None,
                    valid_to: None,
                    created_at: None,
                    updated_at: None,
                },
                vec![],
                vec![],
            )
            .expect("tenant-a ingest should succeed");
        store
            .ingest_bundle(
                Claim {
                    claim_id: "claim-tenant-b".into(),
                    tenant_id: "tenant-b".into(),
                    canonical_text: "Tenant B project update".into(),
                    confidence: 0.9,
                    event_time_unix: None,
                    entities: vec!["Project Beta".into()],
                    embedding_ids: vec!["emb://tenant-b".into()],
                    claim_type: None,
                    valid_from: None,
                    valid_to: None,
                    created_at: None,
                    updated_at: None,
                },
                vec![],
                vec![],
            )
            .expect("tenant-b ingest should succeed");

        let _segment_dir_env = EnvVarGuard::set("DASH_RETRIEVAL_SEGMENT_DIR", root.as_os_str());
        let _segment_refresh_env = EnvVarGuard::set(
            "DASH_RETRIEVAL_SEGMENT_CACHE_REFRESH_MS",
            OsStr::new("600000"),
        );

        let response = execute_api_query(
            &store,
            RetrieveApiRequest {
                tenant_id: "tenant-a".into(),
                query: "project update".into(),
                query_embedding: None,
                entity_filters: vec![],
                embedding_id_filters: vec![],
                top_k: 5,
                stance_mode: StanceMode::Balanced,
                return_graph: false,
                time_range: None,
            },
        );

        assert_eq!(response.results.len(), 1);
        assert_eq!(response.results[0].claim_id, "claim-tenant-a");

        let _ = std::fs::remove_dir_all(root);
        clear_segment_cache_for_tests();
    }

    #[test]
    fn build_segment_prefilter_claim_ids_from_root_is_tenant_scoped() {
        let _env_lock = env_lock().lock().expect("env lock should be available");
        let _lock = segment_cache_test_lock()
            .lock()
            .expect("segment cache test lock should be available");
        clear_segment_cache_for_tests();
        let root = temp_dir("prefilter-tenant-scope");
        let tenant_a_root = root.join("tenant-a");
        let tenant_b_root = root.join("tenant-b");
        persist_segments_atomic(
            &tenant_a_root,
            &[Segment {
                segment_id: "hot-0".into(),
                tier: Tier::Hot,
                claim_ids: vec!["claim-a-1".into(), "claim-a-2".into()],
            }],
        )
        .expect("tenant-a segment persist should succeed");
        persist_segments_atomic(
            &tenant_b_root,
            &[Segment {
                segment_id: "hot-0".into(),
                tier: Tier::Hot,
                claim_ids: vec!["claim-b-1".into()],
            }],
        )
        .expect("tenant-b segment persist should succeed");

        let tenant_a_ids = build_segment_prefilter_claim_ids_from_root("tenant-a", root.clone())
            .expect("tenant-a segment ids should be loaded");
        let tenant_b_ids = build_segment_prefilter_claim_ids_from_root("tenant-b", root.clone())
            .expect("tenant-b segment ids should be loaded");

        assert_eq!(tenant_a_ids.len(), 2);
        assert!(tenant_a_ids.contains("claim-a-1"));
        assert!(tenant_a_ids.contains("claim-a-2"));
        assert!(!tenant_a_ids.contains("claim-b-1"));
        assert_eq!(tenant_b_ids.len(), 1);
        assert!(tenant_b_ids.contains("claim-b-1"));
        assert!(!tenant_b_ids.contains("claim-a-1"));

        let _ = std::fs::remove_dir_all(root);
        clear_segment_cache_for_tests();
    }

    #[test]
    fn build_segment_prefilter_claim_ids_from_root_reads_persisted_segments() {
        let _env_lock = env_lock().lock().expect("env lock should be available");
        let _lock = segment_cache_test_lock()
            .lock()
            .expect("segment cache test lock should be available");
        clear_segment_cache_for_tests();
        let root = temp_dir("prefilter");
        let tenant_root = root.join("tenant-a");
        persist_segments_atomic(
            &tenant_root,
            &[Segment {
                segment_id: "hot-0".into(),
                tier: Tier::Hot,
                claim_ids: vec!["claim-1".into(), "claim-2".into()],
            }],
        )
        .expect("segment persist should succeed");

        let ids = build_segment_prefilter_claim_ids_from_root("tenant-a", root.clone())
            .expect("segment ids should be loaded");
        assert_eq!(ids.len(), 2);
        assert!(ids.contains("claim-1"));
        assert!(ids.contains("claim-2"));

        let _ = std::fs::remove_dir_all(root);
        clear_segment_cache_for_tests();
    }

    #[test]
    fn segment_prefilter_cache_refreshes_after_manifest_update() {
        let _env_lock = env_lock().lock().expect("env lock should be available");
        let _lock = segment_cache_test_lock()
            .lock()
            .expect("segment cache test lock should be available");
        clear_segment_cache_for_tests();
        let root = temp_dir("prefilter-refresh");
        let tenant_root = root.join("tenant-a");

        persist_segments_atomic(
            &tenant_root,
            &[Segment {
                segment_id: "hot-0".into(),
                tier: Tier::Hot,
                claim_ids: vec!["claim-old".into()],
            }],
        )
        .expect("initial segment persist should succeed");

        let first = build_segment_prefilter_claim_ids_from_root("tenant-a", root.clone())
            .expect("first segment load should succeed");
        assert!(first.contains("claim-old"));

        persist_segments_atomic(
            &tenant_root,
            &[Segment {
                segment_id: "hot-0".into(),
                tier: Tier::Hot,
                claim_ids: vec!["claim-new".into(), "claim-new-2".into()],
            }],
        )
        .expect("updated segment persist should succeed");

        std::thread::sleep(segment_prefilter_refresh_interval() + Duration::from_millis(10));
        let second = build_segment_prefilter_claim_ids_from_root("tenant-a", root.clone())
            .expect("second segment load should succeed");
        assert!(!second.contains("claim-old"));
        assert!(second.contains("claim-new"));
        assert!(second.contains("claim-new-2"));

        let _ = std::fs::remove_dir_all(root);
        clear_segment_cache_for_tests();
    }

    #[test]
    fn segment_prefilter_cache_metrics_track_refreshes_and_hits() {
        let _env_lock = env_lock().lock().expect("env lock should be available");
        let _lock = segment_cache_test_lock()
            .lock()
            .expect("segment cache test lock should be available");
        clear_segment_cache_for_tests();
        let root = temp_dir("prefilter-metrics");
        let tenant_root = root.join("tenant-a");
        persist_segments_atomic(
            &tenant_root,
            &[Segment {
                segment_id: "hot-0".into(),
                tier: Tier::Hot,
                claim_ids: vec!["claim-1".into()],
            }],
        )
        .expect("segment persist should succeed");

        let first = build_segment_prefilter_claim_ids_from_root("tenant-a", root.clone())
            .expect("first load should work");
        assert!(first.contains("claim-1"));
        let second = build_segment_prefilter_claim_ids_from_root("tenant-a", root.clone())
            .expect("second load should work");
        assert!(second.contains("claim-1"));

        let metrics = segment_prefilter_cache_metrics_snapshot();
        assert!(metrics.refresh_attempts >= 1);
        assert!(metrics.refresh_successes >= 1);
        assert_eq!(metrics.refresh_failures, 0);
        assert_eq!(metrics.fallback_activations, 0);
        assert_eq!(metrics.fallback_missing_manifest, 0);
        assert_eq!(metrics.fallback_manifest_errors, 0);
        assert_eq!(metrics.fallback_segment_errors, 0);
        assert!(metrics.cache_hits >= 1);
        assert!(metrics.refresh_load_micros > 0);

        let _ = std::fs::remove_dir_all(root);
        clear_segment_cache_for_tests();
    }

    #[test]
    fn segment_prefilter_cache_metrics_track_fallback_activations() {
        let _env_lock = env_lock().lock().expect("env lock should be available");
        let _lock = segment_cache_test_lock()
            .lock()
            .expect("segment cache test lock should be available");
        clear_segment_cache_for_tests();
        let root = temp_dir("prefilter-fallback");
        let tenant_root = root.join("tenant-a");
        std::fs::create_dir_all(&tenant_root).expect("tenant root should exist");

        let first = build_segment_prefilter_claim_ids_from_root("tenant-a", root.clone());
        assert!(first.is_none());
        let second = build_segment_prefilter_claim_ids_from_root("tenant-a", root.clone());
        assert!(second.is_none());

        let metrics = segment_prefilter_cache_metrics_snapshot();
        assert_eq!(metrics.refresh_attempts, 1);
        assert_eq!(metrics.refresh_successes, 0);
        assert_eq!(metrics.refresh_failures, 1);
        assert_eq!(metrics.cache_hits, 1);
        assert_eq!(metrics.fallback_activations, 2);
        assert_eq!(metrics.fallback_missing_manifest, 2);
        assert_eq!(metrics.fallback_manifest_errors, 0);
        assert_eq!(metrics.fallback_segment_errors, 0);

        let _ = std::fs::remove_dir_all(root);
        clear_segment_cache_for_tests();
    }

    #[test]
    fn segment_prefilter_cache_falls_back_when_manifest_is_invalid() {
        let _env_lock = env_lock().lock().expect("env lock should be available");
        let _lock = segment_cache_test_lock()
            .lock()
            .expect("segment cache test lock should be available");
        clear_segment_cache_for_tests();
        let root = temp_dir("prefilter-invalid-manifest");
        let tenant_root = root.join("tenant-a");
        std::fs::create_dir_all(&tenant_root).expect("tenant root should exist");
        std::fs::write(
            tenant_root.join("segments.manifest"),
            "DASHSEG-MANIFEST\t0\ninvalid\n",
        )
        .expect("invalid manifest should be written");

        let ids = build_segment_prefilter_claim_ids_from_root("tenant-a", root.clone());
        assert!(ids.is_none());

        let metrics = segment_prefilter_cache_metrics_snapshot();
        assert_eq!(metrics.refresh_attempts, 1);
        assert_eq!(metrics.refresh_successes, 0);
        assert_eq!(metrics.refresh_failures, 1);
        assert_eq!(metrics.fallback_activations, 1);
        assert_eq!(metrics.fallback_missing_manifest, 0);
        assert_eq!(metrics.fallback_manifest_errors, 1);
        assert_eq!(metrics.fallback_segment_errors, 0);

        let _ = std::fs::remove_dir_all(root);
        clear_segment_cache_for_tests();
    }

    #[test]
    fn segment_prefilter_cache_falls_back_when_segment_file_is_missing() {
        let _env_lock = env_lock().lock().expect("env lock should be available");
        let _lock = segment_cache_test_lock()
            .lock()
            .expect("segment cache test lock should be available");
        clear_segment_cache_for_tests();
        let root = temp_dir("prefilter-missing-segment-file");
        let tenant_root = root.join("tenant-a");

        let manifest = persist_segments_atomic(
            &tenant_root,
            &[Segment {
                segment_id: "hot-0".into(),
                tier: Tier::Hot,
                claim_ids: vec!["claim-1".into()],
            }],
        )
        .expect("segment persist should succeed");
        let segment_file = tenant_root.join(&manifest.entries[0].file_name);
        std::fs::remove_file(segment_file).expect("segment file should be removed");

        let ids = build_segment_prefilter_claim_ids_from_root("tenant-a", root.clone());
        assert!(ids.is_none());

        let metrics = segment_prefilter_cache_metrics_snapshot();
        assert_eq!(metrics.refresh_attempts, 1);
        assert_eq!(metrics.refresh_successes, 0);
        assert_eq!(metrics.refresh_failures, 1);
        assert_eq!(metrics.fallback_activations, 1);
        assert_eq!(metrics.fallback_missing_manifest, 0);
        assert_eq!(metrics.fallback_manifest_errors, 0);
        assert_eq!(metrics.fallback_segment_errors, 1);

        let _ = std::fs::remove_dir_all(root);
        clear_segment_cache_for_tests();
    }
}
