use std::collections::{HashMap, HashSet, VecDeque};

use schema::{ClaimEdge, Relation};

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct EdgeSummary {
    pub supports: usize,
    pub contradicts: usize,
    pub total_strength: f32,
}

pub fn summarize_edges(edges: &[ClaimEdge]) -> EdgeSummary {
    let mut summary = EdgeSummary {
        supports: 0,
        contradicts: 0,
        total_strength: 0.0,
    };

    for edge in edges {
        summary.total_strength += edge.strength;
        match edge.relation {
            Relation::Supports => summary.supports += 1,
            Relation::Contradicts => summary.contradicts += 1,
            _ => {}
        }
    }
    summary
}

pub fn traverse_edges_multi_hop(
    start_claim_ids: &[String],
    all_edges: &[ClaimEdge],
    max_hops: usize,
) -> Vec<ClaimEdge> {
    if max_hops == 0 || start_claim_ids.is_empty() || all_edges.is_empty() {
        return Vec::new();
    }

    let mut outgoing: HashMap<&str, Vec<&ClaimEdge>> = HashMap::new();
    for edge in all_edges {
        outgoing
            .entry(edge.from_claim_id.as_str())
            .or_default()
            .push(edge);
    }

    let mut visited_nodes: HashSet<String> = HashSet::new();
    let mut seen_edges: HashSet<String> = HashSet::new();
    let mut queue: VecDeque<(String, usize)> = VecDeque::new();

    for claim_id in start_claim_ids {
        if visited_nodes.insert(claim_id.clone()) {
            queue.push_back((claim_id.clone(), 0));
        }
    }

    let mut out: Vec<ClaimEdge> = Vec::new();
    while let Some((claim_id, hop)) = queue.pop_front() {
        if hop >= max_hops {
            continue;
        }
        for edge in outgoing.get(claim_id.as_str()).into_iter().flatten() {
            if seen_edges.insert(edge.edge_id.clone()) {
                out.push((*edge).clone());
            }
            if visited_nodes.insert(edge.to_claim_id.clone()) {
                queue.push_back((edge.to_claim_id.clone(), hop + 1));
            }
        }
    }

    out
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct NodeReasoningSignals {
    pub graph_score: f32,
    pub support_path_count: usize,
    pub contradiction_chain_depth: usize,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct GraphReasoningConfig {
    pub max_hops: usize,
    pub edge_depth_decay: f32,
    pub support_path_bonus: f32,
    pub contradiction_depth_penalty: f32,
    pub support_edge_weight: f32,
    pub contradiction_edge_weight: f32,
    pub refine_edge_weight: f32,
    pub depends_on_edge_weight: f32,
    pub duplicate_edge_weight: f32,
}

impl Default for GraphReasoningConfig {
    fn default() -> Self {
        Self {
            max_hops: 3,
            edge_depth_decay: 0.75,
            support_path_bonus: 0.16,
            contradiction_depth_penalty: 0.20,
            support_edge_weight: 1.0,
            contradiction_edge_weight: -1.0,
            refine_edge_weight: 0.55,
            depends_on_edge_weight: 0.35,
            duplicate_edge_weight: 0.2,
        }
    }
}

pub fn compute_node_reasoning(
    start_claim_ids: &[String],
    edges: &[ClaimEdge],
    max_hops: usize,
) -> HashMap<String, NodeReasoningSignals> {
    compute_node_reasoning_with_config(
        start_claim_ids,
        edges,
        GraphReasoningConfig {
            max_hops,
            ..GraphReasoningConfig::default()
        },
    )
}

pub fn compute_node_reasoning_with_config(
    start_claim_ids: &[String],
    edges: &[ClaimEdge],
    config: GraphReasoningConfig,
) -> HashMap<String, NodeReasoningSignals> {
    if start_claim_ids.is_empty() {
        return HashMap::new();
    }

    let max_hops = config.max_hops.max(1);
    let depth_decay = config.edge_depth_decay.clamp(0.0, 1.0);
    let support_path_bonus = config.support_path_bonus.max(0.0);
    let contradiction_depth_penalty = config.contradiction_depth_penalty.max(0.0);

    let mut outgoing: HashMap<&str, Vec<&ClaimEdge>> = HashMap::new();
    let mut nodes: HashSet<String> = start_claim_ids.iter().cloned().collect();

    for edge in edges {
        outgoing
            .entry(edge.from_claim_id.as_str())
            .or_default()
            .push(edge);
        nodes.insert(edge.from_claim_id.clone());
        nodes.insert(edge.to_claim_id.clone());
    }

    let incoming_weight = compute_weighted_incoming_signals(
        start_claim_ids,
        &outgoing,
        max_hops,
        depth_decay,
        config,
    );
    let support_path_count = compute_support_path_count(start_claim_ids, &outgoing, max_hops);
    let contradiction_chain_depth =
        compute_contradiction_chain_depth(start_claim_ids, &outgoing, max_hops);

    let mut reasoning = HashMap::with_capacity(nodes.len());
    for node_id in nodes {
        let support_paths = support_path_count.get(&node_id).copied().unwrap_or(0);
        let contradiction_depth = contradiction_chain_depth
            .get(&node_id)
            .copied()
            .unwrap_or(0);
        let incoming = incoming_weight.get(&node_id).copied().unwrap_or(0.0);
        let support_bonus = (support_paths as f32).ln_1p() * support_path_bonus;
        let contradiction_penalty =
            (contradiction_depth as f32).powf(1.25) * contradiction_depth_penalty;
        let raw_score = incoming + support_bonus - contradiction_penalty;
        let graph_score = ((raw_score.tanh() + 1.0) * 0.5).clamp(0.0, 1.0);
        reasoning.insert(
            node_id,
            NodeReasoningSignals {
                graph_score,
                support_path_count: support_paths,
                contradiction_chain_depth: contradiction_depth,
            },
        );
    }

    reasoning
}

fn compute_weighted_incoming_signals(
    start_claim_ids: &[String],
    outgoing: &HashMap<&str, Vec<&ClaimEdge>>,
    max_hops: usize,
    depth_decay: f32,
    config: GraphReasoningConfig,
) -> HashMap<String, f32> {
    let node_depths = compute_min_hop_depths(start_claim_ids, outgoing, max_hops);
    let mut incoming_weight: HashMap<String, f32> = HashMap::new();

    for (from_claim_id, from_depth) in node_depths {
        if from_depth >= max_hops {
            continue;
        }
        let depth_factor = depth_decay.powi(from_depth as i32);
        for edge in outgoing.get(from_claim_id.as_str()).into_iter().flatten() {
            let relation_weight = relation_weight(&edge.relation, config);
            let weighted = relation_weight * edge.strength * depth_factor;
            incoming_weight
                .entry(edge.to_claim_id.clone())
                .and_modify(|value| *value += weighted)
                .or_insert(weighted);
        }
    }

    incoming_weight
}

fn compute_min_hop_depths(
    start_claim_ids: &[String],
    outgoing: &HashMap<&str, Vec<&ClaimEdge>>,
    max_hops: usize,
) -> HashMap<String, usize> {
    let mut min_depth: HashMap<String, usize> = HashMap::new();
    let mut queue: VecDeque<(String, usize)> = VecDeque::new();

    for claim_id in start_claim_ids {
        if min_depth.insert(claim_id.clone(), 0).is_none() {
            queue.push_back((claim_id.clone(), 0));
        }
    }

    while let Some((claim_id, depth)) = queue.pop_front() {
        if depth >= max_hops {
            continue;
        }
        for edge in outgoing.get(claim_id.as_str()).into_iter().flatten() {
            let next_depth = depth + 1;
            let should_update = min_depth
                .get(&edge.to_claim_id)
                .is_none_or(|existing| next_depth < *existing);
            if should_update {
                min_depth.insert(edge.to_claim_id.clone(), next_depth);
                queue.push_back((edge.to_claim_id.clone(), next_depth));
            }
        }
    }

    min_depth
}

fn compute_support_path_count(
    start_claim_ids: &[String],
    outgoing: &HashMap<&str, Vec<&ClaimEdge>>,
    max_hops: usize,
) -> HashMap<String, usize> {
    let mut totals: HashMap<String, usize> = HashMap::new();
    let mut frontier: HashMap<String, usize> = HashMap::new();

    for claim_id in start_claim_ids {
        totals
            .entry(claim_id.clone())
            .and_modify(|count| *count = count.saturating_add(1))
            .or_insert(1);
        frontier
            .entry(claim_id.clone())
            .and_modify(|count| *count = count.saturating_add(1))
            .or_insert(1);
    }

    for _ in 0..max_hops {
        if frontier.is_empty() {
            break;
        }
        let mut next: HashMap<String, usize> = HashMap::new();
        for (claim_id, path_count) in frontier {
            for edge in outgoing.get(claim_id.as_str()).into_iter().flatten() {
                if !matches!(edge.relation, Relation::Supports) {
                    continue;
                }
                next.entry(edge.to_claim_id.clone())
                    .and_modify(|count| *count = count.saturating_add(path_count))
                    .or_insert(path_count);
            }
        }
        for (claim_id, path_count) in &next {
            totals
                .entry(claim_id.clone())
                .and_modify(|count| *count = count.saturating_add(*path_count))
                .or_insert(*path_count);
        }
        frontier = next;
    }

    totals
}

fn compute_contradiction_chain_depth(
    start_claim_ids: &[String],
    outgoing: &HashMap<&str, Vec<&ClaimEdge>>,
    max_hops: usize,
) -> HashMap<String, usize> {
    let mut best_depth: HashMap<String, usize> = HashMap::new();
    let mut frontier: HashMap<String, usize> = HashMap::new();

    for claim_id in start_claim_ids {
        best_depth.entry(claim_id.clone()).or_insert(0);
        frontier.entry(claim_id.clone()).or_insert(0);
    }

    for _ in 0..max_hops {
        if frontier.is_empty() {
            break;
        }
        let mut next: HashMap<String, usize> = HashMap::new();
        for (claim_id, depth) in frontier {
            for edge in outgoing.get(claim_id.as_str()).into_iter().flatten() {
                let next_depth = if matches!(edge.relation, Relation::Contradicts) {
                    depth.saturating_add(1)
                } else {
                    0
                };
                next.entry(edge.to_claim_id.clone())
                    .and_modify(|existing| {
                        if next_depth > *existing {
                            *existing = next_depth;
                        }
                    })
                    .or_insert(next_depth);
            }
        }
        for (claim_id, depth) in &next {
            best_depth
                .entry(claim_id.clone())
                .and_modify(|existing| {
                    if *depth > *existing {
                        *existing = *depth;
                    }
                })
                .or_insert(*depth);
        }
        frontier = next;
    }

    best_depth
}

fn relation_weight(relation: &Relation, config: GraphReasoningConfig) -> f32 {
    match relation {
        Relation::Supports => config.support_edge_weight,
        Relation::Contradicts => config.contradiction_edge_weight,
        Relation::Refines => config.refine_edge_weight,
        Relation::DependsOn => config.depends_on_edge_weight,
        Relation::Duplicates => config.duplicate_edge_weight,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use schema::{ClaimEdge, Relation};

    #[test]
    fn summarizes_support_and_contradiction_counts() {
        let edges = vec![
            ClaimEdge {
                edge_id: "e1".into(),
                from_claim_id: "c1".into(),
                to_claim_id: "c2".into(),
                relation: Relation::Supports,
                strength: 0.7,
                reason_codes: vec![],
                created_at: None,
            },
            ClaimEdge {
                edge_id: "e2".into(),
                from_claim_id: "c1".into(),
                to_claim_id: "c3".into(),
                relation: Relation::Contradicts,
                strength: 0.6,
                reason_codes: vec![],
                created_at: None,
            },
        ];
        let summary = summarize_edges(&edges);
        assert_eq!(summary.supports, 1);
        assert_eq!(summary.contradicts, 1);
        assert!((summary.total_strength - 1.3).abs() < 0.0001);
    }

    #[test]
    fn multi_hop_traversal_returns_edges_within_hop_budget() {
        let edges = vec![
            ClaimEdge {
                edge_id: "e1".into(),
                from_claim_id: "c1".into(),
                to_claim_id: "c2".into(),
                relation: Relation::Supports,
                strength: 0.8,
                reason_codes: vec![],
                created_at: None,
            },
            ClaimEdge {
                edge_id: "e2".into(),
                from_claim_id: "c2".into(),
                to_claim_id: "c3".into(),
                relation: Relation::Refines,
                strength: 0.7,
                reason_codes: vec![],
                created_at: None,
            },
            ClaimEdge {
                edge_id: "e3".into(),
                from_claim_id: "c3".into(),
                to_claim_id: "c4".into(),
                relation: Relation::DependsOn,
                strength: 0.6,
                reason_codes: vec![],
                created_at: None,
            },
        ];

        let hop1 = traverse_edges_multi_hop(&["c1".to_string()], &edges, 1);
        assert_eq!(hop1.len(), 1);
        assert_eq!(hop1[0].edge_id, "e1");

        let hop2 = traverse_edges_multi_hop(&["c1".to_string()], &edges, 2);
        assert_eq!(hop2.len(), 2);
        assert!(hop2.iter().any(|edge| edge.edge_id == "e2"));
    }

    #[test]
    fn compute_node_reasoning_tracks_support_paths_and_contradiction_depth() {
        let edges = vec![
            ClaimEdge {
                edge_id: "e1".into(),
                from_claim_id: "c0".into(),
                to_claim_id: "c1".into(),
                relation: Relation::Supports,
                strength: 0.9,
                reason_codes: vec![],
                created_at: None,
            },
            ClaimEdge {
                edge_id: "e2".into(),
                from_claim_id: "c1".into(),
                to_claim_id: "c2".into(),
                relation: Relation::Supports,
                strength: 0.8,
                reason_codes: vec![],
                created_at: None,
            },
            ClaimEdge {
                edge_id: "e3".into(),
                from_claim_id: "c0".into(),
                to_claim_id: "c3".into(),
                relation: Relation::Contradicts,
                strength: 0.7,
                reason_codes: vec![],
                created_at: None,
            },
            ClaimEdge {
                edge_id: "e4".into(),
                from_claim_id: "c3".into(),
                to_claim_id: "c4".into(),
                relation: Relation::Contradicts,
                strength: 0.6,
                reason_codes: vec![],
                created_at: None,
            },
        ];

        let reasoning = compute_node_reasoning(&["c0".to_string()], &edges, 2);
        assert_eq!(
            reasoning.get("c0").map(|node| node.support_path_count),
            Some(1)
        );
        assert_eq!(
            reasoning.get("c2").map(|node| node.support_path_count),
            Some(1)
        );
        assert_eq!(
            reasoning
                .get("c4")
                .map(|node| node.contradiction_chain_depth),
            Some(2)
        );
        let support_score = reasoning
            .get("c2")
            .expect("c2 should have reasoning signals")
            .graph_score;
        let contradiction_score = reasoning
            .get("c4")
            .expect("c4 should have reasoning signals")
            .graph_score;
        assert!(support_score > contradiction_score);
    }

    #[test]
    fn compute_node_reasoning_keeps_seed_metrics_without_edges() {
        let reasoning = compute_node_reasoning(&["c-root".to_string()], &[], 2);
        let root = reasoning
            .get("c-root")
            .expect("seed node should be present");
        assert_eq!(root.support_path_count, 1);
        assert_eq!(root.contradiction_chain_depth, 0);
    }

    #[test]
    fn configurable_reasoning_respects_max_hops() {
        let edges = vec![
            ClaimEdge {
                edge_id: "e1".into(),
                from_claim_id: "c0".into(),
                to_claim_id: "c1".into(),
                relation: Relation::Supports,
                strength: 0.9,
                reason_codes: vec![],
                created_at: None,
            },
            ClaimEdge {
                edge_id: "e2".into(),
                from_claim_id: "c1".into(),
                to_claim_id: "c2".into(),
                relation: Relation::Contradicts,
                strength: 0.9,
                reason_codes: vec![],
                created_at: None,
            },
        ];

        let reasoning = compute_node_reasoning_with_config(
            &["c0".to_string()],
            &edges,
            GraphReasoningConfig {
                max_hops: 1,
                ..GraphReasoningConfig::default()
            },
        );
        assert_eq!(
            reasoning.get("c2").map(|node| node.support_path_count),
            Some(0)
        );
        assert_eq!(
            reasoning
                .get("c2")
                .map(|node| node.contradiction_chain_depth),
            Some(0)
        );
    }

    #[test]
    fn configurable_reasoning_depth_decay_reduces_deeper_influence() {
        let edges = vec![
            ClaimEdge {
                edge_id: "e1".into(),
                from_claim_id: "c0".into(),
                to_claim_id: "c1".into(),
                relation: Relation::Supports,
                strength: 1.0,
                reason_codes: vec![],
                created_at: None,
            },
            ClaimEdge {
                edge_id: "e2".into(),
                from_claim_id: "c1".into(),
                to_claim_id: "c2".into(),
                relation: Relation::Supports,
                strength: 1.0,
                reason_codes: vec![],
                created_at: None,
            },
        ];

        let no_decay = compute_node_reasoning_with_config(
            &["c0".to_string()],
            &edges,
            GraphReasoningConfig {
                max_hops: 2,
                edge_depth_decay: 1.0,
                ..GraphReasoningConfig::default()
            },
        );
        let aggressive_decay = compute_node_reasoning_with_config(
            &["c0".to_string()],
            &edges,
            GraphReasoningConfig {
                max_hops: 2,
                edge_depth_decay: 0.1,
                ..GraphReasoningConfig::default()
            },
        );

        let no_decay_score = no_decay
            .get("c2")
            .expect("c2 should be present in no-decay result")
            .graph_score;
        let aggressive_decay_score = aggressive_decay
            .get("c2")
            .expect("c2 should be present in decay result")
            .graph_score;
        assert!(aggressive_decay_score < no_decay_score);
    }
}
