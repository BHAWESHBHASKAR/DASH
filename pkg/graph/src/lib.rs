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

/// Tuning for weighted PageRank / centrality over the support subgraph.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CentralityConfig {
    /// Probability of following an edge vs. teleporting (standard PageRank `d`).
    pub damping: f32,
    /// Hard cap on power-iteration rounds.
    pub max_iterations: usize,
    /// L1 convergence threshold between successive rank vectors.
    pub tolerance: f32,
}

impl Default for CentralityConfig {
    fn default() -> Self {
        Self {
            damping: 0.85,
            max_iterations: 100,
            tolerance: 1e-6,
        }
    }
}

/// Authority signals for a claim, derived from the directed support subgraph.
///
/// `pagerank` is the stationary distribution of a random surfer that follows
/// `Supports` edges (weighted by `strength`); higher means more corroborated by
/// other authoritative claims. The degree counts give a cheap, explainable
/// companion signal (how many claims directly support / are supported by this).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct NodeCentrality {
    pub pagerank: f32,
    pub support_in_degree: usize,
    pub support_out_degree: usize,
}

/// Weighted PageRank + degree centrality over the `Supports` subgraph.
///
/// Only `Supports` edges participate: PageRank requires non-negative transition
/// weights, and "which claims are authoritative" is a property of corroboration,
/// not contradiction. Edge `strength` is used as the transition weight. Dangling
/// nodes (no outgoing support edge) redistribute their mass uniformly so the
/// vector stays a proper probability distribution that sums to 1.
pub fn compute_support_centrality(
    edges: &[ClaimEdge],
    config: CentralityConfig,
) -> HashMap<String, NodeCentrality> {
    let mut nodes: HashSet<&str> = HashSet::new();
    let mut out_neighbors: HashMap<&str, Vec<(&str, f32)>> = HashMap::new();
    let mut out_weight: HashMap<&str, f32> = HashMap::new();
    let mut in_degree: HashMap<&str, usize> = HashMap::new();
    let mut out_degree: HashMap<&str, usize> = HashMap::new();

    for edge in edges {
        if !matches!(edge.relation, Relation::Supports) {
            continue;
        }
        let from = edge.from_claim_id.as_str();
        let to = edge.to_claim_id.as_str();
        let weight = edge.strength.max(0.0);
        nodes.insert(from);
        nodes.insert(to);
        out_neighbors.entry(from).or_default().push((to, weight));
        *out_weight.entry(from).or_insert(0.0) += weight;
        *out_degree.entry(from).or_insert(0) += 1;
        *in_degree.entry(to).or_insert(0) += 1;
    }

    if nodes.is_empty() {
        return HashMap::new();
    }

    let n = nodes.len();
    let inv_n = 1.0 / n as f32;
    let damping = config.damping.clamp(0.0, 1.0);
    let teleport = (1.0 - damping) * inv_n;

    let mut rank: HashMap<&str, f32> = nodes.iter().map(|&node| (node, inv_n)).collect();

    for _ in 0..config.max_iterations.max(1) {
        // Mass held by dangling nodes (no outgoing support edge) is spread evenly.
        let dangling_mass: f32 = nodes
            .iter()
            .filter(|node| out_weight.get(*node).copied().unwrap_or(0.0) <= 0.0)
            .map(|node| rank[node])
            .sum();
        let dangling_share = damping * dangling_mass * inv_n;

        let mut next: HashMap<&str, f32> = nodes
            .iter()
            .map(|&node| (node, teleport + dangling_share))
            .collect();

        for (&from, neighbors) in &out_neighbors {
            let total = out_weight.get(from).copied().unwrap_or(0.0);
            if total <= 0.0 {
                continue;
            }
            let share = damping * rank[from] / total;
            for &(to, weight) in neighbors {
                *next.get_mut(to).expect("edge target is a known node") += share * weight;
            }
        }

        let delta: f32 = nodes
            .iter()
            .map(|node| (next[node] - rank[node]).abs())
            .sum();
        rank = next;
        if delta < config.tolerance {
            break;
        }
    }

    nodes
        .iter()
        .map(|&node| {
            (
                node.to_string(),
                NodeCentrality {
                    pagerank: rank[node],
                    support_in_degree: in_degree.get(node).copied().unwrap_or(0),
                    support_out_degree: out_degree.get(node).copied().unwrap_or(0),
                },
            )
        })
        .collect()
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

    fn support_edge(id: &str, from: &str, to: &str, strength: f32) -> ClaimEdge {
        ClaimEdge {
            edge_id: id.into(),
            from_claim_id: from.into(),
            to_claim_id: to.into(),
            relation: Relation::Supports,
            strength,
            reason_codes: vec![],
            created_at: None,
        }
    }

    #[test]
    fn pagerank_ranks_more_supported_claims_higher() {
        // c1, c2, c3 all support c-hub; c-hub supports c-leaf.
        let edges = vec![
            support_edge("e1", "c1", "c-hub", 1.0),
            support_edge("e2", "c2", "c-hub", 1.0),
            support_edge("e3", "c3", "c-hub", 1.0),
            support_edge("e4", "c-hub", "c-leaf", 1.0),
        ];
        let centrality = compute_support_centrality(&edges, CentralityConfig::default());

        let hub = centrality.get("c-hub").expect("hub present");
        let c1 = centrality.get("c1").expect("c1 present");
        assert_eq!(hub.support_in_degree, 3);
        assert!(
            hub.pagerank > c1.pagerank,
            "well-supported hub should outrank a leaf source: {} vs {}",
            hub.pagerank,
            c1.pagerank
        );

        // Stationary distribution must sum to ~1.
        let total: f32 = centrality.values().map(|node| node.pagerank).sum();
        assert!(
            (total - 1.0).abs() < 1e-3,
            "pagerank should sum to 1, got {total}"
        );
    }

    #[test]
    fn centrality_ignores_non_support_edges_and_empty_graphs() {
        assert!(compute_support_centrality(&[], CentralityConfig::default()).is_empty());

        let contradiction = vec![ClaimEdge {
            edge_id: "e1".into(),
            from_claim_id: "c1".into(),
            to_claim_id: "c2".into(),
            relation: Relation::Contradicts,
            strength: 0.9,
            reason_codes: vec![],
            created_at: None,
        }];
        assert!(
            compute_support_centrality(&contradiction, CentralityConfig::default()).is_empty(),
            "only support edges define the authority subgraph"
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
