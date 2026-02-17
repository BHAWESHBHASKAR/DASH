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
}
