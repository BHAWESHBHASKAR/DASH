//! Approximate Nearest Neighbor (ANN) types and tuning config.
//!
//! This module contains the in-memory graph that powers the
//! vector-search path. Each tenant gets a [`TenantAnnGraph`]
//! keyed by `tenant_id` on `InMemoryStore`. The graph is a
//! multi-level HNSW-style structure: level 0 is the dense
//! neighbor list, and higher levels are sparse entry points
//! that allow greedy search to converge in O(log n).
//!
//! The [`ScoredNode`] ordering is total (score, then claim_id)
//! so the type can sit in a `BinaryHeap<Reverse<ScoredNode>>`
//! without a custom comparator wrapper.

use std::cmp::Ordering;
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Graph geometry constants
// ---------------------------------------------------------------------------

/// Number of levels in the HNSW-style graph. 4 is the standard
/// setting for the 1k-1M vector range; the upper levels become
/// entry-point only and the search converges in O(log n).
pub(crate) const ANN_GRAPH_LEVELS: usize = 4;

/// Default maximum neighbors on the base layer (level 0). This
/// is the recall/speed dial: more neighbors = better recall,
/// slower search. 12 is a conservative default from the
/// original hnswlib benchmark paper.
pub(crate) const ANN_GRAPH_MAX_NEIGHBORS_BASE_DEFAULT: usize = 12;

/// Default maximum neighbors on upper layers (level > 0).
pub(crate) const ANN_GRAPH_MAX_NEIGHBORS_UPPER_DEFAULT: usize = 6;

/// Default expansion factor: the beam is `top_k *
/// search_expansion_factor` when bounded by the min/max below.
pub(crate) const ANN_SEARCH_EXPANSION_FACTOR_DEFAULT: usize = 16;

/// Minimum beam size regardless of top_k (prevents tiny beams
/// when top_k = 1 or 2).
pub(crate) const ANN_SEARCH_EXPANSION_MIN_DEFAULT: usize = 64;

/// Maximum beam size (prevents runaway expansion on very large
/// top_k values).
pub(crate) const ANN_SEARCH_EXPANSION_MAX_DEFAULT: usize = 4096;

/// Default over-fetch multiplier applied to the level-0 exploration
/// budget when a query carries a metadata predicate (time-range or
/// allow-list). Filtered-HNSW traverses the same graph but only *emits*
/// matching nodes, so a selective predicate needs a deeper walk to
/// avoid starving the candidate pool. 1 disables over-fetch.
pub(crate) const ANN_FILTERED_OVERFETCH_FACTOR_DEFAULT: usize = 4;

// ---------------------------------------------------------------------------
// Distance metric
// ---------------------------------------------------------------------------

/// The similarity/distance function used for vector candidate
/// generation and dense scoring. `Cosine` is the calibrated default
/// (the retrieval scoring formula maps cosine `[-1, 1]` into `[0, 1]`);
/// `DotProduct` and `Euclidean` are available for callers whose
/// embeddings are tuned for those geometries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DistanceMetric {
    #[default]
    Cosine,
    DotProduct,
    Euclidean,
}

impl DistanceMetric {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Cosine => "cosine",
            Self::DotProduct => "dot",
            Self::Euclidean => "euclidean",
        }
    }

    /// Parse a metric from an env/config string. Accepts common
    /// aliases (`cos`, `ip`/`inner_product`, `l2`). Returns `None`
    /// for unrecognized values so callers can fall back to the
    /// default and log.
    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "cosine" | "cos" => Some(Self::Cosine),
            "dot" | "dotproduct" | "dot_product" | "ip" | "inner_product" => Some(Self::DotProduct),
            "euclidean" | "l2" | "l2sq" => Some(Self::Euclidean),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Hybrid fusion strategy
// ---------------------------------------------------------------------------

/// How the dense (vector) and lexical (BM25) signals are combined when a
/// query vector is supplied.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum HybridFusion {
    /// Dense similarity is the primary signal and the lexical/BM25 score is
    /// a small additive tie-breaker. Backwards-compatible default and the
    /// right choice when the embedding is the trusted relevance signal.
    #[default]
    SemanticPrimary,
    /// Reciprocal Rank Fusion: each candidate's dense and lexical *ranks*
    /// contribute `1 / (k + rank)`, summed across modalities. Scale-free
    /// (only the orderings matter), so it is robust when the dense and
    /// lexical scores live on different scales — the modern hybrid-search
    /// default (Qdrant/Weaviate/Elastic).
    Rrf,
}

impl HybridFusion {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SemanticPrimary => "semantic_primary",
            Self::Rrf => "rrf",
        }
    }

    /// Parse a fusion strategy from an env/config string. Returns `None`
    /// for unrecognized values so callers can fall back to the default.
    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "semantic_primary" | "semantic" | "dense_primary" | "dense" | "default" => {
                Some(Self::SemanticPrimary)
            }
            "rrf" | "reciprocal_rank_fusion" | "reciprocal-rank-fusion" => Some(Self::Rrf),
            _ => None,
        }
    }
}

/// Default `k` constant for Reciprocal Rank Fusion. 60 is the value from
/// the original Cormack et al. RRF paper and the de-facto industry default.
pub const RRF_K_DEFAULT: f32 = 60.0;

// ---------------------------------------------------------------------------
// Tunable configuration
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub struct AnnTuningConfig {
    pub max_neighbors_base: usize,
    pub max_neighbors_upper: usize,
    pub search_expansion_factor: usize,
    pub search_expansion_min: usize,
    pub search_expansion_max: usize,
    pub metric: DistanceMetric,
    pub hybrid_fusion: HybridFusion,
    pub rrf_k: f32,
    /// Over-fetch multiplier for filtered (predicate-aware) ANN search.
    pub filtered_overfetch_factor: usize,
}

impl Default for AnnTuningConfig {
    fn default() -> Self {
        Self {
            max_neighbors_base: ANN_GRAPH_MAX_NEIGHBORS_BASE_DEFAULT,
            max_neighbors_upper: ANN_GRAPH_MAX_NEIGHBORS_UPPER_DEFAULT,
            search_expansion_factor: ANN_SEARCH_EXPANSION_FACTOR_DEFAULT,
            search_expansion_min: ANN_SEARCH_EXPANSION_MIN_DEFAULT,
            search_expansion_max: ANN_SEARCH_EXPANSION_MAX_DEFAULT,
            metric: DistanceMetric::Cosine,
            hybrid_fusion: HybridFusion::SemanticPrimary,
            rrf_k: RRF_K_DEFAULT,
            filtered_overfetch_factor: ANN_FILTERED_OVERFETCH_FACTOR_DEFAULT,
        }
    }
}

// ---------------------------------------------------------------------------
// Tenant-scoped graph
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub(crate) struct TenantAnnGraph {
    pub(crate) entry_point: Option<String>,
    pub(crate) entry_level: usize,
    pub(crate) levels: Vec<HashMap<String, Vec<String>>>,
    pub(crate) node_levels: HashMap<String, usize>,
}

impl Default for TenantAnnGraph {
    fn default() -> Self {
        Self {
            entry_point: None,
            entry_level: 0,
            levels: (0..ANN_GRAPH_LEVELS).map(|_| HashMap::new()).collect(),
            node_levels: HashMap::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// Heap ordering for the ANN search frontier
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub(crate) struct ScoredNode {
    pub(crate) claim_id: String,
    pub(crate) score: f32,
}

impl PartialEq for ScoredNode {
    fn eq(&self, other: &Self) -> bool {
        self.claim_id == other.claim_id && self.score.to_bits() == other.score.to_bits()
    }
}

impl Eq for ScoredNode {}

impl PartialOrd for ScoredNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScoredNode {
    fn cmp(&self, other: &Self) -> Ordering {
        self.score
            .total_cmp(&other.score)
            .then_with(|| self.claim_id.cmp(&other.claim_id))
    }
}
