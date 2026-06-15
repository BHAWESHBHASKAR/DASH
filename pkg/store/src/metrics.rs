//! Store metrics, load stats, and the vector backend runtime enum.
//!
//! These types are the public counters and runtime descriptors that
//! callers (the retrieval service, dashboards) use to observe the
//! store. They live in their own module so the metrics surface
//! can evolve independently of the InMemoryStore implementation.

use serde::{Deserialize, Serialize};

use crate::WalReplayStats;

/// Environment variable that overrides the default vector backend
/// selection. Values: `cpu`, `gpu`, or unset (auto-detect).
pub(crate) const VECTOR_BACKEND_ENV: &str = "DASH_VECTOR_BACKEND";

/// Caller's intent for the vector backend. Resolved at startup
/// time into a concrete [`VectorBackendRuntime`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VectorBackendPreference {
    Auto,
    Cpu,
    Gpu,
}

/// The vector backend the store is actually running with. This
/// is what observability surfaces expose. The two fallback
/// variants tell operators why the GPU path is not active even
/// when requested.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum VectorBackendRuntime {
    #[default]
    Cpu,
    Gpu,
    CpuFallbackFeatureDisabled,
    CpuFallbackUnavailable,
}

impl VectorBackendRuntime {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Cpu => "cpu",
            Self::Gpu => "gpu",
            Self::CpuFallbackFeatureDisabled => "cpu (gpu-feature-disabled)",
            Self::CpuFallbackUnavailable => "cpu (gpu-unavailable)",
        }
    }

    pub fn is_gpu(self) -> bool {
        matches!(self, Self::Gpu)
    }
}

/// Counters returned by `InMemoryStore::load_from_*` to describe
/// how the on-disk + WAL state was materialized into memory.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct StoreLoadStats {
    pub replay: WalReplayStats,
    pub claims_loaded: usize,
    pub evidence_loaded: usize,
    pub edges_loaded: usize,
    pub vectors_loaded: usize,
}

/// Snapshot of the in-memory index sizes, suitable for `/metrics`
/// scrapes and capacity planning.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct StoreIndexStats {
    pub tenant_count: usize,
    pub claim_count: usize,
    pub vector_count: usize,
    pub inverted_terms: usize,
    pub entity_terms: usize,
    pub temporal_buckets: usize,
    pub ann_vector_buckets: usize,
}
