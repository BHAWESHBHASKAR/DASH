//! Placeholder for a GPU vector-similarity backend.
//!
//! There is NO GPU implementation. This module is gated behind the
//! `gpu-backend` cargo feature only so the call sites in `lib.rs` keep
//! compiling; every function below returns `None`, which makes the store
//! use the CPU cosine implementation (`f64` accumulation) in all cases.
//!
//! Consequences operators should know about:
//!
//! - Building with `--features gpu-backend` does not enable any GPU
//!   acceleration.
//! - `DASH_VECTOR_BACKEND=gpu` is reported as
//!   `cpu (gpu-unavailable)` (feature on) or
//!   `cpu (gpu-feature-disabled)` (feature off); the backend never
//!   reports `gpu` because [`gpu_backend_engine`] never yields an engine.
//!
//! A real backend would have to return scores identical to the CPU path
//! (clamped cosine in `[-1, 1]`, no NaN) before it may replace it.

pub(crate) struct GpuBackendEngine;

/// Always `None`: no GPU adapter is ever initialised.
pub(crate) fn gpu_backend_engine() -> Option<&'static GpuBackendEngine> {
    None
}

/// Always `None`, which tells the caller to fall back to the CPU path.
pub(crate) fn gpu_score_query_candidate_vectors(
    _query_vector: &[f32],
    _candidate_vectors: &[(String, &[f32])],
) -> Option<Vec<(String, f32)>> {
    None
}
