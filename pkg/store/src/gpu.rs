//! GPU-accelerated vector similarity backend (stub).
//!
//! This module is gated behind the `gpu-backend` cargo feature. It is
//! currently a compile-time no-op: no GPU adapter is returned, so the
//! store falls back to the CPU cosine implementation.

pub(crate) struct GpuBackendEngine;

pub(crate) fn gpu_backend_engine() -> Option<&'static GpuBackendEngine> {
    None
}

pub(crate) fn gpu_score_query_candidate_vectors(
    _query_vector: &[f32],
    _candidate_vectors: &[(String, &[f32])],
) -> Option<Vec<(String, f32)>> {
    None
}
