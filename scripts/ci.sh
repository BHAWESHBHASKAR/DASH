#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

echo "[ci] cargo fmt"
cargo fmt --all --check

echo "[ci] cargo clippy"
cargo clippy --workspace --all-targets -- -D warnings

echo "[ci] cargo test"
cargo test --workspace

BENCH_GUARD_MAX_REGRESSION_PCT="${DASH_BENCH_GUARD_MAX_REGRESSION_PCT:-${EME_BENCH_GUARD_MAX_REGRESSION_PCT:-50}}"
INCLUDE_LARGE_GUARD="${DASH_CI_INCLUDE_LARGE_GUARD:-${EME_CI_INCLUDE_LARGE_GUARD:-false}}"
INCLUDE_HYBRID_GUARD="${DASH_CI_INCLUDE_HYBRID_GUARD:-${EME_CI_INCLUDE_HYBRID_GUARD:-false}}"
RUN_BENCH_TREND="${DASH_CI_RUN_BENCH_TREND:-${EME_CI_RUN_BENCH_TREND:-false}}"
BENCH_TREND_INCLUDE_LARGE="${DASH_CI_BENCH_TREND_INCLUDE_LARGE:-${EME_CI_BENCH_TREND_INCLUDE_LARGE:-true}}"
BENCH_TREND_INCLUDE_HYBRID="${DASH_CI_BENCH_TREND_INCLUDE_HYBRID:-${EME_CI_BENCH_TREND_INCLUDE_HYBRID:-false}}"
BENCH_TREND_TAG="${DASH_CI_BENCH_TREND_TAG:-ci-trend}"
CHECK_ASYNC_TRANSPORT="${DASH_CI_CHECK_ASYNC_TRANSPORT:-${EME_CI_CHECK_ASYNC_TRANSPORT:-false}}"
LARGE_GUARD_ITERATIONS="${DASH_CI_LARGE_GUARD_ITERATIONS:-}"
CI_LARGE_ANN_MAX_NEIGHBORS_BASE="${DASH_CI_LARGE_ANN_MAX_NEIGHBORS_BASE:-${DASH_BENCH_ANN_MAX_NEIGHBORS_BASE:-12}}"
CI_LARGE_ANN_MAX_NEIGHBORS_UPPER="${DASH_CI_LARGE_ANN_MAX_NEIGHBORS_UPPER:-${DASH_BENCH_ANN_MAX_NEIGHBORS_UPPER:-6}}"
CI_LARGE_ANN_SEARCH_EXPANSION_FACTOR="${DASH_CI_LARGE_ANN_SEARCH_EXPANSION_FACTOR:-${DASH_BENCH_ANN_SEARCH_EXPANSION_FACTOR:-12}}"
CI_LARGE_ANN_SEARCH_EXPANSION_MIN="${DASH_CI_LARGE_ANN_SEARCH_EXPANSION_MIN:-${DASH_BENCH_ANN_SEARCH_EXPANSION_MIN:-64}}"
CI_LARGE_ANN_SEARCH_EXPANSION_MAX="${DASH_CI_LARGE_ANN_SEARCH_EXPANSION_MAX:-${DASH_BENCH_ANN_SEARCH_EXPANSION_MAX:-4096}}"
CI_LARGE_MIN_CANDIDATE_REDUCTION_PCT="${DASH_CI_LARGE_MIN_CANDIDATE_REDUCTION_PCT:-${DASH_BENCH_LARGE_MIN_CANDIDATE_REDUCTION_PCT:-95}}"
CI_LARGE_MAX_DASH_LATENCY_MS="${DASH_CI_LARGE_MAX_DASH_LATENCY_MS:-${DASH_BENCH_LARGE_MAX_DASH_LATENCY_MS:-120}}"

if [[ "${CHECK_ASYNC_TRANSPORT}" == "true" ]]; then
  echo "[ci] async transport feature checks"
  cargo check -p ingestion --features async-transport
  cargo check -p retrieval --features async-transport
  echo "[ci] async transport feature tests"
  cargo test -p ingestion --features async-transport
  cargo test -p retrieval --features async-transport
fi

echo "[ci] benchmark history guard"
cargo run -p benchmark-smoke --bin benchmark-smoke -- \
  --profile smoke \
  --guard-history docs/benchmarks/history/benchmark-history.md \
  --max-dash-latency-regression-pct "${BENCH_GUARD_MAX_REGRESSION_PCT}"

if [[ "${INCLUDE_LARGE_GUARD}" == "true" ]]; then
  echo "[ci] benchmark history guard (large)"
  LARGE_CMD=(
    cargo run -p benchmark-smoke --bin benchmark-smoke --
    --profile large
    --guard-history docs/benchmarks/history/benchmark-history.md
    --max-dash-latency-regression-pct "${BENCH_GUARD_MAX_REGRESSION_PCT}"
    --large-min-candidate-reduction-pct "${CI_LARGE_MIN_CANDIDATE_REDUCTION_PCT}"
    --large-max-dash-latency-ms "${CI_LARGE_MAX_DASH_LATENCY_MS}"
  )
  if [[ -n "${LARGE_GUARD_ITERATIONS}" ]]; then
    LARGE_CMD+=(--iterations "${LARGE_GUARD_ITERATIONS}")
  fi
  DASH_BENCH_ANN_MAX_NEIGHBORS_BASE="${CI_LARGE_ANN_MAX_NEIGHBORS_BASE}" \
    DASH_BENCH_ANN_MAX_NEIGHBORS_UPPER="${CI_LARGE_ANN_MAX_NEIGHBORS_UPPER}" \
    DASH_BENCH_ANN_SEARCH_EXPANSION_FACTOR="${CI_LARGE_ANN_SEARCH_EXPANSION_FACTOR}" \
    DASH_BENCH_ANN_SEARCH_EXPANSION_MIN="${CI_LARGE_ANN_SEARCH_EXPANSION_MIN}" \
    DASH_BENCH_ANN_SEARCH_EXPANSION_MAX="${CI_LARGE_ANN_SEARCH_EXPANSION_MAX}" \
    "${LARGE_CMD[@]}"
fi

if [[ "${INCLUDE_HYBRID_GUARD}" == "true" ]]; then
  echo "[ci] benchmark history guard (hybrid)"
  cargo run -p benchmark-smoke --bin benchmark-smoke -- \
    --profile hybrid \
    --guard-history docs/benchmarks/history/benchmark-history.md \
    --max-dash-latency-regression-pct "${BENCH_GUARD_MAX_REGRESSION_PCT}"
fi

if [[ "${RUN_BENCH_TREND}" == "true" ]]; then
  echo "[ci] benchmark trend automation"
  DASH_BENCH_INCLUDE_LARGE="${BENCH_TREND_INCLUDE_LARGE}" \
    DASH_BENCH_INCLUDE_HYBRID="${BENCH_TREND_INCLUDE_HYBRID}" \
    scripts/benchmark_trend.sh --run-tag "${BENCH_TREND_TAG}"
fi

echo "[ci] benchmark smoke"
cargo run -p benchmark-smoke --bin benchmark-smoke -- --smoke

echo "[ci] all checks passed"
