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
INCLUDE_XLARGE_GUARD="${DASH_CI_INCLUDE_XLARGE_GUARD:-${EME_CI_INCLUDE_XLARGE_GUARD:-false}}"
INCLUDE_HYBRID_GUARD="${DASH_CI_INCLUDE_HYBRID_GUARD:-${EME_CI_INCLUDE_HYBRID_GUARD:-false}}"
RUN_BENCH_TREND="${DASH_CI_RUN_BENCH_TREND:-${EME_CI_RUN_BENCH_TREND:-false}}"
BENCH_TREND_INCLUDE_LARGE="${DASH_CI_BENCH_TREND_INCLUDE_LARGE:-${EME_CI_BENCH_TREND_INCLUDE_LARGE:-true}}"
BENCH_TREND_INCLUDE_XLARGE="${DASH_CI_BENCH_TREND_INCLUDE_XLARGE:-${EME_CI_BENCH_TREND_INCLUDE_XLARGE:-false}}"
BENCH_TREND_INCLUDE_HYBRID="${DASH_CI_BENCH_TREND_INCLUDE_HYBRID:-${EME_CI_BENCH_TREND_INCLUDE_HYBRID:-false}}"
BENCH_TREND_TAG="${DASH_CI_BENCH_TREND_TAG:-ci-trend}"
CHECK_ASYNC_TRANSPORT="${DASH_CI_CHECK_ASYNC_TRANSPORT:-${EME_CI_CHECK_ASYNC_TRANSPORT:-false}}"
INCLUDE_SLO_GUARD="${DASH_CI_INCLUDE_SLO_GUARD:-${EME_CI_INCLUDE_SLO_GUARD:-false}}"
CI_SLO_INCLUDE_RECOVERY_DRILL="${DASH_CI_SLO_INCLUDE_RECOVERY_DRILL:-false}"
LARGE_GUARD_ITERATIONS="${DASH_CI_LARGE_GUARD_ITERATIONS:-}"
XLARGE_GUARD_ITERATIONS="${DASH_CI_XLARGE_GUARD_ITERATIONS:-}"
CI_LARGE_ANN_MAX_NEIGHBORS_BASE="${DASH_CI_LARGE_ANN_MAX_NEIGHBORS_BASE:-${DASH_BENCH_ANN_MAX_NEIGHBORS_BASE:-12}}"
CI_LARGE_ANN_MAX_NEIGHBORS_UPPER="${DASH_CI_LARGE_ANN_MAX_NEIGHBORS_UPPER:-${DASH_BENCH_ANN_MAX_NEIGHBORS_UPPER:-6}}"
CI_LARGE_ANN_SEARCH_EXPANSION_FACTOR="${DASH_CI_LARGE_ANN_SEARCH_EXPANSION_FACTOR:-${DASH_BENCH_ANN_SEARCH_EXPANSION_FACTOR:-12}}"
CI_LARGE_ANN_SEARCH_EXPANSION_MIN="${DASH_CI_LARGE_ANN_SEARCH_EXPANSION_MIN:-${DASH_BENCH_ANN_SEARCH_EXPANSION_MIN:-64}}"
CI_LARGE_ANN_SEARCH_EXPANSION_MAX="${DASH_CI_LARGE_ANN_SEARCH_EXPANSION_MAX:-${DASH_BENCH_ANN_SEARCH_EXPANSION_MAX:-4096}}"
CI_LARGE_MIN_CANDIDATE_REDUCTION_PCT="${DASH_CI_LARGE_MIN_CANDIDATE_REDUCTION_PCT:-${DASH_BENCH_LARGE_MIN_CANDIDATE_REDUCTION_PCT:-95}}"
CI_LARGE_MAX_DASH_LATENCY_MS="${DASH_CI_LARGE_MAX_DASH_LATENCY_MS:-${DASH_BENCH_LARGE_MAX_DASH_LATENCY_MS:-120}}"
CI_XLARGE_ANN_MAX_NEIGHBORS_BASE="${DASH_CI_XLARGE_ANN_MAX_NEIGHBORS_BASE:-${DASH_BENCH_ANN_MAX_NEIGHBORS_BASE:-12}}"
CI_XLARGE_ANN_MAX_NEIGHBORS_UPPER="${DASH_CI_XLARGE_ANN_MAX_NEIGHBORS_UPPER:-${DASH_BENCH_ANN_MAX_NEIGHBORS_UPPER:-6}}"
CI_XLARGE_ANN_SEARCH_EXPANSION_FACTOR="${DASH_CI_XLARGE_ANN_SEARCH_EXPANSION_FACTOR:-${DASH_BENCH_ANN_SEARCH_EXPANSION_FACTOR:-12}}"
CI_XLARGE_ANN_SEARCH_EXPANSION_MIN="${DASH_CI_XLARGE_ANN_SEARCH_EXPANSION_MIN:-${DASH_BENCH_ANN_SEARCH_EXPANSION_MIN:-64}}"
CI_XLARGE_ANN_SEARCH_EXPANSION_MAX="${DASH_CI_XLARGE_ANN_SEARCH_EXPANSION_MAX:-${DASH_BENCH_ANN_SEARCH_EXPANSION_MAX:-4096}}"
CI_XLARGE_MIN_CANDIDATE_REDUCTION_PCT="${DASH_CI_XLARGE_MIN_CANDIDATE_REDUCTION_PCT:-${DASH_BENCH_XLARGE_MIN_CANDIDATE_REDUCTION_PCT:-96}}"
CI_XLARGE_MAX_DASH_LATENCY_MS="${DASH_CI_XLARGE_MAX_DASH_LATENCY_MS:-${DASH_BENCH_XLARGE_MAX_DASH_LATENCY_MS:-250}}"

if [[ "${CHECK_ASYNC_TRANSPORT}" == "true" ]]; then
  echo "[ci] async transport feature checks"
  cargo check -p ingestion --features async-transport
  cargo check -p retrieval --features async-transport
  echo "[ci] async transport feature tests"
  cargo test -p ingestion --features async-transport
  cargo test -p retrieval --features async-transport
fi

echo "[ci] backup/restore script checks"
scripts/backup_state_bundle.sh --help >/dev/null
scripts/restore_state_bundle.sh --help >/dev/null
scripts/recovery_drill.sh --help >/dev/null
scripts/slo_guard.sh --help >/dev/null

echo "[ci] backup/restore script functional smoke"
(
  set -euo pipefail
  TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/dash-ci-backup-XXXXXX")"
  trap 'rm -rf "${TMP_DIR}"' EXIT
  SRC_DIR="${TMP_DIR}/source"
  DST_DIR="${TMP_DIR}/restore"
  OUT_DIR="${TMP_DIR}/out"
  mkdir -p "${SRC_DIR}/segments/tenant-a" "${DST_DIR}" "${OUT_DIR}"
  printf "CLAIM\t1\n" > "${SRC_DIR}/claims.wal"
  printf "SNAP\t1\nCLAIM\t1\n" > "${SRC_DIR}/claims.wal.snapshot"
  printf "tenant-a,0,1,node-a,leader,healthy\n" > "${SRC_DIR}/placement.csv"
  printf "segment-marker\n" > "${SRC_DIR}/segments/tenant-a/.marker"

  scripts/backup_state_bundle.sh \
    --wal-path "${SRC_DIR}/claims.wal" \
    --segment-dir "${SRC_DIR}/segments" \
    --placement-file "${SRC_DIR}/placement.csv" \
    --output-dir "${OUT_DIR}" \
    --bundle-label ci-smoke >/dev/null
  BUNDLE_PATH="$(find "${OUT_DIR}" -type f -name 'dash-backup-ci-smoke*.tar.gz' | head -n 1)"
  [[ -n "${BUNDLE_PATH}" ]]

  scripts/restore_state_bundle.sh \
    --bundle "${BUNDLE_PATH}" \
    --wal-path "${DST_DIR}/claims.wal" \
    --segment-dir "${DST_DIR}/segments" \
    --placement-file "${DST_DIR}/placement.csv" \
    --force true >/dev/null

  cmp "${SRC_DIR}/claims.wal" "${DST_DIR}/claims.wal"
  cmp "${SRC_DIR}/claims.wal.snapshot" "${DST_DIR}/claims.wal.snapshot"
  cmp "${SRC_DIR}/placement.csv" "${DST_DIR}/placement.csv"
  cmp "${SRC_DIR}/segments/tenant-a/.marker" "${DST_DIR}/segments/tenant-a/.marker"
)

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

if [[ "${INCLUDE_XLARGE_GUARD}" == "true" ]]; then
  echo "[ci] benchmark history guard (xlarge)"
  XLARGE_CMD=(
    cargo run -p benchmark-smoke --bin benchmark-smoke --
    --profile xlarge
    --guard-history docs/benchmarks/history/benchmark-history.md
    --max-dash-latency-regression-pct "${BENCH_GUARD_MAX_REGRESSION_PCT}"
    --xlarge-min-candidate-reduction-pct "${CI_XLARGE_MIN_CANDIDATE_REDUCTION_PCT}"
    --xlarge-max-dash-latency-ms "${CI_XLARGE_MAX_DASH_LATENCY_MS}"
  )
  if [[ -n "${XLARGE_GUARD_ITERATIONS}" ]]; then
    XLARGE_CMD+=(--iterations "${XLARGE_GUARD_ITERATIONS}")
  fi
  DASH_BENCH_ANN_MAX_NEIGHBORS_BASE="${CI_XLARGE_ANN_MAX_NEIGHBORS_BASE}" \
    DASH_BENCH_ANN_MAX_NEIGHBORS_UPPER="${CI_XLARGE_ANN_MAX_NEIGHBORS_UPPER}" \
    DASH_BENCH_ANN_SEARCH_EXPANSION_FACTOR="${CI_XLARGE_ANN_SEARCH_EXPANSION_FACTOR}" \
    DASH_BENCH_ANN_SEARCH_EXPANSION_MIN="${CI_XLARGE_ANN_SEARCH_EXPANSION_MIN}" \
    DASH_BENCH_ANN_SEARCH_EXPANSION_MAX="${CI_XLARGE_ANN_SEARCH_EXPANSION_MAX}" \
    "${XLARGE_CMD[@]}"
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
    DASH_BENCH_INCLUDE_XLARGE="${BENCH_TREND_INCLUDE_XLARGE}" \
    DASH_BENCH_INCLUDE_HYBRID="${BENCH_TREND_INCLUDE_HYBRID}" \
    scripts/benchmark_trend.sh --run-tag "${BENCH_TREND_TAG}"
fi

if [[ "${INCLUDE_SLO_GUARD}" == "true" ]]; then
  echo "[ci] slo guard"
  DASH_SLO_INCLUDE_RECOVERY_DRILL="${CI_SLO_INCLUDE_RECOVERY_DRILL}" \
    scripts/slo_guard.sh --run-tag ci
fi

echo "[ci] benchmark smoke"
cargo run -p benchmark-smoke --bin benchmark-smoke -- --smoke

echo "[ci] all checks passed"
