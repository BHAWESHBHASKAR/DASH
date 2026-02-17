#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

HISTORY_PATH="${DASH_BENCH_HISTORY_PATH:-docs/benchmarks/history/benchmark-history.md}"
SCORECARD_DIR="${DASH_BENCH_SCORECARD_DIR:-docs/benchmarks/scorecards}"
SUMMARY_DIR="${DASH_BENCH_SUMMARY_DIR:-docs/benchmarks/history/runs}"
MAX_REGRESSION_PCT="${DASH_BENCH_GUARD_MAX_REGRESSION_PCT:-${EME_BENCH_GUARD_MAX_REGRESSION_PCT:-50}}"
INCLUDE_LARGE="${DASH_BENCH_INCLUDE_LARGE:-true}"
INCLUDE_XLARGE="${DASH_BENCH_INCLUDE_XLARGE:-false}"
INCLUDE_HYBRID="${DASH_BENCH_INCLUDE_HYBRID:-false}"
RUN_TAG="${DASH_BENCH_RUN_TAG:-release-candidate}"
SMOKE_ITERATIONS=""
LARGE_ITERATIONS=""
XLARGE_ITERATIONS=""
HYBRID_ITERATIONS=""

usage() {
  cat <<'USAGE'
Usage: scripts/benchmark_trend.sh [options]

Options:
  --history-path PATH           Benchmark history markdown path
  --scorecard-dir DIR           Output directory for scorecards
  --summary-dir DIR             Output directory for run summaries
  --max-regression-pct N        Max allowed DASH avg-latency regression percentage
  --include-large true|false    Include the large (50k) profile run (default: true)
  --include-xlarge true|false   Include the xlarge (100k) profile run (default: false)
  --include-hybrid true|false   Include the hybrid metadata+embedding profile run (default: false)
  --run-tag TAG                 Suffix tag in generated artifact filenames
  --smoke-iterations N          Override smoke profile iteration count
  --large-iterations N          Override large profile iteration count
  --xlarge-iterations N         Override xlarge profile iteration count
  --hybrid-iterations N         Override hybrid profile iteration count
  -h, --help                    Show this help
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --history-path)
      HISTORY_PATH="$2"
      shift 2
      ;;
    --scorecard-dir)
      SCORECARD_DIR="$2"
      shift 2
      ;;
    --summary-dir)
      SUMMARY_DIR="$2"
      shift 2
      ;;
    --max-regression-pct)
      MAX_REGRESSION_PCT="$2"
      shift 2
      ;;
    --include-large)
      INCLUDE_LARGE="$2"
      shift 2
      ;;
    --include-xlarge)
      INCLUDE_XLARGE="$2"
      shift 2
      ;;
    --include-hybrid)
      INCLUDE_HYBRID="$2"
      shift 2
      ;;
    --run-tag)
      RUN_TAG="$2"
      shift 2
      ;;
    --smoke-iterations)
      SMOKE_ITERATIONS="$2"
      shift 2
      ;;
    --large-iterations)
      LARGE_ITERATIONS="$2"
      shift 2
      ;;
    --xlarge-iterations)
      XLARGE_ITERATIONS="$2"
      shift 2
      ;;
    --hybrid-iterations)
      HYBRID_ITERATIONS="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

case "${INCLUDE_LARGE}" in
  true|false) ;;
  *)
    echo "--include-large must be true or false" >&2
    exit 2
    ;;
esac

case "${INCLUDE_XLARGE}" in
  true|false) ;;
  *)
    echo "--include-xlarge must be true or false" >&2
    exit 2
    ;;
esac

case "${INCLUDE_HYBRID}" in
  true|false) ;;
  *)
    echo "--include-hybrid must be true or false" >&2
    exit 2
    ;;
esac

mkdir -p "$(dirname "${HISTORY_PATH}")" "${SCORECARD_DIR}" "${SUMMARY_DIR}"

RUN_STAMP="$(date -u +%Y%m%d-%H%M%S)"
RUN_ID="${RUN_STAMP}-${RUN_TAG}"
SUMMARY_PATH="${SUMMARY_DIR}/${RUN_ID}.md"
ROWS_TMP="$(mktemp)"
DETAILS_TMP="$(mktemp)"

cleanup() {
  rm -f "${ROWS_TMP}" "${DETAILS_TMP}"
}
trap cleanup EXIT

render_summary() {
  cat > "${SUMMARY_PATH}" <<EOF_SUMMARY
# DASH Benchmark Trend Run

- run_id: ${RUN_ID}
- run_utc: $(date -u +%Y-%m-%dT%H:%M:%SZ)
- history_path: ${HISTORY_PATH}
- max_regression_pct: ${MAX_REGRESSION_PCT}

| profile | baseline_avg_ms | dash_avg_ms | quality_probes | scorecard |
|---|---:|---:|---|---|
EOF_SUMMARY
  cat "${ROWS_TMP}" >> "${SUMMARY_PATH}"
  echo >> "${SUMMARY_PATH}"
  cat "${DETAILS_TMP}" >> "${SUMMARY_PATH}"
}

run_profile() {
  local profile="$1"
  local iterations="$2"
  local scorecard_path="${SCORECARD_DIR}/${RUN_ID}-${profile}.md"

  local cmd=(
    cargo run -p benchmark-smoke --bin benchmark-smoke --
    --profile "${profile}"
    --guard-history "${HISTORY_PATH}"
    --max-dash-latency-regression-pct "${MAX_REGRESSION_PCT}"
    --history-out "${HISTORY_PATH}"
    --scorecard-out "${scorecard_path}"
  )
  if [[ -n "${iterations}" ]]; then
    cmd+=(--iterations "${iterations}")
  fi

  echo "[bench-trend] running ${profile} profile"
  echo "[bench-trend] command: ${cmd[*]}"

  local output
  if ! output="$("${cmd[@]}" 2>&1)"; then
    echo "${output}"
    {
      echo
      echo "## ${profile}"
      echo
      echo "status: FAIL"
      echo "scorecard: ${scorecard_path}"
      echo
      echo '```text'
      echo "${output}"
      echo '```'
    } >> "${DETAILS_TMP}"
    printf '| %s | %s | %s | %s | %s |\n' \
      "${profile}" "n/a" "n/a" "fail" "${scorecard_path}" >> "${ROWS_TMP}"
    return 1
  fi

  echo "${output}"

  local baseline_avg
  local dash_avg
  local quality
  baseline_avg="$(printf '%s\n' "${output}" | awk -F': ' '/Baseline avg latency \(ms\):/{print $2; exit}')"
  dash_avg="$(printf '%s\n' "${output}" | awk -F': ' '/DASH avg latency \(ms\):/{print $2; exit}')"
  quality="$(printf '%s\n' "${output}" | awk -F': ' '/Quality probes passed:/{print $2; exit}')"

  [[ -n "${baseline_avg}" ]] || baseline_avg="n/a"
  [[ -n "${dash_avg}" ]] || dash_avg="n/a"
  [[ -n "${quality}" ]] || quality="n/a"

  printf '| %s | %s | %s | %s | %s |\n' \
    "${profile}" "${baseline_avg}" "${dash_avg}" "${quality}" "${scorecard_path}" >> "${ROWS_TMP}"

  {
    echo
    echo "## ${profile}"
    echo
    echo "status: PASS"
    echo "scorecard: ${scorecard_path}"
    echo
    echo '```text'
    echo "${output}"
    echo '```'
  } >> "${DETAILS_TMP}"
}

FAILED=false

if ! run_profile "smoke" "${SMOKE_ITERATIONS}"; then
  FAILED=true
fi
if [[ "${INCLUDE_LARGE}" == "true" ]]; then
  if ! run_profile "large" "${LARGE_ITERATIONS}"; then
    FAILED=true
  fi
fi
if [[ "${INCLUDE_XLARGE}" == "true" ]]; then
  if ! run_profile "xlarge" "${XLARGE_ITERATIONS}"; then
    FAILED=true
  fi
fi
if [[ "${INCLUDE_HYBRID}" == "true" ]]; then
  if ! run_profile "hybrid" "${HYBRID_ITERATIONS}"; then
    FAILED=true
  fi
fi

render_summary

if [[ "${FAILED}" == "true" ]]; then
  echo "[bench-trend] failed"
  echo "[bench-trend] summary: ${SUMMARY_PATH}"
  exit 1
fi

echo "[bench-trend] completed"
echo "[bench-trend] summary: ${SUMMARY_PATH}"
