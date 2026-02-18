#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

PROFILE="${DASH_SLO_PROFILE:-smoke}"
ITERATIONS="${DASH_SLO_ITERATIONS:-}"
RUN_TAG="${DASH_SLO_RUN_TAG:-manual}"
SLO_HISTORY_PATH="${DASH_SLO_HISTORY_PATH:-docs/benchmarks/history/runs/slo-history.csv}"
SUMMARY_DIR="${DASH_SLO_SUMMARY_DIR:-docs/benchmarks/history/runs}"

MAX_DASH_LATENCY_MS="${DASH_SLO_MAX_DASH_LATENCY_MS:-120}"
MIN_CONTRADICTION_F1="${DASH_SLO_MIN_CONTRADICTION_F1:-0.80}"
MIN_ANN_RECALL_AT_10="${DASH_SLO_MIN_ANN_RECALL_AT_10:-0.95}"
MIN_ANN_RECALL_AT_100="${DASH_SLO_MIN_ANN_RECALL_AT_100:-0.99}"
MIN_QUALITY_PROBES_PASSED="${DASH_SLO_MIN_QUALITY_PROBES_PASSED:-5}"
REQUIRE_DASH_HIT="${DASH_SLO_REQUIRE_DASH_HIT:-true}"

WINDOW_RUNS="${DASH_SLO_WINDOW_RUNS:-20}"
MAX_FAILED_RUN_PCT="${DASH_SLO_MAX_FAILED_RUN_PCT:-10}"
REQUIRE_CURRENT_PASS="${DASH_SLO_REQUIRE_CURRENT_PASS:-true}"

INCLUDE_RECOVERY_DRILL="${DASH_SLO_INCLUDE_RECOVERY_DRILL:-false}"
RECOVERY_MAX_RTO_SECONDS="${DASH_SLO_RECOVERY_MAX_RTO_SECONDS:-60}"
RECOVERY_MAX_RPO_CLAIM_GAP="${DASH_SLO_RECOVERY_MAX_RPO_CLAIM_GAP:-0}"

usage() {
  cat <<'USAGE'
Usage: scripts/slo_guard.sh [options]

Run SLO/error-budget guard checks for DASH and persist a run record.

Options:
  --profile NAME                    Benchmark profile (default: smoke)
  --iterations N                    Benchmark iterations override
  --run-tag TAG                     Run tag in output artifacts (default: manual)
  --slo-history-path PATH           CSV path for SLO run history
  --summary-dir DIR                 Output directory for SLO summaries
  --max-dash-latency-ms N           SLO max DASH avg latency (ms)
  --min-contradiction-f1 N          Minimum contradiction F1 gate
  --min-ann-recall-at-10 N          Minimum ANN recall@10 gate
  --min-ann-recall-at-100 N         Minimum ANN recall@100 gate
  --min-quality-probes-passed N     Minimum quality probes passed count
  --require-dash-hit true|false     Require "DASH hit expected: true"
  --window-runs N                   Error-budget lookback window size
  --max-failed-run-pct N            Max allowed failed-run percentage in window
  --require-current-pass true|false Require current run to pass
  --include-recovery-drill true|false
                                    Include recovery drill as SLO signal
  --recovery-max-rto-seconds N      Max allowed recovery drill RTO (seconds)
  --recovery-max-rpo-claim-gap N    Max allowed recovery drill RPO claim gap
  -h, --help                        Show help

Environment:
  DASH_SLO_* variables are supported as defaults for all options.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --profile)
      PROFILE="$2"
      shift 2
      ;;
    --iterations)
      ITERATIONS="$2"
      shift 2
      ;;
    --run-tag)
      RUN_TAG="$2"
      shift 2
      ;;
    --slo-history-path)
      SLO_HISTORY_PATH="$2"
      shift 2
      ;;
    --summary-dir)
      SUMMARY_DIR="$2"
      shift 2
      ;;
    --max-dash-latency-ms)
      MAX_DASH_LATENCY_MS="$2"
      shift 2
      ;;
    --min-contradiction-f1)
      MIN_CONTRADICTION_F1="$2"
      shift 2
      ;;
    --min-ann-recall-at-10)
      MIN_ANN_RECALL_AT_10="$2"
      shift 2
      ;;
    --min-ann-recall-at-100)
      MIN_ANN_RECALL_AT_100="$2"
      shift 2
      ;;
    --min-quality-probes-passed)
      MIN_QUALITY_PROBES_PASSED="$2"
      shift 2
      ;;
    --require-dash-hit)
      REQUIRE_DASH_HIT="$2"
      shift 2
      ;;
    --window-runs)
      WINDOW_RUNS="$2"
      shift 2
      ;;
    --max-failed-run-pct)
      MAX_FAILED_RUN_PCT="$2"
      shift 2
      ;;
    --require-current-pass)
      REQUIRE_CURRENT_PASS="$2"
      shift 2
      ;;
    --include-recovery-drill)
      INCLUDE_RECOVERY_DRILL="$2"
      shift 2
      ;;
    --recovery-max-rto-seconds)
      RECOVERY_MAX_RTO_SECONDS="$2"
      shift 2
      ;;
    --recovery-max-rpo-claim-gap)
      RECOVERY_MAX_RPO_CLAIM_GAP="$2"
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

for raw_bool in \
  "${REQUIRE_DASH_HIT}" \
  "${REQUIRE_CURRENT_PASS}" \
  "${INCLUDE_RECOVERY_DRILL}"; do
  case "${raw_bool}" in
    true|false) ;;
    *)
      echo "boolean options must be true|false (got '${raw_bool}')" >&2
      exit 2
      ;;
  esac
done

if [[ -n "${ITERATIONS}" && ! "${ITERATIONS}" =~ ^[0-9]+$ ]]; then
  echo "--iterations must be a non-negative integer" >&2
  exit 2
fi
for int_opt in "${WINDOW_RUNS}" "${MIN_QUALITY_PROBES_PASSED}" "${RECOVERY_MAX_RTO_SECONDS}" "${RECOVERY_MAX_RPO_CLAIM_GAP}"; do
  if [[ ! "${int_opt}" =~ ^[0-9]+$ ]]; then
    echo "integer options must be non-negative integers" >&2
    exit 2
  fi
done

mkdir -p "$(dirname "${SLO_HISTORY_PATH}")" "${SUMMARY_DIR}"

RUN_STAMP="$(date -u +%Y%m%d-%H%M%S)"
RUN_ID="${RUN_STAMP}-${RUN_TAG}-${PROFILE}"
RUN_EPOCH_SECS="$(date -u +%s)"
SUMMARY_PATH="${SUMMARY_DIR}/${RUN_ID}-slo.md"

BENCH_CMD=(
  cargo run -p benchmark-smoke --bin benchmark-smoke --
  --profile "${PROFILE}"
)
if [[ -n "${ITERATIONS}" ]]; then
  BENCH_CMD+=(--iterations "${ITERATIONS}")
fi

echo "[slo-guard] running benchmark profile=${PROFILE}"
echo "[slo-guard] command: ${BENCH_CMD[*]}"
BENCH_OUTPUT="$("${BENCH_CMD[@]}" 2>&1)"
echo "${BENCH_OUTPUT}"

DASH_AVG_MS="$(printf '%s\n' "${BENCH_OUTPUT}" | awk -F': ' '/DASH avg latency \(ms\):/{print $2; exit}')"
DASH_HIT_EXPECTED="$(printf '%s\n' "${BENCH_OUTPUT}" | awk -F': ' '/DASH hit expected:/{print $2; exit}')"
QUALITY_PROBES_RAW="$(printf '%s\n' "${BENCH_OUTPUT}" | awk -F': ' '/Quality probes passed:/{print $2; exit}')"
CONTRADICTION_F1_RAW="$(printf '%s\n' "${BENCH_OUTPUT}" | awk -F': ' '/Quality probe contradiction_detection_f1:/{print $2; exit}')"
ANN_RECALL_10="$(printf '%s\n' "${BENCH_OUTPUT}" | awk -F': ' '/ANN recall@10:/{print $2; exit}')"
ANN_RECALL_100="$(printf '%s\n' "${BENCH_OUTPUT}" | awk -F': ' '/ANN recall@100:/{print $2; exit}')"

if [[ -z "${DASH_AVG_MS}" || -z "${QUALITY_PROBES_RAW}" || -z "${CONTRADICTION_F1_RAW}" || -z "${ANN_RECALL_10}" || -z "${ANN_RECALL_100}" ]]; then
  echo "[slo-guard] failed parsing benchmark output" >&2
  exit 1
fi

QUALITY_PROBES_PASSED="${QUALITY_PROBES_RAW%%/*}"
QUALITY_PROBES_TOTAL="${QUALITY_PROBES_RAW##*/}"
CONTRADICTION_F1="$(printf '%s' "${CONTRADICTION_F1_RAW}" | awk '{print $1}')"

latency_pass="false"
if awk -v v="${DASH_AVG_MS}" -v max="${MAX_DASH_LATENCY_MS}" 'BEGIN { exit (v <= max ? 0 : 1) }'; then
  latency_pass="true"
fi

quality_count_pass="false"
if awk -v passed="${QUALITY_PROBES_PASSED}" -v min="${MIN_QUALITY_PROBES_PASSED}" 'BEGIN { exit (passed >= min ? 0 : 1) }'; then
  quality_count_pass="true"
fi

contradiction_f1_pass="false"
if awk -v v="${CONTRADICTION_F1}" -v min="${MIN_CONTRADICTION_F1}" 'BEGIN { exit (v >= min ? 0 : 1) }'; then
  contradiction_f1_pass="true"
fi

ann_recall_10_pass="false"
if awk -v v="${ANN_RECALL_10}" -v min="${MIN_ANN_RECALL_AT_10}" 'BEGIN { exit (v >= min ? 0 : 1) }'; then
  ann_recall_10_pass="true"
fi

ann_recall_100_pass="false"
if awk -v v="${ANN_RECALL_100}" -v min="${MIN_ANN_RECALL_AT_100}" 'BEGIN { exit (v >= min ? 0 : 1) }'; then
  ann_recall_100_pass="true"
fi

dash_hit_pass="true"
if [[ "${REQUIRE_DASH_HIT}" == "true" ]]; then
  if [[ "${DASH_HIT_EXPECTED}" != "true" ]]; then
    dash_hit_pass="false"
  fi
fi

recovery_enabled="${INCLUDE_RECOVERY_DRILL}"
recovery_rto_seconds="n/a"
recovery_rpo_claim_gap="n/a"
recovery_pass="true"
recovery_output=""
if [[ "${INCLUDE_RECOVERY_DRILL}" == "true" ]]; then
  echo "[slo-guard] running recovery drill"
  recovery_output="$(scripts/recovery_drill.sh --max-rto-seconds "${RECOVERY_MAX_RTO_SECONDS}" --keep-artifacts false 2>&1)"
  echo "${recovery_output}"
  recovery_rto_seconds="$(printf '%s\n' "${recovery_output}" | awk -F= '/\[drill\] rto_seconds=/{print $2; exit}')"
  recovery_rpo_claim_gap="$(printf '%s\n' "${recovery_output}" | awk -F= '/\[drill\] rpo_claim_gap=/{print $2; exit}')"
  if [[ -z "${recovery_rto_seconds}" || -z "${recovery_rpo_claim_gap}" ]]; then
    recovery_pass="false"
  else
    if ! awk -v v="${recovery_rto_seconds}" -v max="${RECOVERY_MAX_RTO_SECONDS}" 'BEGIN { exit (v <= max ? 0 : 1) }'; then
      recovery_pass="false"
    fi
    if ! awk -v v="${recovery_rpo_claim_gap}" -v max="${RECOVERY_MAX_RPO_CLAIM_GAP}" 'BEGIN { exit (v <= max ? 0 : 1) }'; then
      recovery_pass="false"
    fi
  fi
fi

current_pass="true"
for gate in \
  "${latency_pass}" \
  "${quality_count_pass}" \
  "${contradiction_f1_pass}" \
  "${ann_recall_10_pass}" \
  "${ann_recall_100_pass}" \
  "${dash_hit_pass}" \
  "${recovery_pass}"; do
  if [[ "${gate}" != "true" ]]; then
    current_pass="false"
    break
  fi
done

if [[ ! -f "${SLO_HISTORY_PATH}" ]]; then
  cat > "${SLO_HISTORY_PATH}" <<'EOF_HEADER'
run_epoch_secs,run_id,profile,dash_avg_ms,dash_hit_expected,quality_probes_passed,quality_probes_total,contradiction_f1,ann_recall_at_10,ann_recall_at_100,recovery_enabled,recovery_rto_seconds,recovery_rpo_claim_gap,current_pass
EOF_HEADER
fi

printf '%s,%s,%s,%.4f,%s,%s,%s,%.4f,%.4f,%.4f,%s,%s,%s,%s\n' \
  "${RUN_EPOCH_SECS}" \
  "${RUN_ID}" \
  "${PROFILE}" \
  "${DASH_AVG_MS}" \
  "${DASH_HIT_EXPECTED:-unknown}" \
  "${QUALITY_PROBES_PASSED}" \
  "${QUALITY_PROBES_TOTAL}" \
  "${CONTRADICTION_F1}" \
  "${ANN_RECALL_10}" \
  "${ANN_RECALL_100}" \
  "${recovery_enabled}" \
  "${recovery_rto_seconds}" \
  "${recovery_rpo_claim_gap}" \
  "${current_pass}" >> "${SLO_HISTORY_PATH}"

WINDOW_TMP="$(mktemp)"
cleanup() {
  rm -f "${WINDOW_TMP}"
}
trap cleanup EXIT

awk 'NR>1' "${SLO_HISTORY_PATH}" | tail -n "${WINDOW_RUNS}" > "${WINDOW_TMP}" || true
window_total="$(awk 'END {print NR+0}' "${WINDOW_TMP}")"
window_failed="$(awk -F',' 'BEGIN{n=0} NF>0 && $14!="true" {n++} END{print n+0}' "${WINDOW_TMP}")"
window_failed_pct="0.00"
if [[ "${window_total}" -gt 0 ]]; then
  window_failed_pct="$(awk -v failed="${window_failed}" -v total="${window_total}" 'BEGIN { printf "%.2f", (failed * 100.0) / total }')"
fi
error_budget_pass="false"
if awk -v v="${window_failed_pct}" -v max="${MAX_FAILED_RUN_PCT}" 'BEGIN { exit (v <= max ? 0 : 1) }'; then
  error_budget_pass="true"
fi

overall_pass="${error_budget_pass}"
if [[ "${REQUIRE_CURRENT_PASS}" == "true" && "${current_pass}" != "true" ]]; then
  overall_pass="false"
fi

{
  echo "# DASH SLO Guard Summary"
  echo
  echo "- run_id: ${RUN_ID}"
  echo "- run_utc: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "- profile: ${PROFILE}"
  echo "- iterations: ${ITERATIONS:-default}"
  echo "- slo_history_path: ${SLO_HISTORY_PATH}"
  echo
  echo "## Current Run Gates"
  echo
  echo "- dash_avg_ms: ${DASH_AVG_MS} (max ${MAX_DASH_LATENCY_MS}) -> ${latency_pass}"
  echo "- dash_hit_expected: ${DASH_HIT_EXPECTED:-unknown} (required=${REQUIRE_DASH_HIT}) -> ${dash_hit_pass}"
  echo "- quality_probes_passed: ${QUALITY_PROBES_PASSED}/${QUALITY_PROBES_TOTAL} (min ${MIN_QUALITY_PROBES_PASSED}) -> ${quality_count_pass}"
  echo "- contradiction_f1: ${CONTRADICTION_F1} (min ${MIN_CONTRADICTION_F1}) -> ${contradiction_f1_pass}"
  echo "- ann_recall_at_10: ${ANN_RECALL_10} (min ${MIN_ANN_RECALL_AT_10}) -> ${ann_recall_10_pass}"
  echo "- ann_recall_at_100: ${ANN_RECALL_100} (min ${MIN_ANN_RECALL_AT_100}) -> ${ann_recall_100_pass}"
  echo "- recovery_drill_enabled: ${recovery_enabled}"
  if [[ "${recovery_enabled}" == "true" ]]; then
    echo "- recovery_rto_seconds: ${recovery_rto_seconds} (max ${RECOVERY_MAX_RTO_SECONDS})"
    echo "- recovery_rpo_claim_gap: ${recovery_rpo_claim_gap} (max ${RECOVERY_MAX_RPO_CLAIM_GAP})"
    echo "- recovery_pass: ${recovery_pass}"
  fi
  echo "- current_pass: ${current_pass}"
  echo
  echo "## Error Budget Window"
  echo
  echo "- window_runs: ${window_total} (requested=${WINDOW_RUNS})"
  echo "- window_failed_runs: ${window_failed}"
  echo "- window_failed_run_pct: ${window_failed_pct} (max ${MAX_FAILED_RUN_PCT}) -> ${error_budget_pass}"
  echo
  echo "## Decision"
  echo
  echo "- require_current_pass: ${REQUIRE_CURRENT_PASS}"
  echo "- overall_pass: ${overall_pass}"
} > "${SUMMARY_PATH}"

echo "[slo-guard] summary: ${SUMMARY_PATH}"
echo "[slo-guard] current_pass=${current_pass}"
echo "[slo-guard] window_failed_run_pct=${window_failed_pct} (max ${MAX_FAILED_RUN_PCT})"
echo "[slo-guard] overall_pass=${overall_pass}"

if [[ "${overall_pass}" != "true" ]]; then
  exit 1
fi
