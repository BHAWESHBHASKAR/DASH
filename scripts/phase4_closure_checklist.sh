#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

TIER="tier-b"
RUN_ID=""
SUMMARY_PATH=""
OUT_PATH=""
MIN_ANN_RECALL_10="0.95"
MIN_ANN_RECALL_100="0.98"
MIN_SHARDS="8"
RECOVERY_ARTIFACT=""
INCIDENT_ARTIFACT=""

usage() {
  cat <<'USAGE'
Usage: scripts/phase4_closure_checklist.sh [options]

Generate and evaluate a Phase 4 closure checklist from a phase4_scale_proof summary.

Options:
  --tier NAME                    tier-a|tier-b|tier-c (aliases: 1m|10m|100m)
                                 default: tier-b
  --run-id ID                    run id used by phase4_scale_proof summary
  --summary-path PATH            explicit summary markdown path
                                 default: docs/benchmarks/history/runs/<run-id>.md
  --out PATH                     output checklist markdown path
                                 default: docs/benchmarks/history/runs/<run-id>-closure-<tier>.md
  --min-ann-recall-10 VALUE      minimum ann_recall_at_10 (default: 0.95)
  --min-ann-recall-100 VALUE     minimum ann_recall_at_100 (default: 0.98)
  --min-shards N                 minimum shards for tier-b/tier-c (default: 8)
  --recovery-artifact PATH       required only for tier-c
  --incident-artifact PATH       required only for tier-c
  -h, --help                     Show help
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --tier) TIER="$2"; shift 2 ;;
    --run-id) RUN_ID="$2"; shift 2 ;;
    --summary-path) SUMMARY_PATH="$2"; shift 2 ;;
    --out) OUT_PATH="$2"; shift 2 ;;
    --min-ann-recall-10) MIN_ANN_RECALL_10="$2"; shift 2 ;;
    --min-ann-recall-100) MIN_ANN_RECALL_100="$2"; shift 2 ;;
    --min-shards) MIN_SHARDS="$2"; shift 2 ;;
    --recovery-artifact) RECOVERY_ARTIFACT="$2"; shift 2 ;;
    --incident-artifact) INCIDENT_ARTIFACT="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

case "${TIER}" in
  tier-a|1m) TIER="tier-a" ;;
  tier-b|10m) TIER="tier-b" ;;
  tier-c|100m) TIER="tier-c" ;;
  *)
    echo "--tier must be tier-a|tier-b|tier-c (or 1m|10m|100m)" >&2
    exit 2
    ;;
esac

if [[ -n "${RUN_ID}" && -z "${SUMMARY_PATH}" ]]; then
  SUMMARY_PATH="docs/benchmarks/history/runs/${RUN_ID}.md"
fi

if [[ -z "${SUMMARY_PATH}" ]]; then
  echo "either --run-id or --summary-path must be provided" >&2
  exit 2
fi

if [[ ! -f "${SUMMARY_PATH}" ]]; then
  echo "summary file not found: ${SUMMARY_PATH}" >&2
  exit 1
fi

if [[ ! "${MIN_SHARDS}" =~ ^[0-9]+$ ]] || [[ "${MIN_SHARDS}" -lt 1 ]]; then
  echo "--min-shards must be a positive integer" >&2
  exit 2
fi

summary_line_value() {
  local path="$1"
  local key="$2"
  sed -nE "s/^- ${key}: (.*)\$/\\1/p" "${path}" | head -n 1
}

markdown_table_value() {
  local path="$1"
  local key="$2"
  awk -F'|' -v target="${key}" '
    {
      k=$2
      v=$3
      gsub(/^[ \t]+|[ \t]+$/, "", k)
      gsub(/^[ \t]+|[ \t]+$/, "", v)
      if (k == target) {
        print v
        exit
      }
    }
  ' "${path}"
}

step_status_value() {
  local path="$1"
  local step_prefix="$2"
  awk -F'|' -v prefix="${step_prefix}" '
    {
      step=$2
      status=$3
      gsub(/^[ \t]+|[ \t]+$/, "", step)
      gsub(/^[ \t]+|[ \t]+$/, "", status)
      if (index(step, prefix) == 1) {
        print status
        exit
      }
    }
  ' "${path}"
}

artifact_path_value() {
  local path="$1"
  local key="$2"
  sed -nE "s/^- ${key}: (.*)\$/\\1/p" "${path}" | head -n 1
}

PASS_COUNT=0
FAIL_COUNT=0
CHECK_ROWS=()

add_check() {
  local check="$1"
  local result="$2"
  local details="$3"
  CHECK_ROWS+=("| ${check} | ${result} | ${details} |")
  if [[ "${result}" == "PASS" ]]; then
    PASS_COUNT=$((PASS_COUNT + 1))
  else
    FAIL_COUNT=$((FAIL_COUNT + 1))
  fi
}

require_file_check() {
  local label="$1"
  local path="$2"
  if [[ -n "${path}" && -f "${path}" ]]; then
    add_check "${label}" "PASS" "${path}"
  else
    if [[ -z "${path}" ]]; then
      add_check "${label}" "FAIL" "missing path in summary"
    else
      add_check "${label}" "FAIL" "file not found: ${path}"
    fi
  fi
}

overall_status="$(summary_line_value "${SUMMARY_PATH}" "overall_status")"
run_id_from_summary="$(summary_line_value "${SUMMARY_PATH}" "run_id")"
if [[ -z "${RUN_ID}" ]]; then
  RUN_ID="${run_id_from_summary:-unknown-run}"
fi

history_path="$(summary_line_value "${SUMMARY_PATH}" "history_path")"
scorecard_path="$(summary_line_value "${SUMMARY_PATH}" "scorecard")"
ann_recall_10="$(markdown_table_value "${SUMMARY_PATH}" "ann_recall_at_10")"
ann_recall_100="$(markdown_table_value "${SUMMARY_PATH}" "ann_recall_at_100")"
freshness_guard="$(awk -F'|' '/\| ingestion_freshness_guard/{value=$3; gsub(/^[ \t]+|[ \t]+$/, "", value); print value; exit}' "${SUMMARY_PATH}")"
rebalance_shard_count_after="$(markdown_table_value "${SUMMARY_PATH}" "rebalance_shard_count_after")"
rebalance_shard_gate_pass="$(markdown_table_value "${SUMMARY_PATH}" "rebalance_shard_gate_pass")"
storage_promotion_boundary_trend_window_samples="$(markdown_table_value "${SUMMARY_PATH}" "storage_promotion_boundary_trend_window_samples")"
storage_promotion_boundary_min_trend_samples="$(markdown_table_value "${SUMMARY_PATH}" "storage_promotion_boundary_min_trend_samples")"
storage_promotion_boundary_trend_sample_gate_pass="$(markdown_table_value "${SUMMARY_PATH}" "storage_promotion_boundary_trend_sample_gate_pass")"

retrieval_artifact="$(artifact_path_value "${SUMMARY_PATH}" "retrieval_concurrency_summary")"
ingestion_artifact="$(artifact_path_value "${SUMMARY_PATH}" "ingestion_concurrency_summary")"
rebalance_artifact="$(artifact_path_value "${SUMMARY_PATH}" "rebalance_summary")"
storage_promotion_boundary_artifact="$(artifact_path_value "${SUMMARY_PATH}" "storage_promotion_boundary_summary")"

benchmark_step="$(step_status_value "${SUMMARY_PATH}" "benchmark (")"
retrieval_step="$(step_status_value "${SUMMARY_PATH}" "retrieval transport concurrency")"
ingestion_step="$(step_status_value "${SUMMARY_PATH}" "ingestion transport concurrency")"
failover_step="$(step_status_value "${SUMMARY_PATH}" "failover drill")"
rebalance_step="$(step_status_value "${SUMMARY_PATH}" "rebalance drill")"
storage_promotion_boundary_guard_step="$(step_status_value "${SUMMARY_PATH}" "storage promotion-boundary guard")"
storage_promotion_boundary_trend_step="$(step_status_value "${SUMMARY_PATH}" "storage promotion-boundary trend")"

if [[ "${overall_status}" == "PASS" ]]; then
  add_check "summary overall_status == PASS" "PASS" "overall_status=${overall_status}"
else
  add_check "summary overall_status == PASS" "FAIL" "overall_status=${overall_status:-missing}"
fi

for tuple in \
  "benchmark step:${benchmark_step}" \
  "retrieval concurrency step:${retrieval_step}" \
  "ingestion concurrency step:${ingestion_step}" \
  "failover step:${failover_step}"; do
  label="${tuple%%:*}"
  value="${tuple#*:}"
  if [[ "${value}" == "PASS" ]]; then
    add_check "${label}" "PASS" "${value}"
  else
    add_check "${label}" "FAIL" "${value:-missing}"
  fi
done

if awk -v actual="${ann_recall_10:-0}" -v min="${MIN_ANN_RECALL_10}" 'BEGIN { exit !(actual+0 >= min+0) }'; then
  add_check "ann_recall_at_10 >= ${MIN_ANN_RECALL_10}" "PASS" "actual=${ann_recall_10}"
else
  add_check "ann_recall_at_10 >= ${MIN_ANN_RECALL_10}" "FAIL" "actual=${ann_recall_10:-missing}"
fi

if awk -v actual="${ann_recall_100:-0}" -v min="${MIN_ANN_RECALL_100}" 'BEGIN { exit !(actual+0 >= min+0) }'; then
  add_check "ann_recall_at_100 >= ${MIN_ANN_RECALL_100}" "PASS" "actual=${ann_recall_100}"
else
  add_check "ann_recall_at_100 >= ${MIN_ANN_RECALL_100}" "FAIL" "actual=${ann_recall_100:-missing}"
fi

if [[ "${freshness_guard}" == "true" ]]; then
  add_check "ingestion freshness guard" "PASS" "${freshness_guard}"
else
  add_check "ingestion freshness guard" "FAIL" "${freshness_guard:-missing}"
fi

require_file_check "history artifact exists" "${history_path}"
require_file_check "scorecard artifact exists" "${scorecard_path}"
require_file_check "retrieval concurrency artifact exists" "${retrieval_artifact}"
require_file_check "ingestion concurrency artifact exists" "${ingestion_artifact}"

if [[ "${TIER}" == "tier-b" || "${TIER}" == "tier-c" ]]; then
  if [[ "${rebalance_step}" == "PASS" ]]; then
    add_check "rebalance step" "PASS" "${rebalance_step}"
  else
    add_check "rebalance step" "FAIL" "${rebalance_step:-missing}"
  fi
  if [[ "${rebalance_shard_gate_pass}" == "true" ]]; then
    add_check "rebalance shard gate pass" "PASS" "${rebalance_shard_gate_pass}"
  else
    add_check "rebalance shard gate pass" "FAIL" "${rebalance_shard_gate_pass:-missing}"
  fi
  if [[ "${rebalance_shard_count_after}" =~ ^[0-9]+$ ]] && (( rebalance_shard_count_after >= MIN_SHARDS )); then
    add_check "rebalance_shard_count_after >= ${MIN_SHARDS}" "PASS" "actual=${rebalance_shard_count_after}"
  else
    add_check "rebalance_shard_count_after >= ${MIN_SHARDS}" "FAIL" "actual=${rebalance_shard_count_after:-missing}"
  fi
  require_file_check "rebalance summary artifact exists" "${rebalance_artifact}"

  if [[ "${storage_promotion_boundary_guard_step}" == "PASS" ]]; then
    add_check "storage promotion-boundary guard step" "PASS" "${storage_promotion_boundary_guard_step}"
  else
    add_check "storage promotion-boundary guard step" "FAIL" "${storage_promotion_boundary_guard_step:-missing}"
  fi
  if [[ "${storage_promotion_boundary_trend_step}" == "PASS" ]]; then
    add_check "storage promotion-boundary trend step" "PASS" "${storage_promotion_boundary_trend_step}"
  else
    add_check "storage promotion-boundary trend step" "FAIL" "${storage_promotion_boundary_trend_step:-missing}"
  fi
  if [[ "${storage_promotion_boundary_trend_sample_gate_pass}" == "true" ]]; then
    add_check "storage promotion-boundary trend sample gate" "PASS" "${storage_promotion_boundary_trend_sample_gate_pass}"
  else
    add_check "storage promotion-boundary trend sample gate" "FAIL" "${storage_promotion_boundary_trend_sample_gate_pass:-missing}"
  fi
  if [[ "${storage_promotion_boundary_min_trend_samples}" =~ ^[0-9]+$ ]] && \
    [[ "${storage_promotion_boundary_trend_window_samples}" =~ ^[0-9]+$ ]] && \
    (( storage_promotion_boundary_trend_window_samples >= storage_promotion_boundary_min_trend_samples )); then
    add_check "storage trend samples >= required minimum" "PASS" "actual=${storage_promotion_boundary_trend_window_samples}, required=${storage_promotion_boundary_min_trend_samples}"
  else
    add_check "storage trend samples >= required minimum" "FAIL" "actual=${storage_promotion_boundary_trend_window_samples:-missing}, required=${storage_promotion_boundary_min_trend_samples:-missing}"
  fi
  require_file_check "storage promotion-boundary summary artifact exists" "${storage_promotion_boundary_artifact}"
fi

if [[ "${TIER}" == "tier-c" ]]; then
  require_file_check "tier-c recovery artifact exists" "${RECOVERY_ARTIFACT}"
  require_file_check "tier-c incident artifact exists" "${INCIDENT_ARTIFACT}"
fi

overall_decision="PASS"
if (( FAIL_COUNT > 0 )); then
  overall_decision="FAIL"
fi

if [[ -z "${OUT_PATH}" ]]; then
  OUT_PATH="docs/benchmarks/history/runs/${RUN_ID}-closure-${TIER}.md"
fi
mkdir -p "$(dirname "${OUT_PATH}")"

{
  echo "# DASH Phase 4 Closure Checklist"
  echo
  echo "- run_id: ${RUN_ID}"
  echo "- tier: ${TIER}"
  echo "- generated_utc: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "- summary_path: ${SUMMARY_PATH}"
  echo "- overall_decision: ${overall_decision}"
  echo "- pass_count: ${PASS_COUNT}"
  echo "- fail_count: ${FAIL_COUNT}"
  echo
  echo "| check | status | details |"
  echo "|---|---|---|"
  for row in "${CHECK_ROWS[@]}"; do
    echo "${row}"
  done
} > "${OUT_PATH}"

echo "[phase4-closure] tier=${TIER}"
echo "[phase4-closure] run_id=${RUN_ID}"
echo "[phase4-closure] decision=${overall_decision}"
echo "[phase4-closure] checklist=${OUT_PATH}"

if [[ "${overall_decision}" != "PASS" ]]; then
  exit 1
fi
