#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

MODE="${DASH_PHASE4_TIERC_MODE:-quick}"

# Pre-scan arguments so mode can define defaults before main parse.
args=("$@")
for ((i=0; i<${#args[@]}; i++)); do
  if [[ "${args[$i]}" == "--mode" ]]; then
    if (( i + 1 >= ${#args[@]} )); then
      echo "--mode requires a value (quick|staged)" >&2
      exit 2
    fi
    MODE="${args[$((i + 1))]}"
    i=$((i + 1))
  fi
done

case "${MODE}" in
  quick|staged) ;;
  *)
    echo "--mode must be quick or staged" >&2
    exit 2
    ;;
esac

RUN_ID="${DASH_PHASE4_TIERC_RUN_ID:-$(date -u +%Y%m%d-%H%M%S)-phase4-tierc}"
TARGET_SHARDS="${DASH_PHASE4_TIERC_TARGET_SHARDS:-8}"
MIN_SHARDS_GATE="${DASH_PHASE4_TIERC_MIN_SHARDS_GATE:-8}"
REBALANCE_REQUIRE_MOVED_KEYS="${DASH_PHASE4_TIERC_REBALANCE_REQUIRE_MOVED_KEYS:-1}"
REBALANCE_MAX_WAIT_SECONDS="${DASH_PHASE4_TIERC_REBALANCE_MAX_WAIT_SECONDS:-90}"
PLACEMENT_RELOAD_INTERVAL_MS="${DASH_PHASE4_TIERC_PLACEMENT_RELOAD_INTERVAL_MS:-200}"
FAILOVER_MAX_WAIT_SECONDS="${DASH_PHASE4_TIERC_FAILOVER_MAX_WAIT_SECONDS:-30}"
RUN_RECOVERY_DRILL="${DASH_PHASE4_TIERC_RUN_RECOVERY_DRILL:-true}"
RUN_INCIDENT_GATE="${DASH_PHASE4_TIERC_RUN_INCIDENT_GATE:-true}"
RUN_CLOSURE_CHECKLIST="${DASH_PHASE4_TIERC_RUN_CLOSURE_CHECKLIST:-true}"
RUN_SCALE_PROOF="${DASH_PHASE4_TIERC_RUN_SCALE_PROOF:-true}"
RUN_STORAGE_PROMOTION_BOUNDARY_GUARD="${DASH_PHASE4_TIERC_RUN_STORAGE_PROMOTION_BOUNDARY_GUARD:-false}"
RECOVERY_MAX_RTO_SECONDS="${DASH_PHASE4_TIERC_RECOVERY_MAX_RTO_SECONDS:-120}"
INCIDENT_FAILOVER_MODE="${DASH_PHASE4_TIERC_INCIDENT_FAILOVER_MODE:-both}"
INCIDENT_FAILOVER_MAX_WAIT_SECONDS="${DASH_PHASE4_TIERC_INCIDENT_FAILOVER_MAX_WAIT_SECONDS:-30}"
INCIDENT_AUTH_MAX_WAIT_SECONDS="${DASH_PHASE4_TIERC_INCIDENT_AUTH_MAX_WAIT_SECONDS:-30}"
INCIDENT_RECOVERY_MAX_RTO_SECONDS="${DASH_PHASE4_TIERC_INCIDENT_RECOVERY_MAX_RTO_SECONDS:-120}"
RECOVERY_ARTIFACT_DIR="${DASH_PHASE4_TIERC_RECOVERY_ARTIFACT_DIR:-docs/benchmarks/history/recovery}"
INCIDENT_SUMMARY_DIR="${DASH_PHASE4_TIERC_INCIDENT_SUMMARY_DIR:-docs/benchmarks/history/incidents}"
RECOVERY_ARTIFACT_OVERRIDE="${DASH_PHASE4_TIERC_RECOVERY_ARTIFACT:-}"
INCIDENT_ARTIFACT_OVERRIDE="${DASH_PHASE4_TIERC_INCIDENT_ARTIFACT:-}"
SUMMARY_PATH_OVERRIDE="${DASH_PHASE4_TIERC_SUMMARY_PATH:-}"

if [[ "${MODE}" == "staged" ]]; then
  PROFILE="${DASH_PHASE4_TIERC_PROFILE:-xlarge}"
  FIXTURE_SIZE="${DASH_PHASE4_TIERC_FIXTURE_SIZE:-150000}"
  ITERATIONS="${DASH_PHASE4_TIERC_ITERATIONS:-5}"
  BENCH_RELEASE="${DASH_PHASE4_TIERC_BENCH_RELEASE:-true}"
  FAILOVER_MODE="${DASH_PHASE4_TIERC_FAILOVER_MODE:-both}"
  RETRIEVAL_WORKERS_LIST="${DASH_PHASE4_TIERC_RETRIEVAL_WORKERS_LIST:-4}"
  INGESTION_WORKERS_LIST="${DASH_PHASE4_TIERC_INGESTION_WORKERS_LIST:-4}"
  RETRIEVAL_CLIENTS="${DASH_PHASE4_TIERC_RETRIEVAL_CLIENTS:-16}"
  INGESTION_CLIENTS="${DASH_PHASE4_TIERC_INGESTION_CLIENTS:-16}"
  RETRIEVAL_REQUESTS_PER_WORKER="${DASH_PHASE4_TIERC_RETRIEVAL_REQUESTS_PER_WORKER:-12}"
  INGESTION_REQUESTS_PER_WORKER="${DASH_PHASE4_TIERC_INGESTION_REQUESTS_PER_WORKER:-12}"
  RETRIEVAL_WARMUP_REQUESTS="${DASH_PHASE4_TIERC_RETRIEVAL_WARMUP_REQUESTS:-4}"
  INGESTION_WARMUP_REQUESTS="${DASH_PHASE4_TIERC_INGESTION_WARMUP_REQUESTS:-4}"
  REBALANCE_PROBE_KEYS_COUNT="${DASH_PHASE4_TIERC_REBALANCE_PROBE_KEYS_COUNT:-256}"
  RUN_STORAGE_PROMOTION_BOUNDARY_GUARD="${DASH_PHASE4_TIERC_RUN_STORAGE_PROMOTION_BOUNDARY_GUARD:-true}"
else
  PROFILE="${DASH_PHASE4_TIERC_PROFILE:-smoke}"
  FIXTURE_SIZE="${DASH_PHASE4_TIERC_FIXTURE_SIZE:-6000}"
  ITERATIONS="${DASH_PHASE4_TIERC_ITERATIONS:-5}"
  BENCH_RELEASE="${DASH_PHASE4_TIERC_BENCH_RELEASE:-false}"
  FAILOVER_MODE="${DASH_PHASE4_TIERC_FAILOVER_MODE:-both}"
  RETRIEVAL_WORKERS_LIST="${DASH_PHASE4_TIERC_RETRIEVAL_WORKERS_LIST:-2}"
  INGESTION_WORKERS_LIST="${DASH_PHASE4_TIERC_INGESTION_WORKERS_LIST:-2}"
  RETRIEVAL_CLIENTS="${DASH_PHASE4_TIERC_RETRIEVAL_CLIENTS:-4}"
  INGESTION_CLIENTS="${DASH_PHASE4_TIERC_INGESTION_CLIENTS:-4}"
  RETRIEVAL_REQUESTS_PER_WORKER="${DASH_PHASE4_TIERC_RETRIEVAL_REQUESTS_PER_WORKER:-4}"
  INGESTION_REQUESTS_PER_WORKER="${DASH_PHASE4_TIERC_INGESTION_REQUESTS_PER_WORKER:-4}"
  RETRIEVAL_WARMUP_REQUESTS="${DASH_PHASE4_TIERC_RETRIEVAL_WARMUP_REQUESTS:-1}"
  INGESTION_WARMUP_REQUESTS="${DASH_PHASE4_TIERC_INGESTION_WARMUP_REQUESTS:-1}"
  REBALANCE_PROBE_KEYS_COUNT="${DASH_PHASE4_TIERC_REBALANCE_PROBE_KEYS_COUNT:-96}"
  RUN_STORAGE_PROMOTION_BOUNDARY_GUARD="${DASH_PHASE4_TIERC_RUN_STORAGE_PROMOTION_BOUNDARY_GUARD:-false}"
fi

usage() {
  cat <<'USAGE'
Usage: scripts/phase4_tierc_rehearsal.sh [options]

Runs Phase 4 scale-proof with Tier-C orchestration:
  phase4_scale_proof + recovery drill + incident simulation + tier-c closure checklist.

Options:
  --mode MODE                            quick|staged preset mode (default: quick)
  --run-id ID                            Explicit run id
  --profile NAME                         Benchmark profile (mode default)
  --fixture-size N                       Benchmark fixture size (mode default)
  --iterations N                         Benchmark iterations (mode default)
  --bench-release true|false             Benchmark release mode (mode default)
  --target-shards N                      Rebalance target shards (default: 8)
  --min-shards-gate N                    Minimum shard gate (default: 8)
  --failover-mode MODE                   restart|no-restart|both (mode default)
  --retrieval-workers-list CSV           Retrieval worker list (mode default)
  --ingestion-workers-list CSV           Ingestion worker list (mode default)
  --retrieval-clients N                  Retrieval clients (mode default)
  --ingestion-clients N                  Ingestion clients (mode default)
  --retrieval-requests-per-worker N      Retrieval requests per worker (mode default)
  --ingestion-requests-per-worker N      Ingestion requests per worker (mode default)
  --retrieval-warmup-requests N          Retrieval warmup requests (mode default)
  --ingestion-warmup-requests N          Ingestion warmup requests (mode default)
  --rebalance-probe-keys-count N         Rebalance probe keys (mode default)
  --rebalance-require-moved-keys N       Rebalance moved key threshold (default: 1)
  --rebalance-max-wait-seconds N         Rebalance max wait (default: 90)
  --placement-reload-interval-ms N       Placement reload interval (default: 200)
  --failover-max-wait-seconds N          Failover max wait (default: 30)
  --run-recovery-drill true|false        Run recovery drill step (default: true)
  --run-incident-gate true|false         Run incident simulation gate step (default: true)
  --run-closure-checklist true|false     Run tier-c closure checklist (default: true)
  --run-scale-proof true|false           Run phase4_scale_proof step (default: true)
  --run-storage-promotion-boundary-guard true|false
                                         Enable promotion-boundary guard inside scale proof
  --summary-path PATH                    Existing scale-proof summary path to reuse when
                                         --run-scale-proof=false (or to force closure input)
  --recovery-max-rto-seconds N           Recovery drill max RTO (default: 120)
  --recovery-artifact-dir DIR            Output dir for generated recovery summary
                                         (default: docs/benchmarks/history/recovery)
  --recovery-artifact PATH               Existing recovery artifact path when recovery step is disabled
  --incident-summary-dir DIR             Output dir for incident gate summary artifacts
                                         (default: docs/benchmarks/history/incidents)
  --incident-artifact PATH               Existing incident artifact path when incident step is disabled
  --incident-failover-mode MODE          restart|no-restart|both (default: both)
  --incident-failover-max-wait-seconds N Incident failover drill wait (default: 30)
  --incident-auth-max-wait-seconds N     Incident auth drill wait (default: 30)
  --incident-recovery-max-rto-seconds N  Incident recovery drill max RTO (default: 120)
  -h, --help                             Show help

Any extra args after `--` are forwarded to `phase4_scale_proof.sh`.
USAGE
}

EXTRA_ARGS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode) MODE="$2"; shift 2 ;;
    --run-id) RUN_ID="$2"; shift 2 ;;
    --profile) PROFILE="$2"; shift 2 ;;
    --fixture-size) FIXTURE_SIZE="$2"; shift 2 ;;
    --iterations) ITERATIONS="$2"; shift 2 ;;
    --bench-release) BENCH_RELEASE="$2"; shift 2 ;;
    --target-shards) TARGET_SHARDS="$2"; shift 2 ;;
    --min-shards-gate) MIN_SHARDS_GATE="$2"; shift 2 ;;
    --failover-mode) FAILOVER_MODE="$2"; shift 2 ;;
    --retrieval-workers-list) RETRIEVAL_WORKERS_LIST="$2"; shift 2 ;;
    --ingestion-workers-list) INGESTION_WORKERS_LIST="$2"; shift 2 ;;
    --retrieval-clients) RETRIEVAL_CLIENTS="$2"; shift 2 ;;
    --ingestion-clients) INGESTION_CLIENTS="$2"; shift 2 ;;
    --retrieval-requests-per-worker) RETRIEVAL_REQUESTS_PER_WORKER="$2"; shift 2 ;;
    --ingestion-requests-per-worker) INGESTION_REQUESTS_PER_WORKER="$2"; shift 2 ;;
    --retrieval-warmup-requests) RETRIEVAL_WARMUP_REQUESTS="$2"; shift 2 ;;
    --ingestion-warmup-requests) INGESTION_WARMUP_REQUESTS="$2"; shift 2 ;;
    --rebalance-probe-keys-count) REBALANCE_PROBE_KEYS_COUNT="$2"; shift 2 ;;
    --rebalance-require-moved-keys) REBALANCE_REQUIRE_MOVED_KEYS="$2"; shift 2 ;;
    --rebalance-max-wait-seconds) REBALANCE_MAX_WAIT_SECONDS="$2"; shift 2 ;;
    --placement-reload-interval-ms) PLACEMENT_RELOAD_INTERVAL_MS="$2"; shift 2 ;;
    --failover-max-wait-seconds) FAILOVER_MAX_WAIT_SECONDS="$2"; shift 2 ;;
    --run-recovery-drill) RUN_RECOVERY_DRILL="$2"; shift 2 ;;
    --run-incident-gate) RUN_INCIDENT_GATE="$2"; shift 2 ;;
    --run-closure-checklist) RUN_CLOSURE_CHECKLIST="$2"; shift 2 ;;
    --run-scale-proof) RUN_SCALE_PROOF="$2"; shift 2 ;;
    --run-storage-promotion-boundary-guard) RUN_STORAGE_PROMOTION_BOUNDARY_GUARD="$2"; shift 2 ;;
    --summary-path) SUMMARY_PATH_OVERRIDE="$2"; shift 2 ;;
    --recovery-max-rto-seconds) RECOVERY_MAX_RTO_SECONDS="$2"; shift 2 ;;
    --recovery-artifact-dir) RECOVERY_ARTIFACT_DIR="$2"; shift 2 ;;
    --recovery-artifact) RECOVERY_ARTIFACT_OVERRIDE="$2"; shift 2 ;;
    --incident-summary-dir) INCIDENT_SUMMARY_DIR="$2"; shift 2 ;;
    --incident-artifact) INCIDENT_ARTIFACT_OVERRIDE="$2"; shift 2 ;;
    --incident-failover-mode) INCIDENT_FAILOVER_MODE="$2"; shift 2 ;;
    --incident-failover-max-wait-seconds) INCIDENT_FAILOVER_MAX_WAIT_SECONDS="$2"; shift 2 ;;
    --incident-auth-max-wait-seconds) INCIDENT_AUTH_MAX_WAIT_SECONDS="$2"; shift 2 ;;
    --incident-recovery-max-rto-seconds) INCIDENT_RECOVERY_MAX_RTO_SECONDS="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    --)
      shift
      EXTRA_ARGS=("$@")
      break
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

case "${MODE}" in
  quick|staged) ;;
  *)
    echo "--mode must be quick or staged" >&2
    exit 2
    ;;
esac

for bool_opt in "${RUN_RECOVERY_DRILL}" "${RUN_INCIDENT_GATE}" "${RUN_CLOSURE_CHECKLIST}" \
  "${RUN_SCALE_PROOF}" "${BENCH_RELEASE}" "${RUN_STORAGE_PROMOTION_BOUNDARY_GUARD}"; do
  case "${bool_opt}" in
    true|false) ;;
    *)
      echo "boolean options must be true or false" >&2
      exit 2
      ;;
  esac
done

case "${FAILOVER_MODE}" in
  restart|no-restart|both) ;;
  *)
    echo "--failover-mode must be restart|no-restart|both" >&2
    exit 2
    ;;
esac

case "${INCIDENT_FAILOVER_MODE}" in
  restart|no-restart|both) ;;
  *)
    echo "--incident-failover-mode must be restart|no-restart|both" >&2
    exit 2
    ;;
esac

for numeric in "${FIXTURE_SIZE}" "${ITERATIONS}" "${TARGET_SHARDS}" "${MIN_SHARDS_GATE}" \
  "${RETRIEVAL_CLIENTS}" "${INGESTION_CLIENTS}" "${RETRIEVAL_REQUESTS_PER_WORKER}" \
  "${INGESTION_REQUESTS_PER_WORKER}" "${RETRIEVAL_WARMUP_REQUESTS}" "${INGESTION_WARMUP_REQUESTS}" \
  "${REBALANCE_PROBE_KEYS_COUNT}" "${REBALANCE_REQUIRE_MOVED_KEYS}" "${REBALANCE_MAX_WAIT_SECONDS}" \
  "${PLACEMENT_RELOAD_INTERVAL_MS}" "${FAILOVER_MAX_WAIT_SECONDS}" "${RECOVERY_MAX_RTO_SECONDS}" \
  "${INCIDENT_FAILOVER_MAX_WAIT_SECONDS}" "${INCIDENT_AUTH_MAX_WAIT_SECONDS}" \
  "${INCIDENT_RECOVERY_MAX_RTO_SECONDS}"; do
  if [[ ! "${numeric}" =~ ^[0-9]+$ ]] || [[ "${numeric}" -eq 0 ]]; then
    echo "all numeric options must be positive integers" >&2
    exit 2
  fi
done

if [[ "${TARGET_SHARDS}" -lt 8 ]]; then
  echo "--target-shards must be >= 8 for Tier-C rehearsal" >&2
  exit 2
fi

if [[ "${MIN_SHARDS_GATE}" -lt 8 ]]; then
  echo "--min-shards-gate must be >= 8 for Tier-C rehearsal" >&2
  exit 2
fi

echo "[phase4-tierc] run_id=${RUN_ID}"
echo "[phase4-tierc] mode=${MODE}"
echo "[phase4-tierc] profile=${PROFILE} fixture_size=${FIXTURE_SIZE} iterations=${ITERATIONS}"
echo "[phase4-tierc] shard_target=${TARGET_SHARDS} shard_gate=${MIN_SHARDS_GATE}"
echo "[phase4-tierc] scale_proof=${RUN_SCALE_PROOF} recovery=${RUN_RECOVERY_DRILL} incident=${RUN_INCIDENT_GATE} closure=${RUN_CLOSURE_CHECKLIST}"
echo "[phase4-tierc] storage_promotion_boundary_guard=${RUN_STORAGE_PROMOTION_BOUNDARY_GUARD}"

cmd=(
  scripts/phase4_scale_proof.sh
  --run-id "${RUN_ID}"
  --profile "${PROFILE}"
  --fixture-size "${FIXTURE_SIZE}"
  --iterations "${ITERATIONS}"
  --bench-release "${BENCH_RELEASE}"
  --failover-mode "${FAILOVER_MODE}"
  --placement-reload-interval-ms "${PLACEMENT_RELOAD_INTERVAL_MS}"
  --failover-max-wait-seconds "${FAILOVER_MAX_WAIT_SECONDS}"
  --retrieval-workers-list "${RETRIEVAL_WORKERS_LIST}"
  --ingestion-workers-list "${INGESTION_WORKERS_LIST}"
  --retrieval-clients "${RETRIEVAL_CLIENTS}"
  --ingestion-clients "${INGESTION_CLIENTS}"
  --retrieval-requests-per-worker "${RETRIEVAL_REQUESTS_PER_WORKER}"
  --ingestion-requests-per-worker "${INGESTION_REQUESTS_PER_WORKER}"
  --retrieval-warmup-requests "${RETRIEVAL_WARMUP_REQUESTS}"
  --ingestion-warmup-requests "${INGESTION_WARMUP_REQUESTS}"
  --run-rebalance-drill true
  --run-storage-promotion-boundary-guard "${RUN_STORAGE_PROMOTION_BOUNDARY_GUARD}"
  --rebalance-target-shards "${TARGET_SHARDS}"
  --rebalance-min-shards-gate "${MIN_SHARDS_GATE}"
  --rebalance-probe-keys-count "${REBALANCE_PROBE_KEYS_COUNT}"
  --rebalance-require-moved-keys "${REBALANCE_REQUIRE_MOVED_KEYS}"
  --rebalance-max-wait-seconds "${REBALANCE_MAX_WAIT_SECONDS}"
)

SUMMARY_PATH="${SUMMARY_PATH_OVERRIDE}"

if [[ "${RUN_SCALE_PROOF}" == "true" ]]; then
  if [[ "${#EXTRA_ARGS[@]}" -gt 0 ]]; then
    cmd+=("${EXTRA_ARGS[@]}")
  fi

  "${cmd[@]}"

  if [[ -z "${SUMMARY_PATH}" ]]; then
    SUMMARY_PATH="docs/benchmarks/history/runs/${RUN_ID}.md"
  fi

  if [[ ! -f "${SUMMARY_PATH}" ]]; then
    echo "unable to find scale-proof summary at ${SUMMARY_PATH}" >&2
    echo "set --summary-path if phase4_scale_proof used a non-default summary dir" >&2
    exit 1
  fi
else
  if [[ -z "${SUMMARY_PATH}" ]]; then
    candidate_path="docs/benchmarks/history/runs/${RUN_ID}.md"
    if [[ -f "${candidate_path}" ]]; then
      SUMMARY_PATH="${candidate_path}"
    fi
  fi

  if [[ "${RUN_CLOSURE_CHECKLIST}" == "true" ]]; then
    if [[ -z "${SUMMARY_PATH}" ]]; then
      echo "--run-scale-proof=false with closure enabled requires --summary-path (or existing docs/benchmarks/history/runs/<run-id>.md)" >&2
      exit 2
    fi
    if [[ ! -f "${SUMMARY_PATH}" ]]; then
      echo "summary path not found: ${SUMMARY_PATH}" >&2
      exit 1
    fi
  fi
fi

if [[ -n "${SUMMARY_PATH}" ]]; then
  echo "[phase4-tierc] summary_path=${SUMMARY_PATH}"
fi

RECOVERY_ARTIFACT_PATH="${RECOVERY_ARTIFACT_OVERRIDE}"
INCIDENT_ARTIFACT_PATH="${INCIDENT_ARTIFACT_OVERRIDE}"

RECOVERY_TMP="$(mktemp)"
INCIDENT_TMP="$(mktemp)"
cleanup() {
  rm -f "${RECOVERY_TMP}" "${INCIDENT_TMP}"
}
trap cleanup EXIT

if [[ "${RUN_RECOVERY_DRILL}" == "true" ]]; then
  mkdir -p "${RECOVERY_ARTIFACT_DIR}"
  RECOVERY_ARTIFACT_PATH="${RECOVERY_ARTIFACT_DIR}/${RUN_ID}-recovery-drill.md"
  RECOVERY_WORK_DIR="${RECOVERY_ARTIFACT_DIR}/${RUN_ID}-recovery-workdir"
  if scripts/recovery_drill.sh \
    --work-dir "${RECOVERY_WORK_DIR}" \
    --max-rto-seconds "${RECOVERY_MAX_RTO_SECONDS}" \
    --keep-artifacts true >"${RECOVERY_TMP}" 2>&1; then
    :
  else
    cat "${RECOVERY_TMP}" >&2
    exit 1
  fi

  recovery_rto="$(awk -F= '/\[drill\] rto_seconds=/{print $2; exit}' "${RECOVERY_TMP}")"
  recovery_rpo="$(awk -F= '/\[drill\] rpo_claim_gap=/{print $2; exit}' "${RECOVERY_TMP}")"
  recovery_artifacts_dir="$(awk -F= '/\[drill\] artifacts_dir=/{print $2; exit}' "${RECOVERY_TMP}")"

  {
    echo "# DASH Tier-C Recovery Drill Artifact"
    echo
    echo "- run_id: ${RUN_ID}"
    echo "- generated_utc: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "- max_rto_seconds: ${RECOVERY_MAX_RTO_SECONDS}"
    echo "- measured_rto_seconds: ${recovery_rto:-unknown}"
    echo "- measured_rpo_claim_gap: ${recovery_rpo:-unknown}"
    echo "- artifacts_dir: ${recovery_artifacts_dir:-unknown}"
    echo "- work_dir: ${RECOVERY_WORK_DIR}"
    echo "- result: PASS"
    echo
    echo "## Command"
    echo
    echo "\`scripts/recovery_drill.sh --work-dir ${RECOVERY_WORK_DIR} --max-rto-seconds ${RECOVERY_MAX_RTO_SECONDS} --keep-artifacts true\`"
    echo
    echo "## Command Output"
    echo
    echo '```text'
    cat "${RECOVERY_TMP}"
    echo '```'
  } > "${RECOVERY_ARTIFACT_PATH}"
fi

if [[ "${RUN_INCIDENT_GATE}" == "true" ]]; then
  mkdir -p "${INCIDENT_SUMMARY_DIR}"
  if scripts/incident_simulation_gate.sh \
    --run-tag "${RUN_ID}-tierc" \
    --summary-dir "${INCIDENT_SUMMARY_DIR}" \
    --run-failover-drill true \
    --failover-mode "${INCIDENT_FAILOVER_MODE}" \
    --failover-max-wait-seconds "${INCIDENT_FAILOVER_MAX_WAIT_SECONDS}" \
    --run-auth-revocation-drill true \
    --auth-max-wait-seconds "${INCIDENT_AUTH_MAX_WAIT_SECONDS}" \
    --run-recovery-drill true \
    --recovery-max-rto-seconds "${INCIDENT_RECOVERY_MAX_RTO_SECONDS}" >"${INCIDENT_TMP}" 2>&1; then
    :
  else
    cat "${INCIDENT_TMP}" >&2
    exit 1
  fi

  INCIDENT_ARTIFACT_PATH="$(awk -F': ' '/\[incident-gate\] summary:/{print $2; exit}' "${INCIDENT_TMP}")"
  if [[ -z "${INCIDENT_ARTIFACT_PATH}" || ! -f "${INCIDENT_ARTIFACT_PATH}" ]]; then
    echo "failed to resolve incident summary artifact from incident gate output" >&2
    cat "${INCIDENT_TMP}" >&2
    exit 1
  fi
fi

if [[ "${RUN_CLOSURE_CHECKLIST}" == "true" ]]; then
  if [[ -z "${RECOVERY_ARTIFACT_PATH}" || ! -f "${RECOVERY_ARTIFACT_PATH}" ]]; then
    echo "tier-c closure requires a recovery artifact (missing: ${RECOVERY_ARTIFACT_PATH:-unset})" >&2
    exit 1
  fi
  if [[ -z "${INCIDENT_ARTIFACT_PATH}" || ! -f "${INCIDENT_ARTIFACT_PATH}" ]]; then
    echo "tier-c closure requires an incident artifact (missing: ${INCIDENT_ARTIFACT_PATH:-unset})" >&2
    exit 1
  fi

  scripts/phase4_closure_checklist.sh \
    --tier tier-c \
    --run-id "${RUN_ID}" \
    --summary-path "${SUMMARY_PATH}" \
    --min-shards "${MIN_SHARDS_GATE}" \
    --recovery-artifact "${RECOVERY_ARTIFACT_PATH}" \
    --incident-artifact "${INCIDENT_ARTIFACT_PATH}"
fi

echo "[phase4-tierc] recovery_artifact=${RECOVERY_ARTIFACT_PATH:-n/a}"
echo "[phase4-tierc] incident_artifact=${INCIDENT_ARTIFACT_PATH:-n/a}"
