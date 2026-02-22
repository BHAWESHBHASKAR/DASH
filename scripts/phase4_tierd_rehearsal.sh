#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

MODE="${DASH_PHASE4_TIERD_MODE:-quick}"

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

RUN_ID="${DASH_PHASE4_TIERD_RUN_ID:-$(date -u +%Y%m%d-%H%M%S)-phase4-tierd}"
RUN_RECOVERY_DRILL="${DASH_PHASE4_TIERD_RUN_RECOVERY_DRILL:-true}"
RUN_INCIDENT_GATE="${DASH_PHASE4_TIERD_RUN_INCIDENT_GATE:-true}"
RUN_CLOSURE_CHECKLIST="${DASH_PHASE4_TIERD_RUN_CLOSURE_CHECKLIST:-true}"
RUN_SCALE_PROOF="${DASH_PHASE4_TIERD_RUN_SCALE_PROOF:-true}"
RUN_STORAGE_PROMOTION_BOUNDARY_GUARD="${DASH_PHASE4_TIERD_RUN_STORAGE_PROMOTION_BOUNDARY_GUARD:-false}"
SUMMARY_PATH="${DASH_PHASE4_TIERD_SUMMARY_PATH:-}"
RECOVERY_ARTIFACT="${DASH_PHASE4_TIERD_RECOVERY_ARTIFACT:-}"
INCIDENT_ARTIFACT="${DASH_PHASE4_TIERD_INCIDENT_ARTIFACT:-}"

if [[ "${MODE}" == "staged" ]]; then
  PROFILE="${DASH_PHASE4_TIERD_PROFILE:-xxlarge}"
  FIXTURE_SIZE="${DASH_PHASE4_TIERD_FIXTURE_SIZE:-1000000}"
  ITERATIONS="${DASH_PHASE4_TIERD_ITERATIONS:-5}"
  BENCH_RELEASE="${DASH_PHASE4_TIERD_BENCH_RELEASE:-true}"
  TARGET_SHARDS="${DASH_PHASE4_TIERD_TARGET_SHARDS:-12}"
  MIN_SHARDS_GATE="${DASH_PHASE4_TIERD_MIN_SHARDS_GATE:-12}"
  FAILOVER_MODE="${DASH_PHASE4_TIERD_FAILOVER_MODE:-both}"
  RETRIEVAL_WORKERS_LIST="${DASH_PHASE4_TIERD_RETRIEVAL_WORKERS_LIST:-6}"
  INGESTION_WORKERS_LIST="${DASH_PHASE4_TIERD_INGESTION_WORKERS_LIST:-6}"
  RETRIEVAL_CLIENTS="${DASH_PHASE4_TIERD_RETRIEVAL_CLIENTS:-24}"
  INGESTION_CLIENTS="${DASH_PHASE4_TIERD_INGESTION_CLIENTS:-24}"
  RETRIEVAL_REQUESTS_PER_WORKER="${DASH_PHASE4_TIERD_RETRIEVAL_REQUESTS_PER_WORKER:-16}"
  INGESTION_REQUESTS_PER_WORKER="${DASH_PHASE4_TIERD_INGESTION_REQUESTS_PER_WORKER:-16}"
  RETRIEVAL_WARMUP_REQUESTS="${DASH_PHASE4_TIERD_RETRIEVAL_WARMUP_REQUESTS:-4}"
  INGESTION_WARMUP_REQUESTS="${DASH_PHASE4_TIERD_INGESTION_WARMUP_REQUESTS:-4}"
  REBALANCE_PROBE_KEYS_COUNT="${DASH_PHASE4_TIERD_REBALANCE_PROBE_KEYS_COUNT:-512}"
  REBALANCE_MAX_WAIT_SECONDS="${DASH_PHASE4_TIERD_REBALANCE_MAX_WAIT_SECONDS:-180}"
  RUN_STORAGE_PROMOTION_BOUNDARY_GUARD="${DASH_PHASE4_TIERD_RUN_STORAGE_PROMOTION_BOUNDARY_GUARD:-true}"
else
  PROFILE="${DASH_PHASE4_TIERD_PROFILE:-smoke}"
  FIXTURE_SIZE="${DASH_PHASE4_TIERD_FIXTURE_SIZE:-8000}"
  ITERATIONS="${DASH_PHASE4_TIERD_ITERATIONS:-5}"
  BENCH_RELEASE="${DASH_PHASE4_TIERD_BENCH_RELEASE:-false}"
  TARGET_SHARDS="${DASH_PHASE4_TIERD_TARGET_SHARDS:-8}"
  MIN_SHARDS_GATE="${DASH_PHASE4_TIERD_MIN_SHARDS_GATE:-8}"
  FAILOVER_MODE="${DASH_PHASE4_TIERD_FAILOVER_MODE:-both}"
  RETRIEVAL_WORKERS_LIST="${DASH_PHASE4_TIERD_RETRIEVAL_WORKERS_LIST:-2}"
  INGESTION_WORKERS_LIST="${DASH_PHASE4_TIERD_INGESTION_WORKERS_LIST:-2}"
  RETRIEVAL_CLIENTS="${DASH_PHASE4_TIERD_RETRIEVAL_CLIENTS:-4}"
  INGESTION_CLIENTS="${DASH_PHASE4_TIERD_INGESTION_CLIENTS:-4}"
  RETRIEVAL_REQUESTS_PER_WORKER="${DASH_PHASE4_TIERD_RETRIEVAL_REQUESTS_PER_WORKER:-4}"
  INGESTION_REQUESTS_PER_WORKER="${DASH_PHASE4_TIERD_INGESTION_REQUESTS_PER_WORKER:-4}"
  RETRIEVAL_WARMUP_REQUESTS="${DASH_PHASE4_TIERD_RETRIEVAL_WARMUP_REQUESTS:-1}"
  INGESTION_WARMUP_REQUESTS="${DASH_PHASE4_TIERD_INGESTION_WARMUP_REQUESTS:-1}"
  REBALANCE_PROBE_KEYS_COUNT="${DASH_PHASE4_TIERD_REBALANCE_PROBE_KEYS_COUNT:-128}"
  REBALANCE_MAX_WAIT_SECONDS="${DASH_PHASE4_TIERD_REBALANCE_MAX_WAIT_SECONDS:-90}"
  RUN_STORAGE_PROMOTION_BOUNDARY_GUARD="${DASH_PHASE4_TIERD_RUN_STORAGE_PROMOTION_BOUNDARY_GUARD:-false}"
fi

usage() {
  cat <<'USAGE'
Usage: scripts/phase4_tierd_rehearsal.sh [options]

Runs Phase 4 Tier-D orchestration by invoking phase4_tierc_rehearsal with
post-xlarge defaults (mode=staged -> profile=xxlarge, fixture=1,000,000).

Options:
  --mode MODE                            quick|staged preset mode (default: quick)
  --run-id ID                            Explicit run id
  --profile NAME                         Benchmark profile (mode default)
  --fixture-size N                       Benchmark fixture size (mode default)
  --iterations N                         Benchmark iterations (mode default)
  --bench-release true|false             Benchmark release mode (mode default)
  --target-shards N                      Rebalance target shards (mode default)
  --min-shards-gate N                    Minimum shard gate (mode default)
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
  --rebalance-max-wait-seconds N         Rebalance max wait (mode default)
  --run-recovery-drill true|false        Run recovery drill step (default: true)
  --run-incident-gate true|false         Run incident simulation gate step (default: true)
  --run-closure-checklist true|false     Run tier-c closure checklist (default: true)
  --run-scale-proof true|false           Run phase4_scale_proof step (default: true)
  --run-storage-promotion-boundary-guard true|false
                                         Enable promotion-boundary guard inside scale proof
  --summary-path PATH                    Existing scale-proof summary path
  --recovery-artifact PATH               Existing recovery artifact path
  --incident-artifact PATH               Existing incident artifact path
  -h, --help                             Show help

Any extra args after `--` are forwarded to `phase4_tierc_rehearsal.sh`.
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
    --rebalance-max-wait-seconds) REBALANCE_MAX_WAIT_SECONDS="$2"; shift 2 ;;
    --run-recovery-drill) RUN_RECOVERY_DRILL="$2"; shift 2 ;;
    --run-incident-gate) RUN_INCIDENT_GATE="$2"; shift 2 ;;
    --run-closure-checklist) RUN_CLOSURE_CHECKLIST="$2"; shift 2 ;;
    --run-scale-proof) RUN_SCALE_PROOF="$2"; shift 2 ;;
    --run-storage-promotion-boundary-guard) RUN_STORAGE_PROMOTION_BOUNDARY_GUARD="$2"; shift 2 ;;
    --summary-path) SUMMARY_PATH="$2"; shift 2 ;;
    --recovery-artifact) RECOVERY_ARTIFACT="$2"; shift 2 ;;
    --incident-artifact) INCIDENT_ARTIFACT="$2"; shift 2 ;;
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
  "${RUN_SCALE_PROOF}" "${RUN_STORAGE_PROMOTION_BOUNDARY_GUARD}" "${BENCH_RELEASE}"; do
  case "${bool_opt}" in
    true|false) ;;
    *)
      echo "boolean options must be true or false" >&2
      exit 2
      ;;
  esac
done

echo "[phase4-tierd] run_id=${RUN_ID}"
echo "[phase4-tierd] mode=${MODE}"
echo "[phase4-tierd] profile=${PROFILE} fixture_size=${FIXTURE_SIZE} iterations=${ITERATIONS}"
echo "[phase4-tierd] shard_target=${TARGET_SHARDS} shard_gate=${MIN_SHARDS_GATE}"
echo "[phase4-tierd] storage_promotion_boundary_guard=${RUN_STORAGE_PROMOTION_BOUNDARY_GUARD}"

cmd=(
  scripts/phase4_tierc_rehearsal.sh
  --mode "${MODE}"
  --run-id "${RUN_ID}"
  --profile "${PROFILE}"
  --fixture-size "${FIXTURE_SIZE}"
  --iterations "${ITERATIONS}"
  --bench-release "${BENCH_RELEASE}"
  --target-shards "${TARGET_SHARDS}"
  --min-shards-gate "${MIN_SHARDS_GATE}"
  --failover-mode "${FAILOVER_MODE}"
  --retrieval-workers-list "${RETRIEVAL_WORKERS_LIST}"
  --ingestion-workers-list "${INGESTION_WORKERS_LIST}"
  --retrieval-clients "${RETRIEVAL_CLIENTS}"
  --ingestion-clients "${INGESTION_CLIENTS}"
  --retrieval-requests-per-worker "${RETRIEVAL_REQUESTS_PER_WORKER}"
  --ingestion-requests-per-worker "${INGESTION_REQUESTS_PER_WORKER}"
  --retrieval-warmup-requests "${RETRIEVAL_WARMUP_REQUESTS}"
  --ingestion-warmup-requests "${INGESTION_WARMUP_REQUESTS}"
  --rebalance-probe-keys-count "${REBALANCE_PROBE_KEYS_COUNT}"
  --rebalance-max-wait-seconds "${REBALANCE_MAX_WAIT_SECONDS}"
  --run-recovery-drill "${RUN_RECOVERY_DRILL}"
  --run-incident-gate "${RUN_INCIDENT_GATE}"
  --run-closure-checklist "${RUN_CLOSURE_CHECKLIST}"
  --run-scale-proof "${RUN_SCALE_PROOF}"
  --run-storage-promotion-boundary-guard "${RUN_STORAGE_PROMOTION_BOUNDARY_GUARD}"
)

if [[ -n "${SUMMARY_PATH}" ]]; then
  cmd+=(--summary-path "${SUMMARY_PATH}")
fi
if [[ -n "${RECOVERY_ARTIFACT}" ]]; then
  cmd+=(--recovery-artifact "${RECOVERY_ARTIFACT}")
fi
if [[ -n "${INCIDENT_ARTIFACT}" ]]; then
  cmd+=(--incident-artifact "${INCIDENT_ARTIFACT}")
fi
if [[ "${#EXTRA_ARGS[@]}" -gt 0 ]]; then
  cmd+=(-- "${EXTRA_ARGS[@]}")
fi

"${cmd[@]}"
