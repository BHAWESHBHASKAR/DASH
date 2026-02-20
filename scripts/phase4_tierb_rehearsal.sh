#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

MODE="${DASH_PHASE4_TIERB_MODE:-quick}"

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

RUN_ID="${DASH_PHASE4_TIERB_RUN_ID:-$(date -u +%Y%m%d-%H%M%S)-phase4-tierb}"
TARGET_SHARDS="${DASH_PHASE4_TIERB_TARGET_SHARDS:-8}"
MIN_SHARDS_GATE="${DASH_PHASE4_TIERB_MIN_SHARDS_GATE:-8}"
REBALANCE_REQUIRE_MOVED_KEYS="${DASH_PHASE4_TIERB_REBALANCE_REQUIRE_MOVED_KEYS:-1}"
REBALANCE_MAX_WAIT_SECONDS="${DASH_PHASE4_TIERB_REBALANCE_MAX_WAIT_SECONDS:-60}"
PLACEMENT_RELOAD_INTERVAL_MS="${DASH_PHASE4_TIERB_PLACEMENT_RELOAD_INTERVAL_MS:-200}"
FAILOVER_MAX_WAIT_SECONDS="${DASH_PHASE4_TIERB_FAILOVER_MAX_WAIT_SECONDS:-30}"

if [[ "${MODE}" == "staged" ]]; then
  PROFILE="${DASH_PHASE4_TIERB_PROFILE:-xlarge}"
  FIXTURE_SIZE="${DASH_PHASE4_TIERB_FIXTURE_SIZE:-150000}"
  ITERATIONS="${DASH_PHASE4_TIERB_ITERATIONS:-1}"
  BENCH_RELEASE="${DASH_PHASE4_TIERB_BENCH_RELEASE:-true}"
  FAILOVER_MODE="${DASH_PHASE4_TIERB_FAILOVER_MODE:-both}"
  RETRIEVAL_WORKERS_LIST="${DASH_PHASE4_TIERB_RETRIEVAL_WORKERS_LIST:-4}"
  INGESTION_WORKERS_LIST="${DASH_PHASE4_TIERB_INGESTION_WORKERS_LIST:-4}"
  RETRIEVAL_CLIENTS="${DASH_PHASE4_TIERB_RETRIEVAL_CLIENTS:-16}"
  INGESTION_CLIENTS="${DASH_PHASE4_TIERB_INGESTION_CLIENTS:-16}"
  RETRIEVAL_REQUESTS_PER_WORKER="${DASH_PHASE4_TIERB_RETRIEVAL_REQUESTS_PER_WORKER:-12}"
  INGESTION_REQUESTS_PER_WORKER="${DASH_PHASE4_TIERB_INGESTION_REQUESTS_PER_WORKER:-12}"
  RETRIEVAL_WARMUP_REQUESTS="${DASH_PHASE4_TIERB_RETRIEVAL_WARMUP_REQUESTS:-4}"
  INGESTION_WARMUP_REQUESTS="${DASH_PHASE4_TIERB_INGESTION_WARMUP_REQUESTS:-4}"
  REBALANCE_PROBE_KEYS_COUNT="${DASH_PHASE4_TIERB_REBALANCE_PROBE_KEYS_COUNT:-256}"
else
  PROFILE="${DASH_PHASE4_TIERB_PROFILE:-smoke}"
  FIXTURE_SIZE="${DASH_PHASE4_TIERB_FIXTURE_SIZE:-4000}"
  ITERATIONS="${DASH_PHASE4_TIERB_ITERATIONS:-1}"
  BENCH_RELEASE="${DASH_PHASE4_TIERB_BENCH_RELEASE:-false}"
  FAILOVER_MODE="${DASH_PHASE4_TIERB_FAILOVER_MODE:-no-restart}"
  RETRIEVAL_WORKERS_LIST="${DASH_PHASE4_TIERB_RETRIEVAL_WORKERS_LIST:-2}"
  INGESTION_WORKERS_LIST="${DASH_PHASE4_TIERB_INGESTION_WORKERS_LIST:-2}"
  RETRIEVAL_CLIENTS="${DASH_PHASE4_TIERB_RETRIEVAL_CLIENTS:-2}"
  INGESTION_CLIENTS="${DASH_PHASE4_TIERB_INGESTION_CLIENTS:-2}"
  RETRIEVAL_REQUESTS_PER_WORKER="${DASH_PHASE4_TIERB_RETRIEVAL_REQUESTS_PER_WORKER:-2}"
  INGESTION_REQUESTS_PER_WORKER="${DASH_PHASE4_TIERB_INGESTION_REQUESTS_PER_WORKER:-2}"
  RETRIEVAL_WARMUP_REQUESTS="${DASH_PHASE4_TIERB_RETRIEVAL_WARMUP_REQUESTS:-1}"
  INGESTION_WARMUP_REQUESTS="${DASH_PHASE4_TIERB_INGESTION_WARMUP_REQUESTS:-1}"
  REBALANCE_PROBE_KEYS_COUNT="${DASH_PHASE4_TIERB_REBALANCE_PROBE_KEYS_COUNT:-64}"
fi

usage() {
  cat <<'USAGE'
Usage: scripts/phase4_tierb_rehearsal.sh [options]

Runs Phase 4 scale-proof with Tier-B rebalance defaults (8-shard gate).

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
  --rebalance-max-wait-seconds N         Rebalance max wait (default: 60)
  --placement-reload-interval-ms N       Placement reload interval (default: 200)
  --failover-max-wait-seconds N          Failover max wait (default: 30)
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

for numeric in "${FIXTURE_SIZE}" "${ITERATIONS}" "${TARGET_SHARDS}" "${MIN_SHARDS_GATE}" \
  "${RETRIEVAL_CLIENTS}" "${INGESTION_CLIENTS}" "${RETRIEVAL_REQUESTS_PER_WORKER}" \
  "${INGESTION_REQUESTS_PER_WORKER}" "${RETRIEVAL_WARMUP_REQUESTS}" "${INGESTION_WARMUP_REQUESTS}" \
  "${REBALANCE_PROBE_KEYS_COUNT}" "${REBALANCE_REQUIRE_MOVED_KEYS}" "${REBALANCE_MAX_WAIT_SECONDS}" \
  "${PLACEMENT_RELOAD_INTERVAL_MS}" "${FAILOVER_MAX_WAIT_SECONDS}"; do
  if [[ ! "${numeric}" =~ ^[0-9]+$ ]] || [[ "${numeric}" -eq 0 ]]; then
    echo "all numeric options must be positive integers" >&2
    exit 2
  fi
done

if [[ "${TARGET_SHARDS}" -lt 8 ]]; then
  echo "--target-shards must be >= 8 for Tier-B rehearsal" >&2
  exit 2
fi

if [[ "${MIN_SHARDS_GATE}" -lt 8 ]]; then
  echo "--min-shards-gate must be >= 8 for Tier-B rehearsal" >&2
  exit 2
fi

echo "[phase4-tierb] run_id=${RUN_ID}"
echo "[phase4-tierb] mode=${MODE}"
echo "[phase4-tierb] profile=${PROFILE} fixture_size=${FIXTURE_SIZE} iterations=${ITERATIONS}"
echo "[phase4-tierb] shard_target=${TARGET_SHARDS} shard_gate=${MIN_SHARDS_GATE}"

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
  --rebalance-target-shards "${TARGET_SHARDS}"
  --rebalance-min-shards-gate "${MIN_SHARDS_GATE}"
  --rebalance-probe-keys-count "${REBALANCE_PROBE_KEYS_COUNT}"
  --rebalance-require-moved-keys "${REBALANCE_REQUIRE_MOVED_KEYS}"
  --rebalance-max-wait-seconds "${REBALANCE_MAX_WAIT_SECONDS}"
)

if [[ "${#EXTRA_ARGS[@]}" -gt 0 ]]; then
  cmd+=("${EXTRA_ARGS[@]}")
fi

"${cmd[@]}"
