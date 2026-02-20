#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

RUN_ID="${DASH_PHASE4_LONG_RUN_ID:-}"
PROFILE="${DASH_PHASE4_LONG_PROFILE:-xlarge}"
FIXTURE_SIZE="${DASH_PHASE4_LONG_FIXTURE_SIZE:-1000000}"
ITERATIONS="${DASH_PHASE4_LONG_ITERATIONS:-1}"
HISTORY_PATH="${DASH_PHASE4_LONG_HISTORY_PATH:-docs/benchmarks/history/benchmark-history.md}"
SUMMARY_DIR="${DASH_PHASE4_LONG_SUMMARY_DIR:-docs/benchmarks/history/runs}"
SCORECARD_DIR="${DASH_PHASE4_LONG_SCORECARD_DIR:-docs/benchmarks/scorecards}"
LOG_PATH="${DASH_PHASE4_LONG_LOG_PATH:-}"
HEARTBEAT_PATH="${DASH_PHASE4_LONG_HEARTBEAT_PATH:-}"
HEARTBEAT_INTERVAL_SECONDS="${DASH_PHASE4_LONG_HEARTBEAT_INTERVAL_SECONDS:-30}"
WAL_SCALE_CLAIMS="${DASH_PHASE4_LONG_WAL_SCALE_CLAIMS:-5000}"
BENCH_RELEASE="${DASH_PHASE4_LONG_BENCH_RELEASE:-true}"
RETRIEVAL_WORKERS_LIST="${DASH_PHASE4_LONG_RETRIEVAL_WORKERS_LIST:-4}"
RETRIEVAL_CLIENTS="${DASH_PHASE4_LONG_RETRIEVAL_CLIENTS:-24}"
RETRIEVAL_REQUESTS_PER_WORKER="${DASH_PHASE4_LONG_RETRIEVAL_REQUESTS_PER_WORKER:-20}"
RETRIEVAL_WARMUP_REQUESTS="${DASH_PHASE4_LONG_RETRIEVAL_WARMUP_REQUESTS:-10}"
INGESTION_WORKERS_LIST="${DASH_PHASE4_LONG_INGESTION_WORKERS_LIST:-4}"
INGESTION_CLIENTS="${DASH_PHASE4_LONG_INGESTION_CLIENTS:-24}"
INGESTION_REQUESTS_PER_WORKER="${DASH_PHASE4_LONG_INGESTION_REQUESTS_PER_WORKER:-20}"
INGESTION_WARMUP_REQUESTS="${DASH_PHASE4_LONG_INGESTION_WARMUP_REQUESTS:-10}"
INGEST_FRESHNESS_P95_SLO_MS="${DASH_PHASE4_LONG_INGEST_FRESHNESS_P95_SLO_MS:-5000}"
FAILOVER_MODE="${DASH_PHASE4_LONG_FAILOVER_MODE:-both}"
PLACEMENT_RELOAD_INTERVAL_MS="${DASH_PHASE4_LONG_PLACEMENT_RELOAD_INTERVAL_MS:-200}"
FAILOVER_MAX_WAIT_SECONDS="${DASH_PHASE4_LONG_FAILOVER_MAX_WAIT_SECONDS:-30}"

usage() {
  cat <<'USAGE'
Usage: scripts/phase4_long_soak.sh [options]

Required:
  --run-id ID                              Stable run id used for deterministic output paths

Options:
  --profile NAME                           benchmark profile (default: xlarge)
  --fixture-size N                         benchmark fixture size (default: 1000000)
  --iterations N                           benchmark iterations (default: 1)
  --history-path PATH                      benchmark history markdown path
  --summary-dir DIR                        summary output directory
  --scorecard-dir DIR                      scorecard output directory
  --log-path PATH                          explicit launcher log file path
  --heartbeat-path PATH                    explicit heartbeat file path
  --heartbeat-interval-seconds N           heartbeat write interval (default: 30)
  --wal-scale-claims N                     WAL scale-slice claims
  --bench-release true|false               run benchmark in release mode (default: true)
  --retrieval-workers-list CSV             retrieval worker list
  --retrieval-clients N                    retrieval benchmark clients
  --retrieval-requests-per-worker N        retrieval requests per worker
  --retrieval-warmup-requests N            retrieval warmup requests
  --ingestion-workers-list CSV             ingestion worker list
  --ingestion-clients N                    ingestion benchmark clients
  --ingestion-requests-per-worker N        ingestion requests per worker
  --ingestion-warmup-requests N            ingestion warmup requests
  --ingest-freshness-p95-slo-ms N          ingestion p95 freshness threshold
  --failover-mode MODE                     restart|no-restart|both
  --placement-reload-interval-ms N         placement reload interval
  --failover-max-wait-seconds N            failover max wait seconds
  -h, --help                               Show help
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --run-id) RUN_ID="$2"; shift 2 ;;
    --profile) PROFILE="$2"; shift 2 ;;
    --fixture-size) FIXTURE_SIZE="$2"; shift 2 ;;
    --iterations) ITERATIONS="$2"; shift 2 ;;
    --history-path) HISTORY_PATH="$2"; shift 2 ;;
    --summary-dir) SUMMARY_DIR="$2"; shift 2 ;;
    --scorecard-dir) SCORECARD_DIR="$2"; shift 2 ;;
    --log-path) LOG_PATH="$2"; shift 2 ;;
    --heartbeat-path) HEARTBEAT_PATH="$2"; shift 2 ;;
    --heartbeat-interval-seconds) HEARTBEAT_INTERVAL_SECONDS="$2"; shift 2 ;;
    --wal-scale-claims) WAL_SCALE_CLAIMS="$2"; shift 2 ;;
    --bench-release) BENCH_RELEASE="$2"; shift 2 ;;
    --retrieval-workers-list) RETRIEVAL_WORKERS_LIST="$2"; shift 2 ;;
    --retrieval-clients) RETRIEVAL_CLIENTS="$2"; shift 2 ;;
    --retrieval-requests-per-worker) RETRIEVAL_REQUESTS_PER_WORKER="$2"; shift 2 ;;
    --retrieval-warmup-requests) RETRIEVAL_WARMUP_REQUESTS="$2"; shift 2 ;;
    --ingestion-workers-list) INGESTION_WORKERS_LIST="$2"; shift 2 ;;
    --ingestion-clients) INGESTION_CLIENTS="$2"; shift 2 ;;
    --ingestion-requests-per-worker) INGESTION_REQUESTS_PER_WORKER="$2"; shift 2 ;;
    --ingestion-warmup-requests) INGESTION_WARMUP_REQUESTS="$2"; shift 2 ;;
    --ingest-freshness-p95-slo-ms) INGEST_FRESHNESS_P95_SLO_MS="$2"; shift 2 ;;
    --failover-mode) FAILOVER_MODE="$2"; shift 2 ;;
    --placement-reload-interval-ms) PLACEMENT_RELOAD_INTERVAL_MS="$2"; shift 2 ;;
    --failover-max-wait-seconds) FAILOVER_MAX_WAIT_SECONDS="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

if [[ -z "${RUN_ID}" ]]; then
  echo "--run-id is required" >&2
  exit 2
fi

for required in "${FIXTURE_SIZE}" "${ITERATIONS}" "${HEARTBEAT_INTERVAL_SECONDS}" \
  "${WAL_SCALE_CLAIMS}" "${RETRIEVAL_CLIENTS}" "${RETRIEVAL_REQUESTS_PER_WORKER}" \
  "${RETRIEVAL_WARMUP_REQUESTS}" "${INGESTION_CLIENTS}" "${INGESTION_REQUESTS_PER_WORKER}" \
  "${INGESTION_WARMUP_REQUESTS}" "${INGEST_FRESHNESS_P95_SLO_MS}" \
  "${PLACEMENT_RELOAD_INTERVAL_MS}" "${FAILOVER_MAX_WAIT_SECONDS}"; do
  if [[ ! "${required}" =~ ^[0-9]+$ ]] || [[ "${required}" -eq 0 ]]; then
    echo "all numeric options must be positive integers" >&2
    exit 2
  fi
done

case "${BENCH_RELEASE}" in
  true|false) ;;
  *)
    echo "--bench-release must be true or false" >&2
    exit 2
    ;;
esac

case "${FAILOVER_MODE}" in
  restart|no-restart|both) ;;
  *)
    echo "--failover-mode must be restart|no-restart|both" >&2
    exit 2
    ;;
esac

mkdir -p "$(dirname "${HISTORY_PATH}")" "${SUMMARY_DIR}" "${SCORECARD_DIR}"
if [[ -z "${LOG_PATH}" ]]; then
  LOG_PATH="${SUMMARY_DIR}/${RUN_ID}.log"
fi
if [[ -z "${HEARTBEAT_PATH}" ]]; then
  HEARTBEAT_PATH="${SUMMARY_DIR}/${RUN_ID}.heartbeat"
fi

START_EPOCH="$(date +%s)"
SUMMARY_PATH="${SUMMARY_DIR}/${RUN_ID}.md"
SCORECARD_PATH="${SCORECARD_DIR}/${RUN_ID}-${PROFILE}-fixture-${FIXTURE_SIZE}.md"

heartbeat_loop() {
  while true; do
    local now_epoch elapsed
    now_epoch="$(date +%s)"
    elapsed="$((now_epoch - START_EPOCH))"
    cat > "${HEARTBEAT_PATH}" <<EOF
run_id=${RUN_ID}
status=running
heartbeat_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)
elapsed_seconds=${elapsed}
summary_path=${SUMMARY_PATH}
scorecard_path=${SCORECARD_PATH}
log_path=${LOG_PATH}
EOF
    sleep "${HEARTBEAT_INTERVAL_SECONDS}"
  done
}

heartbeat_loop &
HEARTBEAT_PID=$!

cleanup() {
  if kill -0 "${HEARTBEAT_PID}" >/dev/null 2>&1; then
    kill "${HEARTBEAT_PID}" >/dev/null 2>&1 || true
    wait "${HEARTBEAT_PID}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

{
  echo "[phase4-long-soak] run_id=${RUN_ID}"
  echo "[phase4-long-soak] start_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "[phase4-long-soak] summary_path=${SUMMARY_PATH}"
  echo "[phase4-long-soak] scorecard_path=${SCORECARD_PATH}"
  echo "[phase4-long-soak] heartbeat_path=${HEARTBEAT_PATH}"
  echo "[phase4-long-soak] invoking phase4_scale_proof"
} | tee "${LOG_PATH}"

set +e
scripts/phase4_scale_proof.sh \
  --run-id "${RUN_ID}" \
  --run-tag "long-soak" \
  --profile "${PROFILE}" \
  --fixture-size "${FIXTURE_SIZE}" \
  --iterations "${ITERATIONS}" \
  --history-path "${HISTORY_PATH}" \
  --summary-dir "${SUMMARY_DIR}" \
  --scorecard-dir "${SCORECARD_DIR}" \
  --wal-scale-claims "${WAL_SCALE_CLAIMS}" \
  --bench-release "${BENCH_RELEASE}" \
  --retrieval-workers-list "${RETRIEVAL_WORKERS_LIST}" \
  --retrieval-clients "${RETRIEVAL_CLIENTS}" \
  --retrieval-requests-per-worker "${RETRIEVAL_REQUESTS_PER_WORKER}" \
  --retrieval-warmup-requests "${RETRIEVAL_WARMUP_REQUESTS}" \
  --ingestion-workers-list "${INGESTION_WORKERS_LIST}" \
  --ingestion-clients "${INGESTION_CLIENTS}" \
  --ingestion-requests-per-worker "${INGESTION_REQUESTS_PER_WORKER}" \
  --ingestion-warmup-requests "${INGESTION_WARMUP_REQUESTS}" \
  --ingest-freshness-p95-slo-ms "${INGEST_FRESHNESS_P95_SLO_MS}" \
  --failover-mode "${FAILOVER_MODE}" \
  --placement-reload-interval-ms "${PLACEMENT_RELOAD_INTERVAL_MS}" \
  --failover-max-wait-seconds "${FAILOVER_MAX_WAIT_SECONDS}" \
  >> "${LOG_PATH}" 2>&1
status=$?
set -e

end_utc="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
elapsed_total="$(( $(date +%s) - START_EPOCH ))"
final_status="success"
if [[ ${status} -ne 0 ]]; then
  final_status="failed"
fi

cat > "${HEARTBEAT_PATH}" <<EOF
run_id=${RUN_ID}
status=${final_status}
heartbeat_utc=${end_utc}
elapsed_seconds=${elapsed_total}
summary_path=${SUMMARY_PATH}
scorecard_path=${SCORECARD_PATH}
log_path=${LOG_PATH}
exit_code=${status}
EOF

echo "[phase4-long-soak] end_utc=${end_utc}" | tee -a "${LOG_PATH}"
echo "[phase4-long-soak] status=${final_status} exit_code=${status}" | tee -a "${LOG_PATH}"
echo "[phase4-long-soak] summary_path=${SUMMARY_PATH}" | tee -a "${LOG_PATH}"
echo "[phase4-long-soak] heartbeat_path=${HEARTBEAT_PATH}" | tee -a "${LOG_PATH}"

exit "${status}"
