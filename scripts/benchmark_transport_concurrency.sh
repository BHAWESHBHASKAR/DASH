#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

BIND_ADDR="${DASH_CONCURRENCY_BIND_ADDR:-127.0.0.1:18080}"
TARGET="${DASH_CONCURRENCY_TARGET:-retrieval}"
WORKERS_LIST="${DASH_CONCURRENCY_WORKERS_LIST:-1,2,4,8}"
CONCURRENCY="${DASH_CONCURRENCY_CLIENTS:-32}"
REQUESTS_PER_WORKER="${DASH_CONCURRENCY_REQUESTS_PER_WORKER:-40}"
WARMUP_REQUESTS="${DASH_CONCURRENCY_WARMUP_REQUESTS:-10}"
CONNECT_TIMEOUT_MS="${DASH_CONCURRENCY_CONNECT_TIMEOUT_MS:-2000}"
READ_TIMEOUT_MS="${DASH_CONCURRENCY_READ_TIMEOUT_MS:-5000}"
OUTPUT_DIR="${DASH_CONCURRENCY_OUTPUT_DIR:-docs/benchmarks/history/concurrency}"
RUN_TAG="${DASH_CONCURRENCY_RUN_TAG:-transport-worker-pool}"
INGEST_WAL_PATH="${DASH_CONCURRENCY_INGEST_WAL_PATH:-}"
INGEST_WAL_SYNC_EVERY_RECORDS="${DASH_CONCURRENCY_INGEST_WAL_SYNC_EVERY_RECORDS:-1}"
INGEST_WAL_APPEND_BUFFER_RECORDS="${DASH_CONCURRENCY_INGEST_WAL_APPEND_BUFFER_RECORDS:-1}"
INGEST_WAL_SYNC_INTERVAL_MS="${DASH_CONCURRENCY_INGEST_WAL_SYNC_INTERVAL_MS:-}"
INGEST_WAL_ASYNC_FLUSH_INTERVAL_MS="${DASH_CONCURRENCY_INGEST_WAL_ASYNC_FLUSH_INTERVAL_MS:-}"
INGEST_WAL_BACKGROUND_FLUSH_ONLY="${DASH_CONCURRENCY_INGEST_WAL_BACKGROUND_FLUSH_ONLY:-false}"
INGEST_ALLOW_UNSAFE_WAL_DURABILITY="${DASH_CONCURRENCY_INGEST_ALLOW_UNSAFE_WAL_DURABILITY:-false}"
INGEST_BODY_TEMPLATE='{"claim":{"claim_id":"bench-claim-%WORKER%-%REQUEST%-%EPOCH_MS%","tenant_id":"bench-tenant","canonical_text":"Load benchmark claim","confidence":0.9},"evidence":[{"evidence_id":"bench-evidence-%WORKER%-%REQUEST%-%EPOCH_MS%","claim_id":"bench-claim-%WORKER%-%REQUEST%-%EPOCH_MS%","source_id":"bench://source-%WORKER%-%REQUEST%","stance":"supports","source_quality":0.8}]}'

usage() {
  cat <<'USAGE'
Usage: scripts/benchmark_transport_concurrency.sh [options]

Options:
  --target retrieval|ingestion  benchmark target service
  --bind-addr HOST:PORT         service bind address for benchmark runs
  --workers-list CSV            worker pool sizes, e.g. 1,2,4,8
  --clients N                   concurrent benchmark clients
  --requests-per-worker N       requests per client thread
  --warmup-requests N           warmup requests before measurement
  --connect-timeout-ms N        socket connect timeout
  --read-timeout-ms N           socket read/write timeout
  --ingest-wal-path PATH        WAL path used for ingestion target (persistent mode)
  --ingest-wal-sync-every-records N
                                ingest WAL sync batch threshold (1=strict)
  --ingest-wal-append-buffer-records N
                                ingest WAL append-buffer threshold
  --ingest-wal-sync-interval-ms N|off
                                ingest WAL max sync interval; use off to disable
  --ingest-wal-async-flush-interval-ms N|off
                                async WAL flush worker interval; use off to disable
  --ingest-wal-background-flush-only true|false
                                force background-only WAL flush mode for ingestion benchmarks
  --ingest-allow-unsafe-wal-durability true|false
                                set unsafe WAL durability override for ingestion service benchmark runs
  --output-dir DIR              markdown output directory
  --run-tag TAG                 suffix for output filename
  -h, --help                    show help
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --target)
      TARGET="$2"
      shift 2
      ;;
    --bind-addr)
      BIND_ADDR="$2"
      shift 2
      ;;
    --workers-list)
      WORKERS_LIST="$2"
      shift 2
      ;;
    --clients)
      CONCURRENCY="$2"
      shift 2
      ;;
    --requests-per-worker)
      REQUESTS_PER_WORKER="$2"
      shift 2
      ;;
    --warmup-requests)
      WARMUP_REQUESTS="$2"
      shift 2
      ;;
    --connect-timeout-ms)
      CONNECT_TIMEOUT_MS="$2"
      shift 2
      ;;
    --read-timeout-ms)
      READ_TIMEOUT_MS="$2"
      shift 2
      ;;
    --ingest-wal-path)
      INGEST_WAL_PATH="$2"
      shift 2
      ;;
    --ingest-wal-sync-every-records)
      INGEST_WAL_SYNC_EVERY_RECORDS="$2"
      shift 2
      ;;
    --ingest-wal-append-buffer-records)
      INGEST_WAL_APPEND_BUFFER_RECORDS="$2"
      shift 2
      ;;
    --ingest-wal-sync-interval-ms)
      case "$2" in
        off|none|unset)
          INGEST_WAL_SYNC_INTERVAL_MS=""
          ;;
        *)
          INGEST_WAL_SYNC_INTERVAL_MS="$2"
          ;;
      esac
      shift 2
      ;;
    --ingest-wal-async-flush-interval-ms)
      case "$2" in
        off|none|unset)
          INGEST_WAL_ASYNC_FLUSH_INTERVAL_MS=""
          ;;
        *)
          INGEST_WAL_ASYNC_FLUSH_INTERVAL_MS="$2"
          ;;
      esac
      shift 2
      ;;
    --ingest-wal-background-flush-only)
      INGEST_WAL_BACKGROUND_FLUSH_ONLY="$2"
      shift 2
      ;;
    --ingest-allow-unsafe-wal-durability)
      INGEST_ALLOW_UNSAFE_WAL_DURABILITY="$2"
      shift 2
      ;;
    --output-dir)
      OUTPUT_DIR="$2"
      shift 2
      ;;
    --run-tag)
      RUN_TAG="$2"
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

mkdir -p "${OUTPUT_DIR}"

RUN_STAMP="$(date -u +%Y%m%d-%H%M%S)"
RUN_ID="${RUN_STAMP}-${RUN_TAG}"
OUT_PATH="${OUTPUT_DIR}/${RUN_ID}.md"
if [[ -z "${INGEST_WAL_PATH}" ]]; then
  INGEST_WAL_PATH="/tmp/dash-ingest-concurrency-${RUN_ID}.wal"
fi
ROWS_TMP="$(mktemp)"
DETAILS_TMP="$(mktemp)"

SERVER_PID=""
SERVER_LOG=""

if [[ "${TARGET}" != "retrieval" && "${TARGET}" != "ingestion" ]]; then
  echo "invalid --target value: ${TARGET} (allowed: retrieval, ingestion)" >&2
  exit 2
fi

is_positive_integer() {
  local value="$1"
  [[ "${value}" =~ ^[0-9]+$ ]] && [[ "${value}" -gt 0 ]]
}

if ! is_positive_integer "${INGEST_WAL_SYNC_EVERY_RECORDS}"; then
  echo "invalid --ingest-wal-sync-every-records: ${INGEST_WAL_SYNC_EVERY_RECORDS}" >&2
  exit 2
fi

if ! is_positive_integer "${INGEST_WAL_APPEND_BUFFER_RECORDS}"; then
  echo "invalid --ingest-wal-append-buffer-records: ${INGEST_WAL_APPEND_BUFFER_RECORDS}" >&2
  exit 2
fi

if [[ -n "${INGEST_WAL_SYNC_INTERVAL_MS}" ]] && ! is_positive_integer "${INGEST_WAL_SYNC_INTERVAL_MS}"; then
  echo "invalid --ingest-wal-sync-interval-ms: ${INGEST_WAL_SYNC_INTERVAL_MS}" >&2
  exit 2
fi
if [[ -n "${INGEST_WAL_ASYNC_FLUSH_INTERVAL_MS}" ]] && ! is_positive_integer "${INGEST_WAL_ASYNC_FLUSH_INTERVAL_MS}"; then
  echo "invalid --ingest-wal-async-flush-interval-ms: ${INGEST_WAL_ASYNC_FLUSH_INTERVAL_MS}" >&2
  exit 2
fi

if [[ "${INGEST_ALLOW_UNSAFE_WAL_DURABILITY}" != "true" && "${INGEST_ALLOW_UNSAFE_WAL_DURABILITY}" != "false" ]]; then
  echo "invalid --ingest-allow-unsafe-wal-durability: ${INGEST_ALLOW_UNSAFE_WAL_DURABILITY}" >&2
  exit 2
fi
if [[ "${INGEST_WAL_BACKGROUND_FLUSH_ONLY}" != "true" && "${INGEST_WAL_BACKGROUND_FLUSH_ONLY}" != "false" ]]; then
  echo "invalid --ingest-wal-background-flush-only: ${INGEST_WAL_BACKGROUND_FLUSH_ONLY}" >&2
  exit 2
fi

stop_server() {
  if [[ -n "${SERVER_PID}" ]] && kill -0 "${SERVER_PID}" >/dev/null 2>&1; then
    kill "${SERVER_PID}" >/dev/null 2>&1 || true
    wait "${SERVER_PID}" >/dev/null 2>&1 || true
  fi
  SERVER_PID=""
  if [[ -n "${SERVER_LOG}" && -f "${SERVER_LOG}" ]]; then
    rm -f "${SERVER_LOG}"
  fi
  SERVER_LOG=""
}

cleanup() {
  stop_server
  if [[ "${TARGET}" == "ingestion" ]]; then
    rm -f "${INGEST_WAL_PATH}" "${INGEST_WAL_PATH}.snapshot"
  fi
  rm -f "${ROWS_TMP}" "${DETAILS_TMP}"
}
trap cleanup EXIT

start_server() {
  local workers="$1"
  stop_server
  SERVER_LOG="$(mktemp)"

  if [[ "${TARGET}" == "retrieval" ]]; then
    DASH_RETRIEVAL_BIND="${BIND_ADDR}" \
      DASH_RETRIEVAL_HTTP_WORKERS="${workers}" \
      cargo run -p retrieval -- --serve >"${SERVER_LOG}" 2>&1 &
  else
    rm -f "${INGEST_WAL_PATH}" "${INGEST_WAL_PATH}.snapshot"
    if [[ -n "${INGEST_WAL_SYNC_INTERVAL_MS}" ]]; then
      if [[ -n "${INGEST_WAL_ASYNC_FLUSH_INTERVAL_MS}" ]]; then
        DASH_INGEST_BIND="${BIND_ADDR}" \
          DASH_INGEST_HTTP_WORKERS="${workers}" \
          DASH_INGEST_WAL_PATH="${INGEST_WAL_PATH}" \
          DASH_INGEST_WAL_SYNC_EVERY_RECORDS="${INGEST_WAL_SYNC_EVERY_RECORDS}" \
          DASH_INGEST_WAL_APPEND_BUFFER_RECORDS="${INGEST_WAL_APPEND_BUFFER_RECORDS}" \
          DASH_INGEST_WAL_SYNC_INTERVAL_MS="${INGEST_WAL_SYNC_INTERVAL_MS}" \
          DASH_INGEST_WAL_ASYNC_FLUSH_INTERVAL_MS="${INGEST_WAL_ASYNC_FLUSH_INTERVAL_MS}" \
          DASH_INGEST_WAL_BACKGROUND_FLUSH_ONLY="${INGEST_WAL_BACKGROUND_FLUSH_ONLY}" \
          DASH_INGEST_ALLOW_UNSAFE_WAL_DURABILITY="${INGEST_ALLOW_UNSAFE_WAL_DURABILITY}" \
          cargo run -p ingestion -- --serve >"${SERVER_LOG}" 2>&1 &
      else
        DASH_INGEST_BIND="${BIND_ADDR}" \
          DASH_INGEST_HTTP_WORKERS="${workers}" \
          DASH_INGEST_WAL_PATH="${INGEST_WAL_PATH}" \
          DASH_INGEST_WAL_SYNC_EVERY_RECORDS="${INGEST_WAL_SYNC_EVERY_RECORDS}" \
          DASH_INGEST_WAL_APPEND_BUFFER_RECORDS="${INGEST_WAL_APPEND_BUFFER_RECORDS}" \
          DASH_INGEST_WAL_SYNC_INTERVAL_MS="${INGEST_WAL_SYNC_INTERVAL_MS}" \
          DASH_INGEST_WAL_BACKGROUND_FLUSH_ONLY="${INGEST_WAL_BACKGROUND_FLUSH_ONLY}" \
          DASH_INGEST_ALLOW_UNSAFE_WAL_DURABILITY="${INGEST_ALLOW_UNSAFE_WAL_DURABILITY}" \
          cargo run -p ingestion -- --serve >"${SERVER_LOG}" 2>&1 &
      fi
    else
      if [[ -n "${INGEST_WAL_ASYNC_FLUSH_INTERVAL_MS}" ]]; then
        DASH_INGEST_BIND="${BIND_ADDR}" \
          DASH_INGEST_HTTP_WORKERS="${workers}" \
          DASH_INGEST_WAL_PATH="${INGEST_WAL_PATH}" \
          DASH_INGEST_WAL_SYNC_EVERY_RECORDS="${INGEST_WAL_SYNC_EVERY_RECORDS}" \
          DASH_INGEST_WAL_APPEND_BUFFER_RECORDS="${INGEST_WAL_APPEND_BUFFER_RECORDS}" \
          DASH_INGEST_WAL_ASYNC_FLUSH_INTERVAL_MS="${INGEST_WAL_ASYNC_FLUSH_INTERVAL_MS}" \
          DASH_INGEST_WAL_BACKGROUND_FLUSH_ONLY="${INGEST_WAL_BACKGROUND_FLUSH_ONLY}" \
          DASH_INGEST_ALLOW_UNSAFE_WAL_DURABILITY="${INGEST_ALLOW_UNSAFE_WAL_DURABILITY}" \
          cargo run -p ingestion -- --serve >"${SERVER_LOG}" 2>&1 &
      else
        DASH_INGEST_BIND="${BIND_ADDR}" \
          DASH_INGEST_HTTP_WORKERS="${workers}" \
          DASH_INGEST_WAL_PATH="${INGEST_WAL_PATH}" \
          DASH_INGEST_WAL_SYNC_EVERY_RECORDS="${INGEST_WAL_SYNC_EVERY_RECORDS}" \
          DASH_INGEST_WAL_APPEND_BUFFER_RECORDS="${INGEST_WAL_APPEND_BUFFER_RECORDS}" \
          DASH_INGEST_WAL_BACKGROUND_FLUSH_ONLY="${INGEST_WAL_BACKGROUND_FLUSH_ONLY}" \
          DASH_INGEST_ALLOW_UNSAFE_WAL_DURABILITY="${INGEST_ALLOW_UNSAFE_WAL_DURABILITY}" \
          cargo run -p ingestion -- --serve >"${SERVER_LOG}" 2>&1 &
      fi
    fi
  fi
  SERVER_PID=$!

  for _ in $(seq 1 25); do
    if cargo run -p benchmark-smoke --bin concurrent_load -- \
      --addr "${BIND_ADDR}" \
      --path "/health" \
      --concurrency 1 \
      --requests-per-worker 1 \
      --warmup-requests 0 \
      --connect-timeout-ms 500 \
      --read-timeout-ms 500 >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.2
  done

  echo "[concurrency] ${TARGET} failed to start for workers=${workers}" >&2
  cat "${SERVER_LOG}" >&2 || true
  return 1
}

render_output() {
cat > "${OUT_PATH}" <<EOF_MD
# DASH Transport Concurrency Benchmark

- run_id: ${RUN_ID}
- run_utc: $(date -u +%Y-%m-%dT%H:%M:%SZ)
- target: ${TARGET} HTTP transport
- bind_addr: ${BIND_ADDR}
- clients: ${CONCURRENCY}
- requests_per_worker: ${REQUESTS_PER_WORKER}
- warmup_requests: ${WARMUP_REQUESTS}
- workers_list: ${WORKERS_LIST}
$(if [[ "${TARGET}" == "ingestion" ]]; then echo "- ingest_wal_path: ${INGEST_WAL_PATH}"; fi)
$(if [[ "${TARGET}" == "ingestion" ]]; then echo "- ingest_wal_sync_every_records: ${INGEST_WAL_SYNC_EVERY_RECORDS}"; fi)
$(if [[ "${TARGET}" == "ingestion" ]]; then echo "- ingest_wal_append_buffer_records: ${INGEST_WAL_APPEND_BUFFER_RECORDS}"; fi)
$(if [[ "${TARGET}" == "ingestion" ]]; then echo "- ingest_wal_sync_interval_ms: ${INGEST_WAL_SYNC_INTERVAL_MS:-off}"; fi)
$(if [[ "${TARGET}" == "ingestion" ]]; then echo "- ingest_wal_async_flush_interval_ms: ${INGEST_WAL_ASYNC_FLUSH_INTERVAL_MS:-auto}"; fi)
$(if [[ "${TARGET}" == "ingestion" ]]; then echo "- ingest_wal_background_flush_only: ${INGEST_WAL_BACKGROUND_FLUSH_ONLY}"; fi)
$(if [[ "${TARGET}" == "ingestion" ]]; then echo "- ingest_allow_unsafe_wal_durability: ${INGEST_ALLOW_UNSAFE_WAL_DURABILITY}"; fi)

| transport_workers | total_requests | throughput_rps | latency_avg_ms | latency_p95_ms | latency_p99_ms | success_rate_pct |
|---:|---:|---:|---:|---:|---:|---:|
EOF_MD
cat "${ROWS_TMP}" >> "${OUT_PATH}"
echo >> "${OUT_PATH}"
cat "${DETAILS_TMP}" >> "${OUT_PATH}"
}

IFS=',' read -r -a workers_array <<< "${WORKERS_LIST}"

for raw_workers in "${workers_array[@]}"; do
  workers="$(printf '%s' "${raw_workers}" | tr -d '[:space:]')"
  if [[ -z "${workers}" || ! "${workers}" =~ ^[0-9]+$ || "${workers}" -eq 0 ]]; then
    echo "invalid workers value in --workers-list: ${raw_workers}" >&2
    exit 2
  fi

  echo "[concurrency] running workers=${workers}"
  start_server "${workers}"

  set +e
  if [[ "${TARGET}" == "retrieval" ]]; then
    output="$(cargo run -p benchmark-smoke --bin concurrent_load -- \
      --addr "${BIND_ADDR}" \
      --path "/v1/retrieve?tenant_id=sample-tenant&query=retrieval+initialized&top_k=5&stance_mode=balanced" \
      --concurrency "${CONCURRENCY}" \
      --requests-per-worker "${REQUESTS_PER_WORKER}" \
      --warmup-requests "${WARMUP_REQUESTS}" \
      --connect-timeout-ms "${CONNECT_TIMEOUT_MS}" \
      --read-timeout-ms "${READ_TIMEOUT_MS}" 2>&1)"
  else
    output="$(cargo run -p benchmark-smoke --bin concurrent_load -- \
      --addr "${BIND_ADDR}" \
      --path "/v1/ingest" \
      --method POST \
      --content-type "application/json" \
      --body "${INGEST_BODY_TEMPLATE}" \
      --concurrency "${CONCURRENCY}" \
      --requests-per-worker "${REQUESTS_PER_WORKER}" \
      --warmup-requests "${WARMUP_REQUESTS}" \
      --connect-timeout-ms "${CONNECT_TIMEOUT_MS}" \
      --read-timeout-ms "${READ_TIMEOUT_MS}" 2>&1)"
  fi
  status=$?
  set -e

  if [[ ${status} -ne 0 ]]; then
    {
      echo
      echo "## workers=${workers}"
      echo
      echo "status: FAIL"
      echo
      echo '```text'
      echo "${output}"
      echo '```'
      echo
      echo "server_log:"
      echo '```text'
      cat "${SERVER_LOG}" || true
      echo '```'
    } >> "${DETAILS_TMP}"
    printf '| %s | %s | %s | %s | %s | %s | %s |\n' \
      "${workers}" "n/a" "n/a" "n/a" "n/a" "n/a" "fail" >> "${ROWS_TMP}"
    echo "[concurrency] run failed for workers=${workers}" >&2
    render_output
    stop_server
    exit 1
  fi

  total_requests="$(printf '%s\n' "${output}" | awk -F': ' '/total_requests:/{print $2; exit}')"
  throughput_rps="$(printf '%s\n' "${output}" | awk -F': ' '/throughput_rps:/{print $2; exit}')"
  latency_avg_ms="$(printf '%s\n' "${output}" | awk -F': ' '/latency_avg_ms:/{print $2; exit}')"
  latency_p95_ms="$(printf '%s\n' "${output}" | awk -F': ' '/latency_p95_ms:/{print $2; exit}')"
  latency_p99_ms="$(printf '%s\n' "${output}" | awk -F': ' '/latency_p99_ms:/{print $2; exit}')"
  success_rate_pct="$(printf '%s\n' "${output}" | awk -F': ' '/success_rate_pct:/{print $2; exit}')"

  printf '| %s | %s | %s | %s | %s | %s | %s |\n' \
    "${workers}" "${total_requests}" "${throughput_rps}" "${latency_avg_ms}" "${latency_p95_ms}" "${latency_p99_ms}" "${success_rate_pct}" >> "${ROWS_TMP}"

  {
    echo
    echo "## workers=${workers}"
    echo
    echo "status: PASS"
    echo
    echo '```text'
    echo "${output}"
    echo '```'
  } >> "${DETAILS_TMP}"

  stop_server
  echo "[concurrency] workers=${workers} done"
done

render_output

echo "[concurrency] completed"
echo "[concurrency] output: ${OUT_PATH}"
