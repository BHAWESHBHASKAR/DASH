#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

BIND_ADDR="${DASH_WAL_DURABILITY_BIND_ADDR:-127.0.0.1:18080}"
WORKERS="${DASH_WAL_DURABILITY_WORKERS:-4}"
CLIENTS="${DASH_WAL_DURABILITY_CLIENTS:-32}"
REQUESTS_PER_WORKER="${DASH_WAL_DURABILITY_REQUESTS_PER_WORKER:-40}"
WARMUP_REQUESTS="${DASH_WAL_DURABILITY_WARMUP_REQUESTS:-10}"
CONNECT_TIMEOUT_MS="${DASH_WAL_DURABILITY_CONNECT_TIMEOUT_MS:-2000}"
READ_TIMEOUT_MS="${DASH_WAL_DURABILITY_READ_TIMEOUT_MS:-5000}"
OUTPUT_DIR="${DASH_WAL_DURABILITY_OUTPUT_DIR:-docs/benchmarks/history/concurrency/wal-durability}"
RAW_OUTPUT_DIR="${DASH_WAL_DURABILITY_RAW_OUTPUT_DIR:-docs/benchmarks/history/concurrency}"
RUN_TAG="${DASH_WAL_DURABILITY_RUN_TAG:-ingest-wal-durability}"
ALLOW_UNSAFE_WAL_DURABILITY="${DASH_WAL_DURABILITY_ALLOW_UNSAFE_WAL_DURABILITY:-true}"
ASYNC_FLUSH_INTERVAL_MS="${DASH_WAL_DURABILITY_ASYNC_FLUSH_INTERVAL_MS:-auto}"
BACKGROUND_FLUSH_ONLY="${DASH_WAL_DURABILITY_BACKGROUND_FLUSH_ONLY:-false}"

STRICT_SYNC_EVERY="${DASH_WAL_DURABILITY_STRICT_SYNC_EVERY:-1}"
STRICT_APPEND_BUFFER="${DASH_WAL_DURABILITY_STRICT_APPEND_BUFFER:-1}"
STRICT_INTERVAL_MS="${DASH_WAL_DURABILITY_STRICT_INTERVAL_MS:-off}"

GROUPED_SYNC_EVERY="${DASH_WAL_DURABILITY_GROUPED_SYNC_EVERY:-32}"
GROUPED_APPEND_BUFFER="${DASH_WAL_DURABILITY_GROUPED_APPEND_BUFFER:-1}"
GROUPED_INTERVAL_MS="${DASH_WAL_DURABILITY_GROUPED_INTERVAL_MS:-off}"

BUFFERED_SYNC_EVERY="${DASH_WAL_DURABILITY_BUFFERED_SYNC_EVERY:-64}"
BUFFERED_APPEND_BUFFER="${DASH_WAL_DURABILITY_BUFFERED_APPEND_BUFFER:-32}"
BUFFERED_INTERVAL_MS="${DASH_WAL_DURABILITY_BUFFERED_INTERVAL_MS:-250}"

usage() {
  cat <<'USAGE'
Usage: scripts/benchmark_ingest_wal_durability.sh [options]

Options:
  --bind-addr HOST:PORT             service bind address
  --workers N                       ingestion transport worker count
  --clients N                       concurrent benchmark clients
  --requests-per-worker N           requests per client
  --warmup-requests N               warmup requests before measurement
  --connect-timeout-ms N            socket connect timeout
  --read-timeout-ms N               socket read/write timeout
  --output-dir DIR                  WAL-durability summary output directory
  --raw-output-dir DIR              per-mode transport output directory
  --run-tag TAG                     suffix for output filename
  --allow-unsafe-wal-durability true|false
                                  set ingestion unsafe WAL durability override for stress modes
  --async-flush-interval-ms N|off|auto
                                  ingestion async WAL flush worker interval for benchmark runs
  --background-flush-only true|false
                                  enable background-only WAL flushing during benchmark runs
  --strict-sync-every N             strict mode sync threshold
  --strict-append-buffer N          strict mode append buffer threshold
  --strict-interval-ms N|off        strict mode sync interval
  --grouped-sync-every N            grouped mode sync threshold
  --grouped-append-buffer N         grouped mode append buffer threshold
  --grouped-interval-ms N|off       grouped mode sync interval
  --buffered-sync-every N           buffered mode sync threshold
  --buffered-append-buffer N        buffered mode append buffer threshold
  --buffered-interval-ms N|off      buffered mode sync interval
  -h, --help                        show help
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --bind-addr)
      BIND_ADDR="$2"
      shift 2
      ;;
    --workers)
      WORKERS="$2"
      shift 2
      ;;
    --clients)
      CLIENTS="$2"
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
    --output-dir)
      OUTPUT_DIR="$2"
      shift 2
      ;;
    --raw-output-dir)
      RAW_OUTPUT_DIR="$2"
      shift 2
      ;;
    --run-tag)
      RUN_TAG="$2"
      shift 2
      ;;
    --allow-unsafe-wal-durability)
      ALLOW_UNSAFE_WAL_DURABILITY="$2"
      shift 2
      ;;
    --async-flush-interval-ms)
      ASYNC_FLUSH_INTERVAL_MS="$2"
      shift 2
      ;;
    --background-flush-only)
      BACKGROUND_FLUSH_ONLY="$2"
      shift 2
      ;;
    --strict-sync-every)
      STRICT_SYNC_EVERY="$2"
      shift 2
      ;;
    --strict-append-buffer)
      STRICT_APPEND_BUFFER="$2"
      shift 2
      ;;
    --strict-interval-ms)
      STRICT_INTERVAL_MS="$2"
      shift 2
      ;;
    --grouped-sync-every)
      GROUPED_SYNC_EVERY="$2"
      shift 2
      ;;
    --grouped-append-buffer)
      GROUPED_APPEND_BUFFER="$2"
      shift 2
      ;;
    --grouped-interval-ms)
      GROUPED_INTERVAL_MS="$2"
      shift 2
      ;;
    --buffered-sync-every)
      BUFFERED_SYNC_EVERY="$2"
      shift 2
      ;;
    --buffered-append-buffer)
      BUFFERED_APPEND_BUFFER="$2"
      shift 2
      ;;
    --buffered-interval-ms)
      BUFFERED_INTERVAL_MS="$2"
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

is_positive_integer() {
  local value="$1"
  [[ "${value}" =~ ^[0-9]+$ ]] && [[ "${value}" -gt 0 ]]
}

is_non_negative_integer() {
  local value="$1"
  [[ "${value}" =~ ^[0-9]+$ ]]
}

normalize_interval() {
  local value="$1"
  case "${value}" in
    off|none|unset|"")
      printf 'off'
      ;;
    *)
      printf '%s' "${value}"
      ;;
  esac
}

trim() {
  local value="$1"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf '%s' "${value}"
}

if ! is_positive_integer "${WORKERS}"; then
  echo "invalid --workers: ${WORKERS}" >&2
  exit 2
fi
if ! is_positive_integer "${CLIENTS}"; then
  echo "invalid --clients: ${CLIENTS}" >&2
  exit 2
fi
if ! is_positive_integer "${REQUESTS_PER_WORKER}"; then
  echo "invalid --requests-per-worker: ${REQUESTS_PER_WORKER}" >&2
  exit 2
fi
if ! is_non_negative_integer "${WARMUP_REQUESTS}"; then
  echo "invalid --warmup-requests: ${WARMUP_REQUESTS}" >&2
  exit 2
fi
if ! is_positive_integer "${CONNECT_TIMEOUT_MS}"; then
  echo "invalid --connect-timeout-ms: ${CONNECT_TIMEOUT_MS}" >&2
  exit 2
fi
if ! is_positive_integer "${READ_TIMEOUT_MS}"; then
  echo "invalid --read-timeout-ms: ${READ_TIMEOUT_MS}" >&2
  exit 2
fi
if [[ "${ALLOW_UNSAFE_WAL_DURABILITY}" != "true" && "${ALLOW_UNSAFE_WAL_DURABILITY}" != "false" ]]; then
  echo "invalid --allow-unsafe-wal-durability: ${ALLOW_UNSAFE_WAL_DURABILITY}" >&2
  exit 2
fi
if [[ "${BACKGROUND_FLUSH_ONLY}" != "true" && "${BACKGROUND_FLUSH_ONLY}" != "false" ]]; then
  echo "invalid --background-flush-only: ${BACKGROUND_FLUSH_ONLY}" >&2
  exit 2
fi

STRICT_INTERVAL_MS="$(normalize_interval "${STRICT_INTERVAL_MS}")"
GROUPED_INTERVAL_MS="$(normalize_interval "${GROUPED_INTERVAL_MS}")"
BUFFERED_INTERVAL_MS="$(normalize_interval "${BUFFERED_INTERVAL_MS}")"
ASYNC_FLUSH_INTERVAL_MS="$(trim "${ASYNC_FLUSH_INTERVAL_MS}")"
if [[ -z "${ASYNC_FLUSH_INTERVAL_MS}" ]]; then
  ASYNC_FLUSH_INTERVAL_MS="auto"
fi
case "${ASYNC_FLUSH_INTERVAL_MS}" in
  auto|off|none|unset) ;;
  *)
    if ! is_positive_integer "${ASYNC_FLUSH_INTERVAL_MS}"; then
      echo "invalid --async-flush-interval-ms: ${ASYNC_FLUSH_INTERVAL_MS}" >&2
      exit 2
    fi
    ;;
esac

mode_sync_values=("${STRICT_SYNC_EVERY}" "${GROUPED_SYNC_EVERY}" "${BUFFERED_SYNC_EVERY}")
mode_append_values=("${STRICT_APPEND_BUFFER}" "${GROUPED_APPEND_BUFFER}" "${BUFFERED_APPEND_BUFFER}")
mode_interval_values=("${STRICT_INTERVAL_MS}" "${GROUPED_INTERVAL_MS}" "${BUFFERED_INTERVAL_MS}")

for value in "${mode_sync_values[@]}"; do
  if ! is_positive_integer "${value}"; then
    echo "invalid sync threshold value: ${value}" >&2
    exit 2
  fi
done

for value in "${mode_append_values[@]}"; do
  if ! is_positive_integer "${value}"; then
    echo "invalid append buffer threshold value: ${value}" >&2
    exit 2
  fi
done

for value in "${mode_interval_values[@]}"; do
  if [[ "${value}" != "off" ]] && ! is_positive_integer "${value}"; then
    echo "invalid interval value: ${value}" >&2
    exit 2
  fi
done

mkdir -p "${OUTPUT_DIR}" "${RAW_OUTPUT_DIR}"

RUN_STAMP="$(date -u +%Y%m%d-%H%M%S)"
RUN_ID="${RUN_STAMP}-${RUN_TAG}"
OUT_PATH="${OUTPUT_DIR}/${RUN_ID}.md"
ROWS_TMP="$(mktemp)"
DETAILS_TMP="$(mktemp)"

cleanup() {
  rm -f "${ROWS_TMP}" "${DETAILS_TMP}"
}
trap cleanup EXIT

mode_names=("strict" "grouped" "buffered_interval")
mode_labels=("strict_per_record" "grouped_sync" "buffered_interval")

strict_throughput=""

for idx in "${!mode_names[@]}"; do
  mode="${mode_names[$idx]}"
  label="${mode_labels[$idx]}"
  sync_every="${mode_sync_values[$idx]}"
  append_buffer="${mode_append_values[$idx]}"
  interval_ms="${mode_interval_values[$idx]}"

  wal_path="/tmp/dash-ingest-wal-durability-${RUN_ID}-${mode}.wal"
  mode_tag="${RUN_TAG}-${mode}"

  echo "[wal-durability] running mode=${mode} sync_every=${sync_every} append_buffer=${append_buffer} interval_ms=${interval_ms}"

  set +e
  async_flush_args=()
  if [[ "${ASYNC_FLUSH_INTERVAL_MS}" != "auto" ]]; then
    async_flush_args+=(--ingest-wal-async-flush-interval-ms "${ASYNC_FLUSH_INTERVAL_MS}")
  fi
  cmd_output="$(
    scripts/benchmark_transport_concurrency.sh \
      --target ingestion \
      --bind-addr "${BIND_ADDR}" \
      --workers-list "${WORKERS}" \
      --clients "${CLIENTS}" \
      --requests-per-worker "${REQUESTS_PER_WORKER}" \
      --warmup-requests "${WARMUP_REQUESTS}" \
      --connect-timeout-ms "${CONNECT_TIMEOUT_MS}" \
      --read-timeout-ms "${READ_TIMEOUT_MS}" \
      --ingest-wal-path "${wal_path}" \
      --ingest-wal-sync-every-records "${sync_every}" \
      --ingest-wal-append-buffer-records "${append_buffer}" \
      --ingest-wal-sync-interval-ms "${interval_ms}" \
      "${async_flush_args[@]}" \
      --ingest-wal-background-flush-only "${BACKGROUND_FLUSH_ONLY}" \
      --ingest-allow-unsafe-wal-durability "${ALLOW_UNSAFE_WAL_DURABILITY}" \
      --output-dir "${RAW_OUTPUT_DIR}" \
      --run-tag "${mode_tag}" 2>&1
  )"
  status=$?
  set -e

  mode_output_path="$(printf '%s\n' "${cmd_output}" | sed -n 's/^\[concurrency\] output: //p' | tail -n 1)"
  if [[ ${status} -ne 0 || -z "${mode_output_path}" || ! -f "${mode_output_path}" ]]; then
    {
      echo
      echo "## mode=${mode}"
      echo
      echo "status: FAIL"
      echo
      echo '```text'
      echo "${cmd_output}"
      echo '```'
    } >> "${DETAILS_TMP}"
    printf '| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |\n' \
      "${label}" "${sync_every}" "${append_buffer}" "${interval_ms}" "n/a" "n/a" "n/a" "n/a" "fail" "n/a" >> "${ROWS_TMP}"
    continue
  fi

  row="$(awk '/^\|[[:space:]]*[0-9]+[[:space:]]*\|/ {print; exit}' "${mode_output_path}")"
  if [[ -z "${row}" ]]; then
    {
      echo
      echo "## mode=${mode}"
      echo
      echo "status: FAIL"
      echo
      echo "reason: unable to parse concurrency metrics row"
      echo
      echo '```text'
      echo "${cmd_output}"
      echo '```'
    } >> "${DETAILS_TMP}"
    printf '| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |\n' \
      "${label}" "${sync_every}" "${append_buffer}" "${interval_ms}" "n/a" "n/a" "n/a" "n/a" "parse_error" "n/a" >> "${ROWS_TMP}"
    continue
  fi

  IFS='|' read -r _ col_workers col_total_requests col_throughput col_latency_avg col_latency_p95 col_latency_p99 col_success _ <<< "${row}"
  col_workers="$(trim "${col_workers}")"
  col_total_requests="$(trim "${col_total_requests}")"
  col_throughput="$(trim "${col_throughput}")"
  col_latency_avg="$(trim "${col_latency_avg}")"
  col_latency_p95="$(trim "${col_latency_p95}")"
  col_latency_p99="$(trim "${col_latency_p99}")"
  col_success="$(trim "${col_success}")"

  if [[ -z "${strict_throughput}" ]]; then
    strict_throughput="${col_throughput}"
    delta_vs_strict="0.00"
  else
    delta_vs_strict="$(
      awk -v current="${col_throughput}" -v baseline="${strict_throughput}" 'BEGIN {
        if (baseline == 0) {
          print "n/a";
        } else {
          printf "%.2f", ((current - baseline) / baseline) * 100;
        }
      }'
    )"
  fi

  printf '| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s%% |\n' \
    "${label}" "${sync_every}" "${append_buffer}" "${interval_ms}" "${col_throughput}" "${col_latency_avg}" "${col_latency_p95}" "${col_latency_p99}" "${col_success}" "${delta_vs_strict}" >> "${ROWS_TMP}"

  {
    echo
    echo "## mode=${mode}"
    echo
    echo "- workers: ${col_workers}"
    echo "- total_requests: ${col_total_requests}"
    echo "- output_artifact: ${mode_output_path}"
    echo
    echo '```text'
    echo "${cmd_output}"
    echo '```'
  } >> "${DETAILS_TMP}"
done

cat > "${OUT_PATH}" <<EOF_MD
# DASH Ingestion WAL Durability Benchmark

- run_id: ${RUN_ID}
- run_utc: $(date -u +%Y-%m-%dT%H:%M:%SZ)
- bind_addr: ${BIND_ADDR}
- workers: ${WORKERS}
- clients: ${CLIENTS}
- requests_per_worker: ${REQUESTS_PER_WORKER}
- warmup_requests: ${WARMUP_REQUESTS}
- raw_output_dir: ${RAW_OUTPUT_DIR}
- allow_unsafe_wal_durability: ${ALLOW_UNSAFE_WAL_DURABILITY}
- async_flush_interval_ms: ${ASYNC_FLUSH_INTERVAL_MS}
- background_flush_only: ${BACKGROUND_FLUSH_ONLY}

| mode | sync_every_records | append_buffer_records | sync_interval_ms | throughput_rps | latency_avg_ms | latency_p95_ms | latency_p99_ms | success_rate_pct | throughput_vs_strict |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
EOF_MD

cat "${ROWS_TMP}" >> "${OUT_PATH}"
echo >> "${OUT_PATH}"
cat "${DETAILS_TMP}" >> "${OUT_PATH}"

echo "[wal-durability] completed"
echo "[wal-durability] output: ${OUT_PATH}"
