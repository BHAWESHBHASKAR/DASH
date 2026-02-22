#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

HISTORY_PATH="${DASH_PHASE4_HISTORY_PATH:-docs/benchmarks/history/benchmark-history.md}"
SUMMARY_DIR="${DASH_PHASE4_SUMMARY_DIR:-docs/benchmarks/history/runs}"
SCORECARD_DIR="${DASH_PHASE4_SCORECARD_DIR:-docs/benchmarks/scorecards}"
RUN_TAG="${DASH_PHASE4_RUN_TAG:-phase4-scale-proof}"
RUN_ID_OVERRIDE="${DASH_PHASE4_RUN_ID:-}"
PROFILE="${DASH_PHASE4_PROFILE:-xlarge}"
FIXTURE_SIZE="${DASH_PHASE4_FIXTURE_SIZE:-150000}"
ITERATIONS="${DASH_PHASE4_ITERATIONS:-5}"
WAL_SCALE_CLAIMS="${DASH_PHASE4_WAL_SCALE_CLAIMS:-5000}"
BENCH_RELEASE="${DASH_PHASE4_BENCH_RELEASE:-true}"
RETRIEVAL_WORKERS_LIST="${DASH_PHASE4_RETRIEVAL_WORKERS_LIST:-4}"
RETRIEVAL_CLIENTS="${DASH_PHASE4_RETRIEVAL_CLIENTS:-32}"
RETRIEVAL_REQUESTS_PER_WORKER="${DASH_PHASE4_RETRIEVAL_REQUESTS_PER_WORKER:-40}"
RETRIEVAL_WARMUP_REQUESTS="${DASH_PHASE4_RETRIEVAL_WARMUP_REQUESTS:-10}"
INGESTION_WORKERS_LIST="${DASH_PHASE4_INGESTION_WORKERS_LIST:-4}"
INGESTION_CLIENTS="${DASH_PHASE4_INGESTION_CLIENTS:-32}"
INGESTION_REQUESTS_PER_WORKER="${DASH_PHASE4_INGESTION_REQUESTS_PER_WORKER:-40}"
INGESTION_WARMUP_REQUESTS="${DASH_PHASE4_INGESTION_WARMUP_REQUESTS:-10}"
INGEST_FRESHNESS_P95_SLO_MS="${DASH_PHASE4_INGEST_FRESHNESS_P95_SLO_MS:-5000}"
FAILOVER_MODE="${DASH_PHASE4_FAILOVER_MODE:-both}"
PLACEMENT_RELOAD_INTERVAL_MS="${DASH_PHASE4_PLACEMENT_RELOAD_INTERVAL_MS:-200}"
FAILOVER_MAX_WAIT_SECONDS="${DASH_PHASE4_FAILOVER_MAX_WAIT_SECONDS:-30}"
RUN_REBALANCE_DRILL="${DASH_PHASE4_RUN_REBALANCE_DRILL:-true}"
REBALANCE_ARTIFACT_DIR="${DASH_PHASE4_REBALANCE_ARTIFACT_DIR:-docs/benchmarks/history/rebalance}"
REBALANCE_REQUIRE_MOVED_KEYS="${DASH_PHASE4_REBALANCE_REQUIRE_MOVED_KEYS:-1}"
REBALANCE_PROBE_KEYS_COUNT="${DASH_PHASE4_REBALANCE_PROBE_KEYS_COUNT:-128}"
REBALANCE_MAX_WAIT_SECONDS="${DASH_PHASE4_REBALANCE_MAX_WAIT_SECONDS:-30}"
REBALANCE_TARGET_SHARDS="${DASH_PHASE4_REBALANCE_TARGET_SHARDS:-8}"
REBALANCE_MIN_SHARDS_GATE="${DASH_PHASE4_REBALANCE_MIN_SHARDS_GATE:-8}"
RUN_STORAGE_PROMOTION_BOUNDARY_GUARD="${DASH_PHASE4_RUN_STORAGE_PROMOTION_BOUNDARY_GUARD:-false}"
STORAGE_PROMOTION_BOUNDARY_BIND_ADDR="${DASH_PHASE4_STORAGE_PROMOTION_BOUNDARY_BIND_ADDR:-127.0.0.1:18080}"
STORAGE_PROMOTION_BOUNDARY_INGEST_BIND_ADDR="${DASH_PHASE4_STORAGE_PROMOTION_BOUNDARY_INGEST_BIND_ADDR:-127.0.0.1:18081}"
STORAGE_PROMOTION_BOUNDARY_WORKERS="${DASH_PHASE4_STORAGE_PROMOTION_BOUNDARY_WORKERS:-4}"
STORAGE_PROMOTION_BOUNDARY_CLIENTS="${DASH_PHASE4_STORAGE_PROMOTION_BOUNDARY_CLIENTS:-16}"
STORAGE_PROMOTION_BOUNDARY_REQUESTS_PER_WORKER="${DASH_PHASE4_STORAGE_PROMOTION_BOUNDARY_REQUESTS_PER_WORKER:-20}"
STORAGE_PROMOTION_BOUNDARY_WARMUP_REQUESTS="${DASH_PHASE4_STORAGE_PROMOTION_BOUNDARY_WARMUP_REQUESTS:-5}"
STORAGE_PROMOTION_BOUNDARY_CONNECT_TIMEOUT_MS="${DASH_PHASE4_STORAGE_PROMOTION_BOUNDARY_CONNECT_TIMEOUT_MS:-2000}"
STORAGE_PROMOTION_BOUNDARY_READ_TIMEOUT_MS="${DASH_PHASE4_STORAGE_PROMOTION_BOUNDARY_READ_TIMEOUT_MS:-5000}"
STORAGE_PROMOTION_BOUNDARY_CURL_TIMEOUT_SECONDS="${DASH_PHASE4_STORAGE_PROMOTION_BOUNDARY_CURL_TIMEOUT_SECONDS:-10}"
STORAGE_PROMOTION_BOUNDARY_SUMMARY_DIR="${DASH_PHASE4_STORAGE_PROMOTION_BOUNDARY_SUMMARY_DIR:-${SUMMARY_DIR}}"
STORAGE_PROMOTION_BOUNDARY_HISTORY_PATH="${DASH_PHASE4_STORAGE_PROMOTION_BOUNDARY_HISTORY_PATH:-docs/benchmarks/history/runs/storage-promotion-boundary-history.csv}"
STORAGE_PROMOTION_BOUNDARY_TREND_WINDOW_RUNS="${DASH_PHASE4_STORAGE_PROMOTION_BOUNDARY_TREND_WINDOW_RUNS:-10}"
STORAGE_PROMOTION_BOUNDARY_MAX_COUNTER_REGRESSION_PCT="${DASH_PHASE4_STORAGE_PROMOTION_BOUNDARY_MAX_COUNTER_REGRESSION_PCT:-50}"
STORAGE_PROMOTION_BOUNDARY_MIN_TREND_SAMPLES="${DASH_PHASE4_STORAGE_PROMOTION_BOUNDARY_MIN_TREND_SAMPLES:-0}"

usage() {
  cat <<'USAGE'
Usage: scripts/phase4_scale_proof.sh [options]

Options:
  --history-path PATH                     Benchmark history markdown path
  --summary-dir DIR                       Output directory for phase-4 run summaries
  --scorecard-dir DIR                     Output directory for benchmark scorecards
  --run-tag TAG                           Suffix tag in generated artifact filenames
  --run-id ID                             Explicit run id (overrides generated timestamp-based id)
  --profile NAME                          Benchmark profile (default: xlarge)
  --fixture-size N                        Benchmark fixture size override (>100k for phase-4 lane)
  --iterations N                          Benchmark iterations
  --wal-scale-claims N                    WAL checkpoint/replay scale-slice claim count
  --bench-release true|false              Run benchmark-smoke in release mode (default: true)
  --retrieval-workers-list CSV            Worker list for retrieval concurrency benchmark
  --retrieval-clients N                   Concurrent clients for retrieval benchmark
  --retrieval-requests-per-worker N       Requests per retrieval worker
  --retrieval-warmup-requests N           Warmup requests for retrieval benchmark
  --ingestion-workers-list CSV            Worker list for ingestion concurrency benchmark
  --ingestion-clients N                   Concurrent clients for ingestion benchmark
  --ingestion-requests-per-worker N       Requests per ingestion worker
  --ingestion-warmup-requests N           Warmup requests for ingestion benchmark
  --ingest-freshness-p95-slo-ms N         p95 freshness guard (ms) for ingestion benchmark
  --failover-mode MODE                    failover drill mode: restart|no-restart|both
  --placement-reload-interval-ms N        placement reload interval for failover drill
  --failover-max-wait-seconds N           max wait per failover probe
  --run-rebalance-drill true|false        run rebalance/split drill (default: true)
  --rebalance-artifact-dir DIR            artifact root for rebalance drill outputs
  --rebalance-require-moved-keys N        minimum key migrations expected after split
  --rebalance-probe-keys-count N          probe key count for before/after routing diff
  --rebalance-max-wait-seconds N          max wait per rebalance probe
  --rebalance-target-shards N             rebalance drill target shard count (default: 8)
  --rebalance-min-shards-gate N           minimum shard_count_after required by phase gate (default: 8)
  --run-storage-promotion-boundary-guard true|false
                                          run under-load promotion-boundary guard (default: false)
  --storage-promotion-boundary-bind-addr HOST:PORT
                                          retrieval bind address for promotion-boundary guard
  --storage-promotion-boundary-ingest-bind-addr HOST:PORT
                                          ingestion bind address for promotion-boundary bootstrap
  --storage-promotion-boundary-workers N  retrieval workers for promotion-boundary guard
  --storage-promotion-boundary-clients N  concurrent clients for promotion-boundary guard
  --storage-promotion-boundary-requests-per-worker N
                                          requests per worker for promotion-boundary guard
  --storage-promotion-boundary-warmup-requests N
                                          warmup requests for promotion-boundary guard
  --storage-promotion-boundary-connect-timeout-ms N
                                          connect timeout for promotion-boundary guard
  --storage-promotion-boundary-read-timeout-ms N
                                          read timeout for promotion-boundary guard
  --storage-promotion-boundary-curl-timeout-seconds N
                                          curl timeout for promotion-boundary guard
  --storage-promotion-boundary-summary-dir DIR
                                          output dir for promotion-boundary guard summaries
  --storage-promotion-boundary-history-path PATH
                                          CSV path for promotion-boundary trend history
  --storage-promotion-boundary-trend-window-runs N
                                          lookback window for trend regression checks
  --storage-promotion-boundary-max-counter-regression-pct N
                                          max allowed scenario counter regression percentage
  --storage-promotion-boundary-min-trend-samples N
                                          minimum historical trend samples required before PASS
  -h, --help                              Show help
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --history-path) HISTORY_PATH="$2"; shift 2 ;;
    --summary-dir) SUMMARY_DIR="$2"; shift 2 ;;
    --scorecard-dir) SCORECARD_DIR="$2"; shift 2 ;;
    --run-tag) RUN_TAG="$2"; shift 2 ;;
    --run-id) RUN_ID_OVERRIDE="$2"; shift 2 ;;
    --profile) PROFILE="$2"; shift 2 ;;
    --fixture-size) FIXTURE_SIZE="$2"; shift 2 ;;
    --iterations) ITERATIONS="$2"; shift 2 ;;
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
    --run-rebalance-drill) RUN_REBALANCE_DRILL="$2"; shift 2 ;;
    --rebalance-artifact-dir) REBALANCE_ARTIFACT_DIR="$2"; shift 2 ;;
    --rebalance-require-moved-keys) REBALANCE_REQUIRE_MOVED_KEYS="$2"; shift 2 ;;
    --rebalance-probe-keys-count) REBALANCE_PROBE_KEYS_COUNT="$2"; shift 2 ;;
    --rebalance-max-wait-seconds) REBALANCE_MAX_WAIT_SECONDS="$2"; shift 2 ;;
    --rebalance-target-shards) REBALANCE_TARGET_SHARDS="$2"; shift 2 ;;
    --rebalance-min-shards-gate) REBALANCE_MIN_SHARDS_GATE="$2"; shift 2 ;;
    --run-storage-promotion-boundary-guard) RUN_STORAGE_PROMOTION_BOUNDARY_GUARD="$2"; shift 2 ;;
    --storage-promotion-boundary-bind-addr) STORAGE_PROMOTION_BOUNDARY_BIND_ADDR="$2"; shift 2 ;;
    --storage-promotion-boundary-ingest-bind-addr) STORAGE_PROMOTION_BOUNDARY_INGEST_BIND_ADDR="$2"; shift 2 ;;
    --storage-promotion-boundary-workers) STORAGE_PROMOTION_BOUNDARY_WORKERS="$2"; shift 2 ;;
    --storage-promotion-boundary-clients) STORAGE_PROMOTION_BOUNDARY_CLIENTS="$2"; shift 2 ;;
    --storage-promotion-boundary-requests-per-worker) STORAGE_PROMOTION_BOUNDARY_REQUESTS_PER_WORKER="$2"; shift 2 ;;
    --storage-promotion-boundary-warmup-requests) STORAGE_PROMOTION_BOUNDARY_WARMUP_REQUESTS="$2"; shift 2 ;;
    --storage-promotion-boundary-connect-timeout-ms) STORAGE_PROMOTION_BOUNDARY_CONNECT_TIMEOUT_MS="$2"; shift 2 ;;
    --storage-promotion-boundary-read-timeout-ms) STORAGE_PROMOTION_BOUNDARY_READ_TIMEOUT_MS="$2"; shift 2 ;;
    --storage-promotion-boundary-curl-timeout-seconds) STORAGE_PROMOTION_BOUNDARY_CURL_TIMEOUT_SECONDS="$2"; shift 2 ;;
    --storage-promotion-boundary-summary-dir) STORAGE_PROMOTION_BOUNDARY_SUMMARY_DIR="$2"; shift 2 ;;
    --storage-promotion-boundary-history-path) STORAGE_PROMOTION_BOUNDARY_HISTORY_PATH="$2"; shift 2 ;;
    --storage-promotion-boundary-trend-window-runs) STORAGE_PROMOTION_BOUNDARY_TREND_WINDOW_RUNS="$2"; shift 2 ;;
    --storage-promotion-boundary-max-counter-regression-pct) STORAGE_PROMOTION_BOUNDARY_MAX_COUNTER_REGRESSION_PCT="$2"; shift 2 ;;
    --storage-promotion-boundary-min-trend-samples) STORAGE_PROMOTION_BOUNDARY_MIN_TREND_SAMPLES="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

for required in "${FIXTURE_SIZE}" "${ITERATIONS}" "${WAL_SCALE_CLAIMS}" "${RETRIEVAL_CLIENTS}" \
  "${RETRIEVAL_REQUESTS_PER_WORKER}" "${INGESTION_CLIENTS}" "${INGESTION_REQUESTS_PER_WORKER}" \
  "${INGEST_FRESHNESS_P95_SLO_MS}" "${PLACEMENT_RELOAD_INTERVAL_MS}" "${FAILOVER_MAX_WAIT_SECONDS}" \
  "${REBALANCE_REQUIRE_MOVED_KEYS}" "${REBALANCE_PROBE_KEYS_COUNT}" "${REBALANCE_MAX_WAIT_SECONDS}" \
  "${REBALANCE_TARGET_SHARDS}" "${REBALANCE_MIN_SHARDS_GATE}" \
  "${STORAGE_PROMOTION_BOUNDARY_WORKERS}" "${STORAGE_PROMOTION_BOUNDARY_CLIENTS}" \
  "${STORAGE_PROMOTION_BOUNDARY_REQUESTS_PER_WORKER}" "${STORAGE_PROMOTION_BOUNDARY_CONNECT_TIMEOUT_MS}" \
  "${STORAGE_PROMOTION_BOUNDARY_READ_TIMEOUT_MS}" "${STORAGE_PROMOTION_BOUNDARY_CURL_TIMEOUT_SECONDS}" \
  "${STORAGE_PROMOTION_BOUNDARY_TREND_WINDOW_RUNS}"; do
  if [[ ! "${required}" =~ ^[0-9]+$ ]] || [[ "${required}" -eq 0 ]]; then
    echo "all numeric options must be positive integers" >&2
    exit 2
  fi
done

if [[ ! "${STORAGE_PROMOTION_BOUNDARY_WARMUP_REQUESTS}" =~ ^[0-9]+$ ]]; then
  echo "--storage-promotion-boundary-warmup-requests must be a non-negative integer" >&2
  exit 2
fi

if [[ "${REBALANCE_TARGET_SHARDS}" -lt 2 ]]; then
  echo "--rebalance-target-shards must be >= 2" >&2
  exit 2
fi

if [[ "${REBALANCE_MIN_SHARDS_GATE}" -lt 2 ]]; then
  echo "--rebalance-min-shards-gate must be >= 2" >&2
  exit 2
fi

case "${FAILOVER_MODE}" in
  restart|no-restart|both) ;;
  *)
    echo "--failover-mode must be restart|no-restart|both" >&2
    exit 2
    ;;
esac

case "${BENCH_RELEASE}" in
  true|false) ;;
  *)
    echo "--bench-release must be true or false" >&2
    exit 2
    ;;
esac

case "${RUN_REBALANCE_DRILL}" in
  true|false) ;;
  *)
    echo "--run-rebalance-drill must be true or false" >&2
    exit 2
    ;;
esac

case "${RUN_STORAGE_PROMOTION_BOUNDARY_GUARD}" in
  true|false) ;;
  *)
    echo "--run-storage-promotion-boundary-guard must be true or false" >&2
    exit 2
    ;;
esac

if ! awk -v v="${STORAGE_PROMOTION_BOUNDARY_MAX_COUNTER_REGRESSION_PCT}" \
  'BEGIN { exit !(v ~ /^[0-9]+([.][0-9]+)?$/) }'; then
  echo "--storage-promotion-boundary-max-counter-regression-pct must be a non-negative number" >&2
  exit 2
fi

if [[ ! "${STORAGE_PROMOTION_BOUNDARY_MIN_TREND_SAMPLES}" =~ ^[0-9]+$ ]]; then
  echo "--storage-promotion-boundary-min-trend-samples must be a non-negative integer" >&2
  exit 2
fi

mkdir -p "$(dirname "${HISTORY_PATH}")" "${SUMMARY_DIR}" "${SCORECARD_DIR}" \
  "${STORAGE_PROMOTION_BOUNDARY_SUMMARY_DIR}" "$(dirname "${STORAGE_PROMOTION_BOUNDARY_HISTORY_PATH}")"

RUN_STAMP="$(date -u +%Y%m%d-%H%M%S)"
RUN_ID="${RUN_STAMP}-${RUN_TAG}"
if [[ -n "${RUN_ID_OVERRIDE}" ]]; then
  RUN_ID="${RUN_ID_OVERRIDE}"
fi
SCORECARD_PATH="${SCORECARD_DIR}/${RUN_ID}-${PROFILE}-fixture-${FIXTURE_SIZE}.md"
SUMMARY_PATH="${SUMMARY_DIR}/${RUN_ID}.md"

BENCH_TMP="$(mktemp)"
RETR_TMP="$(mktemp)"
INGEST_TMP="$(mktemp)"
FAILOVER_TMP="$(mktemp)"
REBALANCE_TMP="$(mktemp)"
STORAGE_PROMOTION_BOUNDARY_TMP="$(mktemp)"
STORAGE_PROMOTION_TREND_TMP="$(mktemp)"

cleanup() {
  rm -f "${BENCH_TMP}" "${RETR_TMP}" "${INGEST_TMP}" "${FAILOVER_TMP}" "${REBALANCE_TMP}" \
    "${STORAGE_PROMOTION_BOUNDARY_TMP}" "${STORAGE_PROMOTION_TREND_TMP}"
}
trap cleanup EXIT

status_benchmark="FAIL"
status_retrieval="FAIL"
status_ingestion="FAIL"
status_failover="FAIL"
status_rebalance="FAIL"
status_storage_promotion_boundary="SKIP"
status_storage_promotion_boundary_trend="SKIP"
overall_status="PASS"

baseline_avg_ms="n/a"
dash_avg_ms="n/a"
candidate_reduction_pct="n/a"
ann_recall_at_10="n/a"
ann_recall_at_100="n/a"

retrieval_summary_path="n/a"
retrieval_throughput_rps="n/a"
retrieval_latency_p95_ms="n/a"
retrieval_latency_p99_ms="n/a"

ingestion_summary_path="n/a"
ingestion_throughput_rps="n/a"
ingestion_latency_p95_ms="n/a"
ingestion_latency_p99_ms="n/a"
ingestion_success_rate_pct="n/a"
ingestion_freshness_guard_pass="false"
rebalance_summary_path="n/a"
rebalance_target_shards="n/a"
rebalance_shard_count_after="n/a"
rebalance_shard_gate_pass="n/a"
storage_promotion_boundary_summary_path="n/a"
storage_promotion_boundary_history_path="${STORAGE_PROMOTION_BOUNDARY_HISTORY_PATH}"
storage_promotion_boundary_replay_counter="n/a"
storage_promotion_boundary_segment_plus_wal_delta_counter="n/a"
storage_promotion_boundary_segment_fully_promoted_counter="n/a"
storage_promotion_boundary_trend_window_samples="0"
storage_promotion_boundary_replay_regression_pct="n/a"
storage_promotion_boundary_segment_plus_wal_delta_regression_pct="n/a"
storage_promotion_boundary_segment_fully_promoted_regression_pct="n/a"
storage_promotion_boundary_max_regression_pct_observed="n/a"
storage_promotion_boundary_trend_pass="n/a"
storage_promotion_boundary_trend_sample_gate_pass="n/a"

metric_value_from_markdown_table() {
  local path="$1"
  local metric="$2"
  awk -F'|' -v metric="${metric}" '
    {
      key=$2
      value=$3
      gsub(/^[ \t]+|[ \t]+$/, "", key)
      gsub(/^[ \t]+|[ \t]+$/, "", value)
      if (key == metric) {
        print value
        exit
      }
    }
  ' "${path}"
}

latest_concurrency_row() {
  awk '/^\| [0-9]+ \|/{row=$0} END {print row}' "$1"
}

table_col() {
  local row="$1"
  local col="$2"
  printf '%s\n' "${row}" | awk -F'|' -v idx="${col}" '{value=$idx; gsub(/^[ \t]+|[ \t]+$/, "", value); print value}'
}

promotion_guard_table_col() {
  local path="$1"
  local scenario="$2"
  local column="$3"
  awk -F'|' -v scenario="${scenario}" -v column="${column}" '
    {
      key=$2
      gsub(/^[ \t]+|[ \t]+$/, "", key)
      if (key == scenario) {
        value=$column
        gsub(/^[ \t]+|[ \t]+$/, "", value)
        print value
        exit
      }
    }
  ' "${path}"
}

is_non_negative_number() {
  local value="$1"
  [[ "${value}" =~ ^[0-9]+([.][0-9]+)?$ ]]
}

average_recent_history_counter() {
  local column="$1"
  local status_column="$2"
  local workload_key="$3"
  awk -F',' -v workload_key="${workload_key}" -v value_col="${column}" -v status_col="${status_column}" '
    NR > 1 && $4 == workload_key && $3 == "PASS" && $status_col == "PASS" {
      value=$value_col
      if (value ~ /^[0-9]+([.][0-9]+)?$/) {
        print value
      }
    }
  ' "${STORAGE_PROMOTION_BOUNDARY_HISTORY_PATH}" \
    | tail -n "${STORAGE_PROMOTION_BOUNDARY_TREND_WINDOW_RUNS}" \
    | awk '{sum+=$1; n++} END {if (n > 0) printf "%.4f", sum/n}'
}

recent_history_counter_samples() {
  local column="$1"
  local status_column="$2"
  local workload_key="$3"
  awk -F',' -v workload_key="${workload_key}" -v value_col="${column}" -v status_col="${status_column}" '
    NR > 1 && $4 == workload_key && $3 == "PASS" && $status_col == "PASS" {
      value=$value_col
      if (value ~ /^[0-9]+([.][0-9]+)?$/) {
        print value
      }
    }
  ' "${STORAGE_PROMOTION_BOUNDARY_HISTORY_PATH}" \
    | tail -n "${STORAGE_PROMOTION_BOUNDARY_TREND_WINDOW_RUNS}" \
    | awk 'END {print NR+0}'
}

bench_cmd=(cargo run -p benchmark-smoke --bin benchmark-smoke --)
if [[ "${BENCH_RELEASE}" == "true" ]]; then
  bench_cmd=(cargo run --release -p benchmark-smoke --bin benchmark-smoke --)
fi
bench_cmd+=(
  --profile "${PROFILE}"
  --fixture-size "${FIXTURE_SIZE}"
  --iterations "${ITERATIONS}"
  --history-out "${HISTORY_PATH}"
  --scorecard-out "${SCORECARD_PATH}"
)

if DASH_BENCH_WAL_SCALE_CLAIMS="${WAL_SCALE_CLAIMS}" "${bench_cmd[@]}" >"${BENCH_TMP}" 2>&1; then
  status_benchmark="PASS"
  baseline_avg_ms="$(awk -F': ' '/Baseline avg latency \(ms\):/{print $2; exit}' "${BENCH_TMP}")"
  dash_avg_ms="$(awk -F': ' '/DASH avg latency \(ms\):/{print $2; exit}' "${BENCH_TMP}")"
  candidate_reduction_pct="$(awk -F': ' '/Candidate reduction \(%\):/{print $2; exit}' "${BENCH_TMP}")"
  ann_recall_at_10="$(awk -F': ' '/ANN recall@10:/{print $2; exit}' "${BENCH_TMP}")"
  ann_recall_at_100="$(awk -F': ' '/ANN recall@100:/{print $2; exit}' "${BENCH_TMP}")"
else
  overall_status="FAIL"
fi

if scripts/benchmark_transport_concurrency.sh \
  --target retrieval \
  --workers-list "${RETRIEVAL_WORKERS_LIST}" \
  --clients "${RETRIEVAL_CLIENTS}" \
  --requests-per-worker "${RETRIEVAL_REQUESTS_PER_WORKER}" \
  --warmup-requests "${RETRIEVAL_WARMUP_REQUESTS}" \
  --run-tag "${RUN_ID}-retrieval" >"${RETR_TMP}" 2>&1; then
  status_retrieval="PASS"
  retrieval_summary_path="$(awk -F': ' '/\[concurrency\] output:/{print $2; exit}' "${RETR_TMP}")"
  if [[ -n "${retrieval_summary_path}" && -f "${retrieval_summary_path}" ]]; then
    retrieval_row="$(latest_concurrency_row "${retrieval_summary_path}")"
    retrieval_throughput_rps="$(table_col "${retrieval_row}" 4)"
    retrieval_latency_p95_ms="$(table_col "${retrieval_row}" 6)"
    retrieval_latency_p99_ms="$(table_col "${retrieval_row}" 7)"
  else
    retrieval_summary_path="n/a"
  fi
else
  overall_status="FAIL"
fi

if scripts/benchmark_transport_concurrency.sh \
  --target ingestion \
  --workers-list "${INGESTION_WORKERS_LIST}" \
  --clients "${INGESTION_CLIENTS}" \
  --requests-per-worker "${INGESTION_REQUESTS_PER_WORKER}" \
  --warmup-requests "${INGESTION_WARMUP_REQUESTS}" \
  --run-tag "${RUN_ID}-ingestion" >"${INGEST_TMP}" 2>&1; then
  status_ingestion="PASS"
  ingestion_summary_path="$(awk -F': ' '/\[concurrency\] output:/{print $2; exit}' "${INGEST_TMP}")"
  if [[ -n "${ingestion_summary_path}" && -f "${ingestion_summary_path}" ]]; then
    ingestion_row="$(latest_concurrency_row "${ingestion_summary_path}")"
    ingestion_throughput_rps="$(table_col "${ingestion_row}" 4)"
    ingestion_latency_p95_ms="$(table_col "${ingestion_row}" 6)"
    ingestion_latency_p99_ms="$(table_col "${ingestion_row}" 7)"
    ingestion_success_rate_pct="$(table_col "${ingestion_row}" 8)"
  else
    ingestion_summary_path="n/a"
  fi
else
  overall_status="FAIL"
fi

if [[ "${status_ingestion}" == "PASS" ]]; then
  if awk -v p95="${ingestion_latency_p95_ms}" -v slo="${INGEST_FRESHNESS_P95_SLO_MS}" 'BEGIN { exit !(p95+0 <= slo+0) }'; then
    ingestion_freshness_guard_pass="true"
  else
    ingestion_freshness_guard_pass="false"
    overall_status="FAIL"
  fi
fi

if scripts/failover_drill.sh \
  --mode "${FAILOVER_MODE}" \
  --placement-reload-interval-ms "${PLACEMENT_RELOAD_INTERVAL_MS}" \
  --max-wait-seconds "${FAILOVER_MAX_WAIT_SECONDS}" >"${FAILOVER_TMP}" 2>&1; then
  status_failover="PASS"
else
  overall_status="FAIL"
fi

if [[ "${RUN_REBALANCE_DRILL}" == "true" ]]; then
  if scripts/rebalance_drill.sh \
    --run-id "${RUN_ID}-rebalance" \
    --artifact-dir "${REBALANCE_ARTIFACT_DIR}" \
    --local-node-id "node-a" \
    --tenant-id "sample-tenant" \
    --placement-reload-interval-ms "${PLACEMENT_RELOAD_INTERVAL_MS}" \
    --max-wait-seconds "${REBALANCE_MAX_WAIT_SECONDS}" \
    --require-moved-keys "${REBALANCE_REQUIRE_MOVED_KEYS}" \
    --probe-keys-count "${REBALANCE_PROBE_KEYS_COUNT}" \
    --target-shards "${REBALANCE_TARGET_SHARDS}" >"${REBALANCE_TMP}" 2>&1; then
    status_rebalance="PASS"
    rebalance_summary_path="$(awk -F'=' '/\[rebalance-drill\] summary_path=/{print $2; exit}' "${REBALANCE_TMP}")"
    rebalance_target_shards="${REBALANCE_TARGET_SHARDS}"
    if [[ -z "${rebalance_summary_path}" ]]; then
      rebalance_summary_path="n/a"
      status_rebalance="FAIL"
      rebalance_shard_gate_pass="false"
      overall_status="FAIL"
      printf '[phase4-scale-proof] rebalance gate failed: summary path missing\n' >> "${REBALANCE_TMP}"
    elif [[ ! -f "${rebalance_summary_path}" ]]; then
      status_rebalance="FAIL"
      rebalance_shard_gate_pass="false"
      overall_status="FAIL"
      printf '[phase4-scale-proof] rebalance gate failed: summary file not found: %s\n' "${rebalance_summary_path}" >> "${REBALANCE_TMP}"
    else
      rebalance_shard_count_after="$(metric_value_from_markdown_table "${rebalance_summary_path}" "shard_count_after")"
      if [[ ! "${rebalance_shard_count_after}" =~ ^[0-9]+$ ]]; then
        status_rebalance="FAIL"
        rebalance_shard_gate_pass="false"
        overall_status="FAIL"
        printf '[phase4-scale-proof] rebalance gate failed: could not parse shard_count_after from %s\n' "${rebalance_summary_path}" >> "${REBALANCE_TMP}"
      elif (( rebalance_shard_count_after < REBALANCE_MIN_SHARDS_GATE )); then
        status_rebalance="FAIL"
        rebalance_shard_gate_pass="false"
        overall_status="FAIL"
        printf '[phase4-scale-proof] rebalance gate failed: shard_count_after=%s min_required=%s\n' "${rebalance_shard_count_after}" "${REBALANCE_MIN_SHARDS_GATE}" >> "${REBALANCE_TMP}"
      else
        rebalance_shard_gate_pass="true"
      fi
    fi
  else
    rebalance_target_shards="${REBALANCE_TARGET_SHARDS}"
    rebalance_shard_gate_pass="false"
    overall_status="FAIL"
  fi
else
  status_rebalance="SKIP"
  rebalance_shard_gate_pass="n/a"
  printf '[rebalance-drill] skipped (run_rebalance_drill=false)\n' > "${REBALANCE_TMP}"
fi

storage_workload_key="${STORAGE_PROMOTION_BOUNDARY_WORKERS}_${STORAGE_PROMOTION_BOUNDARY_CLIENTS}_${STORAGE_PROMOTION_BOUNDARY_REQUESTS_PER_WORKER}_${STORAGE_PROMOTION_BOUNDARY_WARMUP_REQUESTS}"
if [[ "${RUN_STORAGE_PROMOTION_BOUNDARY_GUARD}" == "true" ]]; then
  status_storage_promotion_boundary="FAIL"
  status_storage_promotion_boundary_trend="FAIL"
  if scripts/storage_promotion_boundary_guard.sh \
    --bind-addr "${STORAGE_PROMOTION_BOUNDARY_BIND_ADDR}" \
    --ingest-bind-addr "${STORAGE_PROMOTION_BOUNDARY_INGEST_BIND_ADDR}" \
    --workers "${STORAGE_PROMOTION_BOUNDARY_WORKERS}" \
    --clients "${STORAGE_PROMOTION_BOUNDARY_CLIENTS}" \
    --requests-per-worker "${STORAGE_PROMOTION_BOUNDARY_REQUESTS_PER_WORKER}" \
    --warmup-requests "${STORAGE_PROMOTION_BOUNDARY_WARMUP_REQUESTS}" \
    --connect-timeout-ms "${STORAGE_PROMOTION_BOUNDARY_CONNECT_TIMEOUT_MS}" \
    --read-timeout-ms "${STORAGE_PROMOTION_BOUNDARY_READ_TIMEOUT_MS}" \
    --curl-timeout-seconds "${STORAGE_PROMOTION_BOUNDARY_CURL_TIMEOUT_SECONDS}" \
    --summary-dir "${STORAGE_PROMOTION_BOUNDARY_SUMMARY_DIR}" \
    --run-tag "${RUN_ID}-phase4" >"${STORAGE_PROMOTION_BOUNDARY_TMP}" 2>&1; then
    status_storage_promotion_boundary="PASS"
    storage_promotion_boundary_summary_path="$(awk -F': ' '/\[promotion-boundary-guard\] summary:/{print $2; exit}' "${STORAGE_PROMOTION_BOUNDARY_TMP}")"
    if [[ -z "${storage_promotion_boundary_summary_path}" || ! -f "${storage_promotion_boundary_summary_path}" ]]; then
      status_storage_promotion_boundary="FAIL"
      status_storage_promotion_boundary_trend="FAIL"
      overall_status="FAIL"
      printf '[phase4-scale-proof] storage promotion boundary guard summary not found\n' >> "${STORAGE_PROMOTION_TREND_TMP}"
    else
      replay_status="$(promotion_guard_table_col "${storage_promotion_boundary_summary_path}" "replay_only" 3)"
      segment_plus_wal_status="$(promotion_guard_table_col "${storage_promotion_boundary_summary_path}" "segment_base_plus_wal_delta" 3)"
      segment_fully_promoted_status="$(promotion_guard_table_col "${storage_promotion_boundary_summary_path}" "segment_base_fully_promoted" 3)"

      replay_counter="$(promotion_guard_table_col "${storage_promotion_boundary_summary_path}" "replay_only" 9)"
      segment_plus_wal_counter="$(promotion_guard_table_col "${storage_promotion_boundary_summary_path}" "segment_base_plus_wal_delta" 9)"
      segment_fully_promoted_counter="$(promotion_guard_table_col "${storage_promotion_boundary_summary_path}" "segment_base_fully_promoted" 9)"

      if [[ "${replay_status}" != "PASS" || "${segment_plus_wal_status}" != "PASS" || "${segment_fully_promoted_status}" != "PASS" ]]; then
        status_storage_promotion_boundary="FAIL"
        status_storage_promotion_boundary_trend="FAIL"
        overall_status="FAIL"
        printf '[phase4-scale-proof] storage promotion boundary guard scenario status is not PASS\n' >> "${STORAGE_PROMOTION_TREND_TMP}"
      elif ! is_non_negative_number "${replay_counter}" || ! is_non_negative_number "${segment_plus_wal_counter}" || ! is_non_negative_number "${segment_fully_promoted_counter}"; then
        status_storage_promotion_boundary="FAIL"
        status_storage_promotion_boundary_trend="FAIL"
        overall_status="FAIL"
        printf '[phase4-scale-proof] storage promotion boundary counters are not numeric\n' >> "${STORAGE_PROMOTION_TREND_TMP}"
      else
        storage_promotion_boundary_replay_counter="${replay_counter}"
        storage_promotion_boundary_segment_plus_wal_delta_counter="${segment_plus_wal_counter}"
        storage_promotion_boundary_segment_fully_promoted_counter="${segment_fully_promoted_counter}"

        if [[ ! -f "${STORAGE_PROMOTION_BOUNDARY_HISTORY_PATH}" ]]; then
          cat > "${STORAGE_PROMOTION_BOUNDARY_HISTORY_PATH}" <<'EOF_PROMOTION_HISTORY'
run_utc,run_id,decision,workload_key,workers,clients,requests_per_worker,warmup_requests,replay_status,replay_counter,segment_plus_wal_delta_status,segment_plus_wal_delta_counter,segment_fully_promoted_status,segment_fully_promoted_counter,trend_pass,max_regression_pct_observed,max_regression_pct_threshold
EOF_PROMOTION_HISTORY
        fi

        replay_baseline_avg="$(average_recent_history_counter 10 9 "${storage_workload_key}")"
        segment_plus_wal_baseline_avg="$(average_recent_history_counter 12 11 "${storage_workload_key}")"
        segment_fully_promoted_baseline_avg="$(average_recent_history_counter 14 13 "${storage_workload_key}")"

        replay_samples="$(recent_history_counter_samples 10 9 "${storage_workload_key}")"
        segment_plus_wal_samples="$(recent_history_counter_samples 12 11 "${storage_workload_key}")"
        segment_fully_promoted_samples="$(recent_history_counter_samples 14 13 "${storage_workload_key}")"

        storage_promotion_boundary_trend_window_samples="$(( replay_samples ))"
        if (( segment_plus_wal_samples < storage_promotion_boundary_trend_window_samples )); then
          storage_promotion_boundary_trend_window_samples="${segment_plus_wal_samples}"
        fi
        if (( segment_fully_promoted_samples < storage_promotion_boundary_trend_window_samples )); then
          storage_promotion_boundary_trend_window_samples="${segment_fully_promoted_samples}"
        fi

        trend_baseline_count=0
        trend_fail=false
        max_regression_pct=0
        storage_promotion_boundary_replay_regression_pct="n/a"
        storage_promotion_boundary_segment_plus_wal_delta_regression_pct="n/a"
        storage_promotion_boundary_segment_fully_promoted_regression_pct="n/a"

        if is_non_negative_number "${replay_baseline_avg}"; then
          trend_baseline_count=$((trend_baseline_count + 1))
          storage_promotion_boundary_replay_regression_pct="$(awk -v current="${replay_counter}" -v baseline="${replay_baseline_avg}" 'BEGIN { if (baseline <= 0 || current >= baseline) { printf "0.00" } else { printf "%.2f", ((baseline - current) * 100.0) / baseline } }')"
          if awk -v actual="${storage_promotion_boundary_replay_regression_pct}" -v max="${STORAGE_PROMOTION_BOUNDARY_MAX_COUNTER_REGRESSION_PCT}" 'BEGIN { exit !(actual > max) }'; then
            trend_fail=true
          fi
          max_regression_pct="$(awk -v current_max="${max_regression_pct}" -v candidate="${storage_promotion_boundary_replay_regression_pct}" 'BEGIN { if (candidate > current_max) printf "%.2f", candidate; else printf "%.2f", current_max }')"
        fi

        if is_non_negative_number "${segment_plus_wal_baseline_avg}"; then
          trend_baseline_count=$((trend_baseline_count + 1))
          storage_promotion_boundary_segment_plus_wal_delta_regression_pct="$(awk -v current="${segment_plus_wal_counter}" -v baseline="${segment_plus_wal_baseline_avg}" 'BEGIN { if (baseline <= 0 || current >= baseline) { printf "0.00" } else { printf "%.2f", ((baseline - current) * 100.0) / baseline } }')"
          if awk -v actual="${storage_promotion_boundary_segment_plus_wal_delta_regression_pct}" -v max="${STORAGE_PROMOTION_BOUNDARY_MAX_COUNTER_REGRESSION_PCT}" 'BEGIN { exit !(actual > max) }'; then
            trend_fail=true
          fi
          max_regression_pct="$(awk -v current_max="${max_regression_pct}" -v candidate="${storage_promotion_boundary_segment_plus_wal_delta_regression_pct}" 'BEGIN { if (candidate > current_max) printf "%.2f", candidate; else printf "%.2f", current_max }')"
        fi

        if is_non_negative_number "${segment_fully_promoted_baseline_avg}"; then
          trend_baseline_count=$((trend_baseline_count + 1))
          storage_promotion_boundary_segment_fully_promoted_regression_pct="$(awk -v current="${segment_fully_promoted_counter}" -v baseline="${segment_fully_promoted_baseline_avg}" 'BEGIN { if (baseline <= 0 || current >= baseline) { printf "0.00" } else { printf "%.2f", ((baseline - current) * 100.0) / baseline } }')"
          if awk -v actual="${storage_promotion_boundary_segment_fully_promoted_regression_pct}" -v max="${STORAGE_PROMOTION_BOUNDARY_MAX_COUNTER_REGRESSION_PCT}" 'BEGIN { exit !(actual > max) }'; then
            trend_fail=true
          fi
          max_regression_pct="$(awk -v current_max="${max_regression_pct}" -v candidate="${storage_promotion_boundary_segment_fully_promoted_regression_pct}" 'BEGIN { if (candidate > current_max) printf "%.2f", candidate; else printf "%.2f", current_max }')"
        fi

        if (( trend_baseline_count == 0 )); then
          status_storage_promotion_boundary_trend="PASS"
          storage_promotion_boundary_trend_pass="true"
          storage_promotion_boundary_max_regression_pct_observed="0.00"
          printf '[phase4-scale-proof] storage promotion boundary trend baseline unavailable (bootstrap run)\n' >> "${STORAGE_PROMOTION_TREND_TMP}"
        elif [[ "${trend_fail}" == "true" ]]; then
          status_storage_promotion_boundary_trend="FAIL"
          storage_promotion_boundary_trend_pass="false"
          storage_promotion_boundary_max_regression_pct_observed="${max_regression_pct}"
          overall_status="FAIL"
          printf '[phase4-scale-proof] storage promotion boundary trend regression exceeded threshold\n' >> "${STORAGE_PROMOTION_TREND_TMP}"
        else
          status_storage_promotion_boundary_trend="PASS"
          storage_promotion_boundary_trend_pass="true"
          storage_promotion_boundary_max_regression_pct_observed="${max_regression_pct}"
        fi

        if (( storage_promotion_boundary_trend_window_samples < STORAGE_PROMOTION_BOUNDARY_MIN_TREND_SAMPLES )); then
          storage_promotion_boundary_trend_sample_gate_pass="false"
          status_storage_promotion_boundary_trend="FAIL"
          storage_promotion_boundary_trend_pass="false"
          overall_status="FAIL"
          printf '[phase4-scale-proof] storage promotion boundary trend sample gate failed: samples=%s min_required=%s\n' \
            "${storage_promotion_boundary_trend_window_samples}" \
            "${STORAGE_PROMOTION_BOUNDARY_MIN_TREND_SAMPLES}" >> "${STORAGE_PROMOTION_TREND_TMP}"
        else
          storage_promotion_boundary_trend_sample_gate_pass="true"
        fi

        {
          echo "[phase4-scale-proof] storage_workload_key=${storage_workload_key}"
          echo "[phase4-scale-proof] replay_baseline_avg=${replay_baseline_avg:-n/a} replay_counter=${replay_counter} replay_regression_pct=${storage_promotion_boundary_replay_regression_pct}"
          echo "[phase4-scale-proof] segment_plus_wal_baseline_avg=${segment_plus_wal_baseline_avg:-n/a} segment_plus_wal_counter=${segment_plus_wal_counter} segment_plus_wal_regression_pct=${storage_promotion_boundary_segment_plus_wal_delta_regression_pct}"
          echo "[phase4-scale-proof] segment_fully_promoted_baseline_avg=${segment_fully_promoted_baseline_avg:-n/a} segment_fully_promoted_counter=${segment_fully_promoted_counter} segment_fully_promoted_regression_pct=${storage_promotion_boundary_segment_fully_promoted_regression_pct}"
          echo "[phase4-scale-proof] trend_window_samples=${storage_promotion_boundary_trend_window_samples}"
          echo "[phase4-scale-proof] max_allowed_regression_pct=${STORAGE_PROMOTION_BOUNDARY_MAX_COUNTER_REGRESSION_PCT}"
          echo "[phase4-scale-proof] max_observed_regression_pct=${storage_promotion_boundary_max_regression_pct_observed}"
          echo "[phase4-scale-proof] min_trend_samples_required=${STORAGE_PROMOTION_BOUNDARY_MIN_TREND_SAMPLES}"
          echo "[phase4-scale-proof] trend_sample_gate_pass=${storage_promotion_boundary_trend_sample_gate_pass}"
          echo "[phase4-scale-proof] trend_pass=${storage_promotion_boundary_trend_pass}"
        } >> "${STORAGE_PROMOTION_TREND_TMP}"
      fi
    fi
  else
    status_storage_promotion_boundary="FAIL"
    status_storage_promotion_boundary_trend="FAIL"
    storage_promotion_boundary_trend_pass="false"
    overall_status="FAIL"
  fi

  if [[ ! -f "${STORAGE_PROMOTION_BOUNDARY_HISTORY_PATH}" ]]; then
    cat > "${STORAGE_PROMOTION_BOUNDARY_HISTORY_PATH}" <<'EOF_PROMOTION_HISTORY'
run_utc,run_id,decision,workload_key,workers,clients,requests_per_worker,warmup_requests,replay_status,replay_counter,segment_plus_wal_delta_status,segment_plus_wal_delta_counter,segment_fully_promoted_status,segment_fully_promoted_counter,trend_pass,max_regression_pct_observed,max_regression_pct_threshold
EOF_PROMOTION_HISTORY
  fi
  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    "${RUN_ID}" \
    "${status_storage_promotion_boundary}" \
    "${storage_workload_key}" \
    "${STORAGE_PROMOTION_BOUNDARY_WORKERS}" \
    "${STORAGE_PROMOTION_BOUNDARY_CLIENTS}" \
    "${STORAGE_PROMOTION_BOUNDARY_REQUESTS_PER_WORKER}" \
    "${STORAGE_PROMOTION_BOUNDARY_WARMUP_REQUESTS}" \
    "${replay_status:-n/a}" \
    "${storage_promotion_boundary_replay_counter}" \
    "${segment_plus_wal_status:-n/a}" \
    "${storage_promotion_boundary_segment_plus_wal_delta_counter}" \
    "${segment_fully_promoted_status:-n/a}" \
    "${storage_promotion_boundary_segment_fully_promoted_counter}" \
    "${storage_promotion_boundary_trend_pass}" \
    "${storage_promotion_boundary_max_regression_pct_observed}" \
    "${STORAGE_PROMOTION_BOUNDARY_MAX_COUNTER_REGRESSION_PCT}" >> "${STORAGE_PROMOTION_BOUNDARY_HISTORY_PATH}"
else
  status_storage_promotion_boundary="SKIP"
  status_storage_promotion_boundary_trend="SKIP"
  storage_promotion_boundary_trend_sample_gate_pass="n/a"
  printf '[storage-promotion-boundary] skipped (run_storage_promotion_boundary_guard=false)\n' > "${STORAGE_PROMOTION_BOUNDARY_TMP}"
  printf '[storage-promotion-boundary-trend] skipped (run_storage_promotion_boundary_guard=false)\n' > "${STORAGE_PROMOTION_TREND_TMP}"
fi

cat > "${SUMMARY_PATH}" <<EOF_SUMMARY
# DASH Phase 4 Scale Proof Run

- run_id: ${RUN_ID}
- run_utc: $(date -u +%Y-%m-%dT%H:%M:%SZ)
- overall_status: ${overall_status}
- history_path: ${HISTORY_PATH}
- scorecard: ${SCORECARD_PATH}
- benchmark_release_mode: ${BENCH_RELEASE}

## Step Status

| step | status |
|---|---|
| benchmark (${PROFILE}, fixture=${FIXTURE_SIZE}) | ${status_benchmark} |
| retrieval transport concurrency | ${status_retrieval} |
| ingestion transport concurrency | ${status_ingestion} |
| failover drill (${FAILOVER_MODE}) | ${status_failover} |
| rebalance drill (split + epoch transition) | ${status_rebalance} |
| storage promotion-boundary guard | ${status_storage_promotion_boundary} |
| storage promotion-boundary trend | ${status_storage_promotion_boundary_trend} |

## Scale Metrics

| metric | value |
|---|---:|
| baseline_avg_ms | ${baseline_avg_ms} |
| dash_avg_ms | ${dash_avg_ms} |
| candidate_reduction_pct | ${candidate_reduction_pct} |
| ann_recall_at_10 | ${ann_recall_at_10} |
| ann_recall_at_100 | ${ann_recall_at_100} |
| retrieval_throughput_rps | ${retrieval_throughput_rps} |
| retrieval_latency_p95_ms | ${retrieval_latency_p95_ms} |
| retrieval_latency_p99_ms | ${retrieval_latency_p99_ms} |
| ingestion_throughput_rps | ${ingestion_throughput_rps} |
| ingestion_latency_p95_ms | ${ingestion_latency_p95_ms} |
| ingestion_latency_p99_ms | ${ingestion_latency_p99_ms} |
| ingestion_success_rate_pct | ${ingestion_success_rate_pct} |
| ingestion_freshness_guard_p95<=${INGEST_FRESHNESS_P95_SLO_MS}ms | ${ingestion_freshness_guard_pass} |
| rebalance_target_shards | ${rebalance_target_shards} |
| rebalance_shard_count_after | ${rebalance_shard_count_after} |
| rebalance_min_shards_gate | ${REBALANCE_MIN_SHARDS_GATE} |
| rebalance_shard_gate_pass | ${rebalance_shard_gate_pass} |
| storage_promotion_boundary_replay_counter | ${storage_promotion_boundary_replay_counter} |
| storage_promotion_boundary_segment_plus_wal_delta_counter | ${storage_promotion_boundary_segment_plus_wal_delta_counter} |
| storage_promotion_boundary_segment_fully_promoted_counter | ${storage_promotion_boundary_segment_fully_promoted_counter} |
| storage_promotion_boundary_trend_window_samples | ${storage_promotion_boundary_trend_window_samples} |
| storage_promotion_boundary_replay_regression_pct | ${storage_promotion_boundary_replay_regression_pct} |
| storage_promotion_boundary_segment_plus_wal_delta_regression_pct | ${storage_promotion_boundary_segment_plus_wal_delta_regression_pct} |
| storage_promotion_boundary_segment_fully_promoted_regression_pct | ${storage_promotion_boundary_segment_fully_promoted_regression_pct} |
| storage_promotion_boundary_max_regression_pct_observed | ${storage_promotion_boundary_max_regression_pct_observed} |
| storage_promotion_boundary_max_counter_regression_pct | ${STORAGE_PROMOTION_BOUNDARY_MAX_COUNTER_REGRESSION_PCT} |
| storage_promotion_boundary_min_trend_samples | ${STORAGE_PROMOTION_BOUNDARY_MIN_TREND_SAMPLES} |
| storage_promotion_boundary_trend_sample_gate_pass | ${storage_promotion_boundary_trend_sample_gate_pass} |
| storage_promotion_boundary_trend_pass | ${storage_promotion_boundary_trend_pass} |

## Artifacts

- retrieval_concurrency_summary: ${retrieval_summary_path}
- ingestion_concurrency_summary: ${ingestion_summary_path}
- rebalance_summary: ${rebalance_summary_path}
- storage_promotion_boundary_summary: ${storage_promotion_boundary_summary_path}
- storage_promotion_boundary_history: ${storage_promotion_boundary_history_path}

## Command Output: Benchmark

\`\`\`text
$(cat "${BENCH_TMP}")
\`\`\`

## Command Output: Retrieval Concurrency

\`\`\`text
$(cat "${RETR_TMP}")
\`\`\`

## Command Output: Ingestion Concurrency

\`\`\`text
$(cat "${INGEST_TMP}")
\`\`\`

## Command Output: Failover Drill

\`\`\`text
$(cat "${FAILOVER_TMP}")
\`\`\`

## Command Output: Rebalance Drill

\`\`\`text
$(cat "${REBALANCE_TMP}")
\`\`\`

## Command Output: Storage Promotion Boundary Guard

\`\`\`text
$(cat "${STORAGE_PROMOTION_BOUNDARY_TMP}")
\`\`\`

## Command Output: Storage Promotion Boundary Trend

\`\`\`text
$(cat "${STORAGE_PROMOTION_TREND_TMP}")
\`\`\`
EOF_SUMMARY

echo "[phase4-scale-proof] summary: ${SUMMARY_PATH}"

if [[ "${overall_status}" != "PASS" ]]; then
  echo "[phase4-scale-proof] failed" >&2
  exit 1
fi

echo "[phase4-scale-proof] completed"
