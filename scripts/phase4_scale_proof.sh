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
ITERATIONS="${DASH_PHASE4_ITERATIONS:-1}"
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
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

for required in "${FIXTURE_SIZE}" "${ITERATIONS}" "${WAL_SCALE_CLAIMS}" "${RETRIEVAL_CLIENTS}" \
  "${RETRIEVAL_REQUESTS_PER_WORKER}" "${INGESTION_CLIENTS}" "${INGESTION_REQUESTS_PER_WORKER}" \
  "${INGEST_FRESHNESS_P95_SLO_MS}" "${PLACEMENT_RELOAD_INTERVAL_MS}" "${FAILOVER_MAX_WAIT_SECONDS}" \
  "${REBALANCE_REQUIRE_MOVED_KEYS}" "${REBALANCE_PROBE_KEYS_COUNT}" "${REBALANCE_MAX_WAIT_SECONDS}" \
  "${REBALANCE_TARGET_SHARDS}" "${REBALANCE_MIN_SHARDS_GATE}"; do
  if [[ ! "${required}" =~ ^[0-9]+$ ]] || [[ "${required}" -eq 0 ]]; then
    echo "all numeric options must be positive integers" >&2
    exit 2
  fi
done

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

mkdir -p "$(dirname "${HISTORY_PATH}")" "${SUMMARY_DIR}" "${SCORECARD_DIR}"

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

cleanup() {
  rm -f "${BENCH_TMP}" "${RETR_TMP}" "${INGEST_TMP}" "${FAILOVER_TMP}" "${REBALANCE_TMP}"
}
trap cleanup EXIT

status_benchmark="FAIL"
status_retrieval="FAIL"
status_ingestion="FAIL"
status_failover="FAIL"
status_rebalance="FAIL"
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

## Artifacts

- retrieval_concurrency_summary: ${retrieval_summary_path}
- ingestion_concurrency_summary: ${ingestion_summary_path}
- rebalance_summary: ${rebalance_summary_path}

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
EOF_SUMMARY

echo "[phase4-scale-proof] summary: ${SUMMARY_PATH}"

if [[ "${overall_status}" != "PASS" ]]; then
  echo "[phase4-scale-proof] failed" >&2
  exit 1
fi

echo "[phase4-scale-proof] completed"
