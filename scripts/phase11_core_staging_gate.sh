#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

MODE="${DASH_PHASE11_GATE_MODE:-quick}"

# Pre-scan args so mode can define defaults before parse.
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

RUN_TAG="${DASH_PHASE11_GATE_RUN_TAG:-phase11-core-gate}"
SUMMARY_DIR="${DASH_PHASE11_GATE_SUMMARY_DIR:-docs/benchmarks/history/runs}"
SLO_PROFILE="${DASH_PHASE11_GATE_SLO_PROFILE:-smoke}"
SLO_INCLUDE_RECOVERY_DRILL="${DASH_PHASE11_GATE_SLO_INCLUDE_RECOVERY_DRILL:-true}"
RUN_BENCHMARK_TREND="${DASH_PHASE11_GATE_RUN_BENCHMARK_TREND:-false}"
RUN_INCIDENT_SIMULATION_GUARD="${DASH_PHASE11_GATE_RUN_INCIDENT_SIMULATION_GUARD:-false}"
RUN_INGEST_THROUGHPUT_GUARD="${DASH_PHASE11_GATE_RUN_INGEST_THROUGHPUT_GUARD:-true}"
RUN_REPLICATION_LAG_GUARD="${DASH_PHASE11_GATE_RUN_REPLICATION_LAG_GUARD:-true}"
REPLICATION_LAG_LEADER_METRICS_URL="${DASH_PHASE11_GATE_REPLICATION_LAG_LEADER_METRICS_URL:-}"
REPLICATION_LAG_FOLLOWER_METRICS_URL="${DASH_PHASE11_GATE_REPLICATION_LAG_FOLLOWER_METRICS_URL:-}"
REPLICATION_LAG_MAX_CLAIM_LAG="${DASH_PHASE11_GATE_REPLICATION_LAG_MAX_CLAIM_LAG:-1000}"
REPLICATION_LAG_MAX_PULL_FAILURES="${DASH_PHASE11_GATE_REPLICATION_LAG_MAX_PULL_FAILURES:-0}"
REPLICATION_LAG_MIN_PULL_SUCCESSES="${DASH_PHASE11_GATE_REPLICATION_LAG_MIN_PULL_SUCCESSES:-1}"
REPLICATION_LAG_REQUIRE_NO_LAST_ERROR="${DASH_PHASE11_GATE_REPLICATION_LAG_REQUIRE_NO_LAST_ERROR:-true}"
RUN_STORAGE_PROMOTION_BOUNDARY_GUARD="${DASH_PHASE11_GATE_RUN_STORAGE_PROMOTION_BOUNDARY_GUARD:-false}"
STORAGE_PROMOTION_BOUNDARY_BIND_ADDR="${DASH_PHASE11_GATE_STORAGE_PROMOTION_BOUNDARY_BIND_ADDR:-127.0.0.1:18080}"
STORAGE_PROMOTION_BOUNDARY_INGEST_BIND_ADDR="${DASH_PHASE11_GATE_STORAGE_PROMOTION_BOUNDARY_INGEST_BIND_ADDR:-127.0.0.1:18081}"
STORAGE_PROMOTION_BOUNDARY_WORKERS="${DASH_PHASE11_GATE_STORAGE_PROMOTION_BOUNDARY_WORKERS:-4}"
STORAGE_PROMOTION_BOUNDARY_CLIENTS="${DASH_PHASE11_GATE_STORAGE_PROMOTION_BOUNDARY_CLIENTS:-16}"
STORAGE_PROMOTION_BOUNDARY_REQUESTS_PER_WORKER="${DASH_PHASE11_GATE_STORAGE_PROMOTION_BOUNDARY_REQUESTS_PER_WORKER:-20}"
STORAGE_PROMOTION_BOUNDARY_WARMUP_REQUESTS="${DASH_PHASE11_GATE_STORAGE_PROMOTION_BOUNDARY_WARMUP_REQUESTS:-5}"
STORAGE_PROMOTION_BOUNDARY_CONNECT_TIMEOUT_MS="${DASH_PHASE11_GATE_STORAGE_PROMOTION_BOUNDARY_CONNECT_TIMEOUT_MS:-2000}"
STORAGE_PROMOTION_BOUNDARY_READ_TIMEOUT_MS="${DASH_PHASE11_GATE_STORAGE_PROMOTION_BOUNDARY_READ_TIMEOUT_MS:-5000}"
STORAGE_PROMOTION_BOUNDARY_CURL_TIMEOUT_SECONDS="${DASH_PHASE11_GATE_STORAGE_PROMOTION_BOUNDARY_CURL_TIMEOUT_SECONDS:-10}"
STORAGE_PROMOTION_BOUNDARY_MIN_PASS_COUNT="${DASH_PHASE11_GATE_STORAGE_PROMOTION_BOUNDARY_MIN_PASS_COUNT:-0}"
INGEST_MIN_RPS="${DASH_PHASE11_GATE_INGEST_MIN_RPS:-100}"
RECOVERY_MAX_RTO_SECONDS="${DASH_PHASE11_GATE_RECOVERY_MAX_RTO_SECONDS:-60}"
INCIDENT_FAILOVER_MODE="${DASH_PHASE11_GATE_INCIDENT_FAILOVER_MODE:-no-restart}"
INCIDENT_FAILOVER_MAX_WAIT_SECONDS="${DASH_PHASE11_GATE_INCIDENT_FAILOVER_MAX_WAIT_SECONDS:-30}"
INCIDENT_AUTH_MAX_WAIT_SECONDS="${DASH_PHASE11_GATE_INCIDENT_AUTH_MAX_WAIT_SECONDS:-30}"
INCIDENT_RECOVERY_MAX_RTO_SECONDS="${DASH_PHASE11_GATE_INCIDENT_RECOVERY_MAX_RTO_SECONDS:-60}"
RUN_FAILOVER_CHAOS_GUARD="${DASH_PHASE11_GATE_RUN_FAILOVER_CHAOS_GUARD:-false}"
FAILOVER_CHAOS_LOOPS="${DASH_PHASE11_GATE_FAILOVER_CHAOS_LOOPS:-3}"
FAILOVER_CHAOS_MODE="${DASH_PHASE11_GATE_FAILOVER_CHAOS_MODE:-no-restart}"
FAILOVER_CHAOS_MAX_FAILURES="${DASH_PHASE11_GATE_FAILOVER_CHAOS_MAX_FAILURES:-0}"
FAILOVER_CHAOS_MAX_WAIT_SECONDS="${DASH_PHASE11_GATE_FAILOVER_CHAOS_MAX_WAIT_SECONDS:-30}"
FAILOVER_CHAOS_MAX_INGEST_CONVERGENCE_SECONDS="${DASH_PHASE11_GATE_FAILOVER_CHAOS_MAX_INGEST_CONVERGENCE_SECONDS:-5}"
FAILOVER_CHAOS_MAX_RETRIEVE_CONVERGENCE_SECONDS="${DASH_PHASE11_GATE_FAILOVER_CHAOS_MAX_RETRIEVE_CONVERGENCE_SECONDS:-5}"
FAILOVER_CHAOS_KEEP_DRILL_ARTIFACTS="${DASH_PHASE11_GATE_FAILOVER_CHAOS_KEEP_DRILL_ARTIFACTS:-true}"
RUN_RETRIEVAL_LATENCY_GUARD="${DASH_PHASE11_GATE_RUN_RETRIEVAL_LATENCY_GUARD:-false}"
RETRIEVAL_LATENCY_BIND_ADDR="${DASH_PHASE11_GATE_RETRIEVAL_LATENCY_BIND_ADDR:-127.0.0.1:18080}"
RETRIEVAL_LATENCY_WORKERS_LIST="${DASH_PHASE11_GATE_RETRIEVAL_LATENCY_WORKERS_LIST:-2,4,8}"
RETRIEVAL_LATENCY_CLIENTS="${DASH_PHASE11_GATE_RETRIEVAL_LATENCY_CLIENTS:-24}"
RETRIEVAL_LATENCY_REQUESTS_PER_WORKER="${DASH_PHASE11_GATE_RETRIEVAL_LATENCY_REQUESTS_PER_WORKER:-30}"
RETRIEVAL_LATENCY_WARMUP_REQUESTS="${DASH_PHASE11_GATE_RETRIEVAL_LATENCY_WARMUP_REQUESTS:-10}"
RETRIEVAL_LATENCY_CONNECT_TIMEOUT_MS="${DASH_PHASE11_GATE_RETRIEVAL_LATENCY_CONNECT_TIMEOUT_MS:-2000}"
RETRIEVAL_LATENCY_READ_TIMEOUT_MS="${DASH_PHASE11_GATE_RETRIEVAL_LATENCY_READ_TIMEOUT_MS:-5000}"
RETRIEVAL_LATENCY_MAX_P95_MS="${DASH_PHASE11_GATE_RETRIEVAL_LATENCY_MAX_P95_MS:-350}"
RETRIEVAL_LATENCY_MAX_P99_MS="${DASH_PHASE11_GATE_RETRIEVAL_LATENCY_MAX_P99_MS:-700}"
RETRIEVAL_LATENCY_MIN_SUCCESS_RATE_PCT="${DASH_PHASE11_GATE_RETRIEVAL_LATENCY_MIN_SUCCESS_RATE_PCT:-99}"
RETRIEVAL_LATENCY_BENCHMARK_OUTPUT_DIR="${DASH_PHASE11_GATE_RETRIEVAL_LATENCY_BENCHMARK_OUTPUT_DIR:-docs/benchmarks/history/concurrency}"
RUN_READ_CONSISTENCY_POLICY_GUARD="${DASH_PHASE11_GATE_RUN_READ_CONSISTENCY_POLICY_GUARD:-false}"
RUN_CAPACITY_MODEL_GUARD="${DASH_PHASE11_GATE_RUN_CAPACITY_MODEL_GUARD:-false}"
CAPACITY_MODEL_HISTORY_PATH="${DASH_PHASE11_GATE_CAPACITY_MODEL_HISTORY_PATH:-docs/benchmarks/history/benchmark-history.md}"
CAPACITY_MODEL_RUNS_DIR="${DASH_PHASE11_GATE_CAPACITY_MODEL_RUNS_DIR:-docs/benchmarks/history/runs}"
CAPACITY_MODEL_OUTPUT_PATH="${DASH_PHASE11_GATE_CAPACITY_MODEL_OUTPUT_PATH:-docs/execution/dash-capacity-model.md}"
CAPACITY_MODEL_MIN_XLARGE_FIXTURE="${DASH_PHASE11_GATE_CAPACITY_MODEL_MIN_XLARGE_FIXTURE:-100000}"
CAPACITY_MODEL_MIN_XXLARGE_FIXTURE="${DASH_PHASE11_GATE_CAPACITY_MODEL_MIN_XXLARGE_FIXTURE:-200000}"
CAPACITY_MODEL_TARGET_POINTS="${DASH_PHASE11_GATE_CAPACITY_MODEL_TARGET_POINTS:-1000000000}"
CAPACITY_MODEL_REPLICATION_FACTOR="${DASH_PHASE11_GATE_CAPACITY_MODEL_REPLICATION_FACTOR:-3}"
RUN_PRODUCTION_SIGNOFF_BUNDLE="${DASH_PHASE11_GATE_RUN_PRODUCTION_SIGNOFF_BUNDLE:-false}"
PRODUCTION_SIGNOFF_CONCURRENCY_DIR="${DASH_PHASE11_GATE_SIGNOFF_CONCURRENCY_DIR:-docs/benchmarks/history/concurrency}"
PRODUCTION_SIGNOFF_RECOVERY_ARTIFACT_DIR="${DASH_PHASE11_GATE_SIGNOFF_RECOVERY_ARTIFACT_DIR:-docs/benchmarks/history/recovery}"
PRODUCTION_SIGNOFF_OUTPUT_PATH="${DASH_PHASE11_GATE_SIGNOFF_OUTPUT_PATH:-}"
PRODUCTION_SIGNOFF_RUN_ROLLBACK_REHEARSAL="${DASH_PHASE11_GATE_SIGNOFF_RUN_ROLLBACK_REHEARSAL:-true}"
PRODUCTION_SIGNOFF_ROLLBACK_MAX_RTO_SECONDS="${DASH_PHASE11_GATE_SIGNOFF_ROLLBACK_MAX_RTO_SECONDS:-60}"
PRODUCTION_SIGNOFF_ROLLBACK_KEEP_ARTIFACTS="${DASH_PHASE11_GATE_SIGNOFF_ROLLBACK_KEEP_ARTIFACTS:-true}"
PRODUCTION_SIGNOFF_REQUIRE_ZERO_MANUAL_WORKAROUND="${DASH_PHASE11_GATE_SIGNOFF_REQUIRE_ZERO_MANUAL_WORKAROUND:-true}"

if [[ "${MODE}" == "staged" ]]; then
  SLO_PROFILE="${DASH_PHASE11_GATE_SLO_PROFILE:-large}"
  RUN_BENCHMARK_TREND="${DASH_PHASE11_GATE_RUN_BENCHMARK_TREND:-true}"
  RUN_INCIDENT_SIMULATION_GUARD="${DASH_PHASE11_GATE_RUN_INCIDENT_SIMULATION_GUARD:-true}"
  RUN_INGEST_THROUGHPUT_GUARD="${DASH_PHASE11_GATE_RUN_INGEST_THROUGHPUT_GUARD:-true}"
  RUN_REPLICATION_LAG_GUARD="${DASH_PHASE11_GATE_RUN_REPLICATION_LAG_GUARD:-true}"
  RUN_STORAGE_PROMOTION_BOUNDARY_GUARD="${DASH_PHASE11_GATE_RUN_STORAGE_PROMOTION_BOUNDARY_GUARD:-true}"
  STORAGE_PROMOTION_BOUNDARY_MIN_PASS_COUNT="${DASH_PHASE11_GATE_STORAGE_PROMOTION_BOUNDARY_MIN_PASS_COUNT:-3}"
  RUN_FAILOVER_CHAOS_GUARD="${DASH_PHASE11_GATE_RUN_FAILOVER_CHAOS_GUARD:-true}"
  RUN_RETRIEVAL_LATENCY_GUARD="${DASH_PHASE11_GATE_RUN_RETRIEVAL_LATENCY_GUARD:-true}"
  RUN_READ_CONSISTENCY_POLICY_GUARD="${DASH_PHASE11_GATE_RUN_READ_CONSISTENCY_POLICY_GUARD:-true}"
  RUN_CAPACITY_MODEL_GUARD="${DASH_PHASE11_GATE_RUN_CAPACITY_MODEL_GUARD:-true}"
  RUN_PRODUCTION_SIGNOFF_BUNDLE="${DASH_PHASE11_GATE_RUN_PRODUCTION_SIGNOFF_BUNDLE:-true}"
  FAILOVER_CHAOS_LOOPS="${DASH_PHASE11_GATE_FAILOVER_CHAOS_LOOPS:-5}"
  INGEST_MIN_RPS="${DASH_PHASE11_GATE_INGEST_MIN_RPS:-120}"
  RETRIEVAL_LATENCY_CLIENTS="${DASH_PHASE11_GATE_RETRIEVAL_LATENCY_CLIENTS:-32}"
  RETRIEVAL_LATENCY_REQUESTS_PER_WORKER="${DASH_PHASE11_GATE_RETRIEVAL_LATENCY_REQUESTS_PER_WORKER:-40}"
fi

usage() {
  cat <<'USAGE'
Usage: scripts/phase11_core_staging_gate.sh [options]

Run core-production release gating for Phase 11 by orchestrating
scripts/release_candidate_gate.sh with distributed-safety defaults.

Options:
  --mode MODE                                 quick|staged preset mode
  --run-tag TAG                               Run tag for release artifacts
  --summary-dir DIR                           Output directory for summaries
  --slo-profile NAME                          SLO profile for release gate
  --slo-include-recovery-drill true|false    Include recovery in SLO step
  --run-benchmark-trend true|false            Enable benchmark trend step
  --run-incident-simulation-guard true|false Enable incident simulation gate
  --run-ingest-throughput-guard true|false   Enable ingest throughput gate
  --ingest-min-rps N                          Throughput floor when enabled
  --recovery-max-rto-seconds N                Recovery drill RTO threshold
  --run-replication-lag-guard true|false      Enable replication lag guard
  --replication-lag-leader-metrics-url URL    Leader ingestion /metrics URL
  --replication-lag-follower-metrics-url URL  Follower ingestion /metrics URL
  --replication-lag-max-claim-lag N           Max allowed claim lag
  --replication-lag-max-pull-failures N       Max allowed pull failures
  --replication-lag-min-pull-successes N      Min required pull successes
  --replication-lag-require-no-last-error true|false
                                               Require replication_last_error == 0
  --run-storage-promotion-boundary-guard true|false
                                              Enable storage promotion-boundary guard
  --storage-promotion-boundary-bind-addr HOST:PORT
                                              Retrieval bind addr for promotion-boundary guard
  --storage-promotion-boundary-ingest-bind-addr HOST:PORT
                                              Ingestion bind addr for promotion-boundary bootstrap
  --storage-promotion-boundary-workers N       Retrieval workers for promotion-boundary guard
  --storage-promotion-boundary-clients N       Concurrent clients for promotion-boundary guard
  --storage-promotion-boundary-requests-per-worker N
                                              Requests per worker for promotion-boundary guard
  --storage-promotion-boundary-warmup-requests N
                                              Warmup requests for promotion-boundary guard
  --storage-promotion-boundary-connect-timeout-ms N
                                              Connect timeout for promotion-boundary guard
  --storage-promotion-boundary-read-timeout-ms N
                                              Read timeout for promotion-boundary guard
  --storage-promotion-boundary-curl-timeout-seconds N
                                              Curl timeout for promotion-boundary guard
  --storage-promotion-boundary-min-pass-count N
                                              Require at least N promotion-boundary scenarios to pass
  --incident-failover-mode MODE               restart|no-restart|both
  --incident-failover-max-wait-seconds N      Failover wait timeout
  --incident-auth-max-wait-seconds N          Auth revocation wait timeout
  --incident-recovery-max-rto-seconds N       Incident recovery max RTO
  --run-failover-chaos-guard true|false       Enable repeated failover chaos loop gate
  --failover-chaos-loops N                    Number of failover drill iterations
  --failover-chaos-mode MODE                  restart|no-restart|both
  --failover-chaos-max-failures N             Max allowed failed chaos iterations
  --failover-chaos-max-wait-seconds N         Per-iteration drill timeout
  --failover-chaos-max-ingest-convergence-seconds N
                                               Max allowed ingest convergence
  --failover-chaos-max-retrieve-convergence-seconds N
                                               Max allowed retrieve convergence
  --failover-chaos-keep-drill-artifacts true|false
                                               Keep per-iteration drill artifacts
  --run-retrieval-latency-guard true|false     Enable retrieval p95/p99 envelope guard
  --retrieval-latency-bind-addr HOST:PORT      Retrieval benchmark bind address
  --retrieval-latency-workers-list CSV         Retrieval benchmark worker sweep list
  --retrieval-latency-clients N                Retrieval benchmark concurrent clients
  --retrieval-latency-requests-per-worker N    Retrieval benchmark requests per worker
  --retrieval-latency-warmup-requests N        Retrieval benchmark warmup requests
  --retrieval-latency-connect-timeout-ms N     Retrieval benchmark connect timeout
  --retrieval-latency-read-timeout-ms N        Retrieval benchmark read timeout
  --retrieval-latency-max-p95-ms N             Max allowed retrieval p95 ms
  --retrieval-latency-max-p99-ms N             Max allowed retrieval p99 ms
  --retrieval-latency-min-success-rate-pct N   Min allowed retrieval success rate
  --retrieval-latency-benchmark-output-dir DIR Raw retrieval benchmark output directory
  --run-read-consistency-policy-guard true|false
                                               Enable policy-specific read-consistency checks
  --run-capacity-model-guard true|false        Enable capacity-model guard/evidence generation
  --capacity-model-history-path PATH           Benchmark history path for model generation
  --capacity-model-runs-dir DIR                Runs dir used for envelope extraction
  --capacity-model-output-path PATH            Capacity model markdown output path
  --capacity-model-min-xlarge-fixture N        Minimum xlarge fixture size for anchor
  --capacity-model-min-xxlarge-fixture N       Minimum xxlarge fixture size for anchor
  --capacity-model-target-points N             Target points for sizing guidance
  --capacity-model-replication-factor N        Replication factor for sizing guidance
  --run-production-signoff-bundle true|false   Require/emit Week 6 production signoff bundle
  --signoff-concurrency-dir DIR                Ingestion throughput artifact directory
  --signoff-recovery-artifact-dir DIR          Rollback rehearsal artifact directory
  --signoff-output-path PATH                   Production signoff bundle output path
  --signoff-run-rollback-rehearsal true|false  Run rollback rehearsal during signoff bundling
  --signoff-rollback-max-rto-seconds N         Rollback rehearsal max RTO
  --signoff-rollback-keep-artifacts true|false Keep rollback rehearsal workdir artifacts
  --signoff-require-zero-manual-workaround true|false
                                               Enforce fully automated signoff path
  -h, --help                                  Show help

Any extra args after `--` are forwarded to release_candidate_gate.sh.
USAGE
}

EXTRA_ARGS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode) MODE="$2"; shift 2 ;;
    --run-tag) RUN_TAG="$2"; shift 2 ;;
    --summary-dir) SUMMARY_DIR="$2"; shift 2 ;;
    --slo-profile) SLO_PROFILE="$2"; shift 2 ;;
    --slo-include-recovery-drill) SLO_INCLUDE_RECOVERY_DRILL="$2"; shift 2 ;;
    --run-benchmark-trend) RUN_BENCHMARK_TREND="$2"; shift 2 ;;
    --run-incident-simulation-guard) RUN_INCIDENT_SIMULATION_GUARD="$2"; shift 2 ;;
    --run-ingest-throughput-guard) RUN_INGEST_THROUGHPUT_GUARD="$2"; shift 2 ;;
    --ingest-min-rps) INGEST_MIN_RPS="$2"; shift 2 ;;
    --recovery-max-rto-seconds) RECOVERY_MAX_RTO_SECONDS="$2"; shift 2 ;;
    --run-replication-lag-guard) RUN_REPLICATION_LAG_GUARD="$2"; shift 2 ;;
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
    --storage-promotion-boundary-min-pass-count) STORAGE_PROMOTION_BOUNDARY_MIN_PASS_COUNT="$2"; shift 2 ;;
    --replication-lag-leader-metrics-url) REPLICATION_LAG_LEADER_METRICS_URL="$2"; shift 2 ;;
    --replication-lag-follower-metrics-url) REPLICATION_LAG_FOLLOWER_METRICS_URL="$2"; shift 2 ;;
    --replication-lag-max-claim-lag) REPLICATION_LAG_MAX_CLAIM_LAG="$2"; shift 2 ;;
    --replication-lag-max-pull-failures) REPLICATION_LAG_MAX_PULL_FAILURES="$2"; shift 2 ;;
    --replication-lag-min-pull-successes) REPLICATION_LAG_MIN_PULL_SUCCESSES="$2"; shift 2 ;;
    --replication-lag-require-no-last-error) REPLICATION_LAG_REQUIRE_NO_LAST_ERROR="$2"; shift 2 ;;
    --incident-failover-mode) INCIDENT_FAILOVER_MODE="$2"; shift 2 ;;
    --incident-failover-max-wait-seconds) INCIDENT_FAILOVER_MAX_WAIT_SECONDS="$2"; shift 2 ;;
    --incident-auth-max-wait-seconds) INCIDENT_AUTH_MAX_WAIT_SECONDS="$2"; shift 2 ;;
    --incident-recovery-max-rto-seconds) INCIDENT_RECOVERY_MAX_RTO_SECONDS="$2"; shift 2 ;;
    --run-failover-chaos-guard) RUN_FAILOVER_CHAOS_GUARD="$2"; shift 2 ;;
    --failover-chaos-loops) FAILOVER_CHAOS_LOOPS="$2"; shift 2 ;;
    --failover-chaos-mode) FAILOVER_CHAOS_MODE="$2"; shift 2 ;;
    --failover-chaos-max-failures) FAILOVER_CHAOS_MAX_FAILURES="$2"; shift 2 ;;
    --failover-chaos-max-wait-seconds) FAILOVER_CHAOS_MAX_WAIT_SECONDS="$2"; shift 2 ;;
    --failover-chaos-max-ingest-convergence-seconds) FAILOVER_CHAOS_MAX_INGEST_CONVERGENCE_SECONDS="$2"; shift 2 ;;
    --failover-chaos-max-retrieve-convergence-seconds) FAILOVER_CHAOS_MAX_RETRIEVE_CONVERGENCE_SECONDS="$2"; shift 2 ;;
    --failover-chaos-keep-drill-artifacts) FAILOVER_CHAOS_KEEP_DRILL_ARTIFACTS="$2"; shift 2 ;;
    --run-retrieval-latency-guard) RUN_RETRIEVAL_LATENCY_GUARD="$2"; shift 2 ;;
    --retrieval-latency-bind-addr) RETRIEVAL_LATENCY_BIND_ADDR="$2"; shift 2 ;;
    --retrieval-latency-workers-list) RETRIEVAL_LATENCY_WORKERS_LIST="$2"; shift 2 ;;
    --retrieval-latency-clients) RETRIEVAL_LATENCY_CLIENTS="$2"; shift 2 ;;
    --retrieval-latency-requests-per-worker) RETRIEVAL_LATENCY_REQUESTS_PER_WORKER="$2"; shift 2 ;;
    --retrieval-latency-warmup-requests) RETRIEVAL_LATENCY_WARMUP_REQUESTS="$2"; shift 2 ;;
    --retrieval-latency-connect-timeout-ms) RETRIEVAL_LATENCY_CONNECT_TIMEOUT_MS="$2"; shift 2 ;;
    --retrieval-latency-read-timeout-ms) RETRIEVAL_LATENCY_READ_TIMEOUT_MS="$2"; shift 2 ;;
    --retrieval-latency-max-p95-ms) RETRIEVAL_LATENCY_MAX_P95_MS="$2"; shift 2 ;;
    --retrieval-latency-max-p99-ms) RETRIEVAL_LATENCY_MAX_P99_MS="$2"; shift 2 ;;
    --retrieval-latency-min-success-rate-pct) RETRIEVAL_LATENCY_MIN_SUCCESS_RATE_PCT="$2"; shift 2 ;;
    --retrieval-latency-benchmark-output-dir) RETRIEVAL_LATENCY_BENCHMARK_OUTPUT_DIR="$2"; shift 2 ;;
    --run-read-consistency-policy-guard) RUN_READ_CONSISTENCY_POLICY_GUARD="$2"; shift 2 ;;
    --run-capacity-model-guard) RUN_CAPACITY_MODEL_GUARD="$2"; shift 2 ;;
    --capacity-model-history-path) CAPACITY_MODEL_HISTORY_PATH="$2"; shift 2 ;;
    --capacity-model-runs-dir) CAPACITY_MODEL_RUNS_DIR="$2"; shift 2 ;;
    --capacity-model-output-path) CAPACITY_MODEL_OUTPUT_PATH="$2"; shift 2 ;;
    --capacity-model-min-xlarge-fixture) CAPACITY_MODEL_MIN_XLARGE_FIXTURE="$2"; shift 2 ;;
    --capacity-model-min-xxlarge-fixture) CAPACITY_MODEL_MIN_XXLARGE_FIXTURE="$2"; shift 2 ;;
    --capacity-model-target-points) CAPACITY_MODEL_TARGET_POINTS="$2"; shift 2 ;;
    --capacity-model-replication-factor) CAPACITY_MODEL_REPLICATION_FACTOR="$2"; shift 2 ;;
    --run-production-signoff-bundle) RUN_PRODUCTION_SIGNOFF_BUNDLE="$2"; shift 2 ;;
    --signoff-concurrency-dir) PRODUCTION_SIGNOFF_CONCURRENCY_DIR="$2"; shift 2 ;;
    --signoff-recovery-artifact-dir) PRODUCTION_SIGNOFF_RECOVERY_ARTIFACT_DIR="$2"; shift 2 ;;
    --signoff-output-path) PRODUCTION_SIGNOFF_OUTPUT_PATH="$2"; shift 2 ;;
    --signoff-run-rollback-rehearsal) PRODUCTION_SIGNOFF_RUN_ROLLBACK_REHEARSAL="$2"; shift 2 ;;
    --signoff-rollback-max-rto-seconds) PRODUCTION_SIGNOFF_ROLLBACK_MAX_RTO_SECONDS="$2"; shift 2 ;;
    --signoff-rollback-keep-artifacts) PRODUCTION_SIGNOFF_ROLLBACK_KEEP_ARTIFACTS="$2"; shift 2 ;;
    --signoff-require-zero-manual-workaround) PRODUCTION_SIGNOFF_REQUIRE_ZERO_MANUAL_WORKAROUND="$2"; shift 2 ;;
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

for bool_opt in \
  "${SLO_INCLUDE_RECOVERY_DRILL}" \
  "${RUN_BENCHMARK_TREND}" \
  "${RUN_INCIDENT_SIMULATION_GUARD}" \
  "${RUN_INGEST_THROUGHPUT_GUARD}" \
  "${RUN_REPLICATION_LAG_GUARD}" \
  "${RUN_STORAGE_PROMOTION_BOUNDARY_GUARD}" \
  "${RUN_FAILOVER_CHAOS_GUARD}" \
  "${RUN_RETRIEVAL_LATENCY_GUARD}" \
  "${RUN_READ_CONSISTENCY_POLICY_GUARD}" \
  "${RUN_CAPACITY_MODEL_GUARD}" \
  "${RUN_PRODUCTION_SIGNOFF_BUNDLE}" \
  "${PRODUCTION_SIGNOFF_RUN_ROLLBACK_REHEARSAL}" \
  "${PRODUCTION_SIGNOFF_ROLLBACK_KEEP_ARTIFACTS}" \
  "${PRODUCTION_SIGNOFF_REQUIRE_ZERO_MANUAL_WORKAROUND}" \
  "${FAILOVER_CHAOS_KEEP_DRILL_ARTIFACTS}" \
  "${REPLICATION_LAG_REQUIRE_NO_LAST_ERROR}"; do
  case "${bool_opt}" in
    true|false) ;;
    *)
      echo "boolean options must be true or false" >&2
      exit 2
      ;;
  esac
done

for numeric in \
  "${INGEST_MIN_RPS}" \
  "${RECOVERY_MAX_RTO_SECONDS}" \
  "${REPLICATION_LAG_MAX_CLAIM_LAG}" \
  "${REPLICATION_LAG_MAX_PULL_FAILURES}" \
  "${REPLICATION_LAG_MIN_PULL_SUCCESSES}" \
  "${STORAGE_PROMOTION_BOUNDARY_WORKERS}" \
  "${STORAGE_PROMOTION_BOUNDARY_CLIENTS}" \
  "${STORAGE_PROMOTION_BOUNDARY_REQUESTS_PER_WORKER}" \
  "${STORAGE_PROMOTION_BOUNDARY_WARMUP_REQUESTS}" \
  "${STORAGE_PROMOTION_BOUNDARY_CONNECT_TIMEOUT_MS}" \
  "${STORAGE_PROMOTION_BOUNDARY_READ_TIMEOUT_MS}" \
  "${STORAGE_PROMOTION_BOUNDARY_CURL_TIMEOUT_SECONDS}" \
  "${STORAGE_PROMOTION_BOUNDARY_MIN_PASS_COUNT}" \
  "${INCIDENT_FAILOVER_MAX_WAIT_SECONDS}" \
  "${INCIDENT_AUTH_MAX_WAIT_SECONDS}" \
  "${INCIDENT_RECOVERY_MAX_RTO_SECONDS}" \
  "${FAILOVER_CHAOS_LOOPS}" \
  "${FAILOVER_CHAOS_MAX_FAILURES}" \
  "${FAILOVER_CHAOS_MAX_WAIT_SECONDS}" \
  "${FAILOVER_CHAOS_MAX_INGEST_CONVERGENCE_SECONDS}" \
  "${FAILOVER_CHAOS_MAX_RETRIEVE_CONVERGENCE_SECONDS}" \
  "${RETRIEVAL_LATENCY_CLIENTS}" \
  "${RETRIEVAL_LATENCY_REQUESTS_PER_WORKER}" \
  "${RETRIEVAL_LATENCY_WARMUP_REQUESTS}" \
  "${RETRIEVAL_LATENCY_CONNECT_TIMEOUT_MS}" \
  "${RETRIEVAL_LATENCY_READ_TIMEOUT_MS}" \
  "${RETRIEVAL_LATENCY_MAX_P95_MS}" \
  "${RETRIEVAL_LATENCY_MAX_P99_MS}" \
  "${RETRIEVAL_LATENCY_MIN_SUCCESS_RATE_PCT}" \
  "${CAPACITY_MODEL_MIN_XLARGE_FIXTURE}" \
  "${CAPACITY_MODEL_MIN_XXLARGE_FIXTURE}" \
  "${CAPACITY_MODEL_TARGET_POINTS}" \
  "${CAPACITY_MODEL_REPLICATION_FACTOR}" \
  "${PRODUCTION_SIGNOFF_ROLLBACK_MAX_RTO_SECONDS}"; do
  if [[ ! "${numeric}" =~ ^[0-9]+$ ]]; then
    echo "numeric options must be non-negative integers" >&2
    exit 2
  fi
done

if [[ "${RUN_REPLICATION_LAG_GUARD}" == "true" ]]; then
  if [[ -z "${REPLICATION_LAG_LEADER_METRICS_URL}" || -z "${REPLICATION_LAG_FOLLOWER_METRICS_URL}" ]]; then
    echo "replication lag guard is enabled but leader/follower metrics URLs are missing" >&2
    exit 2
  fi
fi

case "${FAILOVER_CHAOS_MODE}" in
  restart|no-restart|both) ;;
  *)
    echo "--failover-chaos-mode must be restart|no-restart|both" >&2
    exit 2
    ;;
esac

workers_list_valid="true"
IFS=',' read -r -a retrieval_workers_array <<< "${RETRIEVAL_LATENCY_WORKERS_LIST}"
for raw_worker in "${retrieval_workers_array[@]}"; do
  worker="$(printf '%s' "${raw_worker}" | tr -d '[:space:]')"
  if [[ -z "${worker}" || ! "${worker}" =~ ^[0-9]+$ || "${worker}" -eq 0 ]]; then
    workers_list_valid="false"
    break
  fi
done

if [[ "${workers_list_valid}" != "true" ]]; then
  echo "--retrieval-latency-workers-list must contain comma-separated positive integers" >&2
  exit 2
fi

if [[ "${RETRIEVAL_LATENCY_MIN_SUCCESS_RATE_PCT}" -gt 100 ]]; then
  echo "--retrieval-latency-min-success-rate-pct must be <= 100" >&2
  exit 2
fi
if [[ "${STORAGE_PROMOTION_BOUNDARY_MIN_PASS_COUNT}" -gt 3 ]]; then
  echo "--storage-promotion-boundary-min-pass-count must be <= 3" >&2
  exit 2
fi

echo "[phase11-core-gate] mode=${MODE}"
echo "[phase11-core-gate] run_tag=${RUN_TAG}"
echo "[phase11-core-gate] slo_profile=${SLO_PROFILE}"
echo "[phase11-core-gate] replication_lag_guard=${RUN_REPLICATION_LAG_GUARD}"
echo "[phase11-core-gate] storage_promotion_boundary_guard=${RUN_STORAGE_PROMOTION_BOUNDARY_GUARD}"
echo "[phase11-core-gate] storage_promotion_boundary_min_pass_count=${STORAGE_PROMOTION_BOUNDARY_MIN_PASS_COUNT}"
echo "[phase11-core-gate] failover_chaos_guard=${RUN_FAILOVER_CHAOS_GUARD}"
echo "[phase11-core-gate] retrieval_latency_guard=${RUN_RETRIEVAL_LATENCY_GUARD}"
echo "[phase11-core-gate] read_consistency_policy_guard=${RUN_READ_CONSISTENCY_POLICY_GUARD}"
echo "[phase11-core-gate] capacity_model_guard=${RUN_CAPACITY_MODEL_GUARD}"
echo "[phase11-core-gate] production_signoff_bundle=${RUN_PRODUCTION_SIGNOFF_BUNDLE}"
echo "[phase11-core-gate] incident_guard=${RUN_INCIDENT_SIMULATION_GUARD}"
echo "[phase11-core-gate] benchmark_trend=${RUN_BENCHMARK_TREND}"

cmd=(
  scripts/release_candidate_gate.sh
  --run-tag "${RUN_TAG}"
  --summary-dir "${SUMMARY_DIR}"
  --slo-profile "${SLO_PROFILE}"
  --slo-include-recovery-drill "${SLO_INCLUDE_RECOVERY_DRILL}"
  --run-benchmark-trend "${RUN_BENCHMARK_TREND}"
  --run-incident-simulation-guard "${RUN_INCIDENT_SIMULATION_GUARD}"
  --run-ingest-throughput-guard "${RUN_INGEST_THROUGHPUT_GUARD}"
  --ingest-min-rps "${INGEST_MIN_RPS}"
  --recovery-max-rto-seconds "${RECOVERY_MAX_RTO_SECONDS}"
  --run-replication-lag-guard "${RUN_REPLICATION_LAG_GUARD}"
  --run-storage-promotion-boundary-guard "${RUN_STORAGE_PROMOTION_BOUNDARY_GUARD}"
  --storage-promotion-boundary-bind-addr "${STORAGE_PROMOTION_BOUNDARY_BIND_ADDR}"
  --storage-promotion-boundary-ingest-bind-addr "${STORAGE_PROMOTION_BOUNDARY_INGEST_BIND_ADDR}"
  --storage-promotion-boundary-workers "${STORAGE_PROMOTION_BOUNDARY_WORKERS}"
  --storage-promotion-boundary-clients "${STORAGE_PROMOTION_BOUNDARY_CLIENTS}"
  --storage-promotion-boundary-requests-per-worker "${STORAGE_PROMOTION_BOUNDARY_REQUESTS_PER_WORKER}"
  --storage-promotion-boundary-warmup-requests "${STORAGE_PROMOTION_BOUNDARY_WARMUP_REQUESTS}"
  --storage-promotion-boundary-connect-timeout-ms "${STORAGE_PROMOTION_BOUNDARY_CONNECT_TIMEOUT_MS}"
  --storage-promotion-boundary-read-timeout-ms "${STORAGE_PROMOTION_BOUNDARY_READ_TIMEOUT_MS}"
  --storage-promotion-boundary-curl-timeout-seconds "${STORAGE_PROMOTION_BOUNDARY_CURL_TIMEOUT_SECONDS}"
  --storage-promotion-boundary-min-pass-count "${STORAGE_PROMOTION_BOUNDARY_MIN_PASS_COUNT}"
  --replication-lag-max-claim-lag "${REPLICATION_LAG_MAX_CLAIM_LAG}"
  --replication-lag-max-pull-failures "${REPLICATION_LAG_MAX_PULL_FAILURES}"
  --replication-lag-min-pull-successes "${REPLICATION_LAG_MIN_PULL_SUCCESSES}"
  --replication-lag-require-no-last-error "${REPLICATION_LAG_REQUIRE_NO_LAST_ERROR}"
  --incident-failover-mode "${INCIDENT_FAILOVER_MODE}"
  --incident-failover-max-wait-seconds "${INCIDENT_FAILOVER_MAX_WAIT_SECONDS}"
  --incident-auth-max-wait-seconds "${INCIDENT_AUTH_MAX_WAIT_SECONDS}"
  --incident-recovery-max-rto-seconds "${INCIDENT_RECOVERY_MAX_RTO_SECONDS}"
)

if [[ "${RUN_REPLICATION_LAG_GUARD}" == "true" ]]; then
  cmd+=(--replication-lag-leader-metrics-url "${REPLICATION_LAG_LEADER_METRICS_URL}")
  cmd+=(--replication-lag-follower-metrics-url "${REPLICATION_LAG_FOLLOWER_METRICS_URL}")
fi

if [[ "${#EXTRA_ARGS[@]}" -gt 0 ]]; then
  cmd+=("${EXTRA_ARGS[@]}")
fi

"${cmd[@]}"

if [[ "${RUN_FAILOVER_CHAOS_GUARD}" == "true" ]]; then
  chaos_cmd=(
    scripts/failover_chaos_loop.sh
    --loops "${FAILOVER_CHAOS_LOOPS}"
    --mode "${FAILOVER_CHAOS_MODE}"
    --max-failures "${FAILOVER_CHAOS_MAX_FAILURES}"
    --max-wait-seconds "${FAILOVER_CHAOS_MAX_WAIT_SECONDS}"
    --max-ingest-convergence-seconds "${FAILOVER_CHAOS_MAX_INGEST_CONVERGENCE_SECONDS}"
    --max-retrieve-convergence-seconds "${FAILOVER_CHAOS_MAX_RETRIEVE_CONVERGENCE_SECONDS}"
    --keep-drill-artifacts "${FAILOVER_CHAOS_KEEP_DRILL_ARTIFACTS}"
    --summary-dir "${SUMMARY_DIR}"
    --run-tag "${RUN_TAG}"
  )
  "${chaos_cmd[@]}"
fi

if [[ "${RUN_RETRIEVAL_LATENCY_GUARD}" == "true" ]]; then
  retrieval_guard_cmd=(
    scripts/retrieval_latency_guard.sh
    --bind-addr "${RETRIEVAL_LATENCY_BIND_ADDR}"
    --workers-list "${RETRIEVAL_LATENCY_WORKERS_LIST}"
    --clients "${RETRIEVAL_LATENCY_CLIENTS}"
    --requests-per-worker "${RETRIEVAL_LATENCY_REQUESTS_PER_WORKER}"
    --warmup-requests "${RETRIEVAL_LATENCY_WARMUP_REQUESTS}"
    --connect-timeout-ms "${RETRIEVAL_LATENCY_CONNECT_TIMEOUT_MS}"
    --read-timeout-ms "${RETRIEVAL_LATENCY_READ_TIMEOUT_MS}"
    --max-p95-ms "${RETRIEVAL_LATENCY_MAX_P95_MS}"
    --max-p99-ms "${RETRIEVAL_LATENCY_MAX_P99_MS}"
    --min-success-rate-pct "${RETRIEVAL_LATENCY_MIN_SUCCESS_RATE_PCT}"
    --benchmark-output-dir "${RETRIEVAL_LATENCY_BENCHMARK_OUTPUT_DIR}"
    --summary-dir "${SUMMARY_DIR}"
    --run-tag "${RUN_TAG}"
  )
  "${retrieval_guard_cmd[@]}"
fi

if [[ "${RUN_READ_CONSISTENCY_POLICY_GUARD}" == "true" ]]; then
  scripts/read_consistency_policy_guard.sh \
    --summary-dir "${SUMMARY_DIR}" \
    --run-tag "${RUN_TAG}"
fi

if [[ "${RUN_CAPACITY_MODEL_GUARD}" == "true" ]]; then
  scripts/capacity_model_report.sh \
    --history-path "${CAPACITY_MODEL_HISTORY_PATH}" \
    --runs-dir "${CAPACITY_MODEL_RUNS_DIR}" \
    --model-output-path "${CAPACITY_MODEL_OUTPUT_PATH}" \
    --summary-dir "${SUMMARY_DIR}" \
    --run-tag "${RUN_TAG}" \
    --min-xlarge-fixture "${CAPACITY_MODEL_MIN_XLARGE_FIXTURE}" \
    --min-xxlarge-fixture "${CAPACITY_MODEL_MIN_XXLARGE_FIXTURE}" \
    --target-points "${CAPACITY_MODEL_TARGET_POINTS}" \
    --replication-factor "${CAPACITY_MODEL_REPLICATION_FACTOR}"
fi

if [[ "${RUN_PRODUCTION_SIGNOFF_BUNDLE}" == "true" ]]; then
  signoff_cmd=(
    scripts/phase11_production_signoff_bundle.sh
    --run-tag "${RUN_TAG}"
    --summary-dir "${SUMMARY_DIR}"
    --concurrency-dir "${PRODUCTION_SIGNOFF_CONCURRENCY_DIR}"
    --recovery-artifact-dir "${PRODUCTION_SIGNOFF_RECOVERY_ARTIFACT_DIR}"
    --run-rollback-rehearsal "${PRODUCTION_SIGNOFF_RUN_ROLLBACK_REHEARSAL}"
    --rollback-max-rto-seconds "${PRODUCTION_SIGNOFF_ROLLBACK_MAX_RTO_SECONDS}"
    --rollback-keep-artifacts "${PRODUCTION_SIGNOFF_ROLLBACK_KEEP_ARTIFACTS}"
    --require-zero-manual-workaround "${PRODUCTION_SIGNOFF_REQUIRE_ZERO_MANUAL_WORKAROUND}"
    --promotion-command "scripts/phase11_core_staging_gate.sh --mode ${MODE} --run-tag ${RUN_TAG}"
  )
  if [[ -n "${PRODUCTION_SIGNOFF_OUTPUT_PATH}" ]]; then
    signoff_cmd+=(--output-path "${PRODUCTION_SIGNOFF_OUTPUT_PATH}")
  fi
  "${signoff_cmd[@]}"
fi
