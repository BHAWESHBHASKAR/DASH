#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

RUN_TAG="${DASH_RELEASE_GATE_RUN_TAG:-release-candidate}"
SUMMARY_DIR="${DASH_RELEASE_GATE_SUMMARY_DIR:-docs/benchmarks/history/runs}"
SLO_PROFILE="${DASH_RELEASE_GATE_SLO_PROFILE:-smoke}"
SLO_ITERATIONS="${DASH_RELEASE_GATE_SLO_ITERATIONS:-}"
SLO_HISTORY_PATH="${DASH_RELEASE_GATE_SLO_HISTORY_PATH:-docs/benchmarks/history/runs/slo-history.csv}"
SLO_INCLUDE_RECOVERY_DRILL="${DASH_RELEASE_GATE_SLO_INCLUDE_RECOVERY_DRILL:-false}"
RECOVERY_MAX_RTO_SECONDS="${DASH_RELEASE_GATE_RECOVERY_MAX_RTO_SECONDS:-60}"
RUN_INCIDENT_SIMULATION_GUARD="${DASH_RELEASE_GATE_RUN_INCIDENT_SIMULATION_GUARD:-false}"
INCIDENT_SIMULATION_FAILOVER_MODE="${DASH_RELEASE_GATE_INCIDENT_FAILOVER_MODE:-no-restart}"
INCIDENT_SIMULATION_FAILOVER_MAX_WAIT_SECONDS="${DASH_RELEASE_GATE_INCIDENT_FAILOVER_MAX_WAIT_SECONDS:-30}"
INCIDENT_SIMULATION_AUTH_MAX_WAIT_SECONDS="${DASH_RELEASE_GATE_INCIDENT_AUTH_MAX_WAIT_SECONDS:-30}"
INCIDENT_SIMULATION_RECOVERY_MAX_RTO_SECONDS="${DASH_RELEASE_GATE_INCIDENT_RECOVERY_MAX_RTO_SECONDS:-60}"
RUN_BENCH_TREND="${DASH_RELEASE_GATE_RUN_BENCH_TREND:-false}"
BENCH_INCLUDE_LARGE="${DASH_RELEASE_GATE_BENCH_INCLUDE_LARGE:-true}"
BENCH_INCLUDE_XLARGE="${DASH_RELEASE_GATE_BENCH_INCLUDE_XLARGE:-false}"
BENCH_INCLUDE_HYBRID="${DASH_RELEASE_GATE_BENCH_INCLUDE_HYBRID:-false}"
BENCH_HISTORY_PATH="${DASH_RELEASE_GATE_BENCH_HISTORY_PATH:-docs/benchmarks/history/benchmark-history.md}"
VERIFY_INGESTION_AUDIT_PATH="${DASH_RELEASE_GATE_VERIFY_INGESTION_AUDIT_PATH:-}"
VERIFY_RETRIEVAL_AUDIT_PATH="${DASH_RELEASE_GATE_VERIFY_RETRIEVAL_AUDIT_PATH:-}"
RUN_INGEST_THROUGHPUT_GUARD="${DASH_RELEASE_GATE_RUN_INGEST_THROUGHPUT_GUARD:-true}"
INGEST_THROUGHPUT_MIN_RPS="${DASH_RELEASE_GATE_INGEST_MIN_RPS:-100}"
INGEST_THROUGHPUT_BIND_ADDR="${DASH_RELEASE_GATE_INGEST_BIND_ADDR:-127.0.0.1:18080}"
INGEST_THROUGHPUT_WORKERS="${DASH_RELEASE_GATE_INGEST_WORKERS:-4}"
INGEST_THROUGHPUT_CLIENTS="${DASH_RELEASE_GATE_INGEST_CLIENTS:-16}"
INGEST_THROUGHPUT_REQUESTS_PER_WORKER="${DASH_RELEASE_GATE_INGEST_REQUESTS_PER_WORKER:-30}"
INGEST_THROUGHPUT_WARMUP_REQUESTS="${DASH_RELEASE_GATE_INGEST_WARMUP_REQUESTS:-5}"
INGEST_THROUGHPUT_WAL_SYNC_EVERY_RECORDS="${DASH_RELEASE_GATE_INGEST_WAL_SYNC_EVERY_RECORDS:-1}"
INGEST_THROUGHPUT_WAL_APPEND_BUFFER_RECORDS="${DASH_RELEASE_GATE_INGEST_WAL_APPEND_BUFFER_RECORDS:-1}"
INGEST_THROUGHPUT_WAL_SYNC_INTERVAL_MS="${DASH_RELEASE_GATE_INGEST_WAL_SYNC_INTERVAL_MS:-off}"
INGEST_THROUGHPUT_WAL_ASYNC_FLUSH_INTERVAL_MS="${DASH_RELEASE_GATE_INGEST_WAL_ASYNC_FLUSH_INTERVAL_MS:-auto}"
INGEST_THROUGHPUT_WAL_BACKGROUND_FLUSH_ONLY="${DASH_RELEASE_GATE_INGEST_WAL_BACKGROUND_FLUSH_ONLY:-false}"
INGEST_THROUGHPUT_ALLOW_UNSAFE_WAL_DURABILITY="${DASH_RELEASE_GATE_INGEST_ALLOW_UNSAFE_WAL_DURABILITY:-false}"
RUN_REPLICATION_LAG_GUARD="${DASH_RELEASE_GATE_RUN_REPLICATION_LAG_GUARD:-false}"
REPLICATION_LAG_LEADER_METRICS_URL="${DASH_RELEASE_GATE_REPLICATION_LAG_LEADER_METRICS_URL:-}"
REPLICATION_LAG_FOLLOWER_METRICS_URL="${DASH_RELEASE_GATE_REPLICATION_LAG_FOLLOWER_METRICS_URL:-}"
REPLICATION_LAG_MAX_CLAIM_LAG="${DASH_RELEASE_GATE_REPLICATION_LAG_MAX_CLAIM_LAG:-0}"
REPLICATION_LAG_MAX_PULL_FAILURES="${DASH_RELEASE_GATE_REPLICATION_LAG_MAX_PULL_FAILURES:-0}"
REPLICATION_LAG_MIN_PULL_SUCCESSES="${DASH_RELEASE_GATE_REPLICATION_LAG_MIN_PULL_SUCCESSES:-1}"
REPLICATION_LAG_REQUIRE_NO_LAST_ERROR="${DASH_RELEASE_GATE_REPLICATION_LAG_REQUIRE_NO_LAST_ERROR:-true}"
RUN_STORAGE_PROMOTION_BOUNDARY_GUARD="${DASH_RELEASE_GATE_RUN_STORAGE_PROMOTION_BOUNDARY_GUARD:-false}"
STORAGE_PROMOTION_BOUNDARY_BIND_ADDR="${DASH_RELEASE_GATE_STORAGE_PROMOTION_BOUNDARY_BIND_ADDR:-127.0.0.1:18080}"
STORAGE_PROMOTION_BOUNDARY_INGEST_BIND_ADDR="${DASH_RELEASE_GATE_STORAGE_PROMOTION_BOUNDARY_INGEST_BIND_ADDR:-127.0.0.1:18081}"
STORAGE_PROMOTION_BOUNDARY_WORKERS="${DASH_RELEASE_GATE_STORAGE_PROMOTION_BOUNDARY_WORKERS:-4}"
STORAGE_PROMOTION_BOUNDARY_CLIENTS="${DASH_RELEASE_GATE_STORAGE_PROMOTION_BOUNDARY_CLIENTS:-16}"
STORAGE_PROMOTION_BOUNDARY_REQUESTS_PER_WORKER="${DASH_RELEASE_GATE_STORAGE_PROMOTION_BOUNDARY_REQUESTS_PER_WORKER:-20}"
STORAGE_PROMOTION_BOUNDARY_WARMUP_REQUESTS="${DASH_RELEASE_GATE_STORAGE_PROMOTION_BOUNDARY_WARMUP_REQUESTS:-5}"
STORAGE_PROMOTION_BOUNDARY_CONNECT_TIMEOUT_MS="${DASH_RELEASE_GATE_STORAGE_PROMOTION_BOUNDARY_CONNECT_TIMEOUT_MS:-2000}"
STORAGE_PROMOTION_BOUNDARY_READ_TIMEOUT_MS="${DASH_RELEASE_GATE_STORAGE_PROMOTION_BOUNDARY_READ_TIMEOUT_MS:-5000}"
STORAGE_PROMOTION_BOUNDARY_CURL_TIMEOUT_SECONDS="${DASH_RELEASE_GATE_STORAGE_PROMOTION_BOUNDARY_CURL_TIMEOUT_SECONDS:-10}"
STORAGE_PROMOTION_BOUNDARY_MIN_PASS_COUNT="${DASH_RELEASE_GATE_STORAGE_PROMOTION_BOUNDARY_MIN_PASS_COUNT:-0}"
RUN_SEGMENT_WAL_PARITY_GUARD="${DASH_RELEASE_GATE_RUN_SEGMENT_WAL_PARITY_GUARD:-false}"
SEGMENT_WAL_PARITY_INGEST_BIND_ADDR="${DASH_RELEASE_GATE_SEGMENT_WAL_PARITY_INGEST_BIND_ADDR:-127.0.0.1:18281}"
SEGMENT_WAL_PARITY_RETRIEVE_BIND_ADDR="${DASH_RELEASE_GATE_SEGMENT_WAL_PARITY_RETRIEVE_BIND_ADDR:-127.0.0.1:18280}"
SEGMENT_WAL_PARITY_TOP_K="${DASH_RELEASE_GATE_SEGMENT_WAL_PARITY_TOP_K:-5}"

usage() {
  cat <<'USAGE'
Usage: scripts/release_candidate_gate.sh [options]

Run DASH release-candidate gates and write a summary artifact.

Options:
  --run-tag TAG                        Run tag for artifact naming
  --summary-dir DIR                    Directory for release gate summary/output logs
  --slo-profile NAME                   SLO benchmark profile (default: smoke)
  --slo-iterations N                   Optional SLO benchmark iteration override
  --slo-history-path PATH              SLO history CSV output path
  --slo-include-recovery-drill true|false
                                       Include recovery signal inside slo_guard step
  --recovery-max-rto-seconds N         Max allowed recovery drill RTO seconds
  --run-incident-simulation-guard true|false
                                       Run failover + auth revocation + recovery simulation gate
  --incident-failover-mode MODE        Incident failover mode: restart|no-restart|both
  --incident-failover-max-wait-seconds N
                                       Incident failover drill health/poll timeout
  --incident-auth-max-wait-seconds N   Incident auth revocation drill health timeout
  --incident-recovery-max-rto-seconds N
                                       Incident recovery drill max RTO threshold
  --run-benchmark-trend true|false     Run benchmark_trend as part of gate
  --bench-include-large true|false     Include large profile in benchmark_trend
  --bench-include-xlarge true|false    Include xlarge profile in benchmark_trend
  --bench-include-hybrid true|false    Include hybrid profile in benchmark_trend
  --bench-history-path PATH            Benchmark history markdown path
  --verify-ingestion-audit PATH        Run audit chain verify for ingestion log
  --verify-retrieval-audit PATH        Run audit chain verify for retrieval log
  --run-ingest-throughput-guard true|false
                                       Enable/disable ingestion throughput release gate
  --ingest-min-rps N                   Minimum ingestion throughput (requests/second)
  --ingest-bind-addr HOST:PORT         Ingestion benchmark bind address
  --ingest-workers N                   Ingestion benchmark transport worker count
  --ingest-clients N                   Ingestion benchmark concurrent client count
  --ingest-requests-per-worker N       Ingestion benchmark requests per worker
  --ingest-warmup-requests N           Ingestion benchmark warmup requests
  --ingest-wal-sync-every-records N    Ingestion benchmark WAL sync threshold
  --ingest-wal-append-buffer-records N Ingestion benchmark WAL append-buffer threshold
  --ingest-wal-sync-interval-ms N|off  Ingestion benchmark WAL interval sync policy
  --ingest-wal-async-flush-interval-ms N|off|auto
                                       Ingestion benchmark async flush worker interval
  --ingest-wal-background-flush-only true|false
                                       Ingestion benchmark background-only WAL flush mode
  --ingest-allow-unsafe-wal-durability true|false
                                       Ingestion benchmark unsafe WAL durability override
  --run-replication-lag-guard true|false
                                       Enable follower replication lag guard
  --replication-lag-leader-metrics-url URL
                                       Leader ingestion metrics URL
  --replication-lag-follower-metrics-url URL
                                       Follower ingestion metrics URL
  --replication-lag-max-claim-lag N    Max allowed leader-follower claim lag
  --replication-lag-max-pull-failures N
                                       Max allowed follower replication pull failures
  --replication-lag-min-pull-successes N
                                       Min required follower replication pull successes
  --replication-lag-require-no-last-error true|false
                                       Require follower replication_last_error == 0
  --run-storage-promotion-boundary-guard true|false
                                       Enable retrieval promotion-boundary under-load gate
  --storage-promotion-boundary-bind-addr HOST:PORT
                                       Retrieval bind address for promotion-boundary guard
  --storage-promotion-boundary-ingest-bind-addr HOST:PORT
                                       Ingestion bind address for promotion-boundary bootstrap
  --storage-promotion-boundary-workers N
                                       Retrieval workers for promotion-boundary guard
  --storage-promotion-boundary-clients N
                                       Concurrent clients for promotion-boundary guard
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
  --run-segment-wal-parity-guard true|false
                                       Enable deterministic replay-vs-segment parity gate
  --segment-wal-parity-ingest-bind-addr HOST:PORT
                                       Ingestion bind address for parity gate
  --segment-wal-parity-retrieve-bind-addr HOST:PORT
                                       Retrieval bind address for parity gate
  --segment-wal-parity-top-k N
                                       top_k value for parity gate queries
  -h, --help                           Show help

Environment:
  DASH_RELEASE_GATE_* variables are supported for all options.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --run-tag)
      RUN_TAG="$2"
      shift 2
      ;;
    --summary-dir)
      SUMMARY_DIR="$2"
      shift 2
      ;;
    --slo-profile)
      SLO_PROFILE="$2"
      shift 2
      ;;
    --slo-iterations)
      SLO_ITERATIONS="$2"
      shift 2
      ;;
    --slo-history-path)
      SLO_HISTORY_PATH="$2"
      shift 2
      ;;
    --slo-include-recovery-drill)
      SLO_INCLUDE_RECOVERY_DRILL="$2"
      shift 2
      ;;
    --recovery-max-rto-seconds)
      RECOVERY_MAX_RTO_SECONDS="$2"
      shift 2
      ;;
    --run-incident-simulation-guard)
      RUN_INCIDENT_SIMULATION_GUARD="$2"
      shift 2
      ;;
    --incident-failover-mode)
      INCIDENT_SIMULATION_FAILOVER_MODE="$2"
      shift 2
      ;;
    --incident-failover-max-wait-seconds)
      INCIDENT_SIMULATION_FAILOVER_MAX_WAIT_SECONDS="$2"
      shift 2
      ;;
    --incident-auth-max-wait-seconds)
      INCIDENT_SIMULATION_AUTH_MAX_WAIT_SECONDS="$2"
      shift 2
      ;;
    --incident-recovery-max-rto-seconds)
      INCIDENT_SIMULATION_RECOVERY_MAX_RTO_SECONDS="$2"
      shift 2
      ;;
    --run-benchmark-trend)
      RUN_BENCH_TREND="$2"
      shift 2
      ;;
    --bench-include-large)
      BENCH_INCLUDE_LARGE="$2"
      shift 2
      ;;
    --bench-include-xlarge)
      BENCH_INCLUDE_XLARGE="$2"
      shift 2
      ;;
    --bench-include-hybrid)
      BENCH_INCLUDE_HYBRID="$2"
      shift 2
      ;;
    --bench-history-path)
      BENCH_HISTORY_PATH="$2"
      shift 2
      ;;
    --verify-ingestion-audit)
      VERIFY_INGESTION_AUDIT_PATH="$2"
      shift 2
      ;;
    --verify-retrieval-audit)
      VERIFY_RETRIEVAL_AUDIT_PATH="$2"
      shift 2
      ;;
    --run-ingest-throughput-guard)
      RUN_INGEST_THROUGHPUT_GUARD="$2"
      shift 2
      ;;
    --ingest-min-rps)
      INGEST_THROUGHPUT_MIN_RPS="$2"
      shift 2
      ;;
    --ingest-bind-addr)
      INGEST_THROUGHPUT_BIND_ADDR="$2"
      shift 2
      ;;
    --ingest-workers)
      INGEST_THROUGHPUT_WORKERS="$2"
      shift 2
      ;;
    --ingest-clients)
      INGEST_THROUGHPUT_CLIENTS="$2"
      shift 2
      ;;
    --ingest-requests-per-worker)
      INGEST_THROUGHPUT_REQUESTS_PER_WORKER="$2"
      shift 2
      ;;
    --ingest-warmup-requests)
      INGEST_THROUGHPUT_WARMUP_REQUESTS="$2"
      shift 2
      ;;
    --ingest-wal-sync-every-records)
      INGEST_THROUGHPUT_WAL_SYNC_EVERY_RECORDS="$2"
      shift 2
      ;;
    --ingest-wal-append-buffer-records)
      INGEST_THROUGHPUT_WAL_APPEND_BUFFER_RECORDS="$2"
      shift 2
      ;;
    --ingest-wal-sync-interval-ms)
      INGEST_THROUGHPUT_WAL_SYNC_INTERVAL_MS="$2"
      shift 2
      ;;
    --ingest-wal-async-flush-interval-ms)
      INGEST_THROUGHPUT_WAL_ASYNC_FLUSH_INTERVAL_MS="$2"
      shift 2
      ;;
    --ingest-wal-background-flush-only)
      INGEST_THROUGHPUT_WAL_BACKGROUND_FLUSH_ONLY="$2"
      shift 2
      ;;
    --ingest-allow-unsafe-wal-durability)
      INGEST_THROUGHPUT_ALLOW_UNSAFE_WAL_DURABILITY="$2"
      shift 2
      ;;
    --run-replication-lag-guard)
      RUN_REPLICATION_LAG_GUARD="$2"
      shift 2
      ;;
    --run-storage-promotion-boundary-guard)
      RUN_STORAGE_PROMOTION_BOUNDARY_GUARD="$2"
      shift 2
      ;;
    --storage-promotion-boundary-bind-addr)
      STORAGE_PROMOTION_BOUNDARY_BIND_ADDR="$2"
      shift 2
      ;;
    --storage-promotion-boundary-ingest-bind-addr)
      STORAGE_PROMOTION_BOUNDARY_INGEST_BIND_ADDR="$2"
      shift 2
      ;;
    --storage-promotion-boundary-workers)
      STORAGE_PROMOTION_BOUNDARY_WORKERS="$2"
      shift 2
      ;;
    --storage-promotion-boundary-clients)
      STORAGE_PROMOTION_BOUNDARY_CLIENTS="$2"
      shift 2
      ;;
    --storage-promotion-boundary-requests-per-worker)
      STORAGE_PROMOTION_BOUNDARY_REQUESTS_PER_WORKER="$2"
      shift 2
      ;;
    --storage-promotion-boundary-warmup-requests)
      STORAGE_PROMOTION_BOUNDARY_WARMUP_REQUESTS="$2"
      shift 2
      ;;
    --storage-promotion-boundary-connect-timeout-ms)
      STORAGE_PROMOTION_BOUNDARY_CONNECT_TIMEOUT_MS="$2"
      shift 2
      ;;
    --storage-promotion-boundary-read-timeout-ms)
      STORAGE_PROMOTION_BOUNDARY_READ_TIMEOUT_MS="$2"
      shift 2
      ;;
    --storage-promotion-boundary-curl-timeout-seconds)
      STORAGE_PROMOTION_BOUNDARY_CURL_TIMEOUT_SECONDS="$2"
      shift 2
      ;;
    --storage-promotion-boundary-min-pass-count)
      STORAGE_PROMOTION_BOUNDARY_MIN_PASS_COUNT="$2"
      shift 2
      ;;
    --run-segment-wal-parity-guard)
      RUN_SEGMENT_WAL_PARITY_GUARD="$2"
      shift 2
      ;;
    --segment-wal-parity-ingest-bind-addr)
      SEGMENT_WAL_PARITY_INGEST_BIND_ADDR="$2"
      shift 2
      ;;
    --segment-wal-parity-retrieve-bind-addr)
      SEGMENT_WAL_PARITY_RETRIEVE_BIND_ADDR="$2"
      shift 2
      ;;
    --segment-wal-parity-top-k)
      SEGMENT_WAL_PARITY_TOP_K="$2"
      shift 2
      ;;
    --replication-lag-leader-metrics-url)
      REPLICATION_LAG_LEADER_METRICS_URL="$2"
      shift 2
      ;;
    --replication-lag-follower-metrics-url)
      REPLICATION_LAG_FOLLOWER_METRICS_URL="$2"
      shift 2
      ;;
    --replication-lag-max-claim-lag)
      REPLICATION_LAG_MAX_CLAIM_LAG="$2"
      shift 2
      ;;
    --replication-lag-max-pull-failures)
      REPLICATION_LAG_MAX_PULL_FAILURES="$2"
      shift 2
      ;;
    --replication-lag-min-pull-successes)
      REPLICATION_LAG_MIN_PULL_SUCCESSES="$2"
      shift 2
      ;;
    --replication-lag-require-no-last-error)
      REPLICATION_LAG_REQUIRE_NO_LAST_ERROR="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[release-gate] unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

validate_bool() {
  local key="$1"
  local value="$2"
  case "${value}" in
    true|false) ;;
    *)
      echo "[release-gate] ${key} must be true|false (got '${value}')" >&2
      exit 2
      ;;
  esac
}

validate_bool "--slo-include-recovery-drill" "${SLO_INCLUDE_RECOVERY_DRILL}"
validate_bool "--run-incident-simulation-guard" "${RUN_INCIDENT_SIMULATION_GUARD}"
validate_bool "--run-benchmark-trend" "${RUN_BENCH_TREND}"
validate_bool "--bench-include-large" "${BENCH_INCLUDE_LARGE}"
validate_bool "--bench-include-xlarge" "${BENCH_INCLUDE_XLARGE}"
validate_bool "--bench-include-hybrid" "${BENCH_INCLUDE_HYBRID}"
validate_bool "--run-ingest-throughput-guard" "${RUN_INGEST_THROUGHPUT_GUARD}"
validate_bool "--ingest-wal-background-flush-only" "${INGEST_THROUGHPUT_WAL_BACKGROUND_FLUSH_ONLY}"
validate_bool "--ingest-allow-unsafe-wal-durability" "${INGEST_THROUGHPUT_ALLOW_UNSAFE_WAL_DURABILITY}"
validate_bool "--run-replication-lag-guard" "${RUN_REPLICATION_LAG_GUARD}"
validate_bool "--replication-lag-require-no-last-error" "${REPLICATION_LAG_REQUIRE_NO_LAST_ERROR}"
validate_bool "--run-storage-promotion-boundary-guard" "${RUN_STORAGE_PROMOTION_BOUNDARY_GUARD}"
validate_bool "--run-segment-wal-parity-guard" "${RUN_SEGMENT_WAL_PARITY_GUARD}"

if [[ -n "${SLO_ITERATIONS}" && ! "${SLO_ITERATIONS}" =~ ^[0-9]+$ ]]; then
  echo "[release-gate] --slo-iterations must be a non-negative integer" >&2
  exit 2
fi
if [[ ! "${RECOVERY_MAX_RTO_SECONDS}" =~ ^[0-9]+$ ]]; then
  echo "[release-gate] --recovery-max-rto-seconds must be a non-negative integer" >&2
  exit 2
fi
if [[ ! "${INCIDENT_SIMULATION_FAILOVER_MAX_WAIT_SECONDS}" =~ ^[0-9]+$ || "${INCIDENT_SIMULATION_FAILOVER_MAX_WAIT_SECONDS}" -eq 0 ]]; then
  echo "[release-gate] --incident-failover-max-wait-seconds must be a positive integer" >&2
  exit 2
fi
if [[ ! "${INCIDENT_SIMULATION_AUTH_MAX_WAIT_SECONDS}" =~ ^[0-9]+$ || "${INCIDENT_SIMULATION_AUTH_MAX_WAIT_SECONDS}" -eq 0 ]]; then
  echo "[release-gate] --incident-auth-max-wait-seconds must be a positive integer" >&2
  exit 2
fi
if [[ ! "${INCIDENT_SIMULATION_RECOVERY_MAX_RTO_SECONDS}" =~ ^[0-9]+$ || "${INCIDENT_SIMULATION_RECOVERY_MAX_RTO_SECONDS}" -eq 0 ]]; then
  echo "[release-gate] --incident-recovery-max-rto-seconds must be a positive integer" >&2
  exit 2
fi
case "${INCIDENT_SIMULATION_FAILOVER_MODE}" in
  restart|no-restart|both) ;;
  *)
    echo "[release-gate] --incident-failover-mode must be one of: restart, no-restart, both" >&2
    exit 2
    ;;
esac
for int_opt in \
  "${INGEST_THROUGHPUT_MIN_RPS}" \
  "${INGEST_THROUGHPUT_WORKERS}" \
  "${INGEST_THROUGHPUT_CLIENTS}" \
  "${INGEST_THROUGHPUT_REQUESTS_PER_WORKER}" \
  "${INGEST_THROUGHPUT_WARMUP_REQUESTS}" \
  "${INGEST_THROUGHPUT_WAL_SYNC_EVERY_RECORDS}" \
  "${INGEST_THROUGHPUT_WAL_APPEND_BUFFER_RECORDS}" \
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
  "${SEGMENT_WAL_PARITY_TOP_K}"; do
  if [[ ! "${int_opt}" =~ ^[0-9]+$ ]]; then
    echo "[release-gate] ingestion throughput/replication lag options must be non-negative integers" >&2
    exit 2
  fi
done
if [[ "${INGEST_THROUGHPUT_MIN_RPS}" -le 0 ]]; then
  echo "[release-gate] --ingest-min-rps must be > 0" >&2
  exit 2
fi
if [[ "${STORAGE_PROMOTION_BOUNDARY_WORKERS}" -eq 0 || "${STORAGE_PROMOTION_BOUNDARY_CLIENTS}" -eq 0 || "${STORAGE_PROMOTION_BOUNDARY_REQUESTS_PER_WORKER}" -eq 0 || "${STORAGE_PROMOTION_BOUNDARY_CONNECT_TIMEOUT_MS}" -eq 0 || "${STORAGE_PROMOTION_BOUNDARY_READ_TIMEOUT_MS}" -eq 0 || "${STORAGE_PROMOTION_BOUNDARY_CURL_TIMEOUT_SECONDS}" -eq 0 ]]; then
  echo "[release-gate] storage promotion boundary guard workers/clients/requests/timeouts must be > 0" >&2
  exit 2
fi
if [[ "${STORAGE_PROMOTION_BOUNDARY_MIN_PASS_COUNT}" -gt 3 ]]; then
  echo "[release-gate] --storage-promotion-boundary-min-pass-count must be <= 3 (guard has 3 scenarios)" >&2
  exit 2
fi
if [[ "${SEGMENT_WAL_PARITY_TOP_K}" -eq 0 ]]; then
  echo "[release-gate] --segment-wal-parity-top-k must be > 0" >&2
  exit 2
fi
if [[ "${INGEST_THROUGHPUT_WORKERS}" -eq 0 || "${INGEST_THROUGHPUT_CLIENTS}" -eq 0 || "${INGEST_THROUGHPUT_REQUESTS_PER_WORKER}" -eq 0 || "${INGEST_THROUGHPUT_WAL_SYNC_EVERY_RECORDS}" -eq 0 || "${INGEST_THROUGHPUT_WAL_APPEND_BUFFER_RECORDS}" -eq 0 ]]; then
  echo "[release-gate] ingestion throughput workers/clients/requests/wal thresholds must be > 0" >&2
  exit 2
fi
case "${INGEST_THROUGHPUT_WAL_SYNC_INTERVAL_MS}" in
  off|none|unset|"") INGEST_THROUGHPUT_WAL_SYNC_INTERVAL_MS="off" ;;
  *)
    if [[ ! "${INGEST_THROUGHPUT_WAL_SYNC_INTERVAL_MS}" =~ ^[0-9]+$ || "${INGEST_THROUGHPUT_WAL_SYNC_INTERVAL_MS}" -eq 0 ]]; then
      echo "[release-gate] --ingest-wal-sync-interval-ms must be positive integer or off" >&2
      exit 2
    fi
    ;;
esac
case "${INGEST_THROUGHPUT_WAL_ASYNC_FLUSH_INTERVAL_MS}" in
  auto) INGEST_THROUGHPUT_WAL_ASYNC_FLUSH_INTERVAL_MS="auto" ;;
  off|none|unset|"") INGEST_THROUGHPUT_WAL_ASYNC_FLUSH_INTERVAL_MS="off" ;;
  *)
    if [[ ! "${INGEST_THROUGHPUT_WAL_ASYNC_FLUSH_INTERVAL_MS}" =~ ^[0-9]+$ || "${INGEST_THROUGHPUT_WAL_ASYNC_FLUSH_INTERVAL_MS}" -eq 0 ]]; then
      echo "[release-gate] --ingest-wal-async-flush-interval-ms must be positive integer, auto, or off" >&2
      exit 2
    fi
    ;;
esac
if [[ "${INGEST_THROUGHPUT_WAL_BACKGROUND_FLUSH_ONLY}" == "true" && "${INGEST_THROUGHPUT_WAL_ASYNC_FLUSH_INTERVAL_MS}" == "off" ]]; then
  echo "[release-gate] --ingest-wal-background-flush-only=true requires async flush worker (set --ingest-wal-async-flush-interval-ms to auto or a positive integer)" >&2
  exit 2
fi
if [[ "${RUN_REPLICATION_LAG_GUARD}" == "true" ]]; then
  if [[ -z "${REPLICATION_LAG_LEADER_METRICS_URL}" || -z "${REPLICATION_LAG_FOLLOWER_METRICS_URL}" ]]; then
    echo "[release-gate] replication lag guard enabled but leader/follower metrics URLs are missing" >&2
    exit 2
  fi
fi

mkdir -p "${SUMMARY_DIR}" "$(dirname "${SLO_HISTORY_PATH}")"

RUN_STAMP="$(date -u +%Y%m%d-%H%M%S)"
RUN_UTC="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
RUN_ID="${RUN_STAMP}-${RUN_TAG}"
SUMMARY_PATH="${SUMMARY_DIR}/${RUN_ID}-release-gate.md"
LOG_DIR="${SUMMARY_DIR}/${RUN_ID}-logs"
ROWS_TMP="$(mktemp)"
DETAILS_TMP="$(mktemp)"
mkdir -p "${LOG_DIR}"

PASS_COUNT=0
FAIL_COUNT=0
SKIP_COUNT=0
FAILED=false

cleanup() {
  rm -f "${ROWS_TMP}" "${DETAILS_TMP}"
}
trap cleanup EXIT

append_detail() {
  local step="$1"
  local status="$2"
  local command_text="$3"
  local duration="$4"
  local log_path="$5"

  {
    echo
    echo "## ${step}"
    echo
    echo "- status: ${status}"
    echo "- duration_s: ${duration}"
    echo "- command: \`${command_text}\`"
    echo "- log: \`${log_path}\`"
    echo
    echo '```text'
    if [[ -s "${log_path}" ]]; then
      tail -n 80 "${log_path}"
    else
      echo "(no output)"
    fi
    echo '```'
  } >> "${DETAILS_TMP}"
}

run_step() {
  local step="$1"
  local command_text="$2"
  shift 2

  local safe_step
  safe_step="${step// /-}"
  safe_step="${safe_step//\//-}"
  local log_path="${LOG_DIR}/${safe_step}.log"

  local started finished duration exit_code
  started="$(date -u +%s)"

  echo "[release-gate] ${step}"
  echo "[release-gate] command: ${command_text}"

  local output=""
  if output="$("$@" 2>&1)"; then
    exit_code=0
  else
    exit_code=$?
  fi
  printf '%s\n' "${output}" | tee "${log_path}"

  finished="$(date -u +%s)"
  duration=$((finished - started))

  if [[ ${exit_code} -eq 0 ]]; then
    PASS_COUNT=$((PASS_COUNT + 1))
    printf '| %s | PASS | %s | `%s` |\n' "${step}" "${duration}" "${command_text}" >> "${ROWS_TMP}"
    append_detail "${step}" "PASS" "${command_text}" "${duration}" "${log_path}"
  else
    FAIL_COUNT=$((FAIL_COUNT + 1))
    FAILED=true
    printf '| %s | FAIL | %s | `%s` |\n' "${step}" "${duration}" "${command_text}" >> "${ROWS_TMP}"
    append_detail "${step}" "FAIL (exit ${exit_code})" "${command_text}" "${duration}" "${log_path}"
  fi
}

skip_step() {
  local step="$1"
  local command_text="$2"
  local reason="$3"
  local safe_step
  safe_step="${step// /-}"
  safe_step="${safe_step//\//-}"
  local log_path="${LOG_DIR}/${safe_step}.log"

  echo "[release-gate] ${step}: skipped (${reason})"
  printf '%s\n' "skipped: ${reason}" > "${log_path}"
  SKIP_COUNT=$((SKIP_COUNT + 1))
  printf '| %s | SKIP | - | `%s` |\n' "${step}" "${command_text}" >> "${ROWS_TMP}"
  append_detail "${step}" "SKIP (${reason})" "${command_text}" "-" "${log_path}"
}

run_step "cargo fmt" "cargo fmt --all --check" \
  cargo fmt --all --check
run_step "cargo clippy" "cargo clippy --workspace --all-targets -- -D warnings" \
  cargo clippy --workspace --all-targets -- -D warnings
run_step "cargo test" "cargo test --workspace" \
  cargo test --workspace
run_step "ci pipeline" "scripts/ci.sh" \
  scripts/ci.sh

SLO_CMD=(
  scripts/slo_guard.sh
  --profile "${SLO_PROFILE}"
  --run-tag "${RUN_TAG}"
  --slo-history-path "${SLO_HISTORY_PATH}"
  --summary-dir "${SUMMARY_DIR}"
  --include-recovery-drill "${SLO_INCLUDE_RECOVERY_DRILL}"
)
if [[ -n "${SLO_ITERATIONS}" ]]; then
  SLO_CMD+=(--iterations "${SLO_ITERATIONS}")
fi
run_step "slo guard" "${SLO_CMD[*]}" "${SLO_CMD[@]}"

run_step "recovery drill" "scripts/recovery_drill.sh --max-rto-seconds ${RECOVERY_MAX_RTO_SECONDS}" \
  scripts/recovery_drill.sh --max-rto-seconds "${RECOVERY_MAX_RTO_SECONDS}"

if [[ "${RUN_REPLICATION_LAG_GUARD}" == "true" ]]; then
  REPL_GUARD_CMD=(
    scripts/replication_lag_guard.sh
    --leader-metrics-url "${REPLICATION_LAG_LEADER_METRICS_URL}"
    --follower-metrics-url "${REPLICATION_LAG_FOLLOWER_METRICS_URL}"
    --max-claim-lag "${REPLICATION_LAG_MAX_CLAIM_LAG}"
    --max-pull-failures "${REPLICATION_LAG_MAX_PULL_FAILURES}"
    --min-pull-successes "${REPLICATION_LAG_MIN_PULL_SUCCESSES}"
    --require-no-last-error "${REPLICATION_LAG_REQUIRE_NO_LAST_ERROR}"
    --summary-dir "${SUMMARY_DIR}"
    --run-tag "${RUN_TAG}"
  )
  run_step "replication lag guard" "${REPL_GUARD_CMD[*]}" "${REPL_GUARD_CMD[@]}"
else
  skip_step "replication lag guard" "scripts/replication_lag_guard.sh --leader-metrics-url <leader> --follower-metrics-url <follower>" "disabled"
fi

if [[ "${RUN_STORAGE_PROMOTION_BOUNDARY_GUARD}" == "true" ]]; then
  STORAGE_PROMOTION_CMD=(
    scripts/storage_promotion_boundary_guard.sh
    --bind-addr "${STORAGE_PROMOTION_BOUNDARY_BIND_ADDR}"
    --ingest-bind-addr "${STORAGE_PROMOTION_BOUNDARY_INGEST_BIND_ADDR}"
    --workers "${STORAGE_PROMOTION_BOUNDARY_WORKERS}"
    --clients "${STORAGE_PROMOTION_BOUNDARY_CLIENTS}"
    --requests-per-worker "${STORAGE_PROMOTION_BOUNDARY_REQUESTS_PER_WORKER}"
    --warmup-requests "${STORAGE_PROMOTION_BOUNDARY_WARMUP_REQUESTS}"
    --connect-timeout-ms "${STORAGE_PROMOTION_BOUNDARY_CONNECT_TIMEOUT_MS}"
    --read-timeout-ms "${STORAGE_PROMOTION_BOUNDARY_READ_TIMEOUT_MS}"
    --curl-timeout-seconds "${STORAGE_PROMOTION_BOUNDARY_CURL_TIMEOUT_SECONDS}"
    --summary-dir "${SUMMARY_DIR}"
    --run-tag "${RUN_TAG}"
  )
  step="storage promotion boundary guard"
  safe_step="${step// /-}"
  log_path="${LOG_DIR}/${safe_step}.log"
  started="$(date -u +%s)"
  echo "[release-gate] ${step}"
  echo "[release-gate] command: ${STORAGE_PROMOTION_CMD[*]} (min_pass_count=${STORAGE_PROMOTION_BOUNDARY_MIN_PASS_COUNT})"

  output=""
  if output="$("${STORAGE_PROMOTION_CMD[@]}" 2>&1)"; then
    exit_code=0
  else
    exit_code=$?
  fi
  printf '%s\n' "${output}" | tee "${log_path}"

  promotion_summary_path=""
  promotion_pass_count=""
  if [[ ${exit_code} -eq 0 ]]; then
    promotion_summary_path="$(printf '%s\n' "${output}" | sed -n 's/^\[promotion-boundary-guard\] summary: //p' | tail -n 1)"
    if [[ -z "${promotion_summary_path}" || ! -f "${promotion_summary_path}" ]]; then
      printf '%s\n' "[release-gate] storage promotion boundary guard failed: missing summary path" | tee -a "${log_path}" >&2
      exit_code=1
    else
      promotion_pass_count="$(awk -F': ' '/^- pass_count:/ { print $2; exit }' "${promotion_summary_path}" | tr -d '[:space:]')"
      if [[ -z "${promotion_pass_count}" || ! "${promotion_pass_count}" =~ ^[0-9]+$ ]]; then
        printf '%s\n' "[release-gate] storage promotion boundary guard failed: unable to parse pass_count from ${promotion_summary_path}" | tee -a "${log_path}" >&2
        exit_code=1
      elif (( promotion_pass_count < STORAGE_PROMOTION_BOUNDARY_MIN_PASS_COUNT )); then
        printf '%s\n' "[release-gate] storage promotion boundary guard failed: pass_count=${promotion_pass_count} < min_pass_count=${STORAGE_PROMOTION_BOUNDARY_MIN_PASS_COUNT}" | tee -a "${log_path}" >&2
        exit_code=1
      else
        printf '%s\n' "[release-gate] storage promotion boundary guard passed: pass_count=${promotion_pass_count} >= min_pass_count=${STORAGE_PROMOTION_BOUNDARY_MIN_PASS_COUNT}" | tee -a "${log_path}"
      fi
    fi
  fi

  finished="$(date -u +%s)"
  duration=$((finished - started))
  if [[ ${exit_code} -eq 0 ]]; then
    PASS_COUNT=$((PASS_COUNT + 1))
    printf '| %s | PASS | %s | `%s` |\n' "${step}" "${duration}" "${STORAGE_PROMOTION_CMD[*]} (min_pass_count=${STORAGE_PROMOTION_BOUNDARY_MIN_PASS_COUNT})" >> "${ROWS_TMP}"
    append_detail "${step}" "PASS" "${STORAGE_PROMOTION_CMD[*]} (min_pass_count=${STORAGE_PROMOTION_BOUNDARY_MIN_PASS_COUNT})" "${duration}" "${log_path}"
  else
    FAIL_COUNT=$((FAIL_COUNT + 1))
    FAILED=true
    printf '| %s | FAIL | %s | `%s` |\n' "${step}" "${duration}" "${STORAGE_PROMOTION_CMD[*]} (min_pass_count=${STORAGE_PROMOTION_BOUNDARY_MIN_PASS_COUNT})" >> "${ROWS_TMP}"
    append_detail "${step}" "FAIL (exit ${exit_code})" "${STORAGE_PROMOTION_CMD[*]} (min_pass_count=${STORAGE_PROMOTION_BOUNDARY_MIN_PASS_COUNT})" "${duration}" "${log_path}"
  fi
else
  skip_step "storage promotion boundary guard" "scripts/storage_promotion_boundary_guard.sh --bind-addr <addr>" "disabled"
fi

if [[ "${RUN_SEGMENT_WAL_PARITY_GUARD}" == "true" ]]; then
  SEGMENT_WAL_PARITY_CMD=(
    scripts/segment_wal_parity_gate.sh
    --run-tag "${RUN_TAG}-segment-wal-parity"
    --summary-dir "${SUMMARY_DIR}"
    --ingest-bind-addr "${SEGMENT_WAL_PARITY_INGEST_BIND_ADDR}"
    --retrieve-bind-addr "${SEGMENT_WAL_PARITY_RETRIEVE_BIND_ADDR}"
    --top-k "${SEGMENT_WAL_PARITY_TOP_K}"
  )
  run_step "segment wal parity guard" "${SEGMENT_WAL_PARITY_CMD[*]}" "${SEGMENT_WAL_PARITY_CMD[@]}"
else
  skip_step "segment wal parity guard" "scripts/segment_wal_parity_gate.sh --run-tag ${RUN_TAG}-segment-wal-parity" "disabled"
fi

if [[ "${RUN_INCIDENT_SIMULATION_GUARD}" == "true" ]]; then
  INCIDENT_CMD=(
    scripts/incident_simulation_gate.sh
    --run-tag "${RUN_TAG}-incident"
    --summary-dir "${SUMMARY_DIR}"
    --run-failover-drill true
    --failover-mode "${INCIDENT_SIMULATION_FAILOVER_MODE}"
    --failover-max-wait-seconds "${INCIDENT_SIMULATION_FAILOVER_MAX_WAIT_SECONDS}"
    --run-auth-revocation-drill true
    --auth-max-wait-seconds "${INCIDENT_SIMULATION_AUTH_MAX_WAIT_SECONDS}"
    --run-recovery-drill true
    --recovery-max-rto-seconds "${INCIDENT_SIMULATION_RECOVERY_MAX_RTO_SECONDS}"
  )
  run_step "incident simulation gate" "${INCIDENT_CMD[*]}" "${INCIDENT_CMD[@]}"
else
  skip_step "incident simulation gate" "scripts/incident_simulation_gate.sh --run-tag ${RUN_TAG}-incident" "disabled"
fi

if [[ "${RUN_INGEST_THROUGHPUT_GUARD}" == "true" ]]; then
  THROUGHPUT_CMD=(
    scripts/benchmark_transport_concurrency.sh
    --target ingestion
    --bind-addr "${INGEST_THROUGHPUT_BIND_ADDR}"
    --workers-list "${INGEST_THROUGHPUT_WORKERS}"
    --clients "${INGEST_THROUGHPUT_CLIENTS}"
    --requests-per-worker "${INGEST_THROUGHPUT_REQUESTS_PER_WORKER}"
    --warmup-requests "${INGEST_THROUGHPUT_WARMUP_REQUESTS}"
    --run-tag "${RUN_TAG}-ingest-throughput-gate"
    --ingest-wal-sync-every-records "${INGEST_THROUGHPUT_WAL_SYNC_EVERY_RECORDS}"
    --ingest-wal-append-buffer-records "${INGEST_THROUGHPUT_WAL_APPEND_BUFFER_RECORDS}"
    --ingest-wal-sync-interval-ms "${INGEST_THROUGHPUT_WAL_SYNC_INTERVAL_MS}"
    --ingest-wal-background-flush-only "${INGEST_THROUGHPUT_WAL_BACKGROUND_FLUSH_ONLY}"
    --ingest-allow-unsafe-wal-durability "${INGEST_THROUGHPUT_ALLOW_UNSAFE_WAL_DURABILITY}"
  )
  if [[ "${INGEST_THROUGHPUT_WAL_ASYNC_FLUSH_INTERVAL_MS}" != "auto" && "${INGEST_THROUGHPUT_WAL_ASYNC_FLUSH_INTERVAL_MS}" != "" ]]; then
    THROUGHPUT_CMD+=(--ingest-wal-async-flush-interval-ms "${INGEST_THROUGHPUT_WAL_ASYNC_FLUSH_INTERVAL_MS}")
  fi
  step="ingest throughput guard"
  safe_step="${step// /-}"
  log_path="${LOG_DIR}/${safe_step}.log"
  started="$(date -u +%s)"
  echo "[release-gate] ${step}"
  echo "[release-gate] command: ${THROUGHPUT_CMD[*]} (min_rps=${INGEST_THROUGHPUT_MIN_RPS})"

  output=""
  if output="$("${THROUGHPUT_CMD[@]}" 2>&1)"; then
    exit_code=0
  else
    exit_code=$?
  fi
  printf '%s\n' "${output}" | tee "${log_path}"

  summary_path=""
  throughput_rps=""
  if [[ ${exit_code} -eq 0 ]]; then
    summary_path="$(printf '%s\n' "${output}" | sed -n 's/^\[concurrency\] output: //p' | tail -n 1)"
    if [[ -z "${summary_path}" || ! -f "${summary_path}" ]]; then
      printf '%s\n' "[release-gate] ingestion throughput guard failed: missing benchmark summary path" | tee -a "${log_path}" >&2
      exit_code=1
    else
      throughput_rps="$(awk -F'|' '/^\|[[:space:]]*[0-9]+[[:space:]]*\|/ { gsub(/[[:space:]]/, "", $0); print $4; exit }' "${summary_path}")"
      if [[ -z "${throughput_rps}" ]]; then
        printf '%s\n' "[release-gate] ingestion throughput guard failed: unable to parse throughput_rps from ${summary_path}" | tee -a "${log_path}" >&2
        exit_code=1
      elif ! awk -v v="${throughput_rps}" -v min="${INGEST_THROUGHPUT_MIN_RPS}" 'BEGIN { exit (v >= min ? 0 : 1) }'; then
        printf '%s\n' "[release-gate] ingestion throughput guard failed: throughput_rps=${throughput_rps} < min_rps=${INGEST_THROUGHPUT_MIN_RPS}" | tee -a "${log_path}" >&2
        exit_code=1
      else
        printf '%s\n' "[release-gate] ingestion throughput guard passed: throughput_rps=${throughput_rps} >= min_rps=${INGEST_THROUGHPUT_MIN_RPS}" | tee -a "${log_path}"
      fi
    fi
  fi

  finished="$(date -u +%s)"
  duration=$((finished - started))
  if [[ ${exit_code} -eq 0 ]]; then
    PASS_COUNT=$((PASS_COUNT + 1))
    printf '| %s | PASS | %s | `%s` |\n' "${step}" "${duration}" "${THROUGHPUT_CMD[*]} (min_rps=${INGEST_THROUGHPUT_MIN_RPS})" >> "${ROWS_TMP}"
    append_detail "${step}" "PASS" "${THROUGHPUT_CMD[*]} (min_rps=${INGEST_THROUGHPUT_MIN_RPS})" "${duration}" "${log_path}"
  else
    FAIL_COUNT=$((FAIL_COUNT + 1))
    FAILED=true
    printf '| %s | FAIL | %s | `%s` |\n' "${step}" "${duration}" "${THROUGHPUT_CMD[*]} (min_rps=${INGEST_THROUGHPUT_MIN_RPS})" >> "${ROWS_TMP}"
    append_detail "${step}" "FAIL (exit ${exit_code})" "${THROUGHPUT_CMD[*]} (min_rps=${INGEST_THROUGHPUT_MIN_RPS})" "${duration}" "${log_path}"
  fi
else
  skip_step "ingest throughput guard" "scripts/benchmark_transport_concurrency.sh --target ingestion ..." "disabled"
fi

if [[ -n "${VERIFY_INGESTION_AUDIT_PATH}" ]]; then
  run_step "audit verify ingestion" "scripts/verify_audit_chain.sh --path ${VERIFY_INGESTION_AUDIT_PATH} --service ingestion" \
    scripts/verify_audit_chain.sh --path "${VERIFY_INGESTION_AUDIT_PATH}" --service ingestion
else
  skip_step "audit verify ingestion" "scripts/verify_audit_chain.sh --path <ingestion-audit> --service ingestion" "no path provided"
fi

if [[ -n "${VERIFY_RETRIEVAL_AUDIT_PATH}" ]]; then
  run_step "audit verify retrieval" "scripts/verify_audit_chain.sh --path ${VERIFY_RETRIEVAL_AUDIT_PATH} --service retrieval" \
    scripts/verify_audit_chain.sh --path "${VERIFY_RETRIEVAL_AUDIT_PATH}" --service retrieval
else
  skip_step "audit verify retrieval" "scripts/verify_audit_chain.sh --path <retrieval-audit> --service retrieval" "no path provided"
fi

if [[ "${RUN_BENCH_TREND}" == "true" ]]; then
  BENCH_CMD=(
    scripts/benchmark_trend.sh
    --history-path "${BENCH_HISTORY_PATH}"
    --summary-dir "${SUMMARY_DIR}"
    --run-tag "${RUN_TAG}"
  )
  run_step "benchmark trend" "DASH_BENCH_INCLUDE_LARGE=${BENCH_INCLUDE_LARGE} DASH_BENCH_INCLUDE_XLARGE=${BENCH_INCLUDE_XLARGE} DASH_BENCH_INCLUDE_HYBRID=${BENCH_INCLUDE_HYBRID} ${BENCH_CMD[*]}" \
    env \
    DASH_BENCH_INCLUDE_LARGE="${BENCH_INCLUDE_LARGE}" \
    DASH_BENCH_INCLUDE_XLARGE="${BENCH_INCLUDE_XLARGE}" \
    DASH_BENCH_INCLUDE_HYBRID="${BENCH_INCLUDE_HYBRID}" \
    "${BENCH_CMD[@]}"
else
  skip_step "benchmark trend" "scripts/benchmark_trend.sh --run-tag ${RUN_TAG}" "disabled"
fi

FINAL_STATUS="PASS"
if [[ "${FAILED}" == "true" ]]; then
  FINAL_STATUS="FAIL"
fi
FINISHED_UTC="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

cat > "${SUMMARY_PATH}" <<EOF_SUMMARY
# DASH Release Candidate Gate

- run_id: ${RUN_ID}
- started_utc: ${RUN_UTC}
- finished_utc: ${FINISHED_UTC}
- summary_status: ${FINAL_STATUS}
- pass_count: ${PASS_COUNT}
- fail_count: ${FAIL_COUNT}
- skip_count: ${SKIP_COUNT}

| step | status | duration_s | command |
|---|---|---:|---|
EOF_SUMMARY
cat "${ROWS_TMP}" >> "${SUMMARY_PATH}"
cat "${DETAILS_TMP}" >> "${SUMMARY_PATH}"

if command -v shasum >/dev/null 2>&1; then
  shasum -a 256 "${SUMMARY_PATH}" > "${SUMMARY_PATH}.sha256"
  echo "[release-gate] checksum: ${SUMMARY_PATH}.sha256"
fi

echo "[release-gate] summary: ${SUMMARY_PATH}"

echo "[release-gate] result: ${FINAL_STATUS}"
if [[ "${FAILED}" == "true" ]]; then
  exit 1
fi
