#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

RUN_TAG="${DASH_SECURITY_SIGNOFF_RUN_TAG:-phase3-signoff}"
SUMMARY_DIR="${DASH_SECURITY_SIGNOFF_SUMMARY_DIR:-docs/benchmarks/history/runs}"
INGEST_AUDIT_PATH="${DASH_SECURITY_SIGNOFF_INGEST_AUDIT_PATH:-}"
RETRIEVE_AUDIT_PATH="${DASH_SECURITY_SIGNOFF_RETRIEVE_AUDIT_PATH:-}"
RUN_BENCH_TREND="${DASH_SECURITY_SIGNOFF_RUN_BENCH_TREND:-false}"
RUN_INGEST_THROUGHPUT_GUARD="${DASH_SECURITY_SIGNOFF_RUN_INGEST_THROUGHPUT_GUARD:-false}"
SLO_INCLUDE_RECOVERY_DRILL="${DASH_SECURITY_SIGNOFF_SLO_INCLUDE_RECOVERY_DRILL:-false}"
INCIDENT_FAILOVER_MODE="${DASH_SECURITY_SIGNOFF_INCIDENT_FAILOVER_MODE:-no-restart}"
INCIDENT_FAILOVER_MAX_WAIT_SECONDS="${DASH_SECURITY_SIGNOFF_INCIDENT_FAILOVER_MAX_WAIT_SECONDS:-30}"
INCIDENT_AUTH_MAX_WAIT_SECONDS="${DASH_SECURITY_SIGNOFF_INCIDENT_AUTH_MAX_WAIT_SECONDS:-30}"
INCIDENT_RECOVERY_MAX_RTO_SECONDS="${DASH_SECURITY_SIGNOFF_INCIDENT_RECOVERY_MAX_RTO_SECONDS:-60}"

usage() {
  cat <<'USAGE'
Usage: scripts/security_signoff_gate.sh [options]

Run security sign-off gate with mandatory audit-chain verification and incident simulation.

Options:
  --run-tag TAG                         Run tag for release-gate artifacts
  --summary-dir DIR                     Summary/log artifact directory
  --ingestion-audit PATH                Ingestion audit log path (required)
  --retrieval-audit PATH                Retrieval audit log path (required)
  --run-benchmark-trend true|false      Forwarded to release gate (default: false)
  --run-ingest-throughput-guard true|false
                                         Forwarded to release gate (default: false)
  --slo-include-recovery-drill true|false
                                         Forwarded to release gate (default: false)
  --incident-failover-mode MODE         restart|no-restart|both (default: no-restart)
  --incident-failover-max-wait-seconds N
                                         Incident failover wait timeout (default: 30)
  --incident-auth-max-wait-seconds N    Incident auth drill wait timeout (default: 30)
  --incident-recovery-max-rto-seconds N Incident recovery max RTO (default: 60)
  -h, --help                            Show help

Environment:
  DASH_SECURITY_SIGNOFF_* variables map to these options.
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
    --ingestion-audit)
      INGEST_AUDIT_PATH="$2"
      shift 2
      ;;
    --retrieval-audit)
      RETRIEVE_AUDIT_PATH="$2"
      shift 2
      ;;
    --run-benchmark-trend)
      RUN_BENCH_TREND="$2"
      shift 2
      ;;
    --run-ingest-throughput-guard)
      RUN_INGEST_THROUGHPUT_GUARD="$2"
      shift 2
      ;;
    --slo-include-recovery-drill)
      SLO_INCLUDE_RECOVERY_DRILL="$2"
      shift 2
      ;;
    --incident-failover-mode)
      INCIDENT_FAILOVER_MODE="$2"
      shift 2
      ;;
    --incident-failover-max-wait-seconds)
      INCIDENT_FAILOVER_MAX_WAIT_SECONDS="$2"
      shift 2
      ;;
    --incident-auth-max-wait-seconds)
      INCIDENT_AUTH_MAX_WAIT_SECONDS="$2"
      shift 2
      ;;
    --incident-recovery-max-rto-seconds)
      INCIDENT_RECOVERY_MAX_RTO_SECONDS="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[signoff-gate] unknown argument: $1" >&2
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
      echo "[signoff-gate] ${key} must be true|false (got '${value}')" >&2
      exit 2
      ;;
  esac
}

validate_bool "--run-benchmark-trend" "${RUN_BENCH_TREND}"
validate_bool "--run-ingest-throughput-guard" "${RUN_INGEST_THROUGHPUT_GUARD}"
validate_bool "--slo-include-recovery-drill" "${SLO_INCLUDE_RECOVERY_DRILL}"

if [[ -z "${INGEST_AUDIT_PATH}" || -z "${RETRIEVE_AUDIT_PATH}" ]]; then
  echo "[signoff-gate] --ingestion-audit and --retrieval-audit are required" >&2
  usage >&2
  exit 2
fi
if [[ ! -f "${INGEST_AUDIT_PATH}" ]]; then
  echo "[signoff-gate] ingestion audit file not found: ${INGEST_AUDIT_PATH}" >&2
  exit 2
fi
if [[ ! -f "${RETRIEVE_AUDIT_PATH}" ]]; then
  echo "[signoff-gate] retrieval audit file not found: ${RETRIEVE_AUDIT_PATH}" >&2
  exit 2
fi
if [[ ! "${INCIDENT_FAILOVER_MAX_WAIT_SECONDS}" =~ ^[0-9]+$ || "${INCIDENT_FAILOVER_MAX_WAIT_SECONDS}" -eq 0 ]]; then
  echo "[signoff-gate] --incident-failover-max-wait-seconds must be a positive integer" >&2
  exit 2
fi
if [[ ! "${INCIDENT_AUTH_MAX_WAIT_SECONDS}" =~ ^[0-9]+$ || "${INCIDENT_AUTH_MAX_WAIT_SECONDS}" -eq 0 ]]; then
  echo "[signoff-gate] --incident-auth-max-wait-seconds must be a positive integer" >&2
  exit 2
fi
if [[ ! "${INCIDENT_RECOVERY_MAX_RTO_SECONDS}" =~ ^[0-9]+$ || "${INCIDENT_RECOVERY_MAX_RTO_SECONDS}" -eq 0 ]]; then
  echo "[signoff-gate] --incident-recovery-max-rto-seconds must be a positive integer" >&2
  exit 2
fi
case "${INCIDENT_FAILOVER_MODE}" in
  restart|no-restart|both) ;;
  *)
    echo "[signoff-gate] --incident-failover-mode must be one of: restart, no-restart, both" >&2
    exit 2
    ;;
esac

mkdir -p "${SUMMARY_DIR}"

echo "[signoff-gate] verifying ingestion audit chain"
scripts/verify_audit_chain.sh --path "${INGEST_AUDIT_PATH}" --service ingestion

echo "[signoff-gate] verifying retrieval audit chain"
scripts/verify_audit_chain.sh --path "${RETRIEVE_AUDIT_PATH}" --service retrieval

echo "[signoff-gate] running release candidate gate with incident simulation enabled"
scripts/release_candidate_gate.sh \
  --run-tag "${RUN_TAG}" \
  --summary-dir "${SUMMARY_DIR}" \
  --run-benchmark-trend "${RUN_BENCH_TREND}" \
  --run-ingest-throughput-guard "${RUN_INGEST_THROUGHPUT_GUARD}" \
  --slo-include-recovery-drill "${SLO_INCLUDE_RECOVERY_DRILL}" \
  --run-incident-simulation-guard true \
  --incident-failover-mode "${INCIDENT_FAILOVER_MODE}" \
  --incident-failover-max-wait-seconds "${INCIDENT_FAILOVER_MAX_WAIT_SECONDS}" \
  --incident-auth-max-wait-seconds "${INCIDENT_AUTH_MAX_WAIT_SECONDS}" \
  --incident-recovery-max-rto-seconds "${INCIDENT_RECOVERY_MAX_RTO_SECONDS}" \
  --verify-ingestion-audit "${INGEST_AUDIT_PATH}" \
  --verify-retrieval-audit "${RETRIEVE_AUDIT_PATH}"
