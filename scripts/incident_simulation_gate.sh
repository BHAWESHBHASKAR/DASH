#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

RUN_TAG="${DASH_INCIDENT_GATE_RUN_TAG:-incident-gate}"
SUMMARY_DIR="${DASH_INCIDENT_GATE_SUMMARY_DIR:-docs/benchmarks/history/runs}"
RUN_FAILOVER_DRILL="${DASH_INCIDENT_GATE_RUN_FAILOVER_DRILL:-true}"
FAILOVER_MODE="${DASH_INCIDENT_GATE_FAILOVER_MODE:-no-restart}"
FAILOVER_MAX_WAIT_SECONDS="${DASH_INCIDENT_GATE_FAILOVER_MAX_WAIT_SECONDS:-30}"
RUN_AUTH_REVOCATION_DRILL="${DASH_INCIDENT_GATE_RUN_AUTH_REVOCATION_DRILL:-true}"
AUTH_DRILL_MAX_WAIT_SECONDS="${DASH_INCIDENT_GATE_AUTH_MAX_WAIT_SECONDS:-30}"
RUN_RECOVERY_DRILL="${DASH_INCIDENT_GATE_RUN_RECOVERY_DRILL:-true}"
RECOVERY_MAX_RTO_SECONDS="${DASH_INCIDENT_GATE_RECOVERY_MAX_RTO_SECONDS:-60}"

usage() {
  cat <<'USAGE'
Usage: scripts/incident_simulation_gate.sh [options]

Run Phase 3 incident simulations and emit a summary artifact.

Options:
  --run-tag TAG                         Run tag for artifact naming
  --summary-dir DIR                     Summary/log artifact directory
  --run-failover-drill true|false       Enable routing failover drill (default: true)
  --failover-mode MODE                  restart|no-restart|both (default: no-restart)
  --failover-max-wait-seconds N         Failover drill health/poll timeout (default: 30)
  --run-auth-revocation-drill true|false
                                         Enable auth revocation drill (default: true)
  --auth-max-wait-seconds N             Auth drill health wait timeout (default: 30)
  --run-recovery-drill true|false       Enable backup/restore recovery drill (default: true)
  --recovery-max-rto-seconds N          Recovery drill RTO threshold seconds (default: 60)
  -h, --help                            Show help

Environment:
  DASH_INCIDENT_GATE_* variables are supported for all options.
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
    --run-failover-drill)
      RUN_FAILOVER_DRILL="$2"
      shift 2
      ;;
    --failover-mode)
      FAILOVER_MODE="$2"
      shift 2
      ;;
    --failover-max-wait-seconds)
      FAILOVER_MAX_WAIT_SECONDS="$2"
      shift 2
      ;;
    --run-auth-revocation-drill)
      RUN_AUTH_REVOCATION_DRILL="$2"
      shift 2
      ;;
    --auth-max-wait-seconds)
      AUTH_DRILL_MAX_WAIT_SECONDS="$2"
      shift 2
      ;;
    --run-recovery-drill)
      RUN_RECOVERY_DRILL="$2"
      shift 2
      ;;
    --recovery-max-rto-seconds)
      RECOVERY_MAX_RTO_SECONDS="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[incident-gate] unknown argument: $1" >&2
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
      echo "[incident-gate] ${key} must be true|false (got '${value}')" >&2
      exit 2
      ;;
  esac
}

validate_bool "--run-failover-drill" "${RUN_FAILOVER_DRILL}"
validate_bool "--run-auth-revocation-drill" "${RUN_AUTH_REVOCATION_DRILL}"
validate_bool "--run-recovery-drill" "${RUN_RECOVERY_DRILL}"

if [[ ! "${FAILOVER_MAX_WAIT_SECONDS}" =~ ^[0-9]+$ || "${FAILOVER_MAX_WAIT_SECONDS}" -eq 0 ]]; then
  echo "[incident-gate] --failover-max-wait-seconds must be a positive integer" >&2
  exit 2
fi
if [[ ! "${AUTH_DRILL_MAX_WAIT_SECONDS}" =~ ^[0-9]+$ || "${AUTH_DRILL_MAX_WAIT_SECONDS}" -eq 0 ]]; then
  echo "[incident-gate] --auth-max-wait-seconds must be a positive integer" >&2
  exit 2
fi
if [[ ! "${RECOVERY_MAX_RTO_SECONDS}" =~ ^[0-9]+$ || "${RECOVERY_MAX_RTO_SECONDS}" -eq 0 ]]; then
  echo "[incident-gate] --recovery-max-rto-seconds must be a positive integer" >&2
  exit 2
fi
case "${FAILOVER_MODE}" in
  restart|no-restart|both) ;;
  *)
    echo "[incident-gate] --failover-mode must be one of: restart, no-restart, both" >&2
    exit 2
    ;;
esac

mkdir -p "${SUMMARY_DIR}"

RUN_STAMP="$(date -u +%Y%m%d-%H%M%S)"
RUN_UTC="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
RUN_ID="${RUN_STAMP}-${RUN_TAG}"
SUMMARY_PATH="${SUMMARY_DIR}/${RUN_ID}-incident-gate.md"
LOG_DIR="${SUMMARY_DIR}/${RUN_ID}-incident-logs"
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

  echo "[incident-gate] ${step}"
  echo "[incident-gate] command: ${command_text}"

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

  echo "[incident-gate] ${step}: skipped (${reason})"
  printf '%s\n' "skipped: ${reason}" > "${log_path}"
  SKIP_COUNT=$((SKIP_COUNT + 1))
  printf '| %s | SKIP | - | `%s` |\n' "${step}" "${command_text}" >> "${ROWS_TMP}"
  append_detail "${step}" "SKIP (${reason})" "${command_text}" "-" "${log_path}"
}

if [[ "${RUN_FAILOVER_DRILL}" == "true" ]]; then
  run_step \
    "failover drill" \
    "scripts/failover_drill.sh --mode ${FAILOVER_MODE} --max-wait-seconds ${FAILOVER_MAX_WAIT_SECONDS}" \
    scripts/failover_drill.sh --mode "${FAILOVER_MODE}" --max-wait-seconds "${FAILOVER_MAX_WAIT_SECONDS}"
else
  skip_step "failover drill" "scripts/failover_drill.sh --mode ${FAILOVER_MODE}" "disabled"
fi

if [[ "${RUN_AUTH_REVOCATION_DRILL}" == "true" ]]; then
  run_step \
    "auth revocation drill" \
    "scripts/auth_revocation_drill.sh --max-wait-seconds ${AUTH_DRILL_MAX_WAIT_SECONDS}" \
    scripts/auth_revocation_drill.sh --max-wait-seconds "${AUTH_DRILL_MAX_WAIT_SECONDS}"
else
  skip_step "auth revocation drill" "scripts/auth_revocation_drill.sh" "disabled"
fi

if [[ "${RUN_RECOVERY_DRILL}" == "true" ]]; then
  run_step \
    "recovery drill" \
    "scripts/recovery_drill.sh --max-rto-seconds ${RECOVERY_MAX_RTO_SECONDS}" \
    scripts/recovery_drill.sh --max-rto-seconds "${RECOVERY_MAX_RTO_SECONDS}"
else
  skip_step "recovery drill" "scripts/recovery_drill.sh" "disabled"
fi

FINAL_STATUS="PASS"
if [[ "${FAILED}" == "true" ]]; then
  FINAL_STATUS="FAIL"
fi
FINISHED_UTC="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

cat > "${SUMMARY_PATH}" <<EOF_SUMMARY
# DASH Incident Simulation Gate

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
  echo "[incident-gate] checksum: ${SUMMARY_PATH}.sha256"
fi

echo "[incident-gate] summary: ${SUMMARY_PATH}"
echo "[incident-gate] result: ${FINAL_STATUS}"

if [[ "${FAILED}" == "true" ]]; then
  exit 1
fi
