#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

SUMMARY_DIR="${DASH_PHASE4_LONG_SUMMARY_DIR:-docs/benchmarks/history/runs}"
ACTION="${1:-}"

usage() {
  cat <<'USAGE'
Usage: scripts/phase4_long_soak_ctl.sh <action> [options] [-- phase4_long_soak args]

Actions:
  start      Start long-soak run in background (writes pid/log/heartbeat files)
  run        Run long-soak in foreground
  status     Show run status from pid/heartbeat/summary artifacts
  tail       Tail run log
  stop       Stop background run by pid

Common options:
  --run-id ID                  Required run id
  --summary-dir DIR            Summary/log/heartbeat directory
  --lines N                    Lines for tail action (default: 100)
  --follow true|false          Follow mode for tail action (default: true)
  --signal NAME                Signal for stop action (default: TERM)
  -h, --help                   Show help

Examples:
  scripts/phase4_long_soak_ctl.sh start --run-id phase4-1m-long -- --fixture-size 1000000 --iterations 1
  scripts/phase4_long_soak_ctl.sh status --run-id phase4-1m-long
  scripts/phase4_long_soak_ctl.sh tail --run-id phase4-1m-long --lines 200 --follow true
  scripts/phase4_long_soak_ctl.sh stop --run-id phase4-1m-long
USAGE
}

if [[ -z "${ACTION}" ]]; then
  usage
  exit 2
fi
if [[ "${ACTION}" == "--help" || "${ACTION}" == "-h" ]]; then
  usage
  exit 0
fi
shift || true

RUN_ID=""
LINES=100
FOLLOW="true"
STOP_SIGNAL="TERM"
FORWARD_ARGS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --run-id)
      RUN_ID="$2"
      shift 2
      ;;
    --summary-dir)
      SUMMARY_DIR="$2"
      shift 2
      ;;
    --lines)
      LINES="$2"
      shift 2
      ;;
    --follow)
      FOLLOW="$2"
      shift 2
      ;;
    --signal)
      STOP_SIGNAL="$2"
      shift 2
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    --)
      shift
      FORWARD_ARGS+=("$@")
      break
      ;;
    *)
      case "${ACTION}" in
        start|run)
          FORWARD_ARGS+=("$1")
          shift
          ;;
        *)
          echo "unknown argument for action '${ACTION}': $1" >&2
          exit 2
          ;;
      esac
      ;;
  esac
done

if [[ -z "${RUN_ID}" ]]; then
  echo "--run-id is required" >&2
  exit 2
fi
if [[ ! "${LINES}" =~ ^[0-9]+$ ]] || [[ "${LINES}" -eq 0 ]]; then
  echo "--lines must be a positive integer" >&2
  exit 2
fi
case "${FOLLOW}" in
  true|false) ;;
  *)
    echo "--follow must be true or false" >&2
    exit 2
    ;;
esac

mkdir -p "${SUMMARY_DIR}"

PID_PATH="${SUMMARY_DIR}/${RUN_ID}.pid"
LOG_PATH="${SUMMARY_DIR}/${RUN_ID}.log"
HEARTBEAT_PATH="${SUMMARY_DIR}/${RUN_ID}.heartbeat"
SUMMARY_PATH="${SUMMARY_DIR}/${RUN_ID}.md"
LAUNCH_LOG_PATH="${SUMMARY_DIR}/${RUN_ID}.launcher.log"

pid_is_running() {
  local pid="$1"
  kill -0 "${pid}" >/dev/null 2>&1
}

read_pid() {
  if [[ ! -f "${PID_PATH}" ]]; then
    return 1
  fi
  local pid
  pid="$(tr -d '[:space:]' < "${PID_PATH}")"
  if [[ ! "${pid}" =~ ^[0-9]+$ ]]; then
    return 1
  fi
  echo "${pid}"
}

action_start() {
  local existing_pid
  existing_pid="$(read_pid || true)"
  if [[ -n "${existing_pid}" ]] && pid_is_running "${existing_pid}"; then
    echo "run already active: run_id=${RUN_ID} pid=${existing_pid}" >&2
    exit 1
  fi

  nohup scripts/phase4_long_soak.sh \
    --run-id "${RUN_ID}" \
    --summary-dir "${SUMMARY_DIR}" \
    --log-path "${LOG_PATH}" \
    --heartbeat-path "${HEARTBEAT_PATH}" \
    "${FORWARD_ARGS[@]}" > "${LAUNCH_LOG_PATH}" 2>&1 &
  local pid=$!
  echo "${pid}" > "${PID_PATH}"
  echo "started run_id=${RUN_ID} pid=${pid}"
  echo "summary=${SUMMARY_PATH}"
  echo "heartbeat=${HEARTBEAT_PATH}"
  echo "log=${LOG_PATH}"
  echo "launcher_log=${LAUNCH_LOG_PATH}"
}

action_run() {
  scripts/phase4_long_soak.sh \
    --run-id "${RUN_ID}" \
    --summary-dir "${SUMMARY_DIR}" \
    --log-path "${LOG_PATH}" \
    --heartbeat-path "${HEARTBEAT_PATH}" \
    "${FORWARD_ARGS[@]}"
}

action_status() {
  local pid_state="not_found"
  local pid_value=""
  if pid_value="$(read_pid || true)"; then
    if pid_is_running "${pid_value}"; then
      pid_state="running"
    else
      pid_state="stopped"
    fi
  fi

  echo "run_id=${RUN_ID}"
  echo "pid_state=${pid_state}"
  if [[ -n "${pid_value}" ]]; then
    echo "pid=${pid_value}"
  fi
  echo "summary_path=${SUMMARY_PATH}"
  echo "heartbeat_path=${HEARTBEAT_PATH}"
  echo "log_path=${LOG_PATH}"
  if [[ -f "${SUMMARY_PATH}" ]]; then
    echo "summary_exists=true"
  else
    echo "summary_exists=false"
  fi
  if [[ -f "${HEARTBEAT_PATH}" ]]; then
    echo "heartbeat_exists=true"
    echo "--- heartbeat ---"
    cat "${HEARTBEAT_PATH}"
  else
    echo "heartbeat_exists=false"
  fi
}

action_tail() {
  if [[ "${FOLLOW}" == "true" ]]; then
    tail -n "${LINES}" -f "${LOG_PATH}"
  else
    tail -n "${LINES}" "${LOG_PATH}"
  fi
}

action_stop() {
  local pid
  pid="$(read_pid || true)"
  if [[ -z "${pid}" ]]; then
    echo "no pid file for run_id=${RUN_ID}" >&2
    exit 1
  fi
  if ! pid_is_running "${pid}"; then
    echo "run already stopped: run_id=${RUN_ID} pid=${pid}"
    rm -f "${PID_PATH}"
    exit 0
  fi

  kill "-${STOP_SIGNAL}" "${pid}"
  for _ in $(seq 1 50); do
    if ! pid_is_running "${pid}"; then
      rm -f "${PID_PATH}"
      echo "stopped run_id=${RUN_ID} pid=${pid}"
      return
    fi
    sleep 0.2
  done
  echo "failed to stop run_id=${RUN_ID} pid=${pid}" >&2
  exit 1
}

case "${ACTION}" in
  start) action_start ;;
  run) action_run ;;
  status) action_status ;;
  tail) action_tail ;;
  stop) action_stop ;;
  *)
    echo "unknown action: ${ACTION}" >&2
    usage >&2
    exit 2
    ;;
esac
