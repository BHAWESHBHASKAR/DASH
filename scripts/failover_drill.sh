#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

INGEST_BIND="${DASH_FAILOVER_INGEST_BIND:-127.0.0.1:19081}"
RETRIEVE_BIND="${DASH_FAILOVER_RETRIEVE_BIND:-127.0.0.1:19080}"
LOCAL_NODE_ID="${DASH_FAILOVER_LOCAL_NODE_ID:-node-b}"
TENANT_ID="${DASH_FAILOVER_TENANT_ID:-sample-tenant}"
ENTITY_KEY="${DASH_FAILOVER_ENTITY_KEY:-company-x}"
MAX_WAIT_SECONDS="${DASH_FAILOVER_MAX_WAIT_SECONDS:-30}"
KEEP_ARTIFACTS="false"
MODE="${DASH_FAILOVER_MODE:-both}"
PLACEMENT_RELOAD_INTERVAL_MS="${DASH_FAILOVER_PLACEMENT_RELOAD_INTERVAL_MS:-200}"

usage() {
  cat <<'USAGE'
Usage: scripts/failover_drill.sh [options]

Options:
  --ingest-bind HOST:PORT       Ingestion bind address (default: 127.0.0.1:19081)
  --retrieve-bind HOST:PORT     Retrieval bind address (default: 127.0.0.1:19080)
  --local-node-id NODE          Local node id used by both services (default: node-b)
  --tenant-id TENANT            Tenant id for routing probes (default: sample-tenant)
  --entity-key KEY              Entity key for placement probes (default: company-x)
  --max-wait-seconds N          Health wait timeout per service (default: 30)
  --mode MODE                   Drill mode: restart|no-restart|both (default: both)
  --placement-reload-interval-ms N
                               Placement live-reload interval in ms (default: 200)
  --keep-artifacts true|false   Keep logs/placement files after run (default: false)
  -h, --help                    Show help
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --ingest-bind)
      INGEST_BIND="$2"
      shift 2
      ;;
    --retrieve-bind)
      RETRIEVE_BIND="$2"
      shift 2
      ;;
    --local-node-id)
      LOCAL_NODE_ID="$2"
      shift 2
      ;;
    --tenant-id)
      TENANT_ID="$2"
      shift 2
      ;;
    --entity-key)
      ENTITY_KEY="$2"
      shift 2
      ;;
    --max-wait-seconds)
      MAX_WAIT_SECONDS="$2"
      shift 2
      ;;
    --mode)
      MODE="$2"
      shift 2
      ;;
    --placement-reload-interval-ms)
      PLACEMENT_RELOAD_INTERVAL_MS="$2"
      shift 2
      ;;
    --keep-artifacts)
      KEEP_ARTIFACTS="$2"
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

if ! command -v curl >/dev/null 2>&1; then
  echo "curl is required" >&2
  exit 1
fi

ARTIFACT_DIR="$(mktemp -d "${TMPDIR:-/tmp}/dash-failover-drill-XXXXXX")"
PLACEMENT_FILE="${ARTIFACT_DIR}/placements.csv"
INGEST_LOG="${ARTIFACT_DIR}/ingestion.log"
RETRIEVE_LOG="${ARTIFACT_DIR}/retrieval.log"
INGEST_PID=""
RETRIEVE_PID=""

stop_services() {
  if [[ -n "${INGEST_PID}" ]] && kill -0 "${INGEST_PID}" >/dev/null 2>&1; then
    kill "${INGEST_PID}" >/dev/null 2>&1 || true
    wait "${INGEST_PID}" >/dev/null 2>&1 || true
  fi
  if [[ -n "${RETRIEVE_PID}" ]] && kill -0 "${RETRIEVE_PID}" >/dev/null 2>&1; then
    kill "${RETRIEVE_PID}" >/dev/null 2>&1 || true
    wait "${RETRIEVE_PID}" >/dev/null 2>&1 || true
  fi
  INGEST_PID=""
  RETRIEVE_PID=""
}

cleanup() {
  stop_services
  if [[ "${KEEP_ARTIFACTS}" != "true" ]]; then
    rm -rf "${ARTIFACT_DIR}"
  fi
}
trap cleanup EXIT

write_placement_file() {
  local epoch="$1"
  local leader_node="$2"
  local follower_node="$3"
  cat > "${PLACEMENT_FILE}" <<EOF
${TENANT_ID},0,${epoch},${leader_node},leader,healthy
${TENANT_ID},0,${epoch},${follower_node},follower,healthy
EOF
}

wait_for_health() {
  local name="$1"
  local url="$2"
  local deadline=$((SECONDS + MAX_WAIT_SECONDS))
  while (( SECONDS < deadline )); do
    if curl -sS "${url}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.2
  done
  echo "${name} did not become healthy within ${MAX_WAIT_SECONDS}s" >&2
  return 1
}

start_services() {
  : > "${INGEST_LOG}"
  : > "${RETRIEVE_LOG}"
  DASH_INGEST_BIND="${INGEST_BIND}" \
    DASH_ROUTER_PLACEMENT_FILE="${PLACEMENT_FILE}" \
    DASH_ROUTER_LOCAL_NODE_ID="${LOCAL_NODE_ID}" \
    DASH_ROUTER_PLACEMENT_RELOAD_INTERVAL_MS="${PLACEMENT_RELOAD_INTERVAL_MS}" \
    cargo run -p ingestion -- --serve >"${INGEST_LOG}" 2>&1 &
  INGEST_PID=$!

  DASH_RETRIEVAL_BIND="${RETRIEVE_BIND}" \
    DASH_ROUTER_PLACEMENT_FILE="${PLACEMENT_FILE}" \
    DASH_ROUTER_LOCAL_NODE_ID="${LOCAL_NODE_ID}" \
    DASH_ROUTER_READ_PREFERENCE="leader_only" \
    DASH_ROUTER_PLACEMENT_RELOAD_INTERVAL_MS="${PLACEMENT_RELOAD_INTERVAL_MS}" \
    cargo run -p retrieval -- --serve >"${RETRIEVE_LOG}" 2>&1 &
  RETRIEVE_PID=$!

  wait_for_health "ingestion" "http://${INGEST_BIND}/health"
  wait_for_health "retrieval" "http://${RETRIEVE_BIND}/health"
}

http_status() {
  local method="$1"
  local url="$2"
  local body="${3:-}"
  if [[ -n "${body}" ]]; then
    curl -sS -o /dev/null -w "%{http_code}" -X "${method}" \
      -H "Content-Type: application/json" \
      --data "${body}" \
      "${url}"
  else
    curl -sS -o /dev/null -w "%{http_code}" -X "${method}" "${url}"
  fi
}

assert_status() {
  local phase="$1"
  local actual="$2"
  local expected="$3"
  if [[ "${actual}" != "${expected}" ]]; then
    echo "[failover-drill] ${phase}: expected HTTP ${expected}, got ${actual}" >&2
    echo "[failover-drill] ingestion log: ${INGEST_LOG}" >&2
    echo "[failover-drill] retrieval log: ${RETRIEVE_LOG}" >&2
    exit 1
  fi
}

poll_status_until() {
  local phase="$1"
  local expected="$2"
  local method="$3"
  local url="$4"
  local body="${5:-}"
  local deadline=$((SECONDS + MAX_WAIT_SECONDS))
  local status=""
  while (( SECONDS < deadline )); do
    status="$(http_status "${method}" "${url}" "${body}")"
    if [[ "${status}" == "${expected}" ]]; then
      echo "[failover-drill] ${phase}: observed HTTP ${status}"
      return 0
    fi
    sleep 0.2
  done
  echo "[failover-drill] ${phase}: expected HTTP ${expected}, got ${status}" >&2
  echo "[failover-drill] ingestion log: ${INGEST_LOG}" >&2
  echo "[failover-drill] retrieval log: ${RETRIEVE_LOG}" >&2
  exit 1
}

run_phase1_rejections() {
  local claim_id="$1"
  local phase_body="{\"claim\":{\"claim_id\":\"${claim_id}\",\"tenant_id\":\"${TENANT_ID}\",\"canonical_text\":\"failover drill phase one\",\"confidence\":0.9,\"entities\":[\"${ENTITY_KEY}\"]}}"
  local phase_ingest_status
  local phase_retrieve_status
  phase_ingest_status="$(http_status "POST" "http://${INGEST_BIND}/v1/ingest" "${phase_body}")"
  phase_retrieve_status="$(http_status "GET" "http://${RETRIEVE_BIND}/v1/retrieve?tenant_id=${TENANT_ID}&query=retrieval+initialized&top_k=1&stance_mode=balanced")"
  assert_status "phase1 ingest route rejection" "${phase_ingest_status}" "503"
  assert_status "phase1 retrieve route rejection" "${phase_retrieve_status}" "503"
}

run_restart_scenario() {
  echo "[failover-drill] scenario=restart"
  stop_services
  write_placement_file 1 "node-a" "node-b"
  start_services
  run_phase1_rejections "drill-restart-phase1"

  echo "[failover-drill] scenario=restart phase=2 promote local node and restart services"
  stop_services
  write_placement_file 2 "node-b" "node-a"
  start_services

  local phase2_ingest_body="{\"claim\":{\"claim_id\":\"drill-restart-phase2\",\"tenant_id\":\"${TENANT_ID}\",\"canonical_text\":\"failover drill phase two restart\",\"confidence\":0.9,\"entities\":[\"${ENTITY_KEY}\"]}}"
  local phase2_ingest_status
  local phase2_retrieve_status
  phase2_ingest_status="$(http_status "POST" "http://${INGEST_BIND}/v1/ingest" "${phase2_ingest_body}")"
  phase2_retrieve_status="$(http_status "GET" "http://${RETRIEVE_BIND}/v1/retrieve?tenant_id=${TENANT_ID}&query=retrieval+initialized&top_k=1&stance_mode=balanced")"
  assert_status "restart phase2 ingest route acceptance" "${phase2_ingest_status}" "200"
  assert_status "restart phase2 retrieve route acceptance" "${phase2_retrieve_status}" "200"
}

run_no_restart_scenario() {
  echo "[failover-drill] scenario=no-restart"
  stop_services
  write_placement_file 1 "node-a" "node-b"
  start_services
  run_phase1_rejections "drill-no-restart-phase1"

  echo "[failover-drill] scenario=no-restart phase=2 promote local node without restart"
  write_placement_file 2 "node-b" "node-a"
  local phase2_ingest_body="{\"claim\":{\"claim_id\":\"drill-no-restart-phase2\",\"tenant_id\":\"${TENANT_ID}\",\"canonical_text\":\"failover drill phase two no restart\",\"confidence\":0.9,\"entities\":[\"${ENTITY_KEY}\"]}}"
  poll_status_until \
    "no-restart phase2 ingest route acceptance" \
    "200" \
    "POST" \
    "http://${INGEST_BIND}/v1/ingest" \
    "${phase2_ingest_body}"
  poll_status_until \
    "no-restart phase2 retrieve route acceptance" \
    "200" \
    "GET" \
    "http://${RETRIEVE_BIND}/v1/retrieve?tenant_id=${TENANT_ID}&query=retrieval+initialized&top_k=1&stance_mode=balanced"
}

echo "[failover-drill] artifact_dir=${ARTIFACT_DIR}"

case "${MODE}" in
  restart)
    run_restart_scenario
    ;;
  no-restart)
    run_no_restart_scenario
    ;;
  both)
    run_restart_scenario
    run_no_restart_scenario
    ;;
  *)
    echo "invalid mode '${MODE}'. expected restart|no-restart|both" >&2
    exit 2
    ;;
esac

echo "[failover-drill] success"
echo "[failover-drill] mode=${MODE}"
echo "[failover-drill] placement_reload_interval_ms=${PLACEMENT_RELOAD_INTERVAL_MS}"
echo "[failover-drill] placement_file=${PLACEMENT_FILE}"
echo "[failover-drill] ingestion_log=${INGEST_LOG}"
echo "[failover-drill] retrieval_log=${RETRIEVE_LOG}"
