#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

INGEST_BIND="${DASH_AUTH_DRILL_INGEST_BIND:-127.0.0.1:19181}"
RETRIEVE_BIND="${DASH_AUTH_DRILL_RETRIEVE_BIND:-127.0.0.1:19180}"
TENANT_ID="${DASH_AUTH_DRILL_TENANT_ID:-sample-tenant}"
ACTIVE_KEY="${DASH_AUTH_DRILL_ACTIVE_KEY:-dash-active-key}"
REVOKED_KEY="${DASH_AUTH_DRILL_REVOKED_KEY:-dash-revoked-key}"
MAX_WAIT_SECONDS="${DASH_AUTH_DRILL_MAX_WAIT_SECONDS:-30}"
KEEP_ARTIFACTS="false"

usage() {
  cat <<'USAGE'
Usage: scripts/auth_revocation_drill.sh [options]

Run a local auth revocation drill:
1) start ingestion/retrieval services with active + revoked API key config
2) verify active key succeeds for ingest/retrieve
3) verify revoked key is denied for ingest/retrieve
4) verify missing key is denied for ingest/retrieve

Options:
  --ingest-bind HOST:PORT       Ingestion bind address (default: 127.0.0.1:19181)
  --retrieve-bind HOST:PORT     Retrieval bind address (default: 127.0.0.1:19180)
  --tenant-id TENANT            Tenant id used for drill requests (default: sample-tenant)
  --active-key KEY              API key that must remain accepted (default: dash-active-key)
  --revoked-key KEY             API key that must be denied (default: dash-revoked-key)
  --max-wait-seconds N          Health wait timeout per service (default: 30)
  --keep-artifacts true|false   Keep logs/artifacts after run (default: false)
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
    --tenant-id)
      TENANT_ID="$2"
      shift 2
      ;;
    --active-key)
      ACTIVE_KEY="$2"
      shift 2
      ;;
    --revoked-key)
      REVOKED_KEY="$2"
      shift 2
      ;;
    --max-wait-seconds)
      MAX_WAIT_SECONDS="$2"
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

if [[ "${KEEP_ARTIFACTS}" != "true" && "${KEEP_ARTIFACTS}" != "false" ]]; then
  echo "--keep-artifacts must be true|false" >&2
  exit 2
fi
if [[ ! "${MAX_WAIT_SECONDS}" =~ ^[0-9]+$ || "${MAX_WAIT_SECONDS}" -eq 0 ]]; then
  echo "--max-wait-seconds must be a positive integer" >&2
  exit 2
fi
if ! command -v curl >/dev/null 2>&1; then
  echo "curl is required" >&2
  exit 1
fi

ARTIFACT_DIR="$(mktemp -d "${TMPDIR:-/tmp}/dash-auth-revoke-drill-XXXXXX")"
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
  DASH_INGEST_API_KEYS="${ACTIVE_KEY},${REVOKED_KEY}" \
  DASH_INGEST_REVOKED_API_KEYS="${REVOKED_KEY}" \
  DASH_INGEST_API_KEY_SCOPES="${ACTIVE_KEY}:${TENANT_ID};${REVOKED_KEY}:${TENANT_ID}" \
  cargo run -p ingestion -- --serve >"${INGEST_LOG}" 2>&1 &
  INGEST_PID=$!

  DASH_RETRIEVAL_BIND="${RETRIEVE_BIND}" \
  DASH_RETRIEVAL_API_KEYS="${ACTIVE_KEY},${REVOKED_KEY}" \
  DASH_RETRIEVAL_REVOKED_API_KEYS="${REVOKED_KEY}" \
  DASH_RETRIEVAL_API_KEY_SCOPES="${ACTIVE_KEY}:${TENANT_ID};${REVOKED_KEY}:${TENANT_ID}" \
  cargo run -p retrieval -- --serve >"${RETRIEVE_LOG}" 2>&1 &
  RETRIEVE_PID=$!

  wait_for_health "ingestion" "http://${INGEST_BIND}/health"
  wait_for_health "retrieval" "http://${RETRIEVE_BIND}/health"
}

http_status() {
  local method="$1"
  local url="$2"
  local api_key="$3"
  local body="${4:-}"

  local args=(-sS -o /dev/null -w "%{http_code}" -X "${method}")
  if [[ -n "${api_key}" ]]; then
    args+=( -H "X-API-Key: ${api_key}" )
  fi
  if [[ -n "${body}" ]]; then
    args+=( -H "Content-Type: application/json" --data "${body}" )
  fi
  args+=("${url}")
  curl "${args[@]}"
}

assert_status() {
  local phase="$1"
  local actual="$2"
  local expected="$3"
  if [[ "${actual}" != "${expected}" ]]; then
    echo "[auth-drill] ${phase}: expected HTTP ${expected}, got ${actual}" >&2
    echo "[auth-drill] ingestion_log=${INGEST_LOG}" >&2
    echo "[auth-drill] retrieval_log=${RETRIEVE_LOG}" >&2
    exit 1
  fi
}

echo "[auth-drill] artifact_dir=${ARTIFACT_DIR}"
echo "[auth-drill] tenant_id=${TENANT_ID}"

start_services

CLAIM_SUFFIX="$(date +%s)"
INGEST_BODY_ACTIVE="{\"claim\":{\"claim_id\":\"auth-drill-active-${CLAIM_SUFFIX}\",\"tenant_id\":\"${TENANT_ID}\",\"canonical_text\":\"auth drill active key\",\"confidence\":0.9}}"
INGEST_BODY_REVOKED="{\"claim\":{\"claim_id\":\"auth-drill-revoked-${CLAIM_SUFFIX}\",\"tenant_id\":\"${TENANT_ID}\",\"canonical_text\":\"auth drill revoked key\",\"confidence\":0.9}}"

ingest_active_status="$(http_status "POST" "http://${INGEST_BIND}/v1/ingest" "${ACTIVE_KEY}" "${INGEST_BODY_ACTIVE}")"
ingest_revoked_status="$(http_status "POST" "http://${INGEST_BIND}/v1/ingest" "${REVOKED_KEY}" "${INGEST_BODY_REVOKED}")"
ingest_missing_status="$(http_status "POST" "http://${INGEST_BIND}/v1/ingest" "" "${INGEST_BODY_REVOKED}")"

retrieve_active_status="$(http_status "GET" "http://${RETRIEVE_BIND}/v1/retrieve?tenant_id=${TENANT_ID}&query=retrieval+initialized&top_k=1" "${ACTIVE_KEY}")"
retrieve_revoked_status="$(http_status "GET" "http://${RETRIEVE_BIND}/v1/retrieve?tenant_id=${TENANT_ID}&query=retrieval+initialized&top_k=1" "${REVOKED_KEY}")"
retrieve_missing_status="$(http_status "GET" "http://${RETRIEVE_BIND}/v1/retrieve?tenant_id=${TENANT_ID}&query=retrieval+initialized&top_k=1" "")"

assert_status "ingest active key accepted" "${ingest_active_status}" "200"
assert_status "ingest revoked key denied" "${ingest_revoked_status}" "401"
assert_status "ingest missing key denied" "${ingest_missing_status}" "401"
assert_status "retrieve active key accepted" "${retrieve_active_status}" "200"
assert_status "retrieve revoked key denied" "${retrieve_revoked_status}" "401"
assert_status "retrieve missing key denied" "${retrieve_missing_status}" "401"

echo "[auth-drill] ingest_active_status=${ingest_active_status}"
echo "[auth-drill] ingest_revoked_status=${ingest_revoked_status}"
echo "[auth-drill] ingest_missing_status=${ingest_missing_status}"
echo "[auth-drill] retrieve_active_status=${retrieve_active_status}"
echo "[auth-drill] retrieve_revoked_status=${retrieve_revoked_status}"
echo "[auth-drill] retrieve_missing_status=${retrieve_missing_status}"
echo "[auth-drill] success"
