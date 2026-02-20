#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

INGEST_BIND="${DASH_REBALANCE_INGEST_BIND:-127.0.0.1:19181}"
RETRIEVE_BIND="${DASH_REBALANCE_RETRIEVE_BIND:-127.0.0.1:19180}"
LOCAL_NODE_ID="${DASH_REBALANCE_LOCAL_NODE_ID:-node-a}"
TENANT_ID="${DASH_REBALANCE_TENANT_ID:-sample-tenant}"
MAX_WAIT_SECONDS="${DASH_REBALANCE_MAX_WAIT_SECONDS:-30}"
PLACEMENT_RELOAD_INTERVAL_MS="${DASH_REBALANCE_PLACEMENT_RELOAD_INTERVAL_MS:-200}"
REQUIRE_MOVED_KEYS="${DASH_REBALANCE_REQUIRE_MOVED_KEYS:-1}"
ARTIFACT_DIR="${DASH_REBALANCE_ARTIFACT_DIR:-docs/benchmarks/history/rebalance}"
RUN_ID="${DASH_REBALANCE_RUN_ID:-$(date -u +%Y%m%d-%H%M%S)-rebalance-drill}"
PROBE_KEYS_COUNT="${DASH_REBALANCE_PROBE_KEYS_COUNT:-128}"
TARGET_SHARDS="${DASH_REBALANCE_TARGET_SHARDS:-8}"

usage() {
  cat <<'USAGE'
Usage: scripts/rebalance_drill.sh [options]

Options:
  --run-id ID                           Artifact run id (default: UTC timestamp)
  --artifact-dir DIR                    Artifact root directory
                                         (default: docs/benchmarks/history/rebalance)
  --ingest-bind HOST:PORT               Ingestion bind address (default: 127.0.0.1:19181)
  --retrieve-bind HOST:PORT             Retrieval bind address (default: 127.0.0.1:19180)
  --local-node-id NODE                  Local node id for routing probes (default: node-a)
  --tenant-id TENANT                    Tenant id used for placement probes
                                         (default: sample-tenant)
  --max-wait-seconds N                  Max wait for service health and reload (default: 30)
  --placement-reload-interval-ms N      Placement reload interval (default: 200)
  --probe-keys-count N                  Number of probe keys to compare before/after
                                         (default: 128)
  --target-shards N                     Final shard count after split/rebalance
                                         (default: 8)
  --require-moved-keys N                Minimum keys that must move shards after split
                                         (default: 1)
  -h, --help                            Show help
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --run-id)
      RUN_ID="$2"
      shift 2
      ;;
    --artifact-dir)
      ARTIFACT_DIR="$2"
      shift 2
      ;;
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
    --max-wait-seconds)
      MAX_WAIT_SECONDS="$2"
      shift 2
      ;;
    --placement-reload-interval-ms)
      PLACEMENT_RELOAD_INTERVAL_MS="$2"
      shift 2
      ;;
    --probe-keys-count)
      PROBE_KEYS_COUNT="$2"
      shift 2
      ;;
    --target-shards)
      TARGET_SHARDS="$2"
      shift 2
      ;;
    --require-moved-keys)
      REQUIRE_MOVED_KEYS="$2"
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

for numeric in "${MAX_WAIT_SECONDS}" "${PLACEMENT_RELOAD_INTERVAL_MS}" "${REQUIRE_MOVED_KEYS}" "${PROBE_KEYS_COUNT}" "${TARGET_SHARDS}"; do
  if [[ ! "${numeric}" =~ ^[0-9]+$ ]] || [[ "${numeric}" -eq 0 ]]; then
    echo "all numeric options must be positive integers" >&2
    exit 2
  fi
done

if [[ "${TARGET_SHARDS}" -lt 2 ]]; then
  echo "--target-shards must be >= 2" >&2
  exit 2
fi

if ! command -v curl >/dev/null 2>&1; then
  echo "curl is required" >&2
  exit 1
fi

RUN_DIR="${ARTIFACT_DIR}/${RUN_ID}"
mkdir -p "${RUN_DIR}"

PLACEMENT_FILE="${RUN_DIR}/placements.csv"
PLACEMENT_BEFORE="${RUN_DIR}/placement-before.csv"
PLACEMENT_AFTER="${RUN_DIR}/placement-after.csv"
INGEST_LOG="${RUN_DIR}/ingestion.log"
RETRIEVE_LOG="${RUN_DIR}/retrieval.log"
RETRIEVE_BEFORE_JSON="${RUN_DIR}/retrieval-debug-before.json"
RETRIEVE_AFTER_JSON="${RUN_DIR}/retrieval-debug-after.json"
INGEST_BEFORE_JSON="${RUN_DIR}/ingestion-debug-before.json"
INGEST_AFTER_JSON="${RUN_DIR}/ingestion-debug-after.json"
PROBE_BEFORE_CSV="${RUN_DIR}/probe-before.csv"
PROBE_AFTER_CSV="${RUN_DIR}/probe-after.csv"
PROBE_DIFF_CSV="${RUN_DIR}/probe-diff.csv"
SUMMARY_PATH="${RUN_DIR}/summary.md"

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
}
trap cleanup EXIT

wait_for_health() {
  local service_name="$1"
  local url="$2"
  local deadline=$((SECONDS + MAX_WAIT_SECONDS))
  while (( SECONDS < deadline )); do
    if curl -sS "${url}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.2
  done
  echo "${service_name} did not become healthy within ${MAX_WAIT_SECONDS}s" >&2
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

write_initial_placement() {
  cat > "${PLACEMENT_FILE}" <<EOF
${TENANT_ID},0,1,node-a,leader,healthy
${TENANT_ID},0,1,node-b,follower,healthy
EOF
  cp "${PLACEMENT_FILE}" "${PLACEMENT_BEFORE}"
}

write_rebalanced_placement() {
  : > "${PLACEMENT_FILE}"
  local shard_id epoch leader follower
  for ((shard_id=0; shard_id<TARGET_SHARDS; shard_id++)); do
    epoch=1
    if [[ "${shard_id}" -eq 0 ]]; then
      epoch=2
    fi
    if (( shard_id % 2 == 0 )); then
      leader="node-a"
      follower="node-b"
    else
      leader="node-b"
      follower="node-a"
    fi
    printf '%s,%s,%s,%s,leader,healthy\n' \
      "${TENANT_ID}" "${shard_id}" "${epoch}" "${leader}" >> "${PLACEMENT_FILE}"
    printf '%s,%s,%s,%s,follower,healthy\n' \
      "${TENANT_ID}" "${shard_id}" "${epoch}" "${follower}" >> "${PLACEMENT_FILE}"
  done
  cp "${PLACEMENT_FILE}" "${PLACEMENT_AFTER}"
}

debug_ingestion() {
  local key="$1"
  curl -fsS "http://${INGEST_BIND}/debug/placement?tenant_id=${TENANT_ID}&entity_key=${key}"
}

debug_retrieval() {
  local key="$1"
  curl -fsS "http://${RETRIEVE_BIND}/debug/placement?tenant_id=${TENANT_ID}&entity_key=${key}"
}

json_int() {
  local raw="$1"
  local key="$2"
  local value
  value="$(printf '%s' "${raw}" | sed -nE "s/.*\"${key}\":([0-9]+).*/\\1/p" | head -n 1)"
  if [[ -z "${value}" ]]; then
    echo "0"
  else
    echo "${value}"
  fi
}

json_bool() {
  local raw="$1"
  local key="$2"
  local value
  value="$(printf '%s' "${raw}" | sed -nE "s/.*\"${key}\":(true|false).*/\\1/p" | head -n 1)"
  if [[ -z "${value}" ]]; then
    echo "false"
  else
    echo "${value}"
  fi
}

json_string() {
  local raw="$1"
  local key="$2"
  local value
  value="$(printf '%s' "${raw}" | sed -nE "s/.*\"${key}\":\"([^\"]*)\".*/\\1/p" | head -n 1)"
  if [[ -z "${value}" ]]; then
    echo "n/a"
  else
    echo "${value}"
  fi
}

route_probe_int() {
  local raw="$1"
  local key="$2"
  local value
  value="$(printf '%s' "${raw}" | sed -nE "s/.*\"route_probe\":\\{[^}]*\"${key}\":([0-9]+).*/\\1/p" | head -n 1)"
  if [[ -z "${value}" ]]; then
    echo "0"
  else
    echo "${value}"
  fi
}

route_probe_bool() {
  local raw="$1"
  local key="$2"
  local value
  value="$(printf '%s' "${raw}" | sed -nE "s/.*\"route_probe\":\\{[^}]*\"${key}\":(true|false).*/\\1/p" | head -n 1)"
  if [[ -z "${value}" ]]; then
    echo "false"
  else
    echo "${value}"
  fi
}

route_probe_string() {
  local raw="$1"
  local key="$2"
  local value
  value="$(printf '%s' "${raw}" | sed -nE "s/.*\"route_probe\":\\{[^}]*\"${key}\":\"([^\"]*)\".*/\\1/p" | head -n 1)"
  if [[ -z "${value}" ]]; then
    echo "n/a"
  else
    echo "${value}"
  fi
}

json_shard_epoch() {
  local raw="$1"
  local shard_id="$2"
  local value
  value="$(printf '%s' "${raw}" | sed -nE "s/.*\"shard_id\":${shard_id},\"epoch\":([0-9]+).*/\\1/p" | head -n 1)"
  if [[ -z "${value}" ]]; then
    echo "0"
  else
    echo "${value}"
  fi
}

wait_for_rebalance_reload() {
  local probe_key="$1"
  local deadline=$((SECONDS + MAX_WAIT_SECONDS))
  while (( SECONDS < deadline )); do
    local ing_raw ret_raw
    ing_raw="$(debug_ingestion "${probe_key}")" || true
    ret_raw="$(debug_retrieval "${probe_key}")" || true
    local ing_shards ret_shards ing_epoch ret_epoch
    ing_shards="$(json_int "${ing_raw}" "shard_count")"
    ret_shards="$(json_int "${ret_raw}" "shard_count")"
    ing_epoch="$(json_shard_epoch "${ing_raw}" 0)"
    ret_epoch="$(json_shard_epoch "${ret_raw}" 0)"
    if [[ "${ing_shards}" -ge "${TARGET_SHARDS}" && "${ret_shards}" -ge "${TARGET_SHARDS}" && "${ing_epoch}" -ge 2 && "${ret_epoch}" -ge 2 ]]; then
      return 0
    fi
    sleep 0.2
  done
  return 1
}

record_probes() {
  local out_path="$1"
  : > "${out_path}"
  printf 'key,shard_id,epoch,target_node_id,status,local_admission\n' >> "${out_path}"
  for ((i=0; i<PROBE_KEYS_COUNT; i++)); do
    local key raw shard_id epoch target_node status local_admission
    key="$(printf "probe-key-%03d" "${i}")"
    raw="$(debug_retrieval "${key}")"
    shard_id="$(route_probe_int "${raw}" "shard_id")"
    epoch="$(route_probe_int "${raw}" "epoch")"
    target_node="$(route_probe_string "${raw}" "target_node_id")"
    status="$(route_probe_string "${raw}" "status")"
    local_admission="$(route_probe_bool "${raw}" "local_admission")"
    printf '%s,%s,%s,%s,%s,%s\n' \
      "${key}" "${shard_id}" "${epoch}" "${target_node}" "${status}" "${local_admission}" >> "${out_path}"
  done
}

diff_probe_files() {
  awk -F',' '
    NR==1 { next }
    FNR==NR { before[$1]=$2; next }
    FNR==1 { next }
    {
      key=$1
      before_shard=before[key]
      after_shard=$2
      moved=(before_shard != after_shard ? "true" : "false")
      print key "," before_shard "," after_shard "," moved
    }
  ' "${PROBE_BEFORE_CSV}" "${PROBE_AFTER_CSV}" > "${PROBE_DIFF_CSV}"
}

echo "[rebalance-drill] run_id=${RUN_ID}"
echo "[rebalance-drill] artifact_dir=${RUN_DIR}"

write_initial_placement
start_services

PRIMARY_PROBE_KEY="probe-key-000"

debug_ingestion "${PRIMARY_PROBE_KEY}" > "${INGEST_BEFORE_JSON}"
debug_retrieval "${PRIMARY_PROBE_KEY}" > "${RETRIEVE_BEFORE_JSON}"
record_probes "${PROBE_BEFORE_CSV}"

write_rebalanced_placement
if ! wait_for_rebalance_reload "${PRIMARY_PROBE_KEY}"; then
  echo "[rebalance-drill] timed out waiting for placement reload" >&2
  exit 1
fi

debug_ingestion "${PRIMARY_PROBE_KEY}" > "${INGEST_AFTER_JSON}"
debug_retrieval "${PRIMARY_PROBE_KEY}" > "${RETRIEVE_AFTER_JSON}"
record_probes "${PROBE_AFTER_CSV}"
diff_probe_files

before_shard_count="$(json_int "$(cat "${RETRIEVE_BEFORE_JSON}")" "shard_count")"
after_shard_count="$(json_int "$(cat "${RETRIEVE_AFTER_JSON}")" "shard_count")"
before_epoch_shard0="$(json_shard_epoch "$(cat "${RETRIEVE_BEFORE_JSON}")" 0)"
after_epoch_shard0="$(json_shard_epoch "$(cat "${RETRIEVE_AFTER_JSON}")" 0)"
before_reload_success="$(json_int "$(cat "${RETRIEVE_BEFORE_JSON}")" "success_total")"
after_reload_success="$(json_int "$(cat "${RETRIEVE_AFTER_JSON}")" "success_total")"

moved_keys_count="$(awk -F',' '$4=="true" {count++} END {print count+0}' "${PROBE_DIFF_CSV}")"
moved_to_new_shard_count="$(awk -F',' '$2=="0" && $3!="0" {count++} END {print count+0}' "${PROBE_DIFF_CSV}")"

if [[ "${after_shard_count}" -lt "${TARGET_SHARDS}" ]]; then
  echo "[rebalance-drill] expected shard_count >= ${TARGET_SHARDS} after rebalance, got ${after_shard_count}" >&2
  exit 1
fi

if [[ "${after_epoch_shard0}" -le "${before_epoch_shard0}" ]]; then
  echo "[rebalance-drill] expected shard-0 epoch increase, before=${before_epoch_shard0} after=${after_epoch_shard0}" >&2
  exit 1
fi

if [[ "${moved_keys_count}" -lt "${REQUIRE_MOVED_KEYS}" ]]; then
  echo "[rebalance-drill] expected >= ${REQUIRE_MOVED_KEYS} moved keys, observed ${moved_keys_count}" >&2
  exit 1
fi

{
  echo "# DASH Rebalance Drill Summary"
  echo
  echo "- run_id: ${RUN_ID}"
  echo "- run_utc: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "- tenant_id: ${TENANT_ID}"
  echo "- local_node_id: ${LOCAL_NODE_ID}"
  echo "- ingest_bind: ${INGEST_BIND}"
  echo "- retrieve_bind: ${RETRIEVE_BIND}"
  echo "- placement_reload_interval_ms: ${PLACEMENT_RELOAD_INTERVAL_MS}"
  echo "- max_wait_seconds: ${MAX_WAIT_SECONDS}"
  echo "- target_shards: ${TARGET_SHARDS}"
  echo
  echo "## Status"
  echo
  echo "| check | result |"
  echo "|---|---|"
  echo "| shard_count increase (>=${TARGET_SHARDS}) | PASS |"
  echo "| shard-0 epoch transition | PASS |"
  echo "| moved key threshold (${REQUIRE_MOVED_KEYS}) | PASS |"
  echo
  echo "## Metrics"
  echo
  echo "| metric | value |"
  echo "|---|---:|"
  echo "| target_shards | ${TARGET_SHARDS} |"
  echo "| probe_keys_count | ${PROBE_KEYS_COUNT} |"
  echo "| moved_keys_count | ${moved_keys_count} |"
  echo "| moved_to_new_shard_count | ${moved_to_new_shard_count} |"
  echo "| shard_count_before | ${before_shard_count} |"
  echo "| shard_count_after | ${after_shard_count} |"
  echo "| shard0_epoch_before | ${before_epoch_shard0} |"
  echo "| shard0_epoch_after | ${after_epoch_shard0} |"
  echo "| reload_success_before | ${before_reload_success} |"
  echo "| reload_success_after | ${after_reload_success} |"
  echo
  echo "## Moved Keys (first 20)"
  echo
  echo "| key | before_shard | after_shard | moved |"
  echo "|---|---:|---:|---|"
  awk -F',' '$4=="true" {printf("| %s | %s | %s | %s |\n", $1, $2, $3, $4)}' "${PROBE_DIFF_CSV}" | head -n 20
  echo
  echo "## Artifacts"
  echo
  echo "- placement_before: ${PLACEMENT_BEFORE}"
  echo "- placement_after: ${PLACEMENT_AFTER}"
  echo "- retrieval_debug_before: ${RETRIEVE_BEFORE_JSON}"
  echo "- retrieval_debug_after: ${RETRIEVE_AFTER_JSON}"
  echo "- ingestion_debug_before: ${INGEST_BEFORE_JSON}"
  echo "- ingestion_debug_after: ${INGEST_AFTER_JSON}"
  echo "- probe_before: ${PROBE_BEFORE_CSV}"
  echo "- probe_after: ${PROBE_AFTER_CSV}"
  echo "- probe_diff: ${PROBE_DIFF_CSV}"
  echo "- ingestion_log: ${INGEST_LOG}"
  echo "- retrieval_log: ${RETRIEVE_LOG}"
} > "${SUMMARY_PATH}"

echo "[rebalance-drill] status=success"
echo "[rebalance-drill] summary_path=${SUMMARY_PATH}"
