#!/usr/bin/env bash
set -euo pipefail

# Backup/restore drill for DASH. Exercises the backup/restore scripts
# end-to-end through docker compose: ingest, back up, destroy state,
# restore, and assert the same retrieval results.
#
# Environment:
#   COMPOSE_DIR            - docker compose directory (default: deploy/container)
#   DASH_BACKUP_OUTPUT_DIR - backup destination (default: /tmp/dash-drill-backups)
#   DASH_RESTORE_FORCE     - overwrite existing state (default: true)

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_DIR="${COMPOSE_DIR:-${ROOT_DIR}/deploy/container}"
BACKUP_DIR="${DASH_BACKUP_OUTPUT_DIR:-/tmp/dash-drill-backups}"
FORCE="${DASH_RESTORE_FORCE:-true}"

INGEST_URL="${DASH_DRILL_INGEST_URL:-http://127.0.0.1:8081}"
RETRIEVE_URL="${DASH_DRILL_RETRIEVE_URL:-http://127.0.0.1:8080}"

WAL_CONTAINER_PATH="/var/lib/dash/wal/ingestion.wal"

curl_ingest() {
  if [[ -n "${DASH_INGEST_API_KEY:-}" ]]; then
    curl -s -H "authorization: Bearer ${DASH_INGEST_API_KEY}" "$@"
  else
    curl -s "$@"
  fi
}

curl_retrieve() {
  if [[ -n "${DASH_RETRIEVAL_API_KEY:-}" ]]; then
    curl -s -H "authorization: Bearer ${DASH_RETRIEVAL_API_KEY}" "$@"
  else
    curl -s "$@"
  fi
}

wait_ready() {
  local url="$1"
  for _ in {1..60}; do
    if curl -sf "${url}/ready" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "service not ready: ${url}" >&2
  return 1
}

extract_claim_ids() {
  python3 -c 'import json,sys; d=json.load(sys.stdin); ids=sorted(r["claim_id"] for r in d.get("results", [])); print(json.dumps(ids))'
}

cd "${COMPOSE_DIR}"

# Ensure secrets exist.
if [[ ! -f .env ]]; then
  bash "${ROOT_DIR}/scripts/generate-secrets.sh"
fi
set -a
source .env
set +a

# Bring up a fresh stack.
docker compose down -v >/dev/null 2>&1 || true
docker compose up -d --build
trap 'docker compose down' EXIT

wait_ready "${INGEST_URL}"
wait_ready "${RETRIEVE_URL}"

# Ingest a small document.
if ! curl_ingest -sf -X POST "${INGEST_URL}/v1/ingest" \
     -H 'content-type: application/json' \
     -d '{"claim":{"claim_id":"drill-claim-1","tenant_id":"drill-tenant","canonical_text":"The capital of France is Paris.","confidence":0.95},"evidence":[{"evidence_id":"drill-evidence-1","claim_id":"drill-claim-1","source_id":"drill-source-1","stance":"supports","source_quality":0.9}]}' >/dev/null; then
  echo "ingest request failed" >&2
  exit 1
fi

# Wait for replication and ANN index visibility.
sleep 5

BEFORE="$(mktemp /tmp/dash-drill-before-XXXXXX.json)"
if ! curl_retrieve -sf -X POST "${RETRIEVE_URL}/v1/retrieve" \
     -H 'content-type: application/json' \
     -d '{"tenant_id":"drill-tenant","query":"capital of France","top_k":5}' >"${BEFORE}"; then
  echo "retrieve request failed before backup" >&2
  exit 1
fi

if ! python3 -c 'import json,sys; d=json.load(open(sys.argv[1])); exit(0 if len(d.get("results",[]))>0 else 1)' "${BEFORE}"; then
  echo "retrieve returned no results before backup" >&2
  cat "${BEFORE}" >&2
  exit 1
fi

# Capture pre-backup claim ids.
BEFORE_IDS="$(extract_claim_ids < "${BEFORE}")"

# Backup the WAL.
rm -rf "${BACKUP_DIR}"
mkdir -p "${BACKUP_DIR}"
TMP_WAL="/tmp/ingestion.wal"
docker compose cp "ingestion:${WAL_CONTAINER_PATH}" "${TMP_WAL}" >/dev/null

bash "${ROOT_DIR}/scripts/backup_state_bundle.sh" \
  --wal-path "${TMP_WAL}" \
  --output-dir "${BACKUP_DIR}" \
  --bundle-label drill

BUNDLE="$(ls -t "${BACKUP_DIR}"/dash-backup-drill*.tar.gz 2>/dev/null | head -n 1 || true)"
if [[ -z "${BUNDLE}" ]]; then
  echo "backup bundle not found" >&2
  exit 1
fi

# Destroy all state and start fresh.
docker compose down -v
docker compose up -d

wait_ready "${INGEST_URL}"
wait_ready "${RETRIEVE_URL}"

# Restore the bundle into the ingestion container.
docker compose cp "${BUNDLE}" "ingestion:/tmp/dash-backup.tar.gz"
docker compose exec -T ingestion bash -c \
  'set -e; cd /tmp; rm -rf dash-backup-drill; tar -xzf dash-backup.tar.gz; mkdir -p /var/lib/dash/wal; cp -p dash-backup-*/data/wal/* /var/lib/dash/wal/'

# Restart ingestion so it replays the restored WAL.
docker compose restart ingestion
wait_ready "${INGEST_URL}"
wait_ready "${RETRIEVE_URL}"

# Allow the retrieval follower to catch up. The ingestion container
# may need an extra few seconds after restart before the replication
# endpoint can serve the restored WAL.
sleep 10

AFTER="$(mktemp /tmp/dash-drill-after-XXXXXX.json)"
if ! curl_retrieve -sf -X POST "${RETRIEVE_URL}/v1/retrieve" \
     -H 'content-type: application/json' \
     -d '{"tenant_id":"drill-tenant","query":"capital of France","top_k":5}' >"${AFTER}"; then
  echo "retrieve request failed after restore" >&2
  exit 1
fi

AFTER_IDS="$(extract_claim_ids < "${AFTER}")"

if [[ "${BEFORE_IDS}" != "${AFTER_IDS}" ]]; then
  echo "backup/restore drill FAILED: claim ids differ" >&2
  echo "before: ${BEFORE_IDS}" >&2
  echo "after:  ${AFTER_IDS}" >&2
  exit 1
fi

echo "backup/restore drill passed: bundle=${BUNDLE} claims=${AFTER_IDS}"
