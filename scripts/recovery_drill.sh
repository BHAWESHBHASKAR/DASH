#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

WORK_DIR=""
KEEP_ARTIFACTS="false"
MAX_RTO_SECONDS="${DASH_RECOVERY_DRILL_MAX_RTO_SECONDS:-60}"

usage() {
  cat <<'USAGE'
Usage: scripts/recovery_drill.sh [options]

Run a local recovery drill:
1) create persistent WAL+snapshot state
2) backup state bundle
3) restore into a clean target path
4) verify retrieval can replay restored state

Options:
  --work-dir DIR             Working directory (default: temporary directory)
  --max-rto-seconds N        Fail if measured recovery time exceeds N (default: 60)
  --keep-artifacts true|false Keep logs/artifacts after run (default: false)
  -h, --help                 Show help
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --work-dir)
      WORK_DIR="$2"
      shift 2
      ;;
    --max-rto-seconds)
      MAX_RTO_SECONDS="$2"
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

if [[ -z "${WORK_DIR}" ]]; then
  WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/dash-recovery-drill-XXXXXX")"
else
  mkdir -p "${WORK_DIR}"
fi

cleanup() {
  if [[ "${KEEP_ARTIFACTS}" != "true" ]]; then
    rm -rf "${WORK_DIR}"
  fi
}
trap cleanup EXIT

SOURCE_ROOT="${WORK_DIR}/source"
RESTORE_ROOT="${WORK_DIR}/restore"
BACKUP_OUT="${WORK_DIR}/backup"
INGEST_LOG="${WORK_DIR}/ingestion.log"
RETRIEVE_LOG="${WORK_DIR}/retrieval.log"

mkdir -p "${SOURCE_ROOT}" "${RESTORE_ROOT}" "${BACKUP_OUT}"
SOURCE_WAL="${SOURCE_ROOT}/claims.wal"
RESTORE_WAL="${RESTORE_ROOT}/claims.wal"
SAMPLE_SEGMENTS="${SOURCE_ROOT}/segments"
mkdir -p "${SAMPLE_SEGMENTS}/tenant-a"
echo "sample-segment-marker" > "${SAMPLE_SEGMENTS}/tenant-a/.marker"

echo "[drill] generating source state via ingestion bootstrap"
DASH_INGEST_WAL_PATH="${SOURCE_WAL}" \
  DASH_CHECKPOINT_MAX_WAL_RECORDS=1 \
  DASH_INGEST_SEGMENT_DIR="${SAMPLE_SEGMENTS}" \
  cargo run -p ingestion >"${INGEST_LOG}" 2>&1

SOURCE_CLAIMS="$(grep -oE "claims=[0-9]+" "${INGEST_LOG}" | head -n 1 | cut -d= -f2)"
if [[ -z "${SOURCE_CLAIMS}" ]]; then
  echo "failed to parse source claim count from ingestion output" >&2
  echo "log: ${INGEST_LOG}" >&2
  exit 1
fi

echo "[drill] creating backup bundle"
scripts/backup_state_bundle.sh \
  --wal-path "${SOURCE_WAL}" \
  --segment-dir "${SAMPLE_SEGMENTS}" \
  --output-dir "${BACKUP_OUT}" \
  --bundle-label "drill"

BUNDLE_PATH="$(find "${BACKUP_OUT}" -type f -name 'dash-backup-drill*.tar.gz' | head -n 1)"
if [[ -z "${BUNDLE_PATH}" ]]; then
  echo "backup bundle not found under ${BACKUP_OUT}" >&2
  exit 1
fi

RESTORE_START_NS="$(date +%s%N)"
echo "[drill] restoring backup bundle"
scripts/restore_state_bundle.sh \
  --bundle "${BUNDLE_PATH}" \
  --wal-path "${RESTORE_WAL}" \
  --segment-dir "${RESTORE_ROOT}/segments" \
  --force true

echo "[drill] verifying retrieval replay from restored WAL"
DASH_RETRIEVAL_WAL_PATH="${RESTORE_WAL}" \
  DASH_RETRIEVAL_SEGMENT_DIR="${RESTORE_ROOT}/segments" \
  cargo run -p retrieval >"${RETRIEVE_LOG}" 2>&1
RESTORE_END_NS="$(date +%s%N)"

RESTORED_CLAIMS="$(grep -oE "claims=[0-9]+" "${RETRIEVE_LOG}" | head -n 1 | cut -d= -f2)"
if [[ -z "${RESTORED_CLAIMS}" ]]; then
  echo "failed to parse restored claim count from retrieval output" >&2
  echo "log: ${RETRIEVE_LOG}" >&2
  exit 1
fi

RPO_CLAIM_GAP=$((SOURCE_CLAIMS - RESTORED_CLAIMS))
if (( RPO_CLAIM_GAP < 0 )); then
  RPO_CLAIM_GAP=$((RPO_CLAIM_GAP * -1))
fi
RTO_SECONDS=$(( (RESTORE_END_NS - RESTORE_START_NS) / 1000000000 ))

echo "[drill] source_claims=${SOURCE_CLAIMS} restored_claims=${RESTORED_CLAIMS}"
echo "[drill] rpo_claim_gap=${RPO_CLAIM_GAP}"
echo "[drill] rto_seconds=${RTO_SECONDS}"
echo "[drill] artifacts_dir=${WORK_DIR}"

if (( RPO_CLAIM_GAP != 0 )); then
  echo "recovery drill failed: non-zero RPO claim gap" >&2
  exit 1
fi
if (( RTO_SECONDS > MAX_RTO_SECONDS )); then
  echo "recovery drill failed: RTO ${RTO_SECONDS}s exceeds max ${MAX_RTO_SECONDS}s" >&2
  exit 1
fi

echo "[drill] success"
