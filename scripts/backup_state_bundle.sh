#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

WAL_PATH="${DASH_INGEST_WAL_PATH:-${DASH_RETRIEVAL_WAL_PATH:-}}"
SEGMENT_DIR="${DASH_INGEST_SEGMENT_DIR:-${DASH_RETRIEVAL_SEGMENT_DIR:-}}"
PLACEMENT_FILE="${DASH_ROUTER_PLACEMENT_FILE:-}"
OUTPUT_DIR="${DASH_BACKUP_OUTPUT_DIR:-dist/backups}"
BUNDLE_LABEL="${DASH_BACKUP_LABEL:-$(date -u +%Y%m%d-%H%M%S)}"

usage() {
  cat <<'USAGE'
Usage: scripts/backup_state_bundle.sh [options]

Create a compressed backup bundle for DASH runtime state.

Options:
  --wal-path PATH            WAL file path (default: DASH_INGEST_WAL_PATH or DASH_RETRIEVAL_WAL_PATH)
  --segment-dir PATH         Segment root directory to include (optional)
  --placement-file PATH      Placement CSV file to include (optional)
  --output-dir DIR           Backup output directory (default: dist/backups)
  --bundle-label LABEL       Bundle label suffix (default: UTC timestamp)
  -h, --help                 Show help

Output:
  <output-dir>/dash-backup-<bundle-label>.tar.gz
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --wal-path)
      WAL_PATH="$2"
      shift 2
      ;;
    --segment-dir)
      SEGMENT_DIR="$2"
      shift 2
      ;;
    --placement-file)
      PLACEMENT_FILE="$2"
      shift 2
      ;;
    --output-dir)
      OUTPUT_DIR="$2"
      shift 2
      ;;
    --bundle-label)
      BUNDLE_LABEL="$2"
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

if [[ -z "${WAL_PATH}" ]]; then
  echo "wal path is required (set --wal-path or DASH_INGEST_WAL_PATH / DASH_RETRIEVAL_WAL_PATH)" >&2
  exit 2
fi
if [[ ! -f "${WAL_PATH}" ]]; then
  echo "wal file not found: ${WAL_PATH}" >&2
  exit 1
fi
if [[ -n "${SEGMENT_DIR}" && ! -d "${SEGMENT_DIR}" ]]; then
  echo "segment dir not found: ${SEGMENT_DIR}" >&2
  exit 1
fi
if [[ -n "${PLACEMENT_FILE}" && ! -f "${PLACEMENT_FILE}" ]]; then
  echo "placement file not found: ${PLACEMENT_FILE}" >&2
  exit 1
fi

if ! command -v tar >/dev/null 2>&1; then
  echo "tar is required" >&2
  exit 1
fi

HASH_CMD=""
if command -v shasum >/dev/null 2>&1; then
  HASH_CMD="shasum -a 256"
elif command -v sha256sum >/dev/null 2>&1; then
  HASH_CMD="sha256sum"
fi

SNAPSHOT_PATH="${WAL_PATH}.snapshot"
mkdir -p "${OUTPUT_DIR}"

TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/dash-backup-XXXXXX")"
BUNDLE_ROOT_NAME="dash-backup-${BUNDLE_LABEL}"
BUNDLE_ROOT="${TMP_DIR}/${BUNDLE_ROOT_NAME}"
DATA_ROOT="${BUNDLE_ROOT}/data"
mkdir -p "${DATA_ROOT}/wal"

MANIFEST_PATH="${BUNDLE_ROOT}/MANIFEST.tsv"
{
  printf "key\tvalue\n"
  printf "bundle_label\t%s\n" "${BUNDLE_LABEL}"
  printf "created_at_utc\t%s\n" "$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  printf "hostname\t%s\n" "$(hostname)"
} > "${MANIFEST_PATH}"

WAL_BASENAME="$(basename "${WAL_PATH}")"
cp -p "${WAL_PATH}" "${DATA_ROOT}/wal/${WAL_BASENAME}"
printf "artifact\t%s\t%s\n" "${WAL_PATH}" "data/wal/${WAL_BASENAME}" >> "${MANIFEST_PATH}"

if [[ -f "${SNAPSHOT_PATH}" ]]; then
  SNAPSHOT_BASENAME="$(basename "${SNAPSHOT_PATH}")"
  cp -p "${SNAPSHOT_PATH}" "${DATA_ROOT}/wal/${SNAPSHOT_BASENAME}"
  printf "artifact\t%s\t%s\n" "${SNAPSHOT_PATH}" "data/wal/${SNAPSHOT_BASENAME}" >> "${MANIFEST_PATH}"
fi

if [[ -n "${SEGMENT_DIR}" ]]; then
  mkdir -p "${DATA_ROOT}/segments"
  cp -Rp "${SEGMENT_DIR}/." "${DATA_ROOT}/segments/"
  printf "artifact\t%s\t%s\n" "${SEGMENT_DIR}" "data/segments" >> "${MANIFEST_PATH}"
fi

if [[ -n "${PLACEMENT_FILE}" ]]; then
  mkdir -p "${DATA_ROOT}/placement"
  PLACEMENT_BASENAME="$(basename "${PLACEMENT_FILE}")"
  cp -p "${PLACEMENT_FILE}" "${DATA_ROOT}/placement/${PLACEMENT_BASENAME}"
  printf "artifact\t%s\t%s\n" "${PLACEMENT_FILE}" "data/placement/${PLACEMENT_BASENAME}" >> "${MANIFEST_PATH}"
fi

if [[ -n "${HASH_CMD}" ]]; then
  (
    cd "${BUNDLE_ROOT}"
    find data -type f -print0 | sort -z | xargs -0 ${HASH_CMD}
  ) > "${BUNDLE_ROOT}/CHECKSUMS.sha256"
fi

ARCHIVE_PATH="${OUTPUT_DIR}/${BUNDLE_ROOT_NAME}.tar.gz"
tar -czf "${ARCHIVE_PATH}" -C "${TMP_DIR}" "${BUNDLE_ROOT_NAME}"

rm -rf "${TMP_DIR}"

echo "[backup] bundle: ${ARCHIVE_PATH}"
echo "[backup] wal: ${WAL_PATH}"
if [[ -f "${SNAPSHOT_PATH}" ]]; then
  echo "[backup] snapshot: ${SNAPSHOT_PATH}"
fi
if [[ -n "${SEGMENT_DIR}" ]]; then
  echo "[backup] segments: ${SEGMENT_DIR}"
fi
if [[ -n "${PLACEMENT_FILE}" ]]; then
  echo "[backup] placement: ${PLACEMENT_FILE}"
fi
