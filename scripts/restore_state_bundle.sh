#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

BUNDLE_PATH=""
WAL_PATH="${DASH_INGEST_WAL_PATH:-${DASH_RETRIEVAL_WAL_PATH:-}}"
SEGMENT_DIR="${DASH_INGEST_SEGMENT_DIR:-${DASH_RETRIEVAL_SEGMENT_DIR:-}}"
PLACEMENT_FILE="${DASH_ROUTER_PLACEMENT_FILE:-}"
FORCE_OVERWRITE="false"
VERIFY_ONLY="false"

usage() {
  cat <<'USAGE'
Usage: scripts/restore_state_bundle.sh [options]

Restore a backup bundle created by scripts/backup_state_bundle.sh.

Options:
  --bundle PATH              Backup bundle tar.gz path (required)
  --wal-path PATH            WAL restore target path
  --segment-dir PATH         Segment restore target root (required when bundle includes segments)
  --placement-file PATH      Placement restore target file (required when bundle includes placement)
  --force true|false         Overwrite existing targets (default: false)
  --verify-only true|false   Verify bundle checksums only, no writes (default: false)
  -h, --help                 Show help
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --bundle)
      BUNDLE_PATH="$2"
      shift 2
      ;;
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
    --force)
      FORCE_OVERWRITE="$2"
      shift 2
      ;;
    --verify-only)
      VERIFY_ONLY="$2"
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

if [[ -z "${BUNDLE_PATH}" ]]; then
  echo "bundle path is required (--bundle)" >&2
  exit 2
fi
if [[ ! -f "${BUNDLE_PATH}" ]]; then
  echo "bundle file not found: ${BUNDLE_PATH}" >&2
  exit 1
fi
if [[ -z "${WAL_PATH}" ]]; then
  echo "wal path is required (set --wal-path or DASH_INGEST_WAL_PATH / DASH_RETRIEVAL_WAL_PATH)" >&2
  exit 2
fi
if ! command -v tar >/dev/null 2>&1; then
  echo "tar is required" >&2
  exit 1
fi

assert_safe_destructive_target() {
  local target="$1"
  if [[ -z "${target}" || "${target}" == "/" || "${target}" == "." ]]; then
    echo "unsafe destructive target path: '${target}'" >&2
    exit 2
  fi
}

ensure_writable_target() {
  local target="$1"
  if [[ -e "${target}" && "${FORCE_OVERWRITE}" != "true" ]]; then
    echo "target exists: ${target} (set --force true to overwrite)" >&2
    exit 1
  fi
}

TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/dash-restore-XXXXXX")"
cleanup() {
  rm -rf "${TMP_DIR}"
}
trap cleanup EXIT

tar -xzf "${BUNDLE_PATH}" -C "${TMP_DIR}"

ROOT_CANDIDATE="$(find "${TMP_DIR}" -mindepth 1 -maxdepth 1 -type d | head -n 1)"
if [[ -z "${ROOT_CANDIDATE}" ]]; then
  echo "bundle is invalid: missing root directory" >&2
  exit 1
fi

if [[ -f "${ROOT_CANDIDATE}/CHECKSUMS.sha256" ]]; then
  (
    cd "${ROOT_CANDIDATE}"
    if command -v shasum >/dev/null 2>&1; then
      shasum -a 256 --check CHECKSUMS.sha256
    elif command -v sha256sum >/dev/null 2>&1; then
      sha256sum --check CHECKSUMS.sha256
    else
      echo "checksum file exists but no sha256 tool found (shasum/sha256sum)" >&2
      exit 1
    fi
  )
fi

if [[ "${VERIFY_ONLY}" == "true" ]]; then
  echo "[restore] bundle verification passed: ${BUNDLE_PATH}"
  exit 0
fi

WAL_SOURCE="$(find "${ROOT_CANDIDATE}/data/wal" -mindepth 1 -maxdepth 1 -type f ! -name "*.snapshot" | head -n 1 || true)"
if [[ -z "${WAL_SOURCE}" ]]; then
  echo "bundle is invalid: missing WAL artifact under data/wal" >&2
  exit 1
fi
SNAPSHOT_SOURCE="${WAL_SOURCE}.snapshot"

mkdir -p "$(dirname "${WAL_PATH}")"
ensure_writable_target "${WAL_PATH}"
if [[ "${FORCE_OVERWRITE}" == "true" && -f "${WAL_PATH}" ]]; then
  rm -f "${WAL_PATH}"
fi
cp -p "${WAL_SOURCE}" "${WAL_PATH}"
echo "[restore] wal restored: ${WAL_PATH}"

WAL_SNAPSHOT_TARGET="${WAL_PATH}.snapshot"
if [[ -f "${SNAPSHOT_SOURCE}" ]]; then
  ensure_writable_target "${WAL_SNAPSHOT_TARGET}"
  if [[ "${FORCE_OVERWRITE}" == "true" && -f "${WAL_SNAPSHOT_TARGET}" ]]; then
    rm -f "${WAL_SNAPSHOT_TARGET}"
  fi
  cp -p "${SNAPSHOT_SOURCE}" "${WAL_SNAPSHOT_TARGET}"
  echo "[restore] snapshot restored: ${WAL_SNAPSHOT_TARGET}"
fi

SEGMENT_SOURCE="${ROOT_CANDIDATE}/data/segments"
if [[ -d "${SEGMENT_SOURCE}" ]]; then
  if [[ -z "${SEGMENT_DIR}" ]]; then
    echo "bundle includes segments, but --segment-dir is unset" >&2
    exit 2
  fi
  if [[ -d "${SEGMENT_DIR}" && "${FORCE_OVERWRITE}" != "true" ]]; then
    echo "segment dir exists: ${SEGMENT_DIR} (set --force true to overwrite)" >&2
    exit 1
  fi
  mkdir -p "${SEGMENT_DIR}"
  if [[ "${FORCE_OVERWRITE}" == "true" ]]; then
    assert_safe_destructive_target "${SEGMENT_DIR}"
    find "${SEGMENT_DIR}" -mindepth 1 -maxdepth 1 -exec rm -rf {} +
  fi
  cp -Rp "${SEGMENT_SOURCE}/." "${SEGMENT_DIR}/"
  echo "[restore] segments restored: ${SEGMENT_DIR}"
fi

PLACEMENT_SOURCE="$(find "${ROOT_CANDIDATE}/data/placement" -mindepth 1 -maxdepth 1 -type f 2>/dev/null | head -n 1 || true)"
if [[ -n "${PLACEMENT_SOURCE}" ]]; then
  if [[ -z "${PLACEMENT_FILE}" ]]; then
    echo "bundle includes placement data, but --placement-file is unset" >&2
    exit 2
  fi
  mkdir -p "$(dirname "${PLACEMENT_FILE}")"
  ensure_writable_target "${PLACEMENT_FILE}"
  if [[ "${FORCE_OVERWRITE}" == "true" && -f "${PLACEMENT_FILE}" ]]; then
    rm -f "${PLACEMENT_FILE}"
  fi
  cp -p "${PLACEMENT_SOURCE}" "${PLACEMENT_FILE}"
  echo "[restore] placement restored: ${PLACEMENT_FILE}"
fi

echo "[restore] completed from bundle: ${BUNDLE_PATH}"
