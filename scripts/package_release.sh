#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

OUT_DIR="${1:-dist}"
RELEASE_VERSION="${DASH_RELEASE_VERSION:-$(date -u +%Y%m%d-%H%M%S)}"
PACKAGE_NAME="dash-${RELEASE_VERSION}"
STAGE_DIR="${OUT_DIR}/${PACKAGE_NAME}"
RUN_BENCH_TRENDS="${DASH_RELEASE_RUN_BENCH_TRENDS:-false}"

if [[ "${RUN_BENCH_TRENDS}" == "true" ]]; then
    echo "[package] running release benchmark trend automation"
    scripts/benchmark_trend.sh --run-tag "release-${RELEASE_VERSION}"
fi

echo "[package] building release binaries"
cargo build --release -p ingestion -p retrieval -p benchmark-smoke

echo "[package] preparing staging directory: ${STAGE_DIR}"
rm -rf "${STAGE_DIR}"
mkdir -p "${STAGE_DIR}/bin" "${STAGE_DIR}/docs" "${STAGE_DIR}/deploy" "${STAGE_DIR}/scripts"

cp target/release/ingestion "${STAGE_DIR}/bin/"
cp target/release/retrieval "${STAGE_DIR}/bin/"
cp target/release/benchmark-smoke "${STAGE_DIR}/bin/"

cp docs/architecture/eme-architecture.md "${STAGE_DIR}/docs/"
cp docs/benchmarks/evaluation-protocol.md "${STAGE_DIR}/docs/"
cp docs/execution/eme-master-plan.md "${STAGE_DIR}/docs/"
cp docs/execution/dash-production-runbook.md "${STAGE_DIR}/docs/"
cp docs/execution/dash-startup-env-matrix.md "${STAGE_DIR}/docs/"
cp docs/execution/dash-release-checklist.md "${STAGE_DIR}/docs/"
cp -R deploy/systemd "${STAGE_DIR}/deploy/"
cp -R deploy/container "${STAGE_DIR}/deploy/"
cp scripts/deploy_systemd.sh "${STAGE_DIR}/scripts/"
cp scripts/deploy_container.sh "${STAGE_DIR}/scripts/"
cp scripts/failover_drill.sh "${STAGE_DIR}/scripts/"
cp scripts/backup_state_bundle.sh "${STAGE_DIR}/scripts/"
cp scripts/restore_state_bundle.sh "${STAGE_DIR}/scripts/"
cp scripts/recovery_drill.sh "${STAGE_DIR}/scripts/"
cp scripts/slo_guard.sh "${STAGE_DIR}/scripts/"

mkdir -p "${OUT_DIR}"
ARCHIVE_PATH="${OUT_DIR}/${PACKAGE_NAME}.tar.gz"
echo "[package] creating archive: ${ARCHIVE_PATH}"
tar -czf "${ARCHIVE_PATH}" -C "${OUT_DIR}" "${PACKAGE_NAME}"

if command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "${ARCHIVE_PATH}" > "${ARCHIVE_PATH}.sha256"
    echo "[package] checksum: ${ARCHIVE_PATH}.sha256"
fi

echo "[package] done"
echo "[package] archive: ${ARCHIVE_PATH}"
