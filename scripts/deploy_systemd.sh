#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEPLOY_DIR="${ROOT_DIR}/deploy/systemd"

MODE="plan"
SERVICE="all"
PREFIX_DIR="/opt/dash"
ETC_DIR="/etc/dash"
SYSTEMD_DIR="/etc/systemd/system"

usage() {
  cat <<'USAGE'
Usage: scripts/deploy_systemd.sh [options]

Options:
  --mode plan|apply              Default: plan
  --service ingestion|retrieval|maintenance|all
  --prefix-dir PATH              Default: /opt/dash
  --etc-dir PATH                 Default: /etc/dash
  --systemd-dir PATH             Default: /etc/systemd/system
  -h, --help

Examples:
  scripts/deploy_systemd.sh --mode plan --service all
  scripts/deploy_systemd.sh --mode apply --service retrieval --prefix-dir /srv/dash
  scripts/deploy_systemd.sh --mode apply --service maintenance --prefix-dir /srv/dash
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode)
      MODE="${2:-}"
      shift 2
      ;;
    --service)
      SERVICE="${2:-}"
      shift 2
      ;;
    --prefix-dir)
      PREFIX_DIR="${2:-}"
      shift 2
      ;;
    --etc-dir)
      ETC_DIR="${2:-}"
      shift 2
      ;;
    --systemd-dir)
      SYSTEMD_DIR="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage
      exit 2
      ;;
  esac
done

if [[ "${MODE}" != "plan" && "${MODE}" != "apply" ]]; then
  echo "--mode must be plan or apply" >&2
  exit 2
fi

if [[ "${SERVICE}" != "ingestion" && "${SERVICE}" != "retrieval" && "${SERVICE}" != "maintenance" && "${SERVICE}" != "all" ]]; then
  echo "--service must be ingestion, retrieval, maintenance, or all" >&2
  exit 2
fi

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "${TMP_DIR}"' EXIT

stage_unit() {
  local src="$1"
  local dst="$2"
  sed \
    -e "s|/opt/dash|${PREFIX_DIR}|g" \
    -e "s|/etc/dash|${ETC_DIR}|g" \
    "${src}" > "${dst}"
}

stage_selected_units() {
  if [[ "${SERVICE}" == "ingestion" || "${SERVICE}" == "all" ]]; then
    stage_unit "${DEPLOY_DIR}/dash-ingestion.service" "${TMP_DIR}/dash-ingestion.service"
    cp "${DEPLOY_DIR}/ingestion.env.example" "${TMP_DIR}/ingestion.env.example"
  fi
  if [[ "${SERVICE}" == "retrieval" || "${SERVICE}" == "all" ]]; then
    stage_unit "${DEPLOY_DIR}/dash-retrieval.service" "${TMP_DIR}/dash-retrieval.service"
    cp "${DEPLOY_DIR}/retrieval.env.example" "${TMP_DIR}/retrieval.env.example"
  fi
  if [[ "${SERVICE}" == "maintenance" || "${SERVICE}" == "all" ]]; then
    stage_unit "${DEPLOY_DIR}/dash-segment-maintenance.service" "${TMP_DIR}/dash-segment-maintenance.service"
    cp "${DEPLOY_DIR}/segment-maintenance.env.example" "${TMP_DIR}/segment-maintenance.env.example"
  fi
}

print_plan() {
  echo "[deploy-systemd] mode=plan"
  echo "[deploy-systemd] service=${SERVICE}"
  echo "[deploy-systemd] prefix_dir=${PREFIX_DIR}"
  echo "[deploy-systemd] etc_dir=${ETC_DIR}"
  echo "[deploy-systemd] systemd_dir=${SYSTEMD_DIR}"
  echo
  echo "Planned commands:"
  echo "  install -d \"${SYSTEMD_DIR}\" \"${ETC_DIR}\""
  if [[ "${SERVICE}" == "ingestion" || "${SERVICE}" == "all" ]]; then
    echo "  install -m 0644 \"${TMP_DIR}/dash-ingestion.service\" \"${SYSTEMD_DIR}/dash-ingestion.service\""
    echo "  install -m 0644 \"${TMP_DIR}/ingestion.env.example\" \"${ETC_DIR}/ingestion.env\""
    echo "  systemctl enable --now dash-ingestion.service"
  fi
  if [[ "${SERVICE}" == "retrieval" || "${SERVICE}" == "all" ]]; then
    echo "  install -m 0644 \"${TMP_DIR}/dash-retrieval.service\" \"${SYSTEMD_DIR}/dash-retrieval.service\""
    echo "  install -m 0644 \"${TMP_DIR}/retrieval.env.example\" \"${ETC_DIR}/retrieval.env\""
    echo "  systemctl enable --now dash-retrieval.service"
  fi
  if [[ "${SERVICE}" == "maintenance" || "${SERVICE}" == "all" ]]; then
    echo "  install -m 0644 \"${TMP_DIR}/dash-segment-maintenance.service\" \"${SYSTEMD_DIR}/dash-segment-maintenance.service\""
    echo "  install -m 0644 \"${TMP_DIR}/segment-maintenance.env.example\" \"${ETC_DIR}/segment-maintenance.env\""
    echo "  systemctl enable --now dash-segment-maintenance.service"
  fi
  echo "  systemctl daemon-reload"
}

apply_plan() {
  echo "[deploy-systemd] mode=apply"
  install -d "${SYSTEMD_DIR}" "${ETC_DIR}"

  if [[ "${SERVICE}" == "ingestion" || "${SERVICE}" == "all" ]]; then
    install -m 0644 "${TMP_DIR}/dash-ingestion.service" "${SYSTEMD_DIR}/dash-ingestion.service"
    install -m 0644 "${TMP_DIR}/ingestion.env.example" "${ETC_DIR}/ingestion.env"
  fi
  if [[ "${SERVICE}" == "retrieval" || "${SERVICE}" == "all" ]]; then
    install -m 0644 "${TMP_DIR}/dash-retrieval.service" "${SYSTEMD_DIR}/dash-retrieval.service"
    install -m 0644 "${TMP_DIR}/retrieval.env.example" "${ETC_DIR}/retrieval.env"
  fi
  if [[ "${SERVICE}" == "maintenance" || "${SERVICE}" == "all" ]]; then
    install -m 0644 "${TMP_DIR}/dash-segment-maintenance.service" "${SYSTEMD_DIR}/dash-segment-maintenance.service"
    install -m 0644 "${TMP_DIR}/segment-maintenance.env.example" "${ETC_DIR}/segment-maintenance.env"
  fi

  systemctl daemon-reload
  if [[ "${SERVICE}" == "ingestion" || "${SERVICE}" == "all" ]]; then
    systemctl enable --now dash-ingestion.service
  fi
  if [[ "${SERVICE}" == "retrieval" || "${SERVICE}" == "all" ]]; then
    systemctl enable --now dash-retrieval.service
  fi
  if [[ "${SERVICE}" == "maintenance" || "${SERVICE}" == "all" ]]; then
    systemctl enable --now dash-segment-maintenance.service
  fi
  echo "[deploy-systemd] apply complete"
}

stage_selected_units
if [[ "${MODE}" == "plan" ]]; then
  print_plan
else
  apply_plan
fi
