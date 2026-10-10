#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEPLOY_DIR="${ROOT_DIR}/deploy/systemd"

MODE="plan"
SERVICE="all"
PREFIX_DIR="/opt/dash"
ETC_DIR="/etc/dash"
SYSTEMD_DIR="/etc/systemd/system"
SERVICE_USER="dash"

usage() {
  cat <<'USAGE'
Usage: scripts/deploy_systemd.sh [options]

Options:
  --mode plan|apply              Default: plan
  --service ingestion|retrieval|control-plane|maintenance|all
  --prefix-dir PATH              Default: /opt/dash
  --etc-dir PATH                 Default: /etc/dash
  --systemd-dir PATH             Default: /etc/systemd/system
  -h, --help

Env files are installed with mode 0640 (owner root, group dash) and every
REPLACE-ME placeholder is replaced with a freshly generated random secret.
An existing env file is never overwritten. The replication token is shared
between ingestion.env and retrieval.env.

Examples:
  scripts/deploy_systemd.sh --mode plan --service all
  scripts/deploy_systemd.sh --mode apply --service retrieval --prefix-dir /srv/dash
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

case "${SERVICE}" in
  ingestion|retrieval|control-plane|maintenance|all) ;;
  *)
    echo "--service must be ingestion, retrieval, control-plane, maintenance, or all" >&2
    exit 2
    ;;
esac

TMP_DIR="$(mktemp -d)"
chmod 700 "${TMP_DIR}"
trap 'rm -rf "${TMP_DIR}"' EXIT

selected() {
  [[ "${SERVICE}" == "$1" || "${SERVICE}" == "all" ]]
}

# unit name, env file stem for each selectable service
unit_for() {
  case "$1" in
    ingestion) echo "dash-ingestion" ;;
    retrieval) echo "dash-retrieval" ;;
    control-plane) echo "dash-control-plane" ;;
    maintenance) echo "dash-segment-maintenance" ;;
  esac
}

env_for() {
  case "$1" in
    ingestion) echo "ingestion" ;;
    retrieval) echo "retrieval" ;;
    control-plane) echo "control-plane" ;;
    maintenance) echo "segment-maintenance" ;;
  esac
}

SERVICES=()
for s in ingestion retrieval control-plane maintenance; do
  if selected "$s"; then
    SERVICES+=("$s")
  fi
done

random_hex() {
  openssl rand -hex 32
}

# Replace every REPLACE-ME placeholder in an env file with a random secret.
# The replication token is generated once and reused across files.
REPLICATION_TOKEN=""

# Reuse the token from an already-installed ingestion.env / retrieval.env so
# services deployed in separate runs still agree.
existing_replication_token() {
  local f
  for f in "${ETC_DIR}/ingestion.env" "${ETC_DIR}/retrieval.env"; do
    if [[ -r "${f}" ]]; then
      sed -n 's/^DASH_[A-Z]*_REPLICATION_TOKEN=//p' "${f}" | head -n 1
      return 0
    fi
  done
  return 0
}

fill_secrets() {
  local src="$1" dst="$2" line name
  : > "${dst}"
  while IFS= read -r line || [[ -n "${line}" ]]; do
    if [[ "${line}" == *"=REPLACE-ME"* ]]; then
      name="${line%%=*}"
      case "${name}" in
        *_REPLICATION_TOKEN)
          if [[ -z "${REPLICATION_TOKEN}" ]]; then
            REPLICATION_TOKEN="$(existing_replication_token)"
          fi
          if [[ -z "${REPLICATION_TOKEN}" ]]; then
            REPLICATION_TOKEN="$(random_hex)"
          fi
          line="${name}=${REPLICATION_TOKEN}"
          ;;
        *)
          line="${name}=$(random_hex)"
          ;;
      esac
    fi
    printf '%s\n' "${line}" >> "${dst}"
  done < "${src}"
}

stage_unit() {
  local src="$1"
  local dst="$2"
  sed \
    -e "s|/opt/dash|${PREFIX_DIR}|g" \
    -e "s|/etc/dash|${ETC_DIR}|g" \
    "${src}" > "${dst}"
}

stage_selected_units() {
  local s unit env
  for s in "${SERVICES[@]}"; do
    unit="$(unit_for "${s}")"
    env="$(env_for "${s}")"
    stage_unit "${DEPLOY_DIR}/${unit}.service" "${TMP_DIR}/${unit}.service"
    cp "${DEPLOY_DIR}/${env}.env.example" "${TMP_DIR}/${env}.env.example"
  done
}

print_plan() {
  local s unit env
  echo "[deploy-systemd] mode=plan"
  echo "[deploy-systemd] service=${SERVICE}"
  echo "[deploy-systemd] prefix_dir=${PREFIX_DIR}"
  echo "[deploy-systemd] etc_dir=${ETC_DIR}"
  echo "[deploy-systemd] systemd_dir=${SYSTEMD_DIR}"
  echo
  echo "Planned commands:"
  echo "  useradd --system --no-create-home --shell /usr/sbin/nologin ${SERVICE_USER}   # if missing"
  echo "  install -d \"${SYSTEMD_DIR}\""
  echo "  install -d -m 0750 -o root -g ${SERVICE_USER} \"${ETC_DIR}\""
  for s in "${SERVICES[@]}"; do
    unit="$(unit_for "${s}")"
    env="$(env_for "${s}")"
    echo "  install -m 0644 \"${TMP_DIR}/${unit}.service\" \"${SYSTEMD_DIR}/${unit}.service\""
    echo "  install -m 0640 -o root -g ${SERVICE_USER} <${env}.env.example with generated secrets> \"${ETC_DIR}/${env}.env\"   # only if missing"
    echo "  systemctl enable --now ${unit}.service"
  done
  echo "  systemctl daemon-reload"
}

apply_plan() {
  local s unit env filled
  echo "[deploy-systemd] mode=apply"

  if ! id -u "${SERVICE_USER}" >/dev/null 2>&1; then
    useradd --system --no-create-home --shell /usr/sbin/nologin "${SERVICE_USER}"
  fi

  install -d "${SYSTEMD_DIR}"
  install -d -m 0750 -o root -g "${SERVICE_USER}" "${ETC_DIR}"

  for s in "${SERVICES[@]}"; do
    unit="$(unit_for "${s}")"
    env="$(env_for "${s}")"
    install -m 0644 "${TMP_DIR}/${unit}.service" "${SYSTEMD_DIR}/${unit}.service"
    if [[ -e "${ETC_DIR}/${env}.env" ]]; then
      echo "[deploy-systemd] keeping existing ${ETC_DIR}/${env}.env"
    else
      filled="${TMP_DIR}/${env}.env.filled"
      fill_secrets "${TMP_DIR}/${env}.env.example" "${filled}"
      install -m 0640 -o root -g "${SERVICE_USER}" "${filled}" "${ETC_DIR}/${env}.env"
    fi
  done

  systemctl daemon-reload
  for s in "${SERVICES[@]}"; do
    systemctl enable --now "$(unit_for "${s}").service"
  done
  echo "[deploy-systemd] apply complete"
  echo "[deploy-systemd] NOTE: DASH_INGEST_REPLICATION_TOKEN (ingestion.env) and"
  echo "[deploy-systemd]       DASH_RETRIEVAL_REPLICATION_TOKEN (retrieval.env) must match."
}

stage_selected_units
if [[ "${MODE}" == "plan" ]]; then
  print_plan
else
  apply_plan
fi
