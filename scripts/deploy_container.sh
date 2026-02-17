#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${ROOT_DIR}/deploy/container/docker-compose.yml"
ACTION="${1:-help}"

usage() {
  cat <<'USAGE'
Usage: scripts/deploy_container.sh <action>

Actions:
  build    Build container images
  up       Start stack in detached mode
  down     Stop and remove stack
  ps       Show service status
  logs     Tail logs for both services
  help

Examples:
  scripts/deploy_container.sh build
  scripts/deploy_container.sh up
  scripts/deploy_container.sh logs
USAGE
}

if [[ "${ACTION}" == "help" || "${ACTION}" == "-h" || "${ACTION}" == "--help" ]]; then
  usage
  exit 0
fi

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required" >&2
  exit 127
fi

if docker compose version >/dev/null 2>&1; then
  COMPOSE=(docker compose)
elif command -v docker-compose >/dev/null 2>&1; then
  COMPOSE=(docker-compose)
else
  echo "docker compose (plugin or binary) is required" >&2
  exit 127
fi

run_compose() {
  "${COMPOSE[@]}" -f "${COMPOSE_FILE}" "$@"
}

case "${ACTION}" in
  build)
    run_compose build
    ;;
  up)
    run_compose up -d
    ;;
  down)
    run_compose down
    ;;
  ps)
    run_compose ps
    ;;
  logs)
    run_compose logs -f --tail=200
    ;;
  *)
    echo "Unknown action: ${ACTION}" >&2
    usage
    exit 2
    ;;
esac
