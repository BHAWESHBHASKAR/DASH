#!/usr/bin/env bash
# Generate a .env file with strong random secrets for a local DASH
# Docker Compose deployment.
#
# Usage:
#   ./scripts/generate-secrets.sh [--force] [--output PATH]
#
# The generated file is written to deploy/container/.env, which Docker
# Compose loads automatically. It is ignored by .gitignore and created
# with mode 0600. An existing file is never overwritten unless --force
# is given.
#
# Quickstart:
#   scripts/generate-secrets.sh && \
#     docker compose -f deploy/container/docker-compose.yml up -d --build

set -euo pipefail
umask 077

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="$REPO_ROOT/deploy/container/.env"
FORCE=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --force)
            FORCE=1
            shift
            ;;
        --output)
            ENV_FILE="${2:?--output requires a path}"
            shift 2
            ;;
        -h|--help)
            sed -n '2,15p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        *)
            echo "unknown option: $1" >&2
            exit 2
            ;;
    esac
done

if [[ -e "$ENV_FILE" && "$FORCE" -ne 1 ]]; then
    echo "refusing to overwrite existing $ENV_FILE (use --force to replace it)" >&2
    exit 1
fi

# Print N random bytes as lowercase hex (2*N characters).
random_hex() {
    local bytes="$1"
    if command -v openssl >/dev/null 2>&1; then
        openssl rand -hex "$bytes"
    else
        head -c "$bytes" /dev/urandom | od -An -vtx1 | tr -d ' \n'
    fi
}

mkdir -p "$(dirname "$ENV_FILE")"
tmp="$(mktemp "$(dirname "$ENV_FILE")/.env.XXXXXX")"
trap 'rm -f "$tmp"' EXIT
chmod 600 "$tmp"

# Replication and control-plane tokens are shared between the two sides
# of each link, so the same value is written under both variable names.
replication_token="$(random_hex 32)"
control_plane_token="$(random_hex 32)"

cat > "$tmp" <<ENV
# Auto-generated DASH local deployment secrets. Do not commit this file.
# Start the stack with:
#   docker compose -f deploy/container/docker-compose.yml up -d --build
DASH_INGEST_API_KEY=$(random_hex 32)
DASH_INGEST_JWT_HS256_SECRET=$(random_hex 32)
DASH_RETRIEVAL_API_KEY=$(random_hex 32)
DASH_RETRIEVAL_JWT_HS256_SECRET=$(random_hex 32)
DASH_INGEST_REPLICATION_TOKEN=$replication_token
DASH_RETRIEVAL_REPLICATION_TOKEN=$replication_token
DASH_CONTROL_PLANE_TOKEN=$control_plane_token
DASH_ROUTER_CONTROL_PLANE_TOKEN=$control_plane_token
ENV

mv -f "$tmp" "$ENV_FILE"
chmod 600 "$ENV_FILE"
trap - EXIT

echo "Generated secrets in $ENV_FILE (mode 0600)"
echo "Docker Compose loads this file automatically from deploy/container/."
