#!/usr/bin/env bash
# Generate a .env file with strong random secrets for a local DASH
# Docker Compose deployment.
#
# Usage:
#   ./scripts/generate-secrets.sh
#
# The generated file is written to deploy/container/.env, which Docker
# Compose loads automatically. It is already ignored by .gitignore.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ENV_FILE="$REPO_ROOT/deploy/container/.env"

mkdir -p "$(dirname "$ENV_FILE")"

random_hex() {
    local bytes="$1"
    if command -v openssl >/dev/null 2>&1; then
        openssl rand -hex "$bytes"
    else
        head -c "$bytes" /dev/urandom | xxd -p | tr -d '\n'
    fi
}

cat > "$ENV_FILE" <<EOF
# Auto-generated DASH local deployment secrets.
# Run \`docker compose -f deploy/container/docker-compose.yml up -d --build\`
# after this file is created.
DASH_INGEST_API_KEY=$(random_hex 16)
DASH_INGEST_JWT_HS256_SECRET=$(random_hex 32)
DASH_RETRIEVAL_API_KEY=$(random_hex 16)
DASH_RETRIEVAL_JWT_HS256_SECRET=$(random_hex 32)
EOF

echo "Generated secrets in $ENV_FILE"
echo "Load them with: set -a; source $ENV_FILE; set +a"
