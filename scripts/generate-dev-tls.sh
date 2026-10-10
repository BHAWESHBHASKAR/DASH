#!/usr/bin/env bash
# Generate a private CA and one certificate for local TLS testing of the
# Compose stack (deploy/container/docker-compose.tls.yml).
#
# Usage:
#   scripts/generate-dev-tls.sh [--force] [--output DIR] [--uid UID]
#
# Writes DIR (default deploy/container/tls, ignored by git):
#   ca.crt   private CA certificate (trusted by followers and clients)
#   ca.key   CA private key (keep it away from the services)
#   tls.crt  certificate for ingestion, retrieval, control-plane, localhost
#            and 127.0.0.1, usable for both server and client auth
#   tls.key  its private key
#
# The containers run as UID 10001, which must be able to read tls.key. Run as
# root to hand the files to that UID (mode 0400); otherwise tls.key is left
# world-readable (mode 0644) so the container can read it. This material is
# for development only: production certificates come from your PKI or
# cert-manager (docs/operations/tls.md).

set -euo pipefail
umask 077

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="$REPO_ROOT/deploy/container/tls"
FORCE=0
CONTAINER_UID=10001

while [[ $# -gt 0 ]]; do
    case "$1" in
        --force)
            FORCE=1
            shift
            ;;
        --output)
            OUT_DIR="${2:?--output requires a directory}"
            shift 2
            ;;
        --uid)
            CONTAINER_UID="${2:?--uid requires a number}"
            shift 2
            ;;
        -h|--help)
            sed -n '2,20p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        *)
            echo "unknown option: $1" >&2
            exit 2
            ;;
    esac
done

if ! command -v openssl >/dev/null 2>&1; then
    echo "openssl is required" >&2
    exit 1
fi

if [[ -e "$OUT_DIR/tls.crt" && "$FORCE" -ne 1 ]]; then
    echo "$OUT_DIR/tls.crt exists; pass --force to replace it" >&2
    exit 1
fi

mkdir -p "$OUT_DIR"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

openssl ecparam -name prime256v1 -genkey -noout -out "$work/ca.key"
openssl req -x509 -new -key "$work/ca.key" -sha256 -days 365 \
    -subj "/CN=DASH development CA" \
    -addext "basicConstraints=critical,CA:TRUE" \
    -addext "keyUsage=critical,keyCertSign,cRLSign" \
    -out "$work/ca.crt"

openssl ecparam -name prime256v1 -genkey -noout -out "$work/tls.ec.key"
# PKCS#8, the most widely supported key encoding.
openssl pkcs8 -topk8 -nocrypt -in "$work/tls.ec.key" -out "$work/tls.key"
openssl req -new -key "$work/tls.key" -subj "/CN=dash-dev" -out "$work/tls.csr"
cat > "$work/ext.cnf" <<'EOF'
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature
extendedKeyUsage=serverAuth,clientAuth
subjectAltName=DNS:ingestion,DNS:retrieval,DNS:control-plane,DNS:localhost,IP:127.0.0.1
EOF
openssl x509 -req -in "$work/tls.csr" -CA "$work/ca.crt" -CAkey "$work/ca.key" \
    -CAcreateserial -days 90 -sha256 -extfile "$work/ext.cnf" -out "$work/tls.crt"

install -m 0644 "$work/ca.crt" "$OUT_DIR/ca.crt"
install -m 0600 "$work/ca.key" "$OUT_DIR/ca.key"
install -m 0644 "$work/tls.crt" "$OUT_DIR/tls.crt"
if [[ "$(id -u)" -eq 0 ]]; then
    install -m 0400 -o "$CONTAINER_UID" -g "$CONTAINER_UID" "$work/tls.key" "$OUT_DIR/tls.key"
else
    install -m 0644 "$work/tls.key" "$OUT_DIR/tls.key"
    echo "note: $OUT_DIR/tls.key is world-readable so UID $CONTAINER_UID can read it (development only)" >&2
fi

fingerprint="$(openssl x509 -in "$OUT_DIR/tls.crt" -outform der | sha256sum | cut -d' ' -f1)"
echo "wrote $OUT_DIR/{ca.crt,ca.key,tls.crt,tls.key}"
echo "client certificate fingerprint (for DASH_INGEST_REPLICATION_ALLOWED_CLIENT_CERTS): $fingerprint"
