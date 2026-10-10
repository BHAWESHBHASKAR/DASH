#!/bin/sh
# DASH container healthcheck.
# Probes the readiness endpoint of the service selected by DASH_BIN.
# DASH_HEALTHCHECK_URL overrides the derived target.
# When the service's listener serves TLS (its DASH_*_TLS_CERT_FILE is set)
# the probe uses https:// and verifies the certificate against
# DASH_HEALTHCHECK_CA_FILE when that is set; without it the loopback probe
# skips verification (it sends no credentials and reads only the status).
# Exits 0 on HTTP 2xx, non-zero otherwise.
set -eu

DASH_BIN="${DASH_BIN:-retrieval}"
scheme="http"

case "$DASH_BIN" in
    ingestion)
        if [ -n "${DASH_INGEST_TLS_CERT_FILE:-}" ]; then scheme="https"; fi
        default_url="$scheme://127.0.0.1:8081/v1/ready"
        ;;
    retrieval)
        if [ -n "${DASH_RETRIEVAL_TLS_CERT_FILE:-}" ]; then scheme="https"; fi
        default_url="$scheme://127.0.0.1:8080/v1/ready"
        ;;
    control-plane)
        if [ -n "${DASH_CONTROL_PLANE_TLS_CERT_FILE:-}" ]; then scheme="https"; fi
        default_url="$scheme://127.0.0.1:8090/v1/control-plane/ready"
        ;;
    segment-maintenance-daemon)
        # No HTTP endpoint; check that the process is alive.
        if [ -z "${DASH_HEALTHCHECK_URL:-}" ]; then
            # /proc/<pid>/comm is truncated to 15 characters.
            for comm in /proc/[0-9]*/comm; do
                case "$(cat "$comm" 2>/dev/null || true)" in
                    segment-mainten*) exit 0 ;;
                esac
            done
            exit 1
        fi
        default_url=""
        ;;
    *)
        echo "dash-healthcheck: unknown DASH_BIN='$DASH_BIN'" >&2
        exit 2
        ;;
esac

url="${DASH_HEALTHCHECK_URL:-$default_url}"

# wget issues a GET request and exits non-zero on non-2xx or on
# transport failure. The body is discarded and the timeout is tight so a
# hung service fails the healthcheck quickly.
case "$url" in
    https://*)
        if [ -n "${DASH_HEALTHCHECK_CA_FILE:-}" ]; then
            exec wget --quiet --output-document=/dev/null --timeout=4 --tries=1 \
                --ca-certificate="$DASH_HEALTHCHECK_CA_FILE" "$url"
        fi
        exec wget --quiet --output-document=/dev/null --timeout=4 --tries=1 \
            --no-check-certificate "$url"
        ;;
esac
exec wget --quiet --output-document=/dev/null --timeout=4 --tries=1 "$url"
