#!/bin/sh
# DASH container healthcheck.
# Probes the readiness endpoint of the service selected by DASH_BIN.
# DASH_HEALTHCHECK_URL overrides the derived target.
# Exits 0 on HTTP 2xx, non-zero otherwise.
set -eu

DASH_BIN="${DASH_BIN:-retrieval}"

case "$DASH_BIN" in
    ingestion)
        default_url="http://127.0.0.1:8081/v1/ready"
        ;;
    retrieval)
        default_url="http://127.0.0.1:8080/v1/ready"
        ;;
    control-plane)
        default_url="http://127.0.0.1:8090/v1/control-plane/ready"
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
exec wget --quiet --output-document=/dev/null --timeout=4 --tries=1 "$url"
