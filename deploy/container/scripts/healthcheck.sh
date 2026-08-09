#!/bin/sh
# DASH container healthcheck.
# Probes the running service's /health endpoint over HTTP.
# Uses DASH_HEALTHCHECK_URL env var, falling back to the retrieval port.
# Returns 0 on HTTP 2xx, non-zero otherwise.
set -eu

url="${DASH_HEALTHCHECK_URL:-http://127.0.0.1:8080/health}"

# wget -qO- issues a GET request and exits non-zero on non-2xx
# or on transport failure. We discard the body and use a tight timeout
# so a hung service fails the healthcheck quickly.
exec wget --quiet --output-document=/dev/null --timeout=4 --tries=1 "$url"
