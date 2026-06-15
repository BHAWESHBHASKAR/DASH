#!/bin/sh
# DASH container entrypoint.
# Dispatches to the right service binary based on the DASH_BIN env var
# (one of: ingestion, retrieval, segment-maintenance-daemon).
# All other arguments are forwarded as-is.
set -eu

DASH_HOME="${DASH_HOME:-/opt/dash}"
DASH_BIN="${DASH_BIN:-retrieval}"

case "$DASH_BIN" in
    ingestion)
        BIN="$DASH_HOME/bin/ingestion"
        ;;
    retrieval)
        BIN="$DASH_HOME/bin/retrieval"
        ;;
    segment-maintenance-daemon)
        BIN="$DASH_HOME/bin/segment-maintenance-daemon"
        ;;
    *)
        echo "dash-entrypoint: unknown DASH_BIN='$DASH_BIN' (expected: ingestion | retrieval | segment-maintenance-daemon)" >&2
        exit 2
        ;;
esac

if [ ! -x "$BIN" ]; then
    echo "dash-entrypoint: missing or non-executable binary: $BIN" >&2
    exit 127
fi

# Default CMD is "--serve"; both ingestion and retrieval honor this flag.
# The segment-maintenance-daemon runs its loop when invoked with no flag.
if [ "$#" -eq 0 ] && [ "$DASH_BIN" != "segment-maintenance-daemon" ]; then
    set -- --serve
fi

exec "$BIN" "$@"
