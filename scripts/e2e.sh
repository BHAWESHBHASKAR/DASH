#!/usr/bin/env bash
# Black-box end-to-end suite: builds the real ingestion, retrieval and
# control-plane binaries and runs tests/e2e (crate dash-e2e) against them as
# child processes on 127.0.0.1 with ephemeral ports, temp dirs and freshly
# generated secrets.
#
# Usage:
#   scripts/e2e.sh                 # whole suite (about 5 minutes)
#   scripts/e2e.sh s4_crash        # only test files whose name matches
#   scripts/e2e.sh -- --nocapture  # extra arguments go to the test binary
#
# Environment:
#   E2E_CRASH_CYCLES   kill -9 cycles in the crash-consistency scenario (default 100)
#   E2E_SEED           fixed RNG seed for that scenario (printed on failure)
#   DASH_E2E_PROFILE   "release" to test release binaries (default: debug)
#   DASH_E2E_BIN_DIR   directory with prebuilt ingestion, retrieval and
#                      control-plane binaries (skips the cargo build)
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

profile_flag=()
if [[ "${DASH_E2E_PROFILE:-debug}" == "release" ]]; then
  profile_flag=(--release)
fi

if [[ -z "${DASH_E2E_BIN_DIR:-}" ]]; then
  echo "[e2e] building service binaries"
  cargo build "${profile_flag[@]}" -p ingestion -p retrieval -p control-plane
fi

filter=()
if [[ $# -gt 0 && "$1" != "--" ]]; then
  filter=(--test "$1")
  shift
fi
if [[ $# -gt 0 && "$1" == "--" ]]; then
  shift
fi

echo "[e2e] running dash-e2e (crash cycles: ${E2E_CRASH_CYCLES:-100})"
cargo test "${profile_flag[@]}" -p dash-e2e "${filter[@]}" -- "$@"
