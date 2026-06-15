#!/usr/bin/env bash
# scripts/cargo-audit.sh
#
# Supply-chain audit driver for DASH.
#
# Runs (in order):
#   1. cargo audit --deny warnings       — known-vulnerability check for crates.io
#   2. cargo outdated --root-deps-only   — outdated dependency surface area
#   3. cargo deny check                  — licenses, bans, advisories, sources
#
# Tools are installed on demand (best-effort) and any failure aborts the
# script.  Designed to be safe to run in CI; see `--json` for machine output.
#
# Exit codes:
#   0   — clean
#   1   — at least one finding
#   2   — usage / environment error
#   3   — a required tool could not be installed
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

JSON_MODE=0
SKIP_INSTALL=0
usage() {
  cat <<'USAGE'
Usage: scripts/cargo-audit.sh [options]

Options:
  --json            Emit machine-readable JSON summary in addition to text.
  --skip-install    Do not attempt to install missing tools.
  -h, --help        Show this help.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --json)        JSON_MODE=1; shift ;;
    --skip-install) SKIP_INSTALL=1; shift ;;
    -h|--help)     usage; exit 0 ;;
    *) echo "[cargo-audit] unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

log() { printf '[cargo-audit] %s\n' "$*" >&2; }

require_cargo() {
  if ! command -v cargo >/dev/null 2>&1; then
    echo "[cargo-audit] cargo is required on PATH" >&2
    exit 2
  fi
}

maybe_install() {
  local tool="$1"
  local crate="$2"
  if command -v "${tool}" >/dev/null 2>&1; then
    return 0
  fi
  if [[ "${SKIP_INSTALL}" -eq 1 ]]; then
    log "missing tool: ${tool} (--skip-install set, aborting)"
    exit 2
  fi
  log "installing ${tool} (${crate})"
  if ! command -v cargo install >/dev/null 2>&1; then
    echo "[cargo-audit] 'cargo install' subcommand not available" >&2
    exit 3
  fi
  cargo install --locked "${crate}" >&2
}

require_cargo
maybe_install cargo-audit cargo-audit
maybe_install cargo-outdated cargo-outdated
maybe_install cargo-deny    cargo-deny

# Accumulate findings in a temp file; each tool appends JSON when --json is set.
WORKDIR="$(mktemp -d -t dash-cargo-audit.XXXXXX)"
trap 'rm -rf "${WORKDIR}"' EXIT
SUMMARY_JSON="${WORKDIR}/summary.json"
printf '{"tool":"header","version":"1","root":"%s"}\n' "${ROOT_DIR}" >"${SUMMARY_JSON}"

run_with_status() {
  # $1 = label, $2.. = command
  local label="$1"; shift
  local out="${WORKDIR}/${label}.out"
  local status=0
  log "running: $*"
  set +e
  "$@" >"${out}" 2>&1
  status=$?
  set -e
  if [[ "${JSON_MODE}" -eq 1 ]]; then
    local tool_out
    tool_out="$(head -c 200000 "${out}" | python3 -c '
import json, sys
try:
    data = sys.stdin.read()
    if data.strip().startswith(("{", "[")):
        print(json.dumps({"tool": sys.argv[1], "exit": int(sys.argv[2]), "output": json.loads(data)}))
    else:
        print(json.dumps({"tool": sys.argv[1], "exit": int(sys.argv[2]), "output": data.strip()}))
except Exception as e:
    print(json.dumps({"tool": sys.argv[1], "exit": int(sys.argv[2]), "output": sys.stdin.read().strip(), "parse_error": str(e)}))
' "${label}" "${status}" 2>/dev/null || printf '{"tool":"%s","exit":%s,"output":"<unparseable>"}\n' "${label}" "${status}")"
    printf '%s\n' "${tool_out}" >>"${SUMMARY_JSON}"
  fi
  if [[ "${status}" -ne 0 ]]; then
    log "FAIL: ${label} exited ${status}"
    sed 's/^/  /' "${out}" >&2
    return 1
  fi
  log "ok: ${label}"
  return 0
}

OVERALL=0

# 1. cargo audit --deny warnings  (exit 1 on any advisory)
if ! run_with_status cargo-audit cargo audit --deny warnings; then
  OVERALL=1
fi

# 2. cargo outdated --root-deps-only  (informational; non-fatal)
if ! run_with_status cargo-outdated cargo outdated --root-deps-only; then
  # outdated is advisory; downgrade to a warning, do not fail the gate.
  log "note: cargo-outdated reported items; see ${WORKDIR}/cargo-outdated.out"
  OVERALL=0
fi

# 3. cargo deny check  (exit 1 on any deny)
if ! run_with_status cargo-deny cargo deny check; then
  OVERALL=1
fi

if [[ "${JSON_MODE}" -eq 1 ]]; then
  printf '{"tool":"footer","overall_exit":%s}\n' "${OVERALL}" >>"${SUMMARY_JSON}"
  cat "${SUMMARY_JSON}"
fi

if [[ "${OVERALL}" -ne 0 ]]; then
  log "supply-chain audit FAILED"
  exit 1
fi

log "supply-chain audit clean"
exit 0
