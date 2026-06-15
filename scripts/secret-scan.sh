#!/usr/bin/env bash
# scripts/secret-scan.sh
#
# Run gitleaks across the working tree (without requiring a git index) to
# detect accidentally committed secrets.  Installs gitleaks on demand.
#
# Exit codes:
#   0   — clean
#   1   — at least one finding
#   2   — usage / environment error
#   3   — gitleaks could not be installed
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

JSON_MODE=0
SKIP_INSTALL=0
REPORT_PATH=""

usage() {
  cat <<'USAGE'
Usage: scripts/secret-scan.sh [options]

Options:
  --json              Emit machine-readable JSON summary.
  --report PATH       Also write the gitleaks report to PATH.
  --skip-install      Do not attempt to install gitleaks if missing.
  -h, --help          Show this help.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --json)         JSON_MODE=1; shift ;;
    --report)       REPORT_PATH="$2"; shift 2 ;;
    --skip-install) SKIP_INSTALL=1; shift ;;
    -h|--help)      usage; exit 0 ;;
    *) echo "[secret-scan] unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

log() { printf '[secret-scan] %s\n' "$*" >&2; }

install_gitleaks() {
  if [[ "${SKIP_INSTALL}" -eq 1 ]]; then
    echo "[secret-scan] gitleaks not found and --skip-install set" >&2
    exit 2
  fi
  log "installing gitleaks"
  case "$(uname -s)" in
    Darwin)
      if command -v brew >/dev/null 2>&1; then
        brew install gitleaks >&2
      else
        echo "[secret-scan] Homebrew not found; install gitleaks manually: https://github.com/gitleaks/gitleaks" >&2
        exit 3
      fi
      ;;
    Linux)
      if command -v apt-get >/dev/null 2>&1; then
        # Prefer the official install script; fall back to apt if it fails.
        if ! (curl -fsSL https://raw.githubusercontent.com/gitleaks/gitleaks/main/scripts/install.sh | sh -s -- -b /usr/local/bin) >&2; then
          log "official installer failed; trying apt"
          sudo apt-get update >&2 && sudo apt-get install -y gitleaks >&2
        fi
      else
        curl -fsSL https://raw.githubusercontent.com/gitleaks/gitleaks/main/scripts/install.sh | sh -s -- -b /usr/local/bin >&2
      fi
      ;;
    *)
      echo "[secret-scan] unsupported platform: $(uname -s)" >&2
      exit 3
      ;;
  esac
}

if ! command -v gitleaks >/dev/null 2>&1; then
  install_gitleaks
fi

if ! command -v gitleaks >/dev/null 2>&1; then
  echo "[secret-scan] gitleaks still not available on PATH" >&2
  exit 3
fi

WORKDIR="$(mktemp -d -t dash-secret-scan.XXXXXX)"
trap 'rm -rf "${WORKDIR}"' EXIT
LOG_OUT="${WORKDIR}/gitleaks.log"

# --no-git lets us scan untracked and ignored files too (CI workspaces often
# have material that is not yet committed).  --redact prints findings with
# secret values replaced so the log is safe to share.
log "scanning ${ROOT_DIR}"
set +e
gitleaks detect --source . --no-git --redact --exit-code 1 >"${LOG_OUT}" 2>&1
status=$?
set -e

if [[ -n "${REPORT_PATH}" ]]; then
  mkdir -p "$(dirname "${REPORT_PATH}")"
  cp "${LOG_OUT}" "${REPORT_PATH}"
fi

if [[ "${status}" -ne 0 ]]; then
  log "FAIL: gitleaks reported findings (exit ${status})"
  sed 's/^/  /' "${LOG_OUT}" >&2
  if [[ "${JSON_MODE}" -eq 1 ]]; then
    printf '{"tool":"gitleaks","status":"fail","report":"%s"}\n' "${LOG_OUT}"
  fi
  exit 1
fi

log "clean: no secrets detected"
if [[ "${JSON_MODE}" -eq 1 ]]; then
  printf '{"tool":"gitleaks","status":"ok"}\n'
fi
exit 0
