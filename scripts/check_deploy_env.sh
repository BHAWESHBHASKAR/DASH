#!/usr/bin/env bash
# Verify that every DASH_* environment variable named in deploy/** is
# actually read by the code.
#
# Deploy artifacts that set a variable nothing reads fail silently at
# runtime (the setting is ignored), so this check fails CI instead.
#
# A name found in deploy/** is accepted when it appears as a string literal
# ("NAME") in the Rust sources under services/ or pkg/, or in the container
# shell scripts (deploy/container/scripts/*.sh), or in one of the two
# allowlists below. EME_* names are the deprecated prefix and may not be used
# in deploy artifacts at all.
#
# Usage: scripts/check_deploy_env.sh [--verbose]
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

VERBOSE=0
if [[ "${1:-}" == "--verbose" ]]; then
  VERBOSE=1
fi

# Variables consumed by docker compose itself (interpolation), not by the
# services.
COMPOSE_ONLY=(
  DASH_PUBLISH_ADDR
  DASH_DEV_UID
  DASH_DEV_GID
)

# Variables introduced by in-flight work that the Rust sources do not read yet
# on this branch. Remove an entry when the code that reads it lands; the
# script reports entries that are already read so stale ones are noticed.
PENDING_CODE=()

DEPLOY_FILES=()
while IFS= read -r f; do
  DEPLOY_FILES+=("${f}")
done < <(find deploy -type f ! -name '.env' | sort)

if [[ "${#DEPLOY_FILES[@]}" -eq 0 ]]; then
  echo "check_deploy_env: no files under deploy/" >&2
  exit 2
fi

# Names used by deploy artifacts. Prefix-only fragments such as "DASH_" or
# "DASH_INGEST_" (from wildcards in prose) are ignored.
mapfile -t used < <(
  grep -ohE '\b(DASH|EME)_[A-Z0-9_]*[A-Z0-9]\b' "${DEPLOY_FILES[@]}" | sort -u
)

# Names the Rust sources read (string literals).
rust_names="$(grep -rhoE '"(DASH|EME)_[A-Z0-9_]+"' --include='*.rs' services pkg | tr -d '"' | sort -u)"

# The shared auth policy (services/common/src/policy.rs) builds per-service
# names at runtime as DASH_<PREFIX>_<SUFFIX>. Derive those names from the
# suffix literals passed to svc.var()/svc.dash() and the service prefixes so
# they are checked against real code instead of being allow-listed by hand.
policy_suffixes="$(grep -ohE 'svc\.(var|dash)\([^"]*"[A-Z0-9_]+"' services/common/src/policy.rs | grep -oE '"[A-Z0-9_]+"' | tr -d '"' | sort -u)"
policy_prefixes="$(grep -rhoE 'prefix: "[A-Z_]+"' --include='*.rs' services | grep -oE '"[A-Z_]+"' | tr -d '"' | grep -vE '^(TEST|CELLTEST)$' | sort -u)"
derived_names=""
for prefix in ${policy_prefixes}; do
  for suffix in ${policy_suffixes}; do
    derived_names+="DASH_${prefix}_${suffix}"$'\n'
  done
done
rust_names="$(printf '%s\n%s\n' "${rust_names}" "${derived_names}" | sort -u)"

# Names consumed by the container shell scripts.
shell_names="$(grep -ohE '\b(DASH|EME)_[A-Z0-9_]*[A-Z0-9]\b' deploy/container/scripts/*.sh | sort -u)"

in_list() {
  local needle="$1" item
  shift
  for item in "$@"; do
    [[ "${item}" == "${needle}" ]] && return 0
  done
  return 1
}

has_line() {
  grep -qxF "$1" <<<"$2"
}

failures=0
for name in "${used[@]}"; do
  case "${name}" in
    EME_*)
      echo "ERROR: ${name} uses the deprecated EME_ prefix; deploy artifacts must use DASH_*" >&2
      grep -rnw --exclude='.env' -- "${name}" deploy >&2 || true
      failures=$((failures + 1))
      continue
      ;;
  esac

  if has_line "${name}" "${rust_names}"; then
    if in_list "${name}" "${PENDING_CODE[@]}"; then
      echo "NOTE: ${name} is now read by the code; remove it from PENDING_CODE in $0" >&2
    fi
    [[ "${VERBOSE}" -eq 1 ]] && echo "ok   ${name} (rust)"
    continue
  fi
  if has_line "${name}" "${shell_names}"; then
    [[ "${VERBOSE}" -eq 1 ]] && echo "ok   ${name} (container script)"
    continue
  fi
  if in_list "${name}" "${COMPOSE_ONLY[@]}"; then
    [[ "${VERBOSE}" -eq 1 ]] && echo "ok   ${name} (compose-only)"
    continue
  fi
  if in_list "${name}" "${PENDING_CODE[@]}"; then
    echo "WARN: ${name} is not read by the code yet (pending in-flight change)" >&2
    continue
  fi

  echo "ERROR: ${name} is set/mentioned in deploy/ but never read by services/ or pkg/" >&2
  grep -rnw --exclude='.env' -- "${name}" deploy >&2 || true
  failures=$((failures + 1))
done

if [[ "${failures}" -gt 0 ]]; then
  echo "check_deploy_env: ${failures} unknown environment variable(s) in deploy/" >&2
  exit 1
fi

echo "check_deploy_env: all ${#used[@]} DASH_* names in deploy/ are read by the code"
