#!/usr/bin/env bash
# Fail when the configuration reference is out of date.
#
# The reference page (docs-site/docs/reference/configuration.md) is generated
# from the settings registry in pkg/config (pkg/config/src/registry.rs). This
# script is a thin wrapper around `dash-config docs --check`, which exits
# non-zero when the committed page differs from what the registry generates.
#
# The completeness check of the registry itself (every DASH_* / EME_* name the
# code reads is registered, and every registered name is read) is the test
# `registry_covers_every_env_var_read_by_code` in pkg/config.
#
# To update the page after changing the registry:
#   cargo run -p dash-config -- docs
#
# Usage: scripts/check_config_docs.sh [doc-path]
# Exit codes: 0 up to date, 1 stale, missing or the check could not run.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

args=(docs --check)
if [[ $# -ge 1 ]]; then
  args+=(--path "$1")
fi

if ! cargo run --quiet -p dash-config -- "${args[@]}"; then
  exit 1
fi
