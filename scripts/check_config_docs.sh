#!/usr/bin/env bash
# Fail when an environment variable read by the code is missing from the
# configuration reference (docs-site/docs/reference/configuration.md).
#
# What it checks (grep based, no build needed):
#   1. every "DASH_*" / "EME_*" string literal in non-test Rust sources under
#      services/*/src, pkg/*/src and tools/*/src (the file is cut at its first
#      #[cfg(test)] line; tests/ directories and *tests.rs files are skipped);
#   2. the per-service names built at runtime: the suffixes passed to
#      `svc.var(lookup, "SUFFIX")` in services/common/src/policy.rs are expanded
#      to DASH_INGEST_<SUFFIX> and DASH_RETRIEVAL_<SUFFIX>, and the audit
#      options DASH_<PREFIX>_AUDIT_FSYNC / _AUDIT_FAIL_CLOSED are expanded the
#      same way.
#
# A variable counts as documented when its exact name appears in the
# reference. An EME_* name is satisfied by its DASH_* twin being documented
# (the reference says which variables have a legacy EME_ alias).
#
# Usage: scripts/check_config_docs.sh [doc-path]

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

DOC="${1:-docs-site/docs/reference/configuration.md}"
if [[ ! -f "${DOC}" ]]; then
  echo "FAIL: configuration reference not found: ${DOC}" >&2
  exit 1
fi

tmp="$(mktemp)"
trap 'rm -f "${tmp}"' EXIT

# 1. String literals from non-test sources.
while IFS= read -r file; do
  awk '/^[[:space:]]*#\[cfg\(test\)\]/ { exit } { print }' "${file}" \
    | grep -oE '"(DASH|EME)_[A-Z0-9_]+"' || true
done < <(
  find services pkg tools -type f -name '*.rs' \
    -path '*/src/*' \
    -not -path '*/tests/*' \
    -not -name '*tests.rs' \
    -not -path '*/target/*' | sort
) | tr -d '"' >> "${tmp}"

# 2a. Suffixes built by the shared auth policy.
if [[ -f services/common/src/policy.rs ]]; then
  while IFS= read -r suffix; do
    printf 'DASH_INGEST_%s\nDASH_RETRIEVAL_%s\n' "${suffix}" "${suffix}" >> "${tmp}"
  done < <(
    awk '/^[[:space:]]*#\[cfg\(test\)\]/ { exit } { print }' services/common/src/policy.rs \
      | grep -oE 'svc\.var\(lookup, "[A-Z0-9_]+"\)' \
      | sed -E 's/.*"([A-Z0-9_]+)".*/\1/' | sort -u
  )
fi

# 2b. Audit options.
if grep -q 'AUDIT_FSYNC' services/common/src/audit.rs 2>/dev/null; then
  for prefix in INGEST RETRIEVAL; do
    printf 'DASH_%s_AUDIT_FSYNC\nDASH_%s_AUDIT_FAIL_CLOSED\n' "${prefix}" "${prefix}" >> "${tmp}"
  done
fi

# Names that appear in the code only inside format strings or doc comments
# are not env reads; drop the bare prefixes that grep can pick up.
names="$(sort -u "${tmp}" | grep -vE '^(DASH|EME)_$' || true)"

total=0
missing=0
while IFS= read -r name; do
  [[ -z "${name}" ]] && continue
  total=$((total + 1))
  if grep -Fq -- "${name}" "${DOC}"; then
    continue
  fi
  if [[ "${name}" == EME_* ]]; then
    twin="DASH_${name#EME_}"
    if grep -Fq -- "${twin}" "${DOC}"; then
      continue
    fi
  fi
  echo "MISSING: ${name} is read by the code but not documented in ${DOC}" >&2
  missing=$((missing + 1))
done <<< "${names}"

if [[ "${total}" -eq 0 ]]; then
  echo "FAIL: found no environment variables in the sources (script is broken?)" >&2
  exit 1
fi

if [[ "${missing}" -gt 0 ]]; then
  echo "config docs check FAILED: ${missing} of ${total} variable(s) undocumented" >&2
  exit 1
fi

echo "config docs OK: ${total} environment variable names are documented in ${DOC}"
