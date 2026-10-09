#!/usr/bin/env bash
# Verify docs/claims-ledger.md against the repository.
#
# Checks (see the ledger header for the syntax):
#   1. every ledger row (| Cnn | ... |) has a status of verified, partial or planned;
#   2. every `path::name` test reference points at a file that exists and
#      contains the named function (grep-based);
#   3. every `doc:path` reference points at a file or directory that exists;
#   4. every `verified` row has at least one test reference, except rows whose
#      only evidence is a static repository fact (for example the license file),
#      which may cite `doc:` paths alone;
#   5. the Rust test count printed in README.md matches the repository
#      (skip with CLAIMS_LEDGER_SKIP_COUNTS=1).
#
# This proves the references are real, not that the tests pass; CI results
# are the source of truth for pass/fail.
#
# Usage: scripts/check_claims_ledger.sh [ledger-path]

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

LEDGER="${1:-docs/claims-ledger.md}"
README="README.md"
errors=0
rows=0
tests_checked=0
docs_checked=0
doc_only_verified=0

fail() {
  echo "FAIL: $*" >&2
  errors=$((errors + 1))
}

if [[ ! -f "${LEDGER}" ]]; then
  echo "FAIL: ledger not found: ${LEDGER}" >&2
  exit 1
fi

# Does file $1 contain a function/test named $2?
has_function() {
  local file="$1" name="$2"
  case "${file}" in
    *.rs)
      grep -Eq "fn[[:space:]]+${name}[[:space:]]*(<|\()" "${file}"
      ;;
    *.py)
      grep -Eq "def[[:space:]]+${name}[[:space:]]*\(" "${file}"
      ;;
    *.go)
      grep -Eq "func[[:space:]]+${name}[[:space:]]*\(" "${file}"
      ;;
    *)
      grep -Fq -- "${name}" "${file}"
      ;;
  esac
}

while IFS= read -r line; do
  rows=$((rows + 1))
  id="$(printf '%s' "${line}" | awk -F'|' '{gsub(/^[ \t]+|[ \t]+$/, "", $2); print $2}')"
  status="$(printf '%s' "${line}" | awk -F'|' '{gsub(/^[ \t]+|[ \t]+$/, "", $4); print $4}')"
  proof="$(printf '%s' "${line}" | awk -F'|' '{print $5}')"

  case "${status}" in
    verified | partial | planned) ;;
    *) fail "${id}: invalid status '${status}' (expected verified, partial or planned)" ;;
  esac

  test_refs=0
  doc_refs=0
  while IFS= read -r token; do
    [[ -z "${token}" ]] && continue
    token="${token#\`}"
    token="${token%\`}"
    if [[ "${token}" == doc:* ]]; then
      path="${token#doc:}"
      path="${path%%#*}"
      docs_checked=$((docs_checked + 1))
      doc_refs=$((doc_refs + 1))
      if [[ ! -e "${path}" ]]; then
        fail "${id}: referenced path does not exist: ${path}"
      fi
    elif [[ "${token}" == *"::"* ]]; then
      file="${token%%::*}"
      name="${token#*::}"
      test_refs=$((test_refs + 1))
      tests_checked=$((tests_checked + 1))
      if [[ ! -f "${file}" ]]; then
        fail "${id}: test file does not exist: ${file}"
      elif ! has_function "${file}" "${name}"; then
        fail "${id}: '${name}' not found in ${file}"
      fi
    fi
  done < <(printf '%s' "${proof}" | grep -oE '`[^`]+`' || true)

  if [[ "${status}" == "verified" && "${test_refs}" -eq 0 && "${doc_refs}" -eq 0 ]]; then
    fail "${id}: status is verified but the row has no test or document reference"
  fi
  if [[ "${status}" == "verified" && "${test_refs}" -eq 0 ]]; then
    doc_only_verified=$((doc_only_verified + 1))
  fi
  if [[ "${status}" == "planned" && "${test_refs}" -gt 0 ]]; then
    fail "${id}: status is planned but the row cites a test as proof (use partial or verified)"
  fi
done < <(grep -E '^\| C[0-9]+ \|' "${LEDGER}")

if [[ "${rows}" -eq 0 ]]; then
  fail "no ledger rows found in ${LEDGER}"
fi

# README test count must match the repository.
if [[ "${CLAIMS_LEDGER_SKIP_COUNTS:-0}" != "1" ]]; then
  actual="$(grep -rE '#\[(tokio::)?test\]' --include='*.rs' \
    --exclude-dir=target --exclude-dir=node_modules --exclude-dir=.git . | wc -l | tr -d ' ')"
  stated="$(grep -E '^\| Rust workspace' "${README}" | awk -F'|' '{gsub(/[ \t]/, "", $3); print $3}' | head -n 1)"
  if [[ -z "${stated}" ]]; then
    fail "could not find the 'Rust workspace' row in the ${README} test table"
  elif [[ "${stated}" != "${actual}" ]]; then
    fail "README states ${stated} Rust tests but the repository has ${actual}; update README.md (and CHANGELOG/ledger notes if needed)"
  fi
fi

if [[ "${errors}" -gt 0 ]]; then
  echo "claims ledger check FAILED: ${errors} problem(s) in ${rows} row(s)" >&2
  exit 1
fi

echo "claims ledger OK: ${rows} rows, ${tests_checked} test references, ${docs_checked} document references (${doc_only_verified} verified rows rely on documents only)"
