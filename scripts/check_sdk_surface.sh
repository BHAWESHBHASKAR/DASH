#!/usr/bin/env bash
# Guard the public surface of the language SDKs (register item SDK-03).
#
# Three checks, all grep/awk based (no toolchains, no build):
#
#   1. The removed generic `delete` API stays removed. The server never had a
#      `/v1/delete` route; deletes are the scoped routes
#      `DELETE /v1/claims/{id}`, `DELETE /v1/evidence/{id}` and
#      `DELETE /v1/tenants/{id}`, exposed as deleteClaim / deleteEvidence /
#      deleteTenant (DeleteClaimAsync ... in C#). In the Java, Kotlin and C#
#      SDKs:
#        * Sources (sdks/java/src, sdks/kotlin/src, sdks/csharp/{src,tests,samples};
#          *.java, *.kt, *.kts, *.cs) may not contain a `/v1/delete` route, a
#          `DeleteRequest` model (deletes take no body) or a bare
#          `delete(` / `Delete(` / `DeleteAsync(` method. Calls on another
#          object (`builder.delete()`, an HTTP library's DELETE) are allowed.
#        * READMEs (sdks/java, sdks/kotlin, sdks/csharp): a `/v1/delete` route,
#          a bare `delete(` / `Delete(` / `DeleteAsync(` call or a
#          `DeleteRequest` type is allowed only in a prose paragraph that says
#          it was "removed", and never inside a fenced code block.
#        * CHANGELOG files are history and are not scanned: their "Removed"
#          notes (and older "Added" entries) may name the old API.
#
#   2. No SDK README documents an endpoint the server does not have. Every
#      `/v1/...`, `/health`, `/ready`, `/live`, `/metrics`, `/debug/*` and
#      `/internal/*` path in sdks/*/README.md must appear in the route tables of
#      docs-site/docs/reference/api.md. (`/v1/delete` is handled by check 1.)
#
#   3. No SDK source calls an endpoint the server does not have: the same rule
#      as check 2 for every `/v1/...` path in the SDK sources (all languages,
#      tests excluded).
#
# Usage:
#   scripts/check_sdk_surface.sh              check the repository
#   scripts/check_sdk_surface.sh --self-test  plant violations in a temporary copy
#                                             and require that each one is caught
#
# Environment: SDK_SURFACE_ROOT overrides the repository root (used by the
# self-test).

set -euo pipefail

DEFAULT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

API_DOC_REL="docs-site/docs/reference/api.md"
SOURCE_DIRS=(
  sdks/java/src
  sdks/kotlin/src
  sdks/csharp/src
  sdks/csharp/tests
  sdks/csharp/samples
)
DELETE_READMES=(sdks/java/README.md sdks/kotlin/README.md sdks/csharp/README.md)

# Paths mentioned in the text on stdin, one per line, normalized (no trailing
# punctuation, wildcard or slash; bare "/v1" dropped).
extract_paths() {
  grep -oE '/v1/[A-Za-z0-9_./*-]*|/(health|ready|live|metrics)([^A-Za-z0-9_-]|$)|/debug/[a-z-]+|/internal/[a-z/-]+' \
    | sed -E 's/[^A-Za-z0-9]+$//; s/\/\*+$//' \
    | grep -vE '^/v1$|^$' \
    | sort -u
}

# check_delete_sources ROOT -> prints findings, returns number of findings
check_delete_sources() {
  local root="$1" dir hits count=0
  for dir in "${SOURCE_DIRS[@]}"; do
    [[ -d "${root}/${dir}" ]] || continue
    hits="$(grep -rnIE --include='*.java' --include='*.kt' --include='*.kts' --include='*.cs' \
      -e '/[vV]1/[dD][eE][lL][eE][tT][eE]' \
      -e 'DeleteRequest' \
      -e '(^|[^.A-Za-z0-9_])[dD]elete(Async)?[[:space:]]*\(' \
      "${root}/${dir}" || true)"
    if [[ -n "${hits}" ]]; then
      while IFS= read -r line; do
        echo "FAIL: removed delete API in SDK source: ${line#"${root}"/}" >&2
        count=$((count + 1))
      done <<< "${hits}"
    fi
  done
  return $((count > 255 ? 255 : count))
}

# check_delete_readmes ROOT
check_delete_readmes() {
  local root="$1" readme out count=0
  for readme in "${DELETE_READMES[@]}"; do
    [[ -f "${root}/${readme}" ]] || continue
    out="$(awk -v file="${readme}" '
      function exposes(s) {
        return (tolower(s) ~ /\/v1\/delete/) \
          || (s ~ /(^|[^A-Za-z])[dD]elete(Async)?[ ]*\(/) \
          || (s ~ /DeleteRequest/)
      }
      function flush() {
        if (para != "" && exposes(para) && para !~ /[Rr]emoved/) {
          print "FAIL: " file ":" start ": README exposes the delete API without saying it was removed"
        }
        para = ""
      }
      /^[ \t]*```/ { flush(); fence = !fence; next }
      fence {
        if (exposes($0)) {
          print "FAIL: " file ":" NR ": README code block uses the delete API: " $0
        }
        next
      }
      /^[ \t]*$/ { flush(); next }
      { if (para == "") start = NR; para = para "\n" $0 }
      END { flush() }
    ' "${root}/${readme}")"
    if [[ -n "${out}" ]]; then
      echo "${out}" >&2
      count=$((count + $(printf '%s\n' "${out}" | wc -l)))
    fi
  done
  return $((count > 255 ? 255 : count))
}

# check_readme_endpoints ROOT
check_readme_endpoints() {
  local root="$1" api="$1/${API_DOC_REL}" readme path count=0 allowed
  if [[ ! -f "${api}" ]]; then
    echo "FAIL: route reference not found: ${API_DOC_REL}" >&2
    return 1
  fi
  allowed="$(extract_paths < "${api}")"
  if [[ -z "${allowed}" ]]; then
    echo "FAIL: no routes found in ${API_DOC_REL}" >&2
    return 1
  fi
  for readme in "${root}"/sdks/*/README.md; do
    [[ -f "${readme}" ]] || continue
    while IFS= read -r path; do
      [[ -z "${path}" || "${path}" == "/v1/delete" ]] && continue
      if ! grep -Fxq -- "${path}" <<< "${allowed}"; then
        echo "FAIL: ${readme#"${root}"/} documents ${path}, which is not in the route tables of ${API_DOC_REL}" >&2
        count=$((count + 1))
      fi
    done < <(extract_paths < "${readme}")
  done
  return $((count > 255 ? 255 : count))
}

# check_source_endpoints ROOT
check_source_endpoints() {
  local root="$1" api="$1/${API_DOC_REL}" file path count=0 allowed
  [[ -f "${api}" ]] || return 0 # reported by check_readme_endpoints
  allowed="$(extract_paths < "${api}")"
  while IFS= read -r file; do
    while IFS= read -r path; do
      [[ -z "${path}" || "${path}" == "/v1/delete" ]] && continue
      [[ "${path}" == /v1/* ]] || continue
      if ! grep -Fxq -- "${path}" <<< "${allowed}"; then
        echo "FAIL: ${file#"${root}"/} calls ${path}, which is not in the route tables of ${API_DOC_REL}" >&2
        count=$((count + 1))
      fi
    done < <(extract_paths < "${file}")
  done < <(find "${root}/sdks" \
    \( -name node_modules -o -name target -o -name build -o -name bin -o -name obj \
       -o -name tests -o -name test -o -name examples -o -name samples \) -prune -o \
    -type f \( -name '*.java' -o -name '*.kt' -o -name '*.cs' -o -name '*.py' -o -name '*.ts' -o -name '*.go' \) \
    -not -name '*_test.go' -print 2> /dev/null)
  return $((count > 255 ? 255 : count))
}

# run_checks ROOT -> returns the number of findings (0 = clean)
run_checks() {
  local root="$1" total=0 n=0
  check_delete_sources "${root}" || n=$?
  total=$((total + n))
  n=0
  check_delete_readmes "${root}" || n=$?
  total=$((total + n))
  n=0
  check_readme_endpoints "${root}" || n=$?
  total=$((total + n))
  n=0
  check_source_endpoints "${root}" || n=$?
  total=$((total + n))
  return $((total > 255 ? 255 : total))
}

self_test() {
  local root="${DEFAULT_ROOT}" tmp status=0 n
  tmp="$(mktemp -d)"
  SELF_TEST_TMP="${tmp}"
  trap 'rm -rf "${SELF_TEST_TMP:-}"' EXIT

  mkdir -p "${tmp}/base/docs-site/docs/reference"
  cp -R "${root}/sdks" "${tmp}/base/sdks"
  cp "${root}/${API_DOC_REL}" "${tmp}/base/${API_DOC_REL}"
  # Drop build output so the copy stays small.
  find "${tmp}/base/sdks" -type d \( -name node_modules -o -name target -o -name build -o -name bin -o -name obj \) -prune -exec rm -rf {} + 2> /dev/null || true

  n=0
  run_checks "${tmp}/base" 2> /dev/null || n=$?
  if [[ "${n}" -ne 0 ]]; then
    echo "self-test FAIL: the unmodified copy does not pass (${n} finding(s)); run without --self-test to see them" >&2
    return 1
  fi

  # expect_failure NAME MUTATION...: mutate a fresh copy and require findings.
  expect_failure() {
    local name="$1"
    shift
    local work="${tmp}/case"
    rm -rf "${work}"
    cp -R "${tmp}/base" "${work}"
    ( cd "${work}" && "$@" )
    n=0
    run_checks "${work}" 2> /dev/null || n=$?
    if [[ "${n}" -eq 0 ]]; then
      echo "self-test FAIL: planted violation not detected: ${name}" >&2
      status=1
    else
      echo "self-test ok: ${name} detected (${n} finding(s))"
    fi
  }

  plant_java_method() {
    mkdir -p sdks/java/src/main/java/dash
    printf 'package dash;\npublic class Planted {\n  public void delete(String id) {}\n}\n' > sdks/java/src/main/java/dash/Planted.java
  }
  plant_kotlin_model() {
    mkdir -p sdks/kotlin/src/main/kotlin
    printf 'data class DeleteRequest(val id: String)\n' > sdks/kotlin/src/main/kotlin/Planted.kt
  }
  plant_csharp_route() {
    mkdir -p sdks/csharp/src
    printf 'class Planted { string Route = "/v1/delete"; }\n' > sdks/csharp/src/Planted.cs
  }
  plant_readme_delete_example() {
    # shellcheck disable=SC2016
    printf '\n```java\nclient.delete(request);\n```\n' >> sdks/java/README.md
  }
  plant_readme_delete_prose() {
    # shellcheck disable=SC2016
    printf '\nCall `DeleteAsync(id)` to remove a claim.\n' >> sdks/csharp/README.md
  }
  plant_kotlin_bare_delete() {
    mkdir -p sdks/kotlin/src/main/kotlin
    printf 'class Planted { suspend fun delete(id: String) = Unit }\n' > sdks/kotlin/src/main/kotlin/Planted.kt
  }
  plant_source_unknown_endpoint() {
    printf 'package dash\n\nconst plantedPath = "/v1/frobnicate"\n' > sdks/go/planted.go
  }
  plant_readme_unknown_endpoint() {
    printf '\nThe client also calls POST /v1/frobnicate.\n' >> sdks/python/README.md
  }

  expect_failure "Java delete() method" plant_java_method
  expect_failure "Kotlin DeleteRequest model" plant_kotlin_model
  expect_failure "C# /v1/delete route" plant_csharp_route
  expect_failure "README code block calling delete()" plant_readme_delete_example
  expect_failure "README prose advertising DeleteAsync" plant_readme_delete_prose
  expect_failure "README documenting an unknown endpoint" plant_readme_unknown_endpoint
  expect_failure "Kotlin bare delete() method" plant_kotlin_bare_delete
  expect_failure "SDK source calling an unknown endpoint" plant_source_unknown_endpoint

  if [[ "${status}" -eq 0 ]]; then
    echo "self-test OK: every planted violation was caught"
  fi
  return "${status}"
}

main() {
  case "${1:-}" in
    --self-test)
      self_test
      return $?
      ;;
    "")
      ;;
    *)
      echo "usage: ${0##*/} [--self-test]" >&2
      return 2
      ;;
  esac

  local root="${SDK_SURFACE_ROOT:-${DEFAULT_ROOT}}" n=0
  run_checks "${root}" || n=$?
  if [[ "${n}" -ne 0 ]]; then
    echo "sdk surface check FAILED: ${n} problem(s)" >&2
    return 1
  fi
  echo "sdk surface OK: no removed /v1/delete API in java/kotlin/csharp, and every README and source endpoint is in ${API_DOC_REL}"
}

main "$@"
