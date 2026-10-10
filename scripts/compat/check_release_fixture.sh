#!/usr/bin/env bash
# Release gate: the release being tagged has an upgrade-compatibility fixture.
#
# Every release must capture the state it writes so that later releases are
# tested against it (docs/operations/upgrades.md, "Release checklist"):
#
#   scripts/compat/generate_fixtures.sh --ref <release commit> --label <tag>
#
# commit it with the label registered in FIXTURES (tests/compat/src/lib.rs),
# then tag. This script checks both for one tag, and that the fixture was
# generated from the tagged commit or an ancestor of it.
#
# Usage: scripts/compat/check_release_fixture.sh <tag>
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TAG="${1:-}"
if [[ -z "$TAG" ]]; then
  echo "usage: $0 <tag>" >&2
  exit 2
fi

DIR="$ROOT/tests/compat/fixtures/$TAG"
status=0
if [[ ! -f "$DIR/FIXTURE.txt" ]]; then
  echo "FAIL: no upgrade fixture for $TAG (expected $DIR/FIXTURE.txt)." >&2
  echo "      Run: scripts/compat/generate_fixtures.sh --ref <release commit> --label $TAG" >&2
  status=1
fi
if ! grep -Fq "label: \"$TAG\"" "$ROOT/tests/compat/src/lib.rs"; then
  echo "FAIL: fixture label \"$TAG\" is not registered in FIXTURES (tests/compat/src/lib.rs)." >&2
  status=1
fi
if [[ "$status" -eq 0 ]]; then
  # Generated from the release candidate commit and committed before
  # tagging: the recorded commit is the tagged commit or an ancestor.
  recorded="$(sed -n 's/^commit: //p' "$DIR/FIXTURE.txt")"
  if git -C "$ROOT" rev-parse -q --verify "$TAG^{commit}" >/dev/null; then
    if ! git -C "$ROOT" merge-base --is-ancestor "$recorded" "$TAG" 2>/dev/null; then
      echo "FAIL: $DIR/FIXTURE.txt names commit '$recorded', which is not $TAG or an ancestor of it." >&2
      status=1
    fi
  fi
fi
if [[ "$status" -eq 0 ]]; then
  echo "upgrade fixture for $TAG present and registered"
fi
exit "$status"
