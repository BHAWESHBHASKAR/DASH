#!/usr/bin/env bash
# Captures an upgrade-compatibility fixture from a released version of DASH.
#
# The fixture is the on-disk state and the wire answers that release produced
# for the fixed dataset in tests/compat/dataset. The compat tests
# (tests/compat) start the CURRENT code on every fixture and check that it
# loads, serves the same results and migrates forward.
#
# Usage:
#   scripts/compat/generate_fixtures.sh --ref <git ref> --label <name> [options]
#
# Options:
#   --ref REF        git ref of the release to capture (e.g. v0.3.0, origin/main)
#   --label NAME     fixture directory name under tests/compat/fixtures
#   --bin-dir DIR    use already-built binaries (ingestion, retrieval,
#                    control-plane) instead of building REF
#   --scratch DIR    scratch directory (default: mktemp -d); removed at exit
#   --keep-scratch   keep the scratch directory (worktree and build output)
#
# The release is built in a temporary git worktree with its OWN target
# directory under the scratch directory (a different source tree must never
# share a target directory with the working tree), and both are deleted when
# the script exits. Needs: git, cargo, python3, gzip.
#
# Release checklist: run this for every release against the release
# candidate commit with the tag as label, add the label to FIXTURES in
# tests/compat/src/lib.rs, commit, then tag (see the release checklist in
# docs/operations/upgrades.md; scripts/compat/check_release_fixture.sh is
# the release workflow's gate).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
REF=""
LABEL=""
BIN_DIR=""
SCRATCH=""
KEEP=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --ref) REF="$2"; shift 2 ;;
    --label) LABEL="$2"; shift 2 ;;
    --bin-dir) BIN_DIR="$2"; shift 2 ;;
    --scratch) SCRATCH="$2"; shift 2 ;;
    --keep-scratch) KEEP=1; shift ;;
    -h|--help) sed -n '2,29p' "$0"; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

if [[ -z "$LABEL" ]]; then
  echo "--label is required" >&2
  exit 2
fi
if [[ -z "$REF" && -z "$BIN_DIR" ]]; then
  echo "one of --ref or --bin-dir is required" >&2
  exit 2
fi
if [[ ! "$LABEL" =~ ^[A-Za-z0-9._-]+$ ]]; then
  echo "--label may only contain letters, digits, '.', '_' and '-'" >&2
  exit 2
fi

OUT="$ROOT/tests/compat/fixtures/$LABEL"
if [[ -e "$OUT" ]]; then
  echo "fixture '$OUT' already exists; remove it first to regenerate" >&2
  exit 1
fi

SCRATCH="${SCRATCH:-$(mktemp -d -t dash-compat-gen-XXXXXX)}"
mkdir -p "$SCRATCH"
WORKTREE="$SCRATCH/src"

cleanup() {
  if [[ "$KEEP" -eq 0 ]]; then
    if [[ -d "$WORKTREE" ]]; then
      git -C "$ROOT" worktree remove --force "$WORKTREE" >/dev/null 2>&1 || true
    fi
    rm -rf "$SCRATCH"
  else
    echo "scratch kept at $SCRATCH"
  fi
}
trap cleanup EXIT

COMMIT="unknown"
if [[ -z "$BIN_DIR" ]]; then
  COMMIT="$(git -C "$ROOT" rev-parse "$REF^{commit}")"
  git -C "$ROOT" worktree add --detach "$WORKTREE" "$COMMIT" >/dev/null
  echo "building $REF ($COMMIT) in $WORKTREE"
  (
    cd "$WORKTREE"
    CARGO_TARGET_DIR="$SCRATCH/target" CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
      cargo build -p ingestion -p retrieval -p control-plane --bins
  )
  BIN_DIR="$SCRATCH/target/debug"
elif [[ -n "$REF" ]]; then
  COMMIT="$(git -C "$ROOT" rev-parse "$REF^{commit}")"
fi

mkdir -p "$OUT"
python3 -I "$ROOT/scripts/compat/run_scenario.py" \
  --bin-dir "$BIN_DIR" --dataset "$ROOT/tests/compat/dataset" --out "$OUT"

# redb preallocates; compress it to keep the fixture small.
gzip -9 -n "$OUT/state/ingest.redb"

cat > "$OUT/FIXTURE.txt" <<EOF
label: $LABEL
ref: ${REF:-<prebuilt binaries>}
commit: $COMMIT
generated: $(date -u +%Y-%m-%d)
generator: scripts/compat/generate_fixtures.sh
dataset: tests/compat/dataset (ingest.jsonl, deletes.jsonl, retrieve.jsonl, placements.csv)
EOF

echo "fixture written to $OUT"
du -sh "$OUT"
