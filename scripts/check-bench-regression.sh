#!/usr/bin/env bash
# Run the Criterion benchmark suite and fail the build if any metric
# regressed by more than the threshold (default: 5%) against the saved
# baseline.
#
# This is the CI gate. On the first run, no baseline exists and the
# script saves one. On subsequent runs, the baseline is loaded and
# Criterion's --threshold flag fails if any metric moved beyond it.
#
# Usage:
#
#   # First run (saves baseline)
#   ./scripts/check-bench-regression.sh --save-baseline main
#
#   # Subsequent runs (compare against saved baseline)
#   ./scripts/check-bench-regression.sh --baseline main --threshold 5
#
# The script is meant to be wired into .github/workflows/rust.yml
# on a schedule (nightly) and on every PR that touches a hot-path
# file. The default threshold of 5% catches real regressions while
# tolerating normal benchmark noise.

set -euo pipefail

# Defaults
THRESHOLD=${THRESHOLD:-5}
BASELINE=${BASELINE:-main}
BENCH_NAME=${BENCH_NAME:-criterion_benches}
MEASUREMENT_TIME=${MEASUREMENT_TIME:-5}
SAMPLE_SIZE=${SAMPLE_SIZE:-100}

# Parse args
SAVE_BASELINE=""
while [ $# -gt 0 ]; do
    case "$1" in
        --threshold)
            THRESHOLD="$2"
            shift 2
            ;;
        --baseline)
            BASELINE="$2"
            shift 2
            ;;
        --save-baseline)
            SAVE_BASELINE="$1"
            shift
            ;;
        --bench)
            BENCH_NAME="$2"
            shift 2
            ;;
        --measurement-time)
            MEASUREMENT_TIME="$2"
            shift 2
            ;;
        --sample-size)
            SAMPLE_SIZE="$2"
            shift 2
            ;;
        --help|-h)
            cat <<EOF
check-bench-regression.sh - fail CI if a benchmark regresses

USAGE:
    check-bench-regression.sh [OPTIONS]

OPTIONS:
    --save-baseline <NAME>    Save a new baseline with this name (no comparison)
    --baseline <NAME>        Compare against this saved baseline
    --threshold <PCT>        Fail if any metric regressed by more than PCT%
                             (default: 5)
    --bench <NAME>           Name of the [[bench]] target (default: criterion_benches)
    --sample-size <N>        Number of samples to collect (default: 100)
    --measurement-time <SEC>  How long to measure each bench (default: 5)
    -h, --help                Show this help

EXIT CODES:
    0  all metrics within threshold
    1  at least one metric regressed beyond threshold
    2  usage error
EOF
            exit 0
            ;;
        *)
            echo "unknown flag: $1" >&2
            exit 2
            ;;
    esac
done

# Verify cargo and the bench target are available
if ! command -v cargo >/dev/null 2>&1; then
    echo "ERROR: cargo not in PATH" >&2
    exit 2
fi

# The Criterion baseline directory lives at
# target/criterion/<BENCH_NAME>/<baseline-name>
BASELINE_DIR="target/criterion/${BENCH_NAME}/${BASELINE}"

if [ -n "${SAVE_BASELINE}" ]; then
    echo "==> saving baseline '${BASELINE}' (this is the first run, no comparison)"
    cargo bench --bench "${BENCH_NAME}" -- --sample-size "${SAMPLE_SIZE}" \
        --measurement-time "${MEASUREMENT_TIME}" --save-baseline "${BASELINE}"
    echo
    echo "Baseline saved to ${BASELINE_DIR}/"
    echo "Future runs can compare against it via: --baseline ${BASELINE}"
    exit 0
fi

if [ ! -d "${BASELINE_DIR}" ]; then
    echo "ERROR: baseline '${BASELINE}' not found at ${BASELINE_DIR}" >&2
    echo "Save it first with: --save-baseline ${BASELINE}" >&2
    exit 2
fi

echo "==> running benches and comparing against baseline '${BASELINE}' (threshold=${THRESHOLD}%)"
echo "    baseline: ${BASELINE_DIR}/"
echo "    sample-size: ${SAMPLE_SIZE}"
echo "    measurement-time: ${MEASUREMENT_TIME}s per bench"
echo

# Criterion exits with non-zero status if --threshold is exceeded.
# We capture the exit code so we can print a summary, but we don't
# fail the whole script prematurely.
set +e
OUTPUT=$(cargo bench --bench "${BENCH_NAME}" -- --sample-size "${SAMPLE_SIZE}" \
    --measurement-time "${MEASUREMENT_TIME}" \
    --baseline "${BASELINE}" \
    --threshold "${THRESHOLD}" \
    --output-format bencher 2>&1)
BENCH_EXIT=$?
set -e

echo "${OUTPUT}" | tail -80

if [ "${BENCH_EXIT}" -ne 0 ]; then
    echo
    echo "BENCH REGRESSION DETECTED: at least one metric regressed by more than ${THRESHOLD}%"
    echo "Inspect the output above. To accept the new numbers, save a fresh baseline:"
    echo "  cargo bench --bench ${BENCH_NAME} -- --save-baseline ${BASELINE}.new"
    echo "and review the diff before merging."
    exit 1
fi

echo
echo "All metrics within threshold. No regression."
exit 0
