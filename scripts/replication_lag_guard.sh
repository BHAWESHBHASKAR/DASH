#!/usr/bin/env bash
# Replication lag guard.
#
# Compares Prometheus-style metrics from a leader and a follower to verify
# replication is keeping up and healthy. Designed to be invoked by
# scripts/release_candidate_gate.sh before promoting a release.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=./common.sh
source "${ROOT_DIR}/scripts/common.sh" 2>/dev/null || true

LEADER_URL=""
FOLLOWER_URL=""
MAX_CLAIM_LAG=""
MAX_PULL_FAILURES=""
MIN_PULL_SUCCESSES=""
REQUIRE_NO_LAST_ERROR="false"
SUMMARY_DIR=""
RUN_TAG=""
HTTP_CLIENT=""

usage() {
    cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Required:
  --leader-metrics-url URL         Metrics endpoint of the leader (ingestion or source of truth)
  --follower-metrics-url URL       Metrics endpoint of the follower

Optional:
  --max-claim-lag N                Maximum allowed difference in claim counts (leader - follower)
  --max-pull-failures N            Maximum allowed replication pull failures on the follower
  --min-pull-successes N           Minimum required replication pull successes on the follower
  --require-no-last-error {true|false}  Fail if the follower reports a replication last_error flag
  --summary-dir DIR                Directory to write a JSON summary
  --run-tag TAG                    Tag to include in the summary
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --leader-metrics-url)
            LEADER_URL="$2"
            shift 2
            ;;
        --follower-metrics-url)
            FOLLOWER_URL="$2"
            shift 2
            ;;
        --max-claim-lag)
            MAX_CLAIM_LAG="$2"
            shift 2
            ;;
        --max-pull-failures)
            MAX_PULL_FAILURES="$2"
            shift 2
            ;;
        --min-pull-successes)
            MIN_PULL_SUCCESSES="$2"
            shift 2
            ;;
        --require-no-last-error)
            REQUIRE_NO_LAST_ERROR="$2"
            shift 2
            ;;
        --summary-dir)
            SUMMARY_DIR="$2"
            shift 2
            ;;
        --run-tag)
            RUN_TAG="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "unknown argument: $1" >&2
            usage >&2
            exit 2
            ;;
    esac
done

if [[ -z "${LEADER_URL}" || -z "${FOLLOWER_URL}" ]]; then
    echo "--leader-metrics-url and --follower-metrics-url are required" >&2
    usage >&2
    exit 2
fi

detect_http_client() {
    if command -v curl >/dev/null 2>&1; then
        HTTP_CLIENT="curl"
    elif command -v wget >/dev/null 2>&1; then
        HTTP_CLIENT="wget"
    else
        echo "no HTTP client found (curl or wget required)" >&2
        exit 2
    fi
}

fetch_metrics() {
    local url="$1"
    case "${HTTP_CLIENT}" in
        curl)
            curl -fsS --max-time 10 "${url}"
            ;;
        wget)
            wget --quiet --output-document=- --timeout=10 --tries=1 "${url}"
            ;;
    esac
}

extract_metric() {
    local metrics="$1"
    local name="$2"
    # Prometheus exposition: metric line is `name value`. The metric name
    # alone appears on its line (no braces) for the gauges DASH emits.
    awk -v metric="${name}" '$1 == metric {print $2; exit}' <<<"${metrics}"
}

parse_int() {
    local raw="$1"
    if [[ -z "${raw}" ]]; then
        echo ""
        return
    fi
    # Strip decimal part and trim whitespace.
    printf '%s' "${raw}" | sed 's/\..*//' | tr -d '[:space:]'
}

leader_metrics=""
follower_metrics=""

detect_http_client
leader_metrics="$(fetch_metrics "${LEADER_URL}")"
follower_metrics="$(fetch_metrics "${FOLLOWER_URL}")"

# Claim-count lag: leader claims total minus follower visible claims.
# If the follower is itself an ingestion replica, it also exposes dash_ingest_claims_total.
leader_claims="$(extract_metric "${leader_metrics}" "dash_ingest_claims_total")"
follower_claims="$(extract_metric "${follower_metrics}" "dash_ingest_claims_total")"
if [[ -z "${follower_claims}" ]]; then
    follower_claims="$(extract_metric "${follower_metrics}" "dash_retrieve_storage_last_storage_visible_count")"
fi

leader_claims_int="$(parse_int "${leader_claims}")"
follower_claims_int="$(parse_int "${follower_claims}")"

claim_lag=""
if [[ -n "${leader_claims_int}" && -n "${follower_claims_int}" ]]; then
    claim_lag=$((leader_claims_int - follower_claims_int))
    if [[ "${claim_lag}" -lt 0 ]]; then
        claim_lag=0
    fi
else
    claim_lag=""
fi

# Pull health (when the follower is an ingestion replica pulling from the leader).
follower_pull_failures="$(extract_metric "${follower_metrics}" "dash_ingest_replication_pull_failure_total")"
follower_pull_successes="$(extract_metric "${follower_metrics}" "dash_ingest_replication_pull_success_total")"
follower_last_error="$(extract_metric "${follower_metrics}" "dash_ingest_replication_last_error")"

follower_pull_failures_int="$(parse_int "${follower_pull_failures:-0}")"
follower_pull_successes_int="$(parse_int "${follower_pull_successes:-0}")"
follower_last_error_int="$(parse_int "${follower_last_error:-0}")"

# Build a JSON summary regardless of pass/fail so callers can archive it.
summary() {
    local status="$1"
    local reason="$2"
    cat <<EOF
{
  "run_tag": "${RUN_TAG:-}",
  "status": "${status}",
  "reason": "${reason}",
  "leader_url": "${LEADER_URL}",
  "follower_url": "${FOLLOWER_URL}",
  "leader_claims": ${leader_claims_int:-null},
  "follower_claims": ${follower_claims_int:-null},
  "claim_lag": ${claim_lag:-null},
  "max_claim_lag": ${MAX_CLAIM_LAG:-null},
  "follower_pull_failures": ${follower_pull_failures_int:-0},
  "max_pull_failures": ${MAX_PULL_FAILURES:-null},
  "follower_pull_successes": ${follower_pull_successes_int:-0},
  "min_pull_successes": ${MIN_PULL_SUCCESSES:-null},
  "follower_last_error": ${follower_last_error_int:-0},
  "require_no_last_error": ${REQUIRE_NO_LAST_ERROR}
}
EOF
}

pass=true
reasons=()

if [[ -n "${MAX_CLAIM_LAG}" ]]; then
    if [[ -z "${claim_lag}" ]]; then
        pass=false
        reasons+=("claim_lag could not be computed (missing metrics)")
    elif [[ "${claim_lag}" -gt "${MAX_CLAIM_LAG}" ]]; then
        pass=false
        reasons+=("claim_lag ${claim_lag} exceeds max_claim_lag ${MAX_CLAIM_LAG}")
    fi
fi

if [[ -n "${MAX_PULL_FAILURES}" ]]; then
    if [[ "${follower_pull_failures_int}" -gt "${MAX_PULL_FAILURES}" ]]; then
        pass=false
        reasons+=("follower_pull_failures ${follower_pull_failures_int} exceeds max ${MAX_PULL_FAILURES}")
    fi
fi

if [[ -n "${MIN_PULL_SUCCESSES}" ]]; then
    if [[ "${follower_pull_successes_int}" -lt "${MIN_PULL_SUCCESSES}" ]]; then
        pass=false
        reasons+=("follower_pull_successes ${follower_pull_successes_int} below min ${MIN_PULL_SUCCESSES}")
    fi
fi

if [[ "${REQUIRE_NO_LAST_ERROR}" == "true" && "${follower_last_error_int}" -ne 0 ]]; then
    pass=false
    reasons+=("follower reports replication_last_error=1")
fi

if [[ "${pass}" == "true" ]]; then
    reason="replication lag and health within configured bounds"
else
    reason="$(printf '%s; ' "${reasons[@]}")"
    reason="${reason%; }"
fi

if [[ -n "${SUMMARY_DIR}" ]]; then
    mkdir -p "${SUMMARY_DIR}"
    summary "$([[ "${pass}" == "true" ]] && echo "pass" || echo "fail")" "${reason}" \
        >"${SUMMARY_DIR}/replication-lag-${RUN_TAG:-guard}.json"
fi

if [[ "${pass}" == "true" ]]; then
    echo "replication lag guard passed: ${reason}"
    exit 0
else
    echo "replication lag guard failed: ${reason}" >&2
    exit 1
fi
