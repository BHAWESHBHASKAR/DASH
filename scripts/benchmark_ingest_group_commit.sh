#!/usr/bin/env bash
# Single-ingest throughput (POST /v1/ingest) against a persistent WAL with the
# default strict durability (fsync before every acknowledgement), for several
# client counts, with WAL group commit off and on.
#
# Every run starts a fresh ingestion service on an empty WAL + redb in
# DATA_ROOT, drives it with the concurrent_load tool and prints one row per
# run. DATA_ROOT must be on the disk you want to measure (not tmpfs, where
# fsync is free).
#
# Build first:
#   cargo build --release -p ingestion --bin ingestion -p benchmark-smoke --bin concurrent_load
#
# Environment (all optional):
#   DASH_GC_BENCH_INGEST_BIN        ingestion binary (target/release/ingestion)
#   DASH_GC_BENCH_LOAD_BIN          load tool (target/release/concurrent_load)
#   DASH_GC_BENCH_CLIENTS           client counts ("1 8 32 64")
#   DASH_GC_BENCH_REQUESTS          requests per client (200)
#   DASH_GC_BENCH_WORKERS           HTTP worker threads (64; >= clients, or
#                                   the worker pool caps concurrency)
#   DASH_GC_BENCH_MODES             "off on" (values of DASH_INGEST_WAL_GROUP_COMMIT;
#                                   "base" runs the binary without setting it)
#   DASH_GC_BENCH_MAX_WAIT_US       DASH_INGEST_WAL_GROUP_COMMIT_MAX_WAIT_US (0)
#   DASH_GC_BENCH_DATA_ROOT         directory for the per-run data (target/gc-bench)
#   DASH_GC_BENCH_BIND              listen address (127.0.0.1:18091)
#   DASH_GC_BENCH_LABEL             label printed in the first column
#   DASH_GC_BENCH_REDB              "on" (default) keeps the redb mirror, "off"
#                                   sets DASH_INGEST_PERSISTENCE_DISABLE=1 to
#                                   measure the WAL path alone

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

INGEST_BIN="${DASH_GC_BENCH_INGEST_BIN:-target/release/ingestion}"
LOAD_BIN="${DASH_GC_BENCH_LOAD_BIN:-target/release/concurrent_load}"
CLIENTS="${DASH_GC_BENCH_CLIENTS:-1 8 32 64}"
REQUESTS="${DASH_GC_BENCH_REQUESTS:-200}"
WORKERS="${DASH_GC_BENCH_WORKERS:-64}"
MODES="${DASH_GC_BENCH_MODES:-off on}"
MAX_WAIT_US="${DASH_GC_BENCH_MAX_WAIT_US:-0}"
DATA_ROOT="${DASH_GC_BENCH_DATA_ROOT:-target/gc-bench}"
BIND="${DASH_GC_BENCH_BIND:-127.0.0.1:18091}"
LABEL="${DASH_GC_BENCH_LABEL:-$(basename "${INGEST_BIN}")}"
REDB="${DASH_GC_BENCH_REDB:-on}"

for bin in "${INGEST_BIN}" "${LOAD_BIN}"; do
  if [[ ! -x "${bin}" ]]; then
    echo "missing binary: ${bin} (see the build command in this script's header)" >&2
    exit 1
  fi
done

SERVER_PID=""
cleanup() {
  if [[ -n "${SERVER_PID}" ]]; then
    kill "${SERVER_PID}" 2>/dev/null || true
    wait "${SERVER_PID}" 2>/dev/null || true
  fi
}
trap cleanup EXIT

wait_for_health() {
  local deadline=$((SECONDS + 30))
  while ((SECONDS < deadline)); do
    if curl -fsS "http://${BIND}/health" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.1
  done
  echo "ingestion did not become healthy on ${BIND}" >&2
  return 1
}

metric() {
  curl -fsS "http://${BIND}/metrics" | awk -v name="$1" '$1 == name { print $2 }'
}

# Body: a new claim per request with a fixed 4-dimensional embedding (so no
# embedding provider runs) and one evidence row.
BODY='{"claim":{"claim_id":"gc-%WORKER%-%REQUEST%","tenant_id":"bench","canonical_text":"Benchmark claim %WORKER% %REQUEST% about group commit","confidence":0.9},"claim_embedding":[0.1,0.2,0.3,0.4],"evidence":[{"evidence_id":"gc-e-%WORKER%-%REQUEST%","claim_id":"gc-%WORKER%-%REQUEST%","source_id":"source://bench","stance":"supports","source_quality":0.9}]}'

printf '%-16s %-5s %7s %8s %7s %10s %9s %9s %9s %10s\n' \
  label mode clients ok failed rps p50_ms p95_ms p99_ms avg_batch
for mode in ${MODES}; do
  for clients in ${CLIENTS}; do
    run_dir="${DATA_ROOT}/${LABEL}-${mode}-${clients}"
    rm -rf "${run_dir}"
    mkdir -p "${run_dir}"
    env_args=(
      DASH_INSECURE_DEV_MODE=1
      DASH_INGEST_BIND="${BIND}"
      DASH_INGEST_HTTP_WORKERS="${WORKERS}"
      DASH_INGEST_HTTP_QUEUE_CAPACITY=4096
      DASH_INGEST_RATE_LIMIT_PER_TENANT_RPS=0
      DASH_INGEST_WAL_PATH="${run_dir}/wal.log"
      DASH_INGEST_PERSISTENCE_PATH="${run_dir}/store.redb"
      RUST_LOG=warn
    )
    if [[ "${REDB}" == "off" ]]; then
      env_args+=(DASH_INGEST_PERSISTENCE_DISABLE=1)
    fi
    if [[ "${mode}" != "base" ]]; then
      env_args+=(
        DASH_INGEST_WAL_GROUP_COMMIT="$([[ "${mode}" == "on" ]] && echo true || echo false)"
        DASH_INGEST_WAL_GROUP_COMMIT_MAX_WAIT_US="${MAX_WAIT_US}"
      )
    fi
    env "${env_args[@]}" "${INGEST_BIN}" >"${run_dir}/server.log" 2>&1 &
    SERVER_PID=$!
    wait_for_health

    # The load tool exits non-zero when any request failed; keep its report
    # and print the failure count instead of aborting the whole run.
    output="$("${LOAD_BIN}" --addr "${BIND}" --path /v1/ingest --method POST \
      --body "${BODY}" --concurrency "${clients}" --requests-per-worker "${REQUESTS}" \
      --warmup-requests 0 --read-timeout-ms 30000 2>&1 || true)"
    field() { awk -v key="$1:" '$1 == key { print $2 }' <<<"${output}"; }
    batches="$(metric dash_ingest_wal_group_commit_batches_total || true)"
    entries="$(metric dash_ingest_wal_group_commit_entries_total || true)"
    avg_batch="-"
    if [[ -n "${batches}" && "${batches}" != "0" ]]; then
      avg_batch="$(awk -v e="${entries}" -v b="${batches}" 'BEGIN { printf "%.2f", e / b }')"
    fi
    printf '%-16s %-5s %7s %8s %7s %10s %9s %9s %9s %10s\n' \
      "${LABEL}" "${mode}" "${clients}" "$(field successful_requests)" "$(field failed_requests)" \
      "$(field throughput_rps)" "$(field latency_p50_ms)" "$(field latency_p95_ms)" \
      "$(field latency_p99_ms)" "${avg_batch}"

    cleanup
    SERVER_PID=""
    rm -rf "${run_dir}"
  done
done
