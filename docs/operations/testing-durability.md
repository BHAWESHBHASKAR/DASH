# Testing durability, disk-full behavior and load

This page describes the harness that backs DASH's durability claims and
its load numbers: what each tool checks, how to run it, what it does
**not** prove, and where it runs in CI.

| Tool | What it proves | PR CI | Nightly |
|---|---|---|---|
| `tools/crash-test` | Acknowledged ingests survive `kill -9` at random moments; no duplicates, no partial bundles or batches, clean WAL, service ready after restart | 25 cycles (`rust.yml`, job `crash-test`) | 1000 cycles, 200 cycles with checkpoint pressure, plus a self-check (`durability-nightly.yml`) |
| `tests/e2e/tests/s4_crash_consistency.rs` | The same oracle across the leader **and** the retrieval follower | 100 cycles (job `e2e`) | 500 cycles |
| `tests/e2e/tests/s10_disk_full.rs` | Writes on a full volume fail with a 5xx, `/ready` goes not-ready, nothing acknowledged is lost, recovery in place (WAL) or on restart (redb mirror) | yes (job `e2e`) | yes |
| `tools/loadgen` | Throughput and latency (p50/p95/p99/max) of an ingest/retrieve mix; soak with server memory sampling | no | 30-minute soak + 2-minute load report, uploaded as artifacts |

All of them start the real `ingestion` (and, for loadgen and s4, `retrieval`)
binaries through the e2e harness (`tests/e2e/src`), on loopback with
ephemeral ports, generated credentials and a temporary state directory.
Binaries come from `DASH_E2E_BIN_DIR` when set (use it to test release
builds or to skip the cargo build); otherwise the harness runs
`cargo build` for `ingestion`, `retrieval`, `control-plane` and `wal-inspect`
(`DASH_E2E_PROFILE=release` builds release binaries).

## Crash consistency: `crash-test`

```bash
cargo build --release -p ingestion -p retrieval -p control-plane -p wal-inspect -p crash-test
DASH_E2E_BIN_DIR=$PWD/target/release target/release/crash-test --cycles 200 --writers 6
```

Each cycle:

1. `ingestion` runs on a persistent state directory (WAL, snapshot, redb
   mirror) with the default, strict WAL durability settings.
2. `--writers` threads send valid writes as fast as the server answers: single
   bundles (1-4 evidence items, half of them with an edge to the writer's
   previous claim) and, for `--batch-percent` of requests, 3-item atomic
   batches with a commit id. Every claim id is unique.
3. After a seeded random delay (0 to `--max-kill-delay-ms`) the process gets
   `SIGKILL`. A writer whose connection breaks records that request as
   *unknown* and stops.
4. `wal-inspect verify` checks the WAL and snapshot as they were left by the
   crash (a torn final line is allowed: it is dropped on open).
5. The service restarts and must answer `GET /ready` with 200 within
   `--ready-timeout-secs` (default 60). The restart-to-ready time is recorded.
6. The oracle reads the leader's full state through
   `/internal/replication/export` and checks:
   * every request answered 2xx before the kill is present: the claim, all of
     its evidence (mapped to it) and its edge;
   * an unknown request is applied completely or not at all (single bundle:
     claim without all its evidence is a failure; batch: some but not all
     claims is a failure);
   * no claim that was never sent, no evidence without an expected claim, no
     evidence record stored twice, and nothing from an earlier cycle vanished;
   * any non-2xx answer to a valid write before the kill fails the run.
7. `wal-inspect verify` runs again on the recovered files.

The seed is printed on the first line; `--seed N` with the same
`--cycles`/`--writers` replays the same random choices (thread timing still
varies). On failure the state directory is kept and printed, the error names
the cycle and the claim, and the reproduce command is printed. `--json-out`
writes a summary (`acked_requests`, `acked_claims`, `unknown_requests`,
`unknown_applied`, recovery time p50/p95/max, duration).

Other options: `--checkpoint-every N` (sets `DASH_CHECKPOINT_MAX_WAL_RECORDS`
so kills land during snapshot/compaction too), `--fresh-every N` (start from an
empty directory every N cycles to bound replay time on long runs),
`--env KEY=VALUE` (any extra service setting), `--keep-state`.

**Self-check.** The harness must be able to fail. With WAL durability
deliberately disabled (`DASH_INGEST_WAL_BACKGROUND_FLUSH_ONLY=true`, large
append buffer, `DASH_INGEST_ALLOW_UNSAFE_WAL_DURABILITY=true`) acknowledged
writes live only in process memory, and crash-test reports
`acknowledged claim ... is missing after kill -9` within a few cycles. The
nightly workflow runs this and fails if the harness does **not** fail.

### What this guarantees, and what it does not

Guaranteed under the default WAL settings (sync every record, no append
buffer) and tested as above:

* A write acknowledged with a 2xx by `/v1/ingest` or `/v1/ingest/batch`
  survives a crash of the ingestion process at any moment.
* A single ingest and a batch are atomic across a crash: after restart they
  are present completely or not at all.
* Recovery needs no operator action: the torn tail is dropped, the service
  replays and reports ready.

Not covered:

* **Power loss / kernel crash.** `SIGKILL` kills the process, not the page
  cache, so data written but not yet `fsync`ed still reaches the disk. The
  fsync and rename ordering is covered separately by the store failpoint
  tests (`pkg/store/src/failpoint.rs`), not by an end-to-end power-cut test.
* Relaxed durability settings (`DASH_INGEST_WAL_SYNC_EVERY_RECORDS` > 1,
  append buffers, background flush): they are refused unless
  `DASH_INGEST_ALLOW_UNSAFE_WAL_DURABILITY=true`, and acknowledged writes can
  then be lost, as the self-check shows.
* `/v1/ingest/document` is not exercised by crash-test (it is covered by the
  e2e durability scenario s3).
* Replicated (quorum) writes; there is no quorum write path yet.

## Disk full: `s10_disk_full`

```bash
cargo test -p dash-e2e --test s10_disk_full -- --nocapture   # Linux only
```

Unprivileged CI cannot mount a tiny tmpfs or loop device, so the test gives
the ingestion process a file size limit (`RLIMIT_FSIZE`, set in a `pre_exec`
hook, with `SIGXFSZ` ignored). Writing past the limit then fails with
`EFBIG`, which the WAL, snapshot and redb code see exactly like a full
volume. Process output goes through pipes so the limit never applies to the
log. The limit is raised again on the live process with `prlimit(2)` to
model "space came back". Scenarios:

| Test | Setup | Checks |
|---|---|---|
| `full_wal_volume_fails_writes_with_5xx_and_recovers_in_place` | WAL only (redb mirror off), limit = largest file + 24 KiB | Every write answers 200 or 5xx (never another code); once writes fail `/ready` is 503 `wal_write_failed`; after the limit is lifted `/ready` is 200 again and writes succeed without a restart; fill again, `kill -9`, `wal-inspect verify` passes, restart, every acknowledged bundle present with all evidence exactly once |
| `full_volume_during_checkpoints_keeps_acknowledged_writes` | Same, with a checkpoint every 40 WAL records, so the snapshot write hits the limit too | Same checks; a failed checkpoint is deferred and never loses a committed write |
| `full_volume_with_redb_mirror_is_not_ready_and_recovers_on_restart` | WAL + redb mirror, limit below the preallocated redb file | Same answer and data checks; `/ready` is 503 while writes fail; after a restart without the limit the service is ready and accepts writes |

Behavior on a full volume (the guarantee):

* A write that cannot be appended to and synced in the WAL is answered with
  **500** (`internal persistence error`) and is rolled back: the WAL is
  truncated to the last committed group, memory and redb are untouched.
* `/ready` answers **503** with `{"status":"not_ready","reason":"wal_write_failed"}`
  as soon as a write could not be persisted, so the load balancer stops
  sending writes. While in that state each `/ready` call writes, syncs and
  removes a 1 MiB scratch file (`<wal>.space-probe`) next to the WAL; when that
  succeeds (and the WAL file exists) the node is ready again. A persisted
  write also clears the state.
* Metrics: `dash_ingest_wal_write_failure_total`,
  `dash_ingest_wal_write_recovered_total`, `dash_ingest_wal_write_failing`
  (gauge).
* A failed redb mirror write never fails the request (the WAL is the source of
  truth), but detaches the mirror and keeps `/ready` at 503
  `disk_unavailable` until the process restarts. Restart is the recovery for
  that case.

Not covered: real `ENOSPC` on a small filesystem (the `EFBIG` path is the
same code), and `EIO`/fsync errors from failing hardware.

## Load and soak: `loadgen`

```bash
cargo build --release -p ingestion -p retrieval -p control-plane -p wal-inspect -p loadgen
DASH_E2E_BIN_DIR=$PWD/target/release target/release/loadgen \
  --duration-secs 60 --concurrency 16 --ingest-percent 50 --dim 384 --preload 5000 \
  --json-out load.json
```

`loadgen` is closed-loop: `--concurrency` threads each keep one request in
flight, so the reported throughput is what the servers sustain at that
concurrency. Ingests go to the ingestion service, retrieves to the retrieval
follower. `--dim N` sends an `N`-dimensional `claim_embedding` with every
claim and a `query_embedding` with every retrieve (384 matches the default
hash embedder; `--dim 0` lets the server embed the text). `--batch-size N`
sends `N` claims per `/v1/ingest/batch` request. Latency of successful
requests goes into HDR histograms; failures are counted by kind
(`status_503`, `transport_timedout`, ...). The text report and the JSON
(`--json-out`) carry throughput, p50/p95/p99/max/mean latency per operation,
errors and server RSS. The exit status is 1 when the error rate exceeds
`--max-error-rate` (default 0) or RSS grows more than `--max-rss-growth-mib`.

By default it spawns its own ingestion + retrieval pair. To load an existing
deployment pass `--ingest-url`, `--retrieve-url`, `--ingest-key`,
`--retrieve-key` (and `--server-pid NAME=PID` to sample local server memory).

**Soak.** `--report-every-secs 60` prints one line per interval (rps,
p50/p99, errors, server RSS from `/proc/<pid>/status`). With `--id-space N`
ingests update a fixed set of `N` claims, so after `--preload N` the data
size is constant and memory should plateau; `--max-rss-growth-mib` then
catches a leak. The nightly soak runs 30 minutes with
`--preload 20000 --id-space 20000 --max-rss-growth-mib 256`.

### Reference numbers

Measured 2026-10-09 on a shared 4-vCPU development VM (other jobs running),
release build, default settings (strict WAL durability: one `fsync` per WAL
record, so an ingest with 2 evidence items and a vector costs six fsyncs), loopback, 384-d
vectors, `top_k` 10, 2 evidence items per claim. They are a baseline for
regressions, not a capacity statement; run loadgen on your own hardware.

| Workload | Concurrency | Throughput | p50 | p95 | p99 | max |
|---|---|---|---|---|---|---|
| 50/50 mix, 5,000 preloaded claims: ingest | 16 | 61.8/s | 64.5 ms | 230.5 ms | 343.0 ms | 1881 ms |
| 50/50 mix, 5,000 preloaded claims: retrieve | 16 | 60.5/s | 138.0 ms | 347.1 ms | 416.3 ms | 604 ms |
| ingest only, 2,000 preloaded | 16 | 189.8/s | 71.0 ms | 136.3 ms | 202.6 ms | 1510 ms |
| retrieve only, 2,000 preloaded | 16 | 262.8/s | 58.7 ms | 100.0 ms | 134.8 ms | 204 ms |
| ingest only, batches of 16 claims | 8 | 6.0 req/s (96 claims/s) | 762 ms | 4411 ms | 5612 ms | 6222 ms |

Errors were zero in every run. Batches are slower per claim than single
ingests because a batch stages its writes on a full copy of the in-memory
store (`clone_detached`), which grows with the data set; removing that copy
is part of the P2 storage engine work (master plan P2 item 3).

## Where it runs

* PR CI (`.github/workflows/rust.yml`): job `crash-test` (25 cycles, debug
  build, summary uploaded as `crash-test-pr`), job `e2e` (s4 with 100 cycles
  and s10).
* Nightly (`.github/workflows/durability-nightly.yml`, 02:30 UTC and manual
  dispatch with `crash_cycles` and `soak_minutes` inputs): job `crash`
  (1000 cycles, checkpoint pressure, self-check), job `disk-full` (s10 and s4
  with 500 cycles), job `soak` (30-minute soak and a 2-minute load report;
  `soak-load-reports` artifact and a job summary).
