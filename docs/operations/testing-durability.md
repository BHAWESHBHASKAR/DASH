# Testing durability, disk-full behavior and load

This page describes the harness that backs DASH's durability claims and
its load numbers: what each tool checks, how to run it, what it does
**not** prove, and where it runs in CI.

| Tool | What it proves | PR CI | Nightly |
|---|---|---|---|
| `tools/crash-test` | Acknowledged ingests survive `kill -9` at random moments; no duplicates, no partial bundles or batches, clean WAL, service ready after restart | 25 cycles (`rust.yml`, job `crash-test`) | 1000 cycles, 200 cycles with checkpoint pressure, plus a self-check (`durability-nightly.yml`) |
| `tests/e2e/tests/s4_crash_consistency.rs` | The same oracle across the leader **and** the retrieval follower | 100 cycles (job `e2e`) | 500 cycles |
| `tools/crash-test --failover` | Leader failover: SIGKILL of the ingestion leader at random moments in a three-node cluster with synchronous replication; no acknowledged write lost, one leader at a time, the killed node rejoins and converges | no (run by hand; e2e `s14_leader_failover` runs in job `e2e`) | no |
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
   mirror) with the default, strict WAL durability settings, including WAL
   group commit for single ingests (on by default; see
   [`wal-durability.md`](wal-durability.md)). Concurrent single ingests
   therefore share fsyncs, which is exactly the path the oracle has to hold
   for: a request is acknowledged only after its group is durable.
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
     evidence record written twice within the snapshot or within the WAL,
     and nothing from an earlier cycle vanished. (A kill between a
     checkpoint's snapshot rename and its WAL truncation legitimately leaves
     the same records in both files; replay is idempotent and the next
     checkpoint removes the overlap. The first checkpoint-pressure run found
     exactly that state, which is why the check is per file.);
   * any non-2xx answer to a valid write before the kill fails the run.
7. `wal-inspect verify` runs again on the recovered files.

The seed is printed on the first line; `--seed N` with the same
`--cycles`/`--writers` replays the same random choices (thread timing still
varies). On failure the state directory is kept and printed, the error names
the cycle and the claim, and the reproduce command is printed. `--json-out`
writes a summary (`acked_requests`, `acked_claims`, `unknown_requests`,
`unknown_applied`, recovery time p50/p95/max, duration).

Just before each kill the harness scrapes `/metrics` and records whether
group commit was enabled and how many entries shared an fsync; the summary
line `group commit before the kills: enabled in N/N scrapes, E entries in B
batches, largest batch M` (and the `group_commit` object in the JSON) shows
that the run really exercised group commit. Pass
`--env DASH_INGEST_WAL_GROUP_COMMIT=false` to test the direct write path
instead.

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

Guaranteed under the default WAL settings (every acknowledged write synced,
no append buffer, group commit on) and tested as above:

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
* Replicated (quorum) writes; there is no quorum write path yet. Synchronous
  replication across a leader failover is covered by `crash-test --failover`
  (below).

## Leader failover: `crash-test --failover`

```bash
cargo build --release -p ingestion -p retrieval -p control-plane -p wal-inspect -p crash-test
DASH_E2E_BIN_DIR=$PWD/target/release target/release/crash-test --failover --cycles 25 --checkpoint-every 30
```

A control plane and three ingestion nodes (`n1` the first writer, `n2` and
`n3` its followers) run with `DASH_INGEST_MIN_SYNC_REPLICAS=1`, a 2 s lease,
0.5 s grace and 200 ms heartbeats. Each cycle runs the same writers as above
against the current leader, SIGKILLs it after a random delay, waits for a
follower to be promoted (exactly one node may answer `/v1/ready/leader`),
checks that every acknowledged request is on the new leader with all its
evidence and that the request in flight is there completely or not at all,
restarts the killed node and waits until it and the other follower hold the
new leader's data. `--checkpoint-every` adds checkpoints, so failovers happen
across generation changes. With `--env DASH_INGEST_MIN_SYNC_REPLICAS=0` the
same run measures asynchronous replication: lost acknowledged writes are
counted (`acked_lost`) instead of failing the run.

Measured on a 4-vCPU development VM with debug builds:

| Run | Cycles | Acknowledged requests | Lost | Failover (p50 / max) | Rejoin (p50 / max) |
|---|---|---|---|---|---|
| synchronous, `--checkpoint-every 30` | 25 | 413 | 0 | 2.68 s / 2.89 s | 3.6 s / 5.8 s |
| asynchronous (`MIN_SYNC_REPLICAS=0`) | 3 | 337 | 4 | 2.59 s / 2.59 s | 2.4 s / 4.3 s |

Failover time is measured from the SIGKILL to the first `/v1/ready/leader`
200 on a follower; with these settings the floor is lease + grace = 2.5 s.

Not covered: network partitions (the e2e scenario approximates an isolated
leader with SIGSTOP/SIGCONT), control-plane failures during a failover, and
two simultaneous node losses (outside the single-node-loss guarantee).

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

* A write that cannot be appended to the WAL is answered with **500**
  (`internal persistence error`) and is rolled back: the WAL is truncated to
  the last committed group, memory and redb are untouched. With group commit
  every request of the failed group gets the 500; other groups are
  unaffected.
* A failed **fsync** is different: the WAL is poisoned (an fsync error can
  drop dirty pages, so retrying it could acknowledge lost data). Every later
  write answers **503** `wal_poisoned` and `/ready` reports `wal_poisoned`
  until the process restarts and re-reads the WAL from disk. The space probe
  below does not clear that state.
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

Not covered here: real `ENOSPC` on a small filesystem (the `EFBIG` path is
the same code), and `EIO`/fsync errors from failing hardware (the fsync
poisoning is tested in the store and ingestion unit tests with a failpoint
and a test hook, not end to end). `RLIMIT_FSIZE` fails `write(2)`, never
`fsync(2)`, so s10 exercises the append path only.

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
`--preload 20000 --id-space 20000 --checkpoint-every 20000
--max-rss-growth-mib 256`.

**Memory under repeated updates (resolved).** An earlier soak found the
ingestion RSS growing with the WAL when checkpoints were off (118 MiB to
712 MiB in a 3-minute update-only run on 2,000 claims). Two causes were
measured and fixed:

* Every replication poll from a follower read the whole WAL file into memory,
  filtered it and copied the requested frame out of it. The leader's memory
  per poll (and its latency) grew with the log; with the e2e retrieval
  follower polling every 100 ms this dominated the RSS. The WAL now indexes
  each line once, as it is appended, and a frame reads only the lines it
  ships (`pkg/store/src/wal/replication_index.rs`).
* redb 2.6.3 grew its page cache without bound when the same keys were
  written over and over (fixed upstream in 2.6.4, which DASH now uses).

The same run (`--preload 2000 --id-space 2000 --ingest-percent 100 --dim 384
--concurrency 16`, no checkpoints, 180 s, release build, shared 4-vCPU VM):

| Build | Updates | Ingestion RSS first / peak / last | Ingest throughput |
|---|---|---|---|
| before | 51,602 | 102 / 851 / 838 MiB | 287/s, p99 487 ms |
| after | 100,475 | 32 / 38 / 37 MiB | 558/s, p99 117 ms |

`pkg/store/tests/memory_bounded_updates.rs` guards both (heap and per-poll
peak with a counting allocator). With checkpoints on, the RSS still moved
between about 70 and 140 MiB in this run: each checkpoint built the snapshot
in memory and forced every follower to resync, which made the leader build a
full export in memory. Followers now cross a checkpoint without a resync and
a resync streams a chunked export from a file
(`docs/operations/replication-limits.md`); the snapshot itself is still
built from the in-memory state. Without checkpoints the WAL grows on disk and
restart replay time grows with it, so the ingestion service now checkpoints
at 256 MiB of WAL by default (`DASH_CHECKPOINT_MAX_WAL_BYTES`, `0` turns it
off).

### Follower apply throughput (resolved)

An update-heavy soak showed the retrieval follower's WAL trailing the
leader's by about half. `loadgen` now samples the follower's lag in WAL
records every interval (against the leader's live position) and measures how
long the follower takes to catch up after the load stops. Stack samples of
the follower thread (gdb, 15 samples during catch-up) found it in
`fdatasync` 13 times: every replicated record was fsynced on its own in the
follower WAL, every redb write was its own durable transaction (two for a
claim), and every frame was staged on a full copy of the store. The follower
now appends a frame with one write and one fsync, applies it in place
(whole commit groups per write-lock hold) and writes its redb mutations in
one transaction; the leader's own ingest path writes a bundle's redb
mutations in one transaction too.

Same machine, release builds, `--concurrency 16 --ingest-percent 100 --dim
384 --seed 7`, no checkpoints (retrieval follower with WAL and redb, poll
interval 100 ms, 512 records per frame):

| Workload | Build | Leader ingests/s | Follower lag (WAL records): mean / max / at stop | Catch-up after stop |
|---|---|---|---|---|
| 2,000 claims updated (`--preload 2000 --id-space 2000`), 90 s | before | 647.6 | 106,479 / 178,032 / 177,660 | 70.9 s |
| same | after | 1,293.8 | 519 / 1,008 / 600 | 0.15 s |
| 20,000 claims updated (`--preload 20000 --id-space 20000`), 60 s | before | 419.4 | 66,270 / 96,948 / 97,146 | 81.1 s |
| same | after | 540.5 | 716 / 924 / 636 | 0.06 s |

Before, the follower applied about 1,900 records/s against the leader's
3,900 in the first run (it trailed by half, as observed); after, it keeps up
with a leader that is twice as fast, staying within about two frames. The
50/50 mix (`--preload 1000 --ingest-percent 50`, 30 s) went from 167.6
ingests/s and 165.0 retrieves/s (retrieve p99 141 ms) to 190.2 and 187.6
(p99 116 ms): holding the store's write lock per commit group instead of
swapping in a staged copy did not slow down readers.

### Reference numbers

Measured 2026-10-10 on a shared 4-vCPU development VM (other builds running
at the same time), release build, default settings (strict WAL durability
with group commit), loopback, 384-d vectors, `top_k` 10, 2 evidence items
per claim, 30 s (mix) and 15 s (ingest only) measured after a warm-up. They
are a baseline for regressions, not a capacity statement; run loadgen on your
own hardware.

| Workload | Concurrency | Throughput | p50 | p95 | p99 | max |
|---|---|---|---|---|---|---|
| 50/50 mix, 1,000 preloaded claims: ingest | 16 | 164.8/s | 34.9 ms | 73.3 ms | 107.7 ms | 179 ms |
| 50/50 mix, 1,000 preloaded claims: retrieve | 16 | 163.3/s | 54.4 ms | 127.0 ms | 149.4 ms | 169 ms |
| ingest only, new claims | 16 | 430.8/s | 29.4 ms | 80.7 ms | 118.3 ms | 211 ms |

Errors were zero in every run. The previous round (2026-10-09, before group
commit, one fsync per WAL record) measured 61.8 ingests/s in the 50/50 mix
with 5,000 preloaded claims and 189.8/s ingest only; batches of 16 claims ran
at 6.0 requests/s (96 claims/s) because a batch stages its writes on a full
copy of the in-memory store (`clone_detached`), which grows with the data
set. Removing that copy is part of the P2 storage engine work (master plan P2
item 3). On this shared VM, throughput of both services sometimes dropped
for tens of seconds while other jobs were building on the same disk; treat
single intervals with a collapsed rate as host noise unless they repeat on
a quiet machine.

## Where it runs

* PR CI (`.github/workflows/rust.yml`): job `crash-test` (25 cycles, debug
  build, summary uploaded as `crash-test-pr`), job `e2e` (s4 with 100 cycles
  and s10).
* Nightly (`.github/workflows/durability-nightly.yml`, 02:30 UTC and manual
  dispatch with `crash_cycles` and `soak_minutes` inputs): job `crash`
  (1000 cycles, checkpoint pressure, self-check), job `disk-full` (s10 and s4
  with 500 cycles), job `soak` (30-minute soak and a 2-minute load report;
  `soak-load-reports` artifact and a job summary).
