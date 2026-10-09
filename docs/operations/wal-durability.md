# WAL durability and group commit

This page describes when the ingestion service acknowledges a write, how
concurrent single ingests share fsyncs (group commit), and what happens when
the disk reports an error. Damaged files and the `wal-inspect` tool are
covered in [WAL recovery](wal-recovery.md); every setting named here is in
the [configuration reference](../../docs-site/docs/reference/configuration.md)
(section "Persistence and WAL").

## The guarantee

With the default write policy (`DASH_INGEST_WAL_SYNC_EVERY_RECORDS=1`, no
append buffer, no background-only flushing) a `200` from `POST /v1/ingest`,
`/v1/ingest/batch`, `/v1/ingest/raw` or `/v1/ingest/document` is sent only
after the write's WAL records were written and `fdatasync`ed. A request that
gets an error was not applied to memory or redb. Its records may still be in
the WAL if the error came from a failed fsync, so the outcome of an errored
request is unknown until a restart; clients retry, and retries are idempotent
(an identical single ingest is a no-op, batches are idempotent by commit id
and content).

The relaxed policies (sync every N records, sync interval, append buffer,
background-only flushing) trade this guarantee for throughput and are guarded
at startup (`DASH_INGEST_ALLOW_UNSAFE_WAL_DURABILITY`). Group commit honours
whichever policy is configured: with a relaxed policy a batch is flushed
according to that policy, not fsynced unconditionally.

## Group commit (single ingests)

Enabled by default (`DASH_INGEST_WAL_GROUP_COMMIT=true`) whenever a WAL path
is set. A single ingest runs in three steps:

1. **Prepare, under the runtime lock.** The request waits until no in-flight
   ingest touches the same state (see "Ordering" below), checks placement,
   validates against the store, encodes its WAL commit group and puts it on
   the committer's queue. The lock is released.
2. **Commit, without any lock.** One committer thread takes everything that
   is queued (up to `DASH_INGEST_WAL_GROUP_COMMIT_MAX_BATCH_BYTES`, default
   1 MiB), writes it with one `write` and makes it durable with one
   `fdatasync`, then wakes every request of the batch with the same result.
   Requests that arrive while an fsync is running form the next batch, so
   batches grow with load without any added delay.
3. **Apply, under the runtime lock.** Each request waits for its turn in WAL
   order, then applies its bundle to memory and redb, but only if its batch
   was durable.

`DASH_INGEST_WAL_GROUP_COMMIT_MAX_WAIT_US` (default `0`, at most `10000`)
lets the committer hold a batch open for that long after the first request
arrives, to collect more requests. An idle request never waits longer than
this value; with the default it does not wait at all. On a fast disk a small
value (a few hundred microseconds) can make batches larger at the cost of the
same added latency; measure before changing it.

Batch, raw and document ingests and replication apply are unchanged: they
first wait for in-flight single ingests to finish (the pipeline drains), then
append and fsync with exclusive use of the WAL. New single ingests hold back
while such a writer is waiting, so it is not starved.

With `DASH_INGEST_WAL_GROUP_COMMIT=false` every single ingest writes and
fsyncs its own commit group while holding the runtime lock (one fsync per
ingest; before this release it was one fsync per record).

### Ordering

* Records reach the WAL in the order requests were queued, and become
  visible in memory and redb in exactly that order.
* Two in-flight ingests are serialized (the second waits before validating)
  when they write the same claim, when one has an edge to a claim the other
  writes, or when both carry the first vector of a tenant (which fixes that
  tenant's dimension). Every validation therefore sees the effect of every
  earlier WAL record it could depend on, and the state after any set of
  concurrent requests equals a serial replay of the WAL.
* A checkpoint runs only when no ingest is between commit and apply, so a
  snapshot never misses a durable record.

### Backpressure

The committer's queue holds at most
`DASH_INGEST_WAL_GROUP_COMMIT_QUEUE_CAPACITY` requests (default 1024). When it
is full the request is rejected with `503 wal_group_commit_queue_full` and
`Retry-After: 1`; nothing was written. In practice the HTTP worker pool
(`DASH_INGEST_HTTP_WORKERS`) bounds the number of waiting requests first, and
the transport queue (`DASH_INGEST_HTTP_QUEUE_CAPACITY`) answers 503 before
the committer queue fills.

## fsync failures: the WAL is poisoned

If `fdatasync` on the WAL fails, the kernel may already have dropped the
dirty pages and marked them clean, so a retried fsync can report success for
data that never reached the disk ("fsyncgate"). DASH therefore never retries:

* every request of the failing batch gets `503 wal_poisoned`, and nothing of
  the batch is applied to memory or redb;
* the WAL is marked poisoned: every later write (single, batch, raw,
  document, replication apply, checkpoint) fails immediately with
  `503 wal_poisoned` without touching the file;
* `/ready` returns `503 {"status":"not_ready","reason":"wal_poisoned"}`, and
  `dash_ingest_wal_poisoned` is `1`.

Recovery: fix the disk, then restart the service. On startup the WAL is read
from disk again (a torn tail is truncated as usual, see
[WAL recovery](wal-recovery.md)); records of the failed batch that did reach
the disk are replayed, which is why errored requests must be treated as
"unknown outcome" and retried.

A write error that is not an fsync error (for example the file cannot be
opened) fails every request of the batch with `500`, truncates the file back
to where the batch started and leaves the WAL usable.

## Metrics

| Metric | Meaning |
| --- | --- |
| `dash_ingest_wal_group_commit_enabled` | 1 when single ingests use group commit |
| `dash_ingest_wal_group_commit_batches_total` | batches handed to the WAL |
| `dash_ingest_wal_group_commit_entries_total` | requests in those batches |
| `dash_ingest_wal_group_commit_avg_batch_entries` | requests per batch (higher means more fsyncs saved) |
| `dash_ingest_wal_group_commit_last_batch_entries`, `..._max_batch_entries` | size of the last and the largest batch |
| `dash_ingest_wal_group_commit_failed_batches_total` | batches whose write or fsync failed |
| `dash_ingest_wal_group_commit_queue_depth`, `..._queue_capacity` | queued requests and the bound |
| `dash_ingest_wal_group_commit_queue_full_reject_total` | requests rejected with 503 because the queue was full |
| `dash_ingest_wal_group_commit_in_flight` | requests between enqueue and apply |
| `dash_ingest_wal_group_commit_conflict_waits_total` | requests that waited for a conflicting in-flight ingest |
| `dash_ingest_wal_group_commit_max_wait_us` | configured linger |
| `dash_ingest_wal_poisoned` | 1 after an fsync failure, until restart |

## Throughput

`scripts/benchmark_ingest_group_commit.sh` starts a fresh ingestion service
per run (empty WAL, strict default durability, dev-mode auth, rate limit off)
and drives `POST /v1/ingest` with N concurrent clients; each request is a new
claim with one evidence row and a 4-dimensional vector. The numbers below are
one round on a shared 4 vCPU VM with an ext4 virtual disk (`fdatasync` of a
small append measured at about 4 ms), 64 HTTP workers and 150 requests per
client, release build. "before" is the previous commit; "off" is this release
with `DASH_INGEST_WAL_GROUP_COMMIT=false`; "on" is the default. In this
environment every HTTP request (even `/health`) costs about 40 to 50 ms
outside the service logic, which is why one client tops out near 20
requests/s in every mode; the machine was shared with other jobs, so treat
differences under about 15% as noise.

WAL only (`DASH_INGEST_PERSISTENCE_DISABLE=1`), requests per second
(average requests per batch in brackets):

| Clients | before | off | on |
| ---: | ---: | ---: | ---: |
| 1 | 19.8 | 19.4 | 19.8 (1.00) |
| 8 | 135.8 | 286.5 | 418.3 (2.36) |
| 32 | 106.0 | 967.6 | 2062.9 (6.50) |
| 64 | 444.7 | 1989.4 | 2108.9 (3.02) |

p99 latency at 32 clients: before 1077 ms, off 112 ms, on 63 ms. In the
64-client "on" run one of 9600 requests failed (the run happened while the
shared disk was filling up; the cause was not captured).

Default configuration (WAL and redb mirror):

| Clients | before | off | on |
| ---: | ---: | ---: | ---: |
| 1 | 19.7 | 19.9 | 19.1 (1.00) |
| 8 | 269.2 | 266.1 | 492.2 (1.75) |
| 32 | 287.4 | 574.7 | 623.5 (1.40) |

(The 64-client runs with redb crashed in every mode, including "before",
because the shared disk ran out of space; they are not reported.)

How to read this:

* "before" to "off" is the single-fsync commit group: a single ingest used to
  fsync once per record (begin marker, claim, evidence, vector, end marker);
  it now fsyncs once per ingest.
* "off" to "on" is group commit. Without redb it roughly doubles throughput
  at 32 clients (6.5 requests per fsync). At 64 clients on 4 vCPUs the CPU
  and the runtime lock become the limit, so batches get smaller.
* With the redb mirror the gain from group commit is small at 32 clients:
  each redb mirror write is its own durable redb transaction (several fsyncs
  per ingest) made while holding the runtime lock, and that now dominates.
  Batching or relaxing the redb mirror writes is a separate change.
