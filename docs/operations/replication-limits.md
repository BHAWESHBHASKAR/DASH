# Replication limits and failure modes

This page lists the hard limits of WAL replication (ingestion leader to
ingestion or retrieval followers), how a follower crosses a checkpoint and
rebuilds its state, and how each failure shows up. Failures are reported
loudly: the follower's `/ready` answers 503 with a `reason`, and a metric
flips to 1.

## Checkpoints: switching generations instead of resyncing

A checkpoint writes the leader's state to `<wal>.snapshot`, starts a new,
empty WAL and a new WAL generation. Offsets of the old generation mean
nothing in the new one, so before this release every checkpoint sent every
follower through a full resync. Now:

* The leader records each checkpoint as a transition `(closed generation,
  replication view length at the checkpoint, new generation)` in
  `<wal>.gen.transitions` (the newest 16), and keeps the closed generation's
  WAL file as `<wal>.closed.<generation>` until the next checkpoint.
* A follower inside the closed generation (offset below its end) keeps
  receiving frames of that generation, read from the closed file. A
  checkpoint runs right after the write that triggered it, under the same
  lock, so a live follower is practically always a few records behind when
  it happens: this is the common case.
* A follower at the exact end of the closed generation receives a frame
  that starts at offset 0 of the current generation and names the position
  it switches from (`switch_from=<generation>:<offset>`). The follower
  compacts its own WAL the same way (a snapshot of its state, an empty WAL)
  and continues; `generation_switches_total` in `/ready` and
  `dash_*_replication_generation_switches_total` count these.
* Transitions are followed in a chain, so several checkpoints in a row with
  nothing written between them are crossed at once.
* Everything else resyncs, as before: a follower in an older generation than
  the retained one (it missed a whole generation), a follower ahead of the
  closed generation's end, a generation that a rollback, reset or resync
  replaced (no transition leads to it), a leader that lost its transitions
  file, and followers that do not send `gen_switch=1` (earlier builds).

**Why the switch is safe.** The snapshot a checkpoint writes is the leader's
in-memory state at that moment, and that state is exactly the result of
applying the replication view of the closed generation, in order, on top of
the generation's starting snapshot (the WAL is the source of truth: a write
is validated before it is appended and applied after; replay and followers
apply the same lines with the same lenient rules; a commit group is never
open at a checkpoint because groups are appended in one unit under the WAL
lock). A follower that applied every line of the closed generation up to
its end (`offset == from_records`, generation equal) therefore holds the new
snapshot's state; the new generation starts with an empty WAL, so offset 0
of it is the next record. No record is skipped (the follower is at the end
of the old view and at the start of the new one) and none is applied twice
(the new WAL holds only records written after the checkpoint). The leader
checks the follower's position against the recorded transition and the
follower checks the leader's `switch_from` against its own cursor; both
must match exactly. The transition is recorded only after the new WAL is
durable (snapshot fsynced and renamed, generation file written, old WAL
renamed to the closed file, new WAL fsynced): a crash anywhere before that
leaves no transition, and the follower resyncs. A crash during the
follower's own compaction leaves a local WAL whose length no longer matches
the saved offset, which the follower detects at startup and answers with a
resync. Tests: `pkg/store/src/wal/replication_tests.rs` (exact end, behind,
ahead, chained checkpoints, rollback, damaged transitions file, crash
points), `pkg/store/src/failpoint.rs` (crash at every checkpoint step), and
the follower tests in `services/*/tests` and `tests/e2e/tests/s12_chunked_resync.rs`.

Cost: the closed file is one extra WAL's worth of disk on the leader (and
on an ingestion follower, which may itself be followed). Retrieval followers
delete theirs right away.

## Full resync: chunked export

A follower that must rebuild (fresh follower of a leader that has
checkpointed, missed generation, stale cursor) downloads the leader's export
in chunks:

1. `GET /internal/replication/export/begin` freezes the leader's state under
   the WAL lock (opens the snapshot, copies the WAL's replication lines) and
   writes `<wal>.exports/<id>.export` from the frozen inputs without the
   lock, streaming. The answer is a manifest: export id, generation, record
   counts, size and SHA-256. A leader whose generation and WAL are unchanged
   hands out the export it already has.
2. `GET /internal/replication/export/chunk?export_id=&offset=&max_bytes=`
   returns up to `max_bytes` of the file, cut back to the last complete
   line, read straight from the file (no lock, no copy of the export in
   memory). The leader serves at most 32 MiB per chunk.
3. The follower appends each chunk to `<wal>.resync.part` (fsync per chunk;
   the manifest is kept next to it as `<wal>.resync.manifest`), verifies the
   SHA-256 of the complete file, removes its cursor, replaces its snapshot
   and WAL from the file (streaming), swaps in a store built from it, writes
   the cursor `(generation, wal_records)` from the manifest and deletes the
   download. It then continues with delta frames, generation switches
   included (an export frozen just before a checkpoint is crossed without a
   second resync).

Interruptions: a network error or a restart resumes the download from the
part file's length with the same export id. An export the leader no longer
has (404: pruned or leader restarted with other data) restarts the download
with a new export. A checksum mismatch discards the download and asks for a
different export (`begin?avoid=<id>`); after four failed attempts the error
is reported. A crash between removing the cursor and writing the new one
restarts into a resync that applies the already verified download again,
without downloading it.

Memory: the leader holds one chunk per request; the follower holds one
chunk plus the store it builds (the store is in memory by design; the
export text is not). Disk: the leader keeps the newest 2 exports, deletes
older ones and any export not read for 15 minutes (checked every minute);
`dash_ingest_replication_exports_retained_bytes` shows the space used. Each
export is about the size of `<wal>.snapshot` plus `<wal>`. The follower
needs room for one export next to its WAL.

Chunk size: `DASH_INGEST_REPLICATION_EXPORT_CHUNK_BYTES` /
`DASH_RETRIEVAL_REPLICATION_EXPORT_CHUNK_BYTES` (default 4 MiB), capped so a
chunk and its header fit in the follower's response limit. Leaders without
chunked export (earlier 0.3 builds) answer `begin` with 404; the follower
then falls back to the single-response `/internal/replication/export`, which
is still served for them.

Metrics: `dash_*_replication_export_bytes_total` (follower, downloaded),
`dash_ingest_replication_exports_built_total`, `_exports_reused_total`,
`_export_chunks_served_total`, `_export_bytes_served_total`,
`_exports_retained`, `_exports_retained_bytes` (leader).

## Response size

The follower rejects any response larger than its `max_response_bytes`
(default 64 MiB):

* ingestion follower: `DASH_INGEST_REPLICATION_MAX_RESPONSE_BYTES`
* retrieval follower: `DASH_RETRIEVAL_REPLICATION_MAX_RESPONSE_BYTES`

This bounds a delta frame and one export chunk, not the data set: the
export is downloaded in chunks below the limit, so any data set can be
replicated. A delta frame larger than the limit (a frame of
`max_records` very large records, or a single line longer than the limit)
cannot be applied; the follower keeps retrying with backoff and reports:

* `/ready`: status 503, `"reason":"replication_response_too_large"`, and the
  same value in `replication.blocked_reason`;
* `dash_ingest_replication_blocked_response_too_large 1` (ingestion
  follower) or `dash_retrieval_replication_blocked_response_too_large 1`
  (retrieval follower).

The condition clears on the next successful pull. Remedy: lower the
follower's `MAX_RECORDS` or raise `MAX_RESPONSE_BYTES`. Against a leader
without chunked export the single-response export is still bounded by the
limit, as before.

## Applying frames

A follower applies a frame in place: the frame is mirrored to its WAL with
one write and one fsync, then applied to the live store whole commit groups
at a time (readers never see part of a group; the retrieval follower holds
its write lock for at most about 64 records), with the redb writes of the
whole frame in one transaction. Before, every frame was staged on a full
copy of the store (time and memory proportional to the data set, per
frame), every record was fsynced on its own in the follower WAL, and every
redb write was its own durable transaction: under an update-heavy soak the
retrieval follower trailed the leader by about half of the leader's WAL
(see `docs/operations/testing-durability.md` for the measurements). Every
line is parsed before anything is written, so a frame with an unreadable
record changes nothing. A record that parses but cannot be applied means
the follower diverged from the leader: the WAL append is rolled back and
the follower rebuilds from a full resync (earlier it retried the same frame
forever).

## Leader cost of a poll

A delta frame (`/internal/replication/wal`) reads only the WAL lines appended
since the previous poll plus the lines of the frame itself (and fewer than 64
lines before it): the leader indexes each line once as it is appended and
keeps one byte offset per 64 lines. A caught-up follower costs no file reads,
and the leader's memory per poll is bounded by the frame size, not by the WAL
length. Frames of the closed generation are read the same way from the
closed file (its index is kept from before the checkpoint, or built once
after a restart). An export is built once per leader state and served in
chunks from its file.

## Automatic checkpoints

The ingestion service checkpoints once its WAL reaches 256 MiB unless
`DASH_CHECKPOINT_MAX_WAL_BYTES` says otherwise (`0` turns the size threshold
off; `DASH_CHECKPOINT_MAX_WAL_RECORDS` adds a record threshold, off by
default). Earlier releases had no default because each checkpoint forced
every follower through a full resync whose export had to fit in one
response. Neither holds any more: followers that keep up cross a checkpoint
with a generation switch, and a resync is chunked. What a checkpoint still
costs is writing the snapshot (proportional to the data set, under the
ingestion lock, so writes wait for it) and, on followers, the same local
compaction. 256 MiB bounds the WAL on disk (plus one closed generation of the
same size), the replay time at restart, and the replication index, while a
data set well below that size is rewritten at most about once per 256 MiB of
writes. Deployments with a data set much larger than 256 MiB should raise
the threshold (a checkpoint rewrites the whole data set) or accept the
write pauses; the deployment templates in `deploy/` set 50 MiB.

## Commit group size

The leader never ends a delta frame inside a commit group (a batch or a
single-ingest bundle). A frame is therefore extended past the follower's
requested `max_records` up to the end of the group, as long as the group is
at most `REPLICATION_GROUP_EXTENSION_MAX` records (1,000,000 records, begin
and end markers included). Followers accept frames of
`max_records + 1,000,000` lines.

A group that starts exactly at the follower's offset and is larger than the
cap cannot be shipped. The leader answers HTTP 500 with
`replication_group_too_large`, increments
`dash_ingest_replication_group_too_large_total`, and the follower reports
`"reason":"replication_group_too_large"` in `/ready`
(`dash_*_replication_blocked_group_too_large 1`). If such a group starts
later in the frame, the frame simply ends before it and the error is raised
on the next poll. A group that is still open at the end of the WAL is a write
in progress and is not an error.

Ingest batches are bounded by the maximum batch size, so a group this large
only occurs if that bound is raised far beyond its default.

## Records the leader's replay would quarantine

Legacy lines that lenient replay quarantines because they cannot be parsed
(and the records that depend on them) are not served to followers. Offsets
in replication frames index this filtered view, and the number of left-out
lines is exposed as `dash_ingest_replication_view_skipped_lines` on the
leader. Legacy records that parse but fail validation against the store
state (a control character in an id, a vector of the wrong dimension) are
still served; followers skip and count them
(`*_replication_skipped_records_total`) while still mirroring the line into
their own WAL, so a follower never wedges on them and converges to the
leader's lenient-replay state.

## Follower cursor

A follower persists `(generation, offset)` next to its WAL
(`<wal>.replication`). At startup it compares the saved offset with the
number of records in its local WAL; if they differ (a restored or truncated
WAL, or a stale state file) it discards the cursor and performs a full
resync, instead of resuming and silently missing records. The same check
catches the two crash windows this release adds: a resync removes the
cursor before it replaces the local snapshot and WAL (a populated WAL
without a cursor means "resync"), and a generation switch compacts the
local WAL before it moves the cursor to offset 0 (a crash in between leaves
an offset that no longer matches the WAL).

## Leader failover and synchronous replication

With automatic failover ([failover.md](failover.md), ADR 0006) the leader can
change, and every follower must notice it without mixing the two histories:

* **Terms on the wire.** A failover follower adds `term=<T>` to every poll;
  the leader answers with a `term=` line after `switch_from=` (only to
  followers that sent `term=` and `gen_switch=1`, so older followers get the
  old layout). A follower refuses a frame from an older term
  (`replication frame from a deposed leader`,
  `dash_ingest_failover_stale_term_frames_total`), and a leader that receives
  a newer term answers `409 stale_leader_term` and stops accepting writes.
* **The fencing checkpoint.** A promoted follower names its WAL after the old
  leader's generation (`FileWal::adopt_generation`, its WAL holds exactly the
  first `n` lines of it) and checkpoints at once. The recorded transition
  `(old generation, n, new generation)` lets the old leader's other followers
  continue exactly as across any checkpoint: behind `n`, they finish the closed
  generation from its retained file; at `n`, they switch; ahead of `n` (they
  hold records the new leader never received), they no longer match and get a
  full resync, which discards those records.
* **Deposed leaders** have no follower cursor: they copy their WAL to
  `<wal>.deposed-t<term>-<unix ms>` and resync from the new leader.
* **Long polls.** Followers send `wait_ms` (at most 1000); a leader that has
  nothing new holds the poll until a write commits or the wait ends. A
  follower polls again at once while frames carry records. Followers' WAL
  polls and commit acks are served by a reserved worker lane (the health
  lane, enlarged by four workers), so they never queue behind writes that
  hold every general worker while they wait for confirmations.
* **Per-read timeouts.** A failover follower's WAL polls and acks use a
  per-read timeout of `wait_ms + 2 s` and no overall deadline: a dead, paused
  or partitioned leader is given up on within that, and the next poll goes to
  the leader the control plane names, while a large frame that keeps arriving
  is never cut off. (The shared replication client otherwise applies an
  overall request deadline, which in the HTTP client used takes precedence
  over the per-read timeout.)
* **Synchronous replication** (`DASH_INGEST_MIN_SYNC_REPLICAS`): a follower's
  poll from offset `p` in generation `g` (with `replica_id` and `durable=1`)
  proves it fsynced its WAL and cursor up to `p`. The leader answers a write
  once enough followers of the current term proved a position at or after the
  write (a follower in a later generation of the leader's checkpoint chain
  counts as past it).

## What `/ready` reports about a failure

`/ready` embeds the follower state as one JSON object under `replication`.
`replication.last_error` is a short code, never the raw error text, because
the raw text can contain hostnames, filesystem paths and part of the leader's
response body. The codes are `source_unreachable`, `source_timeout`,
`source_rejected_credentials`, `source_error_status`, `response_too_large`,
`group_too_large`, `token_transport_refused`, `ack_failed`, `apply_failed`,
`invalid_response` and `replication_error`. The full message is written to the
service log and, on the retrieval follower, kept in the in-process status.
`replication.skipped_records_total` is the number of replicated lines skipped
because lenient replay quarantines them. A disk failure is reported as
`"reason":"disk_unavailable"` without the underlying error.
