# Replication limits and failure modes

This page lists the hard limits of WAL replication (ingestion leader to
ingestion or retrieval followers) and how each one shows up. All of them are
reported loudly: the follower's `/ready` answers 503 with a `reason`, and a
metric flips to 1.

## Response size

A full resync (`/internal/replication/export`) is a single HTTP response that
carries the snapshot and the whole WAL. The follower rejects any response
larger than its `max_response_bytes` (default 64 MiB):

* ingestion follower: `DASH_INGEST_REPLICATION_MAX_RESPONSE_BYTES`
* retrieval follower: `DASH_RETRIEVAL_REPLICATION_MAX_RESPONSE_BYTES`

If the export (or a single delta frame) is larger than the limit, the
follower can never finish a resync. It keeps retrying with backoff and
reports:

* `/ready`: status 503, `"reason":"replication_response_too_large"`, and the
  same value in `replication.blocked_reason`;
* `dash_ingest_replication_blocked_response_too_large 1` (ingestion
  follower) or `dash_retrieval_replication_blocked_response_too_large 1`
  (retrieval follower).

The condition clears on the next successful pull. Remedy: raise the
follower's `MAX_RESPONSE_BYTES` above the leader's export size (the size of
`<wal>.snapshot` plus `<wal>`; a checkpoint on the leader does not shrink
the export, it moves records from the WAL into the snapshot). There is no
chunked export: the export is not paginated, so the limit is a hard ceiling
on the dataset size a follower can bootstrap from. Followers also hold the
whole export in memory while applying it.

## Leader cost of a poll

A delta frame (`/internal/replication/wal`) reads only the WAL lines appended
since the previous poll plus the lines of the frame itself (and fewer than 64
lines before it): the leader indexes each line once as it is appended and
keeps one byte offset per 64 lines. A caught-up follower costs no file reads,
and the leader's memory per poll is bounded by the frame size, not by the WAL
length. A full export is different: the leader reads the snapshot and the
whole WAL into memory to build it, and every checkpoint (a new WAL
generation) sends each follower through one.

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
resync, instead of resuming and silently missing records.

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
