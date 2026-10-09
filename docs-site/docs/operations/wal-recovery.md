# WAL recovery

!!! note
    This page is a copy of [`docs/operations/wal-recovery.md`](https://github.com/BHAWESHBHASKAR/DASH/blob/main/docs/operations/wal-recovery.md) in the repository, which is canonical.

This page describes how the store treats damaged or old-format write-ahead
log (WAL) and snapshot files at startup, and how to inspect and repair them
with `wal-inspect`.

Files involved, for a WAL at `<wal>` (for example the path in
`DASH_INGEST_WAL_PATH` or `DASH_RETRIEVAL_WAL_PATH`):

| File | Purpose |
| --- | --- |
| `<wal>` | append-only log |
| `<wal>.snapshot` | checkpoint written by compaction, replayed before the WAL |
| `<wal>.gen` | WAL lineage id used by replication |
| `<wal>.quarantine` | records replay could not apply (created on demand) |
| `<wal>.bak` | copy taken by `wal-inspect repair` before it changes a file |

## Record formats

Current releases write checksummed records: kinds `C2`, `E2`, `G2`, `V2`,
`B2`, each ending in `\tcrc=<8 hex digits>`, with every field escaped.
Older releases wrote the legacy kinds `C`, `E`, `G`, `V`, `B` with no
checksum. Legacy records are still read. A legacy record could contain
values that current validation rejects (ASCII control characters in ids,
tenant or entity names) or could not be parsed at all (the old writer did
not escape the entity and embedding-id lists, so a tab or newline inside an
entity broke the record). A legacy vector record may also have been written
without validation (wrong dimension, non-finite values, unknown claim).

## Replay policy

Replay reads the snapshot first, then the WAL.

Torn tail. If the final WAL line is incomplete or fails its checksum it is
treated as an interrupted write: `FileWal::open` truncates it, prints a
warning and counts it in `torn_tail_dropped`. A newline-terminated legacy
line is never treated as a torn tail; it is handled by the rules below.

Lenient mode (default). Replay continues past records it cannot use, and
the service starts:

| Situation | Result |
| --- | --- |
| Legacy record (`C E G V B`) that cannot be parsed | quarantined |
| Legacy record that parses but fails validation (for example a control character in an id) | quarantined |
| Vector record (`V` or `V2`) that fails validation: dimension mismatch, non-finite, or unknown claim | quarantined |
| Evidence, edge or vector that references a quarantined claim | skipped, counted as `dependent_skipped`, also copied to the quarantine file |
| Line directly after an unparseable legacy line that has no recognisable record kind (the rest of a record split by a newline in an entity) | quarantined with it |
| Bad or missing checksum on a `C2 E2 G2 V2 B2` record anywhere except the tail | hard error naming the line, for example `wal line 2: wal record checksum mismatch` |
| `C2 E2 G2 B2` record that parses but fails validation | hard error |
| Any other unparseable line in the middle (unknown kind, invalid UTF-8) | hard error naming the line |
| Cross-tenant claim id collision or conflicting batch commit | hard error |
| Header of the snapshot is missing or invalid | hard error |

The same rules apply to `<wal>.snapshot`; its lines are reported as
`snapshot record N` (N counts records after the header).

Quarantining never modifies the WAL or snapshot. The raw line is appended
(and fsynced) to `<wal>.quarantine`. Lines already present in that file are
not appended again, so restarting does not grow it. Replay fails if the
quarantine file cannot be written, because the record would otherwise be
lost. A quarantined claim that is later redefined by a valid record is
treated as present again from that point. Dependent records of a versioned
format are written to the quarantine file in their canonical encoding
rather than byte for byte.

Strict mode. Set `DASH_WAL_REPLAY_STRICT` to `1`, `true`, `yes` or `on`
(case-insensitive) and replay fails on the first record that lenient mode
would quarantine, with an error naming the line, and writes no quarantine
file. Parse failures are found while reading, before any record is applied,
so they are reported ahead of validation failures that come earlier in the
file. Library callers can choose the policy explicitly with
`InMemoryStore::load_from_wal_with_policy`.

## What operators see

At startup each service logs the normal `startup replay` line and, when
anything was quarantined or skipped, a warning with both counts and the
quarantine path. The counts are available programmatically as
`StoreLoadStats.replay.quarantined_records` and
`StoreLoadStats.replay.dependent_skipped`. `snapshot_records` and
`wal_records` count records that were read successfully, which includes
records quarantined later during apply.

Each quarantined record is also printed to stderr as a `warning:` line with
its position.

## Recovery procedures

All commands take the WAL path. Stop the service before running `repair`.
Build the tool with `cargo build --release -p wal-inspect`; the binary is
`target/release/wal-inspect`.

### Inspect

    wal-inspect inspect <wal>

Prints the number of valid records per kind and how many are legacy, the
generation from `<wal>.gen`, whether there is a torn tail (line and bytes),
the number of invalid lines and checksum failures, and the first invalid
line. It does not modify anything. A path ending in `.snapshot` is
detected by its header and reported as a snapshot.

### Verify

    wal-inspect verify <wal>

Exit status 0 if there is no invalid line other than a torn tail, 1 if any
interior line fails to parse or fails its checksum (each is listed), and 2
for usage or I/O errors. `verify` checks line syntax and checksums; it does
not apply the schema validation that replay performs, so a legacy record
with a control character in an id passes `verify` and is quarantined during
replay.

### Repair

    wal-inspect repair <wal> [--dry-run] [--quarantine]

* Always: drops a torn tail and terminates a final line that lacks its
  newline.
* `--quarantine`: moves invalid interior lines out of the file and appends
  them to `<wal>.quarantine`. Without this flag they are left in place and
  the command exits 1.
* `--dry-run`: reports what would happen and changes nothing.

Before any change the tool copies the file to `<file>.bak`. It refuses to
run (exit 2, nothing modified) if that `.bak` already exists. The new
contents are written to a temporary file, fsynced and renamed over the
original. When interior lines were removed the tool writes a new
generation to `<wal>.gen`, so replication followers resync instead of
continuing from stale offsets. Pass a `<wal>.snapshot` path to repair a
snapshot in the same way.

#### Procedure: service refuses to start with a middle-of-file error

1. Stop the service and copy the data directory somewhere safe.
2. `wal-inspect verify <wal>` to list the bad lines.
3. `wal-inspect repair <wal> --dry-run --quarantine` and check the plan.
4. `wal-inspect repair <wal> --quarantine`.
5. `wal-inspect verify <wal>` should now exit 0. Start the service.
6. Review `<wal>.quarantine`. Records lost this way (and anything that
   depended on them) must be re-ingested from the source of truth.
   If the `.bak` should be discarded, delete it before the next repair.

On a replica, discard the replica's WAL and resync from the leader instead
of repairing it.

#### Procedure: legacy WAL, warning about quarantined records

No repair is needed; the service has already started. Review
`<wal>.quarantine`, re-ingest what matters, and leave the file in place or
archive it. Records in the WAL remain as they were and are quarantined
again (without duplicating entries) on every restart until the next
checkpoint rewrites the snapshot from memory and truncates the WAL.
Run with `DASH_WAL_REPLAY_STRICT=1` in staging if you want such WALs to
fail loudly instead.
