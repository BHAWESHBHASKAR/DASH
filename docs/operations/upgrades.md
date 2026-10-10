# Upgrades and rollbacks

This page is the operator's reference for moving a running deployment from one
DASH release to another and back: which paths are supported, in which order to
restart the nodes, what to back up, how to roll back, and which release can
read which on-disk and wire format. The 0.2.x and 0.3.0 columns of the format
table and the procedures below are backed by the compatibility tests in
`tests/compat` (see [How this is tested](#how-this-is-tested)); where a test
can only check an older release's rules instead of running its binary, or a
statement rests on other tests, that is said.

The release-specific checklist for 0.2.x to 0.3.0 (secrets, roles, rate limits,
SDK changes) is the "Upgrading to 0.3.0" section of
[`CHANGELOG.md`](../../CHANGELOG.md); do both.

## Supported paths

| From | To | In place | Rollback |
|---|---|---|---|
| 0.2.x (`main` up to `ae86667`) | 0.3.0 | Yes. The WAL, snapshot, redb mirror, segment directories, audit log, lease file and placement state are read and migrated forward as described below. | Only by restoring the backup taken before the upgrade. 0.2.x cannot read what 0.3.0 writes (WAL records, redb rows, lease file). |
| 0.3.0 pre-release build with deletes | 0.3.0 pre-release build without deletes | Downgrade only after a checkpoint (see [Tombstones](#tombstones-t2)). | n/a |
| 0.3.x | 0.3.y | Intended; covered once the 0.3.0 fixture is tagged and later releases add theirs (see [Release checklist](#release-checklist)). | Within a line, a rollback is possible while the newer release has written no format the older one cannot read; the table records what each release writes. |

Skipping releases is not tested: upgrade through each release in turn.

## Format versions

"Reads" means the current code loads the artifact and serves the same data;
"migrates" means it rewrites it in its own format; "0.2 reads 0.3" is what a
rollback without restoring a backup would face.

| Artifact | 0.2.x writes | 0.3.0 writes | 0.3.0 reads 0.2.x | 0.2.x reads 0.3.0 |
|---|---|---|---|---|
| WAL `<wal>` | unchecksummed records `C` `E` `G` `V` `B` | checksummed records `C2` `E2` `G2` `V2` `B2` (`\tcrc=<8 hex>`), commit-group markers (`B2 ~grp:` / `B2 ~tx:`), tombstones `T2` | Yes; legacy records replay, duplicate evidence rows collapse to the last one | No: an unknown record kind fails the whole replay |
| WAL lineage `<wal>.gen` | not written | 16 hex digits; changes on every checkpoint, reset and resync | Created on first open, stable across restarts | Ignored |
| Snapshot `<wal>.snapshot` | `SNAP\t1` + legacy records | `SNAP\t1` + checksummed records | Yes; the first checkpoint rewrites it in the 0.3 format | No (same record kinds as the WAL) |
| redb mirror (`DASH_*_PERSISTENCE_PATH`) | `bincode` 1.x values | same layout with the 8-byte header `DASHv2\0\xff` | Yes; a row is rewritten with the header when it changes, a follower resync rewrites every row; mixed files load | No: 0.2 decodes a claim's evidence and edge blob on every write and fails on a headered blob |
| Persisted vector index `<wal>.vindex` | not written | `DASHVIDX`, format version 1, manifest with WAL generation | n/a (built from the WAL and saved) | Ignored. A file of another format version or WAL generation is rebuilt, never served |
| Segment directories (`DASH_*_SEGMENT_DIR`) | `DASHSEG-MANIFEST 1`, `DASHSEG 1`, directory per tenant with a lossy name, no marker | same manifest and segment formats; collision-free directory names (`tenant_b` becomes `tenant_5fb`), `segments.tenant` marker, `segments.fingerprint`, unique file names | Yes. A legacy directory whose name the new mapping changes has no marker, so it is left untouched and the tenant is republished into its new directory on its next write; until then retrieval scans the tenant without a prefilter | Manifest and segment formats yes, but tenants whose directory name changed are looked up in the old, stale directory: restore the segment directory with the WAL |
| Audit log (`DASH_*_AUDIT_LOG_PATH`) | JSON lines without `v` (ingestion: sorted keys; retrieval: insertion order) | record version 2 (`"v":2`, canonical field order) | Yes; the verifier accepts both, the writer continues the chain (`seq`, `prev_hash`) | The 0.2 writer continues from the last `seq`/`hash`; verify the mixed chain with 0.3's `audit-verify` |
| Control-plane lease (`DASH_CONTROL_PLANE_LEASE_PATH`) | `node_id,epoch,expires_at_ms` | `node_id,epoch,expires_at_ms,instance_id`, plus `leader.lease.epoch` (fencing floor) and `leader.lease.lock` (flock) | Yes; the first acquisition issues a fencing token above the old one | No: 0.2 requires exactly three fields and never becomes leader |
| Placement state (`DASH_CONTROL_PLANE_STATE_PATH`) | CSV `tenant_id,shard_id,epoch,node_id,role,health` | same | Yes | Yes |
| Replication frames (`/internal/replication/wal`, `/export`) | no `generation=` line | `generation=<lineage>` as the second line | Refused: both followers apply nothing and report `replication_leader_too_old` | Refused: the 0.2 follower expects `needs_resync=` on the second line and fails every poll |
| Follower cursor (`<wal>.replication`, `DASH_RETRIEVAL_REPLICATION_OFFSET_PATH`) | bare offset (retrieval only) | `generation=…` and `offset=…` | A bare offset is read as a cursor without a generation, which the leader answers with a resync (the parser is unit-tested in `services/retrieval`, not in `tests/compat`) | n/a |
| HTTP API `/v1` | | | Request bodies and the API key header of 0.2 clients are accepted; every response field 0.2 returned is still returned. Authentication and role defaults changed (CHANGELOG checklist, steps 2 to 5) | n/a |

### Why a 0.2 leader cannot be followed

A 0.2 leader answers `from_offset=0` with its WAL tail only and never with the
snapshot, and it reports a compaction only when the follower's offset happens
to be beyond the new end of the WAL. A follower could therefore silently miss
everything that was checkpointed, or skip records after a compaction. 0.3
followers refuse any frame without a generation instead of guessing; `/ready`
reports `replication_leader_too_old` and nothing is applied.

### Retrieval answers after 0.2 to 0.3

The data is identical after the upgrade (the tests compare every claim,
evidence row, edge and vector dimension with what was written), but 0.3.0
changed ranking on purpose: an edge `from supports to` credits the target
instead of its author, support and contradiction bonuses saturate, a
re-ingested evidence id counts once, and the "scan every vector" fallback for
a query vector of the wrong dimension is gone. Expect different scores and
some different orderings. The exact differences on the test dataset, each
with its cause, are in `tests/compat/expected/v0.2-main-ae86667.json`.

## Before any upgrade: back up

Stop writes (or stop ingestion) and take a state bundle on every node that
has a WAL:

```bash
scripts/backup_state_bundle.sh --wal-path <wal> --segment-dir <segments> \
  [--placement-file <placements.csv>] --output-dir <dir>
wal-inspect verify <wal> && wal-inspect verify <wal>.snapshot
```

The bundle holds the WAL, its snapshot, the segment directory and the
placement file. Also copy, outside the bundle:

- the audit logs (`DASH_*_AUDIT_LOG_PATH`), if you keep them on the node;
- the control-plane state (`DASH_CONTROL_PLANE_STATE_PATH` and its checksum
  file) and the lease file with `leader.lease.epoch` if it exists;
- your environment / secrets files.

The redb file, `<wal>.vindex` and `<wal>.gen` are derived and need no backup:
the redb mirror and the vector index are rebuilt from the WAL, and a restored
WAL without its `.gen` file starts a new lineage, which makes followers
resync from a full export (the safe outcome).

## Order of operations

### Single node

1. Back up (above).
2. Stop the service (SIGTERM; it drains in-flight requests).
3. Install the new binary or image and update the environment for the new
   release (CHANGELOG checklist).
4. Start it. Replay reads the old WAL and snapshot; check the startup log line
   with `snapshot_records` / `wal_delta_records` and that no
   `<wal>.quarantine` file appeared (`DASH_WAL_REPLAY_STRICT=1` turns any
   unreadable record into a startup error, useful in staging).
5. Check `/ready`, run a few known queries, run `audit-verify` on the audit
   logs.
6. Optional: trigger a checkpoint (or wait for `DASH_CHECKPOINT_MAX_WAL_*`) to
   rewrite the snapshot in the new format. After this point rollback is only
   by restore.

### Ingestion leader with followers (ingestion or retrieval)

Replication between 0.2 and 0.3 is refused in both directions, so there is
no mixed-version window in which data flows. Upgrade **the leader first,
then the followers**:

1. Back up the leader (followers can be rebuilt from it).
2. Upgrade and start the leader as for a single node. Old followers now fail
   every poll (they keep serving the data they have; watch their logs and
   lag metrics) and apply nothing.
3. Upgrade each follower. On its first poll it has no 0.3 cursor, so it
   resyncs from the leader's full export (replacing its state, redb rewritten
   in the new format) and then follows deltas. `/ready` turns 200 once it has
   synced and is within `DASH_*_REPLICATION_MAX_LAG_RECORDS`.

Upgrading followers first also works, but they report not ready (`/ready`
503 with `replication_leader_too_old`), so a load balancer takes them out of
rotation until the leader is upgraded. Prefer leader first so readers keep a
(stale) answer during the window.

### Control plane replicas

0.2 cannot read the lease file 0.3 writes, and 0.2 does not take the
`leader.lease.lock` file lock 0.3 uses. Do not run 0.2 and 0.3 control planes
against the same lease path at the same time: stop every control-plane
replica, upgrade all of them, start them. The first 0.3 replica takes over the
old (expired) lease with a fencing token above the old epoch. The persisted
placement state is read unchanged.

## Rollback

### 0.3.0 to 0.2.x

0.2 cannot read the WAL or snapshot after 0.3 has written to them, so a
rollback is a restore. To roll back, restore the backup taken before the
upgrade:

1. Stop every node (ingestion, retrieval, control plane).
2. Restore the state bundle with `scripts/restore_state_bundle.sh` (WAL,
   snapshot, segment directory, placement file).
3. Delete the redb file (`DASH_*_PERSISTENCE_PATH`): 0.2 cannot decode rows
   with the 0.3 header. 0.2 creates a fresh mirror.
4. Delete `<wal>.gen`, `<wal>.vindex` and `<wal>.replication` (0.2 ignores
   them; removing them keeps a later re-upgrade clean).
5. On the control plane, delete `leader.lease`, `leader.lease.epoch` and
   `leader.lease.lock` (0.2 cannot read the 0.3 lease) and restore the
   placement state if it changed.
6. Start the 0.2 leader, then the followers. Followers that ran 0.3 hold
   0.3 records too: restore them from their own pre-upgrade backup, or
   rebuild them from a copy of the leader's restored WAL and snapshot. Do not
   let a 0.2 follower start empty: the 0.2 protocol sends a fresh follower
   only the leader's WAL tail, never the snapshot.

Writes accepted after the upgrade are lost by this procedure; replay them from
your source system. The audit log can stay: 0.2 continues the chain.

### Tombstones (`T2`)

A build without deletes fails its replay on a tombstone record. Before running
such a build on a WAL written by a release with deletes, run a checkpoint
before the downgrade on every node whose WAL holds tombstones: lower
`DASH_CHECKPOINT_MAX_WAL_RECORDS` temporarily and send a write that crosses
it (as in [data deletion](data-deletion.md)), then confirm with
`wal-inspect inspect <wal>` and on `<wal>.snapshot` that no `T2` kind is
counted. The checkpoint drops deleted rows from the snapshot for good and
leaves no `T2` record behind. Followers resync after the leader's checkpoint
(the generation changes) and then hold no tombstone either.

### Persisted vector index

`<wal>.vindex` is derived data: deleting it is always safe (the next start
rebuilds the index from the WAL and logs `no saved index yet`). A file of
another format version, or saved for another WAL generation, is discarded
with a warning and rebuilt, so a downgrade or re-upgrade never serves a stale
index.

## How this is tested

`tests/compat` (crate `dash-compat`, part of the normal workspace tests) holds
a fixture per release under `tests/compat/fixtures/<label>/`: the state
directory a release wrote for a fixed dataset (`tests/compat/dataset`), the
answers it gave to the dataset's ingest and retrieve requests, the replication
frames it served and its control-plane files. The current code is started on
a scratch copy of each fixture and must:

- replay the WAL and snapshot with the strict replay policy and hold exactly
  the data the dataset wrote (`wal_snapshot.rs`);
- serve the retrieve answers the old release recorded, before and after a
  checkpoint (`retrieve_results.rs`), with the by-design differences listed
  per fixture in `tests/compat/expected/`;
- load the redb mirror, rewrite changed rows with the header and load mixed
  files (`redb_mirror.rs`), load or rebuild the persisted vector index
  (`vector_index_file.rs`), verify the segment directories (`segments.rs`),
  continue the audit chain (`audit_chain.rs`), take over the lease and read
  the placement state (`control_plane.rs`);
- refuse 0.2 replication frames, follow recorded 0.3 frames, and serve
  followers from an upgraded leader whose WAL still holds 0.2 records
  (`replication_wire.rs`);
- accept the old releases' ingest requests with compatible responses
  (`http_api.rs`).

Running a 0.2 binary on 0.3 output is not part of the test run. The downgrade
statements are checked against 0.2's rules instead: `dash_compat::old_readers`
reproduces, from the 0.2 source, the WAL record kinds it accepts, its lease
and replication-frame parsers and its redb blob decoding, and the tests assert
that 0.3 output fails them (and that the procedures above are in this page).

## Release checklist

For every release, before tagging:

1. Capture its fixture from the release candidate commit, labelled with the
   tag it will get:
   `scripts/compat/generate_fixtures.sh --ref <release commit> --label <tag>`.
   The script builds that commit in a temporary git worktree with its own
   target directory, runs `scripts/compat/run_scenario.py` against the
   binaries, and deletes the worktree and build output afterwards. It needs
   `git`, `cargo`, `python3` and `gzip`; the fixture is a few hundred KB.
2. Add the label to `FIXTURES` in `tests/compat/src/lib.rs` (the
   `every_fixture_directory_is_registered` test fails until you do) and set
   its `Era` and `has_deletes`. Once the release is tagged, its fixture
   replaces the `v0.3.0-dev`-style pre-release fixture of the same line.
3. If the release changes ranking on purpose, add
   `tests/compat/expected/<older label>.json` entries with the reason for each
   changed answer; any unexplained difference fails the tests.
4. If the release introduces a format, add a row to the table above, a
   downgrade rule if older releases cannot read it, and an oracle in
   `dash_compat::old_readers` for the previous release's reader.
5. Run `cargo test -p dash-compat` (it is also part of
   `cargo test --workspace` in CI), commit, then tag. The release workflow's
   `upgrade-fixture` job runs `scripts/compat/check_release_fixture.sh <tag>`
   and blocks the GitHub release when the tag has no registered fixture
   generated from the tagged commit or an ancestor of it.
