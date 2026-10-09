# Persistence

DASH persists state in two layers: an append-only **write-ahead log (WAL)** file with an optional compacted **snapshot**, and an optional **`redb`** database that mirrors writes to disk. This page describes what the code does today and where it has known defects. It replaces an earlier version that described a binary WAL with CRC-32c records, redb sequence numbers and idempotency-key tables; none of those exist.

## Write-ahead log

The WAL is a **text file with one record per line**, tab-separated, with a one-letter record type: `C` claim, `E` evidence, `G` edge, `V` claim vector, `B` batch commit. It is implemented by `FileWal` in `pkg/store/src/wal.rs`. There is no per-record checksum and no length prefix; values are escaped (`\t`, newline) inside fields.

- **Path:** `DASH_INGEST_WAL_PATH` (the ingestion service only keeps state across restarts when this is set; without it the service is in-memory). The retrieval service can also replay a local WAL via `DASH_RETRIEVAL_WAL_PATH`, but normally it follows ingestion by replication.
- **Durability policy:** by default every record is fsynced. Batching knobs (`DASH_INGEST_WAL_SYNC_EVERY_RECORDS`, `..._APPEND_BUFFER_RECORDS`, `..._SYNC_INTERVAL_MS`, `..._BACKGROUND_FLUSH_ONLY`) are guarded: values beyond the safe limits make the service exit unless `DASH_INGEST_ALLOW_UNSAFE_WAL_DURABILITY=true`.
- **Checkpoints:** when `DASH_CHECKPOINT_MAX_WAL_RECORDS` or `DASH_CHECKPOINT_MAX_WAL_BYTES` is exceeded, the log is compacted into a snapshot file next to the WAL (`<wal path>.snapshot`, header `SNAP\t1`) and the WAL is truncated. On restart the snapshot is loaded, then the WAL delta is replayed.
- **Atomicity:** a single ingest appends claim, evidence, edges and vector as separate records with no commit marker, so a crash between records can leave a partial bundle (register DATA-10).

### Recovery

On start, the service replays the snapshot and then the WAL into the in-memory store (`load_from_wal_with_stats_and_ann_tuning`) and logs counts (`claims_loaded`, `evidence_loaded`, `snapshot_records`, `wal_delta_records`). If the WAL cannot be parsed or replayed, the service logs the error and exits with status 1.

Changes in v0.3.0: a torn tail record (partial last line after a crash) is truncated on recovery instead of failing startup, and WAL generation ids force a follower to resync from a full export after the primary compacts, so followers cannot apply a stale offset to a compacted log. Both are listed in the [changelog](../about/changelog.md) as planned for 0.3.0 and are not in v0.2.x.

## redb

[`redb`](https://github.com/cberner/redb) is an embedded, ACID, single-file key-value database. DASH's `DiskBackedStore` (`pkg/store/src/disk.rs`) mirrors claims, evidence, edges, vectors, tenant dimensions, the tenant-to-claims membership set and batch-commit records into redb tables (`dash_claims`, `dash_evidence`, `dash_edges`, `dash_claim_vectors`, `dash_tenant_dims`, `dash_tenant_claims_set`, plus batch commits).

- **On by default** when a WAL path is configured: `./data/dash-ingestion.redb` and `./data/dash-retrieval.redb`. Override with `DASH_INGEST_PERSISTENCE_PATH` / `DASH_RETRIEVAL_PERSISTENCE_PATH`. Turn off with `DASH_INGEST_PERSISTENCE_DISABLE=1` / `DASH_RETRIEVAL_PERSISTENCE_DISABLE=1`. An earlier version of this page said redb was off by default; that was wrong.
- **Failure behavior:** if the file cannot be opened (for example a read-only filesystem), the service logs an error and continues in memory with `disk_status = Unavailable`. `/ready` returns 503 in that case only when a persistence path was explicitly configured.
- **Write order:** each mutation is written to redb before the in-memory state changes; a redb write failure aborts the apply.
- **Single process:** redb takes a file lock; one service process per file.
- Tests: `disk_persistence_round_trip`, `disk_fallback_to_wal_only_on_open_failure`, `disk_open_failure_does_not_crash_service`, `disk_bulk_load_rebuilds_ann_index` in `pkg/store/tests/integration_retrieval.rs`.

### Known defects (v0.2.x)

- Evidence is appended without de-duplication, so restart paths that combine a redb bulk load with WAL replay can duplicate evidence (DATA-01, fixed in v0.3.0 by idempotent upserts).
- Re-upserting a claim drops its in-memory vector while redb keeps it, so memory and disk can diverge (DATA-04).
- The ANN graph is not persisted; it is rebuilt in memory from stored vectors at startup, and the rebuild cost is quadratic in the number of vectors (IDX-01, planned P2).

## Replication offset

The retrieval follower stores the last applied WAL offset in `DASH_RETRIEVAL_REPLICATION_OFFSET_PATH` (default `/var/lib/dash/state/retrieval-replication.offset`) and pulls deltas from ingestion's `/internal/replication/wal`.

## Backup

Use `scripts/backup_state_bundle.sh` and `scripts/restore_state_bundle.sh` (see [Backup](../operations/backup.md)). Copying a live redb or WAL file with `cp` is not guaranteed consistent; stop the service or use the bundle script. The earlier example here that used `systemctl reload` as a checkpoint trigger, `redb-checksum` and `DASH_INGEST_CHECKPOINT_ON_SIGHUP` referred to features that do not exist.

Nothing DASH writes to disk is encrypted by DASH. Use an encrypted volume.
