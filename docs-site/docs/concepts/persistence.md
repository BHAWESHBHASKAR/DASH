# Persistence

DASH persists state in two layers: an append-only **write-ahead log (WAL)** file with an optional compacted **snapshot**, and an optional **`redb`** database that mirrors writes to disk. This page describes what the code does in 0.3.0 (unreleased). It replaces an earlier version that described a binary WAL with CRC-32c records, redb sequence numbers and idempotency-key tables; the WAL is still a text file, and redb does not hold sequence numbers or idempotency keys.

## Write-ahead log

The WAL is a **text file with one record per line**, tab-separated, implemented by `FileWal` in `pkg/store/src/wal.rs`. Records written by 0.3.0 use the kinds `C2` claim, `E2` evidence, `G2` edge, `V2` claim vector and `B2` batch commit. Every field is escaped, and each record ends with `\tcrc=<8 hex digits>`, a CRC-32 of the record body (IEEE 802.3). The checksum detects torn writes and accidental corruption; it is not keyed, so it does not protect against someone who can rewrite the file. Legacy records (`C`, `E`, `G`, `V`, `B`, written by 0.2.x and earlier, no checksum) are still read.

Files next to a WAL at `<wal>`: `<wal>.snapshot` (checkpoint, header `SNAP\t1`), `<wal>.gen` (the WAL generation id used by replication), `<wal>.quarantine` (records replay could not apply, created on demand) and `<wal>.bak` (written by `wal-inspect repair`).

- **Path:** `DASH_INGEST_WAL_PATH` (the ingestion service only keeps state across restarts when this is set; without it the service is in-memory). The retrieval service can also keep a local WAL via `DASH_RETRIEVAL_WAL_PATH`; when it follows ingestion, replicated records are mirrored into it so a restart can resume from the saved offset.
- **Durability policy:** by default every record is fsynced. Batching knobs (`DASH_INGEST_WAL_SYNC_EVERY_RECORDS`, `..._APPEND_BUFFER_RECORDS`, `..._SYNC_INTERVAL_MS`, `..._BACKGROUND_FLUSH_ONLY`) are guarded: values beyond the safe limits make the service exit unless `DASH_INGEST_ALLOW_UNSAFE_WAL_DURABILITY=true`. Concurrent single ingests share fsyncs through group commit (`DASH_INGEST_WAL_GROUP_COMMIT`, on by default): each request is still acknowledged only after its record is durable, and records become visible in WAL order. After a failed fsync the WAL is poisoned: every write answers 503 and `/ready` reports `wal_poisoned` until a restart. Details: [WAL durability and group commit](https://github.com/BHAWESHBHASKAR/DASH/blob/main/docs/operations/wal-durability.md).
- **Checkpoints:** when `DASH_CHECKPOINT_MAX_WAL_RECORDS` or `DASH_CHECKPOINT_MAX_WAL_BYTES` is exceeded, the log is compacted into the snapshot file and the WAL is truncated. On restart the snapshot is loaded, then the WAL delta is replayed. A checkpoint changes the WAL generation; followers that keep up finish the closed generation from the leader's retained copy and switch to the new one without a resync, others rebuild from a chunked export (see docs/operations/replication-limits.md). The service checkpoints at 256 MiB of WAL unless `DASH_CHECKPOINT_MAX_WAL_BYTES` says otherwise (`0` turns it off). If the checkpoint fails after a write has committed, the write is still durable and the response carries `checkpoint_deferred: true`; a later write retries it.
- **Atomicity (0.3.0):** a single ingest is written as one commit group: a `B2` marker whose commit id starts with `~grp:`, the claim, evidence, edge and vector records, and a closing `B2` marker. Replay applies a group completely or not at all, and a group whose closing marker is missing (a crash) is discarded. Batches are staged on a detached copy of the store and committed as a unit.

### Recovery

On start, the service replays the snapshot and then the WAL into the in-memory store and logs counts (`claims_loaded`, `evidence_loaded`, `snapshot_records`, `wal_delta_records`).

- **Torn tail:** a partial or checksum-failing last line is treated as an interrupted write; the file is truncated at open, a warning is printed and the count is reported.
- **Unreadable legacy records:** legacy records that cannot be parsed or that fail current validation (for example a control character in an id), and vector records that fail validation, are appended to `<wal>.quarantine` instead of stopping startup; records that depend on a quarantined claim are skipped. The service logs a warning with both counts. `DASH_WAL_REPLAY_STRICT=1` makes replay fail on the first such record instead.
- **Damaged interior records:** a checksummed record with a bad or missing checksum anywhere but the tail, or any other unparseable line in the middle, is a hard error naming the line, and the service exits with status 1. Use `wal-inspect` (below) to inspect and repair.

Offline tooling: `wal-inspect inspect|verify|repair` (build with `cargo build --release -p wal-inspect`). The full recovery procedures are in [WAL recovery](../operations/wal-recovery.md).

## redb

[`redb`](https://github.com/cberner/redb) is an embedded, ACID, single-file key-value database. DASH's `DiskBackedStore` (`pkg/store/src/disk.rs`) mirrors claims, evidence, edges, vectors, tenant dimensions, the tenant-to-claims membership set and batch-commit records into redb tables (`dash_claims`, `dash_evidence`, `dash_edges`, `dash_claim_vectors`, `dash_tenant_dims`, `dash_tenant_claims_set`, plus batch commits).

- **On by default** when a WAL path is configured: `./data/dash-ingestion.redb` and `./data/dash-retrieval.redb`. Override with `DASH_INGEST_PERSISTENCE_PATH` / `DASH_RETRIEVAL_PERSISTENCE_PATH`. Turn off with `DASH_INGEST_PERSISTENCE_DISABLE=1` / `DASH_RETRIEVAL_PERSISTENCE_DISABLE=1`. An earlier version of this page said redb was off by default; that was wrong.
- **Failure behavior:** if the file cannot be opened (for example a read-only filesystem), the service logs an error and continues in memory with `disk_status = Unavailable`. `/ready` returns 503 in that case only when a persistence path was explicitly configured.
- **Write order:** the WAL is the source of truth. For a single ingest or a batch, the WAL records are appended first, then the in-memory state is swapped in, and the staged redb writes are applied last. If a redb write fails after the WAL commit, the in-memory commit stands, the disk handle is dropped and `disk_status` becomes `Unavailable` (so redb never silently diverges). Claim, evidence and edge writes to redb go through a single transaction.
- **Single process:** redb takes a file lock; one service process per file.
- Tests: `disk_persistence_round_trip`, `disk_fallback_to_wal_only_on_open_failure`, `disk_open_failure_does_not_crash_service`, `disk_bulk_load_rebuilds_ann_index` in `pkg/store/tests/integration_retrieval.rs`.

### Idempotency and known gaps

- Evidence is upserted by `evidence_id` and edges by `(from, to, relation)`, in memory, in redb, on bulk load and on replication re-apply, so a restart that combines a redb bulk load with WAL replay no longer duplicates evidence (DATA-01).
- Re-upserting a claim keeps its vector and ANN entry, in memory and in redb (DATA-04).
- The vector index is not persisted; it is rebuilt in memory from the stored vectors at startup. Replay collects the vectors first and then builds each tenant once (a tenant at or below the flat threshold costs a copy; a larger one builds a `usearch` HNSW from several threads). Measured: replaying a WAL with 100,000 x 384-d vectors takes about 24 s on 4 vCPUs, 18.6 s of it the index build; building the same index one insert at a time takes about 75 s. Persisting or memory-mapping the index is a follow-up (ADR 0003 measured a 45 ms `view` of a 500k-vector index).
- Services still cold-start from the WAL (and snapshot), not from redb (DATA-11, planned P2).

## Replication offset

A follower stores `(generation, offset)` together. The retrieval follower writes it to `DASH_RETRIEVAL_REPLICATION_OFFSET_PATH`, defaulting to `<retrieval WAL path>.replication` when a retrieval WAL is configured (without a retrieval WAL nothing replicated survives a restart, so it always starts with a full resync). An ingestion node that follows another ingestion node does the same with `DASH_INGEST_REPLICATION_OFFSET_PATH`. When the leader's WAL generation changes (after a checkpoint, or after a repair that removed lines), or the saved state does not match the local WAL, the follower replaces its state from a full export. Frames never end inside a commit group, and `/ready` reports follower lag and staleness.

## Backup

Use `scripts/backup_state_bundle.sh` and `scripts/restore_state_bundle.sh` (see [Backup](../operations/backup.md)). Copying a live redb or WAL file with `cp` is not guaranteed consistent; stop the service or use the bundle script. The earlier example here that used `systemctl reload` as a checkpoint trigger, `redb-checksum` and `DASH_INGEST_CHECKPOINT_ON_SIGHUP` referred to features that do not exist.

Nothing DASH writes to disk is encrypted by DASH. Use an encrypted volume.
