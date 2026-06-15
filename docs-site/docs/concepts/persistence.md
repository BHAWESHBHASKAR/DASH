# Persistence

DASH persists every state change to disk. The persistence story has two layers: an append-only **write-ahead log (WAL)** that is the first thing written, and a **`redb` snapshot** that is the durable, queryable on-disk representation. This page describes both layers, the crash-recovery semantics, and the backup procedure.

## redb architecture

[`redb`](https://github.com/cberner/redb) is a pure-Rust, ACID, single-file embedded database. DASH uses redb PR 1 (additive, default off; enable with `DASH_INGEST_PERSISTENCE_PATH` / `DASH_RETRIEVAL_PERSISTENCE_PATH`).

### File layout

A redb file is a single file on disk. DASH uses one redb file per service, so a typical deployment has:

```text
/var/lib/dash/
├── ingest.redb         # the ingestion service's redb
└── retrieval.redb      # the retrieval service's redb
```

The file is mmap'd at startup. redb manages its own page cache; DASH does not pin pages in the OS page cache explicitly.

### Tables

The table layout is described in [Multi-tenancy → Per-tenant key spaces](multi-tenancy.md#per-tenant-key-spaces-in-redb). The short version: one table per type, with `(tenant_id, id)` keys.

### Transactions

redb is ACID. DASH uses **one transaction per ingest** (a `Claim` + its `Evidence` + its `ClaimEdge`s + its `Vector` are committed atomically) and **one transaction per retrieval-side mutation** (none, in the current design — the retrieval service is read-only against redb).

A failed transaction rolls back the file to the previous commit point. The file is never partially written.

### Why redb (and not sled / rocksdb)

The full decision is in [ADR-001](../reference/architecture-decisions.md#adr-001-why-redb-over-sledrocksdb). The short version: redb is pure Rust (no CGo, no `librocksdb` to vendor), has a tiny API surface, and its on-disk format is forward-compatible across versions. The trade-off — single-process write lock — is acceptable because DASH is designed to scale horizontally with more processes, not more concurrency inside a process.

## WAL fallback

When `DASH_*_PERSISTENCE_PATH` is **unset** (the default), the ingestion service runs in **WAL-only mode**. Every `IngestRequest` is appended to the WAL as a length-prefixed, CRC-32c-checked record, and the in-memory state is updated. The redb snapshot is not written.

This is the pre-redb behavior, preserved bit-for-bit. The motivation is to keep the deployment surface area small: an operator who is not ready to commit to a redb file lifecycle can run DASH in WAL-only mode and replay on every restart.

The WAL lives at the path configured by `DASH_INGEST_WAL_PATH` (default: `/var/lib/dash/ingest.wal`).

### WAL record format

```text
┌──────────┬──────────┬─────────────┬────────────────┐
│ len: u32 │ crc: u32 │ kind: u8    │ payload bytes  │
└──────────┴──────────┴─────────────┴────────────────┘
```

- `len` — total record length including the header.
- `crc` — CRC-32c over the payload bytes.
- `kind` — `1 = Ingest`, `2 = TenantCreate`, `3 = TenantDelete`.
- `payload` — bincode-encoded `IngestRecord` (or the analogous type).

A corrupted record (CRC mismatch) terminates replay with an error. The recovery procedure is documented below.

## Crash recovery semantics

DASH's recovery story is **WAL-replay-into-redb**, with the redb snapshot as a restart-time accelerator.

### Restart in WAL-only mode

1. The ingestion service starts.
2. It opens the WAL with `FileWal::open`.
3. It reads records sequentially, validates CRCs, and re-applies each `IngestRecord` to the `InMemoryStore`.
4. The first CRC failure halts the replay. The service starts with the partial state recovered up to that point and logs the position of the failed record.

### Restart with redb enabled

1. The ingestion service starts.
2. It opens the redb file with `DiskBackedStore::open`.
3. It loads the `claims`, `evidence`, `vectors`, and `ann_index` tables into the in-memory caches.
4. It opens the WAL and replays any records with `seq > last_redb_seq`. The `last_redb_seq` is stored in a redb-internal key.
5. The ANN graph is rebuilt from the recovered vectors (the on-disk HNSW is treated as advisory; the in-memory graph is the source of truth during a process's lifetime).

The result is **at-least-once** durability with **exactly-once** semantics on the application level (idempotency keys on `IngestRequest` make a re-replay safe).

### What "exactly-once" means

A `POST /v1/ingest` with `idempotency_key: "abc"` can be safely retried by the client. The ingestion service stores the `(tenant_id, idempotency_key) → result` mapping in redb; a retry with the same key returns the original result without re-applying the bundle. See [Ingest → Idempotency](../guides/ingest.md#idempotency).

## Backup procedure

The recommended backup procedure is **filesystem-level snapshot + WAL archive**. Run periodically (cron, `systemd` timer, or a Kubernetes `CronJob`):

```bash
#!/usr/bin/env bash
# Backup DASH state. Run from a host with read-only access to the data dir.
set -euo pipefail

DATA=/var/lib/dash
BACKUP=/var/backups/dash/$(date -u +%Y%m%dT%H%M%SZ)
mkdir -p "$BACKUP"

# 1. Flush redb to disk (durable, fsync'd)
systemctl reload dash-ingestion   # SIGHUP triggers a checkpoint
systemctl reload dash-retrieval

# 2. Snapshot the redb files
install -m 0644 "$DATA/ingest.redb"    "$BACKUP/ingest.redb"
install -m 0644 "$DATA/retrieval.redb" "$BACKUP/retrieval.redb"

# 3. Archive the WAL (in case the redb snapshot is older than the WAL head)
install -m 0644 "$DATA/ingest.wal"     "$BACKUP/ingest.wal"

# 4. Verify
redb-checksum "$BACKUP/ingest.redb"
redb-checksum "$BACKUP/retrieval.redb"

# 5. Upload to object storage
aws s3 cp --recursive "$BACKUP" "s3://my-bucket/dash-backups/$(basename "$BACKUP")/"
```

For cross-region durability, ship the snapshot to a second region immediately after step 4. The replication target is responsible for re-running step 5 against the second bucket.

For an in-depth operations runbook, see [Backup](../operations/backup.md).
