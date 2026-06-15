# DASH Persistence — redb-backed On-Disk Storage

Date: 2026-06-13
Status: Design — ready for the next-session sprint
Owner: TBD
Estimated effort: 5-7 focused days

## Why this is the #1 follow-up

The current `InMemoryStore` in `pkg/store` holds every claim, evidence
edge, embedding, and index entry in `HashMap`s. A process restart
forces a full WAL replay (`load_from_wal_with_stats_and_ann_tuning`) to
rebuild the in-memory state, which:

- Takes minutes on million-claim corpora.
- Doubles memory consumption (WAL on disk + full in-memory copy).
- Loses the ANN index — it has to be rebuilt from scratch, which is the
  slowest part of startup.
- Blocks the path to horizontal scale: a fresh replica cannot catch up
  to a peer without replaying the full WAL from genesis.

`redb` is already in the workspace dependency set (`Cargo.toml:34`)
and is the right tool: an embedded, ACID, B-tree–keyed key/value store
written in pure Rust. Using it for on-disk materialization gives us
fast cold-start (load only what's in the snapshot, tail the WAL),
durability (snapshot + WAL atomic), and a clean path to replication
(disk snapshot + WAL delta = a transferable state).

This document captures the design, the file-level change list, the
phasing, and the acceptance criteria. Implementation begins in the
next session.

## Goals

1. **Crash-safe durability** — process kill at any moment, on restart
   the store is at-least-as-recent as the last successfully committed
   WAL batch + atomic snapshot.
2. **Fast cold start** — `O(redb-snapshot-load) + O(wal-tail-replay)`,
   not `O(full-wal-replay)`. A 1M-claim corpus with 1k recent writes
   should start in under 5 seconds.
3. **No behavior change for existing API** — the public
   `InMemoryStore` interface (and the `retrieve*` / `ingest_bundle*` /
   ANN / cross-tenant semantics) is unchanged. The new persistence is
   transparent.
4. **Replication-friendly** — the on-disk state is the natural
   replication unit (snapshot file + bounded WAL tail).
5. **redb failure is not a fatal error** — if redb can't be opened or
   a write fails, the store falls back to "WAL-only" mode (the
   pre-redb behavior) and surfaces a metric + log. This is the
   production-readiness property: never lose data to a new dependency
   failure.

## Non-goals (this iteration)

- Distributed replication (covered in `phase-07-routing-placement-and-failover.md`)
- Product quantization / scalar quantization (covered in `phase-09-performance-scale-and-benchmarking.md`)
- Sharding
- Online index migration (snapshot→snapshot)

## Data model

`redb` is a B-tree store keyed by `(table, key)` tuples. We define
one table per data kind, with the table key encoding the natural
primary key:

```rust
const TABLE_CLAIMS:        TableDefinition<&str, &[u8]>;     // key = claim_id
const TABLE_EVIDENCE:      TableDefinition<&str, &[u8]>;     // key = claim_id (Vec<Evidence> serialized as one row, replaced atomically)
const TABLE_EDGES:         TableDefinition<&str, &[u8]>;     // key = from_claim_id
const TABLE_CLAIM_VECTORS: TableDefinition<&str, &[u8]>;     // key = claim_id
const TABLE_TENANT_DIMS:   TableDefinition<&str, u64>;       // key = tenant_id
const TABLE_TENANT_CLAIMS: TableDefinition<&str, u64>;       // key = tenant_id, value = tombstone counter (monotonic; used for tenant->claim set membership checks)
const TABLE_TENANT_CLAIMS_SET: TableDefinition<(&str, &str), ()>; // composite (tenant_id, claim_id) -> () for fast "does tenant X have claim Y?" checks
const TABLE_BATCH_COMMITS:  TableDefinition<&str, &[u8]>;    // key = commit_id
const TABLE_INDEX_STATS:    TableDefinition<&str, &[u8]>;    // key = "stats" (singleton)
```

Evidence-by-claim is stored as a single `Vec<Evidence>` blob per
`claim_id`, replaced atomically on every evidence update for that
claim. This keeps the read path simple (one row per claim) and the
write path simple (one transaction per claim update). The trade-off is
that partial evidence updates require reading the whole Vec, modifying
in place, and writing it back — that's acceptable because evidence
volume per claim is bounded (< 100 in practice) and the write path is
already the slow path.

Edges are stored similarly as a `Vec<ClaimEdge>` per `from_claim_id`.

Vectors are stored as a `Vec<f32>` blob per `claim_id` (length-prefixed
or bincode-serialized).

The `TABLE_TENANT_CLAIMS_SET` table exists to make the
"is this claim in this tenant's set?" check O(1) instead of
"scan all claims for tenant_id matches". Insertions add a row;
deletions (which never happen in DASH today) would remove a row. This
table is updated synchronously with `apply_claim` and consumed by
`retrieve*`'s tenant filter.

The ANN index (`TenantAnnIndex` wrapping `usearch::Index`) is
**not** stored in redb. It is rebuilt from `TABLE_CLAIM_VECTORS` on
startup, just as it is today from `claim_vectors`. Rebuilding a
usearch index is fast (< 1 second for 100k vectors of dimension 384)
and the alternative — persisting the HNSW graph — would couple us
to usearch's on-disk format which is unstable across versions.

## Write path

The current `ingest_bundle_persistent` flow is:

```
ingest_bundle -> validate_bundle -> wal.append_claim -> apply_claim
                                                        -> apply_evidence (per item)
                                                        -> apply_edge (per item)
```

The new write path keeps the WAL as the source of truth, but uses
redb as the materialized state for fast cold-start:

```
ingest_bundle -> validate_bundle -> wal.append_claim -> apply_claim
                                                          -> apply_to_redb (atomic txn)
                                                          -> apply_to_ann (in-memory)
                                                        -> apply_evidence (per item)
                                                          -> apply_to_redb (atomic txn per claim's evidence blob)
                                                        -> apply_edge (per item)
                                                          -> apply_to_redb (atomic txn per edge source's edges blob)
```

A new module `pkg/store/src/disk.rs` contains the `DiskBackedStore`
struct, which holds a `redb::Database` and exposes typed read/write
methods. The new `InMemoryStore::apply_claim` etc. call
`DiskBackedStore::put_claim` inside a `redb::WriteTransaction`. On
commit failure, the in-memory state is **not** updated, the WAL
record is **not** appended (the WAL append happens *after* successful
redb commit), and the error is returned to the caller. This
preserves the existing "all or nothing" semantic.

Read-side fan-out: every read that currently hits
`self.claims.get(&claim_id)` is unchanged — it still hits the
in-memory map. The redb store is **only** consulted on cold start
(after WAL replay) to verify or to skip WAL replay entirely if the
snapshot is current.

## Read path (cold start)

The new cold-start path is the heart of the change:

```rust
fn load(store: &mut InMemoryStore, db: &redb::Database) -> Result<()> {
    // 1. Open the redb snapshot — this is the durable materialized state.
    // 2. Bulk-load every claim into store.claims.
    // 3. Bulk-load every evidence blob into store.evidence_by_claim.
    // 4. Bulk-load every edge blob into store.edges_by_claim.
    // 5. Bulk-load every vector into store.claim_vectors.
    // 6. Bulk-load the tenant->claim set and tenant->dim map.
    // 7. Rebuild each tenant's usearch index from the vectors loaded in 5.
    // 8. Replay any WAL records with sequence > the snapshot's high-water-mark.
    //    (We embed the HWM in the snapshot; WAL records have a sequence.)
    // 9. Compact: write a fresh snapshot reflecting the replayed state, then
    //    truncate the WAL.
    // 10. Return ready.
}
```

Step 1-7 are O(redb size) which is fast. Step 8 is O(WAL tail) which is
small. Step 9 ensures the next cold start skips the WAL tail entirely.

## Crash recovery semantics

A crash can happen at any of these points:

1. **Before `redb::WriteTransaction::commit`**: nothing visible, no WAL
   record. On restart, the previous snapshot is intact and consistent.
2. **After `redb::WriteTransaction::commit` but before WAL append**:
   redb has the new claim, WAL doesn't. On restart, we load from redb
   (which has the claim) and the WAL tail is empty. Correct.
3. **After WAL append**: both redb and WAL agree. On restart, we
   load from redb and the WAL tail is empty. Correct.

The dangerous scenario is the inverse: WAL has a record that redb
doesn't. This cannot happen with the design above because the WAL
append is the **last** step. If the WAL append fails, we return an
error to the caller and the in-memory + redb state are unchanged.

## Failure mode: redb can't open

If `redb::Database::open` fails on startup (corrupt file, schema
mismatch, etc.), the store logs the error, sets a `disk_status:
DiskStatus::Unavailable` flag, and falls back to "WAL-only" mode:

```rust
match redb::Database::open("dash.db") {
    Ok(db) => store.disk = Some(DiskBackedStore::new(db)),
    Err(e) => {
        tracing::error!(error = %e, "redb open failed; falling back to WAL-only mode");
        store.disk_status = DiskStatus::Unavailable;
        // Existing WAL replay path runs unchanged.
    }
}
```

WAL-only mode behaves exactly like the pre-redb `InMemoryStore`.
Writes still go to WAL; reads still hit the in-memory map; the only
loss is fast cold start. This is the production-readiness property:
**a new dependency never makes the system less available than it was
before the dependency existed**.

## File-level change list

| File | Change |
|---|---|
| `pkg/store/Cargo.toml` | Add `redb = { workspace = true }` (already present) — no change |
| `pkg/store/src/lib.rs` | New `mod disk;` module; `InMemoryStore` gains a `disk: Option<DiskBackedStore>` field, a `disk_status: DiskStatus` field, a `disk_snapshot_high_water_mark: u64` field, and a `load_from_disk_and_wal(path, ann_tuning)` constructor; `apply_claim`/`apply_evidence`/`apply_edge`/`apply_claim_vector`/`apply_batch_commit_record` gain a `try_apply_to_disk` call that fails the in-memory apply if redb returns Err |
| `pkg/store/src/disk.rs` (new) | `DiskBackedStore` struct: `new(path)`, `put_claim(claim)`, `get_claim(id) -> Option<Claim>`, `put_evidence_blob(claim_id, &[Evidence])`, `get_evidence_blob(claim_id) -> Option<Vec<Evidence>>`, `put_edge_blob(from, &[ClaimEdge])`, `get_edge_blob(from) -> Option<Vec<ClaimEdge>>`, `put_vector(claim_id, &[f32])`, `get_vector(claim_id) -> Option<Vec<f32>>`, `put_batch_commit(commit)`, `put_tenant_dim(tenant, dim)`, `get_tenant_dim(tenant) -> Option<usize>`, `add_claim_to_tenant(tenant, claim)`, `claim_in_tenant(tenant, claim) -> bool`, `for_each_claim_in_tenant(tenant, &mut F)`, `set_stats(s)`, `get_stats()`, `high_water_mark() -> u64`, `set_high_water_mark(u64)`, `commit_pending_writes()` |
| `pkg/store/src/disk.rs` (new) | `DiskStatus` enum: `Available`, `Unavailable { reason: String }`, `Recovering` |
| `pkg/store/src/lib.rs` | `apply_claim_vector` no longer marks a separate "dirty" set; on-disk and in-memory are updated together |
| `pkg/store/src/lib.rs` | `InMemoryStore::load_from_disk_and_wal` constructor: opens redb, bulk-loads everything, rebuilds the ANN, then replays the WAL tail (`wal_replication_delta_from` returns a delta from any offset) |
| `pkg/store/src/lib.rs` | New `InMemoryStore::checkpoint_to_disk` method: writes a fresh redb snapshot, truncates the WAL. Invoked periodically by the existing checkpoint policy. |
| `pkg/store/src/lib.rs` | `InMemoryStore::index_stats()`: if the in-memory map disagrees with the redb count (e.g. disk fell back), surface the discrepancy via a new `disk_status` field on `StoreIndexStats` |
| `pkg/store/tests/integration_retrieval.rs` | New tests: `disk_persistence_round_trip`, `disk_fallback_to_wal_only_on_open_failure`, `disk_recovery_after_partial_write`, `disk_crash_recovery_simulated_via_drop`, `disk_tenant_set_membership_consistency`, `disk_snapshot_compaction_clears_wal_tail` |
| `services/ingestion/src/main.rs` | `DASH_DASH_PERSISTENCE_PATH` env var (default `/var/lib/dash/state/dash.db`); if set, the service opens the disk store; if unset, it falls back to the existing in-memory-only mode |
| `services/retrieval/src/main.rs` | Same env var |
| `docs/operations/redb-persistence.md` (new) | Operator-facing runbook: how to back up the redb file, how to recover from corruption, how to migrate from WAL-only to redb-backed, how to monitor `disk_status` |

## Phasing

The change is large enough to be split into three reviewable PRs:

**PR 1 (1-2 days):** Add the `DiskBackedStore` struct, the redb tables, the
`put_*` / `get_*` API, and the bulk-load cold-start path. No behavior
change for callers — the in-memory state is rebuilt from redb on
startup, and writes go to both. Add a `DASH_PERSISTENCE_PATH` env var
that, if set, opens the redb file. Default is "unset" (in-memory only).
This is a pure-additive change. Tests: 5 new integration tests.

**PR 2 (1-2 days):** Make redb the default. The env var is still
honored for WAL-only mode, but the default is redb-backed. The
`checkpoint_to_disk` method is wired into the existing `CheckpointPolicy`.
Add a `/v1/admin/disk_status` endpoint to the retrieval service that
returns the current `DiskStatus`. Tests: 3 new integration tests +
update existing tests to use redb by default.

**PR 3 (1-2 days):** Polish. The `DiskStatus` enum is exposed as a metric
(`dash_storage_disk_status{status="available|unavailable|recovering"}`).
A background reaper checks redb file size growth and triggers an early
checkpoint if the file is growing faster than the checkpoint policy
allows. A "force recompact" admin endpoint rebuilds the redb file from
the in-memory state. Tests: 2 new integration tests.

**Future (not this iteration):** Sharding (split the redb file by
tenant_id prefix), replication (the redb file + the WAL tail are
exactly the state to ship to a replica).

## Acceptance criteria

The PR is done when:

1. All 371 existing tests still pass.
2. The new `disk_*` integration tests (8 total) pass.
3. `cargo clippy --workspace --all-targets` is clean.
4. A process killed mid-write recovers to a consistent state on
   restart (verified by a chaos test that injects `kill -9` at random
   points during a stress loop of 10k ingest+checkpoint cycles).
5. Cold-start time for a 1M-claim corpus with a 1k-record WAL tail is
   under 5 seconds (measured by a new benchmark in `tests/benchmarks`).
6. The `redb` file size for 1M claims + 2.5M evidence + 5M edges is
   under 4 GB (measured by the same benchmark).
7. The `DASH_PERSISTENCE_PATH` env var, when set to an unwritable
   path, produces a clear error and falls back to WAL-only mode (no
   crash).
8. The operator runbook (`docs/operations/redb-persistence.md`)
   documents the backup, restore, corruption-recovery, and
   redb-to-wal-only-fallback procedures with runnable examples.

## Out-of-scope but worth noting

- **Encryption at rest** — `redb` does not natively encrypt the file.
  If the deployment requires encryption, the file should live on an
  encrypted block device (LUKS, AWS EBS encryption, etc.). A
  follow-up could add an opt-in `redb::Builder::encryption` config
  if `redb` adds native support; today the workaround is filesystem-
  level.
- **Online schema migration** — if the `redb` table layout changes
  in a future release, we need a migration path. The recommended
  approach is to version the redb file (embed a `SCHEMA_VERSION`
  constant in the file's first row) and refuse to open files with
  unknown versions, falling back to WAL-only mode. A migration
  command (`dash-admin migrate-disk`) can rebuild the file from the
  WAL. This is deferred to the schema-migration design doc.
- **Multi-region replication** — the redb file + WAL tail is the
  natural replication unit. The replication protocol is documented
  in `phase-07-routing-placement-and-failover.md` and is out of scope
  for this design.

## What this design does NOT do

- It does not change the public `InMemoryStore` API. The
  `ingest_bundle*`, `retrieve*`, `claim_by_id`, `tenant_ids`, etc.
  methods all keep their signatures and semantics.
- It does not introduce a new query language. The retrieval API is
  unchanged.
- It does not shard the store. Single-node only. Sharding comes
  after this, in a separate design doc.
- It does not change the on-disk WAL format. The TSV WAL records
  are still what hits disk first; the redb file is the materialized
  post-WAL state. Operators who back up the WAL directory still get
  a valid recovery point even if they don't back up the redb file.

## Estimated effort

- PR 1: 1-2 focused days
- PR 2: 1-2 focused days
- PR 3: 1-2 focused days
- Operator runbook + chaos testing: 0.5 day

Total: 5-7 focused days for a single contributor.

## Open questions for the implementer

1. **Should we persist `evidence_by_claim` as one row per claim
   (replaced atomically on update) or one row per evidence item?**
   Recommendation: one row per claim (simpler, evidence volume is small).
2. **Should the redb file live next to the WAL or in a separate
   directory?** Recommendation: separate directory (default
   `/var/lib/dash/state/dash.db` for the redb file,
   `/var/lib/dash/wal/` for the WALs). This lets the operator back up
   each independently.
3. **What's the right redb file page size?** Default (4 KB) is
   probably fine; benchmark in PR 1 to confirm.
4. **Should the disk store be opened with `OpenOptions::read_only`
   in WAL-only mode?** No — if it's not opened, it can't be
   re-attached without restart. The right behavior is to log the
   failure, set `DiskStatus::Unavailable`, and continue.

These are easy to answer during PR 1.
