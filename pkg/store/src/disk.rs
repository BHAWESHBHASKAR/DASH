//! `redb`-backed on-disk materialization of the DASH in-memory store.
//!
//! This module adds an opt-in durability layer to the in-memory store.
//! When a `DiskBackedStore` is attached, every successful write to the
//! in-memory maps is mirrored to a `redb::Database` *before* the
//! in-memory state changes. If the on-disk write fails, the in-memory
//! mutation is aborted and the error is returned to the caller — this
//! preserves the existing "all or nothing" ingest semantic.
//!
//! The on-disk layout is purely a materialized view of the in-memory
//! state. The WAL remains the source of truth for cold-start replay
//! (rebuilding the in-memory state). The redb file exists to make
//! that rebuild fast: open the snapshot, bulk-load into the in-memory
//! store, then replay only the WAL tail.
//!
//! All methods return `Result<_, String>` (not `Result<_, StoreError>`)
//! so that the disk module has zero coupling to the in-memory store's
//! error type. The in-memory store maps the disk's `String` errors
//! into its own `StoreError::Io` variant at the call site.

use std::path::Path;

use redb::{Database, ReadableTable, TableDefinition, TableError};
use schema::{Claim, ClaimEdge, Evidence};

use crate::{BatchCommitMetadata, InMemoryStore, StoreIndexStats};

const TABLE_CLAIMS: TableDefinition<&str, &[u8]> = TableDefinition::new("dash_claims");
const TABLE_EVIDENCE: TableDefinition<&str, &[u8]> = TableDefinition::new("dash_evidence");
const TABLE_EDGES: TableDefinition<&str, &[u8]> = TableDefinition::new("dash_edges");
const TABLE_CLAIM_VECTORS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("dash_claim_vectors");
const TABLE_TENANT_DIMS: TableDefinition<&str, u64> = TableDefinition::new("dash_tenant_dims");
const TABLE_TENANT_CLAIMS_SET: TableDefinition<(&str, &str), ()> =
    TableDefinition::new("dash_tenant_claims_set");
const TABLE_BATCH_COMMITS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("dash_batch_commits");
const TABLE_STATS: TableDefinition<&str, &[u8]> = TableDefinition::new("dash_stats");
const TABLE_HWM: TableDefinition<&str, u64> = TableDefinition::new("dash_hwm");

const HWM_KEY: &str = "hwm";
const STATS_KEY: &str = "stats";

/// Runtime status of the disk-backed store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DiskStatus {
    /// Disk is open, healthy, and mirroring every write.
    Available,
    /// Disk could not be opened or a write failed; store is in
    /// WAL-only mode.
    Unavailable { reason: String },
    /// Disk is open but a cold-start replay is in progress. Read-side
    /// queries are served from the in-memory map (which is the same
    /// as `Available` from the caller's perspective).
    Recovering,
}

impl Default for DiskStatus {
    fn default() -> Self {
        Self::Unavailable {
            reason: "no disk attached".to_string(),
        }
    }
}

fn err<E: std::fmt::Display>(ctx: &str, e: E) -> String {
    format!("redb {ctx}: {e}")
}

fn map_bincode_err(ctx: &str, e: bincode::Error) -> String {
    format!("bincode {ctx}: {e}")
}

fn read_bytes<V: serde::de::DeserializeOwned>(bytes: Vec<u8>, ctx: &str) -> Result<V, String> {
    bincode::deserialize(&bytes).map_err(|e| map_bincode_err(ctx, e))
}

/// `redb`-backed persistence for the in-memory store.
///
/// Holds a `redb::Database` and exposes typed read/write methods for
/// every logical table the store needs. All write paths are
/// transaction-per-call; the caller (the in-memory store) opens a
/// write transaction, performs its mutations, and commits before
/// touching the in-memory state.
pub struct DiskBackedStore {
    db: Database,
}

impl DiskBackedStore {
    /// Open (or create) a `redb` database at `path`.
    pub fn new(path: impl AsRef<Path>) -> Result<Self, String> {
        let db = Database::create(path.as_ref()).map_err(|e| err("create", e))?;
        Ok(Self { db })
    }

    /// Report the runtime disk status. Constructed stores are always
    /// `Available`; the `Unavailable` variant is only produced by the
    /// in-memory store wrapper when the open failed.
    pub fn status(&self) -> &DiskStatus {
        const AVAILABLE: DiskStatus = DiskStatus::Available;
        &AVAILABLE
    }

    /// High-water mark of the WAL snapshot that has been materialized
    /// to disk. The HWM is updated by `set_high_water_mark` and is
    /// `0` for a freshly-created disk.
    pub fn high_water_mark(&self) -> Result<u64, String> {
        let txn = self.db.begin_read().map_err(|e| err("begin_read", e))?;
        let table = match txn.open_table(TABLE_HWM) {
            Ok(table) => table,
            Err(TableError::TableDoesNotExist(_)) => return Ok(0),
            Err(e) => return Err(err("open hwm table", e)),
        };
        let value = match table.get(HWM_KEY) {
            Ok(Some(v)) => v.value(),
            Ok(None) => 0,
            Err(e) => return Err(err("read hwm", e)),
        };
        Ok(value)
    }

    /// Persist a new high-water mark. Replaces any prior value.
    pub fn set_high_water_mark(&self, hwm: u64) -> Result<(), String> {
        let txn = self.db.begin_write().map_err(|e| err("begin_write", e))?;
        {
            let mut table = txn
                .open_table(TABLE_HWM)
                .map_err(|e| err("open hwm table", e))?;
            table
                .insert(HWM_KEY, hwm)
                .map_err(|e| err("write hwm", e))?;
        }
        txn.commit().map_err(|e| err("commit hwm", e))?;
        Ok(())
    }

    /// Persist a claim (replaces any prior claim with the same
    /// `claim_id`).
    pub fn put_claim(&self, claim: &Claim) -> Result<(), String> {
        let bytes = bincode::serialize(claim).map_err(|e| map_bincode_err("serialize claim", e))?;
        let txn = self.db.begin_write().map_err(|e| err("begin_write", e))?;
        {
            let mut table = txn
                .open_table(TABLE_CLAIMS)
                .map_err(|e| err("open claims", e))?;
            table
                .insert(claim.claim_id.as_str(), bytes.as_slice())
                .map_err(|e| err("write claim", e))?;
        }
        txn.commit().map_err(|e| err("commit claim", e))?;
        Ok(())
    }

    /// Read a claim by id, or `None` if not present.
    pub fn get_claim(&self, id: &str) -> Result<Option<Claim>, String> {
        let txn = self.db.begin_read().map_err(|e| err("begin_read", e))?;
        let table = match txn.open_table(TABLE_CLAIMS) {
            Ok(table) => table,
            Err(TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(e) => return Err(err("open claims", e)),
        };
        match table.get(id) {
            Ok(Some(v)) => {
                let value = v.value().to_vec();
                let claim: Claim = read_bytes(value, "deserialize claim")?;
                Ok(Some(claim))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(err("read claim", e)),
        }
    }

    /// Persist the full evidence blob for a claim. Replaces any prior
    /// evidence list for the same `claim_id` atomically.
    pub fn put_evidence_blob(&self, claim_id: &str, evidence: &[Evidence]) -> Result<(), String> {
        let bytes =
            bincode::serialize(evidence).map_err(|e| map_bincode_err("serialize evidence", e))?;
        let txn = self.db.begin_write().map_err(|e| err("begin_write", e))?;
        {
            let mut table = txn
                .open_table(TABLE_EVIDENCE)
                .map_err(|e| err("open evidence", e))?;
            table
                .insert(claim_id, bytes.as_slice())
                .map_err(|e| err("write evidence", e))?;
        }
        txn.commit().map_err(|e| err("commit evidence", e))?;
        Ok(())
    }

    /// Read the full evidence blob for a claim, or `None` if no
    /// evidence has been recorded for that claim.
    pub fn get_evidence_blob(&self, claim_id: &str) -> Result<Option<Vec<Evidence>>, String> {
        let txn = self.db.begin_read().map_err(|e| err("begin_read", e))?;
        let table = match txn.open_table(TABLE_EVIDENCE) {
            Ok(table) => table,
            Err(TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(e) => return Err(err("open evidence", e)),
        };
        match table.get(claim_id) {
            Ok(Some(v)) => {
                let value = v.value().to_vec();
                let evidence: Vec<Evidence> = read_bytes(value, "deserialize evidence")?;
                Ok(Some(evidence))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(err("read evidence", e)),
        }
    }

    /// Persist the full edge blob for a source claim. Replaces any
    /// prior edge list for the same `from` atomically.
    pub fn put_edge_blob(&self, from: &str, edges: &[ClaimEdge]) -> Result<(), String> {
        let bytes = bincode::serialize(edges).map_err(|e| map_bincode_err("serialize edges", e))?;
        let txn = self.db.begin_write().map_err(|e| err("begin_write", e))?;
        {
            let mut table = txn
                .open_table(TABLE_EDGES)
                .map_err(|e| err("open edges", e))?;
            table
                .insert(from, bytes.as_slice())
                .map_err(|e| err("write edges", e))?;
        }
        txn.commit().map_err(|e| err("commit edges", e))?;
        Ok(())
    }

    /// Read the full edge blob for a source claim, or `None` if no
    /// edges have been recorded for that claim.
    pub fn get_edge_blob(&self, from: &str) -> Result<Option<Vec<ClaimEdge>>, String> {
        let txn = self.db.begin_read().map_err(|e| err("begin_read", e))?;
        let table = match txn.open_table(TABLE_EDGES) {
            Ok(table) => table,
            Err(TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(e) => return Err(err("open edges", e)),
        };
        match table.get(from) {
            Ok(Some(v)) => {
                let value = v.value().to_vec();
                let edges: Vec<ClaimEdge> = read_bytes(value, "deserialize edges")?;
                Ok(Some(edges))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(err("read edges", e)),
        }
    }

    /// Persist an embedding vector for a claim. Replaces any prior
    /// vector for the same `claim_id` atomically.
    pub fn put_vector(&self, claim_id: &str, vector: &[f32]) -> Result<(), String> {
        let bytes =
            bincode::serialize(vector).map_err(|e| map_bincode_err("serialize vector", e))?;
        let txn = self.db.begin_write().map_err(|e| err("begin_write", e))?;
        {
            let mut table = txn
                .open_table(TABLE_CLAIM_VECTORS)
                .map_err(|e| err("open claim_vectors", e))?;
            table
                .insert(claim_id, bytes.as_slice())
                .map_err(|e| err("write claim_vector", e))?;
        }
        txn.commit().map_err(|e| err("commit claim_vector", e))?;
        Ok(())
    }

    /// Read an embedding vector for a claim, or `None` if no vector
    /// has been recorded.
    pub fn get_vector(&self, claim_id: &str) -> Result<Option<Vec<f32>>, String> {
        let txn = self.db.begin_read().map_err(|e| err("begin_read", e))?;
        let table = match txn.open_table(TABLE_CLAIM_VECTORS) {
            Ok(table) => table,
            Err(TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(e) => return Err(err("open claim_vectors", e)),
        };
        match table.get(claim_id) {
            Ok(Some(v)) => {
                let value = v.value().to_vec();
                let vector: Vec<f32> = read_bytes(value, "deserialize vector")?;
                Ok(Some(vector))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(err("read claim_vector", e)),
        }
    }

    /// Persist batch-commit metadata. Replaces any prior entry with
    /// the same `commit_id` atomically.
    pub fn put_batch_commit(&self, commit: &BatchCommitMetadata) -> Result<(), String> {
        let bytes =
            bincode::serialize(commit).map_err(|e| map_bincode_err("serialize batch_commit", e))?;
        let txn = self.db.begin_write().map_err(|e| err("begin_write", e))?;
        {
            let mut table = txn
                .open_table(TABLE_BATCH_COMMITS)
                .map_err(|e| err("open batch_commits", e))?;
            table
                .insert(commit.commit_id.as_str(), bytes.as_slice())
                .map_err(|e| err("write batch_commit", e))?;
        }
        txn.commit().map_err(|e| err("commit batch_commit", e))?;
        Ok(())
    }

    /// Read batch-commit metadata by id, or `None` if not present.
    pub fn get_batch_commit(&self, id: &str) -> Result<Option<BatchCommitMetadata>, String> {
        let txn = self.db.begin_read().map_err(|e| err("begin_read", e))?;
        let table = match txn.open_table(TABLE_BATCH_COMMITS) {
            Ok(table) => table,
            Err(TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(e) => return Err(err("open batch_commits", e)),
        };
        match table.get(id) {
            Ok(Some(v)) => {
                let value = v.value().to_vec();
                let commit: BatchCommitMetadata = read_bytes(value, "deserialize batch_commit")?;
                Ok(Some(commit))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(err("read batch_commit", e)),
        }
    }

    /// Persist a tenant's vector dimension. Replaces any prior value.
    pub fn put_tenant_dim(&self, tenant: &str, dim: usize) -> Result<(), String> {
        let txn = self.db.begin_write().map_err(|e| err("begin_write", e))?;
        {
            let mut table = txn
                .open_table(TABLE_TENANT_DIMS)
                .map_err(|e| err("open tenant_dims", e))?;
            table
                .insert(tenant, dim as u64)
                .map_err(|e| err("write tenant_dim", e))?;
        }
        txn.commit().map_err(|e| err("commit tenant_dim", e))?;
        Ok(())
    }

    /// Read a tenant's vector dimension, or `None` if unknown.
    pub fn get_tenant_dim(&self, tenant: &str) -> Result<Option<usize>, String> {
        let txn = self.db.begin_read().map_err(|e| err("begin_read", e))?;
        let table = match txn.open_table(TABLE_TENANT_DIMS) {
            Ok(table) => table,
            Err(TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(e) => return Err(err("open tenant_dims", e)),
        };
        match table.get(tenant) {
            Ok(Some(v)) => Ok(Some(v.value() as usize)),
            Ok(None) => Ok(None),
            Err(e) => Err(err("read tenant_dim", e)),
        }
    }

    /// Add `claim` to the tenant's claim set. Idempotent: adding a
    /// claim that is already in the set is a no-op.
    pub fn add_claim_to_tenant(&self, tenant: &str, claim: &str) -> Result<(), String> {
        let txn = self.db.begin_write().map_err(|e| err("begin_write", e))?;
        {
            let mut table = txn
                .open_table(TABLE_TENANT_CLAIMS_SET)
                .map_err(|e| err("open tenant_claims_set", e))?;
            let key: (&str, &str) = (tenant, claim);
            table
                .insert(key, ())
                .map_err(|e| err("write tenant_claims_set", e))?;
        }
        txn.commit()
            .map_err(|e| err("commit tenant_claims_set", e))?;
        Ok(())
    }

    /// Returns `true` if `claim` is recorded in `tenant`'s claim set.
    pub fn claim_in_tenant(&self, tenant: &str, claim: &str) -> Result<bool, String> {
        let txn = self.db.begin_read().map_err(|e| err("begin_read", e))?;
        let table = match txn.open_table(TABLE_TENANT_CLAIMS_SET) {
            Ok(table) => table,
            Err(TableError::TableDoesNotExist(_)) => return Ok(false),
            Err(e) => return Err(err("open tenant_claims_set", e)),
        };
        let key: (&str, &str) = (tenant, claim);
        let present = table
            .get(key)
            .map_err(|e| err("read tenant_claims_set", e))?
            .is_some();
        Ok(present)
    }

    /// Invoke `f` for every claim id recorded in `tenant`'s claim set.
    pub fn for_each_claim_in_tenant(
        &self,
        tenant: &str,
        f: &mut dyn FnMut(&str),
    ) -> Result<(), String> {
        let txn = self.db.begin_read().map_err(|e| err("begin_read", e))?;
        let table = match txn.open_table(TABLE_TENANT_CLAIMS_SET) {
            Ok(table) => table,
            Err(TableError::TableDoesNotExist(_)) => return Ok(()),
            Err(e) => return Err(err("open tenant_claims_set", e)),
        };
        // Iterate the full table and filter on the read side:
        // redb's range bounds on composite keys do not support a
        // "prefix matches first element" range, so this is the
        // most portable approach for the small N of typical
        // tenant/claim sets.
        let iter = table.iter().map_err(|e| err("iter tenant_claims_set", e))?;
        for entry in iter {
            let entry = entry.map_err(|e| err("scan tenant_claims_set", e))?;
            let key = entry.0.value();
            let (key_tenant, key_claim) = key;
            if key_tenant == tenant {
                f(key_claim);
            }
        }
        Ok(())
    }

    /// Persist the index stats singleton.
    pub fn set_stats(&self, stats: &StoreIndexStats) -> Result<(), String> {
        let bytes = bincode::serialize(stats).map_err(|e| map_bincode_err("serialize stats", e))?;
        let txn = self.db.begin_write().map_err(|e| err("begin_write", e))?;
        {
            let mut table = txn
                .open_table(TABLE_STATS)
                .map_err(|e| err("open stats", e))?;
            table
                .insert(STATS_KEY, bytes.as_slice())
                .map_err(|e| err("write stats", e))?;
        }
        txn.commit().map_err(|e| err("commit stats", e))?;
        Ok(())
    }

    /// Read the index stats singleton, or a default `StoreIndexStats`
    /// if no stats have been persisted.
    pub fn get_stats(&self) -> Result<StoreIndexStats, String> {
        let txn = self.db.begin_read().map_err(|e| err("begin_read", e))?;
        let table = match txn.open_table(TABLE_STATS) {
            Ok(table) => table,
            Err(TableError::TableDoesNotExist(_)) => return Ok(StoreIndexStats::default()),
            Err(e) => return Err(err("open stats", e)),
        };
        match table.get(STATS_KEY) {
            Ok(Some(v)) => {
                let value = v.value().to_vec();
                let stats: StoreIndexStats = read_bytes(value, "deserialize stats")?;
                Ok(stats)
            }
            Ok(None) => Ok(StoreIndexStats::default()),
            Err(e) => Err(err("read stats", e)),
        }
    }

    /// Read every record from the redb file into `dest`, rebuilding
    /// the in-memory inverted/entity/embedding/temporal indices, the
    /// tenant→claim sets, the per-tenant ANN index, and the
    /// `claim_vectors` map. Returns the number of claims loaded.
    ///
    /// `dest` must be empty; this function does not clear it. The
    /// caller is expected to construct `dest` via
    /// `InMemoryStore::new_with_ann_tuning`.
    pub fn bulk_load_claims_into(&self, dest: &mut InMemoryStore) -> Result<usize, String> {
        // 1. Read every claim and apply it (this also updates the
        //    tenant→claim set, inverted index, entity index,
        //    embedding index, and temporal BTree via
        //    `add_claim_indexes`).
        let mut claims_loaded = 0usize;
        {
            let txn = self.db.begin_read().map_err(|e| err("begin_read", e))?;
            let table = match txn.open_table(TABLE_CLAIMS) {
                Ok(table) => table,
                Err(TableError::TableDoesNotExist(_)) => return Ok(0),
                Err(e) => return Err(err("open claims", e)),
            };
            let iter = table.iter().map_err(|e| err("iter claims", e))?;
            for entry in iter {
                let entry = entry.map_err(|e| err("scan claims", e))?;
                let value = entry.1.value().to_vec();
                let claim: Claim = bincode::deserialize(&value)
                    .map_err(|e| map_bincode_err("deserialize claim", e))?;
                dest.apply_claim_for_load(claim)
                    .map_err(|e| format!("apply_claim_for_load: {e:?}"))?;
                claims_loaded += 1;
            }
        }

        // 2. Read every evidence blob and apply it. Tables that
        //    don't exist (because no records of that kind have
        //    been written) are treated as empty.
        {
            let txn = self.db.begin_read().map_err(|e| err("begin_read", e))?;
            if let Ok(table) = txn.open_table(TABLE_EVIDENCE) {
                let iter = table.iter().map_err(|e| err("iter evidence", e))?;
                for entry in iter {
                    let entry = entry.map_err(|e| err("scan evidence", e))?;
                    let key = entry.0.value().to_string();
                    let value = entry.1.value().to_vec();
                    let evidence: Vec<Evidence> = bincode::deserialize(&value)
                        .map_err(|e| map_bincode_err("deserialize evidence", e))?;
                    dest.apply_evidence_blob_for_load(&key, &evidence)
                        .map_err(|e| format!("apply_evidence_blob_for_load: {e:?}"))?;
                }
            }
        }

        // 3. Read every edge blob and apply it.
        {
            let txn = self.db.begin_read().map_err(|e| err("begin_read", e))?;
            if let Ok(table) = txn.open_table(TABLE_EDGES) {
                let iter = table.iter().map_err(|e| err("iter edges", e))?;
                for entry in iter {
                    let entry = entry.map_err(|e| err("scan edges", e))?;
                    let key = entry.0.value().to_string();
                    let value = entry.1.value().to_vec();
                    let edges: Vec<ClaimEdge> = bincode::deserialize(&value)
                        .map_err(|e| map_bincode_err("deserialize edges", e))?;
                    dest.apply_edge_blob_for_load(&key, &edges)
                        .map_err(|e| format!("apply_edge_blob_for_load: {e:?}"))?;
                }
            }
        }

        // 4. Read every vector blob and apply it. The apply method
        //    inserts into the ANN index too.
        {
            let txn = self.db.begin_read().map_err(|e| err("begin_read", e))?;
            match txn.open_table(TABLE_CLAIM_VECTORS) {
                Ok(table) => {
                    let iter = table.iter().map_err(|e| err("iter claim_vectors", e))?;
                    for entry in iter {
                        let entry = entry.map_err(|e| err("scan claim_vectors", e))?;
                        let key = entry.0.value().to_string();
                        let value = entry.1.value().to_vec();
                        let vector: Vec<f32> = bincode::deserialize(&value)
                            .map_err(|e| map_bincode_err("deserialize vector", e))?;
                        dest.apply_claim_vector_blob_for_load(&key, vector)
                            .map_err(|e| format!("apply_claim_vector_blob_for_load: {e:?}"))?;
                    }
                }
                Err(TableError::TableDoesNotExist(_)) => {
                    // No vectors have been written yet — that's fine.
                }
                Err(e) => return Err(err("open claim_vectors", e)),
            }
        }

        // 5. Read every batch-commit record and apply it.
        {
            let txn = self.db.begin_read().map_err(|e| err("begin_read", e))?;
            if let Ok(table) = txn.open_table(TABLE_BATCH_COMMITS) {
                let iter = table.iter().map_err(|e| err("iter batch_commits", e))?;
                for entry in iter {
                    let entry = entry.map_err(|e| err("scan batch_commits", e))?;
                    let value = entry.1.value().to_vec();
                    let commit: BatchCommitMetadata = bincode::deserialize(&value)
                        .map_err(|e| map_bincode_err("deserialize batch_commit", e))?;
                    dest.apply_batch_commit_for_load(&commit)
                        .map_err(|e| format!("apply_batch_commit_for_load: {e:?}"))?;
                }
            }
        }

        // 6. Read every tenant dimension.
        {
            let txn = self.db.begin_read().map_err(|e| err("begin_read", e))?;
            if let Ok(table) = txn.open_table(TABLE_TENANT_DIMS) {
                let iter = table.iter().map_err(|e| err("iter tenant_dims", e))?;
                for entry in iter {
                    let entry = entry.map_err(|e| err("scan tenant_dims", e))?;
                    let key = entry.0.value().to_string();
                    let dim = entry.1.value() as usize;
                    dest.apply_tenant_dim_for_load(&key, dim);
                }
            }
        }

        // 7. Read every tenant-claim set membership and record it.
        {
            let txn = self.db.begin_read().map_err(|e| err("begin_read", e))?;
            if let Ok(table) = txn.open_table(TABLE_TENANT_CLAIMS_SET) {
                let iter = table.iter().map_err(|e| err("iter tenant_claims_set", e))?;
                for entry in iter {
                    let entry = entry.map_err(|e| err("scan tenant_claims_set", e))?;
                    let key = entry.0.value();
                    let (tenant, claim) = key;
                    dest.apply_tenant_claim_set_for_load(tenant, claim);
                }
            }
        }

        Ok(claims_loaded)
    }

    /// Take every record currently in the in-memory `store` and write
    /// it to the redb file. This is the "checkpoint" path: it is
    /// called from `InMemoryStore::checkpoint_to_disk` (added in
    /// PR 2) to materialize the current state. In PR 1 it is exposed
    /// on `DiskBackedStore` for testability and to support the
    /// future `InMemoryStore::checkpoint_to_disk` call site.
    pub fn checkpoint_from(&self, store: &InMemoryStore) -> Result<(), String> {
        // One big write transaction keeps the checkpoint atomic.
        let txn = self.db.begin_write().map_err(|e| err("begin_write", e))?;
        {
            let mut claims_table = txn
                .open_table(TABLE_CLAIMS)
                .map_err(|e| err("open claims", e))?;
            for claim in store.claims_iter() {
                let bytes =
                    bincode::serialize(claim).map_err(|e| map_bincode_err("serialize claim", e))?;
                claims_table
                    .insert(claim.claim_id.as_str(), bytes.as_slice())
                    .map_err(|e| err("write claim", e))?;
            }

            let mut evidence_table = txn
                .open_table(TABLE_EVIDENCE)
                .map_err(|e| err("open evidence", e))?;
            for (claim_id, evidence) in store.evidence_iter() {
                let bytes = bincode::serialize(evidence)
                    .map_err(|e| map_bincode_err("serialize evidence", e))?;
                evidence_table
                    .insert(claim_id, bytes.as_slice())
                    .map_err(|e| err("write evidence", e))?;
            }

            let mut edges_table = txn
                .open_table(TABLE_EDGES)
                .map_err(|e| err("open edges", e))?;
            for (from, edges) in store.edges_iter() {
                let bytes =
                    bincode::serialize(edges).map_err(|e| map_bincode_err("serialize edges", e))?;
                edges_table
                    .insert(from, bytes.as_slice())
                    .map_err(|e| err("write edges", e))?;
            }

            let mut vectors_table = txn
                .open_table(TABLE_CLAIM_VECTORS)
                .map_err(|e| err("open claim_vectors", e))?;
            for (claim_id, vector) in store.claim_vectors_iter() {
                let bytes = bincode::serialize(vector)
                    .map_err(|e| map_bincode_err("serialize vector", e))?;
                vectors_table
                    .insert(claim_id, bytes.as_slice())
                    .map_err(|e| err("write claim_vector", e))?;
            }

            let mut batch_commits_table = txn
                .open_table(TABLE_BATCH_COMMITS)
                .map_err(|e| err("open batch_commits", e))?;
            for commit in store.batch_commits_iter() {
                let bytes = bincode::serialize(commit)
                    .map_err(|e| map_bincode_err("serialize batch_commit", e))?;
                batch_commits_table
                    .insert(commit.commit_id.as_str(), bytes.as_slice())
                    .map_err(|e| err("write batch_commit", e))?;
            }

            let mut tenant_dims_table = txn
                .open_table(TABLE_TENANT_DIMS)
                .map_err(|e| err("open tenant_dims", e))?;
            for (tenant, dim) in store.tenant_dims_iter() {
                tenant_dims_table
                    .insert(tenant, *dim as u64)
                    .map_err(|e| err("write tenant_dim", e))?;
            }

            let mut tenant_claims_set_table = txn
                .open_table(TABLE_TENANT_CLAIMS_SET)
                .map_err(|e| err("open tenant_claims_set", e))?;
            for (tenant, claim) in store.tenant_claim_set_iter() {
                let key: (&str, &str) = (tenant.as_str(), claim.as_str());
                tenant_claims_set_table
                    .insert(key, ())
                    .map_err(|e| err("write tenant_claims_set", e))?;
            }
        }
        txn.commit().map_err(|e| err("commit checkpoint", e))?;
        Ok(())
    }

    /// Force any pending redb writes to disk. This is a no-op for
    /// the default immediate-durability mode (redb syncs on every
    /// commit), but is preserved for API stability in case
    /// `redb::Durability::Eventual` is wired up later.
    pub fn commit_pending_writes(&self) -> Result<(), String> {
        // redb commits are durable per `WriteTransaction::commit`, so
        // there is no separate "flush" step in immediate mode. We
        // open and immediately commit an empty transaction as a
        // flush barrier — this gives the caller a single point to
        // hook durability changes in the future.
        let txn = self.db.begin_write().map_err(|e| err("begin_write", e))?;
        txn.commit().map_err(|e| err("commit flush", e))?;
        Ok(())
    }
}
