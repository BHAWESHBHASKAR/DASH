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

use redb::{Database, ReadableTable, TableDefinition, TableError, TableHandle};
use schema::{Claim, ClaimEdge, Evidence};

use crate::{BatchCommitMetadata, InMemoryStore, StoreIndexStats, value_codec};

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

fn map_codec_err(ctx: &str, e: value_codec::CodecError) -> String {
    format!("value codec {ctx}: {e}")
}

/// Collapse duplicate evidence ids: the last occurrence wins, positioned
/// where the id first appeared.
fn dedupe_evidence(evidence: &[Evidence]) -> Vec<Evidence> {
    let mut out: Vec<Evidence> = Vec::with_capacity(evidence.len());
    let mut index: std::collections::HashMap<&str, usize> = std::collections::HashMap::new();
    for item in evidence {
        match index.get(item.evidence_id.as_str()) {
            Some(&pos) => out[pos] = item.clone(),
            None => {
                index.insert(item.evidence_id.as_str(), out.len());
                out.push(item.clone());
            }
        }
    }
    out
}

/// Collapse duplicate edges keyed by `(from, to, relation)`: the last
/// occurrence wins, positioned where the key first appeared.
fn dedupe_edges(edges: &[ClaimEdge]) -> Vec<ClaimEdge> {
    let mut out: Vec<ClaimEdge> = Vec::with_capacity(edges.len());
    let mut index: std::collections::HashMap<(&str, &str, String), usize> =
        std::collections::HashMap::new();
    for edge in edges {
        let key = (
            edge.from_claim_id.as_str(),
            edge.to_claim_id.as_str(),
            format!("{:?}", edge.relation),
        );
        match index.get(&key) {
            Some(&pos) => out[pos] = edge.clone(),
            None => {
                index.insert(key, out.len());
                out.push(edge.clone());
            }
        }
    }
    out
}


const TABLE_CRYPTO: TableDefinition<&str, &str> = TableDefinition::new("dash_crypto");
const CRYPTO_DEK_KEY: &str = "dek";
/// Value-codec style marker of an encrypted value: `DASH` + `e1` + `\0\xff`.
/// Releases without encryption refuse it (unknown `DASH....\xff` version).
const ENCRYPTED_VALUE_HEADER: [u8; 8] = *b"DASHe1\x00\xff";

/// Seals redb values (ADR 0005, section 3.4): the database's DEK is kept
/// wrapped in table `dash_crypto`; every value is sealed with AAD
/// `table \0 key`, so a value cannot be moved to another row.
pub(crate) struct ValueCrypt {
    cipher: Option<encryption::RecordCipher>,
    key_id: Option<String>,
    what: String,
}

impl ValueCrypt {
    fn open(
        db: &Database,
        keyring: Option<&encryption::Keyring>,
        what: &str,
    ) -> Result<Self, String> {
        let stored: Option<String> = {
            let txn = db.begin_read().map_err(|e| err("begin_read", e))?;
            match txn.open_table(TABLE_CRYPTO) {
                Ok(table) => table
                    .get(CRYPTO_DEK_KEY)
                    .map_err(|e| err("read data key", e))?
                    .map(|v| v.value().to_string()),
                Err(TableError::TableDoesNotExist(_)) => None,
                Err(e) => return Err(err("open dash_crypto", e)),
            }
        };
        let store_header = |header: &encryption::FileHeader| -> Result<(), String> {
            let txn = db.begin_write().map_err(|e| err("begin_write", e))?;
            {
                let mut table = txn
                    .open_table(TABLE_CRYPTO)
                    .map_err(|e| err("open dash_crypto", e))?;
                table
                    .insert(
                        CRYPTO_DEK_KEY,
                        encryption::render_header_line(header).as_str(),
                    )
                    .map_err(|e| err("write data key", e))?;
            }
            txn.commit().map_err(|e| err("commit data key", e))
        };
        let key = match (stored, keyring) {
            (None, None) => {
                return Ok(Self {
                    cipher: None,
                    key_id: None,
                    what: what.to_string(),
                });
            }
            (None, Some(keyring)) => {
                let key = keyring.new_file_key().map_err(|e| e.to_string())?;
                store_header(key.header())?;
                key
            }
            (Some(line), keyring) => {
                let header = encryption::parse_header_line(&line)
                    .map_err(|e| format!("{what}: {e}"))?;
                let mut key = encryption::open_file_key(keyring, &header, what)
                    .map_err(|e| e.to_string())?;
                if let Some(keyring) = keyring
                    && header.key_id != keyring.active_key_id()
                {
                    // Key rotation: rewrap the data key with the active KEK.
                    let rewrapped = keyring.rewrap(&header, what).map_err(|e| e.to_string())?;
                    store_header(&rewrapped)?;
                    key = keyring
                        .open_file_key(&rewrapped, what)
                        .map_err(|e| e.to_string())?;
                }
                key
            }
        };
        Ok(Self {
            key_id: Some(key.key_id().to_string()),
            cipher: Some(encryption::RecordCipher::new(
                &key,
                encryption::REDB_LABEL,
            )),
            what: what.to_string(),
        })
    }

    fn aad(table: &str, key: &str) -> Vec<u8> {
        let mut aad = Vec::with_capacity(table.len() + 1 + key.len());
        aad.extend_from_slice(table.as_bytes());
        aad.push(0);
        aad.extend_from_slice(key.as_bytes());
        aad
    }

    /// The stored bytes of `value` in row `key` of `table`.
    fn encode<T: serde::Serialize + ?Sized, K: AsRef<str> + ?Sized>(
        &self,
        table: &str,
        key: &K,
        value: &T,
        ctx: &str,
    ) -> Result<Vec<u8>, String> {
        let plain = value_codec::encode(value).map_err(|e| map_codec_err(ctx, e))?;
        let Some(cipher) = &self.cipher else {
            return Ok(plain);
        };
        let sealed = cipher
            .seal(&plain, &Self::aad(table, key.as_ref()))
            .map_err(|e| format!("{ctx}: {e}"))?;
        let mut out = Vec::with_capacity(ENCRYPTED_VALUE_HEADER.len() + sealed.len());
        out.extend_from_slice(&ENCRYPTED_VALUE_HEADER);
        out.extend_from_slice(&sealed);
        Ok(out)
    }

    /// Decodes a stored value (sealed or, from before encryption was
    /// enabled, plaintext).
    fn decode<T: serde::de::DeserializeOwned, K: AsRef<str> + ?Sized>(
        &self,
        table: &str,
        key: &K,
        bytes: &[u8],
        ctx: &str,
    ) -> Result<T, String> {
        let Some(sealed) = bytes.strip_prefix(&ENCRYPTED_VALUE_HEADER[..]) else {
            return value_codec::decode(bytes).map_err(|e| map_codec_err(ctx, e));
        };
        let Some(cipher) = &self.cipher else {
            return Err(format!(
                "{}: {ctx}: the value is encrypted but the database has no data key",
                self.what
            ));
        };
        let plain = cipher
            .open(sealed, &Self::aad(table, key.as_ref()))
            .map_err(|_| {
                format!(
                    "{}: {ctx}: row '{}' of {table}: authentication failed (wrong key, or the value was modified or damaged)",
                    self.what,
                    key.as_ref()
                )
            })?;
        value_codec::decode(&plain).map_err(|e| map_codec_err(ctx, e))
    }
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
    values: ValueCrypt,
}

impl DiskBackedStore {
    /// Open (or create) a `redb` database at `path`, with the keyring in
    /// effect (`encryption::current()`).
    pub fn new(path: impl AsRef<Path>) -> Result<Self, String> {
        Self::new_with_keyring(path, crate::crypt::current_keyring())
    }

    /// [`DiskBackedStore::new`] with an explicit keyring. With a keyring the
    /// values are sealed (keys stay plaintext); existing plaintext values
    /// stay readable and are sealed when rewritten. A database whose data
    /// key exists cannot be opened without a keyring (fail closed).
    pub fn new_with_keyring(
        path: impl AsRef<Path>,
        keyring: Option<std::sync::Arc<encryption::Keyring>>,
    ) -> Result<Self, String> {
        let db = Database::create(path.as_ref()).map_err(|e| err("create", e))?;
        let what = format!("redb mirror {}", path.as_ref().display());
        let values = ValueCrypt::open(&db, keyring.as_deref(), &what)?;
        Ok(Self { db, values })
    }

    /// `true` when values written by this handle are encrypted.
    pub fn encrypts_values(&self) -> bool {
        self.values.cipher.is_some()
    }

    /// KEK id that wraps this database's data key, if it has one.
    pub fn encryption_key_id(&self) -> Option<String> {
        self.values.key_id.clone()
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
    /// `claim_id`) and record its tenant membership in the SAME write
    /// transaction, so the claim row and the tenant set cannot diverge.
    pub fn put_claim(&self, claim: &Claim) -> Result<(), String> {
        let bytes = self.values.encode(TABLE_CLAIMS.name(), &claim.claim_id, claim, "serialize claim")?;
        let txn = self.db.begin_write().map_err(|e| err("begin_write", e))?;
        {
            let mut table = txn
                .open_table(TABLE_CLAIMS)
                .map_err(|e| err("open claims", e))?;
            table
                .insert(claim.claim_id.as_str(), bytes.as_slice())
                .map_err(|e| err("write claim", e))?;
            let mut set = txn
                .open_table(TABLE_TENANT_CLAIMS_SET)
                .map_err(|e| err("open tenant_claims_set", e))?;
            let key: (&str, &str) = (claim.tenant_id.as_str(), claim.claim_id.as_str());
            set.insert(key, ())
                .map_err(|e| err("write tenant_claims_set", e))?;
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
                let claim: Claim = self.values.decode(TABLE_CLAIMS.name(), id, &value, "deserialize claim")?;
                Ok(Some(claim))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(err("read claim", e)),
        }
    }

    /// Persist the full evidence blob for a claim. Replaces any prior
    /// evidence list for the same `claim_id` atomically.
    /// Duplicate `evidence_id`s inside the blob are collapsed (last wins).
    pub fn put_evidence_blob(&self, claim_id: &str, evidence: &[Evidence]) -> Result<(), String> {
        let evidence = dedupe_evidence(evidence);
        let bytes =
            self.values.encode(TABLE_EVIDENCE.name(), claim_id, &evidence, "serialize evidence")?;
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

    /// Insert or replace one evidence record, keyed by `evidence_id`, in a
    /// single read-modify-write transaction. Re-upserting the same id never
    /// duplicates.
    pub fn upsert_evidence(&self, evidence: &Evidence) -> Result<(), String> {
        let txn = self.db.begin_write().map_err(|e| err("begin_write", e))?;
        {
            let mut table = txn
                .open_table(TABLE_EVIDENCE)
                .map_err(|e| err("open evidence", e))?;
            let mut current: Vec<Evidence> = match table
                .get(evidence.claim_id.as_str())
                .map_err(|e| err("read evidence", e))?
            {
                Some(v) => self.values.decode(TABLE_EVIDENCE.name(), &evidence.claim_id, &v.value().to_vec(), "deserialize evidence")?,
                None => Vec::new(),
            };
            current.push(evidence.clone());
            let current = dedupe_evidence(&current);
            let bytes = self.values.encode(TABLE_EVIDENCE.name(), &evidence.claim_id, &current, "serialize evidence")?;
            table
                .insert(evidence.claim_id.as_str(), bytes.as_slice())
                .map_err(|e| err("write evidence", e))?;
        }
        txn.commit().map_err(|e| err("commit evidence", e))?;
        Ok(())
    }

    /// Insert or replace one edge, keyed by `(from, to, relation)`, in a
    /// single read-modify-write transaction.
    pub fn upsert_edge(&self, edge: &ClaimEdge) -> Result<(), String> {
        let txn = self.db.begin_write().map_err(|e| err("begin_write", e))?;
        {
            let mut table = txn
                .open_table(TABLE_EDGES)
                .map_err(|e| err("open edges", e))?;
            let mut current: Vec<ClaimEdge> = match table
                .get(edge.from_claim_id.as_str())
                .map_err(|e| err("read edges", e))?
            {
                Some(v) => self.values.decode(TABLE_EDGES.name(), &edge.from_claim_id, &v.value().to_vec(), "deserialize edges")?,
                None => Vec::new(),
            };
            current.push(edge.clone());
            let current = dedupe_edges(&current);
            let bytes =
                self.values.encode(TABLE_EDGES.name(), &edge.from_claim_id, &current, "serialize edges")?;
            table
                .insert(edge.from_claim_id.as_str(), bytes.as_slice())
                .map_err(|e| err("write edges", e))?;
        }
        txn.commit().map_err(|e| err("commit edges", e))?;
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
                let evidence: Vec<Evidence> = self.values.decode(TABLE_EVIDENCE.name(), claim_id, &value, "deserialize evidence")?;
                Ok(Some(evidence))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(err("read evidence", e)),
        }
    }

    /// Persist the full edge blob for a source claim. Replaces any
    /// prior edge list for the same `from` atomically.
    /// Duplicate `(from, to, relation)` edges inside the blob are
    /// collapsed (last wins).
    pub fn put_edge_blob(&self, from: &str, edges: &[ClaimEdge]) -> Result<(), String> {
        let edges = dedupe_edges(edges);
        let bytes = self.values.encode(TABLE_EDGES.name(), from, &edges, "serialize edges")?;
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
                let edges: Vec<ClaimEdge> = self.values.decode(TABLE_EDGES.name(), from, &value, "deserialize edges")?;
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
            self.values.encode(TABLE_CLAIM_VECTORS.name(), claim_id, vector, "serialize vector")?;
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
                let vector: Vec<f32> = self.values.decode(TABLE_CLAIM_VECTORS.name(), claim_id, &value, "deserialize vector")?;
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
            self.values.encode(TABLE_BATCH_COMMITS.name(), &commit.commit_id, commit, "serialize batch_commit")?;
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
                let commit: BatchCommitMetadata = self.values.decode(TABLE_BATCH_COMMITS.name(), id, &value, "deserialize batch_commit")?;
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
        let bytes = self.values.encode(TABLE_STATS.name(), STATS_KEY, stats, "serialize stats")?;
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
                let stats: StoreIndexStats = self.values.decode(TABLE_STATS.name(), STATS_KEY, &value, "deserialize stats")?;
                Ok(stats)
            }
            Ok(None) => Ok(StoreIndexStats::default()),
            Err(e) => Err(err("read stats", e)),
        }
    }

    /// Read every record from the redb file into `dest`, rebuilding
    /// the in-memory inverted/entity/embedding/temporal indices, the
    /// tenant→claim sets and the `claim_vectors` map (the
    /// per-tenant vector indexes are built after the WAL tail replay). Returns the number of claims loaded.
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
                let key = entry.0.value().to_string();
                let value = entry.1.value().to_vec();
                let claim: Claim = self.values.decode(TABLE_CLAIMS.name(), &key, &value, "deserialize claim")?;
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
                    let evidence: Vec<Evidence> = self.values.decode(TABLE_EVIDENCE.name(), &key, &value, "deserialize evidence")?;
                    let evidence = dedupe_evidence(&evidence);
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
                    let edges: Vec<ClaimEdge> = self.values.decode(TABLE_EDGES.name(), &key, &value, "deserialize edges")?;
                    let edges = dedupe_edges(&edges);
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
                        let vector: Vec<f32> = self.values.decode(TABLE_CLAIM_VECTORS.name(), &key, &value, "deserialize vector")?;
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
                    let key = entry.0.value().to_string();
                    let value = entry.1.value().to_vec();
                    let commit: BatchCommitMetadata = self.values.decode(TABLE_BATCH_COMMITS.name(), &key, &value, "deserialize batch_commit")?;
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

    /// Writes a run of staged mutations (in order) in ONE write transaction
    /// and one commit, instead of one durable commit per mutation. Used for
    /// a replicated frame, a batch and an atomic bundle: either all of the
    /// run reaches redb or none of it does.
    pub(crate) fn write_ops(&self, ops: &[crate::StagedDiskOp]) -> Result<(), String> {
        use crate::StagedDiskOp;
        if ops.is_empty() {
            return Ok(());
        }
        let txn = self.db.begin_write().map_err(|e| err("begin_write", e))?;
        for op in ops {
            match op {
                StagedDiskOp::Claim(claim) => {
                    let bytes = self.values.encode(TABLE_CLAIMS.name(), &claim.claim_id, claim, "serialize claim")?;
                    let mut table = txn
                        .open_table(TABLE_CLAIMS)
                        .map_err(|e| err("open claims", e))?;
                    table
                        .insert(claim.claim_id.as_str(), bytes.as_slice())
                        .map_err(|e| err("write claim", e))?;
                    let mut set = txn
                        .open_table(TABLE_TENANT_CLAIMS_SET)
                        .map_err(|e| err("open tenant_claims_set", e))?;
                    let key: (&str, &str) = (claim.tenant_id.as_str(), claim.claim_id.as_str());
                    set.insert(key, ())
                        .map_err(|e| err("write tenant_claims_set", e))?;
                }
                StagedDiskOp::Evidence(evidence) => {
                    let mut table = txn
                        .open_table(TABLE_EVIDENCE)
                        .map_err(|e| err("open evidence", e))?;
                    let mut current: Vec<Evidence> = match table
                        .get(evidence.claim_id.as_str())
                        .map_err(|e| err("read evidence", e))?
                    {
                        Some(v) => self.values.decode(TABLE_EVIDENCE.name(), &evidence.claim_id, &v.value().to_vec(), "deserialize evidence")?,
                        None => Vec::new(),
                    };
                    crate::upsert_evidence(&mut current, evidence.clone());
                    let bytes = self.values.encode(TABLE_EVIDENCE.name(), &evidence.claim_id, &dedupe_evidence(&current), "serialize evidence")?;
                    table
                        .insert(evidence.claim_id.as_str(), bytes.as_slice())
                        .map_err(|e| err("write evidence", e))?;
                }
                StagedDiskOp::Edge(edge) => {
                    let mut table = txn
                        .open_table(TABLE_EDGES)
                        .map_err(|e| err("open edges", e))?;
                    let mut current: Vec<ClaimEdge> = match table
                        .get(edge.from_claim_id.as_str())
                        .map_err(|e| err("read edges", e))?
                    {
                        Some(v) => self.values.decode(TABLE_EDGES.name(), &edge.from_claim_id, &v.value().to_vec(), "deserialize edges")?,
                        None => Vec::new(),
                    };
                    crate::upsert_edge(&mut current, edge.clone());
                    let bytes = self.values.encode(TABLE_EDGES.name(), &edge.from_claim_id, &dedupe_edges(&current), "serialize edges")?;
                    table
                        .insert(edge.from_claim_id.as_str(), bytes.as_slice())
                        .map_err(|e| err("write edges", e))?;
                }
                StagedDiskOp::Vector {
                    claim_id,
                    tenant_id,
                    vector,
                    new_dim,
                } => {
                    let bytes = self.values.encode(TABLE_CLAIM_VECTORS.name(), claim_id, vector, "serialize vector")?;
                    let mut table = txn
                        .open_table(TABLE_CLAIM_VECTORS)
                        .map_err(|e| err("open claim_vectors", e))?;
                    table
                        .insert(claim_id.as_str(), bytes.as_slice())
                        .map_err(|e| err("write claim_vector", e))?;
                    if let Some(dim) = new_dim {
                        let mut dims = txn
                            .open_table(TABLE_TENANT_DIMS)
                            .map_err(|e| err("open tenant_dims", e))?;
                        dims.insert(tenant_id.as_str(), *dim as u64)
                            .map_err(|e| err("write tenant_dim", e))?;
                    }
                }
                StagedDiskOp::BatchCommit(commit) => {
                    let bytes = self.values.encode(TABLE_BATCH_COMMITS.name(), &commit.commit_id, commit, "serialize batch_commit")?;
                    let mut table = txn
                        .open_table(TABLE_BATCH_COMMITS)
                        .map_err(|e| err("open batch_commits", e))?;
                    table
                        .insert(commit.commit_id.as_str(), bytes.as_slice())
                        .map_err(|e| err("write batch_commit", e))?;
                }
                StagedDiskOp::Delete(deletion) => apply_deletion_in(&txn, deletion, &self.values)?,
            }
        }
        txn.commit().map_err(|e| err("commit batch", e))?;
        Ok(())
    }

    /// Delete every row from every data table (replication resync replaces
    /// the whole state). Done in one transaction.
    pub fn clear_all(&self) -> Result<(), String> {
        let txn = self.db.begin_write().map_err(|e| err("begin_write", e))?;
        {
            macro_rules! clear {
                ($table:expr, $name:expr) => {
                    txn.delete_table($table)
                        .map_err(|e| err(concat!("delete ", $name), e))?;
                    txn.open_table($table)
                        .map_err(|e| err(concat!("recreate ", $name), e))?;
                };
            }
            clear!(TABLE_CLAIMS, "claims");
            clear!(TABLE_EVIDENCE, "evidence");
            clear!(TABLE_EDGES, "edges");
            clear!(TABLE_CLAIM_VECTORS, "claim_vectors");
            clear!(TABLE_TENANT_DIMS, "tenant_dims");
            clear!(TABLE_TENANT_CLAIMS_SET, "tenant_claims_set");
            clear!(TABLE_BATCH_COMMITS, "batch_commits");
        }
        txn.commit().map_err(|e| err("commit clear", e))?;
        Ok(())
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
                    self.values.encode(TABLE_CLAIMS.name(), &claim.claim_id, claim, "serialize claim")?;
                claims_table
                    .insert(claim.claim_id.as_str(), bytes.as_slice())
                    .map_err(|e| err("write claim", e))?;
            }

            let mut evidence_table = txn
                .open_table(TABLE_EVIDENCE)
                .map_err(|e| err("open evidence", e))?;
            for (claim_id, evidence) in store.evidence_iter() {
                let evidence = dedupe_evidence(evidence);
                let bytes = self.values.encode(TABLE_EVIDENCE.name(), claim_id, &evidence, "serialize evidence")?;
                evidence_table
                    .insert(claim_id, bytes.as_slice())
                    .map_err(|e| err("write evidence", e))?;
            }

            let mut edges_table = txn
                .open_table(TABLE_EDGES)
                .map_err(|e| err("open edges", e))?;
            for (from, edges) in store.edges_iter() {
                let edges = dedupe_edges(edges);
                let bytes =
                    self.values.encode(TABLE_EDGES.name(), from, &edges, "serialize edges")?;
                edges_table
                    .insert(from, bytes.as_slice())
                    .map_err(|e| err("write edges", e))?;
            }

            let mut vectors_table = txn
                .open_table(TABLE_CLAIM_VECTORS)
                .map_err(|e| err("open claim_vectors", e))?;
            for (claim_id, vector) in store.claim_vectors_iter() {
                let bytes = self.values.encode(TABLE_CLAIM_VECTORS.name(), claim_id, vector, "serialize vector")?;
                vectors_table
                    .insert(claim_id, bytes.as_slice())
                    .map_err(|e| err("write claim_vector", e))?;
            }

            let mut batch_commits_table = txn
                .open_table(TABLE_BATCH_COMMITS)
                .map_err(|e| err("open batch_commits", e))?;
            for commit in store.batch_commits_iter() {
                let bytes = self.values.encode(TABLE_BATCH_COMMITS.name(), &commit.commit_id, commit, "serialize batch_commit")?;
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

/// The row changes of one tombstone inside `txn` (see
/// [`DiskBackedStore::write_ops`]): blob rewrites first, then row removals,
/// so a rewritten blob of a claim the same tombstone removes is dropped with
/// it.
fn apply_deletion_in(
    txn: &redb::WriteTransaction,
    deletion: &crate::delete::DiskDeletion,
    values: &ValueCrypt,
) -> Result<(), String> {
    let mut evidence_table = txn
        .open_table(TABLE_EVIDENCE)
        .map_err(|e| err("open evidence", e))?;
    for (claim_id, evidence) in &deletion.evidence_blobs {
        if evidence.is_empty() {
            evidence_table
                .remove(claim_id.as_str())
                .map_err(|e| err("remove evidence", e))?;
        } else {
            let bytes = values.encode(TABLE_EVIDENCE.name(), claim_id, &dedupe_evidence(evidence), "serialize evidence")?;
            evidence_table
                .insert(claim_id.as_str(), bytes.as_slice())
                .map_err(|e| err("write evidence", e))?;
        }
    }
    let mut edges_table = txn
        .open_table(TABLE_EDGES)
        .map_err(|e| err("open edges", e))?;
    for (from, edges) in &deletion.edge_blobs {
        if edges.is_empty() {
            edges_table
                .remove(from.as_str())
                .map_err(|e| err("remove edges", e))?;
        } else {
            let bytes = values.encode(TABLE_EDGES.name(), from, &dedupe_edges(edges), "serialize edges")?;
            edges_table
                .insert(from.as_str(), bytes.as_slice())
                .map_err(|e| err("write edges", e))?;
        }
    }
    let mut claims_table = txn
        .open_table(TABLE_CLAIMS)
        .map_err(|e| err("open claims", e))?;
    let mut set_table = txn
        .open_table(TABLE_TENANT_CLAIMS_SET)
        .map_err(|e| err("open tenant_claims_set", e))?;
    let mut vectors_table = txn
        .open_table(TABLE_CLAIM_VECTORS)
        .map_err(|e| err("open claim_vectors", e))?;
    for (tenant_id, claim_id) in &deletion.claims {
        let claim_id = claim_id.as_str();
        claims_table
            .remove(claim_id)
            .map_err(|e| err("remove claim", e))?;
        let key: (&str, &str) = (tenant_id.as_str(), claim_id);
        set_table
            .remove(key)
            .map_err(|e| err("remove tenant_claims_set", e))?;
        evidence_table
            .remove(claim_id)
            .map_err(|e| err("remove evidence", e))?;
        edges_table
            .remove(claim_id)
            .map_err(|e| err("remove edges", e))?;
        vectors_table
            .remove(claim_id)
            .map_err(|e| err("remove claim_vector", e))?;
    }
    let mut dims_table = txn
        .open_table(TABLE_TENANT_DIMS)
        .map_err(|e| err("open tenant_dims", e))?;
    for tenant_id in &deletion.tenant_dims {
        dims_table
            .remove(tenant_id.as_str())
            .map_err(|e| err("remove tenant_dim", e))?;
    }
    let mut commits_table = txn
        .open_table(TABLE_BATCH_COMMITS)
        .map_err(|e| err("open batch_commits", e))?;
    for commit_id in &deletion.batch_commits {
        commits_table
            .remove(commit_id.as_str())
            .map_err(|e| err("remove batch_commit", e))?;
    }
    Ok(())
}
