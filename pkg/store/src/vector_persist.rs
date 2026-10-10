//! Persisted per-tenant vector indexes (ADR 0003, "Persisted vector index").
//!
//! Without this, every start rebuilds every tenant's HNSW from the replayed
//! vectors, which dominates cold start (about 24 s at 100k vectors). The
//! service instead saves its indexes to one file next to the WAL and, on the
//! next start, loads that file, applies only the vector records written after
//! it was saved, verifies the result against the replayed vectors and only
//! then serves it. Anything that does not check out is discarded with a
//! warning and the indexes are rebuilt from the WAL, the source of truth.
//!
//! # File format (version [`VECTOR_INDEX_FORMAT_VERSION`])
//!
//! ```text
//! "DASHVIDX"              8 bytes magic
//! format version          u32 little endian
//! manifest length         u64 little endian
//! manifest                JSON (see `Manifest`)
//! header digest           SHA-256 of everything above (32 bytes)
//! tenant sections         one per manifest tenant, in manifest order;
//!                         each is `TenantVectorIndex::encode` output and the
//!                         manifest carries its length and SHA-256
//! ```
//!
//! The manifest records the format version, the tuning the indexes were
//! built with, the WAL position they reflect (lineage generation and record
//! count), the vector count and, per tenant, the dimension, backend, vector
//! count, section length and checksum.
//!
//! # Load rules
//!
//! The persisted indexes are used only when all of these hold; otherwise the
//! indexes are rebuilt and the reason is logged:
//!
//! 1. magic, format version, header digest and every section checksum match;
//! 2. the tuning equals the configured tuning;
//! 3. the saved WAL generation equals the current one and the WAL still
//!    holds at least the saved number of records;
//! 4. every tenant's dimension equals the tenant's stored dimension and each
//!    section decodes to a self-consistent index;
//! 5. after catch-up (re-applying the current stored vector of every claim
//!    named by a vector record after the saved position), every index holds
//!    exactly the indexable vectors of its tenant, each with the fingerprint
//!    of the stored vector.
//!
//! Rule 5 makes the load safe even if the saved position were wrong: a stale
//! entry or a missing one fails the comparison.
//!
//! # Saving
//!
//! [`InMemoryStore::vector_index_snapshot`] takes a cheap copy of the indexes
//! under the caller's lock; [`VectorIndexSnapshot::save`] then serialises it
//! without any lock and replaces the file atomically (write a temporary file,
//! fsync it, rename it over the old file, fsync the directory). A crash at
//! any point leaves either the old file or the new one.

use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions, create_dir_all, remove_file};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::vector_index::{TenantVectorIndex, is_indexable, vector_fingerprint};
use crate::wal::{VectorCatchUp, rename_file, sync_file, sync_parent_dir};
use crate::{AnnTuningConfig, FileWal, InMemoryStore, StoreError, WalPosition};

/// Version of the persisted index file layout. Bump it whenever the layout
/// or the meaning of a field changes; files of another version are rebuilt.
pub const VECTOR_INDEX_FORMAT_VERSION: u32 = 1;

const MAGIC: &[u8; 8] = b"DASHVIDX";
const PREFIX_LEN: usize = 8 + 4 + 8;
const DIGEST_LEN: usize = 32;
/// Upper bound on the manifest size accepted when reading a header.
const MANIFEST_MAX: u64 = 64 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct TuningManifest {
    connectivity: usize,
    expansion_add: usize,
    expansion_search: usize,
    flat_threshold: usize,
    rerank: usize,
}

impl From<&AnnTuningConfig> for TuningManifest {
    fn from(t: &AnnTuningConfig) -> Self {
        Self {
            connectivity: t.connectivity,
            expansion_add: t.expansion_add,
            expansion_search: t.expansion_search,
            flat_threshold: t.flat_threshold,
            rerank: t.rerank,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct TenantManifest {
    tenant_id: String,
    dimension: usize,
    backend: String,
    vectors: usize,
    bytes: u64,
    sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Manifest {
    format_version: u32,
    created_unix_ms: u64,
    tuning: TuningManifest,
    wal: WalPosition,
    vector_count: usize,
    tenants: Vec<TenantManifest>,
}

/// How [`InMemoryStore::load_from_wal_with_vector_index`] obtained the
/// vector indexes.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum VectorIndexRestore {
    /// No persisted index was configured; the indexes were built.
    #[default]
    NotConfigured,
    /// No persisted index file existed yet; the indexes were built.
    Missing,
    /// The persisted index was loaded and caught up with the WAL.
    Loaded {
        tenants: usize,
        vectors: usize,
        /// Claims (and erased tenants) re-applied from the vector records and
        /// tombstones after the saved position.
        caught_up: usize,
        saved_position: WalPosition,
    },
    /// The persisted index was discarded for `reason` and the indexes were
    /// built from the replayed vectors.
    Rebuilt { reason: String },
}

impl VectorIndexRestore {
    pub fn is_loaded(&self) -> bool {
        matches!(self, Self::Loaded { .. })
    }

    /// One-line description for startup logs.
    pub fn describe(&self) -> String {
        match self {
            Self::NotConfigured => "not configured (indexes built from the WAL)".to_string(),
            Self::Missing => "no saved index yet (indexes built from the WAL)".to_string(),
            Self::Loaded {
                tenants,
                vectors,
                caught_up,
                saved_position,
            } => format!(
                "loaded {vectors} vectors in {tenants} tenant(s) saved at WAL generation {:016x} record {}, caught up {caught_up} claim(s)",
                saved_position.generation, saved_position.records
            ),
            Self::Rebuilt { reason } => {
                format!("discarded ({reason}); indexes rebuilt from the WAL")
            }
        }
    }
}

/// Outcome of one successful save.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorIndexSaveStats {
    pub position: WalPosition,
    pub tenants: usize,
    pub vectors: usize,
    pub bytes: u64,
    pub elapsed: Duration,
}

/// A point-in-time copy of every tenant's vector index; see
/// [`InMemoryStore::vector_index_snapshot`].
#[derive(Clone)]
pub struct VectorIndexSnapshot {
    position: WalPosition,
    tuning: AnnTuningConfig,
    tenants: Vec<(String, TenantVectorIndex)>,
}

impl VectorIndexSnapshot {
    pub(crate) fn new(
        position: WalPosition,
        tuning: AnnTuningConfig,
        tenants: Vec<(String, TenantVectorIndex)>,
    ) -> Self {
        Self {
            position,
            tuning,
            tenants,
        }
    }

    /// The WAL position the snapshot reflects.
    pub fn position(&self) -> WalPosition {
        self.position
    }

    pub fn vector_count(&self) -> usize {
        self.tenants.iter().map(|(_, index)| index.len()).sum()
    }

    /// Write the snapshot to `path`, atomically replacing any previous file.
    /// Takes no lock on the store; safe to run on a background thread.
    pub fn save(&self, path: &Path) -> Result<VectorIndexSaveStats, StoreError> {
        let started = Instant::now();
        let mut sections = Vec::with_capacity(self.tenants.len());
        let mut tenants = Vec::with_capacity(self.tenants.len());
        for (tenant_id, index) in &self.tenants {
            let bytes = index
                .encode()
                .map_err(|e| StoreError::Io(format!("encode vector index '{tenant_id}': {e}")))?;
            tenants.push(TenantManifest {
                tenant_id: tenant_id.clone(),
                dimension: index.dimensions(),
                backend: if index.is_hnsw() { "hnsw" } else { "flat" }.to_string(),
                vectors: index.len(),
                bytes: bytes.len() as u64,
                sha256: hex::encode(Sha256::digest(&bytes)),
            });
            sections.push(bytes);
        }
        let manifest = Manifest {
            format_version: VECTOR_INDEX_FORMAT_VERSION,
            created_unix_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
            tuning: TuningManifest::from(&self.tuning),
            wal: self.position,
            vector_count: self.vector_count(),
            tenants,
        };
        let manifest_json = serde_json::to_vec(&manifest)
            .map_err(|e| StoreError::Io(format!("encode vector index manifest: {e}")))?;
        let mut header = Vec::with_capacity(PREFIX_LEN + manifest_json.len() + DIGEST_LEN);
        header.extend_from_slice(MAGIC);
        header.extend_from_slice(&VECTOR_INDEX_FORMAT_VERSION.to_le_bytes());
        header.extend_from_slice(&(manifest_json.len() as u64).to_le_bytes());
        header.extend_from_slice(&manifest_json);
        let digest = Sha256::digest(&header);
        header.extend_from_slice(&digest);

        let bytes = header.len() as u64 + sections.iter().map(|s| s.len() as u64).sum::<u64>();
        write_atomically(path, |file| {
            file.write_all(&header)?;
            for section in &sections {
                file.write_all(section)?;
            }
            Ok(())
        })?;
        Ok(VectorIndexSaveStats {
            position: self.position,
            tenants: self.tenants.len(),
            vectors: manifest.vector_count,
            bytes,
            elapsed: started.elapsed(),
        })
    }
}

fn temp_path_for(path: &Path) -> PathBuf {
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".tmp");
    PathBuf::from(tmp)
}

/// Write `path` through a temporary file: write, fsync, rename, fsync the
/// directory. The temporary file is removed when writing it fails.
fn write_atomically(
    path: &Path,
    write: impl FnOnce(&mut File) -> std::io::Result<()>,
) -> Result<(), StoreError> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        create_dir_all(parent)?;
    }
    let tmp = temp_path_for(path);
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&tmp)?;
    let written = (|| -> std::io::Result<()> {
        write(&mut file)?;
        failpoint!("vindex.tmp_written");
        sync_file(&file)?;
        failpoint!("vindex.fsynced");
        Ok(())
    })();
    drop(file);
    if let Err(err) = written {
        let _ = remove_file(&tmp);
        return Err(err.into());
    }
    rename_file(&tmp, path)?;
    failpoint!("vindex.renamed");
    sync_parent_dir(path)?;
    Ok(())
}

fn read_u32(bytes: &[u8], at: usize) -> u32 {
    let mut word = [0u8; 4];
    word.copy_from_slice(&bytes[at..at + 4]);
    u32::from_le_bytes(word)
}

fn read_u64(bytes: &[u8], at: usize) -> u64 {
    let mut word = [0u8; 8];
    word.copy_from_slice(&bytes[at..at + 8]);
    u64::from_le_bytes(word)
}

/// Checks the fixed prefix and returns the manifest length.
fn check_prefix(prefix: &[u8]) -> Result<usize, String> {
    if prefix.len() < PREFIX_LEN || &prefix[..8] != MAGIC {
        return Err("not a DASH vector index file".to_string());
    }
    let version = read_u32(prefix, 8);
    if version != VECTOR_INDEX_FORMAT_VERSION {
        return Err(format!(
            "format version {version}, this build reads {VECTOR_INDEX_FORMAT_VERSION}"
        ));
    }
    let len = read_u64(prefix, 12);
    if len > MANIFEST_MAX {
        return Err("manifest length out of range".to_string());
    }
    Ok(len as usize)
}

/// Verifies the header digest and parses the manifest. `header` holds the
/// prefix, the manifest and the digest.
fn parse_header(header: &[u8], manifest_len: usize) -> Result<Manifest, String> {
    let body_end = PREFIX_LEN + manifest_len;
    if header.len() < body_end + DIGEST_LEN {
        return Err("file is truncated".to_string());
    }
    let digest = Sha256::digest(&header[..body_end]);
    if digest.as_slice() != &header[body_end..body_end + DIGEST_LEN] {
        return Err("header checksum mismatch".to_string());
    }
    let manifest: Manifest = serde_json::from_slice(&header[PREFIX_LEN..body_end])
        .map_err(|e| format!("manifest unreadable: {e}"))?;
    if manifest.format_version != VECTOR_INDEX_FORMAT_VERSION {
        return Err(format!(
            "manifest format version {}, this build reads {VECTOR_INDEX_FORMAT_VERSION}",
            manifest.format_version
        ));
    }
    Ok(manifest)
}

/// The WAL position recorded in the file header at `path`, if the header is
/// intact. Reads only the header.
pub(crate) fn peek_saved_position(path: &Path) -> Option<WalPosition> {
    let mut file = File::open(path).ok()?;
    let mut prefix = [0u8; PREFIX_LEN];
    file.read_exact(&mut prefix).ok()?;
    let manifest_len = check_prefix(&prefix).ok()?;
    let mut header = prefix.to_vec();
    header.resize(PREFIX_LEN + manifest_len + DIGEST_LEN, 0);
    file.read_exact(&mut header[PREFIX_LEN..]).ok()?;
    parse_header(&header, manifest_len).ok().map(|m| m.wal)
}

struct LoadedFile {
    manifest: Manifest,
    tenants: Vec<(String, TenantVectorIndex)>,
}

/// Reads and fully verifies the file. `Ok(None)` when it does not exist.
fn read_index_file(path: &Path, tuning: &AnnTuningConfig) -> Result<Option<LoadedFile>, String> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(format!("read failed: {e}")),
    };
    let manifest_len = check_prefix(&bytes)?;
    let manifest = parse_header(&bytes, manifest_len)?;
    let configured = TuningManifest::from(tuning);
    if manifest.tuning != configured {
        return Err(format!(
            "saved with tuning {:?}, configured tuning is {configured:?}",
            manifest.tuning
        ));
    }
    let mut offset = PREFIX_LEN + manifest_len + DIGEST_LEN;
    let mut tenants = Vec::with_capacity(manifest.tenants.len());
    let mut seen = HashSet::new();
    let mut vectors = 0usize;
    for entry in &manifest.tenants {
        if !seen.insert(entry.tenant_id.as_str()) {
            return Err(format!("tenant '{}' listed twice", entry.tenant_id));
        }
        let len = usize::try_from(entry.bytes).map_err(|_| "section length overflow")?;
        let end = offset
            .checked_add(len)
            .filter(|end| *end <= bytes.len())
            .ok_or_else(|| "file is truncated".to_string())?;
        let section = &bytes[offset..end];
        if hex::encode(Sha256::digest(section)) != entry.sha256 {
            return Err(format!(
                "checksum mismatch in the section of tenant '{}'",
                entry.tenant_id
            ));
        }
        let index = TenantVectorIndex::decode(section, entry.dimension, tuning.clone())
            .map_err(|e| format!("tenant '{}': {e}", entry.tenant_id))?;
        if index.len() != entry.vectors {
            return Err(format!(
                "tenant '{}' holds {} vectors, manifest says {}",
                entry.tenant_id,
                index.len(),
                entry.vectors
            ));
        }
        vectors += index.len();
        tenants.push((entry.tenant_id.clone(), index));
        offset = end;
    }
    if offset != bytes.len() {
        return Err("trailing bytes after the last section".to_string());
    }
    if vectors != manifest.vector_count {
        return Err(format!(
            "sections hold {vectors} vectors, manifest says {}",
            manifest.vector_count
        ));
    }
    Ok(Some(LoadedFile { manifest, tenants }))
}

impl InMemoryStore {
    /// Restore the vector indexes from `path` (see the module docs), or
    /// build them when that is not possible. Called at the end of a WAL
    /// replay with the vector indexes deferred. `caught_up` holds the vector
    /// records and tombstones after the saved position, as collected by the
    /// replay (`None` when it could not be collected).
    pub(crate) fn restore_vector_indexes(
        &mut self,
        path: &Path,
        wal: &FileWal,
        caught_up: Option<VectorCatchUp>,
    ) -> VectorIndexRestore {
        match self.try_restore_vector_indexes(path, wal, caught_up) {
            Ok(Some(restored)) => restored,
            Ok(None) => {
                self.rebuild_vector_indexes();
                VectorIndexRestore::Missing
            }
            Err(reason) => {
                eprintln!(
                    "warning: discarding persisted vector index '{}': {reason}; rebuilding from the WAL",
                    path.display()
                );
                self.rebuild_vector_indexes();
                VectorIndexRestore::Rebuilt { reason }
            }
        }
    }

    fn try_restore_vector_indexes(
        &mut self,
        path: &Path,
        wal: &FileWal,
        caught_up: Option<VectorCatchUp>,
    ) -> Result<Option<VectorIndexRestore>, String> {
        let Some(loaded) = read_index_file(path, &self.ann_tuning)? else {
            return Ok(None);
        };
        let saved = loaded.manifest.wal;
        if saved.generation != wal.generation() {
            return Err(format!(
                "saved for WAL generation {:016x}, the WAL is at generation {:016x}",
                saved.generation,
                wal.generation()
            ));
        }
        let caught_up = caught_up.ok_or_else(|| {
            format!(
                "the WAL holds fewer than the {} records the index reflects",
                saved.records
            )
        })?;

        // A tenant erased after the save starts from nothing: its saved
        // index (and dimension) no longer applies. Vectors written after the
        // erasure are in `claim_ids` and are re-added below.
        let mut indexes: HashMap<String, TenantVectorIndex> = loaded
            .tenants
            .into_iter()
            .filter(|(tenant, _)| !caught_up.erased_tenants.contains(tenant))
            .collect();
        // Claims deleted after the save leave the index of the tenant that
        // owned them (the claim id may since have been reused, even by
        // another tenant; the catch-up below re-adds it where it now lives).
        // An index emptied this way is dropped before the dimension check:
        // deleting a tenant's last vector releases its dimension.
        for (tenant, claim_id) in &caught_up.deleted_claims {
            if let Some(index) = indexes.get_mut(tenant) {
                index.remove(claim_id);
            }
        }
        indexes.retain(|_, index| !index.is_empty());
        for (tenant, index) in &indexes {
            match self.tenant_vector_dims.get(tenant) {
                Some(dim) if *dim == index.dimensions() => {}
                Some(dim) => {
                    return Err(format!(
                        "tenant '{tenant}' index has dimension {}, the tenant's vectors have {dim}",
                        index.dimensions()
                    ));
                }
                None => return Err(format!("tenant '{tenant}' has no stored vectors")),
            }
        }

        // Catch up: give every claim named after the saved position its
        // current stored vector (or none), exactly as the live apply path
        // would have left it.
        let mut ids: Vec<&String> = caught_up.claim_ids.iter().collect();
        ids.sort_unstable();
        for claim_id in &ids {
            let Some(claim) = self.claims.get(claim_id.as_str()) else {
                continue;
            };
            let tenant = &claim.tenant_id;
            match self.claim_vectors.get(claim_id.as_str()) {
                Some(vector) => {
                    let dim = self
                        .tenant_vector_dims
                        .get(tenant)
                        .copied()
                        .unwrap_or(vector.len());
                    let tuning = &self.ann_tuning;
                    let index = indexes
                        .entry(tenant.clone())
                        .or_insert_with(|| TenantVectorIndex::new(dim, tuning.clone()));
                    // An unindexable vector leaves the claim out of the
                    // index, as `add_vector_index_entry` does.
                    let _ = index.upsert(claim_id, vector);
                }
                None => {
                    if let Some(index) = indexes.get_mut(tenant) {
                        index.remove(claim_id);
                    }
                }
            }
        }
        indexes.retain(|_, index| !index.is_empty());
        self.verify_vector_indexes(&indexes)?;

        let tenants = indexes.len();
        let vectors = indexes.values().map(TenantVectorIndex::len).sum();
        self.vector_indexes = indexes;
        self.defer_vector_index = false;
        Ok(Some(VectorIndexRestore::Loaded {
            tenants,
            vectors,
            caught_up: ids.len() + caught_up.deleted_claims.len() + caught_up.erased_tenants.len(),
            saved_position: saved,
        }))
    }

    /// Every index must hold exactly its tenant's indexable stored vectors,
    /// each with the fingerprint of the stored vector.
    fn verify_vector_indexes(
        &self,
        indexes: &HashMap<String, TenantVectorIndex>,
    ) -> Result<(), String> {
        let mut expected: HashMap<&str, usize> = HashMap::new();
        for (claim_id, vector) in &self.claim_vectors {
            let Some(claim) = self.claims.get(claim_id) else {
                continue;
            };
            let dim = self
                .tenant_vector_dims
                .get(&claim.tenant_id)
                .copied()
                .unwrap_or(vector.len());
            if vector.len() == dim && is_indexable(vector) {
                *expected.entry(claim.tenant_id.as_str()).or_default() += 1;
            }
        }
        for (tenant, index) in indexes {
            let want = expected.remove(tenant.as_str()).unwrap_or(0);
            if index.len() != want {
                return Err(format!(
                    "tenant '{tenant}' index holds {} vectors, the WAL holds {want}",
                    index.len()
                ));
            }
            for (claim_id, fingerprint) in index.fingerprints() {
                let matches = self
                    .claims
                    .get(claim_id)
                    .is_some_and(|claim| claim.tenant_id == *tenant)
                    && self
                        .claim_vectors
                        .get(claim_id)
                        .is_some_and(|v| vector_fingerprint(v) == fingerprint);
                if !matches {
                    return Err(format!(
                        "tenant '{tenant}': claim '{claim_id}' does not match its stored vector"
                    ));
                }
            }
        }
        if let Some((tenant, count)) = expected.into_iter().next() {
            return Err(format!(
                "tenant '{tenant}' has {count} stored vectors but no index"
            ));
        }
        Ok(())
    }
}

#[derive(Default)]
struct SaverSignal {
    requested: bool,
    stopped: bool,
}

/// Save scheduling for a service: periodic saves, saves requested at a
/// checkpoint, and a final save at shutdown, never two at once, and never a
/// save of a position that is already on disk.
///
/// The service runs [`Self::run`] on a background thread with a closure that
/// takes a [`VectorIndexSnapshot`] under its own lock (cheap), so request
/// handling is blocked only for the copy, never for the write.
pub struct VectorIndexPersistence {
    path: PathBuf,
    interval: Option<Duration>,
    signal: Mutex<SaverSignal>,
    wake: Condvar,
    /// Last position written (or loaded); held for the whole of a save.
    saved: Mutex<Option<WalPosition>>,
}

impl VectorIndexPersistence {
    /// `interval` of `None` disables periodic saves (checkpoint and shutdown
    /// saves still happen).
    pub fn new(path: impl Into<PathBuf>, interval: Option<Duration>) -> Self {
        Self {
            path: path.into(),
            interval: interval.filter(|d| !d.is_zero()),
            signal: Mutex::new(SaverSignal::default()),
            wake: Condvar::new(),
            saved: Mutex::new(None),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn interval(&self) -> Option<Duration> {
        self.interval
    }

    /// Tell the scheduler what the startup restore found, so an index that
    /// is already on disk at the current position is not written again.
    pub fn note_restored(&self, restore: &VectorIndexRestore) {
        if let VectorIndexRestore::Loaded { saved_position, .. } = restore {
            *self.saved.lock().unwrap_or_else(|p| p.into_inner()) = Some(*saved_position);
        }
    }

    /// The last position written or loaded.
    pub fn last_saved_position(&self) -> Option<WalPosition> {
        *self.saved.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Ask the background loop to save as soon as possible (for example
    /// right after a WAL checkpoint changed the WAL generation).
    pub fn request_save(&self) {
        self.signal
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .requested = true;
        self.wake.notify_all();
    }

    /// Make [`Self::run`] return. A save in progress completes first.
    pub fn stop(&self) {
        self.signal
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .stopped = true;
        self.wake.notify_all();
    }

    /// Save `snapshot` unless its position is the one already on disk.
    /// `Ok(None)` means there was nothing new to write.
    pub fn save_if_changed(
        &self,
        snapshot: &VectorIndexSnapshot,
    ) -> Result<Option<VectorIndexSaveStats>, StoreError> {
        let mut saved = self.saved.lock().unwrap_or_else(|p| p.into_inner());
        if *saved == Some(snapshot.position()) {
            return Ok(None);
        }
        let stats = snapshot.save(&self.path)?;
        *saved = Some(stats.position);
        Ok(Some(stats))
    }

    /// Background loop: wait for the interval or a request, take a snapshot
    /// with `snapshot` (which returns `None` when there is nothing to save)
    /// and save it; hand every outcome to `report`. Returns after
    /// [`Self::stop`].
    pub fn run(
        &self,
        mut snapshot: impl FnMut() -> Option<VectorIndexSnapshot>,
        mut report: impl FnMut(Result<Option<VectorIndexSaveStats>, StoreError>),
    ) {
        loop {
            if !self.wait_for_turn() {
                return;
            }
            if let Some(snap) = snapshot() {
                report(self.save_if_changed(&snap));
            }
        }
    }

    /// Blocks until a save is due (`true`) or the loop is stopped (`false`).
    fn wait_for_turn(&self) -> bool {
        let deadline = self.interval.map(|interval| Instant::now() + interval);
        let mut signal = self.signal.lock().unwrap_or_else(|p| p.into_inner());
        loop {
            if signal.stopped {
                return false;
            }
            if signal.requested {
                signal.requested = false;
                return true;
            }
            match deadline {
                Some(deadline) => {
                    let now = Instant::now();
                    if now >= deadline {
                        return true;
                    }
                    signal = self
                        .wake
                        .wait_timeout(signal, deadline - now)
                        .unwrap_or_else(|p| p.into_inner())
                        .0;
                }
                None => {
                    signal = self.wake.wait(signal).unwrap_or_else(|p| p.into_inner());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::{Arc, mpsc};

    use schema::claim_builder;
    use tempfile::TempDir;

    use super::*;
    use crate::ReplayPolicy;
    use crate::failpoint::{arm, disarm, take_trace};

    const DIM: usize = 8;

    fn tuning() -> AnnTuningConfig {
        AnnTuningConfig {
            flat_threshold: 16,
            ..AnnTuningConfig::default()
        }
    }

    fn vector(i: usize) -> Vec<f32> {
        (0..DIM)
            .map(|d| ((i * 31 + d * 7) % 17) as f32 + 1.0)
            .collect()
    }

    fn ingest(wal: &mut FileWal, store: &mut InMemoryStore, from: usize, to: usize) {
        for i in from..to {
            let claim = claim_builder(&format!("c{i}"), "tenant-a", &format!("claim {i}"), 0.9);
            store
                .ingest_atomic_persistent(wal, claim, vec![], vec![], Some(vector(i)), i as u64)
                .unwrap();
        }
    }

    fn load(wal: &FileWal, index: &Path) -> (InMemoryStore, VectorIndexRestore) {
        let (store, stats) = InMemoryStore::load_from_wal_with_vector_index(
            wal,
            tuning(),
            ReplayPolicy::Strict,
            Some(index),
        )
        .unwrap();
        (store, stats.vector_index)
    }

    fn top(store: &InMemoryStore) -> Vec<Vec<String>> {
        (0..10)
            .map(|i| store.ann_vector_top_candidates("tenant-a", &vector(i * 3 + 1), 5))
            .collect()
    }

    fn snapshot(store: &InMemoryStore, wal: &FileWal) -> VectorIndexSnapshot {
        store.vector_index_snapshot(wal.position())
    }

    #[test]
    fn save_orders_write_fsync_rename_dir_fsync() {
        let dir = TempDir::new().unwrap();
        let mut wal = FileWal::open(dir.path().join("dash.wal")).unwrap();
        let mut store = InMemoryStore::new_with_ann_tuning(tuning());
        ingest(&mut wal, &mut store, 0, 40);
        take_trace();
        snapshot(&store, &wal)
            .save(&dir.path().join("dash.wal.vindex"))
            .unwrap();
        assert_eq!(
            take_trace(),
            vec![
                "vindex.tmp_written",
                "fsync_file",
                "vindex.fsynced",
                "rename",
                "vindex.renamed",
                "fsync_dir",
            ]
        );
    }

    #[test]
    fn a_crash_at_every_save_step_leaves_a_usable_old_or_new_index() {
        for (point, survives_new) in [
            ("vindex.tmp_written", false),
            ("vindex.fsynced", false),
            ("vindex.renamed", true),
        ] {
            let dir = TempDir::new().unwrap();
            let index = dir.path().join("dash.wal.vindex");
            let mut wal = FileWal::open(dir.path().join("dash.wal")).unwrap();
            let mut store = InMemoryStore::new_with_ann_tuning(tuning());
            ingest(&mut wal, &mut store, 0, 30);
            let old = wal.position();
            snapshot(&store, &wal).save(&index).unwrap();
            ingest(&mut wal, &mut store, 30, 50);
            let new = wal.position();

            arm(point);
            let result = snapshot(&store, &wal).save(&index);
            disarm();
            assert!(result.is_err(), "{point}: the save must fail");
            assert!(
                !temp_path_for(&index).exists(),
                "{point}: no temp file is left behind"
            );

            // The crash state is whatever is on disk right now.
            let crashed = TempDir::new().unwrap();
            for entry in fs::read_dir(dir.path()).unwrap() {
                let entry = entry.unwrap();
                fs::copy(entry.path(), crashed.path().join(entry.file_name())).unwrap();
            }
            drop(wal);
            let wal = FileWal::open(crashed.path().join("dash.wal")).unwrap();
            let (restored, report) = load(&wal, &crashed.path().join("dash.wal.vindex"));
            let expected = if survives_new { new } else { old };
            match report {
                VectorIndexRestore::Loaded { saved_position, .. } => {
                    assert_eq!(saved_position, expected, "{point}")
                }
                other => panic!("{point}: expected a load, got {other:?}"),
            }
            assert_eq!(top(&restored), top(&store), "{point}");
        }
    }

    #[test]
    fn the_scheduler_saves_on_request_skips_unchanged_positions_and_stops() {
        let dir = TempDir::new().unwrap();
        let index = dir.path().join("dash.wal.vindex");
        let mut wal = FileWal::open(dir.path().join("dash.wal")).unwrap();
        let mut store = InMemoryStore::new_with_ann_tuning(tuning());
        ingest(&mut wal, &mut store, 0, 20);
        let shared = Arc::new(Mutex::new((store, wal)));
        // No periodic saves: only requests wake the loop.
        let persistence = Arc::new(VectorIndexPersistence::new(&index, None));
        let (tx, rx) = mpsc::channel();
        let worker = {
            let persistence = Arc::clone(&persistence);
            let shared = Arc::clone(&shared);
            std::thread::spawn(move || {
                persistence.run(
                    || {
                        let guard = shared.lock().unwrap();
                        Some(guard.0.vector_index_snapshot(guard.1.position()))
                    },
                    |outcome| tx.send(outcome.map(|s| s.map(|s| s.position))).unwrap(),
                )
            })
        };
        // An upper bound for a slow runner, not a sleep: recv returns as soon
        // as the save is reported.
        let bound = Duration::from_secs(60);
        let first = shared.lock().unwrap().1.position();
        persistence.request_save();
        assert_eq!(rx.recv_timeout(bound).unwrap(), Ok(Some(first)));
        persistence.request_save();
        assert_eq!(rx.recv_timeout(bound).unwrap(), Ok(None), "nothing new");
        {
            let mut guard = shared.lock().unwrap();
            let (store, wal) = &mut *guard;
            ingest(wal, store, 20, 25);
        }
        let second = shared.lock().unwrap().1.position();
        persistence.request_save();
        assert_eq!(rx.recv_timeout(bound).unwrap(), Ok(Some(second)));
        persistence.stop();
        worker.join().unwrap();
        assert_eq!(persistence.last_saved_position(), Some(second));

        // A restart that loaded the file at that position does not rewrite it.
        let guard = shared.lock().unwrap();
        let (restored, report) = load(&guard.1, &index);
        let again = VectorIndexPersistence::new(&index, None);
        again.note_restored(&report);
        let snap = restored.vector_index_snapshot(guard.1.position());
        assert_eq!(again.save_if_changed(&snap).unwrap(), None);
    }

    #[test]
    fn the_scheduler_saves_periodically_without_requests() {
        let dir = TempDir::new().unwrap();
        let index = dir.path().join("dash.wal.vindex");
        let mut wal = FileWal::open(dir.path().join("dash.wal")).unwrap();
        let mut store = InMemoryStore::new_with_ann_tuning(tuning());
        ingest(&mut wal, &mut store, 0, 5);
        let snap = store.vector_index_snapshot(wal.position());
        let persistence = Arc::new(VectorIndexPersistence::new(
            &index,
            Some(Duration::from_millis(1)),
        ));
        let (tx, rx) = mpsc::channel();
        let worker = {
            let persistence = Arc::clone(&persistence);
            std::thread::spawn(move || {
                persistence.run(
                    || Some(snap.clone()),
                    |outcome| {
                        let _ = tx.send(outcome.is_ok());
                    },
                )
            })
        };
        let bound = Duration::from_secs(60);
        assert_eq!(rx.recv_timeout(bound), Ok(true));
        assert_eq!(rx.recv_timeout(bound), Ok(true));
        persistence.stop();
        worker.join().unwrap();
        assert!(index.exists());
    }
}
