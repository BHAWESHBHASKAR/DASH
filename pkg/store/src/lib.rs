use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    sync::Arc,
};

use graph::summarize_incoming_edges;
use ranking::{RankSignals, bm25_score, score_claim_with_bm25};
use schema::{
    Citation, Claim, ClaimEdge, Evidence, Relation, RetrievalRequest, RetrievalResult, Stance,
    StanceMode, ValidationError, tokenize, validate_claim, validate_edge, validate_evidence,
};

#[macro_use]
mod failpoint;
mod delete;
mod disk;
mod value_codec;
pub use delete::{DeleteOutcome, DeleteStats, PreparedDelete};
pub use disk::{DiskBackedStore, DiskStatus};

#[cfg(feature = "gpu-backend")]
mod gpu;
mod group_commit;
mod metrics;
pub mod vector_index;
mod vector_persist;
mod wal;
pub use metrics::{StoreIndexStats, StoreLoadStats, VectorBackendRuntime};
pub(crate) use metrics::{VECTOR_BACKEND_ENV, VectorBackendPreference};
pub use vector_index::AnnTuningConfig;
use vector_index::{TenantVectorIndex, exact_top_k};
pub use vector_persist::{
    VECTOR_INDEX_FORMAT_VERSION, VectorIndexPersistence, VectorIndexRestore, VectorIndexSaveStats,
    VectorIndexSnapshot,
};

#[derive(Default)]
pub(crate) struct Bm25Context {
    doc_freq: HashMap<String, usize>,
    total_docs: usize,
    avg_doc_len: f32,
}

pub use group_commit::{
    CommitTicket, GROUP_COMMIT_MAX_WAIT_LIMIT, GroupCommitConfig, GroupCommitError, GroupCommitLog,
    GroupCommitStats, GroupCommitter,
};
pub(crate) use wal::{
    BatchCommitRecord, ClaimVectorRecord, PersistedRecord, ReplayItem, line_to_record,
    record_to_line,
};
pub use wal::{
    CheckpointPolicy, FileWal, ReplayPolicy, WAL_POISONED_PREFIX, WAL_REPLAY_STRICT_ENV,
    WalCheckpointStats, WalEvent, WalInspection, WalInvalidLine, WalPosition, WalRepairOptions,
    WalRepairReport, WalReplayBoundary, WalReplayStats, WalReplicationDelta, WalReplicationExport,
    WalReplicationFrame, WalRollbackPoint, WalWritePolicy, inspect_wal_file, repair_wal_file,
};
pub use wal::{
    ChunkFetch, ChunkRead, DownloadOutcome, DownloadPaths, DownloadedExport,
    EXPORT_CHUNK_DEFAULT_BYTES, EXPORT_CHUNK_HEADER_RESERVE, EXPORT_CHUNK_MAX_BYTES,
    EXPORT_IDLE_TTL, EXPORTS_RETAINED, ExportSection, ExportSource, GENERATION_TRANSITIONS_KEPT,
    GenerationTransition, HttpExportSource, LocalExportSource, ReplicationExportChunk,
    ReplicationExportFile, ReplicationExportManifest, ReplicationExportStore, download_export,
    hash_file, valid_export_id,
};
pub use wal::{
    GROUP_BEGIN_PREFIX, REPLICATION_GROUP_EXTENSION_MAX, SINGLE_TX_PREFIX,
    batch_commit_id_from_wal_line, complete_group_prefix_len, is_group_marker_commit_id,
};
pub use wal::{Tombstone, tombstone_from_wal_line};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct BatchCommitMetadata {
    pub commit_id: String,
    pub batch_size: usize,
    pub ts_unix_ms: u64,
    pub claim_ids: Vec<String>,
    pub payload_fingerprint: String,
}

/// A validated single-bundle ingest whose WAL lines are encoded but not yet
/// written. Produced by [`InMemoryStore::prepare_atomic_ingest`]; the caller
/// makes [`PreparedIngest::wal_lines`] durable (directly or through a
/// [`GroupCommitter`]) and only then calls
/// [`InMemoryStore::apply_prepared_ingest`].
#[derive(Debug, Clone)]
pub struct PreparedIngest {
    claim: Claim,
    evidence: Vec<Evidence>,
    edges: Vec<ClaimEdge>,
    vector: Option<Vec<f32>>,
    wal_lines: Vec<String>,
}

impl PreparedIngest {
    /// The encoded commit group: begin marker, records, closing marker.
    pub fn wal_lines(&self) -> &[String] {
        &self.wal_lines
    }

    pub fn claim_id(&self) -> &str {
        &self.claim.claim_id
    }
}

/// Result of [`InMemoryStore::apply_replicated_lines`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReplicatedApply {
    pub applied: usize,
    /// Lines lenient replay would quarantine (see
    /// [`InMemoryStore::apply_persisted_record_line_lenient`]).
    pub skipped: usize,
    /// The redb write of the frame failed: the handle was detached and the
    /// disk marked unavailable (the WAL stays the source of truth).
    pub disk_error: Option<String>,
}

/// Result of [`InMemoryStore::ingest_atomic_persistent`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AtomicIngestOutcome {
    /// `false` when the bundle was already fully applied (idempotent retry).
    pub applied: bool,
    /// Set when redb rejected the mirrored write after the WAL commit. The
    /// write itself is durable (WAL) and visible; redb was detached.
    pub disk_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum StoreError {
    Validation(ValidationError),
    MissingClaim(String),
    Conflict(String),
    InvalidVector(String),
    Io(String),
    Parse(String),
}

const FNV1A_64_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
const FNV1A_64_PRIME: u64 = 0x100000001b3;

fn fnv1a64_feed(state: &mut u64, bytes: &[u8]) {
    for byte in bytes {
        *state ^= u64::from(*byte);
        *state = state.wrapping_mul(FNV1A_64_PRIME);
    }
}

pub fn batch_commit_payload_fingerprint(batch_size: usize, claim_ids: &[String]) -> String {
    let mut state = FNV1A_64_OFFSET_BASIS;
    fnv1a64_feed(&mut state, &(batch_size as u64).to_le_bytes());
    fnv1a64_feed(&mut state, &(claim_ids.len() as u64).to_le_bytes());
    for claim_id in claim_ids {
        fnv1a64_feed(&mut state, &(claim_id.len() as u64).to_le_bytes());
        fnv1a64_feed(&mut state, claim_id.as_bytes());
    }
    format!("{state:016x}")
}

impl From<ValidationError> for StoreError {
    fn from(value: ValidationError) -> Self {
        Self::Validation(value)
    }
}

impl From<std::io::Error> for StoreError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value.to_string())
    }
}

/// Upper bound on the in-memory WAL event ring (PERF-05). The ring is
/// only an observability aid (`wal_len`); the durable log is the
/// `FileWal`. Without a bound the leader grew by one entry per applied
/// record, forever.
pub const DEFAULT_WAL_EVENT_CAPACITY: usize = 8192;

/// Bounded ring of recent [`WalEvent`]s plus a monotonic total.
#[derive(Debug)]
struct WalEventRing {
    events: VecDeque<WalEvent>,
    total: u64,
    capacity: usize,
}

impl Default for WalEventRing {
    fn default() -> Self {
        Self {
            events: VecDeque::new(),
            total: 0,
            capacity: DEFAULT_WAL_EVENT_CAPACITY,
        }
    }
}

impl WalEventRing {
    fn push(&mut self, event: WalEvent) {
        self.total = self.total.saturating_add(1);
        if self.capacity == 0 {
            return;
        }
        while self.events.len() >= self.capacity {
            self.events.pop_front();
        }
        self.events.push_back(event);
    }

    fn trim(&mut self) {
        while self.events.len() > self.capacity {
            self.events.pop_front();
        }
    }
}

impl Clone for WalEventRing {
    /// A cloned ring starts empty (it keeps the capacity and the running
    /// total) so cloning a store never copies thousands of events.
    fn clone(&self) -> Self {
        Self {
            events: VecDeque::new(),
            total: self.total,
            capacity: self.capacity,
        }
    }
}

/// One disk mutation recorded by a detached (staged) store so that
/// [`InMemoryStore::commit_staged`] can replay it onto the real redb
/// handle after the WAL append succeeded (DATA-09).
#[derive(Debug, Clone)]
enum StagedDiskOp {
    Claim(Claim),
    Evidence(Evidence),
    Edge(ClaimEdge),
    Vector {
        claim_id: String,
        tenant_id: String,
        vector: Vec<f32>,
        new_dim: Option<usize>,
    },
    BatchCommit(BatchCommitMetadata),
    /// Everything one tombstone removes, applied in one redb transaction.
    Delete(delete::DiskDeletion),
}

/// Reverse edge index entry: `from` points at the indexed claim.
#[derive(Debug, Clone)]
struct IncomingEdge {
    from_claim_id: String,
    relation: Relation,
    strength: f32,
}

/// In-memory claim store.
///
/// # Cloning and the disk handle (DATA-09)
///
/// `Clone` is **detached**: the clone never holds the redb handle. A
/// clone is meant to be a *staged* copy: mutate it, append to the WAL,
/// and only then call [`InMemoryStore::commit_staged`] on the live
/// store, which replays the staged disk writes onto redb and swaps the
/// in-memory state. If the batch is rolled back, the staged clone is
/// simply dropped and redb was never touched. `clone_detached` is the
/// explicit spelling of the same operation.
///
/// Do not assign a staged clone over the live store (`self.store =
/// staged`) when a disk is attached: that would drop the handle. Use
/// `commit_staged`.
#[derive(Default)]
pub struct InMemoryStore {
    claims: HashMap<String, Claim>,
    evidence_by_claim: HashMap<String, Vec<Evidence>>,
    edges_by_claim: HashMap<String, Vec<ClaimEdge>>,
    /// Reverse index: target claim id -> edges pointing at it.
    edges_in: HashMap<String, Vec<IncomingEdge>>,
    claim_vectors: HashMap<String, Vec<f32>>,
    /// Per-tenant vector index (flat below the threshold, HNSW above).
    /// Holds no full-precision copy: rerank reads `claim_vectors`.
    vector_indexes: HashMap<String, TenantVectorIndex>,
    /// `true` while a bulk load is replaying vectors: they are collected in
    /// `claim_vectors` only and the indexes are built once afterwards.
    defer_vector_index: bool,
    tenant_vector_dims: HashMap<String, usize>,
    tenant_claim_ids: HashMap<String, HashSet<String>>,
    inverted_index: HashMap<String, HashMap<String, HashSet<String>>>,
    entity_index: HashMap<String, HashMap<String, HashSet<String>>>,
    embedding_index: HashMap<String, HashMap<String, HashSet<String>>>,
    temporal_index: HashMap<String, BTreeMap<i64, HashSet<String>>>,
    batch_commits: HashMap<String, BatchCommitMetadata>,
    claim_tokens: HashMap<String, Vec<String>>,
    ann_tuning: AnnTuningConfig,
    vector_backend_runtime: VectorBackendRuntime,
    wal: WalEventRing,
    disk: Option<Arc<disk::DiskBackedStore>>,
    disk_status: disk::DiskStatus,
    /// `Some` only on detached clones: disk writes recorded for
    /// `commit_staged`.
    staged_disk_ops: Option<Vec<StagedDiskOp>>,
    /// Claim ids of legacy claim records a replication follower skipped, so
    /// records depending on them are skipped too.
    replica_skipped_claims: HashSet<String>,
    /// The WAL position this state reflects, when the owner of the WAL keeps
    /// it here (set by the WAL loaders and the replication follower). A
    /// persisted vector index records it. Kept by `commit_staged` and
    /// `replace_state_from`: only the WAL owner moves it.
    wal_position: Option<WalPosition>,
}

impl Clone for InMemoryStore {
    /// Detached clone; see the type-level docs. Equivalent to
    /// [`InMemoryStore::clone_detached`].
    fn clone(&self) -> Self {
        self.clone_detached()
    }
}

impl InMemoryStore {
    pub fn new() -> Self {
        Self::new_with_ann_tuning(AnnTuningConfig::default())
    }

    pub fn new_with_ann_tuning(ann_tuning: AnnTuningConfig) -> Self {
        let vector_backend_runtime =
            resolve_vector_backend_runtime(parse_vector_backend_preference());
        Self {
            ann_tuning,
            vector_backend_runtime,
            ..Self::default()
        }
    }

    pub fn ann_tuning(&self) -> &AnnTuningConfig {
        &self.ann_tuning
    }

    /// Change the vector index tuning. Existing indexes are rebuilt from the
    /// stored vectors so the new connectivity, beam widths, flat threshold
    /// and rerank width apply to them too (a cold-start-sized cost).
    pub fn set_ann_tuning(&mut self, ann_tuning: AnnTuningConfig) {
        if self.ann_tuning == ann_tuning {
            return;
        }
        self.ann_tuning = ann_tuning;
        self.rebuild_vector_indexes();
    }

    pub fn vector_backend_runtime(&self) -> VectorBackendRuntime {
        self.vector_backend_runtime
    }

    pub fn vector_backend_label(&self) -> &'static str {
        self.vector_backend_runtime.as_str()
    }

    /// Attach a `redb`-backed disk store to this in-memory store. The
    /// disk store is opened at `path` (creating it on first use).
    /// Every subsequent `apply_*` call will mirror the in-memory
    /// mutation to disk BEFORE the in-memory state changes. If the
    /// disk write fails, the in-memory apply is aborted and the error
    /// is returned to the caller.
    ///
    /// On open failure, returns the in-memory store with the disk
    /// detached and `disk_status` set to `Unavailable { reason }`.
    /// The error is also returned so the caller can log it. The
    /// store is always returned (the caller's in-memory state is
    /// preserved).
    pub fn with_disk(self, path: impl AsRef<std::path::Path>) -> Result<Self, String> {
        Ok(self.attach_disk(path))
    }

    /// Infallible form of [`InMemoryStore::with_disk`]: on open failure the
    /// in-memory state is kept and [`InMemoryStore::disk_status`] reports
    /// `Unavailable { reason }`, so callers inspect the status instead of a
    /// `Result` that can never be `Err`.
    pub fn attach_disk(self, path: impl AsRef<std::path::Path>) -> Self {
        match disk::DiskBackedStore::new(path) {
            Ok(disk) => Self {
                disk: Some(Arc::new(disk)),
                disk_status: disk::DiskStatus::Available,
                ..self
            },
            Err(reason) => Self {
                disk: None,
                disk_status: disk::DiskStatus::Unavailable { reason },
                ..self
            },
        }
    }

    /// The current disk status. Returns `DiskStatus::Unavailable` if
    /// no disk was attached (the default in-memory mode).
    pub fn disk_status(&self) -> &disk::DiskStatus {
        &self.disk_status
    }

    /// Clone the in-memory state WITHOUT the redb handle (DATA-09).
    ///
    /// The returned store records the disk writes it would have made
    /// instead of performing them. Use it as the staging area for an
    /// atomic batch: mutate the clone, append to the WAL, and only after
    /// the WAL append succeeded call [`InMemoryStore::commit_staged`] on
    /// the live store. Dropping the clone (rollback) leaves redb
    /// untouched. `Clone::clone` is the same operation.
    pub fn clone_detached(&self) -> Self {
        Self {
            claims: self.claims.clone(),
            evidence_by_claim: self.evidence_by_claim.clone(),
            edges_by_claim: self.edges_by_claim.clone(),
            edges_in: self.edges_in.clone(),
            claim_vectors: self.claim_vectors.clone(),
            vector_indexes: self.vector_indexes.clone(),
            defer_vector_index: false,
            tenant_vector_dims: self.tenant_vector_dims.clone(),
            tenant_claim_ids: self.tenant_claim_ids.clone(),
            inverted_index: self.inverted_index.clone(),
            entity_index: self.entity_index.clone(),
            embedding_index: self.embedding_index.clone(),
            temporal_index: self.temporal_index.clone(),
            batch_commits: self.batch_commits.clone(),
            claim_tokens: self.claim_tokens.clone(),
            ann_tuning: self.ann_tuning.clone(),
            vector_backend_runtime: self.vector_backend_runtime,
            wal: self.wal.clone(),
            disk: None,
            disk_status: self.disk_status.clone(),
            staged_disk_ops: Some(Vec::new()),
            replica_skipped_claims: self.replica_skipped_claims.clone(),
            wal_position: self.wal_position,
        }
    }

    /// The WAL position this state reflects, if the WAL owner recorded one.
    pub fn wal_position(&self) -> Option<WalPosition> {
        self.wal_position
    }

    /// Record the WAL position this state reflects. Call it while holding
    /// whatever serialises WAL appends with store mutations, right after the
    /// mutation that the last appended record describes.
    pub fn set_wal_position(&mut self, position: Option<WalPosition>) {
        self.wal_position = position;
    }

    /// A point-in-time copy of every tenant's vector index for saving with
    /// [`VectorIndexSnapshot::save`]. Cheap: an HNSW index is shared
    /// copy-on-write (the next write to the live index copies it once while
    /// the snapshot is alive); a flat index (at most the flat threshold of
    /// vectors) is copied. `position` must be the WAL position this state
    /// reflects.
    pub fn vector_index_snapshot(&self, position: WalPosition) -> VectorIndexSnapshot {
        let mut tenants: Vec<(String, TenantVectorIndex)> = self
            .vector_indexes
            .iter()
            .map(|(tenant, index)| (tenant.clone(), index.clone()))
            .collect();
        tenants.sort_by(|a, b| a.0.cmp(&b.0));
        VectorIndexSnapshot::new(position, self.ann_tuning.clone(), tenants)
    }

    /// Adopt a staged clone produced by [`InMemoryStore::clone_detached`]
    /// (or `clone()`), after the caller has durably appended the same
    /// mutations to the WAL.
    ///
    /// In-memory state is swapped in first (the WAL is the source of
    /// truth and is already committed), then the staged disk writes are
    /// replayed onto this store's redb handle. If a disk write fails the
    /// disk handle is dropped and `disk_status` becomes `Unavailable` so
    /// redb is never left silently diverging, and the error is returned;
    /// the in-memory commit still stands. A no-disk store simply swaps.
    pub fn commit_staged(&mut self, staged: InMemoryStore) -> Result<(), StoreError> {
        let mut staged = staged;
        let ops = staged.staged_disk_ops.take().unwrap_or_default();

        let disk = self.disk.take();
        let disk_status = self.disk_status.clone();
        let mut own_staging = self.staged_disk_ops.take();
        let wal_position = self.wal_position;
        let mut ring = std::mem::take(&mut self.wal);
        ring.total = ring.total.max(staged.wal.total);
        ring.events.append(&mut staged.wal.events);
        ring.trim();

        *self = staged;
        self.wal = ring;
        self.disk = disk;
        self.disk_status = disk_status;
        self.wal_position = wal_position;

        match self.disk.clone() {
            Some(disk) => {
                if let Err(reason) = disk.write_ops(&ops) {
                    self.disk = None;
                    self.disk_status = disk::DiskStatus::Unavailable {
                        reason: reason.clone(),
                    };
                    return Err(StoreError::Io(reason));
                }
            }
            None => {
                if let Some(buffer) = own_staging.as_mut() {
                    buffer.extend(ops);
                }
            }
        }
        self.staged_disk_ops = own_staging;
        Ok(())
    }

    /// Replace ALL in-memory state with `fresh` (built from a replication
    /// export) instead of merging onto it, keeping this store's redb handle.
    /// The redb file is cleared and rewritten from the new state so it never
    /// retains rows the leader no longer has. If the redb rewrite fails the
    /// handle is dropped, `disk_status` becomes `Unavailable`, and the error
    /// is returned; the in-memory replacement still stands.
    pub fn replace_state_from(&mut self, fresh: InMemoryStore) -> Result<(), StoreError> {
        let mut fresh = fresh;
        let disk = self.disk.take();
        let disk_status = self.disk_status.clone();
        let own_staging = self.staged_disk_ops.take();
        let wal_position = self.wal_position;
        let mut ring = std::mem::take(&mut self.wal);
        ring.total = ring.total.max(fresh.wal.total);
        ring.events.append(&mut fresh.wal.events);
        ring.trim();

        *self = fresh;
        self.wal = ring;
        self.disk = disk;
        self.disk_status = disk_status;
        self.staged_disk_ops = own_staging;
        self.wal_position = wal_position;

        if let Some(disk) = self.disk.clone()
            && let Err(reason) = disk.clear_all().and_then(|()| disk.checkpoint_from(self))
        {
            self.disk = None;
            self.disk_status = disk::DiskStatus::Unavailable {
                reason: reason.clone(),
            };
            return Err(StoreError::Io(reason));
        }
        Ok(())
    }

    /// Construct an `InMemoryStore` by bulk-loading from a disk
    /// snapshot, then replaying any WAL delta. Returns the new store
    /// + load stats. This is the cold-start path when both the WAL
    /// and the redb snapshot are available.
    #[allow(clippy::doc_lazy_continuation)]
    pub fn load_from_disk_and_wal(
        disk_path: impl AsRef<std::path::Path>,
        wal: &mut FileWal,
        ann_tuning: AnnTuningConfig,
    ) -> Result<(Self, StoreLoadStats), String> {
        // Helper: replay the WAL into an in-memory store, returning
        // the breakdown stats. Errors are converted to `String` for
        // the disk API.
        fn replay_into(
            store: &mut InMemoryStore,
            wal: &mut FileWal,
        ) -> Result<StoreLoadStats, String> {
            store
                .replay_wal(wal, ReplayPolicy::from_env())
                .map_err(|e| format!("wal replay: {e:?}"))
        }

        // 1. Open the disk. If the open fails, fall back to the
        //    WAL-only path.
        let disk = match disk::DiskBackedStore::new(disk_path) {
            Ok(disk) => Some(Arc::new(disk)),
            Err(reason) => {
                let mut store = Self::new_with_ann_tuning(ann_tuning);
                let stats = replay_into(&mut store, wal)?;
                store.disk_status = disk::DiskStatus::Unavailable { reason };
                return Ok((store, stats));
            }
        };

        // 2. Set status to Recovering for the duration of the bulk
        //    load + WAL tail replay.
        let mut store = Self {
            disk,
            disk_status: disk::DiskStatus::Recovering,
            // The bulk load only fills `claim_vectors`; the WAL replay that
            // follows builds the vector indexes once at its end.
            defer_vector_index: true,
            ..Self::new_with_ann_tuning(ann_tuning)
        };
        // Arc::clone the disk handle so we can hold a borrow on the
        // store across the bulk-load call. The Arc refcount is bumped
        // to 2 (the store holds one, the local `disk` binding holds
        // the other), and dropped when the binding goes out of scope
        // at the end of this function. This replaces the pre-PR-2
        // pattern of `take()` + restore, which was only needed when
        // the disk was an owned `DiskBackedStore` (not Clone-able
        // because it wraps a `redb::Database`).
        let disk = Arc::clone(store.disk.as_ref().expect("disk was just attached"));
        // With a tombstone in the WAL the redb state cannot serve as the
        // base of the replay (see `FileWal::contains_tombstones`): rebuild
        // redb from the WAL instead, which is the source of truth anyway.
        let reset = wal
            .contains_tombstones()
            .map_err(|e| format!("wal scan: {e:?}"))?;
        if reset {
            disk.clear_all()
                .map_err(|e| format!("disk reset before tombstone replay: {e}"))?;
        }
        let claims_loaded = disk
            .bulk_load_claims_into(&mut store)
            .map_err(|e| format!("disk bulk load: {e}"))?;
        // 3. Replay the WAL tail over the bulk-loaded state.
        let mut stats = replay_into(&mut store, wal)?;
        // The bulk-loaded count is the dominant figure; merge it
        // with the WAL tail counts (claims loaded by the bulk
        // path will be overwritten in `claims_loaded` by the WAL
        // tail counter, so we explicitly prefer the bulk count).
        if !reset {
            stats.claims_loaded = claims_loaded;
        }
        store.disk_status = disk::DiskStatus::Available;
        Ok((store, stats))
    }

    pub fn load_from_wal(wal: &FileWal) -> Result<Self, StoreError> {
        let (store, _) = Self::load_from_wal_with_stats(wal)?;
        Ok(store)
    }

    pub fn load_from_wal_with_ann_tuning(
        wal: &FileWal,
        ann_tuning: AnnTuningConfig,
    ) -> Result<Self, StoreError> {
        let (store, _) = Self::load_from_wal_with_stats_and_ann_tuning(wal, ann_tuning)?;
        Ok(store)
    }

    pub fn load_from_wal_with_stats(wal: &FileWal) -> Result<(Self, StoreLoadStats), StoreError> {
        Self::load_from_wal_with_stats_and_ann_tuning(wal, AnnTuningConfig::default())
    }

    pub fn load_from_wal_with_stats_and_ann_tuning(
        wal: &FileWal,
        ann_tuning: AnnTuningConfig,
    ) -> Result<(Self, StoreLoadStats), StoreError> {
        Self::load_from_wal_with_policy(wal, ann_tuning, ReplayPolicy::from_env())
    }

    /// Like [`Self::load_from_wal_with_stats_and_ann_tuning`] with an explicit
    /// [`ReplayPolicy`] instead of the `DASH_WAL_REPLAY_STRICT` environment
    /// variable.
    pub fn load_from_wal_with_policy(
        wal: &FileWal,
        ann_tuning: AnnTuningConfig,
        policy: ReplayPolicy,
    ) -> Result<(Self, StoreLoadStats), StoreError> {
        Self::load_from_wal_with_vector_index(wal, ann_tuning, policy, None)
    }

    /// Like [`Self::load_from_wal_with_policy`], restoring the vector indexes
    /// from the persisted index at `vector_index_path` instead of building
    /// them, when that file is intact and matches: same format version and
    /// tuning, saved for the current WAL generation. Only the vector records
    /// after the saved WAL position are then applied (catch-up), and the
    /// result is verified against every replayed vector before it is used.
    /// Any mismatch or corruption logs a warning and falls back to building
    /// the indexes from the replayed vectors, so a stale or corrupt index is
    /// never served. `stats.vector_index` reports which path was taken.
    pub fn load_from_wal_with_vector_index(
        wal: &FileWal,
        ann_tuning: AnnTuningConfig,
        policy: ReplayPolicy,
        vector_index_path: Option<&std::path::Path>,
    ) -> Result<(Self, StoreLoadStats), StoreError> {
        let mut store = Self::new_with_ann_tuning(ann_tuning);
        let stats = store.replay_wal_with_vector_index(wal, policy, vector_index_path)?;
        Ok((store, stats))
    }

    /// Replays the snapshot and WAL into `self`.
    ///
    /// Lenient policy: a legacy (pre-checksum) record that cannot be parsed
    /// or fails validation, and a vector that fails validation, is copied to
    /// `<wal>.quarantine`, counted in `quarantined_records` and skipped;
    /// records that depend on a quarantined claim are skipped and counted in
    /// `dependent_skipped`. Any other record that cannot be applied, and every
    /// such record under the strict policy, fails the load.
    fn replay_wal(
        &mut self,
        wal: &FileWal,
        policy: ReplayPolicy,
    ) -> Result<StoreLoadStats, StoreError> {
        self.replay_wal_with_vector_index(wal, policy, None)
    }

    fn replay_wal_with_vector_index(
        &mut self,
        wal: &FileWal,
        policy: ReplayPolicy,
        vector_index_path: Option<&std::path::Path>,
    ) -> Result<StoreLoadStats, StoreError> {
        // Vectors are collected first and indexed once at the end: restored
        // from the persisted index, or built per tenant in bulk
        // (multi-threaded) instead of one insert per record.
        self.defer_vector_index = true;
        // The saved position decides which vector records replay reports as
        // the catch-up set; the file itself is fully verified afterwards.
        let collect_from = vector_index_path
            .and_then(vector_persist::peek_saved_position)
            .filter(|saved| saved.generation == wal.generation())
            .map(|saved| saved.records);
        let (mut stats, caught_up) = match self.replay_wal_records(wal, policy, collect_from) {
            Ok(result) => result,
            Err(err) => {
                self.rebuild_vector_indexes();
                return Err(err);
            }
        };
        stats.vector_index = match vector_index_path {
            Some(path) => self.restore_vector_indexes(path, wal, caught_up),
            None => {
                self.rebuild_vector_indexes();
                VectorIndexRestore::NotConfigured
            }
        };
        self.wal_position = Some(wal.position());
        Ok(stats)
    }

    fn replay_wal_records(
        &mut self,
        wal: &FileWal,
        policy: ReplayPolicy,
        collect_vectors_from: Option<usize>,
    ) -> Result<(StoreLoadStats, Option<wal::VectorCatchUp>), StoreError> {
        fn hard(err: StoreError, origin: &str) -> StoreError {
            match err {
                StoreError::Validation(_)
                | StoreError::MissingClaim(_)
                | StoreError::InvalidVector(_) => StoreError::Parse(format!("{origin}: {err:?}")),
                other => other,
            }
        }
        let lenient = policy == ReplayPolicy::Lenient;
        let replay = wal.replay_with_policy(policy, collect_vectors_from)?;
        let caught_up = replay.vector_catch_up;
        let mut stats = replay.stats;
        let mut sink = replay.sink;
        let mut bad_claims = replay.quarantined_claim_ids;
        let mut claims_loaded = 0usize;
        let mut evidence_loaded = 0usize;
        let mut edges_loaded = 0usize;
        let mut vectors_loaded = 0usize;

        for item in replay.items {
            if lenient && !bad_claims.is_empty() && item.depends_on(&bad_claims) {
                eprintln!(
                    "warning: skipping {} (depends on a quarantined claim)",
                    item.origin
                );
                sink.push(&item.quarantine_line());
                stats.dependent_skipped += 1;
                continue;
            }
            let ReplayItem {
                record,
                legacy,
                raw,
                origin,
            } = item;
            if lenient
                && let PersistedRecord::ClaimVector(v) = &record
                && let Err(
                    e @ (StoreError::InvalidVector(_)
                    | StoreError::MissingClaim(_)
                    | StoreError::Validation(_)),
                ) = self.validate_claim_vector(&v.claim_id, &v.values)
            {
                eprintln!("warning: quarantining poisoned vector at {origin}: {e:?}");
                sink.push(&raw.unwrap_or_else(|| record_to_line(&record)));
                stats.quarantined_records += 1;
                continue;
            }
            let claim_id = match &record {
                PersistedRecord::Claim(c) if legacy || !bad_claims.is_empty() => {
                    Some(c.claim_id.clone())
                }
                _ => None,
            };
            let counter = match &record {
                PersistedRecord::Claim(_) => Some(0),
                PersistedRecord::Evidence(_) => Some(1),
                PersistedRecord::Edge(_) => Some(2),
                PersistedRecord::ClaimVector(_) => Some(3),
                PersistedRecord::BatchCommit(_) | PersistedRecord::Tombstone(_) => None,
            };
            let known_claim = claim_id
                .as_ref()
                .is_some_and(|id| self.claims.contains_key(id));
            match self.apply_persisted_record(record) {
                Ok(()) => {
                    match counter {
                        Some(0) => claims_loaded += 1,
                        Some(1) => evidence_loaded += 1,
                        Some(2) => edges_loaded += 1,
                        Some(3) => vectors_loaded += 1,
                        _ => {}
                    }
                    if let Some(id) = &claim_id {
                        bad_claims.remove(id);
                    }
                }
                Err(
                    e @ (StoreError::Validation(_)
                    | StoreError::MissingClaim(_)
                    | StoreError::InvalidVector(_)),
                ) if lenient && legacy => {
                    eprintln!("warning: quarantining legacy record at {origin}: {e:?}");
                    if let Some(line) = &raw {
                        sink.push(line);
                    }
                    stats.quarantined_records += 1;
                    if let Some(id) = claim_id
                        && !known_claim
                    {
                        bad_claims.insert(id);
                    }
                }
                Err(e) => return Err(hard(e, &origin)),
            }
        }
        sink.flush()?;
        Ok((
            StoreLoadStats {
                replay: stats,
                claims_loaded,
                evidence_loaded,
                edges_loaded,
                vectors_loaded,
                vector_index: VectorIndexRestore::NotConfigured,
            },
            caught_up,
        ))
    }

    pub fn ingest_bundle(
        &mut self,
        claim: Claim,
        evidence: Vec<Evidence>,
        edges: Vec<ClaimEdge>,
    ) -> Result<(), StoreError> {
        self.validate_bundle(&claim, &evidence, &edges)?;
        self.apply_bundle(claim, evidence, edges)
    }

    pub fn ingest_bundle_persistent(
        &mut self,
        wal: &mut FileWal,
        claim: Claim,
        evidence: Vec<Evidence>,
        edges: Vec<ClaimEdge>,
    ) -> Result<(), StoreError> {
        self.validate_bundle(&claim, &evidence, &edges)?;

        wal.append_claim(&claim)?;
        for evd in &evidence {
            wal.append_evidence(evd)?;
        }
        for edge in &edges {
            wal.append_edge(edge)?;
        }

        self.apply_bundle(claim, evidence, edges)
    }

    /// `true` when applying this exact bundle (and vector) would not change
    /// the store: the claim, every evidence row, every edge and the vector
    /// are already present and identical. Used to make retries of an
    /// ambiguous request a no-op (DATA-10, DATA-12).
    pub fn bundle_already_applied(
        &self,
        claim: &Claim,
        evidence: &[Evidence],
        edges: &[ClaimEdge],
        vector: Option<&[f32]>,
    ) -> bool {
        if self.claims.get(&claim.claim_id) != Some(claim) {
            return false;
        }
        let stored_evidence = self.evidence_by_claim.get(&claim.claim_id);
        for evd in evidence {
            let found = stored_evidence.is_some_and(|list| {
                list.iter()
                    .any(|e| e.evidence_id == evd.evidence_id && e == evd)
            });
            if !found {
                return false;
            }
        }
        let stored_edges = self.edges_by_claim.get(&claim.claim_id);
        for edge in edges {
            let found = stored_edges.is_some_and(|list| {
                list.iter().any(|e| {
                    e.to_claim_id == edge.to_claim_id && e.relation == edge.relation && e == edge
                })
            });
            if !found {
                return false;
            }
        }
        match vector {
            Some(values) => self
                .claim_vectors
                .get(&claim.claim_id)
                .is_some_and(|stored| stored.as_slice() == values),
            None => true,
        }
    }

    /// Validate a vector for a claim that may not exist yet: non-empty,
    /// finite and matching the tenant's established dimension.
    fn validate_vector_for_tenant(
        &self,
        tenant_id: &str,
        vector: &[f32],
    ) -> Result<(), StoreError> {
        validate_vector(vector)?;
        if let Some(existing_dim) = self.tenant_vector_dims.get(tenant_id)
            && *existing_dim != vector.len()
        {
            return Err(StoreError::InvalidVector(format!(
                "vector dimension mismatch for tenant '{tenant_id}': expected {existing_dim}, got {}",
                vector.len()
            )));
        }
        Ok(())
    }

    /// Run `apply` against the in-memory state with disk writes deferred,
    /// then replay them onto redb. A disk failure drops the handle and
    /// marks the disk `Unavailable` (the WAL is the source of truth) and is
    /// reported as `Ok(Some(reason))`.
    fn apply_with_deferred_disk<F>(&mut self, apply: F) -> Result<Option<String>, StoreError>
    where
        F: FnOnce(&mut Self) -> Result<(), StoreError>,
    {
        let disk = self.disk.take();
        let previous_staging = self.staged_disk_ops.replace(Vec::new());
        let result = apply(self);
        let ops = self.staged_disk_ops.take().unwrap_or_default();
        self.staged_disk_ops = previous_staging;
        self.disk = disk;
        let mut disk_failure = None;
        if let Some(disk) = self.disk.clone()
            && let Err(reason) = disk.write_ops(&ops)
        {
            self.disk = None;
            self.disk_status = disk::DiskStatus::Unavailable {
                reason: reason.clone(),
            };
            disk_failure = Some(reason);
        }
        result.map(|()| disk_failure)
    }

    /// Atomically ingest one bundle (claim, evidence, edges and optional
    /// vector) as a single WAL commit group (DATA-10).
    ///
    /// Everything is validated first; the WAL group (`begin` marker, the
    /// records, closing commit marker) is appended next and rolled back on
    /// failure; only then is the in-memory state (and redb) updated. A
    /// crash inside the group leaves a torn group that replay discards, so
    /// a partial bundle can never be observed. Nothing is written when the
    /// bundle is already fully applied (an idempotent retry).
    pub fn ingest_atomic_persistent(
        &mut self,
        wal: &mut FileWal,
        claim: Claim,
        evidence: Vec<Evidence>,
        edges: Vec<ClaimEdge>,
        vector: Option<Vec<f32>>,
        ts_unix_ms: u64,
    ) -> Result<AtomicIngestOutcome, StoreError> {
        let Some(prepared) =
            self.prepare_atomic_ingest(claim, evidence, edges, vector, ts_unix_ms)?
        else {
            return Ok(AtomicIngestOutcome {
                applied: false,
                disk_error: None,
            });
        };
        wal.append_group_lines(prepared.wal_lines())?;
        self.apply_prepared_ingest(prepared)
    }

    /// First half of [`InMemoryStore::ingest_atomic_persistent`]: validates
    /// the bundle against the current state and encodes its WAL commit group
    /// without writing anything. Returns `None` when the bundle is already
    /// fully applied (an idempotent retry needs no WAL write).
    pub fn prepare_atomic_ingest(
        &self,
        claim: Claim,
        evidence: Vec<Evidence>,
        edges: Vec<ClaimEdge>,
        vector: Option<Vec<f32>>,
        ts_unix_ms: u64,
    ) -> Result<Option<PreparedIngest>, StoreError> {
        self.validate_bundle(&claim, &evidence, &edges)?;
        if let Some(values) = vector.as_deref() {
            self.validate_vector_for_tenant(&claim.tenant_id, values)?;
        }
        if self.bundle_already_applied(&claim, &evidence, &edges, vector.as_deref()) {
            return Ok(None);
        }

        let claim_id = claim.claim_id.clone();
        let mut records = Vec::with_capacity(evidence.len() + edges.len() + 4);
        records.push(PersistedRecord::BatchCommit(BatchCommitRecord {
            commit_id: format!("{GROUP_BEGIN_PREFIX}{claim_id}"),
            batch_size: 0,
            ts_unix_ms,
            claim_ids: Vec::new(),
        }));
        records.push(PersistedRecord::Claim(claim.clone()));
        records.extend(evidence.iter().cloned().map(PersistedRecord::Evidence));
        records.extend(edges.iter().cloned().map(PersistedRecord::Edge));
        if let Some(values) = vector.as_ref() {
            records.push(PersistedRecord::ClaimVector(ClaimVectorRecord {
                claim_id: claim_id.clone(),
                values: values.clone(),
            }));
        }
        records.push(PersistedRecord::BatchCommit(BatchCommitRecord {
            commit_id: format!("{SINGLE_TX_PREFIX}{claim_id}"),
            batch_size: 1,
            ts_unix_ms,
            claim_ids: vec![claim_id],
        }));
        let wal_lines = records.iter().map(record_to_line).collect();
        Ok(Some(PreparedIngest {
            claim,
            evidence,
            edges,
            vector,
            wal_lines,
        }))
    }

    /// Second half of [`InMemoryStore::ingest_atomic_persistent`]: applies a
    /// prepared bundle to memory (and redb) after its WAL lines are durable.
    /// Callers must apply prepared ingests in WAL order and must not let a
    /// conflicting write (same claim, edge target or a first vector for the
    /// tenant) reach the store between prepare and apply.
    pub fn apply_prepared_ingest(
        &mut self,
        prepared: PreparedIngest,
    ) -> Result<AtomicIngestOutcome, StoreError> {
        let PreparedIngest {
            claim,
            evidence,
            edges,
            vector,
            ..
        } = prepared;
        let claim_id = claim.claim_id.clone();
        let disk_error = self.apply_with_deferred_disk(|store| {
            store.apply_bundle(claim, evidence, edges)?;
            if let Some(values) = vector {
                store.apply_claim_vector(&claim_id, values)?;
            }
            Ok(())
        })?;
        Ok(AtomicIngestOutcome {
            applied: true,
            disk_error,
        })
    }

    /// The vector dimension established for `tenant_id`, if any.
    pub fn tenant_vector_dim(&self, tenant_id: &str) -> Option<usize> {
        self.tenant_vector_dims.get(tenant_id).copied()
    }

    pub fn ingest_bundle_persistent_with_policy(
        &mut self,
        wal: &mut FileWal,
        policy: &CheckpointPolicy,
        claim: Claim,
        evidence: Vec<Evidence>,
        edges: Vec<ClaimEdge>,
    ) -> Result<Option<WalCheckpointStats>, StoreError> {
        self.ingest_bundle_persistent(wal, claim, evidence, edges)?;
        if self.should_checkpoint(wal, policy)? {
            let stats = self.checkpoint_and_compact(wal)?;
            Ok(Some(stats))
        } else {
            Ok(None)
        }
    }

    pub fn upsert_claim_vector(
        &mut self,
        claim_id: &str,
        vector: Vec<f32>,
    ) -> Result<(), StoreError> {
        self.apply_claim_vector(claim_id, vector)
    }

    pub fn upsert_claim_vector_persistent(
        &mut self,
        wal: &mut FileWal,
        claim_id: &str,
        vector: Vec<f32>,
    ) -> Result<(), StoreError> {
        // Validate (claim exists, finite, dimension) BEFORE the WAL append
        // so a rejected request can never poison replay (DATA-03).
        self.validate_claim_vector(claim_id, &vector)?;
        wal.append_claim_vector(claim_id, &vector)?;
        self.apply_claim_vector(claim_id, vector)
    }

    pub fn checkpoint_and_compact(
        &self,
        wal: &mut FileWal,
    ) -> Result<WalCheckpointStats, StoreError> {
        let records = self.snapshot_records();
        wal.compact_with_snapshot(&records)
    }

    pub fn observe_batch_commit(
        &mut self,
        commit_id: &str,
        batch_size: usize,
        ts_unix_ms: u64,
        claim_ids: &[String],
    ) -> Result<(), StoreError> {
        self.apply_batch_commit_record(BatchCommitRecord {
            commit_id: commit_id.to_string(),
            batch_size,
            ts_unix_ms,
            claim_ids: claim_ids.to_vec(),
        })
    }

    pub fn batch_commit_metadata(&self, commit_id: &str) -> Option<&BatchCommitMetadata> {
        self.batch_commits.get(commit_id)
    }

    pub fn apply_persisted_record_line(&mut self, line: &str) -> Result<(), StoreError> {
        self.apply_persisted_record(line_to_record(line)?)
    }

    /// Applies one replicated line the way lenient WAL replay would: a legacy
    /// record that cannot be parsed or validated, a poisoned vector, and a
    /// record depending on a skipped legacy claim are skipped (`Ok(false)`)
    /// instead of failing, so a follower converges to the leader's lenient
    /// replay state rather than wedging. Everything else behaves like
    /// [`InMemoryStore::apply_persisted_record_line`]; `Ok(true)` means the
    /// line was applied.
    pub fn apply_persisted_record_line_lenient(&mut self, line: &str) -> Result<bool, StoreError> {
        let kind = line.split('\t').next().unwrap_or_default();
        let legacy = matches!(kind, "C" | "E" | "G" | "V" | "B");
        let record = match line_to_record(line) {
            Ok(record) => record,
            Err(err) => {
                if !legacy {
                    return Err(err);
                }
                eprintln!("warning: replication skipping unreadable legacy record: {err:?}");
                if kind == "C"
                    && let Some(id) = line.split('\t').nth(1)
                    && let Ok(id) = wal::unescape_field(id)
                {
                    self.replica_skipped_claims.insert(id);
                }
                return Ok(false);
            }
        };
        let depends = match &record {
            PersistedRecord::Evidence(e) => self.replica_skipped_claims.contains(&e.claim_id),
            PersistedRecord::Edge(e) => {
                self.replica_skipped_claims.contains(&e.from_claim_id)
                    || self.replica_skipped_claims.contains(&e.to_claim_id)
            }
            PersistedRecord::ClaimVector(v) => self.replica_skipped_claims.contains(&v.claim_id),
            PersistedRecord::Claim(_)
            | PersistedRecord::BatchCommit(_)
            | PersistedRecord::Tombstone(_) => false,
        };
        if depends {
            return Ok(false);
        }
        if let PersistedRecord::ClaimVector(v) = &record
            && let Err(
                StoreError::InvalidVector(_)
                | StoreError::MissingClaim(_)
                | StoreError::Validation(_),
            ) = self.validate_claim_vector(&v.claim_id, &v.values)
        {
            eprintln!(
                "warning: replication skipping poisoned vector for '{}'",
                v.claim_id
            );
            return Ok(false);
        }
        let claim_id = match &record {
            PersistedRecord::Claim(c) => Some(c.claim_id.clone()),
            _ => None,
        };
        let known_claim = claim_id
            .as_ref()
            .is_some_and(|id| self.claims.contains_key(id));
        match self.apply_persisted_record(record) {
            Ok(()) => {
                if let Some(id) = claim_id {
                    self.replica_skipped_claims.remove(&id);
                }
                Ok(true)
            }
            Err(
                err @ (StoreError::Validation(_)
                | StoreError::MissingClaim(_)
                | StoreError::InvalidVector(_)),
            ) if legacy => {
                eprintln!(
                    "warning: replication skipping legacy record that fails validation: {err:?}"
                );
                if let Some(id) = claim_id
                    && !known_claim
                {
                    self.replica_skipped_claims.insert(id);
                }
                Ok(false)
            }
            Err(err) => Err(err),
        }
    }

    /// Applies a run of replicated lines (one delta frame) to this store in
    /// place, each the way [`Self::apply_persisted_record_line_lenient`]
    /// does, with the redb writes of the whole run in one transaction.
    ///
    /// Unlike staging on [`Self::clone_detached`] this costs time and memory
    /// proportional to the frame, not to the store. Every line is parsed
    /// before anything is applied, so a frame with an unreadable record is
    /// rejected untouched. A record that parses but cannot be applied (the
    /// follower diverged from the leader) fails the call after a prefix of
    /// the frame was applied in memory; redb is not written then. The
    /// caller must restore its state in that case (reload from its WAL,
    /// which does not hold the frame yet, or resync).
    ///
    /// With `keep_wal_events == false` the in-memory event ring is left as
    /// it was (the retrieval follower does not serve the event stream).
    pub fn apply_replicated_lines(
        &mut self,
        lines: &[String],
        keep_wal_events: bool,
    ) -> Result<ReplicatedApply, StoreError> {
        for line in lines {
            wal::check_replicated_line(line)?;
        }
        let saved_ring = (!keep_wal_events).then(|| std::mem::take(&mut self.wal));
        let disk = self.disk.take();
        let previous_staging = self.staged_disk_ops.replace(Vec::new());
        let mut outcome = ReplicatedApply::default();
        let result = (|| {
            for line in lines {
                if self.apply_persisted_record_line_lenient(line)? {
                    outcome.applied += 1;
                } else {
                    outcome.skipped += 1;
                }
            }
            Ok::<(), StoreError>(())
        })();
        let ops = self.staged_disk_ops.take().unwrap_or_default();
        self.staged_disk_ops = previous_staging;
        self.disk = disk;
        if let Some(mut ring) = saved_ring {
            ring.total = ring.total.max(self.wal.total);
            self.wal = ring;
        }
        result?;
        if let Some(disk) = self.disk.clone() {
            if let Err(reason) = disk.write_ops(&ops) {
                self.disk = None;
                self.disk_status = disk::DiskStatus::Unavailable {
                    reason: reason.clone(),
                };
                outcome.disk_error = Some(reason);
            }
        } else if let Some(buffer) = self.staged_disk_ops.as_mut() {
            buffer.extend(ops);
        }
        Ok(outcome)
    }

    pub fn retrieve(&self, req: &RetrievalRequest) -> Vec<RetrievalResult> {
        self.retrieve_with_time_range_and_query_vector(req, None, None, None)
    }

    /// Semantic-first retrieval. Takes a pre-computed embedding of the
    /// query and uses it as the primary ranking signal (cosine in
    /// `[-1, 1]`, mapped to `[0, 1]`); the lexical+BM25 score becomes a
    /// small tie-breaker. When the query vector is the same shape as
    /// the stored vectors for the tenant, this is the recommended
    /// retrieval entry point — the lexical fallback is for environments
    /// that haven't yet wired up an embedding model.
    pub fn retrieve_semantic(
        &self,
        req: &RetrievalRequest,
        query_vector: &[f32],
    ) -> Vec<RetrievalResult> {
        self.retrieve_with_time_range_and_query_vector(req, None, None, Some(query_vector))
    }

    pub fn retrieve_with_time_range(
        &self,
        req: &RetrievalRequest,
        from_unix: Option<i64>,
        to_unix: Option<i64>,
    ) -> Vec<RetrievalResult> {
        self.retrieve_with_time_range_and_query_vector(req, from_unix, to_unix, None)
    }

    pub fn retrieve_with_time_range_and_query_vector(
        &self,
        req: &RetrievalRequest,
        from_unix: Option<i64>,
        to_unix: Option<i64>,
        query_vector: Option<&[f32]>,
    ) -> Vec<RetrievalResult> {
        self.retrieve_with_time_range_query_vector_and_allowed_claim_ids(
            req,
            from_unix,
            to_unix,
            query_vector,
            None,
        )
    }

    pub fn retrieve_with_time_range_query_vector_and_allowed_claim_ids(
        &self,
        req: &RetrievalRequest,
        from_unix: Option<i64>,
        to_unix: Option<i64>,
        query_vector: Option<&[f32]>,
        allowed_claim_ids: Option<&HashSet<String>>,
    ) -> Vec<RetrievalResult> {
        let candidates = self.candidate_claim_ids(
            &req.tenant_id,
            &req.query,
            (from_unix, to_unix),
            query_vector,
            req.top_k,
            allowed_claim_ids,
        );
        self.score_and_rank_candidate_claim_ids(req, query_vector, candidates)
    }

    /// Like [`Self::retrieve_with_time_range_query_vector_and_allowed_claim_ids`]
    /// but also returns the number of candidates scanned, so callers do not
    /// need a second candidate pass just to report the count.
    pub fn retrieve_with_candidate_count_query_vector_and_allowed_claim_ids(
        &self,
        req: &RetrievalRequest,
        from_unix: Option<i64>,
        to_unix: Option<i64>,
        query_vector: Option<&[f32]>,
        allowed_claim_ids: Option<&HashSet<String>>,
    ) -> (Vec<RetrievalResult>, usize) {
        let candidates = self.candidate_claim_ids(
            &req.tenant_id,
            &req.query,
            (from_unix, to_unix),
            query_vector,
            req.top_k,
            allowed_claim_ids,
        );
        let candidate_count = candidates.len();
        (
            self.score_and_rank_candidate_claim_ids(req, query_vector, candidates),
            candidate_count,
        )
    }

    pub fn retrieve_with_time_range_query_vector_and_explicit_candidate_claim_ids(
        &self,
        req: &RetrievalRequest,
        from_unix: Option<i64>,
        to_unix: Option<i64>,
        query_vector: Option<&[f32]>,
        candidate_claim_ids: &HashSet<String>,
        allowed_claim_ids: Option<&HashSet<String>>,
    ) -> Vec<RetrievalResult> {
        let mut candidates: Vec<String> = candidate_claim_ids
            .iter()
            .filter_map(|claim_id| {
                let claim = self.claims.get(claim_id)?;
                if claim.tenant_id != req.tenant_id {
                    return None;
                }
                if !claim_matches_time_range(claim, from_unix, to_unix) {
                    return None;
                }
                if let Some(allowed_ids) = allowed_claim_ids
                    && !allowed_ids.contains(claim_id.as_str())
                {
                    return None;
                }
                Some(claim_id.clone())
            })
            .collect();
        candidates.sort_unstable();
        self.score_and_rank_candidate_claim_ids(req, query_vector, candidates)
    }

    fn score_and_rank_candidate_claim_ids(
        &self,
        req: &RetrievalRequest,
        query_vector: Option<&[f32]>,
        candidates: Vec<String>,
    ) -> Vec<RetrievalResult> {
        // DATA-05: a malformed query vector (non-finite, zero norm, wrong
        // dimension) must not silently degrade into NaN scores or a
        // cross-dimension scan. Callers wanting a precise error use
        // `validate_query_vector`.
        if let Some(vector) = query_vector
            && self.validate_query_vector(&req.tenant_id, vector).is_err()
        {
            return Vec::new();
        }
        let mut ranked: Vec<RetrievalResult> = Vec::new();
        let bm25_context = self.bm25_context_for_tenant(&req.tenant_id, &req.query);
        let dense_similarities = query_vector.map(|vector| {
            let candidate_vectors: Vec<(String, &[f32])> = candidates
                .iter()
                .filter_map(|claim_id| {
                    let claim = self.claims.get(claim_id)?;
                    if claim.tenant_id != req.tenant_id {
                        return None;
                    }
                    let claim_vector = self.claim_vectors.get(claim_id)?;
                    Some((claim_id.clone(), claim_vector.as_slice()))
                })
                .collect();
            self.score_query_candidate_vectors(vector, candidate_vectors)
                .into_iter()
                .collect::<HashMap<String, f32>>()
        });

        for claim_id in candidates {
            let Some(claim) = self.claims.get(&claim_id) else {
                continue;
            };

            // Borrow, never clone, on the scoring path (PERF-02).
            let evidence: &[Evidence] = self
                .evidence_by_claim
                .get(&claim.claim_id)
                .map(Vec::as_slice)
                .unwrap_or(&[]);

            // IDX-05: an edge `from --supports--> to` supports the TARGET
            // claim. Only edges pointing at this claim count, both
            // endpoints must exist in this claim's tenant, and
            // self-edges are ignored.
            let edge_summary = summarize_incoming_edges(
                self.edges_in
                    .get(&claim.claim_id)
                    .into_iter()
                    .flatten()
                    .filter(|edge| {
                        edge.from_claim_id != claim.claim_id
                            && self
                                .claims
                                .get(&edge.from_claim_id)
                                .is_some_and(|source| source.tenant_id == claim.tenant_id)
                    })
                    .map(|edge| (edge.from_claim_id.as_str(), &edge.relation, edge.strength)),
            );

            let mut evidence_supports = 0usize;
            let mut evidence_contradicts = 0usize;
            let mut support_sources: HashSet<&str> = HashSet::new();
            let mut contradiction_sources: HashSet<&str> = HashSet::new();
            for evd in evidence {
                match evd.stance {
                    Stance::Supports => {
                        evidence_supports += 1;
                        support_sources.insert(evd.source_id.as_str());
                    }
                    Stance::Contradicts => {
                        evidence_contradicts += 1;
                        contradiction_sources.insert(evd.source_id.as_str());
                    }
                    Stance::Neutral => {}
                }
            }
            // Reported counts are raw (evidence rows + distinct edge
            // sources); the ranking signal counts each distinct
            // source_id once and is saturated inside `ranking`.
            let supports = evidence_supports + edge_summary.supports;
            let contradicts = evidence_contradicts + edge_summary.contradicts;
            let signal_supports = support_sources.len() + edge_summary.supports;
            let signal_contradicts = contradiction_sources.len() + edge_summary.contradicts;

            if matches!(req.stance_mode, StanceMode::SupportOnly) && contradicts > supports {
                continue;
            }

            let avg_quality = if evidence.is_empty() {
                0.0
            } else {
                evidence.iter().map(|e| e.source_quality).sum::<f32>() / evidence.len() as f32
            };

            let bm25 = self
                .claim_tokens
                .get(&claim.claim_id)
                .map(|tokens| {
                    bm25_score(
                        &req.query,
                        tokens,
                        &bm25_context.doc_freq,
                        bm25_context.total_docs,
                        bm25_context.avg_doc_len,
                    )
                })
                .unwrap_or(0.0);

            let dense_similarity = dense_similarities
                .as_ref()
                .and_then(|scores| scores.get(&claim.claim_id))
                .copied()
                .unwrap_or(0.0);

            let lexical_score = score_claim_with_bm25(
                &req.query,
                claim,
                avg_quality,
                RankSignals {
                    supports: signal_supports,
                    contradicts: signal_contradicts,
                },
                bm25,
            );

            let score = if query_vector.is_some() {
                // Semantic-first retrieval: dense similarity is the
                // PRIMARY signal (cosine in [-1, 1] -> mapped to
                // [0, 1] via the embedding backend). The lexical/BM25
                // score is a small tie-breaker when dense similarities
                // are tied. This replaces the historical 0.35 additive
                // weight with semantic-primary scoring, which is the
                // right default when the caller explicitly provides
                // a query vector.
                let dense_primary = (dense_similarity + 1.0) * 0.5;
                dense_primary + (lexical_score * 0.1)
            } else {
                // Lexical-only retrieval: historical behavior
                // (dense_similarity is 0.0 when no query_vector).
                lexical_score + (dense_similarity * 0.35)
            };

            let citations = evidence
                .iter()
                .map(|e| Citation {
                    evidence_id: e.evidence_id.clone(),
                    source_id: e.source_id.clone(),
                    stance: e.stance.clone(),
                    source_quality: e.source_quality,
                    chunk_id: e.chunk_id.clone(),
                    span_start: e.span_start,
                    span_end: e.span_end,
                    doc_id: e.doc_id.clone(),
                    extraction_model: e.extraction_model.clone(),
                    ingested_at: e.ingested_at,
                })
                .collect();
            ranked.push(RetrievalResult {
                claim_id: claim.claim_id.clone(),
                canonical_text: claim.canonical_text.clone(),
                score,
                supports,
                contradicts,
                citations,
            });
        }

        ranked.sort_by(|a, b| b.score.total_cmp(&a.score));
        ranked.into_iter().take(req.top_k).collect()
    }

    /// All claims of a tenant, sorted by `claim_id`. Uses the per-tenant
    /// claim-id index, so the cost is O(tenant), not O(all claims)
    /// (PERF-02).
    pub fn claims_for_tenant(&self, tenant_id: &str) -> Vec<Claim> {
        let mut out: Vec<Claim> = self.tenant_claims(tenant_id).cloned().collect();
        out.sort_unstable_by(|a, b| a.claim_id.cmp(&b.claim_id));
        out
    }

    /// Number of claims owned by `tenant_id` (O(1)).
    pub fn claim_count_for_tenant(&self, tenant_id: &str) -> usize {
        self.tenant_claim_ids
            .get(tenant_id)
            .map(HashSet::len)
            .unwrap_or(0)
    }

    /// Borrowing iterator over a tenant's claims, in unspecified order.
    fn tenant_claims<'a>(&'a self, tenant_id: &str) -> impl Iterator<Item = &'a Claim> + 'a {
        self.tenant_claim_ids
            .get(tenant_id)
            .into_iter()
            .flatten()
            .filter_map(|claim_id| self.claims.get(claim_id))
    }

    pub fn tenant_ids(&self) -> Vec<String> {
        let mut out: Vec<String> = self.tenant_claim_ids.keys().cloned().collect();
        out.sort_unstable();
        out
    }

    pub fn claim_ids_for_tenant(&self, tenant_id: &str) -> HashSet<String> {
        self.tenant_claim_ids
            .get(tenant_id)
            .cloned()
            .unwrap_or_default()
    }

    pub fn claim_by_id(&self, claim_id: &str) -> Option<&Claim> {
        self.claims.get(claim_id)
    }

    pub fn claim_ids_for_entity(&self, tenant_id: &str, entity: &str) -> HashSet<String> {
        let key = normalize_index_key(entity);
        if key.is_empty() {
            return HashSet::new();
        }
        self.entity_index
            .get(tenant_id)
            .and_then(|index| index.get(&key))
            .cloned()
            .unwrap_or_default()
    }

    pub fn claim_ids_for_embedding_id(
        &self,
        tenant_id: &str,
        embedding_id: &str,
    ) -> HashSet<String> {
        let key = embedding_id.trim();
        if key.is_empty() {
            return HashSet::new();
        }
        self.embedding_index
            .get(tenant_id)
            .and_then(|index| index.get(key))
            .cloned()
            .unwrap_or_default()
    }

    pub fn edges_for_claim(&self, claim_id: &str) -> Vec<ClaimEdge> {
        self.edges_by_claim
            .get(claim_id)
            .cloned()
            .unwrap_or_default()
    }

    /// Evidence rows attached to `claim_id`, in stored order.
    pub fn evidence_for_claim(&self, claim_id: &str) -> Vec<Evidence> {
        self.evidence_by_claim
            .get(claim_id)
            .cloned()
            .unwrap_or_default()
    }

    pub fn claims_for_entity(&self, tenant_id: &str, entity: &str) -> Vec<Claim> {
        let mut out: Vec<Claim> = self
            .claim_ids_for_entity(tenant_id, entity)
            .iter()
            .filter_map(|id| self.claims.get(id).cloned())
            .collect();
        out.sort_by(|a, b| a.claim_id.cmp(&b.claim_id));
        out
    }

    pub fn index_stats(&self) -> StoreIndexStats {
        let inverted_terms = self
            .inverted_index
            .values()
            .map(|tenant_index| tenant_index.len())
            .sum();
        let entity_terms = self
            .entity_index
            .values()
            .map(|tenant_index| tenant_index.len())
            .sum();
        let temporal_buckets = self
            .temporal_index
            .values()
            .map(|timeline| timeline.len())
            .sum();
        let ann_vector_buckets = self.vector_indexes.values().map(|index| index.len()).sum();
        let vector_index_bytes = self
            .vector_indexes
            .values()
            .map(|index| index.heap_bytes())
            .sum();
        StoreIndexStats {
            tenant_count: self.tenant_claim_ids.len(),
            claim_count: self.claims.len(),
            vector_count: self.claim_vectors.len(),
            inverted_terms,
            entity_terms,
            temporal_buckets,
            ann_vector_buckets,
            vector_index_bytes,
        }
    }

    pub fn candidate_count(
        &self,
        tenant_id: &str,
        query: &str,
        from_unix: Option<i64>,
        to_unix: Option<i64>,
    ) -> usize {
        self.candidate_claim_ids(tenant_id, query, (from_unix, to_unix), None, 5, None)
            .len()
    }

    pub fn candidate_count_for_retrieval_request(&self, req: &RetrievalRequest) -> usize {
        self.candidate_count_with_query_vector(req, None, None, None)
    }

    pub fn candidate_count_with_query_vector(
        &self,
        req: &RetrievalRequest,
        query_vector: Option<&[f32]>,
        from_unix: Option<i64>,
        to_unix: Option<i64>,
    ) -> usize {
        self.candidate_count_with_query_vector_and_allowed_claim_ids(
            req,
            query_vector,
            (from_unix, to_unix),
            None,
        )
    }

    pub fn candidate_count_with_query_vector_and_allowed_claim_ids(
        &self,
        req: &RetrievalRequest,
        query_vector: Option<&[f32]>,
        time_range: (Option<i64>, Option<i64>),
        allowed_claim_ids: Option<&HashSet<String>>,
    ) -> usize {
        let (from_unix, to_unix) = time_range;
        self.candidate_claim_ids(
            &req.tenant_id,
            &req.query,
            (from_unix, to_unix),
            query_vector,
            req.top_k,
            allowed_claim_ids,
        )
        .len()
    }

    pub fn ann_candidate_count_for_query_vector(
        &self,
        tenant_id: &str,
        query_vector: &[f32],
        top_k: usize,
    ) -> usize {
        if query_vector.is_empty() {
            return 0;
        }
        let vector_top_n = (top_k.saturating_mul(20)).clamp(100, 5000);
        self.vector_candidates(tenant_id, query_vector, vector_top_n, (None, None), None)
            .len()
    }

    pub fn ann_vector_top_candidates(
        &self,
        tenant_id: &str,
        query_vector: &[f32],
        top_n: usize,
    ) -> Vec<String> {
        if query_vector.is_empty() || top_n == 0 {
            return Vec::new();
        }
        self.vector_candidates(tenant_id, query_vector, top_n, (None, None), None)
    }

    /// Like [`Self::ann_vector_top_candidates`] but only claims matching
    /// `time_range` and (when given) `allowed_claim_ids` are candidates, so
    /// the result is the `top_n` best of the ALLOWED set rather than the
    /// allowed part of the global top `top_n`. Small allowed sets are scanned
    /// exactly (see the strategy note on `vector_candidates`).
    pub fn ann_vector_top_candidates_filtered(
        &self,
        tenant_id: &str,
        query_vector: &[f32],
        top_n: usize,
        time_range: (Option<i64>, Option<i64>),
        allowed_claim_ids: Option<&HashSet<String>>,
    ) -> Vec<String> {
        if query_vector.is_empty() || top_n == 0 {
            return Vec::new();
        }
        self.vector_candidates(
            tenant_id,
            query_vector,
            top_n,
            time_range,
            allowed_claim_ids,
        )
    }

    pub fn exact_vector_top_candidates(
        &self,
        tenant_id: &str,
        query_vector: &[f32],
        top_n: usize,
    ) -> Vec<String> {
        if top_n == 0 || self.validate_query_vector(tenant_id, query_vector).is_err() {
            return Vec::new();
        }

        // Scan only this tenant's claims (IDX-03), never the global
        // vector map.
        let candidate_vectors: Vec<(String, &[f32])> = self
            .tenant_claim_ids
            .get(tenant_id)
            .into_iter()
            .flatten()
            .filter_map(|claim_id| {
                let vector = self.claim_vectors.get(claim_id)?;
                Some((claim_id.clone(), vector.as_slice()))
            })
            .collect();
        let mut scored = self.score_query_candidate_vectors(query_vector, candidate_vectors);
        scored.sort_by(|a, b| b.1.total_cmp(&a.1));
        scored
            .into_iter()
            .take(top_n)
            .map(|(claim_id, _)| claim_id)
            .collect()
    }

    /// Number of events currently buffered in the in-memory event ring.
    /// The ring is bounded (see [`DEFAULT_WAL_EVENT_CAPACITY`]), so this
    /// never exceeds the configured capacity; use
    /// [`InMemoryStore::wal_events_total`] for the monotonic count.
    pub fn wal_len(&self) -> usize {
        self.wal.events.len()
    }

    /// Monotonic number of events ever applied to this store instance.
    pub fn wal_events_total(&self) -> u64 {
        self.wal.total
    }

    /// Maximum number of events retained in the in-memory ring.
    pub fn wal_event_capacity(&self) -> usize {
        self.wal.capacity
    }

    /// Change the in-memory event ring capacity (0 keeps nothing).
    /// Shrinking drops the oldest buffered events immediately.
    pub fn set_wal_event_capacity(&mut self, capacity: usize) {
        self.wal.capacity = capacity;
        self.wal.trim();
    }

    /// Clear the in-memory WAL event buffer without affecting the
    /// stored claims/evidence/edges. This is used by follower replicas
    /// that apply replicated records to an in-memory store but never
    /// truncate their own WAL file.
    pub fn clear_wal_events(&mut self) {
        self.wal.events.clear();
    }

    pub fn claims_len(&self) -> usize {
        self.claims.len()
    }

    // ----------------------------------------------------------------
    // Internal iterators used by the disk module's checkpoint path.
    // `pub(crate)` so the disk module can read every record but the
    // public API stays unchanged.
    // ----------------------------------------------------------------

    pub(crate) fn claims_iter(&self) -> impl Iterator<Item = &Claim> {
        self.claims.values()
    }

    pub(crate) fn evidence_iter(&self) -> impl Iterator<Item = (&str, &Vec<Evidence>)> {
        self.evidence_by_claim.iter().map(|(k, v)| (k.as_str(), v))
    }

    pub(crate) fn edges_iter(&self) -> impl Iterator<Item = (&str, &Vec<ClaimEdge>)> {
        self.edges_by_claim.iter().map(|(k, v)| (k.as_str(), v))
    }

    pub(crate) fn claim_vectors_iter(&self) -> impl Iterator<Item = (&str, &Vec<f32>)> {
        self.claim_vectors.iter().map(|(k, v)| (k.as_str(), v))
    }

    pub(crate) fn batch_commits_iter(&self) -> impl Iterator<Item = &BatchCommitMetadata> {
        self.batch_commits.values()
    }

    pub(crate) fn tenant_dims_iter(&self) -> impl Iterator<Item = (&str, &usize)> {
        self.tenant_vector_dims.iter().map(|(k, v)| (k.as_str(), v))
    }

    pub(crate) fn tenant_claim_set_iter(&self) -> impl Iterator<Item = (String, String)> {
        self.tenant_claim_ids.iter().flat_map(|(tenant, claims)| {
            claims
                .iter()
                .map(move |claim| (tenant.clone(), claim.clone()))
        })
    }

    fn should_checkpoint(
        &self,
        wal: &FileWal,
        policy: &CheckpointPolicy,
    ) -> Result<bool, StoreError> {
        let record_threshold_met = match policy.max_wal_records {
            Some(threshold) if threshold > 0 => wal.wal_record_count()? >= threshold,
            _ => false,
        };
        let byte_threshold_met = match policy.max_wal_bytes {
            Some(threshold) if threshold > 0 => wal.wal_size_bytes()? >= threshold,
            _ => false,
        };
        Ok(record_threshold_met || byte_threshold_met)
    }

    fn candidate_claim_ids(
        &self,
        tenant_id: &str,
        query: &str,
        time_range: (Option<i64>, Option<i64>),
        query_vector: Option<&[f32]>,
        top_k: usize,
        allowed_claim_ids: Option<&HashSet<String>>,
    ) -> Vec<String> {
        let (from_unix, to_unix) = time_range;
        let mut candidates: HashSet<String> = HashSet::new();
        let query_tokens = tokenize(query);

        if query_tokens.is_empty() {
            if let Some(ids) = self.tenant_claim_ids.get(tenant_id) {
                candidates.extend(ids.iter().cloned());
            }
        } else if let Some(tenant_index) = self.inverted_index.get(tenant_id) {
            for token in query_tokens {
                if let Some(ids) = tenant_index.get(&token) {
                    candidates.extend(ids.iter().cloned());
                }
            }
            if candidates.is_empty()
                && let Some(ids) = self.tenant_claim_ids.get(tenant_id)
            {
                candidates.extend(ids.iter().cloned());
            }
        }

        if let Some(vector) = query_vector {
            let vector_top_n = (top_k.saturating_mul(20)).clamp(100, 5000);
            for claim_id in self.vector_candidates(
                tenant_id,
                vector,
                vector_top_n,
                time_range,
                allowed_claim_ids,
            ) {
                candidates.insert(claim_id);
            }
        }

        if from_unix.is_some() || to_unix.is_some() {
            candidates.retain(|claim_id| {
                self.claims
                    .get(claim_id)
                    .is_some_and(|claim| claim_matches_time_range(claim, from_unix, to_unix))
            });
        }
        if let Some(allowed_ids) = allowed_claim_ids {
            candidates = candidates.intersection(allowed_ids).cloned().collect();
        }

        let mut out: Vec<String> = candidates
            .into_iter()
            .filter(|claim_id| {
                self.claims
                    .get(claim_id)
                    .is_some_and(|claim| claim.tenant_id == tenant_id)
            })
            .collect();
        out.sort_unstable();
        out
    }

    /// Vector candidates for `tenant_id`: the `top_n` claim ids most similar
    /// to `query_vector`, best first, restricted to claims that match
    /// `time_range` and (when given) `allowed_claim_ids`.
    ///
    /// Invalid (empty, non-finite, zero-norm, wrong-dimension) query vectors
    /// yield no candidates: never a fallback scan (IDX-03). The search only
    /// ever touches the tenant's own index.
    ///
    /// Strategy (ADR 0003): a tenant at or below the flat threshold is
    /// scanned exactly. Above it, a filter whose allowed set is no larger
    /// than the threshold is also scanned exactly (filtered HNSW is slower
    /// and less exact on small allowed sets); otherwise the HNSW runs with
    /// the filter as a predicate.
    fn vector_candidates(
        &self,
        tenant_id: &str,
        query_vector: &[f32],
        top_n: usize,
        time_range: (Option<i64>, Option<i64>),
        allowed_claim_ids: Option<&HashSet<String>>,
    ) -> Vec<String> {
        if top_n == 0 || self.validate_query_vector(tenant_id, query_vector).is_err() {
            return Vec::new();
        }
        let Some(index) = self.vector_indexes.get(tenant_id) else {
            return Vec::new();
        };
        let (from_unix, to_unix) = time_range;
        let has_time = from_unix.is_some() || to_unix.is_some();
        let in_range = |claim_id: &str| {
            !has_time
                || self
                    .claims
                    .get(claim_id)
                    .is_some_and(|claim| claim_matches_time_range(claim, from_unix, to_unix))
        };
        let passes = |claim_id: &str| {
            allowed_claim_ids.is_none_or(|allowed| allowed.contains(claim_id)) && in_range(claim_id)
        };

        if (!has_time && allowed_claim_ids.is_none()) || !index.is_hnsw() {
            let filter: Option<&dyn Fn(&str) -> bool> = if has_time || allowed_claim_ids.is_some() {
                Some(&passes)
            } else {
                None
            };
            return index
                .search(query_vector, top_n, filter, &self.claim_vectors)
                .into_iter()
                .map(|(claim_id, _)| claim_id)
                .collect();
        }

        // Large tenant with a filter: size the allowed set first.
        let limit = self.ann_tuning.flat_threshold;
        let exact_ids: Option<Vec<&str>> = match allowed_claim_ids {
            Some(allowed) if allowed.len() <= limit => Some(
                allowed
                    .iter()
                    .map(String::as_str)
                    .filter(|id| index.contains(id) && in_range(id))
                    .collect(),
            ),
            Some(_) => None,
            None => {
                // Time filter only: count matches, stopping once the set is
                // too large to scan.
                let mut matching: Vec<&str> = Vec::new();
                let mut small = true;
                for id in self.tenant_claim_ids.get(tenant_id).into_iter().flatten() {
                    if index.contains(id) && in_range(id) {
                        if matching.len() == limit {
                            small = false;
                            break;
                        }
                        matching.push(id.as_str());
                    }
                }
                small.then_some(matching)
            }
        };
        match exact_ids {
            Some(ids) => exact_top_k(
                query_vector,
                ids.into_iter()
                    .filter_map(|id| self.claim_vectors.get(id).map(|v| (id, v.as_slice()))),
                top_n,
            )
            .into_iter()
            .map(|(claim_id, _)| claim_id.to_string())
            .collect(),
            None => index
                .search(query_vector, top_n, Some(&passes), &self.claim_vectors)
                .into_iter()
                .map(|(claim_id, _)| claim_id)
                .collect(),
        }
    }

    fn score_query_candidate_vectors(
        &self,
        query_vector: &[f32],
        candidate_vectors: Vec<(String, &[f32])>,
    ) -> Vec<(String, f32)> {
        if candidate_vectors.is_empty() || query_vector.is_empty() {
            return Vec::new();
        }

        if self.vector_backend_runtime.is_gpu() {
            #[cfg(feature = "gpu-backend")]
            if let Some(scored) =
                gpu::gpu_score_query_candidate_vectors(query_vector, &candidate_vectors)
            {
                return scored;
            }
        }

        score_query_candidate_vectors_cpu(query_vector, &candidate_vectors)
    }

    fn bm25_context_for_tenant(&self, tenant_id: &str, query: &str) -> Bm25Context {
        let total_docs = self
            .tenant_claim_ids
            .get(tenant_id)
            .map(|ids| ids.len())
            .unwrap_or(0);
        if total_docs == 0 {
            return Bm25Context::default();
        }

        let mut total_len = 0usize;
        for claim_id in self.tenant_claim_ids.get(tenant_id).into_iter().flatten() {
            total_len += self
                .claim_tokens
                .get(claim_id)
                .map(|tokens| tokens.len())
                .unwrap_or(0);
        }
        let avg_doc_len = (total_len as f32 / total_docs as f32).max(1.0);

        let mut doc_freq = HashMap::new();
        if let Some(index) = self.inverted_index.get(tenant_id) {
            for token in tokenize(query) {
                doc_freq.insert(
                    token.clone(),
                    index.get(&token).map(|ids| ids.len()).unwrap_or(0),
                );
            }
        }

        Bm25Context {
            doc_freq,
            total_docs,
            avg_doc_len,
        }
    }

    fn snapshot_records(&self) -> Vec<PersistedRecord> {
        let mut claim_ids: Vec<String> = self.claims.keys().cloned().collect();
        claim_ids.sort_unstable();

        let mut records = Vec::new();
        for claim_id in &claim_ids {
            if let Some(claim) = self.claims.get(claim_id) {
                records.push(PersistedRecord::Claim(claim.clone()));
            }
        }

        for claim_id in &claim_ids {
            if let Some(values) = self.claim_vectors.get(claim_id) {
                records.push(PersistedRecord::ClaimVector(ClaimVectorRecord {
                    claim_id: claim_id.clone(),
                    values: values.clone(),
                }));
            }
        }

        for claim_id in &claim_ids {
            if let Some(evidence) = self.evidence_by_claim.get(claim_id) {
                let mut evidence = evidence.clone();
                evidence.sort_by(|a, b| a.evidence_id.cmp(&b.evidence_id));
                for evd in evidence {
                    records.push(PersistedRecord::Evidence(evd));
                }
            }
        }

        for claim_id in &claim_ids {
            if let Some(edges) = self.edges_by_claim.get(claim_id) {
                let mut edges = edges.clone();
                edges.sort_by(|a, b| a.edge_id.cmp(&b.edge_id));
                for edge in edges {
                    records.push(PersistedRecord::Edge(edge));
                }
            }
        }

        let mut commit_ids: Vec<&String> = self.batch_commits.keys().collect();
        commit_ids.sort_unstable();
        for commit_id in commit_ids {
            let metadata = self
                .batch_commits
                .get(commit_id)
                .expect("batch commit should exist");
            records.push(PersistedRecord::BatchCommit(BatchCommitRecord {
                commit_id: metadata.commit_id.clone(),
                batch_size: metadata.batch_size,
                ts_unix_ms: metadata.ts_unix_ms,
                claim_ids: metadata.claim_ids.clone(),
            }));
        }

        records
    }

    fn validate_bundle(
        &self,
        claim: &Claim,
        evidence: &[Evidence],
        edges: &[ClaimEdge],
    ) -> Result<(), StoreError> {
        self.check_claim_applicable(claim)?;
        for evd in evidence {
            validate_evidence(evd)?;
            if evd.claim_id != claim.claim_id {
                return Err(StoreError::MissingClaim(evd.claim_id.clone()));
            }
        }
        for edge in edges {
            validate_edge(edge)?;
            if edge.from_claim_id != claim.claim_id {
                return Err(StoreError::MissingClaim(edge.from_claim_id.clone()));
            }
        }
        Ok(())
    }

    /// Validate a claim-vector upsert against the current state without
    /// mutating anything. Callers that append to a WAL MUST call this
    /// (or go through `upsert_claim_vector_persistent`) BEFORE the
    /// append so a rejected request never reaches the log (DATA-03).
    ///
    /// Rejects: empty or non-finite vectors, an unknown claim, and a
    /// dimension that differs from the tenant's established dimension.
    pub fn validate_claim_vector(&self, claim_id: &str, vector: &[f32]) -> Result<(), StoreError> {
        validate_vector(vector)?;
        let claim = self
            .claims
            .get(claim_id)
            .ok_or_else(|| StoreError::MissingClaim(claim_id.to_string()))?;
        if let Some(existing_dim) = self.tenant_vector_dims.get(&claim.tenant_id)
            && *existing_dim != vector.len()
        {
            return Err(StoreError::InvalidVector(format!(
                "vector dimension mismatch for tenant '{}': expected {}, got {}",
                claim.tenant_id,
                existing_dim,
                vector.len()
            )));
        }
        Ok(())
    }

    /// Validate a query vector for `tenant_id` (DATA-05, IDX-03): it must
    /// be non-empty, finite, have a non-zero norm, and match the tenant's
    /// vector dimension when the tenant has vectors. Retrieval entry
    /// points return an empty result for an invalid query vector; call
    /// this first to surface a precise error to the client.
    pub fn validate_query_vector(
        &self,
        tenant_id: &str,
        query_vector: &[f32],
    ) -> Result<(), StoreError> {
        validate_vector(query_vector)?;
        let norm_sq: f64 = query_vector
            .iter()
            .map(|v| f64::from(*v) * f64::from(*v))
            .sum();
        if norm_sq <= 0.0 {
            return Err(StoreError::InvalidVector(
                "query vector must have a non-zero norm".to_string(),
            ));
        }
        if let Some(dim) = self.tenant_vector_dims.get(tenant_id)
            && *dim != query_vector.len()
        {
            return Err(StoreError::InvalidVector(format!(
                "query vector dimension mismatch: expected {}, got {}",
                dim,
                query_vector.len()
            )));
        }
        Ok(())
    }

    /// Claim-level checks shared by the pre-WAL validation and the apply
    /// path: field validation plus the cross-tenant `claim_id` conflict.
    /// The conflict error is deliberately generic (SEC-18): it must not
    /// reveal which other tenant owns the id.
    fn check_claim_applicable(&self, claim: &Claim) -> Result<(), StoreError> {
        validate_claim(claim)?;
        if let Some(existing) = self.claims.get(&claim.claim_id)
            && existing.tenant_id != claim.tenant_id
        {
            return Err(StoreError::Conflict("claim_id already exists".to_string()));
        }
        Ok(())
    }

    fn apply_bundle(
        &mut self,
        claim: Claim,
        evidence: Vec<Evidence>,
        edges: Vec<ClaimEdge>,
    ) -> Result<(), StoreError> {
        self.apply_claim(claim)?;
        for evd in evidence {
            self.apply_evidence(evd)?;
        }
        for edge in edges {
            self.apply_edge(edge)?;
        }
        Ok(())
    }

    fn apply_persisted_record(&mut self, record: PersistedRecord) -> Result<(), StoreError> {
        match record {
            PersistedRecord::Claim(claim) => self.apply_claim(claim),
            PersistedRecord::Evidence(evidence) => self.apply_evidence(evidence),
            PersistedRecord::Edge(edge) => self.apply_edge(edge),
            PersistedRecord::ClaimVector(record) => {
                self.apply_claim_vector(&record.claim_id, record.values)
            }
            PersistedRecord::BatchCommit(record) => self.apply_batch_commit_record(record),
            PersistedRecord::Tombstone(record) => {
                self.apply_tombstone(&record.tombstone).map(|_| ())
            }
        }
    }

    /// Mirror one mutation to the disk store. With a disk attached the
    /// write happens immediately (disk BEFORE memory, as before). On a
    /// detached staged clone the op is only recorded; `commit_staged`
    /// replays it after the caller's WAL append (DATA-09). With neither,
    /// this is a no-op.
    fn mirror_op(&mut self, op: StagedDiskOp) -> Result<(), StoreError> {
        if let Some(disk) = self.disk.as_ref() {
            disk.write_ops(std::slice::from_ref(&op))
                .map_err(StoreError::Io)
        } else {
            if let Some(buffer) = self.staged_disk_ops.as_mut() {
                buffer.push(op);
            }
            Ok(())
        }
    }

    fn apply_claim(&mut self, claim: Claim) -> Result<(), StoreError> {
        // Validate first so a rejected claim never touches disk.
        self.check_claim_applicable(&claim)?;
        self.mirror_op(StagedDiskOp::Claim(claim.clone()))?;
        self.apply_claim_inner(claim)
    }

    /// In-memory only — no disk mirror. Used by the disk module's
    /// bulk-load path (`bulk_load_claims_into`) to avoid re-writing
    /// data that's already on disk.
    pub(crate) fn apply_claim_for_load(&mut self, claim: Claim) -> Result<(), StoreError> {
        self.apply_claim_inner(claim)
    }

    fn apply_claim_inner(&mut self, claim: Claim) -> Result<(), StoreError> {
        self.check_claim_applicable(&claim)?;
        let claim_id = claim.claim_id.clone();
        if let Some(previous) = self.claims.get(&claim_id).cloned() {
            // Re-upsert: refresh the text-derived indexes only. The
            // claim's vector and ANN entry are kept (DATA-04); a new
            // vector replaces them only via an explicit vector upsert.
            self.remove_claim_indexes(&previous);
        }
        self.add_claim_indexes(&claim);
        self.claims.insert(claim_id.clone(), claim);
        self.wal.push(WalEvent::ClaimUpsert(claim_id));
        Ok(())
    }

    fn check_evidence_applicable(&self, evidence: &Evidence) -> Result<(), StoreError> {
        validate_evidence(evidence)?;
        if !self.claims.contains_key(&evidence.claim_id) {
            return Err(StoreError::MissingClaim(evidence.claim_id.clone()));
        }
        Ok(())
    }

    fn apply_evidence(&mut self, evidence: Evidence) -> Result<(), StoreError> {
        self.check_evidence_applicable(&evidence)?;
        self.mirror_op(StagedDiskOp::Evidence(evidence.clone()))?;
        self.apply_evidence_inner(evidence)
    }

    /// Apply a pre-built evidence blob to the in-memory state.
    /// No disk mirror. Used by the bulk-load path. Upserts by
    /// `evidence_id`, so a blob that already contains duplicates (from
    /// before DATA-01) is collapsed on load.
    pub(crate) fn apply_evidence_blob_for_load(
        &mut self,
        claim_id: &str,
        evidence: &[Evidence],
    ) -> Result<(), StoreError> {
        if !self.claims.contains_key(claim_id) {
            return Err(StoreError::MissingClaim(claim_id.to_string()));
        }
        let entry = self
            .evidence_by_claim
            .entry(claim_id.to_string())
            .or_default();
        for evd in evidence {
            upsert_evidence(entry, evd.clone());
        }
        Ok(())
    }

    fn apply_evidence_inner(&mut self, evidence: Evidence) -> Result<(), StoreError> {
        self.check_evidence_applicable(&evidence)?;
        let evidence_id = evidence.evidence_id.clone();
        upsert_evidence(
            self.evidence_by_claim
                .entry(evidence.claim_id.clone())
                .or_default(),
            evidence,
        );
        self.wal.push(WalEvent::EvidenceUpsert(evidence_id));
        Ok(())
    }

    fn check_edge_applicable(&self, edge: &ClaimEdge) -> Result<(), StoreError> {
        validate_edge(edge)?;
        if !self.claims.contains_key(&edge.from_claim_id) {
            return Err(StoreError::MissingClaim(edge.from_claim_id.clone()));
        }
        Ok(())
    }

    fn apply_edge(&mut self, edge: ClaimEdge) -> Result<(), StoreError> {
        self.check_edge_applicable(&edge)?;
        self.mirror_op(StagedDiskOp::Edge(edge.clone()))?;
        self.apply_edge_inner(edge)
    }

    /// Apply a pre-built edge blob to the in-memory state. No disk
    /// mirror. Used by the bulk-load path. Upserts by
    /// `(from, to, relation)`.
    pub(crate) fn apply_edge_blob_for_load(
        &mut self,
        from: &str,
        edges: &[ClaimEdge],
    ) -> Result<(), StoreError> {
        if !self.claims.contains_key(from) {
            return Err(StoreError::MissingClaim(from.to_string()));
        }
        for edge in edges {
            self.upsert_edge_in_memory(edge.clone());
        }
        Ok(())
    }

    fn apply_edge_inner(&mut self, edge: ClaimEdge) -> Result<(), StoreError> {
        self.check_edge_applicable(&edge)?;
        let edge_id = edge.edge_id.clone();
        self.upsert_edge_in_memory(edge);
        self.wal.push(WalEvent::EdgeUpsert(edge_id));
        Ok(())
    }

    /// Upsert by `(from, to, relation)` and keep the reverse index in
    /// sync. Re-applying the same edge never adds a second copy.
    fn upsert_edge_in_memory(&mut self, edge: ClaimEdge) {
        let list = self
            .edges_by_claim
            .entry(edge.from_claim_id.clone())
            .or_default();
        let existing = list
            .iter()
            .position(|e| e.to_claim_id == edge.to_claim_id && e.relation == edge.relation);
        match existing {
            Some(pos) => {
                if let Some(incoming) = self.edges_in.get_mut(&edge.to_claim_id)
                    && let Some(entry) = incoming.iter_mut().find(|i| {
                        i.from_claim_id == edge.from_claim_id && i.relation == edge.relation
                    })
                {
                    entry.strength = edge.strength;
                }
                list[pos] = edge;
            }
            None => {
                self.edges_in
                    .entry(edge.to_claim_id.clone())
                    .or_default()
                    .push(IncomingEdge {
                        from_claim_id: edge.from_claim_id.clone(),
                        relation: edge.relation.clone(),
                        strength: edge.strength,
                    });
                list.push(edge);
            }
        }
    }

    fn apply_claim_vector(&mut self, claim_id: &str, vector: Vec<f32>) -> Result<(), StoreError> {
        // Validate (claim exists, finite, dimension) BEFORE any disk I/O
        // so we never write a half-bad state.
        self.validate_claim_vector(claim_id, &vector)?;
        let tenant_id = self
            .claims
            .get(claim_id)
            .map(|claim| claim.tenant_id.clone())
            .ok_or_else(|| StoreError::MissingClaim(claim_id.to_string()))?;
        let new_dim = if self.tenant_vector_dims.contains_key(&tenant_id) {
            None
        } else {
            Some(vector.len())
        };
        self.mirror_op(StagedDiskOp::Vector {
            claim_id: claim_id.to_string(),
            tenant_id,
            vector: vector.clone(),
            new_dim,
        })?;
        self.apply_claim_vector_inner(claim_id, vector)
    }

    /// Apply a vector to the in-memory state (rebuilds the ANN
    /// index). No disk mirror. Used by the bulk-load path.
    pub(crate) fn apply_claim_vector_blob_for_load(
        &mut self,
        claim_id: &str,
        vector: Vec<f32>,
    ) -> Result<(), StoreError> {
        self.apply_claim_vector_inner(claim_id, vector)
    }

    fn apply_claim_vector_inner(
        &mut self,
        claim_id: &str,
        vector: Vec<f32>,
    ) -> Result<(), StoreError> {
        self.validate_claim_vector(claim_id, &vector)?;
        let tenant_id = self
            .claims
            .get(claim_id)
            .map(|claim| claim.tenant_id.clone())
            .ok_or_else(|| StoreError::MissingClaim(claim_id.to_string()))?;
        self.tenant_vector_dims
            .entry(tenant_id.clone())
            .or_insert(vector.len());

        if self.claim_vectors.contains_key(claim_id) {
            self.remove_vector_index_entry(&tenant_id, claim_id);
        }

        let stored_vector = vector.clone();
        self.claim_vectors.insert(claim_id.to_string(), vector);
        self.add_vector_index_entry(&tenant_id, claim_id, &stored_vector);
        self.wal
            .push(WalEvent::ClaimVectorUpsert(claim_id.to_string()));
        Ok(())
    }

    fn apply_batch_commit_record(&mut self, record: BatchCommitRecord) -> Result<(), StoreError> {
        // Group delimiters carry no batch metadata of their own.
        if is_group_marker_commit_id(&record.commit_id) {
            return Ok(());
        }
        // Compute the metadata the same way the inner function will,
        // so we can mirror to disk before mutating in-memory state.
        let payload_fingerprint =
            batch_commit_payload_fingerprint(record.batch_size, &record.claim_ids);
        if let Some(existing) = self.batch_commits.get(&record.commit_id) {
            if existing.payload_fingerprint != payload_fingerprint {
                return Err(StoreError::Conflict(format!(
                    "batch commit_id '{}' already exists with different payload (existing_fingerprint={}, incoming_fingerprint={})",
                    record.commit_id, existing.payload_fingerprint, payload_fingerprint
                )));
            }
            // Idempotent: the record is already in memory and on disk.
            return Ok(());
        }

        let metadata = BatchCommitMetadata {
            commit_id: record.commit_id.clone(),
            batch_size: record.batch_size,
            ts_unix_ms: record.ts_unix_ms,
            claim_ids: record.claim_ids.clone(),
            payload_fingerprint: payload_fingerprint.clone(),
        };
        self.mirror_op(StagedDiskOp::BatchCommit(metadata.clone()))?;
        self.batch_commits
            .insert(record.commit_id.clone(), metadata);
        self.wal.push(WalEvent::BatchCommit(record.commit_id));
        Ok(())
    }

    /// Apply a batch-commit metadata to the in-memory state. No
    /// disk mirror. Used by the bulk-load path.
    pub(crate) fn apply_batch_commit_for_load(
        &mut self,
        commit: &BatchCommitMetadata,
    ) -> Result<(), StoreError> {
        if let Some(existing) = self.batch_commits.get(&commit.commit_id) {
            if existing.payload_fingerprint != commit.payload_fingerprint {
                return Err(StoreError::Conflict(format!(
                    "batch commit_id '{}' already exists with different payload (existing_fingerprint={}, incoming_fingerprint={})",
                    commit.commit_id, existing.payload_fingerprint, commit.payload_fingerprint
                )));
            }
            return Ok(());
        }
        self.batch_commits
            .insert(commit.commit_id.clone(), commit.clone());
        Ok(())
    }

    /// Apply a tenant vector dimension to the in-memory state. No
    /// disk mirror. Used by the bulk-load path.
    pub(crate) fn apply_tenant_dim_for_load(&mut self, tenant: &str, dim: usize) {
        self.tenant_vector_dims.insert(tenant.to_string(), dim);
    }

    /// Apply a tenant-claim set membership to the in-memory state.
    /// No disk mirror. Used by the bulk-load path.
    pub(crate) fn apply_tenant_claim_set_for_load(&mut self, tenant: &str, claim: &str) {
        self.tenant_claim_ids
            .entry(tenant.to_string())
            .or_default()
            .insert(claim.to_string());
    }

    fn add_vector_index_entry(&mut self, tenant_id: &str, claim_id: &str, vector: &[f32]) {
        if self.defer_vector_index {
            return;
        }
        let tuning = &self.ann_tuning;
        let index = self
            .vector_indexes
            .entry(tenant_id.to_string())
            .or_insert_with(|| TenantVectorIndex::new(vector.len(), tuning.clone()));
        // A vector that cannot be indexed (zero norm) stays stored but is
        // unreachable by similarity search, as it never had a cosine score.
        let _ = index.upsert(claim_id, vector);
    }

    fn remove_vector_index_entry(&mut self, tenant_id: &str, claim_id: &str) {
        if self.defer_vector_index {
            return;
        }
        if let Some(index) = self.vector_indexes.get_mut(tenant_id) {
            index.remove(claim_id);
            if index.is_empty() {
                self.vector_indexes.remove(tenant_id);
            }
        }
    }

    /// Rebuild every tenant's vector index from `claim_vectors`, building
    /// large tenants multi-threaded. Used when the tuning changes and at the
    /// end of a bulk load; replaces the incremental per-vector inserts.
    pub(crate) fn rebuild_vector_indexes(&mut self) {
        self.defer_vector_index = false;
        let mut by_tenant: HashMap<&str, Vec<(&str, &[f32])>> = HashMap::new();
        for (claim_id, vector) in &self.claim_vectors {
            if let Some(claim) = self.claims.get(claim_id) {
                by_tenant
                    .entry(claim.tenant_id.as_str())
                    .or_default()
                    .push((claim_id.as_str(), vector.as_slice()));
            }
        }
        let mut indexes = HashMap::with_capacity(by_tenant.len());
        for (tenant_id, mut items) in by_tenant {
            // Deterministic insertion order regardless of hash iteration.
            items.sort_unstable_by_key(|(claim_id, _)| *claim_id);
            let dim = self
                .tenant_vector_dims
                .get(tenant_id)
                .copied()
                .unwrap_or_else(|| items[0].1.len());
            let built = TenantVectorIndex::build(dim, self.ann_tuning.clone(), items.into_iter());
            if let Ok(index) = built
                && !index.is_empty()
            {
                indexes.insert(tenant_id.to_string(), index);
            }
        }
        self.vector_indexes = indexes;
    }

    fn add_claim_indexes(&mut self, claim: &Claim) {
        self.tenant_claim_ids
            .entry(claim.tenant_id.clone())
            .or_default()
            .insert(claim.claim_id.clone());

        let tokens = tokenize(&claim.canonical_text);
        self.claim_tokens
            .insert(claim.claim_id.clone(), tokens.clone());
        let token_index = self
            .inverted_index
            .entry(claim.tenant_id.clone())
            .or_default();
        let mut seen = HashSet::new();
        for token in tokens {
            if seen.insert(token.clone()) {
                token_index
                    .entry(token)
                    .or_default()
                    .insert(claim.claim_id.clone());
            }
        }

        let entity_index = self
            .entity_index
            .entry(claim.tenant_id.clone())
            .or_default();
        for entity in &claim.entities {
            let key = normalize_index_key(entity);
            if key.is_empty() {
                continue;
            }
            entity_index
                .entry(key)
                .or_default()
                .insert(claim.claim_id.clone());
        }

        let embedding_index = self
            .embedding_index
            .entry(claim.tenant_id.clone())
            .or_default();
        let mut seen_embedding = HashSet::new();
        for embedding_id in &claim.embedding_ids {
            let key = embedding_id.trim();
            if key.is_empty() || !seen_embedding.insert(key.to_string()) {
                continue;
            }
            embedding_index
                .entry(key.to_string())
                .or_default()
                .insert(claim.claim_id.clone());
        }

        if let Some(ts) = claim.event_time_unix {
            self.temporal_index
                .entry(claim.tenant_id.clone())
                .or_default()
                .entry(ts)
                .or_default()
                .insert(claim.claim_id.clone());
        }
    }

    fn remove_claim_indexes(&mut self, claim: &Claim) {
        // Vectors and ANN entries are intentionally NOT touched here
        // (DATA-04): re-upserting a claim keeps its vector.
        let mut drop_tenant_claim_ids = false;
        if let Some(ids) = self.tenant_claim_ids.get_mut(&claim.tenant_id) {
            ids.remove(&claim.claim_id);
            drop_tenant_claim_ids = ids.is_empty();
        }
        if drop_tenant_claim_ids {
            self.tenant_claim_ids.remove(&claim.tenant_id);
        }

        if let Some(tokens) = self.claim_tokens.remove(&claim.claim_id)
            && let Some(token_index) = self.inverted_index.get_mut(&claim.tenant_id)
        {
            let mut seen = HashSet::new();
            let mut remove_tokens = Vec::new();
            for token in tokens {
                if !seen.insert(token.clone()) {
                    continue;
                }
                if let Some(ids) = token_index.get_mut(&token) {
                    ids.remove(&claim.claim_id);
                    if ids.is_empty() {
                        remove_tokens.push(token);
                    }
                }
            }
            for token in remove_tokens {
                token_index.remove(&token);
            }
        }
        if self
            .inverted_index
            .get(&claim.tenant_id)
            .is_some_and(|index| index.is_empty())
        {
            self.inverted_index.remove(&claim.tenant_id);
        }

        let mut remove_entity_index = false;
        if let Some(entity_index) = self.entity_index.get_mut(&claim.tenant_id) {
            let mut remove_keys = Vec::new();
            for entity in &claim.entities {
                let key = normalize_index_key(entity);
                if let Some(ids) = entity_index.get_mut(&key) {
                    ids.remove(&claim.claim_id);
                    if ids.is_empty() {
                        remove_keys.push(key);
                    }
                }
            }
            for key in remove_keys {
                entity_index.remove(&key);
            }
            remove_entity_index = entity_index.is_empty();
        }
        if remove_entity_index {
            self.entity_index.remove(&claim.tenant_id);
        }

        let mut remove_embedding_index = false;
        if let Some(embedding_index) = self.embedding_index.get_mut(&claim.tenant_id) {
            let mut remove_keys = Vec::new();
            let mut seen_embedding = HashSet::new();
            for embedding_id in &claim.embedding_ids {
                let key = embedding_id.trim();
                if key.is_empty() || !seen_embedding.insert(key.to_string()) {
                    continue;
                }
                if let Some(ids) = embedding_index.get_mut(key) {
                    ids.remove(&claim.claim_id);
                    if ids.is_empty() {
                        remove_keys.push(key.to_string());
                    }
                }
            }
            for key in remove_keys {
                embedding_index.remove(&key);
            }
            remove_embedding_index = embedding_index.is_empty();
        }
        if remove_embedding_index {
            self.embedding_index.remove(&claim.tenant_id);
        }

        let mut remove_temporal_index = false;
        if let Some(ts) = claim.event_time_unix
            && let Some(timeline) = self.temporal_index.get_mut(&claim.tenant_id)
        {
            let mut drop_ts = false;
            if let Some(ids) = timeline.get_mut(&ts) {
                ids.remove(&claim.claim_id);
                drop_ts = ids.is_empty();
            }
            if drop_ts {
                timeline.remove(&ts);
            }
            remove_temporal_index = timeline.is_empty();
        }
        if remove_temporal_index {
            self.temporal_index.remove(&claim.tenant_id);
        }
    }
}

fn parse_vector_backend_preference() -> VectorBackendPreference {
    match std::env::var(VECTOR_BACKEND_ENV)
        .ok()
        .map(|raw| raw.trim().to_ascii_lowercase())
        .as_deref()
    {
        Some("cpu") => VectorBackendPreference::Cpu,
        Some("gpu") => VectorBackendPreference::Gpu,
        _ => VectorBackendPreference::Auto,
    }
}

fn resolve_vector_backend_runtime(preference: VectorBackendPreference) -> VectorBackendRuntime {
    match preference {
        VectorBackendPreference::Cpu => VectorBackendRuntime::Cpu,
        VectorBackendPreference::Gpu => resolve_gpu_backend_runtime(true),
        VectorBackendPreference::Auto => resolve_gpu_backend_runtime(false),
    }
}

#[cfg(feature = "gpu-backend")]
fn resolve_gpu_backend_runtime(explicit_gpu_required: bool) -> VectorBackendRuntime {
    if gpu::gpu_backend_engine().is_some() {
        VectorBackendRuntime::Gpu
    } else if explicit_gpu_required {
        VectorBackendRuntime::CpuFallbackUnavailable
    } else {
        VectorBackendRuntime::Cpu
    }
}

#[cfg(not(feature = "gpu-backend"))]
fn resolve_gpu_backend_runtime(explicit_gpu_required: bool) -> VectorBackendRuntime {
    if explicit_gpu_required {
        VectorBackendRuntime::CpuFallbackFeatureDisabled
    } else {
        VectorBackendRuntime::Cpu
    }
}

fn score_query_candidate_vectors_cpu(
    query_vector: &[f32],
    candidate_vectors: &[(String, &[f32])],
) -> Vec<(String, f32)> {
    candidate_vectors
        .iter()
        .filter_map(|(claim_id, candidate_vector)| {
            let score = cosine_similarity(query_vector, candidate_vector)?;
            Some((claim_id.clone(), score))
        })
        .collect()
}

fn value_in_time_range(value: i64, from_unix: Option<i64>, to_unix: Option<i64>) -> bool {
    if let Some(from) = from_unix
        && value < from
    {
        return false;
    }
    if let Some(to) = to_unix
        && value > to
    {
        return false;
    }
    true
}

fn time_windows_overlap(
    window_start: Option<i64>,
    window_end: Option<i64>,
    query_from: Option<i64>,
    query_to: Option<i64>,
) -> bool {
    let start = window_start.unwrap_or(i64::MIN);
    let end = window_end.unwrap_or(i64::MAX);
    let query_start = query_from.unwrap_or(i64::MIN);
    let query_end = query_to.unwrap_or(i64::MAX);
    start <= query_end && end >= query_start
}

fn claim_matches_time_range(claim: &Claim, from_unix: Option<i64>, to_unix: Option<i64>) -> bool {
    if from_unix.is_none() && to_unix.is_none() {
        return true;
    }

    let event_match = claim
        .event_time_unix
        .is_some_and(|value| value_in_time_range(value, from_unix, to_unix));
    let has_valid_window = claim.valid_from.is_some() || claim.valid_to.is_some();
    let validity_match = has_valid_window
        && time_windows_overlap(claim.valid_from, claim.valid_to, from_unix, to_unix);

    match (claim.event_time_unix.is_some(), has_valid_window) {
        (true, true) => event_match && validity_match,
        (true, false) => event_match,
        (false, true) => validity_match,
        (false, false) => false,
    }
}

fn normalize_index_key(value: &str) -> String {
    value.trim().to_ascii_lowercase()
}

fn validate_vector(vector: &[f32]) -> Result<(), StoreError> {
    if vector.is_empty() {
        return Err(StoreError::InvalidVector(
            "vector cannot be empty".to_string(),
        ));
    }
    if !vector.iter().all(|v| v.is_finite()) {
        return Err(StoreError::InvalidVector(
            "vector values must be finite".to_string(),
        ));
    }
    Ok(())
}

/// Replace the evidence with the same `evidence_id`, or append it
/// (DATA-01). Returns `true` when a new entry was appended.
fn upsert_evidence(list: &mut Vec<Evidence>, evidence: Evidence) -> bool {
    match list
        .iter_mut()
        .find(|existing| existing.evidence_id == evidence.evidence_id)
    {
        Some(slot) => {
            *slot = evidence;
            false
        }
        None => {
            list.push(evidence);
            true
        }
    }
}

/// Replace the edge with the same `(from, to, relation)`, or append it
/// (DATA-01). Returns `true` when a new entry was appended.
fn upsert_edge(list: &mut Vec<ClaimEdge>, edge: ClaimEdge) -> bool {
    match list.iter_mut().find(|existing| {
        existing.from_claim_id == edge.from_claim_id
            && existing.to_claim_id == edge.to_claim_id
            && existing.relation == edge.relation
    }) {
        Some(slot) => {
            *slot = edge;
            false
        }
        None => {
            list.push(edge);
            true
        }
    }
}

/// Cosine similarity accumulated in `f64` and clamped to `[-1, 1]`
/// (DATA-05). Returns `None` for mismatched/empty inputs, zero norms, or
/// any non-finite intermediate, so callers can never see a NaN score.
fn cosine_similarity(a: &[f32], b: &[f32]) -> Option<f32> {
    if a.len() != b.len() || a.is_empty() {
        return None;
    }
    let mut dot = 0.0f64;
    let mut norm_a = 0.0f64;
    let mut norm_b = 0.0f64;
    for (x, y) in a.iter().zip(b.iter()) {
        let (x, y) = (f64::from(*x), f64::from(*y));
        dot += x * y;
        norm_a += x * x;
        norm_b += y * y;
    }
    let denom = norm_a.sqrt() * norm_b.sqrt();
    if !denom.is_finite() || denom <= f64::MIN_POSITIVE {
        return None;
    }
    let cosine = dot / denom;
    if !cosine.is_finite() {
        return None;
    }
    Some(cosine.clamp(-1.0, 1.0) as f32)
}

#[cfg(test)]
mod tests {
    use super::*;
    use schema::{Claim, ClaimEdge, ClaimType, Relation, RetrievalRequest, Stance, StanceMode};
    use std::path::PathBuf;
    use std::time::Duration;
    use std::{
        fs::{read_to_string, remove_file},
        sync::atomic::{AtomicU64, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    fn claim_for_tenant(id: &str, text: &str, tenant_id: &str) -> Claim {
        Claim {
            claim_id: id.to_string(),
            tenant_id: tenant_id.to_string(),
            canonical_text: text.to_string(),
            confidence: 0.9,
            event_time_unix: None,
            entities: vec![],
            embedding_ids: vec![],
            claim_type: None,
            valid_from: None,
            valid_to: None,
            created_at: None,
            updated_at: None,
        }
    }

    fn claim(id: &str, text: &str) -> Claim {
        claim_for_tenant(id, text, "tenant-a")
    }

    fn temp_wal_path() -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let mut path = std::env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock should be valid")
            .as_nanos();
        let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
        path.push(format!("eme-wal-{}-{nanos}-{seq}.log", std::process::id()));
        path
    }

    fn cleanup_persistence_files(wal: &FileWal) {
        let _ = remove_file(wal.path());
        let _ = remove_file(wal.snapshot_path());
    }

    struct EnvVarGuard {
        key: &'static str,
        previous: Option<String>,
    }

    impl EnvVarGuard {
        fn set(key: &'static str, value: &str) -> Self {
            let previous = std::env::var(key).ok();
            #[allow(unused_unsafe)]
            unsafe {
                std::env::set_var(key, value);
            }
            Self { key, previous }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            match &self.previous {
                Some(value) => {
                    #[allow(unused_unsafe)]
                    unsafe {
                        std::env::set_var(self.key, value);
                    }
                }
                None => {
                    #[allow(unused_unsafe)]
                    unsafe {
                        std::env::remove_var(self.key);
                    }
                }
            }
        }
    }

    #[test]
    fn ingest_bundle_writes_claim_and_wal_entries() {
        let mut store = InMemoryStore::new();
        let claim = claim("c1", "Company X acquired Company Y");
        let evidence = vec![Evidence {
            evidence_id: "e1".into(),
            claim_id: "c1".into(),
            source_id: "doc-1".into(),
            stance: Stance::Supports,
            source_quality: 0.9,
            chunk_id: None,
            span_start: None,
            span_end: None,
            doc_id: None,
            extraction_model: None,
            ingested_at: None,
        }];
        let edges = vec![ClaimEdge {
            edge_id: "edge1".into(),
            from_claim_id: "c1".into(),
            to_claim_id: "c2".into(),
            relation: Relation::Supports,
            strength: 0.6,
            reason_codes: vec![],
            created_at: None,
        }];

        store.ingest_bundle(claim, evidence, edges).unwrap();
        assert_eq!(store.claims_len(), 1);
        assert_eq!(store.wal_len(), 3);
    }

    #[test]
    fn retrieve_ranks_high_overlap_claim_first() {
        let mut store = InMemoryStore::new();

        store
            .ingest_bundle(
                claim("c1", "Company X acquired Company Y"),
                vec![Evidence {
                    evidence_id: "e1".into(),
                    claim_id: "c1".into(),
                    source_id: "doc-1".into(),
                    stance: Stance::Supports,
                    source_quality: 0.9,
                    chunk_id: None,
                    span_start: None,
                    span_end: None,
                    doc_id: None,
                    extraction_model: None,
                    ingested_at: None,
                }],
                vec![],
            )
            .unwrap();

        store
            .ingest_bundle(
                claim("c2", "Company Z opened a new office"),
                vec![Evidence {
                    evidence_id: "e2".into(),
                    claim_id: "c2".into(),
                    source_id: "doc-2".into(),
                    stance: Stance::Supports,
                    source_quality: 0.8,
                    chunk_id: None,
                    span_start: None,
                    span_end: None,
                    doc_id: None,
                    extraction_model: None,
                    ingested_at: None,
                }],
                vec![],
            )
            .unwrap();

        let results = store.retrieve(&RetrievalRequest {
            tenant_id: "tenant-a".into(),
            query: "Did Company X acquire Company Y?".into(),
            top_k: 2,
            stance_mode: StanceMode::Balanced,
        });

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].claim_id, "c1");
    }

    #[test]
    fn retrieve_with_time_range_filters_out_of_window_claims() {
        let mut store = InMemoryStore::new();

        store
            .ingest_bundle(
                Claim {
                    claim_id: "c-old".into(),
                    tenant_id: "tenant-a".into(),
                    canonical_text: "Project Orion launch milestone".into(),
                    confidence: 0.9,
                    event_time_unix: Some(100),
                    entities: vec![],
                    embedding_ids: vec![],
                    claim_type: None,
                    valid_from: None,
                    valid_to: None,
                    created_at: None,
                    updated_at: None,
                },
                vec![Evidence {
                    evidence_id: "e-old".into(),
                    claim_id: "c-old".into(),
                    source_id: "doc-old".into(),
                    stance: Stance::Supports,
                    source_quality: 0.9,
                    chunk_id: None,
                    span_start: None,
                    span_end: None,
                    doc_id: None,
                    extraction_model: None,
                    ingested_at: None,
                }],
                vec![],
            )
            .unwrap();
        store
            .ingest_bundle(
                Claim {
                    claim_id: "c-new".into(),
                    tenant_id: "tenant-a".into(),
                    canonical_text: "Project Orion launch milestone".into(),
                    confidence: 0.9,
                    event_time_unix: Some(200),
                    entities: vec![],
                    embedding_ids: vec![],
                    claim_type: None,
                    valid_from: None,
                    valid_to: None,
                    created_at: None,
                    updated_at: None,
                },
                vec![Evidence {
                    evidence_id: "e-new".into(),
                    claim_id: "c-new".into(),
                    source_id: "doc-new".into(),
                    stance: Stance::Supports,
                    source_quality: 0.9,
                    chunk_id: None,
                    span_start: None,
                    span_end: None,
                    doc_id: None,
                    extraction_model: None,
                    ingested_at: None,
                }],
                vec![],
            )
            .unwrap();
        store
            .ingest_bundle(
                Claim {
                    claim_id: "c-no-time".into(),
                    tenant_id: "tenant-a".into(),
                    canonical_text: "Project Orion launch milestone".into(),
                    confidence: 0.9,
                    event_time_unix: None,
                    entities: vec![],
                    embedding_ids: vec![],
                    claim_type: None,
                    valid_from: None,
                    valid_to: None,
                    created_at: None,
                    updated_at: None,
                },
                vec![Evidence {
                    evidence_id: "e-no-time".into(),
                    claim_id: "c-no-time".into(),
                    source_id: "doc-no-time".into(),
                    stance: Stance::Supports,
                    source_quality: 0.9,
                    chunk_id: None,
                    span_start: None,
                    span_end: None,
                    doc_id: None,
                    extraction_model: None,
                    ingested_at: None,
                }],
                vec![],
            )
            .unwrap();

        let req = RetrievalRequest {
            tenant_id: "tenant-a".into(),
            query: "project orion launch milestone".into(),
            top_k: 5,
            stance_mode: StanceMode::Balanced,
        };
        let results = store.retrieve_with_time_range(&req, Some(150), Some(250));

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].claim_id, "c-new");
    }

    #[test]
    fn retrieve_with_time_range_uses_validity_window_when_event_time_missing() {
        let mut store = InMemoryStore::new();
        store
            .ingest_bundle(
                Claim {
                    claim_id: "c-window-hit".into(),
                    tenant_id: "tenant-a".into(),
                    canonical_text: "Claim with active validity window".into(),
                    confidence: 0.9,
                    event_time_unix: None,
                    entities: vec![],
                    embedding_ids: vec![],
                    claim_type: Some(ClaimType::Temporal),
                    valid_from: Some(120),
                    valid_to: Some(260),
                    created_at: Some(10),
                    updated_at: Some(20),
                },
                vec![Evidence {
                    evidence_id: "e-window-hit".into(),
                    claim_id: "c-window-hit".into(),
                    source_id: "doc-window-hit".into(),
                    stance: Stance::Supports,
                    source_quality: 0.9,
                    chunk_id: None,
                    span_start: None,
                    span_end: None,
                    doc_id: None,
                    extraction_model: None,
                    ingested_at: None,
                }],
                vec![],
            )
            .unwrap();
        store
            .ingest_bundle(
                Claim {
                    claim_id: "c-window-miss".into(),
                    tenant_id: "tenant-a".into(),
                    canonical_text: "Claim with non-overlapping validity window".into(),
                    confidence: 0.9,
                    event_time_unix: None,
                    entities: vec![],
                    embedding_ids: vec![],
                    claim_type: Some(ClaimType::Temporal),
                    valid_from: Some(400),
                    valid_to: Some(500),
                    created_at: Some(11),
                    updated_at: Some(21),
                },
                vec![Evidence {
                    evidence_id: "e-window-miss".into(),
                    claim_id: "c-window-miss".into(),
                    source_id: "doc-window-miss".into(),
                    stance: Stance::Supports,
                    source_quality: 0.9,
                    chunk_id: None,
                    span_start: None,
                    span_end: None,
                    doc_id: None,
                    extraction_model: None,
                    ingested_at: None,
                }],
                vec![],
            )
            .unwrap();

        let req = RetrievalRequest {
            tenant_id: "tenant-a".into(),
            query: "claim validity window".into(),
            top_k: 5,
            stance_mode: StanceMode::Balanced,
        };
        let results = store.retrieve_with_time_range(&req, Some(150), Some(240));
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].claim_id, "c-window-hit");
    }

    #[test]
    fn retrieve_with_time_range_requires_event_and_validity_match_when_both_present() {
        let mut store = InMemoryStore::new();
        store
            .ingest_bundle(
                Claim {
                    claim_id: "c-both-miss".into(),
                    tenant_id: "tenant-a".into(),
                    canonical_text: "Claim where event and validity disagree".into(),
                    confidence: 0.9,
                    event_time_unix: Some(100),
                    entities: vec![],
                    embedding_ids: vec![],
                    claim_type: Some(ClaimType::Temporal),
                    valid_from: Some(140),
                    valid_to: Some(260),
                    created_at: Some(12),
                    updated_at: Some(22),
                },
                vec![Evidence {
                    evidence_id: "e-both-miss".into(),
                    claim_id: "c-both-miss".into(),
                    source_id: "doc-both-miss".into(),
                    stance: Stance::Supports,
                    source_quality: 0.9,
                    chunk_id: None,
                    span_start: None,
                    span_end: None,
                    doc_id: None,
                    extraction_model: None,
                    ingested_at: None,
                }],
                vec![],
            )
            .unwrap();
        store
            .ingest_bundle(
                Claim {
                    claim_id: "c-both-hit".into(),
                    tenant_id: "tenant-a".into(),
                    canonical_text: "Claim where event and validity align".into(),
                    confidence: 0.9,
                    event_time_unix: Some(200),
                    entities: vec![],
                    embedding_ids: vec![],
                    claim_type: Some(ClaimType::Temporal),
                    valid_from: Some(140),
                    valid_to: Some(260),
                    created_at: Some(13),
                    updated_at: Some(23),
                },
                vec![Evidence {
                    evidence_id: "e-both-hit".into(),
                    claim_id: "c-both-hit".into(),
                    source_id: "doc-both-hit".into(),
                    stance: Stance::Supports,
                    source_quality: 0.9,
                    chunk_id: None,
                    span_start: None,
                    span_end: None,
                    doc_id: None,
                    extraction_model: None,
                    ingested_at: None,
                }],
                vec![],
            )
            .unwrap();

        let req = RetrievalRequest {
            tenant_id: "tenant-a".into(),
            query: "claim event validity".into(),
            top_k: 5,
            stance_mode: StanceMode::Balanced,
        };
        let results = store.retrieve_with_time_range(&req, Some(150), Some(240));
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].claim_id, "c-both-hit");
    }

    #[test]
    fn support_only_filters_claims_with_more_contradictions() {
        let mut store = InMemoryStore::new();
        store
            .ingest_bundle(
                claim("c1", "Company X acquired Company Y"),
                vec![
                    Evidence {
                        evidence_id: "e1".into(),
                        claim_id: "c1".into(),
                        source_id: "doc-1".into(),
                        stance: Stance::Supports,
                        source_quality: 0.9,
                        chunk_id: None,
                        span_start: None,
                        span_end: None,
                        doc_id: None,
                        extraction_model: None,
                        ingested_at: None,
                    },
                    Evidence {
                        evidence_id: "e2".into(),
                        claim_id: "c1".into(),
                        source_id: "doc-2".into(),
                        stance: Stance::Contradicts,
                        source_quality: 0.8,
                        chunk_id: None,
                        span_start: None,
                        span_end: None,
                        doc_id: None,
                        extraction_model: None,
                        ingested_at: None,
                    },
                    Evidence {
                        evidence_id: "e3".into(),
                        claim_id: "c1".into(),
                        source_id: "doc-3".into(),
                        stance: Stance::Contradicts,
                        source_quality: 0.8,
                        chunk_id: None,
                        span_start: None,
                        span_end: None,
                        doc_id: None,
                        extraction_model: None,
                        ingested_at: None,
                    },
                ],
                vec![],
            )
            .unwrap();

        let support_only_results = store.retrieve(&RetrievalRequest {
            tenant_id: "tenant-a".into(),
            query: "Company X acquired Company Y".into(),
            top_k: 10,
            stance_mode: StanceMode::SupportOnly,
        });
        assert!(support_only_results.is_empty());
    }

    #[test]
    fn persistent_wal_replay_restores_claims_and_retrieval() {
        let wal_path = temp_wal_path();
        let mut wal = FileWal::open(&wal_path).unwrap();
        let mut store = InMemoryStore::new();

        store
            .ingest_bundle_persistent(
                &mut wal,
                claim("c1", "Company X acquired Company Y"),
                vec![Evidence {
                    evidence_id: "e1".into(),
                    claim_id: "c1".into(),
                    source_id: "doc-1".into(),
                    stance: Stance::Supports,
                    source_quality: 0.9,
                    chunk_id: None,
                    span_start: None,
                    span_end: None,
                    doc_id: None,
                    extraction_model: None,
                    ingested_at: None,
                }],
                vec![],
            )
            .unwrap();
        drop(store);

        let replayed = InMemoryStore::load_from_wal(&wal).unwrap();
        assert_eq!(replayed.claims_len(), 1);
        let results = replayed.retrieve(&RetrievalRequest {
            tenant_id: "tenant-a".into(),
            query: "company x acquired company y".into(),
            top_k: 1,
            stance_mode: StanceMode::Balanced,
        });
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].claim_id, "c1");

        cleanup_persistence_files(&wal);
    }

    #[test]
    fn load_from_wal_with_stats_reports_replay_breakdown() {
        let wal_path = temp_wal_path();
        let mut wal = FileWal::open(&wal_path).unwrap();
        let mut store = InMemoryStore::new();

        store
            .ingest_bundle_persistent(
                &mut wal,
                claim("c1", "Company X acquired Company Y"),
                vec![Evidence {
                    evidence_id: "e1".into(),
                    claim_id: "c1".into(),
                    source_id: "doc-1".into(),
                    stance: Stance::Supports,
                    source_quality: 0.9,
                    chunk_id: None,
                    span_start: None,
                    span_end: None,
                    doc_id: None,
                    extraction_model: None,
                    ingested_at: None,
                }],
                vec![],
            )
            .unwrap();

        assert_eq!(wal.wal_record_count().unwrap(), 2);
        drop(wal);
        let reopened = FileWal::open(&wal_path).unwrap();
        assert_eq!(reopened.wal_record_count().unwrap(), 2);

        let (replayed, stats) = InMemoryStore::load_from_wal_with_stats(&reopened).unwrap();
        assert_eq!(replayed.claims_len(), 1);
        assert_eq!(stats.replay.snapshot_records, 0);
        assert_eq!(stats.replay.wal_records, 2);
        assert_eq!(stats.claims_loaded, 1);
        assert_eq!(stats.evidence_loaded, 1);
        assert_eq!(stats.edges_loaded, 0);

        cleanup_persistence_files(&reopened);
    }

    #[test]
    fn wal_escaping_round_trip_handles_tabs_and_newlines() {
        let wal_path = temp_wal_path();
        let mut wal = FileWal::open(&wal_path).unwrap();
        let mut store = InMemoryStore::new();

        store
            .ingest_bundle_persistent(
                &mut wal,
                claim("c-tab", "Company X\tacquired\nCompany Y"),
                vec![Evidence {
                    evidence_id: "e-tab".into(),
                    claim_id: "c-tab".into(),
                    source_id: "doc\tline\nbreak".into(),
                    stance: Stance::Supports,
                    source_quality: 0.9,
                    chunk_id: None,
                    span_start: None,
                    span_end: None,
                    doc_id: None,
                    extraction_model: None,
                    ingested_at: None,
                }],
                vec![],
            )
            .unwrap();

        let replayed = InMemoryStore::load_from_wal(&wal).unwrap();
        let results = replayed.retrieve(&RetrievalRequest {
            tenant_id: "tenant-a".into(),
            query: "company x acquired company y".into(),
            top_k: 1,
            stance_mode: StanceMode::Balanced,
        });
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].claim_id, "c-tab");

        cleanup_persistence_files(&wal);
    }

    #[test]
    fn wal_open_with_sync_every_records_clamps_to_minimum_one() {
        let wal_path = temp_wal_path();
        let wal = FileWal::open_with_sync_every_records(&wal_path, 0).unwrap();
        assert_eq!(wal.sync_every_records(), 1);
        cleanup_persistence_files(&wal);
    }

    #[test]
    fn wal_batch_sync_triggers_on_configured_interval() {
        let wal_path = temp_wal_path();
        let mut wal = FileWal::open_with_sync_every_records(&wal_path, 2).unwrap();
        let mut store = InMemoryStore::new();
        store
            .ingest_bundle_persistent(
                &mut wal,
                claim("c-batch-1", "Batch sync one"),
                vec![],
                vec![],
            )
            .unwrap();
        assert_eq!(wal.unsynced_records, 1);

        store
            .ingest_bundle_persistent(
                &mut wal,
                claim("c-batch-2", "Batch sync two"),
                vec![],
                vec![],
            )
            .unwrap();
        assert_eq!(wal.unsynced_records, 0);
        assert_eq!(wal.wal_record_count().unwrap(), 2);

        cleanup_persistence_files(&wal);
    }

    #[test]
    fn wal_append_buffer_batches_disk_writes_until_threshold() {
        let wal_path = temp_wal_path();
        let mut wal = FileWal::open_with_policy(
            &wal_path,
            WalWritePolicy {
                sync_every_records: 10,
                append_buffer_max_records: 3,
                sync_interval: None,
                background_flush_only: false,
            },
        )
        .unwrap();
        let mut store = InMemoryStore::new();

        store
            .ingest_bundle_persistent(&mut wal, claim("c-buf-1", "Buffer one"), vec![], vec![])
            .unwrap();
        store
            .ingest_bundle_persistent(&mut wal, claim("c-buf-2", "Buffer two"), vec![], vec![])
            .unwrap();
        assert_eq!(wal.buffered_record_count(), 2);
        assert_eq!(std::fs::metadata(wal.path()).unwrap().len(), 0);

        store
            .ingest_bundle_persistent(&mut wal, claim("c-buf-3", "Buffer three"), vec![], vec![])
            .unwrap();
        assert_eq!(wal.buffered_record_count(), 0);
        assert!(std::fs::metadata(wal.path()).unwrap().len() > 0);

        cleanup_persistence_files(&wal);
    }

    #[test]
    fn wal_interval_flush_hook_syncs_pending_records() {
        let wal_path = temp_wal_path();
        let mut wal = FileWal::open_with_policy(
            &wal_path,
            WalWritePolicy {
                sync_every_records: 100,
                append_buffer_max_records: 100,
                sync_interval: Some(Duration::from_millis(1)),
                background_flush_only: false,
            },
        )
        .unwrap();
        let mut store = InMemoryStore::new();
        store
            .ingest_bundle_persistent(
                &mut wal,
                claim("c-interval", "Interval flush"),
                vec![],
                vec![],
            )
            .unwrap();
        assert_eq!(wal.unsynced_record_count(), 1);
        std::thread::sleep(Duration::from_millis(2));
        let flushed = wal.flush_pending_sync_if_interval_elapsed().unwrap();
        assert!(flushed);
        assert_eq!(wal.unsynced_record_count(), 0);
        assert_eq!(wal.buffered_record_count(), 0);

        cleanup_persistence_files(&wal);
    }

    #[test]
    fn wal_unsynced_flush_hook_syncs_without_interval_policy() {
        let wal_path = temp_wal_path();
        let mut wal = FileWal::open_with_policy(
            &wal_path,
            WalWritePolicy {
                sync_every_records: 100,
                append_buffer_max_records: 100,
                sync_interval: None,
                background_flush_only: false,
            },
        )
        .unwrap();
        let mut store = InMemoryStore::new();

        store
            .ingest_bundle_persistent(
                &mut wal,
                claim("c-unsynced", "Unsynced flush"),
                vec![],
                vec![],
            )
            .unwrap();
        assert_eq!(wal.unsynced_record_count(), 1);

        let flushed = wal.flush_pending_sync_if_unsynced().unwrap();
        assert!(flushed);
        assert_eq!(wal.unsynced_record_count(), 0);
        assert_eq!(wal.buffered_record_count(), 0);

        cleanup_persistence_files(&wal);
    }

    #[test]
    fn wal_background_flush_only_defers_flush_until_explicit_tick() {
        let wal_path = temp_wal_path();
        let mut wal = FileWal::open_with_policy(
            &wal_path,
            WalWritePolicy {
                sync_every_records: 1,
                append_buffer_max_records: 1,
                sync_interval: None,
                background_flush_only: true,
            },
        )
        .unwrap();
        let mut store = InMemoryStore::new();

        store
            .ingest_bundle_persistent(
                &mut wal,
                claim("c-bg-1", "Background flush one"),
                vec![],
                vec![],
            )
            .unwrap();
        store
            .ingest_bundle_persistent(
                &mut wal,
                claim("c-bg-2", "Background flush two"),
                vec![],
                vec![],
            )
            .unwrap();

        assert!(wal.background_flush_only());
        assert_eq!(wal.unsynced_record_count(), 2);
        assert_eq!(wal.buffered_record_count(), 2);
        assert_eq!(std::fs::metadata(wal.path()).unwrap().len(), 0);

        let flushed = wal.flush_pending_sync_if_unsynced().unwrap();
        assert!(flushed);
        assert_eq!(wal.unsynced_record_count(), 0);
        assert_eq!(wal.buffered_record_count(), 0);
        assert!(std::fs::metadata(wal.path()).unwrap().len() > 0);

        cleanup_persistence_files(&wal);
    }

    #[test]
    fn wal_round_trip_preserves_claim_and_evidence_metadata() {
        let wal_path = temp_wal_path();
        let mut wal = FileWal::open(&wal_path).unwrap();
        let mut store = InMemoryStore::new();

        store
            .ingest_bundle_persistent(
                &mut wal,
                Claim {
                    claim_id: "c-meta".into(),
                    tenant_id: "tenant-a".into(),
                    canonical_text: "Acquisition timeline update".into(),
                    confidence: 0.9,
                    event_time_unix: Some(200),
                    entities: vec!["Company X".into(), "Company Y".into()],
                    embedding_ids: vec!["emb://v1/42".into()],
                    claim_type: Some(ClaimType::Temporal),
                    valid_from: Some(180),
                    valid_to: Some(260),
                    created_at: Some(1_771_620_000_000),
                    updated_at: Some(1_771_620_100_000),
                },
                vec![Evidence {
                    evidence_id: "e-meta".into(),
                    claim_id: "c-meta".into(),
                    source_id: "source://doc-meta".into(),
                    stance: Stance::Supports,
                    source_quality: 0.9,
                    chunk_id: Some("chunk-17".into()),
                    span_start: Some(12),
                    span_end: Some(48),
                    doc_id: Some("doc://meta".into()),
                    extraction_model: Some("extractor-v5".into()),
                    ingested_at: Some(1_771_620_200_000),
                }],
                vec![],
            )
            .unwrap();

        let replayed = InMemoryStore::load_from_wal(&wal).unwrap();
        let claim = replayed
            .claims
            .get("c-meta")
            .expect("claim metadata should be replayed");
        assert_eq!(
            claim.entities,
            vec!["Company X".to_string(), "Company Y".to_string()]
        );
        assert_eq!(claim.embedding_ids, vec!["emb://v1/42".to_string()]);
        assert_eq!(claim.claim_type, Some(ClaimType::Temporal));
        assert_eq!(claim.valid_from, Some(180));
        assert_eq!(claim.valid_to, Some(260));
        assert_eq!(claim.created_at, Some(1_771_620_000_000));
        assert_eq!(claim.updated_at, Some(1_771_620_100_000));

        let evidence = replayed
            .evidence_by_claim
            .get("c-meta")
            .and_then(|items| items.first())
            .expect("evidence metadata should be replayed");
        assert_eq!(evidence.chunk_id.as_deref(), Some("chunk-17"));
        assert_eq!(evidence.span_start, Some(12));
        assert_eq!(evidence.span_end, Some(48));
        assert_eq!(evidence.doc_id.as_deref(), Some("doc://meta"));
        assert_eq!(evidence.extraction_model.as_deref(), Some("extractor-v5"));
        assert_eq!(evidence.ingested_at, Some(1_771_620_200_000));

        cleanup_persistence_files(&wal);
    }

    #[test]
    fn wal_replay_accepts_legacy_record_shape_without_metadata_fields() {
        let wal_path = temp_wal_path();
        std::fs::write(
            &wal_path,
            "C\tc1\ttenant-a\tLegacy claim\t0.9\tnull\nE\te1\tc1\tsource://legacy\tsupports\t0.8\n",
        )
        .unwrap();
        let wal = FileWal::open(&wal_path).unwrap();

        let replayed = InMemoryStore::load_from_wal(&wal).unwrap();
        let claim = replayed.claims.get("c1").expect("legacy claim should load");
        assert!(claim.entities.is_empty());
        assert!(claim.embedding_ids.is_empty());

        let evidence = replayed
            .evidence_by_claim
            .get("c1")
            .and_then(|items| items.first())
            .expect("legacy evidence should load");
        assert_eq!(evidence.chunk_id, None);
        assert_eq!(evidence.span_start, None);
        assert_eq!(evidence.span_end, None);

        cleanup_persistence_files(&wal);
    }

    #[test]
    fn checkpoint_compacts_wal_and_replays_snapshot_plus_delta() {
        let wal_path = temp_wal_path();
        let mut wal = FileWal::open(&wal_path).unwrap();
        let mut store = InMemoryStore::new();

        store
            .ingest_bundle_persistent(
                &mut wal,
                claim("c1", "Company X acquired Company Y"),
                vec![Evidence {
                    evidence_id: "e1".into(),
                    claim_id: "c1".into(),
                    source_id: "doc-1".into(),
                    stance: Stance::Supports,
                    source_quality: 0.9,
                    chunk_id: None,
                    span_start: None,
                    span_end: None,
                    doc_id: None,
                    extraction_model: None,
                    ingested_at: None,
                }],
                vec![],
            )
            .unwrap();
        store
            .ingest_bundle_persistent(
                &mut wal,
                claim("c2", "Company Y integration started"),
                vec![Evidence {
                    evidence_id: "e2".into(),
                    claim_id: "c2".into(),
                    source_id: "doc-2".into(),
                    stance: Stance::Supports,
                    source_quality: 0.88,
                    chunk_id: None,
                    span_start: None,
                    span_end: None,
                    doc_id: None,
                    extraction_model: None,
                    ingested_at: None,
                }],
                vec![],
            )
            .unwrap();

        let wal_before = read_to_string(wal.path()).unwrap();
        assert!(!wal_before.trim().is_empty());

        let stats = store.checkpoint_and_compact(&mut wal).unwrap();
        assert_eq!(stats.snapshot_records, 4);
        assert_eq!(stats.truncated_wal_records, 4);
        assert!(wal.snapshot_path().exists());
        assert!(read_to_string(wal.path()).unwrap().trim().is_empty());

        store
            .ingest_bundle_persistent(
                &mut wal,
                claim("c3", "Post-compaction claim remains queryable"),
                vec![Evidence {
                    evidence_id: "e3".into(),
                    claim_id: "c3".into(),
                    source_id: "doc-3".into(),
                    stance: Stance::Supports,
                    source_quality: 0.9,
                    chunk_id: None,
                    span_start: None,
                    span_end: None,
                    doc_id: None,
                    extraction_model: None,
                    ingested_at: None,
                }],
                vec![],
            )
            .unwrap();

        let replayed = InMemoryStore::load_from_wal(&wal).unwrap();
        assert_eq!(replayed.claims_len(), 3);
        let results = replayed.retrieve(&RetrievalRequest {
            tenant_id: "tenant-a".into(),
            query: "post-compaction claim".into(),
            top_k: 3,
            stance_mode: StanceMode::Balanced,
        });
        assert_eq!(results[0].claim_id, "c3");

        cleanup_persistence_files(&wal);
    }

    #[test]
    fn wal_replay_boundary_tracks_checkpoint_promotion_and_delta_transition() {
        let wal_path = temp_wal_path();
        let mut wal = FileWal::open(&wal_path).unwrap();
        let mut store = InMemoryStore::new();

        store
            .ingest_bundle_persistent(
                &mut wal,
                claim("c1", "Company X announced merger terms"),
                vec![],
                vec![],
            )
            .unwrap();
        store
            .ingest_bundle_persistent(
                &mut wal,
                claim("c2", "Company Y accepted merger terms"),
                vec![],
                vec![],
            )
            .unwrap();

        let before_checkpoint = wal.replay_boundary().unwrap();
        assert!(!before_checkpoint.snapshot_active);
        assert_eq!(before_checkpoint.snapshot_record_count, 0);
        assert_eq!(before_checkpoint.wal_delta_record_count, 2);
        assert_eq!(before_checkpoint.total_replay_record_count, 2);

        store.checkpoint_and_compact(&mut wal).unwrap();
        let after_checkpoint = wal.replay_boundary().unwrap();
        assert!(after_checkpoint.snapshot_active);
        assert_eq!(after_checkpoint.snapshot_record_count, 2);
        assert_eq!(after_checkpoint.wal_delta_record_count, 0);
        assert_eq!(after_checkpoint.total_replay_record_count, 2);

        store
            .ingest_bundle_persistent(
                &mut wal,
                claim("c3", "Company Z approved merger financing"),
                vec![],
                vec![],
            )
            .unwrap();
        let after_delta_append = wal.replay_boundary().unwrap();
        assert!(after_delta_append.snapshot_active);
        assert_eq!(after_delta_append.snapshot_record_count, 2);
        assert_eq!(after_delta_append.wal_delta_record_count, 1);
        assert_eq!(after_delta_append.total_replay_record_count, 3);

        store.checkpoint_and_compact(&mut wal).unwrap();
        let after_second_checkpoint = wal.replay_boundary().unwrap();
        assert!(after_second_checkpoint.snapshot_active);
        assert_eq!(after_second_checkpoint.snapshot_record_count, 3);
        assert_eq!(after_second_checkpoint.wal_delta_record_count, 0);
        assert_eq!(after_second_checkpoint.total_replay_record_count, 3);

        let replayed = InMemoryStore::load_from_wal(&wal).unwrap();
        assert_eq!(replayed.claims_len(), 3);

        cleanup_persistence_files(&wal);
    }

    #[test]
    fn checkpoint_policy_triggers_compaction_by_record_count() {
        let wal_path = temp_wal_path();
        let mut wal = FileWal::open(&wal_path).unwrap();
        let mut store = InMemoryStore::new();
        let policy = CheckpointPolicy {
            max_wal_records: Some(4),
            max_wal_bytes: None,
        };

        let first = store
            .ingest_bundle_persistent_with_policy(
                &mut wal,
                &policy,
                claim("c1", "Company X acquired Company Y"),
                vec![Evidence {
                    evidence_id: "e1".into(),
                    claim_id: "c1".into(),
                    source_id: "doc-1".into(),
                    stance: Stance::Supports,
                    source_quality: 0.9,
                    chunk_id: None,
                    span_start: None,
                    span_end: None,
                    doc_id: None,
                    extraction_model: None,
                    ingested_at: None,
                }],
                vec![],
            )
            .unwrap();
        assert!(first.is_none());

        let second = store
            .ingest_bundle_persistent_with_policy(
                &mut wal,
                &policy,
                claim("c2", "Company Y integration started"),
                vec![Evidence {
                    evidence_id: "e2".into(),
                    claim_id: "c2".into(),
                    source_id: "doc-2".into(),
                    stance: Stance::Supports,
                    source_quality: 0.9,
                    chunk_id: None,
                    span_start: None,
                    span_end: None,
                    doc_id: None,
                    extraction_model: None,
                    ingested_at: None,
                }],
                vec![],
            )
            .unwrap();
        let stats = second.expect("second ingest should trigger checkpoint");
        assert_eq!(stats.truncated_wal_records, 4);
        assert!(read_to_string(wal.path()).unwrap().trim().is_empty());
        assert!(wal.snapshot_path().exists());

        cleanup_persistence_files(&wal);
    }

    #[test]
    fn checkpoint_policy_triggers_compaction_by_wal_bytes() {
        let wal_path = temp_wal_path();
        let mut wal = FileWal::open(&wal_path).unwrap();
        let mut store = InMemoryStore::new();
        let policy = CheckpointPolicy {
            max_wal_records: None,
            max_wal_bytes: Some(1),
        };

        let stats = store
            .ingest_bundle_persistent_with_policy(
                &mut wal,
                &policy,
                claim("c1", "Checkpoint by WAL bytes"),
                vec![Evidence {
                    evidence_id: "e1".into(),
                    claim_id: "c1".into(),
                    source_id: "doc-1".into(),
                    stance: Stance::Supports,
                    source_quality: 0.9,
                    chunk_id: None,
                    span_start: None,
                    span_end: None,
                    doc_id: None,
                    extraction_model: None,
                    ingested_at: None,
                }],
                vec![],
            )
            .unwrap()
            .expect("ingest should trigger byte policy checkpoint");
        assert!(stats.snapshot_records >= 2);
        assert_eq!(wal.wal_record_count().unwrap(), 0);

        cleanup_persistence_files(&wal);
    }

    #[test]
    fn entity_lookup_uses_entity_index() {
        let mut store = InMemoryStore::new();
        store
            .ingest_bundle(
                Claim {
                    claim_id: "c-entity".into(),
                    tenant_id: "tenant-a".into(),
                    canonical_text: "Company X acquired Company Y".into(),
                    confidence: 0.9,
                    event_time_unix: Some(100),
                    entities: vec!["Company X".into(), "Company Y".into()],
                    embedding_ids: vec![],
                    claim_type: None,
                    valid_from: None,
                    valid_to: None,
                    created_at: None,
                    updated_at: None,
                },
                vec![],
                vec![],
            )
            .unwrap();

        let claims = store.claims_for_entity("tenant-a", "company x");
        assert_eq!(claims.len(), 1);
        assert_eq!(claims[0].claim_id, "c-entity");

        let stats = store.index_stats();
        assert_eq!(stats.claim_count, 1);
        assert!(stats.entity_terms >= 2);
        assert!(stats.temporal_buckets >= 1);
    }

    #[test]
    fn embedding_lookup_uses_embedding_index() {
        let mut store = InMemoryStore::new();
        store
            .ingest_bundle(
                Claim {
                    claim_id: "c-embedding".into(),
                    tenant_id: "tenant-a".into(),
                    canonical_text: "Embedding indexed claim".into(),
                    confidence: 0.9,
                    event_time_unix: Some(200),
                    entities: vec![],
                    embedding_ids: vec!["emb://claim-a".into(), "emb://claim-b".into()],
                    claim_type: None,
                    valid_from: None,
                    valid_to: None,
                    created_at: None,
                    updated_at: None,
                },
                vec![],
                vec![],
            )
            .unwrap();

        let ids = store.claim_ids_for_embedding_id("tenant-a", "emb://claim-a");
        assert_eq!(ids.len(), 1);
        assert!(ids.contains("c-embedding"));
    }

    #[test]
    fn retrieve_with_allowed_claim_ids_limits_candidate_pool() {
        let mut store = InMemoryStore::new();
        store
            .ingest_bundle(
                claim("c-allow", "Project Helios acquired Startup Nova"),
                vec![],
                vec![],
            )
            .unwrap();
        store
            .ingest_bundle(
                claim("c-deny", "Project Helios operational update"),
                vec![],
                vec![],
            )
            .unwrap();
        store
            .upsert_claim_vector("c-allow", vec![1.0, 0.0, 0.0, 0.0])
            .unwrap();
        store
            .upsert_claim_vector("c-deny", vec![0.9, 0.1, 0.0, 0.0])
            .unwrap();

        let mut allowed = HashSet::new();
        allowed.insert("c-allow".to_string());

        let results = store.retrieve_with_time_range_query_vector_and_allowed_claim_ids(
            &RetrievalRequest {
                tenant_id: "tenant-a".into(),
                query: "project helios startup nova".into(),
                top_k: 5,
                stance_mode: StanceMode::Balanced,
            },
            None,
            None,
            Some(&[1.0, 0.0, 0.0, 0.0]),
            Some(&allowed),
        );

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].claim_id, "c-allow");
    }

    #[test]
    fn retrieve_with_explicit_candidate_claim_ids_scores_only_explicit_set() {
        let mut store = InMemoryStore::new();
        store
            .ingest_bundle(
                claim("c-segment", "Project Helios acquired Startup Nova"),
                vec![],
                vec![],
            )
            .unwrap();
        store
            .ingest_bundle(
                claim("c-delta", "Project Helios announced expanded merger terms"),
                vec![],
                vec![],
            )
            .unwrap();
        store
            .ingest_bundle(claim("c-other", "Unrelated weather update"), vec![], vec![])
            .unwrap();
        store
            .upsert_claim_vector("c-segment", vec![1.0, 0.0, 0.0, 0.0])
            .unwrap();
        store
            .upsert_claim_vector("c-delta", vec![0.8, 0.2, 0.0, 0.0])
            .unwrap();
        store
            .upsert_claim_vector("c-other", vec![0.1, 0.9, 0.0, 0.0])
            .unwrap();

        let explicit: HashSet<String> = ["c-segment".to_string(), "c-delta".to_string()]
            .into_iter()
            .collect();
        let results = store.retrieve_with_time_range_query_vector_and_explicit_candidate_claim_ids(
            &RetrievalRequest {
                tenant_id: "tenant-a".into(),
                query: "project helios acquisition".into(),
                top_k: 10,
                stance_mode: StanceMode::Balanced,
            },
            None,
            None,
            Some(&[1.0, 0.0, 0.0, 0.0]),
            &explicit,
            None,
        );

        let result_ids: HashSet<String> = results.into_iter().map(|row| row.claim_id).collect();
        assert_eq!(result_ids.len(), 2);
        assert!(result_ids.contains("c-segment"));
        assert!(result_ids.contains("c-delta"));
        assert!(!result_ids.contains("c-other"));
    }

    #[test]
    fn lexical_candidates_and_bm25_rank_relevant_claim_higher() {
        let mut store = InMemoryStore::new();
        store
            .ingest_bundle(
                claim("c-good", "Company X acquired Company Y"),
                vec![Evidence {
                    evidence_id: "e-good".into(),
                    claim_id: "c-good".into(),
                    source_id: "doc-1".into(),
                    stance: Stance::Supports,
                    source_quality: 0.95,
                    chunk_id: None,
                    span_start: None,
                    span_end: None,
                    doc_id: None,
                    extraction_model: None,
                    ingested_at: None,
                }],
                vec![],
            )
            .unwrap();
        store
            .ingest_bundle(
                claim("c-bad", "Weather in city tomorrow"),
                vec![Evidence {
                    evidence_id: "e-bad".into(),
                    claim_id: "c-bad".into(),
                    source_id: "doc-2".into(),
                    stance: Stance::Supports,
                    source_quality: 0.95,
                    chunk_id: None,
                    span_start: None,
                    span_end: None,
                    doc_id: None,
                    extraction_model: None,
                    ingested_at: None,
                }],
                vec![],
            )
            .unwrap();

        let candidate_count =
            store.candidate_count("tenant-a", "did company x acquire y", None, None);
        assert_eq!(candidate_count, 1);

        let results = store.retrieve(&RetrievalRequest {
            tenant_id: "tenant-a".into(),
            query: "did company x acquire y".into(),
            top_k: 2,
            stance_mode: StanceMode::Balanced,
        });
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].claim_id, "c-good");
    }

    #[test]
    fn vector_wal_round_trip_restores_claim_vectors() {
        let wal_path = temp_wal_path();
        let mut wal = FileWal::open(&wal_path).unwrap();
        let mut store = InMemoryStore::new();

        store
            .ingest_bundle_persistent(
                &mut wal,
                claim("c-vec", "Vector indexed claim"),
                vec![],
                vec![],
            )
            .unwrap();
        store
            .upsert_claim_vector_persistent(&mut wal, "c-vec", vec![0.1, 0.3, 0.5, 0.7])
            .unwrap();

        let (replayed, stats) = InMemoryStore::load_from_wal_with_stats(&wal).unwrap();
        assert!(stats.vectors_loaded >= 1);

        let results = replayed.retrieve_with_time_range_and_query_vector(
            &RetrievalRequest {
                tenant_id: "tenant-a".into(),
                query: "vector indexed claim".into(),
                top_k: 1,
                stance_mode: StanceMode::Balanced,
            },
            None,
            None,
            Some(&[0.1, 0.3, 0.5, 0.7]),
        );
        assert_eq!(results.first().map(|r| r.claim_id.as_str()), Some("c-vec"));

        cleanup_persistence_files(&wal);
    }

    #[test]
    fn batch_commit_wal_record_replays_without_mutating_claim_state() {
        let wal_path = temp_wal_path();
        let mut wal = FileWal::open(&wal_path).unwrap();
        let mut store = InMemoryStore::new();

        store
            .ingest_bundle_persistent(
                &mut wal,
                claim("c-batch", "Batch WAL claim"),
                vec![],
                vec![],
            )
            .unwrap();
        wal.append_batch_commit(
            "commit-test-123",
            1,
            1_700_000_000_000,
            &["c-batch".to_string()],
        )
        .expect("batch commit append should succeed");

        let (replayed, stats) = InMemoryStore::load_from_wal_with_stats(&wal).unwrap();
        assert_eq!(replayed.claims_len(), 1);
        assert_eq!(stats.claims_loaded, 1);

        cleanup_persistence_files(&wal);
    }

    #[test]
    fn wal_replication_delta_returns_window_and_resync_signal() {
        let wal_path = temp_wal_path();
        let mut wal = FileWal::open(&wal_path).unwrap();
        let mut store = InMemoryStore::new();

        store
            .ingest_bundle_persistent(
                &mut wal,
                claim("c-r1", "Replication claim one"),
                vec![],
                vec![],
            )
            .unwrap();
        store
            .ingest_bundle_persistent(
                &mut wal,
                claim("c-r2", "Replication claim two"),
                vec![],
                vec![],
            )
            .unwrap();

        let first = wal.replication_delta_from(0, 1).unwrap();
        assert!(!first.needs_resync);
        assert_eq!(first.from_offset, 0);
        assert_eq!(first.next_offset, 1);
        assert_eq!(first.total_records, 2);
        assert_eq!(first.wal_lines.len(), 1);

        let gap = wal.replication_delta_from(10, 2).unwrap();
        assert!(gap.needs_resync);
        assert_eq!(gap.next_offset, 2);
        assert!(gap.wal_lines.is_empty());

        cleanup_persistence_files(&wal);
    }

    #[test]
    fn wal_replication_export_replaces_snapshot_and_wal_state() {
        let src_wal_path = temp_wal_path();
        let mut src_wal = FileWal::open(&src_wal_path).unwrap();
        let mut src_store = InMemoryStore::new();
        src_store
            .ingest_bundle_persistent(
                &mut src_wal,
                claim("c-src-1", "Source claim one"),
                vec![],
                vec![],
            )
            .unwrap();
        src_store.checkpoint_and_compact(&mut src_wal).unwrap();
        src_store
            .ingest_bundle_persistent(
                &mut src_wal,
                claim("c-src-2", "Source claim two"),
                vec![],
                vec![],
            )
            .unwrap();
        let export = src_wal.replication_export().unwrap();
        assert!(!export.snapshot_lines.is_empty());
        assert_eq!(export.wal_lines.len(), 1);

        let dst_wal_path = temp_wal_path();
        let mut dst_wal = FileWal::open(&dst_wal_path).unwrap();
        dst_wal.replace_with_replication_export(&export).unwrap();

        let replayed = InMemoryStore::load_from_wal(&dst_wal).unwrap();
        assert_eq!(replayed.claims_len(), 2);
        assert!(dst_wal.snapshot_path().exists());

        cleanup_persistence_files(&src_wal);
        cleanup_persistence_files(&dst_wal);
    }

    #[test]
    fn observe_batch_commit_is_idempotent_for_same_payload() {
        let mut store = InMemoryStore::new();
        let claim_ids = vec!["c-idem-1".to_string(), "c-idem-2".to_string()];
        store
            .observe_batch_commit(
                "commit-idem-1",
                claim_ids.len(),
                1_700_000_000_000,
                &claim_ids,
            )
            .expect("first commit metadata insert should succeed");
        store
            .observe_batch_commit(
                "commit-idem-1",
                claim_ids.len(),
                1_700_000_000_123,
                &claim_ids,
            )
            .expect("same payload should replay idempotently");
        let metadata = store
            .batch_commit_metadata("commit-idem-1")
            .expect("metadata should be retained");
        assert_eq!(metadata.batch_size, 2);
        assert_eq!(metadata.claim_ids, claim_ids);
        assert_eq!(
            metadata.payload_fingerprint,
            batch_commit_payload_fingerprint(2, &claim_ids)
        );
    }

    #[test]
    fn observe_batch_commit_rejects_payload_conflict_for_existing_commit_id() {
        let mut store = InMemoryStore::new();
        store
            .observe_batch_commit(
                "commit-conflict-1",
                1,
                1_700_000_000_000,
                &["c-conflict-1".to_string()],
            )
            .expect("first commit metadata insert should succeed");
        let err = store
            .observe_batch_commit(
                "commit-conflict-1",
                1,
                1_700_000_000_111,
                &["c-conflict-2".to_string()],
            )
            .expect_err("conflicting payload should fail");
        assert!(matches!(err, StoreError::Conflict(_)));
        let message = format!("{err:?}");
        assert!(message.contains("existing_fingerprint="));
        assert!(message.contains("incoming_fingerprint="));
    }

    #[test]
    fn batch_commit_payload_fingerprint_is_deterministic_and_order_sensitive() {
        let ordered = vec!["c1".to_string(), "c2".to_string()];
        let swapped = vec!["c2".to_string(), "c1".to_string()];
        let first = batch_commit_payload_fingerprint(2, &ordered);
        let second = batch_commit_payload_fingerprint(2, &ordered);
        let third = batch_commit_payload_fingerprint(2, &swapped);
        assert_eq!(first, second);
        assert_ne!(first, third);
    }

    #[test]
    fn apply_persisted_batch_commit_line_rejects_replay_payload_divergence() {
        let mut store = InMemoryStore::new();
        store
            .apply_persisted_record_line("B\tcommit-diverge-1\t1\t1700000000000\t2:c1")
            .expect("first replayed batch commit should apply");
        let err = store
            .apply_persisted_record_line("B\tcommit-diverge-1\t1\t1700000000001\t2:c2")
            .expect_err("payload divergence must be rejected");
        assert!(matches!(err, StoreError::Conflict(_)));
        let message = format!("{err:?}");
        assert!(message.contains("existing_fingerprint="));
        assert!(message.contains("incoming_fingerprint="));
    }

    #[test]
    fn query_embedding_prefers_vector_similar_claim() {
        let mut store = InMemoryStore::new();
        store
            .ingest_bundle(claim("c-near", "Semantic nearby claim"), vec![], vec![])
            .unwrap();
        store
            .ingest_bundle(claim("c-far", "Semantic distant claim"), vec![], vec![])
            .unwrap();
        store
            .upsert_claim_vector("c-near", vec![1.0, 0.0, 0.0, 0.0])
            .unwrap();
        store
            .upsert_claim_vector("c-far", vec![0.0, 1.0, 0.0, 0.0])
            .unwrap();

        let results = store.retrieve_with_time_range_and_query_vector(
            &RetrievalRequest {
                tenant_id: "tenant-a".into(),
                query: "semantic claim".into(),
                top_k: 2,
                stance_mode: StanceMode::Balanced,
            },
            None,
            None,
            Some(&[0.99, 0.01, 0.0, 0.0]),
        );
        assert_eq!(results.first().map(|r| r.claim_id.as_str()), Some("c-near"));

        let stats = store.index_stats();
        assert_eq!(stats.vector_count, 2);
        assert!(stats.ann_vector_buckets >= 1);
    }

    #[test]
    fn ann_and_exact_vector_top_candidates_rank_expected_claim() {
        let mut store = InMemoryStore::new();
        store
            .ingest_bundle(claim("c-near", "Semantic nearby claim"), vec![], vec![])
            .unwrap();
        store
            .ingest_bundle(claim("c-far", "Semantic distant claim"), vec![], vec![])
            .unwrap();
        store
            .upsert_claim_vector("c-near", vec![1.0, 0.0, 0.0, 0.0])
            .unwrap();
        store
            .upsert_claim_vector("c-far", vec![0.0, 1.0, 0.0, 0.0])
            .unwrap();

        let query = [0.99, 0.01, 0.0, 0.0];
        let ann = store.ann_vector_top_candidates("tenant-a", &query, 1);
        let exact = store.exact_vector_top_candidates("tenant-a", &query, 1);

        assert_eq!(ann.first().map(String::as_str), Some("c-near"));
        assert_eq!(exact.first().map(String::as_str), Some("c-near"));
    }

    #[test]
    fn tenant_vector_index_tracks_every_vector_and_converts_to_hnsw() {
        let tuning = AnnTuningConfig {
            flat_threshold: 64,
            ..AnnTuningConfig::default()
        };
        let mut store = InMemoryStore::new_with_ann_tuning(tuning);

        for i in 0..256 {
            let claim_id = format!("c-level-{i}");
            store
                .ingest_bundle(claim(&claim_id, "vector index population"), vec![], vec![])
                .unwrap();
            let vector = vec![0.1 + (i as f32 * 0.001), 0.2, 0.3, 0.4 + (i % 7) as f32];
            store.upsert_claim_vector(&claim_id, vector).unwrap();
            let index = store
                .vector_indexes
                .get("tenant-a")
                .expect("tenant vector index should exist");
            assert_eq!(index.len(), i + 1);
            assert_eq!(index.is_hnsw(), i + 1 > 64, "after {} vectors", i + 1);
        }
        let stats = store.index_stats();
        assert_eq!(stats.ann_vector_buckets, 256);
        assert!(stats.vector_index_bytes > 0);
    }

    #[test]
    fn store_ann_tuning_can_be_overridden() {
        let tuning = AnnTuningConfig {
            connectivity: 8,
            expansion_add: 64,
            expansion_search: 32,
            flat_threshold: 1000,
            rerank: 20,
        };
        let store = InMemoryStore::new_with_ann_tuning(tuning.clone());
        assert_eq!(store.ann_tuning(), &tuning);
    }

    #[test]
    fn set_ann_tuning_rebuilds_existing_indexes() {
        let mut store = InMemoryStore::new();
        for i in 0..100 {
            let claim_id = format!("c-{i}");
            store
                .ingest_bundle(claim(&claim_id, "rebuild me"), vec![], vec![])
                .unwrap();
            store
                .upsert_claim_vector(&claim_id, vec![1.0, i as f32 * 0.01, 0.5, 0.25])
                .unwrap();
        }
        assert!(!store.vector_indexes["tenant-a"].is_hnsw());
        store.set_ann_tuning(AnnTuningConfig {
            flat_threshold: 10,
            ..AnnTuningConfig::default()
        });
        assert!(store.vector_indexes["tenant-a"].is_hnsw());
        assert_eq!(store.vector_indexes["tenant-a"].len(), 100);
        let top = store.ann_vector_top_candidates("tenant-a", &[1.0, 0.0, 0.5, 0.25], 1);
        assert_eq!(top, vec!["c-0".to_string()]);
    }

    #[test]
    fn vector_backend_env_cpu_selects_cpu_runtime() {
        let _guard = EnvVarGuard::set(VECTOR_BACKEND_ENV, "cpu");
        let store = InMemoryStore::new();
        assert_eq!(store.vector_backend_runtime(), VectorBackendRuntime::Cpu);
        assert_eq!(store.vector_backend_label(), "cpu");
    }

    #[test]
    #[cfg(not(feature = "gpu-backend"))]
    fn vector_backend_env_gpu_without_feature_falls_back_to_cpu() {
        let _guard = EnvVarGuard::set(VECTOR_BACKEND_ENV, "gpu");
        let store = InMemoryStore::new();
        assert_eq!(
            store.vector_backend_runtime(),
            VectorBackendRuntime::CpuFallbackFeatureDisabled
        );
        assert_eq!(store.vector_backend_label(), "cpu (gpu-feature-disabled)");
    }

    #[test]
    #[cfg(feature = "gpu-backend")]
    fn vector_backend_env_gpu_with_feature_is_gpu_or_runtime_fallback() {
        let _guard = EnvVarGuard::set(VECTOR_BACKEND_ENV, "gpu");
        let store = InMemoryStore::new();
        assert!(matches!(
            store.vector_backend_runtime(),
            VectorBackendRuntime::Gpu | VectorBackendRuntime::CpuFallbackUnavailable
        ));
    }

    #[test]
    fn tenant_vector_dimension_mismatch_is_rejected() {
        let mut store = InMemoryStore::new();
        store
            .ingest_bundle(claim("c1", "First vector claim"), vec![], vec![])
            .unwrap();
        store
            .ingest_bundle(claim("c2", "Second vector claim"), vec![], vec![])
            .unwrap();

        store
            .upsert_claim_vector("c1", vec![0.1, 0.2, 0.3])
            .unwrap();
        let err = store.upsert_claim_vector("c2", vec![0.1, 0.2]).unwrap_err();
        match err {
            StoreError::InvalidVector(message) => {
                assert!(message.contains("dimension mismatch"));
            }
            other => panic!("expected InvalidVector, got {other:?}"),
        }
    }

    #[test]
    fn cosine_similarity_is_clamped_finite_and_rejects_degenerate_input() {
        // f32 accumulation would overflow to inf/NaN here.
        let huge = [3.0e38f32, 3.0e38, 3.0e38];
        let one = cosine_similarity(&huge, &huge).expect("finite result");
        assert!((-1.0..=1.0).contains(&one));
        assert!((one - 1.0).abs() < 1e-6);
        let opposite = [-3.0e38f32, -3.0e38, -3.0e38];
        assert!((cosine_similarity(&huge, &opposite).unwrap() + 1.0).abs() < 1e-6);
        assert_eq!(cosine_similarity(&[0.0, 0.0], &[1.0, 0.0]), None);
        assert_eq!(cosine_similarity(&[1.0], &[1.0, 0.0]), None);
        assert_eq!(cosine_similarity(&[f32::NAN, 1.0], &[1.0, 1.0]), None);
    }

    #[test]
    fn claim_id_reuse_across_tenants_is_rejected() {
        let mut store = InMemoryStore::new();
        store
            .ingest_bundle(
                claim_for_tenant("c-tenant-collision", "tenant-a claim", "tenant-a"),
                vec![],
                vec![],
            )
            .expect("initial ingest should succeed");

        let err = store
            .ingest_bundle(
                claim_for_tenant("c-tenant-collision", "tenant-b claim", "tenant-b"),
                vec![],
                vec![],
            )
            .expect_err("cross-tenant claim_id reuse should be rejected");
        match err {
            StoreError::Conflict(message) => {
                // SEC-18: the error must not reveal the owning tenant.
                assert_eq!(message, "claim_id already exists");
                assert!(!message.contains("tenant-a"));
            }
            other => panic!("expected Conflict, got {other:?}"),
        }

        assert_eq!(store.claims_for_tenant("tenant-a").len(), 1);
        assert_eq!(store.claims_for_tenant("tenant-b").len(), 0);
    }

    #[test]
    fn ingest_bundle_persistent_rejects_cross_tenant_claim_id_before_wal_append() {
        let wal_path = temp_wal_path();
        let mut wal = FileWal::open(&wal_path).expect("wal should open");
        let mut store = InMemoryStore::new();

        store
            .ingest_bundle_persistent(
                &mut wal,
                claim_for_tenant("c-tenant-collision", "tenant-a claim", "tenant-a"),
                vec![],
                vec![],
            )
            .expect("initial persistent ingest should succeed");
        let before = wal
            .wal_record_count()
            .expect("wal record count should be readable");

        let err = store
            .ingest_bundle_persistent(
                &mut wal,
                claim_for_tenant("c-tenant-collision", "tenant-b claim", "tenant-b"),
                vec![],
                vec![],
            )
            .expect_err("cross-tenant claim_id reuse should be rejected");
        assert!(matches!(err, StoreError::Conflict(_)));
        let after = wal
            .wal_record_count()
            .expect("wal record count should be readable");
        assert_eq!(after, before);

        cleanup_persistence_files(&wal);
    }

    #[test]
    fn load_from_wal_rejects_cross_tenant_claim_id_collision() {
        let wal_path = temp_wal_path();
        let mut wal = FileWal::open(&wal_path).expect("wal should open");

        wal.append_claim(&claim_for_tenant(
            "c-tenant-collision",
            "tenant-a claim",
            "tenant-a",
        ))
        .expect("tenant-a claim append should succeed");
        wal.append_claim(&claim_for_tenant(
            "c-tenant-collision",
            "tenant-b claim",
            "tenant-b",
        ))
        .expect("tenant-b claim append should succeed");

        let err = InMemoryStore::load_from_wal(&wal)
            .err()
            .expect("cross-tenant claim_id collision should fail replay");
        assert!(matches!(err, StoreError::Conflict(_)));

        cleanup_persistence_files(&wal);
    }
}
