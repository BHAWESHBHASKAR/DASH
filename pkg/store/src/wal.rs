//! Write-Ahead Log (WAL) types and the [`FileWal`] implementation.
//!
//! The WAL is the canonical source of truth for crash recovery. On
//! restart, [`super::InMemoryStore::load_from_disk_and_wal`] replays
//! the snapshot (if any) plus the WAL delta. Reordering, batching,
//! and rotation all live behind this module's public API.
//!
//! Two top-level types are exported:
//! - [`WalEvent`] — an in-memory record of one mutation, used by
//!   `InMemoryStore.wal` for the segment-cache replay path.
//! - [`FileWal`] — the on-disk append-only log.
//!
//! All other types in this module are either private helpers
//! ([`PersistedRecord`], [`ClaimVectorRecord`], [`BatchCommitRecord`])
//! or wire/stats types ([`WalReplayBoundary`], [`WalReplicationDelta`],
//! etc.) that the rest of the crate consumes via re-exports from
//! `lib.rs`.

use std::collections::{BTreeMap, HashSet};
use std::fs::{File, OpenOptions, create_dir_all, rename};
use std::io::{BufRead, BufReader, Read, Write};

const SNAPSHOT_HEADER: &str = "SNAP\t1";
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use schema::{Claim, ClaimEdge, ClaimType, Evidence, Relation, Stance};

use crate::StoreError;

mod replication_index;

use replication_index::{ReplicationFilter, ReplicationIndex};

#[derive(Debug, Clone, PartialEq)]
pub enum WalEvent {
    ClaimUpsert(String),
    EvidenceUpsert(String),
    EdgeUpsert(String),
    ClaimVectorUpsert(String),
    BatchCommit(String),
    /// A tombstone was applied; carries the tombstone's target (claim id,
    /// evidence id or tenant id).
    Tombstone(String),
}

#[derive(Debug, Clone)]
pub(crate) enum PersistedRecord {
    Claim(Claim),
    Evidence(Evidence),
    Edge(ClaimEdge),
    ClaimVector(ClaimVectorRecord),
    BatchCommit(BatchCommitRecord),
    Tombstone(TombstoneRecord),
}

/// What a delete removes. Encoded in the WAL as a checksummed `T2` record
/// (see [`record_to_line`]); readers that predate tombstones reject the
/// unknown kind instead of skipping it, so a deleted claim can never be
/// resurrected by an old binary replaying a newer log.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Tombstone {
    /// The claim, its vector, its evidence and every edge from or to it.
    Claim { tenant_id: String, claim_id: String },
    /// Every evidence row with this id on the tenant's claims.
    Evidence {
        tenant_id: String,
        evidence_id: String,
    },
    /// Every claim of the tenant (with vectors, evidence and edges), the
    /// tenant's vector dimension and index, and the batch-commit metadata
    /// that names any of its claims.
    Tenant { tenant_id: String },
}

impl Tombstone {
    pub fn tenant_id(&self) -> &str {
        match self {
            Self::Claim { tenant_id, .. }
            | Self::Evidence { tenant_id, .. }
            | Self::Tenant { tenant_id } => tenant_id,
        }
    }

    /// The deleted object's id (the tenant id for a tenant tombstone).
    pub fn target_id(&self) -> &str {
        match self {
            Self::Claim { claim_id, .. } => claim_id,
            Self::Evidence { evidence_id, .. } => evidence_id,
            Self::Tenant { tenant_id } => tenant_id,
        }
    }

    /// `claim`, `evidence` or `tenant`.
    pub fn scope(&self) -> &'static str {
        match self {
            Self::Claim { .. } => "claim",
            Self::Evidence { .. } => "evidence",
            Self::Tenant { .. } => "tenant",
        }
    }

    /// Rejects empty or whitespace-only identifiers.
    pub fn validate(&self) -> Result<(), StoreError> {
        if self.tenant_id().trim().is_empty() {
            return Err(StoreError::Parse(
                "tombstone tenant_id must not be empty".to_string(),
            ));
        }
        if self.target_id().trim().is_empty() {
            return Err(StoreError::Parse(format!(
                "tombstone {}_id must not be empty",
                self.scope()
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub(crate) struct TombstoneRecord {
    pub(crate) tombstone: Tombstone,
    pub(crate) ts_unix_ms: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct ClaimVectorRecord {
    pub(crate) claim_id: String,
    pub(crate) values: Vec<f32>,
}

#[derive(Debug, Clone)]
pub(crate) struct BatchCommitRecord {
    pub(crate) commit_id: String,
    pub(crate) batch_size: usize,
    pub(crate) ts_unix_ms: u64,
    pub(crate) claim_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalCheckpointStats {
    pub snapshot_records: usize,
    pub truncated_wal_records: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CheckpointPolicy {
    pub max_wal_records: Option<usize>,
    pub max_wal_bytes: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct WalReplayStats {
    pub snapshot_records: usize,
    pub wal_records: usize,
    /// Number of torn (incomplete or corrupt) final WAL lines that were
    /// discarded since this WAL handle was opened.
    pub torn_tail_dropped: usize,
    /// Records that could not be replayed (legacy records that no longer
    /// parse or validate, poisoned vectors) and were copied to
    /// `<wal>.quarantine` instead of being applied.
    pub quarantined_records: usize,
    /// Otherwise valid records skipped because they depend on a
    /// quarantined claim (evidence, edges, vectors).
    pub dependent_skipped: usize,
}

/// Environment variable that switches replay to strict mode.
pub const WAL_REPLAY_STRICT_ENV: &str = "DASH_WAL_REPLAY_STRICT";

/// How replay treats records that cannot be applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReplayPolicy {
    /// Legacy (pre-checksum) records that fail to parse or validate, and
    /// poisoned vectors, are quarantined; replay continues.
    #[default]
    Lenient,
    /// Any record that cannot be applied fails replay.
    Strict,
}

impl ReplayPolicy {
    /// Parses the value of [`WAL_REPLAY_STRICT_ENV`]: `1`, `true`, `yes`
    /// or `on` (case-insensitive) select strict mode.
    pub fn from_env_value(value: Option<&str>) -> Self {
        match value.map(|v| v.trim().to_ascii_lowercase()) {
            Some(v) if matches!(v.as_str(), "1" | "true" | "yes" | "on") => Self::Strict,
            _ => Self::Lenient,
        }
    }

    pub fn from_env() -> Self {
        Self::from_env_value(std::env::var(WAL_REPLAY_STRICT_ENV).ok().as_deref())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct WalReplayBoundary {
    pub snapshot_active: bool,
    pub snapshot_record_count: usize,
    pub wal_delta_record_count: usize,
    pub total_replay_record_count: usize,
    /// Persistent identifier of the current WAL file lineage. Changes
    /// whenever the WAL is compacted or reset.
    pub wal_generation: u64,
}

/// A point in the WAL: `records` lines of the WAL lineage `generation`
/// (the snapshot, if any, is part of that lineage). A persisted vector index
/// records the position it reflects so a restart can replay only the lines
/// after it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct WalPosition {
    pub generation: u64,
    pub records: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalReplicationDelta {
    pub from_offset: usize,
    pub next_offset: usize,
    pub total_records: usize,
    pub needs_resync: bool,
    pub wal_lines: Vec<String>,
}

/// Generation-aware replication frame. `from_offset`/`next_offset` are
/// positions inside the WAL lineage identified by `generation`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalReplicationFrame {
    pub generation: u64,
    pub from_offset: usize,
    pub next_offset: usize,
    pub total_records: usize,
    pub needs_resync: bool,
    pub wal_lines: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalReplicationExport {
    pub snapshot_lines: Vec<String>,
    pub wal_lines: Vec<String>,
}

/// Prefix of the batch-commit record that opens a commit group. A group is
/// `B2(~grp:<id>) <records...> B2(<id>)`; on replay a group whose closing
/// `B2(<id>)` is missing was torn by a crash and is discarded as a whole,
/// so a bundle is never partially applied (DATA-10).
pub const GROUP_BEGIN_PREFIX: &str = "~grp:";

/// Prefix of the commit id used by single `/v1/ingest` transactions. These
/// markers only delimit the WAL group; they are never registered as batch
/// commits in the store.
pub const SINGLE_TX_PREFIX: &str = "~tx:";

/// `true` for commit ids that only delimit WAL groups and carry no batch
/// metadata of their own.
/// Upper bound on how far a replication frame may be extended past
/// `max_records` to end on a commit-group boundary. Followers accept frames
/// of `max_records + REPLICATION_GROUP_EXTENSION_MAX` lines.
pub const REPLICATION_GROUP_EXTENSION_MAX: usize = 1_000_000;

enum GroupEvent {
    Begin(String),
    End(String),
}

fn group_event(line: &str) -> Option<GroupEvent> {
    if !line.starts_with("B2	") {
        return None;
    }
    let Ok(PersistedRecord::BatchCommit(commit)) = line_to_record(line) else {
        return None;
    };
    if let Some(id) = commit.commit_id.strip_prefix(GROUP_BEGIN_PREFIX) {
        return Some(GroupEvent::Begin(id.to_string()));
    }
    Some(GroupEvent::End(commit.commit_id))
}

fn closes_group(open_id: &str, end_id: &str) -> bool {
    end_id == open_id || end_id.strip_prefix(SINGLE_TX_PREFIX) == Some(open_id)
}

/// Number of leading `lines` that form complete commit groups (plus
/// ungrouped legacy records). A trailing unterminated group is excluded, so
/// a replication follower applies only whole groups and re-fetches from the
/// group's first line.
pub fn complete_group_prefix_len(lines: &[String]) -> usize {
    let mut open: Option<(usize, String)> = None;
    for (idx, line) in lines.iter().enumerate() {
        match group_event(line) {
            Some(GroupEvent::Begin(id)) => open = Some((idx, id)),
            Some(GroupEvent::End(id))
                if open.as_ref().is_some_and(|(_, o)| closes_group(o, &id)) =>
            {
                open = None;
            }
            _ => {}
        }
    }
    open.map_or(lines.len(), |(start, _)| start)
}

/// Why a frame cannot be extended to the end of its commit group.
#[derive(Debug, PartialEq, Eq)]
struct GroupTooLarge {
    start: usize,
    cap: usize,
}

/// A replication frame cut from the view, or why it cannot be cut.
#[derive(Debug, PartialEq, Eq)]
enum FrameCut {
    Lines {
        next_offset: usize,
        lines: Vec<String>,
    },
    GroupTooLarge(GroupTooLarge),
}

/// Cuts the frame starting at view line `from` (`lines` yields the view from
/// `from` on; the view holds `total` lines). The frame covers up to
/// `max_records` lines and does not end inside a commit group: a group that
/// closes within `cap` records of its first line is shipped whole. A larger
/// group that starts after `from` is left for the next frame (the frame ends
/// just before it); one that starts at `from` can never be shipped, which is
/// an error rather than a frame the follower would hold back forever. A
/// group still open at the end of the log is a write in progress and is left
/// as is.
///
/// Lines are consumed lazily: past the frame only while a group is open, and
/// only the frame's own lines are kept in memory.
fn cut_frame(
    mut lines: impl Iterator<Item = Result<String, StoreError>>,
    from: usize,
    max_records: usize,
    total: usize,
    cap: usize,
) -> Result<FrameCut, StoreError> {
    let next = from.saturating_add(max_records.max(1)).min(total);
    let mut take = |idx: usize| -> Result<String, StoreError> {
        lines.next().transpose()?.ok_or_else(|| {
            StoreError::Io(format!(
                "replication view ended at line {idx}, expected {total} lines"
            ))
        })
    };
    let mut frame = Vec::with_capacity(next.saturating_sub(from));
    let mut open: Option<(usize, String)> = None;
    for idx in from..next {
        let line = take(idx)?;
        match group_event(&line) {
            Some(GroupEvent::Begin(id)) => open = Some((idx, id)),
            Some(GroupEvent::End(id))
                if open.as_ref().is_some_and(|(_, o)| closes_group(o, &id)) =>
            {
                open = None;
            }
            _ => {}
        }
        frame.push(line);
    }
    let Some((start, id)) = open else {
        return Ok(FrameCut::Lines {
            next_offset: next,
            lines: frame,
        });
    };
    let closes = |line: &str| matches!(group_event(line), Some(GroupEvent::End(end)) if closes_group(&id, &end));
    let bound = total.min(start.saturating_add(cap));
    let mut idx = next;
    while idx < bound {
        let line = take(idx)?;
        idx += 1;
        let done = closes(&line);
        frame.push(line);
        if done {
            return Ok(FrameCut::Lines {
                next_offset: idx,
                lines: frame,
            });
        }
    }
    frame.truncate(next - from);
    let mut closes_beyond_cap = false;
    while idx < total {
        let line = take(idx)?;
        idx += 1;
        if closes(&line) {
            closes_beyond_cap = true;
            break;
        }
    }
    if !closes_beyond_cap {
        return Ok(FrameCut::Lines {
            next_offset: next,
            lines: frame,
        });
    }
    if start > from {
        frame.truncate(start - from);
        Ok(FrameCut::Lines {
            next_offset: start,
            lines: frame,
        })
    } else {
        Ok(FrameCut::GroupTooLarge(GroupTooLarge { start, cap }))
    }
}

/// The commit id carried by a batch-commit WAL line (legacy `B` or checksummed
/// `B2`), or `None` for any other line, an unreadable line, or a commit-group
/// begin/end marker (markers are framing, not client-visible batch commits).
pub fn batch_commit_id_from_wal_line(line: &str) -> Option<String> {
    if !(line.starts_with("B\t") || line.starts_with("B2\t")) {
        return None;
    }
    match line_to_record(line).ok()? {
        PersistedRecord::BatchCommit(commit) if !is_group_marker_commit_id(&commit.commit_id) => {
            Some(commit.commit_id)
        }
        _ => None,
    }
}

/// The tombstone carried by a WAL line, or `None` for any other (or an
/// unreadable) line. Lets a replica see which tenants a replicated batch
/// deleted from, e.g. to refresh derived per-tenant files.
pub fn tombstone_from_wal_line(line: &str) -> Option<Tombstone> {
    if !line.starts_with("T2\t") {
        return None;
    }
    match line_to_record(line).ok()? {
        PersistedRecord::Tombstone(record) => Some(record.tombstone),
        _ => None,
    }
}

pub fn is_group_marker_commit_id(commit_id: &str) -> bool {
    commit_id.starts_with(GROUP_BEGIN_PREFIX) || commit_id.starts_with(SINGLE_TX_PREFIX)
}

/// Drops records of commit groups that were never closed. Records outside
/// any group (legacy appends) pass through untouched. Returns the kept
/// records and the number of discarded records.
pub(crate) fn resolve_commit_groups<T>(
    records: Vec<T>,
    record_of: impl Fn(&T) -> &PersistedRecord,
) -> (Vec<T>, usize) {
    let mut out = Vec::with_capacity(records.len());
    let mut open: Option<(String, Vec<T>)> = None;
    let mut discarded = 0usize;
    for item in records {
        if let PersistedRecord::BatchCommit(commit) = record_of(&item) {
            if let Some(id) = commit.commit_id.strip_prefix(GROUP_BEGIN_PREFIX) {
                if let Some((_, buffered)) = open.take() {
                    discarded += buffered.len() + 1;
                }
                open = Some((id.to_string(), Vec::new()));
                continue;
            }
            let closes = open.as_ref().is_some_and(|(id, _)| {
                commit.commit_id == *id
                    || commit.commit_id.strip_prefix(SINGLE_TX_PREFIX) == Some(id.as_str())
            });
            if closes {
                if let Some((_, buffered)) = open.take() {
                    out.extend(buffered);
                }
                out.push(item);
                continue;
            }
        }
        match open.as_mut() {
            Some((_, buffered)) => buffered.push(item),
            None => out.push(item),
        }
    }
    if let Some((_, buffered)) = open {
        discarded += buffered.len() + 1;
    }
    (out, discarded)
}

pub struct FileWal {
    path: PathBuf,
    wal_records: usize,
    sync_every_records: usize,
    append_buffer_max_records: usize,
    sync_interval: Option<Duration>,
    background_flush_only: bool,
    append_buffer: Vec<String>,
    pub(crate) unsynced_records: usize,
    last_sync_at: Instant,
    generation: u64,
    torn_tail_dropped: usize,
    /// Lines left out of the most recent replication view because lenient
    /// replay would quarantine them.
    replication_skipped: usize,
    /// Largest commit group (in records) a replication frame may be
    /// extended to cover.
    replication_group_cap: usize,
    replication_group_too_large_total: u64,
    /// Incremental index of the replication view, so a replication frame
    /// reads only the lines it ships instead of the whole file.
    replication_index: ReplicationIndex,
    /// Set after an fsync failure. Once set, every write path fails closed:
    /// after a failed fsync the kernel may already have dropped the dirty
    /// pages, so retrying the fsync could report success for data that never
    /// reached the disk (fsyncgate). Only a restart, which re-reads the log
    /// from disk, clears it.
    poisoned: Option<String>,
}

/// Prefix of the error returned by every write to a poisoned WAL.
pub const WAL_POISONED_PREFIX: &str = "wal_poisoned";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalWritePolicy {
    pub sync_every_records: usize,
    pub append_buffer_max_records: usize,
    pub sync_interval: Option<Duration>,
    pub background_flush_only: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalRollbackPoint {
    file_len_bytes: u64,
    wal_records: usize,
}

impl Default for WalWritePolicy {
    fn default() -> Self {
        Self {
            sync_every_records: 1,
            append_buffer_max_records: 1,
            sync_interval: None,
            background_flush_only: false,
        }
    }
}

impl FileWal {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        Self::open_with_sync_every_records(path, 1)
    }

    pub fn open_with_sync_every_records(
        path: impl AsRef<Path>,
        sync_every_records: usize,
    ) -> Result<Self, StoreError> {
        Self::open_with_policy(
            path,
            WalWritePolicy {
                sync_every_records,
                ..WalWritePolicy::default()
            },
        )
    }

    pub fn open_with_policy(
        path: impl AsRef<Path>,
        policy: WalWritePolicy,
    ) -> Result<Self, StoreError> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            create_dir_all(parent)?;
        }
        let existed = path.exists();
        OpenOptions::new().create(true).append(true).open(&path)?;
        if !existed {
            sync_parent_dir(&path)?;
        }
        let torn_tail_dropped = repair_torn_tail(&path)? + truncate_unterminated_group(&path)?;
        let wal_records = count_non_empty_lines(&path)?;
        let generation = load_or_create_generation(&generation_path_for(&path))?;
        Ok(Self {
            path,
            wal_records,
            sync_every_records: policy.sync_every_records.max(1),
            append_buffer_max_records: policy.append_buffer_max_records.max(1),
            sync_interval: policy.sync_interval,
            background_flush_only: policy.background_flush_only,
            append_buffer: Vec::new(),
            unsynced_records: 0,
            last_sync_at: Instant::now(),
            generation,
            torn_tail_dropped,
            replication_skipped: 0,
            replication_group_cap: REPLICATION_GROUP_EXTENSION_MAX,
            replication_group_too_large_total: 0,
            replication_index: ReplicationIndex::default(),
            poisoned: None,
        })
    }

    /// Why this WAL refuses writes, or `None` while it is healthy. Set by the
    /// first failed fsync and never cleared by this handle.
    pub fn poisoned_reason(&self) -> Option<&str> {
        self.poisoned.as_deref()
    }

    /// Testing aid for crates that cannot reach the store's failpoints: puts
    /// the WAL into the state a failed fsync leaves it in.
    #[doc(hidden)]
    pub fn poison_for_testing(&mut self, reason: &str) {
        self.poisoned = Some(reason.to_string());
    }

    fn ensure_writable(&self) -> Result<(), StoreError> {
        match &self.poisoned {
            Some(reason) => Err(StoreError::Io(format!(
                "{WAL_POISONED_PREFIX}: an earlier fsync failed ({reason}); the WAL refuses writes until the service restarts"
            ))),
            None => Ok(()),
        }
    }

    fn poison(&mut self, err: &std::io::Error) -> StoreError {
        let reason = format!("fsync of {} failed: {err}", self.path.display());
        eprintln!("error: {reason}; the WAL is now poisoned and refuses writes");
        self.poisoned = Some(reason.clone());
        StoreError::Io(format!("{WAL_POISONED_PREFIX}: {reason}"))
    }

    /// Appends `lines` as one unit and applies the write policy once for the
    /// whole unit (with the default policy: one write and one fsync). This is
    /// the group-commit entry point: the lines of many requests share a single
    /// fsync.
    ///
    /// On a write error the file is truncated back to where it was, so a
    /// half-written unit never sits in front of later records, and the error
    /// is returned. On an fsync error the WAL is poisoned (see
    /// [`FileWal::poisoned_reason`]) and no rollback is attempted.
    pub fn append_group_lines(&mut self, lines: &[String]) -> Result<(), StoreError> {
        self.ensure_writable()?;
        if lines.is_empty() {
            return Ok(());
        }
        let rollback_point = self.begin_rollback_point()?;
        self.append_buffer.extend(lines.iter().cloned());
        self.wal_records += lines.len();
        self.unsynced_records += lines.len();
        let result = self.apply_write_policy();
        if let Err(err) = result {
            if self.poisoned.is_none() {
                if let Err(rollback_err) = self.rollback_to(rollback_point) {
                    eprintln!(
                        "group commit rollback failed after WAL append error: {rollback_err:?}"
                    );
                }
                // The unit is not in the log, whether or not the truncation
                // above could run (it cannot when the file is unreachable).
                self.append_buffer.clear();
                self.wal_records = rollback_point.wal_records;
                self.unsynced_records = 0;
            }
            return Err(err);
        }
        Ok(())
    }

    /// `true` when the WAL (not the snapshot, which never holds one) contains
    /// a tombstone record. Used by the redb cold-start path: a redb file
    /// reflects a later state than the start of the log, and replaying writes
    /// that a later tombstone undid over that state is only safe from an
    /// empty store (a released vector dimension may have been re-established
    /// with a different size).
    pub fn contains_tombstones(&self) -> Result<bool, StoreError> {
        let is_tombstone = |line: &str| line.starts_with("T2\t");
        if self.append_buffer.iter().any(|line| is_tombstone(line)) {
            return Ok(true);
        }
        let scan = scan_wal(&self.path)?;
        Ok(scan.lines.iter().any(|(_, line)| is_tombstone(line)))
    }

    /// Persistent identifier of the current WAL lineage. It changes every
    /// time the WAL is compacted (checkpoint), replaced by a replication
    /// export, or rolled back over already-flushed records.
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Number of WAL/snapshot lines the most recent replication frame or
    /// export left out because lenient replay quarantines them (unparseable
    /// legacy lines and their dependents). They are never served to
    /// followers; offsets index the served (filtered) view.
    pub fn replication_skipped_lines(&self) -> usize {
        self.replication_skipped
    }

    /// Overrides the largest commit group (in records) a replication frame
    /// may be extended to cover (default [`REPLICATION_GROUP_EXTENSION_MAX`]).
    pub fn set_replication_group_cap(&mut self, cap: usize) {
        self.replication_group_cap = cap.max(1);
    }

    /// Frame requests refused because a commit group exceeded the cap
    /// (`replication_group_too_large`).
    pub fn replication_group_too_large_total(&self) -> u64 {
        self.replication_group_too_large_total
    }

    /// Torn tail lines discarded since this handle was opened.
    pub fn torn_tail_dropped(&self) -> usize {
        self.torn_tail_dropped
    }

    fn bump_generation(&mut self) -> Result<(), StoreError> {
        let mut next = new_generation();
        while next == self.generation {
            next = new_generation();
        }
        write_generation(&generation_path_for(&self.path), next)?;
        self.generation = next;
        Ok(())
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn sync_every_records(&self) -> usize {
        self.sync_every_records
    }

    pub fn append_buffer_max_records(&self) -> usize {
        self.append_buffer_max_records
    }

    pub fn sync_interval(&self) -> Option<Duration> {
        self.sync_interval
    }

    pub fn background_flush_only(&self) -> bool {
        self.background_flush_only
    }

    pub fn unsynced_record_count(&self) -> usize {
        self.unsynced_records
    }

    pub fn buffered_record_count(&self) -> usize {
        self.append_buffer.len()
    }

    pub fn generation_path(&self) -> PathBuf {
        generation_path_for(&self.path)
    }

    pub fn snapshot_path(&self) -> PathBuf {
        let mut path = self.path.clone().into_os_string();
        path.push(".snapshot");
        PathBuf::from(path)
    }

    pub fn append_claim(&mut self, claim: &Claim) -> Result<(), StoreError> {
        self.append_record(&PersistedRecord::Claim(claim.clone()))
    }

    pub fn append_evidence(&mut self, evidence: &Evidence) -> Result<(), StoreError> {
        self.append_record(&PersistedRecord::Evidence(evidence.clone()))
    }

    pub fn append_edge(&mut self, edge: &ClaimEdge) -> Result<(), StoreError> {
        self.append_record(&PersistedRecord::Edge(edge.clone()))
    }

    pub fn append_claim_vector(
        &mut self,
        claim_id: &str,
        values: &[f32],
    ) -> Result<(), StoreError> {
        self.append_record(&PersistedRecord::ClaimVector(ClaimVectorRecord {
            claim_id: claim_id.to_string(),
            values: values.to_vec(),
        }))
    }

    pub fn append_batch_commit(
        &mut self,
        commit_id: &str,
        batch_size: usize,
        ts_unix_ms: u64,
        claim_ids: &[String],
    ) -> Result<(), StoreError> {
        self.append_record(&PersistedRecord::BatchCommit(BatchCommitRecord {
            commit_id: commit_id.to_string(),
            batch_size,
            ts_unix_ms,
            claim_ids: claim_ids.to_vec(),
        }))
    }

    /// Opens a commit group. Every record appended afterwards belongs to
    /// the group until `append_batch_commit` is called with the same
    /// `commit_id` (single ingests use `SINGLE_TX_PREFIX + claim_id` for
    /// the closing record and pass the bare `claim_id`-based id here).
    pub fn begin_group(&mut self, group_id: &str, ts_unix_ms: u64) -> Result<(), StoreError> {
        self.append_record(&PersistedRecord::BatchCommit(BatchCommitRecord {
            commit_id: format!("{GROUP_BEGIN_PREFIX}{group_id}"),
            batch_size: 0,
            ts_unix_ms,
            claim_ids: Vec::new(),
        }))
    }

    pub fn wal_record_count(&self) -> Result<usize, StoreError> {
        Ok(self.wal_records)
    }

    /// Current position: this lineage and every record appended so far
    /// (including records still in the append buffer; flush first when the
    /// position must be durable).
    pub fn position(&self) -> WalPosition {
        WalPosition {
            generation: self.generation,
            records: self.wal_records,
        }
    }

    pub fn wal_size_bytes(&self) -> Result<u64, StoreError> {
        Ok(std::fs::metadata(&self.path)?.len())
    }

    pub fn replay_boundary(&self) -> Result<WalReplayBoundary, StoreError> {
        let snapshot_record_count = self.replay_snapshot_lines_raw()?.len();
        let mut wal_delta_record_count = self.replay_wal_lines_raw()?.len();
        wal_delta_record_count = wal_delta_record_count.saturating_add(self.append_buffer.len());
        Ok(WalReplayBoundary {
            snapshot_active: snapshot_record_count > 0,
            snapshot_record_count,
            wal_delta_record_count,
            total_replay_record_count: snapshot_record_count.saturating_add(wal_delta_record_count),
            wal_generation: self.generation,
        })
    }

    pub fn begin_rollback_point(&mut self) -> Result<WalRollbackPoint, StoreError> {
        self.flush_pending_sync()?;
        Ok(WalRollbackPoint {
            file_len_bytes: self.wal_size_bytes()?,
            wal_records: self.wal_records,
        })
    }

    pub fn rollback_to(&mut self, point: WalRollbackPoint) -> Result<(), StoreError> {
        self.ensure_writable()?;
        let discards_records = self.wal_records > point.wal_records;
        self.append_buffer.clear();
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&self.path)?;
        file.set_len(point.file_len_bytes)?;
        self.replication_index.reset();
        if let Err(err) = sync_wal_data(&file) {
            return Err(self.poison(&err));
        }
        // The file is back at the rollback point: the counters must say so
        // even if the generation bump below fails (a full disk can refuse
        // the new generation file while the truncation succeeded).
        self.wal_records = point.wal_records;
        self.unsynced_records = 0;
        self.last_sync_at = Instant::now();
        if discards_records {
            failpoint!("wal.rollback_truncated");
            // Rolled-back lines may already have been served to followers.
            self.bump_generation()?;
        }
        Ok(())
    }

    pub fn append_raw_record_line(&mut self, line: &str) -> Result<(), StoreError> {
        let line = line.trim();
        if line.is_empty() {
            return Err(StoreError::Parse(
                "raw WAL record line must not be empty".to_string(),
            ));
        }
        check_replicated_line(line)?;
        self.append_raw_record_line_unchecked(line.to_string())
    }

    /// Legacy offset-only delta. Does not detect compaction; prefer
    /// [`FileWal::replication_frame_from`].
    pub fn replication_delta_from(
        &mut self,
        from_offset: usize,
        max_records: usize,
    ) -> Result<WalReplicationDelta, StoreError> {
        let frame = self.replication_frame_inner(None, from_offset, max_records, false)?;
        Ok(WalReplicationDelta {
            from_offset: frame.from_offset,
            next_offset: frame.next_offset,
            total_records: frame.total_records,
            needs_resync: frame.needs_resync,
            wal_lines: frame.wal_lines,
        })
    }

    /// Generation-aware delta. `from_generation` is the generation the
    /// follower last observed (`None` if it has never synced). The frame
    /// has `needs_resync = true` when the generation differs (the WAL was
    /// compacted or reset, so offsets are meaningless), when the follower
    /// has no known generation but is not a fresh follower, or when
    /// `from_offset` is beyond the end of the WAL. On resync,
    /// `next_offset == total_records` and `wal_lines` is empty.
    pub fn replication_frame_from(
        &mut self,
        from_generation: Option<u64>,
        from_offset: usize,
        max_records: usize,
    ) -> Result<WalReplicationFrame, StoreError> {
        self.replication_frame_inner(from_generation, from_offset, max_records, true)
    }

    fn replication_frame_inner(
        &mut self,
        from_generation: Option<u64>,
        from_offset: usize,
        max_records: usize,
        check_generation: bool,
    ) -> Result<WalReplicationFrame, StoreError> {
        self.flush_pending_sync()?;
        // Only the new tail of the file is read here; the view itself is
        // read lazily below, from the frame's first line on.
        if !self.replication_index.refresh(&self.path)? {
            return self.replication_frame_full_scan(
                from_generation,
                from_offset,
                max_records,
                check_generation,
            );
        }
        self.note_replication_skipped(self.replication_index.skipped());
        let total_records = self.replication_index.total();
        if let Some(resync) = self.resync_frame(
            from_generation,
            from_offset,
            total_records,
            check_generation,
        ) {
            return Ok(resync);
        }
        let lines = self.replication_index.lines_from(&self.path, from_offset)?;
        let cut = cut_frame(
            lines,
            from_offset,
            max_records,
            total_records,
            self.replication_group_cap,
        )?;
        self.finish_frame(from_offset, total_records, cut)
    }

    /// [`FileWal::replication_frame_inner`] built from a full scan of the
    /// file. Used only when the incremental index cannot cover the file
    /// (see [`ReplicationIndex::refresh`]).
    fn replication_frame_full_scan(
        &mut self,
        from_generation: Option<u64>,
        from_offset: usize,
        max_records: usize,
        check_generation: bool,
    ) -> Result<WalReplicationFrame, StoreError> {
        let (wal_lines, skipped) = filter_replication_lines(self.replay_wal_lines_raw()?);
        self.note_replication_skipped(skipped);
        let total_records = wal_lines.len();
        if let Some(resync) = self.resync_frame(
            from_generation,
            from_offset,
            total_records,
            check_generation,
        ) {
            return Ok(resync);
        }
        let lines = wal_lines.into_iter().skip(from_offset).map(Ok);
        let cut = cut_frame(
            lines,
            from_offset,
            max_records,
            total_records,
            self.replication_group_cap,
        )?;
        self.finish_frame(from_offset, total_records, cut)
    }

    /// The resync frame when the follower's position is not in this view:
    /// another generation, no known generation on a non-fresh WAL, or an
    /// offset past the end.
    fn resync_frame(
        &self,
        from_generation: Option<u64>,
        from_offset: usize,
        total_records: usize,
        check_generation: bool,
    ) -> Option<WalReplicationFrame> {
        let generation_ok = !check_generation
            || match from_generation {
                Some(g) => g == self.generation,
                None => from_offset == 0 && !self.snapshot_path().exists(),
            };
        if generation_ok && from_offset <= total_records {
            return None;
        }
        Some(WalReplicationFrame {
            generation: self.generation,
            from_offset,
            next_offset: total_records,
            total_records,
            needs_resync: true,
            wal_lines: Vec::new(),
        })
    }

    fn finish_frame(
        &mut self,
        from_offset: usize,
        total_records: usize,
        cut: FrameCut,
    ) -> Result<WalReplicationFrame, StoreError> {
        match cut {
            FrameCut::Lines { next_offset, lines } => Ok(WalReplicationFrame {
                generation: self.generation,
                from_offset,
                next_offset,
                total_records,
                needs_resync: false,
                wal_lines: lines,
            }),
            FrameCut::GroupTooLarge(too_large) => {
                self.replication_group_too_large_total =
                    self.replication_group_too_large_total.saturating_add(1);
                Err(StoreError::Io(format!(
                    "replication_group_too_large: the commit group starting at offset {} exceeds {} records and cannot be replicated",
                    too_large.start, too_large.cap
                )))
            }
        }
    }

    pub fn replication_export(&mut self) -> Result<WalReplicationExport, StoreError> {
        self.flush_pending_sync()?;
        let (snapshot_lines, skipped_snapshot) =
            filter_replication_lines(self.replay_snapshot_lines_raw()?);
        let (wal_lines, skipped_wal) = filter_replication_lines(self.replay_wal_lines_raw()?);
        self.note_replication_skipped(skipped_snapshot + skipped_wal);
        Ok(WalReplicationExport {
            snapshot_lines,
            wal_lines,
        })
    }

    fn note_replication_skipped(&mut self, skipped: usize) {
        if skipped != self.replication_skipped {
            eprintln!(
                "warning: replication view of {} leaves out {skipped} line(s) that lenient replay quarantines",
                self.path.display()
            );
        }
        self.replication_skipped = skipped;
    }

    pub fn replace_with_replication_export(
        &mut self,
        export: &WalReplicationExport,
    ) -> Result<(), StoreError> {
        self.ensure_writable()?;
        self.flush_pending_sync()?;
        for line in export.snapshot_lines.iter().chain(&export.wal_lines) {
            check_replicated_line(line)?;
        }

        self.write_snapshot_lines_raw(&export.snapshot_lines)?;
        self.bump_generation()?;
        self.replication_index.reset();
        self.write_wal_lines_raw(&export.wal_lines)?;
        self.wal_records = export.wal_lines.len();
        self.unsynced_records = 0;
        self.last_sync_at = Instant::now();
        self.append_buffer.clear();
        Ok(())
    }

    fn append_record(&mut self, record: &PersistedRecord) -> Result<(), StoreError> {
        self.append_raw_record_line_unchecked(record_to_line(record))
    }

    fn append_raw_record_line_unchecked(&mut self, line: String) -> Result<(), StoreError> {
        self.ensure_writable()?;
        self.append_buffer.push(line);
        self.wal_records += 1;
        self.unsynced_records += 1;
        self.apply_write_policy()
    }

    /// Flushes and/or syncs the pending records as the write policy demands.
    fn apply_write_policy(&mut self) -> Result<(), StoreError> {
        if self.background_flush_only {
            return Ok(());
        }
        let interval_elapsed = self
            .sync_interval
            .is_some_and(|interval| self.last_sync_at.elapsed() >= interval);
        if self.unsynced_records >= self.sync_every_records || interval_elapsed {
            self.flush_pending_sync()?;
            return Ok(());
        }
        if self.append_buffer.len() >= self.append_buffer_max_records {
            self.flush_append_buffer()?;
        }
        Ok(())
    }

    pub fn flush_pending_sync_if_interval_elapsed(&mut self) -> Result<bool, StoreError> {
        let Some(interval) = self.sync_interval else {
            return Ok(false);
        };
        if self.unsynced_records == 0 || self.last_sync_at.elapsed() < interval {
            return Ok(false);
        }
        self.flush_pending_sync()?;
        Ok(true)
    }

    pub fn flush_pending_sync_if_unsynced(&mut self) -> Result<bool, StoreError> {
        if self.unsynced_records == 0 {
            return Ok(false);
        }
        self.flush_pending_sync()?;
        Ok(true)
    }

    fn flush_append_buffer(&mut self) -> Result<(), StoreError> {
        if self.append_buffer.is_empty() {
            return Ok(());
        }
        self.ensure_writable()?;
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        self.write_append_buffer(&mut file)
    }

    /// Writes the whole append buffer with a single `write_all` and empties
    /// it (also on error, as before).
    fn write_append_buffer(&mut self, file: &mut File) -> Result<(), StoreError> {
        let bytes: usize = self.append_buffer.iter().map(|line| line.len() + 1).sum();
        let mut buf = String::with_capacity(bytes);
        for line in self.append_buffer.drain(..) {
            buf.push_str(&line);
            buf.push('\n');
        }
        file.write_all(buf.as_bytes())?;
        Ok(())
    }

    pub fn flush_pending_sync(&mut self) -> Result<(), StoreError> {
        if self.unsynced_records == 0 && self.append_buffer.is_empty() {
            return Ok(());
        }
        self.ensure_writable()?;
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        self.write_append_buffer(&mut file)?;
        if self.unsynced_records > 0 {
            if let Err(err) = sync_wal_data(&file) {
                return Err(self.poison(&err));
            }
            self.unsynced_records = 0;
            self.last_sync_at = Instant::now();
        }
        Ok(())
    }

    /// Path of the quarantine file: `<wal>.quarantine`.
    pub fn quarantine_path(&self) -> PathBuf {
        quarantine_path_for(&self.path)
    }

    /// Parses the snapshot and WAL (plus the unflushed append buffer)
    /// into replayable items, applying `policy` to lines that cannot be
    /// parsed. See [`ReplayPolicy`] and `docs/operations/wal-recovery.md`.
    ///
    /// With `collect_vectors_from = Some(n)` the replay also reports the
    /// claim ids of the vector records after the first `n` WAL lines (the
    /// catch-up set of a persisted vector index saved at line `n`).
    pub(crate) fn replay_with_policy(
        &self,
        policy: ReplayPolicy,
        collect_vectors_from: Option<usize>,
    ) -> Result<WalReplay, StoreError> {
        let mut sink = QuarantineSink::load(self.quarantine_path())?;
        let mut parser = ReplayParser::new(policy);
        let mut items = Vec::new();

        let snapshot_lines = self.replay_snapshot_lines_raw()?;
        let snapshot_count = {
            let mut n = 0usize;
            for (idx, line) in snapshot_lines.into_iter().enumerate() {
                let origin = format!("snapshot record {}", idx + 1);
                if let Some(item) = parser.parse(line, origin, &mut sink)? {
                    items.push(item);
                    n += 1;
                }
            }
            n
        };
        let scan = scan_wal(&self.path)?;
        // Vector-index changes after line `from` (see
        // `WalReplay::vector_catch_up`); impossible when the WAL is shorter
        // than `from`.
        let mut catch_up = collect_vectors_from
            .filter(|from| *from <= scan.lines.len())
            .map(|_| VectorCatchUp::default());
        let collect_from = collect_vectors_from.unwrap_or(usize::MAX);
        let mut wal_items = Vec::new();
        for (index, (line_no, line)) in scan.lines.into_iter().enumerate() {
            let origin = format!("wal line {line_no}");
            if let Some(item) = parser.parse(line, origin, &mut sink)? {
                if index >= collect_from
                    && let Some(catch_up) = catch_up.as_mut()
                {
                    catch_up.observe(&item.record);
                }
                wal_items.push(item);
            }
        }
        for line in &self.append_buffer {
            let record = line_to_record(line)?;
            if let Some(catch_up) = catch_up.as_mut() {
                catch_up.observe(&record);
            }
            wal_items.push(ReplayItem {
                record,
                legacy: false,
                raw: None,
                origin: "unflushed wal buffer".to_string(),
            });
        }
        // Commit groups that were never closed are torn writes: drop them
        // whole (quarantined lines were already removed, so a group with a
        // quarantined member still closes normally).
        let (wal_items, discarded) = resolve_commit_groups(wal_items, |item| &item.record);
        if discarded > 0 {
            eprintln!("warning: discarded {discarded} records of an unterminated WAL commit group");
        }
        let wal_count = wal_items.len();
        items.extend(wal_items);
        let stats = WalReplayStats {
            snapshot_records: snapshot_count,
            wal_records: wal_count,
            torn_tail_dropped: self.torn_tail_dropped,
            quarantined_records: parser.quarantined,
            dependent_skipped: 0,
        };
        Ok(WalReplay {
            items,
            stats,
            sink,
            quarantined_claim_ids: parser.quarantined_claim_ids,
            vector_catch_up: catch_up,
        })
    }

    fn replay_snapshot_lines_raw(&self) -> Result<Vec<String>, StoreError> {
        let snapshot_path = self.snapshot_path();
        if !snapshot_path.exists() {
            return Ok(Vec::new());
        }
        let file = OpenOptions::new().read(true).open(snapshot_path)?;
        let reader = BufReader::new(file);
        let mut lines = reader.lines();
        let header = loop {
            match lines.next() {
                Some(line) => {
                    let line = line?;
                    if line.trim().is_empty() {
                        continue;
                    }
                    break line;
                }
                None => {
                    return Err(StoreError::Parse("snapshot file is empty".to_string()));
                }
            }
        };
        if header != SNAPSHOT_HEADER {
            return Err(StoreError::Parse(
                "snapshot file has invalid header".to_string(),
            ));
        }

        let mut out = Vec::new();
        for line in lines {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            out.push(line);
        }
        Ok(out)
    }

    fn replay_wal_lines_raw(&self) -> Result<Vec<String>, StoreError> {
        Ok(scan_wal(&self.path)?
            .lines
            .into_iter()
            .map(|(_, line)| line)
            .collect())
    }

    fn write_snapshot_records(&self, records: &[PersistedRecord]) -> Result<(), StoreError> {
        self.write_snapshot_lines_raw(&records.iter().map(record_to_line).collect::<Vec<String>>())
    }

    fn write_snapshot_lines_raw(&self, lines: &[String]) -> Result<(), StoreError> {
        let snapshot_path = self.snapshot_path();
        if let Some(parent) = snapshot_path.parent()
            && !parent.as_os_str().is_empty()
        {
            create_dir_all(parent)?;
        }

        let mut tmp_path = snapshot_path.clone().into_os_string();
        tmp_path.push(".tmp");
        let tmp_path = PathBuf::from(tmp_path);

        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)?;
        write_line(&mut file, SNAPSHOT_HEADER)?;
        for line in lines {
            write_line(&mut file, line)?;
        }
        failpoint!("snapshot.tmp_written");
        sync_file(&file)?;
        failpoint!("snapshot.fsynced");
        drop(file);
        rename_file(&tmp_path, &snapshot_path)?;
        failpoint!("snapshot.renamed");
        sync_parent_dir(&snapshot_path)?;
        failpoint!("snapshot.dir_synced");
        Ok(())
    }

    fn write_wal_lines_raw(&self, lines: &[String]) -> Result<(), StoreError> {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.path)?;
        for line in lines {
            write_line(&mut file, line)?;
        }
        file.sync_all()?;
        sync_parent_dir(&self.path)?;
        Ok(())
    }

    fn truncate_wal(&mut self) -> Result<(), StoreError> {
        self.append_buffer.clear();
        // New lineage first: a crash between the bump and the truncation
        // only causes a spurious resync, never a silent skip.
        self.bump_generation()?;
        failpoint!("wal.generation_bumped");
        self.replication_index.reset();
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.path)?;
        sync_file(&file)?;
        drop(file);
        sync_parent_dir(&self.path)?;
        failpoint!("wal.truncated");
        self.wal_records = 0;
        self.unsynced_records = 0;
        self.last_sync_at = Instant::now();
        Ok(())
    }

    pub(crate) fn compact_with_snapshot(
        &mut self,
        snapshot_records: &[PersistedRecord],
    ) -> Result<WalCheckpointStats, StoreError> {
        self.ensure_writable()?;
        let truncated_wal_records = self.wal_records;
        self.flush_pending_sync()?;
        self.write_snapshot_records(snapshot_records)?;
        self.truncate_wal()?;
        Ok(WalCheckpointStats {
            snapshot_records: snapshot_records.len(),
            truncated_wal_records,
        })
    }
}

impl Drop for FileWal {
    fn drop(&mut self) {
        let _ = self.flush_pending_sync();
    }
}

fn count_non_empty_lines(path: &Path) -> Result<usize, StoreError> {
    let file = OpenOptions::new().read(true).open(path)?;
    let reader = BufReader::new(file);
    let mut count = 0usize;
    for line in reader.lines() {
        let line = line?;
        if !line.trim().is_empty() {
            count += 1;
        }
    }
    Ok(count)
}

fn with_context(err: StoreError, context: &str) -> StoreError {
    match err {
        StoreError::Parse(msg) => StoreError::Parse(format!("{context}: {msg}")),
        other => other,
    }
}

fn write_line(file: &mut File, line: &str) -> Result<(), StoreError> {
    let mut buf = String::with_capacity(line.len() + 1);
    buf.push_str(line);
    buf.push('\n');
    file.write_all(buf.as_bytes())?;
    Ok(())
}

/// fsync the directory containing `path` so that creations, renames and
/// truncations of directory entries are durable. No-op on platforms where
/// directories cannot be opened for syncing.
pub(crate) fn sync_parent_dir(path: &Path) -> Result<(), StoreError> {
    let dir = match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.to_path_buf(),
        _ => PathBuf::from("."),
    };
    trace_op!("fsync_dir");
    #[cfg(unix)]
    {
        File::open(dir)?.sync_all()?;
    }
    #[cfg(not(unix))]
    {
        let _ = dir;
    }
    Ok(())
}

/// `File::sync_data` for WAL appends, with a test-only failpoint (`wal.sync`)
/// so tests can inject an fsync failure.
fn sync_wal_data(file: &File) -> std::io::Result<()> {
    failpoint!("wal.sync");
    file.sync_data()
}

/// `File::sync_all` with a test-only trace of the operation order.
pub(crate) fn sync_file(file: &File) -> std::io::Result<()> {
    trace_op!("fsync_file");
    file.sync_all()
}

/// `fs::rename` with a test-only trace of the operation order.
pub(crate) fn rename_file(from: &Path, to: &Path) -> std::io::Result<()> {
    trace_op!("rename");
    rename(from, to)
}

fn generation_path_for(wal_path: &Path) -> PathBuf {
    let mut path = wal_path.to_path_buf().into_os_string();
    path.push(".gen");
    PathBuf::from(path)
}

fn new_generation() -> u64 {
    loop {
        let value: u64 = rand::random();
        if value != 0 {
            return value;
        }
    }
}

fn write_generation(path: &Path, generation: u64) -> Result<(), StoreError> {
    let mut tmp = path.to_path_buf().into_os_string();
    tmp.push(".tmp");
    let tmp = PathBuf::from(tmp);
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&tmp)?;
    write_line(&mut file, &format!("{generation:016x}"))?;
    sync_file(&file)?;
    drop(file);
    rename_file(&tmp, path)?;
    sync_parent_dir(path)?;
    Ok(())
}

fn load_or_create_generation(path: &Path) -> Result<u64, StoreError> {
    if let Ok(raw) = std::fs::read_to_string(path)
        && let Ok(value) = u64::from_str_radix(raw.trim(), 16)
        && value != 0
    {
        return Ok(value);
    }
    let generation = new_generation();
    write_generation(path, generation)?;
    Ok(generation)
}

struct WalScan {
    /// `(1-based physical line number, line)` for every non-blank line that
    /// is kept (a torn final line is excluded).
    lines: Vec<(usize, String)>,
    /// Byte length of the valid prefix of the file.
    valid_len: u64,
    /// Whether a torn tail was discarded.
    torn_tail: bool,
    /// Whether the valid prefix lacks a trailing newline.
    missing_newline: bool,
}

/// Reads the WAL and separates the valid prefix from a torn tail. Only the
/// final line is parsed here; interior lines are returned verbatim so
/// that a corrupt interior line is reported (with its line number) by
/// the caller rather than silently dropped.
fn scan_wal(path: &Path) -> Result<WalScan, StoreError> {
    let mut bytes = Vec::new();
    OpenOptions::new()
        .read(true)
        .open(path)?
        .read_to_end(&mut bytes)?;

    let raw = physical_lines(&bytes);

    // Decode text; invalid UTF-8 is treated as an unparseable line.
    let decode = |s: usize, e: usize| -> Option<String> {
        let mut slice = &bytes[s..e];
        if slice.last() == Some(&b'\r') {
            slice = &slice[..slice.len() - 1];
        }
        std::str::from_utf8(slice).ok().map(str::to_string)
    };

    // Index of the last non-blank physical line.
    let last_content = raw
        .iter()
        .rposition(|&(s, e, _)| decode(s, e).is_none_or(|t| !t.trim().is_empty()));

    let mut lines = Vec::new();
    let mut valid_len = bytes.len() as u64;
    let mut torn_tail = false;
    let mut missing_newline = false;
    for (idx, &(s, e, terminated)) in raw.iter().enumerate() {
        let text = decode(s, e);
        let is_last = Some(idx) == last_content;
        if is_last {
            let ok = text
                .as_deref()
                .is_some_and(|t| is_valid_tail(t, terminated));
            if !ok {
                torn_tail = true;
                valid_len = s as u64;
                // Blank lines before the torn line stay in the prefix.
                break;
            }
            if !terminated {
                missing_newline = true;
            }
        }
        match text {
            Some(t) if t.trim().is_empty() => {}
            Some(t) => lines.push((idx + 1, t)),
            None => {
                return Err(StoreError::Parse(format!(
                    "wal line {}: invalid UTF-8",
                    idx + 1
                )));
            }
        }
    }
    Ok(WalScan {
        lines,
        valid_len,
        torn_tail,
        missing_newline,
    })
}

/// Splits `bytes` into physical lines: `(start, end, terminated)` with `end`
/// exclusive of the newline.
fn physical_lines(bytes: &[u8]) -> Vec<(usize, usize, bool)> {
    let mut raw = Vec::new();
    let mut start = 0usize;
    for (i, b) in bytes.iter().enumerate() {
        if *b == b'\n' {
            raw.push((start, i, true));
            start = i + 1;
        }
    }
    if start < bytes.len() {
        raw.push((start, bytes.len(), false));
    }
    raw
}

/// Whether a final line is a complete record. An unterminated line is only
/// trusted when it carries a verified checksum.
fn is_valid_tail(text: &str, terminated: bool) -> bool {
    // A newline-terminated legacy line is a complete (if possibly unreadable)
    // record, not a torn write: keep it so replay can quarantine it instead
    // of silently truncating it away.
    if terminated && is_legacy_kind(record_kind(text)) {
        return true;
    }
    let checksummed = split_and_verify_crc(text).is_ok_and(|(_, c)| c);
    // A terminated line with a verified checksum was written whole: if this
    // reader cannot parse it (a record kind from a newer binary), replay must
    // fail on it rather than truncate it away as a torn write.
    if terminated && checksummed {
        return true;
    }
    line_to_record(text).is_ok() && (terminated || checksummed)
}

fn quarantine_path_for(wal_path: &Path) -> PathBuf {
    let mut path = wal_path.to_path_buf().into_os_string();
    path.push(".quarantine");
    PathBuf::from(path)
}

fn record_kind(line: &str) -> &str {
    line.split('\t').next().unwrap_or("")
}

/// Record kinds written before checksums and escape-safe encoding existed.
fn is_legacy_kind(kind: &str) -> bool {
    matches!(kind, "C" | "E" | "G" | "V" | "B")
}

/// Every checksummed record kind this reader understands. `T2` (tombstone)
/// is the newest; a reader that predates it fails replay on the unknown kind
/// (see `ReplayParser::parse`) rather than skipping a delete.
fn is_versioned_kind(kind: &str) -> bool {
    matches!(kind, "C2" | "E2" | "G2" | "V2" | "B2" | "T2")
}

fn is_known_kind(kind: &str) -> bool {
    is_legacy_kind(kind) || is_versioned_kind(kind)
}

/// One parsed replay record plus the metadata the replay policy needs.
pub(crate) struct ReplayItem {
    pub(crate) record: PersistedRecord,
    /// The record was written in a legacy (unchecksummed) format.
    pub(crate) legacy: bool,
    /// Raw line, kept only for legacy records (the quarantine copy).
    pub(crate) raw: Option<String>,
    /// Human-readable position, e.g. `wal line 12`.
    pub(crate) origin: String,
}

impl ReplayItem {
    /// Raw line for the quarantine file: the original text for legacy
    /// records, the canonical encoding otherwise.
    pub(crate) fn quarantine_line(&self) -> String {
        match &self.raw {
            Some(raw) => raw.clone(),
            None => record_to_line(&self.record),
        }
    }

    /// Whether this record references a claim id in `ids`.
    pub(crate) fn depends_on(&self, ids: &HashSet<String>) -> bool {
        match &self.record {
            PersistedRecord::Evidence(e) => ids.contains(&e.claim_id),
            PersistedRecord::Edge(e) => {
                ids.contains(&e.from_claim_id) || ids.contains(&e.to_claim_id)
            }
            PersistedRecord::ClaimVector(v) => ids.contains(&v.claim_id),
            PersistedRecord::Claim(_)
            | PersistedRecord::BatchCommit(_)
            | PersistedRecord::Tombstone(_) => false,
        }
    }
}

/// What changed in the vector indexes after the WAL position a persisted
/// vector index was saved at, so a restore can bring the saved index up to
/// date instead of rebuilding it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct VectorCatchUp {
    /// Claims with a vector record after the saved position.
    pub(crate) claim_ids: HashSet<String>,
    /// `(tenant, claim)` of every claim tombstone after the saved position.
    pub(crate) deleted_claims: Vec<(String, String)>,
    /// Tenants erased after the saved position.
    pub(crate) erased_tenants: HashSet<String>,
}

impl VectorCatchUp {
    fn observe(&mut self, record: &PersistedRecord) {
        match record {
            PersistedRecord::ClaimVector(v) => {
                self.claim_ids.insert(v.claim_id.clone());
            }
            PersistedRecord::Tombstone(t) => match &t.tombstone {
                Tombstone::Claim {
                    tenant_id,
                    claim_id,
                } => {
                    self.deleted_claims
                        .push((tenant_id.clone(), claim_id.clone()));
                }
                Tombstone::Tenant { tenant_id } => {
                    self.erased_tenants.insert(tenant_id.clone());
                }
                Tombstone::Evidence { .. } => {}
            },
            _ => {}
        }
    }
}

pub(crate) struct WalReplay {
    pub(crate) items: Vec<ReplayItem>,
    pub(crate) stats: WalReplayStats,
    pub(crate) sink: QuarantineSink,
    /// Claim ids of legacy claim lines quarantined at parse time.
    pub(crate) quarantined_claim_ids: HashSet<String>,
    /// Vector-index changes after the requested WAL line; `None` when none
    /// was requested or the WAL is shorter than that line.
    pub(crate) vector_catch_up: Option<VectorCatchUp>,
}

/// Collects quarantined raw lines and appends them (fsynced) to
/// `<wal>.quarantine`. Lines already present in the file are not appended
/// again, so restarting never grows the file.
pub(crate) struct QuarantineSink {
    path: PathBuf,
    seen: HashSet<String>,
    pending: Vec<String>,
}

impl QuarantineSink {
    fn load(path: PathBuf) -> Result<Self, StoreError> {
        let mut seen = HashSet::new();
        if path.exists() {
            let bytes = std::fs::read(&path)?;
            for line in String::from_utf8_lossy(&bytes).lines() {
                seen.insert(line.to_string());
            }
        }
        Ok(Self {
            path,
            seen,
            pending: Vec::new(),
        })
    }

    /// A sink that is never flushed to disk.
    fn detached() -> Self {
        Self {
            path: PathBuf::new(),
            seen: HashSet::new(),
            pending: Vec::new(),
        }
    }

    pub(crate) fn push(&mut self, raw: &str) {
        if self.seen.insert(raw.to_string()) {
            self.pending.push(raw.to_string());
        }
    }

    pub(crate) fn flush(&mut self) -> Result<(), StoreError> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let existed = self.path.exists();
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        for line in &self.pending {
            write_line(&mut file, line)?;
        }
        file.sync_all()?;
        drop(file);
        if !existed {
            sync_parent_dir(&self.path)?;
        }
        self.pending.clear();
        Ok(())
    }
}

struct ReplayParser {
    policy: ReplayPolicy,
    quarantined: usize,
    quarantined_claim_ids: HashSet<String>,
    /// The previous line was quarantined as an unparseable legacy record, so
    /// an unrecognisable line directly after it is the remainder of the same
    /// record (a legacy field containing a raw newline).
    prev_failed: bool,
    /// Suppress the per-line warning (replication views re-run the parser on
    /// every poll).
    quiet: bool,
}

impl ReplayParser {
    fn new(policy: ReplayPolicy) -> Self {
        Self {
            policy,
            quarantined: 0,
            quarantined_claim_ids: HashSet::new(),
            prev_failed: false,
            quiet: false,
        }
    }

    fn parse(
        &mut self,
        line: String,
        origin: String,
        sink: &mut QuarantineSink,
    ) -> Result<Option<ReplayItem>, StoreError> {
        match line_to_record(&line) {
            Ok(record) => {
                self.prev_failed = false;
                let legacy = is_legacy_kind(record_kind(&line));
                Ok(Some(ReplayItem {
                    record,
                    legacy,
                    raw: legacy.then_some(line),
                    origin,
                }))
            }
            Err(err) => {
                let kind = record_kind(&line);
                let legacy = is_legacy_kind(kind);
                // A line carrying a verified checksum is a whole record of a
                // kind this reader does not know (written by a newer binary,
                // e.g. a tombstone), never a fragment of a broken legacy line:
                // it must fail the replay rather than be quarantined.
                let checksummed = matches!(split_and_verify_crc(&line), Ok((_, true)));
                let continuation = self.prev_failed && !is_known_kind(kind) && !checksummed;
                if self.policy == ReplayPolicy::Strict || !(legacy || continuation) {
                    return Err(with_context(err, &origin));
                }
                if !self.quiet {
                    eprintln!(
                        "warning: quarantining unreadable legacy record at {origin}: {err:?}"
                    );
                }
                if kind == "C"
                    && let Some(id) = line.split('\t').nth(1).and_then(|f| unescape_field(f).ok())
                {
                    self.quarantined_claim_ids.insert(id);
                }
                sink.push(&line);
                self.quarantined += 1;
                self.prev_failed = true;
                Ok(None)
            }
        }
    }
}

/// A replicated line must parse, except legacy-format lines, which a
/// follower mirrors verbatim (its own lenient replay quarantines them just
/// like the leader's).
fn check_replicated_line(line: &str) -> Result<(), StoreError> {
    match line_to_record(line) {
        Ok(_) => Ok(()),
        Err(_) if is_legacy_kind(record_kind(line)) => Ok(()),
        Err(err) => Err(err),
    }
}

/// Drops the lines lenient replay would quarantine (unparseable legacy
/// lines, continuation fragments and records depending on a quarantined
/// legacy claim) from a replication view, using the replay parser itself so
/// the decision cannot drift. Lines that only fail validation against the
/// store state (control characters in ids, poisoned vectors) are still
/// served; followers skip those (see
/// `InMemoryStore::apply_persisted_record_line_lenient`). Returns the kept
/// lines and the number dropped.
fn filter_replication_lines(lines: Vec<String>) -> (Vec<String>, usize) {
    let mut filter = ReplicationFilter::new();
    let mut kept = Vec::with_capacity(lines.len());
    let mut skipped = 0usize;
    for line in lines {
        if filter.keep(&line) {
            kept.push(line);
        } else {
            skipped += 1;
        }
    }
    (kept, skipped)
}

/// Truncates a torn final WAL line (and terminates an otherwise valid
/// unterminated one). Returns the number of dropped lines (0 or 1).
fn repair_torn_tail(path: &Path) -> Result<usize, StoreError> {
    let scan = scan_wal(path)?;
    if !scan.torn_tail && !scan.missing_newline {
        return Ok(0);
    }
    let file = OpenOptions::new().write(true).open(path)?;
    if scan.torn_tail {
        let saved = save_truncated_tail(path, scan.valid_len)?;
        eprintln!(
            "warning: discarding torn tail of write-ahead log {} (truncating to {} bytes; removed bytes saved to {})",
            path.display(),
            scan.valid_len,
            saved.display()
        );
        file.set_len(scan.valid_len)?;
        file.sync_all()?;
        return Ok(1);
    }
    drop(file);
    let mut file = OpenOptions::new().append(true).open(path)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    Ok(0)
}

/// Copies the bytes of `path` from `from` to EOF into a fresh, fsynced
/// `<path>.truncated-<unix-ms>` sidecar so a truncation never destroys data
/// irrecoverably. Returns the sidecar path.
fn save_truncated_tail(path: &Path, from: u64) -> Result<PathBuf, StoreError> {
    use std::io::{Seek, SeekFrom};
    let mut src = OpenOptions::new().read(true).open(path)?;
    src.seek(SeekFrom::Start(from))?;
    let mut tail = Vec::new();
    src.read_to_end(&mut tail)?;
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    let mut attempt = 0u32;
    loop {
        let mut name = path.to_path_buf().into_os_string();
        if attempt == 0 {
            name.push(format!(".truncated-{ts}"));
        } else {
            name.push(format!(".truncated-{ts}-{attempt}"));
        }
        let sidecar = PathBuf::from(name);
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&sidecar)
        {
            Ok(mut out) => {
                out.write_all(&tail)?;
                out.sync_all()?;
                sync_parent_dir(&sidecar)?;
                return Ok(sidecar);
            }
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => attempt += 1,
            Err(err) => return Err(err.into()),
        }
    }
}

/// Physically truncates an unterminated commit group at the end of the log
/// (see [`GROUP_BEGIN_PREFIX`]) so later appends cannot be mistaken for
/// members of the torn group. Returns the number of dropped lines.
///
/// Interior corruption is never "repaired" by truncation: a `B2` line that
/// fails to parse or verify, or any unreadable line inside the apparently
/// open group, is a hard error naming the line (an unterminated group can
/// only be a crash artifact, so every line in it must be intact). The
/// removed bytes of a genuine torn group are first saved to a
/// `<wal>.truncated-<ts>` sidecar.
fn truncate_unterminated_group(path: &Path) -> Result<usize, StoreError> {
    let scan = scan_wal(path)?;
    let mut open: Option<(usize, String)> = None;
    for (line_no, line) in &scan.lines {
        if !line.starts_with("B2\t") {
            continue;
        }
        let commit = match line_to_record(line) {
            Ok(PersistedRecord::BatchCommit(commit)) => commit,
            Ok(_) => continue,
            Err(err) => return Err(with_context(err, &format!("wal line {line_no}"))),
        };
        if let Some(id) = commit.commit_id.strip_prefix(GROUP_BEGIN_PREFIX) {
            open = Some((*line_no, id.to_string()));
        } else if open.as_ref().is_some_and(|(_, id)| {
            commit.commit_id == *id
                || commit.commit_id.strip_prefix(SINGLE_TX_PREFIX) == Some(id.as_str())
        }) {
            open = None;
        }
    }
    let Some((begin_line, _)) = open else {
        return Ok(0);
    };
    for (line_no, line) in scan.lines.iter().filter(|(n, _)| *n > begin_line) {
        if let Err(err) = line_to_record(line)
            && !is_legacy_kind(record_kind(line))
        {
            return Err(with_context(
                err,
                &format!(
                    "wal line {line_no} (inside the open commit group starting at line {begin_line})"
                ),
            ));
        }
    }
    let mut bytes = Vec::new();
    OpenOptions::new()
        .read(true)
        .open(path)?
        .read_to_end(&mut bytes)?;
    let mut offset = 0usize;
    for (idx, chunk) in bytes.split_inclusive(|b| *b == b'\n').enumerate() {
        if idx + 1 == begin_line {
            break;
        }
        offset += chunk.len();
    }
    let dropped = scan.lines.iter().filter(|(n, _)| *n >= begin_line).count();
    let saved = save_truncated_tail(path, offset as u64)?;
    eprintln!(
        "warning: discarding unterminated commit group in write-ahead log {} ({dropped} records; removed bytes saved to {})",
        path.display(),
        saved.display()
    );
    let file = OpenOptions::new().write(true).open(path)?;
    file.set_len(offset as u64)?;
    file.sync_all()?;
    Ok(dropped)
}

pub(crate) fn record_to_line(record: &PersistedRecord) -> String {
    let body = match record {
        PersistedRecord::Claim(c) => format!(
            "C2\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            escape_field(&c.claim_id),
            escape_field(&c.tenant_id),
            escape_field(&c.canonical_text),
            c.confidence,
            opt_num(c.event_time_unix),
            escape_field(&pack_string_list(&c.entities)),
            escape_field(&pack_string_list(&c.embedding_ids)),
            c.claim_type
                .as_ref()
                .map(claim_type_to_str)
                .unwrap_or("null"),
            opt_num(c.valid_from),
            opt_num(c.valid_to),
            opt_num(c.created_at),
            opt_num(c.updated_at),
        ),
        PersistedRecord::Evidence(e) => format!(
            "E2\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            escape_field(&e.evidence_id),
            escape_field(&e.claim_id),
            escape_field(&e.source_id),
            stance_to_str(&e.stance),
            e.source_quality,
            encode_opt_string(e.chunk_id.as_deref()),
            opt_num(e.span_start),
            opt_num(e.span_end),
            encode_opt_string(e.doc_id.as_deref()),
            encode_opt_string(e.extraction_model.as_deref()),
            opt_num(e.ingested_at),
        ),
        PersistedRecord::Edge(edge) => format!(
            "G2\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            escape_field(&edge.edge_id),
            escape_field(&edge.from_claim_id),
            escape_field(&edge.to_claim_id),
            relation_to_str(&edge.relation),
            edge.strength,
            escape_field(&pack_string_list(&edge.reason_codes)),
            opt_num(edge.created_at),
        ),
        PersistedRecord::ClaimVector(record) => format!(
            "V2\t{}\t{}",
            escape_field(&record.claim_id),
            pack_f32_list(&record.values)
        ),
        PersistedRecord::BatchCommit(record) => format!(
            "B2\t{}\t{}\t{}\t{}",
            escape_field(&record.commit_id),
            record.batch_size,
            record.ts_unix_ms,
            escape_field(&pack_string_list(&record.claim_ids))
        ),
        // `T2 <scope> <tenant> <target> <ts>`; the target of a tenant
        // tombstone is empty (the tenant id is already the second field).
        PersistedRecord::Tombstone(record) => {
            let target = match &record.tombstone {
                Tombstone::Tenant { .. } => "",
                other => other.target_id(),
            };
            format!(
                "T2\t{}\t{}\t{}\t{}",
                record.tombstone.scope(),
                escape_field(record.tombstone.tenant_id()),
                escape_field(target),
                record.ts_unix_ms
            )
        }
    };
    format!("{body}\t{CRC_PREFIX}{:08x}", crc32(body.as_bytes()))
}

fn opt_num<T: ToString>(value: Option<T>) -> String {
    value
        .map(|v| v.to_string())
        .unwrap_or_else(|| "null".to_string())
}

/// Optional strings are tagged so that the literal string "null" round-trips:
/// `-` means absent, `+<escaped>` means present.
fn encode_opt_string(value: Option<&str>) -> String {
    match value {
        None => "-".to_string(),
        Some(v) => format!("+{}", escape_field(v)),
    }
}

fn decode_opt_string(raw: &str) -> Result<Option<String>, StoreError> {
    if raw == "-" {
        return Ok(None);
    }
    match raw.strip_prefix('+') {
        Some(rest) => Ok(Some(unescape_field(rest)?)),
        None => Err(StoreError::Parse(
            "invalid optional string field in wal".to_string(),
        )),
    }
}

fn decode_list(raw: &str) -> Result<Vec<String>, StoreError> {
    unpack_string_list(&unescape_field(raw)?)
}

const CRC_PREFIX: &str = "crc=";

/// CRC-32 (IEEE 802.3, reflected) used as a per-record integrity suffix.
pub(crate) fn crc32(data: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFFu32;
    for &byte in data {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
        }
    }
    !crc
}

/// Splits a trailing `\tcrc=<8 hex>` suffix off `line`. Returns the body and
/// whether a checksum was present (and verified).
fn split_and_verify_crc(line: &str) -> Result<(&str, bool), StoreError> {
    let Some(idx) = line.rfind('\t') else {
        return Ok((line, false));
    };
    let Some(hex) = line[idx + 1..].strip_prefix(CRC_PREFIX) else {
        return Ok((line, false));
    };
    let body = &line[..idx];
    let expected = (hex.len() == 8)
        .then(|| u32::from_str_radix(hex, 16).ok())
        .flatten()
        .ok_or_else(|| StoreError::Parse("wal record has malformed checksum".to_string()))?;
    if crc32(body.as_bytes()) != expected {
        return Err(StoreError::Parse(
            "wal record checksum mismatch".to_string(),
        ));
    }
    Ok((body, true))
}

pub(crate) fn line_to_record(line: &str) -> Result<PersistedRecord, StoreError> {
    let (body, has_crc) = split_and_verify_crc(line)?;
    let parts: Vec<&str> = body.split('\t').collect();
    if parts.is_empty() {
        return Err(StoreError::Parse("empty wal record".to_string()));
    }
    if is_versioned_kind(parts[0]) && !has_crc {
        return Err(StoreError::Parse(
            "wal record is missing its checksum".to_string(),
        ));
    }
    match parts[0] {
        "C2" => {
            if parts.len() != 13 {
                return Err(StoreError::Parse(
                    "claim record has invalid field count".to_string(),
                ));
            }
            Ok(PersistedRecord::Claim(Claim {
                claim_id: unescape_field(parts[1])?,
                tenant_id: unescape_field(parts[2])?,
                canonical_text: unescape_field(parts[3])?,
                confidence: parts[4].parse::<f32>().map_err(|_| {
                    StoreError::Parse("claim record has invalid confidence".to_string())
                })?,
                event_time_unix: parse_optional_i64_field(parts[5], "event_time")?,
                entities: decode_list(parts[6])?,
                embedding_ids: decode_list(parts[7])?,
                claim_type: parse_optional_claim_type_field(parts[8])?,
                valid_from: parse_optional_i64_field(parts[9], "valid_from")?,
                valid_to: parse_optional_i64_field(parts[10], "valid_to")?,
                created_at: parse_optional_i64_field(parts[11], "created_at")?,
                updated_at: parse_optional_i64_field(parts[12], "updated_at")?,
            }))
        }
        "E2" => {
            if parts.len() != 12 {
                return Err(StoreError::Parse(
                    "evidence record has invalid field count".to_string(),
                ));
            }
            Ok(PersistedRecord::Evidence(Evidence {
                evidence_id: unescape_field(parts[1])?,
                claim_id: unescape_field(parts[2])?,
                source_id: unescape_field(parts[3])?,
                stance: str_to_stance(parts[4])?,
                source_quality: parts[5].parse::<f32>().map_err(|_| {
                    StoreError::Parse("evidence record has invalid source_quality".to_string())
                })?,
                chunk_id: decode_opt_string(parts[6])?,
                span_start: parse_optional_u32_field(parts[7], "span_start")?,
                span_end: parse_optional_u32_field(parts[8], "span_end")?,
                doc_id: decode_opt_string(parts[9])?,
                extraction_model: decode_opt_string(parts[10])?,
                ingested_at: parse_optional_i64_field(parts[11], "ingested_at")?,
            }))
        }
        "G2" => {
            if parts.len() != 8 {
                return Err(StoreError::Parse(
                    "edge record has invalid field count".to_string(),
                ));
            }
            Ok(PersistedRecord::Edge(ClaimEdge {
                edge_id: unescape_field(parts[1])?,
                from_claim_id: unescape_field(parts[2])?,
                to_claim_id: unescape_field(parts[3])?,
                relation: str_to_relation(parts[4])?,
                strength: parts[5].parse::<f32>().map_err(|_| {
                    StoreError::Parse("edge record has invalid strength".to_string())
                })?,
                reason_codes: decode_list(parts[6])?,
                created_at: parse_optional_i64_field(parts[7], "created_at")?,
            }))
        }
        "B2" => {
            if parts.len() != 5 {
                return Err(StoreError::Parse(
                    "batch commit record has invalid field count".to_string(),
                ));
            }
            Ok(PersistedRecord::BatchCommit(BatchCommitRecord {
                commit_id: unescape_field(parts[1])?,
                batch_size: parts[2].parse::<usize>().map_err(|_| {
                    StoreError::Parse("batch commit record has invalid batch_size".to_string())
                })?,
                ts_unix_ms: parts[3].parse::<u64>().map_err(|_| {
                    StoreError::Parse("batch commit record has invalid ts_unix_ms".to_string())
                })?,
                claim_ids: decode_list(parts[4])?,
            }))
        }
        "V2" => {
            if parts.len() != 3 {
                return Err(StoreError::Parse(
                    "vector record has invalid field count".to_string(),
                ));
            }
            Ok(PersistedRecord::ClaimVector(ClaimVectorRecord {
                claim_id: unescape_field(parts[1])?,
                values: unpack_f32_list(parts[2])?,
            }))
        }
        "T2" => {
            if parts.len() != 5 {
                return Err(StoreError::Parse(
                    "tombstone record has invalid field count".to_string(),
                ));
            }
            let tenant_id = unescape_field(parts[2])?;
            let target = unescape_field(parts[3])?;
            let tombstone = match parts[1] {
                "claim" => Tombstone::Claim {
                    tenant_id,
                    claim_id: target,
                },
                "evidence" => Tombstone::Evidence {
                    tenant_id,
                    evidence_id: target,
                },
                "tenant" if target.is_empty() => Tombstone::Tenant { tenant_id },
                "tenant" => {
                    return Err(StoreError::Parse(
                        "tenant tombstone must not name a target".to_string(),
                    ));
                }
                _ => {
                    return Err(StoreError::Parse(
                        "tombstone record has unknown scope".to_string(),
                    ));
                }
            };
            tombstone.validate()?;
            let ts_unix_ms = parts[4].parse::<u64>().map_err(|_| {
                StoreError::Parse("tombstone record has invalid ts_unix_ms".to_string())
            })?;
            Ok(PersistedRecord::Tombstone(TombstoneRecord {
                tombstone,
                ts_unix_ms,
            }))
        }
        "C" => {
            if !(parts.len() == 6 || parts.len() == 8 || parts.len() == 13) {
                return Err(StoreError::Parse(
                    "claim record has invalid field count".to_string(),
                ));
            }
            let event_time_unix = if parts[5] == "null" {
                None
            } else {
                Some(parts[5].parse::<i64>().map_err(|_| {
                    StoreError::Parse("claim record has invalid event_time".to_string())
                })?)
            };
            let entities = if parts.len() >= 8 {
                unpack_string_list(parts[6])?
            } else {
                Vec::new()
            };
            let embedding_ids = if parts.len() >= 8 {
                unpack_string_list(parts[7])?
            } else {
                Vec::new()
            };
            let claim_type = if parts.len() >= 13 {
                parse_optional_claim_type_field(parts[8])?
            } else {
                None
            };
            let valid_from = if parts.len() >= 13 {
                parse_optional_i64_field(parts[9], "valid_from")?
            } else {
                None
            };
            let valid_to = if parts.len() >= 13 {
                parse_optional_i64_field(parts[10], "valid_to")?
            } else {
                None
            };
            let created_at = if parts.len() >= 13 {
                parse_optional_i64_field(parts[11], "created_at")?
            } else {
                None
            };
            let updated_at = if parts.len() >= 13 {
                parse_optional_i64_field(parts[12], "updated_at")?
            } else {
                None
            };
            Ok(PersistedRecord::Claim(Claim {
                claim_id: unescape_field(parts[1])?,
                tenant_id: unescape_field(parts[2])?,
                canonical_text: unescape_field(parts[3])?,
                confidence: parts[4].parse::<f32>().map_err(|_| {
                    StoreError::Parse("claim record has invalid confidence".to_string())
                })?,
                event_time_unix,
                entities,
                embedding_ids,
                claim_type,
                valid_from,
                valid_to,
                created_at,
                updated_at,
            }))
        }
        "E" => {
            if !(parts.len() == 6 || parts.len() == 9 || parts.len() == 12) {
                return Err(StoreError::Parse(
                    "evidence record has invalid field count".to_string(),
                ));
            }
            let chunk_id = if parts.len() >= 9 {
                parse_optional_escaped_field(parts[6])?
            } else {
                None
            };
            let span_start = if parts.len() >= 9 {
                parse_optional_u32_field(parts[7], "span_start")?
            } else {
                None
            };
            let span_end = if parts.len() >= 9 {
                parse_optional_u32_field(parts[8], "span_end")?
            } else {
                None
            };
            let doc_id = if parts.len() >= 12 {
                parse_optional_escaped_field(parts[9])?
            } else {
                None
            };
            let extraction_model = if parts.len() >= 12 {
                parse_optional_escaped_field(parts[10])?
            } else {
                None
            };
            let ingested_at = if parts.len() >= 12 {
                parse_optional_i64_field(parts[11], "ingested_at")?
            } else {
                None
            };
            Ok(PersistedRecord::Evidence(Evidence {
                evidence_id: unescape_field(parts[1])?,
                claim_id: unescape_field(parts[2])?,
                source_id: unescape_field(parts[3])?,
                stance: str_to_stance(parts[4])?,
                source_quality: parts[5].parse::<f32>().map_err(|_| {
                    StoreError::Parse("evidence record has invalid source_quality".to_string())
                })?,
                chunk_id,
                span_start,
                span_end,
                doc_id,
                extraction_model,
                ingested_at,
            }))
        }
        "G" => {
            if parts.len() != 6 {
                return Err(StoreError::Parse(
                    "edge record has invalid field count".to_string(),
                ));
            }
            Ok(PersistedRecord::Edge(ClaimEdge {
                edge_id: unescape_field(parts[1])?,
                from_claim_id: unescape_field(parts[2])?,
                to_claim_id: unescape_field(parts[3])?,
                relation: str_to_relation(parts[4])?,
                strength: parts[5].parse::<f32>().map_err(|_| {
                    StoreError::Parse("edge record has invalid strength".to_string())
                })?,
                reason_codes: vec![],
                created_at: None,
            }))
        }
        "V" => {
            if parts.len() != 3 {
                return Err(StoreError::Parse(
                    "vector record has invalid field count".to_string(),
                ));
            }
            Ok(PersistedRecord::ClaimVector(ClaimVectorRecord {
                claim_id: unescape_field(parts[1])?,
                values: unpack_f32_list(parts[2])?,
            }))
        }
        "B" => {
            if parts.len() != 5 {
                return Err(StoreError::Parse(
                    "batch commit record has invalid field count".to_string(),
                ));
            }
            let batch_size = parts[2].parse::<usize>().map_err(|_| {
                StoreError::Parse("batch commit record has invalid batch_size".to_string())
            })?;
            let ts_unix_ms = parts[3].parse::<u64>().map_err(|_| {
                StoreError::Parse("batch commit record has invalid ts_unix_ms".to_string())
            })?;
            Ok(PersistedRecord::BatchCommit(BatchCommitRecord {
                commit_id: unescape_field(parts[1])?,
                batch_size,
                ts_unix_ms,
                claim_ids: unpack_string_list(parts[4])?,
            }))
        }
        _ => Err(StoreError::Parse("unknown wal record kind".to_string())),
    }
}

fn escape_field(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\t', "\\t")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
}

fn pack_string_list(values: &[String]) -> String {
    let mut out = String::new();
    for value in values {
        out.push_str(&format!("{}:", value.len()));
        out.push_str(value);
    }
    out
}

fn unpack_string_list(raw: &str) -> Result<Vec<String>, StoreError> {
    if raw.is_empty() {
        return Ok(Vec::new());
    }
    let bytes = raw.as_bytes();
    let mut offset = 0usize;
    let mut out = Vec::new();
    while offset < bytes.len() {
        let len_start = offset;
        while offset < bytes.len() && bytes[offset].is_ascii_digit() {
            offset += 1;
        }
        if offset == len_start || offset >= bytes.len() || bytes[offset] != b':' {
            return Err(StoreError::Parse(
                "invalid packed list field in wal".to_string(),
            ));
        }
        let len = raw[len_start..offset]
            .parse::<usize>()
            .map_err(|_| StoreError::Parse("invalid packed list length in wal".to_string()))?;
        offset += 1;
        let end = offset
            .checked_add(len)
            .filter(|end| *end <= bytes.len())
            .ok_or_else(|| {
                StoreError::Parse("packed list length exceeds wal field size".to_string())
            })?;
        let value = std::str::from_utf8(&bytes[offset..end])
            .map_err(|_| StoreError::Parse("invalid UTF-8 in packed list field".to_string()))?;
        out.push(value.to_string());
        offset = end;
    }
    Ok(out)
}

fn pack_f32_list(values: &[f32]) -> String {
    values
        .iter()
        .map(|value| value.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

fn unpack_f32_list(raw: &str) -> Result<Vec<f32>, StoreError> {
    if raw.trim().is_empty() {
        return Err(StoreError::Parse("vector list cannot be empty".to_string()));
    }
    let mut values = Vec::new();
    for part in raw.split(',') {
        let parsed = part
            .parse::<f32>()
            .map_err(|_| StoreError::Parse("invalid vector value in wal".to_string()))?;
        if !parsed.is_finite() {
            return Err(StoreError::Parse(
                "non-finite vector value in wal".to_string(),
            ));
        }
        values.push(parsed);
    }
    if values.is_empty() {
        return Err(StoreError::Parse("vector list cannot be empty".to_string()));
    }
    Ok(values)
}

fn parse_optional_escaped_field(raw: &str) -> Result<Option<String>, StoreError> {
    if raw == "null" {
        return Ok(None);
    }
    Ok(Some(unescape_field(raw)?))
}

fn parse_optional_u32_field(raw: &str, field: &str) -> Result<Option<u32>, StoreError> {
    if raw == "null" {
        return Ok(None);
    }
    raw.parse::<u32>()
        .map(Some)
        .map_err(|_| StoreError::Parse(format!("evidence record has invalid {field}")))
}

fn parse_optional_i64_field(raw: &str, field: &str) -> Result<Option<i64>, StoreError> {
    if raw == "null" {
        return Ok(None);
    }
    raw.parse::<i64>()
        .map(Some)
        .map_err(|_| StoreError::Parse(format!("claim record has invalid {field}")))
}

fn parse_optional_claim_type_field(raw: &str) -> Result<Option<ClaimType>, StoreError> {
    if raw == "null" {
        return Ok(None);
    }
    Ok(Some(str_to_claim_type(raw)?))
}

pub(crate) fn unescape_field(value: &str) -> Result<String, StoreError> {
    let mut output = String::with_capacity(value.len());
    let mut escaped = false;
    for ch in value.chars() {
        if escaped {
            match ch {
                '\\' => output.push('\\'),
                't' => output.push('\t'),
                'n' => output.push('\n'),
                'r' => output.push('\r'),
                other => {
                    return Err(StoreError::Parse(format!(
                        "invalid escape sequence: \\{other}"
                    )));
                }
            }
            escaped = false;
        } else if ch == '\\' {
            escaped = true;
        } else {
            output.push(ch);
        }
    }
    if escaped {
        return Err(StoreError::Parse(
            "unterminated escape sequence in wal field".to_string(),
        ));
    }
    Ok(output)
}

fn stance_to_str(stance: &Stance) -> &'static str {
    match stance {
        Stance::Supports => "supports",
        Stance::Contradicts => "contradicts",
        Stance::Neutral => "neutral",
    }
}

fn claim_type_to_str(value: &ClaimType) -> &'static str {
    match value {
        ClaimType::Factual => "factual",
        ClaimType::Opinion => "opinion",
        ClaimType::Prediction => "prediction",
        ClaimType::Temporal => "temporal",
        ClaimType::Causal => "causal",
    }
}

fn str_to_claim_type(value: &str) -> Result<ClaimType, StoreError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "factual" => Ok(ClaimType::Factual),
        "opinion" => Ok(ClaimType::Opinion),
        "prediction" => Ok(ClaimType::Prediction),
        "temporal" => Ok(ClaimType::Temporal),
        "causal" => Ok(ClaimType::Causal),
        _ => Err(StoreError::Parse(
            "claim record has invalid claim_type".to_string(),
        )),
    }
}

fn str_to_stance(raw: &str) -> Result<Stance, StoreError> {
    match raw {
        "supports" => Ok(Stance::Supports),
        "contradicts" => Ok(Stance::Contradicts),
        "neutral" => Ok(Stance::Neutral),
        _ => Err(StoreError::Parse("invalid stance in wal".to_string())),
    }
}

fn relation_to_str(relation: &Relation) -> &'static str {
    match relation {
        Relation::Supports => "supports",
        Relation::Contradicts => "contradicts",
        Relation::Refines => "refines",
        Relation::Duplicates => "duplicates",
        Relation::DependsOn => "depends_on",
    }
}

fn str_to_relation(raw: &str) -> Result<Relation, StoreError> {
    match raw {
        "supports" => Ok(Relation::Supports),
        "contradicts" => Ok(Relation::Contradicts),
        "refines" => Ok(Relation::Refines),
        "duplicates" => Ok(Relation::Duplicates),
        "depends_on" => Ok(Relation::DependsOn),
        _ => Err(StoreError::Parse("invalid relation in wal".to_string())),
    }
}

// ---------------------------------------------------------------------
// Offline inspection and repair (used by the `wal-inspect` tool).
// These functions never go through `FileWal::open`, so inspecting a file
// does not modify it.
// ---------------------------------------------------------------------

/// A line that failed to parse or verify.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalInvalidLine {
    /// 1-based physical line number.
    pub line_no: usize,
    pub error: String,
    /// The error is a checksum failure (mismatch, malformed or missing).
    pub checksum_failure: bool,
    pub raw: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct WalInspection {
    /// The file is a `<wal>.snapshot` (it starts with the snapshot header).
    pub is_snapshot: bool,
    /// Valid records by on-disk kind (`C`, `C2`, `V2`, ...).
    pub kind_counts: BTreeMap<String, usize>,
    pub valid_records: usize,
    /// Valid records in a legacy (unchecksummed) format.
    pub legacy_records: usize,
    /// WAL lineage id from `<wal>.gen`, if present.
    pub generation: Option<u64>,
    /// 1-based line number of a torn final line, if any.
    pub torn_tail_line: Option<usize>,
    pub torn_tail_bytes: u64,
    /// The final valid line lacks its newline terminator.
    pub missing_final_newline: bool,
    /// Invalid lines that are not the torn tail (in file order).
    pub invalid_lines: Vec<WalInvalidLine>,
}

impl WalInspection {
    pub fn first_invalid(&self) -> Option<&WalInvalidLine> {
        self.invalid_lines.first()
    }

    pub fn checksum_failures(&self) -> usize {
        self.invalid_lines
            .iter()
            .filter(|l| l.checksum_failure)
            .count()
    }
}

struct ClassifiedLine {
    line_no: usize,
    start: usize,
    end: usize,
    terminated: bool,
    /// `Ok(kind)` for a valid record.
    verdict: Result<String, WalInvalidLine>,
}

struct Classified {
    is_snapshot: bool,
    header_end: usize,
    lines: Vec<ClassifiedLine>,
    /// Index into `lines` of a torn final line.
    torn_tail: Option<usize>,
}

fn classify_file(bytes: &[u8]) -> Classified {
    let phys = physical_lines(bytes);
    let decode = |s: usize, e: usize| -> Option<&str> {
        let mut slice = &bytes[s..e];
        if slice.last() == Some(&b'\r') {
            slice = &slice[..slice.len() - 1];
        }
        std::str::from_utf8(slice).ok()
    };
    let mut lines = Vec::new();
    let mut is_snapshot = false;
    let mut header_end = 0usize;
    let mut header_seen = false;
    for (idx, &(s, e, terminated)) in phys.iter().enumerate() {
        let text = decode(s, e);
        if text.is_some_and(|t| t.trim().is_empty()) {
            continue;
        }
        if !header_seen {
            header_seen = true;
            if text == Some(SNAPSHOT_HEADER) {
                is_snapshot = true;
                header_end = if terminated { e + 1 } else { e };
                continue;
            }
        }
        let verdict = match text {
            None => Err(WalInvalidLine {
                line_no: idx + 1,
                error: "invalid UTF-8".to_string(),
                checksum_failure: false,
                raw: bytes[s..e].to_vec(),
            }),
            Some(t) => match line_to_record(t) {
                Ok(_) => Ok(record_kind(t).to_string()),
                Err(err) => {
                    let error = match err {
                        StoreError::Parse(m) => m,
                        other => format!("{other:?}"),
                    };
                    Err(WalInvalidLine {
                        line_no: idx + 1,
                        checksum_failure: error.contains("checksum"),
                        error,
                        raw: bytes[s..e].to_vec(),
                    })
                }
            },
        };
        lines.push(ClassifiedLine {
            line_no: idx + 1,
            start: s,
            end: e,
            terminated,
            verdict,
        });
    }
    // Only a WAL has a torn tail; a snapshot is replaced atomically.
    let torn_tail = if is_snapshot {
        None
    } else {
        lines.last().and_then(|last| {
            let ok =
                decode(last.start, last.end).is_some_and(|t| is_valid_tail(t, last.terminated));
            (!ok).then_some(lines.len() - 1)
        })
    };
    Classified {
        is_snapshot,
        header_end,
        lines,
        torn_tail,
    }
}

fn read_generation(path: &Path) -> Option<u64> {
    let raw = std::fs::read_to_string(generation_path_for(path)).ok()?;
    u64::from_str_radix(raw.trim(), 16).ok().filter(|v| *v != 0)
}

/// Inspects a WAL or snapshot file without modifying it.
pub fn inspect_wal_file(path: impl AsRef<Path>) -> Result<WalInspection, StoreError> {
    let path = path.as_ref();
    let bytes = std::fs::read(path)?;
    let classified = classify_file(&bytes);
    let mut out = WalInspection {
        is_snapshot: classified.is_snapshot,
        generation: if classified.is_snapshot {
            None
        } else {
            read_generation(path)
        },
        ..WalInspection::default()
    };
    for (idx, line) in classified.lines.into_iter().enumerate() {
        if classified.torn_tail == Some(idx) {
            out.torn_tail_line = Some(line.line_no);
            out.torn_tail_bytes = bytes.len() as u64 - line.start as u64;
            continue;
        }
        match line.verdict {
            Ok(kind) => {
                out.valid_records += 1;
                if is_legacy_kind(&kind) {
                    out.legacy_records += 1;
                }
                *out.kind_counts.entry(kind).or_default() += 1;
                if !line.terminated {
                    out.missing_final_newline = true;
                }
            }
            Err(invalid) => out.invalid_lines.push(invalid),
        }
    }
    Ok(out)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct WalRepairOptions {
    /// Report what would change without touching any file.
    pub dry_run: bool,
    /// Move invalid non-tail lines into `<wal>.quarantine` and drop them
    /// from the rewritten file. Without this they are left in place.
    pub quarantine_invalid: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct WalRepairReport {
    pub dry_run: bool,
    pub torn_tail_bytes: u64,
    pub torn_tail_dropped: bool,
    pub quarantined_lines: usize,
    /// Invalid lines still present in the file after the repair.
    pub invalid_remaining: usize,
    pub records_kept: usize,
    /// Whether the file was (or, for a dry run, would be) rewritten.
    pub changed: bool,
    pub backup_path: Option<PathBuf>,
    pub quarantine_path: Option<PathBuf>,
}

/// Repairs a WAL (or snapshot) file in place: drops a torn tail and,
/// optionally, quarantines invalid interior lines. Before any change a
/// `<file>.bak` copy is taken (an existing `.bak` is never overwritten) and
/// the new contents are written to a temporary file that is renamed over
/// the original.
pub fn repair_wal_file(
    path: impl AsRef<Path>,
    options: WalRepairOptions,
) -> Result<WalRepairReport, StoreError> {
    let path = path.as_ref();
    let bytes = std::fs::read(path)?;
    let classified = classify_file(&bytes);
    let mut report = WalRepairReport {
        dry_run: options.dry_run,
        ..WalRepairReport::default()
    };
    let mut out: Vec<u8> = bytes[..classified.header_end].to_vec();
    if classified.is_snapshot && !out.is_empty() && !out.ends_with(b"\n") {
        out.push(b'\n');
    }
    let mut quarantined: Vec<Vec<u8>> = Vec::new();
    let mut rewrite_needed = false;
    for (idx, line) in classified.lines.iter().enumerate() {
        if classified.torn_tail == Some(idx) {
            report.torn_tail_dropped = true;
            report.torn_tail_bytes = bytes.len() as u64 - line.start as u64;
            rewrite_needed = true;
            continue;
        }
        if !line.terminated {
            rewrite_needed = true;
        }
        match &line.verdict {
            Err(invalid) if options.quarantine_invalid => {
                quarantined.push(invalid.raw.clone());
                rewrite_needed = true;
            }
            Err(_) => {
                report.invalid_remaining += 1;
                out.extend_from_slice(&bytes[line.start..line.end]);
                out.push(b'\n');
            }
            Ok(_) => {
                report.records_kept += 1;
                out.extend_from_slice(&bytes[line.start..line.end]);
                out.push(b'\n');
            }
        }
    }
    report.quarantined_lines = quarantined.len();
    report.changed = rewrite_needed && out != bytes;
    if !report.changed {
        return Ok(report);
    }

    let wal_path = if classified.is_snapshot {
        path.to_str()
            .and_then(|p| p.strip_suffix(".snapshot"))
            .map(PathBuf::from)
            .unwrap_or_else(|| path.to_path_buf())
    } else {
        path.to_path_buf()
    };
    let mut backup = path.to_path_buf().into_os_string();
    backup.push(".bak");
    let backup = PathBuf::from(backup);
    report.backup_path = Some(backup.clone());
    if !quarantined.is_empty() {
        report.quarantine_path = Some(quarantine_path_for(&wal_path));
    }
    if options.dry_run {
        return Ok(report);
    }

    if backup.exists() {
        return Err(StoreError::Io(format!(
            "refusing to overwrite existing backup {}; move it away first",
            backup.display()
        )));
    }
    std::fs::copy(path, &backup)?;
    File::open(&backup)?.sync_all()?;
    sync_parent_dir(&backup)?;

    if let Some(qpath) = &report.quarantine_path {
        let existed = qpath.exists();
        let mut file = OpenOptions::new().create(true).append(true).open(qpath)?;
        for raw in &quarantined {
            file.write_all(raw)?;
            file.write_all(b"\n")?;
        }
        file.sync_all()?;
        drop(file);
        if !existed {
            sync_parent_dir(qpath)?;
        }
    }

    let mut tmp = path.to_path_buf().into_os_string();
    tmp.push(".repair.tmp");
    let tmp = PathBuf::from(tmp);
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&tmp)?;
    file.write_all(&out)?;
    file.sync_all()?;
    drop(file);
    rename(&tmp, path)?;
    sync_parent_dir(path)?;

    // Removing interior records shifts replication offsets; start a new
    // lineage so followers resync instead of silently skipping.
    if !classified.is_snapshot && !quarantined.is_empty() {
        write_generation(&generation_path_for(&wal_path), new_generation())?;
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::Rng;

    fn random_string(rng: &mut impl Rng) -> String {
        const POOL: &[&str] = &[
            "\t",
            "\n",
            "\r",
            "\r\n",
            "\\",
            "\\t",
            "\\n",
            "null",
            "-",
            "+",
            "crc=00000000",
            "\t crc=",
            "\0",
            "\u{1f}",
            "a",
            "Z",
            "0",
            ":",
            ",",
            " ",
            "\u{e9}",
            "\u{65e5}\u{672c}",
            "\u{1f600}",
            "\u{2028}",
        ];
        let n = rng.gen_range(0..8);
        (0..n)
            .map(|_| POOL[rng.gen_range(0..POOL.len())])
            .collect::<Vec<_>>()
            .concat()
    }

    fn random_opt(rng: &mut impl Rng) -> Option<String> {
        rng.gen_bool(0.5).then(|| random_string(rng))
    }

    fn random_list(rng: &mut impl Rng) -> Vec<String> {
        (0..rng.gen_range(0..4))
            .map(|_| random_string(rng))
            .collect()
    }

    fn roundtrip(record: &PersistedRecord) -> PersistedRecord {
        let line = record_to_line(record);
        assert!(!line.contains('\n') && !line.contains('\r'), "{line:?}");
        line_to_record(&line).unwrap_or_else(|e| panic!("{line:?}: {e:?}"))
    }

    #[test]
    fn random_strings_round_trip_through_every_record_kind() {
        let mut rng = rand::thread_rng();
        for _ in 0..2000 {
            let claim = Claim {
                claim_id: random_string(&mut rng),
                tenant_id: random_string(&mut rng),
                canonical_text: random_string(&mut rng),
                confidence: 0.25,
                event_time_unix: rng.gen_bool(0.5).then(|| rng.gen_range(-5..5)),
                entities: random_list(&mut rng),
                embedding_ids: random_list(&mut rng),
                claim_type: None,
                valid_from: None,
                valid_to: Some(9),
                created_at: Some(1),
                updated_at: None,
            };
            match roundtrip(&PersistedRecord::Claim(claim.clone())) {
                PersistedRecord::Claim(back) => assert_eq!(back, claim),
                other => panic!("{other:?}"),
            }
            let evidence = Evidence {
                evidence_id: random_string(&mut rng),
                claim_id: random_string(&mut rng),
                source_id: random_string(&mut rng),
                stance: Stance::Neutral,
                source_quality: 0.5,
                chunk_id: random_opt(&mut rng),
                span_start: Some(1),
                span_end: None,
                doc_id: random_opt(&mut rng),
                extraction_model: random_opt(&mut rng),
                ingested_at: Some(7),
            };
            match roundtrip(&PersistedRecord::Evidence(evidence.clone())) {
                PersistedRecord::Evidence(back) => assert_eq!(back, evidence),
                other => panic!("{other:?}"),
            }
            let edge = ClaimEdge {
                edge_id: random_string(&mut rng),
                from_claim_id: random_string(&mut rng),
                to_claim_id: random_string(&mut rng),
                relation: Relation::Refines,
                strength: 0.75,
                reason_codes: random_list(&mut rng),
                created_at: rng.gen_bool(0.5).then_some(1_700_000_000),
            };
            match roundtrip(&PersistedRecord::Edge(edge.clone())) {
                PersistedRecord::Edge(back) => assert_eq!(back, edge),
                other => panic!("{other:?}"),
            }
            let batch = BatchCommitRecord {
                commit_id: random_string(&mut rng),
                batch_size: 3,
                ts_unix_ms: 99,
                claim_ids: random_list(&mut rng),
            };
            match roundtrip(&PersistedRecord::BatchCommit(batch.clone())) {
                PersistedRecord::BatchCommit(back) => {
                    assert_eq!(back.commit_id, batch.commit_id);
                    assert_eq!(back.claim_ids, batch.claim_ids);
                }
                other => panic!("{other:?}"),
            }
            let vector = ClaimVectorRecord {
                claim_id: random_string(&mut rng),
                values: vec![0.5, -1.25],
            };
            match roundtrip(&PersistedRecord::ClaimVector(vector.clone())) {
                PersistedRecord::ClaimVector(back) => {
                    assert_eq!(back.claim_id, vector.claim_id);
                    assert_eq!(back.values, vector.values);
                }
                other => panic!("{other:?}"),
            }
        }
    }

    #[test]
    fn legacy_records_without_checksum_still_parse() {
        let legacy_edge = "G\te1\ta\tb\tsupports\t0.5";
        match line_to_record(legacy_edge).unwrap() {
            PersistedRecord::Edge(e) => {
                assert!(e.reason_codes.is_empty());
                assert_eq!(e.created_at, None);
            }
            other => panic!("{other:?}"),
        }
        assert!(line_to_record("C\tc1\tt\ttext\t0.9\tnull\t\t").is_ok());
        assert!(line_to_record("B\tcommit-1\t1\t1700000000000\t2:c1").is_ok());
    }

    #[test]
    fn versioned_records_require_a_valid_checksum() {
        let edge = ClaimEdge {
            edge_id: "e".into(),
            from_claim_id: "a".into(),
            to_claim_id: "b".into(),
            relation: Relation::Supports,
            strength: 0.5,
            reason_codes: vec!["r".into()],
            created_at: Some(5),
        };
        let line = record_to_line(&PersistedRecord::Edge(edge));
        assert!(line.starts_with("G2\t") && line.contains("\tcrc="));
        let (body, _) = line.rsplit_once('\t').unwrap();
        assert!(line_to_record(body).is_err(), "missing checksum must fail");
        let mut bad = line.clone();
        bad.truncate(bad.len() - 1);
        assert!(line_to_record(&bad).is_err());
        let corrupted = line.replacen("G2\te\t", "G2\tx\t", 1);
        assert!(line_to_record(&corrupted).is_err());
    }
}
