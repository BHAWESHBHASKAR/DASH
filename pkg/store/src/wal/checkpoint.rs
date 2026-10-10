//! Checkpoints whose snapshot is written without holding the WAL lock.
//!
//! A checkpoint runs in three steps:
//!
//! 1. **Rotate** ([`FileWal::begin_checkpoint`], under the caller's locks,
//!    milliseconds). The current snapshot is kept as `<wal>.snapshot.base`
//!    (a hard link), a small *pending marker* replaces `<wal>.snapshot`, the
//!    WAL is renamed to `<wal>.closed.<generation>` and a new, empty WAL
//!    starts under a new generation; the generation transition is recorded.
//!    The caller takes a copy-on-write copy of the store state at the same
//!    moment (see `InMemoryStore::begin_checkpoint`).
//! 2. **Write** (no lock). The copy is written to `<wal>.snapshot.tmp` and
//!    fsynced. Writes arriving meanwhile go to the new WAL.
//! 3. **Publish** ([`FileWal::finish_checkpoint`], under the WAL lock,
//!    milliseconds). The temporary file is renamed over the marker (the
//!    commit point) and the directory fsynced; the base snapshot and the
//!    closed WAL files that only replay needed are deleted.
//!
//! On disk, `<wal>.snapshot` is therefore always one of:
//!
//! * absent or a snapshot (`SNAP\t1`): replay it, then `<wal>`;
//! * a pending marker (`SNAP_PENDING\t1`): replay `<wal>.snapshot.base` (if
//!   the marker says there is one), then each closed WAL the marker lists, in
//!   order, then `<wal>`.
//!
//! The state the marker describes is complete: the base snapshot and the
//! closed WAL files were durable before the marker was written, and the new
//! WAL holds only records written after the rotation. A crash at any point
//! recovers either the old snapshot plus every WAL record (marker present) or
//! the new snapshot plus the new WAL (marker replaced), so no acknowledged
//! write is lost and none is applied twice. A marker that lists a closed file
//! which does not exist yet is a crash inside step 1 before the WAL was
//! renamed: [`recover_pending_checkpoint`] drops that entry (its records are
//! still in `<wal>`). A build without this code refuses a pending marker
//! ("snapshot file has invalid header") instead of misreading it.
//!
//! A checkpoint whose write or publish fails leaves the marker in place; the
//! next checkpoint extends the marker's list with its own closed generation
//! and its snapshot supersedes every listed file.

use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use super::{
    FileWal, GenerationTransition, SNAPSHOT_HEADER, closed_path_for, rename_file, sync_file,
    sync_parent_dir,
};
use crate::StoreError;
use crate::crypt::{self, CapturedKeyring, Detected, LineCodec};

/// First line of the pending marker kept at `<wal>.snapshot` while a
/// checkpoint's snapshot is being written.
pub(super) const PENDING_HEADER: &str = "SNAP_PENDING\t1";

/// The snapshot writer flushes its file to disk after every this many
/// bytes.
const SNAPSHOT_SYNC_EVERY_BYTES: usize = 64 * 1024 * 1024;

/// Error text of [`FileWal::begin_checkpoint`] while another checkpoint's
/// snapshot is still being written.
pub const CHECKPOINT_IN_PROGRESS: &str = "checkpoint_in_progress";

/// A checkpoint whose snapshot is not published yet (the marker's content).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PendingCheckpoint {
    /// `<wal>.snapshot.base` holds the snapshot replay starts from.
    pub(super) base: bool,
    /// Closed generations whose WAL files replay applies after the base,
    /// oldest first.
    pub(super) replay: Vec<u64>,
}

impl PendingCheckpoint {
    fn render(&self) -> String {
        let mut out = format!("{PENDING_HEADER}\nbase\t{}\n", u8::from(self.base));
        for generation in &self.replay {
            out.push_str(&format!("replay\t{generation:016x}\n"));
        }
        out
    }

    fn parse(text: &str) -> Result<Self, StoreError> {
        let bad = |what: &str| StoreError::Parse(format!("pending checkpoint marker: {what}"));
        let mut lines = text.lines().filter(|line| !line.trim().is_empty());
        if lines.next() != Some(PENDING_HEADER) {
            return Err(bad("invalid header"));
        }
        let base = match lines.next() {
            Some("base\t1") => true,
            Some("base\t0") => false,
            _ => return Err(bad("missing base line")),
        };
        let mut replay = Vec::new();
        for line in lines {
            let generation = line
                .strip_prefix("replay\t")
                .and_then(|hex| u64::from_str_radix(hex, 16).ok())
                .ok_or_else(|| bad("unreadable replay line"))?;
            replay.push(generation);
        }
        if replay.is_empty() {
            return Err(bad("no closed generation listed"));
        }
        Ok(Self { base, replay })
    }
}

/// `(base, replay)` of a marker's text (for `wal-inspect`).
pub(super) fn parse_pending_marker(text: &str) -> Result<(bool, Vec<u64>), StoreError> {
    PendingCheckpoint::parse(text).map(|p| (p.base, p.replay))
}

/// What `<wal>.snapshot` currently is.
pub(super) enum SnapshotKind {
    Absent,
    /// A snapshot file (or something the snapshot reader will reject with
    /// its usual error).
    Snapshot,
    Pending(PendingCheckpoint),
}

pub(super) fn read_snapshot_kind(
    path: &Path,
    keyring: Option<&std::sync::Arc<encryption::Keyring>>,
) -> Result<SnapshotKind, StoreError> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(SnapshotKind::Absent),
        Err(err) => return Err(err.into()),
    };
    // An encrypted file (marker or snapshot) starts with the encryption
    // header; its lines are read through the file's codec.
    let codec = match crypt::detect_line_file(path, keyring)? {
        Detected::Plain => LineCodec::Plain,
        Detected::Encrypted(codec) => codec,
        // Let the snapshot reader report it.
        Detected::TornHeader => return Ok(SnapshotKind::Snapshot),
    };
    let mut lines = BufReader::new(file).split(b'\n');
    let first = loop {
        match lines.next() {
            None => return Ok(SnapshotKind::Snapshot),
            Some(raw) => {
                let Ok(text) = codec.decode(&raw?) else {
                    return Ok(SnapshotKind::Snapshot);
                };
                if !text.trim().is_empty() {
                    break text;
                }
            }
        }
    };
    if first != PENDING_HEADER {
        return Ok(SnapshotKind::Snapshot);
    }
    let mut text = format!("{PENDING_HEADER}\n");
    for raw in lines {
        let line = codec
            .decode(&raw?)
            .map_err(|reason| StoreError::Parse(format!("pending checkpoint marker: {reason}")))?;
        text.push_str(&line);
        text.push('\n');
    }
    Ok(SnapshotKind::Pending(PendingCheckpoint::parse(&text)?))
}

pub(super) fn snapshot_path_for(wal_path: &Path) -> PathBuf {
    suffixed(wal_path, ".snapshot")
}

pub(super) fn base_snapshot_path_for(wal_path: &Path) -> PathBuf {
    suffixed(wal_path, ".snapshot.base")
}

fn pending_tmp_path_for(wal_path: &Path) -> PathBuf {
    suffixed(wal_path, ".snapshot.pending.tmp")
}

pub(super) fn snapshot_tmp_path_for(wal_path: &Path) -> PathBuf {
    suffixed(wal_path, ".snapshot.tmp")
}

fn suffixed(path: &Path, suffix: &str) -> PathBuf {
    let mut out = path.to_path_buf().into_os_string();
    out.push(suffix);
    PathBuf::from(out)
}

fn remove_if_exists(path: &Path) -> Result<bool, StoreError> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(true),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(err.into()),
    }
}

/// Writes `pending` to `<wal>.snapshot` atomically (temp file, fsync,
/// rename, directory fsync).
fn write_marker(
    wal_path: &Path,
    pending: &PendingCheckpoint,
    keyring: Option<&std::sync::Arc<encryption::Keyring>>,
) -> Result<(), StoreError> {
    let tmp = pending_tmp_path_for(wal_path);
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&tmp)?;
    // Encrypted like a snapshot when encryption is on (it names no data,
    // but every file next to the WAL is then encrypted).
    let codec = LineCodec::create(keyring)?;
    let rendered = pending.render();
    crypt::write_line_file(&mut file, &codec, rendered.lines())?;
    sync_file(&file)?;
    drop(file);
    let snapshot = snapshot_path_for(wal_path);
    rename_file(&tmp, &snapshot)?;
    failpoint!("checkpoint.marker_renamed");
    sync_parent_dir(&snapshot)?;
    failpoint!("checkpoint.marker_written");
    Ok(())
}

/// Brings the files of a checkpoint interrupted by a crash into a state
/// [`FileWal`] can serve, and returns the pending checkpoint, if any. See
/// the module documentation for the states.
pub(super) fn recover_pending_checkpoint(
    wal_path: &Path,
    keyring: Option<&std::sync::Arc<encryption::Keyring>>,
) -> Result<Option<PendingCheckpoint>, StoreError> {
    remove_if_exists(&pending_tmp_path_for(wal_path))?;
    remove_retired_files(wal_path);
    let snapshot = snapshot_path_for(wal_path);
    let base = base_snapshot_path_for(wal_path);
    let mut pending = match read_snapshot_kind(&snapshot, keyring)? {
        SnapshotKind::Absent | SnapshotKind::Snapshot => {
            // Left by a crash after a checkpoint published its snapshot, or
            // inside a rotation before the marker replaced the snapshot.
            remove_if_exists(&base)?;
            return Ok(None);
        }
        SnapshotKind::Pending(pending) => pending,
    };
    if pending.base && !base.exists() {
        return Err(StoreError::Parse(format!(
            "pending checkpoint: base snapshot {} is missing",
            base.display()
        )));
    }
    let last = pending.replay.len() - 1;
    for (index, generation) in pending.replay.iter().enumerate() {
        let path = closed_path_for(wal_path, *generation);
        if !path.exists() && index != last {
            return Err(StoreError::Parse(format!(
                "pending checkpoint: closed WAL {} is missing",
                path.display()
            )));
        }
    }
    let last_generation = pending.replay[last];
    if closed_path_for(wal_path, last_generation).exists() {
        return Ok(Some(pending));
    }
    // The rotation stopped before the WAL was renamed: its records are still
    // in `<wal>`.
    pending.replay.pop();
    if !pending.replay.is_empty() {
        write_marker(wal_path, &pending, keyring)?;
        return Ok(Some(pending));
    }
    if pending.base {
        rename_file(&base, &snapshot)?;
    } else {
        std::fs::remove_file(&snapshot)?;
    }
    sync_parent_dir(&snapshot)?;
    Ok(None)
}

/// A checkpoint between rotation and publication, handed out by
/// [`FileWal::begin_checkpoint`]: where to write the snapshot, and the
/// generation that identifies it to [`FileWal::finish_checkpoint`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointTicket {
    /// The keyring the snapshot is encrypted with (none: plaintext).
    pub(crate) keyring: CapturedKeyring,
    /// The generation the rotation started.
    pub(crate) generation: u64,
    /// `<wal>.snapshot.tmp`, written by the caller without the WAL lock.
    pub(crate) tmp_path: PathBuf,
    /// `<wal>.snapshot`, where [`CheckpointTicket::publish`] renames it.
    pub(crate) snapshot_path: PathBuf,
    /// Files the rotation renamed out of the way (the closed WAL of the
    /// checkpoint before); deleted by the snapshot writer, without the lock.
    pub(crate) retired_on_rotation: Vec<PathBuf>,
    /// WAL records the rotation closed.
    pub(crate) truncated_wal_records: usize,
}

impl CheckpointTicket {
    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn truncated_wal_records(&self) -> usize {
        self.truncated_wal_records
    }

    /// Writes the snapshot file: header, `lines`, fsync. Called without any
    /// lock; returns the number of records written.
    pub(crate) fn write_snapshot(
        &self,
        lines: impl Iterator<Item = String>,
    ) -> Result<usize, StoreError> {
        for path in &self.retired_on_rotation {
            let _ = std::fs::remove_file(path);
        }
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.tmp_path)?;
        let codec = LineCodec::create(self.keyring.0.as_ref())?;
        let mut count = 0usize;
        {
            let mut out = BufWriter::with_capacity(1 << 20, &mut file);
            if let Some(header) = codec.header_line() {
                out.write_all(header.as_bytes())?;
                out.write_all(b"\n")?;
            }
            out.write_all(codec.encode(SNAPSHOT_HEADER).as_bytes())?;
            out.write_all(b"\n")?;
            let mut unsynced = 0usize;
            for line in lines {
                let line = codec.encode(&line);
                out.write_all(line.as_bytes())?;
                out.write_all(b"\n")?;
                count += 1;
                unsynced += line.len() + 1;
                // Write back as we go, so the final fsync is short and WAL
                // fsyncs never queue behind one burst of the whole file.
                if unsynced >= SNAPSHOT_SYNC_EVERY_BYTES {
                    out.flush()?;
                    out.get_ref().sync_data()?;
                    unsynced = 0;
                }
            }
            out.flush()?;
        }
        failpoint!("checkpoint.snapshot_written");
        sync_file(&file)?;
        failpoint!("checkpoint.snapshot_fsynced");
        Ok(count)
    }

    /// Renames the written snapshot over the pending marker (the commit
    /// point of the checkpoint) and fsyncs the directory. Needs no lock: until
    /// [`FileWal::finish_checkpoint`] runs, the base snapshot and the closed
    /// WAL files stay where readers of the pending state expect them.
    pub(crate) fn publish(&self) -> Result<(), StoreError> {
        if let Err(err) = rename_file(&self.tmp_path, &self.snapshot_path) {
            let _ = std::fs::remove_file(&self.tmp_path);
            return Err(err.into());
        }
        failpoint!("checkpoint.snapshot_published");
        sync_parent_dir(&self.snapshot_path)?;
        failpoint!("checkpoint.published_dir_synced");
        Ok(())
    }
}

impl FileWal {
    /// `true` while a checkpoint handed out by
    /// [`FileWal::begin_checkpoint`] is neither finished nor aborted.
    pub fn checkpoint_in_flight(&self) -> bool {
        self.checkpoint_in_flight.is_some()
    }

    /// `true` while `<wal>.snapshot` is a pending marker: a checkpoint's
    /// snapshot is being written, or a failed or interrupted one has not
    /// been superseded yet. Replay then reads the base snapshot and the
    /// closed WAL files the marker lists.
    pub fn checkpoint_pending(&self) -> bool {
        self.pending.is_some()
    }

    pub fn base_snapshot_path(&self) -> PathBuf {
        base_snapshot_path_for(&self.path)
    }

    /// Closed WAL files replay applies after the base snapshot, oldest first
    /// (empty unless a checkpoint is pending).
    pub(super) fn pending_replay_paths(&self) -> Vec<PathBuf> {
        self.pending
            .iter()
            .flat_map(|p| p.replay.iter())
            .map(|g| closed_path_for(&self.path, *g))
            .collect()
    }

    /// The snapshot replay starts from: the base snapshot while a
    /// checkpoint is pending, otherwise `<wal>.snapshot` (`None` if there is
    /// none).
    pub(super) fn replay_base_path(&self) -> Option<PathBuf> {
        match &self.pending {
            Some(pending) => pending.base.then(|| self.base_snapshot_path()),
            None => Some(self.snapshot_path()),
        }
    }

    pub(super) fn is_pending_replay_generation(&self, generation: u64) -> bool {
        self.pending
            .as_ref()
            .is_some_and(|p| p.replay.contains(&generation))
    }

    /// Step 1 of a checkpoint (see the module documentation): marks the
    /// checkpoint pending, closes the current WAL generation and starts a new,
    /// empty WAL, and records the generation transition. Run it while the
    /// store state equals the WAL (the caller copies the state under the same
    /// lock). The caller then writes the snapshot with the ticket and calls
    /// [`FileWal::finish_checkpoint`] (or [`FileWal::abort_checkpoint`]).
    ///
    /// Fails with [`CHECKPOINT_IN_PROGRESS`] while another checkpoint is in
    /// flight. A failure leaves the files in a state replay reads correctly
    /// (the same states a crash leaves).
    pub fn begin_checkpoint(&mut self) -> Result<CheckpointTicket, StoreError> {
        self.ensure_writable()?;
        if self.checkpoint_in_flight.is_some() {
            return Err(StoreError::Conflict(CHECKPOINT_IN_PROGRESS.to_string()));
        }
        let truncated_wal_records = self.wal_records;
        self.flush_pending_sync()?;
        // The view length the snapshot corresponds to. If it cannot be read
        // no transition is recorded and followers resync, as before.
        let closed = match self.replication_view_len() {
            Ok(len) => Some((self.generation, len)),
            Err(err) => {
                eprintln!("warning: checkpoint could not measure the replication view: {err:?}");
                None
            }
        };
        if let Err(err) = self.rotate_for_checkpoint() {
            // Re-read what is on disk so memory matches it again.
            match recover_pending_checkpoint(&self.path, self.keyring.as_ref()) {
                Ok(pending) => self.pending = pending,
                Err(recover_err) => eprintln!(
                    "warning: could not re-read the pending checkpoint after a failed rotation: {recover_err:?}"
                ),
            }
            return Err(err);
        }
        // The new WAL is durable and holds no record of the closed
        // generation, so a follower at the closed generation's exact end may
        // switch to the new one now; it does not depend on the snapshot.
        match closed {
            Some((from_generation, from_records)) => {
                failpoint!("wal.before_transition_recorded");
                if let Some(retained) = self.closed.as_mut() {
                    retained.records = from_records;
                }
                self.record_transition(GenerationTransition {
                    from_generation,
                    from_records,
                    to_generation: self.generation,
                });
            }
            // Nothing can serve followers from the closed file; replay still
            // needs it until the snapshot is published.
            None => self.closed = None,
        }
        self.checkpoint_in_flight = Some(self.generation);
        Ok(CheckpointTicket {
            generation: self.generation,
            tmp_path: snapshot_tmp_path_for(&self.path),
            snapshot_path: self.snapshot_path(),
            keyring: CapturedKeyring(self.keyring.clone()),
            retired_on_rotation: std::mem::take(&mut self.retired_on_rotation),
            truncated_wal_records,
        })
    }

    fn rotate_for_checkpoint(&mut self) -> Result<(), StoreError> {
        let closing = self.generation;
        let next = match &self.pending {
            Some(pending) => {
                let mut next = pending.clone();
                next.replay.push(closing);
                next
            }
            None => {
                let snapshot = self.snapshot_path();
                let base = self.base_snapshot_path();
                remove_if_exists(&base)?;
                let has_base = snapshot.exists();
                if has_base {
                    std::fs::hard_link(&snapshot, &base)?;
                    failpoint!("checkpoint.base_linked");
                    sync_parent_dir(&base)?;
                    failpoint!("checkpoint.base_synced");
                }
                PendingCheckpoint {
                    base: has_base,
                    replay: vec![closing],
                }
            }
        };
        write_marker(&self.path, &next, self.keyring.as_ref())?;
        self.pending = Some(next);
        self.truncate_wal()
    }

    /// Step 3 of a checkpoint, after [`CheckpointTicket::publish`] renamed
    /// the snapshot into place: ends the pending state in memory and moves
    /// the files only the pending replay needed (the base snapshot, closed
    /// WAL files other than the one kept for followers) out of the way by
    /// renaming them. Short (renames only): run it under the WAL lock, and
    /// delete the returned files after releasing it (deleting a large file
    /// can take a while). Until this runs, readers that work from the
    /// pending state (an export frozen meanwhile) still find every file.
    pub fn finish_checkpoint(
        &mut self,
        ticket: &CheckpointTicket,
    ) -> Result<RetiredFiles, StoreError> {
        if self.checkpoint_in_flight != Some(ticket.generation) {
            return Err(StoreError::Conflict(format!(
                "checkpoint for generation {:016x} is not in flight",
                ticket.generation
            )));
        }
        self.checkpoint_in_flight = None;
        if !matches!(
            read_snapshot_kind(&self.snapshot_path(), self.keyring.as_ref())?,
            SnapshotKind::Snapshot
        ) {
            return Err(StoreError::Io(
                "checkpoint finished before its snapshot was published".to_string(),
            ));
        }
        let mut retired = RetiredFiles::default();
        if let Some(pending) = self.pending.take() {
            retired = self.retire_pending_files(&pending, ticket.generation);
        }
        failpoint!("checkpoint.retired");
        Ok(retired)
    }

    /// Ends the checkpoint of `ticket` without publishing its snapshot (the
    /// write failed). The marker stays; the next checkpoint supersedes it.
    pub fn abort_checkpoint(&mut self, ticket: &CheckpointTicket) {
        if self.checkpoint_in_flight == Some(ticket.generation) {
            self.checkpoint_in_flight = None;
            let _ = std::fs::remove_file(&ticket.tmp_path);
        }
    }

    /// Renames the base snapshot and the closed WAL files of a published
    /// checkpoint (except the closed file kept for followers) to
    /// `<wal>.retired-*` names and returns them for deletion. `tag` keeps the
    /// names unique. Leftovers (a crash before the deletion) are removed at
    /// the next open.
    pub(super) fn retire_pending_files(
        &mut self,
        pending: &PendingCheckpoint,
        tag: u64,
    ) -> RetiredFiles {
        let mut retired = RetiredFiles::default();
        let mut retire = |from: PathBuf, what: String| {
            let to = retired_path_for(&self.path, &what, tag);
            if rename_file(&from, &to).is_ok() {
                retired.paths.push(to);
            } else {
                let _ = std::fs::remove_file(&from);
            }
        };
        if pending.base {
            retire(self.base_snapshot_path(), "base".to_string());
        }
        let retained = self.closed.as_ref().map(|c| c.generation);
        for generation in &pending.replay {
            if Some(*generation) != retained {
                retire(
                    closed_path_for(&self.path, *generation),
                    format!("closed-{generation:016x}"),
                );
            }
        }
        retired
    }
}

/// `<wal>.retired-<what>-<tag>`: where a file no longer needed waits for its
/// deletion outside the WAL lock (unlinking a large file takes long enough
/// to stall writes).
pub(super) fn retired_path_for(wal_path: &Path, what: &str, tag: u64) -> PathBuf {
    suffixed(wal_path, &format!(".retired-{what}-{tag:016x}"))
}

/// Files a published checkpoint no longer needs, already renamed out of the
/// way; deleted by [`RetiredFiles::delete`] or when dropped.
#[derive(Debug, Default)]
pub struct RetiredFiles {
    paths: Vec<PathBuf>,
}

impl RetiredFiles {
    pub fn delete(mut self) {
        self.delete_all();
    }

    fn delete_all(&mut self) {
        for path in self.paths.drain(..) {
            let _ = std::fs::remove_file(path);
        }
    }
}

impl Drop for RetiredFiles {
    fn drop(&mut self) {
        self.delete_all();
    }
}

/// Deletes `<wal>.retired-*` files a crash (or an aborted checkpoint) left
/// behind.
fn remove_retired_files(wal_path: &Path) {
    let (Some(dir), Some(name)) = (wal_path.parent(), wal_path.file_name()) else {
        return;
    };
    let dir = if dir.as_os_str().is_empty() {
        Path::new(".")
    } else {
        dir
    };
    let prefix = format!("{}.retired-", name.to_string_lossy());
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            if entry.file_name().to_string_lossy().starts_with(&prefix) {
                let _ = std::fs::remove_file(entry.path());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn marker_round_trips_and_rejects_garbage() {
        let pending = PendingCheckpoint {
            base: true,
            replay: vec![0x1f, u64::MAX],
        };
        assert_eq!(
            PendingCheckpoint::parse(&pending.render()).unwrap(),
            pending
        );
        let no_base = PendingCheckpoint {
            base: false,
            replay: vec![7],
        };
        assert_eq!(
            PendingCheckpoint::parse(&no_base.render()).unwrap(),
            no_base
        );
        for bad in [
            "",
            "SNAP\t1\n",
            "SNAP_PENDING\t1\n",
            "SNAP_PENDING\t1\nbase\t1\n",
            "SNAP_PENDING\t1\nbase\t2\nreplay\t01\n",
            "SNAP_PENDING\t1\nbase\t1\nreplay\tzz\n",
        ] {
            assert!(PendingCheckpoint::parse(bad).is_err(), "{bad:?}");
        }
    }
}
