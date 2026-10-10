//! Test-only failpoints and an operation trace for the checkpoint path.
//!
//! The whole machinery exists only under `cfg(test)`. In every other build
//! `failpoint!` and `trace_op!` expand to nothing and this module is empty, so
//! release binaries carry no registry, no thread-locals and no extra branches.
//!
//! * `failpoint!("name")` records the hit and, when the test armed that name
//!   on the current thread, returns an `io::Error` from the enclosing
//!   function. The files on disk at that moment are the crash state: tests
//!   copy the directory and reopen the copy.
//! * `trace_op!("fsync_file")` only records, so a test can assert the order of
//!   the real filesystem operations (fsync, rename, directory fsync).
//!
//! State is thread-local, so tests running in parallel do not see each
//! other's failpoints or traces.

#[cfg(test)]
macro_rules! failpoint {
    ($name:expr) => {
        crate::failpoint::hit($name)?
    };
}

#[cfg(not(test))]
macro_rules! failpoint {
    ($name:expr) => {};
}

#[cfg(test)]
macro_rules! trace_op {
    ($name:expr) => {
        crate::failpoint::record($name)
    };
}

#[cfg(not(test))]
macro_rules! trace_op {
    ($name:expr) => {};
}

#[cfg(test)]
pub(crate) use test_support::{arm, disarm, hit, record, take_trace};

#[cfg(test)]
mod test_support {
    use std::cell::RefCell;
    use std::io;

    thread_local! {
        static ARMED: RefCell<Option<&'static str>> = const { RefCell::new(None) };
        static TRACE: RefCell<Vec<&'static str>> = const { RefCell::new(Vec::new()) };
    }

    /// Make the next hit of `name` on this thread fail.
    pub(crate) fn arm(name: &'static str) {
        ARMED.with(|a| *a.borrow_mut() = Some(name));
    }

    pub(crate) fn disarm() {
        ARMED.with(|a| *a.borrow_mut() = None);
    }

    pub(crate) fn record(name: &'static str) {
        TRACE.with(|t| t.borrow_mut().push(name));
    }

    /// Returns and clears the operations recorded on this thread.
    pub(crate) fn take_trace() -> Vec<&'static str> {
        TRACE.with(|t| std::mem::take(&mut *t.borrow_mut()))
    }

    pub(crate) fn hit(name: &'static str) -> io::Result<()> {
        record(name);
        let fire = ARMED.with(|a| *a.borrow() == Some(name));
        if fire {
            ARMED.with(|a| *a.borrow_mut() = None);
            return Err(io::Error::other(format!("failpoint {name}")));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;

    use schema::claim_builder;
    use tempfile::TempDir;

    use super::*;
    use crate::{FileWal, InMemoryStore, PersistedRecord, record_to_line};

    /// Every failpoint of a checkpoint, in order. The first two only exist
    /// when a snapshot is already there (it is kept as the base).
    const POINTS: [&str; 13] = [
        "checkpoint.base_linked",
        "checkpoint.base_synced",
        "checkpoint.marker_renamed",
        "checkpoint.marker_written",
        "wal.generation_bumped",
        "wal.closed_renamed",
        "wal.truncated",
        "wal.before_transition_recorded",
        "checkpoint.snapshot_written",
        "checkpoint.snapshot_fsynced",
        "checkpoint.snapshot_published",
        "checkpoint.published_dir_synced",
        "checkpoint.retired",
    ];

    /// Where the crash happened relative to the steps of a checkpoint.
    fn position(point: &str) -> usize {
        POINTS.iter().position(|p| *p == point).unwrap()
    }

    fn claim(i: usize) -> schema::Claim {
        claim_builder(
            &format!("c{i}"),
            "tenant-a",
            &format!("committed claim number {i}"),
            0.9,
        )
    }

    fn committed_store(wal: &mut FileWal, n: usize) -> InMemoryStore {
        let mut store = InMemoryStore::new();
        for i in 0..n {
            store
                .ingest_bundle_persistent(wal, claim(i), vec![], vec![])
                .unwrap();
        }
        store
    }

    /// `n` claims, with a completed checkpoint after the first half, so the
    /// next checkpoint has a snapshot to keep as its base.
    fn committed_store_with_snapshot(wal: &mut FileWal, n: usize) -> InMemoryStore {
        let mut store = InMemoryStore::new();
        for i in 0..n {
            if i == n / 2 {
                store.checkpoint_and_compact(wal).unwrap();
            }
            store
                .ingest_bundle_persistent(wal, claim(i), vec![], vec![])
                .unwrap();
        }
        store
    }

    fn lines(store: &InMemoryStore) -> Vec<String> {
        let mut out: Vec<String> = store
            .snapshot_records()
            .iter()
            .map(|r: &PersistedRecord| record_to_line(r))
            .collect();
        out.sort();
        out
    }

    fn copy_dir(from: &Path, to: &Path) {
        for entry in fs::read_dir(from).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_file() {
                fs::copy(entry.path(), to.join(entry.file_name())).unwrap();
            }
        }
    }

    fn file_names(dir: &Path) -> Vec<String> {
        let mut names: Vec<String> = fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
            .collect();
        names.sort();
        names
    }

    #[test]
    fn checkpoint_orders_marker_rotation_snapshot_then_publication() {
        let dir = TempDir::new().unwrap();
        let mut wal = FileWal::open(dir.path().join("dash.wal")).unwrap();
        let store = committed_store(&mut wal, 3);
        take_trace();

        store.checkpoint_and_compact(&mut wal).unwrap();

        let rotation = vec![
            // the pending marker replaces <wal>.snapshot durably first
            "fsync_file",
            "rename",
            "checkpoint.marker_renamed",
            "fsync_dir",
            "checkpoint.marker_written",
            // new WAL lineage: generation file written durably
            "fsync_file",
            "rename",
            "fsync_dir",
            "wal.generation_bumped",
            // then the WAL becomes the closed file and a new, empty WAL is
            // made durable
            "rename",
            "wal.closed_renamed",
            "fsync_file",
            "fsync_dir",
            "wal.truncated",
            // the generation transition is recorded once the new WAL is
            // durable
            "wal.before_transition_recorded",
            "fsync_file",
            "rename",
            "fsync_dir",
        ];
        let publication = vec![
            // the snapshot is written without any lock and fsynced
            "checkpoint.snapshot_written",
            "fsync_file",
            "checkpoint.snapshot_fsynced",
            // then renamed over the marker (the commit point) and the
            // directory fsynced
            "rename",
            "checkpoint.snapshot_published",
            "fsync_dir",
            "checkpoint.published_dir_synced",
        ];
        // Nothing to retire the first time: the closed WAL is kept for
        // followers and there was no base snapshot.
        let first: Vec<&str> = rotation
            .iter()
            .chain(&publication)
            .copied()
            .chain(["checkpoint.retired"])
            .collect();
        assert_eq!(take_trace(), first);

        // With a snapshot in place it is first kept as the base (hard link,
        // directory fsync) before the marker replaces it.
        store.checkpoint_and_compact(&mut wal).unwrap();
        let mut second = vec![
            "checkpoint.base_linked",
            "fsync_dir",
            "checkpoint.base_synced",
        ];
        // The closed WAL kept from the first checkpoint is renamed out of the
        // way right after the new WAL is durable (deleted later, without the
        // lock).
        let truncated = rotation.iter().position(|p| *p == "wal.truncated").unwrap();
        second.extend(rotation[..=truncated].iter().copied());
        second.push("rename");
        second.extend(
            rotation[truncated + 1..]
                .iter()
                .chain(&publication)
                .copied(),
        );
        // The base snapshot and the previous closed WAL are renamed out of
        // the way under the lock; deleting them happens after it.
        second.extend(["rename", "checkpoint.retired"]);
        assert_eq!(take_trace(), second);
        assert!(!wal.checkpoint_pending());
        assert!(!wal.base_snapshot_path().exists());
    }

    /// The crash state of a checkpoint interrupted at a failpoint (a copy of
    /// the directory at that moment) and what the test expects of it.
    struct Crash {
        dir: TempDir,
        expected: Vec<String>,
        generation_before: u64,
        view_before: usize,
        with_snapshot: bool,
    }

    fn crash_at(point: &'static str, with_snapshot: bool) -> Option<Crash> {
        if !with_snapshot && position(point) < position("checkpoint.marker_renamed") {
            // No snapshot, no base: these failpoints are not reached.
            return None;
        }
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join("dash.wal");
        let mut wal = FileWal::open(&wal_path).unwrap();
        let store = if with_snapshot {
            committed_store_with_snapshot(&mut wal, 4)
        } else {
            committed_store(&mut wal, 4)
        };
        let expected = lines(&store);
        let generation_before = wal.generation();
        let (_, view_before) = wal.replication_position().unwrap();

        arm(point);
        let result = store.checkpoint_and_compact(&mut wal);
        disarm();
        assert!(
            result.is_err(),
            "{point}: checkpoint must fail at the failpoint"
        );
        let crashed = TempDir::new().unwrap();
        copy_dir(dir.path(), crashed.path());
        drop(wal);
        Some(Crash {
            dir: crashed,
            expected,
            generation_before,
            view_before,
            with_snapshot,
        })
    }

    #[test]
    fn crash_at_every_checkpoint_failpoint_recovers_exactly_the_committed_data() {
        for with_snapshot in [false, true] {
            for point in POINTS {
                let Some(crash) = crash_at(point, with_snapshot) else {
                    continue;
                };
                let ctx = format!("{point} (snapshot before: {with_snapshot})");
                let wal_path = crash.dir.path().join("dash.wal");
                let mut recovered_wal = FileWal::open(&wal_path).unwrap();
                let recovered = InMemoryStore::load_from_wal(&recovered_wal).unwrap();
                assert_eq!(
                    lines(&recovered),
                    crash.expected,
                    "{ctx}: recovery must yield exactly the committed records"
                );
                assert_eq!(recovered.claims_len(), 4, "{ctx}: no loss, no duplicates");

                let at = position(point);
                let bumped = at >= position("wal.generation_bumped");
                let rotated = at >= position("wal.closed_renamed");
                let transition = at >= position("checkpoint.snapshot_written");
                let published = at >= position("checkpoint.snapshot_published");
                assert_eq!(
                    recovered_wal.generation() != crash.generation_before,
                    bumped,
                    "{ctx}: the new lineage is durable exactly from the bump on"
                );
                // Before the WAL was renamed the rotation is rolled back; from
                // then on until publication the checkpoint is pending (base
                // snapshot plus closed WAL replayed); after it, the new
                // snapshot is in place.
                assert_eq!(
                    recovered_wal.checkpoint_pending(),
                    rotated && !published,
                    "{ctx}: pending state"
                );
                assert_eq!(
                    recovered_wal.base_snapshot_path().exists(),
                    crash.with_snapshot && rotated && !published,
                    "{ctx}: the base snapshot exists only while it is replayed"
                );

                // A follower inside the old lineage is never silently skipped:
                // a changed generation forces a resync (this request does not
                // offer generation switches).
                let frame = recovered_wal
                    .replication_frame_from(Some(crash.generation_before), 1, 100)
                    .unwrap();
                assert_eq!(frame.needs_resync, bumped, "{ctx}");
                // A follower at the exact old end switches as soon as the
                // transition is recorded (the new WAL is durable and holds no
                // old record); before that it resyncs if the lineage changed.
                let recorded = recovered_wal
                    .generation_transitions()
                    .last()
                    .is_some_and(|t| t.from_generation == crash.generation_before);
                assert_eq!(recorded, transition, "{ctx}: transition");
                let switch = recovered_wal
                    .replication_frame_with_switch(
                        Some(crash.generation_before),
                        crash.view_before,
                        100,
                    )
                    .unwrap();
                assert_eq!(switch.switched_from.is_some(), transition, "{ctx}");
                assert_eq!(switch.needs_resync, bumped && !transition, "{ctx}");

                // Reopening again changes nothing (recovery is idempotent).
                drop(recovered_wal);
                let reopened = FileWal::open(&wal_path).unwrap();
                assert_eq!(
                    lines(&InMemoryStore::load_from_wal(&reopened).unwrap()),
                    crash.expected,
                    "{ctx}: second open"
                );

                // The directory must be usable again: a retried checkpoint on
                // the recovered state succeeds, keeps the same data and
                // leaves no file of the interrupted one behind.
                let mut retry_wal = reopened;
                recovered.checkpoint_and_compact(&mut retry_wal).unwrap();
                let after = InMemoryStore::load_from_wal(&retry_wal).unwrap();
                assert_eq!(lines(&after), crash.expected, "{ctx}: retry checkpoint");
                assert!(!retry_wal.checkpoint_pending(), "{ctx}");
                drop(retry_wal);
                let names = file_names(crash.dir.path());
                for leftover in [
                    "dash.wal.snapshot.tmp",
                    "dash.wal.snapshot.base",
                    "dash.wal.snapshot.pending.tmp",
                ] {
                    assert!(!names.iter().any(|n| n == leftover), "{ctx}: {names:?}");
                }
                assert!(
                    !names.iter().any(|n| n.starts_with("dash.wal.retired-")),
                    "{ctx}: retired files are deleted: {names:?}"
                );
                let closed = names.iter().filter(|n| n.contains(".closed.")).count();
                assert!(
                    closed <= 1,
                    "{ctx}: one closed generation at most: {names:?}"
                );
                let reopened = FileWal::open(&wal_path).unwrap();
                assert_eq!(
                    lines(&InMemoryStore::load_from_wal(&reopened).unwrap()),
                    crash.expected,
                    "{ctx}: after the retry"
                );
            }
        }
    }

    #[test]
    fn writes_after_a_crashed_checkpoint_are_not_duplicated_or_lost() {
        for with_snapshot in [false, true] {
            for point in POINTS {
                let Some(crash) = crash_at(point, with_snapshot) else {
                    continue;
                };
                let wal_path = crash.dir.path().join("dash.wal");
                let mut wal2 = FileWal::open(&wal_path).unwrap();
                let mut recovered = InMemoryStore::load_from_wal(&wal2).unwrap();
                recovered
                    .ingest_bundle_persistent(
                        &mut wal2,
                        claim_builder("late", "tenant-a", "written after the crash", 0.9),
                        vec![],
                        vec![],
                    )
                    .unwrap();
                drop(wal2);

                let wal3 = FileWal::open(&wal_path).unwrap();
                let final_store = InMemoryStore::load_from_wal(&wal3).unwrap();
                assert_eq!(final_store.claims_len(), 5, "{point} ({with_snapshot})");
            }
        }
    }

    /// A checkpoint whose snapshot write fails leaves the pending state; the
    /// next one extends it with its own closed generation, and its snapshot
    /// supersedes every listed file. A crash in between recovers every write.
    #[test]
    fn failed_checkpoints_chain_until_one_publishes() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join("dash.wal");
        let mut wal = FileWal::open(&wal_path).unwrap();
        let mut store = committed_store_with_snapshot(&mut wal, 4);

        arm("checkpoint.snapshot_written");
        assert!(store.checkpoint_and_compact(&mut wal).is_err());
        disarm();
        assert!(wal.checkpoint_pending());
        assert!(!wal.checkpoint_in_flight());
        for i in 4..6 {
            store
                .ingest_bundle_persistent(&mut wal, claim(i), vec![], vec![])
                .unwrap();
        }
        arm("checkpoint.snapshot_fsynced");
        assert!(store.checkpoint_and_compact(&mut wal).is_err());
        disarm();
        store
            .ingest_bundle_persistent(&mut wal, claim(6), vec![], vec![])
            .unwrap();
        let expected = lines(&store);
        let closed_files = file_names(dir.path())
            .into_iter()
            .filter(|n| n.contains(".closed."))
            .count();
        assert_eq!(closed_files, 2, "both closed generations are replayed");

        // Crash now: base snapshot + two closed WAL files + the WAL.
        let crashed = TempDir::new().unwrap();
        copy_dir(dir.path(), crashed.path());
        let crashed_wal = FileWal::open(crashed.path().join("dash.wal")).unwrap();
        assert!(crashed_wal.checkpoint_pending());
        let recovered = InMemoryStore::load_from_wal(&crashed_wal).unwrap();
        assert_eq!(lines(&recovered), expected);
        assert_eq!(recovered.claims_len(), 7);
        let boundary = crashed_wal.replay_boundary().unwrap();
        assert_eq!(boundary.wal_delta_record_count, 1);

        // A successful checkpoint supersedes the chain.
        store.checkpoint_and_compact(&mut wal).unwrap();
        assert!(!wal.checkpoint_pending());
        let names = file_names(dir.path());
        assert!(
            !names.iter().any(|n| n.ends_with(".snapshot.base")),
            "{names:?}"
        );
        assert_eq!(
            names.iter().filter(|n| n.contains(".closed.")).count(),
            1,
            "only the newest closed generation stays (for followers): {names:?}"
        );
        drop(wal);
        let reopened = FileWal::open(&wal_path).unwrap();
        assert_eq!(
            lines(&InMemoryStore::load_from_wal(&reopened).unwrap()),
            expected
        );
    }

    /// A build without background checkpoints must refuse a pending marker
    /// rather than start without the records it points to: the marker does
    /// not carry the snapshot header older readers expect.
    #[test]
    fn pending_marker_is_not_a_snapshot_for_older_readers() {
        let crash = crash_at("checkpoint.snapshot_written", true).unwrap();
        let marker = fs::read_to_string(crash.dir.path().join("dash.wal.snapshot")).unwrap();
        assert!(marker.starts_with("SNAP_PENDING\t1\n"), "{marker}");
        assert!(!marker.starts_with("SNAP\t1\n"));
        // wal-inspect describes it and refuses to "repair" it.
        let marker_path = crash.dir.path().join("dash.wal.snapshot");
        let info = crate::inspect_wal_file(&marker_path).unwrap();
        let pending = info.pending_checkpoint.expect("recognised as a marker");
        assert!(pending.base);
        assert_eq!(pending.replay, vec![crash.generation_before]);
        assert!(info.invalid_lines.is_empty());
        assert!(crate::repair_wal_file(&marker_path, crate::WalRepairOptions::default()).is_err());
        assert_eq!(fs::read_to_string(&marker_path).unwrap(), marker);
    }

    /// A pending chain exists; the next rotation crashes after extending the
    /// marker but before renaming the WAL. Recovery drops only the entry
    /// whose file does not exist and keeps the rest of the chain.
    #[test]
    fn crash_inside_a_rotation_after_a_failed_checkpoint_keeps_the_chain() {
        let dir = TempDir::new().unwrap();
        let mut wal = FileWal::open(dir.path().join("dash.wal")).unwrap();
        let mut store = committed_store_with_snapshot(&mut wal, 4);
        arm("checkpoint.snapshot_written");
        assert!(store.checkpoint_and_compact(&mut wal).is_err());
        disarm();
        store
            .ingest_bundle_persistent(&mut wal, claim(4), vec![], vec![])
            .unwrap();
        let expected = lines(&store);
        drop(wal);
        for point in ["checkpoint.marker_written", "wal.generation_bumped"] {
            let attempt = TempDir::new().unwrap();
            copy_dir(dir.path(), attempt.path());
            let mut attempt_wal = FileWal::open(attempt.path().join("dash.wal")).unwrap();
            let attempt_store = InMemoryStore::load_from_wal(&attempt_wal).unwrap();
            arm(point);
            assert!(
                attempt_store
                    .checkpoint_and_compact(&mut attempt_wal)
                    .is_err()
            );
            disarm();
            let crashed = TempDir::new().unwrap();
            copy_dir(attempt.path(), crashed.path());
            drop(attempt_wal);
            let recovered_wal = FileWal::open(crashed.path().join("dash.wal")).unwrap();
            assert!(recovered_wal.checkpoint_pending(), "{point}");
            let recovered = InMemoryStore::load_from_wal(&recovered_wal).unwrap();
            assert_eq!(lines(&recovered), expected, "{point}");
        }
    }

    #[test]
    fn rollback_keeps_the_record_count_when_the_generation_bump_fails() {
        // A full disk can let the truncation succeed and then refuse the new
        // generation file. The in-memory record count must still match the
        // file, or replication offsets and checkpoints drift from the WAL.
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join("dash.wal");
        let mut wal = FileWal::open(&wal_path).unwrap();
        let mut store = committed_store(&mut wal, 2);
        let committed = wal.wal_record_count().unwrap();
        let size = fs::metadata(&wal_path).unwrap().len();

        let point = wal.begin_rollback_point().unwrap();
        wal.append_claim(&claim_builder("rolled", "tenant-a", "rolled back", 0.9))
            .unwrap();
        arm("wal.rollback_truncated");
        let result = wal.rollback_to(point);
        disarm();
        assert!(result.is_err(), "the failpoint must fail the rollback");
        assert_eq!(fs::metadata(&wal_path).unwrap().len(), size);
        assert_eq!(wal.wal_record_count().unwrap(), committed);

        store
            .ingest_bundle_persistent(
                &mut wal,
                claim_builder("next", "tenant-a", "written after the rollback", 0.9),
                vec![],
                vec![],
            )
            .unwrap();
        let after = wal.wal_record_count().unwrap();
        drop(wal);
        let reopened = FileWal::open(&wal_path).unwrap();
        assert_eq!(reopened.wal_record_count().unwrap(), after);
        assert_eq!(
            InMemoryStore::load_from_wal(&reopened)
                .unwrap()
                .claims_len(),
            3
        );
    }
}
