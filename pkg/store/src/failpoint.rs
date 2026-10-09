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

    /// Every failpoint in checkpoint order. Note the generation is bumped
    /// before the WAL is truncated so that a crash between the two only causes
    /// a spurious follower resync, never a silent skip.
    const POINTS: [&str; 6] = [
        "snapshot.tmp_written",
        "snapshot.fsynced",
        "snapshot.renamed",
        "snapshot.dir_synced",
        "wal.generation_bumped",
        "wal.truncated",
    ];

    fn committed_store(wal: &mut FileWal, n: usize) -> InMemoryStore {
        let mut store = InMemoryStore::new();
        for i in 0..n {
            let claim = claim_builder(
                &format!("c{i}"),
                "tenant-a",
                &format!("committed claim number {i}"),
                0.9,
            );
            store
                .ingest_bundle_persistent(wal, claim, vec![], vec![])
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

    #[test]
    fn checkpoint_orders_fsync_rename_dir_fsync_then_wal_truncate() {
        let dir = TempDir::new().unwrap();
        let mut wal = FileWal::open(dir.path().join("dash.wal")).unwrap();
        let store = committed_store(&mut wal, 3);
        take_trace();

        store.checkpoint_and_compact(&mut wal).unwrap();

        assert_eq!(
            take_trace(),
            vec![
                // snapshot: write tmp, fsync it, rename over, fsync the directory
                "snapshot.tmp_written",
                "fsync_file",
                "snapshot.fsynced",
                "rename",
                "snapshot.renamed",
                "fsync_dir",
                "snapshot.dir_synced",
                // new WAL lineage: generation file written durably first
                "fsync_file",
                "rename",
                "fsync_dir",
                "wal.generation_bumped",
                // only then is the WAL truncated and made durable
                "fsync_file",
                "fsync_dir",
                "wal.truncated",
            ]
        );
    }

    #[test]
    fn crash_at_every_checkpoint_failpoint_recovers_exactly_the_committed_data() {
        for point in POINTS {
            let dir = TempDir::new().unwrap();
            let wal_path = dir.path().join("dash.wal");
            let mut wal = FileWal::open(&wal_path).unwrap();
            let store = committed_store(&mut wal, 4);
            let expected = lines(&store);
            let generation_before = wal.generation();

            arm(point);
            let result = store.checkpoint_and_compact(&mut wal);
            disarm();
            assert!(
                result.is_err(),
                "{point}: checkpoint must fail at the failpoint"
            );

            // The crash state is whatever is on disk right now.
            let crashed = TempDir::new().unwrap();
            copy_dir(dir.path(), crashed.path());
            drop(wal);

            let mut recovered_wal = FileWal::open(crashed.path().join("dash.wal")).unwrap();
            let recovered = InMemoryStore::load_from_wal(&recovered_wal).unwrap();
            assert_eq!(
                lines(&recovered),
                expected,
                "{point}: recovery must yield exactly the committed records"
            );
            assert_eq!(recovered.claims_len(), 4, "{point}: no loss, no duplicates");

            let bumped_already = matches!(point, "wal.generation_bumped" | "wal.truncated");
            if bumped_already {
                assert_ne!(
                    recovered_wal.generation(),
                    generation_before,
                    "{point}: new lineage must be durable"
                );
            } else {
                assert_eq!(
                    recovered_wal.generation(),
                    generation_before,
                    "{point}: lineage must not change before the bump"
                );
            }

            // A follower caught up on the old lineage must never be silently
            // skipped: a changed generation forces a resync, and an unchanged
            // one still serves every record the follower has not seen.
            let frame = recovered_wal
                .replication_frame_from(Some(generation_before), 4, 100)
                .unwrap();
            if bumped_already {
                assert!(
                    frame.needs_resync,
                    "{point}: generation change must force resync"
                );
            } else {
                assert!(!frame.needs_resync, "{point}");
            }

            // The directory must be usable again: a retried checkpoint on the
            // recovered state succeeds and keeps the same data.
            let mut retry_wal = FileWal::open(crashed.path().join("dash.wal")).unwrap();
            recovered.checkpoint_and_compact(&mut retry_wal).unwrap();
            let after = InMemoryStore::load_from_wal(&retry_wal).unwrap();
            assert_eq!(lines(&after), expected, "{point}: retry checkpoint");
            assert!(
                !crashed.path().join("dash.wal.snapshot.tmp").exists(),
                "{point}: retry must not leave a stale tmp snapshot"
            );
        }
    }

    #[test]
    fn writes_after_a_crashed_checkpoint_are_not_duplicated_or_lost() {
        for point in POINTS {
            let dir = TempDir::new().unwrap();
            let mut wal = FileWal::open(dir.path().join("dash.wal")).unwrap();
            let store = committed_store(&mut wal, 2);
            arm(point);
            let _ = store.checkpoint_and_compact(&mut wal);
            disarm();

            let crashed = TempDir::new().unwrap();
            copy_dir(dir.path(), crashed.path());
            drop(wal);

            let mut wal2 = FileWal::open(crashed.path().join("dash.wal")).unwrap();
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

            let wal3 = FileWal::open(crashed.path().join("dash.wal")).unwrap();
            let final_store = InMemoryStore::load_from_wal(&wal3).unwrap();
            assert_eq!(final_store.claims_len(), 3, "{point}");
        }
    }
}
