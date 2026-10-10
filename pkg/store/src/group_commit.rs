//! WAL group commit.
//!
//! Callers enqueue the encoded lines of one write and block on the returned
//! [`CommitTicket`]. A single committer thread takes everything queued (up to
//! [`GroupCommitConfig::max_batch_bytes`]), appends it to the log as one unit
//! and makes it durable with one fsync (subject to the log's own write
//! policy), then wakes every waiter of that batch with the same outcome.
//!
//! Guarantees:
//! * A ticket resolves to `Ok` only after the log reported the whole batch
//!   durable. On an append or fsync error every entry of the batch gets the
//!   error; nothing of it may be applied by the caller.
//! * Entries reach the log in enqueue order, so callers that enqueue under a
//!   lock get the WAL order of that lock.
//! * After an fsync failure the log is poisoned: the batch fails, every later
//!   enqueue fails immediately with [`GroupCommitError::Poisoned`] and the log
//!   is never written again by this committer (no fsync retry, see
//!   "fsyncgate").
//! * The queue is bounded ([`GroupCommitConfig::queue_capacity`] entries); a
//!   full queue rejects with [`GroupCommitError::Overloaded`] instead of
//!   growing.
//! * Batching never adds more than [`GroupCommitConfig::max_wait`] of latency:
//!   with `max_wait == 0` (the default) the committer starts a batch as soon
//!   as one entry is queued, and batches form naturally from the entries that
//!   arrive while the previous fsync is running.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, mpsc};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::{FileWal, StoreError};

/// Upper bound accepted for [`GroupCommitConfig::max_wait`].
pub const GROUP_COMMIT_MAX_WAIT_LIMIT: Duration = Duration::from_millis(10);

/// The log a [`GroupCommitter`] writes to. Implemented by [`FileWal`]; tests
/// inject failures through their own implementations.
pub trait GroupCommitLog: Send + 'static {
    /// Appends `lines` as one unit and makes them durable according to the
    /// log's write policy. On error none of `lines` counts as committed.
    fn commit_lines(&mut self, lines: &[String]) -> Result<(), StoreError>;

    /// `Some(reason)` once the log refuses all further writes (after a failed
    /// fsync).
    fn poisoned_reason(&self) -> Option<String>;
}

impl GroupCommitLog for FileWal {
    fn commit_lines(&mut self, lines: &[String]) -> Result<(), StoreError> {
        self.append_group_lines(lines)
    }

    fn poisoned_reason(&self) -> Option<String> {
        FileWal::poisoned_reason(self).map(str::to_string)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupCommitConfig {
    /// How long the committer may hold a batch open waiting for more entries
    /// once the first one arrived. `0` starts every batch immediately.
    pub max_wait: Duration,
    /// A batch is closed once it holds this many bytes of encoded lines. A
    /// single larger entry still forms a batch of its own.
    pub max_batch_bytes: usize,
    /// Maximum number of queued (not yet committing) entries.
    pub queue_capacity: usize,
}

impl Default for GroupCommitConfig {
    fn default() -> Self {
        Self {
            max_wait: Duration::ZERO,
            max_batch_bytes: 1024 * 1024,
            queue_capacity: 1024,
        }
    }
}

impl GroupCommitConfig {
    /// Clamps every field into its supported range.
    pub fn normalized(mut self) -> Self {
        self.max_wait = self.max_wait.min(GROUP_COMMIT_MAX_WAIT_LIMIT);
        self.max_batch_bytes = self.max_batch_bytes.max(1);
        self.queue_capacity = self.queue_capacity.max(1);
        self
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum GroupCommitError {
    /// The queue is full. Nothing was written; the caller may retry later.
    Overloaded { capacity: usize },
    /// An earlier fsync failed and the log refuses writes until restart.
    Poisoned(String),
    /// The batch holding this entry failed to append or sync.
    Failed(StoreError),
    /// The committer stopped before this entry was committed.
    Stopped,
}

impl std::fmt::Display for GroupCommitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Overloaded { capacity } => {
                write!(f, "WAL group commit queue is full ({capacity} entries)")
            }
            Self::Poisoned(reason) => write!(f, "{}: {reason}", crate::WAL_POISONED_PREFIX),
            Self::Failed(err) => write!(f, "WAL group commit failed: {err:?}"),
            Self::Stopped => write!(f, "WAL group committer stopped"),
        }
    }
}

impl From<GroupCommitError> for StoreError {
    fn from(value: GroupCommitError) -> Self {
        match value {
            GroupCommitError::Failed(err) => err,
            other => StoreError::Io(other.to_string()),
        }
    }
}

/// Counters exposed for metrics.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GroupCommitStats {
    /// Batches handed to the log (successful or not).
    pub batches_total: u64,
    /// Entries in those batches.
    pub entries_total: u64,
    /// Encoded bytes in those batches.
    pub bytes_total: u64,
    /// Batches whose append or fsync failed.
    pub failed_batches_total: u64,
    /// Enqueue attempts rejected because the queue was full.
    pub rejected_total: u64,
    /// Entries in the most recent batch.
    pub last_batch_entries: u64,
    /// Largest batch so far, in entries.
    pub max_batch_entries: u64,
    /// Entries currently queued.
    pub queue_depth: u64,
}

/// Waits for one enqueued entry to be committed.
#[must_use = "a ticket must be waited on before the write is acknowledged"]
pub struct CommitTicket {
    rx: mpsc::Receiver<Result<(), GroupCommitError>>,
}

impl CommitTicket {
    /// Blocks until the batch holding this entry is durable (`Ok`) or failed.
    pub fn wait(self) -> Result<(), GroupCommitError> {
        self.rx.recv().unwrap_or(Err(GroupCommitError::Stopped))
    }
}

struct Entry {
    lines: Vec<String>,
    bytes: usize,
    done: mpsc::SyncSender<Result<(), GroupCommitError>>,
}

#[derive(Default)]
struct QueueState {
    entries: VecDeque<Entry>,
    queued_bytes: usize,
    poisoned: Option<String>,
    stopping: bool,
}

#[derive(Default)]
struct Counters {
    batches_total: AtomicU64,
    entries_total: AtomicU64,
    bytes_total: AtomicU64,
    failed_batches_total: AtomicU64,
    rejected_total: AtomicU64,
    last_batch_entries: AtomicU64,
    max_batch_entries: AtomicU64,
}

struct Shared {
    state: Mutex<QueueState>,
    work_ready: Condvar,
    counters: Counters,
    config: GroupCommitConfig,
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, QueueState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// Handle to the committer thread. Dropping it stops the thread after the
/// queued entries are committed.
pub struct GroupCommitter {
    shared: Arc<Shared>,
    handle: Option<JoinHandle<()>>,
}

impl GroupCommitter {
    /// Starts the committer thread for `log`. Other users of `log` share its
    /// mutex with the committer, so their writes never interleave with a
    /// batch.
    pub fn start<L: GroupCommitLog>(
        log: Arc<Mutex<L>>,
        config: GroupCommitConfig,
    ) -> std::io::Result<Self> {
        let config = config.normalized();
        let poisoned = lock_log(&log).poisoned_reason();
        let shared = Arc::new(Shared {
            state: Mutex::new(QueueState {
                poisoned,
                ..QueueState::default()
            }),
            work_ready: Condvar::new(),
            counters: Counters::default(),
            config,
        });
        let thread_shared = Arc::clone(&shared);
        let handle = std::thread::Builder::new()
            .name("wal-group-commit".to_string())
            .spawn(move || run_committer(&thread_shared, &log))?;
        Ok(Self {
            shared,
            handle: Some(handle),
        })
    }

    pub fn config(&self) -> &GroupCommitConfig {
        &self.shared.config
    }

    /// Queues `lines` for the next batch. Never blocks on I/O: it fails fast
    /// when the queue is full, the log is poisoned or the committer stopped.
    pub fn enqueue(&self, lines: Vec<String>) -> Result<CommitTicket, GroupCommitError> {
        let bytes = lines.iter().map(|line| line.len() + 1).sum();
        let (done, rx) = mpsc::sync_channel(1);
        let mut state = self.shared.lock();
        if let Some(reason) = &state.poisoned {
            return Err(GroupCommitError::Poisoned(reason.clone()));
        }
        if state.stopping {
            return Err(GroupCommitError::Stopped);
        }
        let capacity = self.shared.config.queue_capacity;
        if state.entries.len() >= capacity {
            self.shared
                .counters
                .rejected_total
                .fetch_add(1, Ordering::Relaxed);
            return Err(GroupCommitError::Overloaded { capacity });
        }
        state.entries.push_back(Entry { lines, bytes, done });
        state.queued_bytes += bytes;
        drop(state);
        self.shared.work_ready.notify_one();
        Ok(CommitTicket { rx })
    }

    /// Enqueues `lines` and waits for the outcome.
    pub fn commit(&self, lines: Vec<String>) -> Result<(), GroupCommitError> {
        self.enqueue(lines)?.wait()
    }

    /// Why the committer refuses writes, if it does.
    pub fn poisoned_reason(&self) -> Option<String> {
        self.shared.lock().poisoned.clone()
    }

    pub fn stats(&self) -> GroupCommitStats {
        let c = &self.shared.counters;
        GroupCommitStats {
            batches_total: c.batches_total.load(Ordering::Relaxed),
            entries_total: c.entries_total.load(Ordering::Relaxed),
            bytes_total: c.bytes_total.load(Ordering::Relaxed),
            failed_batches_total: c.failed_batches_total.load(Ordering::Relaxed),
            rejected_total: c.rejected_total.load(Ordering::Relaxed),
            last_batch_entries: c.last_batch_entries.load(Ordering::Relaxed),
            max_batch_entries: c.max_batch_entries.load(Ordering::Relaxed),
            queue_depth: self.shared.lock().entries.len() as u64,
        }
    }
}

impl Drop for GroupCommitter {
    fn drop(&mut self) {
        self.shared.lock().stopping = true;
        self.shared.work_ready.notify_all();
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

fn lock_log<L>(log: &Mutex<L>) -> MutexGuard<'_, L> {
    log.lock().unwrap_or_else(|e| e.into_inner())
}

fn run_committer<L: GroupCommitLog>(shared: &Shared, log: &Mutex<L>) {
    let config = &shared.config;
    loop {
        let mut state = shared.lock();
        while state.entries.is_empty() && !state.stopping {
            state = shared
                .work_ready
                .wait(state)
                .unwrap_or_else(|e| e.into_inner());
        }
        if state.entries.is_empty() {
            // Stopping and nothing left to commit.
            return;
        }
        if !config.max_wait.is_zero() {
            let deadline = Instant::now() + config.max_wait;
            while state.queued_bytes < config.max_batch_bytes && !state.stopping {
                let now = Instant::now();
                if now >= deadline {
                    break;
                }
                state = shared
                    .work_ready
                    .wait_timeout(state, deadline - now)
                    .unwrap_or_else(|e| e.into_inner())
                    .0;
            }
        }
        let mut batch = Vec::new();
        let mut batch_bytes = 0usize;
        while let Some(front) = state.entries.front() {
            if !batch.is_empty() && batch_bytes + front.bytes > config.max_batch_bytes {
                break;
            }
            let entry = state.entries.pop_front().expect("front exists");
            batch_bytes += entry.bytes;
            batch.push(entry);
        }
        state.queued_bytes -= batch_bytes;
        let poisoned = state.poisoned.clone();
        drop(state);

        if let Some(reason) = poisoned {
            for entry in batch {
                let _ = entry
                    .done
                    .send(Err(GroupCommitError::Poisoned(reason.clone())));
            }
            continue;
        }

        let mut lines = Vec::with_capacity(batch.iter().map(|e| e.lines.len()).sum());
        let mut waiters = Vec::with_capacity(batch.len());
        for entry in batch {
            lines.extend(entry.lines);
            waiters.push(entry.done);
        }
        let outcome = {
            let mut log = lock_log(log);
            log.commit_lines(&lines)
                .map_err(|err| (err, log.poisoned_reason()))
        };

        let counters = &shared.counters;
        let entries = waiters.len() as u64;
        counters.batches_total.fetch_add(1, Ordering::Relaxed);
        counters.entries_total.fetch_add(entries, Ordering::Relaxed);
        counters
            .bytes_total
            .fetch_add(batch_bytes as u64, Ordering::Relaxed);
        counters
            .last_batch_entries
            .store(entries, Ordering::Relaxed);
        counters
            .max_batch_entries
            .fetch_max(entries, Ordering::Relaxed);
        crate::observe::observe_group_commit_batch(entries as usize);

        let result = match outcome {
            Ok(()) => Ok(()),
            Err((err, poisoned)) => {
                counters
                    .failed_batches_total
                    .fetch_add(1, Ordering::Relaxed);
                if let Some(reason) = poisoned {
                    shared.lock().poisoned = Some(reason);
                }
                Err(GroupCommitError::Failed(err))
            }
        };
        for done in waiters {
            let _ = done.send(result.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// What the fake log does on its next `commit_lines` call.
    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    enum Fault {
        AppendError,
        SyncError,
    }

    /// In-memory log that records each committed batch. A test can make the
    /// log block inside `commit_lines` (announcing itself on `entered`) until
    /// it is released, and can schedule append/fsync failures by call index.
    struct FakeLog {
        committed: Arc<Mutex<Vec<Vec<String>>>>,
        calls: Arc<AtomicU64>,
        faults: Vec<(u64, Fault)>,
        gate: Option<(mpsc::Sender<u64>, mpsc::Receiver<()>)>,
        poisoned: Option<String>,
    }

    struct Probe {
        committed: Arc<Mutex<Vec<Vec<String>>>>,
        calls: Arc<AtomicU64>,
    }

    impl Probe {
        fn batches(&self) -> Vec<Vec<String>> {
            self.committed.lock().unwrap().clone()
        }
        fn calls(&self) -> u64 {
            self.calls.load(Ordering::SeqCst)
        }
    }

    fn fake(faults: Vec<(u64, Fault)>) -> (FakeLog, Probe) {
        let committed = Arc::new(Mutex::new(Vec::new()));
        let calls = Arc::new(AtomicU64::new(0));
        (
            FakeLog {
                committed: Arc::clone(&committed),
                calls: Arc::clone(&calls),
                faults,
                gate: None,
                poisoned: None,
            },
            Probe { committed, calls },
        )
    }

    /// Adds a gate: returns (entered receiver, release sender).
    fn gated(log: &mut FakeLog) -> (mpsc::Receiver<u64>, mpsc::Sender<()>) {
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        log.gate = Some((entered_tx, release_rx));
        (entered_rx, release_tx)
    }

    impl GroupCommitLog for FakeLog {
        fn commit_lines(&mut self, lines: &[String]) -> Result<(), StoreError> {
            assert!(
                self.poisoned.is_none(),
                "a poisoned log must not be written"
            );
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if let Some((entered, release)) = &self.gate {
                entered.send(call).unwrap();
                release.recv().unwrap();
            }
            match self.faults.iter().find(|(at, _)| *at == call).map(|f| f.1) {
                Some(Fault::AppendError) => Err(StoreError::Io("append failed".into())),
                Some(Fault::SyncError) => {
                    self.poisoned = Some("fsync failed".into());
                    Err(StoreError::Io("wal_poisoned: fsync failed".into()))
                }
                None => {
                    self.committed.lock().unwrap().push(lines.to_vec());
                    Ok(())
                }
            }
        }

        fn poisoned_reason(&self) -> Option<String> {
            self.poisoned.clone()
        }
    }

    fn lines(tag: &str) -> Vec<String> {
        vec![format!("{tag}-begin"), format!("{tag}-end")]
    }

    fn start(log: FakeLog, config: GroupCommitConfig) -> GroupCommitter {
        GroupCommitter::start(Arc::new(Mutex::new(log)), config).unwrap()
    }

    /// Enqueue A and hold the committer inside A's commit; B, C and D queue up
    /// meanwhile. Returns their tickets once the committer is parked on A.
    fn park_on_first(
        committer: &GroupCommitter,
        entered: &mpsc::Receiver<u64>,
    ) -> (CommitTicket, Vec<CommitTicket>) {
        let a = committer.enqueue(lines("a")).unwrap();
        assert_eq!(entered.recv().unwrap(), 0, "committer took A first");
        let rest = ["b", "c", "d"]
            .iter()
            .map(|tag| committer.enqueue(lines(tag)).unwrap())
            .collect();
        (a, rest)
    }

    #[test]
    fn entries_queued_during_a_commit_share_the_next_batch() {
        let (mut log, probe) = fake(vec![]);
        let (entered, release) = gated(&mut log);
        let committer = start(log, GroupCommitConfig::default());
        let (a, rest) = park_on_first(&committer, &entered);
        assert_eq!(committer.stats().queue_depth, 3);

        release.send(()).unwrap();
        a.wait().unwrap();
        assert_eq!(entered.recv().unwrap(), 1);
        release.send(()).unwrap();
        for ticket in rest {
            ticket.wait().unwrap();
        }

        assert_eq!(
            probe.batches(),
            vec![lines("a"), [lines("b"), lines("c"), lines("d")].concat(),],
            "B, C and D must share one append/fsync, in enqueue order"
        );
        let stats = committer.stats();
        assert_eq!(stats.batches_total, 2);
        assert_eq!(stats.entries_total, 4);
        assert_eq!(stats.max_batch_entries, 3);
        assert_eq!(stats.last_batch_entries, 3);
        assert_eq!(stats.queue_depth, 0);
    }

    #[test]
    fn max_batch_bytes_splits_batches_in_order() {
        let (mut log, probe) = fake(vec![]);
        let (entered, release) = gated(&mut log);
        let entry_bytes: usize = lines("b").iter().map(|l| l.len() + 1).sum();
        let committer = start(
            log,
            GroupCommitConfig {
                max_batch_bytes: entry_bytes * 2,
                ..GroupCommitConfig::default()
            },
        );
        let (a, rest) = park_on_first(&committer, &entered);
        release.send(()).unwrap();
        a.wait().unwrap();
        for expected_call in [1, 2] {
            assert_eq!(entered.recv().unwrap(), expected_call);
            release.send(()).unwrap();
        }
        for ticket in rest {
            ticket.wait().unwrap();
        }
        assert_eq!(
            probe.batches(),
            vec![lines("a"), [lines("b"), lines("c")].concat(), lines("d")]
        );
    }

    #[test]
    fn append_error_fails_every_entry_of_the_batch_and_only_that_batch() {
        let (mut log, probe) = fake(vec![(1, Fault::AppendError)]);
        let (entered, release) = gated(&mut log);
        let committer = start(log, GroupCommitConfig::default());
        let (a, rest) = park_on_first(&committer, &entered);
        release.send(()).unwrap();
        assert_eq!(a.wait(), Ok(()));
        assert_eq!(entered.recv().unwrap(), 1);
        release.send(()).unwrap();
        for ticket in rest {
            assert!(
                matches!(ticket.wait(), Err(GroupCommitError::Failed(_))),
                "every entry of the failed batch must see the error"
            );
        }
        assert_eq!(probe.batches(), vec![lines("a")]);
        assert_eq!(committer.stats().failed_batches_total, 1);

        // An append error is not a poison: the next write goes through.
        assert!(committer.poisoned_reason().is_none());
        let e = committer.enqueue(lines("e")).unwrap();
        assert_eq!(entered.recv().unwrap(), 2);
        release.send(()).unwrap();
        e.wait().unwrap();
        assert_eq!(probe.batches(), vec![lines("a"), lines("e")]);
    }

    #[test]
    fn fsync_error_poisons_the_committer_and_never_retries_the_log() {
        let (mut log, probe) = fake(vec![(1, Fault::SyncError)]);
        let (entered, release) = gated(&mut log);
        let committer = start(log, GroupCommitConfig::default());
        let (a, rest) = park_on_first(&committer, &entered);
        release.send(()).unwrap();
        a.wait().unwrap();
        assert_eq!(entered.recv().unwrap(), 1);
        release.send(()).unwrap();
        for ticket in rest {
            assert!(matches!(ticket.wait(), Err(GroupCommitError::Failed(_))));
        }

        assert!(committer.poisoned_reason().is_some());
        match committer.enqueue(lines("late")) {
            Err(GroupCommitError::Poisoned(reason)) => assert!(reason.contains("fsync")),
            other => panic!("expected a poisoned rejection, got {:?}", other.map(|_| ())),
        }
        drop(committer);
        assert_eq!(
            probe.calls(),
            2,
            "the log is never written after the fsync error"
        );
        assert_eq!(probe.batches(), vec![lines("a")]);
    }

    #[test]
    fn entries_queued_behind_a_poisoning_batch_fail_without_touching_the_log() {
        let (mut log, probe) = fake(vec![(0, Fault::SyncError)]);
        let (entered, release) = gated(&mut log);
        let committer = start(log, GroupCommitConfig::default());
        let (a, rest) = park_on_first(&committer, &entered);
        release.send(()).unwrap();
        assert!(matches!(a.wait(), Err(GroupCommitError::Failed(_))));
        for ticket in rest {
            assert!(matches!(ticket.wait(), Err(GroupCommitError::Poisoned(_))));
        }
        drop(committer);
        assert_eq!(probe.calls(), 1);
    }

    #[test]
    fn a_log_poisoned_before_start_rejects_every_enqueue() {
        let (mut log, probe) = fake(vec![]);
        log.poisoned = Some("fsync failed earlier".into());
        let committer = start(log, GroupCommitConfig::default());
        assert!(matches!(
            committer.enqueue(lines("x")),
            Err(GroupCommitError::Poisoned(_))
        ));
        drop(committer);
        assert_eq!(probe.calls(), 0);
    }

    #[test]
    fn a_full_queue_rejects_instead_of_growing() {
        let (mut log, probe) = fake(vec![]);
        let (entered, release) = gated(&mut log);
        let committer = start(
            log,
            GroupCommitConfig {
                queue_capacity: 2,
                ..GroupCommitConfig::default()
            },
        );
        let a = committer.enqueue(lines("a")).unwrap();
        assert_eq!(entered.recv().unwrap(), 0);
        // A is committing (not queued); two more fit, the third is rejected.
        let b = committer.enqueue(lines("b")).unwrap();
        let c = committer.enqueue(lines("c")).unwrap();
        assert_eq!(
            committer.enqueue(lines("d")).err(),
            Some(GroupCommitError::Overloaded { capacity: 2 })
        );
        assert_eq!(committer.stats().rejected_total, 1);
        release.send(()).unwrap();
        a.wait().unwrap();
        assert_eq!(entered.recv().unwrap(), 1);
        release.send(()).unwrap();
        b.wait().unwrap();
        c.wait().unwrap();
        assert_eq!(
            probe.batches(),
            vec![lines("a"), [lines("b"), lines("c")].concat()]
        );
    }

    #[test]
    fn a_full_batch_does_not_wait_for_max_wait() {
        let (log, probe) = fake(vec![]);
        let entry_bytes: usize = lines("a").iter().map(|l| l.len() + 1).sum();
        // max_wait is clamped to the 10 ms limit; a batch that is already full
        // must still be committed without lingering. Hold the queue lock
        // while enqueueing both entries so they are seen together.
        let committer = start(
            log,
            GroupCommitConfig {
                max_wait: Duration::from_secs(3600),
                max_batch_bytes: entry_bytes * 2,
                ..GroupCommitConfig::default()
            },
        );
        assert_eq!(committer.config().max_wait, GROUP_COMMIT_MAX_WAIT_LIMIT);
        let a = committer.enqueue(lines("a")).unwrap();
        let b = committer.enqueue(lines("b")).unwrap();
        a.wait().unwrap();
        b.wait().unwrap();
        let committed: Vec<String> = probe.batches().concat();
        assert_eq!(committed, [lines("a"), lines("b")].concat());
    }

    #[test]
    fn an_idle_single_entry_is_committed_within_max_wait() {
        let (log, probe) = fake(vec![]);
        let max_wait = Duration::from_millis(5);
        let committer = start(
            log,
            GroupCommitConfig {
                max_wait,
                ..GroupCommitConfig::default()
            },
        );
        let started = Instant::now();
        committer.commit(lines("solo")).unwrap();
        let elapsed = started.elapsed();
        assert!(elapsed >= max_wait, "the batch lingers for max_wait");
        // Generous upper bound: lingering is capped by max_wait, not by the
        // arrival of more entries (which never come here).
        assert!(elapsed < Duration::from_secs(5), "took {elapsed:?}");
        assert_eq!(probe.batches(), vec![lines("solo")]);
    }

    #[test]
    fn dropping_the_committer_commits_what_is_queued() {
        let (mut log, probe) = fake(vec![]);
        let (entered, release) = gated(&mut log);
        let committer = start(log, GroupCommitConfig::default());
        let (a, rest) = park_on_first(&committer, &entered);
        let releaser = std::thread::spawn(move || {
            release.send(()).unwrap();
            assert_eq!(entered.recv().unwrap(), 1);
            release.send(()).unwrap();
        });
        drop(committer);
        releaser.join().unwrap();
        a.wait().unwrap();
        for ticket in rest {
            ticket.wait().unwrap();
        }
        assert_eq!(probe.batches().len(), 2);
    }

    #[test]
    fn concurrent_writers_every_acknowledged_entry_survives_reopen() {
        use crate::{InMemoryStore, record_to_line};
        use schema::claim_builder;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");
        let wal = Arc::new(Mutex::new(FileWal::open(&path).unwrap()));
        let committer = Arc::new(
            GroupCommitter::start(Arc::clone(&wal), GroupCommitConfig::default()).unwrap(),
        );

        let threads = 16;
        let per_thread = 25;
        let acknowledged: Vec<Vec<String>> = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..threads)
                .map(|t| {
                    let committer = Arc::clone(&committer);
                    scope.spawn(move || {
                        let mut acked = Vec::new();
                        for i in 0..per_thread {
                            let id = format!("t{t}-c{i}");
                            let store = InMemoryStore::new();
                            let prepared = store
                                .prepare_atomic_ingest(
                                    claim_builder(&id, "tenant-a", &format!("claim {id}"), 0.9),
                                    vec![],
                                    vec![],
                                    None,
                                    1,
                                )
                                .unwrap()
                                .unwrap();
                            committer.commit(prepared.wal_lines().to_vec()).unwrap();
                            acked.push(id);
                        }
                        acked
                    })
                })
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });
        let stats = committer.stats();
        drop(committer);
        drop(wal);

        let reopened = FileWal::open(&path).unwrap();
        let store = InMemoryStore::load_from_wal(&reopened).unwrap();
        for id in acknowledged.iter().flatten() {
            assert!(store.claim_by_id(id).is_some(), "acknowledged {id} lost");
        }
        assert_eq!(store.claims_len(), threads * per_thread);
        assert_eq!(stats.entries_total, (threads * per_thread) as u64);
        assert!(stats.batches_total <= stats.entries_total);
        // Each claim's commit group is contiguous: groups never interleave.
        let text = std::fs::read_to_string(&path).unwrap();
        let mut open: Option<String> = None;
        for line in text.lines() {
            if let Some(id) = line
                .split('\t')
                .nth(1)
                .and_then(|f| f.strip_prefix("~grp:"))
            {
                assert!(open.is_none(), "group opened inside another group");
                open = Some(id.to_string());
            } else if line.contains("~tx:") {
                assert!(open.take().is_some(), "group closed without being opened");
            }
        }
        assert!(open.is_none());
        let _ = record_to_line;
    }

    #[test]
    fn file_wal_poisons_on_fsync_failure_and_refuses_every_write() {
        use schema::claim_builder;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");
        let mut wal = FileWal::open(&path).unwrap();
        wal.append_group_lines(&lines("ok")).unwrap();
        let before = std::fs::metadata(&path).unwrap().len();

        crate::failpoint::arm("wal.sync");
        let err = wal.append_group_lines(&lines("bad")).unwrap_err();
        crate::failpoint::disarm();
        assert!(format!("{err:?}").contains(crate::WAL_POISONED_PREFIX));
        assert!(wal.poisoned_reason().is_some());

        // Every write path now fails closed, without another fsync attempt.
        crate::failpoint::take_trace();
        assert!(wal.append_group_lines(&lines("later")).is_err());
        assert!(
            wal.append_claim(&claim_builder("c", "t", "text", 0.9))
                .is_err()
        );
        assert!(wal.flush_pending_sync().is_err());
        let point_err = wal.begin_rollback_point();
        assert!(point_err.is_err());
        assert!(
            !crate::failpoint::take_trace().contains(&"wal.sync"),
            "a poisoned WAL must not retry fsync"
        );
        // No rollback was attempted over the unsynced bytes either.
        assert!(std::fs::metadata(&path).unwrap().len() >= before);
    }

    #[test]
    fn file_wal_append_error_rolls_the_group_back() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");
        let mut wal = FileWal::open(&path).unwrap();
        wal.append_group_lines(&lines("ok")).unwrap();
        let before = std::fs::read(&path).unwrap();
        let records = wal.wal_record_count().unwrap();

        // Replace the log with a directory: opening it for append fails.
        std::fs::remove_file(&path).unwrap();
        std::fs::create_dir(&path).unwrap();
        assert!(wal.append_group_lines(&lines("bad")).is_err());
        assert!(
            wal.poisoned_reason().is_none(),
            "an open error is not a poison"
        );
        std::fs::remove_dir(&path).unwrap();
        std::fs::write(&path, &before).unwrap();
        assert_eq!(wal.wal_record_count().unwrap(), records);
        wal.append_group_lines(&lines("next")).unwrap();
        let text = std::fs::read_to_string(&path).unwrap();
        assert_eq!(
            text.lines().collect::<Vec<_>>(),
            [lines("ok"), lines("next")].concat()
        );
    }
}
