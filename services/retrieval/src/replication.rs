//! Retrieval-side replication follower.
//!
//! The follower pulls WAL frames from an ingestion leader and applies them
//! to the shared in-memory store. Protocol rules (see the production
//! readiness plan, REP-01..REP-12):
//!
//! * Every frame carries the leader's WAL generation. The follower persists
//!   `(generation, offset)` together and sends `from_generation` on every
//!   poll; a mismatch makes the leader answer `needs_resync=1`.
//! * A resync fetches the full export, builds a FRESH store from it and
//!   swaps it in (never merges), then continues from
//!   `offset = export.wal_lines.len()` (WAL lines only, the same unit the
//!   leader counts in).
//! * A delta batch is applied to a detached clone and committed only if
//!   every record applied, so a failing record never leaves a half-applied
//!   batch behind.
//! * All reads are size- and time-bounded, numeric header values are
//!   validated before use, failures back off exponentially, and a panic
//!   inside the loop is caught and logged instead of silently killing the
//!   thread.
//! * The saved offset is only trusted when the follower's own WAL
//!   (`DASH_RETRIEVAL_WAL_PATH`) holds exactly that many replicated
//!   records. Without a retrieval WAL the follower starts with a full
//!   resync on every boot, because a restart would otherwise resume from the
//!   saved offset into an empty store.
//!
//! The shared secret is `DASH_INGEST_REPLICATION_TOKEN` (the leader checks
//! the same variable); `DASH_RETRIEVAL_REPLICATION_TOKEN` overrides it for
//! the follower when the two services are configured separately. The `EME_`
//! spellings are still accepted.

use std::{
    collections::HashMap,
    io::{Read, Write},
    net::{TcpStream, ToSocketAddrs},
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc, Mutex, OnceLock, RwLock,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use store::{FileWal, InMemoryStore, WalReplicationExport};

const DEFAULT_REPLICATION_POLL_INTERVAL_MS: u64 = 1000;
const DEFAULT_REPLICATION_MAX_RECORDS: usize = 512;
const DEFAULT_MAX_RESPONSE_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_MAX_BACKOFF_MS: u64 = 30_000;
const DEFAULT_MAX_LAG_RECORDS: usize = 100_000;
const DEFAULT_MAX_STALENESS_MS: u64 = 300_000;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const IO_TIMEOUT: Duration = Duration::from_secs(10);
const REQUEST_DEADLINE: Duration = Duration::from_secs(60);
const HEADER_SLACK_BYTES: usize = 16 * 1024;
const PREALLOC_CAP: usize = 4096;

/// Configuration for the retrieval follower that pulls WAL records from
/// an upstream ingestion service. If no source URL is configured the
/// follower thread is not started and retrieval serves whatever was
/// loaded from its local WAL/segments at startup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationFollowerConfig {
    pub source_base_url: String,
    pub poll_interval: Duration,
    pub max_records: usize,
    pub token: Option<String>,
    /// Explicit offset-state path. `None` derives `<wal path>.replication`
    /// from the retrieval WAL when one is configured.
    pub offset_path: Option<String>,
    /// Upper bound for one response body (WAL frame or export).
    pub max_response_bytes: usize,
    /// Upper bound for the failure backoff delay.
    pub max_backoff: Duration,
    /// `/ready` fails when the leader is more than this many records ahead.
    pub max_lag_records: usize,
    /// `/ready` fails when the last successful poll is older than this.
    pub max_staleness_ms: u64,
}

impl ReplicationFollowerConfig {
    pub fn new(source_base_url: impl Into<String>) -> Self {
        Self {
            source_base_url: source_base_url
                .into()
                .trim()
                .trim_end_matches('/')
                .to_string(),
            poll_interval: Duration::from_millis(DEFAULT_REPLICATION_POLL_INTERVAL_MS),
            max_records: DEFAULT_REPLICATION_MAX_RECORDS,
            token: None,
            offset_path: None,
            max_response_bytes: DEFAULT_MAX_RESPONSE_BYTES,
            max_backoff: Duration::from_millis(DEFAULT_MAX_BACKOFF_MS),
            max_lag_records: DEFAULT_MAX_LAG_RECORDS,
            max_staleness_ms: DEFAULT_MAX_STALENESS_MS,
        }
    }

    pub fn from_env() -> Option<Self> {
        let source_base_url = env_with_fallback(
            "DASH_RETRIEVAL_REPLICATION_SOURCE_URL",
            "EME_RETRIEVAL_REPLICATION_SOURCE_URL",
        )?;
        let mut config = Self::new(source_base_url);
        if config.source_base_url.is_empty() {
            return None;
        }
        if let Some(ms) = env_u64(
            "DASH_RETRIEVAL_REPLICATION_POLL_INTERVAL_MS",
            "EME_RETRIEVAL_REPLICATION_POLL_INTERVAL_MS",
        ) {
            config.poll_interval = Duration::from_millis(ms);
        }
        if let Some(n) = env_u64(
            "DASH_RETRIEVAL_REPLICATION_MAX_RECORDS",
            "EME_RETRIEVAL_REPLICATION_MAX_RECORDS",
        ) {
            config.max_records = n as usize;
        }
        if let Some(n) = env_u64(
            "DASH_RETRIEVAL_REPLICATION_MAX_RESPONSE_BYTES",
            "EME_RETRIEVAL_REPLICATION_MAX_RESPONSE_BYTES",
        ) {
            config.max_response_bytes = n as usize;
        }
        if let Some(ms) = env_u64(
            "DASH_RETRIEVAL_REPLICATION_MAX_BACKOFF_MS",
            "EME_RETRIEVAL_REPLICATION_MAX_BACKOFF_MS",
        ) {
            config.max_backoff = Duration::from_millis(ms);
        }
        if let Some(n) = env_u64(
            "DASH_RETRIEVAL_REPLICATION_MAX_LAG_RECORDS",
            "EME_RETRIEVAL_REPLICATION_MAX_LAG_RECORDS",
        ) {
            config.max_lag_records = n as usize;
        }
        if let Some(ms) = env_u64(
            "DASH_RETRIEVAL_REPLICATION_MAX_STALENESS_MS",
            "EME_RETRIEVAL_REPLICATION_MAX_STALENESS_MS",
        ) {
            config.max_staleness_ms = ms;
        }
        config.token = [
            "DASH_RETRIEVAL_REPLICATION_TOKEN",
            "EME_RETRIEVAL_REPLICATION_TOKEN",
            "DASH_INGEST_REPLICATION_TOKEN",
            "EME_INGEST_REPLICATION_TOKEN",
        ]
        .iter()
        .filter_map(|key| std::env::var(key).ok())
        .map(|value| value.trim().to_string())
        .find(|value| !value.is_empty());
        config.offset_path = env_with_fallback(
            "DASH_RETRIEVAL_REPLICATION_OFFSET_PATH",
            "EME_RETRIEVAL_REPLICATION_OFFSET_PATH",
        )
        .filter(|value| !value.trim().is_empty());
        Some(config)
    }

    fn wal_pull_url(&self, from_offset: usize, from_generation: Option<u64>) -> String {
        let mut url = format!(
            "{}/internal/replication/wal?from_offset={from_offset}&max_records={}",
            self.source_base_url, self.max_records
        );
        if let Some(generation) = from_generation {
            url.push_str(&format!("&from_generation={generation}"));
        }
        url
    }

    fn export_url(&self) -> String {
        format!("{}/internal/replication/export", self.source_base_url)
    }

    /// Delay before the next poll after `consecutive_failures` failures in a
    /// row: the poll interval doubled per failure, capped at `max_backoff`.
    pub fn backoff_delay(&self, consecutive_failures: u64) -> Duration {
        if consecutive_failures == 0 {
            return self.poll_interval;
        }
        let shift = (consecutive_failures - 1).min(16) as u32;
        let scaled = self.poll_interval.saturating_mul(1u32 << shift);
        scaled.min(self.max_backoff.max(self.poll_interval))
    }
}

// ---------------------------------------------------------------------
// Status (shared with /ready and /metrics)
// ---------------------------------------------------------------------

/// Observable follower state. Updated by the follower thread, read by the
/// HTTP handlers.
#[derive(Debug)]
pub struct FollowerStatus {
    has_generation: AtomicBool,
    generation: AtomicU64,
    offset: AtomicUsize,
    leader_total: AtomicUsize,
    started_ms: u64,
    last_success_ms: AtomicU64,
    consecutive_failures: AtomicU64,
    failures_total: AtomicU64,
    resyncs_total: AtomicU64,
    applied_total: AtomicU64,
    synced_once: AtomicBool,
    last_error: Mutex<Option<String>>,
    max_lag_records: usize,
    max_staleness_ms: u64,
}

/// Point-in-time copy of [`FollowerStatus`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FollowerStatusSnapshot {
    pub generation: Option<u64>,
    pub offset: usize,
    pub leader_total_records: usize,
    pub lag_records: usize,
    /// Milliseconds since the last successful poll (since start if none).
    pub last_success_age_ms: u64,
    pub consecutive_failures: u64,
    pub failures_total: u64,
    pub resyncs_total: u64,
    pub applied_records_total: u64,
    pub synced_once: bool,
    pub last_error: Option<String>,
}

impl FollowerStatus {
    fn new(config: &ReplicationFollowerConfig) -> Self {
        Self {
            has_generation: AtomicBool::new(false),
            generation: AtomicU64::new(0),
            offset: AtomicUsize::new(0),
            leader_total: AtomicUsize::new(0),
            started_ms: now_ms(),
            last_success_ms: AtomicU64::new(0),
            consecutive_failures: AtomicU64::new(0),
            failures_total: AtomicU64::new(0),
            resyncs_total: AtomicU64::new(0),
            applied_total: AtomicU64::new(0),
            synced_once: AtomicBool::new(false),
            last_error: Mutex::new(None),
            max_lag_records: config.max_lag_records,
            max_staleness_ms: config.max_staleness_ms,
        }
    }

    pub fn snapshot(&self) -> FollowerStatusSnapshot {
        let offset = self.offset.load(Ordering::Relaxed);
        let leader_total = self.leader_total.load(Ordering::Relaxed);
        let last_success = self.last_success_ms.load(Ordering::Relaxed);
        let reference = if last_success == 0 {
            self.started_ms
        } else {
            last_success
        };
        FollowerStatusSnapshot {
            generation: self
                .has_generation
                .load(Ordering::Relaxed)
                .then(|| self.generation.load(Ordering::Relaxed)),
            offset,
            leader_total_records: leader_total,
            lag_records: leader_total.saturating_sub(offset),
            last_success_age_ms: now_ms().saturating_sub(reference),
            consecutive_failures: self.consecutive_failures.load(Ordering::Relaxed),
            failures_total: self.failures_total.load(Ordering::Relaxed),
            resyncs_total: self.resyncs_total.load(Ordering::Relaxed),
            applied_records_total: self.applied_total.load(Ordering::Relaxed),
            synced_once: self.synced_once.load(Ordering::Relaxed),
            last_error: self.last_error.lock().ok().and_then(|guard| guard.clone()),
        }
    }

    /// `Err(reason)` when the follower is not healthy enough to serve.
    pub fn readiness(&self) -> Result<(), &'static str> {
        let snap = self.snapshot();
        if !snap.synced_once {
            return Err("replication_initial_sync_pending");
        }
        if snap.lag_records > self.max_lag_records {
            return Err("replication_lag_exceeded");
        }
        if snap.last_success_age_ms > self.max_staleness_ms {
            return Err("replication_stale");
        }
        Ok(())
    }

    /// JSON object describing follower state, embedded in `/ready`.
    pub fn to_json(&self) -> String {
        let snap = self.snapshot();
        let generation = snap
            .generation
            .map(|g| g.to_string())
            .unwrap_or_else(|| "null".to_string());
        let last_error = match snap.last_error.as_deref() {
            Some(err) => format!("\"{}\"", json_escape(err)),
            None => "null".to_string(),
        };
        format!(
            "{{\"generation\":{generation},\"offset\":{},\"leader_total_records\":{},\"lag_records\":{},\"last_success_age_ms\":{},\"consecutive_failures\":{},\"resyncs_total\":{},\"last_error\":{last_error}}}",
            snap.offset,
            snap.leader_total_records,
            snap.lag_records,
            snap.last_success_age_ms,
            snap.consecutive_failures,
            snap.resyncs_total,
        )
    }

    pub fn render_prometheus(&self) -> String {
        let snap = self.snapshot();
        let ready = self.readiness().is_ok() as u8;
        format!(
            "# TYPE dash_retrieval_replication_enabled gauge\n\
dash_retrieval_replication_enabled 1\n\
# TYPE dash_retrieval_replication_ready gauge\n\
dash_retrieval_replication_ready {ready}\n\
# TYPE dash_retrieval_replication_lag_records gauge\n\
dash_retrieval_replication_lag_records {}\n\
# TYPE dash_retrieval_replication_offset gauge\n\
dash_retrieval_replication_offset {}\n\
# TYPE dash_retrieval_replication_last_success_age_ms gauge\n\
dash_retrieval_replication_last_success_age_ms {}\n\
# TYPE dash_retrieval_replication_consecutive_failures gauge\n\
dash_retrieval_replication_consecutive_failures {}\n\
# TYPE dash_retrieval_replication_failures_total counter\n\
dash_retrieval_replication_failures_total {}\n\
# TYPE dash_retrieval_replication_resyncs_total counter\n\
dash_retrieval_replication_resyncs_total {}\n\
# TYPE dash_retrieval_replication_applied_records_total counter\n\
dash_retrieval_replication_applied_records_total {}\n\
# TYPE dash_retrieval_replication_generation gauge\n\
dash_retrieval_replication_generation {}\n",
            snap.lag_records,
            snap.offset,
            snap.last_success_age_ms,
            snap.consecutive_failures,
            snap.failures_total,
            snap.resyncs_total,
            snap.applied_records_total,
            snap.generation.unwrap_or(0),
        )
    }

    fn record_state(&self, state: &FollowerState) {
        self.offset.store(state.offset, Ordering::Relaxed);
        match state.generation {
            Some(generation) => {
                self.generation.store(generation, Ordering::Relaxed);
                self.has_generation.store(true, Ordering::Relaxed);
            }
            None => self.has_generation.store(false, Ordering::Relaxed),
        }
    }

    fn record_success(&self) {
        self.last_success_ms
            .store(now_ms().max(1), Ordering::Relaxed);
        self.consecutive_failures.store(0, Ordering::Relaxed);
        self.synced_once.store(true, Ordering::Relaxed);
        if let Ok(mut guard) = self.last_error.lock() {
            *guard = None;
        }
    }

    fn record_failure(&self, error: String) {
        self.consecutive_failures.fetch_add(1, Ordering::Relaxed);
        self.failures_total.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut guard) = self.last_error.lock() {
            *guard = Some(error);
        }
    }
}

type StatusRegistry = Mutex<HashMap<usize, Arc<FollowerStatus>>>;

fn registry() -> &'static StatusRegistry {
    static REGISTRY: OnceLock<StatusRegistry> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Follower status for the store at `store_ptr` (the address of the
/// `RwLock<InMemoryStore>` inside its `Arc`), if a follower is attached.
pub(crate) fn status_for_store_ptr(store_ptr: usize) -> Option<Arc<FollowerStatus>> {
    registry().lock().ok()?.get(&store_ptr).cloned()
}

// ---------------------------------------------------------------------
// Follower thread
// ---------------------------------------------------------------------

/// Handle to a running follower.
pub struct FollowerHandle {
    status: Arc<FollowerStatus>,
    stop: Arc<AtomicBool>,
    gate: Arc<PauseGate>,
    thread: Option<thread::JoinHandle<()>>,
    registry_key: usize,
}

/// Lets tests (and operators debugging) hold the follower between polls.
#[derive(Debug, Default)]
struct PauseGate {
    paused: AtomicBool,
    polling: AtomicBool,
}

impl FollowerHandle {
    pub fn status(&self) -> &Arc<FollowerStatus> {
        &self.status
    }

    /// Stop polling. Returns once any in-flight poll has finished, so the
    /// leader can be mutated without the follower observing a half state.
    pub fn pause(&self) {
        self.gate.paused.store(true, Ordering::SeqCst);
        while self.gate.polling.load(Ordering::SeqCst) {
            thread::sleep(Duration::from_millis(2));
        }
    }

    pub fn resume(&self) {
        self.gate.paused.store(false, Ordering::SeqCst);
    }

    /// Stop the follower thread and wait for it to exit.
    pub fn stop(mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
        if let Ok(mut guard) = registry().lock() {
            guard.remove(&self.registry_key);
        }
    }
}

/// Start the follower described by the environment, if one is configured.
/// `wal` is the retrieval WAL the store was loaded from (if any); when
/// present the follower mirrors every applied record into it so a restart
/// can resume from the saved offset.
pub fn spawn_replication_follower(
    store: Arc<RwLock<InMemoryStore>>,
    wal: Option<FileWal>,
) -> Option<FollowerHandle> {
    let config = ReplicationFollowerConfig::from_env()?;
    Some(start_follower(store, config, wal))
}

pub fn start_follower(
    store: Arc<RwLock<InMemoryStore>>,
    config: ReplicationFollowerConfig,
    wal: Option<FileWal>,
) -> FollowerHandle {
    eprintln!(
        "retrieval replication follower: source={}, poll_interval_ms={}, durable={}",
        config.source_base_url,
        config.poll_interval.as_millis(),
        wal.is_some()
    );
    let status = Arc::new(FollowerStatus::new(&config));
    let registry_key = Arc::as_ptr(&store) as usize;
    if let Ok(mut guard) = registry().lock() {
        guard.insert(registry_key, Arc::clone(&status));
    }
    let stop = Arc::new(AtomicBool::new(false));
    let gate = Arc::new(PauseGate::default());
    // Built on the caller thread so the status already reflects resumed
    // state when this function returns.
    let mut follower = Follower::new(store, config, wal, Arc::clone(&status));
    let thread = {
        let stop = Arc::clone(&stop);
        let gate = Arc::clone(&gate);
        thread::spawn(move || {
            follower.run(&stop, &gate);
        })
    };
    FollowerHandle {
        status,
        stop,
        gate,
        thread: Some(thread),
        registry_key,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
struct FollowerState {
    generation: Option<u64>,
    offset: usize,
}

struct Follower {
    store: Arc<RwLock<InMemoryStore>>,
    config: ReplicationFollowerConfig,
    wal: Option<FileWal>,
    state_path: Option<String>,
    state: FollowerState,
    force_resync: bool,
    status: Arc<FollowerStatus>,
}

#[derive(Debug, PartialEq, Eq)]
enum PollOutcome {
    /// Caught up (or nothing to do); wait the poll interval.
    Idle,
    /// More records are waiting; poll again immediately.
    MoreAvailable,
}

impl Follower {
    fn new(
        store: Arc<RwLock<InMemoryStore>>,
        config: ReplicationFollowerConfig,
        wal: Option<FileWal>,
        status: Arc<FollowerStatus>,
    ) -> Self {
        let state_path = match (&config.offset_path, &wal) {
            (Some(path), _) => Some(path.clone()),
            (None, Some(wal)) => Some(format!("{}.replication", wal.path().display())),
            (None, None) => None,
        };
        let mut state = FollowerState::default();
        let mut force_resync = false;
        let mut resumed = false;
        match wal.as_ref() {
            Some(wal) => {
                let wal_count = wal.wal_record_count().unwrap_or(usize::MAX);
                let saved = state_path.as_deref().and_then(read_state);
                match saved {
                    Some(saved) if saved.offset == wal_count => {
                        state = saved;
                        resumed = true;
                        eprintln!(
                            "retrieval replication follower resuming from generation={:?} offset={}",
                            saved.generation, saved.offset
                        );
                    }
                    None if wal_count == 0 => {}
                    _ => {
                        // The WAL does not hold what the saved offset claims
                        // (or there is no saved offset for a populated WAL).
                        eprintln!(
                            "retrieval replication follower: saved state does not match local WAL (records={wal_count}); forcing full resync"
                        );
                        force_resync = true;
                    }
                }
            }
            None => {
                // Without a retrieval WAL nothing replicated survives a
                // restart, so a saved offset would resume into an empty
                // store. Always start from a full resync.
                force_resync = true;
            }
        }
        status.record_state(&state);
        if resumed {
            status.synced_once.store(true, Ordering::Relaxed);
            status.leader_total.store(state.offset, Ordering::Relaxed);
            status
                .last_success_ms
                .store(now_ms().max(1), Ordering::Relaxed);
        }
        Self {
            store,
            config,
            wal,
            state_path,
            state,
            force_resync,
            status,
        }
    }

    fn run(&mut self, stop: &AtomicBool, gate: &PauseGate) {
        while !stop.load(Ordering::SeqCst) {
            gate.polling.store(true, Ordering::SeqCst);
            if gate.paused.load(Ordering::SeqCst) {
                gate.polling.store(false, Ordering::SeqCst);
                thread::sleep(Duration::from_millis(5));
                continue;
            }
            let result = catch_unwind(AssertUnwindSafe(|| self.poll_once()));
            let delay = match result {
                Ok(Ok(PollOutcome::MoreAvailable)) => {
                    self.status.record_success();
                    Duration::ZERO
                }
                Ok(Ok(PollOutcome::Idle)) => {
                    self.status.record_success();
                    self.config.poll_interval
                }
                Ok(Err(err)) => self.on_failure(err),
                Err(panic) => {
                    // State may be half-updated: rebuild it from the leader.
                    self.force_resync = true;
                    self.on_failure(format!("follower loop panicked: {}", panic_message(&panic)))
                }
            };
            gate.polling.store(false, Ordering::SeqCst);
            sleep_interruptible(delay, stop);
        }
    }

    fn on_failure(&self, err: String) -> Duration {
        eprintln!("retrieval replication follower error: {err}");
        self.status.record_failure(err);
        let failures = self.status.consecutive_failures.load(Ordering::Relaxed);
        self.config.backoff_delay(failures)
    }

    fn poll_once(&mut self) -> Result<PollOutcome, String> {
        if self.force_resync {
            return self.resync();
        }
        let url = self
            .config
            .wal_pull_url(self.state.offset, self.state.generation);
        let response = http_get(
            &url,
            self.config.token.as_deref(),
            self.config.max_response_bytes,
        )?;
        if response.status != 200 {
            return Err(format!(
                "replication source returned status {}",
                response.status
            ));
        }
        let frame = parse_delta_frame(&response.body, self.config.max_records)?;
        if frame.from_offset != self.state.offset {
            return Err(format!(
                "replication frame from_offset {} does not match requested {}",
                frame.from_offset, self.state.offset
            ));
        }
        self.status
            .leader_total
            .store(frame.total_records, Ordering::Relaxed);
        let generation_changed = matches!(
            (self.state.generation, frame.generation),
            (Some(ours), Some(theirs)) if ours != theirs
        );
        if frame.needs_resync || generation_changed {
            return self.resync();
        }
        if frame.next_offset > frame.total_records {
            return Err("replication frame next_offset exceeds total_records".to_string());
        }
        // Apply only whole commit groups; an unterminated trailing group is
        // held back and re-fetched from its first line on the next poll.
        let mut frame = frame;
        let keep = store::complete_group_prefix_len(&frame.wal_lines);
        if keep < frame.wal_lines.len() {
            frame.wal_lines.truncate(keep);
            frame.next_offset = frame.from_offset + keep;
        }
        if !frame.wal_lines.is_empty() {
            self.apply_delta(&frame.wal_lines)?;
        }
        self.state = FollowerState {
            generation: frame.generation.or(self.state.generation),
            offset: frame.next_offset,
        };
        self.persist_state();
        self.status
            .applied_total
            .fetch_add(frame.wal_lines.len() as u64, Ordering::Relaxed);
        self.status.record_state(&self.state);
        Ok(if frame.next_offset < frame.total_records {
            PollOutcome::MoreAvailable
        } else {
            PollOutcome::Idle
        })
    }

    /// Apply one batch atomically: stage on a detached clone, mirror to the
    /// WAL, then commit. Any failure leaves the live store untouched.
    fn apply_delta(&mut self, lines: &[String]) -> Result<(), String> {
        let mut staged = {
            let guard = self.store.read().unwrap_or_else(|p| p.into_inner());
            guard.clone_detached()
        };
        for line in lines {
            staged
                .apply_persisted_record_line(line)
                .map_err(|err| format!("failed to apply replicated record: {err:?}"))?;
        }
        staged.clear_wal_events();
        if let Some(wal) = self.wal.as_mut() {
            let point = wal
                .begin_rollback_point()
                .map_err(|err| format!("failed to open WAL rollback point: {err:?}"))?;
            let appended = (|| {
                for line in lines {
                    wal.append_raw_record_line(line)?;
                }
                wal.flush_pending_sync()
            })();
            if let Err(err) = appended {
                if let Err(rollback) = wal.rollback_to(point) {
                    eprintln!("retrieval replication WAL rollback failed: {rollback:?}");
                }
                return Err(format!(
                    "failed to append replicated records to WAL: {err:?}"
                ));
            }
        }
        let mut guard = self.store.write().unwrap_or_else(|p| p.into_inner());
        if let Err(err) = guard.commit_staged(staged) {
            eprintln!("retrieval replication: disk mirror degraded after commit: {err:?}");
        }
        Ok(())
    }

    /// Replace the follower's state with the leader's full export.
    fn resync(&mut self) -> Result<PollOutcome, String> {
        let response = http_get(
            &self.config.export_url(),
            self.config.token.as_deref(),
            self.config.max_response_bytes,
        )?;
        if response.status != 200 {
            return Err(format!(
                "replication source export returned status {}",
                response.status
            ));
        }
        let export = parse_export_frame(&response.body)?;
        let ann_tuning = {
            let guard = self.store.read().unwrap_or_else(|p| p.into_inner());
            guard.ann_tuning().clone()
        };
        let mut fresh = InMemoryStore::new_with_ann_tuning(ann_tuning);
        for line in export
            .export
            .snapshot_lines
            .iter()
            .chain(export.export.wal_lines.iter())
        {
            fresh
                .apply_persisted_record_line(line)
                .map_err(|err| format!("failed to apply exported record: {err:?}"))?;
        }
        fresh.clear_wal_events();
        if let Some(wal) = self.wal.as_mut() {
            wal.replace_with_replication_export(&export.export)
                .map_err(|err| format!("failed to replace local WAL with export: {err:?}"))?;
        }
        {
            let mut guard = self.store.write().unwrap_or_else(|p| p.into_inner());
            if let Err(err) = guard.replace_state_from(fresh) {
                eprintln!("retrieval replication: disk resync degraded: {err:?}");
            }
        }
        let wal_len = export.export.wal_lines.len();
        self.state = FollowerState {
            generation: export.generation,
            offset: wal_len,
        };
        self.force_resync = false;
        self.persist_state();
        self.status.leader_total.store(wal_len, Ordering::Relaxed);
        self.status.resyncs_total.fetch_add(1, Ordering::Relaxed);
        self.status.applied_total.fetch_add(
            (export.export.snapshot_lines.len() + wal_len) as u64,
            Ordering::Relaxed,
        );
        self.status.record_state(&self.state);
        Ok(PollOutcome::MoreAvailable)
    }

    fn persist_state(&self) {
        // Only a follower with a local WAL can resume from the saved state.
        if self.wal.is_none() {
            return;
        }
        let Some(path) = self.state_path.as_deref() else {
            return;
        };
        if let Err(err) = write_state(path, &self.state) {
            eprintln!("retrieval replication follower failed to persist state: {err}");
        }
    }
}

fn sleep_interruptible(total: Duration, stop: &AtomicBool) {
    let deadline = Instant::now() + total;
    while !stop.load(Ordering::SeqCst) {
        let now = Instant::now();
        if now >= deadline {
            return;
        }
        thread::sleep((deadline - now).min(Duration::from_millis(25)));
    }
}

fn panic_message(panic: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(msg) = panic.downcast_ref::<&str>() {
        (*msg).to_string()
    } else if let Some(msg) = panic.downcast_ref::<String>() {
        msg.clone()
    } else {
        "unknown panic".to_string()
    }
}

// ---------------------------------------------------------------------
// HTTP client (bounded)
// ---------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReplicationSourceResponse {
    status: u16,
    body: String,
}

fn http_get(
    url: &str,
    token: Option<&str>,
    max_body_bytes: usize,
) -> Result<ReplicationSourceResponse, String> {
    let (authority, path) = parse_http_url(url)?;
    let addrs = authority
        .to_socket_addrs()
        .map_err(|err| format!("failed resolving replication source '{authority}': {err}"))?;
    let mut stream = None;
    let mut last_err = None;
    for addr in addrs {
        match TcpStream::connect_timeout(&addr, CONNECT_TIMEOUT) {
            Ok(s) => {
                stream = Some(s);
                break;
            }
            Err(err) => last_err = Some(err),
        }
    }
    let mut stream = stream.ok_or_else(|| {
        format!(
            "failed connecting replication source '{authority}': {}",
            last_err
                .map(|e| e.to_string())
                .unwrap_or_else(|| "no addresses".to_string())
        )
    })?;
    stream
        .set_read_timeout(Some(IO_TIMEOUT))
        .and_then(|()| stream.set_write_timeout(Some(IO_TIMEOUT)))
        .map_err(|err| format!("failed setting socket timeouts: {err}"))?;

    let mut request = format!(
        "GET {path} HTTP/1.1\r\nHost: {authority}\r\nConnection: close\r\nContent-Length: 0\r\n"
    );
    if let Some(token) = token {
        request.push_str(&format!("x-replication-token: {token}\r\n"));
    }
    request.push_str("\r\n");
    stream
        .write_all(request.as_bytes())
        .and_then(|()| stream.flush())
        .map_err(|err| format!("failed sending replication request: {err}"))?;

    let limit = max_body_bytes.saturating_add(HEADER_SLACK_BYTES);
    let deadline = Instant::now() + REQUEST_DEADLINE;
    let mut bytes = Vec::new();
    let mut chunk = [0u8; 64 * 1024];
    loop {
        if Instant::now() > deadline {
            return Err("replication response timed out".to_string());
        }
        let n = stream
            .read(&mut chunk)
            .map_err(|err| format!("failed reading replication response: {err}"))?;
        if n == 0 {
            break;
        }
        if bytes.len() + n > limit {
            return Err(format!(
                "replication response exceeds {max_body_bytes} byte limit"
            ));
        }
        bytes.extend_from_slice(&chunk[..n]);
    }
    parse_http_response(&bytes, max_body_bytes)
}

fn parse_http_response(
    bytes: &[u8],
    max_body_bytes: usize,
) -> Result<ReplicationSourceResponse, String> {
    let split = bytes
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .ok_or_else(|| "replication response missing HTTP header terminator".to_string())?;
    let header_block = std::str::from_utf8(&bytes[..split])
        .map_err(|_| "replication response headers are not valid UTF-8".to_string())?;
    let body_bytes = &bytes[split + 4..];
    let mut header_lines = header_block.lines();
    let status_line = header_lines
        .next()
        .ok_or_else(|| "replication response missing status line".to_string())?;
    let status = status_line
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| "replication response status line missing code".to_string())?
        .parse::<u16>()
        .map_err(|_| "replication response has invalid status code".to_string())?;
    for line in header_lines {
        if let Some((name, value)) = line.split_once(':')
            && name.trim().eq_ignore_ascii_case("content-length")
        {
            let declared = value
                .trim()
                .parse::<usize>()
                .map_err(|_| "replication response has invalid Content-Length".to_string())?;
            if declared > max_body_bytes {
                return Err(format!(
                    "replication response exceeds {max_body_bytes} byte limit"
                ));
            }
            if declared != body_bytes.len() {
                return Err("replication response body is truncated".to_string());
            }
        }
    }
    if body_bytes.len() > max_body_bytes {
        return Err(format!(
            "replication response exceeds {max_body_bytes} byte limit"
        ));
    }
    let body = String::from_utf8(body_bytes.to_vec())
        .map_err(|_| "replication response is not valid UTF-8".to_string())?;
    Ok(ReplicationSourceResponse { status, body })
}

// ---------------------------------------------------------------------
// Frame parsing
// ---------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
struct DeltaFrame {
    generation: Option<u64>,
    needs_resync: bool,
    from_offset: usize,
    next_offset: usize,
    total_records: usize,
    wal_lines: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ExportFrame {
    generation: Option<u64>,
    export: WalReplicationExport,
}

fn parse_delta_frame(body: &str, max_records: usize) -> Result<DeltaFrame, String> {
    let mut lines = body.lines().peekable();
    expect_kv(&mut lines, "status", "ok")?;
    let generation = parse_optional_generation(&mut lines)?;
    let needs_resync = parse_kv_bool01(&mut lines, "needs_resync")?;
    let from_offset = parse_kv_usize(&mut lines, "from_offset")?;
    let next_offset = parse_kv_usize(&mut lines, "next_offset")?;
    let total_records = parse_kv_usize(&mut lines, "total_records")?;
    let records = parse_kv_usize(&mut lines, "records")?;
    // Reject absurd counts before touching them: the leader never returns
    // more than it was asked for.
    // Frames may run past `max_records` to end on a commit-group boundary.
    let limit = max_records
        .max(1)
        .saturating_add(store::REPLICATION_GROUP_EXTENSION_MAX);
    if records > limit || records > body.len() {
        return Err(format!(
            "replication delta advertises {records} records (limit {max_records})"
        ));
    }
    if !needs_resync && from_offset.checked_add(records) != Some(next_offset) {
        return Err("replication delta next_offset does not match record count".to_string());
    }
    let mut wal_lines = Vec::with_capacity(records.min(PREALLOC_CAP));
    for _ in 0..records {
        let line = lines
            .next()
            .ok_or_else(|| "replication delta missing WAL line".to_string())?;
        wal_lines.push(line.to_string());
    }
    Ok(DeltaFrame {
        generation,
        needs_resync,
        from_offset,
        next_offset,
        total_records,
        wal_lines,
    })
}

fn parse_export_frame(body: &str) -> Result<ExportFrame, String> {
    let mut lines = body.lines().peekable();
    expect_kv(&mut lines, "status", "ok")?;
    let generation = parse_optional_generation(&mut lines)?;
    let snapshot_records = parse_kv_usize(&mut lines, "snapshot_records")?;
    let wal_records = parse_kv_usize(&mut lines, "wal_records")?;
    if snapshot_records > body.len() || wal_records > body.len() {
        return Err("replication export advertises more records than it carries".to_string());
    }
    let snapshot_marker = lines
        .next()
        .ok_or_else(|| "replication export missing SNAPSHOT marker".to_string())?;
    if snapshot_marker != "SNAPSHOT" {
        return Err("replication export has invalid SNAPSHOT marker".to_string());
    }
    let mut snapshot_lines = Vec::with_capacity(snapshot_records.min(PREALLOC_CAP));
    for _ in 0..snapshot_records {
        let line = lines
            .next()
            .ok_or_else(|| "replication export missing snapshot line".to_string())?;
        snapshot_lines.push(line.to_string());
    }
    let wal_marker = lines
        .next()
        .ok_or_else(|| "replication export missing WAL marker".to_string())?;
    if wal_marker != "WAL" {
        return Err("replication export has invalid WAL marker".to_string());
    }
    let mut wal_lines = Vec::with_capacity(wal_records.min(PREALLOC_CAP));
    for _ in 0..wal_records {
        let line = lines
            .next()
            .ok_or_else(|| "replication export missing WAL line".to_string())?;
        wal_lines.push(line.to_string());
    }
    Ok(ExportFrame {
        generation,
        export: WalReplicationExport {
            snapshot_lines,
            wal_lines,
        },
    })
}

fn parse_optional_generation<'a, I>(
    lines: &mut std::iter::Peekable<I>,
) -> Result<Option<u64>, String>
where
    I: Iterator<Item = &'a str>,
{
    match lines.peek() {
        Some(line) if line.starts_with("generation=") => {
            let line = lines.next().unwrap_or_default();
            let (_, value) = parse_kv_line(line, "generation")?;
            value
                .parse::<u64>()
                .map(Some)
                .map_err(|_| "replication payload has invalid generation".to_string())
        }
        _ => Ok(None),
    }
}

fn parse_http_url(url: &str) -> Result<(String, String), String> {
    let without_scheme = url
        .strip_prefix("http://")
        .ok_or_else(|| "replication source URL must start with http://".to_string())?;
    let (authority, path_and_query) = match without_scheme.split_once('/') {
        Some((authority, suffix)) => (authority, format!("/{}", suffix)),
        None => (without_scheme, "/".to_string()),
    };
    if authority.trim().is_empty() {
        return Err("replication source URL missing host:port authority".to_string());
    }
    Ok((authority.to_string(), path_and_query))
}

fn parse_kv_usize<'a, I>(lines: &mut I, key: &str) -> Result<usize, String>
where
    I: Iterator<Item = &'a str>,
{
    let line = lines
        .next()
        .ok_or_else(|| format!("replication payload missing '{key}'"))?;
    let (_, value) = parse_kv_line(line, key)?;
    value
        .parse::<usize>()
        .map_err(|_| format!("replication payload has invalid numeric value for '{key}'"))
}

fn parse_kv_bool01<'a, I>(lines: &mut I, key: &str) -> Result<bool, String>
where
    I: Iterator<Item = &'a str>,
{
    let line = lines
        .next()
        .ok_or_else(|| format!("replication payload missing '{key}'"))?;
    let (_, value) = parse_kv_line(line, key)?;
    match value {
        "0" => Ok(false),
        "1" => Ok(true),
        _ => Err(format!(
            "replication payload has invalid boolean value for '{key}'"
        )),
    }
}

fn expect_kv<'a, I>(lines: &mut I, key: &str, expected_value: &str) -> Result<(), String>
where
    I: Iterator<Item = &'a str>,
{
    let line = lines
        .next()
        .ok_or_else(|| format!("replication payload missing '{key}'"))?;
    let (_, value) = parse_kv_line(line, key)?;
    if value != expected_value {
        return Err(format!(
            "replication payload has invalid '{key}' value (expected '{expected_value}')"
        ));
    }
    Ok(())
}

fn parse_kv_line<'a>(line: &'a str, expected_key: &str) -> Result<(&'a str, &'a str), String> {
    let (key, value) = line
        .split_once('=')
        .ok_or_else(|| "replication payload has malformed key=value line".to_string())?;
    if key != expected_key {
        return Err(format!(
            "replication payload key mismatch (expected '{expected_key}', got '{key}')"
        ));
    }
    Ok((key, value))
}

// ---------------------------------------------------------------------
// Persisted follower state
// ---------------------------------------------------------------------

/// Parse the state file. The legacy format (a bare offset number) yields a
/// state without a generation, which the leader answers with a resync.
fn read_state(path: &str) -> Option<FollowerState> {
    let contents = std::fs::read_to_string(path).ok()?;
    parse_state(&contents)
}

fn parse_state(contents: &str) -> Option<FollowerState> {
    let trimmed = contents.trim();
    if let Ok(offset) = trimmed.parse::<usize>() {
        return Some(FollowerState {
            generation: None,
            offset,
        });
    }
    let mut generation = None;
    let mut offset = None;
    for line in trimmed.lines() {
        let (key, value) = line.split_once('=')?;
        match key.trim() {
            "generation" => {
                generation = match value.trim() {
                    "none" => None,
                    other => Some(other.parse::<u64>().ok()?),
                }
            }
            "offset" => offset = Some(value.trim().parse::<usize>().ok()?),
            _ => {}
        }
    }
    Some(FollowerState {
        generation,
        offset: offset?,
    })
}

fn render_state(state: &FollowerState) -> String {
    let generation = state
        .generation
        .map(|g| g.to_string())
        .unwrap_or_else(|| "none".to_string());
    format!("generation={generation}\noffset={}\n", state.offset)
}

/// Atomic write: temp file, fsync, rename, fsync of the directory.
fn write_state(path: &str, state: &FollowerState) -> std::io::Result<()> {
    let target = std::path::Path::new(path);
    let dir = target.parent().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "offset path has no parent directory",
        )
    })?;
    if !dir.as_os_str().is_empty() {
        std::fs::create_dir_all(dir)?;
    }
    let tmp = target.with_extension("tmp-write");
    {
        let mut file = std::fs::File::create(&tmp)?;
        file.write_all(render_state(state).as_bytes())?;
        file.sync_all()?;
    }
    std::fs::rename(&tmp, target)?;
    if !dir.as_os_str().is_empty()
        && let Ok(dir_file) = std::fs::File::open(dir)
    {
        let _ = dir_file.sync_all();
    }
    Ok(())
}

// ---------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------

fn env_with_fallback(primary: &str, fallback: &str) -> Option<String> {
    std::env::var(primary)
        .ok()
        .or_else(|| std::env::var(fallback).ok())
}

fn env_u64(primary: &str, fallback: &str) -> Option<u64> {
    env_with_fallback(primary, fallback)
        .and_then(|value| value.trim().parse::<u64>().ok())
        .filter(|value| *value > 0)
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn json_escape(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for ch in raw.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delta_frame_rejects_absurd_record_counts_without_allocating() {
        let body = "status=ok\nneeds_resync=0\nfrom_offset=0\nnext_offset=0\ntotal_records=0\nrecords=18446744073709551615\n";
        assert!(parse_delta_frame(body, 512).is_err());
        let body = "status=ok\nneeds_resync=0\nfrom_offset=0\nnext_offset=1000000\ntotal_records=0\nrecords=1000000\n";
        assert!(parse_delta_frame(body, 512).is_err());
    }

    #[test]
    fn delta_frame_requires_consistent_offsets() {
        let body = "status=ok\ngeneration=7\nneeds_resync=0\nfrom_offset=2\nnext_offset=9\ntotal_records=9\nrecords=1\nline\n";
        assert!(parse_delta_frame(body, 512).is_err());
        let ok = "status=ok\ngeneration=7\nneeds_resync=0\nfrom_offset=2\nnext_offset=3\ntotal_records=9\nrecords=1\nline\n";
        let frame = parse_delta_frame(ok, 512).expect("valid frame");
        assert_eq!(frame.generation, Some(7));
        assert_eq!(frame.wal_lines, vec!["line".to_string()]);
    }

    #[test]
    fn export_frame_parses_generation_and_rejects_inflated_counts() {
        let body =
            "status=ok\ngeneration=3\nsnapshot_records=1\nwal_records=1\nSNAPSHOT\na\nWAL\nb\n";
        let frame = parse_export_frame(body).expect("valid export");
        assert_eq!(frame.generation, Some(3));
        assert_eq!(frame.export.snapshot_lines, vec!["a".to_string()]);
        let inflated = "status=ok\nsnapshot_records=99999999999\nwal_records=0\nSNAPSHOT\nWAL\n";
        assert!(parse_export_frame(inflated).is_err());
    }

    #[test]
    fn state_file_round_trips_and_accepts_legacy_offset() {
        let state = FollowerState {
            generation: Some(42),
            offset: 17,
        };
        assert_eq!(parse_state(&render_state(&state)), Some(state));
        let none = FollowerState {
            generation: None,
            offset: 3,
        };
        assert_eq!(parse_state(&render_state(&none)), Some(none));
        assert_eq!(
            parse_state("12\n"),
            Some(FollowerState {
                generation: None,
                offset: 12
            })
        );
        assert_eq!(parse_state("garbage"), None);
    }

    #[test]
    fn http_response_enforces_declared_and_actual_size() {
        let ok = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nhi";
        assert_eq!(parse_http_response(ok, 10).expect("ok").body, "hi");
        let too_big = b"HTTP/1.1 200 OK\r\nContent-Length: 99999\r\n\r\nhi";
        assert!(parse_http_response(too_big, 10).is_err());
        let truncated = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhi";
        assert!(parse_http_response(truncated, 10).is_err());
    }

    #[test]
    fn backoff_grows_and_caps() {
        let mut config = ReplicationFollowerConfig::new("http://127.0.0.1:1");
        config.poll_interval = Duration::from_millis(100);
        config.max_backoff = Duration::from_millis(1000);
        assert_eq!(config.backoff_delay(0), Duration::from_millis(100));
        assert_eq!(config.backoff_delay(1), Duration::from_millis(100));
        assert_eq!(config.backoff_delay(3), Duration::from_millis(400));
        assert_eq!(config.backoff_delay(50), Duration::from_millis(1000));
    }
}
