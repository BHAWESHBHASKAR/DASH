use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// How long a caller waits for the cross-process lease file lock before
/// giving up with an error (rather than blocking forever on a wedged peer).
const FILE_LOCK_TIMEOUT: Duration = Duration::from_secs(5);

/// Largest disagreement between the wall clock and this process's monotonic
/// elapsed time that is treated as ordinary slew and followed.
const CLOCK_RESYNC_BOUND_MS: u64 = 5_000;

/// How long a larger disagreement must persist before it is adopted as a
/// real clock correction rather than ignored as a transient spike.
const CLOCK_STEP_PERSIST: Duration = Duration::from_secs(60);

/// Clock used for lease arithmetic: wall-clock based (the lease is shared
/// between hosts) but never backwards, and not latched by transient spikes.
///
/// Readings follow the wall clock while it agrees with the monotonic clock
/// to within [`CLOCK_RESYNC_BOUND_MS`]. A bigger jump is ignored (time keeps
/// advancing with the monotonic clock) until it has persisted for
/// [`CLOCK_STEP_PERSIST`], at which point it is adopted. A backwards step is
/// never followed (readings hold or advance monotonically), so after a
/// permanent backwards correction readings run ahead of the wall clock.
#[derive(Debug, Default)]
struct LeaseClock {
    last_ms: u64,
    base_wall_ms: u64,
    base_mono: Option<Instant>,
    out_of_bound_since: Option<Instant>,
}

impl LeaseClock {
    fn observe(&mut self, wall_ms: u64, mono: Instant) -> u64 {
        let Some(base_mono) = self.base_mono else {
            self.base_wall_ms = wall_ms;
            self.base_mono = Some(mono);
            self.last_ms = wall_ms;
            return wall_ms;
        };
        let elapsed = mono.saturating_duration_since(base_mono).as_millis() as u64;
        let computed = self.base_wall_ms.saturating_add(elapsed);
        let adopt = if wall_ms.abs_diff(computed) <= CLOCK_RESYNC_BOUND_MS {
            self.out_of_bound_since = None;
            true
        } else {
            let since = *self.out_of_bound_since.get_or_insert(mono);
            mono.saturating_duration_since(since) >= CLOCK_STEP_PERSIST
        };
        let candidate = if adopt { wall_ms } else { computed };
        let reading = candidate.max(self.last_ms);
        if adopt {
            self.base_wall_ms = reading;
            self.base_mono = Some(mono);
            self.out_of_bound_since = None;
        }
        self.last_ms = reading;
        reading
    }
}

/// On-disk lease record used for simple leader election across
/// control-plane replicas. The process whose `node_id` matches the
/// non-expired lease is the leader.
///
/// `epoch` is the **fencing token**: it strictly increases every time the
/// lease is acquired by a new holder (or re-acquired after lapsing), and is
/// unrelated to the placement epochs. Downstream systems should reject
/// writes that carry a token lower than the highest they have seen.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseRecord {
    pub node_id: String,
    pub epoch: u64,
    pub expires_at_ms: u64,
}

impl LeaseRecord {
    pub fn is_expired_at(&self, now_ms: u64) -> bool {
        self.expires_at_ms <= now_ms
    }
}

/// Result of a successful acquisition attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Acquisition {
    pub record: LeaseRecord,
    /// `true` when leadership was newly obtained (new fencing token) as
    /// opposed to an in-place extension of a lease this node already held.
    /// Callers must reload persisted state when this is set.
    pub newly_acquired: bool,
}

/// File-backed leader lease. Not a consensus protocol, but good enough
/// for the single-shared-volume deployments DASH targets in this phase.
///
/// Cross-process safety: every read-check-write sequence runs while holding
/// an exclusive advisory lock (`flock`) on `<lease>.lock`, and the lease file
/// itself is replaced via temp file + fsync + rename + directory fsync.
///
/// Clock assumption: expiry is compared on the wall clock because the lease
/// is shared between processes (and hosts) where monotonic clocks are not
/// comparable. All nodes sharing the volume must keep their clocks within
/// `safety_margin_ms` of each other (NTP/chrony). The margin works in both
/// directions: a holder stops acting as leader `margin` before the recorded
/// expiry, and a challenger waits `margin` after it. Within one process the
/// clock reading never goes backwards.
#[derive(Debug)]
pub struct LeaderLease {
    node_id: String,
    lease_path: PathBuf,
    lease_duration_ms: u64,
    renewal_interval_ms: u64,
    safety_margin_ms: u64,
    // Serializes in-process callers; cross-process safety comes from the
    // advisory file lock taken inside this guard.
    lock: Mutex<()>,
    clock: Mutex<LeaseClock>,
}

impl LeaderLease {
    pub fn new(
        node_id: impl Into<String>,
        lease_path: impl Into<PathBuf>,
        lease_duration_ms: u64,
        renewal_interval_ms: u64,
    ) -> Self {
        Self {
            node_id: node_id.into(),
            lease_path: lease_path.into(),
            lease_duration_ms,
            renewal_interval_ms,
            safety_margin_ms: 0,
            lock: Mutex::new(()),
            clock: Mutex::new(LeaseClock::default()),
        }
    }

    /// Create a lease with defaults suitable for a container deployment
    /// when only the lease path is configured explicitly.
    pub fn with_defaults(node_id: impl Into<String>, lease_path: impl Into<PathBuf>) -> Self {
        Self::new(
            node_id, lease_path, // 30-second lease, 10-second renewal interval.
            30_000, 10_000,
        )
        .with_safety_margin_ms(1_000)
    }

    /// Configure the clock-skew safety margin (clamped to half the lease
    /// duration so a lease can never be shorter than its own margin).
    pub fn with_safety_margin_ms(mut self, margin_ms: u64) -> Self {
        self.safety_margin_ms = margin_ms.min(self.lease_duration_ms / 2);
        self
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    pub fn lease_duration_ms(&self) -> u64 {
        self.lease_duration_ms
    }

    pub fn renewal_interval_ms(&self) -> u64 {
        self.renewal_interval_ms
    }

    pub fn safety_margin_ms(&self) -> u64 {
        self.safety_margin_ms
    }

    /// Attempt to become leader.
    ///
    /// The `_placement_epoch` argument is accepted for call-site
    /// compatibility but deliberately ignored: the lease epoch (fencing
    /// token) is derived solely from the previous lease record.
    pub fn try_acquire(&self, _placement_epoch: u64) -> Result<bool, String> {
        Ok(self.acquire()?.is_some())
    }

    /// Attempt to become leader, returning the acquired record.
    pub fn acquire(&self) -> Result<Option<Acquisition>, String> {
        self.validate_node_id()?;
        let _guard = self.guard()?;
        let _file_lock = self.lock_file()?;
        let now = self.now_ms()?;
        self.try_acquire_locked(now)
    }

    /// Acquisition logic. Must only be called while holding both the
    /// in-process guard and the cross-process file lock (this is what
    /// `renew` previously got wrong by re-entering the non-reentrant mutex).
    fn try_acquire_locked(&self, now: u64) -> Result<Option<Acquisition>, String> {
        let existing = read_lease(&self.lease_path)?;
        // Highest fencing token ever issued from this lease path. It survives
        // deletion or truncation of the lease file, so tokens never go back.
        let floor = read_epoch_floor(&self.epoch_floor_path())?;
        let (epoch, newly_acquired) = match &existing {
            None => (next_epoch(floor)?, true),
            Some(record) if record.node_id == self.node_id => {
                if record.epoch >= floor
                    && now < record.expires_at_ms.saturating_add(self.safety_margin_ms)
                {
                    // Still ours and nobody may take it yet: extend in place.
                    (record.epoch, false)
                } else {
                    // We let it lapse; treat as a fresh acquisition.
                    (next_epoch(record.epoch.max(floor))?, true)
                }
            }
            Some(record) => {
                if now < record.expires_at_ms.saturating_add(self.safety_margin_ms) {
                    return Ok(None);
                }
                (next_epoch(record.epoch.max(floor))?, true)
            }
        };
        let record = LeaseRecord {
            node_id: self.node_id.clone(),
            epoch,
            expires_at_ms: now.saturating_add(self.lease_duration_ms),
        };
        // Floor first: a crash between the two writes can only leave the
        // floor ahead of the lease, never behind it.
        if epoch > floor {
            write_file_atomic(&self.epoch_floor_path(), &format!("{epoch}\n"))?;
        }
        write_lease(&self.lease_path, &record)?;
        Ok(Some(Acquisition {
            record,
            newly_acquired,
        }))
    }

    /// Renew the lease if this process is still the leader.
    ///
    /// Returns `true` when this node is still the leader after renewal.
    /// `_placement_epoch` is ignored (see [`try_acquire`](Self::try_acquire)).
    pub fn renew(&self, _placement_epoch: u64) -> Result<bool, String> {
        Ok(self.renew_detailed()?.is_some())
    }

    /// Like [`renew`](Self::renew) but returns the resulting record. When the
    /// lease had lapsed and was re-acquired, `newly_acquired` is set.
    pub fn renew_detailed(&self) -> Result<Option<Acquisition>, String> {
        self.validate_node_id()?;
        let _guard = self.guard()?;
        let _file_lock = self.lock_file()?;
        let now = self.now_ms()?;
        match read_lease(&self.lease_path)? {
            Some(record) if record.node_id == self.node_id => {
                // Both the extend and the lapsed-re-acquire cases are handled
                // by the locked helper; no second lock is taken.
                self.try_acquire_locked(now)
            }
            _ => Ok(None),
        }
    }

    /// Check whether this process currently holds a non-expired lease. A
    /// holder stops reporting leadership `safety_margin_ms` before expiry.
    pub fn is_leader(&self) -> Result<bool, String> {
        let now = self.now_ms()?;
        match read_lease(&self.lease_path)? {
            Some(record) => Ok(record.node_id == self.node_id
                && now.saturating_add(self.safety_margin_ms) < record.expires_at_ms),
            None => Ok(false),
        }
    }

    /// The fencing token of the lease this node currently holds, if any.
    pub fn fencing_token(&self) -> Result<Option<u64>, String> {
        let now = self.now_ms()?;
        match read_lease(&self.lease_path)? {
            Some(record)
                if record.node_id == self.node_id
                    && now.saturating_add(self.safety_margin_ms) < record.expires_at_ms =>
            {
                Ok(Some(record.epoch))
            }
            _ => Ok(None),
        }
    }

    /// Return the current leader information, or `None` if there is no
    /// valid lease.
    pub fn current_leader(&self) -> Result<Option<LeaseRecord>, String> {
        let now = self.now_ms()?;
        match read_lease(&self.lease_path)? {
            Some(record) if !record.is_expired_at(now) => Ok(Some(record)),
            _ => Ok(None),
        }
    }

    fn validate_node_id(&self) -> Result<(), String> {
        if self.node_id.is_empty() {
            return Err("leader lease node_id must not be empty".to_string());
        }
        if self.node_id.contains([',', '\n', '\r']) {
            return Err("leader lease node_id must not contain ',' or newlines".to_string());
        }
        Ok(())
    }

    fn guard(&self) -> Result<std::sync::MutexGuard<'_, ()>, String> {
        self.lock
            .lock()
            .map_err(|_| "leader lease lock poisoned by a panicked thread".to_string())
    }

    fn lock_file(&self) -> Result<FileLockGuard, String> {
        FileLockGuard::acquire(&lock_path_for(&self.lease_path))
    }

    /// Wall-clock milliseconds, never decreasing within this process and not
    /// latched by transient forward spikes (see [`LeaseClock`]).
    fn now_ms(&self) -> Result<u64, String> {
        let wall = ms_since_epoch(SystemTime::now())?;
        Ok(self.now_ms_from(wall, Instant::now()))
    }

    /// [`now_ms`](Self::now_ms) with the clock readings injected.
    fn now_ms_from(&self, wall_ms: u64, mono: Instant) -> u64 {
        self.clock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .observe(wall_ms, mono)
    }

    fn epoch_floor_path(&self) -> PathBuf {
        let mut name = self.lease_path.clone().into_os_string();
        name.push(".epoch");
        PathBuf::from(name)
    }
}

/// Exponential backoff used by the maintenance loop when a follower keeps
/// failing to acquire.
#[derive(Debug, Clone)]
pub struct Backoff {
    base: Duration,
    max: Duration,
    current: Duration,
}

impl Backoff {
    pub fn new(base: Duration, max: Duration) -> Self {
        Self {
            base,
            max: max.max(base),
            current: base,
        }
    }

    /// Delay to wait before the next attempt; doubles up to the maximum.
    pub fn next_delay(&mut self) -> Duration {
        let delay = self.current;
        self.current = (self.current.saturating_mul(2)).min(self.max);
        delay
    }

    pub fn reset(&mut self) {
        self.current = self.base;
    }
}

fn next_epoch(previous: u64) -> Result<u64, String> {
    previous
        .checked_add(1)
        .ok_or_else(|| "leader lease epoch overflow".to_string())
}

fn ms_since_epoch(time: SystemTime) -> Result<u64, String> {
    time.duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .map_err(|_| "system clock is set before the UNIX epoch".to_string())
}

fn lock_path_for(lease_path: &Path) -> PathBuf {
    let mut name = lease_path
        .file_name()
        .map(|name| name.to_os_string())
        .unwrap_or_else(|| "lease".into());
    name.push(".lock");
    lease_path.with_file_name(name)
}

/// Exclusive advisory lock on a sidecar file, released on drop.
struct FileLockGuard {
    file: File,
}

impl FileLockGuard {
    fn acquire(path: &Path) -> Result<Self, String> {
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            fs::create_dir_all(parent).map_err(|err| {
                format!(
                    "failed creating lease parent dir '{}': {err}",
                    parent.display()
                )
            })?;
        }
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(path)
            .map_err(|err| format!("failed opening lease lock '{}': {err}", path.display()))?;
        let deadline = Instant::now() + FILE_LOCK_TIMEOUT;
        loop {
            match file.try_lock() {
                Ok(()) => return Ok(Self { file }),
                Err(TryLockError::WouldBlock) => {
                    if Instant::now() >= deadline {
                        return Err(format!(
                            "timed out waiting for lease lock '{}'",
                            path.display()
                        ));
                    }
                    std::thread::sleep(Duration::from_millis(2));
                }
                Err(TryLockError::Error(err)) => {
                    return Err(format!(
                        "failed locking lease lock '{}': {err}",
                        path.display()
                    ));
                }
            }
        }
    }
}

impl Drop for FileLockGuard {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

fn read_lease(path: &Path) -> Result<Option<LeaseRecord>, String> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(format!(
                "failed reading lease file '{}': {err}",
                path.display()
            ));
        }
    };
    if bytes.is_empty() {
        return Ok(None);
    }
    // The lease file is a single line: node_id,epoch,expires_at_ms
    let text = String::from_utf8(bytes)
        .map_err(|_| format!("lease file '{}' is not valid UTF-8", path.display()))?;
    let line = text.lines().next().unwrap_or("").trim();
    if line.is_empty() {
        return Ok(None);
    }
    let parts: Vec<&str> = line.split(',').collect();
    if parts.len() != 3 {
        return Err(format!(
            "lease file '{}' has invalid format (expected node_id,epoch,expires_at_ms)",
            path.display()
        ));
    }
    let node_id = parts[0].to_string();
    if node_id.is_empty() {
        return Err(format!("lease file '{}' has empty node_id", path.display()));
    }
    let epoch = parts[1]
        .parse::<u64>()
        .map_err(|_| format!("lease file '{}' has invalid epoch", path.display()))?;
    let expires_at_ms = parts[2]
        .parse::<u64>()
        .map_err(|_| format!("lease file '{}' has invalid expires_at_ms", path.display()))?;
    Ok(Some(LeaseRecord {
        node_id,
        epoch,
        expires_at_ms,
    }))
}

static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Durably replace the lease: write a temp file, fsync it, rename over the
/// lease, then fsync the directory so the rename survives a crash.
fn write_lease(path: &Path, record: &LeaseRecord) -> Result<(), String> {
    write_file_atomic(
        path,
        &format!(
            "{},{},{}\n",
            record.node_id, record.epoch, record.expires_at_ms
        ),
    )
}

/// Reads the epoch floor sidecar (`0` when it does not exist yet).
fn read_epoch_floor(path: &Path) -> Result<u64, String> {
    match fs::read_to_string(path) {
        Ok(text) if text.trim().is_empty() => Ok(0),
        Ok(text) => text.trim().parse::<u64>().map_err(|_| {
            format!(
                "lease epoch floor '{}' is not a valid number",
                path.display()
            )
        }),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(err) => Err(format!(
            "failed reading lease epoch floor '{}': {err}",
            path.display()
        )),
    }
}

fn write_file_atomic(path: &Path, contents: &str) -> Result<(), String> {
    let parent = match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.to_path_buf(),
        _ => PathBuf::from("."),
    };
    fs::create_dir_all(&parent).map_err(|err| {
        format!(
            "failed creating lease parent dir '{}': {err}",
            parent.display()
        )
    })?;
    let line = contents;
    let tmp_path = path.with_extension(format!(
        "lease-tmp-{}-{}",
        std::process::id(),
        TMP_COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    let write_result = (|| -> Result<(), String> {
        let mut file = File::create(&tmp_path).map_err(|err| {
            format!(
                "failed creating lease temp file '{}': {err}",
                tmp_path.display()
            )
        })?;
        file.write_all(line.as_bytes()).map_err(|err| {
            format!(
                "failed writing lease temp file '{}': {err}",
                tmp_path.display()
            )
        })?;
        file.sync_all().map_err(|err| {
            format!(
                "failed syncing lease temp file '{}': {err}",
                tmp_path.display()
            )
        })
    })();
    if let Err(err) = write_result {
        let _ = fs::remove_file(&tmp_path);
        return Err(err);
    }
    if let Err(err) = fs::rename(&tmp_path, path) {
        let _ = fs::remove_file(&tmp_path);
        return Err(format!(
            "failed renaming lease temp file to '{}': {err}",
            path.display()
        ));
    }
    File::open(&parent)
        .and_then(|dir| dir.sync_all())
        .map_err(|err| {
            format!(
                "failed syncing lease directory '{}': {err}",
                parent.display()
            )
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;
    use std::sync::{Arc, Barrier};

    #[test]
    fn leader_lease_acquire_and_renew() {
        let dir = temp_dir("lease-acquire");
        let path = dir.join("lease.txt");
        let lease = LeaderLease::new("node-a", path, 1_000, 100);

        // Initial acquisition succeeds.
        assert!(lease.try_acquire(1).unwrap());
        assert!(lease.is_leader().unwrap());

        // A second process with the same file cannot steal while fresh.
        let other = LeaderLease::new("node-b", lease.lease_path.clone(), 1_000, 100);
        assert!(!other.try_acquire(2).unwrap());

        // Renewal keeps leadership.
        assert!(lease.renew(2).unwrap());
        assert!(lease.is_leader().unwrap());
    }

    #[test]
    fn leader_lease_transfers_after_expiry() {
        let dir = temp_dir("lease-expiry");
        let path = dir.join("lease.txt");
        let lease_a = LeaderLease::new("node-a", &path, 50, 10);
        assert!(lease_a.try_acquire(1).unwrap());

        // Wait for the lease to expire.
        std::thread::sleep(Duration::from_millis(60));
        let lease_b = LeaderLease::new("node-b", &path, 1_000, 100);
        assert!(lease_b.try_acquire(2).unwrap());
        assert!(!lease_a.is_leader().unwrap());
        assert!(lease_b.is_leader().unwrap());
    }

    #[test]
    fn leader_lease_rejects_empty_node_id() {
        let dir = temp_dir("lease-empty");
        let path = dir.join("lease.txt");
        let result = LeaderLease::new("", &path, 1_000, 100).try_acquire(1);
        assert!(result.is_err());
    }

    #[test]
    fn renew_after_lapse_returns_instead_of_deadlocking() {
        let dir = temp_dir("lease-renew-lapsed");
        let path = dir.join("lease.txt");
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let lease = LeaderLease::new("node-a", &path, 300, 10);
            assert!(lease.try_acquire(1).unwrap());
            std::thread::sleep(Duration::from_millis(350));
            // Previously: renew() held self.lock and re-entered try_acquire(),
            // which locked the same std Mutex again and hung forever.
            let renewed = lease.renew(1);
            let _ = tx.send((renewed, lease.fencing_token()));
        });
        let (renewed, token) = rx
            .recv_timeout(Duration::from_secs(5))
            .expect("renew() must not deadlock after the lease lapsed");
        assert!(renewed.unwrap());
        // Re-acquiring a lapsed lease mints a new fencing token.
        assert_eq!(token.unwrap(), Some(2));
    }

    #[test]
    fn epoch_strictly_increases_across_holders_and_ignores_placement_epoch() {
        let dir = temp_dir("lease-epoch");
        let path = dir.join("lease.txt");
        let a = LeaderLease::new("node-a", &path, 300, 10);
        let b = LeaderLease::new("node-b", &path, 300, 10);

        // A huge placement epoch must not leak into the fencing token.
        assert!(a.try_acquire(9_999).unwrap());
        assert_eq!(a.fencing_token().unwrap(), Some(1));

        std::thread::sleep(Duration::from_millis(350));
        assert!(b.try_acquire(0).unwrap());
        assert_eq!(b.fencing_token().unwrap(), Some(2));

        std::thread::sleep(Duration::from_millis(350));
        assert!(a.try_acquire(0).unwrap());
        assert_eq!(a.fencing_token().unwrap(), Some(3));
    }

    #[test]
    fn renewal_by_current_holder_keeps_epoch() {
        let dir = temp_dir("lease-keep-epoch");
        let lease = LeaderLease::new("node-a", dir.join("lease.txt"), 5_000, 100);
        let first = lease.acquire().unwrap().unwrap();
        assert!(first.newly_acquired);
        let second = lease.renew_detailed().unwrap().unwrap();
        assert!(!second.newly_acquired);
        assert_eq!(first.record.epoch, second.record.epoch);
        assert!(second.record.expires_at_ms >= first.record.expires_at_ms);
    }

    #[test]
    fn concurrent_acquisition_has_exactly_one_winner() {
        for round in 0..20 {
            let dir = temp_dir(&format!("lease-race-{round}"));
            let path = dir.join("lease.txt");
            let contenders = 12;
            let barrier = Arc::new(Barrier::new(contenders));
            let handles: Vec<_> = (0..contenders)
                .map(|i| {
                    let barrier = Arc::clone(&barrier);
                    let path = path.clone();
                    std::thread::spawn(move || {
                        // Separate instances model separate processes: they
                        // share nothing but the lease file.
                        let lease = LeaderLease::new(format!("node-{i}"), path, 10_000, 1_000);
                        barrier.wait();
                        lease.try_acquire(0).unwrap()
                    })
                })
                .collect();
            let winners = handles
                .into_iter()
                .map(|handle| handle.join().unwrap())
                .filter(|won| *won)
                .count();
            assert_eq!(winners, 1, "round {round}: expected a single leader");
            assert_eq!(read_lease(&path).unwrap().unwrap().epoch, 1);
        }
    }

    #[test]
    fn safety_margin_delays_takeover_and_early_stops_holder() {
        let dir = temp_dir("lease-margin");
        let path = dir.join("lease.txt");
        let a = LeaderLease::new("node-a", &path, 400, 50).with_safety_margin_ms(150);
        let b = LeaderLease::new("node-b", &path, 400, 50).with_safety_margin_ms(150);
        assert!(a.try_acquire(0).unwrap());
        assert!(a.is_leader().unwrap());

        // Past expiry - margin: the holder already considers itself deposed...
        std::thread::sleep(Duration::from_millis(300));
        assert!(!a.is_leader().unwrap());
        // ...but the challenger must still wait out expiry + margin.
        std::thread::sleep(Duration::from_millis(150));
        assert!(!b.try_acquire(0).unwrap(), "takeover inside the margin");
        std::thread::sleep(Duration::from_millis(150));
        assert!(b.try_acquire(0).unwrap());
    }

    #[test]
    fn corrupt_lease_file_is_an_error_not_a_silent_reset() {
        let dir = temp_dir("lease-corrupt");
        let path = dir.join("lease.txt");
        fs::write(&path, "garbage").unwrap();
        let err = LeaderLease::new("node-a", &path, 1_000, 100)
            .try_acquire(1)
            .expect_err("corrupt lease must not reset the fencing token");
        assert!(err.contains("invalid format"));
    }

    #[test]
    fn poisoned_lock_returns_error_instead_of_panicking() {
        let dir = temp_dir("lease-poison");
        let lease = Arc::new(LeaderLease::new(
            "node-a",
            dir.join("lease.txt"),
            1_000,
            100,
        ));
        let poisoner = Arc::clone(&lease);
        let _ = std::thread::spawn(move || {
            let _guard = poisoner.lock.lock().unwrap();
            panic!("poison the lease lock");
        })
        .join();
        let err = lease.try_acquire(1).expect_err("must be a graceful error");
        assert!(err.contains("poisoned"));
        assert!(lease.renew(1).is_err());
    }

    #[test]
    fn clock_before_unix_epoch_is_an_error() {
        let before_epoch = UNIX_EPOCH - Duration::from_secs(5);
        let err = ms_since_epoch(before_epoch).expect_err("pre-epoch clock must not panic");
        assert!(err.contains("before the UNIX epoch"));
    }

    #[test]
    fn forward_clock_spike_is_not_latched() {
        let dir = temp_dir("lease-spike");
        let lease = LeaderLease::new("node-a", dir.join("lease.txt"), 1_000, 100);
        let t0 = Instant::now();
        let wall0 = 1_700_000_000_000u64;
        assert_eq!(lease.now_ms_from(wall0, t0), wall0);
        // A wall-clock spike of ten hours is ignored: time keeps advancing
        // with the monotonic clock only.
        let spiked = lease.now_ms_from(wall0 + 36_000_000, t0 + Duration::from_secs(1));
        assert_eq!(spiked, wall0 + 1_000);
        // Once the wall clock is sane again the reading follows it, not the
        // spike (the old latch would still report wall0 + 10h here).
        let back = lease.now_ms_from(wall0 + 2_000, t0 + Duration::from_secs(2));
        assert_eq!(back, wall0 + 2_000);
    }

    #[test]
    fn persistent_clock_step_is_adopted_and_never_goes_backwards() {
        let dir = temp_dir("lease-step");
        let lease = LeaderLease::new("node-a", dir.join("lease.txt"), 1_000, 100);
        let t0 = Instant::now();
        let wall0 = 1_700_000_000_000u64;
        lease.now_ms_from(wall0, t0);
        let step = 3_600_000u64; // the clock was corrected forward an hour
        let mut last = wall0;
        let mut adopted_at = None;
        for secs in 1..=120u64 {
            let value =
                lease.now_ms_from(wall0 + step + secs * 1_000, t0 + Duration::from_secs(secs));
            assert!(value >= last, "went backwards at {secs}s");
            last = value;
            if value >= wall0 + step && adopted_at.is_none() {
                adopted_at = Some(secs);
            }
        }
        assert!(adopted_at.is_some(), "a persistent step must be adopted");
        // A small backwards wobble never moves the reading backwards.
        let wobble = lease.now_ms_from(last - 500, t0 + Duration::from_secs(121));
        assert!(wobble >= last);
    }

    #[test]
    fn epoch_never_restarts_after_the_lease_file_is_deleted_or_emptied() {
        let dir = temp_dir("lease-epoch-floor");
        let path = dir.join("lease.txt");
        let a = LeaderLease::new("node-a", &path, 300, 10);
        assert!(a.try_acquire(0).unwrap());
        assert_eq!(a.fencing_token().unwrap(), Some(1));
        std::thread::sleep(Duration::from_millis(350));
        let b = LeaderLease::new("node-b", &path, 300, 10);
        assert!(b.try_acquire(0).unwrap());
        assert_eq!(b.fencing_token().unwrap(), Some(2));

        fs::remove_file(&path).unwrap();
        let c = LeaderLease::new("node-c", &path, 300, 10);
        let acquired = c.acquire().unwrap().unwrap();
        assert!(
            acquired.record.epoch > 2,
            "fencing token went backwards to {}",
            acquired.record.epoch
        );

        std::thread::sleep(Duration::from_millis(350));
        fs::write(&path, "").unwrap();
        let d = LeaderLease::new("node-d", &path, 300, 10);
        let again = d.acquire().unwrap().unwrap();
        assert!(again.record.epoch > acquired.record.epoch);
        assert!(dir.join("lease.txt.epoch").exists());
    }

    #[test]
    fn write_leaves_no_temp_files_and_round_trips() {
        let dir = temp_dir("lease-tmp");
        let path = dir.join("lease.txt");
        let lease = LeaderLease::new("node-a", &path, 1_000, 100);
        assert!(lease.try_acquire(0).unwrap());
        assert!(lease.renew(0).unwrap());
        let names: Vec<String> = fs::read_dir(&dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().to_string())
            .collect();
        assert!(
            names.iter().all(|name| !name.contains("lease-tmp")),
            "{names:?}"
        );
        assert_eq!(read_lease(&path).unwrap().unwrap().node_id, "node-a");
    }

    #[test]
    fn backoff_doubles_then_caps_and_resets() {
        let mut backoff = Backoff::new(Duration::from_millis(100), Duration::from_millis(350));
        assert_eq!(backoff.next_delay(), Duration::from_millis(100));
        assert_eq!(backoff.next_delay(), Duration::from_millis(200));
        assert_eq!(backoff.next_delay(), Duration::from_millis(350));
        assert_eq!(backoff.next_delay(), Duration::from_millis(350));
        backoff.reset();
        assert_eq!(backoff.next_delay(), Duration::from_millis(100));
    }

    fn temp_dir(prefix: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("dash-{prefix}-{nanos}"));
        fs::create_dir_all(&dir).unwrap();
        dir
    }
}
