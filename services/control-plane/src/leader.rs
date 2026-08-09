use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

/// On-disk lease record used for simple leader election across
/// control-plane replicas. The process whose `node_id` matches the
/// non-expired lease is the leader.
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

/// File-backed leader lease. Not a consensus protocol, but good enough
/// for the single-shared-volume deployments DASH targets in this phase.
///
/// Leases are written atomically (temp file + rename) so concurrent
/// acquisitions from different control-plane processes do not corrupt
/// the lease file, although the last atomic rename wins.
#[derive(Debug)]
pub struct LeaderLease {
    node_id: String,
    lease_path: PathBuf,
    lease_duration_ms: u64,
    renewal_interval_ms: u64,
    // Serializes in-process renewals; cross-process safety comes from
    // the atomic write and the freshness check on every read.
    lock: Mutex<()>,
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
            lock: Mutex::new(()),
        }
    }

    /// Create a lease with defaults suitable for a container deployment
    /// when only the lease path is configured explicitly.
    pub fn with_defaults(node_id: impl Into<String>, lease_path: impl Into<PathBuf>) -> Self {
        Self::new(
            node_id, lease_path, // 30-second lease, 10-second renewal interval.
            30_000, 10_000,
        )
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

    /// Attempt to become leader by writing a fresh lease.
    ///
    /// `epoch` is typically the highest placement epoch the caller knows
    /// so the lease embeds a monotonically increasing value.
    pub fn try_acquire(&self, epoch: u64) -> Result<bool, String> {
        if self.node_id.is_empty() {
            return Err("leader lease node_id must not be empty".to_string());
        }
        let now = now_ms();
        let _guard = self.lock.lock().unwrap();
        let can_acquire = match read_lease(&self.lease_path) {
            Ok(Some(record)) => record.is_expired_at(now) || record.node_id == self.node_id,
            Ok(None) => true,
            Err(_) => true,
        };
        if !can_acquire {
            return Ok(false);
        }
        write_lease(
            &self.lease_path,
            &LeaseRecord {
                node_id: self.node_id.clone(),
                epoch,
                expires_at_ms: now + self.lease_duration_ms,
            },
        )?;
        Ok(true)
    }

    /// Renew the lease if this process is still the leader.
    ///
    /// Returns `true` when this node is still the leader after renewal.
    pub fn renew(&self, epoch: u64) -> Result<bool, String> {
        if self.node_id.is_empty() {
            return Err("leader lease node_id must not be empty".to_string());
        }
        let _guard = self.lock.lock().unwrap();
        let now = now_ms();
        match read_lease(&self.lease_path)? {
            Some(record) if record.node_id == self.node_id => {
                if record.is_expired_at(now) {
                    // We let it lapse; re-acquire.
                    return self.try_acquire(epoch);
                }
                write_lease(
                    &self.lease_path,
                    &LeaseRecord {
                        node_id: self.node_id.clone(),
                        epoch,
                        expires_at_ms: now + self.lease_duration_ms,
                    },
                )?;
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    /// Check whether this process currently holds a non-expired lease.
    pub fn is_leader(&self) -> Result<bool, String> {
        let now = now_ms();
        match read_lease(&self.lease_path)? {
            Some(record) => Ok(!record.is_expired_at(now) && record.node_id == self.node_id),
            None => Ok(false),
        }
    }

    /// Return the current leader information, or `None` if there is no
    /// valid lease.
    pub fn current_leader(&self) -> Result<Option<LeaseRecord>, String> {
        let now = now_ms();
        match read_lease(&self.lease_path)? {
            Some(record) if !record.is_expired_at(now) => Ok(Some(record)),
            _ => Ok(None),
        }
    }

    /// Run a background renewal loop. This blocks the calling thread and
    /// only returns if renewal fails repeatedly.
    pub fn run_renewal_loop(&self, get_epoch: impl Fn() -> u64) -> Result<(), String> {
        let interval = std::time::Duration::from_millis(self.renewal_interval_ms);
        loop {
            std::thread::sleep(interval);
            if !self.renew(get_epoch())? {
                // If we are no longer leader, stop renewing. The caller
                // can decide to re-acquire or exit.
                return Err("leader lease lost during renewal".to_string());
            }
        }
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after epoch")
        .as_millis() as u64
}

fn read_lease(path: &Path) -> Result<Option<LeaseRecord>, String> {
    if !path.exists() {
        return Ok(None);
    }
    let bytes = fs::read(path)
        .map_err(|err| format!("failed reading lease file '{}': {err}", path.display()))?;
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

fn write_lease(path: &Path, record: &LeaseRecord) -> Result<(), String> {
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
    let line = format!(
        "{},{},{}",
        record.node_id, record.epoch, record.expires_at_ms
    );
    let tmp_path = path.with_extension(format!(
        "lease-tmp-{}-{}",
        std::process::id(),
        std::thread::current().name().unwrap_or("leader")
    ));
    {
        let mut file = fs::File::create(&tmp_path).map_err(|err| {
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
        file.write_all(b"\n").map_err(|err| {
            format!(
                "failed writing lease temp file '{}': {err}",
                tmp_path.display()
            )
        })?;
    }
    fs::rename(&tmp_path, path).map_err(|err| {
        format!(
            "failed renaming lease temp file to '{}': {err}",
            path.display()
        )
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

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
        std::thread::sleep(std::time::Duration::from_millis(60));
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
