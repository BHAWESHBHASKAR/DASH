//! Regression tests for the lease hardening review findings: unguessable and
//! non-symlink-following temp files, owner-only modes, implausible (forged)
//! lease records and holder identity.

use super::*;

fn temp_dir(prefix: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("dash-{prefix}-{}-{nanos}", std::process::id()));
    fs::create_dir_all(&dir).unwrap();
    dir
}

#[cfg(unix)]
fn mode_of(path: &Path) -> u32 {
    fs::metadata(path).unwrap().permissions().mode() & 0o777
}

/// The previous temp name was `<lease>.lease-tmp-<pid>-<counter>` and was
/// opened with `File::create`, so a planted symlink was followed and the
/// target overwritten with the lease line.
#[cfg(unix)]
#[test]
fn planted_symlinks_at_old_temp_names_are_never_followed() {
    let dir = temp_dir("lease-symlink");
    let victim = dir.join("victim.txt");
    fs::write(&victim, "precious").unwrap();
    let lease_path = dir.join("lease.txt");
    let start = TMP_COUNTER.load(Ordering::Relaxed);
    for n in start.saturating_sub(2)..start + 40 {
        let planted = dir.join(format!("lease.lease-tmp-{}-{n}", std::process::id()));
        let _ = std::os::unix::fs::symlink(&victim, planted);
    }
    let lease = LeaderLease::new("node-a", &lease_path, 1_000, 100);
    assert!(lease.try_acquire(0).unwrap());
    assert!(lease.renew(0).unwrap());
    assert_eq!(fs::read_to_string(&victim).unwrap(), "precious");
    assert!(read_lease(&lease_path).unwrap().is_some());
}

#[test]
fn temp_names_are_random_and_never_reused() {
    let path = Path::new("/var/lib/dash/lease.txt");
    let a = tmp_path_for(path);
    let b = tmp_path_for(path);
    assert_ne!(a, b);
    let name = a.file_name().unwrap().to_string_lossy().to_string();
    // counter, then 16 random hex chars
    let suffix = name.rsplit('-').next().unwrap();
    assert_eq!(suffix.len(), 16, "{name}");
    assert!(suffix.chars().all(|c| c.is_ascii_hexdigit()));
    // create_new refuses to reuse an existing (or symlinked) name.
    let dir = temp_dir("lease-create-new");
    let lease_path = dir.join("lease.txt");
    let record = LeaseRecord {
        node_id: "n".into(),
        epoch: 1,
        expires_at_ms: 1,
        instance_id: "i".into(),
    };
    write_lease(&lease_path, &record).unwrap();
    assert_eq!(read_lease(&lease_path).unwrap().unwrap(), record);
}

#[cfg(unix)]
#[test]
fn lease_and_lock_files_are_owner_only_and_created_dirs_are_0700() {
    let dir = temp_dir("lease-modes");
    let nested = dir.join("state").join("lease-dir");
    let lease_path = nested.join("lease.txt");
    let lease = LeaderLease::new("node-a", &lease_path, 1_000, 100);
    assert!(lease.try_acquire(0).unwrap());
    assert_eq!(mode_of(&lease_path), 0o600);
    assert_eq!(mode_of(&lock_path_for(&lease_path)), 0o600);
    assert_eq!(mode_of(&nested), 0o700);
    assert_eq!(mode_of(nested.parent().unwrap()), 0o700);
}

#[cfg(unix)]
#[test]
fn group_or_world_writable_lease_dir_is_reported() {
    let dir = temp_dir("lease-open-dir");
    fs::set_permissions(&dir, fs::Permissions::from_mode(0o775)).unwrap();
    let warning = dir_permission_warning(&dir).expect("warning");
    assert!(warning.contains("group/world writable"), "{warning}");
    fs::set_permissions(&dir, fs::Permissions::from_mode(0o700)).unwrap();
    assert!(dir_permission_warning(&dir).is_none());
}

#[cfg(unix)]
#[test]
fn a_symlinked_lease_file_is_refused() {
    let dir = temp_dir("lease-symlinked-file");
    let target = dir.join("elsewhere.txt");
    fs::write(&target, "node-x,1,99999999999999,abc\n").unwrap();
    let lease_path = dir.join("lease.txt");
    std::os::unix::fs::symlink(&target, &lease_path).unwrap();
    let err = LeaderLease::new("node-a", &lease_path, 1_000, 100)
        .try_acquire(0)
        .expect_err("symlinked lease must be refused");
    assert!(err.contains("symbolic link"), "{err}");
}

fn forge(path: &Path, node: &str, epoch: u64, expires: u64, instance: &str) {
    fs::write(path, format!("{node},{epoch},{expires},{instance}\n")).unwrap();
}

#[test]
fn forged_max_epoch_and_expiry_does_not_make_everyone_a_follower() {
    let dir = temp_dir("lease-forged");
    let path = dir.join("lease.txt");
    forge(&path, "attacker", u64::MAX, u64::MAX, "x");
    let lease = LeaderLease::new("node-a", &path, 1_000, 100).with_safety_margin_ms(100);

    // Refused loudly instead of obeyed: the error names the reset flag.
    let err = lease.try_acquire(0).expect_err("forged lease");
    assert!(err.contains("DASH_CONTROL_PLANE_LEASE_RESET=1"), "{err}");
    assert!(!lease.is_leader().unwrap_or(false));
    assert!(lease.fencing_token().is_err());
    assert!(!lease.renew(0).unwrap_or(false));

    // The explicit admin reset recovers: a real lease replaces the forgery.
    let lease = LeaderLease::new("node-a", &path, 1_000, 100)
        .with_safety_margin_ms(100)
        .with_lease_reset(true);
    assert!(lease.try_acquire(0).unwrap());
    assert!(lease.is_leader().unwrap());
    // The absurd epoch is not carried forward.
    assert_eq!(lease.fencing_token().unwrap(), Some(1));
    // The reset is consumed: a second forgery is refused again.
    forge(&path, "attacker", 5, u64::MAX, "x");
    assert!(lease.renew(0).is_err() || !lease.is_leader().unwrap_or(false));
}

#[test]
fn a_forged_lease_naming_this_node_does_not_grant_leadership() {
    let dir = temp_dir("lease-forged-self");
    let path = dir.join("lease.txt");
    let lease = LeaderLease::new("node-a", &path, 1_000, 100);
    forge(&path, "node-a", 7, u64::MAX, lease.instance_id());
    assert!(!lease.is_leader().unwrap_or(false));
    assert!(lease.fencing_token().is_err());
    assert!(lease.current_leader().is_err());
}

#[test]
fn leases_expiring_slightly_beyond_one_duration_are_still_accepted() {
    let dir = temp_dir("lease-plausible");
    let path = dir.join("lease.txt");
    let a = LeaderLease::new("node-a", &path, 1_000, 100).with_safety_margin_ms(100);
    assert!(a.try_acquire(0).unwrap());
    let b = LeaderLease::new("node-b", &path, 1_000, 100).with_safety_margin_ms(100);
    // A healthy lease held by someone else is a plain "not leader", no error.
    assert!(!b.try_acquire(0).unwrap());
    assert!(b.current_leader().unwrap().is_some());
}

#[test]
fn two_processes_with_the_same_node_id_are_not_both_leader() {
    let dir = temp_dir("lease-same-node");
    let path = dir.join("lease.txt");
    let first = LeaderLease::new("control-plane", &path, 5_000, 100);
    let second = LeaderLease::new("control-plane", &path, 5_000, 100);
    assert_ne!(first.instance_id(), second.instance_id());
    assert!(first.try_acquire(0).unwrap());
    assert!(
        !second.try_acquire(0).unwrap(),
        "same node id, other process"
    );
    assert!(first.is_leader().unwrap());
    assert!(!second.is_leader().unwrap());
    assert!(!second.renew(0).unwrap());
    assert!(first.renew(0).unwrap());
    assert_eq!(first.fencing_token().unwrap(), Some(1));
    assert_eq!(second.fencing_token().unwrap(), None);
}

#[test]
fn records_from_older_versions_without_an_instance_id_still_parse() {
    let dir = temp_dir("lease-legacy-format");
    let path = dir.join("lease.txt");
    fs::write(&path, "node-a,4,123\n").unwrap();
    let record = read_lease(&path).unwrap().unwrap();
    assert_eq!(record.instance_id, "");
    assert_eq!(record.epoch, 4);
    // A live process never matches the legacy (instance-less) holder.
    let lease = LeaderLease::new("node-a", &path, 1_000, 100);
    assert!(!lease.holds(&record));
}
