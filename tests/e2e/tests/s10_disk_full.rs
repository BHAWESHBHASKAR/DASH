//! Scenario 10: disk full. The ingestion process runs with a file size limit
//! (`RLIMIT_FSIZE`), so once its WAL (or redb file, or snapshot) would grow
//! past the limit every write fails with `EFBIG`, which is what a full volume
//! looks like to the process. Checked:
//!
//! * a write that cannot be persisted is answered with a 5xx, never a 2xx;
//! * `/ready` reports not-ready while writes are failing;
//! * once space is back (the limit is raised on the live process, or the
//!   process restarts without it) the service is ready again and accepts
//!   writes (in place for the WAL; a failed redb mirror needs a restart);
//! * every acknowledged write survives a restart, no bundle is half applied,
//!   no evidence is duplicated and the WAL verifies clean.
//!
//! Linux only: the limit is raised on the running process with `prlimit`.

#![cfg(target_os = "linux")]

use std::collections::BTreeMap;
use std::path::Path;
use std::process::Command;
use std::time::Duration;

use dash_e2e::*;

const T: &str = "tenant-a";
/// Headroom above the current largest file when the limit is applied.
const HEADROOM: u64 = 24 * 1024;
const MAX_ATTEMPTS: usize = 5_000;

fn largest_file(dir: &Path) -> u64 {
    std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok()?.metadata().ok())
        .filter(|m| m.is_file())
        .map(|m| m.len())
        .max()
        .unwrap_or(0)
}

/// Set the soft file size limit of a running process (`None`: back to the
/// hard limit, which is what "space came back" means here).
fn set_file_size_limit(pid: u32, limit: Option<u64>) {
    let mut old = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: plain syscalls on a pid we own; pointers are to valid locals.
    let rc = unsafe {
        libc::prlimit(
            pid as libc::pid_t,
            libc::RLIMIT_FSIZE,
            std::ptr::null(),
            &mut old,
        )
    };
    assert_eq!(rc, 0, "prlimit get: {}", std::io::Error::last_os_error());
    let rl = libc::rlimit {
        rlim_cur: limit.map_or(old.rlim_max, |n| (n as libc::rlim_t).min(old.rlim_max)),
        rlim_max: old.rlim_max,
    };
    let rc = unsafe {
        libc::prlimit(
            pid as libc::pid_t,
            libc::RLIMIT_FSIZE,
            &rl,
            std::ptr::null_mut(),
        )
    };
    assert_eq!(
        rc,
        0,
        "prlimit({pid}) failed: {}",
        std::io::Error::last_os_error()
    );
}

fn wal_inspect(wal: &Path) -> (bool, String) {
    let out = Command::new(bin_path("wal-inspect"))
        .args(["verify", &wal.display().to_string()])
        .output()
        .expect("run wal-inspect");
    (
        out.status.success(),
        format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        ),
    )
}

struct Run {
    /// claim id -> evidence count of every acknowledged bundle.
    acked: BTreeMap<String, usize>,
    /// Status codes of the failed writes.
    failures: Vec<u16>,
}

/// Ingest bundles until `want_failures` writes have failed (or the attempt
/// budget runs out). Every answer must be 200 or a 5xx.
fn ingest_until_failing(s: &Stack, prefix: &str, want_failures: usize) -> Run {
    let mut run = Run {
        acked: BTreeMap::new(),
        failures: vec![],
    };
    for i in 0..MAX_ATTEMPTS {
        let claim = format!("{prefix}-{i}");
        let n = 1 + i % 3;
        let body = bundle(
            T,
            &claim,
            &format!("disk full claim {claim} about pump station {}", i % 17),
            n,
        );
        let r = s
            .try_ingest_as(T, &body)
            .unwrap_or_else(|e| panic!("ingest {claim}: transport error {e}\n{}", s.ingest_log()));
        match r.status {
            200 => {
                run.acked.insert(claim, n);
            }
            st if st >= 500 => {
                run.failures.push(st);
                if run.failures.len() >= want_failures {
                    return run;
                }
            }
            st => panic!(
                "write on a full disk answered {st} (expected 200 or 5xx): {}",
                r.body
            ),
        }
    }
    panic!(
        "no write failed within {MAX_ATTEMPTS} attempts; the file size limit had no effect\n{}",
        s.ingest_log()
    );
}

fn assert_acked_present(s: &Stack, acked: &BTreeMap<String, usize>, label: &str) {
    let leader = s.leader_state();
    for (claim, n) in acked {
        assert!(
            leader.claims.contains_key(claim),
            "{label}: acknowledged claim {claim} lost"
        );
        for i in 0..*n {
            let e = format!("{claim}-e{i}");
            assert_eq!(
                leader.evidence.get(&e).map(String::as_str),
                Some(claim.as_str()),
                "{label}: acknowledged bundle {claim} is missing evidence {e}"
            );
        }
    }
    for (e, count) in &leader.evidence_lines {
        assert_eq!(*count, 1, "{label}: evidence {e} stored {count} times");
    }
    // A claim the service never acknowledged may exist only as a whole bundle.
    for claim in leader.claims.keys() {
        if acked.contains_key(claim) {
            continue;
        }
        let have = leader.evidence.values().filter(|c| *c == claim).count();
        assert!(
            have >= 1,
            "{label}: unacknowledged claim {claim} is present without its evidence"
        );
    }
}

#[derive(Clone, Copy)]
enum Mode {
    /// WAL only (redb mirror disabled): the volume fills, space comes back
    /// on the live process, it fills again and the process is killed.
    WalOnly,
    /// WAL plus redb mirror. The limit sits below the preallocated redb
    /// file, so the mirror fails first; a failed mirror is detached until the
    /// next start, so recovery is checked across a restart only.
    WithRedb,
}

fn scenario(label: &str, mode: Mode, extra_env: Vec<(String, String)>) {
    let mut env = extra_env;
    if matches!(mode, Mode::WalOnly) {
        env.push(("DASH_INGEST_PERSISTENCE_DISABLE".into(), "1".into()));
    }
    let mut s = Stack::new(StackOpts {
        tenants: vec![T.into()],
        extra_ingest_env: env,
        ..Default::default()
    });
    s.start_ingest();
    let mut acked = BTreeMap::new();
    for i in 0..10 {
        let claim = format!("{label}-pre-{i}");
        let r = s.ingest_as(
            T,
            &bundle(T, &claim, "claim written before the disk filled", 2),
        );
        assert_eq!(r.status, 200, "{}", r.body);
        acked.insert(claim, 2);
    }
    assert!(
        s.ingest
            .as_mut()
            .unwrap()
            .terminate(Duration::from_secs(15))
    );

    // The volume fills up while the service is running.
    let base = match mode {
        Mode::WalOnly => largest_file(s.dir.path()),
        Mode::WithRedb => std::fs::metadata(s.path("ingest.wal")).unwrap().len(),
    };
    let limit = base + HEADROOM;
    s.start_ingest_with(&SpawnOpts {
        file_size_limit: Some(limit),
    });
    if matches!(mode, Mode::WalOnly) {
        s.wait_ingest_ready(Duration::from_secs(10));
    }
    let run = ingest_until_failing(&s, &format!("{label}-full"), 5);
    println!(
        "{label}: limit={limit} acknowledged={} failures={:?}",
        run.acked.len(),
        run.failures
    );
    acked.extend(run.acked);
    assert_eq!(
        s.ingest_ready_status(),
        Some(503),
        "{label}: /ready must report not-ready while writes fail\n{}",
        s.ingest_log()
    );
    let pid = s.ingest.as_ref().unwrap().pid();

    if matches!(mode, Mode::WalOnly) {
        // Space comes back without a restart.
        set_file_size_limit(pid, None);
        s.wait_ingest_ready(Duration::from_secs(10));
        for i in 0..5 {
            let claim = format!("{label}-after-{i}");
            let r = s.ingest_as(
                T,
                &bundle(T, &claim, "claim written after space came back", 2),
            );
            assert_eq!(
                r.status, 200,
                "{label}: write after space came back: {}",
                r.body
            );
            acked.insert(claim, 2);
        }
        assert_acked_present(&s, &acked, label);

        // Fill it again.
        let limit = largest_file(s.dir.path()) + HEADROOM;
        set_file_size_limit(pid, Some(limit));
        let run = ingest_until_failing(&s, &format!("{label}-full2"), 3);
        acked.extend(run.acked);
        assert_eq!(s.ingest_ready_status(), Some(503), "{label}: second fill");
    }

    // Crash while full, then restart with space available.
    s.kill_ingest();
    let (ok, report) = wal_inspect(&s.path("ingest.wal"));
    assert!(
        ok,
        "{label}: WAL does not verify after disk full:\n{report}"
    );
    s.start_ingest();
    s.wait_ingest_ready(Duration::from_secs(20));
    assert_acked_present(&s, &acked, label);
    let r = s.ingest_as(T, &bundle(T, &format!("{label}-final"), "after restart", 1));
    assert_eq!(r.status, 200, "{label}: write after restart: {}", r.body);
    assert_eq!(
        s.ingest_ready_status(),
        Some(200),
        "{label}: ready after restart"
    );
}

#[test]
fn full_wal_volume_fails_writes_with_5xx_and_recovers_in_place() {
    scenario("walonly", Mode::WalOnly, vec![]);
}

#[test]
fn full_volume_during_checkpoints_keeps_acknowledged_writes() {
    scenario(
        "ckpt",
        Mode::WalOnly,
        vec![("DASH_CHECKPOINT_MAX_WAL_RECORDS".into(), "40".into())],
    );
}

#[test]
fn full_volume_with_redb_mirror_is_not_ready_and_recovers_on_restart() {
    scenario("redb", Mode::WithRedb, vec![]);
}
