use std::path::Path;
use std::process::{Command, Output};

use schema::claim_builder;
use store::FileWal;
use tempfile::TempDir;

fn run(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_wal-inspect"))
        .args(args)
        .output()
        .unwrap()
}

fn code(out: &Output) -> i32 {
    out.status.code().unwrap()
}

fn text(out: &Output) -> String {
    String::from_utf8_lossy(&out.stdout).to_string()
}

fn write_wal(path: &Path, n: usize) -> Vec<String> {
    let mut wal = FileWal::open(path).unwrap();
    for i in 0..n {
        wal.append_claim(&claim_builder(
            &format!("c{i}"),
            "tenant-a",
            "some text",
            0.9,
        ))
        .unwrap();
    }
    wal.flush_pending_sync().unwrap();
    drop(wal);
    std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(str::to_string)
        .collect()
}

fn write_lines(path: &Path, lines: &[String]) {
    std::fs::write(path, lines.join("\n") + "\n").unwrap();
}

#[test]
fn inspect_reports_counts_generation_and_torn_tail() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("dash.wal");
    write_wal(&path, 3);
    let mut bytes = std::fs::read(&path).unwrap();
    bytes.extend_from_slice(b"C2\ttruncated-record");
    std::fs::write(&path, &bytes).unwrap();

    let out = run(&["inspect", path.to_str().unwrap()]);
    assert_eq!(code(&out), 0);
    let t = text(&out);
    assert!(t.contains("valid records: 3 (0 legacy)"), "{t}");
    assert!(t.contains("C2: 3"), "{t}");
    assert!(t.contains("generation: "), "{t}");
    assert!(t.contains("torn tail: yes (line 4"), "{t}");
    assert!(t.contains("invalid lines: 0"), "{t}");
    // inspect never modifies the file
    assert_eq!(std::fs::read(&path).unwrap(), bytes);
}

#[test]
fn verify_fails_on_a_middle_checksum_failure_but_not_on_a_torn_tail() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("dash.wal");
    let mut lines = write_wal(&path, 4);

    // torn tail only: verify passes
    let mut torn = lines.join("\n");
    torn.push_str("\nC2\tcut-off");
    std::fs::write(&path, &torn).unwrap();
    let out = run(&["verify", path.to_str().unwrap()]);
    assert_eq!(code(&out), 0, "{}", text(&out));

    // flip a byte in the second record
    lines[1] = lines[1].replacen("some text", "some t3xt", 1);
    write_lines(&path, &lines);
    let out = run(&["verify", path.to_str().unwrap()]);
    assert_eq!(code(&out), 1);
    let t = text(&out);
    assert!(t.contains("line 2: checksum failure"), "{t}");
    assert!(t.contains("1 checksum failure"), "{t}");

    let out = run(&["inspect", path.to_str().unwrap()]);
    assert!(
        text(&out).contains("first invalid line: 2"),
        "{}",
        text(&out)
    );
}

#[test]
fn verify_fails_on_an_unparseable_middle_line() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("dash.wal");
    let mut lines = write_wal(&path, 3);
    lines[1] = "garbage".to_string();
    write_lines(&path, &lines);
    assert_eq!(code(&run(&["verify", path.to_str().unwrap()])), 1);
}

#[test]
fn repair_dry_run_changes_nothing() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("dash.wal");
    let lines = write_wal(&path, 3);
    let mut bytes = lines.join("\n").into_bytes();
    bytes.extend_from_slice(b"\nC2\tcut-off");
    std::fs::write(&path, &bytes).unwrap();

    let out = run(&["repair", path.to_str().unwrap(), "--dry-run"]);
    assert_eq!(code(&out), 0, "{}", text(&out));
    assert!(
        text(&out).contains("would: truncate torn tail"),
        "{}",
        text(&out)
    );
    assert_eq!(std::fs::read(&path).unwrap(), bytes);
    assert!(!dir.path().join("dash.wal.bak").exists());
}

#[test]
fn repair_truncates_the_torn_tail_and_keeps_a_backup() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("dash.wal");
    let lines = write_wal(&path, 3);
    let mut bytes = lines.join("\n").into_bytes();
    bytes.extend_from_slice(b"\nC2\tcut-off");
    std::fs::write(&path, &bytes).unwrap();

    let out = run(&["repair", path.to_str().unwrap()]);
    assert_eq!(code(&out), 0, "{}", text(&out));
    assert_eq!(
        std::fs::read_to_string(&path).unwrap(),
        lines.join("\n") + "\n"
    );
    assert_eq!(
        std::fs::read(dir.path().join("dash.wal.bak")).unwrap(),
        bytes
    );
    assert_eq!(code(&run(&["verify", path.to_str().unwrap()])), 0);

    // a second repair has nothing to do and must not touch the backup
    let out = run(&["repair", path.to_str().unwrap()]);
    assert_eq!(code(&out), 0);
    assert!(text(&out).contains("nothing to change"));
}

#[test]
fn repair_quarantines_invalid_middle_lines_only_when_asked() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("dash.wal");
    let mut lines = write_wal(&path, 4);
    let good = lines.clone();
    lines[1] = lines[1].replacen("some text", "some t3xt", 1);
    let corrupted = lines[1].clone();
    write_lines(&path, &lines);

    // without --quarantine the invalid line stays and the exit code says so
    let out = run(&["repair", path.to_str().unwrap()]);
    assert_eq!(code(&out), 1, "{}", text(&out));
    assert!(text(&out).contains("1 invalid line(s) remain"));
    assert_eq!(
        std::fs::read_to_string(&path).unwrap(),
        lines.join("\n") + "\n"
    );

    // WAL opens only after repair: before it, replay is a hard error
    let wal = FileWal::open(&path).unwrap();
    let err = store::InMemoryStore::load_from_wal(&wal).err().unwrap();
    assert!(format!("{err:?}").contains("line 2"), "{err:?}");
    drop(wal);

    let gen_before = std::fs::read_to_string(dir.path().join("dash.wal.gen")).unwrap();
    let out = run(&["repair", path.to_str().unwrap(), "--quarantine"]);
    assert_eq!(code(&out), 0, "{}", text(&out));
    let expected: Vec<String> = [0usize, 2, 3].iter().map(|&i| good[i].clone()).collect();
    assert_eq!(
        std::fs::read_to_string(&path).unwrap(),
        expected.join("\n") + "\n"
    );
    assert_eq!(
        std::fs::read_to_string(dir.path().join("dash.wal.quarantine")).unwrap(),
        corrupted + "\n"
    );
    assert!(dir.path().join("dash.wal.bak").exists());
    // lineage changed so replication followers resync
    let gen_after = std::fs::read_to_string(dir.path().join("dash.wal.gen")).unwrap();
    assert_ne!(gen_before, gen_after);
    assert_eq!(code(&run(&["verify", path.to_str().unwrap()])), 0);

    let wal = FileWal::open(&path).unwrap();
    let store = store::InMemoryStore::load_from_wal(&wal).unwrap();
    assert!(store.claim_by_id("c0").is_some());
    assert!(store.claim_by_id("c1").is_none());
    assert!(store.claim_by_id("c3").is_some());
}

#[test]
fn repair_refuses_to_overwrite_an_existing_backup() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("dash.wal");
    let lines = write_wal(&path, 2);
    let mut bytes = lines.join("\n").into_bytes();
    bytes.extend_from_slice(b"\nC2\tcut-off");
    std::fs::write(&path, &bytes).unwrap();
    std::fs::write(dir.path().join("dash.wal.bak"), b"precious").unwrap();

    let out = run(&["repair", path.to_str().unwrap()]);
    assert_eq!(code(&out), 2);
    assert_eq!(std::fs::read(&path).unwrap(), bytes);
    assert_eq!(
        std::fs::read(dir.path().join("dash.wal.bak")).unwrap(),
        b"precious"
    );
}

#[test]
fn usage_errors_exit_with_code_two() {
    assert_eq!(code(&run(&[])), 2);
    assert_eq!(code(&run(&["frobnicate", "x"])), 2);
    assert_eq!(code(&run(&["verify"])), 2);
    assert_eq!(code(&run(&["verify", "/nonexistent/dash.wal"])), 2);
}

fn key_file(dir: &Path, name: &str, byte: u8) -> std::path::PathBuf {
    let path = dir.join(name);
    let hex: String = std::iter::repeat_n(format!("{byte:02x}"), 32).collect();
    std::fs::write(&path, hex).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    path
}

fn run_with_keys(args: &[&str], active: Option<&Path>, previous: Option<&Path>) -> Output {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_wal-inspect"));
    cmd.args(args)
        .env_remove("DASH_ENCRYPTION_KEY_FILE")
        .env_remove("DASH_ENCRYPTION_PREVIOUS_KEY_FILES");
    if let Some(active) = active {
        cmd.env("DASH_ENCRYPTION_KEY_FILE", active);
    }
    if let Some(previous) = previous {
        cmd.env("DASH_ENCRYPTION_PREVIOUS_KEY_FILES", previous);
    }
    cmd.output().unwrap()
}

#[test]
fn encrypted_files_are_verified_listed_and_rewrapped_with_the_configured_keys() {
    use std::sync::Arc;
    use store::encryption::{Keyring, local_key_id, with_keyring};
    let dir = TempDir::new().unwrap();
    let keys = TempDir::new().unwrap();
    let old_key = key_file(keys.path(), "old.key", 0x11);
    let new_key = key_file(keys.path(), "new.key", 0x22);
    let wal = dir.path().join("dash.wal");
    with_keyring(
        Some(Arc::new(Keyring::local([0x11; 32], &[]).unwrap())),
        || write_encrypted(&wal),
    );
    let wal_arg = wal.to_str().unwrap();

    let out = run_with_keys(&["verify", wal_arg], Some(&old_key), None);
    assert_eq!(code(&out), 0, "{}", text(&out));
    assert!(text(&out).contains("ok: 3 valid records"), "{}", text(&out));

    let out = run_with_keys(&["verify", wal_arg], None, None);
    assert_eq!(code(&out), 2);
    let err = String::from_utf8_lossy(&out.stderr).to_string();
    assert!(err.contains("no encryption key is configured"), "{err}");

    let dir_arg = dir.path().to_str().unwrap();
    let out = run_with_keys(&["keys", dir_arg], None, None);
    assert_eq!(code(&out), 0);
    let listed = text(&out);
    assert!(listed.contains(&local_key_id(&[0x11; 32])), "{listed}");
    assert!(listed.contains("encrypted lines"), "{listed}");

    // Rewrap needs the old key to unwrap; afterwards the new key alone works.
    let out = run_with_keys(&["rewrap", dir_arg], Some(&new_key), None);
    assert_eq!(code(&out), 1, "{}", text(&out));
    let out = run_with_keys(&["rewrap", dir_arg], Some(&new_key), Some(&old_key));
    assert_eq!(code(&out), 0, "{}", text(&out));
    assert!(text(&out).contains("rewrapped"), "{}", text(&out));
    let out = run_with_keys(&["verify", wal_arg], Some(&new_key), None);
    assert_eq!(code(&out), 0, "{}", text(&out));
    let out = run_with_keys(&["keys", dir_arg], None, None);
    assert!(text(&out).contains(&local_key_id(&[0x22; 32])));
}

fn write_encrypted(path: &Path) {
    let mut wal = FileWal::open(path).unwrap();
    for i in 0..3 {
        wal.append_claim(&claim_builder(
            &format!("c{i}"),
            "tenant-a",
            "secret text",
            0.9,
        ))
        .unwrap();
    }
    wal.flush_pending_sync().unwrap();
}
