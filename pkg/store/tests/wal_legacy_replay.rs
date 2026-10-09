//! Legacy and poisoned WAL recovery: lenient replay quarantines what cannot
//! be applied, strict replay and mid-file corruption stay hard errors.
//!
//! The WAL files here are written by hand in the formats produced by older
//! releases (`C`/`E`/`G`/`V`/`B` records without a checksum, with entity
//! lists packed unescaped and ids escaped with `\t` / `\n` only).

use std::path::{Path, PathBuf};

use store::{AnnTuningConfig, FileWal, InMemoryStore, ReplayPolicy, StoreError, WalReplayStats};
use tempfile::TempDir;

fn crc32(data: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFFu32;
    for &byte in data {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
        }
    }
    !crc
}

/// Appends the checksum suffix to a versioned (`C2`, `V2`, ...) record body.
fn sealed(body: &str) -> String {
    format!("{body}\tcrc={:08x}", crc32(body.as_bytes()))
}

const TAIL: &str = "null\tnull\tnull\tnull\tnull";

fn legacy_claim(id: &str, entities: &str) -> String {
    // C id tenant text confidence event_time entities embedding_ids type vf vt ca ua
    format!("C\t{id}\ttenant-a\ttext of {id}\t0.9\tnull\t{entities}\t\t{TAIL}")
}

fn legacy_evidence(id: &str, claim: &str) -> String {
    format!("E\t{id}\t{claim}\tsource-1\tsupports\t0.8")
}

fn legacy_edge(id: &str, from: &str, to: &str) -> String {
    format!("G\t{id}\t{from}\t{to}\tsupports\t0.5")
}

fn legacy_vector(claim: &str, values: &str) -> String {
    format!("V\t{claim}\t{values}")
}

fn write_lines(path: &Path, lines: &[String]) {
    std::fs::write(path, lines.join("\n") + "\n").unwrap();
}

fn quarantine_lines(wal: &Path) -> Vec<String> {
    let mut p = wal.as_os_str().to_owned();
    p.push(".quarantine");
    match std::fs::read_to_string(PathBuf::from(p)) {
        Ok(text) => text.lines().map(str::to_string).collect(),
        Err(_) => Vec::new(),
    }
}

fn sorted(mut v: Vec<String>) -> Vec<String> {
    v.sort();
    v
}

fn load(
    path: &Path,
    policy: ReplayPolicy,
) -> Result<(InMemoryStore, store::StoreLoadStats), StoreError> {
    let wal = FileWal::open(path).unwrap();
    InMemoryStore::load_from_wal_with_policy(&wal, AnnTuningConfig::default(), policy)
}

/// A WAL as an old release could have left it: ids with control characters,
/// an entity containing a tab, a poisoned vector, plus healthy records.
fn synthetic_legacy_wal() -> (Vec<String>, Vec<String>) {
    let tab_id_claim = legacy_claim("c\\tbad", "");
    let tab_entity_claim = legacy_claim("c-ent", "5:a\tb c");
    let vec_dim_mismatch = legacy_vector("c-ok2", "1,2");
    let vec_nan = legacy_vector("c-ok", "1,NaN,3");
    let vec_ghost = legacy_vector("ghost", "1,2,3");
    let dep_evidence_a = legacy_evidence("e-dep-a", "c\\tbad");
    let dep_evidence_b = legacy_evidence("e-dep-b", "c-ent");
    let dep_edge = legacy_edge("g-dep", "c-ok", "c\\tbad");
    let lines = vec![
        legacy_claim("c-ok", "3:foo"),                              // 1
        tab_id_claim.clone(),                                       // 2: control char in id
        legacy_claim("c-ok2", "3:bar"),                             // 3
        tab_entity_claim.clone(),        // 4: tab in entity -> unparseable
        legacy_evidence("e-ok", "c-ok"), // 5
        dep_evidence_a.clone(),          // 6: dependent
        dep_evidence_b.clone(),          // 7: dependent
        dep_edge.clone(),                // 8: dependent (target quarantined)
        legacy_edge("g-ok", "c-ok", "c-ok2"), // 9
        legacy_vector("c-ok", "1,2,3"),  // 10: sets the tenant dimension
        vec_dim_mismatch.clone(),        // 11: poisoned
        vec_nan.clone(),                 // 12: poisoned (non-finite)
        vec_ghost.clone(),               // 13: poisoned (missing claim)
        "B\tcommit-1\t2\t1700000000000\t4:c-ok5:c-ok2".to_string(), // 14
    ];
    let expected_quarantine = vec![
        tab_id_claim,
        tab_entity_claim,
        dep_evidence_a,
        dep_evidence_b,
        dep_edge,
        vec_dim_mismatch,
        vec_nan,
        vec_ghost,
    ];
    (lines, expected_quarantine)
}

#[test]
fn synthetic_legacy_wal_loads_and_quarantines_the_unreadable_records() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("legacy.wal");
    let (lines, expected_quarantine) = synthetic_legacy_wal();
    write_lines(&path, &lines);
    let before = std::fs::read_to_string(&path).unwrap();

    let (store, stats) = load(&path, ReplayPolicy::Lenient).expect("legacy wal must load");

    assert_eq!(stats.claims_loaded, 2);
    assert_eq!(stats.evidence_loaded, 1);
    assert_eq!(stats.edges_loaded, 1);
    assert_eq!(stats.vectors_loaded, 1);
    // c-tab-id, c-ent, three poisoned vectors.
    assert_eq!(stats.replay.quarantined_records, 5, "{:?}", stats.replay);
    assert_eq!(stats.replay.dependent_skipped, 3, "{:?}", stats.replay);
    assert!(store.claim_by_id("c-ok").is_some());
    assert!(store.claim_by_id("c-ok2").is_some());
    assert!(store.claim_by_id("c\tbad").is_none());
    assert!(store.claim_by_id("c-ent").is_none());
    assert!(store.batch_commit_metadata("commit-1").is_some());
    assert_eq!(store.index_stats().vector_count, 1);

    // Every offending line is preserved verbatim in the quarantine file.
    assert_eq!(
        sorted(quarantine_lines(&path)),
        sorted(expected_quarantine.clone())
    );
    // The WAL itself is untouched; replay never rewrites it.
    assert_eq!(std::fs::read_to_string(&path).unwrap(), before);

    // Replaying again is stable and does not duplicate quarantine entries.
    let (_, again) = load(&path, ReplayPolicy::Lenient).unwrap();
    assert_eq!(again.replay.quarantined_records, 5);
    assert_eq!(again.replay.dependent_skipped, 3);
    assert_eq!(sorted(quarantine_lines(&path)), sorted(expected_quarantine));
}

#[test]
fn strict_policy_fails_on_the_first_unreadable_legacy_record() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("legacy.wal");
    write_lines(&path, &synthetic_legacy_wal().0);

    let err = load(&path, ReplayPolicy::Strict).err().expect("must fail");
    let msg = format!("{err:?}");
    // Unparseable lines are reported while reading, before any record is applied.
    assert!(msg.contains("wal line 4"), "{msg}");
    assert!(
        quarantine_lines(&path).is_empty(),
        "strict must not quarantine"
    );
}

#[test]
fn replay_policy_env_value_parsing() {
    assert_eq!(ReplayPolicy::from_env_value(None), ReplayPolicy::Lenient);
    assert_eq!(
        ReplayPolicy::from_env_value(Some("0")),
        ReplayPolicy::Lenient
    );
    assert_eq!(
        ReplayPolicy::from_env_value(Some("")),
        ReplayPolicy::Lenient
    );
    assert_eq!(
        ReplayPolicy::from_env_value(Some("1")),
        ReplayPolicy::Strict
    );
    assert_eq!(
        ReplayPolicy::from_env_value(Some(" TRUE ")),
        ReplayPolicy::Strict
    );
}

#[test]
fn newline_inside_a_legacy_entity_quarantines_both_fragments() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("legacy.wal");
    // The old writer did not escape entities, so a newline split the record
    // over two physical lines.
    let lines = vec![
        legacy_claim("c-ok", "3:foo"),
        "C\tc-nl\ttenant-a\ttext\t0.9\tnull\t3:a".to_string(),
        format!("b\t\t{TAIL}"),
        legacy_evidence("e-dep", "c-nl"),
        legacy_evidence("e-ok", "c-ok"),
    ];
    write_lines(&path, &lines);

    let (store, stats) = load(&path, ReplayPolicy::Lenient).unwrap();
    assert_eq!(stats.claims_loaded, 1);
    assert_eq!(stats.evidence_loaded, 1);
    assert_eq!(stats.replay.quarantined_records, 2);
    assert_eq!(stats.replay.dependent_skipped, 1);
    assert!(store.claim_by_id("c-nl").is_none());
    assert_eq!(quarantine_lines(&path), lines[1..4].to_vec());
}

#[test]
fn unreadable_legacy_final_line_is_quarantined_not_truncated() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("legacy.wal");
    let bad = legacy_claim("c-ent", "5:a\tb c");
    write_lines(&path, &[legacy_claim("c-ok", "3:foo"), bad.clone()]);

    let wal = FileWal::open(&path).unwrap();
    assert_eq!(wal.torn_tail_dropped(), 0);
    let (_, stats) = InMemoryStore::load_from_wal_with_stats(&wal).unwrap();
    assert_eq!(stats.replay.quarantined_records, 1);
    assert_eq!(stats.claims_loaded, 1);
    assert_eq!(quarantine_lines(&path), vec![bad.clone()]);
    assert!(std::fs::read_to_string(&path).unwrap().contains(&bad));
}

// ---- policy matrix -------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Outcome {
    /// Load succeeds; N records quarantined.
    Quarantined(usize),
    /// Load fails with an error naming the given line.
    FailsAtLine(usize),
}

fn run_matrix_case(name: &str, lines: Vec<String>, lenient: Outcome, strict: Outcome) {
    for (policy, expected) in [
        (ReplayPolicy::Lenient, lenient),
        (ReplayPolicy::Strict, strict),
    ] {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("case.wal");
        write_lines(&path, &lines);
        let result = load(&path, policy);
        match expected {
            Outcome::Quarantined(n) => {
                let (_, stats) = result.unwrap_or_else(|e| panic!("{name}/{policy:?}: {e:?}"));
                assert_eq!(
                    stats.replay.quarantined_records, n,
                    "{name}/{policy:?}: {:?}",
                    stats.replay
                );
            }
            Outcome::FailsAtLine(line) => {
                let err = result
                    .err()
                    .unwrap_or_else(|| panic!("{name}/{policy:?}: must fail"));
                let msg = format!("{err:?}");
                assert!(
                    msg.contains(&format!("wal line {line}")),
                    "{name}/{policy:?}: {msg}"
                );
            }
        }
    }
}

#[test]
fn policy_matrix() {
    use Outcome::*;
    let ok = legacy_claim("c-ok", "3:foo");
    let ok2 = legacy_claim("c-ok2", "");
    let first_vec = legacy_vector("c-ok", "1,2,3");

    // legacy record that cannot be parsed
    run_matrix_case(
        "legacy-unparseable",
        vec![ok.clone(), "E\tonly\tthree".into(), ok2.clone()],
        Quarantined(1),
        FailsAtLine(2),
    );
    // legacy record rejected by validation (control character in an id)
    run_matrix_case(
        "legacy-control-char",
        vec![ok.clone(), legacy_claim("c\\nx", ""), ok2.clone()],
        Quarantined(1),
        FailsAtLine(2),
    );
    // legacy poisoned vectors
    run_matrix_case(
        "legacy-vector-dimension",
        vec![ok.clone(), first_vec.clone(), legacy_vector("c-ok", "1,2")],
        Quarantined(1),
        FailsAtLine(3),
    );
    run_matrix_case(
        "legacy-vector-missing-claim",
        vec![ok.clone(), legacy_vector("nope", "1,2,3"), ok2.clone()],
        Quarantined(1),
        FailsAtLine(2),
    );
    // versioned record with a bad checksum in the middle: always fatal
    let mut bad_crc = sealed("V2\tc-ok\t1,2,3");
    bad_crc = bad_crc.replacen("1,2,3", "1,2,4", 1);
    run_matrix_case(
        "versioned-bad-crc-middle",
        vec![ok.clone(), bad_crc, ok2.clone()],
        FailsAtLine(2),
        FailsAtLine(2),
    );
    // versioned record that fails schema validation (checksum is valid)
    run_matrix_case(
        "versioned-control-char",
        vec![
            ok.clone(),
            sealed(&format!(
                "C2\tc\\tx\ttenant-a\ttext\t0.9\tnull\t\t\tnull\t{TAIL}"
            )),
            ok2.clone(),
        ],
        FailsAtLine(2),
        FailsAtLine(2),
    );
    // versioned poisoned vector (valid checksum, wrong dimension): quarantined
    run_matrix_case(
        "versioned-vector-dimension",
        vec![
            ok.clone(),
            first_vec.clone(),
            sealed("V2\tc-ok\t1,2"),
            ok2.clone(),
        ],
        Quarantined(1),
        FailsAtLine(3),
    );
    // garbage that is not a record of any kind: always fatal in the middle
    run_matrix_case(
        "garbage-middle",
        vec![ok.clone(), "complete nonsense".into(), ok2.clone()],
        FailsAtLine(2),
        FailsAtLine(2),
    );
}

#[test]
fn conflict_is_not_quarantinable() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("c.wal");
    write_lines(
        &path,
        &[
            legacy_claim("c-ok", ""),
            "C\tc-ok\ttenant-b\ttext\t0.9\tnull\t\t\tnull\tnull\tnull\tnull\tnull".to_string(),
        ],
    );
    let err = load(&path, ReplayPolicy::Lenient).err().unwrap();
    assert!(matches!(err, StoreError::Conflict(_)), "{err:?}");
}

// ---- snapshots -----------------------------------------------------------

#[test]
fn snapshot_gets_the_same_treatment() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("dash.wal");
    let bad_claim = legacy_claim("c\\tbad", "");
    let bad_vec = legacy_vector("c-ok", "1,2");
    let mut snapshot = vec![
        "SNAP\t1".to_string(),
        legacy_claim("c-ok", ""),
        bad_claim.clone(),
        legacy_vector("c-ok", "1,2,3"),
        bad_vec.clone(),
        legacy_evidence("e-dep", "c\\tbad"),
    ];
    let mut snap_path = path.as_os_str().to_owned();
    snap_path.push(".snapshot");
    let snap_path = PathBuf::from(snap_path);
    write_lines(&snap_path, &snapshot);
    write_lines(&path, &[legacy_claim("c-wal", "")]);

    let (store, stats) = load(&path, ReplayPolicy::Lenient).unwrap();
    assert_eq!(stats.claims_loaded, 2);
    assert_eq!(stats.vectors_loaded, 1);
    assert_eq!(stats.replay.quarantined_records, 2);
    assert_eq!(stats.replay.dependent_skipped, 1);
    assert_eq!(stats.replay.snapshot_records, 5); // records read, before apply-time quarantine
    assert!(store.claim_by_id("c-wal").is_some());
    assert_eq!(quarantine_lines(&path).len(), 3);
    assert_eq!(quarantine_lines(&path)[0], bad_claim);

    let err = load(&path, ReplayPolicy::Strict).err().expect("strict");
    assert!(format!("{err:?}").contains("snapshot record 2"), "{err:?}");

    // A corrupt versioned record inside a snapshot is fatal.
    snapshot.push(sealed("G2\tg\ta\tb\tsupports\t0.5\t\tnull").replacen("0.5", "0.6", 1));
    write_lines(&snap_path, &snapshot);
    let err = load(&path, ReplayPolicy::Lenient).err().expect("fatal");
    assert!(format!("{err:?}").contains("snapshot record 6"), "{err:?}");
}

#[test]
fn disk_backed_load_also_survives_a_legacy_wal() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("legacy.wal");
    let disk_path = dir.path().join("dash.redb");
    write_lines(&path, &synthetic_legacy_wal().0);

    let mut wal = FileWal::open(&path).unwrap();
    let (store, stats) =
        InMemoryStore::load_from_disk_and_wal(&disk_path, &mut wal, AnnTuningConfig::default())
            .expect("legacy wal must not block startup");
    assert_eq!(stats.replay.quarantined_records, 5);
    assert_eq!(stats.replay.dependent_skipped, 3);
    assert!(store.claim_by_id("c-ok").is_some());
    assert!(store.claim_by_id("c\tbad").is_none());
}

#[test]
fn quarantined_counts_are_part_of_the_default_stats() {
    let stats = WalReplayStats::default();
    assert_eq!(stats.quarantined_records, 0);
    assert_eq!(stats.dependent_skipped, 0);
}
