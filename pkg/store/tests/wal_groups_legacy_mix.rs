//! Legacy-record quarantine composes with commit-group resolution: a WAL
//! mixing legacy records, checksummed records and commit groups loads, a
//! quarantined record inside a group does not break the group, and an
//! unterminated trailing group is discarded.

use std::path::Path;

use schema::{Claim, Evidence, Stance};
use store::{AnnTuningConfig, FileWal, InMemoryStore, ReplayPolicy};
use tempfile::TempDir;

fn claim(id: &str) -> Claim {
    Claim {
        claim_id: id.to_string(),
        tenant_id: "tenant-a".to_string(),
        canonical_text: format!("text of {id}"),
        confidence: 0.9,
        event_time_unix: None,
        entities: vec![],
        embedding_ids: vec![],
        claim_type: None,
        valid_from: None,
        valid_to: None,
        created_at: None,
        updated_at: None,
    }
}

fn evidence(id: &str, claim_id: &str) -> Evidence {
    Evidence {
        evidence_id: id.to_string(),
        claim_id: claim_id.to_string(),
        source_id: "src".to_string(),
        stance: Stance::Supports,
        source_quality: 0.9,
        chunk_id: None,
        span_start: None,
        span_end: None,
        doc_id: None,
        extraction_model: None,
        ingested_at: None,
    }
}

/// Lines a checksummed single-bundle commit group produces, plus the number
/// of lines each part contributes.
fn group_lines(dir: &Path, id: &str) -> Vec<String> {
    let path = dir.join(format!("scratch-{id}.wal"));
    let mut wal = FileWal::open(&path).unwrap();
    let mut store = InMemoryStore::new();
    store
        .ingest_atomic_persistent(
            &mut wal,
            claim(id),
            vec![evidence(&format!("e-{id}"), id)],
            vec![],
            None,
            1,
        )
        .unwrap();
    drop(wal);
    std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(str::to_string)
        .collect()
}

fn legacy_claim(id: &str) -> String {
    format!("C\t{id}\ttenant-a\ttext of {id}\t0.9\tnull\t\t\tnull\tnull\tnull\tnull\tnull")
}

fn legacy_evidence(id: &str, claim: &str) -> String {
    format!("E\t{id}\t{claim}\tsource-1\tsupports\t0.8")
}

fn load(path: &Path) -> (InMemoryStore, store::StoreLoadStats) {
    let wal = FileWal::open(path).unwrap();
    InMemoryStore::load_from_wal_with_policy(
        &wal,
        AnnTuningConfig::default(),
        ReplayPolicy::Lenient,
    )
    .expect("mixed wal must load")
}

#[test]
fn legacy_only_wal_without_groups_still_loads() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("legacy.wal");
    let lines = [
        legacy_claim("l1"),
        legacy_evidence("le1", "l1"),
        legacy_claim("l2"),
    ];
    std::fs::write(&path, lines.join("\n") + "\n").unwrap();
    let (store, stats) = load(&path);
    assert!(store.claim_by_id("l1").is_some() && store.claim_by_id("l2").is_some());
    assert_eq!(stats.evidence_loaded, 1);
}

#[test]
fn legacy_v2_and_grouped_records_load_together() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("mixed.wal");
    let mut lines = vec![legacy_claim("l1"), legacy_evidence("le1", "l1")];
    lines.extend(group_lines(dir.path(), "g1"));
    lines.push(legacy_claim("l2"));
    lines.extend(group_lines(dir.path(), "g2"));
    std::fs::write(&path, lines.join("\n") + "\n").unwrap();

    let (store, _) = load(&path);
    for id in ["l1", "l2", "g1", "g2"] {
        assert!(store.claim_by_id(id).is_some(), "{id} missing");
    }
    assert_eq!(store.claim_count_for_tenant("tenant-a"), 4);
}

#[test]
fn quarantined_claim_inside_a_group_keeps_the_rest_of_the_group() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("quarantine.wal");
    let g = group_lines(dir.path(), "g1");
    // g = [begin, claim, evidence, commit]; inject an unreadable legacy
    // claim (control character in the id) and its dependent evidence.
    let bad_claim = legacy_claim("c\\tbad");
    let bad_evidence = legacy_evidence("e-bad", "c\\tbad");
    let lines = [
        g[0].clone(),
        bad_claim.clone(),
        bad_evidence.clone(),
        g[1].clone(),
        g[2].clone(),
        g[3].clone(),
    ];
    std::fs::write(&path, lines.join("\n") + "\n").unwrap();

    let (store, stats) = load(&path);
    assert!(store.claim_by_id("g1").is_some(), "good member survives");
    assert!(store.claim_by_id("c\tbad").is_none());
    assert!(stats.replay.quarantined_records >= 1, "{:?}", stats.replay);
    let mut quarantine = path.clone().into_os_string();
    quarantine.push(".quarantine");
    let quarantined = std::fs::read_to_string(quarantine).unwrap();
    assert!(quarantined.contains("c\\tbad"));
}

#[test]
fn unterminated_trailing_group_is_discarded_while_legacy_records_survive() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("torn.wal");
    let g = group_lines(dir.path(), "g1");
    let mut lines = vec![legacy_claim("l1")];
    lines.extend(g[..g.len() - 1].iter().cloned()); // no closing commit
    std::fs::write(&path, lines.join("\n") + "\n").unwrap();

    let (store, _) = load(&path);
    assert!(store.claim_by_id("l1").is_some());
    assert!(store.claim_by_id("g1").is_none(), "torn group must vanish");
}
