//! redb mirror compatibility: rows written by 0.2 (`bincode` 1.x layout, no
//! header) load with the current codec, are rewritten with the `DASHv2`
//! header as they change, mixed files load, and the documented downgrade
//! rule (a 0.2 build cannot decode headered rows: delete the redb file, it
//! is rebuilt from the WAL) follows from the 0.2 decoding rules.

mod support;

use std::collections::BTreeMap;
use std::path::Path;

use dash_compat::{Era, FIXTURES, old_readers};
use redb::{Database, ReadableTable, TableDefinition};
use schema::{Claim, Evidence, Stance};
use store::{AnnTuningConfig, DiskBackedStore, DiskStatus, FileWal, InMemoryStore};
use support::{diff_against_recorded, load_strict, run_retrieves};

const CLAIMS: TableDefinition<&str, &[u8]> = TableDefinition::new("dash_claims");
const EVIDENCE: TableDefinition<&str, &[u8]> = TableDefinition::new("dash_evidence");
const HEADER: &[u8; 8] = b"DASHv2\x00\xff";

/// Raw values of one table, by key.
fn raw_rows(path: &Path, table: TableDefinition<&str, &[u8]>) -> BTreeMap<String, Vec<u8>> {
    let db = Database::open(path).expect("open redb");
    let txn = db.begin_read().expect("read txn");
    let table = txn.open_table(table).expect("open table");
    let mut out = BTreeMap::new();
    for row in table.iter().expect("iter") {
        let (key, value) = row.expect("row");
        out.insert(key.value().to_string(), value.value().to_vec());
    }
    out
}

fn headered(bytes: &[u8]) -> bool {
    bytes.starts_with(HEADER)
}

#[test]
fn rows_carry_the_header_of_the_release_that_wrote_them() {
    for fixture in FIXTURES {
        let state = fixture.scratch_state();
        let claims = raw_rows(&state.redb(), CLAIMS);
        let evidence = raw_rows(&state.redb(), EVIDENCE);
        assert!(
            !claims.is_empty() && !evidence.is_empty(),
            "{}",
            fixture.label
        );
        for value in claims.values().chain(evidence.values()) {
            assert_eq!(
                headered(value),
                fixture.era != Era::V0_2,
                "{}",
                fixture.label
            );
        }
    }
}

#[test]
fn every_old_row_decodes_with_the_current_codec() {
    for fixture in FIXTURES {
        let state = fixture.scratch_state();
        let (store, _, _) = load_strict(&state);
        let disk = DiskBackedStore::new(state.redb()).expect("open redb");
        for tenant in store.tenant_ids() {
            for claim in store.claims_for_tenant(&tenant) {
                let row = disk
                    .get_claim(&claim.claim_id)
                    .expect("decode claim row")
                    .expect("claim row present");
                assert_eq!(
                    row, claim,
                    "{}: claim row equals the WAL state",
                    fixture.label
                );
                let blob = disk
                    .get_evidence_blob(&claim.claim_id)
                    .expect("decode evidence blob")
                    .unwrap_or_default();
                for item in store.evidence_for_claim(&claim.claim_id) {
                    assert!(
                        blob.iter().any(|row| row.evidence_id == item.evidence_id),
                        "{}: evidence {} mirrored",
                        fixture.label,
                        item.evidence_id
                    );
                }
            }
        }
        disk.get_stats().expect("stats row decodes");
    }
}

/// The cold-start path that bulk-loads redb and replays the WAL tail over it
/// accepts an old redb file and serves what the old release recorded.
#[test]
fn bulk_load_from_an_old_redb_plus_wal_serves_the_recorded_answers() {
    for fixture in FIXTURES {
        let state = fixture.scratch_state();
        let mut wal = FileWal::open(state.wal()).expect("open WAL");
        let (store, _) = InMemoryStore::load_from_disk_and_wal(
            state.redb(),
            &mut wal,
            AnnTuningConfig::default(),
        )
        .expect("load redb + WAL");
        assert_eq!(
            store.disk_status(),
            &DiskStatus::Available,
            "{}",
            fixture.label
        );
        assert_eq!(
            store.claims_len(),
            fixture.expected_claims_total(),
            "{}",
            fixture.label
        );
        let answers = run_retrieves(&store, Some(&state.segments()));
        let diffs = diff_against_recorded(fixture, &answers);
        assert!(diffs.is_empty(), "{}:\n{}", fixture.label, diffs.join("\n"));
    }
}

/// Upgrade migrates rows forward lazily: a changed row is rewritten with the
/// header, untouched rows stay legacy, and the mixed file still loads.
#[test]
fn changed_rows_are_rewritten_with_the_header_and_mixed_files_load() {
    let fixture = FIXTURES
        .iter()
        .find(|f| f.era == Era::V0_2)
        .expect("0.2 fixture");
    let state = fixture.scratch_state();
    {
        let (store, _, _) = load_strict(&state);
        let mut store = store.attach_disk(state.redb());
        assert_eq!(store.disk_status(), &DiskStatus::Available);
        let mut claim = store
            .claims_for_tenant("tenant-a")
            .into_iter()
            .find(|c| c.claim_id == "a-c01")
            .expect("a-c01");
        claim.canonical_text = "Company X completed its acquisition of Company Y".into();
        let evidence = Evidence {
            evidence_id: "a-e01-upgrade".into(),
            claim_id: "a-c01".into(),
            source_id: "news://after-upgrade".into(),
            stance: Stance::Supports,
            source_quality: 0.8,
            chunk_id: None,
            span_start: None,
            span_end: None,
            doc_id: None,
            extraction_model: None,
            ingested_at: None,
        };
        store
            .ingest_bundle(claim, vec![evidence], vec![])
            .expect("write after upgrade");
    }
    let claims = raw_rows(&state.redb(), CLAIMS);
    let evidence = raw_rows(&state.redb(), EVIDENCE);
    assert!(
        headered(&claims["a-c01"]),
        "updated claim row rewritten in the current format"
    );
    assert!(
        headered(&evidence["a-c01"]),
        "updated evidence blob rewritten"
    );
    assert!(
        !headered(&claims["a-c02"]),
        "untouched rows are left as they were"
    );
    // A mixed file loads.
    let disk = DiskBackedStore::new(state.redb()).expect("open");
    let a01: Claim = disk.get_claim("a-c01").expect("decode").expect("row");
    assert!(a01.canonical_text.contains("completed"));
    assert!(disk.get_claim("a-c02").expect("decode legacy").is_some());
    drop(disk);
    let mut wal = FileWal::open(state.wal()).expect("open WAL");
    InMemoryStore::load_from_disk_and_wal(state.redb(), &mut wal, AnnTuningConfig::default())
        .expect("mixed redb loads");
}

/// A full rewrite (what a follower resync does) leaves no legacy row.
#[test]
fn a_full_rewrite_migrates_every_row() {
    let fixture = FIXTURES
        .iter()
        .find(|f| f.era == Era::V0_2)
        .expect("0.2 fixture");
    let state = fixture.scratch_state();
    let (store, _, _) = load_strict(&state);
    let mut live = store.clone_detached().attach_disk(state.redb());
    live.replace_state_from(store)
        .expect("rewrite redb from state");
    drop(live);
    for (key, value) in raw_rows(&state.redb(), CLAIMS)
        .into_iter()
        .chain(raw_rows(&state.redb(), EVIDENCE))
    {
        assert!(headered(&value), "{key} still legacy after a full rewrite");
    }
}

/// Downgrade 0.3 -> 0.2 with the redb file kept: 0.2 reads a claim's
/// evidence blob on every evidence write and cannot decode a headered blob,
/// so writes to any claim the current code touched would fail. The rule is
/// to delete the redb file before starting 0.2 (it is a mirror rebuilt from
/// the WAL; 0.2 never reads it at startup).
#[test]
fn a_0_2_build_cannot_decode_rows_the_current_code_wrote() {
    for fixture in FIXTURES {
        let state = fixture.scratch_state();
        for (claim, blob) in raw_rows(&state.redb(), EVIDENCE) {
            assert_eq!(
                old_readers::v0_2_decodes_redb_blob(&blob),
                fixture.era == Era::V0_2,
                "{}: evidence blob of {claim}",
                fixture.label
            );
        }
    }
    let guide =
        std::fs::read_to_string(dash_compat::repo_root().join("docs/operations/upgrades.md"))
            .expect("guide");
    assert!(
        guide.contains("delete the redb file"),
        "the guide documents the redb downgrade rule"
    );
}
