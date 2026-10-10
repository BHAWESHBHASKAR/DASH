//! WAL and snapshot compatibility: the current code replays the WAL and
//! snapshot every earlier release wrote, holds exactly the data that was
//! written to it, migrates the files forward (generation file, checkpoint
//! in the current record format), and the documented downgrade constraints
//! hold (checked against the older readers' rules, see
//! `dash_compat::old_readers`).

mod support;

use std::collections::{BTreeMap, BTreeSet};
use std::fs;

use dash_compat::{Era, FIXTURES, Fixture, ingest_requests, old_readers, record_kinds, repo_root};
use serde_json::Value;
use store::{FileWal, InMemoryStore, inspect_wal_file};
use support::load_strict;

#[test]
fn every_fixture_directory_is_registered() {
    let mut on_disk: Vec<String> = fs::read_dir(dash_compat::fixtures_dir())
        .expect("fixtures dir")
        .map(|entry| {
            entry
                .expect("entry")
                .file_name()
                .to_string_lossy()
                .to_string()
        })
        .collect();
    on_disk.sort();
    let mut registered: Vec<String> = FIXTURES.iter().map(|f| f.label.to_string()).collect();
    registered.sort();
    assert_eq!(
        on_disk, registered,
        "add every fixture directory to dash_compat::FIXTURES (and nothing else)"
    );
    for fixture in FIXTURES {
        let meta = fs::read_to_string(fixture.path("FIXTURE.txt")).expect("FIXTURE.txt");
        assert!(
            meta.contains(&format!("label: {}", fixture.label)),
            "{meta}"
        );
        assert!(meta.contains("commit: "), "{meta}");
    }
}

#[test]
fn old_wal_and_snapshot_replay_strictly_without_quarantine() {
    for fixture in FIXTURES {
        let state = fixture.scratch_state();
        for path in [state.wal(), state.snapshot()] {
            let inspection = inspect_wal_file(&path).expect("inspect");
            assert!(
                inspection.invalid_lines.is_empty() && inspection.torn_tail_line.is_none(),
                "{}: {} has invalid lines: {:?}",
                fixture.label,
                path.display(),
                inspection.first_invalid()
            );
            match fixture.era {
                // 0.2 wrote only unchecksummed records.
                Era::V0_2 => assert_eq!(
                    inspection.legacy_records,
                    inspection.valid_records,
                    "{}: {}",
                    fixture.label,
                    path.display()
                ),
                Era::V0_3 => assert_eq!(inspection.legacy_records, 0, "{}", fixture.label),
            }
        }
        let (store, stats, _) = load_strict(&state);
        assert_eq!(stats.replay.quarantined_records, 0, "{}", fixture.label);
        assert_eq!(stats.replay.dependent_skipped, 0, "{}", fixture.label);
        assert!(
            stats.replay.snapshot_records > 0,
            "{}: snapshot replayed",
            fixture.label
        );
        assert!(
            stats.replay.wal_records > 0,
            "{}: WAL tail replayed",
            fixture.label
        );
        assert_eq!(
            store.claims_len(),
            fixture.expected_claims_total(),
            "{}: claims the old build reported",
            fixture.label
        );
    }
}

/// Last-write-wins view of the dataset: what a correct store holds after
/// all the requests (and, for releases with deletes, the deletes).
struct ExpectedData {
    claims: BTreeMap<String, Value>,
    evidence: BTreeMap<(String, String), Value>,
    edges: BTreeSet<(String, String, String)>,
    vectors: BTreeMap<String, usize>,
}

fn expected_data(fixture: &Fixture) -> ExpectedData {
    let mut data = ExpectedData {
        claims: BTreeMap::new(),
        evidence: BTreeMap::new(),
        edges: BTreeSet::new(),
        vectors: BTreeMap::new(),
    };
    let mut bundles = Vec::new();
    for row in ingest_requests() {
        match row["body"]["items"].as_array() {
            Some(items) => bundles.extend(items.iter().cloned()),
            None => bundles.push(row["body"].clone()),
        }
    }
    for bundle in bundles {
        let claim = bundle["claim"].clone();
        let claim_id = claim["claim_id"].as_str().expect("claim_id").to_string();
        let dim = bundle["claim_embedding"].as_array().map(Vec::len);
        if let Some(dim) = dim {
            data.vectors.insert(claim_id.clone(), dim);
        }
        data.claims.insert(claim_id.clone(), claim);
        for ev in bundle["evidence"].as_array().into_iter().flatten() {
            let id = ev["evidence_id"].as_str().expect("evidence_id").to_string();
            data.evidence.insert((claim_id.clone(), id), ev.clone());
        }
        for edge in bundle["edges"].as_array().into_iter().flatten() {
            data.edges.insert((
                edge["from_claim_id"].as_str().expect("from").to_string(),
                edge["to_claim_id"].as_str().expect("to").to_string(),
                edge["relation"].as_str().expect("relation").to_string(),
            ));
        }
    }
    if fixture.has_deletes {
        // tests/compat/dataset/deletes.jsonl
        let gone: BTreeSet<String> = data
            .claims
            .iter()
            .filter(|(id, claim)| *id == "a-c06" || claim["tenant_id"] == "tenant-hash")
            .map(|(id, _)| id.clone())
            .collect();
        data.claims.retain(|id, _| !gone.contains(id));
        data.vectors.retain(|id, _| !gone.contains(id));
        data.evidence
            .retain(|(claim, id), _| !gone.contains(claim) && id != "a-e02-2");
        data.edges
            .retain(|(from, to, _)| !gone.contains(from) && !gone.contains(to));
    }
    data
}

fn assert_fields_match(label: &str, what: &str, expected: &Value, actual: &Value) {
    for (key, want) in expected.as_object().expect("object") {
        let got = &actual[key];
        let same = match (want.as_f64(), got.as_f64()) {
            (Some(a), Some(b)) => (a - b).abs() < 1e-6,
            _ => want == got,
        };
        assert!(
            same,
            "{label}: {what}.{key} is {got}, was written as {want}"
        );
    }
}

#[test]
fn upgraded_store_holds_exactly_the_data_that_was_written() {
    for fixture in FIXTURES {
        let state = fixture.scratch_state();
        let (store, _, _) = load_strict(&state);
        let expected = expected_data(fixture);
        let mut actual_claims = BTreeSet::new();
        for tenant in store.tenant_ids() {
            for claim in store.claims_for_tenant(&tenant) {
                actual_claims.insert(claim.claim_id.clone());
            }
        }
        let wanted: BTreeSet<String> = expected.claims.keys().cloned().collect();
        assert_eq!(actual_claims, wanted, "{}: claim ids", fixture.label);
        for (claim_id, want) in &expected.claims {
            let tenant = want["tenant_id"].as_str().expect("tenant");
            let claim = store
                .claims_for_tenant(tenant)
                .into_iter()
                .find(|c| &c.claim_id == claim_id)
                .expect("claim present");
            let got = serde_json::to_value(&claim).expect("serialize claim");
            assert_fields_match(fixture.label, claim_id, want, &got);
            // Evidence: exactly one row per evidence id (0.2 appended a
            // second copy on re-ingest; replay keeps the last one).
            let rows = store.evidence_for_claim(claim_id);
            let ids: Vec<String> = rows.iter().map(|e| e.evidence_id.clone()).collect();
            let unique: BTreeSet<String> = ids.iter().cloned().collect();
            assert_eq!(
                ids.len(),
                unique.len(),
                "{}: duplicate evidence {ids:?}",
                fixture.label
            );
            let want_ids: BTreeSet<String> = expected
                .evidence
                .keys()
                .filter(|(claim, _)| claim == claim_id)
                .map(|(_, id)| id.clone())
                .collect();
            assert_eq!(
                unique, want_ids,
                "{}: evidence of {claim_id}",
                fixture.label
            );
            for row in rows {
                let want = &expected.evidence[&(claim_id.clone(), row.evidence_id.clone())];
                let got = serde_json::to_value(&row).expect("serialize evidence");
                assert_fields_match(fixture.label, &row.evidence_id, want, &got);
            }
        }
        let mut edges = BTreeSet::new();
        for claim_id in expected.claims.keys() {
            for edge in store.edges_for_claim(claim_id) {
                let relation = serde_json::to_value(&edge.relation).expect("relation");
                edges.insert((
                    edge.from_claim_id.clone(),
                    edge.to_claim_id.clone(),
                    relation.as_str().expect("relation string").to_string(),
                ));
            }
        }
        assert_eq!(edges, expected.edges, "{}: edges", fixture.label);
        for (claim_id, dim) in &expected.vectors {
            let tenant = expected.claims[claim_id]["tenant_id"]
                .as_str()
                .expect("tenant");
            assert_eq!(
                store.tenant_vector_dim(tenant),
                Some(*dim),
                "{}",
                fixture.label
            );
        }
    }
}

#[test]
fn a_wal_without_a_generation_file_gets_one_on_first_open() {
    for fixture in FIXTURES {
        let state = fixture.scratch_state();
        let had_generation = state.generation_file().exists();
        assert_eq!(
            had_generation,
            fixture.era != Era::V0_2,
            "{}: 0.2 had no WAL generation file",
            fixture.label
        );
        let first = FileWal::open(state.wal()).expect("open").generation();
        assert!(state.generation_file().exists(), "{}", fixture.label);
        let second = FileWal::open(state.wal()).expect("reopen").generation();
        assert_eq!(
            first, second,
            "{}: the generation is stable across restarts",
            fixture.label
        );
    }
}

#[test]
fn a_checkpoint_rewrites_everything_in_the_current_format_and_reloads() {
    for fixture in FIXTURES {
        let state = fixture.scratch_state();
        let generation_before;
        let claims_before;
        {
            let (store, _, mut wal) = load_strict(&state);
            generation_before = wal.generation();
            claims_before = store.claims_len();
            store.checkpoint_and_compact(&mut wal).expect("checkpoint");
            assert_ne!(
                wal.generation(),
                generation_before,
                "{}: new lineage",
                fixture.label
            );
        }
        let kinds = record_kinds(&state.snapshot());
        assert!(!kinds.is_empty(), "{}", fixture.label);
        for kind in &kinds {
            assert!(
                matches!(kind.as_str(), "C2" | "E2" | "G2" | "V2" | "B2"),
                "{}: snapshot record kind {kind} after a checkpoint",
                fixture.label
            );
        }
        assert!(
            record_kinds(&state.wal()).is_empty(),
            "{}: WAL truncated",
            fixture.label
        );
        let inspection = inspect_wal_file(state.snapshot()).expect("inspect");
        assert_eq!(inspection.legacy_records, 0, "{}", fixture.label);
        assert_eq!(inspection.checksum_failures(), 0, "{}", fixture.label);
        let wal = FileWal::open(state.wal()).expect("reopen");
        let (store, stats) = InMemoryStore::load_from_wal_with_policy(
            &wal,
            store::AnnTuningConfig::default(),
            store::ReplayPolicy::Strict,
        )
        .expect("reload after checkpoint");
        assert_eq!(stats.replay.quarantined_records, 0, "{}", fixture.label);
        assert_eq!(store.claims_len(), claims_before, "{}", fixture.label);
    }
}

/// Downgrade 0.3 -> 0.2 is not possible in place: every record the current
/// code writes (appends and checkpoints) is checksummed, and the 0.2 reader
/// fails its whole replay on a kind it does not know. Rollback is restoring
/// the backup taken before the upgrade (docs/operations/upgrades.md).
#[test]
fn a_0_2_build_cannot_read_what_the_current_code_writes() {
    let fixture = FIXTURES
        .iter()
        .find(|f| f.era == Era::V0_2)
        .expect("0.2 fixture");
    let state = fixture.scratch_state();
    // Before the upgrade touches anything, 0.2 can read its own files.
    for kind in record_kinds(&state.wal())
        .iter()
        .chain(&record_kinds(&state.snapshot()))
    {
        assert!(
            old_readers::V0_2_WAL_KINDS.contains(&kind.as_str()),
            "{kind}"
        );
    }
    {
        let (store, _, mut wal) = load_strict(&state);
        store.checkpoint_and_compact(&mut wal).expect("checkpoint");
    }
    let unreadable = record_kinds(&state.snapshot())
        .into_iter()
        .filter(|kind| !old_readers::V0_2_WAL_KINDS.contains(&kind.as_str()))
        .count();
    assert!(
        unreadable > 0,
        "a checkpointed snapshot is unreadable by 0.2"
    );
    let guide = fs::read_to_string(repo_root().join("docs/operations/upgrades.md")).expect("guide");
    assert!(
        guide.contains("restore the backup taken before the upgrade"),
        "the upgrade guide documents the 0.2 rollback procedure"
    );
}

/// Downgrade from a release with deletes to a 0.3 build without them: a
/// tombstone (`T2`) fails the older build's replay, so the documented rule is
/// "checkpoint first". After a checkpoint no tombstone is left and every
/// record is one the older build reads; the deleted data stays gone.
#[test]
fn after_a_checkpoint_no_tombstone_is_left_for_an_older_reader() {
    for fixture in FIXTURES.iter().filter(|f| f.has_deletes) {
        let state = fixture.scratch_state();
        assert!(
            record_kinds(&state.wal()).iter().any(|kind| kind == "T2"),
            "{}: the fixture's WAL tail holds the deletes",
            fixture.label
        );
        {
            let (store, _, mut wal) = load_strict(&state);
            store.checkpoint_and_compact(&mut wal).expect("checkpoint");
        }
        for path in [state.snapshot(), state.wal()] {
            for kind in record_kinds(&path) {
                assert!(
                    old_readers::V0_3_PRE_DELETE_WAL_KINDS.contains(&kind.as_str()),
                    "{}: {kind} in {} after the checkpoint",
                    fixture.label,
                    path.display()
                );
            }
        }
        let wal = FileWal::open(state.wal()).expect("reopen");
        let store = InMemoryStore::load_from_wal(&wal).expect("reload");
        assert!(
            store.claims_for_tenant("tenant-hash").is_empty(),
            "{}",
            fixture.label
        );
        assert!(
            store
                .claims_for_tenant("tenant-a")
                .iter()
                .all(|c| c.claim_id != "a-c06"),
            "{}",
            fixture.label
        );
        let guide =
            fs::read_to_string(repo_root().join("docs/operations/upgrades.md")).expect("guide");
        assert!(
            guide.contains("checkpoint before the downgrade"),
            "guide documents it"
        );
    }
}
