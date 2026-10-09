//! redb snapshots written before the value codec replaced `bincode` must
//! still load.
//!
//! The database is built table by table with the exact bytes that
//! `bincode` 1.3.3 wrote (tests/fixtures/legacy-bincode-v1.hex), using the
//! same table names and key types as the store, so it is the file an older
//! release left on disk.

use redb::{Database, TableDefinition};
use schema::{ClaimType, Relation, Stance};
use store::{AnnTuningConfig, DiskBackedStore, FileWal, InMemoryStore, StoreIndexStats};
use tempfile::TempDir;

const LEGACY_HEX: &str = include_str!("fixtures/legacy-bincode-v1.hex");

fn legacy(name: &str) -> Vec<u8> {
    let line = LEGACY_HEX
        .lines()
        .find_map(|l| l.strip_prefix(&format!("{name}=")))
        .unwrap_or_else(|| panic!("fixture {name} missing"));
    (0..line.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&line[i..i + 2], 16).unwrap())
        .collect()
}

fn write_legacy_snapshot(path: &std::path::Path) {
    let claims: TableDefinition<&str, &[u8]> = TableDefinition::new("dash_claims");
    let evidence: TableDefinition<&str, &[u8]> = TableDefinition::new("dash_evidence");
    let edges: TableDefinition<&str, &[u8]> = TableDefinition::new("dash_edges");
    let vectors: TableDefinition<&str, &[u8]> = TableDefinition::new("dash_claim_vectors");
    let dims: TableDefinition<&str, u64> = TableDefinition::new("dash_tenant_dims");
    let set: TableDefinition<(&str, &str), ()> = TableDefinition::new("dash_tenant_claims_set");
    let commits: TableDefinition<&str, &[u8]> = TableDefinition::new("dash_batch_commits");
    let stats: TableDefinition<&str, &[u8]> = TableDefinition::new("dash_stats");
    let hwm: TableDefinition<&str, u64> = TableDefinition::new("dash_hwm");

    let db = Database::create(path).unwrap();
    let txn = db.begin_write().unwrap();
    {
        let mut t = txn.open_table(claims).unwrap();
        t.insert("claim-full", legacy("claim_full").as_slice())
            .unwrap();
        t.insert("claim-min", legacy("claim_min").as_slice())
            .unwrap();
        let mut t = txn.open_table(set).unwrap();
        t.insert(("tenant-a", "claim-full"), ()).unwrap();
        t.insert(("tenant-b", "claim-min"), ()).unwrap();
        let mut t = txn.open_table(evidence).unwrap();
        t.insert("claim-full", legacy("evidence").as_slice())
            .unwrap();
        let mut t = txn.open_table(edges).unwrap();
        t.insert("claim-full", legacy("edges").as_slice()).unwrap();
        let mut t = txn.open_table(vectors).unwrap();
        t.insert("claim-full", legacy("vector").as_slice()).unwrap();
        let mut t = txn.open_table(dims).unwrap();
        t.insert("tenant-a", 4u64).unwrap();
        let mut t = txn.open_table(commits).unwrap();
        t.insert("commit-1", legacy("commit").as_slice()).unwrap();
        let mut t = txn.open_table(stats).unwrap();
        t.insert("stats", legacy("stats").as_slice()).unwrap();
        let mut t = txn.open_table(hwm).unwrap();
        t.insert("hwm", 7u64).unwrap();
    }
    txn.commit().unwrap();
}

#[test]
fn snapshot_written_by_the_bincode_release_reads_back_every_table() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("legacy.redb");
    write_legacy_snapshot(&path);

    let disk = DiskBackedStore::new(&path).unwrap();
    let claim = disk.get_claim("claim-full").unwrap().unwrap();
    assert_eq!(
        claim.canonical_text,
        "Café revenue grew 12% in Q3 — ünïcödé ✓"
    );
    assert_eq!(claim.claim_type, Some(ClaimType::Temporal));
    assert_eq!(claim.event_time_unix, Some(-86_400));
    assert_eq!(claim.valid_to, Some(i64::MAX));
    assert_eq!(claim.entities, vec!["Café".to_string(), "Q3".to_string()]);
    assert_eq!(
        disk.get_claim("claim-min").unwrap().unwrap(),
        schema::claim_builder("claim-min", "tenant-b", "plain", 0.5)
    );

    let evidence = disk.get_evidence_blob("claim-full").unwrap().unwrap();
    assert_eq!(evidence.len(), 2);
    assert_eq!(evidence[0].stance, Stance::Contradicts);
    assert_eq!(evidence[0].span_end, Some(u32::MAX));
    assert_eq!(
        evidence[0].extraction_model.as_deref(),
        Some("extractor-v2")
    );
    assert_eq!(evidence[1].chunk_id, None);

    let edges = disk.get_edge_blob("claim-full").unwrap().unwrap();
    assert_eq!(edges.len(), 1);
    assert_eq!(edges[0].relation, Relation::DependsOn);
    assert_eq!(edges[0].strength, -0.5);
    assert_eq!(edges[0].reason_codes, vec!["temporal", "entity-overlap"]);

    assert_eq!(
        disk.get_vector("claim-full").unwrap().unwrap(),
        vec![0.1f32, -2.5, f32::MIN_POSITIVE, 1e30]
    );
    let commit = disk.get_batch_commit("commit-1").unwrap().unwrap();
    assert_eq!(commit.ts_unix_ms, 1_700_000_000_123);
    assert_eq!(commit.claim_ids, vec!["claim-full", "claim-min"]);
    assert_eq!(
        disk.get_stats().unwrap(),
        StoreIndexStats {
            tenant_count: 2,
            claim_count: 2,
            vector_count: 1,
            inverted_terms: 11,
            entity_terms: 2,
            temporal_buckets: 1,
            ann_vector_buckets: 1,
            vector_index_bytes: 4096,
        }
    );
    assert_eq!(disk.get_tenant_dim("tenant-a").unwrap(), Some(4));
    assert_eq!(disk.high_water_mark().unwrap(), 7);
}

#[test]
fn cold_start_bulk_loads_a_legacy_snapshot() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("legacy.redb");
    write_legacy_snapshot(&path);
    let mut wal = FileWal::open(dir.path().join("empty.wal")).unwrap();

    let (store, stats) =
        InMemoryStore::load_from_disk_and_wal(&path, &mut wal, AnnTuningConfig::default())
            .expect("legacy snapshot must load");
    assert_eq!(stats.claims_loaded, 2);
    assert_eq!(store.claims_len(), 2);
    assert!(matches!(store.disk_status(), store::DiskStatus::Available));
    assert_eq!(store.claim_count_for_tenant("tenant-a"), 1);
    assert_eq!(store.claim_count_for_tenant("tenant-b"), 1);
    assert_eq!(store.claim_by_id("claim-full").unwrap().confidence, 0.875);
    assert_eq!(store.edges_for_claim("claim-full").len(), 1);
    assert_eq!(
        store
            .batch_commit_metadata("commit-1")
            .unwrap()
            .payload_fingerprint,
        "sha256:abcdef"
    );
}

#[test]
fn new_writes_into_a_legacy_snapshot_coexist_with_old_rows() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("legacy.redb");
    write_legacy_snapshot(&path);

    {
        let disk = DiskBackedStore::new(&path).unwrap();
        // Rewrite one claim in the new format; leave the other legacy.
        let mut claim = disk.get_claim("claim-min").unwrap().unwrap();
        claim.canonical_text = "rewritten".into();
        disk.put_claim(&claim).unwrap();
        disk.upsert_edge(&schema::ClaimEdge {
            edge_id: "edge-2".into(),
            from_claim_id: "claim-full".into(),
            to_claim_id: "claim-min".into(),
            relation: Relation::Supports,
            strength: 0.75,
            reason_codes: vec![],
            created_at: None,
        })
        .unwrap();
    }

    // The rewritten rows now carry the version header.
    {
        let db = Database::open(&path).unwrap();
        let txn = db.begin_read().unwrap();
        let claims: TableDefinition<&str, &[u8]> = TableDefinition::new("dash_claims");
        let table = txn.open_table(claims).unwrap();
        let new_row = table.get("claim-min").unwrap().unwrap().value().to_vec();
        assert_eq!(&new_row[..8], b"DASHv2\x00\xff");
        let old_row = table.get("claim-full").unwrap().unwrap().value().to_vec();
        assert_eq!(old_row, legacy("claim_full"));
    }

    let disk = DiskBackedStore::new(&path).unwrap();
    assert_eq!(
        disk.get_claim("claim-min").unwrap().unwrap().canonical_text,
        "rewritten"
    );
    assert_eq!(
        disk.get_claim("claim-full").unwrap().unwrap().confidence,
        0.875
    );
    // The edge blob was read in the legacy format, extended, and written
    // back in the new one.
    let edges = disk.get_edge_blob("claim-full").unwrap().unwrap();
    assert_eq!(edges.len(), 2);
    assert_eq!(edges[0].relation, Relation::DependsOn);
    assert_eq!(edges[1].edge_id, "edge-2");
}
