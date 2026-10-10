//! Deletes: claim, evidence and tenant tombstones through the WAL, the redb
//! mirror, replay, checkpoint, replication apply and resync.
//!
//! Every test builds its own WAL in a temp dir; no clocks, no sleeps.

use std::collections::BTreeMap;
use std::path::Path;

use schema::{Claim, ClaimEdge, Evidence, Relation, RetrievalRequest, Stance, StanceMode};
use store::{AnnTuningConfig, FileWal, InMemoryStore, ReplayPolicy, StoreError, Tombstone};
use tempfile::TempDir;

const TS: u64 = 1_700_000_000_000;

fn claim(tenant: &str, id: &str, text: &str) -> Claim {
    Claim {
        claim_id: id.to_string(),
        tenant_id: tenant.to_string(),
        canonical_text: text.to_string(),
        confidence: 0.9,
        event_time_unix: Some(1_000),
        entities: vec!["Acme".to_string()],
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
        source_id: format!("src-{id}"),
        stance: Stance::Supports,
        source_quality: 0.8,
        chunk_id: None,
        span_start: None,
        span_end: None,
        doc_id: None,
        extraction_model: None,
        ingested_at: None,
    }
}

fn edge(id: &str, from: &str, to: &str) -> ClaimEdge {
    ClaimEdge {
        edge_id: id.to_string(),
        from_claim_id: from.to_string(),
        to_claim_id: to.to_string(),
        relation: Relation::Supports,
        strength: 0.5,
        reason_codes: vec![],
        created_at: None,
    }
}

fn claim_tombstone(tenant: &str, id: &str) -> Tombstone {
    Tombstone::Claim {
        tenant_id: tenant.to_string(),
        claim_id: id.to_string(),
    }
}

fn evidence_tombstone(tenant: &str, id: &str) -> Tombstone {
    Tombstone::Evidence {
        tenant_id: tenant.to_string(),
        evidence_id: id.to_string(),
    }
}

fn tenant_tombstone(tenant: &str) -> Tombstone {
    Tombstone::Tenant {
        tenant_id: tenant.to_string(),
    }
}

/// One atomic ingest: claim, evidence, edges and an optional vector.
type Write = (Claim, Vec<Evidence>, Vec<ClaimEdge>, Option<Vec<f32>>);

/// Tenant `t1`: `a` (2 evidence, vector, edge a->b), `b`, `c` (edge c->a).
/// Tenant `t2`: `x` with an evidence id that also exists in `t1`.
fn seed(store: &mut InMemoryStore, wal: &mut FileWal) {
    let writes: Vec<Write> = vec![
        (
            claim("t1", "b", "beta statement about acme"),
            vec![evidence("eb", "b")],
            vec![],
            Some(vec![0.0, 1.0, 0.0]),
        ),
        (
            claim("t1", "a", "alpha statement about acme"),
            vec![evidence("ea1", "a"), evidence("shared", "a")],
            vec![edge("a-b", "a", "b")],
            Some(vec![1.0, 0.0, 0.0]),
        ),
        (
            claim("t1", "c", "gamma statement about acme"),
            vec![evidence("ec", "c")],
            vec![edge("c-a", "c", "a")],
            None,
        ),
        (
            claim("t2", "x", "other tenant statement about acme"),
            vec![evidence("shared", "x")],
            vec![],
            Some(vec![0.5, 0.5]),
        ),
    ];
    for (i, (c, e, g, v)) in writes.into_iter().enumerate() {
        store
            .ingest_atomic_persistent(wal, c, e, g, v, TS + i as u64)
            .unwrap();
    }
}

/// Everything observable about a store, for comparing two of them.
fn dump(store: &InMemoryStore) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    for tenant in store.tenant_ids() {
        let mut claims = store.claims_for_tenant(&tenant);
        claims.sort_by(|a, b| a.claim_id.cmp(&b.claim_id));
        for c in claims {
            let mut evidence: Vec<String> = store
                .evidence_for_claim(&c.claim_id)
                .iter()
                .map(|e| e.evidence_id.clone())
                .collect();
            evidence.sort();
            let mut edges: Vec<String> = store
                .edges_for_claim(&c.claim_id)
                .iter()
                .map(|e| e.edge_id.clone())
                .collect();
            edges.sort();
            out.insert(
                format!("{tenant}/{}", c.claim_id),
                format!("{c:?} ev={evidence:?} edges={edges:?}"),
            );
        }
        out.insert(
            format!("{tenant}/#dim"),
            format!("{:?}", store.tenant_vector_dim(&tenant)),
        );
        let hits: Vec<String> = store
            .retrieve(&request(&tenant, "statement acme"))
            .into_iter()
            .map(|r| r.claim_id)
            .collect();
        out.insert(format!("{tenant}/#retrieve"), format!("{hits:?}"));
    }
    out.insert("#claims".into(), store.claims_len().to_string());
    out
}

fn request(tenant: &str, query: &str) -> RetrievalRequest {
    RetrievalRequest {
        tenant_id: tenant.to_string(),
        query: query.to_string(),
        top_k: 50,
        stance_mode: StanceMode::Balanced,
    }
}

fn reload(wal_path: &Path) -> InMemoryStore {
    let wal = FileWal::open(wal_path).unwrap();
    let (store, _) = InMemoryStore::load_from_wal_with_policy(
        &wal,
        AnnTuningConfig::default(),
        ReplayPolicy::Strict,
    )
    .unwrap();
    store
}

fn file_text(path: &Path) -> String {
    std::fs::read(path)
        .map(|b| String::from_utf8_lossy(&b).into_owned())
        .unwrap_or_default()
}

#[test]
fn claim_delete_removes_vector_evidence_and_edges_in_both_directions() {
    let dir = TempDir::new().unwrap();
    let wal_path = dir.path().join("d.wal");
    let mut wal = FileWal::open(&wal_path).unwrap();
    let mut store = InMemoryStore::new();
    seed(&mut store, &mut wal);

    let outcome = store
        .delete_persistent(&mut wal, claim_tombstone("t1", "a"), TS + 10)
        .unwrap();
    assert!(outcome.deleted());
    assert_eq!(outcome.disk_error, None);
    assert_eq!(
        (
            outcome.stats.claims,
            outcome.stats.evidence,
            outcome.stats.edges,
            outcome.stats.vectors
        ),
        (1, 2, 2, 1)
    );
    assert!(store.claim_by_id("a").is_none());
    assert!(store.evidence_for_claim("a").is_empty());
    assert!(store.edges_for_claim("a").is_empty());
    assert!(
        store.edges_for_claim("c").is_empty(),
        "incoming edge c->a must go"
    );
    assert_eq!(store.claims_len(), 3);
    let hits: Vec<String> = store
        .retrieve(&request("t1", "alpha"))
        .into_iter()
        .map(|r| r.claim_id)
        .collect();
    assert!(!hits.contains(&"a".to_string()), "{hits:?}");
    assert!(
        !store
            .ann_vector_top_candidates("t1", &[1.0, 0.0, 0.0], 10)
            .contains(&"a".to_string())
    );
    assert!(store.claim_ids_for_entity("t1", "acme").len() == 2);
    // The tenant still has a vector (b): its dimension stays.
    assert_eq!(store.tenant_vector_dim("t1"), Some(3));
    // Untouched neighbours.
    assert_eq!(store.evidence_for_claim("x").len(), 1);
    assert_eq!(store.evidence_for_claim("b").len(), 1);

    // Restart: the replayed state equals the live state.
    assert_eq!(dump(&reload(&wal_path)), dump(&store));
}

#[test]
fn deletes_are_idempotent_and_an_absent_target_writes_nothing() {
    let dir = TempDir::new().unwrap();
    let wal_path = dir.path().join("d.wal");
    let mut wal = FileWal::open(&wal_path).unwrap();
    let mut store = InMemoryStore::new();
    seed(&mut store, &mut wal);

    assert!(
        store
            .delete_persistent(&mut wal, claim_tombstone("t1", "a"), TS)
            .unwrap()
            .deleted()
    );
    let records = wal.wal_record_count().unwrap();
    for tombstone in [
        claim_tombstone("t1", "a"),
        claim_tombstone("t1", "missing"),
        // A claim of another tenant is invisible to this tenant's delete.
        claim_tombstone("t1", "x"),
        evidence_tombstone("t1", "nope"),
        tenant_tombstone("never-existed"),
    ] {
        let prepared = store.prepare_delete(tombstone.clone(), TS).unwrap();
        assert!(prepared.is_noop(), "{tombstone:?}");
        let outcome = store.delete_persistent(&mut wal, tombstone, TS).unwrap();
        assert!(!outcome.deleted());
    }
    assert_eq!(wal.wal_record_count().unwrap(), records);
    assert!(store.claim_by_id("x").is_some());
}

#[test]
fn empty_identifiers_are_rejected_before_anything_is_written() {
    let store = InMemoryStore::new();
    for tombstone in [
        claim_tombstone("", "a"),
        claim_tombstone("t1", " "),
        evidence_tombstone("t1", ""),
        tenant_tombstone(""),
    ] {
        assert!(matches!(
            store.prepare_delete(tombstone, TS),
            Err(StoreError::Parse(_))
        ));
    }
}

#[test]
fn evidence_delete_is_scoped_to_the_tenant_and_keeps_the_claim() {
    let dir = TempDir::new().unwrap();
    let wal_path = dir.path().join("d.wal");
    let mut wal = FileWal::open(&wal_path).unwrap();
    let mut store = InMemoryStore::new();
    seed(&mut store, &mut wal);

    let outcome = store
        .delete_persistent(&mut wal, evidence_tombstone("t1", "shared"), TS)
        .unwrap();
    assert_eq!(outcome.stats.evidence, 1);
    assert_eq!(outcome.stats.claims, 0);
    let ids: Vec<String> = store
        .evidence_for_claim("a")
        .into_iter()
        .map(|e| e.evidence_id)
        .collect();
    assert_eq!(ids, vec!["ea1".to_string()]);
    assert!(store.claim_by_id("a").is_some());
    // Same evidence id in another tenant is untouched.
    assert_eq!(store.evidence_for_claim("x").len(), 1);
    // Removing the last evidence row of a claim leaves the claim.
    store
        .delete_persistent(&mut wal, evidence_tombstone("t1", "eb"), TS)
        .unwrap();
    assert!(store.evidence_for_claim("b").is_empty());
    assert!(store.claim_by_id("b").is_some());

    assert_eq!(dump(&reload(&wal_path)), dump(&store));
}

#[test]
fn tenant_erasure_removes_everything_of_the_tenant_only() {
    let dir = TempDir::new().unwrap();
    let wal_path = dir.path().join("d.wal");
    let mut wal = FileWal::open(&wal_path).unwrap();
    let mut store = InMemoryStore::new();
    seed(&mut store, &mut wal);
    store
        .observe_batch_commit("batch-1", 2, TS, &["a".to_string(), "b".to_string()])
        .unwrap();

    let outcome = store
        .delete_persistent(&mut wal, tenant_tombstone("t1"), TS)
        .unwrap();
    assert_eq!(
        (
            outcome.stats.claims,
            outcome.stats.evidence,
            outcome.stats.edges,
            outcome.stats.vectors
        ),
        (3, 4, 2, 2)
    );
    assert!(store.claims_for_tenant("t1").is_empty());
    assert_eq!(store.tenant_ids(), vec!["t2".to_string()]);
    assert_eq!(store.tenant_vector_dim("t1"), None);
    assert!(store.batch_commit_metadata("batch-1").is_none());
    assert!(store.retrieve(&request("t1", "statement")).is_empty());
    assert!(store.claim_by_id("x").is_some());
    assert_eq!(store.tenant_vector_dim("t2"), Some(2));

    // The tenant can be written again, with a new vector dimension.
    store
        .ingest_atomic_persistent(
            &mut wal,
            claim("t1", "a", "alpha again"),
            vec![],
            vec![],
            Some(vec![1.0; 5]),
            TS + 50,
        )
        .unwrap();
    assert_eq!(store.tenant_vector_dim("t1"), Some(5));
    assert_eq!(dump(&reload(&wal_path)), dump(&store));
}

#[test]
fn deleting_the_last_vector_releases_the_tenant_dimension_deterministically() {
    let dir = TempDir::new().unwrap();
    let wal_path = dir.path().join("d.wal");
    let mut wal = FileWal::open(&wal_path).unwrap();
    let mut store = InMemoryStore::new();
    seed(&mut store, &mut wal);
    store
        .delete_persistent(&mut wal, claim_tombstone("t2", "x"), TS)
        .unwrap();
    assert_eq!(store.tenant_vector_dim("t2"), None);
    store
        .ingest_atomic_persistent(
            &mut wal,
            claim("t2", "y", "new"),
            vec![],
            vec![],
            Some(vec![1.0; 4]),
            TS,
        )
        .unwrap();
    assert_eq!(store.tenant_vector_dim("t2"), Some(4));
    assert_eq!(dump(&reload(&wal_path)), dump(&store));
}

#[test]
fn a_deleted_claim_id_can_be_reused() {
    let dir = TempDir::new().unwrap();
    let wal_path = dir.path().join("d.wal");
    let mut wal = FileWal::open(&wal_path).unwrap();
    let mut store = InMemoryStore::new();
    seed(&mut store, &mut wal);
    store
        .delete_persistent(&mut wal, claim_tombstone("t1", "a"), TS)
        .unwrap();
    store
        .ingest_atomic_persistent(
            &mut wal,
            claim("t1", "a", "rewritten alpha"),
            vec![evidence("new-ev", "a")],
            vec![],
            None,
            TS,
        )
        .unwrap();
    assert_eq!(
        store.claim_by_id("a").unwrap().canonical_text,
        "rewritten alpha"
    );
    assert_eq!(store.evidence_for_claim("a").len(), 1);
    // The incoming edge from c was removed with the old claim and stays gone.
    assert!(store.edges_for_claim("c").is_empty());
    assert_eq!(dump(&reload(&wal_path)), dump(&store));
}

#[test]
fn checkpoint_drops_deleted_data_and_the_tombstone_for_good() {
    let dir = TempDir::new().unwrap();
    let wal_path = dir.path().join("d.wal");
    let mut wal = FileWal::open(&wal_path).unwrap();
    let mut store = InMemoryStore::new();
    seed(&mut store, &mut wal);
    store
        .delete_persistent(&mut wal, claim_tombstone("t1", "a"), TS)
        .unwrap();
    store
        .delete_persistent(&mut wal, tenant_tombstone("t2"), TS)
        .unwrap();
    assert!(file_text(&wal_path).contains("alpha statement"));
    assert!(wal.contains_tombstones().unwrap());

    store.checkpoint_and_compact(&mut wal).unwrap();
    let on_disk = format!(
        "{}{}",
        file_text(&wal_path),
        file_text(&wal.snapshot_path())
    );
    for gone in [
        "alpha statement",
        "other tenant statement",
        "ea1",
        "\tt2\t",
        "T2\t",
    ] {
        assert!(!on_disk.contains(gone), "{gone:?} survived the checkpoint");
    }
    assert!(!wal.contains_tombstones().unwrap());
    drop(wal);
    assert_eq!(dump(&reload(&wal_path)), dump(&store));
}

#[test]
fn the_redb_mirror_applies_deletes_and_restarts_from_it_agree() {
    let dir = TempDir::new().unwrap();
    let wal_path = dir.path().join("d.wal");
    let redb_path = dir.path().join("d.redb");
    let mut wal = FileWal::open(&wal_path).unwrap();
    let mut store = InMemoryStore::new().with_disk(&redb_path).unwrap();
    seed(&mut store, &mut wal);
    store
        .delete_persistent(&mut wal, claim_tombstone("t1", "a"), TS)
        .unwrap();
    store
        .delete_persistent(&mut wal, evidence_tombstone("t1", "eb"), TS)
        .unwrap();
    store
        .delete_persistent(&mut wal, tenant_tombstone("t2"), TS)
        .unwrap();
    // Release then re-establish t2's dimension with another size: replaying
    // the log over the later redb state would trip the dimension check.
    store
        .ingest_atomic_persistent(
            &mut wal,
            claim("t2", "z", "fresh"),
            vec![],
            vec![],
            Some(vec![1.0; 6]),
            TS,
        )
        .unwrap();
    let expected = dump(&store);
    drop(store);
    drop(wal);

    // redb alone (no WAL to replay) holds the post-delete state.
    let empty_wal_path = dir.path().join("empty.wal");
    let mut empty = FileWal::open(&empty_wal_path).unwrap();
    let (from_redb, _) =
        InMemoryStore::load_from_disk_and_wal(&redb_path, &mut empty, AnnTuningConfig::default())
            .unwrap();
    assert_eq!(dump(&from_redb), expected);
    drop(from_redb);

    // redb plus the WAL holding the tombstones.
    let mut wal = FileWal::open(&wal_path).unwrap();
    let (restarted, stats) =
        InMemoryStore::load_from_disk_and_wal(&redb_path, &mut wal, AnnTuningConfig::default())
            .unwrap();
    assert_eq!(dump(&restarted), expected);
    assert_eq!(
        stats.claims_loaded, 5,
        "rebuilt from the WAL, not loaded from redb"
    );
}

#[test]
fn replicated_tombstones_apply_in_order_and_resync_converges() {
    let dir = TempDir::new().unwrap();
    let mut leader_wal = FileWal::open(dir.path().join("leader.wal")).unwrap();
    let mut leader = InMemoryStore::new();
    seed(&mut leader, &mut leader_wal);

    let mut follower = InMemoryStore::new();
    let mut offset = 0;
    let mut pull = |leader_wal: &mut FileWal, follower: &mut InMemoryStore| {
        let delta = leader_wal.replication_delta_from(offset, 10_000).unwrap();
        for line in &delta.wal_lines {
            assert!(follower.apply_persisted_record_line_lenient(line).unwrap());
        }
        offset = delta.next_offset;
    };
    pull(&mut leader_wal, &mut follower);
    assert_eq!(dump(&follower), dump(&leader));

    leader
        .delete_persistent(&mut leader_wal, claim_tombstone("t1", "a"), TS)
        .unwrap();
    leader
        .ingest_atomic_persistent(
            &mut leader_wal,
            claim("t1", "a", "alpha reborn"),
            vec![],
            vec![],
            None,
            TS,
        )
        .unwrap();
    leader
        .delete_persistent(&mut leader_wal, tenant_tombstone("t2"), TS)
        .unwrap();
    pull(&mut leader_wal, &mut follower);
    assert_eq!(dump(&follower), dump(&leader));
    assert_eq!(
        follower.claim_by_id("a").unwrap().canonical_text,
        "alpha reborn"
    );

    // Re-applying the same lines (a retried pull) changes nothing.
    let export = leader_wal.replication_export().unwrap();
    for line in export.wal_lines.iter() {
        follower.apply_persisted_record_line_lenient(line).unwrap();
    }
    assert_eq!(dump(&follower), dump(&leader));

    // Resync from a full export, before and after a checkpoint.
    let resync = |export: store::WalReplicationExport| {
        let mut fresh = InMemoryStore::new();
        for line in export.snapshot_lines.iter().chain(export.wal_lines.iter()) {
            fresh.apply_persisted_record_line(line).unwrap();
        }
        fresh
    };
    assert_eq!(
        dump(&resync(leader_wal.replication_export().unwrap())),
        dump(&leader)
    );
    leader.checkpoint_and_compact(&mut leader_wal).unwrap();
    assert_eq!(
        dump(&resync(leader_wal.replication_export().unwrap())),
        dump(&leader)
    );
}

/// CRC-32 (IEEE, reflected), as used by the WAL record suffix.
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

fn with_crc(body: &str) -> String {
    format!("{body}\tcrc={:08x}", crc32(body.as_bytes()))
}

/// A tombstone is to a reader that predates it what an unknown, properly
/// checksummed record kind is to this reader. Replay must stop, under both
/// policies: in the middle of the log, right after a quarantined legacy line
/// (where unknown fragments are otherwise swallowed), and as the final line
/// (where an unparseable line would otherwise be truncated as a torn write).
/// A follower must reject it too.
#[test]
fn readers_fail_closed_on_an_unknown_checksummed_record_kind() {
    let future = with_crc("T9\tclaim\tt1\ta\t1");
    let valid_tail = with_crc("B2\t~tx:unrelated\t0\t1\t");
    let scenarios: Vec<(&str, Vec<String>)> = vec![
        ("interior", vec![future.clone(), valid_tail.clone()]),
        (
            "after a broken legacy line",
            vec![
                "C\tbroken-legacy-line".to_string(),
                future.clone(),
                valid_tail,
            ],
        ),
        ("final line", vec![future.clone()]),
    ];
    for (name, extra) in scenarios {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join("future.wal");
        {
            let mut wal = FileWal::open(&wal_path).unwrap();
            let mut store = InMemoryStore::new();
            seed(&mut store, &mut wal);
        }
        let mut text = file_text(&wal_path);
        for line in &extra {
            text.push_str(line);
            text.push('\n');
        }
        std::fs::write(&wal_path, &text).unwrap();

        let wal = FileWal::open(&wal_path).unwrap();
        assert_eq!(
            wal.torn_tail_dropped(),
            0,
            "{name}: truncated as a torn tail"
        );
        for policy in [ReplayPolicy::Strict, ReplayPolicy::Lenient] {
            let result =
                InMemoryStore::load_from_wal_with_policy(&wal, AnnTuningConfig::default(), policy);
            match result {
                // Strict replay already stops at the broken legacy line.
                Err(_) if name.contains("legacy") && policy == ReplayPolicy::Strict => {}
                Err(err) => assert!(
                    format!("{err:?}").contains("unknown wal record kind"),
                    "{name}: {policy:?} failed for another reason: {err:?}"
                ),
                Ok(_) => panic!("{name}: {policy:?} replay accepted an unknown kind"),
            }
        }
        assert_eq!(file_text(&wal_path), text, "{name}: the log was modified");
    }
    let mut follower = InMemoryStore::new();
    assert!(
        follower
            .apply_persisted_record_line_lenient(&future)
            .is_err()
    );
}

/// Tombstones are framed in a commit group so that they are never the last
/// line: a reader that predates them hits an unknown interior record.
#[test]
fn a_tombstone_is_never_the_last_line_of_the_log() {
    let dir = TempDir::new().unwrap();
    let wal_path = dir.path().join("g.wal");
    let mut wal = FileWal::open(&wal_path).unwrap();
    let mut store = InMemoryStore::new();
    seed(&mut store, &mut wal);
    let before = wal.wal_record_count().unwrap();
    store
        .delete_persistent(&mut wal, claim_tombstone("t1", "a"), TS)
        .unwrap();
    assert_eq!(wal.wal_record_count().unwrap(), before + 3);
    drop(wal);
    let text = file_text(&wal_path);
    let lines: Vec<&str> = text.lines().collect();
    let n = lines.len();
    assert!(
        lines[n - 3].starts_with("B2\t~grp:del:claim:"),
        "{}",
        lines[n - 3]
    );
    assert!(
        lines[n - 2].starts_with("T2\tclaim\tt1\ta\t"),
        "{}",
        lines[n - 2]
    );
    assert!(
        lines[n - 1].starts_with("B2\t~tx:del:claim:"),
        "{}",
        lines[n - 1]
    );

    // A crash after the tombstone but before its commit marker discards it.
    let torn: String = lines[..n - 1].iter().map(|l| format!("{l}\n")).collect();
    std::fs::write(&wal_path, torn).unwrap();
    assert!(reload(&wal_path).claim_by_id("a").is_some());
}

#[test]
fn tombstone_records_are_checksummed_and_strictly_parsed() {
    let dir = TempDir::new().unwrap();
    let wal_path = dir.path().join("t.wal");
    let mut wal = FileWal::open(&wal_path).unwrap();
    let mut store = InMemoryStore::new();
    let odd = "id with spaces, a \\ backslash and unicode \u{e9}";
    store
        .ingest_atomic_persistent(
            &mut wal,
            claim("t 1", odd, "odd ids"),
            vec![],
            vec![],
            None,
            TS,
        )
        .unwrap();
    store
        .delete_persistent(&mut wal, claim_tombstone("t 1", odd), TS)
        .unwrap();
    drop(wal);
    let text = file_text(&wal_path);
    let line = text
        .lines()
        .find(|l| l.starts_with("T2\t"))
        .expect("tombstone line");
    assert!(line.contains("\tcrc="), "{line}");
    assert_eq!(reload(&wal_path).claims_len(), 0);

    let mut follower = InMemoryStore::new();
    // A flipped byte fails the checksum; a missing checksum is refused.
    let tampered = line.replacen("claim", "clain", 1);
    assert!(follower.apply_persisted_record_line(&tampered).is_err());
    let body = &line[..line.rfind('\t').unwrap()];
    assert!(follower.apply_persisted_record_line(body).is_err());
    // Unknown scopes, a tenant tombstone naming a target, bad field counts
    // and empty ids are rejected even with a valid checksum.
    for bad in [
        "T2\tshard\tt1\ta\t1",
        "T2\ttenant\tt1\ta\t1",
        "T2\tclaim\tt1\ta",
        "T2\tclaim\tt1\t\t1",
        "T2\tclaim\t\ta\t1",
        "T2\tclaim\tt1\ta\tnot-a-ts",
    ] {
        assert!(
            follower
                .apply_persisted_record_line(&with_crc(bad))
                .is_err(),
            "{bad:?} accepted"
        );
    }
    assert!(
        follower
            .apply_persisted_record_line(&with_crc("T2\ttenant\tt1\t\t1"))
            .is_ok()
    );
}

#[test]
fn staged_clone_deletes_reach_redb_only_on_commit() {
    let dir = TempDir::new().unwrap();
    let redb_path = dir.path().join("s.redb");
    let wal_path = dir.path().join("s.wal");
    let mut wal = FileWal::open(&wal_path).unwrap();
    let mut store = InMemoryStore::new().with_disk(&redb_path).unwrap();
    seed(&mut store, &mut wal);

    let mut staged = store.clone_detached();
    staged.delete(claim_tombstone("t1", "a")).unwrap();
    // Dropped without commit: the live store and redb keep the claim.
    drop(staged);
    assert!(store.claim_by_id("a").is_some());

    let mut staged = store.clone_detached();
    staged.delete(claim_tombstone("t1", "a")).unwrap();
    store.commit_staged(staged).unwrap();
    assert!(store.claim_by_id("a").is_none());
    let expected = dump(&store);
    drop(store);
    let mut empty = FileWal::open(dir.path().join("empty.wal")).unwrap();
    let (from_redb, _) =
        InMemoryStore::load_from_disk_and_wal(&redb_path, &mut empty, AnnTuningConfig::default())
            .unwrap();
    assert_eq!(dump(&from_redb), expected);
}
