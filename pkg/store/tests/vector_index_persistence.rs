//! Persisted vector index: save, load, catch-up and every fallback to a
//! rebuild (ADR 0003, "Persisted vector index").
//!
//! Each test builds its own WAL in a temp dir from seeded data (no clocks, no
//! sleeps). "Rebuilt" results are always compared with a store loaded without
//! any persisted index, which builds its indexes from the replayed vectors:
//! the behaviour before persistence existed.

use schema::Claim;
use std::path::{Path, PathBuf};
use store::{
    AnnTuningConfig, FileWal, InMemoryStore, ReplayPolicy, StoreLoadStats, Tombstone,
    VectorIndexRestore, WalPosition,
};
use tempfile::TempDir;

const DIM: usize = 24;

struct Rng(u64);

impl Rng {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn gauss(&mut self) -> f32 {
        let u1 = ((self.next_u64() >> 11) as f64 + 1.0) / (1u64 << 53) as f64;
        let u2 = ((self.next_u64() >> 11) as f64 + 1.0) / (1u64 << 53) as f64;
        ((-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos()) as f32
    }

    fn vector(&mut self) -> Vec<f32> {
        (0..DIM).map(|_| self.gauss()).collect()
    }
}

fn tuning() -> AnnTuningConfig {
    // A low flat threshold puts the larger test tenant on the HNSW path.
    AnnTuningConfig {
        flat_threshold: 64,
        ..AnnTuningConfig::default()
    }
}

fn claim(tenant: &str, id: &str) -> Claim {
    Claim {
        claim_id: id.to_string(),
        tenant_id: tenant.to_string(),
        canonical_text: format!("statement {id}"),
        confidence: 0.7,
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

struct Fixture {
    _dir: TempDir,
    wal_path: PathBuf,
    index_path: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let dir = TempDir::new().unwrap();
        Self {
            wal_path: dir.path().join("ingest.wal"),
            index_path: dir.path().join("ingest.wal.vindex"),
            _dir: dir,
        }
    }

    fn wal(&self) -> FileWal {
        FileWal::open(&self.wal_path).unwrap()
    }

    /// Append `count` claims with vectors for `tenant` (ids `<prefix><i>`).
    fn ingest(&self, tenant: &str, prefix: &str, count: usize, seed: u64) {
        let mut wal = self.wal();
        let (mut store, _) = InMemoryStore::load_from_wal_with_vector_index(
            &wal,
            tuning(),
            ReplayPolicy::Strict,
            None,
        )
        .unwrap();
        let mut rng = Rng(seed);
        for i in 0..count {
            store
                .ingest_atomic_persistent(
                    &mut wal,
                    claim(tenant, &format!("{prefix}{i}")),
                    vec![],
                    vec![],
                    Some(rng.vector()),
                    1_700_000_000_000 + i as u64,
                )
                .unwrap();
        }
    }

    /// Replace the vector of existing claims.
    fn replace_vectors(&self, ids: &[&str], seed: u64) {
        let mut wal = self.wal();
        let mut store = InMemoryStore::load_from_wal_with_ann_tuning(&wal, tuning()).unwrap();
        let mut rng = Rng(seed);
        for id in ids {
            store
                .upsert_claim_vector_persistent(&mut wal, id, rng.vector())
                .unwrap();
        }
    }

    /// Append tombstones through the WAL, as the ingestion service does.
    fn delete(&self, tombstones: Vec<Tombstone>) {
        let mut wal = self.wal();
        let mut store = InMemoryStore::load_from_wal_with_ann_tuning(&wal, tuning()).unwrap();
        for (i, tombstone) in tombstones.into_iter().enumerate() {
            let outcome = store
                .delete_persistent(&mut wal, tombstone, 1_800_000_000_000 + i as u64)
                .unwrap();
            assert!(outcome.deleted());
        }
    }

    fn load(&self, with_index: bool) -> (InMemoryStore, StoreLoadStats) {
        self.load_with(with_index, tuning())
    }

    fn load_with(
        &self,
        with_index: bool,
        tuning: AnnTuningConfig,
    ) -> (InMemoryStore, StoreLoadStats) {
        let wal = self.wal();
        InMemoryStore::load_from_wal_with_vector_index(
            &wal,
            tuning,
            ReplayPolicy::Strict,
            with_index.then_some(self.index_path.as_path()),
        )
        .unwrap()
    }

    /// Load (building the indexes) and save them at the current position.
    fn save(&self) -> WalPosition {
        let (store, _) = self.load(false);
        let position = store.wal_position().expect("loader records the position");
        store
            .vector_index_snapshot(position)
            .save(&self.index_path)
            .unwrap();
        position
    }
}

fn queries(seed: u64) -> Vec<Vec<f32>> {
    let mut rng = Rng(seed);
    (0..25).map(|_| rng.vector()).collect()
}

/// Top-10 vector candidates for every query of every tenant.
fn results(store: &InMemoryStore) -> Vec<Vec<String>> {
    let mut out = Vec::new();
    for tenant in ["big", "small", "late"] {
        for q in queries(99) {
            out.push(store.ann_vector_top_candidates(tenant, &q, 10));
        }
    }
    out
}

fn assert_rebuilt(stats: &StoreLoadStats, needle: &str) {
    match &stats.vector_index {
        VectorIndexRestore::Rebuilt { reason } => {
            assert!(
                reason.contains(needle),
                "reason {reason:?} lacks {needle:?}"
            )
        }
        other => panic!("expected a rebuild ({needle}), got {other:?}"),
    }
}

/// Two tenants: "big" on the HNSW path, "small" flat.
fn populated() -> Fixture {
    let fx = Fixture::new();
    fx.ingest("big", "b", 400, 1);
    fx.ingest("small", "s", 30, 2);
    fx
}

#[test]
fn first_start_without_a_file_builds_and_reports_missing() {
    let fx = populated();
    let (store, stats) = fx.load(true);
    assert_eq!(stats.vector_index, VectorIndexRestore::Missing);
    assert_eq!(results(&store), results(&fx.load(false).0));
}

#[test]
fn round_trip_loads_the_saved_index_without_rebuilding() {
    let fx = populated();
    let position = fx.save();
    let (loaded, stats) = fx.load(true);
    assert_eq!(
        stats.vector_index,
        VectorIndexRestore::Loaded {
            tenants: 2,
            vectors: 430,
            caught_up: 0,
            saved_position: position,
        }
    );
    let (rebuilt, _) = fx.load(false);
    assert_eq!(results(&loaded), results(&rebuilt));
    assert_eq!(
        loaded.index_stats().ann_vector_buckets,
        rebuilt.index_stats().ann_vector_buckets
    );
}

#[test]
fn catch_up_from_the_saved_position_matches_a_full_rebuild() {
    let fx = populated();
    let position = fx.save();
    // After the save: new claims in an existing HNSW tenant, replaced
    // vectors in both tenants, a flat tenant pushed over the threshold and a
    // tenant that did not exist at save time.
    fx.ingest("big", "nb", 60, 3);
    fx.replace_vectors(&["b5", "b17", "s3"], 4);
    fx.ingest("small", "ns", 50, 5);
    fx.ingest("late", "l", 20, 6);

    let (loaded, stats) = fx.load(true);
    match &stats.vector_index {
        VectorIndexRestore::Loaded {
            tenants,
            vectors,
            caught_up,
            saved_position,
        } => {
            assert_eq!(*saved_position, position);
            assert_eq!(*caught_up, 60 + 3 + 50 + 20);
            assert_eq!(*tenants, 3);
            assert_eq!(*vectors, 400 + 60 + 30 + 50 + 20);
        }
        other => panic!("expected a load, got {other:?}"),
    }
    let (rebuilt, rebuilt_stats) = fx.load(false);
    assert_eq!(
        rebuilt_stats.vector_index,
        VectorIndexRestore::NotConfigured
    );
    assert_eq!(results(&loaded), results(&rebuilt));
}

#[test]
fn a_corrupt_file_is_discarded_and_rebuilt() {
    let fx = populated();
    fx.save();
    let (reference, _) = fx.load(false);
    let pristine = std::fs::read(&fx.index_path).unwrap();
    let len = pristine.len();
    let cases: Vec<(&str, Vec<u8>, &str)> = vec![
        (
            "section byte",
            flip(&pristine, len - 100),
            "checksum mismatch",
        ),
        (
            "manifest byte",
            flip(&pristine, 30),
            "header checksum mismatch",
        ),
        ("magic", flip(&pristine, 0), "not a DASH vector index file"),
        ("truncated", pristine[..len - 1].to_vec(), "truncated"),
        (
            "trailing",
            [pristine.as_slice(), b"x"].concat(),
            "trailing bytes",
        ),
        ("empty", Vec::new(), "not a DASH vector index file"),
    ];
    for (name, bytes, needle) in cases {
        std::fs::write(&fx.index_path, &bytes).unwrap();
        let (store, stats) = fx.load(true);
        assert_rebuilt(&stats, needle);
        assert_eq!(results(&store), results(&reference), "{name}");
    }
}

fn flip(bytes: &[u8], at: usize) -> Vec<u8> {
    let mut out = bytes.to_vec();
    out[at] ^= 0x5A;
    out
}

#[test]
fn a_format_version_mismatch_is_rebuilt() {
    let fx = populated();
    fx.save();
    let mut bytes = std::fs::read(&fx.index_path).unwrap();
    bytes[8] = bytes[8].wrapping_add(1);
    std::fs::write(&fx.index_path, &bytes).unwrap();
    let (_, stats) = fx.load(true);
    assert_rebuilt(&stats, "format version");
}

#[test]
fn a_tuning_mismatch_is_rebuilt_with_the_configured_tuning() {
    let fx = populated();
    fx.save();
    for changed in [
        AnnTuningConfig {
            expansion_search: 300,
            ..tuning()
        },
        AnnTuningConfig {
            connectivity: 8,
            ..tuning()
        },
        AnnTuningConfig {
            flat_threshold: 1_000,
            ..tuning()
        },
    ] {
        let (store, stats) = fx.load_with(true, changed.clone());
        assert_rebuilt(&stats, "tuning");
        assert_eq!(store.ann_tuning(), &changed);
        let (reference, _) = fx.load_with(false, changed);
        assert_eq!(results(&store), results(&reference));
    }
}

#[test]
fn a_checkpoint_after_the_save_changes_the_generation_and_forces_a_rebuild() {
    let fx = populated();
    fx.save();
    {
        let mut wal = fx.wal();
        let store = InMemoryStore::load_from_wal_with_ann_tuning(&wal, tuning()).unwrap();
        store.checkpoint_and_compact(&mut wal).unwrap();
    }
    let (store, stats) = fx.load(true);
    assert_rebuilt(&stats, "generation");
    assert_eq!(results(&store), results(&fx.load(false).0));
}

#[test]
fn a_wal_shorter_than_the_saved_position_forces_a_rebuild() {
    let fx = populated();
    let position = fx.save();
    // Claim the index reflects records the WAL does not hold.
    let (store, _) = fx.load(false);
    store
        .vector_index_snapshot(WalPosition {
            records: position.records + 10,
            ..position
        })
        .save(&fx.index_path)
        .unwrap();
    let (_, stats) = fx.load(true);
    assert_rebuilt(&stats, "fewer than");
}

#[test]
fn an_index_that_misstates_its_position_fails_verification() {
    let fx = populated();
    let (before, _) = fx.load(false);
    fx.ingest("big", "nb", 10, 7);
    fx.replace_vectors(&["b2"], 8);
    // Saved from the older state but stamped with the newer position, so
    // catch-up skips the records it is missing.
    let (after, _) = fx.load(false);
    before
        .vector_index_snapshot(after.wal_position().unwrap())
        .save(&fx.index_path)
        .unwrap();
    let (store, stats) = fx.load(true);
    assert_rebuilt(&stats, "tenant 'big'");
    assert_eq!(results(&store), results(&after));
}

#[test]
fn saving_replaces_the_file_atomically_and_leaves_no_temp_file() {
    let fx = populated();
    fx.save();
    fx.ingest("late", "l", 5, 9);
    let position = fx.save();
    let (_, stats) = fx.load(true);
    assert!(stats.vector_index.is_loaded());
    let tmp = tmp_path(&fx.index_path);
    assert!(!tmp.exists(), "{} left behind", tmp.display());
    match stats.vector_index {
        VectorIndexRestore::Loaded { saved_position, .. } => assert_eq!(saved_position, position),
        other => panic!("{other:?}"),
    }
}

fn tmp_path(path: &Path) -> PathBuf {
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".tmp");
    PathBuf::from(tmp)
}

#[test]
fn an_unindexable_vector_round_trips_as_unindexed() {
    let fx = populated();
    {
        let mut wal = fx.wal();
        let mut store = InMemoryStore::load_from_wal_with_ann_tuning(&wal, tuning()).unwrap();
        store
            .upsert_claim_vector_persistent(&mut wal, "b9", vec![0.0; DIM])
            .unwrap();
    }
    fx.save();
    fx.replace_vectors(&["b11"], 10);
    {
        let mut wal = fx.wal();
        let mut store = InMemoryStore::load_from_wal_with_ann_tuning(&wal, tuning()).unwrap();
        store
            .upsert_claim_vector_persistent(&mut wal, "s4", vec![0.0; DIM])
            .unwrap();
    }
    let (loaded, stats) = fx.load(true);
    assert!(stats.vector_index.is_loaded(), "{:?}", stats.vector_index);
    let (rebuilt, _) = fx.load(false);
    assert_eq!(results(&loaded), results(&rebuilt));
    assert_eq!(
        loaded.index_stats().ann_vector_buckets,
        rebuilt.index_stats().ann_vector_buckets
    );
}

fn claim_tombstone(tenant: &str, claim: &str) -> Tombstone {
    Tombstone::Claim {
        tenant_id: tenant.to_string(),
        claim_id: claim.to_string(),
    }
}

fn all_candidates(store: &InMemoryStore) -> Vec<String> {
    let mut out: Vec<String> = results(store).into_iter().flatten().collect();
    out.sort();
    out.dedup();
    out
}

/// A saved index still holds claims deleted after the save; the WAL
/// catch-up removes them instead of serving them or rebuilding.
#[test]
fn claims_deleted_after_the_save_are_removed_by_catch_up() {
    let fx = populated();
    let position = fx.save();
    let deleted = ["b3", "b77", "b250", "s0", "s29"];
    fx.delete(
        deleted
            .iter()
            .map(|id| {
                let tenant = if id.starts_with('b') { "big" } else { "small" };
                claim_tombstone(tenant, id)
            })
            .collect(),
    );

    let (loaded, stats) = fx.load(true);
    match &stats.vector_index {
        VectorIndexRestore::Loaded {
            tenants,
            vectors,
            caught_up,
            saved_position,
        } => {
            assert_eq!(*saved_position, position);
            assert_eq!(*caught_up, deleted.len());
            assert_eq!(*tenants, 2);
            assert_eq!(*vectors, 430 - deleted.len());
        }
        other => panic!("expected a load, got {other:?}"),
    }
    let (rebuilt, _) = fx.load(false);
    assert_eq!(results(&loaded), results(&rebuilt));
    let served = all_candidates(&loaded);
    for id in deleted {
        assert!(!served.iter().any(|c| c == id), "{id} still served");
        assert!(loaded.claim_by_id(id).is_none());
    }
}

/// A tenant erased after the save and written again (a claim id reused)
/// starts from an empty index, not the saved one.
#[test]
fn a_tenant_erased_after_the_save_is_dropped_from_the_saved_index() {
    let fx = populated();
    fx.save();
    fx.delete(vec![Tombstone::Tenant {
        tenant_id: "small".to_string(),
    }]);
    // Reuse an id the saved index holds for "small", now in a new claim.
    fx.ingest("small", "s", 3, 42);

    let (loaded, stats) = fx.load(true);
    match &stats.vector_index {
        VectorIndexRestore::Loaded { vectors, .. } => assert_eq!(*vectors, 400 + 3),
        other => panic!("expected a load, got {other:?}"),
    }
    let (rebuilt, _) = fx.load(false);
    assert_eq!(results(&loaded), results(&rebuilt));
    assert_eq!(loaded.claim_count_for_tenant("small"), 3);
}

/// Deleting a tenant's last vector releases its dimension; the saved
/// index of that tenant is emptied by the catch-up, not rejected.
#[test]
fn deleting_the_last_vector_of_a_tenant_after_the_save_still_loads() {
    let fx = populated();
    fx.ingest("late", "l", 2, 6);
    fx.save();
    fx.delete(vec![
        claim_tombstone("late", "l0"),
        claim_tombstone("late", "l1"),
    ]);

    let (loaded, stats) = fx.load(true);
    match &stats.vector_index {
        VectorIndexRestore::Loaded { tenants, .. } => assert_eq!(*tenants, 2),
        other => panic!("expected a load, got {other:?}"),
    }
    assert_eq!(loaded.tenant_vector_dim("late"), None);
    assert_eq!(results(&loaded), results(&fx.load(false).0));
}
