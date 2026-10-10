//! Turning encryption at rest on over data written by earlier releases (ADR
//! 0005): every fixture's plaintext WAL, snapshot, redb mirror, vector index
//! and segment root opens with a key configured and holds the same data;
//! the WAL is rewritten encrypted on open, the snapshot by the next
//! checkpoint, the vector index by the next save, segments by the next
//! publish and redb values by the next checkpoint of the mirror. Afterwards
//! the node no longer opens without the key (fail closed).

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use dash_compat::FIXTURES;
use indexer::{
    CompactionSchedulerConfig, SegmentPublishOptions, load_current_segments, publish_claims_to_dir,
    read_tenant_marker,
};
use store::encryption::{self, FileFormat, Keyring, with_keyring};
use store::{AnnTuningConfig, DiskBackedStore, FileWal, InMemoryStore, ReplayPolicy};

fn keyring() -> Arc<Keyring> {
    Arc::new(Keyring::local([0x4b; 32], &[]).unwrap())
}

fn claim_ids(store: &InMemoryStore) -> BTreeSet<String> {
    store
        .tenant_ids()
        .iter()
        .flat_map(|t| store.claims_for_tenant(t))
        .map(|c| format!("{}|{}", c.claim_id, c.canonical_text))
        .collect()
}

fn format_of(path: &std::path::Path) -> FileFormat {
    encryption::sniff_file(path).unwrap()
}

#[test]
fn plaintext_fixtures_open_with_encryption_on_and_migrate_to_encrypted_files() {
    for fixture in FIXTURES {
        let label = fixture.label;
        let state = fixture.scratch_state();
        let expected_claims = fixture.expected_claims_total();
        for path in [state.wal(), state.snapshot(), state.vindex()] {
            // 0.2 had no persisted vector index.
            if path.exists() {
                assert_eq!(format_of(&path), FileFormat::Plain, "{label}");
            }
        }
        let before = with_keyring(None, || {
            let wal = FileWal::open(state.wal()).unwrap();
            claim_ids(&InMemoryStore::load_from_wal(&wal).unwrap())
        });

        with_keyring(Some(keyring()), || {
            // Open: the live WAL is rewritten encrypted; the snapshot and the
            // vector index are read as they are.
            let mut wal = FileWal::open(state.wal()).unwrap();
            assert!(
                matches!(format_of(&state.wal()), FileFormat::Lines(_)),
                "{label}"
            );
            assert_eq!(format_of(&state.snapshot()), FileFormat::Plain, "{label}");
            let (store, stats) = InMemoryStore::load_from_wal_with_vector_index(
                &wal,
                AnnTuningConfig::default(),
                ReplayPolicy::Strict,
                Some(&state.vindex()),
            )
            .unwrap();
            assert_eq!(stats.replay.quarantined_records, 0, "{label}");
            assert_eq!(claim_ids(&store), before, "{label}: same data");
            assert_eq!(store.claims_len(), expected_claims, "{label}");

            // redb: plaintext values are read, the mirror gets a data key and
            // the next checkpoint rewrites every value sealed.
            let disk = DiskBackedStore::new(state.redb()).unwrap();
            assert!(disk.encrypts_values(), "{label}");
            let mut from_disk = InMemoryStore::new();
            disk.bulk_load_claims_into(&mut from_disk).unwrap();
            assert!(
                !claim_ids(&from_disk).is_empty(),
                "{label}: redb values read"
            );
            disk.checkpoint_from(&store).unwrap();
            drop(disk);

            // Segments: the old plaintext root still loads; a publish writes
            // sealed files.
            let options = SegmentPublishOptions {
                max_segment_size: 10_000,
                scheduler: CompactionSchedulerConfig::default(),
                prune_grace: Duration::ZERO,
            };
            for entry in std::fs::read_dir(state.segments()).unwrap() {
                let dir = entry.unwrap().path();
                load_current_segments(&dir)
                    .unwrap_or_else(|e| panic!("{label}: {}: {e:?}", dir.display()));
                if let Some(tenant) = read_tenant_marker(&dir).unwrap() {
                    let claims = store.claims_for_tenant(&tenant);
                    if !claims.is_empty() {
                        publish_claims_to_dir(&dir, &claims, &options).unwrap();
                        let manifest = dir.join("segments.manifest");
                        assert!(
                            matches!(format_of(&manifest), FileFormat::Sealed(_)),
                            "{label}: {} republished sealed",
                            manifest.display()
                        );
                    }
                }
            }

            // Checkpoint and vector index save finish the migration.
            store.checkpoint_and_compact(&mut wal).unwrap();
            store
                .vector_index_snapshot(wal.position())
                .save(&state.vindex())
                .unwrap();
            assert!(
                matches!(format_of(&state.snapshot()), FileFormat::Lines(_)),
                "{label}"
            );
            assert!(
                matches!(format_of(&state.wal()), FileFormat::Lines(_)),
                "{label}"
            );
            assert!(
                matches!(format_of(&state.vindex()), FileFormat::Sealed(_)),
                "{label}"
            );
            drop(wal);

            // A restart reads the migrated files and loads the saved index.
            let wal = FileWal::open(state.wal()).unwrap();
            let (reloaded, stats) = InMemoryStore::load_from_wal_with_vector_index(
                &wal,
                AnnTuningConfig::default(),
                ReplayPolicy::Strict,
                Some(&state.vindex()),
            )
            .unwrap();
            assert_eq!(claim_ids(&reloaded), before, "{label}: after migration");
            assert!(
                !matches!(
                    stats.vector_index,
                    store::VectorIndexRestore::Rebuilt { .. }
                ),
                "{label}: {:?}",
                stats.vector_index
            );
            let disk = DiskBackedStore::new(state.redb()).unwrap();
            let mut from_disk = InMemoryStore::new();
            disk.bulk_load_claims_into(&mut from_disk).unwrap();
            assert_eq!(
                claim_ids(&from_disk),
                before,
                "{label}: redb after migration"
            );
        });

        // Without the key the migrated node fails closed.
        with_keyring(None, || {
            let err = format!("{:?}", FileWal::open(state.wal()).err().unwrap());
            assert!(
                err.contains("no encryption key is configured"),
                "{label}: {err}"
            );
            let err = store::check_encryption_state(
                None,
                &store::EncryptionStatePaths {
                    wal: Some(state.wal()),
                    redb: Some(state.redb()),
                    vector_index: Some(state.vindex()),
                    segment_dirs: vec![state.segments()],
                },
            )
            .unwrap_err();
            assert!(err.contains("DASH_ENCRYPTION_KEY_FILE"), "{label}: {err}");
        });
    }
}
