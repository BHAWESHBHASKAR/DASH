//! Encryption at rest (ADR 0005, SEC-16): WAL, snapshot, closed generation,
//! quarantine file, redb values and the persisted vector index are
//! encrypted when a keyring is in effect; torn tails are still truncated,
//! interior damage is still a hard error, wrong or missing keys fail with a
//! clear error, rotation keeps old files readable and plaintext files from
//! before encryption are read and migrated.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use schema::{Claim, Evidence, Stance, claim_builder};
use store::encryption::{self, FileFormat, Keyring, with_keyring};
use store::{
    AnnTuningConfig, DiskBackedStore, FileWal, InMemoryStore, ReplayPolicy, WalRepairOptions,
    WalWritePolicy, inspect_wal_file, repair_wal_file,
};
use tempfile::TempDir;

const TENANT: &str = "tenant-a";
const MARKER: &str = "zebra-canary-7f3a";

fn key(byte: u8) -> Arc<Keyring> {
    Arc::new(Keyring::local([byte; 32], &[]).unwrap())
}

fn rotated(active: u8, previous: u8) -> Arc<Keyring> {
    Arc::new(Keyring::local([active; 32], &[[previous; 32]]).unwrap())
}

fn claim(id: &str, n: usize) -> Claim {
    let mut c = claim_builder(id, TENANT, &format!("{MARKER} claim {id} number {n}"), 0.9);
    c.entities = vec![format!("{MARKER}-entity")];
    c
}

fn evidence(id: &str, claim_id: &str) -> Evidence {
    Evidence {
        evidence_id: id.to_string(),
        claim_id: claim_id.to_string(),
        source_id: format!("source://{MARKER}/{id}"),
        stance: Stance::Supports,
        source_quality: 0.8,
        chunk_id: Some(format!("{MARKER}-chunk")),
        span_start: None,
        span_end: None,
        doc_id: None,
        extraction_model: None,
        ingested_at: None,
    }
}

fn write(store: &mut InMemoryStore, wal: &mut FileWal, from: usize, count: usize) {
    for n in from..from + count {
        let id = format!("c{n}");
        store
            .ingest_atomic_persistent(
                wal,
                claim(&id, n),
                vec![evidence(&format!("e{n}"), &id)],
                vec![],
                Some(vec![0.25, (n % 5) as f32 / 5.0, 0.5, 1.0]),
                1_700_000_000_000 + n as u64,
            )
            .unwrap();
    }
}

fn state(store: &InMemoryStore) -> Vec<String> {
    let mut out: Vec<String> = store
        .claims_for_tenant(TENANT)
        .into_iter()
        .map(|c| format!("{}|{}", c.claim_id, c.canonical_text))
        .collect();
    out.sort();
    out
}

fn files_under(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(path) = stack.pop() {
        if path.is_dir() {
            for entry in std::fs::read_dir(&path).unwrap() {
                stack.push(entry.unwrap().path());
            }
        } else {
            out.push(path);
        }
    }
    out
}

fn assert_no_marker(dir: &Path) {
    for path in files_under(dir) {
        let bytes = std::fs::read(&path).unwrap();
        assert!(
            !bytes.windows(MARKER.len()).any(|w| w == MARKER.as_bytes()),
            "{} holds the marker in plaintext",
            path.display()
        );
    }
}

fn format_of(path: &Path) -> FileFormat {
    encryption::sniff_file(path).unwrap()
}

/// Physical end offsets (newline excluded) of the lines after the header.
fn record_line_ends(bytes: &[u8]) -> (usize, Vec<usize>) {
    let mut ends = Vec::new();
    let mut pos = 0usize;
    let mut header_end = 0usize;
    for (idx, line) in bytes.split(|b| *b == b'\n').enumerate() {
        let end = pos + line.len();
        if idx == 0 {
            header_end = end + 1;
        } else if !line.is_empty() {
            ends.push(end);
        }
        pos = end + 1;
    }
    (header_end, ends)
}

#[test]
fn everything_round_trips_encrypted_and_no_plaintext_reaches_the_disk() {
    let dir = TempDir::new().unwrap();
    let wal_path = dir.path().join("dash.wal");
    let redb_path = dir.path().join("dash.redb");
    let vindex_path = dir.path().join("dash.vindex");
    with_keyring(Some(key(1)), || {
        let mut wal = FileWal::open(&wal_path).unwrap();
        assert!(wal.encryption_key_id().is_some());
        let mut store = InMemoryStore::load_from_wal(&wal).unwrap();
        write(&mut store, &mut wal, 0, 20);
        store.checkpoint_and_compact(&mut wal).unwrap();
        write(&mut store, &mut wal, 20, 10);
        store.checkpoint_and_compact(&mut wal).unwrap();
        write(&mut store, &mut wal, 30, 5);
        let disk = DiskBackedStore::new(&redb_path).unwrap();
        assert!(disk.encrypts_values());
        disk.checkpoint_from(&store).unwrap();
        let snapshot = store.vector_index_snapshot(wal.position());
        snapshot.save(&vindex_path).unwrap();
        drop(disk);
        let expected = state(&store);
        drop(wal);

        assert!(matches!(format_of(&wal_path), FileFormat::Lines(_)));
        assert!(matches!(
            format_of(&dir.path().join("dash.wal.snapshot")),
            FileFormat::Lines(_)
        ));
        assert!(matches!(format_of(&vindex_path), FileFormat::Sealed(_)));

        let wal = FileWal::open(&wal_path).unwrap();
        let (reloaded, stats) = InMemoryStore::load_from_wal_with_vector_index(
            &wal,
            AnnTuningConfig::default(),
            ReplayPolicy::Strict,
            Some(&vindex_path),
        )
        .unwrap();
        assert_eq!(state(&reloaded), expected);
        assert!(stats.vector_index.is_loaded(), "{:?}", stats.vector_index);

        let disk = DiskBackedStore::new(&redb_path).unwrap();
        let mut from_disk = InMemoryStore::new();
        disk.bulk_load_claims_into(&mut from_disk).unwrap();
        assert_eq!(state(&from_disk), expected);
        assert_eq!(
            disk.get_claim("c3").unwrap().unwrap().canonical_text,
            format!("{MARKER} claim c3 number 3")
        );
    });
    assert_no_marker(dir.path());
}

#[test]
fn a_torn_encrypted_tail_is_truncated_at_every_byte_offset() {
    let dir = TempDir::new().unwrap();
    let keyring = key(2);
    let full = dir.path().join("full.wal");
    with_keyring(Some(keyring.clone()), || {
        let mut wal = FileWal::open(&full).unwrap();
        for n in 0..4 {
            wal.append_claim(&claim(&format!("c{n}"), n)).unwrap();
        }
        wal.flush_pending_sync().unwrap();
    });
    let bytes = std::fs::read(&full).unwrap();
    let (header_end, ends) = record_line_ends(&bytes);
    assert_eq!(ends.len(), 4);
    for cut in 0..=bytes.len() {
        let case = dir.path().join(format!("cut-{cut}"));
        std::fs::create_dir_all(&case).unwrap();
        let path = case.join("dash.wal");
        std::fs::write(&path, &bytes[..cut]).unwrap();
        with_keyring(Some(keyring.clone()), || {
            let mut wal = FileWal::open(&path)
                .unwrap_or_else(|e| panic!("open must tolerate a cut at {cut}: {e:?}"));
            let kept = wal.replication_export().unwrap().wal_lines;
            // A record line is kept once all of it is on disk (AEAD makes a
            // complete line self-verifying, newline or not).
            let complete = if cut < header_end {
                0
            } else {
                ends.iter().filter(|&&end| end <= cut).count()
            };
            assert_eq!(kept.len(), complete, "cut at {cut}");
            assert_eq!(wal.wal_record_count().unwrap(), complete, "cut at {cut}");
            // The repaired file accepts appends and replays them.
            wal.append_claim(&claim("after", 99)).unwrap();
            wal.flush_pending_sync().unwrap();
            drop(wal);
            let wal = FileWal::open(&path).unwrap();
            let replayed = InMemoryStore::load_from_wal(&wal).unwrap();
            assert_eq!(replayed.claims_for_tenant(TENANT).len(), complete + 1);
            assert!(matches!(format_of(&path), FileFormat::Lines(_)));
        });
        // A sidecar of the removed bytes decodes on its own.
        for sidecar in files_under(&case)
            .into_iter()
            .filter(|p| p.to_string_lossy().contains(".truncated-"))
        {
            let sidecar_bytes = std::fs::read(&sidecar).unwrap();
            if cut >= header_end {
                assert!(encryption::is_header_line(&sidecar_bytes));
            }
        }
    }
}

#[test]
fn interior_damage_is_a_hard_error_naming_the_line() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("dash.wal");
    let keyring = key(3);
    with_keyring(Some(keyring.clone()), || {
        let mut wal = FileWal::open(&path).unwrap();
        for n in 0..4 {
            wal.append_claim(&claim(&format!("c{n}"), n)).unwrap();
        }
        wal.flush_pending_sync().unwrap();
    });
    let pristine = std::fs::read(&path).unwrap();
    let (_, ends) = record_line_ends(&pristine);
    // Flip one character in the middle of the second record (line 3).
    let mut bytes = pristine.clone();
    let at = ends[1] - 30;
    bytes[at] = if bytes[at] == b'A' { b'B' } else { b'A' };
    std::fs::write(&path, &bytes).unwrap();
    with_keyring(Some(keyring.clone()), || {
        let err = FileWal::open(&path)
            .err()
            .expect("interior damage must fail");
        let text = format!("{err:?}");
        assert!(text.contains("wal line 3"), "{text}");
        assert!(text.contains("authentication failed"), "{text}");
        // The inspector reports the same line without changing the file.
        let report = inspect_wal_file(&path).unwrap();
        assert_eq!(report.valid_records, 3);
        assert_eq!(report.invalid_lines.len(), 1);
        assert_eq!(report.invalid_lines[0].line_no, 3);
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
        // Repair with quarantine drops it; the quarantine file is encrypted.
        let repaired = repair_wal_file(
            &path,
            WalRepairOptions {
                dry_run: false,
                quarantine_invalid: true,
            },
        )
        .unwrap();
        assert_eq!(repaired.quarantined_lines, 1);
        assert_eq!(repaired.records_kept, 3);
        let quarantine = repaired.quarantine_path.unwrap();
        assert!(matches!(format_of(&quarantine), FileFormat::Lines(_)));
        let wal = FileWal::open(&path).unwrap();
        assert_eq!(wal.wal_record_count().unwrap(), 3);
    });
    assert_no_marker(dir.path());
}

#[test]
fn wrong_or_missing_keys_fail_closed_with_a_clear_error() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("dash.wal");
    let redb_path = dir.path().join("dash.redb");
    let owner = key(4);
    let records = with_keyring(Some(owner.clone()), || {
        let mut wal = FileWal::open(&path).unwrap();
        let mut store = InMemoryStore::new();
        write(&mut store, &mut wal, 0, 3);
        let disk = DiskBackedStore::new(&redb_path).unwrap();
        disk.checkpoint_from(&store).unwrap();
        wal.wal_record_count().unwrap()
    });
    with_keyring(None, || {
        let err = format!("{:?}", FileWal::open(&path).err().unwrap());
        assert!(err.contains("no encryption key is configured"), "{err}");
        assert!(err.contains(owner.active_key_id()), "{err}");
        assert!(err.contains("DASH_ENCRYPTION_KEY_FILE"), "{err}");
        let err = DiskBackedStore::new(&redb_path).err().unwrap();
        assert!(err.contains("no encryption key is configured"), "{err}");
    });
    with_keyring(Some(key(5)), || {
        let err = format!("{:?}", FileWal::open(&path).err().unwrap());
        assert!(err.contains(owner.active_key_id()), "{err}");
        assert!(err.contains("DASH_ENCRYPTION_PREVIOUS_KEY_FILES"), "{err}");
        assert!(DiskBackedStore::new(&redb_path).is_err());
    });
    // The files are untouched by the failed opens.
    with_keyring(Some(owner), || {
        let wal = FileWal::open(&path).unwrap();
        assert_eq!(wal.wal_record_count().unwrap(), records);
    });
}

#[test]
fn rotation_keeps_old_files_readable_and_moves_new_files_to_the_new_key() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("dash.wal");
    let redb_path = dir.path().join("dash.redb");
    let old = key(6);
    let new = rotated(7, 6);
    let new_only = key(7);
    let expected = with_keyring(Some(old.clone()), || {
        let mut wal = FileWal::open(&path).unwrap();
        let mut store = InMemoryStore::new();
        write(&mut store, &mut wal, 0, 5);
        store.checkpoint_and_compact(&mut wal).unwrap();
        write(&mut store, &mut wal, 5, 5);
        DiskBackedStore::new(&redb_path)
            .unwrap()
            .checkpoint_from(&store)
            .unwrap();
        state(&store)
    });
    with_keyring(Some(new.clone()), || {
        let mut wal = FileWal::open(&path).unwrap();
        // The open WAL keeps its data key (wrapped by the old KEK).
        assert_eq!(wal.encryption_key_id(), Some(old.active_key_id()));
        let store = InMemoryStore::load_from_wal(&wal).unwrap();
        assert_eq!(state(&store), expected);
        // Two checkpoints: snapshot, WAL and the retained closed file all
        // move to the new KEK.
        store.checkpoint_and_compact(&mut wal).unwrap();
        assert_eq!(wal.encryption_key_id(), Some(new.active_key_id()));
        store.checkpoint_and_compact(&mut wal).unwrap();
        // The redb data key is rewrapped on open.
        let disk = DiskBackedStore::new(&redb_path).unwrap();
        assert_eq!(
            disk.encryption_key_id().as_deref(),
            Some(new.active_key_id())
        );
    });
    for file in files_under(dir.path()) {
        if let Some(id) = format_of(&file).key_id() {
            assert_eq!(id, new.active_key_id(), "{}", file.display());
        }
    }
    with_keyring(Some(new_only), || {
        let wal = FileWal::open(&path).unwrap();
        assert_eq!(
            state(&InMemoryStore::load_from_wal(&wal).unwrap()),
            expected
        );
        let disk = DiskBackedStore::new(&redb_path).unwrap();
        let mut from_disk = InMemoryStore::new();
        disk.bulk_load_claims_into(&mut from_disk).unwrap();
        assert_eq!(state(&from_disk), expected);
    });
}

#[test]
fn rewrap_moves_a_closed_file_to_the_active_key_offline() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("dash.wal");
    with_keyring(Some(key(8)), || {
        let mut wal = FileWal::open(&path).unwrap();
        let mut store = InMemoryStore::new();
        write(&mut store, &mut wal, 0, 3);
    });
    let before = std::fs::read(&path).unwrap();
    let outcome = encryption::rewrap_file(&rotated(9, 8), &path).unwrap();
    assert!(matches!(
        outcome,
        encryption::RewrapOutcome::Rewrapped { .. }
    ));
    let after = std::fs::read(&path).unwrap();
    // Only the header line changed.
    let tail = |b: &[u8]| b[b.iter().position(|c| *c == b'\n').unwrap()..].to_vec();
    assert_eq!(tail(&before), tail(&after));
    with_keyring(Some(key(9)), || {
        let wal = FileWal::open(&path).unwrap();
        assert_eq!(
            InMemoryStore::load_from_wal(&wal)
                .unwrap()
                .claims_for_tenant(TENANT)
                .len(),
            3
        );
    });
}

#[test]
fn plaintext_files_open_with_encryption_on_and_migrate() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("dash.wal");
    let snapshot = dir.path().join("dash.wal.snapshot");
    let redb_path = dir.path().join("dash.redb");
    let expected = with_keyring(None, || {
        let mut wal = FileWal::open(&path).unwrap();
        let mut store = InMemoryStore::new();
        write(&mut store, &mut wal, 0, 4);
        store.checkpoint_and_compact(&mut wal).unwrap();
        write(&mut store, &mut wal, 4, 3);
        DiskBackedStore::new(&redb_path)
            .unwrap()
            .checkpoint_from(&store)
            .unwrap();
        state(&store)
    });
    assert_eq!(format_of(&path), FileFormat::Plain);
    assert_eq!(format_of(&snapshot), FileFormat::Plain);
    with_keyring(Some(key(10)), || {
        let mut wal = FileWal::open(&path).unwrap();
        // The live WAL is rewritten encrypted on open; the snapshot is read
        // as it is until the next checkpoint.
        assert!(matches!(format_of(&path), FileFormat::Lines(_)));
        assert_eq!(format_of(&snapshot), FileFormat::Plain);
        let mut store = InMemoryStore::load_from_wal(&wal).unwrap();
        assert_eq!(state(&store), expected);
        // Plaintext redb values are read; new ones are sealed.
        let disk = DiskBackedStore::new(&redb_path).unwrap();
        assert!(disk.encrypts_values());
        let mut from_disk = InMemoryStore::new();
        disk.bulk_load_claims_into(&mut from_disk).unwrap();
        assert_eq!(state(&from_disk), expected);
        write(&mut store, &mut wal, 7, 2);
        disk.checkpoint_from(&store).unwrap();
        store.checkpoint_and_compact(&mut wal).unwrap();
        assert!(matches!(format_of(&snapshot), FileFormat::Lines(_)));
        drop(wal);
        let wal = FileWal::open(&path).unwrap();
        assert_eq!(
            state(&InMemoryStore::load_from_wal(&wal).unwrap()),
            state(&store)
        );
    });
}

#[test]
fn an_incomplete_encryption_header_is_a_torn_creation() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("dash.wal");
    let keyring = key(11);
    with_keyring(Some(keyring.clone()), || {
        drop(FileWal::open(&path).unwrap());
    });
    let header = std::fs::read(&path).unwrap();
    std::fs::write(&path, &header[..header.len() / 2]).unwrap();
    with_keyring(Some(keyring), || {
        let mut wal = FileWal::open(&path).unwrap();
        assert_eq!(wal.torn_tail_dropped(), 1);
        wal.append_claim(&claim("c0", 0)).unwrap();
        wal.flush_pending_sync().unwrap();
        drop(wal);
        let wal = FileWal::open(&path).unwrap();
        assert_eq!(wal.wal_record_count().unwrap(), 1);
    });
}

#[test]
fn redb_values_cannot_be_modified_or_moved_between_rows() {
    let dir = TempDir::new().unwrap();
    let redb_path = dir.path().join("dash.redb");
    let keyring = key(12);
    with_keyring(Some(keyring.clone()), || {
        let disk = DiskBackedStore::new(&redb_path).unwrap();
        disk.put_claim(&claim("a", 1)).unwrap();
        disk.put_claim(&claim("b", 2)).unwrap();
    });
    // Copy row "a" over row "b" (and flip a byte of "a").
    {
        use redb::ReadableTable;
        let table: redb::TableDefinition<&str, &[u8]> = redb::TableDefinition::new("dash_claims");
        let db = redb::Database::create(&redb_path).unwrap();
        let txn = db.begin_write().unwrap();
        {
            let mut t = txn.open_table(table).unwrap();
            let a = t.get("a").unwrap().unwrap().value().to_vec();
            assert!(!a.windows(MARKER.len()).any(|w| w == MARKER.as_bytes()));
            t.insert("b", a.as_slice()).unwrap();
            let mut damaged = a.clone();
            let last = damaged.len() - 1;
            damaged[last] ^= 1;
            t.insert("a", damaged.as_slice()).unwrap();
        }
        txn.commit().unwrap();
    }
    with_keyring(Some(keyring), || {
        let disk = DiskBackedStore::new(&redb_path).unwrap();
        for id in ["a", "b"] {
            let err = disk.get_claim(id).unwrap_err();
            assert!(err.contains("authentication failed"), "{id}: {err}");
        }
    });
}

#[test]
fn a_damaged_sealed_vector_index_is_discarded_not_misread() {
    let dir = TempDir::new().unwrap();
    let wal_path = dir.path().join("dash.wal");
    let vindex = dir.path().join("dash.vindex");
    with_keyring(Some(key(13)), || {
        let mut wal = FileWal::open(&wal_path).unwrap();
        let mut store = InMemoryStore::new();
        write(&mut store, &mut wal, 0, 6);
        store
            .vector_index_snapshot(wal.position())
            .save(&vindex)
            .unwrap();
        let mut bytes = std::fs::read(&vindex).unwrap();
        let at = bytes.len() - 40;
        bytes[at] ^= 0x10;
        std::fs::write(&vindex, &bytes).unwrap();
        let (_, stats) = InMemoryStore::load_from_wal_with_vector_index(
            &wal,
            AnnTuningConfig::default(),
            ReplayPolicy::Strict,
            Some(&vindex),
        )
        .unwrap();
        let reason = format!("{:?}", stats.vector_index);
        assert!(reason.contains("authentication failed"), "{reason}");
    });
}

#[test]
fn quarantined_legacy_lines_stay_encrypted() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("dash.wal");
    with_keyring(Some(key(14)), || {
        let mut wal = FileWal::open_with_policy(&path, WalWritePolicy::default()).unwrap();
        wal.append_claim(&claim("c0", 0)).unwrap();
        wal.append_raw_record_line(&format!("C\t{MARKER}-legacy-broken"))
            .unwrap();
        wal.flush_pending_sync().unwrap();
        drop(wal);
        let wal = FileWal::open(&path).unwrap();
        let (store, stats) = InMemoryStore::load_from_wal_with_policy(
            &wal,
            AnnTuningConfig::default(),
            ReplayPolicy::Lenient,
        )
        .unwrap();
        assert_eq!(stats.replay.quarantined_records, 1);
        assert_eq!(store.claims_for_tenant(TENANT).len(), 1);
        let quarantine = wal.quarantine_path();
        assert!(matches!(format_of(&quarantine), FileFormat::Lines(_)));
        let text = String::from_utf8(
            encryption::read_all(&quarantine, encryption::current().as_deref()).unwrap(),
        )
        .unwrap();
        assert!(text.contains("legacy-broken"));
    });
    assert_no_marker(dir.path());
}
