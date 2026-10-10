//! Background checkpoints: writes proceed while a checkpoint's snapshot is
//! written, and the snapshot holds exactly the state at the rotation.
//!
//! The snapshot writer is held at a gate the test controls, so "the writes
//! did not wait for the snapshot" is shown deterministically: every write
//! completes while the writer has not even started.

use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use schema::{Claim, Evidence, Stance};
use store::{CHECKPOINT_IN_PROGRESS, FileWal, InMemoryStore, StoreError};
use tempfile::TempDir;

const TENANT: &str = "tenant-a";

fn claim(id: &str, version: usize) -> Claim {
    Claim {
        claim_id: id.to_string(),
        tenant_id: TENANT.to_string(),
        canonical_text: format!("background checkpoint claim {id} version {version}"),
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

fn write(store: &mut InMemoryStore, wal: &mut FileWal, id: &str, version: usize) {
    store
        .ingest_atomic_persistent(
            wal,
            claim(id, version),
            vec![evidence(&format!("e{version}-{id}"), id)],
            vec![],
            Some(vec![0.25, version as f32 / 100.0 + 0.01, 0.5, 0.75]),
            1_700_000_000_000 + version as u64,
        )
        .unwrap();
}

fn sorted_claims(store: &InMemoryStore) -> Vec<String> {
    store
        .claims_for_tenant(TENANT)
        .into_iter()
        .map(|c| format!("{}={}", c.claim_id, c.canonical_text))
        .collect()
}

#[test]
fn writes_proceed_while_the_snapshot_is_written_and_the_snapshot_is_the_rotation_state() {
    let dir = TempDir::new().unwrap();
    let wal_path = dir.path().join("dash.wal");
    let mut wal = FileWal::open(&wal_path).unwrap();
    let mut store = InMemoryStore::new();
    for i in 0..2_000 {
        write(&mut store, &mut wal, &format!("c{i}"), 0);
    }
    let before = sorted_claims(&store);
    let store = Arc::new(Mutex::new(store));
    let wal = Arc::new(Mutex::new(wal));

    // Rotation and state copy under both locks, as the services do it.
    let job = {
        let store = store.lock().unwrap();
        let mut wal = wal.lock().unwrap();
        store.begin_checkpoint(&mut wal).unwrap()
    };
    let expected_snapshot_records = job.stats().snapshot_records;
    assert_eq!(
        expected_snapshot_records,
        2_000 * 3,
        "claim, vector, evidence"
    );

    let (started_tx, started_rx) = mpsc::channel::<()>();
    let (gate_tx, gate_rx) = mpsc::channel::<()>();
    let writer = {
        let wal = Arc::clone(&wal);
        thread::spawn(move || {
            started_tx.send(()).unwrap();
            // The slow snapshot writer: it does not start before the test
            // has finished every write below.
            gate_rx.recv().unwrap();
            job.write().unwrap();
            let done = job.finish(&mut wal.lock().unwrap()).unwrap();
            done.stats.clone()
        })
    };
    started_rx.recv().unwrap();

    // Updates of existing claims (shards the frozen copy shares) and new
    // claims, while the snapshot is not written.
    let mut latencies = Vec::new();
    for i in 0..300 {
        let started = Instant::now();
        let mut store = store.lock().unwrap();
        let mut wal = wal.lock().unwrap();
        assert!(
            wal.checkpoint_in_flight(),
            "the snapshot is not written yet"
        );
        if i % 2 == 0 {
            write(&mut store, &mut wal, &format!("c{i}"), 1);
        } else {
            write(&mut store, &mut wal, &format!("new{i}"), 1);
        }
        latencies.push(started.elapsed());
    }
    // A second checkpoint is refused while one is in flight, and changes
    // nothing.
    {
        let store = store.lock().unwrap();
        let mut wal = wal.lock().unwrap();
        let generation = wal.generation();
        match store.begin_checkpoint(&mut wal) {
            Err(StoreError::Conflict(reason)) => assert_eq!(reason, CHECKPOINT_IN_PROGRESS),
            other => panic!("expected checkpoint_in_progress, got {:?}", other.err()),
        }
        assert_eq!(wal.generation(), generation);
    }
    let max = latencies.iter().max().copied().unwrap();
    assert!(
        max < Duration::from_secs(2),
        "a write took {max:?} while the checkpoint was in flight"
    );

    gate_tx.send(()).unwrap();
    let stats = writer.join().unwrap();
    assert_eq!(stats.snapshot_records, expected_snapshot_records);

    let after = sorted_claims(&store.lock().unwrap());
    let mut wal = wal.lock().unwrap();
    assert!(!wal.checkpoint_in_flight());
    assert!(!wal.checkpoint_pending());
    // The writes made during the checkpoint are exactly the new WAL.
    assert_eq!(wal.wal_record_count().unwrap(), 300 * 5);
    let boundary = wal.replay_boundary().unwrap();
    assert_eq!(boundary.snapshot_record_count, expected_snapshot_records);
    assert_eq!(boundary.wal_delta_record_count, 300 * 5);

    // The snapshot alone is the state at the rotation, not the later one.
    let snapshot_only = TempDir::new().unwrap();
    std::fs::copy(
        wal.snapshot_path(),
        snapshot_only.path().join("dash.wal.snapshot"),
    )
    .unwrap();
    let frozen = InMemoryStore::load_from_wal(
        &FileWal::open(snapshot_only.path().join("dash.wal")).unwrap(),
    )
    .unwrap();
    assert_eq!(sorted_claims(&frozen), before);

    // Snapshot plus new WAL is the live state; nothing lost, nothing twice.
    wal.flush_pending_sync().unwrap();
    let replayed = InMemoryStore::load_from_wal(&FileWal::open(&wal_path).unwrap()).unwrap();
    assert_eq!(sorted_claims(&replayed), after);
    assert_eq!(replayed.claims_len(), 2_000 + 150);
}

#[test]
fn a_failed_snapshot_write_leaves_a_recoverable_state_and_the_next_checkpoint_supersedes_it() {
    let dir = TempDir::new().unwrap();
    let wal_path = dir.path().join("dash.wal");
    let mut wal = FileWal::open(&wal_path).unwrap();
    let mut store = InMemoryStore::new();
    for i in 0..50 {
        write(&mut store, &mut wal, &format!("c{i}"), 0);
    }
    store.checkpoint_and_compact(&mut wal).unwrap();
    for i in 50..60 {
        write(&mut store, &mut wal, &format!("c{i}"), 0);
    }
    let job = store.begin_checkpoint(&mut wal).unwrap();
    for i in 60..70 {
        write(&mut store, &mut wal, &format!("c{i}"), 0);
    }
    // The snapshot write fails (here: given up before it ran).
    job.abort(&mut wal);
    assert!(!wal.checkpoint_in_flight());
    assert!(wal.checkpoint_pending());
    let expected = sorted_claims(&store);
    let replayed = InMemoryStore::load_from_wal(&FileWal::open(&wal_path).unwrap()).unwrap();
    assert_eq!(sorted_claims(&replayed), expected);

    for i in 70..75 {
        write(&mut store, &mut wal, &format!("c{i}"), 0);
    }
    store.checkpoint_and_compact(&mut wal).unwrap();
    assert!(!wal.checkpoint_pending());
    assert_eq!(wal.wal_record_count().unwrap(), 0);
    let expected = sorted_claims(&store);
    drop(wal);
    let replayed = InMemoryStore::load_from_wal(&FileWal::open(&wal_path).unwrap()).unwrap();
    assert_eq!(sorted_claims(&replayed), expected);
    assert_eq!(replayed.claims_len(), 75);
}
