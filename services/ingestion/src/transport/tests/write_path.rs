//! Write-path atomicity and idempotency: staged batches (DATA-09), single
//! ingest commit groups and deferred checkpoints (DATA-10), content-aware
//! batch idempotency and document updates (DATA-12).

use std::path::Path;

use schema::Claim;
use store::AnnTuningConfig;

use super::super::*;
use super::{env_lock, sample_runtime};

fn claim(id: &str, tenant: &str, text: &str) -> Claim {
    Claim {
        claim_id: id.to_string(),
        tenant_id: tenant.to_string(),
        canonical_text: text.to_string(),
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

fn item(id: &str, text: &str) -> IngestApiRequest {
    IngestApiRequest {
        claim: claim(id, "tenant-a", text),
        claim_embedding: Some(vec![0.1, 0.2, 0.3]),
        evidence: vec![],
        edges: vec![],
    }
}

fn batch(commit_id: &str, items: Vec<IngestApiRequest>) -> IngestBatchApiRequest {
    IngestBatchApiRequest {
        commit_id: Some(commit_id.to_string()),
        items,
    }
}

fn persistent_runtime(dir: &Path, policy: CheckpointPolicy) -> IngestionRuntime {
    let store = InMemoryStore::new().attach_disk(dir.join("store.redb"));
    assert_eq!(store.disk_status(), &DiskStatus::Available);
    let wal = FileWal::open(dir.join("wal.log")).expect("wal should open");
    IngestionRuntime::persistent(store, wal, policy)
}

/// Cold start from redb alone (an empty WAL), i.e. exactly what is durable
/// in the disk store.
fn load_redb_only(dir: &Path) -> InMemoryStore {
    let mut empty_wal = FileWal::open(dir.join("empty.log")).expect("empty wal");
    let (store, _) = InMemoryStore::load_from_disk_and_wal(
        dir.join("store.redb"),
        &mut empty_wal,
        AnnTuningConfig::default(),
    )
    .expect("redb should load");
    store
}

#[test]
fn failed_wal_append_leaves_memory_and_redb_untouched() {
    let _guard = env_lock().lock().expect("env lock");
    let dir = tempfile::tempdir().unwrap();
    let mut runtime = persistent_runtime(dir.path(), CheckpointPolicy::default());
    runtime
        .ingest_batch(batch(
            "commit-ok",
            vec![item("a1", "First committed claim")],
        ))
        .expect("first batch commits");
    assert_eq!(runtime.store.claims_len(), 1);

    // Make every WAL append fail: replace the log file with a directory.
    let wal_path = dir.path().join("wal.log");
    std::fs::remove_file(&wal_path).unwrap();
    std::fs::create_dir(&wal_path).unwrap();

    let result = runtime.ingest_batch(batch(
        "commit-fails",
        vec![item("b1", "Claim that must not land")],
    ));
    assert!(result.is_err(), "WAL failure must surface as an error");
    assert!(runtime.store.claim_by_id("b1").is_none(), "memory leaked");
    assert_eq!(runtime.store.claims_len(), 1);
    assert!(
        runtime
            .store
            .batch_commit_metadata("commit-fails")
            .is_none()
    );
    drop(runtime);

    let from_redb = load_redb_only(dir.path());
    assert!(from_redb.claim_by_id("a1").is_some());
    assert!(from_redb.claim_by_id("b1").is_none(), "redb leaked");
}

#[test]
fn committed_batch_persists_to_redb_and_survives_restart() {
    let _guard = env_lock().lock().expect("env lock");
    let dir = tempfile::tempdir().unwrap();
    let mut runtime = persistent_runtime(dir.path(), CheckpointPolicy::default());
    runtime
        .ingest_batch(batch(
            "commit-1",
            vec![
                item("a1", "Alpha claim text"),
                item("a2", "Beta claim text"),
            ],
        ))
        .expect("batch commits");
    drop(runtime);

    // redb alone has the batch (it was mirrored after the WAL append).
    let from_redb = load_redb_only(dir.path());
    assert!(from_redb.claim_by_id("a1").is_some());
    assert!(from_redb.claim_by_id("a2").is_some());
    assert!(from_redb.batch_commit_metadata("commit-1").is_some());

    // And a full restart (redb + WAL tail) sees the same state.
    let mut wal = FileWal::open(dir.path().join("wal.log")).unwrap();
    let (restarted, _) = InMemoryStore::load_from_disk_and_wal(
        dir.path().join("store.redb"),
        &mut wal,
        AnnTuningConfig::default(),
    )
    .unwrap();
    assert_eq!(restarted.claims_len(), 2);
    assert!(restarted.batch_commit_metadata("commit-1").is_some());
}

#[test]
fn batch_after_commit_keeps_the_redb_handle_for_later_writes() {
    let _guard = env_lock().lock().expect("env lock");
    let dir = tempfile::tempdir().unwrap();
    let mut runtime = persistent_runtime(dir.path(), CheckpointPolicy::default());
    runtime
        .ingest_batch(batch("c1", vec![item("a1", "First claim text")]))
        .unwrap();
    // A single ingest after a batch must still reach redb.
    runtime
        .ingest(item("s1", "Single claim after batch"))
        .unwrap();
    assert_eq!(runtime.store.disk_status(), &DiskStatus::Available);
    drop(runtime);
    let from_redb = load_redb_only(dir.path());
    assert!(from_redb.claim_by_id("a1").is_some());
    assert!(from_redb.claim_by_id("s1").is_some());
}

#[test]
fn single_ingest_retry_is_a_noop_and_does_not_grow_the_wal() {
    let _guard = env_lock().lock().expect("env lock");
    let dir = tempfile::tempdir().unwrap();
    let mut runtime = persistent_runtime(dir.path(), CheckpointPolicy::default());
    runtime.ingest(item("s1", "Single claim text")).unwrap();
    let records = runtime
        .wal
        .as_ref()
        .map(lock_wal)
        .unwrap()
        .wal_record_count()
        .unwrap();
    let retry = runtime.ingest(item("s1", "Single claim text")).unwrap();
    assert!(!retry.checkpoint_deferred);
    assert_eq!(
        runtime
            .wal
            .as_ref()
            .map(lock_wal)
            .unwrap()
            .wal_record_count()
            .unwrap(),
        records,
        "an identical retry must not append to the WAL"
    );
    assert_eq!(runtime.store.claims_len(), 1);
}

#[test]
fn checkpoint_failure_after_commit_is_reported_as_deferred_not_an_error() {
    let _guard = env_lock().lock().expect("env lock");
    let dir = tempfile::tempdir().unwrap();
    let policy = CheckpointPolicy {
        max_wal_records: Some(1),
        max_wal_bytes: None,
    };
    let mut runtime = persistent_runtime(dir.path(), policy);
    // The rotation writes the pending marker through
    // `<wal>.snapshot.pending.tmp`; a directory there makes the checkpoint
    // fail (synchronously) after the write has been committed.
    let blocker = dir.path().join("wal.log.snapshot.pending.tmp");
    std::fs::create_dir(&blocker).unwrap();

    let response = runtime
        .ingest(item("s1", "Committed despite checkpoint failure"))
        .expect("a committed write must not turn into an error");
    assert!(response.checkpoint_deferred);
    assert!(!response.checkpoint_triggered);
    assert!(runtime.store.claim_by_id("s1").is_some());
    drop(runtime);

    // The write is durable: a restart (once the path is usable again)
    // replays it from the WAL.
    std::fs::remove_dir(&blocker).unwrap();
    let wal = FileWal::open(dir.path().join("wal.log")).unwrap();
    let replayed = InMemoryStore::load_from_wal(&wal).unwrap();
    assert!(replayed.claim_by_id("s1").is_some());
    assert!(replayed.edges_for_claim("s1").is_empty());
}

#[test]
fn a_failed_background_snapshot_write_keeps_every_write_and_the_next_checkpoint_publishes() {
    let _guard = env_lock().lock().expect("env lock");
    let dir = tempfile::tempdir().unwrap();
    let policy = CheckpointPolicy {
        max_wal_records: Some(1),
        max_wal_bytes: None,
    };
    let mut runtime = persistent_runtime(dir.path(), policy);
    runtime.set_checkpoint_retry_interval_for_tests(std::time::Duration::ZERO);
    // The snapshot is written to `<wal>.snapshot.tmp` by the background
    // thread; a directory there makes that write fail after the response.
    let tmp = dir.path().join("wal.log.snapshot.tmp");
    std::fs::create_dir(&tmp).unwrap();

    let response = runtime
        .ingest(item("s1", "Committed before a failing snapshot write"))
        .expect("a committed write must not turn into an error");
    assert!(response.checkpoint_triggered, "the rotation succeeded");
    assert!(!response.checkpoint_deferred);
    runtime.wait_for_checkpoint();
    {
        let wal = lock_wal(runtime.wal.as_ref().unwrap());
        // Held for a retry: no new rotation while the write keeps failing.
        assert!(wal.checkpoint_in_flight());
        assert!(
            wal.checkpoint_pending(),
            "the failed write leaves the pending state"
        );
    }
    let generation = lock_wal(runtime.wal.as_ref().unwrap()).generation();
    let response = runtime
        .ingest(item(
            "s1b",
            "Committed while the snapshot cannot be written",
        ))
        .unwrap();
    assert!(!response.checkpoint_triggered, "no second rotation");
    runtime.wait_for_checkpoint();
    assert_eq!(
        lock_wal(runtime.wal.as_ref().unwrap()).generation(),
        generation,
        "the retry writes the same checkpoint again"
    );
    let pending_copy = tempfile::tempdir().unwrap();
    for entry in std::fs::read_dir(dir.path()).unwrap() {
        let entry = entry.unwrap();
        if entry.file_type().unwrap().is_file() && entry.file_name() != "store.redb" {
            std::fs::copy(entry.path(), pending_copy.path().join(entry.file_name())).unwrap();
        }
    }
    let restarted_wal = FileWal::open(pending_copy.path().join("wal.log")).unwrap();
    let replayed = InMemoryStore::load_from_wal(&restarted_wal).unwrap();
    assert!(replayed.claim_by_id("s1").is_some(), "a restart replays it");
    // A restart supersedes the unpublished checkpoint right away.
    let mut restarted =
        IngestionRuntime::persistent(replayed, restarted_wal, CheckpointPolicy::default());
    restarted.wait_for_checkpoint();
    {
        let wal = lock_wal(restarted.wal.as_ref().unwrap());
        assert!(!wal.checkpoint_pending(), "published at startup");
    }
    drop(restarted);
    let after_restart =
        InMemoryStore::load_from_wal(&FileWal::open(pending_copy.path().join("wal.log")).unwrap())
            .unwrap();
    assert!(after_restart.claim_by_id("s1").is_some());

    std::fs::remove_dir(&tmp).unwrap();
    runtime
        .ingest(item("s2", "Committed after the disk recovered"))
        .unwrap();
    // That write retried the held checkpoint, which now publishes.
    runtime.wait_for_checkpoint();
    {
        let wal = lock_wal(runtime.wal.as_ref().unwrap());
        assert!(!wal.checkpoint_pending(), "the next checkpoint published");
    }
    drop(runtime);
    let replayed =
        InMemoryStore::load_from_wal(&FileWal::open(dir.path().join("wal.log")).unwrap()).unwrap();
    assert!(replayed.claim_by_id("s1").is_some());
    assert!(replayed.claim_by_id("s1b").is_some());
    assert!(replayed.claim_by_id("s2").is_some());
}

#[test]
fn batch_with_same_commit_id_and_edited_content_is_an_update_that_survives_restart() {
    let _guard = env_lock().lock().expect("env lock");
    let dir = tempfile::tempdir().unwrap();
    let mut runtime = persistent_runtime(dir.path(), CheckpointPolicy::default());

    let first = runtime
        .ingest_batch(batch("doc-1", vec![item("d1", "Original sentence text")]))
        .unwrap();
    assert!(!first.idempotent_replay && !first.updated);

    // Same ids, same count, edited text: not a replay, applied as an update.
    let edited = runtime
        .ingest_batch(batch("doc-1", vec![item("d1", "Edited sentence text")]))
        .unwrap();
    assert!(!edited.idempotent_replay);
    assert!(edited.updated);
    assert_eq!(
        runtime.store.claim_by_id("d1").unwrap().canonical_text,
        "Edited sentence text"
    );

    // Identical re-submission is a replay.
    let replay = runtime
        .ingest_batch(batch("doc-1", vec![item("d1", "Edited sentence text")]))
        .unwrap();
    assert!(replay.idempotent_replay && !replay.updated);

    // A different claim-id set under the same commit id is also an update
    // (no 409), recorded under a versioned commit id.
    let grown = runtime
        .ingest_batch(batch(
            "doc-1",
            vec![
                item("d1", "Edited sentence text"),
                item("d2", "A second sentence appears"),
            ],
        ))
        .unwrap();
    assert!(grown.updated);
    assert_eq!(runtime.store.claims_len(), 2);
    drop(runtime);

    // Replay of the whole history (WAL only) reproduces the final state
    // without a commit-id conflict.
    let wal = FileWal::open(dir.path().join("wal.log")).unwrap();
    let replayed = InMemoryStore::load_from_wal(&wal).expect("history replays cleanly");
    assert_eq!(
        replayed.claim_by_id("d1").unwrap().canonical_text,
        "Edited sentence text"
    );
    assert!(replayed.claim_by_id("d2").is_some());
}

#[test]
fn batch_conflict_is_only_reported_for_cross_tenant_claim_ids() {
    let _guard = env_lock().lock().expect("env lock");
    let mut runtime = IngestionRuntime::in_memory(InMemoryStore::new());
    runtime
        .ingest_batch(batch("c1", vec![item("shared-id", "Tenant A claim text")]))
        .unwrap();
    let other_tenant = IngestApiRequest {
        claim: claim("shared-id", "tenant-b", "Tenant B claim text"),
        claim_embedding: None,
        evidence: vec![],
        edges: vec![],
    };
    let err = runtime
        .ingest_batch(batch("c2", vec![other_tenant]))
        .unwrap_err();
    assert!(matches!(err, StoreError::Conflict(_)));
}

fn raw_request(doc: &str, text: &str) -> HttpRequest {
    let body = format!(
        r#"{{"tenant_id":"tenant-a","document_id":"{doc}","source_id":"source://{doc}","text":"{text}","min_sentence_chars":10,"max_claims":8}}"#
    );
    HttpRequest {
        method: "POST".to_string(),
        target: "/v1/ingest/raw".to_string(),
        headers: HashMap::from([("content-type".to_string(), "application/json".to_string())]),
        body: body.into_bytes(),
    }
}

#[test]
fn edited_document_with_same_sentence_count_is_updated_not_dropped() {
    let _guard = env_lock().lock().expect("env lock");
    let runtime = sample_runtime();
    let first = handle_request(
        &runtime,
        &raw_request(
            "doc-edit",
            "Company X acquired Company Y in 2024. Revenue rose by twenty percent.",
        ),
    );
    assert_eq!(first.status, 200, "{}", first.body);
    assert!(first.body.contains("\"idempotent_replay\":false"));

    let edited = handle_request(
        &runtime,
        &raw_request(
            "doc-edit",
            "Company X sold Company Y in 2025. Revenue fell by twenty percent.",
        ),
    );
    assert_eq!(edited.status, 200, "{}", edited.body);
    assert!(
        edited.body.contains("\"idempotent_replay\":false"),
        "{}",
        edited.body
    );
    assert!(edited.body.contains("\"updated\":true"), "{}", edited.body);

    let guard = runtime.lock().unwrap();
    let texts: Vec<String> = guard
        .store
        .claims_for_tenant("tenant-a")
        .into_iter()
        .map(|c| c.canonical_text)
        .collect();
    assert_eq!(texts.len(), 2);
    assert!(texts.iter().any(|t| t.contains("sold Company Y in 2025")));
    assert!(!texts.iter().any(|t| t.contains("acquired")));
    drop(guard);

    let replay = handle_request(
        &runtime,
        &raw_request(
            "doc-edit",
            "Company X sold Company Y in 2025. Revenue fell by twenty percent.",
        ),
    );
    assert!(
        replay.body.contains("\"idempotent_replay\":true"),
        "{}",
        replay.body
    );
}

#[test]
fn colliding_tenant_and_document_ids_do_not_overwrite_each_other() {
    let _guard = env_lock().lock().expect("env lock");
    let runtime = sample_runtime();
    let text = "Company X acquired Company Y in 2024.";
    let a = HttpRequest {
        body: format!(
            r#"{{"tenant_id":"a:b","document_id":"c","source_id":"s","text":"{text}","min_sentence_chars":10}}"#
        )
        .into_bytes(),
        ..raw_request("x", "x")
    };
    let b = HttpRequest {
        body: format!(
            r#"{{"tenant_id":"a","document_id":"b:c","source_id":"s","text":"{text}","min_sentence_chars":10}}"#
        )
        .into_bytes(),
        ..raw_request("x", "x")
    };
    let first = handle_request(&runtime, &a);
    let second = handle_request(&runtime, &b);
    assert_eq!(first.status, 200, "{}", first.body);
    assert_eq!(second.status, 200, "{}", second.body);
    assert!(second.body.contains("\"idempotent_replay\":false"));
    let guard = runtime.lock().unwrap();
    assert_eq!(guard.store.claim_count_for_tenant("a:b"), 1);
    assert_eq!(guard.store.claim_count_for_tenant("a"), 1);
}
