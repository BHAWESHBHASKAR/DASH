//! Chunked export and generation-switch tests. They drive a leader WAL and
//! a follower (WAL + store) exactly the way the services do (see
//! `services/*/src/**/replication.rs`), compare the follower's live and
//! replayed state with the leader's after every step, and break the
//! protocol at the interesting points: an interrupted download, a corrupted
//! chunk, an export pruned mid-download, a checkpoint while an export is
//! built or downloaded, a crash in the middle of the swap, and followers
//! that are behind, at, or ahead of the end of a closed generation.

use std::cell::Cell;
use std::fs;
use std::path::Path;
use std::sync::Mutex;

use schema::{ClaimEdge, Evidence, Relation, Stance, claim_builder};
use tempfile::TempDir;

use super::*;
use crate::InMemoryStore;

const TENANT: &str = "tenant-a";

fn evidence(id: &str, claim_id: &str) -> Evidence {
    Evidence {
        evidence_id: id.to_string(),
        claim_id: claim_id.to_string(),
        source_id: format!("source://{id}"),
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

/// A leader: store plus WAL behind the mutex the export store locks.
struct Leader {
    store: InMemoryStore,
    wal: Mutex<FileWal>,
    exports: ReplicationExportStore,
    next: Cell<usize>,
}

impl Leader {
    fn open(dir: &Path) -> Self {
        let path = dir.join("leader.wal");
        let wal = FileWal::open(&path).unwrap();
        let store = InMemoryStore::load_from_wal(&wal).unwrap();
        Self {
            store,
            exports: ReplicationExportStore::for_wal(&path),
            wal: Mutex::new(wal),
            next: Cell::new(0),
        }
    }

    fn wal(&self) -> std::sync::MutexGuard<'_, FileWal> {
        self.wal.lock().unwrap()
    }

    /// One atomic bundle (claim, two evidence rows, an edge, a vector):
    /// a commit group of several lines. Ids repeat every `id_space` writes,
    /// so later writes are updates.
    fn write(&mut self, id_space: usize) {
        let n = self.next.get();
        self.next.set(n + 1);
        let id = format!("c{}", n % id_space);
        let mut wal = self.wal.lock().unwrap();
        self.store
            .ingest_atomic_persistent(
                &mut wal,
                claim_builder(
                    &id,
                    TENANT,
                    &format!("replicated claim {id} version {n}"),
                    0.9,
                ),
                vec![
                    evidence(&format!("e-{id}"), &id),
                    evidence(&format!("e{n}-{id}"), &id),
                ],
                vec![edge(&format!("g-{id}"), &id, "c0")],
                Some(vec![0.1, (n % 7) as f32 / 7.0, 0.3]),
                1_700_000_000_000 + n as u64,
            )
            .unwrap();
    }

    fn write_n(&mut self, n: usize) {
        for _ in 0..n {
            self.write(25);
        }
    }

    fn delete(&mut self, id: &str) {
        let mut wal = self.wal.lock().unwrap();
        self.store
            .delete_persistent(
                &mut wal,
                Tombstone::Claim {
                    tenant_id: TENANT.to_string(),
                    claim_id: id.to_string(),
                },
                1_700_000_100_000,
            )
            .unwrap();
    }

    fn checkpoint(&self) {
        let mut wal = self.wal.lock().unwrap();
        self.store.checkpoint_and_compact(&mut wal).unwrap();
    }

    fn position(&self) -> (u64, usize) {
        self.wal().replication_position().unwrap()
    }

    fn source(&self) -> LocalExportSource<'_> {
        LocalExportSource {
            store: &self.exports,
            wal: &self.wal,
        }
    }
}

fn state(store: &InMemoryStore) -> Vec<String> {
    let mut out: Vec<String> = store
        .snapshot_records()
        .iter()
        .map(record_to_line)
        .collect();
    out.sort();
    out
}

/// A follower that behaves like the service followers: delta frames with
/// generation switches, whole commit groups only, in-place apply after the
/// WAL append, chunked resync into a fresh store.
struct Follower {
    dir: std::path::PathBuf,
    wal: FileWal,
    store: InMemoryStore,
    generation: Option<u64>,
    offset: usize,
    resyncs: usize,
    switches: usize,
    chunk_bytes: usize,
    max_records: usize,
}

impl Follower {
    fn open(dir: &Path) -> Self {
        let wal = FileWal::open(dir.join("follower.wal")).unwrap();
        let store = InMemoryStore::load_from_wal(&wal).unwrap();
        Self {
            dir: dir.to_path_buf(),
            wal,
            store,
            generation: None,
            offset: 0,
            resyncs: 0,
            switches: 0,
            chunk_bytes: 300,
            max_records: 7,
        }
    }

    fn paths(&self) -> DownloadPaths {
        DownloadPaths::for_wal(self.wal.path())
    }

    /// One poll. Returns `true` while more records are waiting.
    fn poll(&mut self, leader: &Leader) -> bool {
        let frame = leader
            .wal()
            .replication_frame_with_switch(self.generation, self.offset, self.max_records)
            .unwrap();
        if let Some(from) = frame.switched_from {
            assert_eq!(
                Some(from.generation),
                self.generation,
                "switch from our generation"
            );
            assert_eq!(from.records, self.offset, "switch from our offset");
            assert_eq!(frame.from_offset, 0);
            self.store.checkpoint_and_compact(&mut self.wal).unwrap();
            self.switches += 1;
            self.generation = Some(frame.generation);
            self.offset = 0;
        } else if frame.needs_resync || self.generation.is_some_and(|g| g != frame.generation) {
            self.resync(leader);
            return true;
        }
        assert_eq!(frame.from_offset, self.offset);
        let keep = complete_group_prefix_len(&frame.wal_lines);
        let lines = &frame.wal_lines[..keep];
        if !lines.is_empty() {
            self.wal.append_replicated_lines(lines).unwrap();
            self.store.apply_replicated_lines(lines, true).unwrap();
        }
        self.generation = Some(frame.generation);
        self.offset = frame.from_offset + keep;
        self.offset < frame.total_records
    }

    fn sync(&mut self, leader: &Leader) {
        for _ in 0..10_000 {
            if !self.poll(leader) {
                return;
            }
        }
        panic!("follower never caught up");
    }

    fn resync(&mut self, leader: &Leader) {
        let paths = self.paths();
        let outcome = download_export(&mut leader.source(), &paths, self.chunk_bytes).unwrap();
        let DownloadOutcome::Complete(download) = outcome else {
            panic!("local source always supports chunks");
        };
        self.apply(&download);
        paths.remove();
    }

    fn apply(&mut self, download: &DownloadedExport) {
        let mut fresh = InMemoryStore::new();
        download
            .file
            .for_each_line(|_, line| fresh.apply_persisted_record_line_lenient(line).map(|_| ()))
            .unwrap();
        self.wal
            .replace_with_replication_export_file(&download.file)
            .unwrap();
        self.store = fresh;
        self.generation = Some(download.manifest.generation);
        self.offset = download.manifest.wal_records;
        self.resyncs += 1;
    }

    /// The follower's state equals the leader's, live and after a replay
    /// of its own WAL (nothing lost, nothing applied twice).
    fn assert_matches(&self, leader: &Leader, what: &str) {
        let want = state(&leader.store);
        assert_eq!(state(&self.store), want, "{what}: live follower state");
        let replayed =
            InMemoryStore::load_from_wal(&FileWal::open(self.wal.path()).unwrap()).unwrap();
        assert_eq!(state(&replayed), want, "{what}: replayed follower WAL");
        assert_eq!(
            self.offset,
            self.wal.wal_record_count().unwrap(),
            "{what}: the cursor counts exactly the local WAL"
        );
    }

    fn reopen(self) -> Self {
        let (generation, offset, resyncs, switches) =
            (self.generation, self.offset, self.resyncs, self.switches);
        let dir = self.dir.clone();
        drop(self);
        let mut follower = Follower::open(&dir);
        follower.generation = generation;
        follower.offset = offset;
        follower.resyncs = resyncs;
        follower.switches = switches;
        follower
    }
}

// ---------------------------------------------------------------------
// Chunked export
// ---------------------------------------------------------------------

#[test]
fn chunked_export_reproduces_the_single_response_export_and_the_leader_state() {
    let dir = TempDir::new().unwrap();
    let mut leader = Leader::open(dir.path());
    leader.write_n(30);
    leader.checkpoint();
    leader.write_n(12);
    leader.delete("c3");

    let manifest = leader.exports.begin(&leader.wal, None).unwrap();
    let legacy = leader.wal().replication_export().unwrap();
    assert_eq!(manifest.snapshot_records, legacy.snapshot_lines.len());
    assert_eq!(manifest.wal_records, legacy.wal_lines.len());
    assert_eq!(manifest.generation, leader.wal().generation());
    assert!(manifest.total_bytes > 10 * 300, "test premise: many chunks");

    // Bounded chunks, cut on line boundaries.
    let mut offset = 0;
    let mut body = String::new();
    while offset < manifest.total_bytes {
        let ChunkRead::Chunk(chunk) = leader
            .exports
            .read_chunk(&manifest.export_id, offset, 300)
            .unwrap()
        else {
            panic!("chunk must be served");
        };
        assert!(
            chunk.data.len() <= 300,
            "leader memory per chunk is bounded"
        );
        assert!(chunk.data.ends_with('\n'));
        let parsed = ReplicationExportChunk::parse_response(&chunk.render_response()).unwrap();
        assert_eq!(parsed, chunk);
        body.push_str(&chunk.data);
        offset = chunk.next_offset();
    }
    let mut expected = format!(
        "status=ok\ngeneration={}\nsnapshot_records={:020}\nwal_records={:020}\nSNAPSHOT\n",
        manifest.generation,
        legacy.snapshot_lines.len(),
        legacy.wal_lines.len()
    );
    for line in &legacy.snapshot_lines {
        expected.push_str(line);
        expected.push('\n');
    }
    expected.push_str("WAL\n");
    for line in &legacy.wal_lines {
        expected.push_str(line);
        expected.push('\n');
    }
    assert_eq!(body, expected);

    let mut follower = Follower::open(&dir.path().join("f"));
    follower.sync(&leader);
    assert_eq!(
        follower.resyncs, 1,
        "fresh follower of a checkpointed leader resyncs"
    );
    follower.assert_matches(&leader, "after chunked resync");
    assert!(!follower.paths().part.exists(), "download files removed");
    leader.write_n(5);
    follower.sync(&leader);
    follower.assert_matches(&leader, "after more deltas");
    assert_eq!(follower.resyncs, 1);
}

#[test]
fn chunk_requests_outside_the_export_are_refused() {
    let dir = TempDir::new().unwrap();
    let mut leader = Leader::open(dir.path());
    leader.write_n(5);
    let manifest = leader.exports.begin(&leader.wal, None).unwrap();
    let read = |id: &str, offset: u64| leader.exports.read_chunk(id, offset, 64).unwrap();
    assert!(matches!(
        read(&manifest.export_id, 1),
        ChunkRead::BadOffset(_)
    ));
    assert!(matches!(
        read(&manifest.export_id, manifest.total_bytes + 1),
        ChunkRead::BadOffset(_)
    ));
    assert_eq!(read("0123456789abcdef", 0), ChunkRead::NotFound);
    assert_eq!(read("../leader.wal", 0), ChunkRead::NotFound);
    let ChunkRead::Chunk(end) = read(&manifest.export_id, manifest.total_bytes) else {
        panic!("the end offset is a valid, empty chunk");
    };
    assert!(end.data.is_empty());
    // A single line longer than the chunk is served whole.
    let ChunkRead::Chunk(long) = read(&manifest.export_id, 0) else {
        panic!("chunk");
    };
    let ChunkRead::Chunk(tiny) = leader
        .exports
        .read_chunk(&manifest.export_id, 0, 1)
        .unwrap()
    else {
        panic!("chunk");
    };
    assert_eq!(tiny.data, "status=ok\n");
    assert!(long.data.starts_with("status=ok\n"));
}

#[test]
fn an_unchanged_leader_reuses_its_export_and_retention_prunes_old_ones() {
    let dir = TempDir::new().unwrap();
    let mut leader = Leader::open(dir.path());
    leader.write_n(4);
    let first = leader.exports.begin(&leader.wal, None).unwrap();
    let again = leader.exports.begin(&leader.wal, None).unwrap();
    assert_eq!(first, again, "nothing changed: same export");
    let avoided = leader
        .exports
        .begin(&leader.wal, Some(&first.export_id))
        .unwrap();
    assert_ne!(
        avoided.export_id, first.export_id,
        "avoid forces a new export"
    );
    leader.write(25);
    let third = leader.exports.begin(&leader.wal, None).unwrap();
    assert_ne!(third.export_id, avoided.export_id);
    assert!(third.wal_records > first.wal_records);
    let kept: Vec<String> = leader
        .exports
        .manifests()
        .into_iter()
        .map(|m| m.export_id)
        .collect();
    assert_eq!(kept.len(), EXPORTS_RETAINED);
    assert!(kept.contains(&third.export_id));
    assert!(!kept.contains(&first.export_id), "oldest export pruned");
    assert_eq!(
        leader.exports.read_chunk(&first.export_id, 0, 64).unwrap(),
        ChunkRead::NotFound
    );
    let files = fs::read_dir(leader.exports.dir()).unwrap().count();
    assert_eq!(
        files,
        2 * EXPORTS_RETAINED,
        "an export file and a manifest each"
    );
}

/// Wraps a source and breaks it on purpose.
struct FaultySource<'a> {
    inner: LocalExportSource<'a>,
    chunk_calls: usize,
    begins: Vec<Option<String>>,
    offsets: Vec<u64>,
    fail_on_chunk: Option<usize>,
    corrupt_chunks: usize,
    on_chunk: Option<Box<dyn FnMut(usize) + 'a>>,
}

impl<'a> FaultySource<'a> {
    fn new(leader: &'a Leader) -> Self {
        Self {
            inner: leader.source(),
            chunk_calls: 0,
            begins: Vec::new(),
            offsets: Vec::new(),
            fail_on_chunk: None,
            corrupt_chunks: 0,
            on_chunk: None,
        }
    }
}

impl ExportSource for FaultySource<'_> {
    fn begin(&mut self, avoid: Option<&str>) -> Result<Option<ReplicationExportManifest>, String> {
        self.begins.push(avoid.map(str::to_string));
        self.inner.begin(avoid)
    }

    fn chunk(
        &mut self,
        export_id: &str,
        offset: u64,
        max_bytes: usize,
    ) -> Result<ChunkFetch, String> {
        self.chunk_calls += 1;
        self.offsets.push(offset);
        if let Some(hook) = self.on_chunk.as_mut() {
            hook(self.chunk_calls);
        }
        if self.fail_on_chunk == Some(self.chunk_calls) {
            return Err("connection reset by peer".to_string());
        }
        let mut fetched = self.inner.chunk(export_id, offset, max_bytes)?;
        if self.corrupt_chunks > 0
            && let ChunkFetch::Chunk(chunk) = &mut fetched
            && let Some(pos) = chunk.data.find("claim")
        {
            self.corrupt_chunks -= 1;
            chunk.data.replace_range(pos..pos + 5, "clain");
        }
        Ok(fetched)
    }
}

#[test]
fn an_interrupted_download_resumes_from_the_part_file() {
    let dir = TempDir::new().unwrap();
    let mut leader = Leader::open(dir.path());
    leader.write_n(40);
    leader.checkpoint();
    leader.write_n(3);
    let follower_dir = dir.path().join("f");
    let mut follower = Follower::open(&follower_dir);
    let paths = follower.paths();

    let mut source = FaultySource::new(&leader);
    source.fail_on_chunk = Some(4);
    let err = download_export(&mut source, &paths, 256).unwrap_err();
    assert!(err.contains("connection reset"), "{err}");
    // Plaintext bytes held (the file length unless it is encrypted).
    let partial = encryption::read_all(&paths.part, crate::crypt::current_keyring().as_deref())
        .unwrap()
        .len() as u64;
    assert!(partial > 0, "three chunks were kept");
    assert_eq!(source.begins.len(), 1);

    // The process restarts; the next resync continues where it stopped.
    let mut source = FaultySource::new(&leader);
    let DownloadOutcome::Complete(download) = download_export(&mut source, &paths, 256).unwrap()
    else {
        panic!("complete");
    };
    assert!(source.begins.is_empty(), "same export, no new begin");
    assert_eq!(
        source.offsets[0], partial,
        "resumed at the part file's length"
    );
    assert!(download.resumed);
    assert_eq!(
        download.fetched_bytes,
        download.manifest.total_bytes - partial
    );
    follower.apply(&download);
    paths.remove();
    follower.sync(&leader);
    follower.assert_matches(&leader, "after a resumed download");
    assert_eq!(follower.resyncs, 1);
}

#[test]
fn a_checksum_mismatch_discards_the_download_and_asks_for_another_export() {
    let dir = TempDir::new().unwrap();
    let mut leader = Leader::open(dir.path());
    leader.write_n(20);
    let mut follower = Follower::open(&dir.path().join("f"));
    let paths = follower.paths();

    let mut source = FaultySource::new(&leader);
    source.corrupt_chunks = 1;
    let DownloadOutcome::Complete(download) = download_export(&mut source, &paths, 512).unwrap()
    else {
        panic!("complete");
    };
    assert_eq!(source.begins.len(), 2, "one restart");
    let first_id = source.begins[1]
        .clone()
        .expect("the restart avoids the bad export");
    assert_ne!(download.manifest.export_id, first_id);
    let (_, sha) = hash_file(&paths.part).unwrap();
    assert_eq!(sha, download.manifest.sha256);
    follower.apply(&download);
    paths.remove();
    follower.sync(&leader);
    follower.assert_matches(&leader, "after a corrupted chunk");

    // A source that corrupts every attempt is reported, nothing applied.
    let other = dir.path().join("g");
    let paths = DownloadPaths::for_wal(&other.join("follower.wal"));
    let mut source = FaultySource::new(&leader);
    source.corrupt_chunks = usize::MAX;
    let err = download_export(&mut source, &paths, 512).unwrap_err();
    assert!(err.contains("verification"), "{err}");
    assert!(!paths.part.exists(), "a bad download is not kept");
}

#[test]
fn an_export_pruned_mid_download_starts_a_new_one() {
    let dir = TempDir::new().unwrap();
    let mut leader = Leader::open(dir.path());
    leader.write_n(20);
    let paths = DownloadPaths::for_wal(&dir.path().join("f").join("follower.wal"));
    let mut source = FaultySource::new(&leader);
    source.fail_on_chunk = Some(2);
    download_export(&mut source, &paths, 256).unwrap_err();
    let first = source.begins.len();
    assert_eq!(first, 1);
    drop(source);
    // The leader moves on and drops the export (retention or restart).
    leader.write_n(2);
    for manifest in leader.exports.manifests() {
        fs::remove_file(
            leader
                .exports
                .dir()
                .join(format!("{}.manifest", manifest.export_id)),
        )
        .unwrap();
    }
    let mut source = FaultySource::new(&leader);
    let DownloadOutcome::Complete(download) = download_export(&mut source, &paths, 256).unwrap()
    else {
        panic!("complete");
    };
    assert_eq!(source.begins.len(), 1, "a new export was requested");
    assert!(!download.resumed);
    assert_eq!(download.manifest.wal_records, leader.position().1);
}

#[test]
fn a_checkpoint_during_the_download_does_not_change_the_export() {
    let dir = TempDir::new().unwrap();
    let mut leader = Leader::open(dir.path());
    leader.write_n(30);
    leader.checkpoint();
    leader.write_n(4);
    let frozen_state = state(&leader.store);
    let frozen_position = leader.position();

    // The follower starts downloading; meanwhile the leader checkpoints
    // (a new snapshot is renamed over the old one, the WAL is truncated)
    // and takes more writes.
    let mut follower = Follower::open(&dir.path().join("f"));
    let paths = follower.paths();
    let mut source = FaultySource::new(&leader);
    let checkpointed = Cell::new(false);
    source.on_chunk = Some(Box::new(|call| {
        if call == 2 && !checkpointed.replace(true) {
            leader.checkpoint();
        }
    }));
    let DownloadOutcome::Complete(download) = download_export(&mut source, &paths, 256).unwrap()
    else {
        panic!("complete");
    };
    drop(source);
    assert!(checkpointed.get());
    assert_eq!(
        (download.manifest.generation, download.manifest.wal_records),
        frozen_position,
        "the export is the frozen state"
    );
    follower.apply(&download);
    paths.remove();
    assert_eq!(state(&follower.store), frozen_state);
    // The checkpoint happened exactly at the frozen position (no writes in
    // between), so the follower crosses it without a second resync.
    follower.sync(&leader);
    assert_eq!(follower.resyncs, 1);
    assert_eq!(follower.switches, 1);
    follower.assert_matches(&leader, "export then switch");

    // Writes between the freeze and the checkpoint: the follower is behind
    // the closed generation's end and catches up from the closed file.
    let manifest = leader.exports.begin(&leader.wal, None).unwrap();
    let mut late = Follower::open(&dir.path().join("late"));
    let lpaths = late.paths();
    let mut source = LocalExportSource {
        store: &leader.exports,
        wal: &leader.wal,
    };
    let DownloadOutcome::Complete(download) = download_export(&mut source, &lpaths, 256).unwrap()
    else {
        panic!("complete");
    };
    assert_eq!(download.manifest, manifest);
    leader.write_n(2);
    leader.checkpoint();
    late.apply(&download);
    lpaths.remove();
    late.sync(&leader);
    // Nothing was written after the checkpoint, so the closed generation's
    // last frame reported "caught up"; the next poll switches.
    assert!(!late.poll(&leader));
    assert_eq!(
        (late.resyncs, late.switches),
        (1, 1),
        "behind the closed generation's end: served from its file, no second resync"
    );
    late.assert_matches(&leader, "late follower");
}

#[test]
fn a_checkpoint_between_freeze_and_build_does_not_change_the_export() {
    let dir = TempDir::new().unwrap();
    let mut leader = Leader::open(dir.path());
    leader.write_n(20);
    leader.checkpoint();
    leader.write_n(3);
    let expected = leader.wal().replication_export().unwrap();
    let position = leader.position();
    let checkpointed = Cell::new(false);
    let manifest = leader
        .exports
        .begin_with_hook(&leader.wal, None, &mut || {
            leader.checkpoint();
            checkpointed.set(true);
        })
        .unwrap();
    assert!(checkpointed.get());
    assert_ne!(leader.position().0, position.0, "the leader did checkpoint");
    assert_eq!((manifest.generation, manifest.wal_records), position);
    let file = ReplicationExportFile::open(
        leader
            .exports
            .dir()
            .join(format!("{}.export", manifest.export_id)),
    )
    .unwrap();
    let (len, sha) = hash_file(file.path()).unwrap();
    assert_eq!((len, sha), (manifest.total_bytes, manifest.sha256.clone()));
    let mut snapshot = Vec::new();
    let mut wal_lines = Vec::new();
    file.for_each_line(|section, line| {
        match section {
            ExportSection::Snapshot => snapshot.push(line.to_string()),
            ExportSection::Wal => wal_lines.push(line.to_string()),
        }
        Ok(())
    })
    .unwrap();
    assert_eq!(
        snapshot, expected.snapshot_lines,
        "the old snapshot was read"
    );
    assert_eq!(wal_lines, expected.wal_lines);
}

#[test]
fn a_crash_in_the_middle_of_the_swap_is_repaired_by_applying_the_export_again() {
    let dir = TempDir::new().unwrap();
    let mut leader = Leader::open(dir.path());
    leader.write_n(25);
    leader.checkpoint();
    leader.write_n(5);
    let follower_dir = dir.path().join("f");
    let mut follower = Follower::open(&follower_dir);
    // The follower holds stale data of its own.
    let mut stale = Leader::open(&follower_dir.join("stale"));
    stale.write_n(3);
    let stale_lines = stale.wal().replication_export().unwrap().wal_lines;
    follower.wal.append_replicated_lines(&stale_lines).unwrap();
    follower
        .store
        .apply_replicated_lines(&stale_lines, true)
        .unwrap();
    let paths = follower.paths();
    let DownloadOutcome::Complete(download) =
        download_export(&mut leader.source(), &paths, 300).unwrap()
    else {
        panic!("complete");
    };

    // Crash right after the new snapshot replaced the old one, before the
    // WAL was rewritten: the files mix the new snapshot with the old WAL.
    crate::failpoint::arm("export_apply.snapshot_replaced");
    let err = follower
        .wal
        .replace_with_replication_export_file(&download.file);
    crate::failpoint::disarm();
    assert!(err.is_err());
    drop(follower);

    // The services remove their cursor before the swap, so the restarted
    // follower resyncs: the verified download is still there and applies.
    let mut follower = Follower::open(&follower_dir);
    let DownloadOutcome::Complete(again) =
        download_export(&mut leader.source(), &paths, 300).unwrap()
    else {
        panic!("complete");
    };
    assert_eq!(again.fetched_bytes, 0, "nothing downloaded twice");
    assert_eq!(again.manifest, download.manifest);
    follower.apply(&again);
    paths.remove();
    follower.sync(&leader);
    follower.assert_matches(&leader, "after the crashed swap");
}

// ---------------------------------------------------------------------
// Generation switch
// ---------------------------------------------------------------------

#[test]
fn a_follower_at_the_end_of_the_closed_generation_switches_without_a_resync() {
    let dir = TempDir::new().unwrap();
    let mut leader = Leader::open(dir.path());
    let mut follower = Follower::open(&dir.path().join("f"));
    leader.write_n(10);
    follower.sync(&leader);
    follower.assert_matches(&leader, "initial");
    let (closed_generation, closed_len) = leader.position();

    leader.checkpoint();
    let transitions = leader.wal().generation_transitions().to_vec();
    assert_eq!(
        transitions,
        vec![GenerationTransition {
            from_generation: closed_generation,
            from_records: closed_len,
            to_generation: leader.position().0,
        }]
    );
    leader.write_n(6);
    leader.delete("c2");
    follower.sync(&leader);
    assert_eq!(
        follower.resyncs, 0,
        "no resync for a checkpoint at our position"
    );
    assert_eq!(follower.switches, 1);
    follower.assert_matches(&leader, "after the switch");

    // A restarted follower resumes in the new generation.
    let mut follower = follower.reopen();
    leader.write_n(3);
    follower.sync(&leader);
    assert_eq!((follower.resyncs, follower.switches), (0, 1));
    follower.assert_matches(&leader, "after restart");

    // Old followers (no switch support) are still sent to a resync.
    let frame = leader
        .wal()
        .replication_frame_from(Some(closed_generation), closed_len, 10)
        .unwrap();
    assert!(frame.needs_resync);
    assert!(frame.switched_from.is_none());
}

#[test]
fn followers_inside_the_closed_generation_catch_up_from_its_file_then_switch() {
    let dir = TempDir::new().unwrap();
    let mut leader = Leader::open(dir.path());
    leader.write_n(10);
    let (closed_generation, closed_len) = leader.position();
    // A follower that saw part of the generation (a checkpoint runs right
    // after the write that triggers it, so this is the usual case).
    let mut behind = Follower::open(&dir.path().join("behind"));
    behind.max_records = 9;
    behind.poll(&leader);
    assert!(
        behind.offset > 0 && behind.offset < closed_len,
        "test premise"
    );
    leader.checkpoint();
    assert_eq!(
        leader.wal().closed_generation(),
        Some((closed_generation, closed_len))
    );
    leader.write_n(4);

    // Frames of the closed generation, then the switch.
    let frame = leader
        .wal()
        .replication_frame_with_switch(Some(closed_generation), behind.offset, 9)
        .unwrap();
    assert!(!frame.needs_resync);
    assert_eq!(frame.generation, closed_generation);
    let current_len = leader.position().1;
    assert_eq!(
        frame.total_records,
        closed_len + current_len,
        "what is left in both generations"
    );
    assert!(!frame.wal_lines.is_empty());
    behind.sync(&leader);
    assert_eq!((behind.resyncs, behind.switches), (0, 1));
    behind.assert_matches(&leader, "behind follower");

    // Old followers (no switch support) are still sent to a resync.
    let frame = leader
        .wal()
        .replication_frame_from(Some(closed_generation), 3, 10)
        .unwrap();
    assert!(frame.needs_resync);

    // Ahead of the closed generation's end, an unknown generation, no
    // generation: resync.
    for (what, generation, offset) in [
        ("ahead", Some(closed_generation), closed_len + 1),
        ("unknown", Some(12345), 0),
        ("fresh", None, 0),
    ] {
        let frame = leader
            .wal()
            .replication_frame_with_switch(generation, offset, 10)
            .unwrap();
        assert!(frame.needs_resync, "{what}");
        assert!(frame.switched_from.is_none(), "{what}");
        assert!(frame.wal_lines.is_empty(), "{what}");
    }

    // After a restart the closed file is still served.
    drop(leader);
    let mut leader = Leader::open(dir.path());
    let mut late = Follower::open(&dir.path().join("late"));
    late.generation = Some(closed_generation);
    // The late follower holds the closed generation's first lines.
    let first = leader
        .wal()
        .replication_frame_with_switch(Some(closed_generation), 0, 7)
        .unwrap();
    assert_eq!(first.generation, closed_generation);
    let keep = complete_group_prefix_len(&first.wal_lines);
    late.wal
        .append_replicated_lines(&first.wal_lines[..keep])
        .unwrap();
    late.store
        .apply_replicated_lines(&first.wal_lines[..keep], true)
        .unwrap();
    late.offset = keep;
    late.sync(&leader);
    assert_eq!((late.resyncs, late.switches), (0, 1));
    late.assert_matches(&leader, "late follower after a leader restart");

    // The next checkpoint replaces the closed file: a follower still in the
    // older generation resyncs.
    leader.write_n(1);
    leader.checkpoint();
    let frame = leader
        .wal()
        .replication_frame_with_switch(Some(closed_generation), 2, 10)
        .unwrap();
    assert!(frame.needs_resync);
    let closed_files = fs::read_dir(dir.path())
        .unwrap()
        .filter(|e| {
            e.as_ref()
                .unwrap()
                .file_name()
                .to_string_lossy()
                .contains(".closed.")
        })
        .count();
    assert_eq!(closed_files, 1, "only the newest closed generation is kept");
}

#[test]
fn chained_checkpoints_switch_only_while_no_record_was_written_between_them() {
    let dir = TempDir::new().unwrap();
    let mut leader = Leader::open(dir.path());
    leader.write_n(8);
    let mut follower = Follower::open(&dir.path().join("f"));
    follower.sync(&leader);
    // Two checkpoints with nothing written in between: still one switch.
    leader.checkpoint();
    leader.checkpoint();
    leader.write_n(2);
    follower.sync(&leader);
    assert_eq!((follower.resyncs, follower.switches), (0, 1));
    follower.assert_matches(&leader, "chained");

    // Writes between two checkpoints the follower did not see: the second
    // generation's records exist only in the newer snapshot, so it resyncs.
    leader.checkpoint();
    leader.write_n(3);
    leader.checkpoint();
    follower.sync(&leader);
    assert_eq!(follower.resyncs, 1);
    follower.assert_matches(&leader, "missed generation");
}

#[test]
fn a_rollback_or_reset_after_a_checkpoint_breaks_the_chain() {
    let dir = TempDir::new().unwrap();
    let mut leader = Leader::open(dir.path());
    leader.write_n(6);
    let (closed_generation, closed_len) = leader.position();
    leader.checkpoint();
    // A rollback over already-flushed records starts another lineage that
    // no transition leads to.
    {
        let mut wal = leader.wal();
        let point = wal.begin_rollback_point().unwrap();
        wal.append_raw_record_line(&record_to_line(&PersistedRecord::Claim(claim_builder(
            "rolled-back",
            TENANT,
            "never committed",
            0.5,
        ))))
        .unwrap();
        wal.flush_pending_sync().unwrap();
        wal.rollback_to(point).unwrap();
    }
    let frame = leader
        .wal()
        .replication_frame_with_switch(Some(closed_generation), closed_len, 10)
        .unwrap();
    assert!(frame.needs_resync, "rollback lineage: resync");
}

#[test]
fn transitions_survive_a_restart_and_a_damaged_file_only_costs_a_resync() {
    let dir = TempDir::new().unwrap();
    let mut leader = Leader::open(dir.path());
    let mut closed = Vec::new();
    for _ in 0..(GENERATION_TRANSITIONS_KEPT + 3) {
        leader.write(25);
        closed.push(leader.position());
        leader.checkpoint();
    }
    let kept = leader.wal().generation_transitions().to_vec();
    assert_eq!(kept.len(), GENERATION_TRANSITIONS_KEPT);
    let path = transitions_path_for(leader.wal().path());
    drop(leader);
    let leader = Leader::open(dir.path());
    assert_eq!(leader.wal().generation_transitions(), kept.as_slice());
    let (last_generation, last_len) = *closed.last().unwrap();
    let frame = leader
        .wal()
        .replication_frame_with_switch(Some(last_generation), last_len, 10)
        .unwrap();
    assert!(
        frame.switched_from.is_some(),
        "persisted transition is used"
    );
    // The oldest transitions were dropped: those followers resync.
    let (old_generation, old_len) = closed[0];
    let frame = leader
        .wal()
        .replication_frame_with_switch(Some(old_generation), old_len, 10)
        .unwrap();
    assert!(frame.needs_resync);
    drop(leader);
    fs::write(&path, "not a transition\nzz 1 yy\n").unwrap();
    let leader = Leader::open(dir.path());
    assert!(leader.wal().generation_transitions().is_empty());
    let frame = leader
        .wal()
        .replication_frame_with_switch(Some(last_generation), last_len, 10)
        .unwrap();
    assert!(frame.needs_resync);
}

#[test]
fn a_crash_during_the_followers_local_checkpoint_falls_back_to_a_resync() {
    let dir = TempDir::new().unwrap();
    let mut leader = Leader::open(dir.path());
    leader.write_n(9);
    let follower_dir = dir.path().join("f");
    let mut follower = Follower::open(&follower_dir);
    follower.sync(&leader);
    let saved = (follower.generation, follower.offset);
    leader.checkpoint();
    leader.write_n(2);
    // The follower's own checkpoint (part of the switch) crashes after the
    // truncation, before its cursor was moved: the saved offset no longer
    // matches the local WAL, which the services treat as "resync".
    crate::failpoint::arm("wal.truncated");
    assert!(
        follower
            .store
            .checkpoint_and_compact(&mut follower.wal)
            .is_err()
    );
    crate::failpoint::disarm();
    drop(follower);
    let mut follower = Follower::open(&follower_dir);
    assert_ne!(
        Some(saved.1),
        follower.wal.wal_record_count().ok(),
        "the cursor check catches it"
    );
    follower.sync(&leader);
    assert_eq!(follower.resyncs, 1);
    follower.assert_matches(&leader, "after the crashed local checkpoint");
}

// ---------------------------------------------------------------------
// In-place apply of a frame
// ---------------------------------------------------------------------

#[test]
fn commit_group_spans_cover_every_line_and_keep_groups_whole() {
    let dir = TempDir::new().unwrap();
    let mut leader = Leader::open(dir.path());
    leader.write_n(3);
    let lines = leader.wal().replication_export().unwrap().wal_lines;
    let spans = commit_group_spans(&lines);
    assert_eq!(spans.len(), 3, "one span per bundle");
    assert_eq!(spans.first().unwrap().start, 0);
    assert_eq!(spans.last().unwrap().end, lines.len());
    for pair in spans.windows(2) {
        assert_eq!(pair[0].end, pair[1].start, "contiguous");
    }
    let mut mixed = vec!["C\tlegacy\ttenant-a\tungrouped legacy claim\t0.9\tnull\t\t".to_string()];
    mixed.extend(lines.iter().cloned());
    let spans = commit_group_spans(&mixed);
    assert_eq!(spans[0], 0..1, "an ungrouped record is its own span");
    assert_eq!(spans.len(), 4);
    let open_tail = &lines[..lines.len() - 1];
    let spans = commit_group_spans(open_tail);
    assert_eq!(
        spans.last().unwrap().end,
        open_tail.len(),
        "an open group runs to the end"
    );
}

#[test]
fn in_place_apply_writes_redb_once_and_rejects_unreadable_frames_untouched() {
    let dir = TempDir::new().unwrap();
    let mut leader = Leader::open(dir.path());
    leader.write_n(12);
    let lines = leader.wal().replication_export().unwrap().wal_lines;

    let mut follower = InMemoryStore::new().attach_disk(dir.path().join("follower.redb"));
    let mut bad = lines.clone();
    bad.insert(5, "this is not a wal record".to_string());
    assert!(follower.apply_replicated_lines(&bad, false).is_err());
    assert_eq!(
        follower.claims_len(),
        0,
        "nothing applied from an unreadable frame"
    );

    let outcome = follower.apply_replicated_lines(&lines, false).unwrap();
    assert_eq!(outcome.applied, lines.len());
    assert_eq!(outcome.skipped, 0);
    assert!(outcome.disk_error.is_none());
    assert_eq!(state(&follower), state(&leader.store));
    drop(follower);
    // The redb mirror holds the same state.
    let mut wal = FileWal::open(dir.path().join("empty.wal")).unwrap();
    let (reloaded, _) = InMemoryStore::load_from_disk_and_wal(
        dir.path().join("follower.redb"),
        &mut wal,
        crate::AnnTuningConfig::default(),
    )
    .unwrap();
    assert_eq!(state(&reloaded), state(&leader.store));
}

// ---------------------------------------------------------------------
// The same protocol with encryption at rest (ADR 0005): every file the
// leader and the follower write is encrypted, frames and chunks stay
// plaintext, and the follower ends up with the leader's state.
// ---------------------------------------------------------------------

mod encrypted {
    use std::sync::Arc;

    use super::*;

    fn keyring() -> Arc<encryption::Keyring> {
        Arc::new(encryption::Keyring::local([0x5a; 32], &[]).unwrap())
    }

    fn run(test: fn()) {
        encryption::with_keyring(Some(keyring()), test);
    }

    /// No file under `dir` holds `needle` in plaintext, and every data file
    /// is encrypted.
    fn assert_no_plaintext(dir: &Path, needle: &str) {
        let mut stack = vec![dir.to_path_buf()];
        let mut checked = 0usize;
        while let Some(path) = stack.pop() {
            if path.is_dir() {
                for entry in fs::read_dir(&path).unwrap() {
                    stack.push(entry.unwrap().path());
                }
                continue;
            }
            let bytes = fs::read(&path).unwrap();
            let name = path.file_name().unwrap().to_string_lossy().to_string();
            assert!(
                !bytes.windows(needle.len()).any(|w| w == needle.as_bytes()),
                "{} holds plaintext",
                path.display()
            );
            let metadata_only = name.ends_with(".gen")
                || name.ends_with(".transitions")
                || name.ends_with(".manifest");
            if !metadata_only && !bytes.is_empty() {
                let format = encryption::detect_format(&bytes).unwrap();
                assert!(format.is_encrypted(), "{} is not encrypted", path.display());
                checked += 1;
            }
        }
        assert!(checked > 0, "no data files under {}", dir.display());
    }

    #[test]
    fn export_and_resync_round_trip_with_every_file_encrypted() {
        run(|| {
            let dir = TempDir::new().unwrap();
            let mut leader = Leader::open(dir.path());
            leader.write_n(30);
            leader.checkpoint();
            leader.write_n(10);
            let mut follower = Follower::open(&dir.path().join("f"));
            follower.sync(&leader);
            follower.assert_matches(&leader, "encrypted resync");
            assert_eq!(follower.resyncs, 1);
            // A frame on the wire is plaintext.
            let (generation, _) = leader.position();
            let frame = leader
                .wal()
                .replication_frame_from(Some(generation), 0, 3)
                .unwrap();
            assert!(frame.wal_lines.iter().all(|l| !l.starts_with("~E1")));
            assert!(!frame.wal_lines.is_empty());
            // Exports retained on the leader are sealed.
            assert!(!leader.exports.manifests().is_empty());
            assert_no_plaintext(dir.path(), "replicated claim");
        });
    }

    #[test]
    fn chunked_export_reproduces_the_leader_state() {
        run(chunked_export_reproduces_the_single_response_export_and_the_leader_state);
    }

    #[test]
    fn chunk_requests_outside_the_export_are_refused_when_sealed() {
        run(chunk_requests_outside_the_export_are_refused);
    }

    #[test]
    fn interrupted_download_resumes() {
        run(an_interrupted_download_resumes_from_the_part_file);
    }

    #[test]
    fn checksum_mismatch_discards_the_download() {
        run(a_checksum_mismatch_discards_the_download_and_asks_for_another_export);
    }

    #[test]
    fn pruned_export_starts_a_new_one() {
        run(an_export_pruned_mid_download_starts_a_new_one);
    }

    #[test]
    fn checkpoint_during_download() {
        run(a_checkpoint_during_the_download_does_not_change_the_export);
    }

    #[test]
    fn checkpoint_between_freeze_and_build() {
        run(a_checkpoint_between_freeze_and_build_does_not_change_the_export);
    }

    #[test]
    fn crash_in_the_middle_of_the_swap() {
        run(a_crash_in_the_middle_of_the_swap_is_repaired_by_applying_the_export_again);
    }

    #[test]
    fn follower_switches_at_the_end_of_the_closed_generation() {
        run(a_follower_at_the_end_of_the_closed_generation_switches_without_a_resync);
    }

    #[test]
    fn followers_inside_the_closed_generation_catch_up() {
        run(followers_inside_the_closed_generation_catch_up_from_its_file_then_switch);
    }

    #[test]
    fn transitions_survive_a_restart() {
        run(transitions_survive_a_restart_and_a_damaged_file_only_costs_a_resync);
    }

    #[test]
    fn crash_during_the_followers_local_checkpoint() {
        run(a_crash_during_the_followers_local_checkpoint_falls_back_to_a_resync);
    }

    #[test]
    fn torn_encrypted_part_file_line_is_cut_and_the_download_resumes() {
        run(|| {
            let dir = TempDir::new().unwrap();
            let mut leader = Leader::open(dir.path());
            leader.write_n(20);
            let follower_dir = dir.path().join("f");
            let follower = Follower::open(&follower_dir);
            let paths = follower.paths();
            // Fetch part of the export, then tear the last line in half.
            let mut source = leader.source();
            let manifest = source.begin(None).unwrap().unwrap();
            fs::write(&paths.manifest, manifest.render()).unwrap();
            let ChunkFetch::Chunk(chunk) = source.chunk(&manifest.export_id, 0, 400).unwrap()
            else {
                panic!("chunk");
            };
            let codec = LineCodec::create(crate::crypt::current_keyring().as_ref()).unwrap();
            let mut text = String::new();
            text.push_str(codec.header_line().unwrap());
            text.push('\n');
            for line in chunk.data.lines() {
                codec.push_line(&mut text, line);
            }
            let torn = &text[..text.len() - 20];
            fs::write(&paths.part, torn).unwrap();
            let DownloadOutcome::Complete(download) =
                download_export(&mut leader.source(), &paths, 300).unwrap()
            else {
                panic!("complete");
            };
            assert!(download.resumed);
            let (_, sha) = hash_file(&paths.part).unwrap();
            assert_eq!(sha, download.manifest.sha256);
            assert_no_plaintext(&follower_dir, "replicated claim");
        });
    }

    #[test]
    fn a_node_without_the_key_cannot_open_encrypted_files() {
        let dir = TempDir::new().unwrap();
        let paths = encryption::with_keyring(Some(keyring()), || {
            let mut leader = Leader::open(dir.path());
            leader.write_n(5);
            let follower = Follower::open(&dir.path().join("f"));
            let paths = follower.paths();
            let DownloadOutcome::Complete(_) =
                download_export(&mut leader.source(), &paths, 300).unwrap()
            else {
                panic!("complete");
            };
            paths
        });
        encryption::with_keyring(None, || {
            let err = ReplicationExportFile::open(&paths.part).unwrap_err();
            assert!(
                format!("{err:?}").contains("DASH_ENCRYPTION_KEY_FILE"),
                "{err:?}"
            );
            let err = FileWal::open(dir.path().join("leader.wal")).err().unwrap();
            assert!(
                format!("{err:?}").contains("no encryption key is configured"),
                "{err:?}"
            );
        });
    }
}
