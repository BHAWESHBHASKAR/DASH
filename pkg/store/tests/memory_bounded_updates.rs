//! Updating the same claims over and over must not grow memory: neither the
//! live heap of the store with its redb mirror, nor the peak a replication
//! poll needs on the leader.
//!
//! The binary counts heap bytes with its own global allocator, so it holds a
//! single test (another test running in parallel would disturb the count).

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicIsize, Ordering};

use schema::{Evidence, Stance, claim_builder};
use store::{DiskStatus, FileWal, InMemoryStore};
use tempfile::TempDir;

struct CountingAllocator;

static LIVE: AtomicIsize = AtomicIsize::new(0);
static PEAK: AtomicIsize = AtomicIsize::new(0);

fn note(delta: isize) {
    let live = LIVE.fetch_add(delta, Ordering::Relaxed) + delta;
    PEAK.fetch_max(live, Ordering::Relaxed);
}

// SAFETY: every call is forwarded unchanged to the system allocator; only
// the byte counters are updated around it.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        note(layout.size() as isize);
        // SAFETY: same contract as the caller's.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        note(-(layout.size() as isize));
        // SAFETY: same contract as the caller's.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        note(new_size as isize - layout.size() as isize);
        // SAFETY: same contract as the caller's.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn live() -> isize {
    LIVE.load(Ordering::Relaxed)
}

fn evidence(id: String, claim_id: &str) -> Evidence {
    Evidence {
        evidence_id: id,
        claim_id: claim_id.to_string(),
        source_id: "source-1".to_string(),
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

const CLAIMS: usize = 500;
const ROUNDS: u32 = 40;
const DIM: usize = 384;
/// Rounds before the heap baseline: long enough to fill the store's bounded
/// in-memory WAL event ring (8,192 events, about four per update) and the
/// redb page cache.
const WARMUP_ROUNDS: u32 = 10;

#[test]
fn repeated_updates_of_the_same_claims_keep_heap_and_replication_peak_flat() {
    let dir = TempDir::new().unwrap();
    let mut wal = FileWal::open_with_sync_every_records(dir.path().join("leader.wal"), 4096)
        .expect("open wal");
    let mut store = InMemoryStore::new().attach_disk(dir.path().join("leader.redb"));
    assert!(
        matches!(store.disk_status(), DiskStatus::Available),
        "redb mirror attached"
    );

    let mut follower_offset = 0usize;
    let generation = wal.generation();
    let mut heap_after_warmup = 0isize;
    let mut first_poll_peak = 0isize;
    let mut last_poll_peak = 0isize;
    for round in 0..ROUNDS {
        for i in 0..CLAIMS {
            let id = format!("claim-{i}");
            let claim = claim_builder(&id, "tenant-a", &format!("claim {i} revision {round}"), 0.9);
            let evidence = vec![
                evidence(format!("{id}-e0"), &id),
                evidence(format!("{id}-e1"), &id),
            ];
            store
                .ingest_bundle_persistent(&mut wal, claim, evidence, Vec::new())
                .expect("ingest");
            let vector: Vec<f32> = (0..DIM)
                .map(|k| ((k + i + round as usize * 7) % 97) as f32 + 1.0)
                .collect();
            store
                .upsert_claim_vector_persistent(&mut wal, &id, vector)
                .expect("vector");
        }

        // A follower drains the round with 512-record frames. The peak heap
        // a poll needs must not depend on how long the log already is.
        let baseline = live();
        PEAK.store(baseline, Ordering::Relaxed);
        loop {
            let frame = wal
                .replication_frame_from(Some(generation), follower_offset, 512)
                .expect("frame");
            assert!(!frame.needs_resync);
            follower_offset = frame.next_offset;
            if follower_offset == frame.total_records {
                break;
            }
        }
        let poll_peak = PEAK.load(Ordering::Relaxed) - baseline;
        if round == 1 {
            first_poll_peak = poll_peak;
        }
        last_poll_peak = poll_peak;
        if round == WARMUP_ROUNDS - 1 {
            heap_after_warmup = live();
        }
    }
    let heap_growth = live() - heap_after_warmup;
    let wal_bytes = wal.wal_size_bytes().unwrap() as isize;

    // 15,000 updates after the warm-up. With redb 2.6.3 (stale page-cache
    // queue entries) the heap grew by about 480 KiB here, with 2.6.4 by
    // about 13 KiB.
    assert!(
        heap_growth < 128 * 1024,
        "live heap grew by {heap_growth} bytes over {} updates",
        ((ROUNDS - WARMUP_ROUNDS) as usize) * CLAIMS
    );
    // Reading the whole log per poll made the peak track the WAL size
    // (tens of MiB by the last round).
    assert!(
        wal_bytes > 8 * 1024 * 1024,
        "the log ({wal_bytes} bytes) must dwarf a frame for this test to mean anything"
    );
    assert!(
        last_poll_peak < 2 * first_poll_peak.max(1024 * 1024),
        "a poll needed {last_poll_peak} bytes at the end versus {first_poll_peak} at the start (WAL {wal_bytes} bytes)"
    );
}
