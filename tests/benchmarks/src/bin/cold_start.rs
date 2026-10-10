//! Cold start with and without the persisted vector index.
//!
//! Builds a WAL of `N` claims with `DIM`-d vectors (seeded Gaussian mixture,
//! one tenant, default ANN tuning) in a temporary directory, then measures:
//!
//! 0. `replay floor`: the same load with the tenant kept on the flat index
//!    (no HNSW build), i.e. what any start costs before index work;
//! 1. `rebuild`: `load_from_wal_with_vector_index` with no index file, which
//!    builds every HNSW from the replayed vectors (the behaviour before the
//!    index was persisted);
//! 2. `save`: writing the index file (`VectorIndexSnapshot::save`);
//! 3. `load`: the same load with the saved index (no catch-up);
//! 4. `catch-up`: after appending `TAIL` more vectors, the load with the now
//!    older index file (catch-up of `TAIL` claims).
//!
//! Run in release mode:
//!
//! ```bash
//! cargo run --release -p benchmark-smoke --bin cold_start -- [N] [DIM] [TAIL] [RUNS]
//! ```
//!
//! Defaults: N=50000, DIM=384, TAIL=1000, RUNS=2. Prints one line per
//! measurement and a final `COLD_START_JSON:` line. Set
//! `DASH_ENCRYPTION_KEY_FILE` to measure with encryption at rest.

use std::path::Path;
use std::time::{Duration, Instant};

use schema::Claim;
use store::{AnnTuningConfig, FileWal, InMemoryStore, ReplayPolicy, VectorIndexRestore};

const TENANT: &str = "bench";
const CLUSTERS: usize = 64;

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
}

fn claim(i: usize) -> Claim {
    Claim {
        claim_id: format!("claim-{i:08}"),
        tenant_id: TENANT.to_string(),
        canonical_text: format!("benchmark claim {i}"),
        confidence: 0.8,
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

fn append(wal_path: &Path, from: usize, to: usize, dim: usize, centers: &[Vec<f32>]) {
    // Batched fsync: building the fixture is not what is measured.
    let mut wal = FileWal::open_with_sync_every_records(wal_path, 100_000).expect("open wal");
    let mut rng = Rng(0xC0FFEE ^ from as u64);
    for i in from..to {
        let center = &centers[(rng.next_u64() % CLUSTERS as u64) as usize];
        let vector: Vec<f32> = (0..dim).map(|d| center[d] + 0.3 * rng.gauss()).collect();
        let claim = claim(i);
        wal.append_claim(&claim).expect("append claim");
        wal.append_claim_vector(&claim.claim_id, &vector)
            .expect("append vector");
    }
    wal.flush_pending_sync().expect("flush");
}

fn load(wal_path: &Path, index: Option<&Path>) -> (InMemoryStore, VectorIndexRestore, Duration) {
    load_with(wal_path, index, AnnTuningConfig::default())
}

fn load_with(
    wal_path: &Path,
    index: Option<&Path>,
    tuning: AnnTuningConfig,
) -> (InMemoryStore, VectorIndexRestore, Duration) {
    let started = Instant::now();
    let wal = FileWal::open(wal_path).expect("open wal");
    let (store, stats) =
        InMemoryStore::load_from_wal_with_vector_index(&wal, tuning, ReplayPolicy::Strict, index)
            .expect("load");
    (store, stats.vector_index, started.elapsed())
}

fn arg(n: usize, default: usize) -> usize {
    std::env::args()
        .nth(n)
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn secs(d: Duration) -> f64 {
    d.as_secs_f64()
}

fn main() {
    let n = arg(1, 50_000);
    let dim = arg(2, 384);
    let tail = arg(3, 1_000);
    let runs = arg(4, 2).max(1);
    let threads = std::thread::available_parallelism().map_or(1, |p| p.get());
    // With DASH_ENCRYPTION_KEY_FILE set, every file (WAL, vector index) is
    // encrypted at rest, as in a service with that setting.
    let keyring = store::encryption::keyring_from_env().expect("encryption settings");
    let encryption = keyring.is_some();
    store::encryption::install(keyring);
    println!(
        "cold_start: n={n} dim={dim} tail={tail} runs={runs} threads={threads} encryption={encryption}"
    );

    let dir = tempfile::TempDir::new().expect("temp dir");
    let wal_path = dir.path().join("bench.wal");
    let index_path = dir.path().join("bench.wal.vindex");
    let mut rng = Rng(42);
    let centers: Vec<Vec<f32>> = (0..CLUSTERS)
        .map(|_| (0..dim).map(|_| rng.gauss()).collect())
        .collect();

    let started = Instant::now();
    append(&wal_path, 0, n, dim, &centers);
    let wal_bytes = std::fs::metadata(&wal_path).map_or(0, |m| m.len());
    println!(
        "fixture: {} records, {:.1} MB WAL, written in {:.1} s",
        2 * n,
        wal_bytes as f64 / 1e6,
        secs(started.elapsed())
    );

    // Floor: the WAL replay alone. A flat threshold above N keeps the tenant
    // on the flat index, whose build is a normalised copy of the vectors.
    let (_, _, floor) = load_with(
        &wal_path,
        None,
        AnnTuningConfig {
            flat_threshold: usize::MAX,
            ..AnnTuningConfig::default()
        },
    );
    println!("replay floor (flat index, no HNSW): {:.2} s", secs(floor));

    let mut rebuild = Vec::new();
    for run in 0..runs {
        let (_, restore, elapsed) = load(&wal_path, None);
        assert_eq!(restore, VectorIndexRestore::NotConfigured);
        println!("rebuild run {run}: {:.2} s", secs(elapsed));
        rebuild.push(secs(elapsed));
    }

    let (store, _, _) = load(&wal_path, None);
    let snapshot = store.vector_index_snapshot(store.wal_position().expect("position"));
    drop(store);
    let save = snapshot.save(&index_path).expect("save");
    drop(snapshot);
    println!(
        "save: {:.2} s, {:.1} MB",
        secs(save.elapsed),
        save.bytes as f64 / 1e6
    );

    let mut loaded = Vec::new();
    for run in 0..runs {
        let (_, restore, elapsed) = load(&wal_path, Some(&index_path));
        assert!(restore.is_loaded(), "{restore:?}");
        println!(
            "load run {run}: {:.2} s ({})",
            secs(elapsed),
            restore.describe()
        );
        loaded.push(secs(elapsed));
    }

    append(&wal_path, n, n + tail, dim, &centers);
    let mut caught_up = Vec::new();
    for run in 0..runs {
        let (_, restore, elapsed) = load(&wal_path, Some(&index_path));
        assert!(restore.is_loaded(), "{restore:?}");
        println!(
            "catch-up run {run}: {:.2} s ({})",
            secs(elapsed),
            restore.describe()
        );
        caught_up.push(secs(elapsed));
    }
    println!(
        "COLD_START_JSON: {{\"n\":{n},\"dim\":{dim},\"tail\":{tail},\"threads\":{threads},\"wal_mb\":{:.1},\"index_mb\":{:.1},\"replay_floor_s\":{:.3},\"rebuild_s\":{:?},\"save_s\":{:.3},\"load_s\":{:?},\"catch_up_s\":{:?}}}",
        wal_bytes as f64 / 1e6,
        save.bytes as f64 / 1e6,
        secs(floor),
        rebuild,
        secs(save.elapsed),
        loaded,
        caught_up
    );
}
