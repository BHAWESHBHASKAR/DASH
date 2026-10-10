//! Vector index persistence through the real ingestion server: the index is
//! saved at clean shutdown and after a WAL checkpoint, a restart loads it
//! instead of rebuilding, and a crash after more writes catches up from the
//! saved WAL position to exactly the state a full rebuild produces.

use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    sync::{Arc, Once},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use ingestion::transport::{IngestionRuntime, serve_http_with_workers};
use store::{
    AnnTuningConfig, CheckpointPolicy, FileWal, InMemoryStore, ReplayPolicy,
    VectorIndexPersistence, VectorIndexRestore,
};
use tempfile::TempDir;

const TENANT: &str = "tenant-persist";
const DIM: usize = 16;

fn init_env() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        #[allow(unused_unsafe)]
        unsafe {
            std::env::set_var("DASH_INSECURE_DEV_MODE", "1");
            std::env::set_var("DASH_STRICT_SECRETS", "0");
        }
    });
}

fn tuning() -> AnnTuningConfig {
    // Puts the tenant on the HNSW path after 32 vectors.
    AnnTuningConfig {
        flat_threshold: 32,
        ..AnnTuningConfig::default()
    }
}

/// A distinct pseudo-random vector per `i` (splitmix64), short decimals so
/// the JSON round trip is exact.
fn vector(i: usize) -> Vec<f32> {
    let mut state = (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0xD1B5_4A32_D192_ED03;
    (0..DIM)
        .map(|_| {
            state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = state;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^= z >> 31;
            ((z % 2001) as f32 - 1000.0) / 1000.0
        })
        .collect()
}

/// Polls `cond` until it holds; the timeout only bounds a hung test.
fn wait_until(what: &str, mut cond: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(60);
    while Instant::now() < deadline {
        if cond() {
            return;
        }
        thread::sleep(Duration::from_millis(10));
    }
    panic!("timed out waiting for: {what}");
}

fn load(wal_path: &Path, index: Option<&Path>) -> (InMemoryStore, VectorIndexRestore) {
    let wal = FileWal::open(wal_path).unwrap();
    let (store, stats) =
        InMemoryStore::load_from_wal_with_vector_index(&wal, tuning(), ReplayPolicy::Strict, index)
            .unwrap();
    (store, stats.vector_index)
}

fn results(store: &InMemoryStore) -> Vec<Vec<String>> {
    (0..20)
        .map(|q| store.ann_vector_top_candidates(TENANT, &vector(q * 7 + 3), 10))
        .collect()
}

struct Server {
    addr: String,
    shutdown: Arc<dash_common::ShutdownSignal>,
    thread: Option<JoinHandle<()>>,
    persistence: Arc<VectorIndexPersistence>,
    restore: VectorIndexRestore,
}

impl Server {
    /// What `main` does: load the WAL with the persisted index, then serve.
    fn start(wal_path: &Path, index: &Path, policy: CheckpointPolicy) -> Self {
        init_env();
        let wal = FileWal::open(wal_path).unwrap();
        let persistence = Arc::new(VectorIndexPersistence::new(index, None));
        let (store, stats) = InMemoryStore::load_from_wal_with_vector_index(
            &wal,
            tuning(),
            ReplayPolicy::Strict,
            Some(persistence.path()),
        )
        .unwrap();
        persistence.note_restored(&stats.vector_index);
        let runtime = IngestionRuntime::persistent(store, wal, policy)
            .with_vector_index_persistence(Arc::clone(&persistence));
        let addr = {
            let probe = TcpListener::bind("127.0.0.1:0").unwrap();
            format!("127.0.0.1:{}", probe.local_addr().unwrap().port())
        };
        let shutdown = dash_common::ShutdownSignal::manual();
        let thread = {
            let addr = addr.clone();
            let shutdown = Arc::clone(&shutdown);
            thread::spawn(move || {
                serve_http_with_workers(runtime, &addr, 2, shutdown).expect("ingestion server");
            })
        };
        wait_until("server accepting", || TcpStream::connect(&addr).is_ok());
        Self {
            addr,
            shutdown,
            thread: Some(thread),
            persistence,
            restore: stats.vector_index,
        }
    }

    fn ingest(&self, range: std::ops::Range<usize>) {
        for i in range {
            let embedding: Vec<String> = vector(i).iter().map(|x| format!("{x}")).collect();
            let body = format!(
                r#"{{"claim":{{"claim_id":"c{i}","tenant_id":"{TENANT}","canonical_text":"persisted claim {i}","confidence":0.9}},"evidence":[],"claim_embedding":[{}]}}"#,
                embedding.join(",")
            );
            let (status, text) = post(&self.addr, "/v1/ingest", &body);
            assert_eq!(status, 200, "ingest c{i}: {text}");
        }
    }

    fn stop(mut self) {
        self.shutdown.trigger();
        if let Some(thread) = self.thread.take() {
            thread.join().unwrap();
        }
    }
}

fn post(addr: &str, path: &str, body: &str) -> (u16, String) {
    let mut stream = TcpStream::connect(addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(30)))
        .unwrap();
    let request = format!(
        "POST {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(request.as_bytes()).unwrap();
    let mut raw = Vec::new();
    let _ = stream.read_to_end(&mut raw);
    let text = String::from_utf8_lossy(&raw).to_string();
    let status = text
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse().ok())
        .unwrap_or(0);
    (status, text)
}

fn wal_generation(wal_path: &Path) -> u64 {
    let mut gen_path = wal_path.as_os_str().to_owned();
    gen_path.push(".gen");
    let raw = std::fs::read_to_string(PathBuf::from(gen_path)).unwrap();
    u64::from_str_radix(raw.trim(), 16).unwrap()
}

fn copy_dir(from: &Path, to: &Path) {
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        if entry.file_type().unwrap().is_file() {
            std::fs::copy(entry.path(), to.join(entry.file_name())).unwrap();
        }
    }
}

#[test]
fn restart_loads_the_saved_index_and_a_crash_catches_up_from_the_wal() {
    let dir = TempDir::new().unwrap();
    let wal_path = dir.path().join("ingest.wal");
    let index = dir.path().join("ingest.wal.vindex");

    // First run: nothing saved yet; a clean shutdown writes the index.
    let server = Server::start(&wal_path, &index, CheckpointPolicy::default());
    assert_eq!(server.restore, VectorIndexRestore::Missing);
    server.ingest(0..120);
    server.stop();
    assert!(index.exists(), "clean shutdown saves the vector index");

    // Second run: the index is loaded, not rebuilt. Each ingest writes four
    // WAL records, so the policy checkpoints during the tenth ingest below.
    let records_so_far = 120 * 4;
    let policy = CheckpointPolicy {
        max_wal_records: Some(records_so_far + 40),
        max_wal_bytes: None,
    };
    let server = Server::start(&wal_path, &index, policy);
    match &server.restore {
        VectorIndexRestore::Loaded {
            vectors, caught_up, ..
        } => {
            assert_eq!(*vectors, 120);
            assert_eq!(*caught_up, 0);
        }
        other => panic!("expected the saved index to load, got {other:?}"),
    }
    let generation_before = wal_generation(&wal_path);
    server.ingest(120..150);
    let generation_after = wal_generation(&wal_path);
    assert_ne!(generation_before, generation_after, "a checkpoint ran");
    // The checkpoint asked the background saver for a save at the new
    // generation (no periodic saves are configured).
    wait_until("checkpoint save", || {
        server
            .persistence
            .last_saved_position()
            .is_some_and(|p| p.generation == generation_after)
    });
    server.ingest(150..155);

    // A crash now: copy the files as they are on disk.
    let crashed = TempDir::new().unwrap();
    copy_dir(dir.path(), crashed.path());
    server.stop();

    let crashed_wal = crashed.path().join("ingest.wal");
    let (caught_up_store, restore) = load(
        &crashed_wal,
        Some(&crashed.path().join("ingest.wal.vindex")),
    );
    match &restore {
        VectorIndexRestore::Loaded {
            vectors, caught_up, ..
        } => {
            assert_eq!(*vectors, 155);
            assert!(*caught_up >= 5, "caught up {caught_up}");
        }
        other => panic!("expected a catch-up load, got {other:?}"),
    }
    let (rebuilt, _) = load(&crashed_wal, None);
    assert_eq!(results(&caught_up_store), results(&rebuilt));

    // Third run of the original directory: the shutdown save is current.
    let (restarted, restore) = load(&wal_path, Some(&index));
    match &restore {
        VectorIndexRestore::Loaded {
            vectors, caught_up, ..
        } => {
            assert_eq!(*vectors, 155);
            assert_eq!(*caught_up, 0);
        }
        other => panic!("expected the shutdown save to load, got {other:?}"),
    }
    assert_eq!(results(&restarted), results(&load(&wal_path, None).0));
}
