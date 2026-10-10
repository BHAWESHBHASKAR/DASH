//! Replication wire compatibility (`/internal/replication/wal` and
//! `/export` frames).
//!
//! * 0.2 leaders send frames without a WAL generation. Their offset-only
//!   protocol serves a fresh follower only the WAL tail (never the
//!   snapshot), so the current followers (ingestion and retrieval) refuse
//!   such frames, apply nothing and report `replication_leader_too_old`.
//! * Frames recorded from the current format are followed to the leader's
//!   exact state.
//! * An upgraded leader whose WAL and snapshot still hold 0.2 records serves
//!   current followers correctly.
//! * A 0.2 follower cannot parse a current frame (its header parser expects
//!   `needs_resync` on the second line), so it stops replicating instead of
//!   applying anything: upgrade order is leader first, then followers.
//! * The recorded leaders have no chunked export (`/export/begin` answers
//!   404): current followers fall back to the single-response export.
//! * A current leader keeps the earlier 0.3 frame layout for followers that
//!   do not ask for generation switches, still serves the single-response
//!   export, and serves the chunked export with the same records.

mod support;

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use dash_compat::{
    Era, FIXTURES, Fixture, REPLICATION_TOKEN, env_lock, old_readers, remove_env, set_env,
};
use ingestion::transport::{IngestionRuntime, serve_http_with_workers};
use retrieval::replication::{ReplicationFollowerConfig, start_follower};
use store::{CheckpointPolicy, FileWal, InMemoryStore};
use support::{diff_against_recorded, run_retrieves};

fn free_addr() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind probe");
    format!("127.0.0.1:{}", listener.local_addr().expect("addr").port())
}

fn wait_until(what: &str, timeout: Duration, mut cond: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if cond() {
            return;
        }
        thread::sleep(Duration::from_millis(20));
    }
    panic!("timed out waiting for: {what}");
}

fn http_get(addr: &str, path: &str, headers: &[(&str, &str)]) -> (u16, String) {
    let mut stream = TcpStream::connect(addr).expect("connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .expect("timeout");
    let mut req = format!(
        "GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\nContent-Length: 0\r\n"
    );
    for (name, value) in headers {
        req.push_str(&format!("{name}: {value}\r\n"));
    }
    req.push_str("\r\n");
    stream.write_all(req.as_bytes()).expect("write");
    let mut raw = Vec::new();
    let _ = stream.read_to_end(&mut raw);
    let text = String::from_utf8_lossy(&raw).to_string();
    let status = text
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse().ok())
        .unwrap_or(0);
    let body = text
        .split_once("\r\n\r\n")
        .map(|(_, body)| body.to_string())
        .unwrap_or_default();
    (status, body)
}

/// A leader that replays the frames an old release served, as recorded in
/// the fixture (`replication/wal-frame.txt`, `replication/export-frame.txt`).
struct RecordedLeader {
    addr: String,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl RecordedLeader {
    fn start(fixture: &Fixture) -> Self {
        let wal_frame =
            std::fs::read_to_string(fixture.path("replication/wal-frame.txt")).expect("frame");
        let export =
            std::fs::read_to_string(fixture.path("replication/export-frame.txt")).expect("export");
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        listener.set_nonblocking(true).expect("nonblocking");
        let addr = listener.local_addr().expect("addr").to_string();
        let stop = Arc::new(AtomicBool::new(false));
        let thread = {
            let stop = Arc::clone(&stop);
            thread::spawn(move || {
                while !stop.load(Ordering::SeqCst) {
                    match listener.accept() {
                        Ok((mut stream, _)) => {
                            stream.set_nonblocking(false).expect("blocking");
                            stream
                                .set_read_timeout(Some(Duration::from_secs(5)))
                                .expect("timeout");
                            let mut buf = Vec::new();
                            let mut chunk = [0u8; 4096];
                            while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
                                match stream.read(&mut chunk) {
                                    Ok(0) | Err(_) => break,
                                    Ok(n) => buf.extend_from_slice(&chunk[..n]),
                                }
                            }
                            let head = String::from_utf8_lossy(&buf).to_string();
                            let target = head.split_whitespace().nth(1).unwrap_or("").to_string();
                            let (status, body) = if target.starts_with("/internal/replication/wal?")
                            {
                                ("200 OK", wal_frame.as_str())
                            } else if target.starts_with("/internal/replication/export/") {
                                // The recorded releases have no chunked export
                                // (`/export/begin`, `/export/chunk`): followers
                                // fall back to the single-response export.
                                ("404 Not Found", "status=not_found\n")
                            } else if target.starts_with("/internal/replication/export") {
                                ("200 OK", export.as_str())
                            } else {
                                ("404 Not Found", "status=not_found\n")
                            };
                            let response = format!(
                                "HTTP/1.1 {status}\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                                body.len()
                            );
                            let _ = stream.write_all(response.as_bytes());
                        }
                        Err(_) => thread::sleep(Duration::from_millis(5)),
                    }
                }
            })
        };
        Self {
            addr,
            stop,
            thread: Some(thread),
        }
    }
}

impl Drop for RecordedLeader {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// An in-process ingestion server (leader, or follower of `source`).
struct Node {
    addr: String,
    shutdown: Arc<dash_common::ShutdownSignal>,
    thread: Option<JoinHandle<()>>,
}

impl Node {
    fn start(wal_path: &Path, source: Option<&str>) -> Self {
        set_env("DASH_INSECURE_DEV_MODE", "1");
        set_env("DASH_STRICT_SECRETS", "0");
        set_env("DASH_INGEST_REPLICATION_TOKEN", REPLICATION_TOKEN);
        set_env("DASH_INGEST_REPLICATION_POLL_INTERVAL_MS", "20");
        set_env("DASH_INGEST_REPLICATION_MAX_BACKOFF_MS", "100");
        match source {
            Some(url) => set_env("DASH_INGEST_REPLICATION_SOURCE_URL", url),
            None => remove_env("DASH_INGEST_REPLICATION_SOURCE_URL"),
        }
        let addr = free_addr();
        let wal = FileWal::open_with_sync_every_records(wal_path, 1).expect("open wal");
        let store = InMemoryStore::load_from_wal(&wal).expect("load wal");
        let runtime = IngestionRuntime::persistent(store, wal, CheckpointPolicy::default());
        let shutdown = dash_common::ShutdownSignal::manual();
        let thread = {
            let addr = addr.clone();
            let shutdown = Arc::clone(&shutdown);
            thread::spawn(move || {
                serve_http_with_workers(runtime, &addr, 2, shutdown).expect("server");
            })
        };
        wait_until("server accepting", Duration::from_secs(10), || {
            TcpStream::connect(&addr).is_ok()
        });
        if source.is_some() {
            wait_until("follower enabled", Duration::from_secs(10), || {
                http_get(&addr, "/ready", &[]).1.contains("\"replication\"")
            });
        }
        remove_env("DASH_INGEST_REPLICATION_SOURCE_URL");
        Self {
            addr,
            shutdown,
            thread: Some(thread),
        }
    }

    fn ready(&self) -> (u16, String) {
        http_get(&self.addr, "/ready", &[])
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        self.shutdown.trigger();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// Runs `f` with the environment lock released (`run_retrieves` takes it
/// itself), then takes it back.
fn without_env_lock<T>(
    guard: &mut Option<std::sync::MutexGuard<'static, ()>>,
    f: impl FnOnce() -> T,
) -> T {
    guard.take();
    let out = f();
    *guard = Some(env_lock());
    out
}

fn claims_in(wal_path: &Path) -> usize {
    let wal = FileWal::open(wal_path).expect("open");
    InMemoryStore::load_from_wal(&wal)
        .expect("load")
        .claims_len()
}

#[test]
fn ingestion_follower_refuses_a_0_2_leader_and_applies_nothing() {
    let _env = env_lock();
    for fixture in FIXTURES.iter().filter(|f| f.era == Era::V0_2) {
        let frame =
            std::fs::read_to_string(fixture.path("replication/wal-frame.txt")).expect("frame");
        assert!(
            !frame.contains("generation="),
            "0.2 frames carry no generation"
        );
        let leader = RecordedLeader::start(fixture);
        let dir = tempfile::tempdir().expect("tempdir");
        let wal_path = dir.path().join("follower.wal");
        {
            let follower = Node::start(&wal_path, Some(&format!("http://{}", leader.addr)));
            wait_until(
                "follower reports the old leader",
                Duration::from_secs(10),
                || follower.ready().1.contains("replication_leader_too_old"),
            );
            let (status, body) = follower.ready();
            assert_eq!(status, 503, "{body}");
        }
        assert_eq!(
            claims_in(&wal_path),
            0,
            "{}: nothing applied",
            fixture.label
        );
    }
}

#[test]
fn retrieval_follower_refuses_a_0_2_leader_and_applies_nothing() {
    let _env = env_lock();
    for fixture in FIXTURES.iter().filter(|f| f.era == Era::V0_2) {
        let leader = RecordedLeader::start(fixture);
        let store = Arc::new(RwLock::new(InMemoryStore::new()));
        let config = ReplicationFollowerConfig {
            token: Some(REPLICATION_TOKEN.to_string()),
            poll_interval: Duration::from_millis(20),
            max_backoff: Duration::from_millis(100),
            ..ReplicationFollowerConfig::new(format!("http://{}", leader.addr))
        };
        let handle = start_follower(Arc::clone(&store), config, None);
        wait_until(
            "retrieval follower reports the old leader",
            Duration::from_secs(10),
            || handle.status().readiness() == Err("replication_leader_too_old"),
        );
        let error = handle.status().snapshot().last_error.unwrap_or_default();
        assert!(error.contains("older than 0.3.0"), "{error}");
        handle.stop();
        assert_eq!(
            store.read().expect("read").claims_len(),
            0,
            "{}",
            fixture.label
        );
    }
}

/// Frames recorded from a current-format leader are followed to its state:
/// a fresh follower is told to resync and rebuilds from the export.
#[test]
fn followers_reach_the_leader_state_from_recorded_current_frames() {
    let mut env = Some(env_lock());
    for fixture in FIXTURES.iter().filter(|f| f.era != Era::V0_2) {
        let frame =
            std::fs::read_to_string(fixture.path("replication/wal-frame.txt")).expect("frame");
        assert!(
            frame
                .lines()
                .nth(1)
                .is_some_and(|l| l.starts_with("generation="))
        );
        let leader = RecordedLeader::start(fixture);
        // Ingestion follower.
        let dir = tempfile::tempdir().expect("tempdir");
        let wal_path = dir.path().join("follower.wal");
        {
            let follower = Node::start(&wal_path, Some(&format!("http://{}", leader.addr)));
            wait_until("ingestion follower synced", Duration::from_secs(15), || {
                follower.ready().0 == 200
            });
        }
        let wal = FileWal::open(&wal_path).expect("open");
        let store = InMemoryStore::load_from_wal(&wal).expect("load");
        assert_eq!(
            store.claims_len(),
            fixture.expected_claims_total(),
            "{}",
            fixture.label
        );
        // Same answers as the leader served (with the leader's segments as
        // the prefilter, as the scenario's retrieval service had).
        let state = fixture.scratch_state();
        let answers = without_env_lock(&mut env, || run_retrieves(&store, Some(&state.segments())));
        let diffs = diff_against_recorded(fixture, &answers);
        assert!(diffs.is_empty(), "{}:\n{}", fixture.label, diffs.join("\n"));
        // Retrieval follower.
        let shared = Arc::new(RwLock::new(InMemoryStore::new()));
        let config = ReplicationFollowerConfig {
            token: Some(REPLICATION_TOKEN.to_string()),
            poll_interval: Duration::from_millis(20),
            ..ReplicationFollowerConfig::new(format!("http://{}", leader.addr))
        };
        let handle = start_follower(Arc::clone(&shared), config, None);
        wait_until("retrieval follower synced", Duration::from_secs(15), || {
            handle.status().readiness().is_ok()
        });
        handle.stop();
        let follower_store = shared.read().expect("read");
        assert_eq!(
            follower_store.claims_len(),
            fixture.expected_claims_total(),
            "{}",
            fixture.label
        );
    }
}

/// An upgraded leader whose WAL and snapshot still hold 0.2 records serves a
/// current follower, which ends up with the same data and answers.
#[test]
fn an_upgraded_leader_with_a_legacy_wal_serves_current_followers() {
    let mut env = Some(env_lock());
    for fixture in FIXTURES.iter().filter(|f| f.era == Era::V0_2) {
        let state = fixture.scratch_state();
        let leader = Node::start(&state.wal(), None);
        let (status, frame) = http_get(
            &leader.addr,
            "/internal/replication/wal?from_offset=0&max_records=512",
            &[("x-replication-token", REPLICATION_TOKEN)],
        );
        assert_eq!(status, 200, "{frame}");
        // Legacy records are served verbatim, tagged with the generation.
        assert!(
            frame
                .lines()
                .nth(1)
                .is_some_and(|l| l.starts_with("generation=")),
            "{frame}"
        );
        // A 0.2 follower cannot parse the current frame header: it stops
        // replicating (and applies nothing) until it is upgraded.
        assert!(old_readers::v0_2_parses_delta_header(&frame).is_err());
        let recorded =
            std::fs::read_to_string(fixture.path("replication/wal-frame.txt")).expect("frame");
        assert!(
            old_readers::v0_2_parses_delta_header(&recorded).is_ok(),
            "oracle sanity"
        );

        let dir = tempfile::tempdir().expect("tempdir");
        let wal_path = dir.path().join("follower.wal");
        {
            let follower = Node::start(&wal_path, Some(&format!("http://{}", leader.addr)));
            wait_until(
                "follower synced with the upgraded leader",
                Duration::from_secs(15),
                || follower.ready().0 == 200,
            );
        }
        drop(leader);
        let wal = FileWal::open(&wal_path).expect("open");
        let store = InMemoryStore::load_from_wal(&wal).expect("follower state loads");
        assert_eq!(
            store.claims_len(),
            fixture.expected_claims_total(),
            "{}",
            fixture.label
        );
        let segments = state.segments();
        let answers = without_env_lock(&mut env, || run_retrieves(&store, Some(&segments)));
        let diffs = diff_against_recorded(fixture, &answers);
        assert!(diffs.is_empty(), "{}:\n{}", fixture.label, diffs.join("\n"));
    }
}

/// Same-version and previous-0.3-build compatibility of the chunked export
/// and the generation switch:
///
/// * A frame requested without `gen_switch=1` keeps the layout followers of
///   the earlier 0.3.0 build parse (no `switch_from=` line), and the
///   single-response export is still served, so such a follower keeps
///   working against an upgraded leader.
/// * A current follower's request (`gen_switch=1`) gets the `switch_from=`
///   line; the chunked export (`/export/begin`, `/export/chunk`) serves the
///   same records as the single-response export, verified by its SHA-256.
#[test]
fn current_leader_serves_both_the_previous_and_the_chunked_protocol() {
    let _env = env_lock();
    for fixture in FIXTURES.iter().filter(|f| f.era == Era::V0_3) {
        let state = fixture.scratch_state();
        let leader = Node::start(&state.wal(), None);
        let token = [("x-replication-token", REPLICATION_TOKEN)];

        let (status, old_layout) = http_get(
            &leader.addr,
            "/internal/replication/wal?from_offset=0&max_records=512&from_generation=1",
            &token,
        );
        assert_eq!(status, 200, "{old_layout}");
        old_readers::v0_3_0_dev_parses_delta_header(&old_layout)
            .unwrap_or_else(|e| panic!("{}: {e}\n{old_layout}", fixture.label));
        assert!(!old_layout.contains("switch_from="));

        let (status, new_layout) = http_get(
            &leader.addr,
            "/internal/replication/wal?from_offset=0&max_records=512&from_generation=1&gen_switch=1",
            &token,
        );
        assert_eq!(status, 200, "{new_layout}");
        assert_eq!(
            new_layout.lines().nth(3),
            Some("switch_from=none"),
            "{new_layout}"
        );
        assert!(old_readers::v0_3_0_dev_parses_delta_header(&new_layout).is_err());

        let (status, single) = http_get(&leader.addr, "/internal/replication/export", &token);
        assert_eq!(status, 200, "single-response export still served");

        let (status, manifest) =
            http_get(&leader.addr, "/internal/replication/export/begin", &token);
        assert_eq!(status, 200, "{manifest}");
        let manifest = store::ReplicationExportManifest::parse(&manifest).expect("manifest");
        let mut body = String::new();
        while (body.len() as u64) < manifest.total_bytes {
            let (status, chunk) = http_get(
                &leader.addr,
                &format!(
                    "/internal/replication/export/chunk?export_id={}&offset={}&max_bytes=2048",
                    manifest.export_id,
                    body.len()
                ),
                &token,
            );
            assert_eq!(status, 200, "{chunk}");
            let chunk = store::ReplicationExportChunk::parse_response(&chunk).expect("chunk");
            assert!(chunk.data.len() <= 2048 || !chunk.data[..2048].contains('\n'));
            body.push_str(&chunk.data);
        }
        let downloaded = tempfile::NamedTempFile::new().expect("tempfile");
        std::fs::write(downloaded.path(), body.as_bytes()).expect("write download");
        let (len, sha256) = store::hash_file(downloaded.path()).expect("hash");
        assert_eq!(
            (len, sha256),
            (manifest.total_bytes, manifest.sha256.clone())
        );
        // The chunked export carries the same records as the single response
        // (its counts are zero-padded, the records identical).
        let records = |text: &str| -> Vec<String> {
            text.lines()
                .skip_while(|l| *l != "SNAPSHOT")
                .filter(|l| *l != "SNAPSHOT" && *l != "WAL")
                .map(str::to_string)
                .collect()
        };
        assert_eq!(records(&body), records(&single), "{}", fixture.label);

        // An unknown export is a 404 (the follower starts over).
        let (status, _) = http_get(
            &leader.addr,
            "/internal/replication/export/chunk?export_id=0123456789abcdef&offset=0",
            &token,
        );
        assert_eq!(status, 404);
    }
}
