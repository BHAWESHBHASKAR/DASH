//! Ingestion follower replication (REP-01, REP-02, REP-05, REP-06): a real
//! ingestion leader feeds a real ingestion follower over loopback HTTP.
//!
//! The follower reads its source from the environment when its server
//! starts, so every test holds one lock for its whole duration and sets the
//! source variable only while the follower is starting.

use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, MutexGuard, Once, OnceLock},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use ingestion::transport::{IngestionRuntime, serve_http_with_workers};
use store::{AnnTuningConfig, CheckpointPolicy, DiskStatus, FileWal, InMemoryStore};

const TOKEN: &str = "ingest-follower-token-0123456789";
const TENANT: &str = "tenant-follow";

fn serial() -> MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        #[allow(unused_unsafe)]
        unsafe {
            std::env::set_var("DASH_INSECURE_DEV_MODE", "1");
            std::env::set_var("DASH_STRICT_SECRETS", "0");
            std::env::set_var("DASH_INGEST_REPLICATION_TOKEN", TOKEN);
            std::env::set_var("DASH_INGEST_REPLICATION_POLL_INTERVAL_MS", "20");
            std::env::set_var("DASH_INGEST_REPLICATION_MAX_BACKOFF_MS", "200");
            std::env::remove_var("DASH_INGEST_REPLICATION_SOURCE_URL");
        }
    });
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|p| p.into_inner())
}

#[allow(unused_unsafe)]
fn set_env(key: &str, value: &str) {
    unsafe { std::env::set_var(key, value) };
}

#[allow(unused_unsafe)]
fn unset_env(key: &str) {
    unsafe { std::env::remove_var(key) };
}

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
        thread::sleep(Duration::from_millis(10));
    }
    panic!("timed out waiting for: {what}");
}

fn request(
    addr: &str,
    method: &str,
    path: &str,
    body: &str,
    headers: &[(&str, &str)],
) -> (u16, String) {
    let mut stream = TcpStream::connect(addr).expect("connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .expect("timeout");
    let mut req = format!(
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\nContent-Length: {}\r\n",
        body.len()
    );
    if !body.is_empty() {
        req.push_str("Content-Type: application/json\r\n");
    }
    for (name, value) in headers {
        req.push_str(&format!("{name}: {value}\r\n"));
    }
    req.push_str("\r\n");
    req.push_str(body);
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

struct Server {
    addr: String,
    wal_path: PathBuf,
    shutdown: Arc<dash_common::ShutdownSignal>,
    thread: Option<JoinHandle<()>>,
}

impl Server {
    /// `redb` attaches a redb file to the store; `policy` is the checkpoint
    /// policy; `addr` pins the port (leader restarts).
    fn start(
        wal_path: &Path,
        redb: Option<&Path>,
        policy: CheckpointPolicy,
        addr: Option<String>,
    ) -> Self {
        let addr = addr.unwrap_or_else(free_addr);
        let wal = FileWal::open_with_sync_every_records(wal_path, 1).expect("open wal");
        let mut store = InMemoryStore::load_from_wal(&wal).expect("load wal");
        if let Some(redb) = redb {
            store = store.with_disk(redb).expect("attach redb");
            assert!(matches!(store.disk_status(), DiskStatus::Available));
        }
        let runtime = IngestionRuntime::persistent(store, wal, policy);
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
        Self {
            addr,
            wal_path: wal_path.to_path_buf(),
            shutdown,
            thread: Some(thread),
        }
    }

    /// Start a server that follows `leader_addr`. Returns once the follower
    /// is confirmed running (its `/ready` reports replication state).
    fn start_follower(
        leader_addr: &str,
        wal_path: &Path,
        redb: Option<&Path>,
        policy: CheckpointPolicy,
    ) -> Self {
        set_env(
            "DASH_INGEST_REPLICATION_SOURCE_URL",
            &format!("http://{leader_addr}"),
        );
        let server = Server::start(wal_path, redb, policy, None);
        wait_until("follower enabled", Duration::from_secs(10), || {
            request(&server.addr, "GET", "/ready", "", &[])
                .1
                .contains("\"replication\"")
        });
        unset_env("DASH_INGEST_REPLICATION_SOURCE_URL");
        server
    }

    fn stop(&mut self) {
        self.shutdown.trigger();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }

    fn restart_leader(&mut self, policy: CheckpointPolicy) {
        self.stop();
        *self = Server::start(
            &self.wal_path.clone(),
            None,
            policy,
            Some(self.addr.clone()),
        );
    }

    fn ingest(&self, claim_id: &str) {
        let body = format!(
            r#"{{"claim":{{"claim_id":"{claim_id}","tenant_id":"{TENANT}","canonical_text":"follower statement {claim_id}","confidence":0.9}},"evidence":[{{"evidence_id":"ev-{claim_id}","claim_id":"{claim_id}","source_id":"source://{claim_id}","stance":"supports","source_quality":0.9}}]}}"#
        );
        let (status, text) = request(&self.addr, "POST", "/v1/ingest", &body, &[]);
        assert_eq!(status, 200, "ingest {claim_id}: {text}");
    }

    fn frame(&self) -> (u64, usize) {
        let (status, body) = request(
            &self.addr,
            "GET",
            "/internal/replication/wal?from_offset=0&max_records=1",
            "",
            &[("x-replication-token", TOKEN)],
        );
        assert_eq!(status, 200, "{body}");
        let field = |key: &str| -> u64 {
            body.lines()
                .find_map(|line| line.strip_prefix(&format!("{key}=")))
                .unwrap_or_else(|| panic!("frame missing {key}: {body}"))
                .parse()
                .expect("numeric")
        };
        (field("generation"), field("total_records") as usize)
    }

    fn metric(&self, name: &str) -> Option<u64> {
        let (_, body) = request(&self.addr, "GET", "/metrics", "", &[]);
        body.lines()
            .find_map(|line| line.strip_prefix(&format!("{name} ")))
            .and_then(|value| value.trim().parse().ok())
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.stop();
    }
}

fn no_checkpoint() -> CheckpointPolicy {
    CheckpointPolicy::default()
}

fn checkpoint_every_record() -> CheckpointPolicy {
    CheckpointPolicy {
        max_wal_records: Some(1),
        max_wal_bytes: None,
    }
}

fn claim_ids_in_wal(wal_path: &Path) -> Vec<String> {
    let wal = FileWal::open(wal_path).expect("reopen wal");
    let store = InMemoryStore::load_from_wal(&wal).expect("replay wal");
    let mut ids: Vec<String> = store
        .claims_for_tenant(TENANT)
        .into_iter()
        .map(|c| c.claim_id)
        .collect();
    ids.sort();
    ids
}

#[test]
fn follower_restart_resumes_from_persisted_generation_and_offset_without_duplicates() {
    let _guard = serial();
    let dir = tempfile::tempdir().expect("tempdir");
    let leader = Server::start(&dir.path().join("leader.wal"), None, no_checkpoint(), None);
    leader.ingest("f1");
    leader.ingest("f2");
    let follower_wal = dir.path().join("follower.wal");

    let mut follower = Server::start_follower(&leader.addr, &follower_wal, None, no_checkpoint());
    let (generation, total) = leader.frame();
    wait_until("follower catches up", Duration::from_secs(10), || {
        follower.metric("dash_ingest_replication_last_offset") == Some(total as u64)
            && follower.metric("dash_ingest_replication_generation") == Some(generation)
    });
    follower.stop();
    assert!(
        dir.path().join("follower.wal.replication").exists(),
        "offset and generation are persisted next to the WAL"
    );

    leader.ingest("f3");
    let (_, total_after) = leader.frame();
    let mut follower = Server::start_follower(&leader.addr, &follower_wal, None, no_checkpoint());
    wait_until("follower resumes", Duration::from_secs(10), || {
        follower.metric("dash_ingest_replication_last_offset") == Some(total_after as u64)
    });
    assert_eq!(
        follower.metric("dash_ingest_replication_resync_total"),
        Some(0),
        "restart must resume incrementally, not resync"
    );
    assert_eq!(
        follower.metric("dash_ingest_replication_applied_records_total"),
        Some((total_after - total) as u64),
        "only records written after the restart are applied"
    );
    follower.stop();

    assert_eq!(claim_ids_in_wal(&follower_wal), vec!["f1", "f2", "f3"]);
    let wal = FileWal::open(&follower_wal).expect("reopen follower wal");
    assert_eq!(
        wal.wal_record_count().expect("count"),
        total_after,
        "follower WAL holds each record exactly once"
    );
}

#[test]
fn compaction_on_the_leader_forces_one_resync_and_redb_keeps_persisting() {
    let _guard = serial();
    let dir = tempfile::tempdir().expect("tempdir");
    let mut leader = Server::start(
        &dir.path().join("leader.wal"),
        None,
        checkpoint_every_record(),
        None,
    );
    leader.ingest("k1");
    leader.ingest("k2");

    let follower_wal = dir.path().join("follower.wal");
    let redb_path = dir.path().join("follower.redb");
    let mut follower = Server::start_follower(
        &leader.addr,
        &follower_wal,
        Some(&redb_path),
        no_checkpoint(),
    );
    wait_until("first resync", Duration::from_secs(10), || {
        follower.metric("dash_ingest_replication_resync_total") == Some(1)
    });
    // Keep polling: a correct protocol does not resync again by itself.
    thread::sleep(Duration::from_millis(400));
    assert_eq!(
        follower.metric("dash_ingest_replication_resync_total"),
        Some(1)
    );

    // After the resync the follower still has to mirror new writes to redb.
    leader.restart_leader(no_checkpoint());
    leader.ingest("k3");
    let (_, total) = leader.frame();
    wait_until("delta after resync", Duration::from_secs(10), || {
        follower.metric("dash_ingest_replication_last_offset") == Some(total as u64)
            && follower.metric("dash_ingest_replication_applied_records_total") > Some(0)
    });
    assert_eq!(
        follower.metric("dash_ingest_replication_resync_total"),
        Some(1)
    );
    follower.stop();

    let mut empty = FileWal::open(dir.path().join("empty.wal")).expect("empty wal");
    let (from_disk, _) =
        InMemoryStore::load_from_disk_and_wal(&redb_path, &mut empty, AnnTuningConfig::default())
            .expect("reload redb");
    let mut ids: Vec<String> = from_disk
        .claims_for_tenant(TENANT)
        .into_iter()
        .map(|c| c.claim_id)
        .collect();
    ids.sort();
    assert_eq!(
        ids,
        vec!["k1", "k2", "k3"],
        "redb must contain resynced data and writes applied after the resync"
    );
}

#[test]
fn ingestion_follower_reports_unreachable_leader_in_ready_and_metrics() {
    let _guard = serial();
    let dir = tempfile::tempdir().expect("tempdir");
    let dead = free_addr();
    let follower = Server::start_follower(
        &dead,
        &dir.path().join("follower.wal"),
        None,
        no_checkpoint(),
    );
    wait_until("failures counted", Duration::from_secs(10), || {
        follower
            .metric("dash_ingest_replication_consecutive_failures")
            .is_some_and(|n| n >= 2)
    });
    let (status, body) = request(&follower.addr, "GET", "/ready", "", &[]);
    assert_eq!(status, 503, "{body}");
    assert!(body.contains("replication_initial_sync_pending"), "{body}");
    assert!(body.contains("\"last_error\":\""), "{body}");
    assert!(follower.metric("dash_ingest_replication_pull_failure_total") >= Some(2));
}

#[test]
fn leader_frames_and_exports_carry_the_wal_generation() {
    let _guard = serial();
    let dir = tempfile::tempdir().expect("tempdir");
    let leader = Server::start(&dir.path().join("leader.wal"), None, no_checkpoint(), None);
    leader.ingest("g1");
    let (generation, total) = leader.frame();
    assert!(total >= 1);

    let (status, export) = request(
        &leader.addr,
        "GET",
        "/internal/replication/export",
        "",
        &[("x-replication-token", TOKEN)],
    );
    assert_eq!(status, 200);
    assert!(
        export.contains(&format!("generation={generation}\n")),
        "export must carry the generation: {export}"
    );

    // A stale generation (or one the leader never had) forces a resync.
    let (_, body) = request(
        &leader.addr,
        "GET",
        &format!(
            "/internal/replication/wal?from_offset=1&max_records=10&from_generation={}",
            generation.wrapping_add(1)
        ),
        "",
        &[("x-replication-token", TOKEN)],
    );
    assert!(body.contains("needs_resync=1"), "{body}");
    assert!(body.contains(&format!("next_offset={total}")), "{body}");
    let (status, _) = request(
        &leader.addr,
        "GET",
        "/internal/replication/wal?from_offset=0&from_generation=not-a-number",
        "",
        &[("x-replication-token", TOKEN)],
    );
    assert_eq!(status, 400);
    let (_, ok) = request(
        &leader.addr,
        "GET",
        &format!(
            "/internal/replication/wal?from_offset=0&max_records=10&from_generation={generation}"
        ),
        "",
        &[("x-replication-token", TOKEN)],
    );
    assert!(ok.contains("needs_resync=0"), "{ok}");
}

// ---------------------------------------------------------------------
// Commit groups (single ingests and batches) on the replication stream
// ---------------------------------------------------------------------

impl Server {
    fn batch(&self, commit_id: &str, claim_ids: &[&str]) {
        let items: Vec<String> = claim_ids
            .iter()
            .map(|id| {
                format!(
                    r#"{{"claim":{{"claim_id":"{id}","tenant_id":"{TENANT}","canonical_text":"follower statement {id}","confidence":0.9}},"evidence":[{{"evidence_id":"ev-{id}","claim_id":"{id}","source_id":"source://{id}","stance":"supports","source_quality":0.9}}]}}"#
                )
            })
            .collect();
        let body = format!(
            r#"{{"commit_id":"{commit_id}","items":[{}]}}"#,
            items.join(",")
        );
        let (status, text) = request(&self.addr, "POST", "/v1/ingest/batch", &body, &[]);
        assert_eq!(status, 200, "batch {commit_id}: {text}");
    }

    /// One frame as `(next_offset, total_records, lines)`.
    fn frame_from(&self, from: usize, max_records: usize) -> (usize, usize, Vec<String>) {
        let generation = self.frame().0;
        let (status, body) = request(
            &self.addr,
            "GET",
            &format!(
                "/internal/replication/wal?from_offset={from}&max_records={max_records}&from_generation={generation}"
            ),
            "",
            &[("x-replication-token", TOKEN)],
        );
        assert_eq!(status, 200, "{body}");
        let mut lines = body.lines();
        let (mut next, mut total) = (0, 0);
        for line in lines.by_ref() {
            if let Some(v) = line.strip_prefix("next_offset=") {
                next = v.parse().unwrap();
            } else if let Some(v) = line.strip_prefix("total_records=") {
                total = v.parse().unwrap();
            } else if line.starts_with("records=") {
                break;
            }
        }
        (next, total, lines.map(str::to_string).collect())
    }
}

fn wal_edges_for(wal_path: &Path, claim_id: &str) -> usize {
    let wal = FileWal::open(wal_path).expect("reopen wal");
    InMemoryStore::load_from_wal(&wal)
        .expect("replay wal")
        .edges_for_claim(claim_id)
        .len()
}

#[test]
fn leader_frames_never_end_inside_a_commit_group() {
    let _guard = serial();
    let dir = tempfile::tempdir().expect("tempdir");
    let leader = Server::start(&dir.path().join("leader.wal"), None, no_checkpoint(), None);
    leader.ingest("g1");
    leader.batch("batch-1", &["b1", "b2", "b3"]);
    leader.ingest("g2");

    let (_, total, all) = leader.frame_from(0, 10_000);
    assert_eq!(all.len(), total);
    let boundaries: Vec<usize> = (0..=total)
        .filter(|b| store::complete_group_prefix_len(&all[..*b]) == *b)
        .collect();
    assert!(boundaries.len() > 3);
    for &from in boundaries.iter().filter(|b| **b < total) {
        for max in 1..=6 {
            let (next, _, lines) = leader.frame_from(from, max);
            assert_eq!(from + lines.len(), next, "from={from} max={max}");
            assert!(next > from);
            assert_eq!(
                store::complete_group_prefix_len(&lines),
                lines.len(),
                "frame from={from} max={max} ends inside a group"
            );
        }
    }
}

#[test]
fn follower_converges_over_groups_with_tiny_frames_and_restart_mid_stream() {
    let _guard = serial();
    let dir = tempfile::tempdir().expect("tempdir");
    let leader = Server::start(&dir.path().join("leader.wal"), None, no_checkpoint(), None);
    leader.ingest("s1");
    leader.batch("batch-a", &["a1", "a2", "a3"]);
    leader.ingest("s2");

    let follower_wal = dir.path().join("follower.wal");
    set_env("DASH_INGEST_REPLICATION_MAX_RECORDS", "1");
    let mut follower = Server::start_follower(&leader.addr, &follower_wal, None, no_checkpoint());
    wait_until("some records", Duration::from_secs(10), || {
        follower
            .metric("dash_ingest_replication_applied_records_total")
            .is_some_and(|n| n > 0)
    });
    follower.stop();
    leader.batch("batch-b", &["x1", "x2"]);
    leader.ingest("s3");
    let (_, total) = leader.frame();
    let mut follower = Server::start_follower(&leader.addr, &follower_wal, None, no_checkpoint());
    unset_env("DASH_INGEST_REPLICATION_MAX_RECORDS");
    wait_until("converged", Duration::from_secs(15), || {
        follower.metric("dash_ingest_replication_last_offset") == Some(total as u64)
    });
    assert_eq!(
        follower.metric("dash_ingest_replication_resync_total"),
        Some(0)
    );
    follower.stop();

    assert_eq!(
        claim_ids_in_wal(&follower_wal),
        vec!["a1", "a2", "a3", "s1", "s2", "s3", "x1", "x2"]
    );
    let wal = FileWal::open(&follower_wal).expect("reopen");
    assert_eq!(wal.wal_record_count().unwrap(), total, "no duplicates");
    assert_eq!(wal_edges_for(&follower_wal, "s1"), 0);
}

#[test]
fn follower_holds_back_an_unterminated_group_and_refetches_from_its_start() {
    let _guard = serial();
    let dir = tempfile::tempdir().expect("tempdir");
    // Build a real two-group WAL, then serve it with a cut inside group two.
    let src_wal = dir.path().join("src.wal");
    {
        let leader = Server::start(&src_wal, None, no_checkpoint(), None);
        leader.ingest("h1");
        leader.ingest("h2");
    }
    let lines: Vec<String> = std::fs::read_to_string(&src_wal)
        .unwrap()
        .lines()
        .map(str::to_string)
        .collect();
    let first_group = store::complete_group_prefix_len(&lines[..lines.len() - 1]);
    assert!(first_group > 0 && first_group < lines.len() - 1);
    let total = lines.len();

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = format!("127.0.0.1:{}", listener.local_addr().unwrap().port());
    let requested = Arc::new(Mutex::new(Vec::<usize>::new()));
    {
        let requested = Arc::clone(&requested);
        let lines = lines.clone();
        thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { continue };
                let mut buf = [0u8; 4096];
                let n = stream.read(&mut buf).unwrap_or(0);
                let head = String::from_utf8_lossy(&buf[..n]).to_string();
                let from: usize = head
                    .split("from_offset=")
                    .nth(1)
                    .and_then(|r| r.split(['&', ' ']).next())
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(0);
                let first_poll = {
                    let mut guard = requested.lock().unwrap();
                    guard.push(from);
                    guard.len() == 1
                };
                let end = if first_poll { total - 1 } else { total };
                let slice = &lines[from..end];
                let body = format!(
                    "status=ok\ngeneration=1\nneeds_resync=0\nfrom_offset={from}\nnext_offset={end}\ntotal_records={total}\nrecords={}\n{}\n",
                    slice.len(),
                    slice.join("\n")
                );
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = stream.write_all(response.as_bytes());
            }
        });
    }
    let follower_wal = dir.path().join("follower.wal");
    let mut follower = Server::start_follower(&addr, &follower_wal, None, no_checkpoint());
    wait_until("both groups applied", Duration::from_secs(10), || {
        follower.metric("dash_ingest_replication_last_offset") == Some(total as u64)
    });
    follower.stop();
    let polls = requested.lock().unwrap().clone();
    assert_eq!(
        &polls[..2],
        &[0, first_group],
        "second poll restarts at the held-back group"
    );
    assert_eq!(claim_ids_in_wal(&follower_wal), vec!["h1", "h2"]);
    let wal = FileWal::open(&follower_wal).unwrap();
    assert_eq!(wal.wal_record_count().unwrap(), total);
}

#[test]
fn leader_checkpoint_between_groups_forces_resync_and_converges() {
    let _guard = serial();
    let dir = tempfile::tempdir().expect("tempdir");
    let leader = Server::start(
        &dir.path().join("leader.wal"),
        None,
        checkpoint_every_record(),
        None,
    );
    leader.ingest("k1");
    let follower_wal = dir.path().join("follower.wal");
    let mut follower = Server::start_follower(&leader.addr, &follower_wal, None, no_checkpoint());
    wait_until("first resync", Duration::from_secs(10), || {
        follower.metric("dash_ingest_replication_resync_total") == Some(1)
    });
    leader.batch("batch-k", &["k2", "k3"]);
    leader.ingest("k4");
    wait_until("converged", Duration::from_secs(15), || {
        follower
            .metric("dash_ingest_replication_applied_records_total")
            .is_some_and(|n| n >= 4)
    });
    thread::sleep(Duration::from_millis(300));
    follower.stop();
    assert_eq!(
        claim_ids_in_wal(&follower_wal),
        vec!["k1", "k2", "k3", "k4"]
    );
}
