//! End-to-end replication tests: a real ingestion server (the leader) feeds a
//! retrieval follower over HTTP on loopback. Covers REP-01..REP-06, REP-11
//! and REP-12.

use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, Once, RwLock,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use ingestion::transport::{IngestionRuntime, serve_http_with_workers as serve_ingestion};
use retrieval::{
    replication::{FollowerHandle, ReplicationFollowerConfig, start_follower},
    transport::serve_http_with_workers as serve_retrieval,
};
use schema::{RetrievalRequest, StanceMode};
use store::{AnnTuningConfig, CheckpointPolicy, FileWal, InMemoryStore};

const TOKEN: &str = "e2e-replication-token-0123456789";
const TENANT: &str = "tenant-e2e";

fn init_env() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        #[allow(unused_unsafe)]
        unsafe {
            std::env::set_var("DASH_INSECURE_DEV_MODE", "1");
            std::env::set_var("DASH_STRICT_SECRETS", "0");
            std::env::set_var("DASH_INGEST_REPLICATION_TOKEN", TOKEN);
        }
    });
}

fn free_addr() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind probe listener");
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

/// Minimal blocking HTTP client. Returns (status, body).
fn http(addr: &str, method: &str, path: &str, headers: &[(&str, &str)]) -> (u16, String) {
    let mut stream = TcpStream::connect(addr).expect("connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .expect("timeout");
    let mut request = format!(
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\nContent-Length: 0\r\n"
    );
    for (name, value) in headers {
        request.push_str(&format!("{name}: {value}\r\n"));
    }
    request.push_str("\r\n");
    stream.write_all(request.as_bytes()).expect("write");
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

fn http_post_json(addr: &str, path: &str, body: &str) -> (u16, String) {
    let mut stream = TcpStream::connect(addr).expect("connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .expect("timeout");
    let request = format!(
        "POST {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(request.as_bytes()).expect("write");
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

// ---------------------------------------------------------------------
// Leader (real ingestion server)
// ---------------------------------------------------------------------

struct Leader {
    addr: String,
    wal_path: PathBuf,
    shutdown: Arc<dash_common::ShutdownSignal>,
    thread: Option<JoinHandle<()>>,
    anchor_ingested: std::cell::Cell<bool>,
}

/// Claim every test edge points at. Incoming support edges inflate a claim's
/// `supports` count, so tests only look at evidence counts of claims that
/// nothing points to.
const ANCHOR: &str = "anchor";

impl Leader {
    fn start(wal_path: &Path, addr: Option<String>, policy: CheckpointPolicy) -> Self {
        init_env();
        let addr = addr.unwrap_or_else(free_addr);
        let wal = FileWal::open_with_sync_every_records(wal_path, 1).expect("open leader wal");
        let store = InMemoryStore::load_from_wal(&wal).expect("load leader wal");
        let runtime = IngestionRuntime::persistent(store, wal, policy);
        let shutdown = dash_common::ShutdownSignal::manual();
        let thread = {
            let addr = addr.clone();
            let shutdown = Arc::clone(&shutdown);
            thread::spawn(move || {
                serve_ingestion(runtime, &addr, 2, shutdown).expect("leader server");
            })
        };
        wait_until("leader accepting", Duration::from_secs(10), || {
            TcpStream::connect(&addr).is_ok()
        });
        Self {
            addr,
            wal_path: wal_path.to_path_buf(),
            shutdown,
            thread: Some(thread),
            anchor_ingested: std::cell::Cell::new(false),
        }
    }

    fn stop(&mut self) {
        self.shutdown.trigger();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }

    fn restart(&mut self, policy: CheckpointPolicy) {
        self.stop();
        *self = Leader::start(&self.wal_path.clone(), Some(self.addr.clone()), policy);
    }

    fn post_claim(&self, claim_id: &str, body: String) {
        let (status, text) = http_post_json(&self.addr, "/v1/ingest", &body);
        assert_eq!(status, 200, "ingest {claim_id} failed: {text}");
    }

    /// Ingest `claim_id` with one evidence row; with `edge_to` the claim also
    /// gets a support edge to that claim (always `ANCHOR`).
    fn ingest(&self, claim_id: &str, edge_to: Option<&str>) {
        if edge_to.is_some() && !self.anchor_ingested.replace(true) {
            self.post_claim(
                ANCHOR,
                format!(
                    r#"{{"claim":{{"claim_id":"{ANCHOR}","tenant_id":"{TENANT}","canonical_text":"anchor claim","confidence":0.9}},"evidence":[]}}"#
                ),
            );
        }
        let edges = match edge_to {
            Some(to) => format!(
                r#","edges":[{{"edge_id":"edge-{claim_id}-{to}","from_claim_id":"{claim_id}","to_claim_id":"{to}","relation":"supports","strength":0.8}}]"#
            ),
            None => String::new(),
        };
        let body = format!(
            r#"{{"claim":{{"claim_id":"{claim_id}","tenant_id":"{TENANT}","canonical_text":"replicated statement {claim_id}","confidence":0.9}},"evidence":[{{"evidence_id":"ev-{claim_id}","claim_id":"{claim_id}","source_id":"source://{claim_id}","stance":"supports","source_quality":0.9}}]{edges}}}"#
        );
        self.post_claim(claim_id, body);
    }

    /// (generation, total_records) as the leader reports them.
    fn frame(&self) -> (u64, usize) {
        let (status, body) = http(
            &self.addr,
            "GET",
            "/internal/replication/wal?from_offset=0&max_records=1",
            &[("x-replication-token", TOKEN)],
        );
        assert_eq!(status, 200, "leader frame: {body}");
        let field = |key: &str| -> u64 {
            body.lines()
                .find_map(|line| line.strip_prefix(&format!("{key}=")))
                .unwrap_or_else(|| panic!("frame missing {key}: {body}"))
                .parse()
                .expect("numeric frame field")
        };
        (field("generation"), field("total_records") as usize)
    }
}

impl Drop for Leader {
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

// ---------------------------------------------------------------------
// Follower (retrieval)
// ---------------------------------------------------------------------

struct Node {
    store: Arc<RwLock<InMemoryStore>>,
    handle: Option<FollowerHandle>,
}

impl Node {
    fn status(&self) -> retrieval::replication::FollowerStatusSnapshot {
        self.handle.as_ref().expect("running").status().snapshot()
    }

    fn handle(&self) -> &FollowerHandle {
        self.handle.as_ref().expect("running")
    }

    fn claim_ids(&self) -> Vec<String> {
        let mut ids: Vec<String> = self
            .store
            .read()
            .unwrap()
            .claims_for_tenant(TENANT)
            .into_iter()
            .map(|claim| claim.claim_id)
            .filter(|id| id != ANCHOR)
            .collect();
        ids.sort();
        ids
    }

    fn stop(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.stop();
        }
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        self.stop();
    }
}

fn test_config(leader_addr: &str) -> ReplicationFollowerConfig {
    let mut config = ReplicationFollowerConfig::new(format!("http://{leader_addr}"));
    config.poll_interval = Duration::from_millis(20);
    config.max_backoff = Duration::from_millis(200);
    config.token = Some(TOKEN.to_string());
    config
}

/// Follower that mirrors into a retrieval WAL, so restarts can resume.
fn start_durable(config: ReplicationFollowerConfig, wal_path: &Path) -> Node {
    init_env();
    let wal = FileWal::open(wal_path).expect("open follower wal");
    let store = InMemoryStore::load_from_wal(&wal).expect("load follower wal");
    let store = Arc::new(RwLock::new(store));
    let handle = start_follower(Arc::clone(&store), config, Some(wal));
    Node {
        store,
        handle: Some(handle),
    }
}

/// Follower with no retrieval WAL (nothing replicated survives a restart).
fn start_volatile(config: ReplicationFollowerConfig, store: InMemoryStore) -> Node {
    init_env();
    let store = Arc::new(RwLock::new(store));
    let handle = start_follower(Arc::clone(&store), config, None);
    Node {
        store,
        handle: Some(handle),
    }
}

fn supports_for(store: &Arc<RwLock<InMemoryStore>>, claim_id: &str) -> usize {
    let guard = store.read().unwrap();
    guard
        .retrieve(&RetrievalRequest {
            tenant_id: TENANT.to_string(),
            query: format!("replicated statement {claim_id}"),
            top_k: 20,
            stance_mode: StanceMode::Balanced,
        })
        .into_iter()
        .find(|result| result.claim_id == claim_id)
        .map(|result| result.supports)
        .unwrap_or(0)
}

struct RetrievalServer {
    addr: String,
    shutdown: Arc<dash_common::ShutdownSignal>,
    thread: Option<JoinHandle<()>>,
}

impl RetrievalServer {
    fn start(store: Arc<RwLock<InMemoryStore>>) -> Self {
        init_env();
        let addr = free_addr();
        let shutdown = dash_common::ShutdownSignal::manual();
        let thread = {
            let addr = addr.clone();
            let shutdown = Arc::clone(&shutdown);
            thread::spawn(move || {
                serve_retrieval(store, &addr, 2, shutdown).expect("retrieval server");
            })
        };
        wait_until("retrieval accepting", Duration::from_secs(10), || {
            TcpStream::connect(&addr).is_ok()
        });
        Self {
            addr,
            shutdown,
            thread: Some(thread),
        }
    }

    fn get(&self, path: &str) -> (u16, String) {
        http(&self.addr, "GET", path, &[])
    }
}

impl Drop for RetrievalServer {
    fn drop(&mut self) {
        self.shutdown.trigger();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

#[test]
fn initial_and_incremental_sync_report_lag_and_ready() {
    let dir = tempfile::tempdir().expect("tempdir");
    let leader = Leader::start(&dir.path().join("leader.wal"), None, no_checkpoint());
    leader.ingest("c1", None);
    leader.ingest("c2", Some("anchor"));

    let node = start_durable(test_config(&leader.addr), &dir.path().join("follower.wal"));
    wait_until("initial sync", Duration::from_secs(10), || {
        node.claim_ids().len() == 2
    });
    assert_eq!(node.claim_ids(), vec!["c1", "c2"]);
    assert_eq!(supports_for(&node.store, "c1"), 1);
    assert_eq!(
        node.store.read().unwrap().edges_for_claim("c2").len(),
        1,
        "edge replicated exactly once"
    );
    let (_, total) = leader.frame();
    wait_until(
        "offset reaches leader total",
        Duration::from_secs(10),
        || node.status().offset == total,
    );
    assert_eq!(
        node.status().resyncs_total,
        0,
        "fresh follower needs no resync"
    );
    assert!(node.status().generation.is_some());

    leader.ingest("c3", Some("anchor"));
    wait_until("incremental sync", Duration::from_secs(10), || {
        node.claim_ids().len() == 3
    });
    assert_eq!(supports_for(&node.store, "c3"), 1);

    let server = RetrievalServer::start(Arc::clone(&node.store));
    let (status, body) = server.get("/ready");
    assert_eq!(status, 200, "{body}");
    assert!(body.contains("\"replication\""), "{body}");
    assert!(body.contains("\"lag_records\":0"), "{body}");
    let (_, metrics) = server.get("/metrics");
    for gauge in [
        "dash_retrieval_replication_lag_records 0",
        "dash_retrieval_replication_consecutive_failures 0",
        "dash_retrieval_replication_resyncs_total 0",
    ] {
        assert!(metrics.contains(gauge), "missing `{gauge}` in metrics");
    }
    assert!(metrics.contains("dash_retrieval_replication_generation "));
    assert!(metrics.contains("dash_retrieval_replication_last_success_age_ms "));
}

#[test]
fn leader_checkpoint_then_more_writes_is_never_silently_skipped() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut leader = Leader::start(&dir.path().join("leader.wal"), None, no_checkpoint());
    leader.ingest("a", None);
    leader.ingest("b", Some("anchor"));
    let node = start_durable(test_config(&leader.addr), &dir.path().join("follower.wal"));
    wait_until("initial sync", Duration::from_secs(10), || {
        node.claim_ids().len() == 2
    });
    let (generation_before, total_before) = leader.frame();
    wait_until("caught up", Duration::from_secs(10), || {
        node.status().offset == total_before
    });
    node.handle().pause();
    let follower_offset = node.status().offset;

    // The leader compacts its WAL...
    leader.restart(checkpoint_every_record());
    leader.ingest("c", Some("anchor"));
    let (generation_after, _) = leader.frame();
    assert_ne!(
        generation_before, generation_after,
        "test premise: checkpoint must change the WAL generation"
    );
    // ...and then takes more writes without compacting, so the new WAL is at
    // least as long as the follower's old offset. With plain line offsets the
    // follower would continue from `follower_offset` and silently skip the
    // first records of the new WAL.
    leader.restart(no_checkpoint());
    leader.ingest("d", Some("anchor"));
    leader.ingest("e", Some("anchor"));
    leader.ingest("f", Some("anchor"));
    leader.ingest("g", Some("anchor"));
    let (_, total_after) = leader.frame();
    assert!(
        total_after >= follower_offset,
        "test premise: new WAL ({total_after}) must reach the old offset ({follower_offset})"
    );

    node.handle().resume();
    wait_until("follower converges", Duration::from_secs(10), || {
        node.claim_ids().len() == 7
    });
    assert_eq!(node.claim_ids(), vec!["a", "b", "c", "d", "e", "f", "g"]);
    for id in ["a", "b", "c", "d", "e", "f", "g"] {
        assert_eq!(supports_for(&node.store, id), 1, "evidence for {id}");
    }
    wait_until("resync counted", Duration::from_secs(10), || {
        node.status().resyncs_total >= 1
    });
    assert_eq!(node.status().resyncs_total, 1, "exactly one resync");
}

#[test]
fn snapshot_leader_resyncs_exactly_once_and_edge_counts_stay_stable() {
    let dir = tempfile::tempdir().expect("tempdir");
    let leader = Leader::start(
        &dir.path().join("leader.wal"),
        None,
        checkpoint_every_record(),
    );
    leader.ingest("s1", None);
    leader.ingest("s2", Some("anchor"));
    leader.ingest("s3", Some("anchor"));
    let (_, wal_total) = leader.frame();

    let node = start_volatile(test_config(&leader.addr), InMemoryStore::new());
    wait_until("resync", Duration::from_secs(10), || {
        node.claim_ids().len() == 3
    });
    // Keep polling for a while: the old protocol re-synced on every poll.
    let polls_before = Instant::now();
    while polls_before.elapsed() < Duration::from_millis(600) {
        thread::sleep(Duration::from_millis(20));
    }
    let status = node.status();
    assert_eq!(status.resyncs_total, 1, "resync must happen exactly once");
    assert_eq!(
        status.offset, wal_total,
        "offset counts leader WAL lines only"
    );
    assert_eq!(status.consecutive_failures, 0);
    let guard = node.store.read().unwrap();
    assert_eq!(guard.edges_for_claim("s2").len(), 1);
    assert_eq!(guard.edges_for_claim("s3").len(), 1);
    assert_eq!(guard.claims_len(), 4, "three claims plus the anchor");
}

#[test]
fn resync_replaces_stale_state_and_keeps_redb_in_sync() {
    let dir = tempfile::tempdir().expect("tempdir");
    let leader = Leader::start(
        &dir.path().join("leader.wal"),
        None,
        checkpoint_every_record(),
    );
    leader.ingest("r1", None);
    leader.ingest("r2", Some("anchor"));

    // A follower that already holds data the leader does not have.
    let redb_path = dir.path().join("follower.redb");
    let mut stale = InMemoryStore::new()
        .with_disk(&redb_path)
        .expect("attach redb");
    stale
        .ingest_bundle(
            schema::Claim {
                claim_id: "stale-claim".into(),
                tenant_id: TENANT.into(),
                canonical_text: "stale data the leader never had".into(),
                confidence: 0.5,
                event_time_unix: None,
                entities: vec![],
                embedding_ids: vec![],
                claim_type: None,
                valid_from: None,
                valid_to: None,
                created_at: None,
                updated_at: None,
            },
            vec![],
            vec![],
        )
        .expect("seed stale claim");
    assert!(matches!(stale.disk_status(), store::DiskStatus::Available));

    let mut node = start_volatile(test_config(&leader.addr), stale);
    wait_until("resync replaces state", Duration::from_secs(10), || {
        node.claim_ids() == vec!["r1".to_string(), "r2".to_string()]
    });
    assert!(
        node.store
            .read()
            .unwrap()
            .claim_by_id("stale-claim")
            .is_none(),
        "resync must replace, not merge"
    );
    assert!(matches!(
        node.store.read().unwrap().disk_status(),
        store::DiskStatus::Available
    ));

    // The redb file was rewritten from the new state: reopening it shows the
    // leader's claims and nothing stale.
    node.stop();
    let node_store = Arc::try_unwrap(std::mem::replace(
        &mut node.store,
        Arc::new(RwLock::new(InMemoryStore::new())),
    ))
    .ok()
    .expect("follower thread released the store");
    drop(node_store);
    let mut empty_wal = FileWal::open(dir.path().join("empty.wal")).expect("empty wal");
    let (reloaded, _) = InMemoryStore::load_from_disk_and_wal(
        &redb_path,
        &mut empty_wal,
        AnnTuningConfig::default(),
    )
    .expect("reload from redb");
    let mut ids: Vec<String> = reloaded
        .claims_for_tenant(TENANT)
        .into_iter()
        .map(|c| c.claim_id)
        .collect();
    ids.sort();
    assert_eq!(
        ids,
        vec!["anchor", "r1", "r2"],
        "redb must not retain stale rows"
    );
}

#[test]
fn follower_restart_resumes_from_saved_state_without_duplicates() {
    let dir = tempfile::tempdir().expect("tempdir");
    let leader = Leader::start(&dir.path().join("leader.wal"), None, no_checkpoint());
    leader.ingest("p1", None);
    leader.ingest("p2", Some("anchor"));
    let follower_wal = dir.path().join("follower.wal");
    let mut node = start_durable(test_config(&leader.addr), &follower_wal);
    wait_until("sync", Duration::from_secs(10), || {
        node.claim_ids().len() == 2
    });
    let (_, total) = leader.frame();
    wait_until("caught up", Duration::from_secs(10), || {
        node.status().offset == total
    });
    node.stop();
    drop(node);
    let total_before_restart = total;

    leader.ingest("p3", Some("anchor"));
    let node = start_durable(test_config(&leader.addr), &follower_wal);
    assert!(
        node.status().synced_once,
        "a follower resuming durable state starts out synced"
    );
    wait_until("resume", Duration::from_secs(10), || {
        node.claim_ids().len() == 3
    });
    let (_, total) = leader.frame();
    wait_until("caught up again", Duration::from_secs(10), || {
        node.status().offset == total
    });
    let status = node.status();
    assert_eq!(status.resyncs_total, 0, "restart must not trigger a resync");
    assert_eq!(
        status.applied_records_total,
        (total - total_before_restart) as u64,
        "only records written after the restart are applied"
    );
    for id in ["p1", "p2", "p3"] {
        assert_eq!(
            supports_for(&node.store, id),
            1,
            "no duplicate evidence for {id}"
        );
    }
    drop(node);
    let wal = FileWal::open(&follower_wal).expect("reopen follower wal");
    assert_eq!(
        wal.wal_record_count().expect("count"),
        total,
        "follower WAL mirrors the leader WAL without duplicates"
    );
}

#[test]
fn saved_offset_is_not_resumed_into_an_empty_store() {
    let dir = tempfile::tempdir().expect("tempdir");
    let leader = Leader::start(&dir.path().join("leader.wal"), None, no_checkpoint());
    leader.ingest("v1", None);
    leader.ingest("v2", Some("anchor"));
    let (generation, total) = leader.frame();

    // Without a retrieval WAL the saved offset is ignored: a restart does a
    // full resync instead of resuming into an empty store.
    let state_path = dir.path().join("volatile.offset");
    std::fs::write(
        &state_path,
        format!("generation={generation}\noffset={total}\n"),
    )
    .expect("write stale state");
    let mut config = test_config(&leader.addr);
    config.offset_path = Some(state_path.to_string_lossy().to_string());
    let node = start_volatile(config, InMemoryStore::new());
    wait_until("full resync without WAL", Duration::from_secs(10), || {
        node.claim_ids().len() == 2
    });
    wait_until("resync counted", Duration::from_secs(10), || {
        node.status().resyncs_total >= 1
    });
    assert_eq!(node.status().resyncs_total, 1);
    drop(node);

    // With a WAL that was lost (persistence path wiped) the saved offset no
    // longer matches the WAL contents, so the follower resyncs too.
    let follower_wal = dir.path().join("follower.wal");
    let mut node = start_durable(test_config(&leader.addr), &follower_wal);
    wait_until("durable sync", Duration::from_secs(10), || {
        node.claim_ids().len() == 2
    });
    node.stop();
    drop(node);
    for suffix in ["", ".generation", ".snapshot"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", follower_wal.display()));
    }
    let node = start_durable(test_config(&leader.addr), &follower_wal);
    wait_until("resync after WAL loss", Duration::from_secs(10), || {
        node.claim_ids().len() == 2
    });
    wait_until("resync counted", Duration::from_secs(10), || {
        node.status().resyncs_total >= 1
    });
    assert_eq!(node.status().resyncs_total, 1);
}

#[test]
fn leader_restart_keeps_generation_and_follower_recovers_without_resync() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut leader = Leader::start(&dir.path().join("leader.wal"), None, no_checkpoint());
    leader.ingest("l1", None);
    let node = start_durable(test_config(&leader.addr), &dir.path().join("follower.wal"));
    wait_until("sync", Duration::from_secs(10), || {
        node.claim_ids().len() == 1
    });

    leader.stop();
    wait_until(
        "failures observed while leader is down",
        Duration::from_secs(10),
        || node.status().consecutive_failures >= 2,
    );
    assert!(node.status().last_error.is_some());

    leader.restart(no_checkpoint());
    leader.ingest("l2", Some("anchor"));
    wait_until("recovery", Duration::from_secs(10), || {
        node.claim_ids().len() == 2
    });
    wait_until("failure streak cleared", Duration::from_secs(10), || {
        node.status().consecutive_failures == 0
    });
    assert_eq!(
        node.status().resyncs_total,
        0,
        "same WAL lineage, no resync"
    );
    assert_eq!(supports_for(&node.store, "l2"), 1);
}

#[test]
fn unreachable_leader_backs_off_and_readiness_reports_it() {
    let dead_addr = free_addr();
    let node = start_volatile(test_config(&dead_addr), InMemoryStore::new());
    let server = RetrievalServer::start(Arc::clone(&node.store));
    thread::sleep(Duration::from_millis(1200));
    let status = node.status();
    assert!(
        status.failures_total >= 3,
        "expected repeated failures, got {}",
        status.failures_total
    );
    assert!(
        status.failures_total <= 20,
        "backoff should keep attempts well below one per poll interval (got {})",
        status.failures_total
    );
    assert!(status.last_error.is_some());
    let (code, body) = server.get("/ready");
    assert_eq!(code, 503, "{body}");
    assert!(body.contains("replication_initial_sync_pending"), "{body}");
    assert!(body.contains("\"consecutive_failures\""), "{body}");
    assert!(body.contains("\"last_error\":\""), "{body}");
    let (_, metrics) = server.get("/metrics");
    assert!(metrics.contains("dash_retrieval_replication_ready 0"));
}

#[test]
fn token_mismatch_is_rejected_and_nothing_is_applied() {
    let dir = tempfile::tempdir().expect("tempdir");
    let leader = Leader::start(&dir.path().join("leader.wal"), None, no_checkpoint());
    leader.ingest("t1", None);

    let mut wrong = test_config(&leader.addr);
    wrong.token = Some("not-the-token".to_string());
    let node = start_volatile(wrong, InMemoryStore::new());
    wait_until("auth failures", Duration::from_secs(10), || {
        node.status().failures_total >= 2
    });
    assert!(
        node.status()
            .last_error
            .as_deref()
            .unwrap_or_default()
            .contains("403"),
        "{:?}",
        node.status().last_error
    );
    assert!(node.claim_ids().is_empty());
    drop(node);

    let mut missing = test_config(&leader.addr);
    missing.token = None;
    let node = start_volatile(missing, InMemoryStore::new());
    wait_until(
        "auth failures without token",
        Duration::from_secs(10),
        || node.status().failures_total >= 1,
    );
    assert!(node.claim_ids().is_empty());
}

#[test]
fn readiness_goes_stale_when_the_leader_disappears() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut leader = Leader::start(&dir.path().join("leader.wal"), None, no_checkpoint());
    leader.ingest("z1", None);
    let mut config = test_config(&leader.addr);
    config.max_staleness_ms = 300;
    let node = start_durable(config, &dir.path().join("follower.wal"));
    wait_until("sync", Duration::from_secs(10), || {
        node.claim_ids().len() == 1
    });
    let server = RetrievalServer::start(Arc::clone(&node.store));
    wait_until("ready", Duration::from_secs(10), || {
        server.get("/ready").0 == 200
    });
    leader.stop();
    wait_until("stale", Duration::from_secs(10), || {
        let (code, body) = server.get("/ready");
        code == 503 && body.contains("replication_stale")
    });
}

// ---------------------------------------------------------------------
// Misbehaving leaders
// ---------------------------------------------------------------------

/// A leader whose `/internal/replication/wal` response is scripted.
struct MockLeader {
    addr: String,
    body: Arc<Mutex<String>>,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl MockLeader {
    fn start(initial_body: &str) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock");
        let addr = listener.local_addr().expect("addr").to_string();
        listener.set_nonblocking(true).expect("nonblocking");
        let body = Arc::new(Mutex::new(initial_body.to_string()));
        let stop = Arc::new(AtomicBool::new(false));
        let thread = {
            let body = Arc::clone(&body);
            let stop = Arc::clone(&stop);
            thread::spawn(move || {
                while !stop.load(Ordering::SeqCst) {
                    match listener.accept() {
                        Ok((mut stream, _)) => {
                            stream.set_nonblocking(false).ok();
                            stream.set_read_timeout(Some(Duration::from_secs(2))).ok();
                            let mut buf = [0u8; 2048];
                            let _ = stream.read(&mut buf);
                            let payload = body.lock().unwrap().clone();
                            let response = format!(
                                "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{payload}",
                                payload.len()
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
            body,
            stop,
            thread: Some(thread),
        }
    }

    fn set_body(&self, body: &str) {
        *self.body.lock().unwrap() = body.to_string();
    }
}

impl Drop for MockLeader {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

const GOOD_CLAIM_LINE: &str = "C\tclaim-1\ttenant-a\ttext\t0.9\tnull\t\t";

#[test]
fn bad_leader_responses_never_partially_apply_or_kill_the_follower() {
    let dir = tempfile::tempdir().expect("tempdir");
    // Second record is garbage: the first must not be applied either.
    let partial = format!(
        "status=ok\ngeneration=5\nneeds_resync=0\nfrom_offset=0\nnext_offset=2\ntotal_records=2\nrecords=2\n{GOOD_CLAIM_LINE}\nthis is not a wal record\n"
    );
    let mock = MockLeader::start(&partial);
    let node = start_durable(test_config(&mock.addr), &dir.path().join("follower.wal"));
    wait_until("apply failures", Duration::from_secs(10), || {
        node.status().failures_total >= 2
    });
    assert_eq!(
        node.store.read().unwrap().claims_len(),
        0,
        "a failing record must leave no partially applied batch"
    );
    assert_eq!(node.status().offset, 0);

    // An advertised record count far beyond the body must be rejected
    // without allocating for it (and without killing the thread).
    mock.set_body(
        "status=ok\ngeneration=5\nneeds_resync=0\nfrom_offset=0\nnext_offset=99999999999\ntotal_records=99999999999\nrecords=99999999999\n",
    );
    let failures_before = node.status().failures_total;
    wait_until(
        "rejects absurd record count",
        Duration::from_secs(10),
        || node.status().failures_total > failures_before + 1,
    );
    assert_eq!(node.store.read().unwrap().claims_len(), 0);

    // Responses above the byte cap are refused.
    mock.set_body(&"x".repeat(200_000));
    let failures_before = node.status().failures_total;
    wait_until("rejects oversized body", Duration::from_secs(10), || {
        node.status().failures_total > failures_before + 1
    });
    drop(node);
    let mut capped = test_config(&mock.addr);
    capped.max_response_bytes = 1024;
    let node = start_volatile(capped, InMemoryStore::new());
    wait_until(
        "oversized failure reported",
        Duration::from_secs(10),
        || {
            node.status()
                .last_error
                .as_deref()
                .is_some_and(|e| e.contains("exceeds"))
        },
    );
    drop(node);

    // The follower thread survives and recovers once the leader behaves.
    let good = format!(
        "status=ok\ngeneration=5\nneeds_resync=0\nfrom_offset=0\nnext_offset=1\ntotal_records=1\nrecords=1\n{GOOD_CLAIM_LINE}\n"
    );
    let node = start_durable(test_config(&mock.addr), &dir.path().join("recover.wal"));
    mock.set_body(&good);
    wait_until("recovers", Duration::from_secs(10), || {
        node.store.read().unwrap().claims_len() == 1
    });
}

// ---------------------------------------------------------------------
// Commit groups (single ingests and batches) on the replication stream
// ---------------------------------------------------------------------

impl Leader {
    fn post_batch(&self, commit_id: &str, claim_ids: &[&str]) {
        let items: Vec<String> = claim_ids
            .iter()
            .map(|id| {
                format!(
                    r#"{{"claim":{{"claim_id":"{id}","tenant_id":"{TENANT}","canonical_text":"replicated statement {id}","confidence":0.9}},"evidence":[{{"evidence_id":"ev-{id}","claim_id":"{id}","source_id":"source://{id}","stance":"supports","source_quality":0.9}}]}}"#
                )
            })
            .collect();
        let body = format!(
            r#"{{"commit_id":"{commit_id}","items":[{}]}}"#,
            items.join(",")
        );
        let (status, text) = http_post_json(&self.addr, "/v1/ingest/batch", &body);
        assert_eq!(status, 200, "batch {commit_id} failed: {text}");
    }

    /// One replication frame as `(next_offset, total_records, lines)`.
    fn frame_from(&self, from: usize, max_records: usize) -> (usize, usize, Vec<String>) {
        let (status, body) = http(
            &self.addr,
            "GET",
            &format!(
                "/internal/replication/wal?from_offset={from}&max_records={max_records}&from_generation={}",
                self.frame().0
            ),
            &[("x-replication-token", TOKEN)],
        );
        assert_eq!(status, 200, "{body}");
        let mut lines = body.lines();
        let mut next = 0;
        let mut total = 0;
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

#[test]
fn leader_frames_never_end_inside_a_commit_group() {
    let dir = tempfile::tempdir().expect("tempdir");
    let leader = Leader::start(&dir.path().join("leader.wal"), None, no_checkpoint());
    leader.ingest("g1", None);
    leader.post_batch("batch-1", &["b1", "b2", "b3"]);
    leader.ingest("g2", None);

    let (_, total, all) = leader.frame_from(0, 10_000);
    assert_eq!(all.len(), total);
    let boundaries: Vec<usize> = (0..=total)
        .filter(|b| store::complete_group_prefix_len(&all[..*b]) == *b)
        .collect();
    assert!(boundaries.len() > 3, "expected several group boundaries");
    for &from in &boundaries {
        if from == total {
            continue;
        }
        for max in 1..=6 {
            let (next, _, lines) = leader.frame_from(from, max);
            assert_eq!(
                from + lines.len(),
                next,
                "from={from} max={max} lines={lines:?}"
            );
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
    let dir = tempfile::tempdir().expect("tempdir");
    let leader = Leader::start(&dir.path().join("leader.wal"), None, no_checkpoint());
    leader.ingest("s1", Some("anchor"));
    leader.post_batch("batch-a", &["a1", "a2", "a3"]);
    leader.ingest("s2", Some("anchor"));

    let follower_wal = dir.path().join("follower.wal");
    let mut config = test_config(&leader.addr);
    config.max_records = 1;
    let mut node = start_durable(config.clone(), &follower_wal);
    wait_until("first claims", Duration::from_secs(10), || {
        !node.claim_ids().is_empty()
    });
    // Restart mid-stream, with more groups arriving meanwhile.
    node.stop();
    leader.post_batch("batch-b", &["x1", "x2"]);
    leader.ingest("s3", Some("anchor"));
    let node = start_durable(config, &follower_wal);

    let expected = ["a1", "a2", "a3", "s1", "s2", "s3", "x1", "x2"];
    wait_until("converged", Duration::from_secs(15), || {
        node.claim_ids() == expected
    });
    let (_, total) = leader.frame();
    wait_until("offset", Duration::from_secs(10), || {
        node.status().offset == total
    });
    assert_eq!(node.status().resyncs_total, 0);
    assert_eq!(
        node.store.read().unwrap().edges_for_claim("s1").len(),
        1,
        "no duplicate edges"
    );
    assert_eq!(supports_for(&node.store, "a1"), 1, "no duplicate evidence");
    drop(node);
    let wal = FileWal::open(&follower_wal).expect("reopen");
    assert_eq!(
        wal.wal_record_count().unwrap(),
        total,
        "each leader line (markers included) exactly once"
    );
    let replayed = InMemoryStore::load_from_wal(&wal).expect("replay");
    assert_eq!(replayed.claim_count_for_tenant(TENANT), expected.len() + 1);
}

#[test]
fn follower_holds_back_an_unterminated_group_and_refetches_from_its_start() {
    // A scripted leader serves a frame that ends inside a group.
    let dir = tempfile::tempdir().expect("tempdir");
    let src_wal = dir.path().join("src.wal");
    {
        let mut wal = FileWal::open(&src_wal).unwrap();
        let mut s = InMemoryStore::new();
        for id in ["h1", "h2"] {
            s.ingest_atomic_persistent(
                &mut wal,
                schema::Claim {
                    claim_id: id.to_string(),
                    tenant_id: TENANT.to_string(),
                    canonical_text: format!("replicated statement {id}"),
                    confidence: 0.9,
                    event_time_unix: None,
                    entities: vec![],
                    embedding_ids: vec![],
                    claim_type: None,
                    valid_from: None,
                    valid_to: None,
                    created_at: None,
                    updated_at: None,
                },
                vec![],
                vec![],
                None,
                1,
            )
            .unwrap();
        }
    }
    let lines: Vec<String> = std::fs::read_to_string(&src_wal)
        .unwrap()
        .lines()
        .map(str::to_string)
        .collect();
    assert_eq!(lines.len(), 6);
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
                // First poll: cut inside the second group (5 of 6 lines).
                let end = if first_poll { 5 } else { 6 };
                let slice = &lines[from..end];
                let body = format!(
                    "status=ok\ngeneration=1\nneeds_resync=0\nfrom_offset={from}\nnext_offset={end}\ntotal_records=6\nrecords={}\n{}\n",
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
    let node = start_durable(test_config(&addr), &dir.path().join("follower.wal"));
    wait_until("both groups applied", Duration::from_secs(10), || {
        node.claim_ids() == ["h1", "h2"]
    });
    let polls = requested.lock().unwrap().clone();
    assert_eq!(
        &polls[..2],
        &[0, 3],
        "second poll restarts at the held-back group"
    );
    wait_until("offset at end", Duration::from_secs(5), || {
        node.status().offset == 6
    });
}

#[test]
fn leader_checkpoint_between_groups_forces_resync_and_converges() {
    let dir = tempfile::tempdir().expect("tempdir");
    let leader = Leader::start(
        &dir.path().join("leader.wal"),
        None,
        checkpoint_every_record(),
    );
    leader.ingest("k1", None);
    let node = start_durable(test_config(&leader.addr), &dir.path().join("follower.wal"));
    wait_until("k1", Duration::from_secs(10), || node.claim_ids() == ["k1"]);
    leader.post_batch("batch-k", &["k2", "k3"]);
    leader.ingest("k4", None);
    wait_until("converged", Duration::from_secs(15), || {
        node.claim_ids() == ["k1", "k2", "k3", "k4"]
    });
    assert_eq!(supports_for(&node.store, "k2"), 1, "no duplicate evidence");
}
