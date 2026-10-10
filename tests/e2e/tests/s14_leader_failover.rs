//! Scenario 14: automatic failover of the ingestion leader (ADR 0006) with
//! the real binaries: one control plane and three ingestion nodes with
//! synchronous replication (`DASH_INGEST_MIN_SYNC_REPLICAS=1`).
//!
//! * The leader is killed with SIGKILL while a client writes to it; a
//!   follower is promoted within the configured window, every write the
//!   old leader acknowledged is on the new leader, writes resume there,
//!   the placement moves with it, and the followers converge.
//! * The old leader restarts and rejoins as a follower: it refuses writes
//!   (pointing at the new leader), keeps a copy of its WAL and resyncs.
//! * A leader that is paused (SIGSTOP, isolated from everyone) and resumed
//!   after a failover refuses every write: two nodes never accept writes
//!   at the same time.
//!
//! Waits poll real state with deadlines; nothing sleeps to synchronise.

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use dash_e2e::*;
use serde_json::Value;

const T: &str = "tenant-a";
const LEASE_MS: u64 = 2_000;
const GRACE_MS: u64 = 500;
const HEARTBEAT_MS: u64 = 200;

struct Node {
    id: String,
    port: u16,
    proc: Option<Proc>,
}

impl Node {
    fn addr(&self) -> SocketAddr {
        format!("127.0.0.1:{}", self.port).parse().unwrap()
    }
    fn url(&self) -> String {
        format!("http://{}", self.addr())
    }
    fn client(&self) -> Client {
        Client::new(self.addr())
    }
    fn status(&self, path: &str) -> Option<u16> {
        let mut probe = self.client();
        probe.timeout = Duration::from_secs(2);
        probe.request("GET", path, &[], None).ok().map(|r| r.status)
    }
    fn log(&self) -> String {
        self.proc.as_ref().map(|p| p.log()).unwrap_or_default()
    }
}

struct Cluster {
    dir: tempfile::TempDir,
    token: String,
    replication_token: String,
    ingest_key: String,
    cp_port: u16,
    cp: Option<Proc>,
    nodes: Vec<Node>,
}

impl Cluster {
    fn new() -> Cluster {
        let ids = ["n1", "n2", "n3"];
        Cluster {
            dir: tempfile::Builder::new()
                .prefix("dash-e2e-failover-")
                .tempdir()
                .unwrap(),
            token: random_secret(),
            replication_token: random_secret(),
            ingest_key: random_secret(),
            cp_port: free_port(),
            cp: None,
            nodes: ids
                .iter()
                .map(|id| Node {
                    id: id.to_string(),
                    port: free_port(),
                    proc: None,
                })
                .collect(),
        }
    }

    fn path(&self, name: &str) -> PathBuf {
        self.dir.path().join(name)
    }

    fn cp_addr(&self) -> SocketAddr {
        format!("127.0.0.1:{}", self.cp_port).parse().unwrap()
    }

    fn start_control_plane(&mut self) {
        let placement = self.path("placement.seed.csv");
        std::fs::write(
            &placement,
            format!(
                "{T},0,1,n1,leader,healthy\n{T},0,1,n2,follower,healthy\n{T},0,1,n3,follower,healthy\n"
            ),
        )
        .unwrap();
        let env: Vec<(String, String)> = vec![
            ("DASH_CONTROL_PLANE_BIND".into(), self.cp_addr().to_string()),
            ("DASH_CONTROL_PLANE_TOKEN".into(), self.token.clone()),
            ("DASH_CONTROL_PLANE_NODE_ID".into(), "cp-1".into()),
            (
                "DASH_CONTROL_PLANE_STATE_PATH".into(),
                self.path("placement.csv").display().to_string(),
            ),
            (
                "DASH_ROUTER_PLACEMENT_FILE".into(),
                placement.display().to_string(),
            ),
            ("DASH_CONTROL_PLANE_INGEST_FAILOVER".into(), "1".into()),
            (
                "DASH_CONTROL_PLANE_INGEST_LEASE_MS".into(),
                LEASE_MS.to_string(),
            ),
            (
                "DASH_CONTROL_PLANE_INGEST_PROMOTION_GRACE_MS".into(),
                GRACE_MS.to_string(),
            ),
        ];
        let mut proc = Proc::spawn(
            "control-plane",
            "control-plane",
            &[],
            &env,
            &self.path("control-plane.log"),
        );
        proc.wait_live(
            self.cp_addr(),
            "/v1/control-plane/health",
            Duration::from_secs(20),
        );
        self.cp = Some(proc);
    }

    fn node_env(&self, index: usize) -> Vec<(String, String)> {
        let node = &self.nodes[index];
        let mut env: Vec<(String, String)> = vec![
            ("DASH_INGEST_BIND".into(), node.addr().to_string()),
            (
                "DASH_INGEST_WAL_PATH".into(),
                self.path(&format!("{}.wal", node.id)).display().to_string(),
            ),
            (
                "DASH_INGEST_PERSISTENCE_PATH".into(),
                self.path(&format!("{}.redb", node.id))
                    .display()
                    .to_string(),
            ),
            (
                "DASH_INGEST_API_KEY_SCOPES".into(),
                format!("{}:{T}:ingest,read_only", self.ingest_key),
            ),
            (
                "DASH_INGEST_REPLICATION_TOKEN".into(),
                self.replication_token.clone(),
            ),
            ("DASH_INGEST_RATE_LIMIT_PER_TENANT_RPS".into(), "0".into()),
            ("DASH_NODE_ID".into(), node.id.clone()),
            (
                "DASH_INGEST_FAILOVER_CONTROL_PLANE_URL".into(),
                format!("http://{}", self.cp_addr()),
            ),
            ("DASH_INGEST_FAILOVER_ADVERTISE_URL".into(), node.url()),
            (
                "DASH_INGEST_FAILOVER_HEARTBEAT_INTERVAL_MS".into(),
                HEARTBEAT_MS.to_string(),
            ),
            ("DASH_CONTROL_PLANE_TOKEN".into(), self.token.clone()),
            ("DASH_INGEST_MIN_SYNC_REPLICAS".into(), "1".into()),
            (
                "DASH_INGEST_SYNC_REPLICATION_TIMEOUT_MS".into(),
                "10000".into(),
            ),
            (
                "DASH_INGEST_REPLICATION_POLL_INTERVAL_MS".into(),
                "100".into(),
            ),
            (
                "DASH_INGEST_REPLICATION_MAX_BACKOFF_MS".into(),
                "500".into(),
            ),
        ];
        // n1 is configured as the writer of a new cluster; the others start
        // as its followers (so the bootstrap election is deterministic).
        if index > 0 {
            env.push((
                "DASH_INGEST_REPLICATION_SOURCE_URL".into(),
                self.nodes[0].url(),
            ));
        }
        env
    }

    fn start_node(&mut self, index: usize) {
        let env = self.node_env(index);
        let id = self.nodes[index].id.clone();
        let mut proc = Proc::spawn(
            &id,
            "ingestion",
            &[],
            &env,
            &self.path(&format!("{id}.log")),
        );
        proc.wait_live(self.nodes[index].addr(), "/live", Duration::from_secs(30));
        self.nodes[index].proc = Some(proc);
    }

    fn logs(&self) -> String {
        let mut out = String::new();
        if let Some(cp) = &self.cp {
            out.push_str(&format!("--- control plane ---\n{}\n", cp.log()));
        }
        for node in &self.nodes {
            out.push_str(&format!("--- {} ---\n{}\n", node.id, node.log()));
        }
        out
    }

    /// Index of the node answering 200 on `/v1/ready/leader`, polled until
    /// exactly one does (among `candidates`).
    fn wait_leader(&self, candidates: &[usize], timeout: Duration) -> usize {
        let end = Instant::now() + timeout;
        loop {
            let leaders: Vec<usize> = candidates
                .iter()
                .copied()
                .filter(|i| self.nodes[*i].status("/v1/ready/leader") == Some(200))
                .collect();
            assert!(
                leaders.len() <= 1,
                "two nodes report leadership at once: {leaders:?}\n{}",
                self.logs()
            );
            if let [leader] = leaders[..] {
                return leader;
            }
            assert!(
                Instant::now() < end,
                "no leader among {candidates:?} within {timeout:?}\n{}",
                self.logs()
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    fn wait_ready(&self, index: usize, timeout: Duration) {
        let end = Instant::now() + timeout;
        while self.nodes[index].status("/v1/ready") != Some(200) {
            assert!(
                Instant::now() < end,
                "{} never became ready\n{}",
                self.nodes[index].id,
                self.logs()
            );
            std::thread::sleep(Duration::from_millis(25));
        }
    }

    fn ingest(&self, index: usize, claim_id: &str) -> std::io::Result<Resp> {
        self.nodes[index].client().try_post_json(
            "/v1/ingest",
            &[("x-api-key", self.ingest_key.as_str())],
            &bundle(T, claim_id, &format!("failover claim {claim_id}"), 1),
        )
    }

    /// Claim ids in the node's durable state (its replication export).
    fn claims(&self, index: usize) -> BTreeSet<String> {
        let r = self.nodes[index].client().get(
            "/internal/replication/export",
            &[("x-replication-token", self.replication_token.as_str())],
        );
        assert_eq!(
            r.status, 200,
            "export of {}: {}",
            self.nodes[index].id, r.body
        );
        LeaderState::parse(&r.body).claims.into_keys().collect()
    }

    fn wait_same_claims(&self, follower: usize, leader: usize, timeout: Duration) {
        let end = Instant::now() + timeout;
        loop {
            let expected = self.claims(leader);
            if self.claims(follower) == expected {
                return;
            }
            assert!(
                Instant::now() < end,
                "{} did not converge to {}\n{}",
                self.nodes[follower].id,
                self.nodes[leader].id,
                self.logs()
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    fn cp_get(&self, path: &str) -> Resp {
        Client::new(self.cp_addr()).get(
            path,
            &[("Authorization", &format!("Bearer {}", self.token))],
        )
    }

    fn start_all(&mut self) -> usize {
        self.start_control_plane();
        for index in 0..self.nodes.len() {
            self.start_node(index);
        }
        let leader = self.wait_leader(&[0, 1, 2], Duration::from_secs(30));
        assert_eq!(leader, 0, "n1 is the bootstrap writer");
        self.wait_ready(1, Duration::from_secs(30));
        self.wait_ready(2, Duration::from_secs(30));
        leader
    }
}

fn metric(text: &str, name: &str) -> Option<f64> {
    text.lines()
        .find_map(|line| line.strip_prefix(&format!("{name} ")))
        .and_then(|value| value.trim().parse().ok())
}

#[test]
fn leader_kill_promotes_a_follower_without_losing_acknowledged_writes() {
    let mut cluster = Cluster::new();
    cluster.start_all();

    // Before the failover: writes go to n1 and are confirmed by a follower;
    // a follower refuses writes and names the leader.
    for i in 0..10 {
        let r = cluster.ingest(0, &format!("pre-{i}")).unwrap();
        assert_eq!(r.status, 200, "{}", r.body);
        assert!(
            r.header("x-dash-sync-replicas")
                .and_then(|v| v.parse::<u32>().ok())
                .is_some_and(|n| n >= 1),
            "synchronous write confirmed by a follower: {:?}",
            r.headers
        );
    }
    let refused = cluster.ingest(1, "to-a-follower").unwrap();
    assert_eq!(refused.status, 503, "{}", refused.body);
    assert_eq!(refused.header("x-dash-leader"), Some("false"));
    assert_eq!(
        refused.header("x-dash-leader-url"),
        Some(cluster.nodes[0].url().as_str())
    );
    assert!(refused.header("retry-after").is_some());

    // A client keeps writing to n1 while it is killed.
    let acked: Arc<Mutex<Vec<String>>> =
        Arc::new(Mutex::new((0..10).map(|i| format!("pre-{i}")).collect()));
    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
        let acked = Arc::clone(&acked);
        let stop = Arc::clone(&stop);
        let client = cluster.nodes[0].client();
        let key = cluster.ingest_key.clone();
        std::thread::spawn(move || {
            let mut i = 0;
            while !stop.load(Ordering::SeqCst) {
                let id = format!("live-{i}");
                i += 1;
                match client.try_post_json(
                    "/v1/ingest",
                    &[("x-api-key", key.as_str())],
                    &bundle(T, &id, &format!("failover claim {id}"), 1),
                ) {
                    Ok(r) if r.status == 200 => acked.lock().unwrap().push(id),
                    _ => break,
                }
            }
        })
    };
    // Let some live writes land, then kill the leader mid-stream.
    let end = Instant::now() + Duration::from_secs(20);
    while acked.lock().unwrap().len() < 40 {
        assert!(
            Instant::now() < end,
            "writes are not progressing\n{}",
            cluster.logs()
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    let killed_at = Instant::now();
    cluster.nodes[0].proc.as_mut().unwrap().kill9();
    stop.store(true, Ordering::SeqCst);
    writer.join().unwrap();
    let acked: Vec<String> = acked.lock().unwrap().clone();

    let leader = cluster.wait_leader(&[1, 2], Duration::from_secs(30));
    let failover = killed_at.elapsed();
    eprintln!(
        "failover: {} promoted {} ms after SIGKILL of n1 (lease {LEASE_MS} ms, grace {GRACE_MS} ms, heartbeat {HEARTBEAT_MS} ms)",
        cluster.nodes[leader].id,
        failover.as_millis()
    );
    assert!(
        failover <= Duration::from_millis(LEASE_MS + GRACE_MS + 4 * HEARTBEAT_MS + 3_000),
        "failover took {failover:?}"
    );
    let follower = if leader == 1 { 2 } else { 1 };

    // Every acknowledged write survived the leader's death.
    let on_leader = cluster.claims(leader);
    let lost: Vec<&String> = acked.iter().filter(|id| !on_leader.contains(*id)).collect();
    assert!(lost.is_empty(), "acknowledged writes lost: {lost:?}");

    // Writes resume on the new leader (and are confirmed by the follower,
    // which now follows it).
    for i in 0..10 {
        let r = cluster.ingest(leader, &format!("post-{i}")).unwrap();
        assert_eq!(r.status, 200, "{}\n{}", r.body, cluster.logs());
    }
    cluster.wait_same_claims(follower, leader, Duration::from_secs(30));

    // The control plane moved the placement and bumped the term.
    let placement = cluster.cp_get("/v1/control-plane/placement?format=csv");
    assert_eq!(placement.status, 200);
    let leader_line = placement
        .body
        .lines()
        .find(|line| line.contains(",leader,"))
        .unwrap_or_default()
        .to_string();
    assert!(
        leader_line.contains(&cluster.nodes[leader].id) && leader_line.contains(",2,"),
        "placement: {}",
        placement.body
    );
    let status: Value = cluster.cp_get("/v1/control-plane/ingest").json();
    assert_eq!(status["term"], 2, "{status}");
    assert_eq!(status["leader_node_id"], cluster.nodes[leader].id.as_str());

    // The old leader restarts: it rejoins as a follower, refuses writes,
    // keeps its WAL aside and converges to the new leader.
    cluster.start_node(0);
    cluster.wait_ready(0, Duration::from_secs(30));
    assert_eq!(cluster.nodes[0].status("/v1/ready/leader"), Some(503));
    let refused = cluster.ingest(0, "to-the-old-leader").unwrap();
    assert_eq!(refused.status, 503, "{}", refused.body);
    assert_eq!(
        refused.header("x-dash-leader-url"),
        Some(cluster.nodes[leader].url().as_str())
    );
    cluster.wait_same_claims(0, leader, Duration::from_secs(30));
    let r = cluster.ingest(leader, "after-rejoin").unwrap();
    assert_eq!(r.status, 200, "{}", r.body);
    cluster.wait_same_claims(0, leader, Duration::from_secs(30));
    let deposed = std::fs::read_dir(cluster.dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .any(|e| {
            e.file_name()
                .to_string_lossy()
                .starts_with("n1.wal.deposed-t1-")
        });
    assert!(
        deposed,
        "the old leader's WAL is kept aside before the resync"
    );

    let metrics = cluster.cp_get("/metrics").body;
    assert_eq!(
        metric(&metrics, "dash_control_plane_ingest_term"),
        Some(2.0)
    );
    assert_eq!(
        metric(&metrics, "dash_control_plane_ingest_promotions_total"),
        Some(2.0),
        "bootstrap + one failover"
    );
}

#[cfg(unix)]
#[test]
fn a_paused_leader_that_resumes_cannot_write_and_rejoins_as_follower() {
    let mut cluster = Cluster::new();
    cluster.start_all();
    for i in 0..5 {
        assert_eq!(cluster.ingest(0, &format!("a-{i}")).unwrap().status, 200);
    }
    // Isolate n1 completely: it can neither heartbeat nor serve.
    let pid = cluster.nodes[0].proc.as_ref().unwrap().pid() as libc::pid_t;
    // SAFETY: plain signal delivery to our own child process.
    assert_eq!(unsafe { libc::kill(pid, libc::SIGSTOP) }, 0);
    let leader = cluster.wait_leader(&[1, 2], Duration::from_secs(30));
    let r = cluster.ingest(leader, "b-0").unwrap();
    assert_eq!(r.status, 200, "{}\n{}", r.body, cluster.logs());

    // n1 resumes believing nothing happened. Its lease ran out while it was
    // stopped (the monotonic clock kept going), so it refuses writes even
    // before it hears about the new term.
    // SAFETY: as above.
    assert_eq!(unsafe { libc::kill(pid, libc::SIGCONT) }, 0);
    let refused = cluster.ingest(0, "split-brain").unwrap();
    assert_eq!(refused.status, 503, "{}", refused.body);
    assert!(
        cluster.claims(leader).iter().all(|id| id != "split-brain")
            && cluster.claims(0).iter().all(|id| id != "split-brain"),
        "no node accepted the write"
    );
    // It learns the new term, follows the new leader and converges.
    cluster.wait_ready(0, Duration::from_secs(30));
    let end = Instant::now() + Duration::from_secs(30);
    loop {
        let r = cluster.ingest(0, "still-refused").unwrap();
        assert_eq!(r.status, 503, "{}", r.body);
        if r.header("x-dash-leader-url") == Some(cluster.nodes[leader].url().as_str()) {
            break;
        }
        assert!(Instant::now() < end, "n1 never learned the new leader");
        std::thread::sleep(Duration::from_millis(50));
    }
    assert_eq!(cluster.ingest(leader, "b-1").unwrap().status, 200);
    cluster.wait_same_claims(0, leader, Duration::from_secs(30));
    let claims = cluster.claims(0);
    assert!(claims.contains("a-4") && claims.contains("b-1"));
}
