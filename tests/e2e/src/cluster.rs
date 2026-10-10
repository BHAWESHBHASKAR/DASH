//! A failover cluster on loopback: one control plane coordinating
//! automatic failover (ADR 0006) and three ingestion nodes, `n1` (the
//! writer of the new cluster) and its followers `n2` and `n3`. Used by the
//! failover scenario and by `crash-test --failover`.

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use crate::bundle;
use crate::http::{Client, Resp};
use crate::leader::LeaderState;
use crate::proc::{Proc, free_port, random_secret};

/// The tenant the cluster's ingest key is scoped to.
pub const TENANT: &str = "tenant-a";

/// Timing and replication settings of a [`Cluster`].
#[derive(Debug, Clone)]
pub struct ClusterOpts {
    pub lease_ms: u64,
    pub grace_ms: u64,
    pub heartbeat_ms: u64,
    /// `DASH_INGEST_MIN_SYNC_REPLICAS` on every node.
    pub min_sync_replicas: usize,
    pub extra_node_env: Vec<(String, String)>,
}

impl Default for ClusterOpts {
    fn default() -> Self {
        Self {
            lease_ms: 2_000,
            grace_ms: 500,
            heartbeat_ms: 200,
            min_sync_replicas: 1,
            extra_node_env: Vec::new(),
        }
    }
}

pub struct Node {
    pub id: String,
    pub port: u16,
    pub proc: Option<Proc>,
}

impl Node {
    pub fn addr(&self) -> SocketAddr {
        format!("127.0.0.1:{}", self.port).parse().unwrap()
    }
    pub fn url(&self) -> String {
        format!("http://{}", self.addr())
    }
    pub fn client(&self) -> Client {
        Client::new(self.addr())
    }
    pub fn status(&self, path: &str) -> Option<u16> {
        let mut probe = self.client();
        probe.timeout = Duration::from_secs(2);
        probe.request("GET", path, &[], None).ok().map(|r| r.status)
    }
    pub fn log(&self) -> String {
        self.proc.as_ref().map(|p| p.log()).unwrap_or_default()
    }
}

pub struct Cluster {
    pub dir: tempfile::TempDir,
    pub opts: ClusterOpts,
    /// Control-plane bearer token (also the nodes' heartbeat credential).
    pub token: String,
    pub replication_token: String,
    /// Ingest key of [`TENANT`].
    pub ingest_key: String,
    pub cp_port: u16,
    pub cp: Option<Proc>,
    pub nodes: Vec<Node>,
}

impl Cluster {
    pub fn new(opts: ClusterOpts) -> Cluster {
        let ids = ["n1", "n2", "n3"];
        Cluster {
            dir: tempfile::Builder::new()
                .prefix("dash-e2e-failover-")
                .tempdir()
                .unwrap(),
            opts,
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

    pub fn path(&self, name: &str) -> PathBuf {
        self.dir.path().join(name)
    }

    pub fn cp_addr(&self) -> SocketAddr {
        format!("127.0.0.1:{}", self.cp_port).parse().unwrap()
    }

    pub fn start_control_plane(&mut self) {
        let placement = self.path("placement.seed.csv");
        std::fs::write(
            &placement,
            format!(
                "{TENANT},0,1,n1,leader,healthy\n{TENANT},0,1,n2,follower,healthy\n{TENANT},0,1,n3,follower,healthy\n"
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
                self.opts.lease_ms.to_string(),
            ),
            (
                "DASH_CONTROL_PLANE_INGEST_PROMOTION_GRACE_MS".into(),
                self.opts.grace_ms.to_string(),
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

    pub fn node_env(&self, index: usize) -> Vec<(String, String)> {
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
                format!("{}:{TENANT}:ingest,read_only", self.ingest_key),
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
                self.opts.heartbeat_ms.to_string(),
            ),
            ("DASH_CONTROL_PLANE_TOKEN".into(), self.token.clone()),
            (
                "DASH_INGEST_MIN_SYNC_REPLICAS".into(),
                self.opts.min_sync_replicas.to_string(),
            ),
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
        env.extend(self.opts.extra_node_env.clone());
        if index > 0 {
            env.push((
                "DASH_INGEST_REPLICATION_SOURCE_URL".into(),
                self.nodes[0].url(),
            ));
        }
        env
    }

    pub fn start_node(&mut self, index: usize) {
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

    pub fn logs(&self) -> String {
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
    pub fn wait_leader(&self, candidates: &[usize], timeout: Duration) -> usize {
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

    pub fn wait_ready(&self, index: usize, timeout: Duration) {
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

    pub fn ingest(&self, index: usize, claim_id: &str) -> std::io::Result<Resp> {
        self.nodes[index].client().try_post_json(
            "/v1/ingest",
            &[("x-api-key", self.ingest_key.as_str())],
            &bundle(TENANT, claim_id, &format!("failover claim {claim_id}"), 1),
        )
    }

    /// Claim ids in the node's durable state (its replication export).
    pub fn claims(&self, index: usize) -> BTreeSet<String> {
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

    pub fn wait_same_claims(&self, follower: usize, leader: usize, timeout: Duration) {
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

    pub fn cp_get(&self, path: &str) -> Resp {
        Client::new(self.cp_addr()).get(
            path,
            &[("Authorization", &format!("Bearer {}", self.token))],
        )
    }

    pub fn start_all(&mut self) -> usize {
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
