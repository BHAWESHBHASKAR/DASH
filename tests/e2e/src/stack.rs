//! A complete ingestion (+ optional retrieval follower) deployment on
//! loopback, with real credentials and temp-dir storage.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use tempfile::TempDir;

use crate::http::{Client, Resp};
use crate::leader::LeaderState;
use crate::proc::{Proc, free_port, random_secret};

#[derive(Clone, Default)]
pub struct StackOpts {
    /// Tenants that get their own ingest and retrieve keys.
    /// Defaults to `["tenant-a", "tenant-b"]`.
    pub tenants: Vec<String>,
    /// `DASH_CHECKPOINT_MAX_WAL_RECORDS` for ingestion.
    pub checkpoint_every: Option<usize>,
    pub extra_ingest_env: Vec<(String, String)>,
    pub extra_retrieval_env: Vec<(String, String)>,
}

pub struct Stack {
    pub dir: TempDir,
    pub opts: StackOpts,
    pub ingest_port: u16,
    pub retrieval_port: u16,
    pub replication_token: String,
    /// tenant -> ingest key (roles: ingest, read_only; that tenant only).
    pub ingest_keys: BTreeMap<String, String>,
    /// tenant -> retrieve key (roles: retrieve, read_only; that tenant only).
    pub retrieve_keys: BTreeMap<String, String>,
    pub ingest: Option<Proc>,
    pub retrieval: Option<Proc>,
}

impl Stack {
    pub fn new(mut opts: StackOpts) -> Stack {
        if opts.tenants.is_empty() {
            opts.tenants = vec!["tenant-a".into(), "tenant-b".into()];
        }
        let ingest_keys = opts
            .tenants
            .iter()
            .map(|t| (t.clone(), random_secret()))
            .collect();
        let retrieve_keys = opts
            .tenants
            .iter()
            .map(|t| (t.clone(), random_secret()))
            .collect();
        Stack {
            dir: tempfile::Builder::new()
                .prefix("dash-e2e-")
                .tempdir()
                .unwrap(),
            opts,
            ingest_port: free_port(),
            retrieval_port: free_port(),
            replication_token: random_secret(),
            ingest_keys,
            retrieve_keys,
            ingest: None,
            retrieval: None,
        }
    }

    pub fn ingest_addr(&self) -> SocketAddr {
        format!("127.0.0.1:{}", self.ingest_port).parse().unwrap()
    }
    pub fn retrieval_addr(&self) -> SocketAddr {
        format!("127.0.0.1:{}", self.retrieval_port)
            .parse()
            .unwrap()
    }
    pub fn ic(&self) -> Client {
        Client::new(self.ingest_addr())
    }
    pub fn rc(&self) -> Client {
        Client::new(self.retrieval_addr())
    }
    pub fn path(&self, name: &str) -> PathBuf {
        self.dir.path().join(name)
    }

    fn scopes(keys: &BTreeMap<String, String>, roles: &str) -> String {
        keys.iter()
            .map(|(tenant, key)| format!("{key}:{tenant}:{roles}"))
            .collect::<Vec<_>>()
            .join(";")
    }

    pub fn ingest_env(&self) -> Vec<(String, String)> {
        let mut env = vec![
            (
                "DASH_INGEST_BIND".to_string(),
                self.ingest_addr().to_string(),
            ),
            (
                "DASH_INGEST_WAL_PATH".into(),
                self.path("ingest.wal").display().to_string(),
            ),
            (
                "DASH_INGEST_PERSISTENCE_PATH".into(),
                self.path("ingest.redb").display().to_string(),
            ),
            (
                "DASH_INGEST_API_KEY_SCOPES".into(),
                Self::scopes(&self.ingest_keys, "ingest,read_only"),
            ),
            (
                "DASH_INGEST_REPLICATION_TOKEN".into(),
                self.replication_token.clone(),
            ),
            // Lift the default per-tenant rate limit: only the rate-limit
            // scenario wants one.
            ("DASH_INGEST_RATE_LIMIT_PER_TENANT_RPS".into(), "0".into()),
        ];
        if let Some(n) = self.opts.checkpoint_every {
            env.push(("DASH_CHECKPOINT_MAX_WAL_RECORDS".into(), n.to_string()));
        }
        env.extend(self.opts.extra_ingest_env.clone());
        env
    }

    pub fn retrieval_env(&self) -> Vec<(String, String)> {
        let mut env = vec![
            (
                "DASH_RETRIEVAL_BIND".to_string(),
                self.retrieval_addr().to_string(),
            ),
            (
                "DASH_RETRIEVAL_WAL_PATH".into(),
                self.path("retrieval.wal").display().to_string(),
            ),
            (
                "DASH_RETRIEVAL_PERSISTENCE_PATH".into(),
                self.path("retrieval.redb").display().to_string(),
            ),
            (
                "DASH_RETRIEVAL_API_KEY_SCOPES".into(),
                Self::scopes(&self.retrieve_keys, "retrieve,read_only"),
            ),
            (
                "DASH_RETRIEVAL_REPLICATION_SOURCE_URL".into(),
                format!("http://{}", self.ingest_addr()),
            ),
            (
                "DASH_RETRIEVAL_REPLICATION_TOKEN".into(),
                self.replication_token.clone(),
            ),
            (
                "DASH_RETRIEVAL_REPLICATION_OFFSET_PATH".into(),
                self.path("retrieval.offset").display().to_string(),
            ),
            (
                "DASH_RETRIEVAL_REPLICATION_POLL_INTERVAL_MS".into(),
                "100".into(),
            ),
            (
                "DASH_RETRIEVAL_RATE_LIMIT_PER_TENANT_RPS".into(),
                "0".into(),
            ),
        ];
        env.extend(self.opts.extra_retrieval_env.clone());
        env
    }

    pub fn start_ingest(&mut self) {
        let env = self.ingest_env();
        let mut p = Proc::spawn(
            "ingestion",
            "ingestion",
            &[],
            &env,
            &self.path("ingestion.log"),
        );
        p.wait_live(self.ingest_addr(), "/live", Duration::from_secs(30));
        self.ingest = Some(p);
    }

    pub fn start_retrieval(&mut self) {
        let env = self.retrieval_env();
        let mut p = Proc::spawn(
            "retrieval",
            "retrieval",
            &[],
            &env,
            &self.path("retrieval.log"),
        );
        p.wait_live(self.retrieval_addr(), "/live", Duration::from_secs(30));
        self.retrieval = Some(p);
    }

    pub fn start_all(&mut self) {
        self.start_ingest();
        self.start_retrieval();
    }

    /// Restart ingestion; `graceful` sends SIGTERM, otherwise SIGKILL.
    pub fn restart_ingest(&mut self, graceful: bool) {
        if let Some(mut p) = self.ingest.take() {
            if graceful {
                assert!(
                    p.terminate(Duration::from_secs(15)),
                    "ingestion did not exit within 15s of SIGTERM\n{}",
                    p.log()
                );
            } else {
                p.kill9();
            }
        }
        self.start_ingest();
    }

    pub fn restart_retrieval(&mut self, graceful: bool) {
        if let Some(mut p) = self.retrieval.take() {
            if graceful {
                assert!(
                    p.terminate(Duration::from_secs(15)),
                    "retrieval did not exit within 15s of SIGTERM\n{}",
                    p.log()
                );
            } else {
                p.kill9();
            }
        }
        self.start_retrieval();
    }

    pub fn kill_ingest(&mut self) {
        if let Some(mut p) = self.ingest.take() {
            p.kill9();
        }
    }

    pub fn ik(&self, tenant: &str) -> (&'static str, String) {
        ("x-api-key", self.ingest_keys[tenant].clone())
    }
    pub fn rk(&self, tenant: &str) -> (&'static str, String) {
        ("x-api-key", self.retrieve_keys[tenant].clone())
    }

    /// POST /v1/ingest with the tenant's ingest key.
    pub fn ingest_as(&self, tenant: &str, body: &Value) -> Resp {
        let (k, v) = self.ik(tenant);
        self.ic().post_json("/v1/ingest", &[(k, v.as_str())], body)
    }

    pub fn try_ingest_as(&self, tenant: &str, body: &Value) -> std::io::Result<Resp> {
        let (k, v) = self.ik(tenant);
        self.ic()
            .try_post_json("/v1/ingest", &[(k, v.as_str())], body)
    }

    /// POST /v1/retrieve with the tenant's retrieve key.
    pub fn retrieve_as(&self, tenant: &str, body: &Value) -> Resp {
        let (k, v) = self.rk(tenant);
        self.rc()
            .post_json("/v1/retrieve", &[(k, v.as_str())], body)
    }

    /// Basic retrieve of up to `top_k` results.
    pub fn retrieve(&self, tenant: &str, query: &str, top_k: usize) -> Vec<Value> {
        let r = self.retrieve_as(
            tenant,
            &json!({"tenant_id": tenant, "query": query, "top_k": top_k}),
        );
        assert_eq!(r.status, 200, "retrieve failed: {}", r.body);
        r.json()["results"].as_array().cloned().unwrap_or_default()
    }

    /// Poll retrieval `/ready` until it reports a caught-up follower.
    pub fn wait_retrieval_ready(&self, timeout: Duration) {
        let end = Instant::now() + timeout;
        loop {
            if let Ok(r) = self.rc().request("GET", "/ready", &[], None)
                && r.status == 200
            {
                return;
            }
            assert!(
                Instant::now() < end,
                "retrieval never became ready\n{}",
                self.retrieval_log()
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    /// `replication` object from retrieval `/ready` (any status).
    pub fn retrieval_replication(&self) -> Value {
        let r = self.rc().get("/ready", &[]);
        r.json()["replication"].clone()
    }

    /// Wait until retrieval's replication offset equals the leader's WAL
    /// record count and lag is zero. Returns the final replication JSON.
    pub fn wait_caught_up(&self, timeout: Duration) -> Value {
        // Compared against the leader's live position, not the follower's
        // last-seen total (which is stale right after a write).
        let end = Instant::now() + timeout;
        loop {
            let (generation, total) = self.leader_position();
            let rep = self.retrieval_replication();
            if rep["generation"].as_u64() == Some(generation)
                && rep["offset"].as_u64() == Some(total as u64)
                && rep["lag_records"].as_u64() == Some(0)
            {
                return rep;
            }
            assert!(
                Instant::now() < end,
                "retrieval did not catch up to leader (generation={generation}, total={total}): {rep}\n{}",
                self.retrieval_log()
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    /// `(generation, total_records)` as reported by the leader's WAL frame.
    pub fn leader_position(&self) -> (u64, usize) {
        let r = self.ic().get(
            "/internal/replication/wal?from_offset=0&max_records=1",
            &[("x-replication-token", self.replication_token.as_str())],
        );
        assert_eq!(r.status, 200, "leader WAL frame: {}", r.body);
        let kv = |name: &str| -> String {
            r.body
                .lines()
                .find_map(|l| l.strip_prefix(&format!("{name}=")))
                .unwrap_or_else(|| panic!("frame lacks {name}: {}", r.body))
                .to_string()
        };
        (
            kv("generation").parse().unwrap(),
            kv("total_records").parse().unwrap(),
        )
    }

    /// The leader's durable state as exported for followers (snapshot plus
    /// WAL), reduced to ids. Claims and evidence are last-write-wins by id.
    pub fn leader_state(&self) -> LeaderState {
        let r = self.ic().get(
            "/internal/replication/export",
            &[("x-replication-token", self.replication_token.as_str())],
        );
        assert_eq!(r.status, 200, "leader export: {}", r.body);
        LeaderState::parse(&r.body)
    }

    /// Poll retrieval until `claim_id` is visible for `tenant`.
    pub fn wait_claim_visible(
        &self,
        tenant: &str,
        query: &str,
        claim_id: &str,
        timeout: Duration,
    ) -> Value {
        let end = Instant::now() + timeout;
        loop {
            let results = self.retrieve(tenant, query, 50);
            if let Some(r) = results.iter().find(|r| r["claim_id"] == claim_id) {
                return r.clone();
            }
            assert!(
                Instant::now() < end,
                "claim {claim_id} not visible in retrieval for {tenant}; results={results:?}\n{}",
                self.retrieval_log()
            );
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    pub fn ingest_log(&self) -> String {
        self.ingest.as_ref().map(|p| p.log()).unwrap_or_default()
    }
    pub fn retrieval_log(&self) -> String {
        self.retrieval.as_ref().map(|p| p.log()).unwrap_or_default()
    }

    /// claim_id -> sorted evidence ids, for every claim retrieval returns for
    /// `query` (callers pick a query that matches all of their claims).
    pub fn claim_evidence_map(
        &self,
        tenant: &str,
        query: &str,
        top_k: usize,
    ) -> BTreeMap<String, Vec<String>> {
        self.retrieve(tenant, query, top_k)
            .into_iter()
            .map(|r| {
                let mut ev: Vec<String> = r["citations"]
                    .as_array()
                    .map(|a| {
                        a.iter()
                            .map(|c| c["evidence_id"].as_str().unwrap_or_default().to_string())
                            .collect()
                    })
                    .unwrap_or_default();
                ev.sort();
                (r["claim_id"].as_str().unwrap_or_default().to_string(), ev)
            })
            .collect()
    }
}
