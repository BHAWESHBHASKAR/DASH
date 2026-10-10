//! Ingestion side of automatic leader failover (ADR 0006).
//!
//! With `DASH_INGEST_FAILOVER_CONTROL_PLANE_URL` set, this node is a member
//! of an ingestion cluster whose leader the control plane names. It starts
//! in role `unknown` and accepts no writes until the control plane says it
//! is the leader. Every heartbeat answer that says "leader" extends a lease
//! measured on this process' monotonic clock from *before* the heartbeat
//! was sent; writes are refused once it lapses. A node told to follow while
//! it was leader demotes itself: it stops writing, keeps a copy of its WAL
//! (the only place an unreplicated write survives), resyncs from the new
//! leader and follows it.
//!
//! Synchronous replication (`DASH_INGEST_MIN_SYNC_REPLICAS`) is in
//! [`ReplicaProgress`]: a write is answered only after enough promotable
//! followers polled from a WAL position at or after it.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

use metadata_router::{PlacementSourceOptions, control_plane_post};

use super::http::HttpResponse;
use super::replication::ReplicationPullConfig;
use super::{IngestionRuntime, SharedRuntime, group_commit, lock_wal};

pub(crate) const DEFAULT_HEARTBEAT_INTERVAL_MS: u64 = 1_000;
const DEFAULT_SYNC_TIMEOUT_MS: u64 = 5_000;
/// Longest a caught-up follower's poll is held open (`wait_ms`).
pub(crate) const MAX_LONG_POLL_MS: u64 = 1_000;
/// Checkpoint transitions a node reports with its own WAL position.
const HEARTBEAT_CHAIN_MAX: usize = 8;

// ---------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FailoverConfig {
    pub(crate) control_plane_url: String,
    pub(crate) advertise_url: String,
    pub(crate) node_id: String,
    pub(crate) heartbeat_interval: Duration,
    /// No static replication source is configured: this node may be chosen
    /// as the first leader of a new cluster.
    pub(crate) bootstrap: bool,
}

fn env_non_empty(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

impl FailoverConfig {
    pub(crate) fn from_env() -> Result<Option<Self>, String> {
        let Some(control_plane_url) = env_non_empty("DASH_INGEST_FAILOVER_CONTROL_PLANE_URL")
        else {
            return Ok(None);
        };
        let node_id = env_non_empty("DASH_ROUTER_LOCAL_NODE_ID")
            .or_else(|| env_non_empty("DASH_NODE_ID"))
            .ok_or_else(|| {
                "DASH_INGEST_FAILOVER_CONTROL_PLANE_URL is set, but DASH_NODE_ID (or \
                 DASH_ROUTER_LOCAL_NODE_ID) is not: every failover member needs a unique node id"
                    .to_string()
            })?;
        let advertise_url = env_non_empty("DASH_INGEST_FAILOVER_ADVERTISE_URL").ok_or_else(|| {
            "DASH_INGEST_FAILOVER_CONTROL_PLANE_URL is set, but DASH_INGEST_FAILOVER_ADVERTISE_URL \
             (the base URL other nodes use to reach this one) is not"
                .to_string()
        })?;
        Self::new(
            control_plane_url,
            advertise_url,
            node_id,
            env_non_empty("DASH_INGEST_FAILOVER_HEARTBEAT_INTERVAL_MS")
                .map(|raw| {
                    raw.parse::<u64>().ok().filter(|ms| *ms > 0).ok_or_else(|| {
                        "DASH_INGEST_FAILOVER_HEARTBEAT_INTERVAL_MS must be a positive integer"
                            .to_string()
                    })
                })
                .transpose()?
                .unwrap_or(DEFAULT_HEARTBEAT_INTERVAL_MS),
            env_non_empty("DASH_INGEST_REPLICATION_SOURCE_URL").is_none(),
        )
        .map(Some)
    }

    pub(crate) fn new(
        control_plane_url: String,
        advertise_url: String,
        node_id: String,
        heartbeat_interval_ms: u64,
        bootstrap: bool,
    ) -> Result<Self, String> {
        let advertise_url = advertise_url.trim_end_matches('/').to_string();
        if !(advertise_url.starts_with("http://") || advertise_url.starts_with("https://"))
            || advertise_url.contains(char::is_whitespace)
        {
            return Err(
                "DASH_INGEST_FAILOVER_ADVERTISE_URL must be an http:// or https:// base URL"
                    .to_string(),
            );
        }
        if node_id.contains([',', '\n', '\r', ' ']) {
            return Err("the failover node id must not contain ',', spaces or newlines".into());
        }
        Ok(Self {
            control_plane_url: control_plane_url.trim_end_matches('/').to_string(),
            advertise_url,
            node_id,
            heartbeat_interval: Duration::from_millis(heartbeat_interval_ms.max(1)),
            bootstrap,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SyncTimeoutPolicy {
    /// Answer 503 `sync_replication_timeout`; never acknowledge without the
    /// required confirmations.
    Fail,
    /// Answer 200 with `commit_status: sync_degraded`.
    Degrade,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SyncReplicationConfig {
    pub(crate) min_replicas: usize,
    pub(crate) timeout: Duration,
    pub(crate) on_timeout: SyncTimeoutPolicy,
}

impl Default for SyncReplicationConfig {
    fn default() -> Self {
        Self {
            min_replicas: 0,
            timeout: Duration::from_millis(DEFAULT_SYNC_TIMEOUT_MS),
            on_timeout: SyncTimeoutPolicy::Fail,
        }
    }
}

impl SyncReplicationConfig {
    pub(crate) fn from_env() -> Result<Self, String> {
        let mut config = Self::default();
        if let Some(raw) = env_non_empty("DASH_INGEST_MIN_SYNC_REPLICAS") {
            config.min_replicas = raw.parse::<usize>().map_err(|_| {
                "DASH_INGEST_MIN_SYNC_REPLICAS must be a non-negative integer".to_string()
            })?;
        }
        if let Some(raw) = env_non_empty("DASH_INGEST_SYNC_REPLICATION_TIMEOUT_MS") {
            config.timeout = raw
                .parse::<u64>()
                .ok()
                .filter(|ms| *ms > 0)
                .map(Duration::from_millis)
                .ok_or_else(|| {
                    "DASH_INGEST_SYNC_REPLICATION_TIMEOUT_MS must be a positive integer".to_string()
                })?;
        }
        if let Some(raw) = env_non_empty("DASH_INGEST_SYNC_REPLICATION_ON_TIMEOUT") {
            config.on_timeout = match raw.to_ascii_lowercase().as_str() {
                "fail" => SyncTimeoutPolicy::Fail,
                "degrade" => SyncTimeoutPolicy::Degrade,
                _ => {
                    return Err(
                        "DASH_INGEST_SYNC_REPLICATION_ON_TIMEOUT must be 'fail' or 'degrade'"
                            .to_string(),
                    );
                }
            };
        }
        Ok(config)
    }

    pub(crate) fn enabled(&self) -> bool {
        self.min_replicas > 0
    }
}

// ---------------------------------------------------------------------
// Role state (kept on the runtime, under its lock)
// ---------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NodeRole {
    Unknown,
    Leader,
    Follower,
}

impl NodeRole {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Leader => "leader",
            Self::Follower => "follower",
        }
    }
}

/// Why this node refuses a write, with what it knows about the leader.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NotLeader {
    pub(crate) reason: &'static str,
    pub(crate) leader_node_id: Option<String>,
    pub(crate) leader_url: Option<String>,
    pub(crate) term: u64,
}

impl NotLeader {
    pub(crate) fn message(&self) -> String {
        format!(
            "not_leader: this ingestion node does not accept writes ({}); {}",
            self.reason,
            match (&self.leader_node_id, &self.leader_url) {
                (Some(node), Some(url)) => format!("the leader is '{node}' at {url}"),
                _ => "no leader is known yet, retry shortly".to_string(),
            }
        )
    }

    /// 503 with `Retry-After` and the leader hint headers.
    pub(crate) fn response(&self) -> HttpResponse {
        let mut response = HttpResponse::service_unavailable(&self.message());
        response.retry_after_secs = Some(1);
        response
            .headers
            .push(("X-Dash-Leader", "false".to_string()));
        response
            .headers
            .push(("X-Dash-Term", self.term.to_string()));
        if let Some(node) = self.leader_node_id.as_ref().filter(|v| header_safe(v)) {
            response.headers.push(("X-Dash-Leader-Node", node.clone()));
        }
        if let Some(url) = self.leader_url.as_ref().filter(|v| header_safe(v)) {
            response.headers.push(("X-Dash-Leader-Url", url.clone()));
        }
        response
    }
}

fn header_safe(value: &str) -> bool {
    !value.chars().any(|c| c.is_control())
}

#[derive(Debug)]
pub(crate) struct FailoverState {
    pub(crate) enabled: bool,
    pub(crate) node_id: String,
    pub(crate) instance_id: String,
    pub(crate) advertise_url: String,
    pub(crate) bootstrap: bool,
    pub(crate) role: NodeRole,
    pub(crate) term: u64,
    pub(crate) leader_node_id: Option<String>,
    pub(crate) leader_url: Option<String>,
    /// Writes are accepted while `now < lease_until` (leader only).
    pub(crate) lease_until: Option<Instant>,
    /// `<wal>.failover`: the highest term seen, kept across restarts.
    pub(crate) state_path: Option<PathBuf>,
    /// The checkpoint transition this follower crossed last
    /// (`switch_from`), reported so the control plane can order a
    /// generation the leader had no time to report.
    pub(crate) last_switch: Option<(store::WalPosition, u64)>,
    pub(crate) promotions_total: u64,
    pub(crate) demotions_total: u64,
    pub(crate) promotion_failures_total: u64,
    /// The last promotion attempt failed: report `synced=0` so the control
    /// plane picks someone else once the lease lapses.
    pub(crate) promotion_refused: bool,
    pub(crate) heartbeat_success_total: u64,
    pub(crate) heartbeat_failure_total: u64,
    pub(crate) last_heartbeat_error: Option<String>,
    pub(crate) stale_term_frames_total: u64,
    pub(crate) last_heartbeat_ok: Option<Instant>,
}

impl Default for FailoverState {
    fn default() -> Self {
        Self {
            enabled: false,
            node_id: String::new(),
            instance_id: String::new(),
            advertise_url: String::new(),
            bootstrap: false,
            role: NodeRole::Leader,
            term: 0,
            leader_node_id: None,
            leader_url: None,
            lease_until: None,
            state_path: None,
            last_switch: None,
            promotions_total: 0,
            demotions_total: 0,
            promotion_failures_total: 0,
            promotion_refused: false,
            heartbeat_success_total: 0,
            heartbeat_failure_total: 0,
            last_heartbeat_error: None,
            stale_term_frames_total: 0,
            last_heartbeat_ok: None,
        }
    }
}

impl FailoverState {
    /// A member that has not heard from the control plane yet.
    pub(crate) fn member(config: &FailoverConfig, state_path: Option<PathBuf>) -> Self {
        let term = state_path.as_deref().map(read_term).unwrap_or(0);
        Self {
            enabled: true,
            node_id: config.node_id.clone(),
            instance_id: dash_common::random_hex(8),
            advertise_url: config.advertise_url.clone(),
            bootstrap: config.bootstrap,
            role: NodeRole::Unknown,
            term,
            state_path,
            ..Self::default()
        }
    }

    /// `Ok` when this node may accept a write at `now`.
    pub(crate) fn check_write(&self, now: Instant) -> Result<(), NotLeader> {
        if !self.enabled {
            return Ok(());
        }
        let reason = match (self.role, self.lease_until) {
            (NodeRole::Leader, Some(until)) if now < until => return Ok(()),
            (NodeRole::Leader, _) => "leader_lease_expired",
            (NodeRole::Follower, _) => "follower",
            (NodeRole::Unknown, _) => "no_leader_assigned",
        };
        Err(NotLeader {
            reason,
            leader_node_id: self.leader_node_id.clone(),
            leader_url: self.leader_url.clone(),
            term: self.term,
        })
    }

    pub(crate) fn is_leader(&self) -> bool {
        self.enabled && self.role == NodeRole::Leader
    }

    /// Where a failover follower pulls from (`None`: nowhere right now).
    pub(crate) fn pull_source(&self) -> Option<&str> {
        if self.role == NodeRole::Leader {
            return None;
        }
        self.leader_url.as_deref()
    }

    /// A replication poll carried a term newer than ours: another node was
    /// made leader. Stop accepting writes now, before the control plane
    /// tells us whom to follow.
    pub(crate) fn observe_newer_term(&mut self, term: u64) -> bool {
        if !self.enabled || term <= self.term {
            return false;
        }
        if self.role == NodeRole::Leader {
            eprintln!(
                "ingestion failover: a follower reported term {term} (ours is {}); this node was deposed and stops accepting writes",
                self.term
            );
            self.lease_until = None;
        }
        true
    }

    fn persist_term(&self) {
        let Some(path) = self.state_path.as_deref() else {
            return;
        };
        if let Err(err) = write_term(path, self.term) {
            eprintln!(
                "ingestion failover: could not persist term {}: {err}",
                self.term
            );
        }
    }

    pub(crate) fn ready_json(&self, now: Instant) -> String {
        let lease_ms = self
            .lease_until
            .map(|until| until.saturating_duration_since(now).as_millis().to_string())
            .unwrap_or_else(|| "null".to_string());
        let opt = |value: &Option<String>| {
            value
                .as_deref()
                .map(|v| format!("\"{}\"", super::json::json_escape(v)))
                .unwrap_or_else(|| "null".to_string())
        };
        format!(
            "{{\"node_id\":\"{}\",\"role\":\"{}\",\"term\":{},\"accepts_writes\":{},\"lease_remaining_ms\":{},\"leader_node_id\":{},\"leader_url\":{},\"last_heartbeat_error\":{}}}",
            super::json::json_escape(&self.node_id),
            self.role.as_str(),
            self.term,
            self.check_write(now).is_ok(),
            lease_ms,
            opt(&self.leader_node_id),
            opt(&self.leader_url),
            opt(&self.last_heartbeat_error),
        )
    }

    pub(crate) fn metrics_text(&self, now: Instant) -> String {
        if !self.enabled {
            return String::new();
        }
        let role = |r: NodeRole| u8::from(self.role == r);
        format!(
            "# HELP dash_ingest_failover_role 1 for this node's current failover role.\n\
# TYPE dash_ingest_failover_role gauge\n\
dash_ingest_failover_role{{role=\"leader\"}} {}\n\
dash_ingest_failover_role{{role=\"follower\"}} {}\n\
dash_ingest_failover_role{{role=\"unknown\"}} {}\n\
# HELP dash_ingest_failover_term Highest ingestion leader term this node has seen.\n\
# TYPE dash_ingest_failover_term gauge\n\
dash_ingest_failover_term {}\n\
# HELP dash_ingest_failover_accepts_writes 1 while this node holds a valid leader lease.\n\
# TYPE dash_ingest_failover_accepts_writes gauge\n\
dash_ingest_failover_accepts_writes {}\n\
# HELP dash_ingest_failover_promotions_total Times this node became leader.\n\
# TYPE dash_ingest_failover_promotions_total counter\n\
dash_ingest_failover_promotions_total {}\n\
# HELP dash_ingest_failover_demotions_total Times this node was deposed while leader.\n\
# TYPE dash_ingest_failover_demotions_total counter\n\
dash_ingest_failover_demotions_total {}\n\
# HELP dash_ingest_failover_promotion_failures_total Promotions this node could not carry out.\n\
# TYPE dash_ingest_failover_promotion_failures_total counter\n\
dash_ingest_failover_promotion_failures_total {}\n\
# HELP dash_ingest_failover_heartbeat_failures_total Heartbeats to the control plane that failed.\n\
# TYPE dash_ingest_failover_heartbeat_failures_total counter\n\
dash_ingest_failover_heartbeat_failures_total {}\n\
# HELP dash_ingest_failover_stale_term_frames_total Replication frames refused because they came from a deposed leader.\n\
# TYPE dash_ingest_failover_stale_term_frames_total counter\n\
dash_ingest_failover_stale_term_frames_total {}\n",
            role(NodeRole::Leader),
            role(NodeRole::Follower),
            role(NodeRole::Unknown),
            self.term,
            u8::from(self.check_write(now).is_ok()),
            self.promotions_total,
            self.demotions_total,
            self.promotion_failures_total,
            self.heartbeat_failure_total,
            self.stale_term_frames_total,
        )
    }
}

/// `<wal>.failover`.
pub(crate) fn state_path_for_wal(wal_path: &Path) -> PathBuf {
    let mut name = wal_path.as_os_str().to_owned();
    name.push(".failover");
    PathBuf::from(name)
}

fn read_term(path: &Path) -> u64 {
    std::fs::read_to_string(path)
        .ok()
        .and_then(|text| {
            text.lines()
                .find_map(|line| line.strip_prefix("term="))
                .and_then(|value| value.trim().parse::<u64>().ok())
        })
        .unwrap_or(0)
}

fn write_term(path: &Path, term: u64) -> std::io::Result<()> {
    use std::io::Write;
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".tmp");
    let tmp = PathBuf::from(tmp);
    {
        let mut file = std::fs::File::create(&tmp)?;
        file.write_all(format!("term={term}\n").as_bytes())?;
        file.sync_all()?;
    }
    std::fs::rename(&tmp, path)?;
    if let Some(dir) = path.parent().filter(|dir| !dir.as_os_str().is_empty()) {
        std::fs::File::open(dir)?.sync_all()?;
    }
    Ok(())
}

// ---------------------------------------------------------------------
// Heartbeat
// ---------------------------------------------------------------------

/// The control plane's answer to a heartbeat.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HeartbeatReply {
    pub(crate) term: u64,
    pub(crate) leader: bool,
    pub(crate) leader_node_id: Option<String>,
    pub(crate) leader_url: Option<String>,
    pub(crate) lease: Duration,
}

pub(crate) fn parse_heartbeat_reply(body: &str) -> Result<HeartbeatReply, String> {
    let value: serde_json::Value =
        serde_json::from_str(body).map_err(|_| "heartbeat reply is not JSON".to_string())?;
    let text = |key: &str| {
        value[key]
            .as_str()
            .map(str::to_string)
            .filter(|v| !v.is_empty())
    };
    let term = value["term"]
        .as_u64()
        .ok_or_else(|| "heartbeat reply has no term".to_string())?;
    let leader = match value["role"].as_str() {
        Some("leader") => true,
        Some("follower") => false,
        _ => return Err("heartbeat reply has no role".to_string()),
    };
    let lease_ms = value["lease_ms"]
        .as_u64()
        .filter(|ms| *ms > 0)
        .ok_or_else(|| "heartbeat reply has no lease_ms".to_string())?;
    Ok(HeartbeatReply {
        term,
        leader,
        leader_node_id: text("leader_node_id"),
        leader_url: text("leader_url"),
        lease: Duration::from_millis(lease_ms),
    })
}

fn url_encode(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for byte in raw.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char)
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

impl IngestionRuntime {
    /// Query string of this node's heartbeat.
    pub(crate) fn failover_heartbeat_query(&mut self, pull: &ReplicationPullConfig) -> String {
        // Load the follower cursor (if any) so the position reported is
        // the one this node would continue from.
        let _ = self.replication_cursor(pull);
        let follower = &self.replication_follower;
        // A follower reports its cursor and the crossing it made last (with
        // the term of the leader that served it); a node on its own WAL
        // reports that WAL and its recent checkpoint chain.
        let mut chain = Vec::new();
        let (position, prev, synced) = match follower.generation {
            Some(generation) if self.failover.role != NodeRole::Leader => (
                Some((generation, self.replication_last_offset as u64)),
                self.failover
                    .last_switch
                    .map(|(p, term)| (p.generation, p.records as u64, term)),
                follower.synced_once && !follower.force_resync && follower.blocked_reason.is_none(),
            ),
            _ => match self.wal.as_ref() {
                Some(wal) => {
                    let mut wal = lock_wal(wal);
                    match wal.replication_position() {
                        Ok((generation, records)) => {
                            let transitions = wal.generation_transitions();
                            let skip = transitions.len().saturating_sub(HEARTBEAT_CHAIN_MAX);
                            chain = transitions[skip..]
                                .iter()
                                .map(|t| (t.from_generation, t.from_records, t.to_generation))
                                .collect();
                            (Some((generation, records as u64)), None, true)
                        }
                        Err(_) => (None, None, false),
                    }
                }
                None => (Some((0, 0)), None, true),
            },
        };
        let synced = synced && !self.failover.promotion_refused;
        let state = &self.failover;
        let mut query = format!(
            "node_id={}&url={}&instance={}&term={}&role={}&synced={}&bootstrap={}",
            url_encode(&state.node_id),
            url_encode(&state.advertise_url),
            url_encode(&state.instance_id),
            state.term,
            state.role.as_str(),
            u8::from(synced),
            u8::from(state.bootstrap),
        );
        if let Some((generation, records)) = position {
            query.push_str(&format!("&generation={generation}&records={records}"));
        }
        if let Some((generation, records, term)) = prev {
            query.push_str(&format!(
                "&prev_generation={generation}&prev_records={records}&prev_term={term}"
            ));
        }
        if !chain.is_empty() {
            let chain = chain
                .iter()
                .map(|(from, records, to)| format!("{from}:{records}:{to}"))
                .collect::<Vec<_>>()
                .join(",");
            query.push_str(&format!("&chain={}", url_encode(&chain)));
        }
        query
    }

    /// Become leader at `term`. Called with the runtime drained (no write
    /// in flight). A follower first fences its lineage: it names its WAL
    /// after the old leader's generation and checkpoints, so the old
    /// leader's other followers either cross to it with a generation switch
    /// or, if they hold records it never received, resync.
    pub(crate) fn promote_to_leader(
        &mut self,
        term: u64,
        lease_until: Instant,
    ) -> Result<(), String> {
        if let Some(generation) = self.replication_follower.generation {
            if self.replication_follower.force_resync {
                return Err("the follower state needs a full resync".to_string());
            }
            if let Some(wal) = self.wal.clone() {
                let mut wal = lock_wal(&wal);
                let records = wal
                    .wal_record_count()
                    .map_err(|err| format!("cannot count WAL records: {err:?}"))?;
                if records != self.replication_last_offset {
                    return Err(format!(
                        "the WAL holds {records} records but the replication cursor says {}",
                        self.replication_last_offset
                    ));
                }
                wal.adopt_generation(generation)
                    .map_err(|err| format!("cannot adopt generation {generation}: {err:?}"))?;
                self.store
                    .checkpoint_and_compact(&mut wal)
                    .map_err(|err| format!("fencing checkpoint failed: {err:?}"))?;
                drop(wal);
                if let Some(persistence) = self.vector_index_persistence.as_ref() {
                    persistence.request_save();
                }
            }
            self.clear_replication_state()
                .map_err(|err| format!("cannot remove the follower cursor: {err:?}"))?;
        }
        self.replication_follower.enabled = false;
        self.replication_follower.generation = None;
        self.replication_follower.force_resync = false;
        self.replication_follower.blocked_reason = None;
        self.replication_last_offset = 0;
        let state = &mut self.failover;
        state.role = NodeRole::Leader;
        state.term = term;
        state.lease_until = Some(lease_until);
        state.leader_node_id = Some(state.node_id.clone());
        state.leader_url = Some(state.advertise_url.clone());
        state.last_switch = None;
        state.promotion_refused = false;
        state.promotions_total = state.promotions_total.saturating_add(1);
        state.persist_term();
        self.request_placement_reload();
        eprintln!("ingestion failover: this node is now the leader (term {term})");
        Ok(())
    }

    /// Stop being leader (or stop waiting to be one) and follow
    /// `leader_url`. A node that was leader keeps a copy of its WAL and
    /// resyncs from the new leader, discarding records it never shipped.
    pub(crate) fn demote_to_follower(&mut self, term: u64) {
        let was_leader = self.failover.role == NodeRole::Leader;
        let old_term = self.failover.term;
        self.failover.role = NodeRole::Follower;
        self.failover.lease_until = None;
        self.failover.term = self.failover.term.max(term);
        self.failover.persist_term();
        self.replication_follower.enabled = true;
        if was_leader {
            self.failover.demotions_total = self.failover.demotions_total.saturating_add(1);
        }
        // A node without a follower cursor holds its own history (it was
        // leader, in this process or before a restart): whatever it never
        // shipped is discarded by the resync, so keep a copy first.
        let own_history = self.replication_follower.generation.is_none();
        let populated = self
            .wal
            .as_ref()
            .and_then(|wal| lock_wal(wal).wal_record_count().ok())
            .is_some_and(|records| records > 0);
        if !(was_leader || own_history) {
            return;
        }
        if populated && let Some(wal) = self.wal.as_ref() {
            let mut wal = lock_wal(wal);
            let _ = wal.flush_pending_sync();
            let source = wal.path().to_path_buf();
            drop(wal);
            let stamp = super::unix_timestamp_millis();
            let mut copy = source.as_os_str().to_owned();
            copy.push(format!(".deposed-t{old_term}-{stamp}"));
            match std::fs::copy(&source, &copy) {
                Ok(_) => eprintln!(
                    "ingestion failover: deposed at term {old_term}; WAL copied to '{}' before resync",
                    PathBuf::from(&copy).display()
                ),
                Err(err) => eprintln!(
                    "ingestion failover: deposed at term {old_term}; could not copy the WAL aside: {err}"
                ),
            }
        }
        self.replication_follower.synced_once = false;
        self.replication_follower.started = Instant::now();
        self.replication_follower.force_resync = true;
        self.replication_follower.generation = None;
        let leader = self
            .failover
            .leader_url
            .as_deref()
            .unwrap_or("no known leader yet");
        if was_leader {
            eprintln!(
                "ingestion failover: this node was deposed (term {old_term} -> {}); following {leader}",
                self.failover.term
            );
        } else {
            eprintln!(
                "ingestion failover: following {leader} (term {}); local data is replaced by a resync",
                self.failover.term
            );
        }
    }

    /// Apply a heartbeat answer obtained by a heartbeat sent at `sent_at`.
    pub(crate) fn apply_heartbeat_reply(&mut self, reply: &HeartbeatReply, sent_at: Instant) {
        self.failover.heartbeat_success_total =
            self.failover.heartbeat_success_total.saturating_add(1);
        self.failover.last_heartbeat_error = None;
        self.failover.last_heartbeat_ok = Some(Instant::now());
        if reply.term < self.failover.term {
            // The control plane adopts the highest reported term before it
            // answers, so this is a stale answer: ignore it.
            return;
        }
        let lease_until = sent_at + reply.lease;
        if reply.leader {
            if self.failover.role == NodeRole::Leader && self.failover.term == reply.term {
                self.failover.lease_until = Some(lease_until);
                return;
            }
            if self.failover.role == NodeRole::Leader {
                // Re-elected (same process) at a newer term.
                self.failover.term = reply.term;
                self.failover.lease_until = Some(lease_until);
                self.failover.persist_term();
                return;
            }
            if let Err(err) = self.promote_to_leader(reply.term, lease_until) {
                self.failover.promotion_failures_total =
                    self.failover.promotion_failures_total.saturating_add(1);
                self.failover.promotion_refused = true;
                self.failover.term = self.failover.term.max(reply.term);
                eprintln!(
                    "ingestion failover: refusing promotion at term {}: {err}",
                    reply.term
                );
            }
            return;
        }
        let self_named = reply.leader_node_id.as_deref() == Some(self.failover.node_id.as_str());
        if self_named {
            // The control plane still lists this node (another instance of
            // it) as leader: wait for that lease to lapse.
            self.failover.leader_node_id = None;
            self.failover.leader_url = None;
        } else {
            if self.failover.role == NodeRole::Follower
                && reply.leader_url.is_some()
                && reply.leader_url != self.failover.leader_url
            {
                eprintln!(
                    "ingestion failover: following the new leader {} at {} (term {})",
                    reply.leader_node_id.as_deref().unwrap_or("?"),
                    reply.leader_url.as_deref().unwrap_or("?"),
                    reply.term
                );
            }
            self.failover.leader_node_id = reply.leader_node_id.clone();
            self.failover.leader_url = reply.leader_url.clone();
        }
        match self.failover.role {
            NodeRole::Leader => self.demote_to_follower(reply.term),
            NodeRole::Unknown if self.failover.leader_url.is_some() => {
                self.demote_to_follower(reply.term);
            }
            _ => {
                if reply.term > self.failover.term {
                    self.failover.term = reply.term;
                    self.failover.persist_term();
                }
            }
        }
    }

    /// The pull settings for the next tick: the static ones without
    /// failover; with failover the current leader as source and this
    /// node's term (`None` while leader or while no leader is known).
    pub(crate) fn pull_config_for_tick(
        &self,
        base: &ReplicationPullConfig,
    ) -> Option<ReplicationPullConfig> {
        if !self.failover.enabled {
            return Some(base.clone());
        }
        let source = self.failover.pull_source()?;
        let mut config = base.clone();
        config.source_base_url = source.trim().trim_end_matches('/').to_string();
        config.term = Some(self.failover.term);
        if config.local_replica_id.is_none() {
            config.local_replica_id = Some(self.failover.node_id.clone());
        }
        config.durable_replica = true;
        config.delta_io_timeout =
            Some(config.long_poll.unwrap_or(Duration::ZERO) + Duration::from_millis(2_000));
        Some(config)
    }

    fn observe_heartbeat_failure(&mut self, error: String) {
        self.failover.heartbeat_failure_total =
            self.failover.heartbeat_failure_total.saturating_add(1);
        if self.failover.last_heartbeat_error.is_none() {
            eprintln!("ingestion failover: heartbeat failed: {error}");
        }
        self.failover.last_heartbeat_error =
            Some(dash_common::replication_client::error_code(&error).to_string());
    }
}

/// One heartbeat: report under the lock, send without it, apply the answer
/// with the runtime drained (so promotion and demotion never race a write).
pub(crate) fn heartbeat_tick(
    runtime: &SharedRuntime,
    config: &FailoverConfig,
    pull: &ReplicationPullConfig,
    options: &PlacementSourceOptions,
) {
    let query = match runtime.lock() {
        Ok(mut guard) => guard.failover_heartbeat_query(pull),
        Err(_) => return,
    };
    let sent_at = Instant::now();
    let result = control_plane_post(
        &config.control_plane_url,
        &format!("/v1/control-plane/ingest/heartbeat?{query}"),
        options,
    )
    .and_then(|response| {
        if response.from_follower || response.status != 200 {
            return Err(format!(
                "control plane answered the heartbeat with status {} ({})",
                response.status,
                response.body.chars().take(200).collect::<String>()
            ));
        }
        parse_heartbeat_reply(&response.body)
    });
    let Ok(mut guard) = group_commit::lock_drained(runtime) else {
        return;
    };
    match result {
        Ok(reply) => guard.apply_heartbeat_reply(&reply, sent_at),
        Err(err) => guard.observe_heartbeat_failure(err),
    }
}

// ---------------------------------------------------------------------
// Replica progress (long polls and synchronous replication)
// ---------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ReplicaPosition {
    generation: u64,
    records: usize,
    term: u64,
}

#[derive(Debug, Default)]
struct ProgressInner {
    /// Bumped after every successful mutation.
    wal_seq: u64,
    replicas: HashMap<String, ReplicaPosition>,
}

/// Write target of a synchronous write: the leader's durable position
/// right after it, the checkpoint transitions (to recognise followers that
/// already moved past it) and the term followers must be in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SyncTarget {
    pub(crate) generation: u64,
    pub(crate) records: usize,
    pub(crate) transitions: Vec<(u64, u64)>,
    pub(crate) term: Option<u64>,
}

/// Followers' durable positions as proven by their polls, shared outside
/// the runtime lock so writers can wait without holding it.
#[derive(Debug, Default)]
pub(crate) struct ReplicaProgress {
    inner: Mutex<ProgressInner>,
    cond: Condvar,
}

impl ReplicaProgress {
    fn lock(&self) -> std::sync::MutexGuard<'_, ProgressInner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub(crate) fn wal_seq(&self) -> u64 {
        self.lock().wal_seq
    }

    /// A mutation committed: wake long polls (and nothing else).
    pub(crate) fn note_wal_advanced(&self) {
        let mut inner = self.lock();
        inner.wal_seq = inner.wal_seq.wrapping_add(1);
        drop(inner);
        self.cond.notify_all();
    }

    /// Wait until a mutation commits after `seen` or `timeout` passes.
    pub(crate) fn wait_wal_advanced(&self, seen: u64, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        let mut inner = self.lock();
        while inner.wal_seq == seen {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return;
            }
            inner = self
                .cond
                .wait_timeout(inner, remaining)
                .unwrap_or_else(|e| e.into_inner())
                .0;
        }
    }

    /// A promotable follower polled from `(generation, records)`: it has
    /// durably applied everything before that position.
    pub(crate) fn record(&self, replica_id: &str, generation: u64, records: usize, term: u64) {
        let mut inner = self.lock();
        inner.replicas.insert(
            replica_id.to_string(),
            ReplicaPosition {
                generation,
                records,
                term,
            },
        );
        drop(inner);
        self.cond.notify_all();
    }

    fn confirmed(inner: &ProgressInner, target: &SyncTarget) -> usize {
        inner
            .replicas
            .values()
            .filter(|replica| target.term.is_none_or(|term| replica.term == term))
            .filter(|replica| {
                if replica.generation == target.generation {
                    return replica.records >= target.records;
                }
                // A follower in a generation that a checkpoint opened after
                // the target holds everything up to the end of the target's.
                let mut generation = target.generation;
                for _ in 0..=target.transitions.len() {
                    let Some(&(_, to)) = target
                        .transitions
                        .iter()
                        .rev()
                        .find(|(from, _)| *from == generation)
                    else {
                        return false;
                    };
                    if to == replica.generation {
                        return true;
                    }
                    generation = to;
                }
                false
            })
            .count()
    }

    /// Wait until `min` followers confirmed `target`, or `timeout`.
    /// Returns the number that confirmed.
    pub(crate) fn wait_confirmed(
        &self,
        target: &SyncTarget,
        min: usize,
        timeout: Duration,
    ) -> usize {
        let deadline = Instant::now() + timeout;
        let mut inner = self.lock();
        loop {
            let confirmed = Self::confirmed(&inner, target);
            if confirmed >= min {
                return confirmed;
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return confirmed;
            }
            inner = self
                .cond
                .wait_timeout(inner, remaining)
                .unwrap_or_else(|e| e.into_inner())
                .0;
        }
    }
}

#[cfg(test)]
#[path = "failover_tests.rs"]
mod tests;
