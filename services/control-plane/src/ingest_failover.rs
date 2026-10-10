//! Automatic failover of the ingestion (write) leader (ADR 0006).
//!
//! The control plane is the only authority that names the ingestion leader
//! and issues its **term** (fencing token). Ingestion nodes heartbeat it;
//! the leader holds a lease of `lease_ms` measured on its own monotonic
//! clock from *before* it sent the heartbeat, and the control plane does
//! not consider it dead before `lease_ms + grace_ms` after it *processed*
//! that heartbeat. When the lease has lapsed, the most up-to-date eligible
//! member is promoted with `term + 1`; the term is persisted before anyone
//! is told about it.
//!
//! Everything time-dependent takes `now_ms` (a monotonic millisecond
//! reading) explicitly, so tests drive the clock instead of sleeping.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use dash_http::json_escape;

/// Default leader lease (`DASH_CONTROL_PLANE_INGEST_LEASE_MS`).
pub const DEFAULT_LEASE_MS: u64 = 5_000;
/// Default promotion grace (`DASH_CONTROL_PLANE_INGEST_PROMOTION_GRACE_MS`).
pub const DEFAULT_GRACE_MS: u64 = 1_000;
/// Longest node id / URL accepted in a heartbeat.
const MAX_FIELD_LEN: usize = 256;
/// Lineage entries kept (generations of the current leader's history).
const MAX_LINEAGE: usize = 64;
/// Members not heard from for this many leases are forgotten.
const FORGET_AFTER_LEASES: u64 = 60;

/// Monotonic millisecond clock (injectable for tests).
pub type Clock = Arc<dyn Fn() -> u64 + Send + Sync>;

/// Milliseconds since the first call in this process (monotonic).
pub fn monotonic_clock() -> Clock {
    let origin = Instant::now();
    Arc::new(move || origin.elapsed().as_millis() as u64)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngestFailoverConfig {
    pub lease_ms: u64,
    pub grace_ms: u64,
    /// Durable `(term, leader)` record. `None` keeps it in memory only.
    pub state_path: Option<PathBuf>,
}

impl Default for IngestFailoverConfig {
    fn default() -> Self {
        Self {
            lease_ms: DEFAULT_LEASE_MS,
            grace_ms: DEFAULT_GRACE_MS,
            state_path: None,
        }
    }
}

/// `(generation, records)` in a WAL lineage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Position {
    pub generation: u64,
    pub records: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReportedRole {
    Leader,
    Follower,
    Unknown,
}

impl ReportedRole {
    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "leader" => Some(Self::Leader),
            "follower" => Some(Self::Follower),
            "unknown" => Some(Self::Unknown),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Leader => "leader",
            Self::Follower => "follower",
            Self::Unknown => "unknown",
        }
    }
}

/// A checkpoint closed `from` at `records` and opened `to`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Transition {
    pub from: u64,
    pub records: u64,
    pub to: u64,
}

/// What a member reports in a heartbeat.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeartbeatReport {
    pub node_id: String,
    /// Base URL other nodes use to reach this node.
    pub url: String,
    /// Random per-process id: a restarted process is a new instance.
    pub instance_id: String,
    /// Highest term this node has seen.
    pub term: u64,
    pub role: ReportedRole,
    /// Replication position (a follower's cursor, or the node's own WAL).
    pub position: Option<Position>,
    /// A follower's last checkpoint crossing into `position.generation`:
    /// `(previous generation, records at which it was closed)`.
    pub prev: Option<Position>,
    /// Term of the leader that served that crossing. Only a crossing served
    /// by the current term's leader may extend the lineage: an old leader's
    /// checkpoint that the new leader never saw is a divergent history.
    pub prev_term: Option<u64>,
    /// The node's own recent checkpoint transitions, oldest first (reported
    /// for its own WAL: by the leader and by a node without a follower
    /// cursor). The current leader's chain is authoritative.
    pub chain: Vec<Transition>,
    /// Initial sync done, no resync pending, not blocked.
    pub synced: bool,
    /// Configured as a writer (no replication source): may be chosen when
    /// the cluster has no leader yet.
    pub bootstrap: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AssignedRole {
    Leader,
    Follower,
}

impl AssignedRole {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Leader => "leader",
            Self::Follower => "follower",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeartbeatReply {
    pub term: u64,
    pub role: AssignedRole,
    pub leader_node_id: Option<String>,
    pub leader_url: Option<String>,
    pub lease_ms: u64,
}

impl HeartbeatReply {
    pub fn to_json(&self) -> String {
        let opt = |value: &Option<String>| {
            value
                .as_deref()
                .map(|v| format!("\"{}\"", json_escape(v)))
                .unwrap_or_else(|| "null".to_string())
        };
        format!(
            "{{\"term\":{},\"role\":\"{}\",\"leader_node_id\":{},\"leader_url\":{},\"lease_ms\":{}}}",
            self.term,
            self.role.as_str(),
            opt(&self.leader_node_id),
            opt(&self.leader_url),
            self.lease_ms
        )
    }
}

/// Promotion performed while handling a heartbeat; the caller moves the
/// placements and logs it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Promotion {
    pub term: u64,
    pub old_leader: Option<String>,
    pub new_leader: String,
    /// Milliseconds between the old leader's lease lapsing and the
    /// promotion (`None` for the first leader of a cluster).
    pub after_lapse_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LeaderRecord {
    node_id: String,
    url: String,
    /// `None` after a reload: the first heartbeat of that node adopts it.
    instance_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Member {
    url: String,
    instance_id: String,
    last_seen_ms: u64,
    term: u64,
    role: ReportedRole,
    position: Option<Position>,
    synced: bool,
    bootstrap: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LineageEntry {
    generation: u64,
    /// Records at which this generation was closed, when known. An offset
    /// beyond it is a history the current leader does not share.
    end: Option<u64>,
    /// `end` is only a lower bound: set at a promotion to the new leader's
    /// position; its own fencing checkpoint (at or after it) makes it final.
    provisional: bool,
}

/// Why no promotion happened although the leader's lease lapsed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Blocked {
    /// Waiting for live members to report after the lapse.
    WaitingForReports,
    /// No live, synced member of the current lineage.
    NoEligibleCandidate,
}

impl Blocked {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::WaitingForReports => "waiting_for_reports",
            Self::NoEligibleCandidate => "no_eligible_candidate",
        }
    }
}

pub struct IngestFailover {
    config: IngestFailoverConfig,
    clock: Clock,
    term: u64,
    leader: Option<LeaderRecord>,
    /// When the leader's lease was last renewed (or granted).
    leader_seen_ms: u64,
    members: BTreeMap<String, Member>,
    lineage: Vec<LineageEntry>,
    preferred: Option<String>,
    /// Step-down requested: the current leader is no longer renewed.
    revoked: bool,
    /// When this coordinator (re)loaded its state.
    started_ms: u64,
    promotions_total: u64,
    blocked: Option<Blocked>,
    last_failover_ms: Option<u64>,
}

impl std::fmt::Debug for IngestFailover {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IngestFailover")
            .field("term", &self.term)
            .field("leader", &self.leader)
            .field("members", &self.members.len())
            .finish()
    }
}

impl IngestFailover {
    /// Create the coordinator and load the durable record, if any.
    pub fn new(config: IngestFailoverConfig, clock: Clock) -> Result<Self, String> {
        let now = clock();
        let mut failover = Self {
            config,
            clock,
            term: 0,
            leader: None,
            leader_seen_ms: now,
            members: BTreeMap::new(),
            lineage: Vec::new(),
            preferred: None,
            revoked: false,
            started_ms: now,
            promotions_total: 0,
            blocked: None,
            last_failover_ms: None,
        };
        failover.reload()?;
        Ok(failover)
    }

    pub fn now_ms(&self) -> u64 {
        (self.clock)()
    }

    pub fn lease_ms(&self) -> u64 {
        self.config.lease_ms
    }

    pub fn term(&self) -> u64 {
        self.term
    }

    pub fn leader_node_id(&self) -> Option<&str> {
        self.leader.as_ref().map(|leader| leader.node_id.as_str())
    }

    pub fn promotions_total(&self) -> u64 {
        self.promotions_total
    }

    pub fn blocked(&self) -> Option<Blocked> {
        self.blocked
    }

    pub fn last_failover_ms(&self) -> Option<u64> {
        self.last_failover_ms
    }

    pub fn member_count(&self) -> usize {
        self.members.len()
    }

    /// Node ids of every member heard from (used to decide which
    /// placements belong to the ingestion cluster).
    pub fn is_member(&self, node_id: &str) -> bool {
        self.members.contains_key(node_id)
    }

    /// Reload the durable record (called at start and whenever this
    /// control-plane process becomes leader). Heartbeat times are unknown
    /// afterwards, so the leader is treated as seen now: it keeps a full
    /// lease window before anyone may replace it.
    pub fn reload(&mut self) -> Result<(), String> {
        let now = self.now_ms();
        self.members.clear();
        self.lineage.clear();
        self.preferred = None;
        self.revoked = false;
        self.blocked = None;
        self.started_ms = now;
        self.leader_seen_ms = now;
        if let Some(path) = self.config.state_path.clone()
            && let Some(state) = read_state(&path)?
        {
            self.term = self.term.max(state.term);
            self.leader = state.leader;
            self.lineage = state.lineage;
        }
        Ok(())
    }

    /// Handle one heartbeat. Returns the reply and, when this call promoted
    /// a new leader, the promotion.
    pub fn heartbeat(
        &mut self,
        report: HeartbeatReport,
        now_ms: u64,
    ) -> Result<(HeartbeatReply, Option<Promotion>), String> {
        validate_report(&report)?;
        // Terms only move forward. A node that saw a higher term than ours
        // means this control plane lost its state: adopt it so a term is
        // never issued twice.
        if report.term > self.term {
            self.term = report.term;
            self.persist()?;
        }
        if self.leader.is_none()
            && report.role == ReportedRole::Leader
            && report.term == self.term
            && self.term > 0
        {
            // The legitimate leader of this term (terms are only ever
            // granted by the control plane, one node per term) reports in
            // after the record was lost: keep it.
            self.leader = Some(LeaderRecord {
                node_id: report.node_id.clone(),
                url: report.url.clone(),
                instance_id: Some(report.instance_id.clone()),
            });
            self.leader_seen_ms = now_ms;
            self.persist()?;
        }
        self.members.insert(
            report.node_id.clone(),
            Member {
                url: report.url.clone(),
                instance_id: report.instance_id.clone(),
                last_seen_ms: now_ms,
                term: report.term,
                role: report.role,
                position: report.position,
                synced: report.synced,
                bootstrap: report.bootstrap,
            },
        );
        self.forget_stale_members(now_ms);

        let is_leader_instance = self.is_leader_instance(&report);
        let lapsed = self.leader_lease_lapsed(now_ms);
        if is_leader_instance
            && report.term <= self.term
            && !self.revoked
            && report.role != ReportedRole::Leader
            && !lapsed
        {
            // Named leader, but it has not taken over yet (it learns from
            // this answer): tell it again, without renewing its lease. A
            // node that cannot take over keeps reporting so and is
            // replaced once the lease lapses.
            return Ok((self.reply_for(&report), None));
        }
        if is_leader_instance
            && report.term <= self.term
            && !self.revoked
            && report.role == ReportedRole::Leader
        {
            let url_changed = self
                .leader
                .as_ref()
                .is_some_and(|leader| leader.url != report.url);
            if let Some(leader) = self.leader.as_mut() {
                leader.instance_id = Some(report.instance_id.clone());
                leader.url = report.url.clone();
            }
            self.leader_seen_ms = now_ms;
            self.blocked = None;
            if self.observe_leader_lineage(&report) || url_changed {
                self.persist()?;
            }
            return Ok((self.reply_for(&report), None));
        }
        if report.term == self.term && self.observe_follower_lineage(&report) {
            self.persist()?;
        }

        let promotion = if lapsed {
            self.try_elect(now_ms)?
        } else {
            None
        };
        Ok((self.reply_for(&report), promotion))
    }

    /// Stop renewing the current leader. Once its lease has lapsed the
    /// best other member is promoted (`prefer` wins ties). Returns the
    /// deposed node id.
    pub fn step_down(&mut self, prefer: Option<String>) -> Result<String, String> {
        let leader = self
            .leader
            .as_ref()
            .ok_or_else(|| "no ingestion leader to step down".to_string())?
            .node_id
            .clone();
        if let Some(prefer) = prefer.as_deref()
            && prefer == leader
        {
            return Err("the preferred node is the current leader".to_string());
        }
        self.revoked = true;
        self.preferred = prefer;
        Ok(leader)
    }

    fn is_leader_instance(&self, report: &HeartbeatReport) -> bool {
        self.leader.as_ref().is_some_and(|leader| {
            leader.node_id == report.node_id
                && leader
                    .instance_id
                    .as_ref()
                    .is_none_or(|instance| *instance == report.instance_id)
        })
    }

    fn lapse_at(&self) -> u64 {
        self.leader_seen_ms
            .saturating_add(self.config.lease_ms)
            .saturating_add(self.config.grace_ms)
    }

    fn leader_lease_lapsed(&self, now_ms: u64) -> bool {
        match &self.leader {
            None => now_ms >= self.started_ms.saturating_add(self.config.lease_ms),
            Some(_) => now_ms > self.lapse_at(),
        }
    }

    fn reply_for(&self, report: &HeartbeatReport) -> HeartbeatReply {
        let leader = self.leader.as_ref();
        let role = if self.is_leader_instance(report) && !self.revoked {
            AssignedRole::Leader
        } else {
            AssignedRole::Follower
        };
        // A revoked leader is told to follow, but not whom (it must not
        // follow itself); it learns the new leader after the election.
        let (leader_node_id, leader_url) = match leader {
            Some(leader) if !(self.revoked && leader.node_id == report.node_id) => {
                (Some(leader.node_id.clone()), Some(leader.url.clone()))
            }
            _ => (None, None),
        };
        HeartbeatReply {
            term: self.term,
            role,
            leader_node_id,
            leader_url,
            lease_ms: self.config.lease_ms,
        }
    }

    fn lineage_index(&self, generation: u64) -> Option<usize> {
        self.lineage
            .iter()
            .position(|entry| entry.generation == generation)
    }

    fn push_lineage(&mut self, generation: u64) {
        self.lineage.push(LineageEntry {
            generation,
            end: None,
            provisional: false,
        });
        if self.lineage.len() > MAX_LINEAGE {
            let excess = self.lineage.len() - MAX_LINEAGE;
            self.lineage.drain(..excess);
        }
    }

    /// Learn generations from the current leader's report: its checkpoint
    /// chain is authoritative (an entry after a transition's source that is
    /// not its target is dropped), and its current generation is open.
    /// Returns `true` when the lineage changed (it is persisted then).
    fn observe_leader_lineage(&mut self, report: &HeartbeatReport) -> bool {
        let mut changed = false;
        for transition in &report.chain {
            let Some(from) = self.lineage_index(transition.from) else {
                continue;
            };
            let entry = self.lineage[from];
            if entry.end != Some(transition.records) || entry.provisional {
                self.lineage[from].end = Some(transition.records);
                self.lineage[from].provisional = false;
                changed = true;
            }
            if self.lineage.get(from + 1).map(|e| e.generation) == Some(transition.to) {
                continue;
            }
            self.lineage.truncate(from + 1);
            self.push_lineage(transition.to);
            changed = true;
        }
        if let Some(position) = report.position {
            match self.lineage_index(position.generation) {
                Some(index) if index + 1 == self.lineage.len() => {
                    if self.lineage[index].end.is_some() {
                        self.lineage[index].end = None;
                        self.lineage[index].provisional = false;
                        changed = true;
                    }
                }
                Some(_) => {}
                None => {
                    self.push_lineage(position.generation);
                    changed = true;
                }
            }
        }
        changed
    }

    /// A follower of the current term crossed a checkpoint the leader has
    /// not reported (it may have died right after it): extend the lineage
    /// if the crossing was served by the current leader and fits.
    fn observe_follower_lineage(&mut self, report: &HeartbeatReport) -> bool {
        let (Some(position), Some(prev)) = (report.position, report.prev) else {
            return false;
        };
        if report.prev_term != Some(self.term) || self.lineage_index(position.generation).is_some()
        {
            return false;
        }
        let Some(from) = self.lineage_index(prev.generation) else {
            return false;
        };
        if from + 1 < self.lineage.len() {
            // The successor is known and it is not this generation.
            return false;
        }
        let entry = self.lineage[from];
        let fits = match entry.end {
            None => true,
            Some(end) if entry.provisional => prev.records >= end,
            Some(end) => prev.records == end,
        };
        if !fits {
            return false;
        }
        self.lineage[from].end = Some(prev.records);
        self.lineage[from].provisional = false;
        self.push_lineage(position.generation);
        true
    }

    /// Rank of a position in the current lineage, `None` when it is not in
    /// it (unknown generation, or beyond the generation's end).
    fn rank(&self, position: Position) -> Option<(usize, u64)> {
        let index = self
            .lineage
            .iter()
            .position(|entry| entry.generation == position.generation)?;
        let entry = self.lineage[index];
        if entry.end.is_some_and(|end| position.records > end) {
            return None;
        }
        Some((index, position.records))
    }

    fn forget_stale_members(&mut self, now_ms: u64) {
        let horizon = self.config.lease_ms.saturating_mul(FORGET_AFTER_LEASES);
        self.members
            .retain(|_, member| now_ms.saturating_sub(member.last_seen_ms) <= horizon);
    }

    fn try_elect(&mut self, now_ms: u64) -> Result<Option<Promotion>, String> {
        let bootstrap = self.leader.is_none() && self.lineage.is_empty();
        let lapse_at = if self.leader.is_some() {
            self.lapse_at()
        } else {
            self.started_ms.saturating_add(self.config.lease_ms)
        };
        let lease = self.config.lease_ms;
        let deposed = self
            .leader
            .as_ref()
            .filter(|_| self.revoked)
            .map(|leader| leader.node_id.clone());
        // Live: heard from within one lease before the lapse, and not yet
        // silent for a whole lease. Every live member must report after
        // the lapse before anyone is chosen, so positions are final.
        let mut waiting = false;
        let mut best: Option<(String, (usize, u64), bool)> = None;
        for (node_id, member) in &self.members {
            let live_at_lapse = member.last_seen_ms.saturating_add(lease) >= lapse_at;
            let still_live = member.last_seen_ms.saturating_add(lease) >= now_ms;
            if !live_at_lapse || !still_live {
                continue;
            }
            if member.last_seen_ms <= lapse_at && !bootstrap {
                waiting = true;
                continue;
            }
            if deposed.as_deref() == Some(node_id.as_str()) {
                continue;
            }
            let Some(position) = member.position else {
                continue;
            };
            let rank = if bootstrap {
                if !member.bootstrap {
                    continue;
                }
                (0, position.records)
            } else {
                if !member.synced || member.term != self.term {
                    continue;
                }
                match self.rank(position) {
                    Some(rank) => rank,
                    None => continue,
                }
            };
            let preferred = self.preferred.as_deref() == Some(node_id.as_str());
            let better = match &best {
                None => true,
                Some((best_id, best_rank, best_preferred)) => {
                    (rank, preferred, std::cmp::Reverse(node_id.as_str()))
                        > (
                            *best_rank,
                            *best_preferred,
                            std::cmp::Reverse(best_id.as_str()),
                        )
                }
            };
            if better {
                best = Some((node_id.clone(), rank, preferred));
            }
        }
        if waiting {
            self.blocked = Some(Blocked::WaitingForReports);
            return Ok(None);
        }
        let Some((node_id, _, _)) = best else {
            self.blocked = Some(Blocked::NoEligibleCandidate);
            return Ok(None);
        };
        let member = self.members[&node_id].clone();
        let term = self
            .term
            .checked_add(1)
            .ok_or_else(|| "ingestion failover term overflow".to_string())?;
        let old_leader = self.leader.as_ref().map(|leader| leader.node_id.clone());
        let previous = (self.term, self.leader.clone(), self.lineage.clone());
        self.term = term;
        self.leader = Some(LeaderRecord {
            node_id: node_id.clone(),
            url: member.url.clone(),
            instance_id: Some(member.instance_id.clone()),
        });
        // The shared history ends at the new leader's position; anything a
        // member holds beyond it in that generation is a divergent history.
        if let Some(position) = member.position {
            if bootstrap {
                self.lineage = vec![LineageEntry {
                    generation: position.generation,
                    end: None,
                    provisional: false,
                }];
            } else if let Some(index) = self.lineage_index(position.generation) {
                self.lineage.truncate(index + 1);
                self.lineage[index].end = Some(position.records);
                self.lineage[index].provisional = true;
            }
        }
        // Persist before anyone learns the new term.
        if let Err(err) = self.persist() {
            (self.term, self.leader, self.lineage) = previous;
            return Err(err);
        }
        let after_lapse_ms = old_leader.as_ref().map(|_| now_ms.saturating_sub(lapse_at));
        self.leader_seen_ms = now_ms;
        self.revoked = false;
        self.preferred = None;
        self.blocked = None;
        self.promotions_total = self.promotions_total.saturating_add(1);
        self.last_failover_ms = after_lapse_ms;
        Ok(Some(Promotion {
            term,
            old_leader,
            new_leader: node_id,
            after_lapse_ms,
        }))
    }

    fn persist(&self) -> Result<(), String> {
        let Some(path) = self.config.state_path.as_deref() else {
            return Ok(());
        };
        let (leader, url) = self
            .leader
            .as_ref()
            .map(|leader| (leader.node_id.as_str(), leader.url.as_str()))
            .unwrap_or(("", ""));
        let lineage = self
            .lineage
            .iter()
            .map(|entry| match (entry.end, entry.provisional) {
                (Some(end), true) => format!("{:016x}:~{end}", entry.generation),
                (Some(end), false) => format!("{:016x}:{end}", entry.generation),
                (None, _) => format!("{:016x}:-", entry.generation),
            })
            .collect::<Vec<_>>()
            .join(",");
        let body = format!(
            "term={}\nleader={leader}\nleader_url={url}\nlineage={lineage}\n",
            self.term
        );
        crate::persist_atomically(path, body.as_bytes())
    }

    /// `/v1/control-plane/ingest` status document.
    pub fn status_json(&self, now_ms: u64) -> String {
        let leader = self.leader.as_ref();
        let remaining = leader.map(|_| self.lapse_at() as i128 - now_ms as i128);
        let members = self
            .members
            .iter()
            .map(|(node_id, member)| {
                let position = member
                    .position
                    .map(|p| {
                        format!(
                            "{{\"generation\":{},\"records\":{}}}",
                            p.generation, p.records
                        )
                    })
                    .unwrap_or_else(|| "null".to_string());
                format!(
                    "{{\"node_id\":\"{}\",\"url\":\"{}\",\"role\":\"{}\",\"term\":{},\"synced\":{},\"last_seen_ms_ago\":{},\"position\":{}}}",
                    json_escape(node_id),
                    json_escape(&member.url),
                    member.role.as_str(),
                    member.term,
                    member.synced,
                    now_ms.saturating_sub(member.last_seen_ms),
                    position
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        format!(
            "{{\"term\":{},\"leader_node_id\":{},\"leader_url\":{},\"lease_ms\":{},\"grace_ms\":{},\"leader_lapses_in_ms\":{},\"step_down_pending\":{},\"blocked\":{},\"promotions_total\":{},\"members\":[{}]}}",
            self.term,
            leader
                .map(|l| format!("\"{}\"", json_escape(&l.node_id)))
                .unwrap_or_else(|| "null".to_string()),
            leader
                .map(|l| format!("\"{}\"", json_escape(&l.url)))
                .unwrap_or_else(|| "null".to_string()),
            self.config.lease_ms,
            self.config.grace_ms,
            remaining
                .map(|r| r.to_string())
                .unwrap_or_else(|| "null".to_string()),
            self.revoked,
            self.blocked
                .map(|b| format!("\"{}\"", b.as_str()))
                .unwrap_or_else(|| "null".to_string()),
            self.promotions_total,
            members
        )
    }
}

fn validate_report(report: &HeartbeatReport) -> Result<(), String> {
    let field_ok = |value: &str| {
        !value.is_empty()
            && value.len() <= MAX_FIELD_LEN
            && !value
                .chars()
                .any(|c| c.is_control() || c.is_whitespace() || c == ',')
    };
    if !field_ok(&report.node_id) {
        return Err("node_id must be 1-256 printable characters without ',' or spaces".into());
    }
    if !field_ok(&report.instance_id) {
        return Err("instance_id must be 1-256 printable characters".into());
    }
    if !field_ok(&report.url)
        || !(report.url.starts_with("http://") || report.url.starts_with("https://"))
    {
        return Err("url must be an http:// or https:// base URL".into());
    }
    Ok(())
}

struct PersistedState {
    term: u64,
    leader: Option<LeaderRecord>,
    lineage: Vec<LineageEntry>,
}

fn read_state(path: &Path) -> Result<Option<PersistedState>, String> {
    let text = match fs::read_to_string(path) {
        Ok(text) => text,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(format!(
                "failed reading ingest failover state '{}': {err}",
                path.display()
            ));
        }
    };
    let mut term = None;
    let mut leader = String::new();
    let mut url = String::new();
    let mut lineage = Vec::new();
    let invalid = || format!("ingest failover state '{}' is malformed", path.display());
    for line in text.lines() {
        match line.split_once('=') {
            Some(("term", value)) => {
                term = Some(value.trim().parse::<u64>().map_err(|_| {
                    format!(
                        "ingest failover state '{}' has an invalid term",
                        path.display()
                    )
                })?)
            }
            Some(("leader", value)) => leader = value.trim().to_string(),
            Some(("leader_url", value)) => url = value.trim().to_string(),
            Some(("lineage", value)) => {
                for item in value.split(',').filter(|item| !item.trim().is_empty()) {
                    let (generation, end) = item.trim().split_once(':').ok_or_else(invalid)?;
                    let generation = u64::from_str_radix(generation, 16).map_err(|_| invalid())?;
                    let provisional = end.starts_with('~');
                    let end = match end.trim_start_matches('~') {
                        "-" => None,
                        raw => Some(raw.parse::<u64>().map_err(|_| invalid())?),
                    };
                    lineage.push(LineageEntry {
                        generation,
                        end,
                        provisional,
                    });
                }
            }
            _ => {}
        }
    }
    let term = term.ok_or_else(|| {
        format!(
            "ingest failover state '{}' has no term; inspect or remove it",
            path.display()
        )
    })?;
    let leader = (!leader.is_empty() && !url.is_empty()).then_some(LeaderRecord {
        node_id: leader,
        url,
        instance_id: None,
    });
    Ok(Some(PersistedState {
        term,
        leader,
        lineage,
    }))
}

/// Decode `%XX` escapes (and `+` as a space). Invalid escapes are kept
/// verbatim; the result is validated by the caller.
fn percent_decode(raw: &str) -> String {
    let bytes = raw.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => {
                let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).ok();
                match hex.and_then(|h| u8::from_str_radix(h, 16).ok()) {
                    Some(byte) => {
                        out.push(byte);
                        i += 3;
                    }
                    None => {
                        out.push(b'%');
                        i += 1;
                    }
                }
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            byte => {
                out.push(byte);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Parse a heartbeat from its query parameters.
pub fn parse_heartbeat_query(
    query: &std::collections::HashMap<String, String>,
) -> Result<HeartbeatReport, String> {
    // The shared query splitter does not percent-decode; heartbeats carry
    // URLs, so their values are decoded here.
    let get = |key: &str| query.get(key).map(|value| percent_decode(value.trim()));
    let required = |key: &str| {
        get(key)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| format!("{key} query parameter is required"))
    };
    let parse_u64 = |key: &str| -> Result<Option<u64>, String> {
        match get(key).filter(|value| !value.is_empty()) {
            None => Ok(None),
            Some(raw) => raw
                .parse::<u64>()
                .map(Some)
                .map_err(|_| format!("{key} must be an unsigned integer")),
        }
    };
    let parse_gen = |key: &str| -> Result<Option<u64>, String> {
        match get(key).filter(|value| !value.is_empty()) {
            None => Ok(None),
            Some(raw) => raw
                .parse::<u64>()
                .map(Some)
                .map_err(|_| format!("{key} must be a WAL generation (unsigned integer)")),
        }
    };
    let flag = |key: &str| matches!(get(key).as_deref(), Some("1") | Some("true"));
    let role = ReportedRole::parse(&required("role")?)
        .ok_or_else(|| "role must be leader, follower or unknown".to_string())?;
    let position = match (parse_gen("generation")?, parse_u64("records")?) {
        (Some(generation), Some(records)) => Some(Position {
            generation,
            records,
        }),
        _ => None,
    };
    let prev = match (parse_gen("prev_generation")?, parse_u64("prev_records")?) {
        (Some(generation), Some(records)) => Some(Position {
            generation,
            records,
        }),
        _ => None,
    };
    let mut chain = Vec::new();
    if let Some(raw) = get("chain").filter(|raw| !raw.is_empty()) {
        for item in raw.split(',') {
            let parts: Vec<&str> = item.split(':').collect();
            let invalid = || "chain must list from:records:to transitions".to_string();
            let [from, records, to] = parts[..] else {
                return Err(invalid());
            };
            chain.push(Transition {
                from: from.parse().map_err(|_| invalid())?,
                records: records.parse().map_err(|_| invalid())?,
                to: to.parse().map_err(|_| invalid())?,
            });
            if chain.len() > MAX_LINEAGE {
                return Err("chain is too long".to_string());
            }
        }
    }
    Ok(HeartbeatReport {
        node_id: required("node_id")?,
        url: required("url")?,
        instance_id: required("instance")?,
        term: parse_u64("term")?.unwrap_or(0),
        role,
        position,
        prev,
        prev_term: parse_u64("prev_term")?,
        chain,
        synced: flag("synced"),
        bootstrap: flag("bootstrap"),
    })
}

#[cfg(test)]
#[path = "ingest_failover_tests.rs"]
mod tests;
