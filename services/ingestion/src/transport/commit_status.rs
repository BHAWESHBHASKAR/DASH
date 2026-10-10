//! Bounded table of per-commit replication status.
//!
//! The leader records one entry per accepted write so followers can ack it and
//! operators can poll `/internal/replication/commit-status`. An entry is
//! *pending* until its ack count reaches the required quorum and *completed*
//! afterwards.
//!
//! Retention policy:
//! * Pending entries are never evicted: dropping one would silently lose a
//!   commit that is still waiting for quorum. The table can therefore exceed
//!   its cap while many commits are pending; the entries gauge makes that
//!   visible.
//! * Completed entries expire after `ttl` and, when the table is over `max`
//!   entries, the oldest completed entries are evicted first.
//! * A late ack for an evicted (or never known) commit is answered as an
//!   unknown commit (HTTP 404); it never recreates the entry. The follower
//!   treats that answer as benign.
//!
//! All time-dependent methods take `now` explicitly so tests can drive the
//! clock without sleeping.

use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};

pub(crate) const DEFAULT_MAX_ENTRIES: usize = 100_000;
pub(crate) const DEFAULT_TTL: Duration = Duration::from_secs(3600);

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReplicationCommitStatus {
    pub(crate) commit_epoch: Option<u64>,
    pub(crate) ack_count: usize,
    pub(crate) required_acks: usize,
    pub(crate) commit_status: String,
    pub(crate) acknowledged_replicas: HashSet<String>,
    /// Completion stamp: (sequence, time). `None` while pending.
    completed: Option<(u64, Instant)>,
}

impl ReplicationCommitStatus {
    pub(crate) fn new(
        commit_epoch: Option<u64>,
        ack_count: usize,
        required_acks: usize,
        acknowledged_replicas: HashSet<String>,
    ) -> Self {
        Self {
            commit_epoch,
            ack_count,
            required_acks,
            commit_status: String::new(),
            acknowledged_replicas,
            completed: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn is_completed(&self) -> bool {
        self.completed.is_some()
    }
}

#[derive(Debug)]
pub(crate) struct CommitStatusTable {
    entries: HashMap<String, ReplicationCommitStatus>,
    /// Completed entries in completion order (oldest first). Items whose
    /// sequence no longer matches the live entry are stale and skipped.
    completed_order: VecDeque<(u64, String)>,
    next_seq: u64,
    max_entries: usize,
    ttl: Duration,
    evicted_total: u64,
}

impl CommitStatusTable {
    pub(crate) fn new(max_entries: usize, ttl: Duration) -> Self {
        Self {
            entries: HashMap::new(),
            completed_order: VecDeque::new(),
            next_seq: 0,
            max_entries: max_entries.max(1),
            ttl,
            evicted_total: 0,
        }
    }

    pub(crate) fn from_env() -> Self {
        let max = std::env::var("DASH_INGEST_REPLICATION_COMMIT_STATUS_MAX")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(DEFAULT_MAX_ENTRIES);
        let ttl = std::env::var("DASH_INGEST_REPLICATION_COMMIT_STATUS_TTL_SECS")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .filter(|v| *v > 0)
            .map(Duration::from_secs)
            .unwrap_or(DEFAULT_TTL);
        Self::new(max, ttl)
    }

    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(crate) fn evicted_total(&self) -> u64 {
        self.evicted_total
    }

    pub(crate) fn get(&self, commit_id: &str) -> Option<&ReplicationCommitStatus> {
        self.entries.get(commit_id)
    }

    /// Insert (or replace) a commit and enforce the retention policy.
    pub(crate) fn insert(
        &mut self,
        commit_id: String,
        status: ReplicationCommitStatus,
        now: Instant,
    ) {
        self.entries.insert(commit_id.clone(), status);
        self.refresh_status(&commit_id, now);
        self.enforce(now);
    }

    /// Apply an ack to a known commit. Returns `None` for an unknown or
    /// evicted commit (it is never recreated).
    pub(crate) fn ack(
        &mut self,
        commit_id: &str,
        replica_id: &str,
        ack_epoch: Option<u64>,
        now: Instant,
    ) -> Option<&ReplicationCommitStatus> {
        self.expire(now);
        let status = self.entries.get_mut(commit_id)?;
        if status
            .acknowledged_replicas
            .insert(replica_id.trim().to_string())
        {
            status.ack_count = status.ack_count.saturating_add(1);
        }
        if let Some(epoch) = ack_epoch {
            status.commit_epoch = Some(status.commit_epoch.unwrap_or(epoch).max(epoch));
        }
        self.refresh_status(commit_id, now);
        self.entries.get(commit_id)
    }

    /// Drop expired completed entries; also run before gauges are read.
    pub(crate) fn expire(&mut self, now: Instant) {
        while let Some((seq, id)) = self.completed_order.front().cloned() {
            match self.entries.get(&id).and_then(|e| e.completed) {
                Some((live_seq, at)) if live_seq == seq => {
                    if now.saturating_duration_since(at) >= self.ttl {
                        self.completed_order.pop_front();
                        self.entries.remove(&id);
                        self.evicted_total = self.evicted_total.saturating_add(1);
                    } else {
                        break;
                    }
                }
                _ => {
                    self.completed_order.pop_front();
                }
            }
        }
    }

    fn enforce(&mut self, now: Instant) {
        self.expire(now);
        while self.entries.len() > self.max_entries {
            let Some((seq, id)) = self.completed_order.pop_front() else {
                // Only pending entries remain; they are never evicted.
                break;
            };
            if self.entries.get(&id).and_then(|e| e.completed).map(|c| c.0) == Some(seq) {
                self.entries.remove(&id);
                self.evicted_total = self.evicted_total.saturating_add(1);
            }
        }
    }

    fn refresh_status(&mut self, commit_id: &str, now: Instant) {
        let Some(status) = self.entries.get_mut(commit_id) else {
            return;
        };
        if status.ack_count >= status.required_acks {
            status.commit_status = "replication_quorum_met".to_string();
            if status.completed.is_none() {
                let seq = self.next_seq;
                self.next_seq += 1;
                status.completed = Some((seq, now));
                self.completed_order.push_back((seq, commit_id.to_string()));
            }
        } else {
            status.commit_status = "replication_pending".to_string();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn status(ack: usize, required: usize) -> ReplicationCommitStatus {
        ReplicationCommitStatus::new(None, ack, required, HashSet::new())
    }

    #[test]
    fn filling_past_the_cap_evicts_only_completed_entries_oldest_first() {
        let t0 = Instant::now();
        let mut table = CommitStatusTable::new(3, Duration::from_secs(3600));
        for i in 0..3 {
            table.insert(format!("done-{i}"), status(1, 1), t0);
        }
        assert_eq!(table.len(), 3);
        table.insert("done-3".to_string(), status(1, 1), t0);
        assert_eq!(table.len(), 3);
        assert!(table.get("done-0").is_none(), "oldest completed goes first");
        assert!(table.get("done-1").is_some());
        table.insert("done-4".to_string(), status(1, 1), t0);
        assert!(table.get("done-1").is_none());
        assert!(table.get("done-2").is_some());
        assert_eq!(table.evicted_total(), 2);
    }

    #[test]
    fn pending_entries_survive_the_cap() {
        let t0 = Instant::now();
        let mut table = CommitStatusTable::new(2, Duration::from_secs(3600));
        table.insert("pending-a".to_string(), status(1, 2), t0);
        table.insert("pending-b".to_string(), status(1, 2), t0);
        table.insert("pending-c".to_string(), status(1, 2), t0);
        for i in 0..5 {
            table.insert(format!("done-{i}"), status(1, 1), t0);
        }
        for id in ["pending-a", "pending-b", "pending-c"] {
            let entry = table.get(id).expect("pending entry must survive");
            assert!(!entry.is_completed());
        }
        assert_eq!(table.len(), 3, "only pending entries remain over the cap");
        assert_eq!(table.evicted_total(), 5);
    }

    #[test]
    fn a_pending_entry_that_completes_is_evicted_in_completion_order() {
        let t0 = Instant::now();
        let mut table = CommitStatusTable::new(2, Duration::from_secs(3600));
        table.insert("a".to_string(), status(1, 2), t0);
        table.insert("b".to_string(), status(1, 1), t0);
        // a completes after b, so b is the older completed entry.
        assert!(table.ack("a", "r1", None, t0).is_some());
        table.insert("c".to_string(), status(1, 1), t0);
        assert!(table.get("b").is_none());
        assert!(table.get("a").is_some());
        assert!(table.get("c").is_some());
    }

    #[test]
    fn ttl_expires_completed_entries_with_an_injected_clock() {
        let t0 = Instant::now();
        let ttl = Duration::from_secs(60);
        let mut table = CommitStatusTable::new(100, ttl);
        table.insert("done".to_string(), status(1, 1), t0);
        table.insert("pending".to_string(), status(1, 2), t0);
        table.expire(t0 + Duration::from_secs(59));
        assert_eq!(table.len(), 2);
        table.expire(t0 + Duration::from_secs(61));
        assert!(table.get("done").is_none());
        assert!(table.get("pending").is_some(), "pending never expires");
        assert_eq!(table.evicted_total(), 1);
    }

    #[test]
    fn late_ack_for_an_evicted_commit_is_unknown_and_does_not_resurrect_it() {
        let t0 = Instant::now();
        let mut table = CommitStatusTable::new(1, Duration::from_secs(3600));
        table.insert("old".to_string(), status(1, 1), t0);
        table.insert("new".to_string(), status(1, 1), t0);
        assert!(table.get("old").is_none());
        assert!(table.ack("old", "replica-1", Some(7), t0).is_none());
        assert!(
            table.get("old").is_none(),
            "ack must not recreate the entry"
        );
        assert_eq!(table.len(), 1);
    }

    #[test]
    fn duplicate_ack_does_not_double_count() {
        let t0 = Instant::now();
        let mut table = CommitStatusTable::new(10, Duration::from_secs(3600));
        table.insert("c".to_string(), status(1, 3), t0);
        assert_eq!(table.ack("c", "r1", None, t0).unwrap().ack_count, 2);
        assert_eq!(table.ack("c", "r1", None, t0).unwrap().ack_count, 2);
        assert_eq!(
            table.ack("c", "r2", None, t0).unwrap().commit_status,
            "replication_quorum_met"
        );
    }
}
