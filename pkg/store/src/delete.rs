//! Deletes: claim, evidence and whole-tenant tombstones.
//!
//! A delete is a checksummed `T2` WAL record (see [`Tombstone`]). It is
//! written like every other mutation: validated and encoded first
//! ([`InMemoryStore::prepare_delete`]), made durable, and only then applied
//! to memory and the redb mirror ([`InMemoryStore::apply_prepared_delete`]).
//! Replay, replication followers and resync apply the same record through
//! [`InMemoryStore::apply_persisted_record_line`], so every replica converges
//! on the same state. A checkpoint snapshots the current state, which no
//! longer holds the deleted rows, so compaction drops them (and the
//! tombstone) for good.
//!
//! What a tombstone removes is decided when it is applied, from the state
//! at that point of the log, never from the request: applying the same log
//! always yields the same state, and re-applying a tombstone whose target is
//! already gone is a no-op.

use std::collections::{BTreeSet, HashSet};

use schema::{ClaimEdge, Evidence};

use crate::wal::{
    BatchCommitRecord, GROUP_BEGIN_PREFIX, PersistedRecord, SINGLE_TX_PREFIX, Tombstone,
    TombstoneRecord, WalEvent, record_to_line,
};
use crate::{FileWal, InMemoryStore, StagedDiskOp, StoreError};

/// What a delete removed (or, from [`InMemoryStore::plan_delete`], would
/// remove).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DeleteStats {
    pub claims: usize,
    pub evidence: usize,
    pub edges: usize,
    pub vectors: usize,
}

impl DeleteStats {
    /// `true` when nothing was (or would be) removed.
    pub fn is_empty(&self) -> bool {
        self.claims == 0 && self.evidence == 0 && self.edges == 0 && self.vectors == 0
    }

    fn add(&mut self, other: DeleteStats) {
        self.claims += other.claims;
        self.evidence += other.evidence;
        self.edges += other.edges;
        self.vectors += other.vectors;
    }
}

/// A validated delete whose WAL record is encoded but not yet written.
/// Produced by [`InMemoryStore::prepare_delete`]; the caller makes
/// [`PreparedDelete::wal_lines`] durable and only then calls
/// [`InMemoryStore::apply_prepared_delete`]. A delete whose target does not
/// exist has no WAL line and applying it changes nothing.
#[derive(Debug, Clone)]
pub struct PreparedDelete {
    tombstone: Tombstone,
    planned: DeleteStats,
    wal_lines: Vec<String>,
}

impl PreparedDelete {
    /// The encoded tombstone (a one-record commit group: begin marker,
    /// tombstone, commit marker), or nothing when there is nothing to delete.
    pub fn wal_lines(&self) -> &[String] {
        &self.wal_lines
    }

    pub fn tombstone(&self) -> &Tombstone {
        &self.tombstone
    }

    /// What applying this delete to the state it was prepared against
    /// removes.
    pub fn planned(&self) -> DeleteStats {
        self.planned
    }

    /// `true` when the target does not exist (nothing to write or apply).
    pub fn is_noop(&self) -> bool {
        self.wal_lines.is_empty()
    }
}

/// Result of applying a delete.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteOutcome {
    pub stats: DeleteStats,
    /// Set when redb rejected the mirrored delete after the WAL commit. The
    /// delete itself is durable (WAL) and visible; redb was detached.
    pub disk_error: Option<String>,
}

impl DeleteOutcome {
    /// `true` when something was removed.
    pub fn deleted(&self) -> bool {
        !self.stats.is_empty()
    }
}

/// The redb side of one tombstone, applied in a single write transaction:
/// blob rewrites first, then row removals (a rewritten edge blob of a claim
/// that the same tombstone also removes is dropped with the claim).
#[derive(Debug, Clone, Default)]
pub(crate) struct DiskDeletion {
    /// `(tenant, claim)`: claim row, tenant-set row, evidence blob, edge blob
    /// and vector.
    pub(crate) claims: Vec<(String, String)>,
    /// Evidence blobs to rewrite; an empty list removes the blob.
    pub(crate) evidence_blobs: Vec<(String, Vec<Evidence>)>,
    /// Edge blobs (by source claim) to rewrite; an empty list removes it.
    pub(crate) edge_blobs: Vec<(String, Vec<ClaimEdge>)>,
    pub(crate) tenant_dims: Vec<String>,
    pub(crate) batch_commits: Vec<String>,
}

impl InMemoryStore {
    /// What `tombstone` would remove from the current state, without
    /// changing anything.
    pub fn plan_delete(&self, tombstone: &Tombstone) -> DeleteStats {
        match tombstone {
            Tombstone::Claim {
                tenant_id,
                claim_id,
            } => match self.claims.get(claim_id) {
                Some(claim) if claim.tenant_id == *tenant_id => {
                    let only = HashSet::from([claim_id.as_str()]);
                    self.plan_claim(claim_id, &only)
                }
                _ => DeleteStats::default(),
            },
            Tombstone::Evidence {
                tenant_id,
                evidence_id,
            } => DeleteStats {
                evidence: self
                    .tenant_claims_holding_evidence(tenant_id, evidence_id)
                    .len(),
                ..DeleteStats::default()
            },
            Tombstone::Tenant { tenant_id } => {
                let ids: HashSet<&str> = self
                    .tenant_claim_ids
                    .get(tenant_id)
                    .into_iter()
                    .flatten()
                    .map(String::as_str)
                    .collect();
                let mut stats = DeleteStats::default();
                for claim_id in &ids {
                    stats.add(self.plan_claim(claim_id, &ids));
                }
                stats
            }
        }
    }

    /// Counts for removing `claim_id` as part of removing every claim in
    /// `removed` (edges between two removed claims are counted once, as an
    /// outgoing edge).
    fn plan_claim(&self, claim_id: &str, removed: &HashSet<&str>) -> DeleteStats {
        let outgoing = self.edges_by_claim.get(claim_id).map_or(0, Vec::len);
        let incoming = self.edges_in.get(claim_id).map_or(0, |list| {
            list.iter()
                .filter(|edge| !removed.contains(edge.from_claim_id.as_str()))
                .count()
        });
        DeleteStats {
            claims: 1,
            evidence: self.evidence_by_claim.get(claim_id).map_or(0, Vec::len),
            edges: outgoing + incoming,
            vectors: usize::from(self.claim_vectors.contains_key(claim_id)),
        }
    }

    /// Claims of `tenant_id` holding an evidence row `evidence_id`, sorted.
    /// O(claims of the tenant): evidence ids are not indexed.
    fn tenant_claims_holding_evidence(&self, tenant_id: &str, evidence_id: &str) -> Vec<String> {
        let mut out: Vec<String> = self
            .tenant_claim_ids
            .get(tenant_id)
            .into_iter()
            .flatten()
            .filter(|claim_id| {
                self.evidence_by_claim
                    .get(claim_id.as_str())
                    .is_some_and(|list| list.iter().any(|e| e.evidence_id == evidence_id))
            })
            .cloned()
            .collect();
        out.sort_unstable();
        out
    }

    /// Validates `tombstone` and encodes its WAL record (none when there is
    /// nothing to delete). Nothing is written or changed.
    pub fn prepare_delete(
        &self,
        tombstone: Tombstone,
        ts_unix_ms: u64,
    ) -> Result<PreparedDelete, StoreError> {
        tombstone.validate()?;
        let planned = self.plan_delete(&tombstone);
        let wal_lines = if planned.is_empty() {
            Vec::new()
        } else {
            // The tombstone is framed as a one-record commit group. Besides
            // making it an atomic unit for replication frames, this keeps it
            // off the last line of the log: a reader that predates tombstones
            // would take an unknown final line for a torn write and truncate
            // it, while an unknown interior record fails its replay.
            let group = format!("del:{}:{ts_unix_ms}", tombstone.scope());
            let marker = |commit_id: String| {
                PersistedRecord::BatchCommit(BatchCommitRecord {
                    commit_id,
                    batch_size: 0,
                    ts_unix_ms,
                    claim_ids: Vec::new(),
                })
            };
            [
                marker(format!("{GROUP_BEGIN_PREFIX}{group}")),
                PersistedRecord::Tombstone(TombstoneRecord {
                    tombstone: tombstone.clone(),
                    ts_unix_ms,
                }),
                marker(format!("{SINGLE_TX_PREFIX}{group}")),
            ]
            .iter()
            .map(record_to_line)
            .collect()
        };
        Ok(PreparedDelete {
            tombstone,
            planned,
            wal_lines,
        })
    }

    /// Applies a prepared delete to memory (and redb) after its WAL line is
    /// durable. Callers apply prepared writes in WAL order.
    pub fn apply_prepared_delete(
        &mut self,
        prepared: PreparedDelete,
    ) -> Result<DeleteOutcome, StoreError> {
        if prepared.is_noop() {
            return Ok(DeleteOutcome {
                stats: DeleteStats::default(),
                disk_error: None,
            });
        }
        let mut stats = DeleteStats::default();
        let disk_error = self.apply_with_deferred_disk(|store| {
            stats = store.apply_tombstone(&prepared.tombstone)?;
            Ok(())
        })?;
        Ok(DeleteOutcome { stats, disk_error })
    }

    /// Prepare, append to `wal` (one write and, with the default policy, one
    /// fsync) and apply. A delete of something absent writes nothing.
    pub fn delete_persistent(
        &mut self,
        wal: &mut FileWal,
        tombstone: Tombstone,
        ts_unix_ms: u64,
    ) -> Result<DeleteOutcome, StoreError> {
        let prepared = self.prepare_delete(tombstone, ts_unix_ms)?;
        wal.append_group_lines(prepared.wal_lines())?;
        self.apply_prepared_delete(prepared)
    }

    /// In-memory delete (no WAL).
    pub fn delete(&mut self, tombstone: Tombstone) -> Result<DeleteStats, StoreError> {
        tombstone.validate()?;
        self.apply_tombstone(&tombstone)
    }

    /// Applies a tombstone to memory and mirrors it to redb. Idempotent:
    /// whatever is already gone is skipped. Every decision is taken from the
    /// current state, so replaying a log yields the state the live store had.
    pub(crate) fn apply_tombstone(
        &mut self,
        tombstone: &Tombstone,
    ) -> Result<DeleteStats, StoreError> {
        let mut deletion = DiskDeletion::default();
        let mut sources = BTreeSet::new();
        let stats = match tombstone {
            Tombstone::Claim {
                tenant_id,
                claim_id,
            } => {
                let owned = self
                    .claims
                    .get(claim_id)
                    .is_some_and(|claim| claim.tenant_id == *tenant_id);
                if owned {
                    let stats = self.remove_claim(claim_id, &mut deletion, &mut sources);
                    self.drop_tenant_dim_if_unused(tenant_id, &mut deletion);
                    stats
                } else {
                    DeleteStats::default()
                }
            }
            Tombstone::Evidence {
                tenant_id,
                evidence_id,
            } => {
                let mut stats = DeleteStats::default();
                for claim_id in self.tenant_claims_holding_evidence(tenant_id, evidence_id) {
                    let Some(list) = self.evidence_by_claim.get_mut(&claim_id) else {
                        continue;
                    };
                    let before = list.len();
                    list.retain(|e| e.evidence_id != *evidence_id);
                    stats.evidence += before - list.len();
                    let remaining = list.clone();
                    if remaining.is_empty() {
                        self.evidence_by_claim.remove(&claim_id);
                    }
                    deletion.evidence_blobs.push((claim_id, remaining));
                }
                stats
            }
            Tombstone::Tenant { tenant_id } => {
                self.erase_tenant(tenant_id, &mut deletion, &mut sources)
            }
        };
        for source in sources {
            if self.claims.contains_key(&source) {
                let edges = self
                    .edges_by_claim
                    .get(&source)
                    .cloned()
                    .unwrap_or_default();
                deletion.edge_blobs.push((source, edges));
            }
        }
        if !stats.is_empty() || !deletion.tenant_dims.is_empty() {
            self.wal
                .push(WalEvent::Tombstone(tombstone.target_id().to_string()));
            self.mirror_op(StagedDiskOp::Delete(deletion))?;
        }
        Ok(stats)
    }

    /// Removes one claim with its vector, evidence and every edge from or to
    /// it. Source claims whose edge lists changed are added to `sources`.
    fn remove_claim(
        &mut self,
        claim_id: &str,
        deletion: &mut DiskDeletion,
        sources: &mut BTreeSet<String>,
    ) -> DeleteStats {
        let Some(claim) = self.claims.remove(claim_id) else {
            return DeleteStats::default();
        };
        self.remove_claim_indexes(&claim);
        let mut stats = DeleteStats {
            claims: 1,
            ..DeleteStats::default()
        };
        if let Some(edges) = self.edges_by_claim.remove(claim_id) {
            stats.edges += edges.len();
            for edge in &edges {
                let mut now_empty = false;
                if let Some(incoming) = self.edges_in.get_mut(&edge.to_claim_id) {
                    incoming
                        .retain(|i| !(i.from_claim_id == claim_id && i.relation == edge.relation));
                    now_empty = incoming.is_empty();
                }
                if now_empty {
                    self.edges_in.remove(&edge.to_claim_id);
                }
            }
        }
        if let Some(incoming) = self.edges_in.remove(claim_id) {
            for entry in incoming {
                if entry.from_claim_id == claim_id {
                    continue;
                }
                let mut now_empty = false;
                if let Some(list) = self.edges_by_claim.get_mut(&entry.from_claim_id) {
                    let before = list.len();
                    list.retain(|e| !(e.to_claim_id == claim_id && e.relation == entry.relation));
                    stats.edges += before - list.len();
                    now_empty = list.is_empty();
                }
                if now_empty {
                    self.edges_by_claim.remove(&entry.from_claim_id);
                }
                sources.insert(entry.from_claim_id);
            }
        }
        if let Some(evidence) = self.evidence_by_claim.remove(claim_id) {
            stats.evidence += evidence.len();
        }
        if self.claim_vectors.remove(claim_id).is_some() {
            stats.vectors = 1;
            self.remove_vector_index_entry(&claim.tenant_id, claim_id);
        }
        self.replica_skipped_claims.remove(claim_id);
        deletion
            .claims
            .push((claim.tenant_id.clone(), claim_id.to_string()));
        stats
    }

    /// A tenant's vector dimension is established by its first vector; once
    /// its last vector is deleted the dimension is released, exactly as a
    /// restart from a snapshot (which holds no vector for it) would see it.
    fn drop_tenant_dim_if_unused(&mut self, tenant_id: &str, deletion: &mut DiskDeletion) {
        if !self.tenant_vector_dims.contains_key(tenant_id) {
            return;
        }
        let still_used = self
            .tenant_claim_ids
            .get(tenant_id)
            .is_some_and(|ids| ids.iter().any(|id| self.claim_vectors.contains_key(id)));
        if !still_used {
            self.tenant_vector_dims.remove(tenant_id);
            self.vector_indexes.remove(tenant_id);
            deletion.tenant_dims.push(tenant_id.to_string());
        }
    }

    fn erase_tenant(
        &mut self,
        tenant_id: &str,
        deletion: &mut DiskDeletion,
        sources: &mut BTreeSet<String>,
    ) -> DeleteStats {
        // The whole full-text index goes: dropping it first spares a
        // posting-list removal per claim (O(claims x terms) otherwise).
        self.text_indexes.remove(tenant_id);
        let mut ids: Vec<String> = self
            .tenant_claim_ids
            .get(tenant_id)
            .into_iter()
            .flatten()
            .cloned()
            .collect();
        ids.sort_unstable();
        let mut stats = DeleteStats::default();
        for claim_id in &ids {
            stats.add(self.remove_claim(claim_id, deletion, sources));
        }
        // Batch metadata naming any of the tenant's claims goes too: it is
        // only used to recognise a retried batch, and a retry after the
        // erasure is a new write anyway.
        let erased: HashSet<&str> = ids.iter().map(String::as_str).collect();
        let mut commits: Vec<String> = self
            .batch_commits
            .values()
            .filter(|meta| meta.claim_ids.iter().any(|id| erased.contains(id.as_str())))
            .map(|meta| meta.commit_id.clone())
            .collect();
        commits.sort_unstable();
        for commit_id in &commits {
            self.batch_commits.remove(commit_id);
        }
        deletion.batch_commits = commits;
        if self.tenant_vector_dims.remove(tenant_id).is_some() {
            deletion.tenant_dims.push(tenant_id.to_string());
        }
        self.vector_indexes.remove(tenant_id);
        self.tenant_claim_ids.remove(tenant_id);
        self.text_indexes.remove(tenant_id);
        self.entity_index.remove(tenant_id);
        self.embedding_index.remove(tenant_id);
        self.temporal_index.remove(tenant_id);
        stats
    }
}
