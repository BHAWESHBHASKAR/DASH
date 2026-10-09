//! Group commit for `POST /v1/ingest`.
//!
//! A single ingest runs in three steps:
//!
//! 1. Under the runtime lock: wait until no in-flight ingest touches the same
//!    state (see [`conflict_keys`]), check the route, validate against the
//!    store, encode the WAL commit group and enqueue it on the
//!    [`GroupCommitter`]. Enqueueing under the lock makes the enqueue order
//!    the WAL order; each request gets a sequence number in that order.
//! 2. Without any lock: wait for the committer to make the batch durable.
//!    Requests that arrive meanwhile queue up and share the next fsync.
//! 3. Under the runtime lock again: wait for the request's turn (sequence
//!    order, which is WAL order), then apply to memory and redb only if the
//!    batch was durable. A failed batch leaves memory and redb untouched.
//!
//! Requests that touch the same claim, an edge target, or establish a
//! tenant's first vector dimension are serialized by the conflict keys, so
//! every validation sees the effects of every earlier WAL record it could
//! depend on: the outcome equals serial execution in WAL order.
//!
//! Every other writer (batch, raw and document ingest, replication apply)
//! first drains the pipeline ([`drain`]) and then runs exactly as before with
//! exclusive use of the WAL. A checkpoint only runs when the pipeline is idle
//! so a snapshot never misses a durable-but-unapplied record.

use std::collections::HashMap;
use std::sync::{Condvar, MutexGuard};

use store::{GroupCommitConfig, GroupCommitError, GroupCommitter, StoreError};

use super::*;

pub(super) const DEFAULT_GROUP_COMMIT_MAX_WAIT_US: u64 = 0;
pub(super) const DEFAULT_GROUP_COMMIT_MAX_BATCH_BYTES: usize = 1024 * 1024;
pub(super) const DEFAULT_GROUP_COMMIT_QUEUE_CAPACITY: usize = 1024;

/// Reads the group-commit settings. `None` when group commit is disabled.
pub(super) fn resolve_group_commit_config() -> Option<GroupCommitConfig> {
    if let Ok(raw) = std::env::var("DASH_INGEST_WAL_GROUP_COMMIT") {
        match raw.trim().to_ascii_lowercase().as_str() {
            "0" | "false" | "no" | "off" => return None,
            "1" | "true" | "yes" | "on" | "" => {}
            other => eprintln!(
                "ingestion ignoring invalid DASH_INGEST_WAL_GROUP_COMMIT='{other}' (expected true/false); group commit stays enabled"
            ),
        }
    }
    let max_wait_us = parse_env_first_u64(&["DASH_INGEST_WAL_GROUP_COMMIT_MAX_WAIT_US"])
        .unwrap_or(DEFAULT_GROUP_COMMIT_MAX_WAIT_US);
    let max_batch_bytes = parse_env_first_usize(&["DASH_INGEST_WAL_GROUP_COMMIT_MAX_BATCH_BYTES"])
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_GROUP_COMMIT_MAX_BATCH_BYTES);
    let queue_capacity = parse_env_first_usize(&["DASH_INGEST_WAL_GROUP_COMMIT_QUEUE_CAPACITY"])
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_GROUP_COMMIT_QUEUE_CAPACITY);
    Some(
        GroupCommitConfig {
            max_wait: Duration::from_micros(max_wait_us),
            max_batch_bytes,
            queue_capacity,
        }
        .normalized(),
    )
}

/// Sequencing state of in-flight pipelined ingests. Lives inside
/// [`IngestionRuntime`] and is only touched under the runtime lock; `turn` is
/// always waited on with the runtime mutex.
pub(crate) struct GroupCommitPipeline {
    committer: GroupCommitter,
    /// Sequence number the next enqueued ingest gets.
    enqueued: u64,
    /// Number of ingests whose apply step has finished (in sequence order).
    applied: u64,
    /// Conflict keys of in-flight ingests, with reference counts.
    pending_keys: HashMap<String, usize>,
    /// Writers waiting in [`drain`]; new ingests hold back while non-zero.
    drain_waiters: usize,
    turn: Arc<Condvar>,
    conflict_waits_total: u64,
    overload_reject_total: u64,
}

impl GroupCommitPipeline {
    pub(super) fn new(committer: GroupCommitter) -> Self {
        Self {
            committer,
            enqueued: 0,
            applied: 0,
            pending_keys: HashMap::new(),
            drain_waiters: 0,
            turn: Arc::new(Condvar::new()),
            conflict_waits_total: 0,
            overload_reject_total: 0,
        }
    }

    pub(super) fn committer(&self) -> &GroupCommitter {
        &self.committer
    }

    fn in_flight(&self) -> u64 {
        self.enqueued - self.applied
    }

    fn conflicts(&self, keys: &[String]) -> bool {
        keys.iter().any(|key| self.pending_keys.contains_key(key))
    }

    fn hold(&mut self, keys: &[String]) {
        for key in keys {
            *self.pending_keys.entry(key.clone()).or_insert(0) += 1;
        }
    }

    fn release(&mut self, keys: &[String]) {
        for key in keys {
            if let Some(count) = self.pending_keys.get_mut(key) {
                *count -= 1;
                if *count == 0 {
                    self.pending_keys.remove(key);
                }
            }
        }
    }

    pub(super) fn metrics_text(&self) -> String {
        let stats = self.committer.stats();
        let config = self.committer.config();
        let avg_batch = if stats.batches_total > 0 {
            stats.entries_total as f64 / stats.batches_total as f64
        } else {
            0.0
        };
        format!(
            "# TYPE dash_ingest_wal_group_commit_enabled gauge\n\
dash_ingest_wal_group_commit_enabled 1\n\
# TYPE dash_ingest_wal_group_commit_max_wait_us gauge\n\
dash_ingest_wal_group_commit_max_wait_us {}\n\
# TYPE dash_ingest_wal_group_commit_batches_total counter\n\
dash_ingest_wal_group_commit_batches_total {}\n\
# TYPE dash_ingest_wal_group_commit_entries_total counter\n\
dash_ingest_wal_group_commit_entries_total {}\n\
# TYPE dash_ingest_wal_group_commit_bytes_total counter\n\
dash_ingest_wal_group_commit_bytes_total {}\n\
# TYPE dash_ingest_wal_group_commit_failed_batches_total counter\n\
dash_ingest_wal_group_commit_failed_batches_total {}\n\
# TYPE dash_ingest_wal_group_commit_last_batch_entries gauge\n\
dash_ingest_wal_group_commit_last_batch_entries {}\n\
# TYPE dash_ingest_wal_group_commit_max_batch_entries gauge\n\
dash_ingest_wal_group_commit_max_batch_entries {}\n\
# TYPE dash_ingest_wal_group_commit_avg_batch_entries gauge\n\
dash_ingest_wal_group_commit_avg_batch_entries {:.4}\n\
# TYPE dash_ingest_wal_group_commit_queue_depth gauge\n\
dash_ingest_wal_group_commit_queue_depth {}\n\
# TYPE dash_ingest_wal_group_commit_queue_capacity gauge\n\
dash_ingest_wal_group_commit_queue_capacity {}\n\
# TYPE dash_ingest_wal_group_commit_queue_full_reject_total counter\n\
dash_ingest_wal_group_commit_queue_full_reject_total {}\n\
# TYPE dash_ingest_wal_group_commit_in_flight gauge\n\
dash_ingest_wal_group_commit_in_flight {}\n\
# TYPE dash_ingest_wal_group_commit_conflict_waits_total counter\n\
dash_ingest_wal_group_commit_conflict_waits_total {}\n",
            config.max_wait.as_micros(),
            stats.batches_total,
            stats.entries_total,
            stats.bytes_total,
            stats.failed_batches_total,
            stats.last_batch_entries,
            stats.max_batch_entries,
            avg_batch,
            stats.queue_depth,
            config.queue_capacity,
            self.overload_reject_total,
            self.in_flight(),
            self.conflict_waits_total,
        )
    }
}

/// Why a pipelined ingest did not succeed.
pub(super) enum IngestFailure {
    Route(WriteRouteError),
    Store(StoreError),
    /// The group-commit queue is full (503, retry later).
    Overloaded,
    /// The WAL is poisoned after an fsync failure (503 until restart).
    Poisoned(String),
}

impl From<StoreError> for IngestFailure {
    fn from(value: StoreError) -> Self {
        Self::Store(value)
    }
}

/// State an ingest reads besides its own claim, as lock keys. Two in-flight
/// ingests with a common key are serialized.
fn conflict_keys(store: &InMemoryStore, request: &IngestApiRequest) -> Vec<String> {
    let mut keys = vec![format!("claim\u{0}{}", request.claim.claim_id)];
    for edge in &request.edges {
        keys.push(format!("claim\u{0}{}", edge.to_claim_id));
    }
    // The first vector of a tenant establishes its dimension; later vectors
    // are checked against it. Once established it never changes, so only
    // the establishing writes need serializing.
    if request.claim_embedding.is_some()
        && store.tenant_vector_dim(&request.claim.tenant_id).is_none()
    {
        keys.push(format!("tenant-dim\u{0}{}", request.claim.tenant_id));
    }
    keys.sort();
    keys.dedup();
    keys
}

fn wait_turn<'a>(
    guard: MutexGuard<'a, IngestionRuntime>,
    turn: &Condvar,
) -> MutexGuard<'a, IngestionRuntime> {
    turn.wait(guard).unwrap_or_else(|e| e.into_inner())
}

fn pipeline(guard: &mut IngestionRuntime) -> &mut GroupCommitPipeline {
    guard
        .group_commit
        .as_mut()
        .expect("pipelined ingest requires group commit")
}

/// Waits until every in-flight pipelined ingest has been applied, so the
/// caller can use the WAL and the store exclusively (as before group commit)
/// while it holds the returned guard.
pub(super) fn drain(
    mut guard: MutexGuard<'_, IngestionRuntime>,
) -> MutexGuard<'_, IngestionRuntime> {
    let Some(pipeline) = guard.group_commit.as_mut() else {
        return guard;
    };
    if pipeline.in_flight() == 0 {
        return guard;
    }
    pipeline.drain_waiters += 1;
    let turn = Arc::clone(&pipeline.turn);
    while guard
        .group_commit
        .as_ref()
        .is_some_and(|p| p.in_flight() > 0)
    {
        guard = wait_turn(guard, &turn);
    }
    if let Some(pipeline) = guard.group_commit.as_mut() {
        pipeline.drain_waiters -= 1;
    }
    // Ingests held back by this drain re-check once the lock is released.
    turn.notify_all();
    guard
}

/// Locks the runtime and drains the pipeline (see [`drain`]).
pub(super) fn lock_drained(
    runtime: &SharedRuntime,
) -> Result<
    MutexGuard<'_, IngestionRuntime>,
    std::sync::PoisonError<MutexGuard<'_, IngestionRuntime>>,
> {
    runtime.lock().map(drain)
}

/// Runs a single ingest through group commit. Takes the runtime guard and
/// returns a guard again (the lock is released while the WAL write is in
/// flight).
pub(super) fn ingest_pipelined<'a>(
    runtime: &'a SharedRuntime,
    mut guard: MutexGuard<'a, IngestionRuntime>,
    request: IngestApiRequest,
    write_consistency: WriteConsistencyPolicy,
) -> (
    MutexGuard<'a, IngestionRuntime>,
    Result<(IngestApiResponse, WriteRouteResolution), IngestFailure>,
) {
    let turn = Arc::clone(&pipeline(&mut guard).turn);
    let keys = loop {
        let keys = conflict_keys(&guard.store, &request);
        let pipeline = pipeline(&mut guard);
        if pipeline.drain_waiters == 0 && !pipeline.conflicts(&keys) {
            break keys;
        }
        if pipeline.drain_waiters == 0 {
            pipeline.conflict_waits_total += 1;
        }
        guard = wait_turn(guard, &turn);
    };

    let route = match guard.ensure_local_write_route_for_claim(&request.claim, write_consistency) {
        Ok(route) => route,
        Err(err) => return (guard, Err(IngestFailure::Route(err))),
    };
    let claim_id = request.claim.claim_id.clone();
    let tenant_id = request.claim.tenant_id.clone();
    let prepared = match prepare(&guard, request) {
        Ok(Some(prepared)) => prepared,
        Ok(None) => {
            // Idempotent retry: nothing to write. The claim key was not in
            // flight, so the stored state for this claim is final.
            guard.successful_ingests += 1;
            let response = guard.ingest_response(claim_id, None);
            return (guard, Ok((response, route)));
        }
        Err(err) => return (guard, Err(err)),
    };

    let ticket = {
        let pipeline = pipeline(&mut guard);
        match pipeline.committer.enqueue(prepared.wal_lines().to_vec()) {
            Ok(ticket) => ticket,
            Err(GroupCommitError::Overloaded { .. }) => {
                pipeline.overload_reject_total += 1;
                return (guard, Err(IngestFailure::Overloaded));
            }
            Err(GroupCommitError::Poisoned(reason)) => {
                return (guard, Err(IngestFailure::Poisoned(reason)));
            }
            Err(other) => return (guard, Err(IngestFailure::Store(other.into()))),
        }
    };
    let seq = {
        let pipeline = pipeline(&mut guard);
        let seq = pipeline.enqueued;
        pipeline.enqueued += 1;
        pipeline.hold(&keys);
        seq
    };
    drop(guard);

    let committed = ticket.wait();

    // From here on the sequence must advance whatever happens, or every later
    // ingest would wait forever: recover a poisoned runtime mutex.
    let mut guard = runtime.lock().unwrap_or_else(|e| e.into_inner());
    while pipeline(&mut guard).applied != seq {
        guard = wait_turn(guard, &turn);
    }
    let applied = match committed {
        Ok(()) => guard
            .store
            .apply_prepared_ingest(prepared)
            .map_err(IngestFailure::Store),
        Err(GroupCommitError::Poisoned(reason)) => Err(IngestFailure::Poisoned(reason)),
        Err(err) => Err(IngestFailure::Store(err.into())),
    };
    let idle = {
        let pipeline = pipeline(&mut guard);
        pipeline.applied += 1;
        pipeline.release(&keys);
        pipeline.in_flight() == 0
    };
    let result = match applied {
        Ok(outcome) => {
            if let Some(reason) = outcome.disk_error {
                eprintln!("ingestion redb mirror failed after WAL commit: {reason}");
            }
            guard.successful_ingests += 1;
            // A snapshot must not miss a durable-but-unapplied record: only
            // the ingest that leaves the pipeline idle checkpoints.
            let checkpoint = idle.then(|| guard.checkpoint_after_commit("ingest"));
            guard.publish_segments_for_tenant(&tenant_id);
            Ok((guard.ingest_response(claim_id, checkpoint), route))
        }
        Err(err) => Err(err),
    };
    turn.notify_all();
    (guard, result)
}

fn prepare(
    guard: &IngestionRuntime,
    request: IngestApiRequest,
) -> Result<Option<store::PreparedIngest>, IngestFailure> {
    crate::api::validate_ingest_bundles(
        &guard.store,
        &[(
            &request.claim,
            request.claim_embedding.as_deref(),
            request.edges.as_slice(),
        )],
    )?;
    Ok(guard.store.prepare_atomic_ingest(
        request.claim,
        request.evidence,
        request.edges,
        request.claim_embedding,
        unix_timestamp_millis(),
    )?)
}
