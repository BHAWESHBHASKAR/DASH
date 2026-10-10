//! Store side of a checkpoint: a copy-on-write copy of the state taken while
//! the WAL rotates, written out without any lock.
//!
//! ```text
//! let job = store.begin_checkpoint(&mut wal)?;   // under the locks, short
//! drop(locks);
//! job.write()?;                                  // no lock, O(data set); publishes
//! let done = job.finish(&mut wal_locked_again)?; // under the WAL lock, short
//! drop(done);                                    // deletes retired files, no lock
//! ```
//!
//! [`InMemoryStore::checkpoint_and_compact`] runs the three steps on the
//! calling thread. See `wal::checkpoint` for the files and the crash-safety
//! argument.

use std::time::{Duration, Instant};

use schema::{Claim, ClaimEdge, Evidence};

use crate::cow_map::CowMap;
use crate::wal::{CheckpointTicket, FileWal, RetiredFiles};
use crate::{
    BatchCommitMetadata, BatchCommitRecord, ClaimVectorRecord, InMemoryStore, PersistedRecord,
    StoreError, WalCheckpointStats, observe, record_to_line,
};

/// The state a snapshot is written from, frozen at one moment. Taking it
/// clones the shard pointers of the store's maps; later writes to the store
/// copy the shards they touch and never change this copy.
#[derive(Clone)]
pub struct StoreSnapshot {
    claims: CowMap<Claim>,
    evidence_by_claim: CowMap<Vec<Evidence>>,
    edges_by_claim: CowMap<Vec<ClaimEdge>>,
    claim_vectors: CowMap<Vec<f32>>,
    batch_commits: CowMap<BatchCommitMetadata>,
}

impl StoreSnapshot {
    /// Records the snapshot holds (claims, vectors, evidence rows, edges,
    /// batch metadata).
    pub fn record_count(&self) -> usize {
        self.claims.len()
            + self.claim_vectors.len()
            + self.batch_commits.len()
            + self.evidence_by_claim.values().map(Vec::len).sum::<usize>()
            + self.edges_by_claim.values().map(Vec::len).sum::<usize>()
    }

    /// The snapshot records in file order: claims, vectors, evidence and
    /// edges by claim id, then batch metadata by commit id (the order
    /// `InMemoryStore::snapshot_records` uses).
    pub(crate) fn records(&self) -> impl Iterator<Item = PersistedRecord> + '_ {
        let mut claim_ids: Vec<&String> = self.claims.keys().collect();
        claim_ids.sort_unstable();
        let mut commit_ids: Vec<&String> = self.batch_commits.keys().collect();
        commit_ids.sort_unstable();
        let claims = claim_ids
            .clone()
            .into_iter()
            .filter_map(|id| self.claims.get(id.as_str()))
            .map(|claim| PersistedRecord::Claim(claim.clone()));
        let vectors = claim_ids.clone().into_iter().filter_map(|id| {
            self.claim_vectors.get(id.as_str()).map(|values| {
                PersistedRecord::ClaimVector(ClaimVectorRecord {
                    claim_id: id.clone(),
                    values: values.clone(),
                })
            })
        });
        let evidence = claim_ids.clone().into_iter().flat_map(|id| {
            let mut rows = self
                .evidence_by_claim
                .get(id.as_str())
                .cloned()
                .unwrap_or_default();
            rows.sort_by(|a, b| a.evidence_id.cmp(&b.evidence_id));
            rows.into_iter().map(PersistedRecord::Evidence)
        });
        let edges = claim_ids.into_iter().flat_map(|id| {
            let mut rows = self
                .edges_by_claim
                .get(id.as_str())
                .cloned()
                .unwrap_or_default();
            rows.sort_by(|a, b| a.edge_id.cmp(&b.edge_id));
            rows.into_iter().map(PersistedRecord::Edge)
        });
        let commits = commit_ids.into_iter().filter_map(|id| {
            self.batch_commits.get(id.as_str()).map(|metadata| {
                PersistedRecord::BatchCommit(BatchCommitRecord {
                    commit_id: metadata.commit_id.clone(),
                    batch_size: metadata.batch_size,
                    ts_unix_ms: metadata.ts_unix_ms,
                    claim_ids: metadata.claim_ids.clone(),
                })
            })
        });
        claims
            .chain(vectors)
            .chain(evidence)
            .chain(edges)
            .chain(commits)
    }
}

/// A published checkpoint: what it covered, and the files it no longer
/// needs (deleted when this is dropped; drop it after releasing the WAL
/// lock).
#[derive(Debug)]
pub struct FinishedCheckpoint {
    pub stats: WalCheckpointStats,
    pub retired: RetiredFiles,
}

/// A checkpoint between rotation and publication (see the module docs).
pub struct CheckpointJob {
    ticket: CheckpointTicket,
    snapshot: StoreSnapshot,
    stats: WalCheckpointStats,
    started: Instant,
    pause: Duration,
    ended: bool,
}

impl Drop for CheckpointJob {
    /// A job dropped without [`CheckpointJob::finish`] or
    /// [`CheckpointJob::abort`] (its thread died, or a test simulates a
    /// crash) still leaves the in-progress gauge. Its files keep the pending
    /// state; the WAL handle refuses a new checkpoint until it is reopened.
    fn drop(&mut self) {
        if !self.ended {
            observe::observe_checkpoint_ended();
        }
    }
}

impl CheckpointJob {
    /// What the checkpoint covers: the records its snapshot holds and the
    /// WAL records it closed.
    pub fn stats(&self) -> &WalCheckpointStats {
        &self.stats
    }

    /// The WAL generation the checkpoint started.
    pub fn generation(&self) -> u64 {
        self.ticket.generation()
    }

    /// Time spent under the caller's locks by [`InMemoryStore::begin_checkpoint`].
    pub fn pause(&self) -> Duration {
        self.pause
    }

    /// [`Self::write`] on a new thread named `thread_name`; finish it with
    /// [`BackgroundWrite::finish`]. If no thread can be started the snapshot
    /// is written on the calling thread.
    pub fn write_in_background(self, thread_name: &str) -> BackgroundWrite {
        let (tx, rx) = std::sync::mpsc::channel::<CheckpointJob>();
        let spawned = std::thread::Builder::new()
            .name(thread_name.to_string())
            .spawn(move || {
                let job = rx.recv().ok()?;
                let written = job.write_catching_panics();
                Some((job, written))
            });
        let handle = match spawned {
            Ok(handle) => handle,
            Err(err) => {
                eprintln!(
                    "could not start the checkpoint thread ({err}); writing the snapshot inline"
                );
                let written = self.write_catching_panics();
                return BackgroundWrite::Done(Box::new(self), written);
            }
        };
        match tx.send(self) {
            Ok(()) => BackgroundWrite::Thread(handle),
            Err(std::sync::mpsc::SendError(job)) => {
                let written = job.write_catching_panics();
                BackgroundWrite::Done(Box::new(job), written)
            }
        }
    }

    /// [`Self::write`], with a panic reported as an error, so the job is
    /// never lost and the WAL never stays "in flight".
    pub fn write_catching_panics(&self) -> Result<(), StoreError> {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.write())).unwrap_or_else(
            |_| {
                Err(StoreError::Io(
                    "checkpoint snapshot write panicked".to_string(),
                ))
            },
        )
    }

    /// Writes and fsyncs the snapshot file. Takes no lock: the store and the
    /// WAL keep serving writes meanwhile.
    pub fn write(&self) -> Result<(), StoreError> {
        let records = self.snapshot.records();
        let written = self
            .ticket
            .write_snapshot(records.map(|record| record_to_line(&record)))?;
        if written != self.stats.snapshot_records {
            return Err(StoreError::Io(format!(
                "checkpoint snapshot wrote {written} records, expected {}",
                self.stats.snapshot_records
            )));
        }
        // The commit point: the snapshot replaces the pending marker.
        self.ticket.publish()
    }

    /// Ends the checkpoint after a successful [`Self::write`] (see
    /// [`FileWal::finish_checkpoint`]): run it under the WAL lock, and drop
    /// (or [`RetiredFiles::delete`]) the returned files after releasing it.
    pub fn finish(mut self, wal: &mut FileWal) -> Result<FinishedCheckpoint, StoreError> {
        let started = Instant::now();
        let result = wal.finish_checkpoint(&self.ticket);
        observe::observe_checkpoint_pause(started.elapsed());
        self.end(result.is_ok());
        result.map(|retired| FinishedCheckpoint {
            stats: self.stats.clone(),
            retired,
        })
    }

    /// Gives up on the checkpoint (its write failed). The files keep the
    /// pending state; the next checkpoint supersedes it.
    pub fn abort(mut self, wal: &mut FileWal) {
        wal.abort_checkpoint(&self.ticket);
        self.end(false);
    }

    fn end(&mut self, ok: bool) {
        self.ended = true;
        observe::observe_checkpoint_ended();
        observe::observe_checkpoint(self.started.elapsed(), ok);
    }
}

/// A snapshot write running on its own thread (see
/// [`CheckpointJob::write_in_background`]).
pub enum BackgroundWrite {
    Thread(std::thread::JoinHandle<Option<(CheckpointJob, Result<(), StoreError>)>>),
    Done(Box<CheckpointJob>, Result<(), StoreError>),
}

impl BackgroundWrite {
    /// `true` once the snapshot is written (or the write failed).
    pub fn is_finished(&self) -> bool {
        match self {
            Self::Thread(handle) => handle.is_finished(),
            Self::Done(..) => true,
        }
    }

    /// Waits for the write (which publishes the snapshot), then ends the
    /// checkpoint (or, if the write failed, ends it without the snapshot).
    /// Run it with the WAL lock.
    pub fn finish(self, wal: &mut FileWal) -> Result<FinishedCheckpoint, StoreError> {
        let outcome = match self {
            Self::Thread(handle) => handle.join().ok().flatten(),
            Self::Done(job, written) => Some((*job, written)),
        };
        let Some((job, written)) = outcome else {
            // Unreachable: the thread catches panics and always receives
            // the job before it can end.
            return Err(StoreError::Io(
                "checkpoint thread ended without its job".to_string(),
            ));
        };
        match written {
            Ok(()) => job.finish(wal),
            Err(err) => {
                job.abort(wal);
                Err(err)
            }
        }
    }
}

impl InMemoryStore {
    /// A copy-on-write copy of the state a snapshot is written from.
    pub fn snapshot_state(&self) -> StoreSnapshot {
        StoreSnapshot {
            claims: self.claims.clone(),
            evidence_by_claim: self.evidence_by_claim.clone(),
            edges_by_claim: self.edges_by_claim.clone(),
            claim_vectors: self.claim_vectors.clone(),
            batch_commits: self.batch_commits.clone(),
        }
    }

    /// Starts a checkpoint: rotates the WAL ([`FileWal::begin_checkpoint`])
    /// and copies the state (copy-on-write). Call it while the store equals
    /// the WAL, holding whatever serializes WAL appends with store changes;
    /// it returns in milliseconds. Then [`CheckpointJob::write`] without the
    /// locks and [`CheckpointJob::finish`] under the WAL lock.
    pub fn begin_checkpoint(&self, wal: &mut FileWal) -> Result<CheckpointJob, StoreError> {
        let started = Instant::now();
        let ticket = match wal.begin_checkpoint() {
            Ok(ticket) => ticket,
            Err(err) => {
                observe::observe_checkpoint(started.elapsed(), false);
                return Err(err);
            }
        };
        let snapshot = self.snapshot_state();
        let stats = WalCheckpointStats {
            snapshot_records: snapshot.record_count(),
            truncated_wal_records: ticket.truncated_wal_records(),
        };
        let pause = started.elapsed();
        observe::observe_checkpoint_pause(pause);
        observe::observe_checkpoint_started();
        Ok(CheckpointJob {
            ticket,
            snapshot,
            stats,
            started,
            pause,
            ended: false,
        })
    }

    /// A whole checkpoint on the calling thread (rotation, snapshot write,
    /// publication). Services that must keep writing during the snapshot
    /// write use [`Self::begin_checkpoint`] instead.
    pub fn checkpoint_and_compact(
        &self,
        wal: &mut FileWal,
    ) -> Result<WalCheckpointStats, StoreError> {
        let job = self.begin_checkpoint(wal)?;
        match job.write() {
            Ok(()) => job.finish(wal).map(|done| done.stats.clone()),
            Err(err) => {
                job.abort(wal);
                Err(err)
            }
        }
    }
}
