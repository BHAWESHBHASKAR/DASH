//! WAL operations used by leader failover (ADR 0006). Kept in their own
//! module so the checkpoint internals in `wal.rs` stay untouched.

use super::{FileWal, generation_path_for, write_generation};
use crate::StoreError;

impl FileWal {
    /// Continue the replication lineage `generation` with this WAL.
    ///
    /// Used when a follower is promoted to leader: its WAL holds exactly
    /// the first `n` replicated lines of the old leader's generation (its
    /// cursor says so), so naming the WAL after that generation and then
    /// checkpointing records the transition `(generation, n, new)`. The old
    /// leader's other followers can then cross to the new leader with a
    /// generation switch instead of a full resync, and a follower that is
    /// ahead of `n` no longer matches any transition and resyncs. The
    /// caller must checkpoint right after this call (before serving or
    /// accepting anything) and must have verified the record count.
    ///
    /// The retained closed generation of this node's own earlier lineage
    /// is dropped: it belongs to a history no follower of the new leader
    /// can be in.
    pub fn adopt_generation(&mut self, generation: u64) -> Result<(), StoreError> {
        if generation == 0 {
            return Err(StoreError::Io("cannot adopt WAL generation 0".to_string()));
        }
        self.flush_pending_sync()?;
        if generation == self.generation {
            return Ok(());
        }
        write_generation(&generation_path_for(&self.path), generation)?;
        self.generation = generation;
        self.discard_closed_generation();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn adopted_generation_survives_reopen_and_rejects_zero() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("w.wal");
        let mut wal = FileWal::open(&path).unwrap();
        let adopted = wal.generation().wrapping_add(7).max(1);
        wal.adopt_generation(adopted).unwrap();
        assert_eq!(wal.generation(), adopted);
        assert!(wal.adopt_generation(0).is_err());
        drop(wal);
        assert_eq!(FileWal::open(&path).unwrap().generation(), adopted);
    }

    #[test]
    fn checkpoint_after_adoption_records_the_transition_from_the_adopted_generation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("w.wal");
        let mut wal = FileWal::open(&path).unwrap();
        let (generation, records) = wal.replication_position().unwrap();
        assert_eq!(records, 0);
        assert_eq!(generation, wal.generation());
        let adopted = wal.generation().wrapping_add(11).max(1);
        wal.adopt_generation(adopted).unwrap();
        let store = crate::InMemoryStore::new();
        store.checkpoint_and_compact(&mut wal).unwrap();
        let last = *wal.generation_transitions().last().expect("transition");
        assert_eq!(last.from_generation, adopted);
        assert_eq!(last.from_records, 0);
        assert_eq!(last.to_generation, wal.generation());
        assert_ne!(wal.generation(), adopted);
    }
}
