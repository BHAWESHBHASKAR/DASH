//! Saving the retrieval store's vector indexes (see `store::vector_persist`
//! and ADR 0003, "Persisted vector index").
//!
//! The retrieval store records the WAL position its state reflects (set at
//! load and by the replication follower under the store's write lock), so a
//! snapshot taken under the read lock is always consistent with it. Without a
//! retrieval WAL there is no position and nothing is saved.

use std::sync::{Arc, RwLock};
use std::thread::JoinHandle;

use store::{InMemoryStore, StoreError, VectorIndexPersistence, VectorIndexSaveStats};

/// A snapshot of the vector indexes at the store's recorded WAL position.
pub fn snapshot(store: &RwLock<InMemoryStore>) -> Option<store::VectorIndexSnapshot> {
    let guard = store.read().unwrap_or_else(|p| p.into_inner());
    let position = guard.wal_position()?;
    Some(guard.vector_index_snapshot(position))
}

/// Run the periodic save loop on a background thread until
/// `persistence.stop()`.
pub fn spawn_saver(
    store: Arc<RwLock<InMemoryStore>>,
    persistence: Arc<VectorIndexPersistence>,
) -> JoinHandle<()> {
    std::thread::spawn(move || persistence.run(|| snapshot(&store), log_save))
}

/// Stop the save loop, wait for it, then save the final state (call after
/// the server stopped accepting requests).
pub fn save_on_shutdown(
    store: &RwLock<InMemoryStore>,
    persistence: &VectorIndexPersistence,
    saver: Option<JoinHandle<()>>,
) {
    persistence.stop();
    if let Some(saver) = saver {
        let _ = saver.join();
    }
    if let Some(snapshot) = snapshot(store) {
        log_save(persistence.save_if_changed(&snapshot));
    }
}

fn log_save(outcome: Result<Option<VectorIndexSaveStats>, StoreError>) {
    match outcome {
        Ok(Some(stats)) => tracing::info!(
            "retrieval vector index saved: vectors={}, tenants={}, bytes={}, wal_generation={:016x}, wal_records={}, elapsed_ms={}",
            stats.vectors,
            stats.tenants,
            stats.bytes,
            stats.position.generation,
            stats.position.records,
            stats.elapsed.as_millis()
        ),
        Ok(None) => {}
        Err(err) => tracing::warn!("retrieval vector index save failed: {err:?}"),
    }
}
