//! Synchronous replication (`DASH_INGEST_MIN_SYNC_REPLICAS`, ADR 0006).
//!
//! After a mutation succeeded locally the answer is held until at least N
//! promotable followers polled from a WAL position at or after the leader's
//! durable position right after the write (a follower's poll from offset
//! `p` proves that it fsynced its WAL and cursor up to `p`). The position is
//! taken after the write, so it may include later concurrent writes: the
//! wait can only be longer than necessary, never shorter.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use super::failover::{SyncTarget, SyncTimeoutPolicy};
use super::http::HttpResponse;
use super::{IngestionRuntime, SharedRuntime, lock_wal};

/// Counters of the synchronous wait (shared, lock-free).
#[derive(Debug, Default)]
pub(crate) struct SyncReplicationMetrics {
    waits_total: AtomicU64,
    confirmed_total: AtomicU64,
    timeouts_total: AtomicU64,
    degraded_total: AtomicU64,
    wait_micros_total: AtomicU64,
}

impl IngestionRuntime {
    /// The durable position a synchronous write waits for. `Ok(None)`
    /// without a WAL (in-memory mode has nothing to replicate).
    pub(crate) fn sync_target(&mut self) -> Result<Option<SyncTarget>, String> {
        let Some(wal) = self.wal.as_ref() else {
            return Ok(None);
        };
        let mut wal = lock_wal(wal);
        let (generation, records) = wal
            .replication_position()
            .map_err(|err| format!("cannot read the WAL position: {err:?}"))?;
        let transitions = wal
            .generation_transitions()
            .iter()
            .map(|t| (t.from_generation, t.to_generation))
            .collect();
        Ok(Some(SyncTarget {
            generation,
            records,
            transitions,
            term: self.failover.enabled.then_some(self.failover.term),
        }))
    }

    pub(crate) fn sync_replication_metrics_text(&self) -> String {
        let config = self.sync_replication;
        let metrics = &self.sync_metrics;
        format!(
            "# HELP dash_ingest_sync_replication_min_replicas Followers that must confirm a write before it is answered (0: asynchronous).\n\
# TYPE dash_ingest_sync_replication_min_replicas gauge\n\
dash_ingest_sync_replication_min_replicas {}\n\
# HELP dash_ingest_sync_replication_waits_total Writes that waited for follower confirmations.\n\
# TYPE dash_ingest_sync_replication_waits_total counter\n\
dash_ingest_sync_replication_waits_total {}\n\
# HELP dash_ingest_sync_replication_confirmed_total Writes confirmed by enough followers in time.\n\
# TYPE dash_ingest_sync_replication_confirmed_total counter\n\
dash_ingest_sync_replication_confirmed_total {}\n\
# HELP dash_ingest_sync_replication_timeouts_total Writes whose confirmations did not arrive in time.\n\
# TYPE dash_ingest_sync_replication_timeouts_total counter\n\
dash_ingest_sync_replication_timeouts_total {}\n\
# HELP dash_ingest_sync_replication_degraded_total Writes acknowledged without the required confirmations (on_timeout=degrade).\n\
# TYPE dash_ingest_sync_replication_degraded_total counter\n\
dash_ingest_sync_replication_degraded_total {}\n\
# HELP dash_ingest_sync_replication_wait_seconds_total Time writes spent waiting for confirmations.\n\
# TYPE dash_ingest_sync_replication_wait_seconds_total counter\n\
dash_ingest_sync_replication_wait_seconds_total {:.6}\n",
            config.min_replicas,
            metrics.waits_total.load(Ordering::Relaxed),
            metrics.confirmed_total.load(Ordering::Relaxed),
            metrics.timeouts_total.load(Ordering::Relaxed),
            metrics.degraded_total.load(Ordering::Relaxed),
            metrics.wait_micros_total.load(Ordering::Relaxed) as f64 / 1e6,
        )
    }
}

/// Runs after a mutation answered 2xx locally: wakes long-polling
/// followers and, with synchronous replication, holds the answer until
/// enough followers confirmed it (or applies the timeout policy).
pub(super) fn after_mutation(runtime: &SharedRuntime, response: HttpResponse) -> HttpResponse {
    let (progress, metrics, config, target) = {
        let Ok(mut rt) = runtime.lock() else {
            return response;
        };
        let config = rt.sync_replication;
        let target = if config.enabled() {
            Some(rt.sync_target())
        } else {
            None
        };
        (
            std::sync::Arc::clone(&rt.replica_progress),
            std::sync::Arc::clone(&rt.sync_metrics),
            config,
            target,
        )
    };
    progress.note_wal_advanced();
    let target = match target {
        None | Some(Ok(None)) => return response,
        Some(Ok(Some(target))) => target,
        Some(Err(reason)) => {
            eprintln!("ingestion sync replication: {reason}");
            let mut failed = HttpResponse::service_unavailable("sync_replication_unavailable");
            failed.retry_after_secs = Some(1);
            return failed;
        }
    };
    metrics.waits_total.fetch_add(1, Ordering::Relaxed);
    let started = Instant::now();
    let confirmed = progress.wait_confirmed(&target, config.min_replicas, config.timeout);
    metrics
        .wait_micros_total
        .fetch_add(started.elapsed().as_micros() as u64, Ordering::Relaxed);
    let mut response = response;
    if confirmed >= config.min_replicas {
        metrics.confirmed_total.fetch_add(1, Ordering::Relaxed);
        response
            .headers
            .push(("X-Dash-Sync-Replicas", confirmed.to_string()));
        return response;
    }
    metrics.timeouts_total.fetch_add(1, Ordering::Relaxed);
    match config.on_timeout {
        SyncTimeoutPolicy::Fail => {
            // The write is in this node's WAL and may still replicate: its
            // outcome is unknown to the client, which retries (ingest and
            // delete are idempotent by id).
            let mut failed = HttpResponse::service_unavailable(&format!(
                "sync_replication_timeout: {confirmed} of {} required followers confirmed the write within {} ms; it is stored on the leader and may still replicate, retry it",
                config.min_replicas,
                config.timeout.as_millis()
            ));
            failed.retry_after_secs = Some(1);
            failed
                .headers
                .push(("X-Dash-Sync-Replicas", confirmed.to_string()));
            failed
        }
        SyncTimeoutPolicy::Degrade => {
            metrics.degraded_total.fetch_add(1, Ordering::Relaxed);
            response.body = mark_degraded(&response.body);
            response
                .headers
                .push(("X-Dash-Sync-Replication", "degraded".to_string()));
            response
                .headers
                .push(("X-Dash-Sync-Replicas", confirmed.to_string()));
            response
        }
    }
}

/// Replaces the first `"commit_status":"..."` value with `sync_degraded`.
fn mark_degraded(body: &str) -> String {
    const KEY: &str = "\"commit_status\":\"";
    let Some(start) = body.find(KEY) else {
        return body.to_string();
    };
    let value_start = start + KEY.len();
    let Some(len) = body[value_start..].find('"') else {
        return body.to_string();
    };
    format!(
        "{}sync_degraded{}",
        &body[..value_start],
        &body[value_start + len..]
    )
}

#[cfg(test)]
mod tests {
    use super::mark_degraded;

    #[test]
    fn degraded_marker_replaces_only_the_commit_status_value() {
        assert_eq!(
            mark_degraded("{\"a\":1,\"commit_status\":\"accepted\",\"b\":\"accepted\"}"),
            "{\"a\":1,\"commit_status\":\"sync_degraded\",\"b\":\"accepted\"}"
        );
        assert_eq!(mark_degraded("{\"x\":1}"), "{\"x\":1}");
    }
}
