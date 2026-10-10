//! Process-wide storage metrics: WAL append and `fsync` latency, bytes
//! appended, group-commit batch sizes, checkpoint duration and outcomes, and
//! vector index save/load duration.
//!
//! The values are process-global (one WAL per process in every DASH
//! deployment) and are rendered by the services' `/metrics` through
//! [`render_prometheus`]. No label is attached, so cardinality is fixed.

use std::sync::LazyLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use dash_observe::{
    BATCH_SIZE_BUCKETS, FSYNC_SECONDS_BUCKETS, Histogram, MetricsWriter,
    SLOW_OPERATION_SECONDS_BUCKETS,
};

static WAL_APPEND_SECONDS: LazyLock<Histogram> =
    LazyLock::new(|| Histogram::new(FSYNC_SECONDS_BUCKETS));
static WAL_FSYNC_SECONDS: LazyLock<Histogram> =
    LazyLock::new(|| Histogram::new(FSYNC_SECONDS_BUCKETS));
static WAL_GROUP_COMMIT_BATCH_ENTRIES: LazyLock<Histogram> =
    LazyLock::new(|| Histogram::new(BATCH_SIZE_BUCKETS));
static CHECKPOINT_SECONDS: LazyLock<Histogram> =
    LazyLock::new(|| Histogram::new(SLOW_OPERATION_SECONDS_BUCKETS));
static CHECKPOINT_PAUSE_SECONDS: LazyLock<Histogram> =
    LazyLock::new(|| Histogram::new(FSYNC_SECONDS_BUCKETS));
static VECTOR_INDEX_SAVE_SECONDS: LazyLock<Histogram> =
    LazyLock::new(|| Histogram::new(SLOW_OPERATION_SECONDS_BUCKETS));
static VECTOR_INDEX_LOAD_SECONDS: LazyLock<Histogram> =
    LazyLock::new(|| Histogram::new(SLOW_OPERATION_SECONDS_BUCKETS));

static WAL_APPENDED_BYTES_TOTAL: AtomicU64 = AtomicU64::new(0);
static WAL_FSYNC_FAILURES_TOTAL: AtomicU64 = AtomicU64::new(0);
static CHECKPOINTS_TOTAL: AtomicU64 = AtomicU64::new(0);
static CHECKPOINT_FAILURES_TOTAL: AtomicU64 = AtomicU64::new(0);
static CHECKPOINT_LAST_SUCCESS_UNIX: AtomicU64 = AtomicU64::new(0);
static CHECKPOINTS_IN_PROGRESS: AtomicU64 = AtomicU64::new(0);
static VECTOR_INDEX_SAVE_FAILURES_TOTAL: AtomicU64 = AtomicU64::new(0);

/// One WAL append unit (write plus the `fsync` the write policy asked for).
pub(crate) fn observe_wal_append(elapsed: Duration) {
    WAL_APPEND_SECONDS.observe_duration(elapsed);
}

pub(crate) fn observe_wal_fsync(elapsed: Duration, ok: bool) {
    WAL_FSYNC_SECONDS.observe_duration(elapsed);
    if !ok {
        WAL_FSYNC_FAILURES_TOTAL.fetch_add(1, Ordering::Relaxed);
    }
}

pub(crate) fn observe_wal_bytes_written(bytes: usize) {
    WAL_APPENDED_BYTES_TOTAL.fetch_add(bytes as u64, Ordering::Relaxed);
}

pub(crate) fn observe_group_commit_batch(entries: usize) {
    WAL_GROUP_COMMIT_BATCH_ENTRIES.observe(entries as f64);
}

pub(crate) fn observe_checkpoint(elapsed: Duration, ok: bool) {
    CHECKPOINT_SECONDS.observe_duration(elapsed);
    if ok {
        CHECKPOINTS_TOTAL.fetch_add(1, Ordering::Relaxed);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        CHECKPOINT_LAST_SUCCESS_UNIX.store(now, Ordering::Relaxed);
    } else {
        CHECKPOINT_FAILURES_TOTAL.fetch_add(1, Ordering::Relaxed);
    }
}

/// The part of a checkpoint that runs under the WAL lock: rotation and the
/// copy-on-write copy of the state (start) or publication (finish).
pub(crate) fn observe_checkpoint_pause(elapsed: Duration) {
    CHECKPOINT_PAUSE_SECONDS.observe_duration(elapsed);
}

pub(crate) fn observe_checkpoint_started() {
    CHECKPOINTS_IN_PROGRESS.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn observe_checkpoint_ended() {
    let _ = CHECKPOINTS_IN_PROGRESS.try_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
        Some(v.saturating_sub(1))
    });
}

/// Checkpoints whose snapshot is being written right now.
pub fn checkpoints_in_progress() -> u64 {
    CHECKPOINTS_IN_PROGRESS.load(Ordering::Relaxed)
}

pub(crate) fn observe_vector_index_save(elapsed: Duration, ok: bool) {
    VECTOR_INDEX_SAVE_SECONDS.observe_duration(elapsed);
    if !ok {
        VECTOR_INDEX_SAVE_FAILURES_TOTAL.fetch_add(1, Ordering::Relaxed);
    }
}

pub(crate) fn observe_vector_index_load(elapsed: Duration) {
    VECTOR_INDEX_LOAD_SECONDS.observe_duration(elapsed);
}

/// Checkpoints completed by this process.
pub fn checkpoints_total() -> u64 {
    CHECKPOINTS_TOTAL.load(Ordering::Relaxed)
}

/// Checkpoints that failed in this process.
pub fn checkpoint_failures_total() -> u64 {
    CHECKPOINT_FAILURES_TOTAL.load(Ordering::Relaxed)
}

/// Bytes appended to WAL files by this process.
pub fn wal_appended_bytes_total() -> u64 {
    WAL_APPENDED_BYTES_TOTAL.load(Ordering::Relaxed)
}

/// Number of WAL `fsync` calls observed by this process.
pub fn wal_fsync_count() -> u64 {
    WAL_FSYNC_SECONDS.count()
}

/// Render the storage families into `w`.
pub fn render_into(w: &mut MetricsWriter) {
    WAL_APPEND_SECONDS.render(
        w,
        "dash_wal_append_duration_seconds",
        "Latency of one WAL append unit (write plus the fsync the write policy requires).",
    );
    WAL_FSYNC_SECONDS.render(
        w,
        "dash_wal_fsync_duration_seconds",
        "Latency of WAL fdatasync calls.",
    );
    w.counter(
        "dash_wal_fsync_failures_total",
        "WAL fdatasync calls that failed (each one poisons the WAL).",
        WAL_FSYNC_FAILURES_TOTAL.load(Ordering::Relaxed) as f64,
    );
    w.counter(
        "dash_wal_appended_bytes_total",
        "Bytes appended to the WAL by this process.",
        wal_appended_bytes_total() as f64,
    );
    WAL_GROUP_COMMIT_BATCH_ENTRIES.render(
        w,
        "dash_wal_group_commit_batch_entries",
        "Requests committed per group-commit batch (one write and one fsync each).",
    );
    CHECKPOINT_SECONDS.render(
        w,
        "dash_wal_checkpoint_duration_seconds",
        "Duration of WAL checkpoints from rotation to the published snapshot, successful or not.",
    );
    CHECKPOINT_PAUSE_SECONDS.render(
        w,
        "dash_wal_checkpoint_pause_seconds",
        "Time a checkpoint holds the WAL lock: rotation plus state copy, and publication (one observation each).",
    );
    w.gauge(
        "dash_wal_checkpoint_in_progress",
        "Checkpoints whose snapshot is being written (writes continue meanwhile).",
        checkpoints_in_progress() as f64,
    );
    w.counter(
        "dash_wal_checkpoints_total",
        "WAL checkpoints completed by this process.",
        checkpoints_total() as f64,
    );
    w.counter(
        "dash_wal_checkpoint_failures_total",
        "WAL checkpoints that failed in this process.",
        checkpoint_failures_total() as f64,
    );
    w.gauge(
        "dash_wal_checkpoint_last_success_timestamp_seconds",
        "Unix time of the last successful checkpoint in this process (0 when none yet).",
        CHECKPOINT_LAST_SUCCESS_UNIX.load(Ordering::Relaxed) as f64,
    );
    VECTOR_INDEX_SAVE_SECONDS.render(
        w,
        "dash_vector_index_save_duration_seconds",
        "Duration of persisted vector index saves.",
    );
    w.counter(
        "dash_vector_index_save_failures_total",
        "Persisted vector index saves that failed.",
        VECTOR_INDEX_SAVE_FAILURES_TOTAL.load(Ordering::Relaxed) as f64,
    );
    VECTOR_INDEX_LOAD_SECONDS.render(
        w,
        "dash_vector_index_load_duration_seconds",
        "Duration of restoring the vector indexes at startup (load plus WAL catch-up, or rebuild).",
    );
}

/// The storage families as Prometheus text.
pub fn render_prometheus() -> String {
    let mut w = MetricsWriter::new();
    render_into(&mut w);
    w.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn storage_metrics_render_as_valid_exposition() {
        observe_checkpoint(Duration::from_millis(5), true);
        observe_checkpoint(Duration::from_millis(5), false);
        observe_group_commit_batch(3);
        let text = render_prometheus();
        let report = dash_observe::validate(&text).unwrap_or_else(|e| panic!("{e}\n{text}"));
        assert!(report.value("dash_wal_checkpoints_total", &[]).unwrap() >= 1.0);
        assert!(
            report
                .value("dash_wal_checkpoint_failures_total", &[])
                .unwrap()
                >= 1.0
        );
        assert!(
            report
                .value("dash_wal_group_commit_batch_entries_count", &[])
                .unwrap()
                >= 1.0
        );
        assert!(report.has_family("dash_wal_fsync_duration_seconds"));
        assert!(report.has_family("dash_vector_index_load_duration_seconds"));
    }
}
