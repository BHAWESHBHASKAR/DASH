use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use store::FileWal;

static BATCH_COMMIT_COUNTER: AtomicU64 = AtomicU64::new(1);

pub(super) fn resolve_ingest_batch_max_items(default_ingest_batch_max_items: usize) -> usize {
    parse_env_first_usize(&["DASH_INGEST_BATCH_MAX_ITEMS", "EME_INGEST_BATCH_MAX_ITEMS"])
        .filter(|value| *value > 0)
        .unwrap_or(default_ingest_batch_max_items)
}

pub(super) fn generate_batch_commit_id() -> String {
    let nonce = BATCH_COMMIT_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!(
        "commit-{}-{}-{}",
        unix_timestamp_millis(),
        std::process::id(),
        nonce
    )
}

pub(super) fn env_with_fallback(primary: &str, fallback: &str) -> Option<String> {
    std::env::var(primary)
        .ok()
        .or_else(|| std::env::var(fallback).ok())
}

pub(super) fn parse_env_first_usize(keys: &[&str]) -> Option<usize> {
    for key in keys {
        if let Ok(value) = std::env::var(key)
            && let Ok(parsed) = value.parse::<usize>()
        {
            return Some(parsed);
        }
    }
    None
}

pub(super) fn parse_env_first_u64(keys: &[&str]) -> Option<u64> {
    for key in keys {
        if let Ok(value) = std::env::var(key)
            && let Ok(parsed) = value.parse::<u64>()
        {
            return Some(parsed);
        }
    }
    None
}

pub(super) fn resolve_wal_async_flush_interval(
    wal: Option<&FileWal>,
    default_async_wal_flush_interval_ms: u64,
) -> Option<Duration> {
    let wal = wal?;
    if let Some(override_value) = parse_wal_async_flush_interval_override() {
        return override_value;
    }

    let batching_enabled = wal.background_flush_only()
        || wal.sync_every_records() > 1
        || wal.append_buffer_max_records() > 1
        || wal.sync_interval().is_some();
    if !batching_enabled {
        return None;
    }

    let default_interval = Duration::from_millis(default_async_wal_flush_interval_ms);
    Some(
        wal.sync_interval()
            .map(|value| value.min(default_interval))
            .unwrap_or(default_interval),
    )
}

pub(super) fn sanitize_path_component(raw: &str) -> String {
    let mut out: String = raw
        .chars()
        .map(|ch| match ch {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' => ch,
            _ => '_',
        })
        .collect();
    if out.is_empty() {
        out.push('_');
    }
    out
}

pub(super) fn unix_timestamp_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| value.as_millis() as u64)
        .unwrap_or(0)
}

fn parse_wal_async_flush_interval_override() -> Option<Option<Duration>> {
    for key in [
        "DASH_INGEST_WAL_ASYNC_FLUSH_INTERVAL_MS",
        "EME_INGEST_WAL_ASYNC_FLUSH_INTERVAL_MS",
    ] {
        let Ok(raw) = std::env::var(key) else {
            continue;
        };
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Some(None);
        }
        let normalized = trimmed.to_ascii_lowercase();
        if matches!(
            normalized.as_str(),
            "off" | "none" | "false" | "disabled" | "0"
        ) {
            return Some(None);
        }
        match trimmed.parse::<u64>() {
            Ok(value) if value > 0 => return Some(Some(Duration::from_millis(value))),
            Ok(_) => return Some(None),
            Err(_) => {
                eprintln!(
                    "ingestion ignoring invalid {}='{}' (expected positive integer ms or off)",
                    key, trimmed
                );
                return Some(None);
            }
        }
    }
    None
}
