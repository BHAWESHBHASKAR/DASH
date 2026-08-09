use std::{
    io::{Read, Write},
    net::TcpStream,
    sync::{Arc, RwLock},
    thread,
    time::Duration,
};

use store::{InMemoryStore, StoreError, WalReplicationDelta, WalReplicationExport};

const DEFAULT_REPLICATION_POLL_INTERVAL_MS: u64 = 1000;
const DEFAULT_REPLICATION_MAX_RECORDS: usize = 512;

/// Configuration for the retrieval follower that pulls WAL records from
/// an upstream ingestion service. If no source URL is configured the
/// follower thread is not started and retrieval serves whatever was
/// loaded from its local WAL/segments at startup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationFollowerConfig {
    source_base_url: String,
    poll_interval: Duration,
    max_records: usize,
    token: Option<String>,
}

impl ReplicationFollowerConfig {
    pub fn from_env() -> Option<Self> {
        let source_base_url = env_with_fallback(
            "DASH_RETRIEVAL_REPLICATION_SOURCE_URL",
            "EME_RETRIEVAL_REPLICATION_SOURCE_URL",
        )?;
        let source_base_url = source_base_url.trim().trim_end_matches('/').to_string();
        if source_base_url.is_empty() {
            return None;
        }
        let poll_interval_ms = env_with_fallback(
            "DASH_RETRIEVAL_REPLICATION_POLL_INTERVAL_MS",
            "EME_RETRIEVAL_REPLICATION_POLL_INTERVAL_MS",
        )
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_REPLICATION_POLL_INTERVAL_MS);
        let max_records = env_with_fallback(
            "DASH_RETRIEVAL_REPLICATION_MAX_RECORDS",
            "EME_RETRIEVAL_REPLICATION_MAX_RECORDS",
        )
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_REPLICATION_MAX_RECORDS);
        let token = env_with_fallback(
            "DASH_RETRIEVAL_REPLICATION_TOKEN",
            "EME_RETRIEVAL_REPLICATION_TOKEN",
        )
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
        Some(Self {
            source_base_url,
            poll_interval: Duration::from_millis(poll_interval_ms),
            max_records,
            token,
        })
    }

    fn wal_pull_url(&self, from_offset: usize) -> String {
        format!(
            "{}/internal/replication/wal?from_offset={from_offset}&max_records={}",
            self.source_base_url, self.max_records
        )
    }

    fn export_url(&self) -> String {
        format!("{}/internal/replication/export", self.source_base_url)
    }
}

/// Start a background thread that periodically polls the configured
/// ingestion replication source and applies new WAL records to the
/// shared in-memory store. Returns immediately if no source is
/// configured.
pub fn spawn_replication_follower(store: Arc<RwLock<InMemoryStore>>) {
    let Some(config) = ReplicationFollowerConfig::from_env() else {
        return;
    };

    eprintln!(
        "retrieval replication follower: source={}, poll_interval_ms={}",
        config.source_base_url,
        config.poll_interval.as_millis()
    );

    thread::spawn(move || {
        let mut last_offset: usize = 0;
        loop {
            match pull_and_apply(&store, &config, last_offset) {
                Ok(next_offset) => {
                    if next_offset != last_offset {
                        eprintln!(
                            "retrieval replication follower applied up to offset {next_offset}"
                        );
                    }
                    last_offset = next_offset;
                }
                Err(err) => {
                    eprintln!("retrieval replication follower error: {err}");
                }
            }
            thread::sleep(config.poll_interval);
        }
    });
}

fn pull_and_apply(
    store: &Arc<RwLock<InMemoryStore>>,
    config: &ReplicationFollowerConfig,
    from_offset: usize,
) -> Result<usize, String> {
    let delta_response =
        request_replication_source(&config.wal_pull_url(from_offset), config.token.as_deref())?;
    if delta_response.status != 200 {
        return Err(format!(
            "replication source returned status {}",
            delta_response.status
        ));
    }
    let delta_frame = parse_replication_delta_frame(&delta_response.body)?;

    if delta_frame.needs_resync {
        let export_response =
            request_replication_source(&config.export_url(), config.token.as_deref())?;
        if export_response.status != 200 {
            return Err(format!(
                "replication source export returned status {}",
                export_response.status
            ));
        }
        let export_frame = parse_replication_export_frame(&export_response.body)?;
        let mut all_lines =
            Vec::with_capacity(export_frame.snapshot_lines.len() + export_frame.wal_lines.len());
        all_lines.extend(export_frame.snapshot_lines.iter().cloned());
        all_lines.extend(export_frame.wal_lines.iter().cloned());
        apply_lines(store, &all_lines)?;
        return Ok(export_frame.wal_lines.len());
    }

    if !delta_frame.wal_lines.is_empty() {
        apply_lines(store, &delta_frame.wal_lines)?;
    }
    Ok(delta_frame.next_offset)
}

fn apply_lines(store: &Arc<RwLock<InMemoryStore>>, lines: &[String]) -> Result<(), String> {
    let mut guard = store
        .write()
        .map_err(|err| format!("failed to lock store for replication: {err}"))?;
    for line in lines {
        guard
            .apply_persisted_record_line(line)
            .map_err(|err| format!("failed to apply replicated record: {err:?}"))?;
    }
    // The in-memory WAL event vector is not used by the retrieval path,
    // but it would grow unbounded as a follower. Truncate it after each
    // batch to keep memory stable.
    guard.clear_wal_events();
    Ok(())
}

fn request_replication_source(
    url: &str,
    token: Option<&str>,
) -> Result<ReplicationSourceResponse, String> {
    let (authority, path) = parse_http_url(url)?;
    let mut stream = TcpStream::connect(&authority)
        .map_err(|err| format!("failed connecting replication source '{authority}': {err}"))?;
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .map_err(|err| format!("failed setting read timeout: {err}"))?;
    stream
        .set_write_timeout(Some(Duration::from_secs(10)))
        .map_err(|err| format!("failed setting write timeout: {err}"))?;

    let mut request = format!(
        "GET {path} HTTP/1.1\r\nHost: {authority}\r\nConnection: close\r\nContent-Length: 0\r\n"
    );
    if let Some(token) = token {
        request.push_str(&format!("x-replication-token: {token}\r\n"));
    }
    request.push_str("\r\n");
    stream
        .write_all(request.as_bytes())
        .map_err(|err| format!("failed sending replication request: {err}"))?;
    stream
        .flush()
        .map_err(|err| format!("failed flushing replication request: {err}"))?;

    let mut response_bytes = Vec::new();
    stream
        .read_to_end(&mut response_bytes)
        .map_err(|err| format!("failed reading replication response: {err}"))?;
    let response_text = String::from_utf8(response_bytes)
        .map_err(|_| "replication response is not valid UTF-8".to_string())?;
    let (header_block, body) = response_text
        .split_once("\r\n\r\n")
        .ok_or_else(|| "replication response missing HTTP header terminator".to_string())?;
    let status_line = header_block
        .lines()
        .next()
        .ok_or_else(|| "replication response missing status line".to_string())?;
    let status = status_line
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| "replication response status line missing code".to_string())
        .and_then(|value| {
            value
                .parse::<u16>()
                .map_err(|_| "replication response has invalid status code".to_string())
        })?;
    Ok(ReplicationSourceResponse {
        status,
        body: body.to_string(),
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReplicationSourceResponse {
    status: u16,
    body: String,
}

fn parse_replication_delta_frame(body: &str) -> Result<WalReplicationDelta, String> {
    let mut lines = body.lines();
    expect_kv(&mut lines, "status", "ok")?;
    let needs_resync = parse_kv_bool01(&mut lines, "needs_resync")?;
    let from_offset = parse_kv_usize(&mut lines, "from_offset")?;
    let next_offset = parse_kv_usize(&mut lines, "next_offset")?;
    let total_records = parse_kv_usize(&mut lines, "total_records")?;
    let records = parse_kv_usize(&mut lines, "records")?;
    let mut wal_lines = Vec::with_capacity(records);
    for _ in 0..records {
        let line = lines
            .next()
            .ok_or_else(|| "replication delta missing WAL line".to_string())?;
        wal_lines.push(line.to_string());
    }
    Ok(WalReplicationDelta {
        needs_resync,
        from_offset,
        next_offset,
        total_records,
        wal_lines,
    })
}

fn parse_replication_export_frame(body: &str) -> Result<WalReplicationExport, String> {
    let mut lines = body.lines();
    expect_kv(&mut lines, "status", "ok")?;
    let snapshot_records = parse_kv_usize(&mut lines, "snapshot_records")?;
    let wal_records = parse_kv_usize(&mut lines, "wal_records")?;
    let snapshot_marker = lines
        .next()
        .ok_or_else(|| "replication export missing SNAPSHOT marker".to_string())?;
    if snapshot_marker != "SNAPSHOT" {
        return Err("replication export has invalid SNAPSHOT marker".to_string());
    }
    let mut snapshot_lines = Vec::with_capacity(snapshot_records);
    for _ in 0..snapshot_records {
        let line = lines
            .next()
            .ok_or_else(|| "replication export missing snapshot line".to_string())?;
        snapshot_lines.push(line.to_string());
    }
    let wal_marker = lines
        .next()
        .ok_or_else(|| "replication export missing WAL marker".to_string())?;
    if wal_marker != "WAL" {
        return Err("replication export has invalid WAL marker".to_string());
    }
    let mut wal_lines = Vec::with_capacity(wal_records);
    for _ in 0..wal_records {
        let line = lines
            .next()
            .ok_or_else(|| "replication export missing WAL line".to_string())?;
        wal_lines.push(line.to_string());
    }
    Ok(WalReplicationExport {
        snapshot_lines,
        wal_lines,
    })
}

fn parse_http_url(url: &str) -> Result<(String, String), String> {
    let without_scheme = url
        .strip_prefix("http://")
        .ok_or_else(|| "replication source URL must start with http://".to_string())?;
    let (authority, path_and_query) = match without_scheme.split_once('/') {
        Some((authority, suffix)) => (authority, format!("/{}", suffix)),
        None => (without_scheme, "/".to_string()),
    };
    if authority.trim().is_empty() {
        return Err("replication source URL missing host:port authority".to_string());
    }
    Ok((authority.to_string(), path_and_query))
}

fn parse_kv_usize<'a, I>(lines: &mut I, key: &str) -> Result<usize, String>
where
    I: Iterator<Item = &'a str>,
{
    let line = lines
        .next()
        .ok_or_else(|| format!("replication payload missing '{key}'"))?;
    let (_, value) = parse_kv_line(line, key)?;
    value
        .parse::<usize>()
        .map_err(|_| format!("replication payload has invalid numeric value for '{key}'"))
}

fn parse_kv_bool01<'a, I>(lines: &mut I, key: &str) -> Result<bool, String>
where
    I: Iterator<Item = &'a str>,
{
    let line = lines
        .next()
        .ok_or_else(|| format!("replication payload missing '{key}'"))?;
    let (_, value) = parse_kv_line(line, key)?;
    match value {
        "0" => Ok(false),
        "1" => Ok(true),
        _ => Err(format!(
            "replication payload has invalid boolean value for '{key}'"
        )),
    }
}

fn expect_kv<'a, I>(lines: &mut I, key: &str, expected_value: &str) -> Result<(), String>
where
    I: Iterator<Item = &'a str>,
{
    let line = lines
        .next()
        .ok_or_else(|| format!("replication payload missing '{key}'"))?;
    let (_, value) = parse_kv_line(line, key)?;
    if value != expected_value {
        return Err(format!(
            "replication payload has invalid '{key}' value (expected '{expected_value}')"
        ));
    }
    Ok(())
}

fn parse_kv_line<'a>(line: &'a str, expected_key: &str) -> Result<(&'a str, &'a str), String> {
    let (key, value) = line
        .split_once('=')
        .ok_or_else(|| "replication payload has malformed key=value line".to_string())?;
    if key != expected_key {
        return Err(format!(
            "replication payload key mismatch (expected '{expected_key}', got '{key}')"
        ));
    }
    Ok((key, value))
}

fn env_with_fallback(primary: &str, fallback: &str) -> Option<String> {
    std::env::var(primary)
        .ok()
        .or_else(|| std::env::var(fallback).ok())
}

// So the module satisfies the unused-import lint when StoreError is not
// used on paths that compile out. We keep the alias for future error
// enrichment (e.g. per-record counters).
#[allow(dead_code)]
fn _store_error_ref(_: StoreError) {}
