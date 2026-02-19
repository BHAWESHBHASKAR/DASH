use std::{
    io::{Read, Write},
    net::TcpStream,
    time::Duration,
};

use store::{WalReplicationDelta, WalReplicationExport};

const DEFAULT_REPLICATION_POLL_INTERVAL_MS: u64 = 500;
const DEFAULT_REPLICATION_MAX_RECORDS: usize = 512;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReplicationPullConfig {
    pub(crate) source_base_url: String,
    pub(crate) poll_interval: Duration,
    pub(crate) max_records: usize,
    pub(crate) token: Option<String>,
}

impl ReplicationPullConfig {
    pub(crate) fn from_env() -> Option<Self> {
        let source_base_url = env_with_fallback(
            "DASH_INGEST_REPLICATION_SOURCE_URL",
            "EME_INGEST_REPLICATION_SOURCE_URL",
        )?;
        let source_base_url = source_base_url.trim().trim_end_matches('/').to_string();
        if source_base_url.is_empty() {
            return None;
        }
        let poll_interval_ms = env_with_fallback(
            "DASH_INGEST_REPLICATION_POLL_INTERVAL_MS",
            "EME_INGEST_REPLICATION_POLL_INTERVAL_MS",
        )
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_REPLICATION_POLL_INTERVAL_MS);
        let max_records = env_with_fallback(
            "DASH_INGEST_REPLICATION_MAX_RECORDS",
            "EME_INGEST_REPLICATION_MAX_RECORDS",
        )
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_REPLICATION_MAX_RECORDS);
        let token = env_with_fallback(
            "DASH_INGEST_REPLICATION_TOKEN",
            "EME_INGEST_REPLICATION_TOKEN",
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

    pub(crate) fn wal_pull_url(&self, from_offset: usize) -> String {
        format!(
            "{}/internal/replication/wal?from_offset={from_offset}&max_records={}",
            self.source_base_url, self.max_records
        )
    }

    pub(crate) fn export_url(&self) -> String {
        format!("{}/internal/replication/export", self.source_base_url)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReplicationSourceResponse {
    pub(crate) status: u16,
    pub(crate) body: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReplicationDeltaFrame {
    pub(crate) needs_resync: bool,
    pub(crate) next_offset: usize,
    pub(crate) wal_lines: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReplicationExportFrame {
    pub(crate) snapshot_lines: Vec<String>,
    pub(crate) wal_lines: Vec<String>,
}

pub(crate) fn request_replication_source(
    url: &str,
    token: Option<&str>,
) -> Result<ReplicationSourceResponse, String> {
    let (authority, path) = parse_http_url(url)?;
    let mut stream = TcpStream::connect(&authority)
        .map_err(|err| format!("failed connecting replication source '{authority}': {err}"))?;
    let mut request = format!("GET {path} HTTP/1.1\r\nHost: {authority}\r\nConnection: close\r\n");
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

pub(crate) fn render_replication_delta_frame(delta: &WalReplicationDelta) -> String {
    let mut out = format!(
        "status=ok\nneeds_resync={}\nfrom_offset={}\nnext_offset={}\ntotal_records={}\nrecords={}\n",
        if delta.needs_resync { 1 } else { 0 },
        delta.from_offset,
        delta.next_offset,
        delta.total_records,
        delta.wal_lines.len()
    );
    for line in &delta.wal_lines {
        out.push_str(line);
        out.push('\n');
    }
    out
}

pub(crate) fn parse_replication_delta_frame(body: &str) -> Result<ReplicationDeltaFrame, String> {
    let mut lines = body.lines();
    expect_kv(&mut lines, "status", "ok")?;
    let needs_resync = parse_kv_bool01(&mut lines, "needs_resync")?;
    let _from_offset = parse_kv_usize(&mut lines, "from_offset")?;
    let next_offset = parse_kv_usize(&mut lines, "next_offset")?;
    let _total_records = parse_kv_usize(&mut lines, "total_records")?;
    let records = parse_kv_usize(&mut lines, "records")?;
    let mut wal_lines = Vec::with_capacity(records);
    for _ in 0..records {
        let line = lines
            .next()
            .ok_or_else(|| "replication delta missing WAL line".to_string())?;
        wal_lines.push(line.to_string());
    }
    Ok(ReplicationDeltaFrame {
        needs_resync,
        next_offset,
        wal_lines,
    })
}

pub(crate) fn render_replication_export_frame(export: &WalReplicationExport) -> String {
    let mut out = format!(
        "status=ok\nsnapshot_records={}\nwal_records={}\nSNAPSHOT\n",
        export.snapshot_lines.len(),
        export.wal_lines.len()
    );
    for line in &export.snapshot_lines {
        out.push_str(line);
        out.push('\n');
    }
    out.push_str("WAL\n");
    for line in &export.wal_lines {
        out.push_str(line);
        out.push('\n');
    }
    out
}

pub(crate) fn parse_replication_export_frame(body: &str) -> Result<ReplicationExportFrame, String> {
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
    Ok(ReplicationExportFrame {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_and_render_replication_delta_round_trip() {
        let body = render_replication_delta_frame(&WalReplicationDelta {
            from_offset: 2,
            next_offset: 4,
            total_records: 10,
            needs_resync: false,
            wal_lines: vec![
                "C\tc1\ttenant-a\ttext\t0.9\tnull\t\t".to_string(),
                "B\tcommit-1\t1\t1700000000000\t2:c1".to_string(),
            ],
        });
        let frame = parse_replication_delta_frame(&body).expect("delta frame should parse");
        assert!(!frame.needs_resync);
        assert_eq!(frame.next_offset, 4);
        assert_eq!(frame.wal_lines.len(), 2);
    }

    #[test]
    fn parse_and_render_replication_export_round_trip() {
        let body = render_replication_export_frame(&WalReplicationExport {
            snapshot_lines: vec!["C\tc1\ttenant-a\ttext\t0.9\tnull\t\t".to_string()],
            wal_lines: vec!["E\te1\tc1\tsource://x\tsupports\t0.8\tnull\tnull\tnull".to_string()],
        });
        let frame = parse_replication_export_frame(&body).expect("export frame should parse");
        assert_eq!(frame.snapshot_lines.len(), 1);
        assert_eq!(frame.wal_lines.len(), 1);
    }

    #[test]
    fn parse_http_url_accepts_authority_and_path() {
        let (authority, path) =
            parse_http_url("http://127.0.0.1:8081/internal/replication/wal?from_offset=0")
                .expect("url should parse");
        assert_eq!(authority, "127.0.0.1:8081");
        assert_eq!(path, "/internal/replication/wal?from_offset=0");
    }
}
