use std::{
    collections::HashSet,
    io::{Read, Write},
    net::TcpStream,
    time::Duration,
};

use store::{StoreError, WalReplicationDelta, WalReplicationExport};

use super::{SharedRuntime, http::HttpRequest};

const DEFAULT_REPLICATION_POLL_INTERVAL_MS: u64 = 500;
const DEFAULT_REPLICATION_MAX_RECORDS: usize = 512;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReplicationPullConfig {
    pub(crate) source_base_url: String,
    pub(crate) poll_interval: Duration,
    pub(crate) max_records: usize,
    pub(crate) token: Option<String>,
    pub(crate) local_replica_id: Option<String>,
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
        let local_replica_id =
            env_with_fallback("DASH_ROUTER_LOCAL_NODE_ID", "EME_ROUTER_LOCAL_NODE_ID")
                .or_else(|| env_with_fallback("DASH_NODE_ID", "EME_NODE_ID"))
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty());
        Some(Self {
            source_base_url,
            poll_interval: Duration::from_millis(poll_interval_ms),
            max_records,
            token,
            local_replica_id,
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

    pub(crate) fn ack_url(&self, commit_id: &str) -> Option<String> {
        let replica_id = self.local_replica_id.as_deref()?;
        Some(format!(
            "{}/internal/replication/ack?commit_id={}&replica_id={}",
            self.source_base_url,
            url_encode_component(commit_id),
            url_encode_component(replica_id)
        ))
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
    request_replication_source_with_method(url, token, "GET")
}

fn request_replication_ack(
    url: &str,
    token: Option<&str>,
) -> Result<ReplicationSourceResponse, String> {
    request_replication_source_with_method(url, token, "POST")
}

fn request_replication_source_with_method(
    url: &str,
    token: Option<&str>,
    method: &str,
) -> Result<ReplicationSourceResponse, String> {
    let (authority, path) = parse_http_url(url)?;
    let mut stream = TcpStream::connect(&authority)
        .map_err(|err| format!("failed connecting replication source '{authority}': {err}"))?;
    let mut request = format!(
        "{method} {path} HTTP/1.1\r\nHost: {authority}\r\nConnection: close\r\nContent-Length: 0\r\n"
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

pub(super) fn is_replication_request_authorized(request: &HttpRequest) -> bool {
    let Some(expected_token) = replication_token() else {
        return true;
    };
    request
        .headers
        .get("x-replication-token")
        .is_some_and(|value| value == &expected_token)
}

pub(super) fn run_replication_pull_tick(runtime: &SharedRuntime, config: &ReplicationPullConfig) {
    let from_offset = match runtime.lock() {
        Ok(guard) => guard.replication_last_offset,
        Err(_) => return,
    };
    let delta_response = match request_replication_source(
        &config.wal_pull_url(from_offset),
        config.token.as_deref(),
    ) {
        Ok(response) => response,
        Err(err) => {
            if let Ok(mut guard) = runtime.lock() {
                guard.observe_replication_pull_failure(err.clone());
            }
            eprintln!("replication pull tick failed requesting WAL delta: {err}");
            return;
        }
    };
    if delta_response.status != 200 {
        let message = format!(
            "replication source WAL delta returned status {}",
            delta_response.status
        );
        if let Ok(mut guard) = runtime.lock() {
            guard.observe_replication_pull_failure(message.clone());
        }
        eprintln!("{message}");
        return;
    }
    let delta_frame = match parse_replication_delta_frame(&delta_response.body) {
        Ok(frame) => frame,
        Err(err) => {
            if let Ok(mut guard) = runtime.lock() {
                guard.observe_replication_pull_failure(err.clone());
            }
            eprintln!("replication pull tick failed parsing WAL delta: {err}");
            return;
        }
    };
    if delta_frame.needs_resync {
        let export_response =
            match request_replication_source(&config.export_url(), config.token.as_deref()) {
                Ok(response) => response,
                Err(err) => {
                    if let Ok(mut guard) = runtime.lock() {
                        guard.observe_replication_pull_failure(err.clone());
                    }
                    eprintln!("replication resync failed requesting export: {err}");
                    return;
                }
            };
        if export_response.status != 200 {
            let message = format!(
                "replication export returned non-200 status: {}",
                export_response.status
            );
            if let Ok(mut guard) = runtime.lock() {
                guard.observe_replication_pull_failure(message.clone());
            }
            eprintln!("{message}");
            return;
        }
        let export_frame = match parse_replication_export_frame(&export_response.body) {
            Ok(frame) => frame,
            Err(err) => {
                if let Ok(mut guard) = runtime.lock() {
                    guard.observe_replication_pull_failure(err.clone());
                }
                eprintln!("replication resync failed parsing export: {err}");
                return;
            }
        };
        let mut combined_lines =
            Vec::with_capacity(export_frame.snapshot_lines.len() + export_frame.wal_lines.len());
        combined_lines.extend(export_frame.snapshot_lines.iter().cloned());
        combined_lines.extend(export_frame.wal_lines.iter().cloned());
        let commit_ids = match extract_batch_commit_ids_from_wal_lines(&combined_lines) {
            Ok(ids) => ids,
            Err(err) => {
                if let Ok(mut guard) = runtime.lock() {
                    guard.observe_replication_pull_failure(err.clone());
                }
                eprintln!("replication resync failed collecting commit ids: {err}");
                return;
            }
        };
        let result = runtime
            .lock()
            .map_err(|_| StoreError::Io("replication runtime lock unavailable".to_string()))
            .and_then(|mut guard| {
                guard.apply_replication_export(WalReplicationExport {
                    snapshot_lines: export_frame.snapshot_lines,
                    wal_lines: export_frame.wal_lines,
                })
            });
        if let Err(err) = result {
            let message = format!("replication resync apply failed: {err:?}");
            if let Ok(mut guard) = runtime.lock() {
                guard.observe_replication_pull_failure(message.clone());
            }
            eprintln!("{message}");
            return;
        }
        if let Err(err) = acknowledge_replication_commits(config, &commit_ids) {
            if let Ok(mut guard) = runtime.lock() {
                guard.observe_replication_pull_failure(err.clone());
            }
            eprintln!("replication resync commit ack failed: {err}");
        }
        return;
    }
    let commit_ids = match extract_batch_commit_ids_from_wal_lines(&delta_frame.wal_lines) {
        Ok(ids) => ids,
        Err(err) => {
            if let Ok(mut guard) = runtime.lock() {
                guard.observe_replication_pull_failure(err.clone());
            }
            eprintln!("replication delta failed collecting commit ids: {err}");
            return;
        }
    };
    let result = runtime
        .lock()
        .map_err(|_| StoreError::Io("replication runtime lock unavailable".to_string()))
        .and_then(|mut guard| {
            guard.apply_replication_delta_lines(&delta_frame.wal_lines, delta_frame.next_offset)
        });
    if let Err(err) = result {
        let message = format!("replication delta apply failed: {err:?}");
        if let Ok(mut guard) = runtime.lock() {
            guard.observe_replication_pull_failure(message.clone());
        }
        eprintln!("{message}");
        return;
    }
    if let Err(err) = acknowledge_replication_commits(config, &commit_ids) {
        if let Ok(mut guard) = runtime.lock() {
            guard.observe_replication_pull_failure(err.clone());
        }
        eprintln!("replication delta commit ack failed: {err}");
    }
}

fn acknowledge_replication_commits(
    config: &ReplicationPullConfig,
    commit_ids: &[String],
) -> Result<(), String> {
    if commit_ids.is_empty() {
        return Ok(());
    }
    if config.local_replica_id.is_none() {
        return Ok(());
    }
    for commit_id in commit_ids {
        let Some(url) = config.ack_url(commit_id) else {
            continue;
        };
        let response = request_replication_ack(&url, config.token.as_deref())?;
        if response.status != 200 {
            return Err(format!(
                "replication ack failed for commit_id '{}' with status {}",
                commit_id, response.status
            ));
        }
    }
    Ok(())
}

fn extract_batch_commit_ids_from_wal_lines(lines: &[String]) -> Result<Vec<String>, String> {
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    for line in lines {
        let Some(commit_id) = parse_batch_commit_id_from_wal_line(line)? else {
            continue;
        };
        if seen.insert(commit_id.clone()) {
            out.push(commit_id);
        }
    }
    Ok(out)
}

fn parse_batch_commit_id_from_wal_line(line: &str) -> Result<Option<String>, String> {
    if !line.starts_with("B\t") {
        return Ok(None);
    }
    let parts: Vec<&str> = line.split('\t').collect();
    if parts.len() != 5 {
        return Err("batch commit wal line has invalid field count".to_string());
    }
    Ok(Some(unescape_wal_field(parts[1])?))
}

fn unescape_wal_field(value: &str) -> Result<String, String> {
    let mut output = String::with_capacity(value.len());
    let mut escaped = false;
    for ch in value.chars() {
        if escaped {
            match ch {
                '\\' => output.push('\\'),
                't' => output.push('\t'),
                'n' => output.push('\n'),
                other => return Err(format!("invalid escape sequence in WAL field: \\{other}")),
            }
            escaped = false;
        } else if ch == '\\' {
            escaped = true;
        } else {
            output.push(ch);
        }
    }
    if escaped {
        return Err("unterminated escape sequence in WAL field".to_string());
    }
    Ok(output)
}

fn url_encode_component(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for byte in raw.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            out.push(byte as char);
        } else {
            out.push('%');
            out.push_str(&format!("{byte:02X}"));
        }
    }
    out
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

fn replication_token() -> Option<String> {
    env_with_fallback(
        "DASH_INGEST_REPLICATION_TOKEN",
        "EME_INGEST_REPLICATION_TOKEN",
    )
    .map(|value| value.trim().to_string())
    .filter(|value| !value.is_empty())
}

fn env_with_fallback(primary: &str, fallback: &str) -> Option<String> {
    std::env::var(primary)
        .ok()
        .or_else(|| std::env::var(fallback).ok())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        io::{Read, Write},
        net::TcpListener,
        sync::{Arc, Mutex, mpsc},
    };

    fn read_http_request_head(stream: &mut std::net::TcpStream) -> String {
        let mut raw = Vec::new();
        let mut chunk = [0_u8; 1024];
        loop {
            let read = stream
                .read(&mut chunk)
                .expect("mock replication source should read request bytes");
            if read == 0 {
                break;
            }
            raw.extend_from_slice(&chunk[..read]);
            if raw.windows(4).any(|window| window == b"\r\n\r\n") {
                break;
            }
        }
        String::from_utf8(raw).expect("request should be UTF-8")
    }

    fn render_http_response(status_line: &str, body: &str) -> String {
        format!(
            "{status_line}\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        )
    }

    fn spawn_mock_replication_source(
        delta_response_body: String,
        expected_requests: usize,
    ) -> (String, mpsc::Receiver<String>, std::thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .expect("mock replication source should bind a random local port");
        let address = listener
            .local_addr()
            .expect("mock replication source should have local address");
        let (request_tx, request_rx) = mpsc::channel::<String>();
        let handle = std::thread::spawn(move || {
            for _ in 0..expected_requests {
                let (mut stream, _) = listener
                    .accept()
                    .expect("mock replication source should accept connection");
                let request = read_http_request_head(&mut stream);
                let request_line = request.lines().next().unwrap_or_default().to_string();
                request_tx
                    .send(request_line.clone())
                    .expect("mock replication source should publish request line");
                let response = if request_line.starts_with("GET /internal/replication/wal?") {
                    render_http_response("HTTP/1.1 200 OK", &delta_response_body)
                } else if request_line.starts_with("POST /internal/replication/ack?") {
                    render_http_response("HTTP/1.1 200 OK", "status=ok\n")
                } else {
                    render_http_response("HTTP/1.1 404 Not Found", "status=not_found\n")
                };
                stream
                    .write_all(response.as_bytes())
                    .expect("mock replication source should write response");
                stream
                    .flush()
                    .expect("mock replication source should flush response");
            }
        });
        (format!("http://{address}"), request_rx, handle)
    }

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

    #[test]
    fn extract_batch_commit_ids_from_wal_lines_deduplicates_and_unescapes() {
        let lines = vec![
            "C\tclaim-1\ttenant-a\ttext\t0.9\tnull\t\t".to_string(),
            "B\tcommit-1\t1\t1700000000000\t2:c1".to_string(),
            "B\tcommit-1\t1\t1700000000001\t2:c1".to_string(),
            "B\tcommit\\t2\t1\t1700000000002\t2:c2".to_string(),
        ];
        let commit_ids = extract_batch_commit_ids_from_wal_lines(&lines).expect("ids should parse");
        assert_eq!(
            commit_ids,
            vec!["commit-1".to_string(), "commit\t2".to_string()]
        );
    }

    #[test]
    fn replication_pull_config_ack_url_encodes_query_values() {
        let cfg = ReplicationPullConfig {
            source_base_url: "http://127.0.0.1:8081".to_string(),
            poll_interval: Duration::from_millis(500),
            max_records: 128,
            token: None,
            local_replica_id: Some("node a".to_string()),
        };
        let url = cfg
            .ack_url("commit/1")
            .expect("ack url should be present when local replica id is set");
        assert_eq!(
            url,
            "http://127.0.0.1:8081/internal/replication/ack?commit_id=commit%2F1&replica_id=node%20a"
        );
    }

    #[test]
    fn run_replication_pull_tick_applies_delta_and_acks_commit_to_source() {
        let runtime = Arc::new(Mutex::new(super::super::IngestionRuntime::in_memory(
            store::InMemoryStore::new(),
        )));
        let delta_body = "status=ok\nneeds_resync=0\nfrom_offset=0\nnext_offset=2\ntotal_records=2\nrecords=2\nC\tclaim-1\ttenant-a\ttext\t0.9\tnull\t\t\nB\tcommit-1\t1\t1700000000000\t7:claim-1\n".to_string();
        let (source_base_url, requests, source_handle) =
            spawn_mock_replication_source(delta_body, 2);
        let config = ReplicationPullConfig {
            source_base_url,
            poll_interval: Duration::from_millis(500),
            max_records: 64,
            token: None,
            local_replica_id: Some("node-b".to_string()),
        };

        run_replication_pull_tick(&runtime, &config);

        let pull_request = requests
            .recv_timeout(Duration::from_secs(2))
            .expect("source should receive WAL pull request");
        assert!(
            pull_request
                .starts_with("GET /internal/replication/wal?from_offset=0&max_records=64 HTTP/1.1"),
            "unexpected pull request line: {pull_request}"
        );
        let ack_request = requests
            .recv_timeout(Duration::from_secs(2))
            .expect("source should receive replication ack request");
        assert!(
            ack_request.starts_with(
                "POST /internal/replication/ack?commit_id=commit-1&replica_id=node-b HTTP/1.1"
            ),
            "unexpected ack request line: {ack_request}"
        );
        source_handle
            .join()
            .expect("mock replication source should join cleanly");

        let guard = runtime
            .lock()
            .expect("replication runtime should be lockable after pull tick");
        assert_eq!(guard.claims_len(), 1);
        assert_eq!(guard.replication_last_offset, 2);
        assert_eq!(guard.replication_pull_success_total, 1);
        assert_eq!(guard.replication_pull_failure_total, 0);
    }

    #[test]
    fn run_replication_pull_tick_skips_ack_without_local_replica_id() {
        let runtime = Arc::new(Mutex::new(super::super::IngestionRuntime::in_memory(
            store::InMemoryStore::new(),
        )));
        let delta_body = "status=ok\nneeds_resync=0\nfrom_offset=0\nnext_offset=2\ntotal_records=2\nrecords=2\nC\tclaim-2\ttenant-a\ttext\t0.9\tnull\t\t\nB\tcommit-2\t1\t1700000000000\t7:claim-2\n".to_string();
        let (source_base_url, requests, source_handle) =
            spawn_mock_replication_source(delta_body, 1);
        let config = ReplicationPullConfig {
            source_base_url,
            poll_interval: Duration::from_millis(500),
            max_records: 64,
            token: None,
            local_replica_id: None,
        };

        run_replication_pull_tick(&runtime, &config);

        let pull_request = requests
            .recv_timeout(Duration::from_secs(2))
            .expect("source should receive WAL pull request");
        assert!(
            pull_request
                .starts_with("GET /internal/replication/wal?from_offset=0&max_records=64 HTTP/1.1"),
            "unexpected pull request line: {pull_request}"
        );
        assert!(
            requests.recv_timeout(Duration::from_millis(200)).is_err(),
            "source should not receive ack request when local replica id is unset"
        );
        source_handle
            .join()
            .expect("mock replication source should join cleanly");

        let guard = runtime
            .lock()
            .expect("replication runtime should be lockable after pull tick");
        assert_eq!(guard.claims_len(), 1);
        assert_eq!(guard.replication_last_offset, 2);
        assert_eq!(guard.replication_pull_success_total, 1);
        assert_eq!(guard.replication_pull_failure_total, 0);
    }

    #[test]
    fn run_replication_pull_tick_rejects_replay_payload_divergence_without_ack() {
        let runtime = Arc::new(Mutex::new(super::super::IngestionRuntime::in_memory(
            store::InMemoryStore::new(),
        )));
        {
            let mut guard = runtime
                .lock()
                .expect("runtime lock should be available for test setup");
            guard
                .store
                .observe_batch_commit(
                    "commit-diverge-1",
                    1,
                    1_700_000_000_000,
                    &["c-existing".to_string()],
                )
                .expect("initial batch commit metadata should seed runtime");
        }
        let delta_body = "status=ok\nneeds_resync=0\nfrom_offset=0\nnext_offset=1\ntotal_records=1\nrecords=1\nB\tcommit-diverge-1\t1\t1700000000001\t5:c-new\n".to_string();
        let (source_base_url, requests, source_handle) =
            spawn_mock_replication_source(delta_body, 1);
        let config = ReplicationPullConfig {
            source_base_url,
            poll_interval: Duration::from_millis(500),
            max_records: 64,
            token: None,
            local_replica_id: Some("node-b".to_string()),
        };

        run_replication_pull_tick(&runtime, &config);

        let pull_request = requests
            .recv_timeout(Duration::from_secs(2))
            .expect("source should receive WAL pull request");
        assert!(
            pull_request
                .starts_with("GET /internal/replication/wal?from_offset=0&max_records=64 HTTP/1.1"),
            "unexpected pull request line: {pull_request}"
        );
        assert!(
            requests.recv_timeout(Duration::from_millis(200)).is_err(),
            "source should not receive ack request when replication apply fails"
        );
        source_handle
            .join()
            .expect("mock replication source should join cleanly");

        let guard = runtime
            .lock()
            .expect("replication runtime should be lockable after pull tick");
        assert_eq!(guard.replication_pull_success_total, 0);
        assert_eq!(guard.replication_pull_failure_total, 1);
        let error = guard
            .replication_last_error
            .as_deref()
            .expect("replication failure should record error");
        assert!(error.contains("existing_fingerprint="));
        assert!(error.contains("incoming_fingerprint="));
    }
}
