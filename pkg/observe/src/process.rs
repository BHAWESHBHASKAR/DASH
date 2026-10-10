//! Process metrics and build information.
//!
//! On Linux the values come from `/proc/self`; on other platforms only the
//! portable gauges (start time, uptime, build info) are rendered, so a
//! dashboard never shows a fabricated zero.

use std::sync::OnceLock;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use crate::exposition::{MetricKind, MetricsWriter};

/// Git commit the binary was built from, set at compile time through the
/// `DASH_GIT_SHA` environment variable (the container build passes it);
/// `unknown` otherwise.
pub const GIT_SHA: &str = match option_env!("DASH_GIT_SHA") {
    Some(sha) => sha,
    None => "unknown",
};

/// Linux reports CPU times in `/proc/<pid>/stat` in USER_HZ, which the
/// kernel ABI fixes at 100 on every architecture DASH supports.
const USER_HZ: f64 = 100.0;

struct Start {
    instant: Instant,
    unix_seconds: f64,
}

fn start() -> &'static Start {
    static START: OnceLock<Start> = OnceLock::new();
    START.get_or_init(|| Start {
        instant: Instant::now(),
        unix_seconds: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0),
    })
}

/// Record the process start. Call early in `main`; otherwise the first
/// scrape marks the start.
pub fn mark_start() {
    let _ = start();
}

/// Sanitize a build-info label value: printable ASCII, at most 64 bytes.
fn build_label(raw: &str) -> String {
    let cleaned: String = raw
        .chars()
        .filter(|c| c.is_ascii_graphic())
        .take(64)
        .collect();
    if cleaned.is_empty() {
        "unknown".to_string()
    } else {
        cleaned
    }
}

/// Snapshot of `/proc/self` (Linux only).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ProcStats {
    pub resident_memory_bytes: Option<u64>,
    pub virtual_memory_bytes: Option<u64>,
    pub threads: Option<u64>,
    pub open_fds: Option<u64>,
    pub max_fds: Option<u64>,
    pub cpu_seconds: Option<f64>,
}

fn status_kib(status: &str, key: &str) -> Option<u64> {
    status
        .lines()
        .find_map(|line| line.strip_prefix(key))
        .and_then(|rest| rest.split_whitespace().next())
        .and_then(|v| v.parse::<u64>().ok())
}

/// Parse `utime` + `stime` (fields 14 and 15) from `/proc/self/stat`. The
/// command name (field 2) may contain spaces and parentheses, so fields are
/// counted from the last `)`.
fn parse_stat_cpu_seconds(stat: &str) -> Option<f64> {
    let after = &stat[stat.rfind(')')? + 1..];
    let fields: Vec<&str> = after.split_whitespace().collect();
    // `after` starts at field 3 (state); utime is field 14 -> index 11.
    let utime: f64 = fields.get(11)?.parse().ok()?;
    let stime: f64 = fields.get(12)?.parse().ok()?;
    Some((utime + stime) / USER_HZ)
}

fn parse_max_open_files(limits: &str) -> Option<u64> {
    let line = limits
        .lines()
        .find(|line| line.starts_with("Max open files"))?;
    let soft = line
        .trim_start_matches("Max open files")
        .split_whitespace()
        .next()?;
    soft.parse().ok()
}

pub fn proc_stats() -> ProcStats {
    let mut stats = ProcStats::default();
    if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
        stats.resident_memory_bytes = status_kib(&status, "VmRSS:").map(|k| k * 1024);
        stats.virtual_memory_bytes = status_kib(&status, "VmSize:").map(|k| k * 1024);
        stats.threads = status_kib(&status, "Threads:");
    }
    if let Ok(entries) = std::fs::read_dir("/proc/self/fd") {
        // The directory handle itself is one of the entries; do not count it.
        stats.open_fds = Some((entries.count() as u64).saturating_sub(1));
    }
    if let Ok(limits) = std::fs::read_to_string("/proc/self/limits") {
        stats.max_fds = parse_max_open_files(&limits);
    }
    if let Ok(stat) = std::fs::read_to_string("/proc/self/stat") {
        stats.cpu_seconds = parse_stat_cpu_seconds(&stat);
    }
    stats
}

/// Render the process families and `dash_build_info`.
///
/// `service` (rendered as the `component` label, which does not collide with
/// the `service` target label Kubernetes service discovery adds) and
/// `version` label `dash_build_info` (one series per process).
pub fn render(w: &mut MetricsWriter, service: &str, version: &str) {
    let start = start();
    let stats = proc_stats();
    if let Some(v) = stats.cpu_seconds {
        w.counter(
            "process_cpu_seconds_total",
            "Total user and system CPU time spent in seconds.",
            v,
        );
    }
    if let Some(v) = stats.resident_memory_bytes {
        w.gauge(
            "process_resident_memory_bytes",
            "Resident memory size in bytes.",
            v as f64,
        );
    }
    if let Some(v) = stats.virtual_memory_bytes {
        w.gauge(
            "process_virtual_memory_bytes",
            "Virtual memory size in bytes.",
            v as f64,
        );
    }
    if let Some(v) = stats.open_fds {
        w.gauge(
            "process_open_fds",
            "Number of open file descriptors.",
            v as f64,
        );
    }
    if let Some(v) = stats.max_fds {
        w.gauge(
            "process_max_fds",
            "Maximum number of open file descriptors (soft limit).",
            v as f64,
        );
    }
    if let Some(v) = stats.threads {
        w.gauge("process_threads", "Number of OS threads.", v as f64);
    }
    w.gauge(
        "process_start_time_seconds",
        "Start time of the process since unix epoch in seconds.",
        start.unix_seconds,
    );
    w.gauge(
        "dash_process_uptime_seconds",
        "Seconds since the process started.",
        start.instant.elapsed().as_secs_f64(),
    );
    w.header(
        "dash_build_info",
        "Build information; the value is always 1.",
        MetricKind::Gauge,
    );
    let service = build_label(service);
    let version = build_label(version);
    let git_sha = build_label(GIT_SHA);
    w.sample(
        "dash_build_info",
        &[
            ("component", service.as_str()),
            ("version", version.as_str()),
            ("git_sha", git_sha.as_str()),
        ],
        1.0,
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exposition::validate;

    #[test]
    fn stat_parsing_handles_command_names_with_spaces_and_parens() {
        let stat = "1234 (my (odd) proc) S 1 1234 1234 0 -1 4194560 100 0 0 0 250 50 0 0 20 0 4 0 100 1000 200";
        assert_eq!(parse_stat_cpu_seconds(stat), Some(3.0));
        assert_eq!(parse_stat_cpu_seconds("garbage"), None);
    }

    #[test]
    fn limits_parsing_reads_the_soft_limit() {
        let limits = "Limit                     Soft Limit           Hard Limit           Units\n\
Max cpu time              unlimited            unlimited            seconds\n\
Max open files            1024                 524288               files\n";
        assert_eq!(parse_max_open_files(limits), Some(1024));
    }

    #[test]
    fn rendered_process_metrics_validate_and_include_build_info() {
        mark_start();
        let mut w = MetricsWriter::new();
        render(&mut w, "test-svc", "1.2.3\"\n");
        let text = w.finish();
        let report = validate(&text).unwrap_or_else(|e| panic!("{e}\n{text}"));
        assert_eq!(
            report.value(
                "dash_build_info",
                &[
                    ("component", "test-svc"),
                    ("version", "1.2.3\""),
                    ("git_sha", GIT_SHA)
                ]
            ),
            Some(1.0)
        );
        assert!(report.has_family("dash_process_uptime_seconds"));
        if cfg!(target_os = "linux") {
            assert!(report.value("process_resident_memory_bytes", &[]).unwrap() > 0.0);
            assert!(report.value("process_open_fds", &[]).unwrap() >= 3.0);
            assert!(report.has_family("process_cpu_seconds_total"));
        }
    }
}
