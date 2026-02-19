use indexer::{SegmentMaintenanceStats, maintain_segment_root};
use std::{
    env,
    path::{Path, PathBuf},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const DEFAULT_MAINTENANCE_INTERVAL_MS: u64 = 30_000;
const DEFAULT_MIN_STALE_AGE_MS: u64 = 60_000;

#[derive(Debug, Clone, PartialEq, Eq)]
struct Config {
    root_dir: PathBuf,
    interval: Duration,
    min_stale_age: Duration,
    once: bool,
}

fn main() {
    if let Err(err) = run() {
        eprintln!("segment-maintenance-daemon failed: {err}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let args: Vec<String> = env::args().skip(1).collect();
    if args.iter().any(|arg| arg == "--help" || arg == "-h") {
        println!("{}", usage_text());
        return Ok(());
    }
    let config = config_from_inputs(args, |key| env::var(key).ok())?;

    if config.once {
        let started_at = unix_timestamp_seconds();
        let stats = run_tick(&config.root_dir, config.min_stale_age)?;
        print_tick_snapshot(started_at, &config.root_dir, &stats, None);
        return Ok(());
    }

    loop {
        let tick_started_at = unix_timestamp_seconds();
        let tick_timer = Instant::now();
        match run_tick(&config.root_dir, config.min_stale_age) {
            Ok(stats) => {
                print_tick_snapshot(tick_started_at, &config.root_dir, &stats, None);
            }
            Err(err) => {
                print_tick_snapshot(
                    tick_started_at,
                    &config.root_dir,
                    &SegmentMaintenanceStats::default(),
                    Some(&err),
                );
            }
        }

        let sleep_for = config.interval.saturating_sub(tick_timer.elapsed());
        if !sleep_for.is_zero() {
            thread::sleep(sleep_for);
        }
    }
}

fn run_tick(root_dir: &Path, min_stale_age: Duration) -> Result<SegmentMaintenanceStats, String> {
    maintain_segment_root(root_dir, min_stale_age).map_err(|err| format!("{err:?}"))
}

fn usage_text() -> &'static str {
    "Usage: segment_maintenance_daemon [--once] [--root PATH] [--interval-ms N] [--min-stale-age-ms N]\n\
Defaults:\n\
  --root from DASH_INGEST_SEGMENT_DIR (fallback EME_INGEST_SEGMENT_DIR)\n\
  --interval-ms from DASH_INGEST_SEGMENT_MAINTENANCE_INTERVAL_MS (fallback EME_INGEST_SEGMENT_MAINTENANCE_INTERVAL_MS), default 30000\n\
  --min-stale-age-ms from DASH_INGEST_SEGMENT_GC_MIN_STALE_AGE_MS (fallback EME_INGEST_SEGMENT_GC_MIN_STALE_AGE_MS), default 60000"
}

fn print_tick_snapshot(
    started_at_unix: u64,
    root_dir: &Path,
    stats: &SegmentMaintenanceStats,
    error: Option<&str>,
) {
    let error_json = error
        .map(|value| format!("\"{}\"", json_escape(value)))
        .unwrap_or_else(|| "null".to_string());
    println!(
        "{{\"ts_unix\":{},\"root_dir\":\"{}\",\"tenant_dirs_scanned\":{},\"tenant_manifests_found\":{},\"pruned_file_count\":{},\"error\":{}}}",
        started_at_unix,
        json_escape(root_dir.to_string_lossy().as_ref()),
        stats.tenant_dirs_scanned,
        stats.tenant_manifests_found,
        stats.pruned_file_count,
        error_json
    );
}

fn config_from_inputs<I, F>(args: I, env_lookup: F) -> Result<Config, String>
where
    I: IntoIterator<Item = String>,
    F: Fn(&str) -> Option<String>,
{
    let mut once = false;
    let mut root_dir_override: Option<String> = None;
    let mut interval_ms_override: Option<u64> = None;
    let mut min_stale_age_ms_override: Option<u64> = None;

    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--once" => once = true,
            "--root" => {
                root_dir_override = Some(
                    args.next()
                        .ok_or_else(|| "--root requires a value".to_string())?,
                );
            }
            "--interval-ms" => {
                interval_ms_override = Some(parse_u64_arg(
                    "--interval-ms",
                    &args
                        .next()
                        .ok_or_else(|| "--interval-ms requires a value".to_string())?,
                )?);
            }
            "--min-stale-age-ms" => {
                min_stale_age_ms_override = Some(parse_u64_arg(
                    "--min-stale-age-ms",
                    &args
                        .next()
                        .ok_or_else(|| "--min-stale-age-ms requires a value".to_string())?,
                )?);
            }
            _ => {
                if let Some(value) = arg.strip_prefix("--root=") {
                    root_dir_override = Some(value.to_string());
                } else if let Some(value) = arg.strip_prefix("--interval-ms=") {
                    interval_ms_override = Some(parse_u64_arg("--interval-ms", value)?);
                } else if let Some(value) = arg.strip_prefix("--min-stale-age-ms=") {
                    min_stale_age_ms_override = Some(parse_u64_arg("--min-stale-age-ms", value)?);
                } else {
                    return Err(format!("unknown option '{arg}'"));
                }
            }
        }
    }

    let root_dir = root_dir_override
        .or_else(|| env_lookup("DASH_INGEST_SEGMENT_DIR"))
        .or_else(|| env_lookup("EME_INGEST_SEGMENT_DIR"))
        .ok_or_else(|| {
            "segment root is required (--root or DASH_INGEST_SEGMENT_DIR/EME_INGEST_SEGMENT_DIR)"
                .to_string()
        })?;
    if root_dir.trim().is_empty() {
        return Err("segment root is empty".to_string());
    }

    let interval_ms = interval_ms_override
        .or_else(|| {
            parse_env_first_u64(
                &env_lookup,
                &[
                    "DASH_INGEST_SEGMENT_MAINTENANCE_INTERVAL_MS",
                    "EME_INGEST_SEGMENT_MAINTENANCE_INTERVAL_MS",
                ],
            )
        })
        .unwrap_or(DEFAULT_MAINTENANCE_INTERVAL_MS);
    if !once && interval_ms == 0 {
        return Err("maintenance interval must be > 0 in daemon loop mode".to_string());
    }

    let min_stale_age_ms = min_stale_age_ms_override
        .or_else(|| {
            parse_env_first_u64(
                &env_lookup,
                &[
                    "DASH_INGEST_SEGMENT_GC_MIN_STALE_AGE_MS",
                    "EME_INGEST_SEGMENT_GC_MIN_STALE_AGE_MS",
                ],
            )
        })
        .unwrap_or(DEFAULT_MIN_STALE_AGE_MS);

    Ok(Config {
        root_dir: PathBuf::from(root_dir),
        interval: Duration::from_millis(interval_ms.max(1)),
        min_stale_age: Duration::from_millis(min_stale_age_ms),
        once,
    })
}

fn parse_env_first_u64<F>(env_lookup: &F, keys: &[&str]) -> Option<u64>
where
    F: Fn(&str) -> Option<String>,
{
    for key in keys {
        if let Some(raw) = env_lookup(key)
            && let Ok(value) = raw.parse::<u64>()
        {
            return Some(value);
        }
    }
    None
}

fn parse_u64_arg(flag: &str, raw: &str) -> Result<u64, String> {
    raw.parse::<u64>()
        .map_err(|_| format!("{flag} expects an integer, got '{raw}'"))
}

fn unix_timestamp_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| value.as_secs())
        .unwrap_or(0)
}

fn json_escape(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for ch in raw.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            _ => out.push(ch),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn env_lookup(values: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> = values
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect();
        move |key| map.get(key).cloned()
    }

    #[test]
    fn config_from_inputs_defaults_from_env() {
        let env = env_lookup(&[("DASH_INGEST_SEGMENT_DIR", "/tmp/segments")]);
        let config =
            config_from_inputs(Vec::<String>::new(), env).expect("config should resolve from env");

        assert_eq!(config.root_dir, PathBuf::from("/tmp/segments"));
        assert_eq!(config.interval, Duration::from_millis(30_000));
        assert_eq!(config.min_stale_age, Duration::from_millis(60_000));
        assert!(!config.once);
    }

    #[test]
    fn config_from_inputs_applies_cli_overrides() {
        let env = env_lookup(&[("DASH_INGEST_SEGMENT_DIR", "/tmp/default")]);
        let config = config_from_inputs(
            vec![
                "--once".to_string(),
                "--root".to_string(),
                "/tmp/custom".to_string(),
                "--interval-ms=5000".to_string(),
                "--min-stale-age-ms".to_string(),
                "1200".to_string(),
            ],
            env,
        )
        .expect("config should parse cli overrides");

        assert_eq!(config.root_dir, PathBuf::from("/tmp/custom"));
        assert_eq!(config.interval, Duration::from_millis(5000));
        assert_eq!(config.min_stale_age, Duration::from_millis(1200));
        assert!(config.once);
    }

    #[test]
    fn config_from_inputs_requires_segment_root() {
        let env = env_lookup(&[]);
        let err =
            config_from_inputs(Vec::<String>::new(), env).expect_err("missing root should fail");
        assert!(err.contains("segment root is required"));
    }
}
