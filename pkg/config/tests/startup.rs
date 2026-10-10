//! The startup decision (overlay before validation, exit code, warn mode) and
//! the `dash-config` command line tool.
//!
//! The decision logic takes the environment as a map and the file as a
//! closure, so nothing here depends on the process environment except the one
//! test of `apply_to_process_env`, which uses a guard and a private variable.

use std::collections::HashMap;
use std::process::Command;
use std::sync::Mutex;

use dash_config::startup::FileData;
use dash_config::{Service, Source, apply_to_process_env, plan_startup, resolve};

fn env(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

fn file(content: &str, mode: u32) -> impl Fn(&str) -> Result<FileData, String> {
    let content = content.to_string();
    move |_| {
        Ok(FileData {
            content: content.clone(),
            mode: Some(mode),
        })
    }
}

fn no_file(_: &str) -> Result<FileData, String> {
    Err("unexpected file read".to_string())
}

#[test]
fn a_valid_environment_starts() {
    let plan = plan_startup(
        Service::Ingestion,
        &env(&[("DASH_INGEST_HTTP_WORKERS", "4")]),
        &no_file,
    );
    assert_eq!(plan.exit_code(), None);
    assert!(plan.report.warnings.is_empty());
    assert!(plan.applied.is_empty());
}

#[test]
fn errors_stop_startup_with_exit_code_2_and_are_all_listed() {
    let plan = plan_startup(
        Service::Ingestion,
        &env(&[
            ("DASH_INGEST_HTTP_WORKERS", "abc"),
            ("DASH_EMBEDDING_PROVIDER", "olama"),
            ("DASH_INGEST_BIND", "8081"),
        ]),
        &no_file,
    );
    assert_eq!(plan.exit_code(), Some(2));
    assert_eq!(plan.report.errors.len(), 3, "{:?}", plan.report.errors);
}

#[test]
fn warn_mode_downgrades_errors_and_lets_the_service_start() {
    let plan = plan_startup(
        Service::Ingestion,
        &env(&[
            ("DASH_CONFIG_VALIDATION", "warn"),
            ("DASH_INGEST_HTTP_WORKERS", "abc"),
        ]),
        &no_file,
    );
    assert!(plan.warn_only);
    assert_eq!(plan.exit_code(), None);
    assert_eq!(plan.report.errors.len(), 0);
    assert!(
        plan.report
            .warnings
            .iter()
            .any(|w| w.name == "DASH_INGEST_HTTP_WORKERS")
    );
}

#[test]
fn an_invalid_validation_mode_is_an_error_and_means_strict() {
    let plan = plan_startup(
        Service::Ingestion,
        &env(&[("DASH_CONFIG_VALIDATION", "loose")]),
        &no_file,
    );
    assert!(!plan.warn_only);
    assert_eq!(plan.exit_code(), Some(2));
    assert_eq!(plan.report.errors[0].name, "DASH_CONFIG_VALIDATION");
}

#[test]
fn the_file_overlay_is_applied_before_validation() {
    // A bad value that only comes from the file is caught by validation.
    let plan = plan_startup(
        Service::Ingestion,
        &env(&[("DASH_CONFIG_FILE", "/etc/dash.toml")]),
        &file("[ingestion]\nhttp_workers = 0\n", 0o644),
    );
    assert_eq!(plan.exit_code(), Some(2));
    assert_eq!(plan.report.errors[0].name, "DASH_INGEST_HTTP_WORKERS");

    // A valid file value is applied.
    let plan = plan_startup(
        Service::Ingestion,
        &env(&[("DASH_CONFIG_FILE", "/etc/dash.toml")]),
        &file(
            "[ingestion]\nhttp_workers = 6\nwal_path = \"/x.wal\"\n",
            0o644,
        ),
    );
    assert_eq!(plan.exit_code(), None);
    assert_eq!(
        plan.applied,
        [
            ("DASH_INGEST_HTTP_WORKERS".to_string(), "6".to_string()),
            ("DASH_INGEST_WAL_PATH".to_string(), "/x.wal".to_string()),
        ]
    );
}

#[test]
fn an_environment_value_overrides_a_bad_file_value() {
    let plan = plan_startup(
        Service::Ingestion,
        &env(&[
            ("DASH_CONFIG_FILE", "/etc/dash.toml"),
            ("DASH_INGEST_HTTP_WORKERS", "4"),
        ]),
        &file("[ingestion]\nhttp_workers = 0\n", 0o644),
    );
    assert_eq!(plan.exit_code(), None, "{:?}", plan.report.errors);
    assert!(plan.applied.is_empty());
}

#[test]
fn an_unreadable_file_is_an_error() {
    let plan = plan_startup(
        Service::Retrieval,
        &env(&[("DASH_CONFIG_FILE", "/missing.toml")]),
        &|_| Err("entity not found".to_string()),
    );
    assert_eq!(plan.exit_code(), Some(2));
    assert!(plan.report.errors[0].message.contains("/missing.toml"));
}

#[test]
fn a_bad_file_applies_nothing() {
    let plan = plan_startup(
        Service::Ingestion,
        &env(&[("DASH_CONFIG_FILE", "/etc/dash.toml")]),
        &file("[ingestion]\nwal_path = \"/x\"\nwal_paht = \"/y\"\n", 0o644),
    );
    assert_eq!(plan.exit_code(), Some(2));
    assert!(plan.applied.is_empty());
}

#[test]
fn a_world_readable_file_with_secrets_is_refused_and_not_applied() {
    let toml = "[ingestion]\napi_key = \"0123456789abcdef0123456789abcdef\"\n";
    let plan = plan_startup(
        Service::Ingestion,
        &env(&[("DASH_CONFIG_FILE", "/etc/dash.toml")]),
        &file(toml, 0o644),
    );
    assert_eq!(plan.exit_code(), Some(2));
    assert!(plan.applied.is_empty());
    let text = format!("{:?}", plan.report);
    assert!(!text.contains("0123456789abcdef"), "{text}");

    let plan = plan_startup(
        Service::Ingestion,
        &env(&[("DASH_CONFIG_FILE", "/etc/dash.toml")]),
        &file(toml, 0o600),
    );
    assert_eq!(plan.exit_code(), None);
    assert_eq!(plan.applied_names(), ["DASH_INGEST_API_KEY"]);
}

#[test]
fn file_errors_are_downgraded_by_warn_mode_but_still_apply_nothing() {
    let plan = plan_startup(
        Service::Ingestion,
        &env(&[
            ("DASH_CONFIG_FILE", "/etc/dash.toml"),
            ("DASH_CONFIG_VALIDATION", "warn"),
        ]),
        &file("[ingestion]\nwal_paht = \"/y\"\n", 0o644),
    );
    assert_eq!(plan.exit_code(), None);
    assert!(plan.applied.is_empty());
    assert!(!plan.report.warnings.is_empty());
}

#[test]
fn a_blank_config_file_variable_is_not_a_file_read() {
    let plan = plan_startup(
        Service::Ingestion,
        &env(&[("DASH_CONFIG_FILE", "")]),
        &no_file,
    );
    // Blank is reported by validation, but no file is read.
    assert_eq!(plan.exit_code(), Some(2));
    assert_eq!(plan.report.errors.len(), 1);
}

#[test]
fn legacy_prefix_variables_still_warn_during_startup() {
    let plan = plan_startup(
        Service::Ingestion,
        &env(&[("EME_INGEST_BIND", "127.0.0.1:9")]),
        &no_file,
    );
    assert_eq!(plan.exit_code(), None);
    assert_eq!(plan.report.warnings.len(), 1);
}

#[test]
fn resolve_reports_the_source_of_every_value() {
    let entries = dash_config::overlay::parse_file(
        "[ingestion]\nwal_path = \"/file.wal\"\nhttp_workers = 2\napi_key = \"from-file-secret-0123\"\n",
    )
    .unwrap();
    let map = env(&[("EME_INGEST_HTTP_WORKERS", "5")]);
    let resolved = resolve(Service::Ingestion, &map, &entries);
    let get = |name: &str| resolved.iter().find(|r| r.setting.name == name).unwrap();

    assert_eq!(get("DASH_INGEST_WAL_PATH").source, Source::File);
    assert_eq!(
        get("DASH_INGEST_WAL_PATH").value.as_deref(),
        Some("/file.wal")
    );
    // Environment (under its legacy spelling) wins over the file.
    assert_eq!(
        get("DASH_INGEST_HTTP_WORKERS").source,
        Source::Env("EME_INGEST_HTTP_WORKERS".into())
    );
    assert_eq!(get("DASH_INGEST_HTTP_WORKERS").value.as_deref(), Some("5"));
    // Defaults are reported as such.
    assert_eq!(get("DASH_INGEST_BIND").source, Source::Default);
    assert_eq!(
        get("DASH_INGEST_BIND").value.as_deref(),
        Some("127.0.0.1:8081")
    );
    // Unset with no default.
    assert_eq!(
        get("DASH_INGEST_JWT_AUDIENCE").value.as_deref(),
        Some("unset (HS256), **required** (OIDC)")
    );
    assert!(
        resolved
            .iter()
            .all(|r| r.setting.read_by(Service::Ingestion))
    );
}

static ENV_LOCK: Mutex<()> = Mutex::new(());

/// Removes a process variable on drop, holding the lock that serializes
/// tests which touch the process environment.
struct ProcessVar<'a> {
    name: &'static str,
    _guard: std::sync::MutexGuard<'a, ()>,
}

impl<'a> ProcessVar<'a> {
    fn new(name: &'static str) -> Self {
        let guard = ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        // SAFETY: serialized by ENV_LOCK; no other test in this binary reads
        // or writes this private variable, and the other tests here use maps.
        unsafe { std::env::remove_var(name) };
        ProcessVar {
            name,
            _guard: guard,
        }
    }
}

impl Drop for ProcessVar<'_> {
    fn drop(&mut self) {
        // SAFETY: see `new`; the lock is still held until the guard drops.
        unsafe { std::env::remove_var(self.name) };
    }
}

#[test]
fn apply_to_process_env_sets_the_variables() {
    let var = ProcessVar::new("DASH_TEST_PROBE_FOR_APPLY");
    apply_to_process_env(&[(var.name.to_string(), "value-1".to_string())]);
    assert_eq!(std::env::var(var.name).as_deref(), Ok("value-1"));
}

// ---------------------------------------------------------------------------
// The command line tool
// ---------------------------------------------------------------------------

fn dash_config(args: &[&str], envs: &[(&str, &str)]) -> (i32, String, String) {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_dash-config"));
    cmd.args(args).env_clear();
    cmd.env("PATH", std::env::var("PATH").unwrap_or_default());
    cmd.current_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/../.."));
    for (k, v) in envs {
        cmd.env(k, v);
    }
    let out = cmd.output().expect("run dash-config");
    (
        out.status.code().unwrap_or(-1),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

#[test]
fn cli_validate_exits_0_for_a_good_environment_and_2_for_a_bad_one() {
    let (code, out, _) = dash_config(
        &["validate", "--service", "ingestion"],
        &[("DASH_INGEST_HTTP_WORKERS", "4")],
    );
    assert_eq!(code, 0, "{out}");
    assert!(out.contains("0 error(s)"));

    let (code, out, _) = dash_config(
        &["validate", "--service", "retrieval"],
        &[
            ("DASH_RETRIEVAL_HTTP_WORKERS", "abc"),
            ("DASH_RETRIEVAL_WAL_PAHT", "/x"),
        ],
    );
    assert_eq!(code, 2, "{out}");
    assert!(out.contains("error: DASH_RETRIEVAL_HTTP_WORKERS"));
    assert!(out.contains("did you mean DASH_RETRIEVAL_WAL_PATH?"));
}

#[test]
fn cli_validate_is_strict_even_when_the_environment_asks_for_warn() {
    let (code, _, _) = dash_config(
        &["validate", "--service", "ingestion"],
        &[
            ("DASH_CONFIG_VALIDATION", "warn"),
            ("DASH_INGEST_HTTP_WORKERS", "abc"),
        ],
    );
    assert_eq!(code, 2);
}

#[test]
fn cli_print_shows_sources_and_never_prints_secrets() {
    let secret = "0123456789abcdef-super-secret-value";
    let (code, out, _) = dash_config(
        &["print", "--service", "ingestion"],
        &[
            ("DASH_INGEST_API_KEY", secret),
            ("DASH_INGEST_HTTP_WORKERS", "4"),
        ],
    );
    assert_eq!(code, 0);
    assert!(!out.contains(secret), "secret leaked:\n{out}");
    assert!(out.contains("DASH_INGEST_API_KEY=<redacted>  # env DASH_INGEST_API_KEY"));
    assert!(out.contains("DASH_INGEST_HTTP_WORKERS=4  # env DASH_INGEST_HTTP_WORKERS"));
    assert!(out.contains("DASH_INGEST_BIND=127.0.0.1:8081  # default"));
    // Settings of other services are not listed.
    assert!(!out.contains("DASH_RETRIEVAL_BIND"));
}

#[test]
fn cli_print_redacted_hides_every_configured_value_but_keeps_defaults() {
    let (code, out, _) = dash_config(
        &["print", "--service", "ingestion", "--redacted"],
        &[("DASH_INGEST_WAL_PATH", "/very/private/path.wal")],
    );
    assert_eq!(code, 0);
    assert!(!out.contains("/very/private/path.wal"));
    assert!(out.contains("DASH_INGEST_WAL_PATH=<redacted>  # env DASH_INGEST_WAL_PATH"));
    assert!(out.contains("DASH_INGEST_BIND=127.0.0.1:8081  # default"));
}

#[test]
fn cli_print_reads_the_config_file_and_reports_the_file_as_source() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("dash.toml");
    std::fs::write(&path, "[ingestion]\nwal_path = \"/from-file.wal\"\n").unwrap();
    let (code, out, err) = dash_config(
        &["print", "--service", "ingestion"],
        &[("DASH_CONFIG_FILE", path.to_str().unwrap())],
    );
    assert_eq!(code, 0, "{err}");
    assert!(
        out.contains("DASH_INGEST_WAL_PATH=/from-file.wal  # file"),
        "{out}"
    );
}

#[test]
fn cli_list_filters_by_service() {
    let (code, out, _) = dash_config(&["list", "--service", "control-plane"], &[]);
    assert_eq!(code, 0);
    assert!(out.contains("DASH_CONTROL_PLANE_BIND"));
    assert!(out.contains("DASH_INSECURE_DEV_MODE"));
    assert!(!out.contains("DASH_INGEST_WAL_PATH"));
}

#[test]
fn cli_rejects_a_missing_or_unknown_service() {
    assert_eq!(dash_config(&["validate"], &[]).0, 64);
    assert_eq!(dash_config(&["validate", "--service", "nope"], &[]).0, 64);
    assert_eq!(dash_config(&["frobnicate"], &[]).0, 64);
}

#[test]
fn cli_docs_check_passes_on_the_committed_page_and_fails_on_a_stale_one() {
    let (code, out, err) = dash_config(&["docs", "--check"], &[]);
    assert_eq!(code, 0, "{out}{err}");

    let dir = tempfile::tempdir().unwrap();
    let stale = dir.path().join("configuration.md");
    std::fs::write(&stale, "# Configuration\n\nhand edited\n").unwrap();
    let (code, _, err) = dash_config(&["docs", "--check", "--path", stale.to_str().unwrap()], &[]);
    assert_eq!(code, 1);
    assert!(err.contains("differs from the settings registry"), "{err}");

    let missing = dir.path().join("nope.md");
    let (code, _, _) = dash_config(
        &["docs", "--check", "--path", missing.to_str().unwrap()],
        &[],
    );
    assert_eq!(code, 1);
}

#[test]
fn cli_docs_writes_the_page() {
    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("out.md");
    let (code, _, _) = dash_config(&["docs", "--path", target.to_str().unwrap()], &[]);
    assert_eq!(code, 0);
    assert_eq!(
        std::fs::read_to_string(&target).unwrap(),
        dash_config::docs::render()
    );
}
