//! Startup hook used by the services.
//!
//! ```ignore
//! fn main() {
//!     dash_common::init_logging();
//!     dash_config::startup_check(dash_config::Service::Ingestion);
//!     // ...
//! }
//! ```
//!
//! [`startup_check`] must run once, at the top of `main`, before any other
//! thread exists: it applies the configuration file overlay to the process
//! environment (`std::env::set_var` is `unsafe` in edition 2024 because it
//! races with concurrent readers) and then validates the result.
//!
//! The decision logic is pure: [`plan_startup`] takes the environment as a map
//! and a file reader, so it is testable without touching the process
//! environment.

use std::collections::HashMap;

use crate::model::{Service, Setting, settings};
use crate::overlay::{FileEntry, check_permissions, parse_file, pending};
use crate::validate::{EnvSource, Report, validate_env};

/// Environment variable naming the TOML configuration file.
pub const CONFIG_FILE_ENV: &str = "DASH_CONFIG_FILE";
/// Environment variable selecting `error` (default) or `warn` validation.
pub const CONFIG_VALIDATION_ENV: &str = "DASH_CONFIG_VALIDATION";

/// A configuration file read from disk.
#[derive(Debug, Clone)]
pub struct FileData {
    pub content: String,
    /// Unix permission bits, when the platform reports them.
    pub mode: Option<u32>,
}

/// Read a file for [`plan_startup`] from the real filesystem.
pub fn read_file_from_disk(path: &str) -> Result<FileData, String> {
    let content = std::fs::read_to_string(path).map_err(|e| e.kind().to_string())?;
    #[cfg(unix)]
    let mode = {
        use std::os::unix::fs::PermissionsExt;
        std::fs::metadata(path).ok().map(|m| m.permissions().mode())
    };
    #[cfg(not(unix))]
    let mode = None;
    Ok(FileData { content, mode })
}

/// Outcome of the startup check, before any side effect.
#[derive(Debug, Clone, Default)]
pub struct StartupPlan {
    /// Findings. In warn mode errors have already been downgraded.
    pub report: Report,
    /// Variables the file fills, as (canonical name, value). Never printed.
    pub applied: Vec<(String, String)>,
    /// `DASH_CONFIG_VALIDATION=warn` is active.
    pub warn_only: bool,
    /// Entries of the file (empty when there is none or it is invalid).
    pub file_entries: Vec<FileEntry>,
}

impl StartupPlan {
    /// The process exit code the plan asks for (`Some(2)` on errors).
    pub fn exit_code(&self) -> Option<i32> {
        (!self.report.errors.is_empty()).then_some(2)
    }

    /// Names (not values) of the variables filled from the file.
    pub fn applied_names(&self) -> Vec<&str> {
        self.applied.iter().map(|(n, _)| n.as_str()).collect()
    }
}

/// Decide what startup should do for `service` given the raw environment.
pub fn plan_startup(
    service: Service,
    env: &HashMap<String, String>,
    read_file: &dyn Fn(&str) -> Result<FileData, String>,
) -> StartupPlan {
    let mut plan = StartupPlan {
        warn_only: env
            .get(CONFIG_VALIDATION_ENV)
            .is_some_and(|v| v.trim().eq_ignore_ascii_case("warn")),
        ..StartupPlan::default()
    };

    let mut effective = env.clone();
    let path = env
        .get(CONFIG_FILE_ENV)
        .map(|p| p.trim().to_string())
        .filter(|p| !p.is_empty());

    if let Some(path) = path {
        match read_file(&path) {
            Err(why) => plan
                .report
                .error(CONFIG_FILE_ENV, format!("cannot read {path}: {why}")),
            Ok(data) => match parse_file(&data.content) {
                Err(issues) => plan.report.errors.extend(issues),
                Ok(entries) => {
                    let perm = check_permissions(&entries, data.mode, &path);
                    if perm.is_empty() {
                        plan.applied = pending(service, &entries, env);
                        for (name, value) in &plan.applied {
                            effective.insert(name.clone(), value.clone());
                        }
                        plan.file_entries = entries;
                    } else {
                        plan.report.errors.extend(perm);
                    }
                }
            },
        }
    }

    plan.report.merge(validate_env(service, &effective));
    if plan.warn_only {
        plan.report.downgrade_errors();
    }
    plan
}

/// Snapshot of the `DASH_*` / `EME_*` process environment.
pub fn snapshot_process_env() -> HashMap<String, String> {
    std::env::vars_os()
        .filter_map(|(k, v)| Some((k.into_string().ok()?, v.into_string().ok()?)))
        .filter(|(k, _)| k.starts_with("DASH_") || k.starts_with("EME_"))
        .collect()
}

/// Write the overlay values into the process environment.
///
/// Call this only at the very top of `main`, before any thread is spawned.
pub fn apply_to_process_env(applied: &[(String, String)]) {
    for (name, value) in applied {
        // SAFETY: `std::env::set_var` is unsafe because it is not thread-safe
        // against concurrent environment reads or writes. The contract of
        // this function (documented above and used only through
        // `startup_check`, the first statement after logging initialization
        // in each service `main`) is that no other thread is running yet.
        unsafe { std::env::set_var(name, value) };
    }
}

/// Apply the file overlay and validate the environment of `service`.
///
/// Errors are printed to stderr and the process exits with code 2, unless
/// `DASH_CONFIG_VALIDATION=warn`. Warnings are logged. Call it once, first
/// thing in `main` (after logging initialization).
pub fn startup_check(service: Service) {
    let env = snapshot_process_env();
    let plan = plan_startup(service, &env, &read_file_from_disk);

    if plan.exit_code().is_none() {
        apply_to_process_env(&plan.applied);
    }

    let name = service.name();
    for issue in &plan.report.warnings {
        match service {
            Service::ControlPlane => eprintln!("WARNING: {name} configuration: {issue}"),
            _ => tracing::warn!("{name} configuration: {issue}"),
        }
    }
    if !plan.applied.is_empty() && plan.exit_code().is_none() {
        let names = plan.applied_names().join(", ");
        match service {
            Service::ControlPlane => {
                eprintln!("{name}: applied from {CONFIG_FILE_ENV}: {names}");
            }
            _ => tracing::info!("{name}: applied from {CONFIG_FILE_ENV}: {names}"),
        }
    }
    if plan.exit_code().is_some() {
        eprintln!(
            "{name} refusing to start: {} configuration error(s):",
            plan.report.errors.len()
        );
        for issue in &plan.report.errors {
            eprintln!("  - {issue}");
        }
        eprintln!("fix the settings above, or set {CONFIG_VALIDATION_ENV}=warn to start anyway");
        std::process::exit(2);
    }
}

// ---------------------------------------------------------------------------
// Effective values (for `dash-config print`)
// ---------------------------------------------------------------------------

/// Where an effective value comes from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Source {
    /// The environment variable with this name.
    Env(String),
    /// The configuration file.
    File,
    /// The built-in default.
    Default,
}

/// An effective setting.
#[derive(Debug, Clone)]
pub struct Resolved {
    pub setting: &'static Setting,
    /// Value in effect (`None`: unset with no default).
    pub value: Option<String>,
    pub source: Source,
}

/// The effective value of every setting `service` reads.
pub fn resolve(service: Service, env: &dyn EnvSource, entries: &[FileEntry]) -> Vec<Resolved> {
    let mut out = Vec::new();
    for setting in settings()
        .iter()
        .filter(|s| s.read_by(service) && !s.entry.planned)
    {
        let from_env = setting
            .all_names()
            .into_iter()
            .find_map(|n| env.get(n).map(|v| (n.to_string(), v)));
        let resolved = if let Some((name, value)) = from_env {
            Resolved {
                setting,
                value: Some(value),
                source: Source::Env(name),
            }
        } else if let Some(entry) = entries
            .iter()
            .find(|e| std::ptr::eq(e.setting, setting) || e.setting.name == setting.name)
        {
            Resolved {
                setting,
                value: Some(entry.value.clone()),
                source: Source::File,
            }
        } else {
            Resolved {
                setting,
                value: (!setting.default.is_empty()).then(|| setting.default.to_string()),
                source: Source::Default,
            }
        };
        out.push(resolved);
    }
    out
}
