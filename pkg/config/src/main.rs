//! `dash-config`: validate, print and document DASH configuration.

use std::process::ExitCode;

use dash_config::docs::{self, CheckOutcome, DOC_PATH};
use dash_config::startup::read_file_from_disk;
use dash_config::{
    CONFIG_VALIDATION_ENV, ProcessEnv, Scope, Service, Source, plan_startup, resolve, settings,
    snapshot_process_env,
};

const USAGE: &str = "\
dash-config: validate, print and document DASH configuration

USAGE:
    dash-config validate --service <ingestion|retrieval|control-plane>
    dash-config print    --service <ingestion|retrieval|control-plane> [--redacted]
    dash-config docs     [--check] [--path <file>]
    dash-config list     [--service <name>]

validate reads the current environment and DASH_CONFIG_FILE, prints every
error and warning, and exits 0 (no errors) or 2 (errors).
print shows the effective value of every setting and where it comes from
(env, file or default). Secrets are always redacted; --redacted also hides
every explicitly configured value.
docs renders the configuration reference page; with --check it exits 1 when
the committed page differs from what the registry generates.
list prints the registry.
";

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let Some((command, rest)) = args.split_first() else {
        eprint!("{USAGE}");
        return ExitCode::from(64);
    };
    match command.as_str() {
        "validate" => cmd_validate(rest),
        "print" => cmd_print(rest),
        "docs" => cmd_docs(rest),
        "list" => cmd_list(rest),
        "-h" | "--help" | "help" => {
            print!("{USAGE}");
            ExitCode::SUCCESS
        }
        other => {
            eprintln!("unknown command '{other}'\n\n{USAGE}");
            ExitCode::from(64)
        }
    }
}

struct Flags {
    service: Option<String>,
    redacted: bool,
    check: bool,
    path: Option<String>,
}

fn parse_flags(args: &[String]) -> Result<Flags, String> {
    let mut flags = Flags {
        service: None,
        redacted: false,
        check: false,
        path: None,
    };
    let mut it = args.iter();
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--service" => {
                flags.service = Some(it.next().ok_or("--service needs a value")?.clone());
            }
            s if s.starts_with("--service=") => {
                flags.service = Some(s["--service=".len()..].to_string());
            }
            "--redacted" => flags.redacted = true,
            "--check" => flags.check = true,
            "--path" => flags.path = Some(it.next().ok_or("--path needs a value")?.clone()),
            other => return Err(format!("unknown argument '{other}'")),
        }
    }
    Ok(flags)
}

fn service_arg(flags: &Flags) -> Result<Service, String> {
    let raw = flags
        .service
        .as_deref()
        .ok_or("--service <ingestion|retrieval|control-plane> is required")?;
    Service::parse(raw).ok_or_else(|| format!("unknown service '{raw}'"))
}

fn usage_error(message: &str) -> ExitCode {
    eprintln!("dash-config: {message}\n\n{USAGE}");
    ExitCode::from(64)
}

fn cmd_validate(args: &[String]) -> ExitCode {
    let flags = match parse_flags(args) {
        Ok(f) => f,
        Err(e) => return usage_error(&e),
    };
    let service = match service_arg(&flags) {
        Ok(s) => s,
        Err(e) => return usage_error(&e),
    };
    let mut env = snapshot_process_env();
    // The command line check is always strict.
    env.remove(CONFIG_VALIDATION_ENV);
    let plan = plan_startup(service, &env, &read_file_from_disk);
    for issue in &plan.report.warnings {
        println!("warning: {issue}");
    }
    for issue in &plan.report.errors {
        println!("error: {issue}");
    }
    println!(
        "{}: {} error(s), {} warning(s)",
        service.name(),
        plan.report.errors.len(),
        plan.report.warnings.len()
    );
    if plan.report.errors.is_empty() {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(2)
    }
}

fn cmd_print(args: &[String]) -> ExitCode {
    let flags = match parse_flags(args) {
        Ok(f) => f,
        Err(e) => return usage_error(&e),
    };
    let service = match service_arg(&flags) {
        Ok(s) => s,
        Err(e) => return usage_error(&e),
    };
    let mut env = snapshot_process_env();
    env.remove(CONFIG_VALIDATION_ENV);
    let plan = plan_startup(service, &env, &read_file_from_disk);
    if !plan.report.errors.is_empty() {
        for issue in &plan.report.errors {
            eprintln!("error: {issue}");
        }
        return ExitCode::from(2);
    }
    for r in resolve(service, &ProcessEnv, &plan.file_entries) {
        let secret = r.setting.kind().is_secret();
        let value = match (&r.value, &r.source) {
            (None, _) => "(unset)".to_string(),
            (Some(_), _) if secret => "<redacted>".to_string(),
            (Some(_), Source::Env(_) | Source::File) if flags.redacted => "<redacted>".to_string(),
            (Some(v), _) => v.clone(),
        };
        let source = match &r.source {
            Source::Env(name) => format!("env {name}"),
            Source::File => "file".to_string(),
            Source::Default => "default".to_string(),
        };
        println!("{}={}  # {}", r.setting.name, value, source);
    }
    ExitCode::SUCCESS
}

fn cmd_docs(args: &[String]) -> ExitCode {
    let flags = match parse_flags(args) {
        Ok(f) => f,
        Err(e) => return usage_error(&e),
    };
    let path = flags.path.as_deref().unwrap_or(DOC_PATH);
    if flags.check {
        let committed = std::fs::read_to_string(path).ok();
        return match docs::check(committed.as_deref()) {
            CheckOutcome::UpToDate => {
                println!("config docs OK: {path} matches the settings registry");
                ExitCode::SUCCESS
            }
            CheckOutcome::Missing => {
                eprintln!("FAIL: configuration reference not found: {path}");
                ExitCode::from(1)
            }
            CheckOutcome::Stale {
                first_difference_line,
            } => {
                eprintln!(
                    "config docs check FAILED: {path} differs from the settings registry (first difference at line {first_difference_line}).\nRegenerate it with: cargo run -p dash-config -- docs"
                );
                ExitCode::from(1)
            }
        };
    }
    match std::fs::write(path, docs::render()) {
        Ok(()) => {
            println!("wrote {path}");
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("cannot write {path}: {e}");
            ExitCode::from(1)
        }
    }
}

fn cmd_list(args: &[String]) -> ExitCode {
    let flags = match parse_flags(args) {
        Ok(f) => f,
        Err(e) => return usage_error(&e),
    };
    let filter = match flags.service.as_deref() {
        Some(raw) => match Service::parse(raw) {
            Some(s) => Some(s),
            None => return usage_error(&format!("unknown service '{raw}'")),
        },
        None => None,
    };
    for scope in Scope::ALL {
        for s in settings()
            .iter()
            .filter(|s| s.scope == scope && filter.is_none_or(|svc| s.read_by(svc)))
        {
            let default = if s.default.is_empty() {
                "unset"
            } else {
                s.default
            };
            println!(
                "{}\t{}\t{}\t{}",
                s.name,
                scope.table(),
                s.kind().describe(),
                default
            );
        }
    }
    ExitCode::SUCCESS
}
