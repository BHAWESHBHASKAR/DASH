//! `wal-inspect`: offline inspection, verification and repair of DASH
//! write-ahead log (and `.snapshot`) files. See
//! `docs/operations/wal-recovery.md`.
//!
//! Exit codes: 0 success, 1 the file has problems (`verify`, or `repair`
//! left invalid lines behind), 2 usage or I/O error.

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use store::{WalInspection, WalRepairOptions, inspect_wal_file, repair_wal_file};

const USAGE: &str = "usage:
  wal-inspect inspect <wal>
  wal-inspect verify <wal>
  wal-inspect repair <wal> [--dry-run] [--quarantine]";

fn preview(raw: &[u8]) -> String {
    let text = String::from_utf8_lossy(raw);
    let mut out: String = text.chars().take(120).collect();
    if text.chars().count() > 120 {
        out.push_str("...");
    }
    out.escape_debug().to_string()
}

fn print_inspection(path: &Path, info: &WalInspection) {
    println!(
        "file: {} ({})",
        path.display(),
        if info.is_snapshot { "snapshot" } else { "wal" }
    );
    match info.generation {
        Some(g) => println!("generation: {g:016x}"),
        None if !info.is_snapshot => println!("generation: none (no .gen file)"),
        None => {}
    }
    println!(
        "valid records: {} ({} legacy)",
        info.valid_records, info.legacy_records
    );
    for (kind, count) in &info.kind_counts {
        println!("  {kind}: {count}");
    }
    match info.torn_tail_line {
        Some(line) => println!(
            "torn tail: yes (line {line}, {} bytes would be truncated on open)",
            info.torn_tail_bytes
        ),
        None => println!("torn tail: no"),
    }
    if info.missing_final_newline {
        println!("final newline: missing (a newline would be appended on open)");
    }
    println!("invalid lines: {}", info.invalid_lines.len());
    println!("checksum failures: {}", info.checksum_failures());
    if let Some(first) = info.first_invalid() {
        println!(
            "first invalid line: {} ({}): {}",
            first.line_no,
            first.error,
            preview(&first.raw)
        );
    }
}

fn run(args: &[String]) -> Result<ExitCode, String> {
    let Some(cmd) = args.first() else {
        return Err("missing subcommand".to_string());
    };
    let Some(path) = args.get(1) else {
        return Err(format!("{cmd}: missing <wal> path"));
    };
    let path = PathBuf::from(path);
    let flags = &args[2..];
    match cmd.as_str() {
        "inspect" | "verify" => {
            if !flags.is_empty() {
                return Err(format!("{cmd}: unexpected argument {}", flags[0]));
            }
            let info = inspect_wal_file(&path).map_err(|e| format!("{}: {e:?}", path.display()))?;
            if cmd == "inspect" {
                print_inspection(&path, &info);
                return Ok(ExitCode::SUCCESS);
            }
            if info.invalid_lines.is_empty() {
                println!(
                    "ok: {} valid records{}",
                    info.valid_records,
                    if info.torn_tail_line.is_some() {
                        " (torn tail present; it is dropped on open)"
                    } else {
                        ""
                    }
                );
                Ok(ExitCode::SUCCESS)
            } else {
                for line in &info.invalid_lines {
                    println!(
                        "line {}: {}{}: {}",
                        line.line_no,
                        if line.checksum_failure {
                            "checksum failure: "
                        } else {
                            ""
                        },
                        line.error,
                        preview(&line.raw)
                    );
                }
                println!(
                    "FAILED: {} invalid line(s), {} checksum failure(s)",
                    info.invalid_lines.len(),
                    info.checksum_failures()
                );
                Ok(ExitCode::from(1))
            }
        }
        "repair" => {
            let mut options = WalRepairOptions::default();
            for flag in flags {
                match flag.as_str() {
                    "--dry-run" => options.dry_run = true,
                    "--quarantine" => options.quarantine_invalid = true,
                    other => return Err(format!("repair: unknown argument {other}")),
                }
            }
            let report = repair_wal_file(&path, options)
                .map_err(|e| format!("{}: {e:?}", path.display()))?;
            let verb = if report.dry_run { "would" } else { "did" };
            if !report.changed {
                println!("nothing to change");
            } else {
                if report.torn_tail_dropped {
                    println!(
                        "{verb}: truncate torn tail ({} bytes)",
                        report.torn_tail_bytes
                    );
                }
                if report.quarantined_lines > 0 {
                    println!(
                        "{verb}: quarantine {} invalid line(s) into {}",
                        report.quarantined_lines,
                        report
                            .quarantine_path
                            .as_ref()
                            .map(|p| p.display().to_string())
                            .unwrap_or_default()
                    );
                }
                if let Some(bak) = &report.backup_path {
                    println!("{verb}: keep backup copy at {}", bak.display());
                }
                println!("{verb}: keep {} record(s)", report.records_kept);
            }
            if report.invalid_remaining > 0 {
                println!(
                    "{} invalid line(s) remain; re-run with --quarantine to move them aside",
                    report.invalid_remaining
                );
                return Ok(ExitCode::from(1));
            }
            Ok(ExitCode::SUCCESS)
        }
        other => Err(format!("unknown subcommand {other}")),
    }
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match run(&args) {
        Ok(code) => code,
        Err(msg) => {
            eprintln!("error: {msg}\n{USAGE}");
            ExitCode::from(2)
        }
    }
}
