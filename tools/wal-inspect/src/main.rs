//! `wal-inspect`: offline inspection, verification and repair of DASH
//! write-ahead log (and `.snapshot`) files. See
//! `docs/operations/wal-recovery.md`.
//!
//! Encrypted files (ADR 0005, `docs/operations/encryption.md`) are read with
//! the keys in `DASH_ENCRYPTION_KEY_FILE` / `DASH_ENCRYPTION_PREVIOUS_KEY_FILES`.
//! `keys` lists the key id of every file under a path and `rewrap` moves
//! every file to the active key (offline; only file headers change).
//!
//! Exit codes: 0 success, 1 the file has problems (`verify`, or `repair`
//! left invalid lines behind), 2 usage or I/O error.

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use store::encryption::{self, FileFormat, RewrapOutcome};
use store::{DiskBackedStore, WalInspection, WalRepairOptions, inspect_wal_file, repair_wal_file};

const USAGE: &str = "usage:
  wal-inspect inspect <wal>
  wal-inspect verify <wal>
  wal-inspect repair <wal> [--dry-run] [--quarantine]
  wal-inspect keys <file-or-dir>
  wal-inspect rewrap <file-or-dir>";

/// Every file under `path` (or `path` itself).
fn files_under(path: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![path.to_path_buf()];
    while let Some(next) = stack.pop() {
        if next.is_dir() {
            if let Ok(entries) = std::fs::read_dir(&next) {
                for entry in entries.flatten() {
                    stack.push(entry.path());
                }
            }
        } else if next.is_file() {
            out.push(next);
        }
    }
    out.sort();
    out
}

fn is_redb(path: &Path) -> bool {
    let mut magic = [0u8; 4];
    std::fs::File::open(path)
        .and_then(|mut f| std::io::Read::read_exact(&mut f, &mut magic))
        .is_ok()
        && &magic == b"redb"
}

/// `keys`: one line per file: path, storage format, KEK id.
fn list_keys(path: &Path) -> Result<ExitCode, String> {
    for file in files_under(path) {
        let (format, key_id) = if is_redb(&file) {
            match DiskBackedStore::stored_encryption_key_id(&file) {
                Ok(Some(id)) => ("redb (encrypted values)", id),
                Ok(None) => ("redb (plaintext values)", "-".to_string()),
                Err(err) => ("redb (unreadable)", err),
            }
        } else {
            match encryption::sniff_file(&file) {
                Ok(FileFormat::Plain) => ("plaintext", "-".to_string()),
                Ok(FileFormat::Lines(h)) => ("encrypted lines", h.key_id),
                Ok(FileFormat::Sealed(h)) => ("sealed", h.key_id),
                Err(err) => ("unreadable header", err.to_string()),
            }
        };
        println!("{}\t{format}\t{key_id}", file.display());
    }
    Ok(ExitCode::SUCCESS)
}

/// `rewrap`: every encrypted file under `path` to the active KEK.
fn rewrap(path: &Path) -> Result<ExitCode, String> {
    let Some(keyring) = encryption::current() else {
        return Err("rewrap needs DASH_ENCRYPTION_KEY_FILE (the new key) and DASH_ENCRYPTION_PREVIOUS_KEY_FILES (the old ones)".to_string());
    };
    let mut failed = 0usize;
    for file in files_under(path) {
        let name = file.to_string_lossy();
        if name.ends_with(".tmp") {
            continue;
        }
        if is_redb(&file) {
            match DiskBackedStore::stored_encryption_key_id(&file) {
                Ok(None) => println!(
                    "{}\tplaintext values (rewritten encrypted by the next checkpoint)",
                    file.display()
                ),
                Ok(Some(id)) if id == keyring.active_key_id() => {
                    println!("{}\tcurrent", file.display())
                }
                Ok(Some(id)) => {
                    match DiskBackedStore::new_with_keyring(&file, Some(keyring.clone())) {
                        Ok(_) => println!(
                            "{}\trewrapped ({id} -> {})",
                            file.display(),
                            keyring.active_key_id()
                        ),
                        Err(err) => {
                            failed += 1;
                            println!("{}\tFAILED: {err}", file.display());
                        }
                    }
                }
                Err(err) => {
                    failed += 1;
                    println!("{}\tFAILED: {err}", file.display());
                }
            }
            continue;
        }
        match encryption::rewrap_file(&keyring, &file) {
            Ok(RewrapOutcome::Plain) => {}
            Ok(RewrapOutcome::Current) => println!("{}\tcurrent", file.display()),
            Ok(RewrapOutcome::Rewrapped { from }) => println!(
                "{}\trewrapped ({from} -> {})",
                file.display(),
                keyring.active_key_id()
            ),
            Err(err) => {
                failed += 1;
                println!("{}\tFAILED: {err}", file.display());
            }
        }
    }
    if failed > 0 {
        println!("FAILED: {failed} file(s) could not be rewrapped");
        return Ok(ExitCode::from(1));
    }
    Ok(ExitCode::SUCCESS)
}

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
    let keyring = encryption::keyring_from_env().map_err(|e| e.to_string())?;
    encryption::install(keyring);
    match cmd.as_str() {
        "keys" | "rewrap" => {
            if !flags.is_empty() {
                return Err(format!("{cmd}: unexpected argument {}", flags[0]));
            }
            if cmd == "keys" {
                list_keys(&path)
            } else {
                rewrap(&path)
            }
        }
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
