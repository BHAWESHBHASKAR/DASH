//! Verifies DASH audit chain files (ingestion and retrieval, v2 and legacy).
//!
//! Usage: audit-verify --path FILE [--service ingestion|retrieval]
//!                     [--expect-last-seq N --expect-last-hash HEX]

use std::process::ExitCode;

use dash_common::audit::{VerifyOptions, verify_file};

const USAGE: &str = "Usage: audit-verify --path <audit_log.jsonl> [--service ingestion|retrieval] \
[--expect-last-seq N --expect-last-hash HEX]";

fn main() -> ExitCode {
    let mut path = None;
    let mut opts = VerifyOptions::default();
    let mut eseq: Option<u64> = None;
    let mut ehash: Option<String> = None;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--path" | "--service" | "--expect-last-seq" | "--expect-last-hash" => {
                let Some(value) = args.next() else {
                    eprintln!("[audit-verify] {arg} requires a value");
                    return ExitCode::from(1);
                };
                match arg.as_str() {
                    "--path" => path = Some(value),
                    "--service" => {
                        if !value.is_empty() {
                            opts.service = Some(value);
                        }
                    }
                    "--expect-last-seq" => match value.parse() {
                        Ok(n) => eseq = Some(n),
                        Err(_) => {
                            eprintln!("[audit-verify] --expect-last-seq must be an integer");
                            return ExitCode::from(1);
                        }
                    },
                    _ => ehash = Some(value),
                }
            }
            "--help" | "-h" => {
                println!("{USAGE}");
                return ExitCode::SUCCESS;
            }
            other => {
                eprintln!("[audit-verify] unknown argument: {other}\n{USAGE}");
                return ExitCode::from(1);
            }
        }
    }
    let Some(path) = path else {
        eprintln!("[audit-verify] --path is required\n{USAGE}");
        return ExitCode::from(1);
    };
    match (eseq, ehash) {
        (Some(s), Some(h)) => opts.expect_tail = Some((s, h)),
        (None, None) => {}
        _ => {
            eprintln!("[audit-verify] --expect-last-seq and --expect-last-hash go together");
            return ExitCode::from(1);
        }
    }
    match verify_file(&path, &opts) {
        Ok(r) => {
            println!(
                "[audit-verify] ok: path={path} chained_records={} v2_records={} legacy_records={} \
unchained_prefix_lines={} restarts={} quarantined_lines={} last_seq={} last_hash={}",
                r.chained_records,
                r.v2_records,
                r.legacy_records,
                r.unchained_prefix_lines,
                r.restarts,
                r.quarantined_lines,
                r.last_seq,
                r.last_hash
            );
            ExitCode::SUCCESS
        }
        Err(err) => {
            eprintln!("[audit-verify] {err}");
            ExitCode::from(1)
        }
    }
}
