//! `crash-test`: randomized `kill -9` crash-consistency harness for the
//! ingestion service.
//!
//! Every cycle starts the real `ingestion` binary on a persistent state
//! directory, drives concurrent writers (single bundles, some with an edge to
//! the writer's previous claim, and atomic batches), SIGKILLs the process at
//! a random moment, verifies the WAL offline with `wal-inspect`, restarts the
//! service and checks an oracle built from what the clients saw:
//!
//! * every write acknowledged with a 2xx before the kill is present, with all
//!   of its evidence and edges;
//! * nothing that was never sent appears, no evidence record is written twice
//!   within the snapshot or within the WAL, and an unacknowledged request is
//!   applied completely or not at all;
//! * the WAL (and snapshot) verify clean before and after recovery;
//! * the restarted service reports ready within `--ready-timeout-secs`.
//!
//! The RNG seed is printed first; rerun with `--seed` to reproduce a run. On
//! failure the state directory is kept and its path printed. See
//! `docs/operations/testing-durability.md`.

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use dash_e2e::{Client, Stack, StackOpts, bin_path, bundle};

mod failover;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde_json::{Value, json};

const TENANT: &str = "tenant-a";

const USAGE: &str = "usage: crash-test [options]
  --cycles N               kill cycles (default 25)
  --seed N                 RNG seed (default: random, printed)
  --writers N              concurrent writer threads (default 4)
  --max-kill-delay-ms N    longest wait before the kill in a cycle (default 400)
  --batch-percent N        share of requests sent as 3-item batches (default 25)
  --checkpoint-every N     DASH_CHECKPOINT_MAX_WAL_RECORDS for the service
  --fresh-every N          start from an empty state directory every N cycles
                           (default 0: one directory for the whole run)
  --ready-timeout-secs N   longest restart-to-ready time accepted (default 60)
  --env KEY=VALUE          extra environment for the service (repeatable)
  --encryption             run with encryption at rest: generate a key file
                           (outside the state directory) and set
                           DASH_ENCRYPTION_KEY_FILE; the offline checks
                           also require every WAL and snapshot to be
                           encrypted
  --failover               leader-failover scenario: a control plane and three
                           ingestion nodes with synchronous replication; every
                           cycle kills the current leader and checks that no
                           acknowledged write is lost, exactly one node accepts
                           writes and the killed node rejoins as a follower
                           (with --env DASH_INGEST_MIN_SYNC_REPLICAS=0 lost
                           acknowledged writes are counted instead)
  --json-out PATH          write the summary as JSON
  --keep-state             keep the state directory even when the run passes

Binaries come from DASH_E2E_BIN_DIR, or are built with cargo
(DASH_E2E_PROFILE=release for release builds).";

#[derive(Debug, Clone)]
struct Config {
    cycles: usize,
    seed: u64,
    writers: usize,
    max_kill_delay_ms: u64,
    batch_percent: u32,
    checkpoint_every: Option<usize>,
    fresh_every: usize,
    ready_timeout: Duration,
    envs: Vec<(String, String)>,
    json_out: Option<PathBuf>,
    keep_state: bool,
    encryption: bool,
    failover: bool,
}

fn parse_args(args: &[String]) -> Result<Config, String> {
    let mut cfg = Config {
        cycles: 25,
        seed: rand::thread_rng().r#gen(),
        writers: 4,
        max_kill_delay_ms: 400,
        batch_percent: 25,
        checkpoint_every: None,
        fresh_every: 0,
        ready_timeout: Duration::from_secs(60),
        envs: vec![],
        json_out: None,
        keep_state: false,
        encryption: false,
        failover: false,
    };
    let mut it = args.iter();
    while let Some(flag) = it.next() {
        let mut value = || {
            it.next()
                .cloned()
                .ok_or_else(|| format!("{flag} needs a value"))
        };
        fn num<T: std::str::FromStr>(flag: &str, raw: String) -> Result<T, String> {
            raw.parse()
                .map_err(|_| format!("{flag}: not a number: {raw}"))
        }
        match flag.as_str() {
            "--cycles" => cfg.cycles = num(flag, value()?)?,
            "--seed" => cfg.seed = num(flag, value()?)?,
            "--writers" => cfg.writers = num::<usize>(flag, value()?)?.max(1),
            "--max-kill-delay-ms" => cfg.max_kill_delay_ms = num(flag, value()?)?,
            "--batch-percent" => cfg.batch_percent = num::<u32>(flag, value()?)?.min(100),
            "--checkpoint-every" => cfg.checkpoint_every = Some(num(flag, value()?)?),
            "--fresh-every" => cfg.fresh_every = num(flag, value()?)?,
            "--ready-timeout-secs" => cfg.ready_timeout = Duration::from_secs(num(flag, value()?)?),
            "--env" => {
                let raw = value()?;
                let (k, v) = raw
                    .split_once('=')
                    .ok_or_else(|| format!("--env expects KEY=VALUE, got {raw}"))?;
                cfg.envs.push((k.to_string(), v.to_string()));
            }
            "--json-out" => cfg.json_out = Some(PathBuf::from(value()?)),
            "--keep-state" => cfg.keep_state = true,
            "--encryption" => cfg.encryption = true,
            "--failover" => cfg.failover = true,
            "-h" | "--help" => return Err(String::new()),
            other => return Err(format!("unknown argument {other}")),
        }
    }
    Ok(cfg)
}

/// One claim the client sent: its id, evidence count and optional edge.
#[derive(Debug, Clone)]
struct Rec {
    claim: String,
    evidence: usize,
    edge_to: Option<String>,
}

/// One request's worth of records.
#[derive(Debug, Clone)]
struct Group {
    recs: Vec<Rec>,
    batch: bool,
}

#[derive(Debug, Default)]
struct Outcome {
    acked: Vec<Group>,
    /// The request in flight when the connection broke (outcome unknown).
    unknown: Option<Group>,
    /// Non-2xx answers to valid requests (always a failure).
    unexpected: Vec<String>,
}

fn make_rec(rng: &mut StdRng, claim: String, edge_to: Option<String>) -> (Value, Rec) {
    let n = rng.gen_range(1..=4);
    let mut body = bundle(
        TENANT,
        &claim,
        &format!(
            "crash harness claim {claim} about reactor coolant loop {}",
            rng.gen_range(0..64)
        ),
        n,
    );
    if let Some(to) = &edge_to {
        body["edges"] = json!([{
            "edge_id": format!("g-{claim}"), "from_claim_id": claim, "to_claim_id": to,
            "relation": "supports", "strength": 0.6
        }]);
    }
    (
        body,
        Rec {
            claim,
            evidence: n,
            edge_to,
        },
    )
}

/// Send writes until `stop` is set or the server goes away.
fn writer(
    addr: SocketAddr,
    key: String,
    prefix: String,
    seed: u64,
    batch_percent: u32,
    stop: Arc<AtomicBool>,
) -> Outcome {
    let mut client = Client::new(addr);
    client.timeout = Duration::from_secs(30);
    let mut rng = StdRng::seed_from_u64(seed);
    let mut out = Outcome::default();
    let mut prev: Option<String> = None;
    let mut i = 0usize;
    while !stop.load(Ordering::Relaxed) {
        i += 1;
        let batch = rng.gen_range(0..100) < batch_percent;
        let (path, body, group) = if batch {
            let mut items = vec![];
            let mut recs = vec![];
            for j in 0..3 {
                let (b, r) = make_rec(&mut rng, format!("{prefix}-{i}-{j}"), None);
                items.push(b);
                recs.push(r);
            }
            (
                "/v1/ingest/batch",
                json!({"commit_id": format!("{prefix}-commit-{i}"), "items": items}),
                Group { recs, batch: true },
            )
        } else {
            let edge_to = if rng.gen_bool(0.5) {
                prev.clone()
            } else {
                None
            };
            let (b, r) = make_rec(&mut rng, format!("{prefix}-{i}"), edge_to);
            prev = Some(r.claim.clone());
            (
                "/v1/ingest",
                b,
                Group {
                    recs: vec![r],
                    batch: false,
                },
            )
        };
        match client.try_post_json(path, &[("x-api-key", key.as_str())], &body) {
            Ok(r) if (200..300).contains(&r.status) => out.acked.push(group),
            Ok(r) => {
                out.unexpected
                    .push(format!("{path} answered {}: {}", r.status, r.body));
                out.unknown = Some(group);
                break;
            }
            Err(_) => {
                out.unknown = Some(group);
                break;
            }
        }
    }
    out
}

/// `wal-inspect verify` on the WAL and, when present, the snapshot (with
/// the service's encryption settings). With `--encryption` both must also
/// be encrypted files.
fn verify_files(cfg: &Config, state: &Path) -> Result<(), String> {
    for name in ["ingest.wal", "ingest.wal.snapshot"] {
        let path = state.join(name);
        if !path.exists() {
            continue;
        }
        if cfg.encryption {
            let bytes = std::fs::read(&path).map_err(|e| format!("read {name}: {e}"))?;
            if !bytes.is_empty() && !bytes.starts_with(b"~DASHENC1 ") {
                return Err(format!("{name} is not encrypted"));
            }
        }
        let out = Command::new(bin_path("wal-inspect"))
            .arg("verify")
            .arg(&path)
            .envs(
                cfg.envs
                    .iter()
                    .filter(|(k, _)| {
                        k == "DASH_ENCRYPTION_KEY_FILE" || k == "DASH_ENCRYPTION_PREVIOUS_KEY_FILES"
                    })
                    .cloned(),
            )
            .output()
            .map_err(|e| format!("run wal-inspect: {e}"))?;
        if !out.status.success() {
            return Err(format!(
                "wal-inspect verify {name} failed:\n{}{}",
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr)
            ));
        }
    }
    Ok(())
}

#[derive(Debug, Default)]
struct Totals {
    acked_requests: usize,
    acked_claims: usize,
    unknown_requests: usize,
    unknown_applied: usize,
    recovery_ms: Vec<u64>,
    state_dirs: usize,
    final_claims: usize,
    /// Group-commit counters scraped from `/metrics` just before each kill.
    group_commit: GroupCommitSeen,
}

/// What `/metrics` said about WAL group commit right before the kills.
#[derive(Debug, Default, Clone, Copy, PartialEq)]
struct GroupCommitSeen {
    /// Scrapes that answered (a scrape can lose the race with the kill).
    scrapes: usize,
    /// Scrapes that reported `dash_ingest_wal_group_commit_enabled 1`.
    enabled: usize,
    /// Sum over cycles of the batches and entries the committer wrote.
    batches: u64,
    entries: u64,
    /// Largest batch (entries sharing one fsync) seen in any cycle.
    max_batch_entries: u64,
}

impl GroupCommitSeen {
    /// Fold one `/metrics` body into the totals.
    fn observe(&mut self, metrics: &str) {
        let gauge = |name: &str| -> Option<u64> {
            metrics
                .lines()
                .find_map(|line| {
                    let (key, value) = line.split_once(' ')?;
                    (key == name).then(|| value.trim().parse::<f64>().ok())?
                })
                .map(|v| v as u64)
        };
        self.scrapes += 1;
        if gauge("dash_ingest_wal_group_commit_enabled") == Some(1) {
            self.enabled += 1;
        }
        self.batches += gauge("dash_ingest_wal_group_commit_batches_total").unwrap_or(0);
        self.entries += gauge("dash_ingest_wal_group_commit_entries_total").unwrap_or(0);
        self.max_batch_entries = self
            .max_batch_entries
            .max(gauge("dash_ingest_wal_group_commit_max_batch_entries").unwrap_or(0));
    }
}

/// Check the restarted leader against everything the clients saw.
fn check_oracle(
    s: &Stack,
    outcomes: &[Outcome],
    expected: &mut BTreeMap<String, Rec>,
    totals: &mut Totals,
) -> Result<(), String> {
    let leader = s.leader_state();
    let bundle_present = |r: &Rec| -> Result<bool, String> {
        if !leader.claims.contains_key(&r.claim) {
            return Ok(false);
        }
        for i in 0..r.evidence {
            let e = format!("{}-e{i}", r.claim);
            if leader.evidence.get(&e) != Some(&r.claim) {
                return Err(format!(
                    "bundle {} is partial: evidence {e} missing ({} sent)",
                    r.claim, r.evidence
                ));
            }
        }
        Ok(true)
    };
    for o in outcomes {
        if let Some(msg) = o.unexpected.first() {
            return Err(format!("valid write rejected before the kill: {msg}"));
        }
        for g in &o.acked {
            totals.acked_requests += 1;
            for r in &g.recs {
                totals.acked_claims += 1;
                if !bundle_present(r)? {
                    return Err(format!(
                        "acknowledged claim {} is missing after kill -9",
                        r.claim
                    ));
                }
                expected.insert(r.claim.clone(), r.clone());
            }
        }
        if let Some(g) = &o.unknown {
            totals.unknown_requests += 1;
            let mut present = vec![];
            for r in &g.recs {
                if bundle_present(r)? {
                    present.push(r);
                }
            }
            if g.batch && !present.is_empty() && present.len() != g.recs.len() {
                return Err(format!(
                    "partial batch after kill -9: {} of {} claims present ({:?})",
                    present.len(),
                    g.recs.len(),
                    g.recs.iter().map(|r| &r.claim).collect::<Vec<_>>()
                ));
            }
            if !present.is_empty() {
                totals.unknown_applied += 1;
            }
            for r in present {
                expected.insert(r.claim.clone(), r.clone());
            }
        }
    }
    let phantom: Vec<&String> = leader
        .claims
        .keys()
        .filter(|c| !expected.contains_key(*c))
        .collect();
    if !phantom.is_empty() {
        return Err(format!("claims present that were never sent: {phantom:?}"));
    }
    for (claim, rec) in expected.iter() {
        if !bundle_present(rec)? {
            return Err(format!("claim {claim} from an earlier cycle vanished"));
        }
        if let Some(to) = &rec.edge_to
            && !leader
                .edges
                .contains(&(claim.clone(), to.clone(), "supports".to_string()))
        {
            return Err(format!("edge {claim} -> {to} lost"));
        }
    }
    // Once per section: a checkpoint interrupted between the snapshot
    // rename and the WAL truncation legitimately leaves a record in both.
    if let Some((e, [snap, wal])) = leader
        .evidence_lines_by_section
        .iter()
        .find(|(_, [snap, wal])| *snap > 1 || *wal > 1)
    {
        return Err(format!(
            "evidence {e} is written more than once (snapshot {snap}, WAL {wal})"
        ));
    }
    let known: BTreeSet<String> = expected
        .values()
        .flat_map(|r| (0..r.evidence).map(move |i| format!("{}-e{i}", r.claim)))
        .collect();
    if let Some(stray) = leader.evidence.keys().find(|e| !known.contains(*e)) {
        return Err(format!("evidence {stray} belongs to no expected claim"));
    }
    totals.final_claims = expected.len();
    Ok(())
}

fn new_stack(cfg: &Config) -> Stack {
    let mut s = Stack::new(StackOpts {
        tenants: vec![TENANT.into()],
        checkpoint_every: cfg.checkpoint_every,
        extra_ingest_env: cfg.envs.clone(),
        ..Default::default()
    });
    s.start_ingest();
    s
}

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

/// One kill cycle: write, kill, verify offline, restart, check the oracle.
fn cycle(
    cfg: &Config,
    s: &mut Stack,
    rng: &mut StdRng,
    cycle: usize,
    expected: &mut BTreeMap<String, Rec>,
    totals: &mut Totals,
) -> Result<(), String> {
    let key = s.ik(TENANT).1;
    let stop = Arc::new(AtomicBool::new(false));
    let handles: Vec<_> = (0..cfg.writers)
        .map(|w| {
            let (addr, key, stop) = (s.ingest_addr(), key.clone(), stop.clone());
            let seed: u64 = rng.r#gen();
            let prefix = format!("s{}c{cycle}w{w}", totals.state_dirs);
            let batch_percent = cfg.batch_percent;
            thread::spawn(move || writer(addr, key, prefix, seed, batch_percent, stop))
        })
        .collect();
    let delay = rng.gen_range(0..=cfg.max_kill_delay_ms);
    thread::sleep(Duration::from_millis(delay));
    // Record whether the writes went through group commit (the default) and
    // how many shared an fsync; the counters reset with every restart.
    let mut metrics_client = Client::new(s.ingest_addr());
    metrics_client.timeout = Duration::from_secs(2);
    if let Ok(r) = metrics_client.request("GET", "/metrics", &[("x-api-key", &s.ops_key)], None)
        && r.status == 200
    {
        totals.group_commit.observe(&r.body);
    }
    s.kill_ingest();
    stop.store(true, Ordering::Relaxed);
    let outcomes: Vec<Outcome> = handles
        .into_iter()
        .map(|h| h.join().expect("writer thread panicked"))
        .collect();

    verify_files(cfg, s.dir.path()).map_err(|e| format!("after kill: {e}"))?;
    let started = Instant::now();
    s.start_ingest();
    let deadline = started + cfg.ready_timeout;
    while s.ingest_ready_status() != Some(200) {
        if Instant::now() > deadline {
            return Err(format!(
                "not ready {:?} after restart\n{}",
                cfg.ready_timeout,
                s.ingest_log()
            ));
        }
        thread::sleep(Duration::from_millis(10));
    }
    totals
        .recovery_ms
        .push(started.elapsed().as_millis() as u64);
    check_oracle(s, &outcomes, expected, totals)?;
    verify_files(cfg, s.dir.path()).map_err(|e| format!("after recovery: {e}"))
}

fn run(cfg: &Config) -> (Result<(), String>, Totals, Option<PathBuf>, usize) {
    let mut rng = StdRng::seed_from_u64(cfg.seed);
    let mut totals = Totals::default();
    let mut expected: BTreeMap<String, Rec> = BTreeMap::new();
    let mut s = new_stack(cfg);
    totals.state_dirs = 1;
    for c in 0..cfg.cycles {
        if cfg.fresh_every > 0 && c > 0 && c % cfg.fresh_every == 0 {
            if cfg.keep_state {
                let _ = std::mem::replace(&mut s.dir, tempfile_placeholder()).keep();
            }
            s = new_stack(cfg);
            expected.clear();
            totals.state_dirs += 1;
        }
        let result = catch_unwind(AssertUnwindSafe(|| {
            cycle(cfg, &mut s, &mut rng, c, &mut expected, &mut totals)
        }))
        .unwrap_or_else(|panic| {
            let msg = panic
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| panic.downcast_ref::<&str>().map(|m| m.to_string()))
                .unwrap_or_else(|| "panic".to_string());
            Err(format!("harness panic: {msg}"))
        });
        if let Err(e) = result {
            s.kill_ingest();
            let kept = std::mem::replace(&mut s.dir, tempfile_placeholder()).keep();
            return (Err(format!("cycle {c}: {e}")), totals, Some(kept), c);
        }
        if (c + 1) % 10 == 0 || c + 1 == cfg.cycles {
            println!(
                "cycle {}/{}: acked_requests={} acked_claims={} unknown={} (applied {}) claims_now={}",
                c + 1,
                cfg.cycles,
                totals.acked_requests,
                totals.acked_claims,
                totals.unknown_requests,
                totals.unknown_applied,
                totals.final_claims
            );
        }
    }
    s.kill_ingest();
    let kept = cfg
        .keep_state
        .then(|| std::mem::replace(&mut s.dir, tempfile_placeholder()).keep());
    (Ok(()), totals, kept, cfg.cycles)
}

/// An empty temp dir to swap into a `Stack` whose own dir is being kept.
fn tempfile_placeholder() -> tempfile::TempDir {
    tempfile::tempdir().expect("tempdir")
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut cfg = match parse_args(&args) {
        Ok(cfg) => cfg,
        Err(e) => {
            if !e.is_empty() {
                eprintln!("crash-test: {e}");
            }
            eprintln!("{USAGE}");
            return ExitCode::from(2);
        }
    };
    // The key lives outside every state directory, like a mounted Secret.
    let _key_dir = if cfg.encryption {
        let dir = tempfile::tempdir().expect("key dir");
        let path = dir.path().join("dash-kek.key");
        let key: String = (0..64)
            .map(|_| char::from_digit(rand::thread_rng().gen_range(0..16), 16).unwrap())
            .collect();
        std::fs::write(&path, key).expect("write key");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
                .expect("chmod key");
        }
        cfg.envs.push((
            "DASH_ENCRYPTION_KEY_FILE".to_string(),
            path.display().to_string(),
        ));
        Some(dir)
    } else {
        None
    };
    println!(
        "crash-test seed={} cycles={} writers={} max_kill_delay_ms={} batch_percent={} checkpoint_every={:?} fresh_every={} encryption={}",
        cfg.seed,
        cfg.cycles,
        cfg.writers,
        cfg.max_kill_delay_ms,
        cfg.batch_percent,
        cfg.checkpoint_every,
        cfg.fresh_every,
        cfg.encryption
    );
    let t0 = Instant::now();
    if cfg.failover {
        let (result, totals, cycles_done) = failover::run(&cfg);
        let summary = failover::summary(&cfg, &result, &totals, cycles_done, t0.elapsed());
        println!(
            "crash-test --failover {}: synchronous={} cycles={}/{} acked_requests={} acked_claims={} acked_lost={} unknown={} (applied {}) failover_ms={} rejoin_ms={} duration={:.1}s",
            if result.is_ok() { "PASSED" } else { "FAILED" },
            failover::synchronous(&cfg),
            cycles_done,
            cfg.cycles,
            totals.acked_requests,
            totals.acked_claims,
            totals.acked_lost,
            totals.unknown_requests,
            totals.unknown_applied,
            summary["failover_ms"],
            summary["rejoin_ms"],
            t0.elapsed().as_secs_f64()
        );
        if let Some(path) = &cfg.json_out
            && let Err(e) = std::fs::write(path, format!("{summary:#}\n"))
        {
            eprintln!("crash-test: cannot write {}: {e}", path.display());
        }
        return match result {
            Ok(()) if totals.acked_claims == 0 => {
                eprintln!("crash-test: no write was ever acknowledged; nothing was tested");
                ExitCode::from(1)
            }
            Ok(()) => ExitCode::SUCCESS,
            Err(e) => {
                eprintln!("crash-test --failover FAILED: {e}");
                eprintln!(
                    "reproduce with: crash-test --failover --seed {} --cycles {} --writers {}",
                    cfg.seed, cfg.cycles, cfg.writers
                );
                ExitCode::from(1)
            }
        };
    }
    let (result, totals, kept, cycles_done) = run(&cfg);
    let mut rec = totals.recovery_ms.clone();
    rec.sort_unstable();
    let summary = json!({
        "ok": result.is_ok(),
        "error": result.as_ref().err(),
        "seed": cfg.seed,
        "cycles_requested": cfg.cycles,
        "cycles_completed": cycles_done,
        "writers": cfg.writers,
        "checkpoint_every": cfg.checkpoint_every,
        "encryption": cfg.encryption,
        "state_dirs": totals.state_dirs,
        "acked_requests": totals.acked_requests,
        "acked_claims": totals.acked_claims,
        "unknown_requests": totals.unknown_requests,
        "unknown_applied": totals.unknown_applied,
        "final_claims": totals.final_claims,
        "group_commit": {
            "scrapes": totals.group_commit.scrapes,
            "enabled_scrapes": totals.group_commit.enabled,
            "batches": totals.group_commit.batches,
            "entries": totals.group_commit.entries,
            "max_batch_entries": totals.group_commit.max_batch_entries,
        },
        "recovery_ms": {
            "p50": percentile(&rec, 0.50),
            "p95": percentile(&rec, 0.95),
            "max": rec.last().copied().unwrap_or(0),
        },
        "duration_s": t0.elapsed().as_secs_f64(),
        "state_dir": kept.as_ref().map(|p| p.display().to_string()),
    });
    println!(
        "crash-test {}: cycles={}/{} acked_requests={} acked_claims={} unknown={} (applied {}) recovery_ms p50={} p95={} max={} duration={:.1}s",
        if result.is_ok() { "PASSED" } else { "FAILED" },
        cycles_done,
        cfg.cycles,
        totals.acked_requests,
        totals.acked_claims,
        totals.unknown_requests,
        totals.unknown_applied,
        summary["recovery_ms"]["p50"],
        summary["recovery_ms"]["p95"],
        summary["recovery_ms"]["max"],
        t0.elapsed().as_secs_f64()
    );
    let gc = totals.group_commit;
    println!(
        "group commit before the kills: enabled in {}/{} scrapes, {} entries in {} batches, largest batch {}",
        gc.enabled, gc.scrapes, gc.entries, gc.batches, gc.max_batch_entries
    );
    if let Some(path) = &cfg.json_out
        && let Err(e) = std::fs::write(path, format!("{summary:#}\n"))
    {
        eprintln!("crash-test: cannot write {}: {e}", path.display());
    }
    if let Some(dir) = &kept {
        println!("state directory kept at {}", dir.display());
    }
    match result {
        Ok(()) => {
            if totals.acked_claims == 0 {
                eprintln!("crash-test: no write was ever acknowledged; nothing was tested");
                return ExitCode::from(1);
            }
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("crash-test FAILED: {e}");
            eprintln!(
                "reproduce with: crash-test --seed {} --cycles {} --writers {}",
                cfg.seed, cfg.cycles, cfg.writers
            );
            ExitCode::from(1)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(raw: &[&str]) -> Vec<String> {
        raw.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn parses_every_flag() {
        let cfg = parse_args(&args(&[
            "--cycles",
            "7",
            "--seed",
            "42",
            "--writers",
            "3",
            "--checkpoint-every",
            "50",
            "--fresh-every",
            "5",
            "--env",
            "A=b=c",
            "--keep-state",
            "--encryption",
        ]))
        .unwrap();
        assert_eq!(cfg.cycles, 7);
        assert_eq!(cfg.seed, 42);
        assert_eq!(cfg.writers, 3);
        assert_eq!(cfg.checkpoint_every, Some(50));
        assert_eq!(cfg.fresh_every, 5);
        assert_eq!(cfg.envs, vec![("A".to_string(), "b=c".to_string())]);
        assert!(cfg.keep_state);
        assert!(cfg.encryption);
        assert!(!parse_args(&args(&[])).unwrap().encryption);
    }

    #[test]
    fn failover_mode_and_its_replication_mode_are_parsed() {
        let cfg = parse_args(&args(&["--failover"])).unwrap();
        assert!(cfg.failover);
        assert!(failover::synchronous(&cfg), "synchronous by default");
        let cfg = parse_args(&args(&[
            "--failover",
            "--env",
            "DASH_INGEST_MIN_SYNC_REPLICAS=0",
        ]))
        .unwrap();
        assert!(!failover::synchronous(&cfg));
        assert!(!parse_args(&args(&[])).unwrap().failover);
    }

    #[test]
    fn rejects_unknown_and_malformed_flags() {
        assert!(parse_args(&args(&["--bogus"])).is_err());
        assert!(parse_args(&args(&["--cycles", "many"])).is_err());
        assert!(parse_args(&args(&["--cycles"])).is_err());
        assert!(parse_args(&args(&["--env", "novalue"])).is_err());
    }

    #[test]
    fn group_commit_counters_are_read_from_metrics() {
        let mut seen = GroupCommitSeen::default();
        seen.observe(
            "# TYPE dash_ingest_wal_group_commit_enabled gauge\n\
dash_ingest_wal_group_commit_enabled 1\n\
dash_ingest_wal_group_commit_batches_total 40\n\
dash_ingest_wal_group_commit_entries_total 100\n\
dash_ingest_wal_group_commit_max_batch_entries 4\n\
dash_ingest_wal_group_commit_max_batch_entries_other 99\n",
        );
        seen.observe("dash_ingest_wal_group_commit_enabled 0\n");
        assert_eq!(
            seen,
            GroupCommitSeen {
                scrapes: 2,
                enabled: 1,
                batches: 40,
                entries: 100,
                max_batch_entries: 4,
            }
        );
    }

    #[test]
    fn percentile_picks_nearest_rank() {
        let v = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        assert_eq!(percentile(&v, 0.0), 1);
        assert_eq!(percentile(&v, 0.5), 6);
        assert_eq!(percentile(&v, 1.0), 10);
        assert_eq!(percentile(&[], 0.5), 0);
    }
}
