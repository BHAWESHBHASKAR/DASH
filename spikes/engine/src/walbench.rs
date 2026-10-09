//! Experiment E: durability cost on this disk. Plain `std::fs` write + `sync_data`.

use crate::common::*;
use serde_json::json;
use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom, Write};
use std::sync::mpsc;
use std::time::{Duration, Instant};

const PREALLOC: u64 = 128 * 1024 * 1024;

fn payload(size: usize) -> Vec<u8> {
    (0..size).map(|i| (i.wrapping_mul(31) ^ 0x5a) as u8).collect()
}

#[derive(Clone, Copy, PartialEq)]
enum Mode {
    AppendSyncData,
    AppendSyncAll,
    PreallocSyncData,
    NoSync,
}
impl Mode {
    fn name(&self) -> &'static str {
        match self {
            Mode::AppendSyncData => "append+sync_data",
            Mode::AppendSyncAll => "append+sync_all",
            Mode::PreallocSyncData => "prealloc_overwrite+sync_data",
            Mode::NoSync => "append_no_sync(baseline)",
        }
    }
}

fn run_batch(dir: &std::path::Path, mode: Mode, rec: usize, batch: usize, secs: f64) -> serde_json::Value {
    let path = dir.join("wal_test.log");
    let _ = std::fs::remove_file(&path);
    let mut f = OpenOptions::new().create(true).read(true).write(true).truncate(true).open(&path).unwrap();
    if mode == Mode::PreallocSyncData {
        let zeros = vec![0u8; 1 << 20];
        for _ in 0..(PREALLOC >> 20) {
            f.write_all(&zeros).unwrap();
        }
        f.sync_all().unwrap();
        f.seek(SeekFrom::Start(0)).unwrap();
    }
    let one = payload(rec);
    let mut buf = Vec::with_capacity(rec * batch);
    for _ in 0..batch {
        buf.extend_from_slice(&one);
    }
    let mut lat = Lat::new();
    let mut commits = 0u64;
    let mut pos = 0u64;
    let start = Instant::now();
    while start.elapsed().as_secs_f64() < secs {
        let t = Instant::now();
        if (mode == Mode::PreallocSyncData || mode == Mode::NoSync) && pos + buf.len() as u64 > PREALLOC {
            f.seek(SeekFrom::Start(0)).unwrap();
            pos = 0;
        }
        f.write_all(&buf).unwrap();
        pos += buf.len() as u64;
        match mode {
            Mode::AppendSyncData | Mode::PreallocSyncData => f.sync_data().unwrap(),
            Mode::AppendSyncAll => f.sync_all().unwrap(),
            Mode::NoSync => {}
        }
        lat.rec(t.elapsed());
        commits += 1;
    }
    let el = start.elapsed().as_secs_f64();
    drop(f);
    let _ = std::fs::remove_file(&path);
    json!({"exp":"E","kind":"batch","mode":mode.name(),"record_bytes":rec,"batch":batch,
        "commits":commits,"records_per_s":((commits * batch as u64) as f64 / el).round(),
        "commit_ack_latency":lat.json(),"seconds":el})
}

/// Concurrent producers, one committer thread coalescing whatever is queued (adaptive group commit).
fn run_group(dir: &std::path::Path, producers: usize, rec: usize, secs: f64, max_batch: usize) -> serde_json::Value {
    let path = dir.join("wal_group.log");
    let _ = std::fs::remove_file(&path);
    let mut f = OpenOptions::new().create(true).write(true).truncate(true).open(&path).unwrap();
    let (tx, rx) = mpsc::channel::<(Vec<u8>, mpsc::Sender<()>)>();
    let stop = std::sync::atomic::AtomicBool::new(false);
    let one = payload(rec);
    let mut lats: Vec<Lat> = vec![];
    let mut sync_calls = 0u64;
    let mut recs_committed = 0u64;
    let start = Instant::now();
    std::thread::scope(|s| {
        let stop = &stop;
        let committer = s.spawn(move || {
            let mut sync_calls = 0u64;
            let mut recs = 0u64;
            let mut buf: Vec<u8> = Vec::with_capacity(rec * max_batch);
            loop {
                let first = match rx.recv_timeout(Duration::from_millis(50)) {
                    Ok(x) => x,
                    Err(mpsc::RecvTimeoutError::Timeout) => {
                        if stop.load(std::sync::atomic::Ordering::Relaxed) {
                            break;
                        }
                        continue;
                    }
                    Err(_) => break,
                };
                buf.clear();
                let mut acks = vec![first.1];
                buf.extend_from_slice(&first.0);
                while acks.len() < max_batch {
                    match rx.try_recv() {
                        Ok((b, a)) => {
                            buf.extend_from_slice(&b);
                            acks.push(a);
                        }
                        Err(_) => break,
                    }
                }
                f.write_all(&buf).unwrap();
                f.sync_data().unwrap();
                sync_calls += 1;
                recs += acks.len() as u64;
                for a in acks {
                    let _ = a.send(());
                }
            }
            (sync_calls, recs)
        });
        let mut hs = vec![];
        for _ in 0..producers {
            let tx = tx.clone();
            let one = one.clone();
            hs.push(s.spawn(move || {
                let mut lat = Lat::new();
                let (atx, arx) = mpsc::channel();
                while start.elapsed().as_secs_f64() < secs {
                    let t = Instant::now();
                    if tx.send((one.clone(), atx.clone())).is_err() {
                        break;
                    }
                    if arx.recv().is_err() {
                        break;
                    }
                    lat.rec(t.elapsed());
                }
                lat
            }));
        }
        drop(tx);
        for h in hs {
            lats.push(h.join().unwrap());
        }
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        let (sc, rc) = committer.join().unwrap();
        sync_calls = sc;
        recs_committed = rc;
    });
    let el = start.elapsed().as_secs_f64();
    let mut all = Lat::new();
    for l in &lats {
        all.merge(l);
    }
    let _ = std::fs::remove_file(&path);
    json!({"exp":"E","kind":"group_commit_pipelined","producers":producers,"record_bytes":rec,
        "records_per_s":(recs_committed as f64 / el).round(),
        "avg_batch":(recs_committed as f64 / sync_calls.max(1) as f64 * 10.0).round() / 10.0,
        "fsyncs_per_s":(sync_calls as f64 / el).round(),
        "ack_latency":all.json(),"seconds":el})
}

pub fn run(a: &Args) {
    let dir = std::path::PathBuf::from(a.s("dir", scratch_dir().join("wal").to_str().unwrap()));
    std::fs::create_dir_all(&dir).unwrap();
    let secs = a.f("secs", 3.0);
    for rec in [256usize, 4096] {
        for batch in [1usize, 16, 128] {
            for mode in [Mode::AppendSyncData, Mode::PreallocSyncData, Mode::NoSync] {
                emit(run_batch(&dir, mode, rec, batch, secs));
            }
        }
        emit(run_batch(&dir, Mode::AppendSyncAll, rec, 1, secs));
    }
    for rec in [256usize, 4096] {
        for p in [1usize, 4, 16, 64] {
            emit(run_group(&dir, p, rec, secs, 512));
        }
    }
    let _ = std::fs::remove_dir_all(&dir);
}
