//! Experiment D: memtable + immutable segment concurrency model.
//!
//! * big segment: usearch index loaded from the file written by experiment A (immutable, shared via
//!   `arc_swap::ArcSwap<Snapshot>`);
//! * memtable: a small usearch f32 index receiving inserts from one thread at a paced rate;
//! * query = search big (+ exact f32 rerank for i8) and memtable, merge by cosine score.

use crate::common::*;
use crate::vecbench::{opts, row};
use arc_swap::ArcSwap;
use serde_json::json;
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use usearch::{Index, new_index};

struct Snapshot {
    big: Arc<Index>,
    mem: Arc<Index>,
    /// Flat (brute-force) memtable alternative: f32 rows appended under a lock.
    flat: Arc<RwLock<Vec<f32>>>,
    flat_base: usize,
}

fn search_flat(sn: &Snapshot, q: &[f32]) -> Vec<(f32, u64)> {
    let g = sn.flat.read().unwrap();
    let mut t = TopK::new(K);
    for (i, r) in g.chunks_exact(DIM).enumerate() {
        t.push(dot(q, r), i as u32);
    }
    t.v.iter().map(|(s, i)| (*s, (sn.flat_base + *i as usize) as u64)).collect()
}

#[derive(Clone, Copy)]
struct Cfg {
    ef: usize,
    rerank: Option<usize>,
}

fn new_mem(cap: usize) -> Arc<Index> {
    let idx = new_index(&opts("f32", 16, 128)).unwrap();
    idx.reserve_capacity_and_threads(cap, 8).unwrap();
    Arc::new(idx)
}

/// Exact-scored (cosine, key) candidates from the big segment.
fn search_big(big: &Index, data: &[f32], q: &[f32], cfg: Cfg) -> Vec<(f32, u64)> {
    match cfg.rerank {
        None => {
            let m = big.search::<f32>(q, K).unwrap();
            m.keys.into_iter().zip(m.distances).map(|(k, d)| (1.0 - d, k)).collect()
        }
        Some(r) => {
            let m = big.search::<f32>(q, r).unwrap();
            let mut v: Vec<(f32, u64)> = m.keys.iter().map(|&k| (dot(q, row(data, k as usize)), k)).collect();
            v.sort_by(|a, b| b.0.total_cmp(&a.0));
            v.truncate(K);
            v
        }
    }
}

fn search_mem(mem: &Index, q: &[f32]) -> Vec<(f32, u64)> {
    if mem.size() == 0 {
        return vec![];
    }
    let m = mem.search::<f32>(q, K).unwrap();
    m.keys.into_iter().zip(m.distances).map(|(k, d)| (1.0 - d, k)).collect()
}

fn merge(mut a: Vec<(f32, u64)>, b: Vec<(f32, u64)>) -> Vec<u64> {
    a.extend(b);
    a.sort_by(|x, y| y.0.total_cmp(&x.0));
    a.into_iter().take(K).map(|x| x.1).collect()
}

enum Mode {
    BigOnly,
    BigPlusMem,
    BigPlusFlat,
    Single(Arc<Index>),
}

#[allow(clippy::too_many_arguments)]
fn run_queries(
    label: &str,
    mode: &Mode,
    snap: &ArcSwap<Snapshot>,
    data: &[f32],
    queries: &[f32],
    cfg: Cfg,
    threads: usize,
    dur: Duration,
    inserter: Option<(usize, usize, usize, bool)>, // (rate/s, rotate cap, first key, into flat memtable)
    base: &serde_json::Value,
) {
    let stop = AtomicBool::new(false);
    let total = AtomicUsize::new(0);
    let nq = queries.len() / DIM;
    let mut ins_lat = Lat::new();
    let mut inserted = 0usize;
    let mut rotations = 0usize;
    let mut qlats: Vec<Lat> = Vec::new();
    let t0 = Instant::now();
    std::thread::scope(|s| {
        let mut hs = vec![];
        for tid in 0..threads {
            let (stop, total) = (&stop, &total);
            hs.push(s.spawn(move || {
                let mut lat = Lat::new();
                let mut i = tid * 137;
                while !stop.load(Ordering::Relaxed) {
                    let q = row(queries, i % nq);
                    i += 1;
                    let t = Instant::now();
                    let _r = match mode {
                        Mode::BigOnly => {
                            let sn = snap.load();
                            search_big(&sn.big, data, q, cfg).into_iter().map(|x| x.1).collect::<Vec<_>>()
                        }
                        Mode::BigPlusMem => {
                            let sn = snap.load();
                            merge(search_big(&sn.big, data, q, cfg), search_mem(&sn.mem, q))
                        }
                        Mode::BigPlusFlat => {
                            let sn = snap.load();
                            merge(search_big(&sn.big, data, q, cfg), search_flat(&sn, q))
                        }
                        Mode::Single(idx) => search_big(idx, data, q, cfg).into_iter().map(|x| x.1).collect(),
                    };
                    lat.rec(t.elapsed());
                    std::hint::black_box(&_r);
                }
                total.fetch_add(lat.0.len() as usize, Ordering::Relaxed);
                lat
            }));
        }
        let ih = inserter.map(|(rate, cap, first, to_flat)| {
            let stop = &stop;
            s.spawn(move || {
                let mut lat = Lat::new();
                let mut n_ins = 0usize;
                let mut rot = 0usize;
                let start = Instant::now();
                let mut next_key = first;
                let mut in_mem = 0usize;
                let mut mem = snap.load().mem.clone();
                while !stop.load(Ordering::Relaxed) {
                    // pace: target n_ins = rate * elapsed
                    let target = (start.elapsed().as_secs_f64() * rate as f64) as usize;
                    if n_ins >= target {
                        std::thread::sleep(Duration::from_micros(500));
                        continue;
                    }
                    if in_mem >= cap {
                        let big = snap.load().big.clone();
                        if to_flat {
                            snap.store(Arc::new(Snapshot {
                                big,
                                mem: mem.clone(),
                                flat: Arc::new(RwLock::new(Vec::with_capacity(cap * DIM))),
                                flat_base: next_key,
                            }));
                        } else {
                            let fresh = new_mem(cap + 16);
                            snap.store(Arc::new(Snapshot { big, mem: fresh.clone(), flat: Arc::new(RwLock::new(Vec::new())), flat_base: 0 }));
                            mem = fresh;
                        }
                        in_mem = 0;
                        rot += 1;
                    }
                    let t = Instant::now();
                    if to_flat {
                        snap.load().flat.write().unwrap().extend_from_slice(row(data, next_key));
                    } else {
                        mem.add(next_key as u64, row(data, next_key)).unwrap();
                    }
                    lat.rec(t.elapsed());
                    next_key += 1;
                    in_mem += 1;
                    n_ins += 1;
                }
                (lat, n_ins, rot, start.elapsed().as_secs_f64())
            })
        });
        std::thread::sleep(dur);
        stop.store(true, Ordering::Relaxed);
        for h in hs {
            qlats.push(h.join().unwrap());
        }
        if let Some(h) = ih {
            let (l, n, r, el) = h.join().unwrap();
            ins_lat = l;
            inserted = n;
            rotations = r;
            let _ = el;
        }
    });
    let wall = t0.elapsed().as_secs_f64();
    let mut all = Lat::new();
    for l in &qlats {
        all.merge(l);
    }
    let mut o = base.clone();
    o["config"] = json!(label);
    o["query_threads"] = json!(threads);
    o["duration_s"] = json!(wall);
    o["queries"] = json!(all.0.len());
    o["qps"] = json!((all.0.len() as f64 / wall).round());
    o["lat"] = all.json();
    if inserter.is_some() {
        o["insert_rate_target"] = json!(inserter.unwrap().0);
        o["insert_rate_achieved"] = json!((inserted as f64 / wall).round());
        o["insert_lat"] = ins_lat.json();
        o["memtable_rotations"] = json!(rotations);
    }
    emit(o);
}

pub fn run(a: &Args) {
    let kind = a.s("kind", "i8");
    let n = a.u("n", 200_000);
    let ef = a.u("ef", 128);
    let mem_static = a.u("mem", 5_000);
    let dur = Duration::from_secs_f64(a.f("dur", 8.0));
    let rate = a.u("rate", 2000);
    let rotate_cap = a.u("rotate_cap", 4000);
    let extra = 60_000usize;
    let g = Gen::new();
    let (data, _) = g.base(n + extra);
    let queries = g.queries();
    let gt = ground_truth(&data, n, &queries);
    let cfg = Cfg { ef, rerank: if kind == "i8" { Some(50) } else { None } };

    let path = scratch_dir().join(format!("idx_{}_{}_t4.usearch", kind, n));
    let big = new_index(&opts(&kind, 16, 128)).unwrap();
    if path.exists() {
        big.load(path.to_str().unwrap()).unwrap();
    } else {
        crate::vecbench::build(&big, &data, n, 4);
        big.save(path.to_str().unwrap()).unwrap();
    }
    big.change_expansion_search(ef);
    let big = Arc::new(big);
    let base = json!({"exp":"D","kind":kind,"n_big":n,"ef":ef,"rerank":cfg.rerank,"mem_static":mem_static});

    // static memtable with `mem_static` vectors (keys n..n+mem_static)
    let mem = new_mem(mem_static + 16);
    mem.change_expansion_search(ef);
    for j in 0..mem_static {
        mem.add((n + j) as u64, row(&data, n + j)).unwrap();
    }
    // single index with the union
    let single = new_index(&opts(&kind, 16, 128)).unwrap();
    single.load(path.to_str().unwrap()).unwrap();
    single.reserve(n + extra).unwrap();
    for j in 0..mem_static {
        single.add((n + j) as u64, row(&data, n + j)).unwrap();
    }
    single.change_expansion_search(ef);
    let single = Arc::new(single);

    // recall of merged / single vs exact union truth
    let mem_ids: Vec<u32> = (n..n + mem_static).map(|x| x as u32).collect();
    let (mut r_big, mut r_merge, mut r_single) = (0.0, 0.0, 0.0);
    let nq = N_QUERIES;
    for qi in 0..nq {
        let q = row(&queries, qi);
        let mut u: Vec<(f32, u32)> = gt[qi].iter().map(|&i| (dot(q, row(&data, i as usize)), i)).collect();
        for &i in &exact_scan_ids(&data, &mem_ids, q, K) {
            u.push((dot(q, row(&data, i as usize)), i));
        }
        u.sort_by(|a, b| b.0.total_cmp(&a.0));
        let truth: Vec<u32> = u.iter().take(K).map(|x| x.1).collect();
        let big_only: Vec<u64> = search_big(&big, &data, q, cfg).into_iter().map(|x| x.1).collect();
        let merged = merge(search_big(&big, &data, q, cfg), search_mem(&mem, q));
        let sing: Vec<u64> = search_big(&single, &data, q, cfg).into_iter().map(|x| x.1).collect();
        r_big += recall_at_k(&big_only, &truth, K);
        r_merge += recall_at_k(&merged, &truth, K);
        r_single += recall_at_k(&sing, &truth, K);
    }
    let mut o = base.clone();
    o["phase"] = json!("recall_union_truth");
    o["recall_big_only_vs_union_truth"] = json!(r_big / nq as f64);
    o["recall_big_plus_mem_merged"] = json!(r_merge / nq as f64);
    o["recall_single_index_with_union"] = json!(r_single / nq as f64);
    emit(o);

    let mut flat_v = Vec::with_capacity(mem_static * DIM);
    for j in 0..mem_static {
        flat_v.extend_from_slice(row(&data, n + j));
    }
    let snap = ArcSwap::from_pointee(Snapshot { big: big.clone(), mem: mem.clone(), flat: Arc::new(RwLock::new(flat_v)), flat_base: n });
    let mut o = base.clone();
    o["phase"] = json!("throughput");
    for t in [1usize, 2, 4] {
        run_queries("big_only", &Mode::BigOnly, &snap, &data, &queries, cfg, t, dur, None, &o);
    }
    for t in [1usize, 2, 4] {
        run_queries("big_plus_static_memtable", &Mode::BigPlusMem, &snap, &data, &queries, cfg, t, dur, None, &o);
    }
    for t in [1usize, 2, 4] {
        run_queries("big_plus_static_flat_memtable", &Mode::BigPlusFlat, &snap, &data, &queries, cfg, t, dur, None, &o);
    }
    for t in [1usize, 2, 4] {
        run_queries("single_index_union", &Mode::Single(single.clone()), &snap, &data, &queries, cfg, t, dur, None, &o);
    }
    // with concurrent inserter into a rotating memtable
    for t in [1usize, 2, 3, 4] {
        let fresh = new_mem(rotate_cap + 16);
        fresh.change_expansion_search(ef);
        snap.store(Arc::new(Snapshot { big: big.clone(), mem: fresh, flat: Arc::new(RwLock::new(Vec::new())), flat_base: n + mem_static }));
        run_queries(
            "big_plus_hnsw_memtable_with_inserter",
            &Mode::BigPlusMem,
            &snap,
            &data,
            &queries,
            cfg,
            t,
            dur,
            Some((rate, rotate_cap, n + mem_static, false)),
            &o,
        );
    }
    for t in [1usize, 2, 3, 4] {
        snap.store(Arc::new(Snapshot { big: big.clone(), mem: new_mem(16), flat: Arc::new(RwLock::new(Vec::new())), flat_base: n + mem_static }));
        run_queries(
            "big_plus_flat_memtable_with_inserter",
            &Mode::BigPlusFlat,
            &snap,
            &data,
            &queries,
            cfg,
            t,
            dur,
            Some((rate, rotate_cap, n + mem_static, true)),
            &o,
        );
    }
    // big only with the inserter running into an unused memtable (isolates insert-thread CPU contention)
    o["phase"] = json!("done");
    o["peak_rss_mb"] = json!(peak_rss_mb().round());
    emit(o);
}
