//! Experiment A: vector index comparison (exact scan, in-repo ANN, usearch f32/f16/i8).

use crate::common::*;
use rayon::prelude::*;
use serde_json::json;
use std::time::Instant;
use usearch::ffi::{IndexOptions, MetricKind, ScalarKind};
use usearch::{Index, new_index};

pub const M: usize = 16;
pub const EF_CONSTRUCT: usize = 128;

pub fn opts(kind: &str, m: usize, efc: usize) -> IndexOptions {
    IndexOptions {
        dimensions: DIM,
        metric: MetricKind::Cos, // inputs are L2-normalized: cosine == dot
        quantization: match kind {
            "f32" => ScalarKind::F32,
            "f16" => ScalarKind::F16,
            "i8" => ScalarKind::I8,
            _ => panic!("kind"),
        },
        connectivity: m,
        expansion_add: efc,
        expansion_search: 64,
        multi: false,
    }
}

pub fn row(data: &[f32], i: usize) -> &[f32] {
    &data[i * DIM..(i + 1) * DIM]
}

/// Rerank candidate keys with exact f32 dot products, return top `k` keys.
pub fn rerank(data: &[f32], cands: &[u64], q: &[f32], k: usize) -> Vec<u64> {
    let mut t = TopK::new(k);
    for &c in cands {
        t.push(dot(q, row(data, c as usize)), c as u32);
    }
    t.ids().into_iter().map(|x| x as u64).collect()
}

/// Build an index with `threads` insert threads. Returns seconds.
pub fn build(idx: &Index, data: &[f32], n: usize, threads: usize) -> f64 {
    let (_, s) = time(|| {
        idx.reserve_capacity_and_threads(n, threads.max(1)).unwrap();
        if threads <= 1 {
            for i in 0..n {
                idx.add(i as u64, row(data, i)).unwrap();
            }
        } else {
            pool(threads).install(|| {
                (0..n).into_par_iter().for_each(|i| {
                    idx.add(i as u64, row(data, i)).unwrap();
                })
            });
        }
    });
    s
}

/// One search configuration: ef and optional rerank width.
pub struct SearchCfg {
    pub ef: usize,
    pub rerank: Option<usize>,
}

pub fn search_one(idx: &Index, data: &[f32], q: &[f32], cfg: &SearchCfg) -> Vec<u64> {
    match cfg.rerank {
        None => idx.search::<f32>(q, K).unwrap().keys,
        Some(r) => {
            let c = idx.search::<f32>(q, r).unwrap().keys;
            rerank(data, &c, q, K)
        }
    }
}

/// Recall + 1-thread latency + 4-thread QPS (and latency under load).
pub fn eval(idx: &Index, data: &[f32], queries: &[f32], gt: &[Vec<u32>], cfg: &SearchCfg) -> serde_json::Value {
    idx.change_expansion_search(cfg.ef);
    let nq = queries.len() / DIM;
    // warm-up
    for qi in 0..50.min(nq) {
        let _ = search_one(idx, data, row(queries, qi), cfg);
    }
    let mut lat = Lat::new();
    let mut rec = 0.0;
    for qi in 0..nq {
        let q = row(queries, qi);
        let t = Instant::now();
        let r = search_one(idx, data, q, cfg);
        lat.rec(t.elapsed());
        rec += recall_at_k(&r, &gt[qi], K);
    }
    let recall = rec / nq as f64;
    // 4 threads, each query repeated 4x
    let reps = 4;
    let t = Instant::now();
    let lats: Vec<Lat> = pool(4).install(|| {
        (0..nq * reps)
            .into_par_iter()
            .fold(Lat::new, |mut l, i| {
                let q = row(queries, i % nq);
                let t = Instant::now();
                let _ = search_one(idx, data, q, cfg);
                l.rec(t.elapsed());
                l
            })
            .collect()
    });
    let wall = t.elapsed().as_secs_f64();
    let mut l4 = Lat::new();
    for l in &lats {
        l4.merge(l);
    }
    json!({
        "ef": cfg.ef, "rerank": cfg.rerank,
        "recall_at_10": (recall * 10000.0).round() / 10000.0,
        "lat_1t": lat.json(),
        "qps_1t": (1e6 / lat.0.mean()).round(),
        "qps_4t": ((nq * reps) as f64 / wall).round(),
        "lat_4t": l4.json(),
    })
}

fn try_drop_caches() -> bool {
    std::fs::write("/proc/sys/vm/drop_caches", "3").is_ok()
}

pub fn run(a: &Args) {
    let kind = a.s("kind", "f32");
    if kind == "repo" {
        return crate::repoann::run(a);
    }
    let n = a.u("n", 50_000);
    let threads = a.u("threads", 4);
    let full = a.s("eval", "full") == "full";
    let m = a.u("m", M);
    let efc = a.u("efc", EF_CONSTRUCT);
    let g = Gen::new();
    let (gen_data, gen_s) = {
        let (d, s) = time(|| g.base(n));
        (d.0, s)
    };
    let data = gen_data;
    let queries = g.queries();
    let (gt, gt_s) = time(|| ground_truth(&data, n, &queries));
    let rss_base = rss_mb();
    let nq = N_QUERIES;
    let base = json!({"exp":"A","kind":kind,"n":n,"dim":DIM,"threads_build":threads,"m":m,"ef_construction":efc,
        "gen_s":gen_s,"gt_s":gt_s,"rss_base_mb":rss_base.round()});

    if kind == "exact" {
        // single-thread latency (first 200 queries), 4-thread QPS (each query single-threaded)
        let nlat = 200.min(nq);
        let mut lat = Lat::new();
        let mut rec = 0.0;
        for qi in 0..nlat {
            let t = Instant::now();
            let r = exact_scan(&data, n, row(&queries, qi), K);
            lat.rec(t.elapsed());
            rec += recall_at_k(&r.iter().map(|x| *x as u64).collect::<Vec<_>>(), &gt[qi], K);
        }
        let reps = 2;
        let t = Instant::now();
        let lats: Vec<Lat> = pool(4).install(|| {
            (0..nlat * reps)
                .into_par_iter()
                .fold(Lat::new, |mut l, i| {
                    let t = Instant::now();
                    let _ = exact_scan(&data, n, row(&queries, i % nlat), K);
                    l.rec(t.elapsed());
                    l
                })
                .collect()
        });
        let wall = t.elapsed().as_secs_f64();
        let mut l4 = Lat::new();
        for l in &lats {
            l4.merge(l);
        }
        let mut o = base.clone();
        o["recall_at_10"] = json!(rec / nlat as f64);
        o["queries_timed"] = json!(nlat);
        o["lat_1t"] = lat.json();
        o["qps_1t"] = json!((1e6 / lat.0.mean()).round());
        o["qps_4t"] = json!(((nlat * reps) as f64 / wall).round());
        o["lat_4t"] = l4.json();
        o["peak_rss_mb"] = json!(peak_rss_mb().round());
        emit(o);
        return;
    }

    // ---- usearch build
    let idx = new_index(&opts(&kind, m, efc)).unwrap();
    let bs = build(&idx, &data, n, threads);
    let peak_build = peak_rss_mb();
    let mem_usage = idx.memory_usage();
    let path = scratch_dir().join(format!("idx_{}_{}_t{}.usearch", kind, n, threads));
    let (_, save_s) = time(|| idx.save(path.to_str().unwrap()).unwrap());
    let file_bytes = std::fs::metadata(&path).unwrap().len();
    let mut o = base.clone();
    o["phase"] = json!("build");
    o["build_s"] = json!(bs);
    o["build_vec_per_s"] = json!((n as f64 / bs).round());
    o["peak_rss_mb"] = json!(peak_build.round());
    o["peak_rss_minus_base_mb"] = json!((peak_build - rss_base).round());
    o["index_memory_usage_mb"] = json!((mem_usage as f64 / 1048576.0).round());
    o["save_s"] = json!(save_s);
    o["file_mb"] = json!((file_bytes as f64 / 1048576.0 * 10.0).round() / 10.0);
    o["hw_accel"] = json!(idx.hardware_acceleration());
    emit(o);
    drop(idx);

    // ---- load (copy into RAM) vs view (mmap)
    let rss_before_load = rss_mb();
    let idx = new_index(&opts(&kind, m, efc)).unwrap();
    let (_, load_s) = time(|| idx.load(path.to_str().unwrap()).unwrap());
    let rss_after_load = rss_mb();
    let v = new_index(&opts(&kind, m, efc)).unwrap();
    let (_, view_s) = time(|| v.view(path.to_str().unwrap()).unwrap());
    let rss_after_view = rss_mb();
    // first-query + 1000-query pass on viewed index (page cache warm)
    v.change_expansion_search(128);
    let cfg = SearchCfg { ef: 128, rerank: if kind == "i8" { Some(50) } else { None } };
    let mut vl = Lat::new();
    let t = Instant::now();
    for qi in 0..nq {
        let t = Instant::now();
        let _ = search_one(&v, &data, row(&queries, qi), &cfg);
        vl.rec(t.elapsed());
    }
    let view_pass_s = t.elapsed().as_secs_f64();
    drop(v);
    // cold-cache view (needs privileges to drop caches)
    let cold = if try_drop_caches() {
        let v = new_index(&opts(&kind, m, efc)).unwrap();
        let (_, cold_view_s) = time(|| v.view(path.to_str().unwrap()).unwrap());
        let mut cl = Lat::new();
        for qi in 0..200 {
            let t = Instant::now();
            let _ = search_one(&v, &data, row(&queries, qi), &cfg);
            cl.rec(t.elapsed());
        }
        let mut o = cl.json();
        o["view_s"] = json!(cold_view_s);
        Some(o)
    } else {
        None
    };
    let mut o = base.clone();
    o["phase"] = json!("load_view");
    o["load_s"] = json!(load_s);
    o["view_s"] = json!(view_s);
    o["rss_delta_load_mb"] = json!((rss_after_load - rss_before_load).round());
    o["rss_delta_view_mb"] = json!((rss_after_view - rss_after_load).round());
    o["view_lat_1t_ef128"] = vl.json();
    o["view_pass_s"] = json!(view_pass_s);
    o["cold_cache_view_queries200"] = json!(cold);
    emit(o);

    // ---- search sweeps on the loaded (RAM) index
    let efs: Vec<usize> = if full { vec![32, 64, 128, 256, 512] } else { vec![128] };
    let mut cfgs: Vec<SearchCfg> = vec![];
    for &ef in &efs {
        cfgs.push(SearchCfg { ef, rerank: None });
        if kind == "i8" {
            cfgs.push(SearchCfg { ef: ef.max(50), rerank: Some(50) });
        }
    }
    for c in cfgs {
        let mut o = base.clone();
        o["phase"] = json!("search");
        let r = eval(&idx, &data, &queries, &gt, &c);
        o["result"] = r;
        emit(o);
    }
    if a.s("keep", "0") != "1" {
        let _ = std::fs::remove_file(path);
    }
}
