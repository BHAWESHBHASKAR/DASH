//! Experiment B: filtered vector search at several selectivities.
//!
//! Two filter models, both defined over the same synthetic data:
//!  * `uncorrelated`: allowed iff splitmix(i) < sel (models a time-range predicate; the
//!    allowed set is independent of the vector, identical for every query).
//!  * `correlated`: per query, allowed iff ((cluster_i - cluster_q) mod C) + u_i < sel*C with
//!    u_i uniform in [0,1) (models an entity filter: allowed rows are semantically near the
//!    query's own cluster, the usual shape of "claims about entity X").

use crate::common::*;
use crate::vecbench::{opts, row};
use rayon::prelude::*;
use serde_json::json;
use std::time::Instant;
use usearch::Index;
use usearch::new_index;

pub fn splitmix(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E3779B97F4A7C15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
    z ^ (z >> 31)
}
fn unit(i: usize, stream: u64) -> f64 {
    (splitmix(i as u64 ^ stream) >> 11) as f64 / (1u64 << 53) as f64
}

pub fn load_or_build(kind: &str, n: usize, data: &[f32]) -> Index {
    let path = scratch_dir().join(format!("idx_{}_{}_t4.usearch", kind, n));
    let idx = new_index(&opts(kind, 16, 128)).unwrap();
    if path.exists() {
        idx.load(path.to_str().unwrap()).unwrap();
    } else {
        crate::vecbench::build(&idx, data, n, 4);
        idx.save(path.to_str().unwrap()).unwrap();
        let idx2 = new_index(&opts(kind, 16, 128)).unwrap();
        idx2.load(path.to_str().unwrap()).unwrap();
        return idx2;
    }
    idx
}

fn lat_summary(l: &Lat) -> serde_json::Value {
    l.json()
}

pub fn run(a: &Args) {
    let kind = a.s("kind", "i8");
    let n = a.u("n", 200_000);
    let nq = a.u("nq", 500);
    let rerank_w = 50usize;
    let g = Gen::new();
    let (data, cl) = g.base(n);
    let queries = g.queries();
    // query cluster ids (regenerate with cluster output)
    let qcl = query_clusters(&g);
    let idx = load_or_build(&kind, n, &data);
    let nc = nclusters();
    let sels = [1.0f64, 0.3, 0.1, 0.03, 0.01, 0.003, 0.001];
    let u_attr: Vec<f64> = (0..n).into_par_iter().map(|i| unit(i, 0xA11CE)).collect();
    let u_sub: Vec<f64> = (0..n).into_par_iter().map(|i| unit(i, 0xB0B)).collect();

    for mode in ["uncorrelated", "correlated"] {
        for &sel in &sels {
            // allowed-set builder
            let allowed = |qi: usize| -> (Vec<u32>, Vec<bool>) {
                let mut bm = vec![false; n];
                let mut ids = Vec::new();
                for i in 0..n {
                    let ok = if mode == "uncorrelated" {
                        u_attr[i] < sel
                    } else {
                        let d = (cl[i] as usize + nc - qcl[qi] as usize) % nc;
                        (d as f64 + u_sub[i]) < sel * nc as f64
                    };
                    if ok {
                        bm[i] = true;
                        ids.push(i as u32);
                    }
                }
                (ids, bm)
            };
            let shared = if mode == "uncorrelated" { Some(allowed(0)) } else { None };
            let get = |qi: usize| -> (Vec<u32>, Vec<bool>) {
                match &shared {
                    Some((a, b)) => (a.clone(), b.clone()),
                    None => allowed(qi),
                }
            };
            // ground truth (exact over allowed set), parallel, untimed
            let truth: Vec<Vec<u32>> = (0..nq)
                .into_par_iter()
                .map(|qi| {
                    let (ids, _) = match &shared {
                        Some((a, b)) => (a.clone(), b.clone()),
                        None => allowed(qi),
                    };
                    exact_scan_ids(&data, &ids, row(&queries, qi), K)
                })
                .collect();
            let avg_allowed = (0..nq.min(50)).map(|qi| get(qi).0.len()).sum::<usize>() as f64 / nq.min(50) as f64;
            let base = json!({"exp":"B","kind":kind,"n":n,"mode":mode,"selectivity":sel,"avg_allowed":avg_allowed.round(),"nq":nq});

            // M3: pre-filter + exact scan (id list prepared outside timing), first 100 queries
            {
                let mut lat = Lat::new();
                let mut rec = 0.0;
                let nt = 100.min(nq);
                for qi in 0..nt {
                    let (ids, _) = get(qi);
                    let t = Instant::now();
                    let r = exact_scan_ids(&data, &ids, row(&queries, qi), K);
                    lat.rec(t.elapsed());
                    rec += recall_at_k(&r.iter().map(|x| *x as u64).collect::<Vec<_>>(), &truth[qi], K);
                }
                let mut o = base.clone();
                o["method"] = json!("prefilter_exact_f32");
                o["recall_at_10"] = json!(rec / nt as f64);
                o["lat"] = lat_summary(&lat);
                o["queries_timed"] = json!(nt);
                emit(o);
            }

            // M1: usearch filtered_search predicate
            for ef in [64usize, 256] {
                idx.change_expansion_search(ef);
                let mut lat = Lat::new();
                let mut rec = 0.0;
                for qi in 0..nq {
                    let (_, bm) = get(qi);
                    let q = row(&queries, qi);
                    let t = Instant::now();
                    let count = if kind == "i8" { rerank_w } else { K };
                    let m = idx.filtered_search::<f32, _>(q, count, |k| bm[k as usize]).unwrap();
                    let r = if kind == "i8" { crate::vecbench::rerank(&data, &m.keys, q, K) } else { m.keys };
                    lat.rec(t.elapsed());
                    rec += recall_at_k(&r, &truth[qi], K);
                }
                let mut o = base.clone();
                o["method"] = json!("hnsw_filtered_predicate");
                o["ef"] = json!(ef);
                o["recall_at_10"] = json!((rec / nq as f64 * 10000.0).round() / 10000.0);
                o["lat"] = lat_summary(&lat);
                emit(o);
            }

            // M2: post-filter with oversampling
            for over in [10usize, 100, 1000] {
                idx.change_expansion_search(64);
                let mut lat = Lat::new();
                let mut rec = 0.0;
                for qi in 0..nq {
                    let (_, bm) = get(qi);
                    let q = row(&queries, qi);
                    let t = Instant::now();
                    let m = idx.search::<f32>(q, K * over).unwrap();
                    let surv: Vec<u64> = m.keys.into_iter().filter(|k| bm[*k as usize]).collect();
                    let r = if kind == "i8" {
                        crate::vecbench::rerank(&data, &surv, q, K)
                    } else {
                        surv.into_iter().take(K).collect()
                    };
                    lat.rec(t.elapsed());
                    rec += recall_at_k(&r, &truth[qi], K);
                }
                let mut o = base.clone();
                o["method"] = json!("hnsw_postfilter");
                o["oversample"] = json!(over);
                o["recall_at_10"] = json!((rec / nq as f64 * 10000.0).round() / 10000.0);
                o["lat"] = lat_summary(&lat);
                emit(o);
            }
        }
    }
}

fn query_clusters(_g: &Gen) -> Vec<u16> {
    crate::common::query_cluster_ids()
}
