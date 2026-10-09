//! The in-repo ANN, driven through `store`'s public API only
//! (`ingest_bundle` + `upsert_claim_vector`, then `ann_vector_top_candidates`).

use crate::common::*;
use crate::vecbench::row;
use rayon::prelude::*;
use schema::Claim;
use serde_json::json;
use std::time::Instant;
use store::{AnnTuningConfig, InMemoryStore};

pub fn claim(i: usize, text: String) -> Claim {
    Claim {
        claim_id: format!("c{i}"),
        tenant_id: "t0".into(),
        canonical_text: text,
        confidence: 0.5,
        event_time_unix: Some(1_700_000_000 + i as i64),
        entities: vec![],
        embedding_ids: vec![],
        claim_type: None,
        valid_from: None,
        valid_to: None,
        created_at: None,
        updated_at: None,
    }
}

fn tuning(ef: usize) -> AnnTuningConfig {
    AnnTuningConfig {
        max_neighbors_base: 12,
        max_neighbors_upper: 6,
        search_expansion_factor: 1,
        search_expansion_min: ef,
        search_expansion_max: ef,
    }
}

fn ids_to_u64(v: Vec<String>) -> Vec<u64> {
    v.into_iter().map(|s| s[1..].parse().unwrap()).collect()
}

fn eval_point(store: &mut InMemoryStore, data: &[f32], n: usize, queries: &[f32], base: &serde_json::Value) {
    let (gt, gt_s) = time(|| ground_truth(&data[..n * DIM], n, queries));
    let nq = queries.len() / DIM;
    for ef in [32usize, 64, 128, 256] {
        store.set_ann_tuning(tuning(ef));
        let st: &InMemoryStore = store;
        let mut lat = Lat::new();
        let mut rec = 0.0;
        for qi in 0..nq {
            let t = Instant::now();
            let r = ids_to_u64(st.ann_vector_top_candidates("t0", row(queries, qi), K));
            lat.rec(t.elapsed());
            rec += recall_at_k(&r, &gt[qi], K);
        }
        let reps = 2;
        let t = Instant::now();
        pool(4).install(|| {
            (0..nq * reps).into_par_iter().for_each(|i| {
                let _ = st.ann_vector_top_candidates("t0", row(queries, i % nq), K);
            })
        });
        let wall = t.elapsed().as_secs_f64();
        let mut o = base.clone();
        o["phase"] = json!("search");
        o["n"] = json!(n);
        o["gt_s"] = json!(gt_s);
        o["result"] = json!({
            "ef": ef, "recall_at_10": (rec / nq as f64 * 10000.0).round() / 10000.0,
            "lat_1t": lat.json(),
            "qps_1t": (1e6 / lat.0.mean()).round(),
            "qps_4t": ((nq * reps) as f64 / wall).round(),
        });
        emit(o);
    }
}

pub fn run(a: &Args) {
    let n_max = a.u("n", 50_000);
    let cutoff = a.f("cutoff", 300.0);
    let g = Gen::new();
    let (data, _) = g.base(n_max);
    let queries = g.queries();
    let rss_base = rss_mb();
    let base = json!({"exp":"A","kind":"repo","dim":DIM,"threads_build":1,
        "params":{"max_neighbors_base":12,"max_neighbors_upper":6,"levels":4},"rss_base_mb":rss_base.round()});
    let mut store = InMemoryStore::new_with_ann_tuning(tuning(128));
    let eval_at: Vec<usize> = a.s("eval_at", "10000").split(',').map(|x| x.parse().unwrap()).collect();
    let t0 = Instant::now();
    let mut timed_excl_eval = 0.0f64;
    let mut last_n = 0;
    for i in 0..n_max {
        let id = format!("c{i}");
        store.ingest_bundle(claim(i, "x".into()), vec![], vec![]).unwrap();
        store.upsert_claim_vector(&id, row(&data, i).to_vec()).unwrap();
        let n = i + 1;
        last_n = n;
        let step = if n <= 10_000 { 1000 } else { 2500 };
        if n % step == 0 {
            let el = t0.elapsed().as_secs_f64() - timed_excl_eval;
            let mut o = base.clone();
            o["phase"] = json!("build_point");
            o["n"] = json!(n);
            o["cum_build_s"] = json!(el);
            emit(o);
            if eval_at.contains(&n) {
                let te = Instant::now();
                eval_point(&mut store, &data, n, &queries, &base);
                timed_excl_eval += te.elapsed().as_secs_f64();
            }
            if el > cutoff {
                break;
            }
        }
    }
    let el = t0.elapsed().as_secs_f64() - timed_excl_eval;
    let mut o = base.clone();
    o["phase"] = json!("build_final");
    o["n"] = json!(last_n);
    o["cum_build_s"] = json!(el);
    o["peak_rss_mb"] = json!(peak_rss_mb().round());
    o["peak_rss_minus_base_mb"] = json!((peak_rss_mb() - rss_base).round());
    emit(o);
    if !eval_at.contains(&last_n) {
        eval_point(&mut store, &data, last_n, &queries, &base);
    }
}
