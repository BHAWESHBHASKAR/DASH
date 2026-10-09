//! usearch delete / update semantics: remove(), re-add, recall after deletions, compact(), churn.

use crate::common::*;
use crate::filtered::{load_or_build, splitmix};
use crate::vecbench::{rerank, row};
use rayon::prelude::*;
use serde_json::json;
use std::time::Instant;
use usearch::Index;

fn recall_vs(idx: &Index, data: &[f32], queries: &[f32], truth: &[Vec<u32>], rr: Option<usize>) -> f64 {
    let nq = truth.len();
    let rs: Vec<f64> = (0..nq)
        .into_par_iter()
        .map(|qi| {
            let q = row(queries, qi);
            let keys = match rr {
                None => idx.search::<f32>(q, K).unwrap().keys,
                Some(r) => rerank(data, &idx.search::<f32>(q, r).unwrap().keys, q, K),
            };
            recall_at_k(&keys, &truth[qi], K)
        })
        .collect();
    rs.iter().sum::<f64>() / nq as f64
}

fn truth_over(data: &[f32], ids: &[u32], queries: &[f32], nq: usize) -> Vec<Vec<u32>> {
    (0..nq)
        .into_par_iter()
        .map(|qi| exact_scan_ids(data, ids, row(queries, qi), K))
        .collect()
}

pub fn run(a: &Args) {
    let kind = a.s("kind", "f32");
    let n = a.u("n", 50_000);
    let ef = a.u("ef", 128);
    let rr = if kind == "i8" { Some(50) } else { None };
    let g = Gen::new();
    let (data, _) = g.base(n);
    let queries = g.queries();
    let nq = N_QUERIES;
    let gt = ground_truth(&data, n, &queries);
    let idx = load_or_build(&kind, n, &data);
    idx.change_expansion_search(ef);
    // capacity must be reserved ahead of insertions; leave 25% headroom for re-adds
    idx.reserve(n + n / 4).unwrap();
    let base = json!({"exp":"deletes","kind":kind,"n":n,"ef":ef,"rerank":rr});
    let emit_step = |step: &str, extra: serde_json::Value| {
        let mut o = base.clone();
        o["step"] = json!(step);
        o["size"] = json!(idx.size());
        o["memory_usage_mb"] = json!(idx.memory_usage() as f64 / 1048576.0);
        o["detail"] = extra;
        emit(o);
    };
    let r0 = recall_vs(&idx, &data, &queries, &gt, rr);
    emit_step("baseline", json!({"recall_at_10": r0}));

    // choose 20% to delete (seeded)
    let mut del: Vec<u32> = (0..n as u32).filter(|&i| splitmix(i as u64 ^ 0xDE1) % 5 == 0).collect();
    del.sort_unstable();
    let delset: std::collections::HashSet<u32> = del.iter().cloned().collect();
    let remain: Vec<u32> = (0..n as u32).filter(|i| !delset.contains(i)).collect();
    let t = Instant::now();
    let mut removed = 0;
    for &k in &del {
        removed += idx.remove(k as u64).unwrap();
    }
    let rem_s = t.elapsed().as_secs_f64();
    let still_contained = del.iter().filter(|&&k| idx.contains(k as u64)).count();
    emit_step(
        "remove_20pct",
        json!({"requested": del.len(), "removed": removed, "seconds": rem_s,
               "per_remove_us": rem_s * 1e6 / del.len() as f64, "still_contained": still_contained}),
    );

    // recall vs exact over remaining; and leak check
    let truth_rem = truth_over(&data, &remain, &queries, nq);
    let mut leaked = 0usize;
    for qi in 0..nq {
        let keys = idx.search::<f32>(row(&queries, qi), K).unwrap().keys;
        leaked += keys.iter().filter(|k| delset.contains(&(**k as u32))).count();
    }
    let r_del = recall_vs(&idx, &data, &queries, &truth_rem, rr);
    let r_del_256 = {
        idx.change_expansion_search(256);
        let r = recall_vs(&idx, &data, &queries, &truth_rem, rr);
        idx.change_expansion_search(ef);
        r
    };
    emit_step(
        "after_20pct_deleted_search",
        json!({"recall_at_10_vs_exact_over_remaining": r_del, "recall_ef256": r_del_256, "deleted_keys_returned": leaked}),
    );

    // save + reload keeps deletions?
    let path = scratch_dir().join(format!("del_{}_{}.usearch", kind, n));
    idx.save(path.to_str().unwrap()).unwrap();
    let sz_after_del = std::fs::metadata(&path).unwrap().len();
    emit_step("saved_with_deletions", json!({"file_mb": sz_after_del as f64 / 1048576.0}));

    // duplicate add of a live key
    let dup = idx.add(remain[0] as u64, row(&data, remain[0] as usize));
    emit_step("duplicate_add_live_key", json!({"result": format!("{:?}", dup.map_err(|e| e.what().to_string()))}));

    // compact
    let t = Instant::now();
    let c = idx.compact();
    let cs = t.elapsed().as_secs_f64();
    let r_c = recall_vs(&idx, &data, &queries, &truth_rem, rr);
    emit_step(
        "compact",
        json!({"result": format!("{:?}", c.map_err(|e| e.what().to_string())), "seconds": cs, "recall_after": r_c}),
    );

    // re-add the deleted keys with the same vectors
    let t = Instant::now();
    let mut ok = 0;
    let mut err = 0;
    let mut first_err = String::new();
    for &k in &del {
        match idx.add(k as u64, row(&data, k as usize)) {
            Ok(()) => ok += 1,
            Err(e) => {
                err += 1;
                if first_err.is_empty() {
                    first_err = e.what().to_string();
                }
            }
        }
    }
    let readd_s = t.elapsed().as_secs_f64();
    let r_re = recall_vs(&idx, &data, &queries, &gt, rr);
    emit_step(
        "readd_deleted_keys",
        json!({"ok": ok, "err": err, "first_err": first_err, "seconds": readd_s, "recall_at_10_vs_original_truth": r_re}),
    );

    // update semantics: remove + add with a new vector, then query with that vector
    let mut found = 0;
    let probes: Vec<usize> = (0..200).map(|i| (i * 97) % n).collect();
    for (pi, &k) in probes.iter().enumerate() {
        let src = (k + 12345) % n;
        let nv = row(&data, src).to_vec();
        idx.remove(k as u64).unwrap();
        idx.add(k as u64, &nv).unwrap();
        let top = idx.search::<f32>(&nv, 2).unwrap().keys;
        if top.contains(&(k as u64)) {
            found += 1;
        }
        let _ = pi;
    }
    emit_step("update_via_remove_add_200", json!({"new_vector_key_found_in_top2": found}));
    // restore the 200 vectors to their originals for the churn test
    for &k in &probes {
        idx.remove(k as u64).unwrap();
        idx.add(k as u64, row(&data, k)).unwrap();
    }

    // churn: remove random 20% + re-add, 5 rounds
    for round in 1..=5u64 {
        let ks: Vec<u32> = (0..n as u32).filter(|&i| splitmix(i as u64 ^ (round * 7919)) % 5 == 0).collect();
        let t = Instant::now();
        for &k in &ks {
            idx.remove(k as u64).unwrap();
        }
        for &k in &ks {
            idx.add(k as u64, row(&data, k as usize)).unwrap();
        }
        let s = t.elapsed().as_secs_f64();
        let r = recall_vs(&idx, &data, &queries, &gt, rr);
        emit_step("churn_round", json!({"round": round, "churned": ks.len(), "seconds": s, "recall_at_10": r}));
    }
    let _ = std::fs::remove_file(path);
}
