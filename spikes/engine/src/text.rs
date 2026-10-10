//! Experiment C: text index. In-repo inverted index + BM25 (through `store`) vs tantivy.
//!
//! Corpus: Zipf(s=1.0) over a 50k vocabulary, 30..=60 tokens per doc, 20 tenants (doc % 20).
//! Queries are OR queries of 1, 3 or 6 distinct terms sampled from a random document
//! ("natural" mix, Zipf-weighted, so head terms dominate) or from terms of rank >= 100
//! ("midtail" mix, i.e. with the most common terms treated as stop words).

use crate::common::*;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use rayon::prelude::*;
use schema::{RetrievalRequest, StanceMode};
use serde_json::json;
use std::time::Instant;
use store::InMemoryStore;
use tantivy::collector::TopDocs;
use tantivy::query::{BooleanQuery, ConstScoreQuery, Occur, Query, TermQuery};
use tantivy::schema::{
    Field, IndexRecordOption, STRING, Schema, TextFieldIndexing, TextOptions, FAST,
};
use tantivy::{Index, IndexWriter, TantivyDocument, Term, doc};

const VOCAB: usize = 50_000;
const TENANTS: usize = 20;

struct Corpus {
    docs: Vec<Vec<u32>>,
}

fn gen_corpus(n: usize) -> Corpus {
    let mut cdf = Vec::with_capacity(VOCAB);
    let mut acc = 0.0f64;
    for r in 0..VOCAB {
        acc += 1.0 / (r as f64 + 1.0);
        cdf.push(acc);
    }
    let total = acc;
    let docs: Vec<Vec<u32>> = (0..n)
        .into_par_iter()
        .map(|i| {
            let mut rng = StdRng::seed_from_u64(SEED ^ 0xD0C5 ^ (i as u64).wrapping_mul(0x9E3779B97F4A7C15));
            let len = rng.gen_range(30..=60);
            (0..len)
                .map(|_| {
                    let u: f64 = rng.r#gen::<f64>() * total;
                    cdf.partition_point(|&c| c < u).min(VOCAB - 1) as u32
                })
                .collect()
        })
        .collect();
    Corpus { docs }
}

fn doc_text(d: &[u32]) -> String {
    let mut s = String::with_capacity(d.len() * 7);
    for (i, t) in d.iter().enumerate() {
        if i > 0 {
            s.push(' ');
        }
        s.push('w');
        s.push_str(&t.to_string());
    }
    s
}

#[derive(Clone)]
struct Q {
    tenant: usize,
    terms: Vec<u32>,
}

fn gen_queries(c: &Corpus, len: usize, mix: &str, n: usize) -> Vec<Q> {
    let mut rng = StdRng::seed_from_u64(SEED ^ 0x0E57 ^ (len as u64) << 8 ^ (mix.len() as u64));
    let mut out = Vec::new();
    while out.len() < n {
        let di = rng.gen_range(0..c.docs.len());
        let mut terms: Vec<u32> = Vec::new();
        let d = &c.docs[di];
        for _ in 0..50 {
            let t = d[rng.gen_range(0..d.len())];
            if mix == "midtail" && t < 100 {
                continue;
            }
            if !terms.contains(&t) {
                terms.push(t);
            }
            if terms.len() == len {
                break;
            }
        }
        if terms.len() == len {
            out.push(Q { tenant: di % TENANTS, terms });
        }
    }
    out
}

fn qtext(q: &Q) -> String {
    q.terms.iter().map(|t| format!("w{t}")).collect::<Vec<_>>().join(" ")
}

// ---------------------------------------------------------------------------
// tantivy
// ---------------------------------------------------------------------------

struct Tv {
    index: Index,
    body: Field,
    id: Field,
    tenant: Field,
}

fn tv_schema() -> (Schema, Field, Field, Field) {
    let mut sb = Schema::builder();
    let ti = TextFieldIndexing::default()
        .set_tokenizer("default")
        .set_index_option(IndexRecordOption::WithFreqs);
    let body = sb.add_text_field("body", TextOptions::default().set_indexing_options(ti));
    let id = sb.add_u64_field("id", FAST);
    let tenant = sb.add_text_field("tenant", STRING);
    (sb.build(), body, id, tenant)
}

/// Build over doc indices `ids` (u64 ids preserved). Returns (index, build_s, merge_s).
fn tv_build(
    c: &Corpus,
    ids: &[usize],
    threads: usize,
    dir: Option<&std::path::Path>,
    with_tenant: bool,
    force_merge: bool,
) -> (Tv, f64, f64) {
    let (schema, body, id, tenant) = tv_schema();
    let index = match dir {
        Some(d) => {
            std::fs::create_dir_all(d).unwrap();
            Index::create_in_dir(d, schema).unwrap()
        }
        None => Index::create_in_ram(schema),
    };
    let t = Instant::now();
    let mut w: IndexWriter = index.writer_with_num_threads(threads, 512 * 1024 * 1024).unwrap();
    for &i in ids {
        let mut d = doc!(body => doc_text(&c.docs[i]), id => i as u64);
        if with_tenant {
            d.add_text(tenant, format!("t{}", i % TENANTS));
        }
        w.add_document(d).unwrap();
    }
    w.commit().unwrap();
    let build_s = t.elapsed().as_secs_f64();
    let mut merge_s = 0.0;
    if force_merge {
        let t = Instant::now();
        let segs = index.searchable_segment_ids().unwrap();
        if segs.len() > 1 {
            let _ = w.merge(&segs).wait();
        }
        w.wait_merging_threads().unwrap();
        merge_s = t.elapsed().as_secs_f64();
    } else {
        w.wait_merging_threads().unwrap();
    }
    (Tv { index, body, id, tenant }, build_s, merge_s)
}

fn tv_query(tv: &Tv, q: &Q, filter_tenant: bool) -> Box<dyn Query> {
    let shoulds: Vec<(Occur, Box<dyn Query>)> = q
        .terms
        .iter()
        .map(|t| {
            let term = Term::from_field_text(tv.body, &format!("w{t}"));
            (Occur::Should, Box::new(TermQuery::new(term, IndexRecordOption::WithFreqs)) as Box<dyn Query>)
        })
        .collect();
    let text = Box::new(BooleanQuery::new(shoulds)) as Box<dyn Query>;
    if !filter_tenant {
        return text;
    }
    let tq = TermQuery::new(
        Term::from_field_text(tv.tenant, &format!("t{}", q.tenant)),
        IndexRecordOption::Basic,
    );
    let tq = Box::new(ConstScoreQuery::new(Box::new(tq), 0.0)) as Box<dyn Query>;
    Box::new(BooleanQuery::new(vec![(Occur::Must, tq), (Occur::Must, text)]))
}

fn tv_search(tv: &Tv, searcher: &tantivy::Searcher, q: &Q, filter_tenant: bool) -> Vec<u64> {
    let query = tv_query(tv, q, filter_tenant);
    let top = searcher
        .search(&query, &TopDocs::with_limit(10).order_by_score())
        .unwrap();
    let mut out = Vec::with_capacity(10);
    for (_s, addr) in top {
        let col = searcher
            .segment_reader(addr.segment_ord)
            .fast_fields()
            .u64("id")
            .unwrap();
        out.push(col.first(addr.doc_id).unwrap());
    }
    out
}

fn tv_dir_size(p: &std::path::Path) -> u64 {
    std::fs::read_dir(p)
        .map(|rd| rd.filter_map(|e| e.ok()).filter_map(|e| e.metadata().ok()).map(|m| m.len()).sum())
        .unwrap_or(0)
}

/// Time `qs` (single thread, 1 warm-up pass of the first 100 queries). Returns latencies and results.
fn run_tv(tv: &Tv, qs: &[Q], filter_tenant: bool) -> (Lat, Vec<Vec<u64>>) {
    let reader = tv.index.reader().unwrap();
    let searcher = reader.searcher();
    for q in qs.iter().take(100) {
        let _ = tv_search(tv, &searcher, q, filter_tenant);
    }
    let mut lat = Lat::new();
    let mut res = Vec::with_capacity(qs.len());
    for q in qs {
        let t = Instant::now();
        let r = tv_search(tv, &searcher, q, filter_tenant);
        lat.rec(t.elapsed());
        res.push(r);
    }
    (lat, res)
}

// ---------------------------------------------------------------------------
// in-repo
// ---------------------------------------------------------------------------

fn build_repo(c: &Corpus, tenant_split: bool) -> (InMemoryStore, f64) {
    let mut st = InMemoryStore::new();
    let t = Instant::now();
    for (i, d) in c.docs.iter().enumerate() {
        let mut cl = crate::repoann::claim(i, doc_text(d));
        cl.tenant_id = if tenant_split { format!("t{}", i % TENANTS) } else { "t0".into() };
        st.ingest_bundle(cl, vec![], vec![]).unwrap();
    }
    (st, t.elapsed().as_secs_f64())
}

fn repo_search(st: &InMemoryStore, tenant: &str, q: &Q) -> Vec<u64> {
    let req = RetrievalRequest {
        tenant_id: tenant.to_string(),
        query: qtext(q),
        top_k: 10,
        stance_mode: StanceMode::Balanced,
    };
    st.retrieve(&req).into_iter().map(|r| r.claim_id[1..].parse().unwrap()).collect()
}

fn overlap(a: &[Vec<u64>], b: &[Vec<u64>], n: usize) -> f64 {
    let mut s = 0.0;
    let mut cnt = 0;
    for i in 0..n.min(a.len()).min(b.len()) {
        let denom = a[i].len().max(1) as f64;
        s += a[i].iter().filter(|x| b[i].contains(x)).count() as f64 / denom;
        cnt += 1;
    }
    s / cnt.max(1) as f64
}

pub fn run(a: &Args) {
    let ndocs = a.u("docs", 200_000);
    let qn = a.u("qn", 1000);
    let qn_repo = a.u("qn_repo", 200);
    let (corpus, gen_s) = time(|| gen_corpus(ndocs));
    let total_tokens: usize = corpus.docs.iter().map(|d| d.len()).sum();
    let base = json!({"exp":"C","docs":ndocs,"vocab":VOCAB,"zipf_s":1.0,"tenants":TENANTS,"total_tokens":total_tokens,"gen_s":gen_s});
    let all_ids: Vec<usize> = (0..ndocs).collect();
    let scratch = scratch_dir().join("tantivy");
    let _ = std::fs::remove_dir_all(&scratch);

    // ---- S1: single tenant, whole corpus
    let rss0 = rss_mb();
    let (tv_ram1, b1, _) = tv_build(&corpus, &all_ids, 1, None, false, false);
    drop(tv_ram1);
    let (tv_ram, b4, m4) = tv_build(&corpus, &all_ids, 4, None, false, true);
    let ram_bytes = tv_ram.index.reader().unwrap().searcher().space_usage().unwrap().total().get_bytes();
    let (tv_mm, bm, mm) = tv_build(&corpus, &all_ids, 4, Some(&scratch.join("s1")), false, true);
    let mm_bytes = tv_dir_size(&scratch.join("s1"));
    let nseg = tv_mm.index.searchable_segment_ids().unwrap().len();
    let mut o = base.clone();
    o["phase"] = json!("tantivy_build");
    o["ram_build_1thread_s"] = json!(b1);
    o["ram_build_4threads_s"] = json!(b4);
    o["ram_force_merge_to_1_segment_s"] = json!(m4);
    o["mmap_build_4threads_s"] = json!(bm);
    o["mmap_force_merge_s"] = json!(mm);
    o["segments_after_merge"] = json!(nseg);
    o["ram_index_bytes_mb"] = json!((ram_bytes as f64 / 1048576.0).round());
    o["mmap_dir_mb"] = json!((mm_bytes as f64 / 1048576.0).round());
    o["rss_delta_mb_after_builds"] = json!((rss_mb() - rss0).round());
    emit(o);

    // ---- S1: in-repo
    let (repo, repo_build_s) = build_repo(&corpus, false);
    let mut o = base.clone();
    o["phase"] = json!("repo_build");
    o["tenant_split"] = json!(false);
    o["build_s"] = json!(repo_build_s);
    o["peak_rss_mb"] = json!(peak_rss_mb().round());
    emit(o);

    for mix in ["natural", "midtail"] {
        for len in [1usize, 3, 6] {
            let qs = gen_queries(&corpus, len, mix, qn);
            let (lat_r, res_r) = run_tv(&tv_ram, &qs, false);
            let (lat_m, res_m) = run_tv(&tv_mm, &qs, false);
            // in-repo (single tenant "t0")
            let qr = &qs[..qn_repo.min(qs.len())];
            let mut lat_i = Lat::new();
            let mut res_i = Vec::new();
            for q in qr {
                let t = Instant::now();
                let r = repo_search(&repo, "t0", q);
                lat_i.rec(t.elapsed());
                res_i.push(r);
            }
            // candidate counts on 20 queries
            let mut cands = 0usize;
            for q in qr.iter().take(20) {
                let req = RetrievalRequest { tenant_id: "t0".into(), query: qtext(q), top_k: 10, stance_mode: StanceMode::Balanced };
                cands += repo.candidate_count_for_retrieval_request(&req);
            }
            let mut o = base.clone();
            o["phase"] = json!("query_s1_single_tenant");
            o["mix"] = json!(mix);
            o["terms"] = json!(len);
            o["tantivy_ram"] = lat_r.json();
            o["tantivy_mmap"] = lat_m.json();
            o["repo"] = lat_i.json();
            o["repo_queries"] = json!(qr.len());
            o["repo_avg_candidates"] = json!(cands / 20.min(qr.len()).max(1));
            o["top10_overlap_tantivy_vs_repo"] = json!(overlap(&res_r, &res_i, qn_repo));
            o["top10_overlap_ram_vs_mmap"] = json!(overlap(&res_r, &res_m, qn));
            emit(o);
        }
    }
    drop(repo);
    drop(tv_ram);
    drop(tv_mm);

    // ---- S2: 20 tenants x ndocs/20
    let (tv_f, _, _) = tv_build(&corpus, &all_ids, 4, None, true, true);
    let t = Instant::now();
    let per_tenant: Vec<Tv> = (0..TENANTS)
        .map(|t| {
            let ids: Vec<usize> = (0..ndocs).filter(|i| i % TENANTS == t).collect();
            tv_build(&corpus, &ids, 1, None, false, true).0
        })
        .collect();
    let pt_build = t.elapsed().as_secs_f64();
    let (repo2, repo2_build_s) = build_repo(&corpus, true);
    let mut o = base.clone();
    o["phase"] = json!("s2_build");
    o["per_tenant_tantivy_build_s_total_1thread_each"] = json!(pt_build);
    o["repo_build_s"] = json!(repo2_build_s);
    emit(o);
    let readers: Vec<_> = per_tenant.iter().map(|t| t.index.reader().unwrap()).collect();
    let searchers: Vec<_> = readers.iter().map(|r| r.searcher()).collect();
    let f_reader = tv_f.index.reader().unwrap();
    let f_searcher = f_reader.searcher();
    for mix in ["natural", "midtail"] {
        for len in [1usize, 3, 6] {
            let qs = gen_queries(&corpus, len, mix, qn);
            // filter term in one index
            for q in qs.iter().take(100) {
                let _ = tv_search(&tv_f, &f_searcher, q, true);
            }
            let mut lat_f = Lat::new();
            let mut res_f = Vec::new();
            for q in &qs {
                let t = Instant::now();
                let r = tv_search(&tv_f, &f_searcher, q, true);
                lat_f.rec(t.elapsed());
                res_f.push(r);
            }
            // per-tenant index
            let mut lat_p = Lat::new();
            let mut res_p = Vec::new();
            for q in &qs {
                let t = Instant::now();
                let r = tv_search(&per_tenant[q.tenant], &searchers[q.tenant], q, false);
                lat_p.rec(t.elapsed());
                res_p.push(r);
            }
            // in-repo (tenant-partitioned natively)
            let qr = &qs[..qn_repo.min(qs.len())];
            let mut lat_i = Lat::new();
            let mut res_i = Vec::new();
            for q in qr {
                let t = Instant::now();
                let r = repo_search(&repo2, &format!("t{}", q.tenant), q);
                lat_i.rec(t.elapsed());
                res_i.push(r);
            }
            let mut o = base.clone();
            o["phase"] = json!("query_s2_tenant_filter");
            o["mix"] = json!(mix);
            o["terms"] = json!(len);
            o["tantivy_filter_term_one_index"] = lat_f.json();
            o["tantivy_index_per_tenant"] = lat_p.json();
            o["repo_per_tenant"] = lat_i.json();
            o["top10_overlap_filterterm_vs_pertenant"] = json!(overlap(&res_f, &res_p, qn));
            o["top10_overlap_pertenant_vs_repo"] = json!(overlap(&res_p, &res_i, qn_repo));
            emit(o);
        }
    }
    let _ = std::fs::remove_dir_all(&scratch);
}
