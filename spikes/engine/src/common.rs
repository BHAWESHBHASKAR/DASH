//! Shared helpers: deterministic synthetic data, exact search, RSS, latency histograms, CLI args.

use hdrhistogram::Histogram;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use rand_distr::StandardNormal;
use rayon::prelude::*;
use std::collections::HashMap;
use std::time::Instant;

pub const DIM: usize = 384;
pub const SEED: u64 = 42;
pub const N_CLUSTERS: usize = 256;
/// Number of mixture components actually used (override with `clusters=` for the unclustered control).
pub static CLUSTERS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(N_CLUSTERS);
pub fn nclusters() -> usize {
    CLUSTERS.load(std::sync::atomic::Ordering::Relaxed)
}
pub const SUBSPACE: usize = 32;
pub const N_QUERIES: usize = 1000;
pub const K: usize = 10;
const CHUNK: usize = 1024;
/// Chunk id reserved for the held-out query set (never overlaps a base chunk).
const QUERY_CHUNK: usize = 1_000_000;

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

pub struct Args(pub HashMap<String, String>);

impl Args {
    pub fn parse(rest: &[String]) -> Self {
        let mut m = HashMap::new();
        for a in rest {
            if let Some((k, v)) = a.split_once('=') {
                m.insert(k.trim_start_matches('-').to_string(), v.to_string());
            }
        }
        Args(m)
    }
    pub fn s(&self, k: &str, d: &str) -> String {
        self.0.get(k).cloned().unwrap_or_else(|| d.to_string())
    }
    pub fn u(&self, k: &str, d: usize) -> usize {
        self.0.get(k).map(|v| v.parse().expect(k)).unwrap_or(d)
    }
    pub fn f(&self, k: &str, d: f64) -> f64 {
        self.0.get(k).map(|v| v.parse().expect(k)).unwrap_or(d)
    }
}

pub fn emit(v: serde_json::Value) {
    println!("{}", v);
}

pub fn scratch_dir() -> std::path::PathBuf {
    let p = std::path::PathBuf::from(
        std::env::var("SPIKE_SCRATCH").unwrap_or_else(|_| "/home/user/targets/spike/scratch".into()),
    );
    std::fs::create_dir_all(&p).unwrap();
    p
}

// ---------------------------------------------------------------------------
// Process memory and environment
// ---------------------------------------------------------------------------

fn status_kb(field: &str) -> u64 {
    let s = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    for l in s.lines() {
        if let Some(rest) = l.strip_prefix(field) {
            return rest
                .trim_start_matches(':')
                .split_whitespace()
                .next()
                .and_then(|x| x.parse().ok())
                .unwrap_or(0);
        }
    }
    0
}
pub fn rss_mb() -> f64 {
    status_kb("VmRSS") as f64 / 1024.0
}
pub fn peak_rss_mb() -> f64 {
    status_kb("VmHWM") as f64 / 1024.0
}

// ---------------------------------------------------------------------------
// Latency histogram (microseconds)
// ---------------------------------------------------------------------------

pub struct Lat(pub Histogram<u64>);

impl Lat {
    pub fn new() -> Self {
        Lat(Histogram::new_with_bounds(1, 3_600_000_000, 3).unwrap())
    }
    pub fn rec(&mut self, d: std::time::Duration) {
        let us = (d.as_nanos() / 1000).max(1) as u64;
        self.0.record(us).unwrap();
    }
    pub fn merge(&mut self, o: &Lat) {
        self.0.add(&o.0).unwrap();
    }
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "n": self.0.len(),
            "p50_us": self.0.value_at_quantile(0.50),
            "p95_us": self.0.value_at_quantile(0.95),
            "p99_us": self.0.value_at_quantile(0.99),
            "max_us": self.0.max(),
            "mean_us": self.0.mean().round(),
        })
    }
}

pub fn time<T>(f: impl FnOnce() -> T) -> (T, f64) {
    let t = Instant::now();
    let r = f();
    (r, t.elapsed().as_secs_f64())
}

// ---------------------------------------------------------------------------
// Synthetic data: clustered Gaussian mixture in a low-dimensional subspace
// ---------------------------------------------------------------------------
//
// x = normalize( c_k + s1 * B z + s2 * g ), k uniform over N_CLUSTERS,
// z ~ N(0, I_32), g ~ N(0, I_384), c_k a random unit vector, B a random
// 384x32 matrix with ~unit columns. ||s1 B z|| ~ 0.6, ||s2 g|| ~ 0.15.
// Point i of chunk c is a pure function of (SEED, c, i): output does not
// depend on thread count or on N.

pub struct Gen {
    centers: Vec<f32>,
    basis: Vec<f32>, // SUBSPACE rows of DIM
}

impl Gen {
    pub fn new() -> Self {
        let mut rng = StdRng::seed_from_u64(SEED);
        let mut centers = vec![0f32; N_CLUSTERS * DIM];
        for c in centers.chunks_exact_mut(DIM) {
            for v in c.iter_mut() {
                *v = rng.sample::<f32, _>(StandardNormal);
            }
            normalize(c);
        }
        let mut basis = vec![0f32; SUBSPACE * DIM];
        for v in basis.iter_mut() {
            *v = rng.sample::<f32, _>(StandardNormal) / (DIM as f32).sqrt();
        }
        Gen { centers, basis }
    }

    fn chunk(&self, chunk_id: usize, out: &mut [f32], cl: &mut [u16]) {
        let mut rng = StdRng::seed_from_u64(SEED ^ 0x9E37_79B9_7F4A_7C15u64.wrapping_mul(chunk_id as u64 + 1));
        let s1 = 0.6 / (SUBSPACE as f32).sqrt();
        let s2 = 0.15 / (DIM as f32).sqrt();
        for (ri, row) in out.chunks_exact_mut(DIM).enumerate() {
            let k = rng.gen_range(0..nclusters());
            cl[ri] = k as u16;
            row.copy_from_slice(&self.centers[k * DIM..(k + 1) * DIM]);
            for j in 0..SUBSPACE {
                let z: f32 = rng.sample::<f32, _>(StandardNormal) * s1;
                let b = &self.basis[j * DIM..(j + 1) * DIM];
                for d in 0..DIM {
                    row[d] += z * b[d];
                }
            }
            for v in row.iter_mut() {
                *v += rng.sample::<f32, _>(StandardNormal) * s2;
            }
            normalize(row);
        }
    }

    /// First `n` base points (flat row-major) and their cluster ids.
    pub fn base(&self, n: usize) -> (Vec<f32>, Vec<u16>) {
        let chunks = n.div_ceil(CHUNK);
        let mut data = vec![0f32; chunks * CHUNK * DIM];
        let mut cl = vec![0u16; chunks * CHUNK];
        data.par_chunks_mut(CHUNK * DIM)
            .zip(cl.par_chunks_mut(CHUNK))
            .enumerate()
            .for_each(|(c, (out, c_out))| self.chunk(c, out, c_out));
        data.truncate(n * DIM);
        cl.truncate(n);
        (data, cl)
    }

    /// Held-out query set (same distribution, disjoint RNG stream).
    pub fn queries(&self) -> Vec<f32> {
        let mut out = vec![0f32; CHUNK * DIM];
        let mut cl = vec![0u16; CHUNK];
        self.chunk(QUERY_CHUNK, &mut out, &mut cl);
        out.truncate(N_QUERIES * DIM);
        out
    }
}

pub fn normalize(v: &mut [f32]) {
    let n = dot(v, v).sqrt().max(1e-12);
    for x in v.iter_mut() {
        *x /= n;
    }
}

#[inline]
pub fn dot(a: &[f32], b: &[f32]) -> f32 {
    let mut acc = [0f32; 16];
    for (ca, cb) in a.chunks_exact(16).zip(b.chunks_exact(16)) {
        for i in 0..16 {
            acc[i] += ca[i] * cb[i];
        }
    }
    acc.iter().sum()
}

// ---------------------------------------------------------------------------
// Exact search
// ---------------------------------------------------------------------------

/// Tiny sorted top-k (descending by score).
pub struct TopK {
    k: usize,
    pub v: Vec<(f32, u32)>,
}
impl TopK {
    pub fn new(k: usize) -> Self {
        TopK { k, v: Vec::with_capacity(k + 1) }
    }
    #[inline]
    pub fn thresh(&self) -> f32 {
        if self.v.len() < self.k { f32::NEG_INFINITY } else { self.v[self.k - 1].0 }
    }
    #[inline]
    pub fn push(&mut self, s: f32, id: u32) {
        if s <= self.thresh() {
            return;
        }
        let pos = self.v.partition_point(|x| x.0 > s);
        self.v.insert(pos, (s, id));
        self.v.truncate(self.k);
    }
    pub fn ids(&self) -> Vec<u32> {
        self.v.iter().map(|x| x.1).collect()
    }
}

/// Single-query exact scan over the first `n` rows (one thread). Returns ids.
pub fn exact_scan(data: &[f32], n: usize, q: &[f32], k: usize) -> Vec<u32> {
    let mut t = TopK::new(k);
    for i in 0..n {
        t.push(dot(q, &data[i * DIM..(i + 1) * DIM]), i as u32);
    }
    t.ids()
}

/// Exact scan restricted to an id list.
pub fn exact_scan_ids(data: &[f32], ids: &[u32], q: &[f32], k: usize) -> Vec<u32> {
    let mut t = TopK::new(k);
    for &i in ids {
        let i = i as usize;
        t.push(dot(q, &data[i * DIM..(i + 1) * DIM]), i as u32);
    }
    t.ids()
}

/// Ground truth for all queries, tiled 8 queries x all rows, parallel over tiles.
pub fn exact_topk_all(data: &[f32], n: usize, queries: &[f32], k: usize) -> Vec<Vec<u32>> {
    const TILE: usize = 8;
    let nq = queries.len() / DIM;
    let tiles: Vec<usize> = (0..nq.div_ceil(TILE)).collect();
    let res: Vec<Vec<Vec<u32>>> = tiles
        .par_iter()
        .map(|&t| {
            let q0 = t * TILE;
            let q1 = (q0 + TILE).min(nq);
            let mut tops: Vec<TopK> = (q0..q1).map(|_| TopK::new(k)).collect();
            for i in 0..n {
                let row = &data[i * DIM..(i + 1) * DIM];
                for (j, top) in tops.iter_mut().enumerate() {
                    let q = &queries[(q0 + j) * DIM..(q0 + j + 1) * DIM];
                    top.push(dot(q, row), i as u32);
                }
            }
            tops.iter().map(|t| t.ids()).collect()
        })
        .collect();
    res.into_iter().flatten().collect()
}

/// Ground truth cached in the scratch dir (derived, small, not committed).
pub fn ground_truth(data: &[f32], n: usize, queries: &[f32]) -> Vec<Vec<u32>> {
    let path = scratch_dir().join(format!("gt_n{}_s{}_c{}.bin", n, SEED, nclusters()));
    if let Ok(b) = std::fs::read(&path) {
        if b.len() == N_QUERIES * K * 4 {
            return b
                .chunks_exact(4 * K)
                .map(|c| c.chunks_exact(4).map(|x| u32::from_le_bytes(x.try_into().unwrap())).collect())
                .collect();
        }
    }
    let gt = exact_topk_all(data, n, queries, K);
    let mut b = Vec::with_capacity(N_QUERIES * K * 4);
    for g in &gt {
        for &x in g {
            b.extend_from_slice(&x.to_le_bytes());
        }
    }
    std::fs::write(&path, b).unwrap();
    gt
}

pub fn recall_at_k(found: &[u64], truth: &[u32], k: usize) -> f64 {
    let tk = &truth[..k.min(truth.len())];
    if tk.is_empty() {
        return 1.0;
    }
    let hits = found.iter().take(k).filter(|f| tk.contains(&(**f as u32))).count();
    hits as f64 / tk.len() as f64
}

/// Cluster ids of the held-out query set.
pub fn query_cluster_ids() -> Vec<u16> {
    let g = Gen::new();
    let mut out = vec![0f32; CHUNK * DIM];
    let mut cl = vec![0u16; CHUNK];
    g.chunk(QUERY_CHUNK, &mut out, &mut cl);
    cl.truncate(N_QUERIES);
    cl
}

pub fn pool(threads: usize) -> rayon::ThreadPool {
    rayon::ThreadPoolBuilder::new().num_threads(threads).build().unwrap()
}
