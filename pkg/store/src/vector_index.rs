//! Per-tenant vector index layer (replaces the former in-repo graph).
//!
//! Three layers, from the bottom up:
//!
//! - [`FlatIndex`]: exact search over contiguous, pre-normalised `f32`
//!   rows. Used while a tenant is small and as the exact scan for small
//!   filtered candidate sets.
//! - [`HnswIndex`]: a `usearch` HNSW (cosine metric, `i8` scalar
//!   quantisation) followed by an exact `f32` rerank of the best
//!   `rerank` candidates against the full-precision vectors that the store
//!   already keeps. Measured in `docs/adr/0003-storage-engine-indexes.md`:
//!   recall@10 >= 0.98 at 500k clustered vectors with roughly a third of
//!   the memory of an `f32` index.
//! - [`TenantVectorIndex`]: starts as a [`FlatIndex`], converts itself to
//!   an [`HnswIndex`] once the tenant holds more than
//!   [`AnnTuningConfig::flat_threshold`] vectors, interns claim ids to
//!   dense `u64` keys and normalises every vector it indexes.
//!
//! # Memory
//!
//! `HnswIndex` stores only the quantised vectors (`dim` bytes each) plus the
//! graph (about `2 * connectivity * 4` bytes per node at the base layer); it
//! does NOT keep an `f32` copy. The rerank reads the raw vectors through a
//! [`VectorSource`], which in the store is `claim_vectors` (the single
//! full-precision copy). `FlatIndex` keeps its own normalised `f32` copy, so
//! a tenant below the threshold costs `4 * dim` extra bytes per vector, at
//! most `4 * dim * flat_threshold` bytes per tenant (12 MB at 384-d and the
//! default threshold) before it converts.
//!
//! # Concurrency
//!
//! Mutation takes `&mut self`; search takes `&self`. `usearch::Index` is
//! `Send + Sync` and its search path is thread safe (each search borrows one
//! of the thread contexts reserved with the index), so any number of readers
//! can search concurrently under the store's `RwLock` read guard.
//!
//! # Clone
//!
//! The store clones itself to stage atomic batches. [`HnswIndex`] is
//! copy-on-write: a clone shares the `usearch` index (an `Arc`) and the
//! first mutation of a shared index copies it through
//! `save_to_buffer`/`load_from_buffer`. Staging a batch that carries no
//! vectors therefore never copies an index, and a batch that does pays one
//! memcpy-class copy (about 0.3 s per 500k `f32` vectors in the spike).

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Arc, Condvar, Mutex};

use thiserror::Error;
use usearch::ffi::{IndexOptions, MetricKind, ScalarKind};

// ---------------------------------------------------------------------------
// Tuning
// ---------------------------------------------------------------------------

/// HNSW connectivity (`M`). Measured default from the ADR 0003 spike.
pub const ANN_CONNECTIVITY_DEFAULT: usize = 16;
/// Construction beam (`ef_construction`).
pub const ANN_EXPANSION_ADD_DEFAULT: usize = 128;
/// Search beam floor (`ef_search`). usearch widens it to the requested count.
pub const ANN_EXPANSION_SEARCH_DEFAULT: usize = 256;
/// Tenants with at most this many vectors are searched exactly.
pub const VECTOR_FLAT_THRESHOLD_DEFAULT: usize = 8_192;
/// Candidates re-scored with exact `f32` cosine after the quantised search.
pub const VECTOR_RERANK_DEFAULT: usize = 50;

/// Smallest build for which `usearch` is fed from several threads.
const PARALLEL_BUILD_MIN: usize = 2_048;
/// Minimum number of concurrent searches an HNSW serves without queuing.
const MIN_SEARCH_SLOTS: usize = 16;
/// Growth step when an HNSW index runs out of reserved capacity.
const MIN_CAPACITY_STEP: usize = 4_096;

/// Vector index tuning. `connectivity`, `expansion_add` and
/// `expansion_search` map one-to-one onto the usearch parameters of the same
/// meaning; `flat_threshold` and `rerank` belong to this layer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnnTuningConfig {
    /// HNSW connectivity `M` (neighbours per node; the base layer holds `2M`).
    pub connectivity: usize,
    /// `ef_construction`: beam width while inserting.
    pub expansion_add: usize,
    /// `ef_search` floor: beam width while searching (never below the
    /// number of requested results).
    pub expansion_search: usize,
    /// A tenant with at most this many vectors is searched exactly (flat);
    /// above it an HNSW is built.
    pub flat_threshold: usize,
    /// Number of HNSW candidates re-scored exactly in `f32`; `0` returns the
    /// quantised scores unchanged.
    pub rerank: usize,
}

impl Default for AnnTuningConfig {
    fn default() -> Self {
        Self {
            connectivity: ANN_CONNECTIVITY_DEFAULT,
            expansion_add: ANN_EXPANSION_ADD_DEFAULT,
            expansion_search: ANN_EXPANSION_SEARCH_DEFAULT,
            flat_threshold: VECTOR_FLAT_THRESHOLD_DEFAULT,
            rerank: VECTOR_RERANK_DEFAULT,
        }
    }
}

// ---------------------------------------------------------------------------
// Errors and traits
// ---------------------------------------------------------------------------

#[derive(Debug, Error, PartialEq, Eq)]
pub enum VectorIndexError {
    #[error("vector dimension mismatch: expected {expected}, got {got}")]
    DimensionMismatch { expected: usize, got: usize },
    /// Empty, non-finite or zero-norm: such a vector has no direction and
    /// cannot be indexed under the cosine metric.
    #[error("vector cannot be indexed: {0}")]
    InvalidVector(&'static str),
    #[error("vector index backend error: {0}")]
    Backend(String),
    /// A persisted index failed a structural check while loading.
    #[error("persisted vector index is corrupt: {0}")]
    Corrupt(String),
}

/// Full-precision vectors for reranking, addressed by index key.
pub trait VectorSource {
    fn raw(&self, key: u64) -> Option<&[f32]>;
}

/// A cosine-similarity index over `u64` keys. Scores are cosine in
/// `[-1, 1]`, higher is better; ties break towards the smaller key.
pub trait VectorIndex: Send + Sync {
    fn dimensions(&self) -> usize;
    /// Insert, or replace the vector of an existing key.
    fn insert(&mut self, key: u64, vector: &[f32]) -> Result<(), VectorIndexError>;
    /// Remove a key. Returns whether it was present.
    fn remove(&mut self, key: u64) -> bool;
    /// Top `k` keys by cosine, best first. `allowed` restricts the result to
    /// keys it accepts. With a `source`, an approximate index re-scores its
    /// best candidates exactly; exact indexes ignore it.
    fn search(
        &self,
        query: &[f32],
        k: usize,
        allowed: Option<&dyn Fn(u64) -> bool>,
        source: Option<&dyn VectorSource>,
    ) -> Vec<(u64, f32)>;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// Estimate of the heap held by the index itself (excluding any
    /// [`VectorSource`] it reads from).
    fn heap_bytes(&self) -> usize;
}

// ---------------------------------------------------------------------------
// Numeric kernels
// ---------------------------------------------------------------------------

/// Dot product with eight independent accumulators so the loop vectorises.
pub fn dot(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    let mut acc = [0.0f32; 8];
    let (ca, ra) = a.as_chunks::<8>();
    let (cb, rb) = b.as_chunks::<8>();
    for (x, y) in ca.iter().zip(cb) {
        for lane in 0..8 {
            acc[lane] += x[lane] * y[lane];
        }
    }
    let mut sum: f32 = acc.iter().sum();
    for (x, y) in ra.iter().zip(rb) {
        sum += x * y;
    }
    sum
}

/// L2-normalised copy of `v`, or why it cannot be normalised.
fn normalized(v: &[f32]) -> Result<Vec<f32>, VectorIndexError> {
    let inv = (1.0 / checked_norm_sq(v)?.sqrt()) as f32;
    Ok(v.iter().map(|x| x * inv).collect())
}

/// Squared L2 norm, or why `v` has no direction.
fn checked_norm_sq(v: &[f32]) -> Result<f64, VectorIndexError> {
    if v.is_empty() {
        return Err(VectorIndexError::InvalidVector("empty vector"));
    }
    let norm_sq: f64 = v.iter().map(|x| f64::from(*x) * f64::from(*x)).sum();
    if !norm_sq.is_finite() {
        return Err(VectorIndexError::InvalidVector("non-finite vector"));
    }
    if norm_sq <= f64::MIN_POSITIVE {
        return Err(VectorIndexError::InvalidVector("zero-norm vector"));
    }
    Ok(norm_sq)
}

/// `true` when `v` can be indexed under the cosine metric (non-empty,
/// finite, non-zero norm). Vectors that fail it are stored but never indexed.
pub fn is_indexable(v: &[f32]) -> bool {
    checked_norm_sq(v).is_ok()
}

/// 64-bit fingerprint of a raw vector's exact bit pattern (length included).
/// Not cryptographic: it detects a persisted index that no longer matches the
/// stored vectors, which an accidental mismatch defeats with odds of about
/// one in 2^64.
pub fn vector_fingerprint(v: &[f32]) -> u64 {
    const K: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut lanes: [u64; 4] = [
        0x243F_6A88_85A3_08D3,
        0x1319_8A2E_0370_7344,
        0xA409_3822_299F_31D0,
        0x082E_FA98_EC4E_6C89,
    ];
    let (chunks, rest) = v.as_chunks::<8>();
    for chunk in chunks {
        for (lane, state) in lanes.iter_mut().enumerate() {
            let word = u64::from(chunk[2 * lane].to_bits())
                | (u64::from(chunk[2 * lane + 1].to_bits()) << 32);
            *state = (*state ^ word).wrapping_mul(K).rotate_left(29);
        }
    }
    let mut h = (v.len() as u64).wrapping_mul(K);
    for state in lanes {
        h = (h ^ state).wrapping_mul(K).rotate_left(31);
    }
    for x in rest {
        h = (h ^ u64::from(x.to_bits())).wrapping_mul(K).rotate_left(29);
    }
    // splitmix64 finaliser.
    h ^= h >> 30;
    h = h.wrapping_mul(0xBF58_476D_1CE4_E5B9);
    h ^= h >> 27;
    h = h.wrapping_mul(0x94D0_49BB_1331_11EB);
    h ^ (h >> 31)
}

/// Cosine of a unit `query` against an arbitrary (not normalised) `v`.
fn cosine_to_unit(query: &[f32], v: &[f32]) -> Option<f32> {
    if query.len() != v.len() {
        return None;
    }
    let norm_sq = dot(v, v);
    if !norm_sq.is_finite() || norm_sq <= f32::MIN_POSITIVE {
        return None;
    }
    Some((dot(query, v) / norm_sq.sqrt()).clamp(-1.0, 1.0))
}

fn best_first<T: Ord>(a: &(f32, T), b: &(f32, T)) -> Ordering {
    b.0.total_cmp(&a.0).then_with(|| a.1.cmp(&b.1))
}

/// Keep the best `k` of `scored`, sorted best first (ties: smaller key).
fn select_top<T: Ord>(mut scored: Vec<(f32, T)>, k: usize) -> Vec<(f32, T)> {
    if k == 0 {
        return Vec::new();
    }
    if scored.len() > k {
        scored.select_nth_unstable_by(k - 1, best_first);
        scored.truncate(k);
    }
    scored.sort_by(best_first);
    scored
}

/// Exact top-`k` by cosine over `items` (raw, not normalised vectors).
/// Items that cannot be scored (dimension mismatch, zero norm) are skipped.
/// This is the exact scan used for small filtered candidate sets.
pub fn exact_top_k<'a>(
    query: &[f32],
    items: impl Iterator<Item = (&'a str, &'a [f32])>,
    k: usize,
) -> Vec<(&'a str, f32)> {
    let Ok(q) = normalized(query) else {
        return Vec::new();
    };
    let scored: Vec<(f32, &str)> = items
        .filter_map(|(id, v)| Some((cosine_to_unit(&q, v)?, id)))
        .collect();
    select_top(scored, k)
        .into_iter()
        .map(|(score, id)| (id, score))
        .collect()
}

// ---------------------------------------------------------------------------
// FlatIndex
// ---------------------------------------------------------------------------

/// Exact index over contiguous normalised rows; removal swaps the last row
/// into the hole, so storage never fragments.
#[derive(Debug, Clone)]
pub struct FlatIndex {
    dim: usize,
    data: Vec<f32>,
    keys: Vec<u64>,
    slot: HashMap<u64, usize>,
}

impl FlatIndex {
    pub fn new(dim: usize) -> Self {
        Self {
            dim,
            data: Vec::new(),
            keys: Vec::new(),
            slot: HashMap::new(),
        }
    }

    fn row(&self, slot: usize) -> &[f32] {
        &self.data[slot * self.dim..(slot + 1) * self.dim]
    }

    /// `(key, normalised vector)` of every row, in storage order.
    pub fn rows(&self) -> impl Iterator<Item = (u64, &[f32])> {
        self.keys
            .iter()
            .enumerate()
            .map(|(slot, key)| (*key, self.row(slot)))
    }
}

impl VectorIndex for FlatIndex {
    fn dimensions(&self) -> usize {
        self.dim
    }

    fn insert(&mut self, key: u64, vector: &[f32]) -> Result<(), VectorIndexError> {
        if vector.len() != self.dim {
            return Err(VectorIndexError::DimensionMismatch {
                expected: self.dim,
                got: vector.len(),
            });
        }
        let unit = normalized(vector)?;
        match self.slot.get(&key) {
            Some(&slot) => {
                self.data[slot * self.dim..(slot + 1) * self.dim].copy_from_slice(&unit);
            }
            None => {
                self.slot.insert(key, self.keys.len());
                self.keys.push(key);
                self.data.extend_from_slice(&unit);
            }
        }
        Ok(())
    }

    fn remove(&mut self, key: u64) -> bool {
        let Some(slot) = self.slot.remove(&key) else {
            return false;
        };
        let last = self.keys.len() - 1;
        if slot != last {
            let (head, tail) = self.data.split_at_mut(last * self.dim);
            head[slot * self.dim..(slot + 1) * self.dim].copy_from_slice(&tail[..self.dim]);
            let moved = self.keys[last];
            self.keys[slot] = moved;
            self.slot.insert(moved, slot);
        }
        self.keys.pop();
        self.data.truncate(last * self.dim);
        true
    }

    fn search(
        &self,
        query: &[f32],
        k: usize,
        allowed: Option<&dyn Fn(u64) -> bool>,
        _source: Option<&dyn VectorSource>,
    ) -> Vec<(u64, f32)> {
        if k == 0 || query.len() != self.dim {
            return Vec::new();
        }
        let Ok(q) = normalized(query) else {
            return Vec::new();
        };
        let scored: Vec<(f32, u64)> = self
            .keys
            .iter()
            .enumerate()
            .filter(|(_, key)| allowed.is_none_or(|f| f(**key)))
            .map(|(slot, key)| (dot(&q, self.row(slot)).clamp(-1.0, 1.0), *key))
            .collect();
        select_top(scored, k)
            .into_iter()
            .map(|(score, key)| (key, score))
            .collect()
    }

    fn len(&self) -> usize {
        self.keys.len()
    }

    fn heap_bytes(&self) -> usize {
        self.data.capacity() * 4 + self.keys.capacity() * 8 + self.slot.capacity() * 24
    }
}

// ---------------------------------------------------------------------------
// HnswIndex (usearch)
// ---------------------------------------------------------------------------

fn backend_err(e: impl std::fmt::Display) -> VectorIndexError {
    VectorIndexError::Backend(e.to_string())
}

fn index_threads() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

/// Counting semaphore over the usearch thread contexts.
struct SlotGate {
    free: Mutex<usize>,
    freed: Condvar,
}

struct SlotPermit<'a>(&'a SlotGate);

impl SlotGate {
    fn new(slots: usize) -> Self {
        Self {
            free: Mutex::new(slots),
            freed: Condvar::new(),
        }
    }

    fn acquire(&self) -> SlotPermit<'_> {
        let mut free = self.free.lock().unwrap_or_else(|e| e.into_inner());
        while *free == 0 {
            free = self.freed.wait(free).unwrap_or_else(|e| e.into_inner());
        }
        *free -= 1;
        SlotPermit(self)
    }
}

impl Drop for SlotPermit<'_> {
    fn drop(&mut self) {
        *self.0.free.lock().unwrap_or_else(|e| e.into_inner()) += 1;
        self.0.freed.notify_one();
    }
}

/// usearch HNSW with `i8` quantisation, cosine metric and exact rerank.
pub struct HnswIndex {
    index: Arc<usearch::Index>,
    /// Bounds concurrent searches to the thread contexts reserved in
    /// `index` (usearch fails a search when none is free).
    gate: Arc<SlotGate>,
    dim: usize,
    len: usize,
    threads: usize,
    config: AnnTuningConfig,
}

impl HnswIndex {
    fn options(dim: usize, config: &AnnTuningConfig) -> IndexOptions {
        IndexOptions {
            dimensions: dim,
            metric: MetricKind::Cos,
            quantization: ScalarKind::I8,
            connectivity: config.connectivity.max(2),
            expansion_add: config.expansion_add.max(1),
            expansion_search: config.expansion_search.max(1),
            multi: false,
        }
    }

    pub fn new(dim: usize, config: &AnnTuningConfig) -> Result<Self, VectorIndexError> {
        if dim == 0 {
            return Err(VectorIndexError::InvalidVector("zero dimensions"));
        }
        let index = usearch::new_index(&Self::options(dim, config)).map_err(backend_err)?;
        let threads = index_threads().max(MIN_SEARCH_SLOTS);
        Ok(Self {
            index: Arc::new(index),
            gate: Arc::new(SlotGate::new(threads)),
            dim,
            len: 0,
            threads,
            config: config.clone(),
        })
    }

    /// Build from already-normalised rows, feeding usearch from several
    /// threads for large inputs. Used for the flat-to-HNSW conversion and
    /// for the bulk rebuild at startup.
    pub fn build(
        dim: usize,
        config: &AnnTuningConfig,
        rows: &[(u64, &[f32])],
    ) -> Result<Self, VectorIndexError> {
        let this = Self::new(dim, config)?;
        this.index
            .reserve_capacity_and_threads(rows.len() + MIN_CAPACITY_STEP, this.threads)
            .map_err(backend_err)?;
        let workers = if rows.len() >= PARALLEL_BUILD_MIN {
            index_threads().min(this.threads)
        } else {
            1
        };
        if workers <= 1 {
            for (key, v) in rows {
                this.index.add(*key, v).map_err(backend_err)?;
            }
        } else {
            let cursor = AtomicUsize::new(0);
            let index = &*this.index;
            let result: Result<(), VectorIndexError> = std::thread::scope(|scope| {
                let handles: Vec<_> = (0..workers)
                    .map(|_| {
                        scope.spawn(|| -> Result<(), VectorIndexError> {
                            loop {
                                let i = cursor.fetch_add(1, AtomicOrdering::Relaxed);
                                let Some((key, v)) = rows.get(i) else {
                                    return Ok(());
                                };
                                index.add(*key, v).map_err(backend_err)?;
                            }
                        })
                    })
                    .collect();
                for handle in handles {
                    handle
                        .join()
                        .map_err(|_| VectorIndexError::Backend("build worker panicked".into()))??;
                }
                Ok(())
            });
            result?;
        }
        Ok(Self {
            len: rows.len(),
            ..this
        })
    }

    /// Make this handle the sole owner of its usearch index (copy-on-write
    /// after a clone), then make room for one more vector.
    fn prepare_write(&mut self) -> Result<(), VectorIndexError> {
        if Arc::strong_count(&self.index) > 1 {
            self.replace_with_copy()?;
        }
        if self.index.size() >= self.index.capacity() {
            let step = (self.index.capacity() / 2).max(MIN_CAPACITY_STEP);
            self.index
                .reserve_capacity_and_threads(self.index.capacity() + step, self.threads)
                .map_err(backend_err)?;
        }
        Ok(())
    }

    fn replace_with_copy(&mut self) -> Result<(), VectorIndexError> {
        self.index = Arc::new(self.deep_copy()?);
        self.gate = Arc::new(SlotGate::new(self.threads));
        Ok(())
    }

    fn deep_copy(&self) -> Result<usearch::Index, VectorIndexError> {
        Self::load_native(self.dim, &self.config, &self.serialize()?, self.threads)
    }

    /// The usearch index in its own serialised format.
    fn serialize(&self) -> Result<Vec<u8>, VectorIndexError> {
        let mut buffer = vec![0u8; self.index.serialized_length()];
        self.index
            .save_to_buffer(&mut buffer)
            .map_err(backend_err)?;
        Ok(buffer)
    }

    /// A writable usearch index loaded (copied) from `buffer`, with this
    /// layer's beam widths and room to grow.
    fn load_native(
        dim: usize,
        config: &AnnTuningConfig,
        buffer: &[u8],
        threads: usize,
    ) -> Result<usearch::Index, VectorIndexError> {
        let index = usearch::new_index(&Self::options(dim, config)).map_err(backend_err)?;
        index.load_from_buffer(buffer).map_err(backend_err)?;
        index.change_expansion_add(config.expansion_add.max(1));
        index.change_expansion_search(config.expansion_search.max(1));
        index
            .reserve_capacity_and_threads(index.size() + MIN_CAPACITY_STEP, threads)
            .map_err(backend_err)?;
        Ok(index)
    }

    /// Rebuild a handle from [`Self::serialize`] output holding `len` live
    /// vectors. The dimension stored in the buffer must equal `dim`.
    fn from_serialized(
        dim: usize,
        config: &AnnTuningConfig,
        buffer: &[u8],
        len: usize,
    ) -> Result<Self, VectorIndexError> {
        if dim == 0 {
            return Err(VectorIndexError::InvalidVector("zero dimensions"));
        }
        let threads = index_threads().max(MIN_SEARCH_SLOTS);
        let index = Self::load_native(dim, config, buffer, threads)?;
        if index.dimensions() != dim {
            return Err(corrupt("HNSW dimension differs from the header"));
        }
        if index.size() != len {
            return Err(corrupt("HNSW size differs from the header"));
        }
        Ok(Self {
            index: Arc::new(index),
            gate: Arc::new(SlotGate::new(threads)),
            dim,
            len,
            threads,
            config: config.clone(),
        })
    }
}

impl Clone for HnswIndex {
    /// Cheap: shares the usearch index until either side writes.
    fn clone(&self) -> Self {
        Self {
            index: Arc::clone(&self.index),
            gate: Arc::clone(&self.gate),
            dim: self.dim,
            len: self.len,
            threads: self.threads,
            config: self.config.clone(),
        }
    }
}

impl VectorIndex for HnswIndex {
    fn dimensions(&self) -> usize {
        self.dim
    }

    fn insert(&mut self, key: u64, vector: &[f32]) -> Result<(), VectorIndexError> {
        if vector.len() != self.dim {
            return Err(VectorIndexError::DimensionMismatch {
                expected: self.dim,
                got: vector.len(),
            });
        }
        let unit = normalized(vector)?;
        self.prepare_write()?;
        // usearch has no in-place update: replace is remove (soft delete, slot
        // is reused by a later insert) followed by add.
        if self.index.contains(key) {
            self.index.remove(key).map_err(backend_err)?;
            self.len -= 1;
        }
        self.index.add(key, &unit).map_err(backend_err)?;
        self.len += 1;
        Ok(())
    }

    fn remove(&mut self, key: u64) -> bool {
        if !self.index.contains(key) {
            return false;
        }
        if Arc::strong_count(&self.index) > 1 && self.replace_with_copy().is_err() {
            return false;
        }
        match self.index.remove(key) {
            Ok(removed) if removed > 0 => {
                self.len -= 1;
                true
            }
            _ => false,
        }
    }

    fn search(
        &self,
        query: &[f32],
        k: usize,
        allowed: Option<&dyn Fn(u64) -> bool>,
        source: Option<&dyn VectorSource>,
    ) -> Vec<(u64, f32)> {
        if k == 0 || self.len == 0 || query.len() != self.dim {
            return Vec::new();
        }
        let Ok(q) = normalized(query) else {
            return Vec::new();
        };
        let _slot = self.gate.acquire();
        let rerank = source.is_some() && self.config.rerank > 0;
        let fetch = if rerank { k.max(self.config.rerank) } else { k };
        let matches = match allowed {
            None => self.index.search(&q, fetch),
            Some(f) => self.index.filtered_search(&q, fetch, f),
        };
        let Ok(matches) = matches else {
            return Vec::new();
        };
        let mut scored: Vec<(f32, u64)> = matches
            .keys
            .iter()
            .zip(&matches.distances)
            .map(|(key, distance)| (1.0 - *distance, *key))
            .collect();
        if rerank && let Some(source) = source {
            for entry in &mut scored {
                if let Some(exact) = source.raw(entry.1).and_then(|raw| cosine_to_unit(&q, raw)) {
                    entry.0 = exact;
                }
            }
        }
        select_top(scored, k)
            .into_iter()
            .map(|(score, key)| (key, score))
            .collect()
    }

    fn len(&self) -> usize {
        self.len
    }

    fn heap_bytes(&self) -> usize {
        self.index.memory_usage()
    }
}

// ---------------------------------------------------------------------------
// Key interning
// ---------------------------------------------------------------------------

/// Dense `u64` keys for claim ids. Released keys are reused, so the key
/// space (and the HNSW slot table) stays proportional to the live count.
///
/// Every live key also carries the [`vector_fingerprint`] of the raw vector
/// it was indexed with. A persisted index stores them so a restart can prove
/// the loaded index matches the vectors replayed from the WAL.
#[derive(Debug, Clone, Default)]
struct KeyInterner {
    by_id: HashMap<Arc<str>, u64>,
    by_key: Vec<Option<Arc<str>>>,
    fingerprints: Vec<u64>,
    free: Vec<u64>,
}

impl KeyInterner {
    fn key_of(&self, id: &str) -> Option<u64> {
        self.by_id.get(id).copied()
    }

    fn id_of(&self, key: u64) -> Option<&str> {
        self.by_key.get(key as usize)?.as_deref()
    }

    fn alloc(&mut self, id: &str, fingerprint: u64) -> u64 {
        let id: Arc<str> = Arc::from(id);
        let key = match self.free.pop() {
            Some(key) => {
                self.by_key[key as usize] = Some(Arc::clone(&id));
                self.fingerprints[key as usize] = fingerprint;
                key
            }
            None => {
                self.by_key.push(Some(Arc::clone(&id)));
                self.fingerprints.push(fingerprint);
                (self.by_key.len() - 1) as u64
            }
        };
        self.by_id.insert(id, key);
        key
    }

    fn set_fingerprint(&mut self, key: u64, fingerprint: u64) {
        if let Some(slot) = self.fingerprints.get_mut(key as usize) {
            *slot = fingerprint;
        }
    }

    fn release(&mut self, id: &str) {
        if let Some(key) = self.by_id.remove(id) {
            self.by_key[key as usize] = None;
            self.fingerprints[key as usize] = 0;
            self.free.push(key);
        }
    }

    /// `(key, claim id, fingerprint)` of every live key.
    fn live(&self) -> impl Iterator<Item = (u64, &str, u64)> {
        self.by_key
            .iter()
            .zip(&self.fingerprints)
            .enumerate()
            .filter_map(|(key, (id, fp))| Some((key as u64, id.as_deref()?, *fp)))
    }

    fn live_count(&self) -> usize {
        self.by_id.len()
    }

    fn heap_bytes(&self) -> usize {
        let text: usize = self.by_key.iter().flatten().map(|id| id.len() + 16).sum();
        text + self.by_id.capacity() * 32
            + self.by_key.capacity() * 16
            + self.fingerprints.capacity() * 8
            + self.free.capacity() * 8
    }

    fn encode(&self, out: &mut Vec<u8>) {
        put_u64(out, self.by_key.len() as u64);
        for (id, fp) in self.by_key.iter().zip(&self.fingerprints) {
            match id {
                Some(id) => {
                    out.push(1);
                    put_u64(out, id.len() as u64);
                    out.extend_from_slice(id.as_bytes());
                    put_u64(out, *fp);
                }
                None => out.push(0),
            }
        }
    }

    fn decode(input: &mut Reader<'_>) -> Result<Self, VectorIndexError> {
        let slots = input.len_prefix(1)?;
        let mut this = KeyInterner {
            by_id: HashMap::with_capacity(slots),
            by_key: Vec::with_capacity(slots),
            fingerprints: Vec::with_capacity(slots),
            free: Vec::new(),
        };
        for key in 0..slots as u64 {
            match input.u8()? {
                0 => {
                    this.by_key.push(None);
                    this.fingerprints.push(0);
                    this.free.push(key);
                }
                1 => {
                    let len = input.len_prefix(1)?;
                    let id = std::str::from_utf8(input.bytes(len)?)
                        .map_err(|_| corrupt("claim id is not UTF-8"))?;
                    let id: Arc<str> = Arc::from(id);
                    let fp = input.u64()?;
                    if this.by_id.insert(Arc::clone(&id), key).is_some() {
                        return Err(corrupt("duplicate claim id in key table"));
                    }
                    this.by_key.push(Some(id));
                    this.fingerprints.push(fp);
                }
                _ => return Err(corrupt("bad key table entry")),
            }
        }
        // Hand out the lowest free key first.
        this.free.reverse();
        Ok(this)
    }
}

// ---------------------------------------------------------------------------
// Serialisation helpers
// ---------------------------------------------------------------------------

fn corrupt(what: &str) -> VectorIndexError {
    VectorIndexError::Corrupt(what.to_string())
}

fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

/// Bounds-checked little-endian reader over a persisted index section.
pub(crate) struct Reader<'a> {
    data: &'a [u8],
}

impl<'a> Reader<'a> {
    pub(crate) fn new(data: &'a [u8]) -> Self {
        Self { data }
    }

    pub(crate) fn bytes(&mut self, len: usize) -> Result<&'a [u8], VectorIndexError> {
        if self.data.len() < len {
            return Err(corrupt("truncated section"));
        }
        let (head, tail) = self.data.split_at(len);
        self.data = tail;
        Ok(head)
    }

    pub(crate) fn u8(&mut self) -> Result<u8, VectorIndexError> {
        Ok(self.bytes(1)?[0])
    }

    pub(crate) fn u64(&mut self) -> Result<u64, VectorIndexError> {
        let raw = self.bytes(8)?;
        let mut word = [0u8; 8];
        word.copy_from_slice(raw);
        Ok(u64::from_le_bytes(word))
    }

    /// A length prefix counting items of at least `min_item_bytes` each;
    /// rejects lengths the remaining input cannot possibly hold.
    pub(crate) fn len_prefix(&mut self, min_item_bytes: usize) -> Result<usize, VectorIndexError> {
        let len = usize::try_from(self.u64()?).map_err(|_| corrupt("length overflow"))?;
        if len.saturating_mul(min_item_bytes.max(1)) > self.data.len() {
            return Err(corrupt("length exceeds section"));
        }
        Ok(len)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

// ---------------------------------------------------------------------------
// TenantVectorIndex
// ---------------------------------------------------------------------------

#[derive(Clone)]
enum Backend {
    Flat(FlatIndex),
    Hnsw(HnswIndex),
}

impl Backend {
    fn as_index(&self) -> &dyn VectorIndex {
        match self {
            Backend::Flat(index) => index,
            Backend::Hnsw(index) => index,
        }
    }

    fn as_index_mut(&mut self) -> &mut dyn VectorIndex {
        match self {
            Backend::Flat(index) => index,
            Backend::Hnsw(index) => index,
        }
    }
}

struct InternedSource<'a> {
    ids: &'a KeyInterner,
    raw: &'a HashMap<String, Vec<f32>>,
}

impl VectorSource for InternedSource<'_> {
    fn raw(&self, key: u64) -> Option<&[f32]> {
        self.raw.get(self.ids.id_of(key)?).map(Vec::as_slice)
    }
}

/// One tenant's vectors, addressed by claim id.
#[derive(Clone)]
pub struct TenantVectorIndex {
    dim: usize,
    config: AnnTuningConfig,
    backend: Backend,
    ids: KeyInterner,
}

impl TenantVectorIndex {
    pub fn new(dim: usize, config: AnnTuningConfig) -> Self {
        Self {
            dim,
            config,
            backend: Backend::Flat(FlatIndex::new(dim)),
            ids: KeyInterner::default(),
        }
    }

    /// Build an index for `items` in one go. Above the flat threshold the
    /// HNSW is built directly (multi-threaded), skipping the flat stage.
    /// Vectors that cannot be indexed (zero norm) are skipped.
    pub fn build<'a>(
        dim: usize,
        config: AnnTuningConfig,
        items: impl Iterator<Item = (&'a str, &'a [f32])>,
    ) -> Result<Self, VectorIndexError> {
        let mut ids = KeyInterner::default();
        let mut units: Vec<(u64, Vec<f32>)> = Vec::new();
        for (id, v) in items {
            if v.len() != dim {
                continue;
            }
            let Ok(unit) = normalized(v) else { continue };
            units.push((ids.alloc(id, vector_fingerprint(v)), unit));
        }
        let backend = if units.len() > config.flat_threshold {
            let rows: Vec<(u64, &[f32])> = units.iter().map(|(k, v)| (*k, v.as_slice())).collect();
            Backend::Hnsw(HnswIndex::build(dim, &config, &rows)?)
        } else {
            let mut flat = FlatIndex::new(dim);
            for (key, unit) in &units {
                flat.insert(*key, unit)?;
            }
            Backend::Flat(flat)
        };
        Ok(Self {
            dim,
            config,
            backend,
            ids,
        })
    }

    pub fn dimensions(&self) -> usize {
        self.dim
    }

    pub fn len(&self) -> usize {
        self.backend.as_index().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn is_hnsw(&self) -> bool {
        matches!(self.backend, Backend::Hnsw(_))
    }

    pub fn contains(&self, claim_id: &str) -> bool {
        self.ids.key_of(claim_id).is_some()
    }

    /// Insert or replace the vector of `claim_id`. On error the claim is
    /// left out of the index (a replaced vector never stays reachable under
    /// its old value).
    pub fn upsert(&mut self, claim_id: &str, vector: &[f32]) -> Result<(), VectorIndexError> {
        if vector.len() != self.dim {
            return Err(VectorIndexError::DimensionMismatch {
                expected: self.dim,
                got: vector.len(),
            });
        }
        let fingerprint = vector_fingerprint(vector);
        let key = match self.ids.key_of(claim_id) {
            Some(key) => {
                self.ids.set_fingerprint(key, fingerprint);
                key
            }
            None => self.ids.alloc(claim_id, fingerprint),
        };
        if let Err(err) = self.backend.as_index_mut().insert(key, vector) {
            self.backend.as_index_mut().remove(key);
            self.ids.release(claim_id);
            return Err(err);
        }
        if let Backend::Flat(flat) = &self.backend
            && flat.len() > self.config.flat_threshold
        {
            let rows: Vec<(u64, &[f32])> = flat.rows().collect();
            let hnsw = HnswIndex::build(self.dim, &self.config, &rows)?;
            self.backend = Backend::Hnsw(hnsw);
        }
        Ok(())
    }

    /// Remove `claim_id`. Returns whether it was indexed.
    pub fn remove(&mut self, claim_id: &str) -> bool {
        let Some(key) = self.ids.key_of(claim_id) else {
            return false;
        };
        self.backend.as_index_mut().remove(key);
        self.ids.release(claim_id);
        true
    }

    /// Top `k` claim ids by cosine to `query`, best first, with scores.
    /// `allowed` filters by claim id; `raw` supplies the full-precision
    /// vectors for the exact rerank of an HNSW's candidates.
    pub fn search(
        &self,
        query: &[f32],
        k: usize,
        allowed: Option<&dyn Fn(&str) -> bool>,
        raw: &HashMap<String, Vec<f32>>,
    ) -> Vec<(String, f32)> {
        let by_key = |key: u64| {
            self.ids
                .id_of(key)
                .is_some_and(|id| allowed.is_none_or(|f| f(id)))
        };
        let source = InternedSource {
            ids: &self.ids,
            raw,
        };
        let filter: Option<&dyn Fn(u64) -> bool> = allowed.map(|_| &by_key as &dyn Fn(u64) -> bool);
        let mut out: Vec<(String, f32)> = self
            .backend
            .as_index()
            .search(query, k, filter, Some(&source))
            .into_iter()
            .filter_map(|(key, score)| Some((self.ids.id_of(key)?.to_string(), score)))
            .collect();
        // Keys depend on insertion order (and are recycled); equal scores
        // (duplicate vectors) must still order the same way after a rebuild.
        out.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        out
    }

    pub fn heap_bytes(&self) -> usize {
        self.backend.as_index().heap_bytes() + self.ids.heap_bytes()
    }

    /// `(claim id, vector fingerprint)` of every indexed claim.
    pub fn fingerprints(&self) -> impl Iterator<Item = (&str, u64)> {
        self.ids.live().map(|(_, id, fp)| (id, fp))
    }

    /// Serialise the whole index (key table with fingerprints, then the flat
    /// rows or the usearch HNSW in its own format). Inverse of
    /// [`Self::decode`].
    pub fn encode(&self) -> Result<Vec<u8>, VectorIndexError> {
        let mut out = Vec::new();
        out.push(match self.backend {
            Backend::Flat(_) => BACKEND_FLAT,
            Backend::Hnsw(_) => BACKEND_HNSW,
        });
        put_u64(&mut out, self.dim as u64);
        self.ids.encode(&mut out);
        match &self.backend {
            Backend::Flat(flat) => {
                put_u64(&mut out, flat.len() as u64);
                out.reserve(flat.len() * (8 + 4 * self.dim));
                for (key, row) in flat.rows() {
                    put_u64(&mut out, key);
                    for x in row {
                        out.extend_from_slice(&x.to_le_bytes());
                    }
                }
            }
            Backend::Hnsw(hnsw) => {
                put_u64(&mut out, hnsw.len as u64);
                let native = hnsw.serialize()?;
                put_u64(&mut out, native.len() as u64);
                out.extend_from_slice(&native);
            }
        }
        Ok(out)
    }

    /// Load an index written by [`Self::encode`]. The caller has already
    /// verified the bytes against a checksum; this checks that the structure
    /// is self-consistent (every indexed key is in the key table and vice
    /// versa) and that it was built for `dim`.
    pub fn decode(
        bytes: &[u8],
        dim: usize,
        config: AnnTuningConfig,
    ) -> Result<Self, VectorIndexError> {
        let mut input = Reader::new(bytes);
        let kind = input.u8()?;
        let stored_dim = usize::try_from(input.u64()?).map_err(|_| corrupt("dimension"))?;
        if stored_dim != dim {
            return Err(VectorIndexError::DimensionMismatch {
                expected: dim,
                got: stored_dim,
            });
        }
        let ids = KeyInterner::decode(&mut input)?;
        let backend = match kind {
            BACKEND_FLAT => {
                let rows = input.len_prefix(8 + 4 * dim)?;
                if rows != ids.live_count() {
                    return Err(corrupt("flat row count differs from the key table"));
                }
                let mut flat = FlatIndex::new(dim);
                flat.data.reserve(rows * dim);
                for _ in 0..rows {
                    let key = input.u64()?;
                    if ids.id_of(key).is_none() || flat.slot.contains_key(&key) {
                        return Err(corrupt("flat row key is not a unique live key"));
                    }
                    flat.slot.insert(key, flat.keys.len());
                    flat.keys.push(key);
                    for raw in input.bytes(4 * dim)?.as_chunks::<4>().0 {
                        let x = f32::from_le_bytes(*raw);
                        if !x.is_finite() {
                            return Err(corrupt("non-finite flat row"));
                        }
                        flat.data.push(x);
                    }
                }
                Backend::Flat(flat)
            }
            BACKEND_HNSW => {
                let len = usize::try_from(input.u64()?).map_err(|_| corrupt("length"))?;
                if len != ids.live_count() {
                    return Err(corrupt("HNSW size differs from the key table"));
                }
                let native_len = input.len_prefix(1)?;
                let native = input.bytes(native_len)?;
                let hnsw = HnswIndex::from_serialized(dim, &config, native, len)?;
                if ids.live().any(|(key, _, _)| !hnsw.index.contains(key)) {
                    return Err(corrupt("HNSW is missing a key of the key table"));
                }
                Backend::Hnsw(hnsw)
            }
            _ => return Err(corrupt("unknown backend kind")),
        };
        if !input.is_empty() {
            return Err(corrupt("trailing bytes after the index"));
        }
        Ok(Self {
            dim,
            config,
            backend,
            ids,
        })
    }
}

const BACKEND_FLAT: u8 = 1;
const BACKEND_HNSW: u8 = 2;

#[cfg(test)]
mod tests {
    use super::*;

    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = self.0;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        }
        fn gauss(&mut self) -> f32 {
            let u1 = ((self.next() >> 11) as f64 + 1.0) / (1u64 << 53) as f64;
            let u2 = ((self.next() >> 11) as f64 + 1.0) / (1u64 << 53) as f64;
            ((-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos()) as f32
        }
    }

    const DIM: usize = 32;

    fn clustered(n: usize, seed: u64) -> Vec<Vec<f32>> {
        let mut rng = Rng(seed);
        let centers: Vec<Vec<f32>> = (0..16)
            .map(|_| (0..DIM).map(|_| rng.gauss()).collect())
            .collect();
        (0..n)
            .map(|_| {
                let c = &centers[(rng.next() % 16) as usize];
                c.iter().map(|x| x + 0.15 * rng.gauss()).collect()
            })
            .collect()
    }

    fn truth(vectors: &[Vec<f32>], q: &[f32], k: usize) -> Vec<usize> {
        let items = vectors
            .iter()
            .enumerate()
            .map(|(i, v)| (i, v.as_slice()))
            .collect::<Vec<_>>();
        let qn = normalized(q).unwrap();
        let scored: Vec<(f32, usize)> = items
            .iter()
            .map(|(i, v)| (cosine_to_unit(&qn, v).unwrap(), *i))
            .collect();
        select_top(scored, k).into_iter().map(|(_, i)| i).collect()
    }

    fn raw_map(vectors: &[Vec<f32>]) -> HashMap<String, Vec<f32>> {
        vectors
            .iter()
            .enumerate()
            .map(|(i, v)| (format!("c{i}"), v.clone()))
            .collect()
    }

    fn recall(
        index: &TenantVectorIndex,
        vectors: &[Vec<f32>],
        queries: &[Vec<f32>],
        raw: &HashMap<String, Vec<f32>>,
    ) -> f64 {
        let mut hit = 0usize;
        for q in queries {
            let want: Vec<String> = truth(vectors, q, 10)
                .iter()
                .map(|i| format!("c{i}"))
                .collect();
            let got = index.search(q, 10, None, raw);
            hit += got.iter().filter(|(id, _)| want.contains(id)).count();
        }
        hit as f64 / (queries.len() * 10) as f64
    }

    #[test]
    fn index_types_are_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<FlatIndex>();
        assert_send_sync::<HnswIndex>();
        assert_send_sync::<TenantVectorIndex>();
    }

    #[test]
    fn flat_matches_exact_and_handles_swap_remove() {
        let vectors = clustered(300, 1);
        let mut flat = FlatIndex::new(DIM);
        for (i, v) in vectors.iter().enumerate() {
            flat.insert(i as u64, v).unwrap();
        }
        for q in clustered(10, 2) {
            let want = truth(&vectors, &q, 10);
            let got: Vec<usize> = flat
                .search(&q, 10, None, None)
                .into_iter()
                .map(|(k, _)| k as usize)
                .collect();
            assert_eq!(got, want);
        }
        // Remove the first 100 rows (each swaps the last row in) and compare
        // against an exact scan over the survivors.
        for i in 0..100u64 {
            assert!(flat.remove(i));
        }
        assert!(!flat.remove(5));
        assert_eq!(flat.len(), 200);
        let live: Vec<Vec<f32>> = vectors[100..].to_vec();
        for q in clustered(10, 3) {
            let want: Vec<usize> = truth(&live, &q, 10).into_iter().map(|i| i + 100).collect();
            let got: Vec<usize> = flat
                .search(&q, 10, None, None)
                .into_iter()
                .map(|(k, _)| k as usize)
                .collect();
            assert_eq!(got, want);
        }
    }

    #[test]
    fn flat_predicate_replace_and_invalid_input() {
        let mut flat = FlatIndex::new(3);
        flat.insert(1, &[1.0, 0.0, 0.0]).unwrap();
        flat.insert(2, &[0.9, 0.1, 0.0]).unwrap();
        flat.insert(3, &[0.0, 1.0, 0.0]).unwrap();
        let only_odd: &dyn Fn(u64) -> bool = &|k| k % 2 == 1;
        let got = flat.search(&[1.0, 0.0, 0.0], 5, Some(only_odd), None);
        assert_eq!(got.iter().map(|(k, _)| *k).collect::<Vec<_>>(), vec![1, 3]);
        flat.insert(3, &[1.0, 0.0, 0.0]).unwrap();
        let got = flat.search(&[1.0, 0.0, 0.0], 1, None, None);
        assert_eq!(got[0].0, 1, "tie on score breaks to the smaller key");
        assert_eq!(flat.len(), 3);
        assert!(matches!(
            flat.insert(9, &[0.0, 0.0, 0.0]),
            Err(VectorIndexError::InvalidVector(_))
        ));
        assert!(matches!(
            flat.insert(9, &[1.0, 0.0]),
            Err(VectorIndexError::DimensionMismatch { .. })
        ));
        assert!(flat.search(&[0.0, 0.0, 0.0], 3, None, None).is_empty());
        assert!(flat.search(&[1.0, 0.0], 3, None, None).is_empty());
    }

    #[test]
    fn exact_top_k_ranks_by_cosine_and_skips_unscorable() {
        let a = [3.0f32, 0.0];
        let b = [1.0f32, 1.0];
        let zero = [0.0f32, 0.0];
        let wrong = [1.0f32];
        let got = exact_top_k(
            &[1.0, 0.1],
            [
                ("a", &a[..]),
                ("b", &b[..]),
                ("z", &zero[..]),
                ("w", &wrong[..]),
            ]
            .into_iter(),
            5,
        );
        assert_eq!(
            got.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            vec!["a", "b"]
        );
        assert!(got[0].1 > got[1].1 && got[0].1 <= 1.0);
    }

    #[test]
    fn hnsw_recall_with_rerank_is_high_on_clustered_data() {
        let vectors = clustered(6_000, 4);
        let queries = clustered(60, 5);
        let raw = raw_map(&vectors);
        let config = AnnTuningConfig {
            flat_threshold: 100,
            ..AnnTuningConfig::default()
        };
        let mut index = TenantVectorIndex::new(DIM, config);
        for (i, v) in vectors.iter().enumerate() {
            index.upsert(&format!("c{i}"), v).unwrap();
        }
        assert!(index.is_hnsw());
        assert_eq!(index.len(), 6_000);
        let r = recall(&index, &vectors, &queries, &raw);
        assert!(r >= 0.95, "recall {r}");
    }

    #[test]
    fn hnsw_scores_are_exact_after_rerank() {
        let vectors = clustered(1_500, 6);
        let raw = raw_map(&vectors);
        let config = AnnTuningConfig {
            flat_threshold: 50,
            ..AnnTuningConfig::default()
        };
        let mut index = TenantVectorIndex::new(DIM, config);
        for (i, v) in vectors.iter().enumerate() {
            index.upsert(&format!("c{i}"), v).unwrap();
        }
        let q = &clustered(1, 7)[0];
        let qn = normalized(q).unwrap();
        for (id, score) in index.search(q, 10, None, &raw) {
            let exact = cosine_to_unit(&qn, &raw[&id]).unwrap();
            assert!((score - exact).abs() < 1e-6, "{id}: {score} vs {exact}");
        }
    }

    #[test]
    fn hnsw_remove_replace_and_key_reuse() {
        let vectors = clustered(2_000, 8);
        let mut raw = raw_map(&vectors);
        let config = AnnTuningConfig {
            flat_threshold: 100,
            ..AnnTuningConfig::default()
        };
        let mut index = TenantVectorIndex::new(DIM, config);
        for (i, v) in vectors.iter().enumerate() {
            index.upsert(&format!("c{i}"), v).unwrap();
        }
        // Remove 20%, then add new ids that reuse the freed keys.
        for i in (0..2_000).step_by(5) {
            assert!(index.remove(&format!("c{i}")));
            raw.remove(&format!("c{i}"));
        }
        assert!(!index.remove("c0"));
        assert_eq!(index.len(), 1_600);
        let extra = clustered(200, 9);
        for (i, v) in extra.iter().enumerate() {
            let id = format!("new{i}");
            index.upsert(&id, v).unwrap();
            raw.insert(id, v.clone());
        }
        assert_eq!(index.len(), 1_800);
        // Replace: c1 now carries a far-away vector.
        let moved = vec![5.0f32; DIM];
        index.upsert("c1", &moved).unwrap();
        raw.insert("c1".into(), moved.clone());
        assert_eq!(index.len(), 1_800);
        assert_eq!(index.search(&moved, 1, None, &raw)[0].0, "c1");
        let near_old = index.search(&vectors[1], 20, None, &raw);
        assert!(near_old.iter().all(|(id, _)| id != "c1"));
        // Removed ids never come back.
        for q in clustered(20, 10) {
            for (id, _) in index.search(&q, 30, None, &raw) {
                let removed = id
                    .strip_prefix('c')
                    .and_then(|n| n.parse::<usize>().ok())
                    .is_some_and(|n| n % 5 == 0);
                assert!(!removed, "{id} was removed");
            }
        }
    }

    #[test]
    fn hnsw_filtered_search_respects_the_predicate() {
        let vectors = clustered(3_000, 11);
        let raw = raw_map(&vectors);
        let config = AnnTuningConfig {
            flat_threshold: 100,
            ..AnnTuningConfig::default()
        };
        let mut index = TenantVectorIndex::new(DIM, config);
        for (i, v) in vectors.iter().enumerate() {
            index.upsert(&format!("c{i}"), v).unwrap();
        }
        let even: &dyn Fn(&str) -> bool = &|id| id[1..].parse::<usize>().is_ok_and(|n| n % 2 == 0);
        for q in clustered(15, 12) {
            let got = index.search(&q, 20, Some(even), &raw);
            assert_eq!(got.len(), 20);
            assert!(got.iter().all(|(id, _)| even(id)));
        }
    }

    #[test]
    fn grows_capacity_in_chunks_beyond_the_first_reservation() {
        let vectors = clustered(9_500, 13);
        let config = AnnTuningConfig {
            flat_threshold: 10,
            expansion_add: 32,
            ..AnnTuningConfig::default()
        };
        let mut index = TenantVectorIndex::new(DIM, config);
        for (i, v) in vectors.iter().enumerate() {
            index.upsert(&format!("c{i}"), v).unwrap();
        }
        assert_eq!(index.len(), 9_500);
        assert!(index.heap_bytes() > 9_500 * DIM);
    }

    #[test]
    fn converts_to_hnsw_exactly_when_the_threshold_is_crossed() {
        let vectors = clustered(130, 14);
        let config = AnnTuningConfig {
            flat_threshold: 128,
            ..AnnTuningConfig::default()
        };
        let mut index = TenantVectorIndex::new(DIM, config);
        for (i, v) in vectors.iter().enumerate() {
            index.upsert(&format!("c{i}"), v).unwrap();
            assert_eq!(index.is_hnsw(), i + 1 > 128, "after {} inserts", i + 1);
        }
        let raw = raw_map(&vectors);
        let q = &vectors[3];
        assert_eq!(index.search(q, 1, None, &raw)[0].0, "c3");
    }

    #[test]
    fn bulk_build_matches_incremental_recall() {
        let vectors = clustered(5_000, 15);
        let queries = clustered(40, 16);
        let raw = raw_map(&vectors);
        let config = AnnTuningConfig {
            flat_threshold: 100,
            ..AnnTuningConfig::default()
        };
        let ids: Vec<String> = (0..vectors.len()).map(|i| format!("c{i}")).collect();
        let index = TenantVectorIndex::build(
            DIM,
            config,
            ids.iter()
                .zip(&vectors)
                .map(|(id, v)| (id.as_str(), v.as_slice())),
        )
        .unwrap();
        assert!(index.is_hnsw());
        assert_eq!(index.len(), 5_000);
        let r = recall(&index, &vectors, &queries, &raw);
        assert!(r >= 0.95, "recall {r}");
    }

    #[test]
    fn clone_is_independent_copy_on_write() {
        let vectors = clustered(1_200, 17);
        let mut raw = raw_map(&vectors);
        let config = AnnTuningConfig {
            flat_threshold: 50,
            ..AnnTuningConfig::default()
        };
        let mut live = TenantVectorIndex::new(DIM, config);
        for (i, v) in vectors.iter().enumerate() {
            live.upsert(&format!("c{i}"), v).unwrap();
        }
        let mut staged = live.clone();
        let probe = vec![7.0f32; DIM];
        staged.upsert("probe", &probe).unwrap();
        raw.insert("probe".into(), probe.clone());
        assert!(staged.remove("c0"));
        assert_eq!(staged.len(), 1_200);
        assert_eq!(live.len(), 1_200);
        assert!(live.contains("c0") && !live.contains("probe"));
        assert_eq!(live.search(&vectors[0], 1, None, &raw)[0].0, "c0");
        assert_eq!(staged.search(&probe, 1, None, &raw)[0].0, "probe");
        assert!(
            staged
                .search(&vectors[0], 5, None, &raw)
                .iter()
                .all(|(id, _)| id != "c0")
        );
        // The live index is still writable after its clone was written.
        live.upsert("later", &vectors[5]).unwrap();
        assert_eq!(live.len(), 1_201);
    }

    #[test]
    fn concurrent_searches_agree_with_serial_results() {
        let vectors = clustered(3_000, 18);
        let raw = raw_map(&vectors);
        let config = AnnTuningConfig {
            flat_threshold: 100,
            ..AnnTuningConfig::default()
        };
        let mut index = TenantVectorIndex::new(DIM, config);
        for (i, v) in vectors.iter().enumerate() {
            index.upsert(&format!("c{i}"), v).unwrap();
        }
        let queries = clustered(32, 19);
        let serial: Vec<Vec<(String, f32)>> = queries
            .iter()
            .map(|q| index.search(q, 10, None, &raw))
            .collect();
        std::thread::scope(|scope| {
            let handles: Vec<_> = (0..40)
                .map(|_| {
                    scope.spawn(|| {
                        queries
                            .iter()
                            .map(|q| index.search(q, 10, None, &raw))
                            .collect::<Vec<_>>()
                    })
                })
                .collect();
            for handle in handles {
                assert_eq!(handle.join().unwrap(), serial);
            }
        });
    }

    #[test]
    fn unindexable_vectors_are_rejected_and_unlink_the_claim() {
        let mut index = TenantVectorIndex::new(3, AnnTuningConfig::default());
        index.upsert("a", &[1.0, 0.0, 0.0]).unwrap();
        assert!(index.upsert("a", &[0.0, 0.0, 0.0]).is_err());
        assert!(!index.contains("a"));
        assert!(index.is_empty());
        assert!(matches!(
            index.upsert("b", &[1.0, 2.0]),
            Err(VectorIndexError::DimensionMismatch {
                expected: 3,
                got: 2
            })
        ));
        assert!(!index.contains("b"));
    }

    fn churned_index(n: usize, flat_threshold: usize, seed: u64) -> TenantVectorIndex {
        let vectors = clustered(n, seed);
        let config = AnnTuningConfig {
            flat_threshold,
            ..AnnTuningConfig::default()
        };
        let mut index = TenantVectorIndex::new(DIM, config);
        for (i, v) in vectors.iter().enumerate() {
            index.upsert(&format!("c{i}"), v).unwrap();
        }
        // Removals leave free keys and HNSW soft deletes; a replace changes a
        // fingerprint in place.
        for i in (0..n).step_by(7) {
            assert!(index.remove(&format!("c{i}")));
        }
        index.upsert("c1", &[3.0; DIM]).unwrap();
        index
    }

    fn assert_same_index(a: &TenantVectorIndex, b: &TenantVectorIndex, seed: u64) {
        assert_eq!(a.len(), b.len());
        assert_eq!(a.is_hnsw(), b.is_hnsw());
        let mut fa: Vec<(String, u64)> = a.fingerprints().map(|(i, f)| (i.into(), f)).collect();
        let mut fb: Vec<(String, u64)> = b.fingerprints().map(|(i, f)| (i.into(), f)).collect();
        fa.sort();
        fb.sort();
        assert_eq!(fa, fb);
        let raw: HashMap<String, Vec<f32>> = HashMap::new();
        for q in clustered(20, seed) {
            assert_eq!(a.search(&q, 10, None, &raw), b.search(&q, 10, None, &raw));
        }
    }

    #[test]
    fn encode_decode_round_trips_flat_and_hnsw() {
        for (n, threshold) in [(300, 8_192), (2_000, 100)] {
            let index = churned_index(n, threshold, 30);
            let bytes = index.encode().unwrap();
            let decoded = TenantVectorIndex::decode(&bytes, DIM, index.config.clone()).unwrap();
            assert_same_index(&index, &decoded, 31);
            // The decoded index stays writable and reuses freed keys.
            let mut decoded = decoded;
            decoded.upsert("fresh", &[1.0; DIM]).unwrap();
            assert_eq!(decoded.len(), index.len() + 1);
            assert!(decoded.ids.key_of("fresh").unwrap() < n as u64);
        }
    }

    #[test]
    fn decode_rejects_damaged_or_mismatched_input() {
        let index = churned_index(1_000, 100, 32);
        let bytes = index.encode().unwrap();
        let config = index.config.clone();
        assert!(matches!(
            TenantVectorIndex::decode(&bytes, DIM + 1, config.clone()),
            Err(VectorIndexError::DimensionMismatch { .. })
        ));
        for cut in [0, 1, 9, bytes.len() / 2, bytes.len() - 1] {
            assert!(
                TenantVectorIndex::decode(&bytes[..cut], DIM, config.clone()).is_err(),
                "truncated at {cut}"
            );
        }
        let mut trailing = bytes.clone();
        trailing.push(0);
        assert!(TenantVectorIndex::decode(&trailing, DIM, config.clone()).is_err());
        let mut bad_kind = bytes;
        bad_kind[0] = 9;
        assert!(TenantVectorIndex::decode(&bad_kind, DIM, config).is_err());
    }

    #[test]
    fn fingerprints_track_the_raw_vector() {
        let a = [1.0f32, 2.0, 3.0];
        let mut b = a;
        b[2] = f32::from_bits(b[2].to_bits() ^ 1);
        assert_ne!(vector_fingerprint(&a), vector_fingerprint(&b));
        assert_ne!(vector_fingerprint(&a), vector_fingerprint(&a[..2]));
        assert_eq!(vector_fingerprint(&a), vector_fingerprint(&[1.0, 2.0, 3.0]));
        let mut index = TenantVectorIndex::new(3, AnnTuningConfig::default());
        index.upsert("x", &a).unwrap();
        index.upsert("x", &b).unwrap();
        let fps: Vec<(&str, u64)> = index.fingerprints().collect();
        assert_eq!(fps, vec![("x", vector_fingerprint(&b))]);
        assert!(!is_indexable(&[0.0, 0.0]) && !is_indexable(&[]) && is_indexable(&a));
    }

    #[test]
    fn large_k_exceeding_the_beam_still_returns_k() {
        let vectors = clustered(4_000, 20);
        let raw = raw_map(&vectors);
        let config = AnnTuningConfig {
            flat_threshold: 100,
            expansion_search: 16,
            ..AnnTuningConfig::default()
        };
        let mut index = TenantVectorIndex::new(DIM, config);
        for (i, v) in vectors.iter().enumerate() {
            index.upsert(&format!("c{i}"), v).unwrap();
        }
        let got = index.search(&vectors[0], 500, None, &raw);
        assert_eq!(got.len(), 500);
    }
}
