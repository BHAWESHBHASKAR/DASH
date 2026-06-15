use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
};

#[cfg(feature = "gpu-backend")]
use std::sync::OnceLock;

use graph::summarize_edges;
use ranking::{RankSignals, bm25_score, score_claim_with_bm25};
use schema::{
    Citation, Claim, ClaimEdge, Evidence, RetrievalRequest,
    RetrievalResult, Stance, StanceMode, ValidationError, tokenize, validate_claim,
    validate_edge, validate_evidence,
};

mod disk;
pub use disk::{DiskBackedStore, DiskStatus};

mod wal;
mod ann;
mod metrics;
#[cfg(feature = "gpu-backend")]
mod gpu;
pub use ann::AnnTuningConfig;
pub use metrics::{StoreIndexStats, StoreLoadStats, VectorBackendRuntime};
pub(crate) use metrics::{VectorBackendPreference, VECTOR_BACKEND_ENV};
pub(crate) use ann::{TenantAnnGraph, ScoredNode, ANN_GRAPH_LEVELS};

#[derive(Default)]
pub(crate) struct Bm25Context {
    doc_freq: HashMap<String, usize>,
    total_docs: usize,
    avg_doc_len: f32,
}



pub use wal::{
    CheckpointPolicy, FileWal, WalCheckpointStats, WalEvent, WalReplayBoundary,
    WalReplayStats, WalReplicationDelta, WalReplicationExport, WalRollbackPoint,
    WalWritePolicy,
};
pub(crate) use wal::{
    BatchCommitRecord, ClaimVectorRecord, PersistedRecord, line_to_record,
};


#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct BatchCommitMetadata {
    pub commit_id: String,
    pub batch_size: usize,
    pub ts_unix_ms: u64,
    pub claim_ids: Vec<String>,
    pub payload_fingerprint: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum StoreError {
    Validation(ValidationError),
    MissingClaim(String),
    Conflict(String),
    InvalidVector(String),
    Io(String),
    Parse(String),
}

const FNV1A_64_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
const FNV1A_64_PRIME: u64 = 0x100000001b3;

fn fnv1a64_feed(state: &mut u64, bytes: &[u8]) {
    for byte in bytes {
        *state ^= u64::from(*byte);
        *state = state.wrapping_mul(FNV1A_64_PRIME);
    }
}

pub fn batch_commit_payload_fingerprint(batch_size: usize, claim_ids: &[String]) -> String {
    let mut state = FNV1A_64_OFFSET_BASIS;
    fnv1a64_feed(&mut state, &(batch_size as u64).to_le_bytes());
    fnv1a64_feed(&mut state, &(claim_ids.len() as u64).to_le_bytes());
    for claim_id in claim_ids {
        fnv1a64_feed(&mut state, &(claim_id.len() as u64).to_le_bytes());
        fnv1a64_feed(&mut state, claim_id.as_bytes());
    }
    format!("{state:016x}")
}

impl From<ValidationError> for StoreError {
    fn from(value: ValidationError) -> Self {
        Self::Validation(value)
    }
}

impl From<std::io::Error> for StoreError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value.to_string())
    }
}


#[derive(Default, Clone)]
/// `Clone` preserves the disk handle via `Arc` (refcount bump, not a
/// deep redb copy). This is the redb PR 2 fix: cloning a store no
/// longer silently drops the disk handle. Before this change, a
/// manual `Clone` impl was required because `redb::Database` is not
/// `Clone`; the impl set `disk: None` and `disk_status: Unavailable`
/// on the clone, which caused disk writes to be silently lost on any
/// code path that cloned the store. With `Arc<DiskBackedStore>`,
/// the cloned store shares the same redb handle and writes to either
/// are visible to both.
pub struct InMemoryStore {
    claims: HashMap<String, Claim>,
    evidence_by_claim: HashMap<String, Vec<Evidence>>,
    edges_by_claim: HashMap<String, Vec<ClaimEdge>>,
    claim_vectors: HashMap<String, Vec<f32>>,
    ann_vector_graphs: HashMap<String, TenantAnnGraph>,
    tenant_vector_dims: HashMap<String, usize>,
    tenant_claim_ids: HashMap<String, HashSet<String>>,
    inverted_index: HashMap<String, HashMap<String, HashSet<String>>>,
    entity_index: HashMap<String, HashMap<String, HashSet<String>>>,
    embedding_index: HashMap<String, HashMap<String, HashSet<String>>>,
    temporal_index: HashMap<String, BTreeMap<i64, HashSet<String>>>,
    batch_commits: HashMap<String, BatchCommitMetadata>,
    claim_tokens: HashMap<String, Vec<String>>,
    ann_tuning: AnnTuningConfig,
    vector_backend_runtime: VectorBackendRuntime,
    wal: Vec<WalEvent>,
    disk: Option<Arc<disk::DiskBackedStore>>,
    disk_status: disk::DiskStatus,
}

impl InMemoryStore {
    pub fn new() -> Self {
        Self::new_with_ann_tuning(AnnTuningConfig::default())
    }

    pub fn new_with_ann_tuning(ann_tuning: AnnTuningConfig) -> Self {
        let vector_backend_runtime =
            resolve_vector_backend_runtime(parse_vector_backend_preference());
        Self {
            ann_tuning,
            vector_backend_runtime,
            ..Self::default()
        }
    }

    pub fn ann_tuning(&self) -> &AnnTuningConfig {
        &self.ann_tuning
    }

    pub fn set_ann_tuning(&mut self, ann_tuning: AnnTuningConfig) {
        self.ann_tuning = ann_tuning;
    }

    pub fn vector_backend_runtime(&self) -> VectorBackendRuntime {
        self.vector_backend_runtime
    }

    pub fn vector_backend_label(&self) -> &'static str {
        self.vector_backend_runtime.as_str()
    }

    /// Attach a `redb`-backed disk store to this in-memory store. The
    /// disk store is opened at `path` (creating it on first use).
    /// Every subsequent `apply_*` call will mirror the in-memory
    /// mutation to disk BEFORE the in-memory state changes. If the
    /// disk write fails, the in-memory apply is aborted and the error
    /// is returned to the caller.
    ///
    /// On open failure, returns the in-memory store with the disk
    /// detached and `disk_status` set to `Unavailable { reason }`.
    /// The error is also returned so the caller can log it. The
    /// store is always returned (the caller's in-memory state is
    /// preserved).
    pub fn with_disk(self, path: impl AsRef<std::path::Path>) -> Result<Self, String> {
        match disk::DiskBackedStore::new(path) {
            Ok(disk) => Ok(Self {
                disk: Some(Arc::new(disk)),
                disk_status: disk::DiskStatus::Available,
                ..self
            }),
            Err(reason) => Ok(Self {
                disk: None,
                disk_status: disk::DiskStatus::Unavailable { reason: reason.clone() },
                ..self
            }),
        }
    }

    /// The current disk status. Returns `DiskStatus::Unavailable` if
    /// no disk was attached (the default in-memory mode).
    pub fn disk_status(&self) -> &disk::DiskStatus {
        &self.disk_status
    }

    /// Construct an `InMemoryStore` by bulk-loading from a disk
    /// snapshot, then replaying any WAL delta. Returns the new store
    /// + load stats. This is the cold-start path when both the WAL
    /// and the redb snapshot are available.
    #[allow(clippy::doc_lazy_continuation)]
    pub fn load_from_disk_and_wal(
        disk_path: impl AsRef<std::path::Path>,
        wal: &mut FileWal,
        ann_tuning: AnnTuningConfig,
    ) -> Result<(Self, StoreLoadStats), String> {
        // Helper: replay the WAL into an in-memory store, returning
        // the breakdown stats. Errors are converted to `String` for
        // the disk API.
        fn replay_into(
            store: &mut InMemoryStore,
            wal: &mut FileWal,
        ) -> Result<StoreLoadStats, String> {
            let (records, replay_stats) = wal
                .replay_records_with_stats()
                .map_err(|e| format!("wal replay: {e:?}"))?;
            let mut claims_loaded = 0usize;
            let mut evidence_loaded = 0usize;
            let mut edges_loaded = 0usize;
            let mut vectors_loaded = 0usize;
            for record in records {
                match &record {
                    PersistedRecord::Claim(_) => claims_loaded += 1,
                    PersistedRecord::Evidence(_) => evidence_loaded += 1,
                    PersistedRecord::Edge(_) => edges_loaded += 1,
                    PersistedRecord::ClaimVector(_) => vectors_loaded += 1,
                    PersistedRecord::BatchCommit(_) => {}
                }
                store
                    .apply_persisted_record(record)
                    .map_err(|e| format!("apply_persisted_record: {e:?}"))?;
            }
            Ok(StoreLoadStats {
                replay: replay_stats,
                claims_loaded,
                evidence_loaded,
                edges_loaded,
                vectors_loaded,
            })
        }

        // 1. Open the disk. If the open fails, fall back to the
        //    WAL-only path.
        let disk = match disk::DiskBackedStore::new(disk_path) {
            Ok(disk) => Some(Arc::new(disk)),
            Err(reason) => {
                let mut store = Self::new_with_ann_tuning(ann_tuning);
                let stats = replay_into(&mut store, wal)?;
                store.disk_status = disk::DiskStatus::Unavailable { reason };
                return Ok((store, stats));
            }
        };

        // 2. Set status to Recovering for the duration of the bulk
        //    load + WAL tail replay.
        let mut store = Self {
            disk,
            disk_status: disk::DiskStatus::Recovering,
            ..Self::new_with_ann_tuning(ann_tuning)
        };
        // Arc::clone the disk handle so we can hold a borrow on the
        // store across the bulk-load call. The Arc refcount is bumped
        // to 2 (the store holds one, the local `disk` binding holds
        // the other), and dropped when the binding goes out of scope
        // at the end of this function. This replaces the pre-PR-2
        // pattern of `take()` + restore, which was only needed when
        // the disk was an owned `DiskBackedStore` (not Clone-able
        // because it wraps a `redb::Database`).
        let disk = Arc::clone(
            store
                .disk
                .as_ref()
                .expect("disk was just attached"),
        );
        let claims_loaded = disk
            .bulk_load_claims_into(&mut store)
            .map_err(|e| format!("disk bulk load: {e}"))?;
        // 3. Replay the WAL tail over the bulk-loaded state.
        let mut stats = replay_into(&mut store, wal)?;
        // The bulk-loaded count is the dominant figure; merge it
        // with the WAL tail counts (claims loaded by the bulk
        // path will be overwritten in `claims_loaded` by the WAL
        // tail counter, so we explicitly prefer the bulk count).
        stats.claims_loaded = claims_loaded;
        store.disk_status = disk::DiskStatus::Available;
        Ok((store, stats))
    }

    pub fn load_from_wal(wal: &FileWal) -> Result<Self, StoreError> {
        let (store, _) = Self::load_from_wal_with_stats(wal)?;
        Ok(store)
    }

    pub fn load_from_wal_with_ann_tuning(
        wal: &FileWal,
        ann_tuning: AnnTuningConfig,
    ) -> Result<Self, StoreError> {
        let (store, _) = Self::load_from_wal_with_stats_and_ann_tuning(wal, ann_tuning)?;
        Ok(store)
    }

    pub fn load_from_wal_with_stats(wal: &FileWal) -> Result<(Self, StoreLoadStats), StoreError> {
        Self::load_from_wal_with_stats_and_ann_tuning(wal, AnnTuningConfig::default())
    }

    pub fn load_from_wal_with_stats_and_ann_tuning(
        wal: &FileWal,
        ann_tuning: AnnTuningConfig,
    ) -> Result<(Self, StoreLoadStats), StoreError> {
        let mut store = Self::new_with_ann_tuning(ann_tuning);
        let (records, replay_stats) = wal.replay_records_with_stats()?;
        let mut claims_loaded = 0usize;
        let mut evidence_loaded = 0usize;
        let mut edges_loaded = 0usize;
        let mut vectors_loaded = 0usize;

        for record in records {
            match &record {
                PersistedRecord::Claim(_) => claims_loaded += 1,
                PersistedRecord::Evidence(_) => evidence_loaded += 1,
                PersistedRecord::Edge(_) => edges_loaded += 1,
                PersistedRecord::ClaimVector(_) => vectors_loaded += 1,
                PersistedRecord::BatchCommit(_) => {}
            }
            store.apply_persisted_record(record)?;
        }
        Ok((
            store,
            StoreLoadStats {
                replay: replay_stats,
                claims_loaded,
                evidence_loaded,
                edges_loaded,
                vectors_loaded,
            },
        ))
    }

    pub fn ingest_bundle(
        &mut self,
        claim: Claim,
        evidence: Vec<Evidence>,
        edges: Vec<ClaimEdge>,
    ) -> Result<(), StoreError> {
        self.validate_bundle(&claim, &evidence, &edges)?;
        self.apply_bundle(claim, evidence, edges)
    }

    pub fn ingest_bundle_persistent(
        &mut self,
        wal: &mut FileWal,
        claim: Claim,
        evidence: Vec<Evidence>,
        edges: Vec<ClaimEdge>,
    ) -> Result<(), StoreError> {
        self.validate_bundle(&claim, &evidence, &edges)?;

        wal.append_claim(&claim)?;
        for evd in &evidence {
            wal.append_evidence(evd)?;
        }
        for edge in &edges {
            wal.append_edge(edge)?;
        }

        self.apply_bundle(claim, evidence, edges)
    }

    pub fn ingest_bundle_persistent_with_policy(
        &mut self,
        wal: &mut FileWal,
        policy: &CheckpointPolicy,
        claim: Claim,
        evidence: Vec<Evidence>,
        edges: Vec<ClaimEdge>,
    ) -> Result<Option<WalCheckpointStats>, StoreError> {
        self.ingest_bundle_persistent(wal, claim, evidence, edges)?;
        if self.should_checkpoint(wal, policy)? {
            let stats = self.checkpoint_and_compact(wal)?;
            Ok(Some(stats))
        } else {
            Ok(None)
        }
    }

    pub fn upsert_claim_vector(
        &mut self,
        claim_id: &str,
        vector: Vec<f32>,
    ) -> Result<(), StoreError> {
        self.apply_claim_vector(claim_id, vector)
    }

    pub fn upsert_claim_vector_persistent(
        &mut self,
        wal: &mut FileWal,
        claim_id: &str,
        vector: Vec<f32>,
    ) -> Result<(), StoreError> {
        validate_vector(&vector)?;
        wal.append_claim_vector(claim_id, &vector)?;
        self.apply_claim_vector(claim_id, vector)
    }

    pub fn checkpoint_and_compact(
        &self,
        wal: &mut FileWal,
    ) -> Result<WalCheckpointStats, StoreError> {
        let records = self.snapshot_records();
        wal.compact_with_snapshot(&records)
    }

    pub fn observe_batch_commit(
        &mut self,
        commit_id: &str,
        batch_size: usize,
        ts_unix_ms: u64,
        claim_ids: &[String],
    ) -> Result<(), StoreError> {
        self.apply_batch_commit_record(BatchCommitRecord {
            commit_id: commit_id.to_string(),
            batch_size,
            ts_unix_ms,
            claim_ids: claim_ids.to_vec(),
        })
    }

    pub fn batch_commit_metadata(&self, commit_id: &str) -> Option<&BatchCommitMetadata> {
        self.batch_commits.get(commit_id)
    }

    pub fn apply_persisted_record_line(&mut self, line: &str) -> Result<(), StoreError> {
        self.apply_persisted_record(line_to_record(line)?)
    }

    pub fn retrieve(&self, req: &RetrievalRequest) -> Vec<RetrievalResult> {
        self.retrieve_with_time_range_and_query_vector(req, None, None, None)
    }

    /// Semantic-first retrieval. Takes a pre-computed embedding of the
    /// query and uses it as the primary ranking signal (cosine in
    /// `[-1, 1]`, mapped to `[0, 1]`); the lexical+BM25 score becomes a
    /// small tie-breaker. When the query vector is the same shape as
    /// the stored vectors for the tenant, this is the recommended
    /// retrieval entry point — the lexical fallback is for environments
    /// that haven't yet wired up an embedding model.
    pub fn retrieve_semantic(
        &self,
        req: &RetrievalRequest,
        query_vector: &[f32],
    ) -> Vec<RetrievalResult> {
        self.retrieve_with_time_range_and_query_vector(
            req,
            None,
            None,
            Some(query_vector),
        )
    }

    pub fn retrieve_with_time_range(
        &self,
        req: &RetrievalRequest,
        from_unix: Option<i64>,
        to_unix: Option<i64>,
    ) -> Vec<RetrievalResult> {
        self.retrieve_with_time_range_and_query_vector(req, from_unix, to_unix, None)
    }

    pub fn retrieve_with_time_range_and_query_vector(
        &self,
        req: &RetrievalRequest,
        from_unix: Option<i64>,
        to_unix: Option<i64>,
        query_vector: Option<&[f32]>,
    ) -> Vec<RetrievalResult> {
        self.retrieve_with_time_range_query_vector_and_allowed_claim_ids(
            req,
            from_unix,
            to_unix,
            query_vector,
            None,
        )
    }

    pub fn retrieve_with_time_range_query_vector_and_allowed_claim_ids(
        &self,
        req: &RetrievalRequest,
        from_unix: Option<i64>,
        to_unix: Option<i64>,
        query_vector: Option<&[f32]>,
        allowed_claim_ids: Option<&HashSet<String>>,
    ) -> Vec<RetrievalResult> {
        let candidates = self.candidate_claim_ids(
            &req.tenant_id,
            &req.query,
            (from_unix, to_unix),
            query_vector,
            req.top_k,
            allowed_claim_ids,
        );
        self.score_and_rank_candidate_claim_ids(req, query_vector, candidates)
    }

    pub fn retrieve_with_time_range_query_vector_and_explicit_candidate_claim_ids(
        &self,
        req: &RetrievalRequest,
        from_unix: Option<i64>,
        to_unix: Option<i64>,
        query_vector: Option<&[f32]>,
        candidate_claim_ids: &HashSet<String>,
        allowed_claim_ids: Option<&HashSet<String>>,
    ) -> Vec<RetrievalResult> {
        let mut candidates: Vec<String> = candidate_claim_ids
            .iter()
            .filter_map(|claim_id| {
                let claim = self.claims.get(claim_id)?;
                if claim.tenant_id != req.tenant_id {
                    return None;
                }
                if !claim_matches_time_range(claim, from_unix, to_unix) {
                    return None;
                }
                if let Some(allowed_ids) = allowed_claim_ids
                    && !allowed_ids.contains(claim_id.as_str())
                {
                    return None;
                }
                Some(claim_id.clone())
            })
            .collect();
        candidates.sort_unstable();
        self.score_and_rank_candidate_claim_ids(req, query_vector, candidates)
    }

    fn score_and_rank_candidate_claim_ids(
        &self,
        req: &RetrievalRequest,
        query_vector: Option<&[f32]>,
        candidates: Vec<String>,
    ) -> Vec<RetrievalResult> {
        let mut ranked: Vec<RetrievalResult> = Vec::new();
        let bm25_context = self.bm25_context_for_tenant(&req.tenant_id, &req.query);
        let dense_similarities = query_vector.map(|vector| {
            let candidate_vectors: Vec<(String, &[f32])> = candidates
                .iter()
                .filter_map(|claim_id| {
                    let claim = self.claims.get(claim_id)?;
                    if claim.tenant_id != req.tenant_id {
                        return None;
                    }
                    let claim_vector = self.claim_vectors.get(claim_id)?;
                    Some((claim_id.clone(), claim_vector.as_slice()))
                })
                .collect();
            self.score_query_candidate_vectors(vector, candidate_vectors)
                .into_iter()
                .collect::<HashMap<String, f32>>()
        });

        for claim_id in candidates {
            let Some(claim) = self.claims.get(&claim_id) else {
                continue;
            };

            let evidence = self
                .evidence_by_claim
                .get(&claim.claim_id)
                .cloned()
                .unwrap_or_default();
            let edges = self
                .edges_by_claim
                .get(&claim.claim_id)
                .cloned()
                .unwrap_or_default();
            let edge_summary = summarize_edges(&edges);

            let supports = evidence
                .iter()
                .filter(|e| matches!(e.stance, Stance::Supports))
                .count()
                + edge_summary.supports;
            let contradicts = evidence
                .iter()
                .filter(|e| matches!(e.stance, Stance::Contradicts))
                .count()
                + edge_summary.contradicts;

            if matches!(req.stance_mode, StanceMode::SupportOnly) && contradicts > supports {
                continue;
            }

            let avg_quality = if evidence.is_empty() {
                0.0
            } else {
                evidence.iter().map(|e| e.source_quality).sum::<f32>() / evidence.len() as f32
            };

            let bm25 = self
                .claim_tokens
                .get(&claim.claim_id)
                .map(|tokens| {
                    bm25_score(
                        &req.query,
                        tokens,
                        &bm25_context.doc_freq,
                        bm25_context.total_docs,
                        bm25_context.avg_doc_len,
                    )
                })
                .unwrap_or(0.0);

            let dense_similarity = dense_similarities
                .as_ref()
                .and_then(|scores| scores.get(&claim.claim_id))
                .copied()
                .unwrap_or(0.0);

            let lexical_score = score_claim_with_bm25(
                &req.query,
                claim,
                avg_quality,
                RankSignals {
                    supports,
                    contradicts,
                },
                bm25,
            );

            let score = if query_vector.is_some() {
                // Semantic-first retrieval: dense similarity is the
                // PRIMARY signal (cosine in [-1, 1] -> mapped to
                // [0, 1] via the embedding backend). The lexical/BM25
                // score is a small tie-breaker when dense similarities
                // are tied. This replaces the historical 0.35 additive
                // weight with semantic-primary scoring, which is the
                // right default when the caller explicitly provides
                // a query vector.
                let dense_primary = (dense_similarity + 1.0) * 0.5;
                dense_primary + (lexical_score * 0.1)
            } else {
                // Lexical-only retrieval: historical behavior
                // (dense_similarity is 0.0 when no query_vector).
                lexical_score + (dense_similarity * 0.35)
            };

            let citations = evidence
                .iter()
                .map(|e| Citation {
                    evidence_id: e.evidence_id.clone(),
                    source_id: e.source_id.clone(),
                    stance: e.stance.clone(),
                    source_quality: e.source_quality,
                    chunk_id: e.chunk_id.clone(),
                    span_start: e.span_start,
                    span_end: e.span_end,
                    doc_id: e.doc_id.clone(),
                    extraction_model: e.extraction_model.clone(),
                    ingested_at: e.ingested_at,
                })
                .collect();
            ranked.push(RetrievalResult {
                claim_id: claim.claim_id.clone(),
                canonical_text: claim.canonical_text.clone(),
                score,
                supports,
                contradicts,
                citations,
            });
        }

        ranked.sort_by(|a, b| b.score.total_cmp(&a.score));
        ranked.into_iter().take(req.top_k).collect()
    }

    pub fn claims_for_tenant(&self, tenant_id: &str) -> Vec<Claim> {
        self.claims
            .values()
            .filter(|claim| claim.tenant_id == tenant_id)
            .cloned()
            .collect()
    }

    pub fn tenant_ids(&self) -> Vec<String> {
        let mut out: Vec<String> = self.tenant_claim_ids.keys().cloned().collect();
        out.sort_unstable();
        out
    }

    pub fn claim_ids_for_tenant(&self, tenant_id: &str) -> HashSet<String> {
        self.tenant_claim_ids
            .get(tenant_id)
            .cloned()
            .unwrap_or_default()
    }

    pub fn claim_by_id(&self, claim_id: &str) -> Option<&Claim> {
        self.claims.get(claim_id)
    }

    pub fn claim_ids_for_entity(&self, tenant_id: &str, entity: &str) -> HashSet<String> {
        let key = normalize_index_key(entity);
        if key.is_empty() {
            return HashSet::new();
        }
        self.entity_index
            .get(tenant_id)
            .and_then(|index| index.get(&key))
            .cloned()
            .unwrap_or_default()
    }

    pub fn claim_ids_for_embedding_id(
        &self,
        tenant_id: &str,
        embedding_id: &str,
    ) -> HashSet<String> {
        let key = embedding_id.trim();
        if key.is_empty() {
            return HashSet::new();
        }
        self.embedding_index
            .get(tenant_id)
            .and_then(|index| index.get(key))
            .cloned()
            .unwrap_or_default()
    }

    pub fn edges_for_claim(&self, claim_id: &str) -> Vec<ClaimEdge> {
        self.edges_by_claim
            .get(claim_id)
            .cloned()
            .unwrap_or_default()
    }

    pub fn claims_for_entity(&self, tenant_id: &str, entity: &str) -> Vec<Claim> {
        let mut out: Vec<Claim> = self
            .claim_ids_for_entity(tenant_id, entity)
            .iter()
            .filter_map(|id| self.claims.get(id).cloned())
            .collect();
        out.sort_by(|a, b| a.claim_id.cmp(&b.claim_id));
        out
    }

    pub fn index_stats(&self) -> StoreIndexStats {
        let inverted_terms = self
            .inverted_index
            .values()
            .map(|tenant_index| tenant_index.len())
            .sum();
        let entity_terms = self
            .entity_index
            .values()
            .map(|tenant_index| tenant_index.len())
            .sum();
        let temporal_buckets = self
            .temporal_index
            .values()
            .map(|timeline| timeline.len())
            .sum();
        let ann_vector_buckets = self
            .ann_vector_graphs
            .values()
            .map(|graph| graph.levels.first().map(|level| level.len()).unwrap_or(0))
            .sum();
        StoreIndexStats {
            tenant_count: self.tenant_claim_ids.len(),
            claim_count: self.claims.len(),
            vector_count: self.claim_vectors.len(),
            inverted_terms,
            entity_terms,
            temporal_buckets,
            ann_vector_buckets,
        }
    }

    pub fn candidate_count(
        &self,
        tenant_id: &str,
        query: &str,
        from_unix: Option<i64>,
        to_unix: Option<i64>,
    ) -> usize {
        self.candidate_claim_ids(tenant_id, query, (from_unix, to_unix), None, 5, None)
            .len()
    }

    pub fn candidate_count_for_retrieval_request(&self, req: &RetrievalRequest) -> usize {
        self.candidate_count_with_query_vector(req, None, None, None)
    }

    pub fn candidate_count_with_query_vector(
        &self,
        req: &RetrievalRequest,
        query_vector: Option<&[f32]>,
        from_unix: Option<i64>,
        to_unix: Option<i64>,
    ) -> usize {
        self.candidate_count_with_query_vector_and_allowed_claim_ids(
            req,
            query_vector,
            (from_unix, to_unix),
            None,
        )
    }

    pub fn candidate_count_with_query_vector_and_allowed_claim_ids(
        &self,
        req: &RetrievalRequest,
        query_vector: Option<&[f32]>,
        time_range: (Option<i64>, Option<i64>),
        allowed_claim_ids: Option<&HashSet<String>>,
    ) -> usize {
        let (from_unix, to_unix) = time_range;
        self.candidate_claim_ids(
            &req.tenant_id,
            &req.query,
            (from_unix, to_unix),
            query_vector,
            req.top_k,
            allowed_claim_ids,
        )
        .len()
    }

    pub fn ann_candidate_count_for_query_vector(
        &self,
        tenant_id: &str,
        query_vector: &[f32],
        top_k: usize,
    ) -> usize {
        if query_vector.is_empty() {
            return 0;
        }
        let vector_top_n = (top_k.saturating_mul(20)).clamp(100, 5000);
        self.vector_candidates(tenant_id, query_vector, vector_top_n)
            .len()
    }

    pub fn ann_vector_top_candidates(
        &self,
        tenant_id: &str,
        query_vector: &[f32],
        top_n: usize,
    ) -> Vec<String> {
        if query_vector.is_empty() || top_n == 0 {
            return Vec::new();
        }
        self.vector_candidates(tenant_id, query_vector, top_n)
    }

    pub fn exact_vector_top_candidates(
        &self,
        tenant_id: &str,
        query_vector: &[f32],
        top_n: usize,
    ) -> Vec<String> {
        if query_vector.is_empty() || top_n == 0 {
            return Vec::new();
        }

        let candidate_vectors: Vec<(String, &[f32])> = self
            .claim_vectors
            .iter()
            .filter_map(|(claim_id, vector)| {
                let claim = self.claims.get(claim_id)?;
                if claim.tenant_id != tenant_id {
                    return None;
                }
                Some((claim_id.clone(), vector.as_slice()))
            })
            .collect();
        let mut scored = self.score_query_candidate_vectors(query_vector, candidate_vectors);
        scored.sort_by(|a, b| b.1.total_cmp(&a.1));
        scored
            .into_iter()
            .take(top_n)
            .map(|(claim_id, _)| claim_id)
            .collect()
    }

    pub fn wal_len(&self) -> usize {
        self.wal.len()
    }

    pub fn claims_len(&self) -> usize {
        self.claims.len()
    }

    // ----------------------------------------------------------------
    // Internal iterators used by the disk module's checkpoint path.
    // `pub(crate)` so the disk module can read every record but the
    // public API stays unchanged.
    // ----------------------------------------------------------------

    pub(crate) fn claims_iter(&self) -> impl Iterator<Item = &Claim> {
        self.claims.values()
    }

    pub(crate) fn evidence_iter(&self) -> impl Iterator<Item = (&str, &Vec<Evidence>)> {
        self.evidence_by_claim
            .iter()
            .map(|(k, v)| (k.as_str(), v))
    }

    pub(crate) fn edges_iter(&self) -> impl Iterator<Item = (&str, &Vec<ClaimEdge>)> {
        self.edges_by_claim
            .iter()
            .map(|(k, v)| (k.as_str(), v))
    }

    pub(crate) fn claim_vectors_iter(&self) -> impl Iterator<Item = (&str, &Vec<f32>)> {
        self.claim_vectors
            .iter()
            .map(|(k, v)| (k.as_str(), v))
    }

    pub(crate) fn batch_commits_iter(&self) -> impl Iterator<Item = &BatchCommitMetadata> {
        self.batch_commits.values()
    }

    pub(crate) fn tenant_dims_iter(&self) -> impl Iterator<Item = (&str, &usize)> {
        self.tenant_vector_dims
            .iter()
            .map(|(k, v)| (k.as_str(), v))
    }

    pub(crate) fn tenant_claim_set_iter(&self) -> impl Iterator<Item = (String, String)> {
        self.tenant_claim_ids
            .iter()
            .flat_map(|(tenant, claims)| {
                claims.iter().map(move |claim| (tenant.clone(), claim.clone()))
            })
    }

    fn should_checkpoint(
        &self,
        wal: &FileWal,
        policy: &CheckpointPolicy,
    ) -> Result<bool, StoreError> {
        let record_threshold_met = match policy.max_wal_records {
            Some(threshold) if threshold > 0 => wal.wal_record_count()? >= threshold,
            _ => false,
        };
        let byte_threshold_met = match policy.max_wal_bytes {
            Some(threshold) if threshold > 0 => wal.wal_size_bytes()? >= threshold,
            _ => false,
        };
        Ok(record_threshold_met || byte_threshold_met)
    }

    fn candidate_claim_ids(
        &self,
        tenant_id: &str,
        query: &str,
        time_range: (Option<i64>, Option<i64>),
        query_vector: Option<&[f32]>,
        top_k: usize,
        allowed_claim_ids: Option<&HashSet<String>>,
    ) -> Vec<String> {
        let (from_unix, to_unix) = time_range;
        let mut candidates: HashSet<String> = HashSet::new();
        let query_tokens = tokenize(query);

        if query_tokens.is_empty() {
            if let Some(ids) = self.tenant_claim_ids.get(tenant_id) {
                candidates.extend(ids.iter().cloned());
            }
        } else if let Some(tenant_index) = self.inverted_index.get(tenant_id) {
            for token in query_tokens {
                if let Some(ids) = tenant_index.get(&token) {
                    candidates.extend(ids.iter().cloned());
                }
            }
            if candidates.is_empty()
                && let Some(ids) = self.tenant_claim_ids.get(tenant_id)
            {
                candidates.extend(ids.iter().cloned());
            }
        }

        if let Some(vector) = query_vector {
            let vector_top_n = (top_k.saturating_mul(20)).clamp(100, 5000);
            for claim_id in self.vector_candidates(tenant_id, vector, vector_top_n) {
                candidates.insert(claim_id);
            }
        }

        if from_unix.is_some() || to_unix.is_some() {
            candidates.retain(|claim_id| {
                self.claims
                    .get(claim_id)
                    .is_some_and(|claim| claim_matches_time_range(claim, from_unix, to_unix))
            });
        }
        if let Some(allowed_ids) = allowed_claim_ids {
            candidates = candidates.intersection(allowed_ids).cloned().collect();
        }

        let mut out: Vec<String> = candidates
            .into_iter()
            .filter(|claim_id| {
                self.claims
                    .get(claim_id)
                    .is_some_and(|claim| claim.tenant_id == tenant_id)
            })
            .collect();
        out.sort_unstable();
        out
    }

    fn vector_candidates(
        &self,
        tenant_id: &str,
        query_vector: &[f32],
        top_n: usize,
    ) -> Vec<String> {
        if query_vector.is_empty() {
            return Vec::new();
        }

        let mut scoped_ids = self.approximate_vector_candidate_ids(tenant_id, query_vector, top_n);
        if scoped_ids.is_empty() {
            scoped_ids = self
                .claim_vectors
                .keys()
                .filter(|claim_id| {
                    self.claims
                        .get(*claim_id)
                        .is_some_and(|claim| claim.tenant_id == tenant_id)
                })
                .cloned()
                .collect();
        }

        let candidate_vectors: Vec<(String, &[f32])> = scoped_ids
            .into_iter()
            .filter_map(|claim_id| {
                let vector = self.claim_vectors.get(&claim_id)?;
                let claim = self.claims.get(&claim_id)?;
                if claim.tenant_id != tenant_id {
                    return None;
                }
                Some((claim_id, vector.as_slice()))
            })
            .collect();
        let mut scored = self.score_query_candidate_vectors(query_vector, candidate_vectors);
        scored.sort_by(|a, b| b.1.total_cmp(&a.1));
        scored
            .into_iter()
            .take(top_n)
            .map(|(claim_id, _)| claim_id)
            .collect()
    }

    fn score_query_candidate_vectors(
        &self,
        query_vector: &[f32],
        candidate_vectors: Vec<(String, &[f32])>,
    ) -> Vec<(String, f32)> {
        if candidate_vectors.is_empty() || query_vector.is_empty() {
            return Vec::new();
        }

        if self.vector_backend_runtime.is_gpu() {
            #[cfg(feature = "gpu-backend")]
            if let Some(scored) =
                gpu_score_query_candidate_vectors(query_vector, &candidate_vectors)
            {
                return scored;
            }
        }

        score_query_candidate_vectors_cpu(query_vector, &candidate_vectors)
    }

    fn approximate_vector_candidate_ids(
        &self,
        tenant_id: &str,
        query_vector: &[f32],
        top_n: usize,
    ) -> HashSet<String> {
        let mut out = HashSet::new();
        let Some(graph) = self.ann_vector_graphs.get(tenant_id) else {
            return out;
        };
        let Some(entry_point) = graph.entry_point.as_ref() else {
            return out;
        };
        let Some(mut current_score) = self
            .claim_vectors
            .get(entry_point)
            .and_then(|entry_vector| cosine_similarity(query_vector, entry_vector))
        else {
            return out;
        };
        let mut current = entry_point.clone();

        let max_level = graph.entry_level.min(ANN_GRAPH_LEVELS.saturating_sub(1));
        for level in (1..=max_level).rev() {
            loop {
                let mut improved = false;
                let Some(neighbors) = graph.levels[level].get(&current) else {
                    break;
                };
                for neighbor_id in neighbors {
                    let Some(score) =
                        self.claim_vectors
                            .get(neighbor_id)
                            .and_then(|neighbor_vector| {
                                cosine_similarity(query_vector, neighbor_vector)
                            })
                    else {
                        continue;
                    };
                    if score > current_score {
                        current = neighbor_id.clone();
                        current_score = score;
                        improved = true;
                    }
                }
                if !improved {
                    break;
                }
            }
        }

        let mut frontier = std::collections::BinaryHeap::new();
        let mut visited = HashSet::new();
        if visited.insert(current.clone()) {
            frontier.push(ScoredNode {
                claim_id: current,
                score: current_score,
            });
        }
        if visited.insert(entry_point.clone())
            && let Some(score) = self
                .claim_vectors
                .get(entry_point)
                .and_then(|entry_vector| cosine_similarity(query_vector, entry_vector))
        {
            frontier.push(ScoredNode {
                claim_id: entry_point.clone(),
                score,
            });
        }

        let expansion_budget = top_n
            .saturating_mul(self.ann_tuning.search_expansion_factor.max(1))
            .clamp(
                self.ann_tuning.search_expansion_min.max(1),
                self.ann_tuning
                    .search_expansion_max
                    .max(self.ann_tuning.search_expansion_min.max(1)),
            );
        let mut expanded = 0usize;

        while let Some(node) = frontier.pop() {
            out.insert(node.claim_id.clone());
            expanded += 1;
            if expanded >= expansion_budget {
                break;
            }

            let Some(neighbors) = graph.levels[0].get(&node.claim_id) else {
                continue;
            };
            for neighbor_id in neighbors {
                if !visited.insert(neighbor_id.clone()) {
                    continue;
                }
                let Some(neighbor_vector) = self.claim_vectors.get(neighbor_id) else {
                    continue;
                };
                let Some(score) = cosine_similarity(query_vector, neighbor_vector) else {
                    continue;
                };
                frontier.push(ScoredNode {
                    claim_id: neighbor_id.clone(),
                    score,
                });
            }
        }

        out
    }

    fn bm25_context_for_tenant(&self, tenant_id: &str, query: &str) -> Bm25Context {
        let total_docs = self
            .tenant_claim_ids
            .get(tenant_id)
            .map(|ids| ids.len())
            .unwrap_or(0);
        if total_docs == 0 {
            return Bm25Context::default();
        }

        let mut total_len = 0usize;
        for claim_id in self.tenant_claim_ids.get(tenant_id).into_iter().flatten() {
            total_len += self
                .claim_tokens
                .get(claim_id)
                .map(|tokens| tokens.len())
                .unwrap_or(0);
        }
        let avg_doc_len = (total_len as f32 / total_docs as f32).max(1.0);

        let mut doc_freq = HashMap::new();
        if let Some(index) = self.inverted_index.get(tenant_id) {
            for token in tokenize(query) {
                doc_freq.insert(
                    token.clone(),
                    index.get(&token).map(|ids| ids.len()).unwrap_or(0),
                );
            }
        }

        Bm25Context {
            doc_freq,
            total_docs,
            avg_doc_len,
        }
    }

    fn snapshot_records(&self) -> Vec<PersistedRecord> {
        let mut claim_ids: Vec<String> = self.claims.keys().cloned().collect();
        claim_ids.sort_unstable();

        let mut records = Vec::new();
        for claim_id in &claim_ids {
            if let Some(claim) = self.claims.get(claim_id) {
                records.push(PersistedRecord::Claim(claim.clone()));
            }
        }

        for claim_id in &claim_ids {
            if let Some(values) = self.claim_vectors.get(claim_id) {
                records.push(PersistedRecord::ClaimVector(ClaimVectorRecord {
                    claim_id: claim_id.clone(),
                    values: values.clone(),
                }));
            }
        }

        for claim_id in &claim_ids {
            if let Some(evidence) = self.evidence_by_claim.get(claim_id) {
                let mut evidence = evidence.clone();
                evidence.sort_by(|a, b| a.evidence_id.cmp(&b.evidence_id));
                for evd in evidence {
                    records.push(PersistedRecord::Evidence(evd));
                }
            }
        }

        for claim_id in &claim_ids {
            if let Some(edges) = self.edges_by_claim.get(claim_id) {
                let mut edges = edges.clone();
                edges.sort_by(|a, b| a.edge_id.cmp(&b.edge_id));
                for edge in edges {
                    records.push(PersistedRecord::Edge(edge));
                }
            }
        }

        let mut commit_ids: Vec<&String> = self.batch_commits.keys().collect();
        commit_ids.sort_unstable();
        for commit_id in commit_ids {
            let metadata = self
                .batch_commits
                .get(commit_id)
                .expect("batch commit should exist");
            records.push(PersistedRecord::BatchCommit(BatchCommitRecord {
                commit_id: metadata.commit_id.clone(),
                batch_size: metadata.batch_size,
                ts_unix_ms: metadata.ts_unix_ms,
                claim_ids: metadata.claim_ids.clone(),
            }));
        }

        records
    }

    fn validate_bundle(
        &self,
        claim: &Claim,
        evidence: &[Evidence],
        edges: &[ClaimEdge],
    ) -> Result<(), StoreError> {
        validate_claim(claim)?;
        if let Some(existing) = self.claims.get(&claim.claim_id)
            && existing.tenant_id != claim.tenant_id
        {
            return Err(StoreError::Conflict(format!(
                "claim_id '{}' already exists for tenant '{}'",
                claim.claim_id, existing.tenant_id
            )));
        }
        for evd in evidence {
            validate_evidence(evd)?;
            if evd.claim_id != claim.claim_id {
                return Err(StoreError::MissingClaim(evd.claim_id.clone()));
            }
        }
        for edge in edges {
            validate_edge(edge)?;
            if edge.from_claim_id != claim.claim_id {
                return Err(StoreError::MissingClaim(edge.from_claim_id.clone()));
            }
        }
        Ok(())
    }

    fn apply_bundle(
        &mut self,
        claim: Claim,
        evidence: Vec<Evidence>,
        edges: Vec<ClaimEdge>,
    ) -> Result<(), StoreError> {
        self.apply_claim(claim)?;
        for evd in evidence {
            self.apply_evidence(evd)?;
        }
        for edge in edges {
            self.apply_edge(edge)?;
        }
        Ok(())
    }

    fn apply_persisted_record(&mut self, record: PersistedRecord) -> Result<(), StoreError> {
        match record {
            PersistedRecord::Claim(claim) => self.apply_claim(claim),
            PersistedRecord::Evidence(evidence) => self.apply_evidence(evidence),
            PersistedRecord::Edge(edge) => self.apply_edge(edge),
            PersistedRecord::ClaimVector(record) => {
                self.apply_claim_vector(&record.claim_id, record.values)
            }
            PersistedRecord::BatchCommit(record) => self.apply_batch_commit_record(record),
        }
    }

    fn apply_claim(&mut self, claim: Claim) -> Result<(), StoreError> {
        // Write to disk BEFORE mutating in-memory state. If the disk
        // write fails, the in-memory state is unchanged.
        if let Some(disk) = self.disk.as_ref() {
            disk.put_claim(&claim).map_err(StoreError::Io)?;
            disk.add_claim_to_tenant(&claim.tenant_id, &claim.claim_id)
                .map_err(StoreError::Io)?;
        }
        self.apply_claim_inner(claim)
    }

    /// In-memory only — no disk mirror. Used by the disk module's
    /// bulk-load path (`bulk_load_claims_into`) to avoid re-writing
    /// data that's already on disk.
    pub(crate) fn apply_claim_for_load(&mut self, claim: Claim) -> Result<(), StoreError> {
        self.apply_claim_inner(claim)
    }

    fn apply_claim_inner(&mut self, claim: Claim) -> Result<(), StoreError> {
        validate_claim(&claim)?;
        let claim_id = claim.claim_id.clone();
        if let Some(previous) = self.claims.get(&claim_id).cloned() {
            if previous.tenant_id != claim.tenant_id {
                return Err(StoreError::Conflict(format!(
                    "claim_id '{}' already exists for tenant '{}'",
                    claim_id, previous.tenant_id
                )));
            }
            self.remove_claim_indexes(&previous);
        }
        self.add_claim_indexes(&claim);
        self.claims.insert(claim_id.clone(), claim);
        self.wal.push(WalEvent::ClaimUpsert(claim_id));
        Ok(())
    }

    fn apply_evidence(&mut self, evidence: Evidence) -> Result<(), StoreError> {
        // Write to disk BEFORE mutating in-memory state.
        if let Some(disk) = self.disk.as_ref() {
            // Read the current evidence blob (if any), append the
            // new evidence, write it back. The on-disk blob is
            // always a full Vec<Evidence> for the claim, so a single
            // append + replace keeps the on-disk state consistent.
            let mut current: Vec<Evidence> = disk
                .get_evidence_blob(&evidence.claim_id)
                .map_err(StoreError::Io)?
                .unwrap_or_default();
            current.push(evidence.clone());
            disk.put_evidence_blob(&evidence.claim_id, &current)
                .map_err(StoreError::Io)?;
        }
        self.apply_evidence_inner(evidence)
    }

    /// Apply a pre-built evidence blob to the in-memory state.
    /// No disk mirror. Used by the bulk-load path.
    pub(crate) fn apply_evidence_blob_for_load(
        &mut self,
        claim_id: &str,
        evidence: &[Evidence],
    ) -> Result<(), StoreError> {
        if !self.claims.contains_key(claim_id) {
            return Err(StoreError::MissingClaim(claim_id.to_string()));
        }
        let entry = self
            .evidence_by_claim
            .entry(claim_id.to_string())
            .or_default();
        for evd in evidence {
            entry.push(evd.clone());
        }
        Ok(())
    }

    fn apply_evidence_inner(&mut self, evidence: Evidence) -> Result<(), StoreError> {
        validate_evidence(&evidence)?;
        if !self.claims.contains_key(&evidence.claim_id) {
            return Err(StoreError::MissingClaim(evidence.claim_id));
        }
        self.evidence_by_claim
            .entry(evidence.claim_id.clone())
            .or_default()
            .push(evidence.clone());
        self.wal
            .push(WalEvent::EvidenceUpsert(evidence.evidence_id));
        Ok(())
    }

    fn apply_edge(&mut self, edge: ClaimEdge) -> Result<(), StoreError> {
        // Write to disk BEFORE mutating in-memory state.
        if let Some(disk) = self.disk.as_ref() {
            let mut current: Vec<ClaimEdge> = disk
                .get_edge_blob(&edge.from_claim_id)
                .map_err(StoreError::Io)?
                .unwrap_or_default();
            current.push(edge.clone());
            disk.put_edge_blob(&edge.from_claim_id, &current)
                .map_err(StoreError::Io)?;
        }
        self.apply_edge_inner(edge)
    }

    /// Apply a pre-built edge blob to the in-memory state. No disk
    /// mirror. Used by the bulk-load path.
    pub(crate) fn apply_edge_blob_for_load(
        &mut self,
        from: &str,
        edges: &[ClaimEdge],
    ) -> Result<(), StoreError> {
        if !self.claims.contains_key(from) {
            return Err(StoreError::MissingClaim(from.to_string()));
        }
        let entry = self.edges_by_claim.entry(from.to_string()).or_default();
        for edge in edges {
            entry.push(edge.clone());
        }
        Ok(())
    }

    fn apply_edge_inner(&mut self, edge: ClaimEdge) -> Result<(), StoreError> {
        validate_edge(&edge)?;
        if !self.claims.contains_key(&edge.from_claim_id) {
            return Err(StoreError::MissingClaim(edge.from_claim_id));
        }
        self.edges_by_claim
            .entry(edge.from_claim_id.clone())
            .or_default()
            .push(edge.clone());
        self.wal.push(WalEvent::EdgeUpsert(edge.edge_id));
        Ok(())
    }

    fn apply_claim_vector(&mut self, claim_id: &str, vector: Vec<f32>) -> Result<(), StoreError> {
        // Resolve the tenant and check the dimension match BEFORE
        // doing any disk I/O, so we don't write a half-bad state.
        let claim = self
            .claims
            .get(claim_id)
            .ok_or_else(|| StoreError::MissingClaim(claim_id.to_string()))?;
        let tenant_id = claim.tenant_id.clone();
        let new_dim_needed = match self.tenant_vector_dims.get(&tenant_id) {
            Some(existing_dim) if *existing_dim != vector.len() => {
                return Err(StoreError::InvalidVector(format!(
                    "vector dimension mismatch for tenant '{}': expected {}, got {}",
                    tenant_id, existing_dim, vector.len()
                )));
            }
            None => Some(vector.len()),
            _ => None,
        };

        // Write to disk BEFORE mutating in-memory state.
        if let Some(disk) = self.disk.as_ref() {
            disk.put_vector(claim_id, &vector).map_err(StoreError::Io)?;
            if let Some(dim) = new_dim_needed {
                disk.put_tenant_dim(&tenant_id, dim)
                    .map_err(StoreError::Io)?;
            }
        }
        self.apply_claim_vector_inner(claim_id, vector)
    }

    /// Apply a vector to the in-memory state (rebuilds the ANN
    /// index). No disk mirror. Used by the bulk-load path.
    pub(crate) fn apply_claim_vector_blob_for_load(
        &mut self,
        claim_id: &str,
        vector: Vec<f32>,
    ) -> Result<(), StoreError> {
        self.apply_claim_vector_inner(claim_id, vector)
    }

    fn apply_claim_vector_inner(
        &mut self,
        claim_id: &str,
        vector: Vec<f32>,
    ) -> Result<(), StoreError> {
        validate_vector(&vector)?;
        let claim = self
            .claims
            .get(claim_id)
            .ok_or_else(|| StoreError::MissingClaim(claim_id.to_string()))?;
        let tenant_id = claim.tenant_id.clone();
        match self.tenant_vector_dims.get(&tenant_id) {
            Some(existing_dim) if *existing_dim != vector.len() => {
                return Err(StoreError::InvalidVector(format!(
                    "vector dimension mismatch for tenant '{}': expected {}, got {}",
                    tenant_id,
                    existing_dim,
                    vector.len()
                )));
            }
            None => {
                self.tenant_vector_dims
                    .insert(tenant_id.clone(), vector.len());
            }
            _ => {}
        }

        if self.claim_vectors.contains_key(claim_id) {
            self.remove_vector_index_entry(&tenant_id, claim_id);
        }

        self.claim_vectors.insert(claim_id.to_string(), vector);
        let stored_vector =
            self.claim_vectors.get(claim_id).cloned().ok_or_else(|| {
                StoreError::InvalidVector("failed to store claim vector".to_string())
            })?;
        self.add_vector_index_entry(&tenant_id, claim_id, &stored_vector);
        self.wal
            .push(WalEvent::ClaimVectorUpsert(claim_id.to_string()));
        Ok(())
    }

    fn apply_batch_commit_record(&mut self, record: BatchCommitRecord) -> Result<(), StoreError> {
        // Compute the metadata the same way the inner function will,
        // so we can mirror to disk before mutating in-memory state.
        let payload_fingerprint =
            batch_commit_payload_fingerprint(record.batch_size, &record.claim_ids);
        if let Some(existing) = self.batch_commits.get(&record.commit_id) {
            if existing.payload_fingerprint != payload_fingerprint {
                return Err(StoreError::Conflict(format!(
                    "batch commit_id '{}' already exists with different payload (existing_fingerprint={}, incoming_fingerprint={})",
                    record.commit_id, existing.payload_fingerprint, payload_fingerprint
                )));
            }
            // Idempotent: the record is already in memory and on disk.
            return Ok(());
        }

        let metadata = BatchCommitMetadata {
            commit_id: record.commit_id.clone(),
            batch_size: record.batch_size,
            ts_unix_ms: record.ts_unix_ms,
            claim_ids: record.claim_ids.clone(),
            payload_fingerprint: payload_fingerprint.clone(),
        };
        if let Some(disk) = self.disk.as_ref() {
            disk.put_batch_commit(&metadata).map_err(StoreError::Io)?;
        }
        self.batch_commits.insert(record.commit_id.clone(), metadata);
        self.wal.push(WalEvent::BatchCommit(record.commit_id));
        Ok(())
    }

    /// Apply a batch-commit metadata to the in-memory state. No
    /// disk mirror. Used by the bulk-load path.
    pub(crate) fn apply_batch_commit_for_load(
        &mut self,
        commit: &BatchCommitMetadata,
    ) -> Result<(), StoreError> {
        if let Some(existing) = self.batch_commits.get(&commit.commit_id) {
            if existing.payload_fingerprint != commit.payload_fingerprint {
                return Err(StoreError::Conflict(format!(
                    "batch commit_id '{}' already exists with different payload (existing_fingerprint={}, incoming_fingerprint={})",
                    commit.commit_id, existing.payload_fingerprint, commit.payload_fingerprint
                )));
            }
            return Ok(());
        }
        self.batch_commits
            .insert(commit.commit_id.clone(), commit.clone());
        Ok(())
    }

    /// Apply a tenant vector dimension to the in-memory state. No
    /// disk mirror. Used by the bulk-load path.
    pub(crate) fn apply_tenant_dim_for_load(&mut self, tenant: &str, dim: usize) {
        self.tenant_vector_dims.insert(tenant.to_string(), dim);
    }

    /// Apply a tenant-claim set membership to the in-memory state.
    /// No disk mirror. Used by the bulk-load path.
    pub(crate) fn apply_tenant_claim_set_for_load(&mut self, tenant: &str, claim: &str) {
        self.tenant_claim_ids
            .entry(tenant.to_string())
            .or_default()
            .insert(claim.to_string());
    }

    fn add_vector_index_entry(&mut self, tenant_id: &str, claim_id: &str, vector: &[f32]) {
        let node_level = self.assign_ann_level(claim_id);
        {
            let graph = self
                .ann_vector_graphs
                .entry(tenant_id.to_string())
                .or_default();
            graph.node_levels.insert(claim_id.to_string(), node_level);
            for level in 0..=node_level {
                graph.levels[level].entry(claim_id.to_string()).or_default();
            }
            if graph.entry_point.is_none() {
                graph.entry_point = Some(claim_id.to_string());
                graph.entry_level = node_level;
            }
        }

        for level in (0..=node_level).rev() {
            let max_neighbors = self.ann_level_max_neighbors(level);
            let neighbor_ids =
                self.select_ann_neighbors(tenant_id, claim_id, vector, level, max_neighbors);
            for neighbor_id in neighbor_ids {
                self.connect_ann_nodes(tenant_id, level, claim_id, &neighbor_id, max_neighbors);
            }
        }

        if let Some(graph) = self.ann_vector_graphs.get_mut(tenant_id)
            && node_level > graph.entry_level
        {
            graph.entry_point = Some(claim_id.to_string());
            graph.entry_level = node_level;
        }
    }

    fn remove_vector_index_entry(&mut self, tenant_id: &str, claim_id: &str) {
        let mut remove_graph = false;
        if let Some(graph) = self.ann_vector_graphs.get_mut(tenant_id) {
            graph.node_levels.remove(claim_id);
            for level in &mut graph.levels {
                level.remove(claim_id);
                for neighbor_ids in level.values_mut() {
                    neighbor_ids.retain(|id| id != claim_id);
                }
            }
            if graph.entry_point.as_deref() == Some(claim_id) {
                if let Some((next_id, next_level)) = graph
                    .node_levels
                    .iter()
                    .max_by_key(|(_, level)| **level)
                    .map(|(id, level)| (id.clone(), *level))
                {
                    graph.entry_point = Some(next_id);
                    graph.entry_level = next_level;
                } else {
                    graph.entry_point = None;
                    graph.entry_level = 0;
                }
            }
            remove_graph = graph.node_levels.is_empty();
        }
        if remove_graph {
            self.ann_vector_graphs.remove(tenant_id);
        }
    }

    fn select_ann_neighbors(
        &self,
        tenant_id: &str,
        claim_id: &str,
        vector: &[f32],
        level: usize,
        max_neighbors: usize,
    ) -> Vec<String> {
        let mut scored: Vec<(String, f32)> = self
            .claim_vectors
            .iter()
            .filter_map(|(other_claim_id, other_vector)| {
                if other_claim_id == claim_id {
                    return None;
                }
                let claim = self.claims.get(other_claim_id)?;
                if claim.tenant_id != tenant_id {
                    return None;
                }
                if !self.ann_node_is_visible_at_level(tenant_id, other_claim_id, level) {
                    return None;
                }
                let sim = cosine_similarity(vector, other_vector)?;
                Some((other_claim_id.clone(), sim))
            })
            .collect();
        scored.sort_by(|a, b| b.1.total_cmp(&a.1));
        scored
            .into_iter()
            .take(max_neighbors)
            .map(|(id, _)| id)
            .collect()
    }

    fn connect_ann_nodes(
        &mut self,
        tenant_id: &str,
        level: usize,
        claim_id: &str,
        neighbor_id: &str,
        max_neighbors: usize,
    ) {
        if claim_id == neighbor_id {
            return;
        }
        if level >= ANN_GRAPH_LEVELS {
            return;
        }
        if let Some(graph) = self.ann_vector_graphs.get_mut(tenant_id) {
            if graph
                .node_levels
                .get(claim_id)
                .is_none_or(|node_level| *node_level < level)
                || graph
                    .node_levels
                    .get(neighbor_id)
                    .is_none_or(|node_level| *node_level < level)
            {
                return;
            }
            let claim_neighbors = graph.levels[level].entry(claim_id.to_string()).or_default();
            if !claim_neighbors.iter().any(|id| id == neighbor_id) {
                claim_neighbors.push(neighbor_id.to_string());
            }

            let neighbor_neighbors = graph.levels[level]
                .entry(neighbor_id.to_string())
                .or_default();
            if !neighbor_neighbors.iter().any(|id| id == claim_id) {
                neighbor_neighbors.push(claim_id.to_string());
            }
        }
        self.prune_ann_neighbors(tenant_id, level, claim_id, max_neighbors);
        self.prune_ann_neighbors(tenant_id, level, neighbor_id, max_neighbors);
    }

    fn prune_ann_neighbors(
        &mut self,
        tenant_id: &str,
        level: usize,
        claim_id: &str,
        max_neighbors: usize,
    ) {
        if level >= ANN_GRAPH_LEVELS {
            return;
        }
        let Some(node_vector) = self.claim_vectors.get(claim_id).cloned() else {
            return;
        };
        let Some(candidate_neighbors) = self
            .ann_vector_graphs
            .get(tenant_id)
            .and_then(|graph| graph.levels[level].get(claim_id))
            .cloned()
        else {
            return;
        };
        if candidate_neighbors.len() <= max_neighbors {
            return;
        }

        let mut scored: Vec<(String, f32)> = candidate_neighbors
            .into_iter()
            .filter_map(|neighbor_id| {
                let neighbor_vector = self.claim_vectors.get(&neighbor_id)?;
                let similarity = cosine_similarity(&node_vector, neighbor_vector)?;
                Some((neighbor_id, similarity))
            })
            .collect();
        scored.sort_by(|a, b| b.1.total_cmp(&a.1));
        let keep: Vec<String> = scored
            .into_iter()
            .take(max_neighbors)
            .map(|(neighbor_id, _)| neighbor_id)
            .collect();

        if let Some(graph) = self.ann_vector_graphs.get_mut(tenant_id)
            && let Some(neighbors) = graph.levels[level].get_mut(claim_id)
        {
            neighbors.clear();
            neighbors.extend(keep);
        }
    }

    fn ann_node_is_visible_at_level(&self, tenant_id: &str, claim_id: &str, level: usize) -> bool {
        self.ann_vector_graphs
            .get(tenant_id)
            .and_then(|graph| graph.node_levels.get(claim_id))
            .is_some_and(|node_level| *node_level >= level)
    }

    fn ann_level_max_neighbors(&self, level: usize) -> usize {
        if level == 0 {
            self.ann_tuning.max_neighbors_base.max(1)
        } else {
            self.ann_tuning.max_neighbors_upper.max(1)
        }
    }

    fn assign_ann_level(&self, claim_id: &str) -> usize {
        use std::hash::{Hash, Hasher};

        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        claim_id.hash(&mut hasher);
        let hash = hasher.finish();
        let level = (hash.trailing_zeros() as usize) / 4;
        level.min(ANN_GRAPH_LEVELS.saturating_sub(1))
    }

    fn add_claim_indexes(&mut self, claim: &Claim) {
        self.tenant_claim_ids
            .entry(claim.tenant_id.clone())
            .or_default()
            .insert(claim.claim_id.clone());

        let tokens = tokenize(&claim.canonical_text);
        self.claim_tokens
            .insert(claim.claim_id.clone(), tokens.clone());
        let token_index = self
            .inverted_index
            .entry(claim.tenant_id.clone())
            .or_default();
        let mut seen = HashSet::new();
        for token in tokens {
            if seen.insert(token.clone()) {
                token_index
                    .entry(token)
                    .or_default()
                    .insert(claim.claim_id.clone());
            }
        }

        let entity_index = self
            .entity_index
            .entry(claim.tenant_id.clone())
            .or_default();
        for entity in &claim.entities {
            let key = normalize_index_key(entity);
            if key.is_empty() {
                continue;
            }
            entity_index
                .entry(key)
                .or_default()
                .insert(claim.claim_id.clone());
        }

        let embedding_index = self
            .embedding_index
            .entry(claim.tenant_id.clone())
            .or_default();
        let mut seen_embedding = HashSet::new();
        for embedding_id in &claim.embedding_ids {
            let key = embedding_id.trim();
            if key.is_empty() || !seen_embedding.insert(key.to_string()) {
                continue;
            }
            embedding_index
                .entry(key.to_string())
                .or_default()
                .insert(claim.claim_id.clone());
        }

        if let Some(ts) = claim.event_time_unix {
            self.temporal_index
                .entry(claim.tenant_id.clone())
                .or_default()
                .entry(ts)
                .or_default()
                .insert(claim.claim_id.clone());
        }
    }

    fn remove_claim_indexes(&mut self, claim: &Claim) {
        if let Some(previous) = self.claim_vectors.remove(&claim.claim_id) {
            let _ = previous;
            self.remove_vector_index_entry(&claim.tenant_id, &claim.claim_id);
        }

        let mut drop_tenant_claim_ids = false;
        if let Some(ids) = self.tenant_claim_ids.get_mut(&claim.tenant_id) {
            ids.remove(&claim.claim_id);
            drop_tenant_claim_ids = ids.is_empty();
        }
        if drop_tenant_claim_ids {
            self.tenant_claim_ids.remove(&claim.tenant_id);
        }

        if let Some(tokens) = self.claim_tokens.remove(&claim.claim_id)
            && let Some(token_index) = self.inverted_index.get_mut(&claim.tenant_id)
        {
            let mut seen = HashSet::new();
            let mut remove_tokens = Vec::new();
            for token in tokens {
                if !seen.insert(token.clone()) {
                    continue;
                }
                if let Some(ids) = token_index.get_mut(&token) {
                    ids.remove(&claim.claim_id);
                    if ids.is_empty() {
                        remove_tokens.push(token);
                    }
                }
            }
            for token in remove_tokens {
                token_index.remove(&token);
            }
        }
        if self
            .inverted_index
            .get(&claim.tenant_id)
            .is_some_and(|index| index.is_empty())
        {
            self.inverted_index.remove(&claim.tenant_id);
        }

        let mut remove_entity_index = false;
        if let Some(entity_index) = self.entity_index.get_mut(&claim.tenant_id) {
            let mut remove_keys = Vec::new();
            for entity in &claim.entities {
                let key = normalize_index_key(entity);
                if let Some(ids) = entity_index.get_mut(&key) {
                    ids.remove(&claim.claim_id);
                    if ids.is_empty() {
                        remove_keys.push(key);
                    }
                }
            }
            for key in remove_keys {
                entity_index.remove(&key);
            }
            remove_entity_index = entity_index.is_empty();
        }
        if remove_entity_index {
            self.entity_index.remove(&claim.tenant_id);
        }

        let mut remove_embedding_index = false;
        if let Some(embedding_index) = self.embedding_index.get_mut(&claim.tenant_id) {
            let mut remove_keys = Vec::new();
            let mut seen_embedding = HashSet::new();
            for embedding_id in &claim.embedding_ids {
                let key = embedding_id.trim();
                if key.is_empty() || !seen_embedding.insert(key.to_string()) {
                    continue;
                }
                if let Some(ids) = embedding_index.get_mut(key) {
                    ids.remove(&claim.claim_id);
                    if ids.is_empty() {
                        remove_keys.push(key.to_string());
                    }
                }
            }
            for key in remove_keys {
                embedding_index.remove(&key);
            }
            remove_embedding_index = embedding_index.is_empty();
        }
        if remove_embedding_index {
            self.embedding_index.remove(&claim.tenant_id);
        }

        let mut remove_temporal_index = false;
        if let Some(ts) = claim.event_time_unix
            && let Some(timeline) = self.temporal_index.get_mut(&claim.tenant_id)
        {
            let mut drop_ts = false;
            if let Some(ids) = timeline.get_mut(&ts) {
                ids.remove(&claim.claim_id);
                drop_ts = ids.is_empty();
            }
            if drop_ts {
                timeline.remove(&ts);
            }
            remove_temporal_index = timeline.is_empty();
        }
        if remove_temporal_index {
            self.temporal_index.remove(&claim.tenant_id);
        }

        let has_remaining_vectors_for_tenant = self.claim_vectors.keys().any(|claim_id| {
            self.claims
                .get(claim_id)
                .is_some_and(|stored_claim| stored_claim.tenant_id == claim.tenant_id)
        });
        if !has_remaining_vectors_for_tenant {
            self.tenant_vector_dims.remove(&claim.tenant_id);
        }
    }
}

fn parse_vector_backend_preference() -> VectorBackendPreference {
    match std::env::var(VECTOR_BACKEND_ENV)
        .ok()
        .map(|raw| raw.trim().to_ascii_lowercase())
        .as_deref()
    {
        Some("cpu") => VectorBackendPreference::Cpu,
        Some("gpu") => VectorBackendPreference::Gpu,
        _ => VectorBackendPreference::Auto,
    }
}

fn resolve_vector_backend_runtime(preference: VectorBackendPreference) -> VectorBackendRuntime {
    match preference {
        VectorBackendPreference::Cpu => VectorBackendRuntime::Cpu,
        VectorBackendPreference::Gpu => resolve_gpu_backend_runtime(true),
        VectorBackendPreference::Auto => resolve_gpu_backend_runtime(false),
    }
}

#[cfg(feature = "gpu-backend")]
fn resolve_gpu_backend_runtime(explicit_gpu_required: bool) -> VectorBackendRuntime {
    if gpu_backend_engine().is_some() {
        VectorBackendRuntime::Gpu
    } else if explicit_gpu_required {
        VectorBackendRuntime::CpuFallbackUnavailable
    } else {
        VectorBackendRuntime::Cpu
    }
}

#[cfg(not(feature = "gpu-backend"))]
fn resolve_gpu_backend_runtime(explicit_gpu_required: bool) -> VectorBackendRuntime {
    if explicit_gpu_required {
        VectorBackendRuntime::CpuFallbackFeatureDisabled
    } else {
        VectorBackendRuntime::Cpu
    }
}

fn score_query_candidate_vectors_cpu(
    query_vector: &[f32],
    candidate_vectors: &[(String, &[f32])],
) -> Vec<(String, f32)> {
    candidate_vectors
        .iter()
        .filter_map(|(claim_id, candidate_vector)| {
            let score = cosine_similarity(query_vector, candidate_vector)?;
            Some((claim_id.clone(), score))
        })
        .collect()
}

fn value_in_time_range(value: i64, from_unix: Option<i64>, to_unix: Option<i64>) -> bool {
    if let Some(from) = from_unix
        && value < from
    {
        return false;
    }
    if let Some(to) = to_unix
        && value > to
    {
        return false;
    }
    true
}

fn time_windows_overlap(
    window_start: Option<i64>,
    window_end: Option<i64>,
    query_from: Option<i64>,
    query_to: Option<i64>,
) -> bool {
    let start = window_start.unwrap_or(i64::MIN);
    let end = window_end.unwrap_or(i64::MAX);
    let query_start = query_from.unwrap_or(i64::MIN);
    let query_end = query_to.unwrap_or(i64::MAX);
    start <= query_end && end >= query_start
}

fn claim_matches_time_range(claim: &Claim, from_unix: Option<i64>, to_unix: Option<i64>) -> bool {
    if from_unix.is_none() && to_unix.is_none() {
        return true;
    }

    let event_match = claim
        .event_time_unix
        .is_some_and(|value| value_in_time_range(value, from_unix, to_unix));
    let has_valid_window = claim.valid_from.is_some() || claim.valid_to.is_some();
    let validity_match = has_valid_window
        && time_windows_overlap(claim.valid_from, claim.valid_to, from_unix, to_unix);

    match (claim.event_time_unix.is_some(), has_valid_window) {
        (true, true) => event_match && validity_match,
        (true, false) => event_match,
        (false, true) => validity_match,
        (false, false) => false,
    }
}

fn normalize_index_key(value: &str) -> String {
    value.trim().to_ascii_lowercase()
}

fn validate_vector(vector: &[f32]) -> Result<(), StoreError> {
    if vector.is_empty() {
        return Err(StoreError::InvalidVector(
            "vector cannot be empty".to_string(),
        ));
    }
    if !vector.iter().all(|v| v.is_finite()) {
        return Err(StoreError::InvalidVector(
            "vector values must be finite".to_string(),
        ));
    }
    Ok(())
}

#[cfg(feature = "gpu-backend")]
const GPU_COSINE_SHADER: &str = r#"
struct CosineParams {
    dimension: u32,
    candidate_count: u32,
    _pad0: u32,
    _pad1: u32,
};

@group(0) @binding(0)
var<storage, read> query: array<f32>;

@group(0) @binding(1)
var<storage, read> candidates: array<f32>;

@group(0) @binding(2)
var<storage, read_write> output_scores: array<f32>;

@group(0) @binding(3)
var<uniform> params: CosineParams;

@compute @workgroup_size(64)
fn main(@builtin(global_invocation_id) gid: vec3<u32>) {
    let candidate_index = gid.x;
    if (candidate_index >= params.candidate_count) {
        return;
    }

    let base = candidate_index * params.dimension;
    var dot: f32 = 0.0;
    var norm_query: f32 = 0.0;
    var norm_candidate: f32 = 0.0;
    var i: u32 = 0u;

    loop {
        if (i >= params.dimension) {
            break;
        }
        let q = query[i];
        let c = candidates[base + i];
        dot = dot + (q * c);
        norm_query = norm_query + (q * q);
        norm_candidate = norm_candidate + (c * c);
        i = i + 1u;
    }

    let denom = sqrt(norm_query) * sqrt(norm_candidate);
    if (denom <= 0.0000001) {
        output_scores[candidate_index] = 0.0;
    } else {
        output_scores[candidate_index] = dot / denom;
    }
}
"#;

#[cfg(feature = "gpu-backend")]
#[repr(C)]
#[derive(Clone, Copy, bytemuck::Pod, bytemuck::Zeroable)]
struct GpuCosineParams {
    dimension: u32,
    candidate_count: u32,
    pad0: u32,
    pad1: u32,
}

fn cosine_similarity(a: &[f32], b: &[f32]) -> Option<f32> {
    if a.len() != b.len() || a.is_empty() {
        return None;
    }
    let mut dot = 0.0f32;
    let mut norm_a = 0.0f32;
    let mut norm_b = 0.0f32;
    for i in 0..a.len() {
        dot += a[i] * b[i];
        norm_a += a[i] * a[i];
        norm_b += b[i] * b[i];
    }
    let denom = norm_a.sqrt() * norm_b.sqrt();
    if denom <= f32::EPSILON {
        None
    } else {
        Some(dot / denom)
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use schema::{Claim, ClaimEdge, ClaimType, Relation, RetrievalRequest, Stance, StanceMode};
    use std::path::PathBuf;
    use std::time::Duration;
    use std::{
        fs::{read_to_string, remove_file},
        sync::atomic::{AtomicU64, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    fn claim_for_tenant(id: &str, text: &str, tenant_id: &str) -> Claim {
        Claim {
            claim_id: id.to_string(),
            tenant_id: tenant_id.to_string(),
            canonical_text: text.to_string(),
            confidence: 0.9,
            event_time_unix: None,
            entities: vec![],
            embedding_ids: vec![],
            claim_type: None,
            valid_from: None,
            valid_to: None,
            created_at: None,
            updated_at: None,
        }
    }

    fn claim(id: &str, text: &str) -> Claim {
        claim_for_tenant(id, text, "tenant-a")
    }

    fn temp_wal_path() -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let mut path = std::env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock should be valid")
            .as_nanos();
        let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
        path.push(format!("eme-wal-{}-{nanos}-{seq}.log", std::process::id()));
        path
    }

    fn cleanup_persistence_files(wal: &FileWal) {
        let _ = remove_file(wal.path());
        let _ = remove_file(wal.snapshot_path());
    }

    struct EnvVarGuard {
        key: &'static str,
        previous: Option<String>,
    }

    impl EnvVarGuard {
        fn set(key: &'static str, value: &str) -> Self {
            let previous = std::env::var(key).ok();
            #[allow(unused_unsafe)]
            unsafe {
                std::env::set_var(key, value);
            }
            Self { key, previous }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            match &self.previous {
                Some(value) => {
                    #[allow(unused_unsafe)]
                    unsafe {
                        std::env::set_var(self.key, value);
                    }
                }
                None => {
                    #[allow(unused_unsafe)]
                    unsafe {
                        std::env::remove_var(self.key);
                    }
                }
            }
        }
    }

    #[test]
    fn ingest_bundle_writes_claim_and_wal_entries() {
        let mut store = InMemoryStore::new();
        let claim = claim("c1", "Company X acquired Company Y");
        let evidence = vec![Evidence {
            evidence_id: "e1".into(),
            claim_id: "c1".into(),
            source_id: "doc-1".into(),
            stance: Stance::Supports,
            source_quality: 0.9,
            chunk_id: None,
            span_start: None,
            span_end: None,
            doc_id: None,
            extraction_model: None,
            ingested_at: None,
        }];
        let edges = vec![ClaimEdge {
            edge_id: "edge1".into(),
            from_claim_id: "c1".into(),
            to_claim_id: "c2".into(),
            relation: Relation::Supports,
            strength: 0.6,
            reason_codes: vec![],
            created_at: None,
        }];

        store.ingest_bundle(claim, evidence, edges).unwrap();
        assert_eq!(store.claims_len(), 1);
        assert_eq!(store.wal_len(), 3);
    }

    #[test]
    fn retrieve_ranks_high_overlap_claim_first() {
        let mut store = InMemoryStore::new();

        store
            .ingest_bundle(
                claim("c1", "Company X acquired Company Y"),
                vec![Evidence {
                    evidence_id: "e1".into(),
                    claim_id: "c1".into(),
                    source_id: "doc-1".into(),
                    stance: Stance::Supports,
                    source_quality: 0.9,
                    chunk_id: None,
                    span_start: None,
                    span_end: None,
                    doc_id: None,
                    extraction_model: None,
                    ingested_at: None,
                }],
                vec![],
            )
            .unwrap();

        store
            .ingest_bundle(
                claim("c2", "Company Z opened a new office"),
                vec![Evidence {
                    evidence_id: "e2".into(),
                    claim_id: "c2".into(),
                    source_id: "doc-2".into(),
                    stance: Stance::Supports,
                    source_quality: 0.8,
                    chunk_id: None,
                    span_start: None,
                    span_end: None,
                    doc_id: None,
                    extraction_model: None,
                    ingested_at: None,
                }],
                vec![],
            )
            .unwrap();

        let results = store.retrieve(&RetrievalRequest {
            tenant_id: "tenant-a".into(),
            query: "Did Company X acquire Company Y?".into(),
            top_k: 2,
            stance_mode: StanceMode::Balanced,
        });

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].claim_id, "c1");
    }

    #[test]
    fn retrieve_with_time_range_filters_out_of_window_claims() {
        let mut store = InMemoryStore::new();

        store
            .ingest_bundle(
                Claim {
                    claim_id: "c-old".into(),
                    tenant_id: "tenant-a".into(),
                    canonical_text: "Project Orion launch milestone".into(),
                    confidence: 0.9,
                    event_time_unix: Some(100),
                    entities: vec![],
                    embedding_ids: vec![],
                    claim_type: None,
                    valid_from: None,
                    valid_to: None,
                    created_at: None,
                    updated_at: None,
                },
                vec![Evidence {
                    evidence_id: "e-old".into(),
                    claim_id: "c-old".into(),
                    source_id: "doc-old".into(),
                    stance: Stance::Supports,
                    source_quality: 0.9,
                    chunk_id: None,
                    span_start: None,
                    span_end: None,
                    doc_id: None,
                    extraction_model: None,
                    ingested_at: None,
                }],
                vec![],
            )
            .unwrap();
        store
            .ingest_bundle(
                Claim {
                    claim_id: "c-new".into(),
                    tenant_id: "tenant-a".into(),
                    canonical_text: "Project Orion launch milestone".into(),
                    confidence: 0.9,
                    event_time_unix: Some(200),
                    entities: vec![],
                    embedding_ids: vec![],
                    claim_type: None,
                    valid_from: None,
                    valid_to: None,
                    created_at: None,
                    updated_at: None,
                },
                vec![Evidence {
                    evidence_id: "e-new".into(),
                    claim_id: "c-new".into(),
                    source_id: "doc-new".into(),
                    stance: Stance::Supports,
                    source_quality: 0.9,
                    chunk_id: None,
                    span_start: None,
                    span_end: None,
                    doc_id: None,
                    extraction_model: None,
                    ingested_at: None,
                }],
                vec![],
            )
            .unwrap();
        store
            .ingest_bundle(
                Claim {
                    claim_id: "c-no-time".into(),
                    tenant_id: "tenant-a".into(),
                    canonical_text: "Project Orion launch milestone".into(),
                    confidence: 0.9,
                    event_time_unix: None,
                    entities: vec![],
                    embedding_ids: vec![],
                    claim_type: None,
                    valid_from: None,
                    valid_to: None,
                    created_at: None,
                    updated_at: None,
                },
                vec![Evidence {
                    evidence_id: "e-no-time".into(),
                    claim_id: "c-no-time".into(),
                    source_id: "doc-no-time".into(),
                    stance: Stance::Supports,
                    source_quality: 0.9,
                    chunk_id: None,
                    span_start: None,
                    span_end: None,
                    doc_id: None,
                    extraction_model: None,
                    ingested_at: None,
                }],
                vec![],
            )
            .unwrap();

        let req = RetrievalRequest {
            tenant_id: "tenant-a".into(),
            query: "project orion launch milestone".into(),
            top_k: 5,
            stance_mode: StanceMode::Balanced,
        };
        let results = store.retrieve_with_time_range(&req, Some(150), Some(250));

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].claim_id, "c-new");
    }

    #[test]
    fn retrieve_with_time_range_uses_validity_window_when_event_time_missing() {
        let mut store = InMemoryStore::new();
        store
            .ingest_bundle(
                Claim {
                    claim_id: "c-window-hit".into(),
                    tenant_id: "tenant-a".into(),
                    canonical_text: "Claim with active validity window".into(),
                    confidence: 0.9,
                    event_time_unix: None,
                    entities: vec![],
                    embedding_ids: vec![],
                    claim_type: Some(ClaimType::Temporal),
                    valid_from: Some(120),
                    valid_to: Some(260),
                    created_at: Some(10),
                    updated_at: Some(20),
                },
                vec![Evidence {
                    evidence_id: "e-window-hit".into(),
                    claim_id: "c-window-hit".into(),
                    source_id: "doc-window-hit".into(),
                    stance: Stance::Supports,
                    source_quality: 0.9,
                    chunk_id: None,
                    span_start: None,
                    span_end: None,
                    doc_id: None,
                    extraction_model: None,
                    ingested_at: None,
                }],
                vec![],
            )
            .unwrap();
        store
            .ingest_bundle(
                Claim {
                    claim_id: "c-window-miss".into(),
                    tenant_id: "tenant-a".into(),
                    canonical_text: "Claim with non-overlapping validity window".into(),
                    confidence: 0.9,
                    event_time_unix: None,
                    entities: vec![],
                    embedding_ids: vec![],
                    claim_type: Some(ClaimType::Temporal),
                    valid_from: Some(400),
                    valid_to: Some(500),
                    created_at: Some(11),
                    updated_at: Some(21),
                },
                vec![Evidence {
                    evidence_id: "e-window-miss".into(),
                    claim_id: "c-window-miss".into(),
                    source_id: "doc-window-miss".into(),
                    stance: Stance::Supports,
                    source_quality: 0.9,
                    chunk_id: None,
                    span_start: None,
                    span_end: None,
                    doc_id: None,
                    extraction_model: None,
                    ingested_at: None,
                }],
                vec![],
            )
            .unwrap();

        let req = RetrievalRequest {
            tenant_id: "tenant-a".into(),
            query: "claim validity window".into(),
            top_k: 5,
            stance_mode: StanceMode::Balanced,
        };
        let results = store.retrieve_with_time_range(&req, Some(150), Some(240));
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].claim_id, "c-window-hit");
    }

    #[test]
    fn retrieve_with_time_range_requires_event_and_validity_match_when_both_present() {
        let mut store = InMemoryStore::new();
        store
            .ingest_bundle(
                Claim {
                    claim_id: "c-both-miss".into(),
                    tenant_id: "tenant-a".into(),
                    canonical_text: "Claim where event and validity disagree".into(),
                    confidence: 0.9,
                    event_time_unix: Some(100),
                    entities: vec![],
                    embedding_ids: vec![],
                    claim_type: Some(ClaimType::Temporal),
                    valid_from: Some(140),
                    valid_to: Some(260),
                    created_at: Some(12),
                    updated_at: Some(22),
                },
                vec![Evidence {
                    evidence_id: "e-both-miss".into(),
                    claim_id: "c-both-miss".into(),
                    source_id: "doc-both-miss".into(),
                    stance: Stance::Supports,
                    source_quality: 0.9,
                    chunk_id: None,
                    span_start: None,
                    span_end: None,
                    doc_id: None,
                    extraction_model: None,
                    ingested_at: None,
                }],
                vec![],
            )
            .unwrap();
        store
            .ingest_bundle(
                Claim {
                    claim_id: "c-both-hit".into(),
                    tenant_id: "tenant-a".into(),
                    canonical_text: "Claim where event and validity align".into(),
                    confidence: 0.9,
                    event_time_unix: Some(200),
                    entities: vec![],
                    embedding_ids: vec![],
                    claim_type: Some(ClaimType::Temporal),
                    valid_from: Some(140),
                    valid_to: Some(260),
                    created_at: Some(13),
                    updated_at: Some(23),
                },
                vec![Evidence {
                    evidence_id: "e-both-hit".into(),
                    claim_id: "c-both-hit".into(),
                    source_id: "doc-both-hit".into(),
                    stance: Stance::Supports,
                    source_quality: 0.9,
                    chunk_id: None,
                    span_start: None,
                    span_end: None,
                    doc_id: None,
                    extraction_model: None,
                    ingested_at: None,
                }],
                vec![],
            )
            .unwrap();

        let req = RetrievalRequest {
            tenant_id: "tenant-a".into(),
            query: "claim event validity".into(),
            top_k: 5,
            stance_mode: StanceMode::Balanced,
        };
        let results = store.retrieve_with_time_range(&req, Some(150), Some(240));
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].claim_id, "c-both-hit");
    }

    #[test]
    fn support_only_filters_claims_with_more_contradictions() {
        let mut store = InMemoryStore::new();
        store
            .ingest_bundle(
                claim("c1", "Company X acquired Company Y"),
                vec![
                    Evidence {
                        evidence_id: "e1".into(),
                        claim_id: "c1".into(),
                        source_id: "doc-1".into(),
                        stance: Stance::Supports,
                        source_quality: 0.9,
                        chunk_id: None,
                        span_start: None,
                        span_end: None,
                        doc_id: None,
                        extraction_model: None,
                        ingested_at: None,
                    },
                    Evidence {
                        evidence_id: "e2".into(),
                        claim_id: "c1".into(),
                        source_id: "doc-2".into(),
                        stance: Stance::Contradicts,
                        source_quality: 0.8,
                        chunk_id: None,
                        span_start: None,
                        span_end: None,
                        doc_id: None,
                        extraction_model: None,
                        ingested_at: None,
                    },
                    Evidence {
                        evidence_id: "e3".into(),
                        claim_id: "c1".into(),
                        source_id: "doc-3".into(),
                        stance: Stance::Contradicts,
                        source_quality: 0.8,
                        chunk_id: None,
                        span_start: None,
                        span_end: None,
                        doc_id: None,
                        extraction_model: None,
                        ingested_at: None,
                    },
                ],
                vec![],
            )
            .unwrap();

        let support_only_results = store.retrieve(&RetrievalRequest {
            tenant_id: "tenant-a".into(),
            query: "Company X acquired Company Y".into(),
            top_k: 10,
            stance_mode: StanceMode::SupportOnly,
        });
        assert!(support_only_results.is_empty());
    }

    #[test]
    fn persistent_wal_replay_restores_claims_and_retrieval() {
        let wal_path = temp_wal_path();
        let mut wal = FileWal::open(&wal_path).unwrap();
        let mut store = InMemoryStore::new();

        store
            .ingest_bundle_persistent(
                &mut wal,
                claim("c1", "Company X acquired Company Y"),
                vec![Evidence {
                    evidence_id: "e1".into(),
                    claim_id: "c1".into(),
                    source_id: "doc-1".into(),
                    stance: Stance::Supports,
                    source_quality: 0.9,
                    chunk_id: None,
                    span_start: None,
                    span_end: None,
                    doc_id: None,
                    extraction_model: None,
                    ingested_at: None,
                }],
                vec![],
            )
            .unwrap();
        drop(store);

        let replayed = InMemoryStore::load_from_wal(&wal).unwrap();
        assert_eq!(replayed.claims_len(), 1);
        let results = replayed.retrieve(&RetrievalRequest {
            tenant_id: "tenant-a".into(),
            query: "company x acquired company y".into(),
            top_k: 1,
            stance_mode: StanceMode::Balanced,
        });
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].claim_id, "c1");

        cleanup_persistence_files(&wal);
    }

    #[test]
    fn load_from_wal_with_stats_reports_replay_breakdown() {
        let wal_path = temp_wal_path();
        let mut wal = FileWal::open(&wal_path).unwrap();
        let mut store = InMemoryStore::new();

        store
            .ingest_bundle_persistent(
                &mut wal,
                claim("c1", "Company X acquired Company Y"),
                vec![Evidence {
                    evidence_id: "e1".into(),
                    claim_id: "c1".into(),
                    source_id: "doc-1".into(),
                    stance: Stance::Supports,
                    source_quality: 0.9,
                    chunk_id: None,
                    span_start: None,
                    span_end: None,
                    doc_id: None,
                    extraction_model: None,
                    ingested_at: None,
                }],
                vec![],
            )
            .unwrap();

        assert_eq!(wal.wal_record_count().unwrap(), 2);
        drop(wal);
        let reopened = FileWal::open(&wal_path).unwrap();
        assert_eq!(reopened.wal_record_count().unwrap(), 2);

        let (replayed, stats) = InMemoryStore::load_from_wal_with_stats(&reopened).unwrap();
        assert_eq!(replayed.claims_len(), 1);
        assert_eq!(stats.replay.snapshot_records, 0);
        assert_eq!(stats.replay.wal_records, 2);
        assert_eq!(stats.claims_loaded, 1);
        assert_eq!(stats.evidence_loaded, 1);
        assert_eq!(stats.edges_loaded, 0);

        cleanup_persistence_files(&reopened);
    }

    #[test]
    fn wal_escaping_round_trip_handles_tabs_and_newlines() {
        let wal_path = temp_wal_path();
        let mut wal = FileWal::open(&wal_path).unwrap();
        let mut store = InMemoryStore::new();

        store
            .ingest_bundle_persistent(
                &mut wal,
                claim("c-tab", "Company X\tacquired\nCompany Y"),
                vec![Evidence {
                    evidence_id: "e-tab".into(),
                    claim_id: "c-tab".into(),
                    source_id: "doc\tline\nbreak".into(),
                    stance: Stance::Supports,
                    source_quality: 0.9,
                    chunk_id: None,
                    span_start: None,
                    span_end: None,
                    doc_id: None,
                    extraction_model: None,
                    ingested_at: None,
                }],
                vec![],
            )
            .unwrap();

        let replayed = InMemoryStore::load_from_wal(&wal).unwrap();
        let results = replayed.retrieve(&RetrievalRequest {
            tenant_id: "tenant-a".into(),
            query: "company x acquired company y".into(),
            top_k: 1,
            stance_mode: StanceMode::Balanced,
        });
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].claim_id, "c-tab");

        cleanup_persistence_files(&wal);
    }

    #[test]
    fn wal_open_with_sync_every_records_clamps_to_minimum_one() {
        let wal_path = temp_wal_path();
        let wal = FileWal::open_with_sync_every_records(&wal_path, 0).unwrap();
        assert_eq!(wal.sync_every_records(), 1);
        cleanup_persistence_files(&wal);
    }

    #[test]
    fn wal_batch_sync_triggers_on_configured_interval() {
        let wal_path = temp_wal_path();
        let mut wal = FileWal::open_with_sync_every_records(&wal_path, 2).unwrap();
        let mut store = InMemoryStore::new();
        store
            .ingest_bundle_persistent(
                &mut wal,
                claim("c-batch-1", "Batch sync one"),
                vec![],
                vec![],
            )
            .unwrap();
        assert_eq!(wal.unsynced_records, 1);

        store
            .ingest_bundle_persistent(
                &mut wal,
                claim("c-batch-2", "Batch sync two"),
                vec![],
                vec![],
            )
            .unwrap();
        assert_eq!(wal.unsynced_records, 0);
        assert_eq!(wal.wal_record_count().unwrap(), 2);

        cleanup_persistence_files(&wal);
    }

    #[test]
    fn wal_append_buffer_batches_disk_writes_until_threshold() {
        let wal_path = temp_wal_path();
        let mut wal = FileWal::open_with_policy(
            &wal_path,
            WalWritePolicy {
                sync_every_records: 10,
                append_buffer_max_records: 3,
                sync_interval: None,
                background_flush_only: false,
            },
        )
        .unwrap();
        let mut store = InMemoryStore::new();

        store
            .ingest_bundle_persistent(&mut wal, claim("c-buf-1", "Buffer one"), vec![], vec![])
            .unwrap();
        store
            .ingest_bundle_persistent(&mut wal, claim("c-buf-2", "Buffer two"), vec![], vec![])
            .unwrap();
        assert_eq!(wal.buffered_record_count(), 2);
        assert_eq!(std::fs::metadata(wal.path()).unwrap().len(), 0);

        store
            .ingest_bundle_persistent(&mut wal, claim("c-buf-3", "Buffer three"), vec![], vec![])
            .unwrap();
        assert_eq!(wal.buffered_record_count(), 0);
        assert!(std::fs::metadata(wal.path()).unwrap().len() > 0);

        cleanup_persistence_files(&wal);
    }

    #[test]
    fn wal_interval_flush_hook_syncs_pending_records() {
        let wal_path = temp_wal_path();
        let mut wal = FileWal::open_with_policy(
            &wal_path,
            WalWritePolicy {
                sync_every_records: 100,
                append_buffer_max_records: 100,
                sync_interval: Some(Duration::from_millis(1)),
                background_flush_only: false,
            },
        )
        .unwrap();
        let mut store = InMemoryStore::new();
        store
            .ingest_bundle_persistent(
                &mut wal,
                claim("c-interval", "Interval flush"),
                vec![],
                vec![],
            )
            .unwrap();
        assert_eq!(wal.unsynced_record_count(), 1);
        std::thread::sleep(Duration::from_millis(2));
        let flushed = wal.flush_pending_sync_if_interval_elapsed().unwrap();
        assert!(flushed);
        assert_eq!(wal.unsynced_record_count(), 0);
        assert_eq!(wal.buffered_record_count(), 0);

        cleanup_persistence_files(&wal);
    }

    #[test]
    fn wal_unsynced_flush_hook_syncs_without_interval_policy() {
        let wal_path = temp_wal_path();
        let mut wal = FileWal::open_with_policy(
            &wal_path,
            WalWritePolicy {
                sync_every_records: 100,
                append_buffer_max_records: 100,
                sync_interval: None,
                background_flush_only: false,
            },
        )
        .unwrap();
        let mut store = InMemoryStore::new();

        store
            .ingest_bundle_persistent(
                &mut wal,
                claim("c-unsynced", "Unsynced flush"),
                vec![],
                vec![],
            )
            .unwrap();
        assert_eq!(wal.unsynced_record_count(), 1);

        let flushed = wal.flush_pending_sync_if_unsynced().unwrap();
        assert!(flushed);
        assert_eq!(wal.unsynced_record_count(), 0);
        assert_eq!(wal.buffered_record_count(), 0);

        cleanup_persistence_files(&wal);
    }

    #[test]
    fn wal_background_flush_only_defers_flush_until_explicit_tick() {
        let wal_path = temp_wal_path();
        let mut wal = FileWal::open_with_policy(
            &wal_path,
            WalWritePolicy {
                sync_every_records: 1,
                append_buffer_max_records: 1,
                sync_interval: None,
                background_flush_only: true,
            },
        )
        .unwrap();
        let mut store = InMemoryStore::new();

        store
            .ingest_bundle_persistent(
                &mut wal,
                claim("c-bg-1", "Background flush one"),
                vec![],
                vec![],
            )
            .unwrap();
        store
            .ingest_bundle_persistent(
                &mut wal,
                claim("c-bg-2", "Background flush two"),
                vec![],
                vec![],
            )
            .unwrap();

        assert!(wal.background_flush_only());
        assert_eq!(wal.unsynced_record_count(), 2);
        assert_eq!(wal.buffered_record_count(), 2);
        assert_eq!(std::fs::metadata(wal.path()).unwrap().len(), 0);

        let flushed = wal.flush_pending_sync_if_unsynced().unwrap();
        assert!(flushed);
        assert_eq!(wal.unsynced_record_count(), 0);
        assert_eq!(wal.buffered_record_count(), 0);
        assert!(std::fs::metadata(wal.path()).unwrap().len() > 0);

        cleanup_persistence_files(&wal);
    }

    #[test]
    fn wal_round_trip_preserves_claim_and_evidence_metadata() {
        let wal_path = temp_wal_path();
        let mut wal = FileWal::open(&wal_path).unwrap();
        let mut store = InMemoryStore::new();

        store
            .ingest_bundle_persistent(
                &mut wal,
                Claim {
                    claim_id: "c-meta".into(),
                    tenant_id: "tenant-a".into(),
                    canonical_text: "Acquisition timeline update".into(),
                    confidence: 0.9,
                    event_time_unix: Some(200),
                    entities: vec!["Company X".into(), "Company Y".into()],
                    embedding_ids: vec!["emb://v1/42".into()],
                    claim_type: Some(ClaimType::Temporal),
                    valid_from: Some(180),
                    valid_to: Some(260),
                    created_at: Some(1_771_620_000_000),
                    updated_at: Some(1_771_620_100_000),
                },
                vec![Evidence {
                    evidence_id: "e-meta".into(),
                    claim_id: "c-meta".into(),
                    source_id: "source://doc-meta".into(),
                    stance: Stance::Supports,
                    source_quality: 0.9,
                    chunk_id: Some("chunk-17".into()),
                    span_start: Some(12),
                    span_end: Some(48),
                    doc_id: Some("doc://meta".into()),
                    extraction_model: Some("extractor-v5".into()),
                    ingested_at: Some(1_771_620_200_000),
                }],
                vec![],
            )
            .unwrap();

        let replayed = InMemoryStore::load_from_wal(&wal).unwrap();
        let claim = replayed
            .claims
            .get("c-meta")
            .expect("claim metadata should be replayed");
        assert_eq!(
            claim.entities,
            vec!["Company X".to_string(), "Company Y".to_string()]
        );
        assert_eq!(claim.embedding_ids, vec!["emb://v1/42".to_string()]);
        assert_eq!(claim.claim_type, Some(ClaimType::Temporal));
        assert_eq!(claim.valid_from, Some(180));
        assert_eq!(claim.valid_to, Some(260));
        assert_eq!(claim.created_at, Some(1_771_620_000_000));
        assert_eq!(claim.updated_at, Some(1_771_620_100_000));

        let evidence = replayed
            .evidence_by_claim
            .get("c-meta")
            .and_then(|items| items.first())
            .expect("evidence metadata should be replayed");
        assert_eq!(evidence.chunk_id.as_deref(), Some("chunk-17"));
        assert_eq!(evidence.span_start, Some(12));
        assert_eq!(evidence.span_end, Some(48));
        assert_eq!(evidence.doc_id.as_deref(), Some("doc://meta"));
        assert_eq!(evidence.extraction_model.as_deref(), Some("extractor-v5"));
        assert_eq!(evidence.ingested_at, Some(1_771_620_200_000));

        cleanup_persistence_files(&wal);
    }

    #[test]
    fn wal_replay_accepts_legacy_record_shape_without_metadata_fields() {
        let wal_path = temp_wal_path();
        std::fs::write(
            &wal_path,
            "C\tc1\ttenant-a\tLegacy claim\t0.9\tnull\nE\te1\tc1\tsource://legacy\tsupports\t0.8\n",
        )
        .unwrap();
        let wal = FileWal::open(&wal_path).unwrap();

        let replayed = InMemoryStore::load_from_wal(&wal).unwrap();
        let claim = replayed.claims.get("c1").expect("legacy claim should load");
        assert!(claim.entities.is_empty());
        assert!(claim.embedding_ids.is_empty());

        let evidence = replayed
            .evidence_by_claim
            .get("c1")
            .and_then(|items| items.first())
            .expect("legacy evidence should load");
        assert_eq!(evidence.chunk_id, None);
        assert_eq!(evidence.span_start, None);
        assert_eq!(evidence.span_end, None);

        cleanup_persistence_files(&wal);
    }

    #[test]
    fn checkpoint_compacts_wal_and_replays_snapshot_plus_delta() {
        let wal_path = temp_wal_path();
        let mut wal = FileWal::open(&wal_path).unwrap();
        let mut store = InMemoryStore::new();

        store
            .ingest_bundle_persistent(
                &mut wal,
                claim("c1", "Company X acquired Company Y"),
                vec![Evidence {
                    evidence_id: "e1".into(),
                    claim_id: "c1".into(),
                    source_id: "doc-1".into(),
                    stance: Stance::Supports,
                    source_quality: 0.9,
                    chunk_id: None,
                    span_start: None,
                    span_end: None,
                    doc_id: None,
                    extraction_model: None,
                    ingested_at: None,
                }],
                vec![],
            )
            .unwrap();
        store
            .ingest_bundle_persistent(
                &mut wal,
                claim("c2", "Company Y integration started"),
                vec![Evidence {
                    evidence_id: "e2".into(),
                    claim_id: "c2".into(),
                    source_id: "doc-2".into(),
                    stance: Stance::Supports,
                    source_quality: 0.88,
                    chunk_id: None,
                    span_start: None,
                    span_end: None,
                    doc_id: None,
                    extraction_model: None,
                    ingested_at: None,
                }],
                vec![],
            )
            .unwrap();

        let wal_before = read_to_string(wal.path()).unwrap();
        assert!(!wal_before.trim().is_empty());

        let stats = store.checkpoint_and_compact(&mut wal).unwrap();
        assert_eq!(stats.snapshot_records, 4);
        assert_eq!(stats.truncated_wal_records, 4);
        assert!(wal.snapshot_path().exists());
        assert!(read_to_string(wal.path()).unwrap().trim().is_empty());

        store
            .ingest_bundle_persistent(
                &mut wal,
                claim("c3", "Post-compaction claim remains queryable"),
                vec![Evidence {
                    evidence_id: "e3".into(),
                    claim_id: "c3".into(),
                    source_id: "doc-3".into(),
                    stance: Stance::Supports,
                    source_quality: 0.9,
                    chunk_id: None,
                    span_start: None,
                    span_end: None,
                    doc_id: None,
                    extraction_model: None,
                    ingested_at: None,
                }],
                vec![],
            )
            .unwrap();

        let replayed = InMemoryStore::load_from_wal(&wal).unwrap();
        assert_eq!(replayed.claims_len(), 3);
        let results = replayed.retrieve(&RetrievalRequest {
            tenant_id: "tenant-a".into(),
            query: "post-compaction claim".into(),
            top_k: 3,
            stance_mode: StanceMode::Balanced,
        });
        assert_eq!(results[0].claim_id, "c3");

        cleanup_persistence_files(&wal);
    }

    #[test]
    fn wal_replay_boundary_tracks_checkpoint_promotion_and_delta_transition() {
        let wal_path = temp_wal_path();
        let mut wal = FileWal::open(&wal_path).unwrap();
        let mut store = InMemoryStore::new();

        store
            .ingest_bundle_persistent(
                &mut wal,
                claim("c1", "Company X announced merger terms"),
                vec![],
                vec![],
            )
            .unwrap();
        store
            .ingest_bundle_persistent(
                &mut wal,
                claim("c2", "Company Y accepted merger terms"),
                vec![],
                vec![],
            )
            .unwrap();

        let before_checkpoint = wal.replay_boundary().unwrap();
        assert!(!before_checkpoint.snapshot_active);
        assert_eq!(before_checkpoint.snapshot_record_count, 0);
        assert_eq!(before_checkpoint.wal_delta_record_count, 2);
        assert_eq!(before_checkpoint.total_replay_record_count, 2);

        store.checkpoint_and_compact(&mut wal).unwrap();
        let after_checkpoint = wal.replay_boundary().unwrap();
        assert!(after_checkpoint.snapshot_active);
        assert_eq!(after_checkpoint.snapshot_record_count, 2);
        assert_eq!(after_checkpoint.wal_delta_record_count, 0);
        assert_eq!(after_checkpoint.total_replay_record_count, 2);

        store
            .ingest_bundle_persistent(
                &mut wal,
                claim("c3", "Company Z approved merger financing"),
                vec![],
                vec![],
            )
            .unwrap();
        let after_delta_append = wal.replay_boundary().unwrap();
        assert!(after_delta_append.snapshot_active);
        assert_eq!(after_delta_append.snapshot_record_count, 2);
        assert_eq!(after_delta_append.wal_delta_record_count, 1);
        assert_eq!(after_delta_append.total_replay_record_count, 3);

        store.checkpoint_and_compact(&mut wal).unwrap();
        let after_second_checkpoint = wal.replay_boundary().unwrap();
        assert!(after_second_checkpoint.snapshot_active);
        assert_eq!(after_second_checkpoint.snapshot_record_count, 3);
        assert_eq!(after_second_checkpoint.wal_delta_record_count, 0);
        assert_eq!(after_second_checkpoint.total_replay_record_count, 3);

        let replayed = InMemoryStore::load_from_wal(&wal).unwrap();
        assert_eq!(replayed.claims_len(), 3);

        cleanup_persistence_files(&wal);
    }

    #[test]
    fn checkpoint_policy_triggers_compaction_by_record_count() {
        let wal_path = temp_wal_path();
        let mut wal = FileWal::open(&wal_path).unwrap();
        let mut store = InMemoryStore::new();
        let policy = CheckpointPolicy {
            max_wal_records: Some(4),
            max_wal_bytes: None,
        };

        let first = store
            .ingest_bundle_persistent_with_policy(
                &mut wal,
                &policy,
                claim("c1", "Company X acquired Company Y"),
                vec![Evidence {
                    evidence_id: "e1".into(),
                    claim_id: "c1".into(),
                    source_id: "doc-1".into(),
                    stance: Stance::Supports,
                    source_quality: 0.9,
                    chunk_id: None,
                    span_start: None,
                    span_end: None,
                    doc_id: None,
                    extraction_model: None,
                    ingested_at: None,
                }],
                vec![],
            )
            .unwrap();
        assert!(first.is_none());

        let second = store
            .ingest_bundle_persistent_with_policy(
                &mut wal,
                &policy,
                claim("c2", "Company Y integration started"),
                vec![Evidence {
                    evidence_id: "e2".into(),
                    claim_id: "c2".into(),
                    source_id: "doc-2".into(),
                    stance: Stance::Supports,
                    source_quality: 0.9,
                    chunk_id: None,
                    span_start: None,
                    span_end: None,
                    doc_id: None,
                    extraction_model: None,
                    ingested_at: None,
                }],
                vec![],
            )
            .unwrap();
        let stats = second.expect("second ingest should trigger checkpoint");
        assert_eq!(stats.truncated_wal_records, 4);
        assert!(read_to_string(wal.path()).unwrap().trim().is_empty());
        assert!(wal.snapshot_path().exists());

        cleanup_persistence_files(&wal);
    }

    #[test]
    fn checkpoint_policy_triggers_compaction_by_wal_bytes() {
        let wal_path = temp_wal_path();
        let mut wal = FileWal::open(&wal_path).unwrap();
        let mut store = InMemoryStore::new();
        let policy = CheckpointPolicy {
            max_wal_records: None,
            max_wal_bytes: Some(1),
        };

        let stats = store
            .ingest_bundle_persistent_with_policy(
                &mut wal,
                &policy,
                claim("c1", "Checkpoint by WAL bytes"),
                vec![Evidence {
                    evidence_id: "e1".into(),
                    claim_id: "c1".into(),
                    source_id: "doc-1".into(),
                    stance: Stance::Supports,
                    source_quality: 0.9,
                    chunk_id: None,
                    span_start: None,
                    span_end: None,
                    doc_id: None,
                    extraction_model: None,
                    ingested_at: None,
                }],
                vec![],
            )
            .unwrap()
            .expect("ingest should trigger byte policy checkpoint");
        assert!(stats.snapshot_records >= 2);
        assert_eq!(wal.wal_record_count().unwrap(), 0);

        cleanup_persistence_files(&wal);
    }

    #[test]
    fn entity_lookup_uses_entity_index() {
        let mut store = InMemoryStore::new();
        store
            .ingest_bundle(
                Claim {
                    claim_id: "c-entity".into(),
                    tenant_id: "tenant-a".into(),
                    canonical_text: "Company X acquired Company Y".into(),
                    confidence: 0.9,
                    event_time_unix: Some(100),
                    entities: vec!["Company X".into(), "Company Y".into()],
                    embedding_ids: vec![],
                    claim_type: None,
                    valid_from: None,
                    valid_to: None,
                    created_at: None,
                    updated_at: None,
                },
                vec![],
                vec![],
            )
            .unwrap();

        let claims = store.claims_for_entity("tenant-a", "company x");
        assert_eq!(claims.len(), 1);
        assert_eq!(claims[0].claim_id, "c-entity");

        let stats = store.index_stats();
        assert_eq!(stats.claim_count, 1);
        assert!(stats.entity_terms >= 2);
        assert!(stats.temporal_buckets >= 1);
    }

    #[test]
    fn embedding_lookup_uses_embedding_index() {
        let mut store = InMemoryStore::new();
        store
            .ingest_bundle(
                Claim {
                    claim_id: "c-embedding".into(),
                    tenant_id: "tenant-a".into(),
                    canonical_text: "Embedding indexed claim".into(),
                    confidence: 0.9,
                    event_time_unix: Some(200),
                    entities: vec![],
                    embedding_ids: vec!["emb://claim-a".into(), "emb://claim-b".into()],
                    claim_type: None,
                    valid_from: None,
                    valid_to: None,
                    created_at: None,
                    updated_at: None,
                },
                vec![],
                vec![],
            )
            .unwrap();

        let ids = store.claim_ids_for_embedding_id("tenant-a", "emb://claim-a");
        assert_eq!(ids.len(), 1);
        assert!(ids.contains("c-embedding"));
    }

    #[test]
    fn retrieve_with_allowed_claim_ids_limits_candidate_pool() {
        let mut store = InMemoryStore::new();
        store
            .ingest_bundle(
                claim("c-allow", "Project Helios acquired Startup Nova"),
                vec![],
                vec![],
            )
            .unwrap();
        store
            .ingest_bundle(
                claim("c-deny", "Project Helios operational update"),
                vec![],
                vec![],
            )
            .unwrap();
        store
            .upsert_claim_vector("c-allow", vec![1.0, 0.0, 0.0, 0.0])
            .unwrap();
        store
            .upsert_claim_vector("c-deny", vec![0.9, 0.1, 0.0, 0.0])
            .unwrap();

        let mut allowed = HashSet::new();
        allowed.insert("c-allow".to_string());

        let results = store.retrieve_with_time_range_query_vector_and_allowed_claim_ids(
            &RetrievalRequest {
                tenant_id: "tenant-a".into(),
                query: "project helios startup nova".into(),
                top_k: 5,
                stance_mode: StanceMode::Balanced,
            },
            None,
            None,
            Some(&[1.0, 0.0, 0.0, 0.0]),
            Some(&allowed),
        );

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].claim_id, "c-allow");
    }

    #[test]
    fn retrieve_with_explicit_candidate_claim_ids_scores_only_explicit_set() {
        let mut store = InMemoryStore::new();
        store
            .ingest_bundle(
                claim("c-segment", "Project Helios acquired Startup Nova"),
                vec![],
                vec![],
            )
            .unwrap();
        store
            .ingest_bundle(
                claim("c-delta", "Project Helios announced expanded merger terms"),
                vec![],
                vec![],
            )
            .unwrap();
        store
            .ingest_bundle(claim("c-other", "Unrelated weather update"), vec![], vec![])
            .unwrap();
        store
            .upsert_claim_vector("c-segment", vec![1.0, 0.0, 0.0, 0.0])
            .unwrap();
        store
            .upsert_claim_vector("c-delta", vec![0.8, 0.2, 0.0, 0.0])
            .unwrap();
        store
            .upsert_claim_vector("c-other", vec![0.1, 0.9, 0.0, 0.0])
            .unwrap();

        let explicit: HashSet<String> = ["c-segment".to_string(), "c-delta".to_string()]
            .into_iter()
            .collect();
        let results = store.retrieve_with_time_range_query_vector_and_explicit_candidate_claim_ids(
            &RetrievalRequest {
                tenant_id: "tenant-a".into(),
                query: "project helios acquisition".into(),
                top_k: 10,
                stance_mode: StanceMode::Balanced,
            },
            None,
            None,
            Some(&[1.0, 0.0, 0.0, 0.0]),
            &explicit,
            None,
        );

        let result_ids: HashSet<String> = results.into_iter().map(|row| row.claim_id).collect();
        assert_eq!(result_ids.len(), 2);
        assert!(result_ids.contains("c-segment"));
        assert!(result_ids.contains("c-delta"));
        assert!(!result_ids.contains("c-other"));
    }

    #[test]
    fn lexical_candidates_and_bm25_rank_relevant_claim_higher() {
        let mut store = InMemoryStore::new();
        store
            .ingest_bundle(
                claim("c-good", "Company X acquired Company Y"),
                vec![Evidence {
                    evidence_id: "e-good".into(),
                    claim_id: "c-good".into(),
                    source_id: "doc-1".into(),
                    stance: Stance::Supports,
                    source_quality: 0.95,
                    chunk_id: None,
                    span_start: None,
                    span_end: None,
                    doc_id: None,
                    extraction_model: None,
                    ingested_at: None,
                }],
                vec![],
            )
            .unwrap();
        store
            .ingest_bundle(
                claim("c-bad", "Weather in city tomorrow"),
                vec![Evidence {
                    evidence_id: "e-bad".into(),
                    claim_id: "c-bad".into(),
                    source_id: "doc-2".into(),
                    stance: Stance::Supports,
                    source_quality: 0.95,
                    chunk_id: None,
                    span_start: None,
                    span_end: None,
                    doc_id: None,
                    extraction_model: None,
                    ingested_at: None,
                }],
                vec![],
            )
            .unwrap();

        let candidate_count =
            store.candidate_count("tenant-a", "did company x acquire y", None, None);
        assert_eq!(candidate_count, 1);

        let results = store.retrieve(&RetrievalRequest {
            tenant_id: "tenant-a".into(),
            query: "did company x acquire y".into(),
            top_k: 2,
            stance_mode: StanceMode::Balanced,
        });
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].claim_id, "c-good");
    }

    #[test]
    fn vector_wal_round_trip_restores_claim_vectors() {
        let wal_path = temp_wal_path();
        let mut wal = FileWal::open(&wal_path).unwrap();
        let mut store = InMemoryStore::new();

        store
            .ingest_bundle_persistent(
                &mut wal,
                claim("c-vec", "Vector indexed claim"),
                vec![],
                vec![],
            )
            .unwrap();
        store
            .upsert_claim_vector_persistent(&mut wal, "c-vec", vec![0.1, 0.3, 0.5, 0.7])
            .unwrap();

        let (replayed, stats) = InMemoryStore::load_from_wal_with_stats(&wal).unwrap();
        assert!(stats.vectors_loaded >= 1);

        let results = replayed.retrieve_with_time_range_and_query_vector(
            &RetrievalRequest {
                tenant_id: "tenant-a".into(),
                query: "vector indexed claim".into(),
                top_k: 1,
                stance_mode: StanceMode::Balanced,
            },
            None,
            None,
            Some(&[0.1, 0.3, 0.5, 0.7]),
        );
        assert_eq!(results.first().map(|r| r.claim_id.as_str()), Some("c-vec"));

        cleanup_persistence_files(&wal);
    }

    #[test]
    fn batch_commit_wal_record_replays_without_mutating_claim_state() {
        let wal_path = temp_wal_path();
        let mut wal = FileWal::open(&wal_path).unwrap();
        let mut store = InMemoryStore::new();

        store
            .ingest_bundle_persistent(
                &mut wal,
                claim("c-batch", "Batch WAL claim"),
                vec![],
                vec![],
            )
            .unwrap();
        wal.append_batch_commit(
            "commit-test-123",
            1,
            1_700_000_000_000,
            &["c-batch".to_string()],
        )
        .expect("batch commit append should succeed");

        let (replayed, stats) = InMemoryStore::load_from_wal_with_stats(&wal).unwrap();
        assert_eq!(replayed.claims_len(), 1);
        assert_eq!(stats.claims_loaded, 1);

        cleanup_persistence_files(&wal);
    }

    #[test]
    fn wal_replication_delta_returns_window_and_resync_signal() {
        let wal_path = temp_wal_path();
        let mut wal = FileWal::open(&wal_path).unwrap();
        let mut store = InMemoryStore::new();

        store
            .ingest_bundle_persistent(
                &mut wal,
                claim("c-r1", "Replication claim one"),
                vec![],
                vec![],
            )
            .unwrap();
        store
            .ingest_bundle_persistent(
                &mut wal,
                claim("c-r2", "Replication claim two"),
                vec![],
                vec![],
            )
            .unwrap();

        let first = wal.replication_delta_from(0, 1).unwrap();
        assert!(!first.needs_resync);
        assert_eq!(first.from_offset, 0);
        assert_eq!(first.next_offset, 1);
        assert_eq!(first.total_records, 2);
        assert_eq!(first.wal_lines.len(), 1);

        let gap = wal.replication_delta_from(10, 2).unwrap();
        assert!(gap.needs_resync);
        assert_eq!(gap.next_offset, 2);
        assert!(gap.wal_lines.is_empty());

        cleanup_persistence_files(&wal);
    }

    #[test]
    fn wal_replication_export_replaces_snapshot_and_wal_state() {
        let src_wal_path = temp_wal_path();
        let mut src_wal = FileWal::open(&src_wal_path).unwrap();
        let mut src_store = InMemoryStore::new();
        src_store
            .ingest_bundle_persistent(
                &mut src_wal,
                claim("c-src-1", "Source claim one"),
                vec![],
                vec![],
            )
            .unwrap();
        src_store.checkpoint_and_compact(&mut src_wal).unwrap();
        src_store
            .ingest_bundle_persistent(
                &mut src_wal,
                claim("c-src-2", "Source claim two"),
                vec![],
                vec![],
            )
            .unwrap();
        let export = src_wal.replication_export().unwrap();
        assert!(!export.snapshot_lines.is_empty());
        assert_eq!(export.wal_lines.len(), 1);

        let dst_wal_path = temp_wal_path();
        let mut dst_wal = FileWal::open(&dst_wal_path).unwrap();
        dst_wal.replace_with_replication_export(&export).unwrap();

        let replayed = InMemoryStore::load_from_wal(&dst_wal).unwrap();
        assert_eq!(replayed.claims_len(), 2);
        assert!(dst_wal.snapshot_path().exists());

        cleanup_persistence_files(&src_wal);
        cleanup_persistence_files(&dst_wal);
    }

    #[test]
    fn observe_batch_commit_is_idempotent_for_same_payload() {
        let mut store = InMemoryStore::new();
        let claim_ids = vec!["c-idem-1".to_string(), "c-idem-2".to_string()];
        store
            .observe_batch_commit(
                "commit-idem-1",
                claim_ids.len(),
                1_700_000_000_000,
                &claim_ids,
            )
            .expect("first commit metadata insert should succeed");
        store
            .observe_batch_commit(
                "commit-idem-1",
                claim_ids.len(),
                1_700_000_000_123,
                &claim_ids,
            )
            .expect("same payload should replay idempotently");
        let metadata = store
            .batch_commit_metadata("commit-idem-1")
            .expect("metadata should be retained");
        assert_eq!(metadata.batch_size, 2);
        assert_eq!(metadata.claim_ids, claim_ids);
        assert_eq!(
            metadata.payload_fingerprint,
            batch_commit_payload_fingerprint(2, &claim_ids)
        );
    }

    #[test]
    fn observe_batch_commit_rejects_payload_conflict_for_existing_commit_id() {
        let mut store = InMemoryStore::new();
        store
            .observe_batch_commit(
                "commit-conflict-1",
                1,
                1_700_000_000_000,
                &["c-conflict-1".to_string()],
            )
            .expect("first commit metadata insert should succeed");
        let err = store
            .observe_batch_commit(
                "commit-conflict-1",
                1,
                1_700_000_000_111,
                &["c-conflict-2".to_string()],
            )
            .expect_err("conflicting payload should fail");
        assert!(matches!(err, StoreError::Conflict(_)));
        let message = format!("{err:?}");
        assert!(message.contains("existing_fingerprint="));
        assert!(message.contains("incoming_fingerprint="));
    }

    #[test]
    fn batch_commit_payload_fingerprint_is_deterministic_and_order_sensitive() {
        let ordered = vec!["c1".to_string(), "c2".to_string()];
        let swapped = vec!["c2".to_string(), "c1".to_string()];
        let first = batch_commit_payload_fingerprint(2, &ordered);
        let second = batch_commit_payload_fingerprint(2, &ordered);
        let third = batch_commit_payload_fingerprint(2, &swapped);
        assert_eq!(first, second);
        assert_ne!(first, third);
    }

    #[test]
    fn apply_persisted_batch_commit_line_rejects_replay_payload_divergence() {
        let mut store = InMemoryStore::new();
        store
            .apply_persisted_record_line("B\tcommit-diverge-1\t1\t1700000000000\t2:c1")
            .expect("first replayed batch commit should apply");
        let err = store
            .apply_persisted_record_line("B\tcommit-diverge-1\t1\t1700000000001\t2:c2")
            .expect_err("payload divergence must be rejected");
        assert!(matches!(err, StoreError::Conflict(_)));
        let message = format!("{err:?}");
        assert!(message.contains("existing_fingerprint="));
        assert!(message.contains("incoming_fingerprint="));
    }

    #[test]
    fn query_embedding_prefers_vector_similar_claim() {
        let mut store = InMemoryStore::new();
        store
            .ingest_bundle(claim("c-near", "Semantic nearby claim"), vec![], vec![])
            .unwrap();
        store
            .ingest_bundle(claim("c-far", "Semantic distant claim"), vec![], vec![])
            .unwrap();
        store
            .upsert_claim_vector("c-near", vec![1.0, 0.0, 0.0, 0.0])
            .unwrap();
        store
            .upsert_claim_vector("c-far", vec![0.0, 1.0, 0.0, 0.0])
            .unwrap();

        let results = store.retrieve_with_time_range_and_query_vector(
            &RetrievalRequest {
                tenant_id: "tenant-a".into(),
                query: "semantic claim".into(),
                top_k: 2,
                stance_mode: StanceMode::Balanced,
            },
            None,
            None,
            Some(&[0.99, 0.01, 0.0, 0.0]),
        );
        assert_eq!(results.first().map(|r| r.claim_id.as_str()), Some("c-near"));

        let stats = store.index_stats();
        assert_eq!(stats.vector_count, 2);
        assert!(stats.ann_vector_buckets >= 1);
    }

    #[test]
    fn ann_and_exact_vector_top_candidates_rank_expected_claim() {
        let mut store = InMemoryStore::new();
        store
            .ingest_bundle(claim("c-near", "Semantic nearby claim"), vec![], vec![])
            .unwrap();
        store
            .ingest_bundle(claim("c-far", "Semantic distant claim"), vec![], vec![])
            .unwrap();
        store
            .upsert_claim_vector("c-near", vec![1.0, 0.0, 0.0, 0.0])
            .unwrap();
        store
            .upsert_claim_vector("c-far", vec![0.0, 1.0, 0.0, 0.0])
            .unwrap();

        let query = [0.99, 0.01, 0.0, 0.0];
        let ann = store.ann_vector_top_candidates("tenant-a", &query, 1);
        let exact = store.exact_vector_top_candidates("tenant-a", &query, 1);

        assert_eq!(ann.first().map(String::as_str), Some("c-near"));
        assert_eq!(exact.first().map(String::as_str), Some("c-near"));
    }

    #[test]
    fn ann_graph_populates_multiple_levels_for_tenant() {
        let mut store = InMemoryStore::new();
        let mut high_level_claim_id = None;

        for i in 0..256 {
            let claim_id = format!("c-level-{i}");
            let level = store.assign_ann_level(&claim_id);
            if level > 0 {
                high_level_claim_id = Some(claim_id.clone());
            }

            store
                .ingest_bundle(
                    claim(&claim_id, "ANN graph level population"),
                    vec![],
                    vec![],
                )
                .unwrap();
            let vector = vec![
                0.1 + (i as f32 * 0.001),
                0.2,
                0.3,
                if level > 0 { 0.99 } else { 0.4 },
            ];
            store.upsert_claim_vector(&claim_id, vector).unwrap();
        }

        let high_level_claim_id =
            high_level_claim_id.expect("expected at least one claim to land in an upper ANN level");
        let graph = store
            .ann_vector_graphs
            .get("tenant-a")
            .expect("tenant ANN graph should exist");

        assert_eq!(graph.levels[0].len(), 256);
        assert!(!graph.levels[1].is_empty());
        assert!(graph.entry_level >= 1);
        assert!(graph.levels[graph.entry_level].contains_key(&high_level_claim_id));
    }

    #[test]
    fn store_ann_tuning_can_be_overridden() {
        let tuning = AnnTuningConfig {
            max_neighbors_base: 8,
            max_neighbors_upper: 4,
            search_expansion_factor: 9,
            search_expansion_min: 32,
            search_expansion_max: 2048,
        };
        let store = InMemoryStore::new_with_ann_tuning(tuning.clone());
        assert_eq!(store.ann_tuning(), &tuning);
    }

    #[test]
    fn vector_backend_env_cpu_selects_cpu_runtime() {
        let _guard = EnvVarGuard::set(VECTOR_BACKEND_ENV, "cpu");
        let store = InMemoryStore::new();
        assert_eq!(store.vector_backend_runtime(), VectorBackendRuntime::Cpu);
        assert_eq!(store.vector_backend_label(), "cpu");
    }

    #[test]
    #[cfg(not(feature = "gpu-backend"))]
    fn vector_backend_env_gpu_without_feature_falls_back_to_cpu() {
        let _guard = EnvVarGuard::set(VECTOR_BACKEND_ENV, "gpu");
        let store = InMemoryStore::new();
        assert_eq!(
            store.vector_backend_runtime(),
            VectorBackendRuntime::CpuFallbackFeatureDisabled
        );
        assert_eq!(store.vector_backend_label(), "cpu (gpu-feature-disabled)");
    }

    #[test]
    #[cfg(feature = "gpu-backend")]
    fn vector_backend_env_gpu_with_feature_is_gpu_or_runtime_fallback() {
        let _guard = EnvVarGuard::set(VECTOR_BACKEND_ENV, "gpu");
        let store = InMemoryStore::new();
        assert!(matches!(
            store.vector_backend_runtime(),
            VectorBackendRuntime::Gpu | VectorBackendRuntime::CpuFallbackUnavailable
        ));
    }

    #[test]
    fn tenant_vector_dimension_mismatch_is_rejected() {
        let mut store = InMemoryStore::new();
        store
            .ingest_bundle(claim("c1", "First vector claim"), vec![], vec![])
            .unwrap();
        store
            .ingest_bundle(claim("c2", "Second vector claim"), vec![], vec![])
            .unwrap();

        store
            .upsert_claim_vector("c1", vec![0.1, 0.2, 0.3])
            .unwrap();
        let err = store.upsert_claim_vector("c2", vec![0.1, 0.2]).unwrap_err();
        match err {
            StoreError::InvalidVector(message) => {
                assert!(message.contains("dimension mismatch"));
            }
            other => panic!("expected InvalidVector, got {other:?}"),
        }
    }

    #[test]
    fn claim_id_reuse_across_tenants_is_rejected() {
        let mut store = InMemoryStore::new();
        store
            .ingest_bundle(
                claim_for_tenant("c-tenant-collision", "tenant-a claim", "tenant-a"),
                vec![],
                vec![],
            )
            .expect("initial ingest should succeed");

        let err = store
            .ingest_bundle(
                claim_for_tenant("c-tenant-collision", "tenant-b claim", "tenant-b"),
                vec![],
                vec![],
            )
            .expect_err("cross-tenant claim_id reuse should be rejected");
        match err {
            StoreError::Conflict(message) => {
                assert!(message.contains("c-tenant-collision"));
                assert!(message.contains("tenant-a"));
            }
            other => panic!("expected Conflict, got {other:?}"),
        }

        assert_eq!(store.claims_for_tenant("tenant-a").len(), 1);
        assert_eq!(store.claims_for_tenant("tenant-b").len(), 0);
    }

    #[test]
    fn ingest_bundle_persistent_rejects_cross_tenant_claim_id_before_wal_append() {
        let wal_path = temp_wal_path();
        let mut wal = FileWal::open(&wal_path).expect("wal should open");
        let mut store = InMemoryStore::new();

        store
            .ingest_bundle_persistent(
                &mut wal,
                claim_for_tenant("c-tenant-collision", "tenant-a claim", "tenant-a"),
                vec![],
                vec![],
            )
            .expect("initial persistent ingest should succeed");
        let before = wal
            .wal_record_count()
            .expect("wal record count should be readable");

        let err = store
            .ingest_bundle_persistent(
                &mut wal,
                claim_for_tenant("c-tenant-collision", "tenant-b claim", "tenant-b"),
                vec![],
                vec![],
            )
            .expect_err("cross-tenant claim_id reuse should be rejected");
        assert!(matches!(err, StoreError::Conflict(_)));
        let after = wal
            .wal_record_count()
            .expect("wal record count should be readable");
        assert_eq!(after, before);

        cleanup_persistence_files(&wal);
    }

    #[test]
    fn load_from_wal_rejects_cross_tenant_claim_id_collision() {
        let wal_path = temp_wal_path();
        let mut wal = FileWal::open(&wal_path).expect("wal should open");

        wal.append_claim(&claim_for_tenant(
            "c-tenant-collision",
            "tenant-a claim",
            "tenant-a",
        ))
        .expect("tenant-a claim append should succeed");
        wal.append_claim(&claim_for_tenant(
            "c-tenant-collision",
            "tenant-b claim",
            "tenant-b",
        ))
        .expect("tenant-b claim append should succeed");

        let err = InMemoryStore::load_from_wal(&wal)
            .err()
            .expect("cross-tenant claim_id collision should fail replay");
        assert!(matches!(err, StoreError::Conflict(_)));

        cleanup_persistence_files(&wal);
    }
}
