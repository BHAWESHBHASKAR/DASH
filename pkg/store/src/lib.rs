use std::{
    cmp::Ordering,
    collections::{BTreeMap, HashMap, HashSet},
    fs::{OpenOptions, create_dir_all, rename},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use graph::summarize_edges;
use ranking::{RankSignals, bm25_score, score_claim_with_bm25};
use schema::{
    Citation, Claim, ClaimEdge, Evidence, Relation, RetrievalRequest, RetrievalResult, Stance,
    StanceMode, ValidationError, tokenize, validate_claim, validate_edge, validate_evidence,
};

#[derive(Debug, Clone, PartialEq)]
pub enum WalEvent {
    ClaimUpsert(String),
    EvidenceUpsert(String),
    EdgeUpsert(String),
    ClaimVectorUpsert(String),
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

#[derive(Debug, Clone)]
enum PersistedRecord {
    Claim(Claim),
    Evidence(Evidence),
    Edge(ClaimEdge),
    ClaimVector(ClaimVectorRecord),
}

#[derive(Debug, Clone)]
struct ClaimVectorRecord {
    claim_id: String,
    values: Vec<f32>,
}

const SNAPSHOT_HEADER: &str = "SNAP\t1";
const ANN_GRAPH_LEVELS: usize = 4;
const ANN_GRAPH_MAX_NEIGHBORS_BASE_DEFAULT: usize = 12;
const ANN_GRAPH_MAX_NEIGHBORS_UPPER_DEFAULT: usize = 6;
const ANN_SEARCH_EXPANSION_FACTOR_DEFAULT: usize = 12;
const ANN_SEARCH_EXPANSION_MIN_DEFAULT: usize = 64;
const ANN_SEARCH_EXPANSION_MAX_DEFAULT: usize = 4096;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalCheckpointStats {
    pub snapshot_records: usize,
    pub truncated_wal_records: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CheckpointPolicy {
    pub max_wal_records: Option<usize>,
    pub max_wal_bytes: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct WalReplayStats {
    pub snapshot_records: usize,
    pub wal_records: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct StoreLoadStats {
    pub replay: WalReplayStats,
    pub claims_loaded: usize,
    pub evidence_loaded: usize,
    pub edges_loaded: usize,
    pub vectors_loaded: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct StoreIndexStats {
    pub tenant_count: usize,
    pub claim_count: usize,
    pub vector_count: usize,
    pub inverted_terms: usize,
    pub entity_terms: usize,
    pub temporal_buckets: usize,
    pub ann_vector_buckets: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnnTuningConfig {
    pub max_neighbors_base: usize,
    pub max_neighbors_upper: usize,
    pub search_expansion_factor: usize,
    pub search_expansion_min: usize,
    pub search_expansion_max: usize,
}

impl Default for AnnTuningConfig {
    fn default() -> Self {
        Self {
            max_neighbors_base: ANN_GRAPH_MAX_NEIGHBORS_BASE_DEFAULT,
            max_neighbors_upper: ANN_GRAPH_MAX_NEIGHBORS_UPPER_DEFAULT,
            search_expansion_factor: ANN_SEARCH_EXPANSION_FACTOR_DEFAULT,
            search_expansion_min: ANN_SEARCH_EXPANSION_MIN_DEFAULT,
            search_expansion_max: ANN_SEARCH_EXPANSION_MAX_DEFAULT,
        }
    }
}

#[derive(Default)]
struct Bm25Context {
    doc_freq: HashMap<String, usize>,
    total_docs: usize,
    avg_doc_len: f32,
}

#[derive(Debug, Clone)]
struct TenantAnnGraph {
    entry_point: Option<String>,
    entry_level: usize,
    levels: Vec<HashMap<String, Vec<String>>>,
    node_levels: HashMap<String, usize>,
}

impl Default for TenantAnnGraph {
    fn default() -> Self {
        Self {
            entry_point: None,
            entry_level: 0,
            levels: (0..ANN_GRAPH_LEVELS).map(|_| HashMap::new()).collect(),
            node_levels: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone)]
struct ScoredNode {
    claim_id: String,
    score: f32,
}

impl PartialEq for ScoredNode {
    fn eq(&self, other: &Self) -> bool {
        self.claim_id == other.claim_id && self.score.to_bits() == other.score.to_bits()
    }
}

impl Eq for ScoredNode {}

impl PartialOrd for ScoredNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScoredNode {
    fn cmp(&self, other: &Self) -> Ordering {
        self.score
            .total_cmp(&other.score)
            .then_with(|| self.claim_id.cmp(&other.claim_id))
    }
}

pub struct FileWal {
    path: PathBuf,
    wal_records: usize,
    sync_every_records: usize,
    append_buffer_max_records: usize,
    sync_interval: Option<Duration>,
    background_flush_only: bool,
    append_buffer: Vec<String>,
    unsynced_records: usize,
    last_sync_at: Instant,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalWritePolicy {
    pub sync_every_records: usize,
    pub append_buffer_max_records: usize,
    pub sync_interval: Option<Duration>,
    pub background_flush_only: bool,
}

impl Default for WalWritePolicy {
    fn default() -> Self {
        Self {
            sync_every_records: 1,
            append_buffer_max_records: 1,
            sync_interval: None,
            background_flush_only: false,
        }
    }
}

impl FileWal {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        Self::open_with_sync_every_records(path, 1)
    }

    pub fn open_with_sync_every_records(
        path: impl AsRef<Path>,
        sync_every_records: usize,
    ) -> Result<Self, StoreError> {
        Self::open_with_policy(
            path,
            WalWritePolicy {
                sync_every_records,
                ..WalWritePolicy::default()
            },
        )
    }

    pub fn open_with_policy(
        path: impl AsRef<Path>,
        policy: WalWritePolicy,
    ) -> Result<Self, StoreError> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            create_dir_all(parent)?;
        }
        OpenOptions::new().create(true).append(true).open(&path)?;
        let wal_records = count_non_empty_lines(&path)?;
        Ok(Self {
            path,
            wal_records,
            sync_every_records: policy.sync_every_records.max(1),
            append_buffer_max_records: policy.append_buffer_max_records.max(1),
            sync_interval: policy.sync_interval,
            background_flush_only: policy.background_flush_only,
            append_buffer: Vec::new(),
            unsynced_records: 0,
            last_sync_at: Instant::now(),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn sync_every_records(&self) -> usize {
        self.sync_every_records
    }

    pub fn append_buffer_max_records(&self) -> usize {
        self.append_buffer_max_records
    }

    pub fn sync_interval(&self) -> Option<Duration> {
        self.sync_interval
    }

    pub fn background_flush_only(&self) -> bool {
        self.background_flush_only
    }

    pub fn unsynced_record_count(&self) -> usize {
        self.unsynced_records
    }

    pub fn buffered_record_count(&self) -> usize {
        self.append_buffer.len()
    }

    pub fn snapshot_path(&self) -> PathBuf {
        let mut path = self.path.clone().into_os_string();
        path.push(".snapshot");
        PathBuf::from(path)
    }

    pub fn append_claim(&mut self, claim: &Claim) -> Result<(), StoreError> {
        self.append_record(&PersistedRecord::Claim(claim.clone()))
    }

    pub fn append_evidence(&mut self, evidence: &Evidence) -> Result<(), StoreError> {
        self.append_record(&PersistedRecord::Evidence(evidence.clone()))
    }

    pub fn append_edge(&mut self, edge: &ClaimEdge) -> Result<(), StoreError> {
        self.append_record(&PersistedRecord::Edge(edge.clone()))
    }

    pub fn append_claim_vector(
        &mut self,
        claim_id: &str,
        values: &[f32],
    ) -> Result<(), StoreError> {
        self.append_record(&PersistedRecord::ClaimVector(ClaimVectorRecord {
            claim_id: claim_id.to_string(),
            values: values.to_vec(),
        }))
    }

    pub fn wal_record_count(&self) -> Result<usize, StoreError> {
        Ok(self.wal_records)
    }

    pub fn wal_size_bytes(&self) -> Result<u64, StoreError> {
        Ok(std::fs::metadata(&self.path)?.len())
    }

    fn append_record(&mut self, record: &PersistedRecord) -> Result<(), StoreError> {
        self.append_buffer.push(record_to_line(record));
        self.wal_records += 1;
        self.unsynced_records += 1;
        if self.background_flush_only {
            return Ok(());
        }
        if self.append_buffer.len() >= self.append_buffer_max_records {
            self.flush_append_buffer()?;
        }
        let interval_elapsed = self
            .sync_interval
            .is_some_and(|interval| self.last_sync_at.elapsed() >= interval);
        if self.unsynced_records >= self.sync_every_records || interval_elapsed {
            self.flush_pending_sync()?;
        }
        Ok(())
    }

    pub fn flush_pending_sync_if_interval_elapsed(&mut self) -> Result<bool, StoreError> {
        let Some(interval) = self.sync_interval else {
            return Ok(false);
        };
        if self.unsynced_records == 0 || self.last_sync_at.elapsed() < interval {
            return Ok(false);
        }
        self.flush_pending_sync()?;
        Ok(true)
    }

    pub fn flush_pending_sync_if_unsynced(&mut self) -> Result<bool, StoreError> {
        if self.unsynced_records == 0 {
            return Ok(false);
        }
        self.flush_pending_sync()?;
        Ok(true)
    }

    fn flush_append_buffer(&mut self) -> Result<(), StoreError> {
        if self.append_buffer.is_empty() {
            return Ok(());
        }
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        for line in self.append_buffer.drain(..) {
            writeln!(file, "{line}")?;
        }
        Ok(())
    }

    pub fn flush_pending_sync(&mut self) -> Result<(), StoreError> {
        self.flush_append_buffer()?;
        if self.unsynced_records == 0 {
            return Ok(());
        }
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        file.sync_data()?;
        self.unsynced_records = 0;
        self.last_sync_at = Instant::now();
        Ok(())
    }

    fn replay_records_with_stats(
        &self,
    ) -> Result<(Vec<PersistedRecord>, WalReplayStats), StoreError> {
        let snapshot_records = self.replay_snapshot_records()?;
        let mut wal_records = self.replay_wal_records()?;
        if !self.append_buffer.is_empty() {
            for line in &self.append_buffer {
                wal_records.push(line_to_record(line)?);
            }
        }
        let stats = WalReplayStats {
            snapshot_records: snapshot_records.len(),
            wal_records: wal_records.len(),
        };

        let mut out = snapshot_records;
        out.extend(wal_records);
        Ok((out, stats))
    }

    fn replay_snapshot_records(&self) -> Result<Vec<PersistedRecord>, StoreError> {
        let snapshot_path = self.snapshot_path();
        if !snapshot_path.exists() {
            return Ok(Vec::new());
        }
        let file = OpenOptions::new().read(true).open(snapshot_path)?;
        let reader = BufReader::new(file);
        let mut lines = reader.lines();
        let header = loop {
            match lines.next() {
                Some(line) => {
                    let line = line?;
                    if line.trim().is_empty() {
                        continue;
                    }
                    break line;
                }
                None => {
                    return Err(StoreError::Parse("snapshot file is empty".to_string()));
                }
            }
        };
        if header != SNAPSHOT_HEADER {
            return Err(StoreError::Parse(
                "snapshot file has invalid header".to_string(),
            ));
        }

        let mut out = Vec::new();
        for line in lines {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            out.push(line_to_record(&line)?);
        }
        Ok(out)
    }

    fn replay_wal_records(&self) -> Result<Vec<PersistedRecord>, StoreError> {
        let file = OpenOptions::new().read(true).open(&self.path)?;
        let reader = BufReader::new(file);
        let mut out = Vec::new();
        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            out.push(line_to_record(&line)?);
        }
        Ok(out)
    }

    fn write_snapshot_records(&self, records: &[PersistedRecord]) -> Result<(), StoreError> {
        let snapshot_path = self.snapshot_path();
        if let Some(parent) = snapshot_path.parent()
            && !parent.as_os_str().is_empty()
        {
            create_dir_all(parent)?;
        }

        let mut tmp_path = snapshot_path.clone().into_os_string();
        tmp_path.push(".tmp");
        let tmp_path = PathBuf::from(tmp_path);

        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)?;
        writeln!(file, "{SNAPSHOT_HEADER}")?;
        for record in records {
            writeln!(file, "{}", record_to_line(record))?;
        }
        file.sync_all()?;
        rename(tmp_path, snapshot_path)?;
        Ok(())
    }

    fn truncate_wal(&mut self) -> Result<(), StoreError> {
        self.append_buffer.clear();
        OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.path)?;
        self.wal_records = 0;
        self.unsynced_records = 0;
        self.last_sync_at = Instant::now();
        Ok(())
    }

    fn compact_with_snapshot(
        &mut self,
        snapshot_records: &[PersistedRecord],
    ) -> Result<WalCheckpointStats, StoreError> {
        let truncated_wal_records = self.wal_records;
        self.flush_pending_sync()?;
        self.write_snapshot_records(snapshot_records)?;
        self.truncate_wal()?;
        Ok(WalCheckpointStats {
            snapshot_records: snapshot_records.len(),
            truncated_wal_records,
        })
    }
}

impl Drop for FileWal {
    fn drop(&mut self) {
        let _ = self.flush_pending_sync();
    }
}

#[derive(Default)]
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
    claim_tokens: HashMap<String, Vec<String>>,
    ann_tuning: AnnTuningConfig,
    wal: Vec<WalEvent>,
}

impl InMemoryStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn new_with_ann_tuning(ann_tuning: AnnTuningConfig) -> Self {
        Self {
            ann_tuning,
            ..Self::default()
        }
    }

    pub fn ann_tuning(&self) -> &AnnTuningConfig {
        &self.ann_tuning
    }

    pub fn set_ann_tuning(&mut self, ann_tuning: AnnTuningConfig) {
        self.ann_tuning = ann_tuning;
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
            match record {
                PersistedRecord::Claim(claim) => {
                    claims_loaded += 1;
                    store.apply_claim(claim)?;
                }
                PersistedRecord::Evidence(evidence) => {
                    evidence_loaded += 1;
                    store.apply_evidence(evidence)?;
                }
                PersistedRecord::Edge(edge) => {
                    edges_loaded += 1;
                    store.apply_edge(edge)?;
                }
                PersistedRecord::ClaimVector(record) => {
                    vectors_loaded += 1;
                    store.apply_claim_vector(&record.claim_id, record.values)?;
                }
            }
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

    pub fn retrieve(&self, req: &RetrievalRequest) -> Vec<RetrievalResult> {
        self.retrieve_with_time_range_and_query_vector(req, None, None, None)
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
        let mut ranked: Vec<RetrievalResult> = Vec::new();

        let candidates = self.candidate_claim_ids(
            &req.tenant_id,
            &req.query,
            (from_unix, to_unix),
            query_vector,
            req.top_k,
            allowed_claim_ids,
        );
        let bm25_context = self.bm25_context_for_tenant(&req.tenant_id, &req.query);

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

            let dense_similarity = match query_vector {
                Some(vector) => self
                    .claim_vectors
                    .get(&claim.claim_id)
                    .and_then(|claim_vector| cosine_similarity(vector, claim_vector)),
                None => None,
            }
            .unwrap_or(0.0);

            let score = score_claim_with_bm25(
                &req.query,
                claim,
                avg_quality,
                RankSignals {
                    supports,
                    contradicts,
                },
                bm25,
            ) + (dense_similarity * 0.35);

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

        let mut scored: Vec<(String, f32)> = self
            .claim_vectors
            .iter()
            .filter_map(|(claim_id, vector)| {
                let claim = self.claims.get(claim_id)?;
                if claim.tenant_id != tenant_id {
                    return None;
                }
                let score = cosine_similarity(query_vector, vector)?;
                Some((claim_id.clone(), score))
            })
            .collect();
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
            let ranged = self.temporal_claim_ids(tenant_id, from_unix, to_unix);
            candidates = candidates.intersection(&ranged).cloned().collect();
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

        let mut scored: Vec<(String, f32)> = scoped_ids
            .into_iter()
            .filter_map(|claim_id| {
                let vector = self.claim_vectors.get(&claim_id)?;
                let claim = self.claims.get(&claim_id)?;
                if claim.tenant_id != tenant_id {
                    return None;
                }
                let sim = cosine_similarity(query_vector, vector)?;
                Some((claim_id, sim))
            })
            .collect();
        scored.sort_by(|a, b| b.1.total_cmp(&a.1));
        scored
            .into_iter()
            .take(top_n)
            .map(|(claim_id, _)| claim_id)
            .collect()
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

    fn temporal_claim_ids(
        &self,
        tenant_id: &str,
        from_unix: Option<i64>,
        to_unix: Option<i64>,
    ) -> HashSet<String> {
        let mut out = HashSet::new();
        let Some(tenant_timeline) = self.temporal_index.get(tenant_id) else {
            return out;
        };

        let start = from_unix.unwrap_or(i64::MIN);
        let end = to_unix.unwrap_or(i64::MAX);
        for (_, ids) in tenant_timeline.range(start..=end) {
            out.extend(ids.iter().cloned());
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

    fn apply_claim(&mut self, claim: Claim) -> Result<(), StoreError> {
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

fn record_to_line(record: &PersistedRecord) -> String {
    match record {
        PersistedRecord::Claim(c) => format!(
            "C\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            escape_field(&c.claim_id),
            escape_field(&c.tenant_id),
            escape_field(&c.canonical_text),
            c.confidence,
            c.event_time_unix
                .map(|v| v.to_string())
                .unwrap_or_else(|| "null".to_string()),
            pack_string_list(&c.entities),
            pack_string_list(&c.embedding_ids)
        ),
        PersistedRecord::Evidence(e) => format!(
            "E\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            escape_field(&e.evidence_id),
            escape_field(&e.claim_id),
            escape_field(&e.source_id),
            stance_to_str(&e.stance),
            e.source_quality,
            e.chunk_id
                .as_ref()
                .map(|v| escape_field(v))
                .unwrap_or_else(|| "null".to_string()),
            e.span_start
                .map(|v| v.to_string())
                .unwrap_or_else(|| "null".to_string()),
            e.span_end
                .map(|v| v.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        PersistedRecord::Edge(edge) => format!(
            "G\t{}\t{}\t{}\t{}\t{}",
            escape_field(&edge.edge_id),
            escape_field(&edge.from_claim_id),
            escape_field(&edge.to_claim_id),
            relation_to_str(&edge.relation),
            edge.strength
        ),
        PersistedRecord::ClaimVector(record) => format!(
            "V\t{}\t{}",
            escape_field(&record.claim_id),
            pack_f32_list(&record.values)
        ),
    }
}

fn count_non_empty_lines(path: &Path) -> Result<usize, StoreError> {
    let file = OpenOptions::new().read(true).open(path)?;
    let reader = BufReader::new(file);
    let mut count = 0usize;
    for line in reader.lines() {
        let line = line?;
        if !line.trim().is_empty() {
            count += 1;
        }
    }
    Ok(count)
}

fn line_to_record(line: &str) -> Result<PersistedRecord, StoreError> {
    let parts: Vec<&str> = line.split('\t').collect();
    if parts.is_empty() {
        return Err(StoreError::Parse("empty wal record".to_string()));
    }
    match parts[0] {
        "C" => {
            if !(parts.len() == 6 || parts.len() == 8) {
                return Err(StoreError::Parse(
                    "claim record has invalid field count".to_string(),
                ));
            }
            let event_time_unix = if parts[5] == "null" {
                None
            } else {
                Some(parts[5].parse::<i64>().map_err(|_| {
                    StoreError::Parse("claim record has invalid event_time".to_string())
                })?)
            };
            let entities = if parts.len() >= 8 {
                unpack_string_list(parts[6])?
            } else {
                Vec::new()
            };
            let embedding_ids = if parts.len() >= 8 {
                unpack_string_list(parts[7])?
            } else {
                Vec::new()
            };
            Ok(PersistedRecord::Claim(Claim {
                claim_id: unescape_field(parts[1])?,
                tenant_id: unescape_field(parts[2])?,
                canonical_text: unescape_field(parts[3])?,
                confidence: parts[4].parse::<f32>().map_err(|_| {
                    StoreError::Parse("claim record has invalid confidence".to_string())
                })?,
                event_time_unix,
                entities,
                embedding_ids,
                claim_type: None,
                valid_from: None,
                valid_to: None,
                created_at: None,
                updated_at: None,
            }))
        }
        "E" => {
            if !(parts.len() == 6 || parts.len() == 9) {
                return Err(StoreError::Parse(
                    "evidence record has invalid field count".to_string(),
                ));
            }
            let chunk_id = if parts.len() >= 9 {
                parse_optional_escaped_field(parts[6])?
            } else {
                None
            };
            let span_start = if parts.len() >= 9 {
                parse_optional_u32_field(parts[7], "span_start")?
            } else {
                None
            };
            let span_end = if parts.len() >= 9 {
                parse_optional_u32_field(parts[8], "span_end")?
            } else {
                None
            };
            Ok(PersistedRecord::Evidence(Evidence {
                evidence_id: unescape_field(parts[1])?,
                claim_id: unescape_field(parts[2])?,
                source_id: unescape_field(parts[3])?,
                stance: str_to_stance(parts[4])?,
                source_quality: parts[5].parse::<f32>().map_err(|_| {
                    StoreError::Parse("evidence record has invalid source_quality".to_string())
                })?,
                chunk_id,
                span_start,
                span_end,
                doc_id: None,
                extraction_model: None,
                ingested_at: None,
            }))
        }
        "G" => {
            if parts.len() != 6 {
                return Err(StoreError::Parse(
                    "edge record has invalid field count".to_string(),
                ));
            }
            Ok(PersistedRecord::Edge(ClaimEdge {
                edge_id: unescape_field(parts[1])?,
                from_claim_id: unescape_field(parts[2])?,
                to_claim_id: unescape_field(parts[3])?,
                relation: str_to_relation(parts[4])?,
                strength: parts[5].parse::<f32>().map_err(|_| {
                    StoreError::Parse("edge record has invalid strength".to_string())
                })?,
                reason_codes: vec![],
                created_at: None,
            }))
        }
        "V" => {
            if parts.len() != 3 {
                return Err(StoreError::Parse(
                    "vector record has invalid field count".to_string(),
                ));
            }
            Ok(PersistedRecord::ClaimVector(ClaimVectorRecord {
                claim_id: unescape_field(parts[1])?,
                values: unpack_f32_list(parts[2])?,
            }))
        }
        _ => Err(StoreError::Parse("unknown wal record kind".to_string())),
    }
}

fn escape_field(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\t', "\\t")
        .replace('\n', "\\n")
}

fn pack_string_list(values: &[String]) -> String {
    let mut out = String::new();
    for value in values {
        out.push_str(&format!("{}:", value.len()));
        out.push_str(value);
    }
    out
}

fn unpack_string_list(raw: &str) -> Result<Vec<String>, StoreError> {
    if raw.is_empty() {
        return Ok(Vec::new());
    }
    let bytes = raw.as_bytes();
    let mut offset = 0usize;
    let mut out = Vec::new();
    while offset < bytes.len() {
        let len_start = offset;
        while offset < bytes.len() && bytes[offset].is_ascii_digit() {
            offset += 1;
        }
        if offset == len_start || offset >= bytes.len() || bytes[offset] != b':' {
            return Err(StoreError::Parse(
                "invalid packed list field in wal".to_string(),
            ));
        }
        let len = raw[len_start..offset]
            .parse::<usize>()
            .map_err(|_| StoreError::Parse("invalid packed list length in wal".to_string()))?;
        offset += 1;
        if offset + len > bytes.len() {
            return Err(StoreError::Parse(
                "packed list length exceeds wal field size".to_string(),
            ));
        }
        let value = std::str::from_utf8(&bytes[offset..offset + len])
            .map_err(|_| StoreError::Parse("invalid UTF-8 in packed list field".to_string()))?;
        out.push(value.to_string());
        offset += len;
    }
    Ok(out)
}

fn pack_f32_list(values: &[f32]) -> String {
    values
        .iter()
        .map(|value| value.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

fn unpack_f32_list(raw: &str) -> Result<Vec<f32>, StoreError> {
    if raw.trim().is_empty() {
        return Err(StoreError::Parse("vector list cannot be empty".to_string()));
    }
    let mut values = Vec::new();
    for part in raw.split(',') {
        let parsed = part
            .parse::<f32>()
            .map_err(|_| StoreError::Parse("invalid vector value in wal".to_string()))?;
        if !parsed.is_finite() {
            return Err(StoreError::Parse(
                "non-finite vector value in wal".to_string(),
            ));
        }
        values.push(parsed);
    }
    if values.is_empty() {
        return Err(StoreError::Parse("vector list cannot be empty".to_string()));
    }
    Ok(values)
}

fn parse_optional_escaped_field(raw: &str) -> Result<Option<String>, StoreError> {
    if raw == "null" {
        return Ok(None);
    }
    Ok(Some(unescape_field(raw)?))
}

fn parse_optional_u32_field(raw: &str, field: &str) -> Result<Option<u32>, StoreError> {
    if raw == "null" {
        return Ok(None);
    }
    raw.parse::<u32>()
        .map(Some)
        .map_err(|_| StoreError::Parse(format!("evidence record has invalid {field}")))
}

fn unescape_field(value: &str) -> Result<String, StoreError> {
    let mut output = String::with_capacity(value.len());
    let mut escaped = false;
    for ch in value.chars() {
        if escaped {
            match ch {
                '\\' => output.push('\\'),
                't' => output.push('\t'),
                'n' => output.push('\n'),
                other => {
                    return Err(StoreError::Parse(format!(
                        "invalid escape sequence: \\{other}"
                    )));
                }
            }
            escaped = false;
        } else if ch == '\\' {
            escaped = true;
        } else {
            output.push(ch);
        }
    }
    if escaped {
        return Err(StoreError::Parse(
            "unterminated escape sequence in wal field".to_string(),
        ));
    }
    Ok(output)
}

fn stance_to_str(stance: &Stance) -> &'static str {
    match stance {
        Stance::Supports => "supports",
        Stance::Contradicts => "contradicts",
        Stance::Neutral => "neutral",
    }
}

fn str_to_stance(raw: &str) -> Result<Stance, StoreError> {
    match raw {
        "supports" => Ok(Stance::Supports),
        "contradicts" => Ok(Stance::Contradicts),
        "neutral" => Ok(Stance::Neutral),
        _ => Err(StoreError::Parse("invalid stance in wal".to_string())),
    }
}

fn relation_to_str(relation: &Relation) -> &'static str {
    match relation {
        Relation::Supports => "supports",
        Relation::Contradicts => "contradicts",
        Relation::Refines => "refines",
        Relation::Duplicates => "duplicates",
        Relation::DependsOn => "depends_on",
    }
}

fn str_to_relation(raw: &str) -> Result<Relation, StoreError> {
    match raw {
        "supports" => Ok(Relation::Supports),
        "contradicts" => Ok(Relation::Contradicts),
        "refines" => Ok(Relation::Refines),
        "duplicates" => Ok(Relation::Duplicates),
        "depends_on" => Ok(Relation::DependsOn),
        _ => Err(StoreError::Parse("invalid relation in wal".to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use schema::{Claim, ClaimEdge, Relation, RetrievalRequest, Stance, StanceMode};
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
                    claim_type: None,
                    valid_from: None,
                    valid_to: None,
                    created_at: None,
                    updated_at: None,
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
                    doc_id: None,
                    extraction_model: None,
                    ingested_at: None,
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

        let evidence = replayed
            .evidence_by_claim
            .get("c-meta")
            .and_then(|items| items.first())
            .expect("evidence metadata should be replayed");
        assert_eq!(evidence.chunk_id.as_deref(), Some("chunk-17"));
        assert_eq!(evidence.span_start, Some(12));
        assert_eq!(evidence.span_end, Some(48));

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
