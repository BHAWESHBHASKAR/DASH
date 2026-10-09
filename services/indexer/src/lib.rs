use schema::Claim;
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet},
    fs::{File, OpenOptions, create_dir_all, read_dir, remove_file, rename},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use store::{InMemoryStore, StoreIndexStats};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Tier {
    Hot,
    Warm,
    Cold,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentPlacement {
    pub claim_id: String,
    pub tier: Tier,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TierCounts {
    pub hot: usize,
    pub warm: usize,
    pub cold: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Segment {
    pub segment_id: String,
    pub tier: Tier,
    pub claim_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionPlan {
    pub tier: Tier,
    pub segments: Vec<Segment>,
    pub merged_segment: Segment,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionSchedulerConfig {
    pub max_segments_per_tier: usize,
    pub max_compaction_input_segments: usize,
}

impl Default for CompactionSchedulerConfig {
    fn default() -> Self {
        Self {
            max_segments_per_tier: 8,
            max_compaction_input_segments: 4,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentManifestEntry {
    pub segment_id: String,
    pub tier: Tier,
    pub file_name: String,
    pub claim_count: usize,
    pub checksum: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SegmentManifest {
    pub entries: Vec<SegmentManifestEntry>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SegmentMaintenanceStats {
    pub tenant_dirs_scanned: usize,
    pub tenant_manifests_found: usize,
    pub pruned_file_count: usize,
    pub tmp_files_removed: usize,
    pub tenant_error_count: usize,
}

/// Outcome of one maintenance pass: aggregate stats plus the per-tenant
/// failures that were isolated (one tenant failing never aborts the pass).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SegmentMaintenanceReport {
    pub stats: SegmentMaintenanceStats,
    pub tenant_errors: Vec<(String, SegmentStoreError)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SegmentStoreError {
    Io(String),
    /// A segment file referenced by a manifest no longer exists (typically
    /// pruned after a newer manifest was published).
    MissingFile(String),
    Parse(String),
    Integrity(String),
}

impl From<std::io::Error> for SegmentStoreError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value.to_string())
    }
}

const MANIFEST_FILE_NAME: &str = "segments.manifest";
const MANIFEST_HEADER: &str = "DASHSEG-MANIFEST\t1";
const SEGMENT_FILE_SUFFIX: &str = ".seg";
const SEGMENT_HEADER: &str = "DASHSEG\t1";
const FINGERPRINT_FILE_NAME: &str = "segments.fingerprint";
const FINGERPRINT_HEADER: &str = "DASHSEG-FP\t1";
const TMP_SUFFIX: &str = ".tmp";
/// Default grace period before an unreferenced segment file may be deleted.
pub const DEFAULT_PRUNE_GRACE: Duration = Duration::from_secs(60);
/// How many times a reader re-reads the manifest when a segment file vanishes
/// underneath it (a newer manifest was published and the old files pruned).
const MANIFEST_RELOAD_ATTEMPTS: usize = 3;
const MAX_PLAIN_DIR_NAME_LEN: usize = 96;
const HASHED_DIR_PREFIX_LEN: usize = 48;
const HASHED_DIR_HASH_HEX_LEN: usize = 32;

static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);
static LAST_GENERATION: AtomicU64 = AtomicU64::new(0);

/// Maps a tenant id to a single, traversal-safe, collision-free directory name.
///
/// Every byte outside `[a-z0-9-]` (including upper-case letters, so the mapping
/// stays injective on case-insensitive file systems, and `_`, `.`, `/`) becomes
/// `_xx` (lower-case hex). Because the escape introducer `_` is itself escaped,
/// the mapping is injective. Names longer than 96 characters are replaced by a
/// readable prefix plus `~` plus 128 bits of SHA-256 of the full id; `~` never
/// appears in escaped names so the two forms cannot collide. The empty id maps
/// to `_`, which no escaped name can equal.
pub fn tenant_dir_name(tenant_id: &str) -> String {
    let mut tokens: Vec<String> = Vec::with_capacity(tenant_id.len());
    for byte in tenant_id.bytes() {
        match byte {
            b'a'..=b'z' | b'0'..=b'9' | b'-' => tokens.push((byte as char).to_string()),
            _ => tokens.push(format!("_{byte:02x}")),
        }
    }
    let total: usize = tokens.iter().map(String::len).sum();
    if total == 0 {
        return "_".to_string();
    }
    if total <= MAX_PLAIN_DIR_NAME_LEN {
        return tokens.concat();
    }
    let mut prefix = String::new();
    for token in &tokens {
        if prefix.len() + token.len() > HASHED_DIR_PREFIX_LEN {
            break;
        }
        prefix.push_str(token);
    }
    let digest = Sha256::digest(tenant_id.as_bytes());
    let mut hex = String::with_capacity(HASHED_DIR_HASH_HEX_LEN);
    for byte in digest.iter().take(HASHED_DIR_HASH_HEX_LEN / 2) {
        hex.push_str(&format!("{byte:02x}"));
    }
    format!("{prefix}~{hex}")
}

/// The lossy sanitizer used before `tenant_dir_name` existed. Only kept to find
/// and migrate legacy directories.
pub fn legacy_tenant_dir_name(tenant_id: &str) -> String {
    let mut out: String = tenant_id
        .chars()
        .map(|ch| match ch {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' => ch,
            _ => '_',
        })
        .collect();
    if out.is_empty() {
        out.push('_');
    }
    out
}

/// Returns `<root>/<tenant_dir_name>`, first migrating (renaming, atomically)
/// a legacy sanitized directory into place when the new one does not exist.
/// Safe to call concurrently from several threads or processes: `rename` is
/// atomic and losers of the race simply observe the migrated directory.
///
/// Note: legacy names were ambiguous, so a legacy directory that was shared by
/// colliding tenants is adopted by whichever tenant touches it first. Segment
/// data is derived and is rewritten wholesale on the next publish.
pub fn resolve_tenant_dir(root_dir: &Path, tenant_id: &str) -> PathBuf {
    let new_dir = root_dir.join(tenant_dir_name(tenant_id));
    let legacy_name = legacy_tenant_dir_name(tenant_id);
    if legacy_name == tenant_dir_name(tenant_id) {
        return new_dir;
    }
    let legacy_dir = root_dir.join(&legacy_name);
    if legacy_dir.is_dir() && !new_dir.exists() {
        match rename(&legacy_dir, &new_dir) {
            Ok(()) => {
                let _ = sync_dir(root_dir);
                eprintln!(
                    "indexer: migrated legacy segment dir '{}' -> '{}'",
                    legacy_dir.display(),
                    new_dir.display()
                );
            }
            Err(err) if legacy_dir.exists() && !new_dir.exists() => {
                eprintln!(
                    "indexer: failed to migrate legacy segment dir '{}' -> '{}': {err}",
                    legacy_dir.display(),
                    new_dir.display()
                );
            }
            Err(_) => {}
        }
    }
    new_dir
}

pub fn classify_claim_tier(claim: &Claim) -> Tier {
    if claim.confidence >= 0.85 {
        Tier::Hot
    } else if claim.confidence >= 0.6 {
        Tier::Warm
    } else {
        Tier::Cold
    }
}

pub fn preview_segment_plan(store: &InMemoryStore, claims: &[Claim]) -> Vec<SegmentPlacement> {
    let _ = store.index_stats();
    claims
        .iter()
        .map(|c| SegmentPlacement {
            claim_id: c.claim_id.clone(),
            tier: classify_claim_tier(c),
        })
        .collect()
}

pub fn summarize_tiers(claims: &[Claim]) -> TierCounts {
    let mut counts = TierCounts::default();
    for claim in claims {
        match classify_claim_tier(claim) {
            Tier::Hot => counts.hot += 1,
            Tier::Warm => counts.warm += 1,
            Tier::Cold => counts.cold += 1,
        }
    }
    counts
}

pub fn build_segments(claims: &[Claim], max_segment_size: usize) -> Vec<Segment> {
    let max_segment_size = max_segment_size.max(1);
    let mut buckets: HashMap<Tier, Vec<String>> = HashMap::new();
    for claim in claims {
        buckets
            .entry(classify_claim_tier(claim))
            .or_default()
            .push(claim.claim_id.clone());
    }

    let mut out = Vec::new();
    for tier in [Tier::Hot, Tier::Warm, Tier::Cold] {
        let mut ids = buckets.remove(&tier).unwrap_or_default();
        // Deterministic membership: identical claim sets must produce
        // byte-identical segments regardless of store iteration order.
        ids.sort_unstable();
        ids.dedup();
        for (idx, chunk) in ids.chunks(max_segment_size).enumerate() {
            out.push(Segment {
                segment_id: format!("{:?}-{}", tier, idx).to_ascii_lowercase(),
                tier: tier.clone(),
                claim_ids: chunk.to_vec(),
            });
        }
    }
    out
}

pub fn plan_tier_compaction(
    tier: Tier,
    segments: &[Segment],
    max_compaction_input_segments: usize,
) -> Option<CompactionPlan> {
    let max_compaction_input_segments = max_compaction_input_segments.max(2);
    let selected: Vec<Segment> = segments
        .iter()
        .filter(|segment| segment.tier == tier)
        .take(max_compaction_input_segments)
        .cloned()
        .collect();

    if selected.len() < 2 {
        return None;
    }

    let mut merged_ids = Vec::new();
    for segment in &selected {
        merged_ids.extend(segment.claim_ids.iter().cloned());
    }
    merged_ids.sort_unstable();
    merged_ids.dedup();

    Some(CompactionPlan {
        tier: tier.clone(),
        segments: selected,
        merged_segment: Segment {
            segment_id: format!("{:?}-merged", tier).to_ascii_lowercase(),
            tier,
            claim_ids: merged_ids,
        },
    })
}

pub fn plan_compaction_round(
    segments: &[Segment],
    config: &CompactionSchedulerConfig,
) -> Vec<CompactionPlan> {
    let max_segments_per_tier = config.max_segments_per_tier.max(1);
    let mut plans = Vec::new();
    for tier in [Tier::Hot, Tier::Warm, Tier::Cold] {
        let tier_count = segments
            .iter()
            .filter(|segment| segment.tier == tier)
            .count();
        if tier_count <= max_segments_per_tier {
            continue;
        }
        if let Some(plan) =
            plan_tier_compaction(tier, segments, config.max_compaction_input_segments)
        {
            plans.push(plan);
        }
    }
    plans
}

pub fn apply_compaction_plan(segments: &[Segment], plan: &CompactionPlan) -> Vec<Segment> {
    let remove_ids: std::collections::HashSet<&str> = plan
        .segments
        .iter()
        .map(|segment| segment.segment_id.as_str())
        .collect();
    let mut out: Vec<Segment> = segments
        .iter()
        .filter(|segment| !remove_ids.contains(segment.segment_id.as_str()))
        .cloned()
        .collect();
    out.push(plan.merged_segment.clone());
    out
}

pub fn persist_segments_atomic(
    root_dir: &Path,
    segments: &[Segment],
) -> Result<SegmentManifest, SegmentStoreError> {
    create_dir_all(root_dir)?;
    let previous_manifest = load_manifest(root_dir).ok().flatten();
    let mut entries = Vec::with_capacity(segments.len());
    for segment in segments {
        let checksum = segment_checksum(&segment.tier, &segment.claim_ids);
        // Every publish writes brand-new files; a file a live reader may hold
        // open is never overwritten.
        let (file_name, path) = loop {
            let file_name = format!(
                "{}-{:016x}-{:016x}{}",
                sanitize_segment_id(&segment.segment_id),
                stable_hash64(&segment.segment_id),
                next_generation(),
                SEGMENT_FILE_SUFFIX
            );
            let path = root_dir.join(&file_name);
            if !path.exists() {
                break (file_name, path);
            }
        };
        write_segment_file_atomic(&path, segment, checksum)?;
        entries.push(SegmentManifestEntry {
            segment_id: segment.segment_id.clone(),
            tier: segment.tier.clone(),
            file_name,
            claim_count: segment.claim_ids.len(),
            checksum,
        });
    }
    sync_dir(root_dir)?;
    let manifest = SegmentManifest { entries };
    write_manifest_atomic(root_dir, &manifest)?;

    // Files dropped by this publish become stale now: stamp them so the
    // pruning grace period counts from the swap, not from file creation.
    if let Some(previous) = previous_manifest {
        let live: HashSet<&str> = manifest
            .entries
            .iter()
            .map(|entry| entry.file_name.as_str())
            .collect();
        for entry in &previous.entries {
            if !live.contains(entry.file_name.as_str()) {
                touch_file(&root_dir.join(&entry.file_name));
            }
        }
    }
    Ok(manifest)
}

pub fn prune_unreferenced_segment_files(
    root_dir: &Path,
    active_manifest: &SegmentManifest,
    previous_manifest: Option<&SegmentManifest>,
) -> Result<usize, SegmentStoreError> {
    prune_unreferenced_segment_files_with_min_stale_age(
        root_dir,
        active_manifest,
        previous_manifest,
        DEFAULT_PRUNE_GRACE,
    )
}

pub fn prune_unreferenced_segment_files_with_min_stale_age(
    root_dir: &Path,
    active_manifest: &SegmentManifest,
    previous_manifest: Option<&SegmentManifest>,
    min_stale_age: Duration,
) -> Result<usize, SegmentStoreError> {
    // Re-read the on-disk manifest: a publish may have swapped it after the
    // caller loaded `active_manifest`, and its files must never be pruned.
    let current_on_disk = load_manifest(root_dir).ok().flatten();
    let mut keep_files: HashSet<&str> = active_manifest
        .entries
        .iter()
        .map(|entry| entry.file_name.as_str())
        .collect();
    if let Some(previous) = previous_manifest {
        keep_files.extend(
            previous
                .entries
                .iter()
                .map(|entry| entry.file_name.as_str()),
        );
    }
    if let Some(current) = current_on_disk.as_ref() {
        keep_files.extend(current.entries.iter().map(|entry| entry.file_name.as_str()));
    }

    let mut removed_total = 0usize;
    for entry in read_dir(root_dir)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let Some(file_name) = path.file_name().and_then(|value| value.to_str()) else {
            continue;
        };
        if !file_name.ends_with(SEGMENT_FILE_SUFFIX) {
            continue;
        }
        if keep_files.contains(file_name) {
            continue;
        }
        if min_stale_age > Duration::ZERO {
            let metadata = entry.metadata()?;
            if let Ok(modified) = metadata.modified()
                && let Ok(elapsed) = modified.elapsed()
                && elapsed < min_stale_age
            {
                continue;
            }
        }
        match remove_file(&path) {
            Ok(()) => {
                removed_total += 1;
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(SegmentStoreError::Io(err.to_string())),
        }
    }
    Ok(removed_total)
}

/// Removes `*.tmp` files left behind by crashed publishes. Only files whose
/// last modification is at least `min_age` old are removed so a publish that
/// is running right now (possibly in another process) is not disturbed.
pub fn cleanup_stale_tmp_files(dir: &Path, min_age: Duration) -> Result<usize, SegmentStoreError> {
    let mut removed = 0usize;
    for entry in read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
            continue;
        };
        if !name.ends_with(TMP_SUFFIX) || !path.is_file() {
            continue;
        }
        if min_age > Duration::ZERO
            && let Ok(modified) = entry.metadata()?.modified()
            && let Ok(elapsed) = modified.elapsed()
            && elapsed < min_age
        {
            continue;
        }
        match remove_file(&path) {
            Ok(()) => removed += 1,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(SegmentStoreError::Io(err.to_string())),
        }
    }
    Ok(removed)
}

/// Maintenance pass over every tenant directory. A failure in one tenant is
/// recorded in the report and does not stop the other tenants; only a failure
/// to enumerate the root itself is returned as `Err`.
pub fn maintain_segment_root_report(
    root_dir: &Path,
    min_stale_age: Duration,
) -> Result<SegmentMaintenanceReport, SegmentStoreError> {
    create_dir_all(root_dir)?;
    let mut report = SegmentMaintenanceReport::default();
    for entry in read_dir(root_dir)? {
        let entry = match entry {
            Ok(entry) => entry,
            Err(err) => {
                report
                    .tenant_errors
                    .push(("<unreadable-entry>".to_string(), err.into()));
                continue;
            }
        };
        let tenant_dir = entry.path();
        if !tenant_dir.is_dir() {
            continue;
        }
        let tenant_name = entry.file_name().to_string_lossy().to_string();
        report.stats.tenant_dirs_scanned += 1;
        if let Err(err) = maintain_tenant_dir(&tenant_dir, min_stale_age, &mut report.stats) {
            report.tenant_errors.push((tenant_name, err));
        }
    }
    report.stats.tenant_error_count = report.tenant_errors.len();
    Ok(report)
}

fn maintain_tenant_dir(
    tenant_dir: &Path,
    min_stale_age: Duration,
    stats: &mut SegmentMaintenanceStats,
) -> Result<(), SegmentStoreError> {
    stats.tmp_files_removed += cleanup_stale_tmp_files(tenant_dir, min_stale_age)?;
    let Some((manifest, _segments)) = load_current_segments(tenant_dir)? else {
        return Ok(());
    };
    stats.tenant_manifests_found += 1;
    stats.pruned_file_count += prune_unreferenced_segment_files_with_min_stale_age(
        tenant_dir,
        &manifest,
        None,
        min_stale_age,
    )?;
    Ok(())
}

/// Backwards-compatible wrapper: per-tenant failures are isolated and counted
/// in `tenant_error_count` rather than aborting the pass.
pub fn maintain_segment_root(
    root_dir: &Path,
    min_stale_age: Duration,
) -> Result<SegmentMaintenanceStats, SegmentStoreError> {
    Ok(maintain_segment_root_report(root_dir, min_stale_age)?.stats)
}

/// Strict variant: runs the full pass, then fails if any tenant failed.
pub fn maintain_segment_root_strict(
    root_dir: &Path,
    min_stale_age: Duration,
) -> Result<SegmentMaintenanceStats, SegmentStoreError> {
    let report = maintain_segment_root_report(root_dir, min_stale_age)?;
    if let Some((tenant, err)) = report.tenant_errors.first() {
        return Err(SegmentStoreError::Integrity(format!(
            "{} tenant(s) failed maintenance, first: '{tenant}': {err:?}",
            report.tenant_errors.len()
        )));
    }
    Ok(report.stats)
}

/// Loads the current manifest and its segments. If a segment file disappears
/// mid-read (a newer manifest was published and the old files were pruned) the
/// manifest is reloaded and the read retried.
pub fn load_current_segments(
    root_dir: &Path,
) -> Result<Option<(SegmentManifest, Vec<Segment>)>, SegmentStoreError> {
    let mut attempt = 0;
    loop {
        attempt += 1;
        let Some(manifest) = load_manifest(root_dir)? else {
            return Ok(None);
        };
        match load_segments_from_manifest(root_dir, &manifest) {
            Ok(segments) => return Ok(Some((manifest, segments))),
            Err(SegmentStoreError::MissingFile(_)) if attempt < MANIFEST_RELOAD_ATTEMPTS => {
                continue;
            }
            Err(err) => return Err(err),
        }
    }
}

/// Cheap, order-independent fingerprint of everything that determines segment
/// contents (claim ids and their tiers) plus a caller-provided config salt.
pub fn claim_set_fingerprint(claims: &[Claim], salt: &str) -> u64 {
    let mut sum = 0u64;
    let mut xor = 0u64;
    for claim in claims {
        let mut h = stable_hash64(&claim.claim_id);
        h = fnv1a_update(h, b"|");
        h = fnv1a_update(h, format_tier(&classify_claim_tier(claim)).as_bytes());
        let h = mix64(h);
        sum = sum.wrapping_add(h);
        xor ^= h.rotate_left(17);
    }
    let mut state = stable_hash64(salt);
    for word in [claims.len() as u64, sum, xor] {
        state = fnv1a_update(state, &word.to_le_bytes());
    }
    mix64(state)
}

#[derive(Debug, Clone)]
pub struct SegmentPublishOptions {
    pub max_segment_size: usize,
    pub scheduler: CompactionSchedulerConfig,
    pub prune_grace: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentPublishResult {
    pub claim_count: usize,
    pub segment_count: usize,
    pub compaction_plan_count: usize,
    pub stale_file_pruned_count: usize,
    /// True when the claim-set fingerprint was unchanged and no files were
    /// rebuilt or fsynced.
    pub skipped_unchanged: bool,
}

/// Publishes the segments for one tenant directory. Skips the rebuild and all
/// fsyncs when the claim-set fingerprint matches the last successful publish.
pub fn publish_claims_to_dir(
    tenant_dir: &Path,
    claims: &[Claim],
    options: &SegmentPublishOptions,
) -> Result<SegmentPublishResult, SegmentStoreError> {
    let salt = format!(
        "{}|{}|{}",
        options.max_segment_size,
        options.scheduler.max_segments_per_tier,
        options.scheduler.max_compaction_input_segments
    );
    let fingerprint = claim_set_fingerprint(claims, &salt);
    if read_fingerprint(tenant_dir) == Some(fingerprint)
        && let Ok(Some(manifest)) = load_manifest(tenant_dir)
        && manifest
            .entries
            .iter()
            .all(|entry| tenant_dir.join(&entry.file_name).is_file())
    {
        return Ok(SegmentPublishResult {
            claim_count: claims.len(),
            segment_count: manifest.entries.len(),
            compaction_plan_count: 0,
            stale_file_pruned_count: 0,
            skipped_unchanged: true,
        });
    }

    let mut segments = build_segments(claims, options.max_segment_size);
    let plans = plan_compaction_round(&segments, &options.scheduler);
    for plan in &plans {
        segments = apply_compaction_plan(&segments, plan);
    }
    let previous_manifest = load_manifest(tenant_dir).ok().flatten();
    let manifest = persist_segments_atomic(tenant_dir, &segments)?;
    write_fingerprint(tenant_dir, fingerprint)?;
    let stale_file_pruned_count = prune_unreferenced_segment_files_with_min_stale_age(
        tenant_dir,
        &manifest,
        previous_manifest.as_ref(),
        options.prune_grace,
    )?;
    Ok(SegmentPublishResult {
        claim_count: claims.len(),
        segment_count: manifest.entries.len(),
        compaction_plan_count: plans.len(),
        stale_file_pruned_count,
        skipped_unchanged: false,
    })
}

fn read_fingerprint(dir: &Path) -> Option<u64> {
    let raw = std::fs::read_to_string(dir.join(FINGERPRINT_FILE_NAME)).ok()?;
    let mut parts = raw.trim_end().split('\t');
    let header = format!("{}\t{}", parts.next()?, parts.next()?);
    if header != FINGERPRINT_HEADER {
        return None;
    }
    u64::from_str_radix(parts.next()?, 16).ok()
}

fn write_fingerprint(dir: &Path, fingerprint: u64) -> Result<(), SegmentStoreError> {
    let path = dir.join(FINGERPRINT_FILE_NAME);
    let tmp_path = temp_path(&path);
    {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&tmp_path)?;
        writeln!(file, "{FINGERPRINT_HEADER}\t{fingerprint:016x}")?;
        file.sync_all()?;
    }
    rename(tmp_path, path)?;
    sync_dir(dir)?;
    Ok(())
}

pub fn load_manifest(root_dir: &Path) -> Result<Option<SegmentManifest>, SegmentStoreError> {
    let manifest_path = root_dir.join(MANIFEST_FILE_NAME);
    if !manifest_path.exists() {
        return Ok(None);
    }
    let file = match File::open(manifest_path) {
        Ok(file) => file,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err.into()),
    };
    let mut reader = BufReader::new(file);
    let mut header = String::new();
    let header_bytes = reader.read_line(&mut header)?;
    if header_bytes == 0 {
        return Err(SegmentStoreError::Parse(
            "segment manifest is empty".to_string(),
        ));
    }
    if header.trim_end() != MANIFEST_HEADER {
        return Err(SegmentStoreError::Parse(
            "segment manifest header is invalid".to_string(),
        ));
    }

    let mut entries = Vec::new();
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let parts: Vec<&str> = line.split('\t').collect();
        if parts.len() != 5 {
            return Err(SegmentStoreError::Parse(format!(
                "segment manifest row is invalid: {line}"
            )));
        }
        let tier = parse_tier(parts[1])?;
        let claim_count = parts[3].parse::<usize>().map_err(|_| {
            SegmentStoreError::Parse("segment manifest claim_count is invalid".to_string())
        })?;
        let checksum = parts[4].parse::<u64>().map_err(|_| {
            SegmentStoreError::Parse("segment manifest checksum is invalid".to_string())
        })?;
        entries.push(SegmentManifestEntry {
            segment_id: unescape_field(parts[0])?,
            tier,
            file_name: unescape_field(parts[2])?,
            claim_count,
            checksum,
        });
    }
    Ok(Some(SegmentManifest { entries }))
}

pub fn load_segments_from_manifest(
    root_dir: &Path,
    manifest: &SegmentManifest,
) -> Result<Vec<Segment>, SegmentStoreError> {
    let mut segments = Vec::with_capacity(manifest.entries.len());
    for entry in &manifest.entries {
        let path = root_dir.join(&entry.file_name);
        let segment = read_segment_file(&path)?;
        if segment.segment_id != entry.segment_id {
            return Err(SegmentStoreError::Integrity(format!(
                "segment id mismatch for '{}'",
                entry.file_name
            )));
        }
        if segment.tier != entry.tier {
            return Err(SegmentStoreError::Integrity(format!(
                "segment tier mismatch for '{}'",
                entry.file_name
            )));
        }
        if segment.claim_ids.len() != entry.claim_count {
            return Err(SegmentStoreError::Integrity(format!(
                "segment claim count mismatch for '{}'",
                entry.file_name
            )));
        }
        let checksum = segment_checksum(&segment.tier, &segment.claim_ids);
        if checksum != entry.checksum {
            return Err(SegmentStoreError::Integrity(format!(
                "segment checksum mismatch for '{}'",
                entry.file_name
            )));
        }
        segments.push(segment);
    }
    Ok(segments)
}

pub fn indexer_health_snapshot(
    store: &InMemoryStore,
    claims: &[Claim],
) -> (StoreIndexStats, TierCounts) {
    (store.index_stats(), summarize_tiers(claims))
}

fn write_manifest_atomic(
    root_dir: &Path,
    manifest: &SegmentManifest,
) -> Result<(), SegmentStoreError> {
    let manifest_path = root_dir.join(MANIFEST_FILE_NAME);
    let tmp_path = temp_path(&manifest_path);
    {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&tmp_path)?;
        writeln!(file, "{MANIFEST_HEADER}")?;
        for entry in &manifest.entries {
            writeln!(
                file,
                "{}\t{}\t{}\t{}\t{}",
                escape_field(&entry.segment_id),
                format_tier(&entry.tier),
                escape_field(&entry.file_name),
                entry.claim_count,
                entry.checksum
            )?;
        }
        file.sync_all()?;
    }
    rename(tmp_path, manifest_path)?;
    sync_dir(root_dir)?;
    Ok(())
}

fn write_segment_file_atomic(
    path: &Path,
    segment: &Segment,
    checksum: u64,
) -> Result<(), SegmentStoreError> {
    let tmp_path = temp_path(path);
    {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&tmp_path)?;
        writeln!(
            file,
            "{SEGMENT_HEADER}\t{}\t{}\t{}\t{}",
            escape_field(&segment.segment_id),
            format_tier(&segment.tier),
            segment.claim_ids.len(),
            checksum
        )?;
        for claim_id in &segment.claim_ids {
            writeln!(file, "{}", escape_field(claim_id))?;
        }
        file.sync_all()?;
    }
    rename(tmp_path, path)?;
    Ok(())
}

fn read_segment_file(path: &Path) -> Result<Segment, SegmentStoreError> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Err(SegmentStoreError::MissingFile(path.display().to_string()));
        }
        Err(err) => return Err(err.into()),
    };
    let mut reader = BufReader::new(file);
    let mut header = String::new();
    let header_bytes = reader.read_line(&mut header)?;
    if header_bytes == 0 {
        return Err(SegmentStoreError::Parse(format!(
            "segment file '{}' is empty",
            path.display()
        )));
    }
    let header = header.trim_end();
    let parts: Vec<&str> = header.split('\t').collect();
    if parts.len() != 6 || parts[0] != "DASHSEG" || parts[1] != "1" {
        return Err(SegmentStoreError::Parse(format!(
            "segment file '{}' has invalid header",
            path.display()
        )));
    }

    let segment_id = unescape_field(parts[2])?;
    let tier = parse_tier(parts[3])?;
    let claim_count = parts[4]
        .parse::<usize>()
        .map_err(|_| SegmentStoreError::Parse("segment claim count is invalid".to_string()))?;
    let expected_checksum = parts[5]
        .parse::<u64>()
        .map_err(|_| SegmentStoreError::Parse("segment checksum is invalid".to_string()))?;

    let mut claim_ids = Vec::with_capacity(claim_count);
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        claim_ids.push(unescape_field(&line)?);
    }
    if claim_ids.len() != claim_count {
        return Err(SegmentStoreError::Integrity(format!(
            "segment '{}' claim count mismatch: header={}, body={}",
            segment_id,
            claim_count,
            claim_ids.len()
        )));
    }
    let actual_checksum = segment_checksum(&tier, &claim_ids);
    if actual_checksum != expected_checksum {
        return Err(SegmentStoreError::Integrity(format!(
            "segment '{}' checksum mismatch: expected={}, actual={}",
            segment_id, expected_checksum, actual_checksum
        )));
    }

    Ok(Segment {
        segment_id,
        tier,
        claim_ids,
    })
}

fn temp_path(path: &Path) -> PathBuf {
    let mut tmp = path.to_path_buf().into_os_string();
    tmp.push(format!(
        ".{}.{}{TMP_SUFFIX}",
        std::process::id(),
        TMP_COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    PathBuf::from(tmp)
}

fn next_generation() -> u64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| value.as_nanos() as u64)
        .unwrap_or(0);
    let mut last = LAST_GENERATION.load(Ordering::Relaxed);
    loop {
        let candidate = now.max(last.saturating_add(1));
        match LAST_GENERATION.compare_exchange_weak(
            last,
            candidate,
            Ordering::SeqCst,
            Ordering::Relaxed,
        ) {
            Ok(_) => return candidate,
            Err(observed) => last = observed,
        }
    }
}

fn touch_file(path: &Path) {
    if let Ok(file) = OpenOptions::new().write(true).open(path) {
        let _ = file.set_modified(SystemTime::now());
    }
}

#[cfg(unix)]
fn sync_dir(dir: &Path) -> Result<(), SegmentStoreError> {
    File::open(dir)?.sync_all()?;
    Ok(())
}

#[cfg(not(unix))]
fn sync_dir(_dir: &Path) -> Result<(), SegmentStoreError> {
    Ok(())
}

fn mix64(mut z: u64) -> u64 {
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
    z ^ (z >> 31)
}

fn format_tier(tier: &Tier) -> &'static str {
    match tier {
        Tier::Hot => "hot",
        Tier::Warm => "warm",
        Tier::Cold => "cold",
    }
}

fn parse_tier(raw: &str) -> Result<Tier, SegmentStoreError> {
    match raw {
        "hot" => Ok(Tier::Hot),
        "warm" => Ok(Tier::Warm),
        "cold" => Ok(Tier::Cold),
        _ => Err(SegmentStoreError::Parse(format!("tier is invalid: {raw}"))),
    }
}

fn sanitize_segment_id(segment_id: &str) -> String {
    segment_id
        .chars()
        .map(|ch| match ch {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' => ch,
            _ => '_',
        })
        .collect()
}

fn segment_checksum(tier: &Tier, claim_ids: &[String]) -> u64 {
    let mut state = stable_hash64(format_tier(tier));
    state = fnv1a_update(state, b"|");
    for claim_id in claim_ids {
        state = fnv1a_update(state, claim_id.as_bytes());
        state = fnv1a_update(state, b"\n");
    }
    state
}

fn stable_hash64(raw: &str) -> u64 {
    fnv1a_update(0xcbf29ce484222325, raw.as_bytes())
}

fn fnv1a_update(mut state: u64, bytes: &[u8]) -> u64 {
    for byte in bytes {
        state ^= *byte as u64;
        state = state.wrapping_mul(0x100000001b3);
    }
    state
}

fn escape_field(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for ch in raw.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '\t' => out.push_str("\\t"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            _ => out.push(ch),
        }
    }
    out
}

fn unescape_field(raw: &str) -> Result<String, SegmentStoreError> {
    let mut out = String::with_capacity(raw.len());
    let mut chars = raw.chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            out.push(ch);
            continue;
        }
        let Some(escaped) = chars.next() else {
            return Err(SegmentStoreError::Parse(
                "segment field has invalid escape".to_string(),
            ));
        };
        match escaped {
            '\\' => out.push('\\'),
            't' => out.push('\t'),
            'n' => out.push('\n'),
            'r' => out.push('\r'),
            _ => {
                return Err(SegmentStoreError::Parse(format!(
                    "segment field has unsupported escape: \\{escaped}"
                )));
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fs,
        path::PathBuf,
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    fn claim(claim_id: &str, confidence: f32) -> Claim {
        Claim {
            claim_id: claim_id.into(),
            tenant_id: "tenant-a".into(),
            canonical_text: "claim".into(),
            confidence,
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

    fn temp_dir(tag: &str) -> PathBuf {
        let mut out = std::env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock should be monotonic")
            .as_nanos();
        out.push(format!(
            "dash-indexer-{}-{}-{}",
            tag,
            std::process::id(),
            nanos
        ));
        out
    }

    #[test]
    fn classifies_hot_tier_for_high_confidence_claim() {
        let claim = claim("c1", 0.9);
        assert_eq!(classify_claim_tier(&claim), Tier::Hot);
    }

    #[test]
    fn builds_segments_and_compaction_plan() {
        let claims = vec![claim("c1", 0.91), claim("c2", 0.92), claim("c3", 0.93)];

        let segments = build_segments(&claims, 1);
        let hot_count = segments
            .iter()
            .filter(|segment| segment.tier == Tier::Hot)
            .count();
        assert_eq!(hot_count, 3);

        let plan = plan_tier_compaction(Tier::Hot, &segments, 2).expect("hot compaction plan");
        assert_eq!(plan.segments.len(), 2);
        assert_eq!(plan.merged_segment.claim_ids.len(), 2);
    }

    #[test]
    fn persists_and_loads_segments_with_manifest_round_trip() {
        let root = temp_dir("segment-roundtrip");
        let segments = vec![
            Segment {
                segment_id: "hot-0".into(),
                tier: Tier::Hot,
                claim_ids: vec!["claim-1".into(), "claim-2".into()],
            },
            Segment {
                segment_id: "warm-0".into(),
                tier: Tier::Warm,
                claim_ids: vec!["claim-\t3".into(), "claim-4\nline".into()],
            },
        ];

        let manifest =
            persist_segments_atomic(&root, &segments).expect("segment persist should succeed");
        assert_eq!(manifest.entries.len(), 2);

        let loaded_manifest = load_manifest(&root)
            .expect("load manifest should succeed")
            .expect("manifest should exist");
        assert_eq!(loaded_manifest.entries.len(), 2);

        let loaded_segments = load_segments_from_manifest(&root, &loaded_manifest)
            .expect("segment load should succeed");
        assert_eq!(loaded_segments, segments);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn rejects_segment_file_with_checksum_mismatch() {
        let root = temp_dir("segment-corruption");
        let segments = vec![Segment {
            segment_id: "hot-0".into(),
            tier: Tier::Hot,
            claim_ids: vec!["claim-1".into(), "claim-2".into()],
        }];

        let manifest =
            persist_segments_atomic(&root, &segments).expect("segment persist should succeed");
        let entry = manifest.entries.first().expect("entry should exist");
        let segment_path = root.join(&entry.file_name);

        let content = fs::read_to_string(&segment_path).expect("segment file should be readable");
        let mut lines: Vec<String> = content.lines().map(|line| line.to_string()).collect();
        let header = lines.first_mut().expect("header should exist");
        let mut parts: Vec<&str> = header.split('\t').collect();
        parts[5] = "0";
        *header = parts.join("\t");
        let rewritten = format!("{}\n{}\n{}\n", lines[0], lines[1], lines[2]);
        fs::write(&segment_path, rewritten).expect("segment overwrite should succeed");

        let loaded_manifest = load_manifest(&root)
            .expect("manifest load should succeed")
            .expect("manifest should exist");
        let err = load_segments_from_manifest(&root, &loaded_manifest)
            .expect_err("checksum mismatch should fail");
        assert!(matches!(err, SegmentStoreError::Integrity(_)));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn compaction_scheduler_plans_when_tier_exceeds_limit() {
        let claims = vec![
            claim("c1", 0.91),
            claim("c2", 0.92),
            claim("c3", 0.93),
            claim("c4", 0.94),
        ];
        let segments = build_segments(&claims, 1);
        let plans = plan_compaction_round(
            &segments,
            &CompactionSchedulerConfig {
                max_segments_per_tier: 2,
                max_compaction_input_segments: 3,
            },
        );
        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].tier, Tier::Hot);
        assert_eq!(plans[0].segments.len(), 3);
    }

    #[test]
    fn apply_compaction_plan_replaces_input_segments_with_merged_output() {
        let claims = vec![claim("c1", 0.91), claim("c2", 0.92), claim("c3", 0.93)];
        let segments = build_segments(&claims, 1);
        let plan = plan_tier_compaction(Tier::Hot, &segments, 2).expect("plan should exist");
        let compacted = apply_compaction_plan(&segments, &plan);

        assert_eq!(compacted.len(), 2);
        assert!(
            compacted
                .iter()
                .any(|segment| segment.segment_id.ends_with("merged"))
        );
    }

    #[test]
    fn prune_unreferenced_segment_files_keeps_active_and_previous_manifests() {
        let root = temp_dir("segment-prune");
        let first_segments = vec![
            Segment {
                segment_id: "hot-0".into(),
                tier: Tier::Hot,
                claim_ids: vec!["claim-1".into()],
            },
            Segment {
                segment_id: "warm-0".into(),
                tier: Tier::Warm,
                claim_ids: vec!["claim-2".into()],
            },
        ];
        let previous_manifest =
            persist_segments_atomic(&root, &first_segments).expect("first persist should succeed");

        let second_segments = vec![Segment {
            segment_id: "hot-0".into(),
            tier: Tier::Hot,
            claim_ids: vec!["claim-1".into(), "claim-3".into()],
        }];
        let active_manifest =
            persist_segments_atomic(&root, &second_segments).expect("second persist should work");

        let removed = prune_unreferenced_segment_files_with_min_stale_age(
            &root,
            &active_manifest,
            Some(&previous_manifest),
            Duration::ZERO,
        )
        .expect("prune should succeed");
        assert_eq!(removed, 0);
        let old_only_file = previous_manifest
            .entries
            .iter()
            .find(|entry| {
                !active_manifest
                    .entries
                    .iter()
                    .any(|current| current.file_name == entry.file_name)
            })
            .expect("old-only file should exist")
            .file_name
            .clone();
        assert!(root.join(old_only_file).exists());

        let third_segments = vec![Segment {
            segment_id: "hot-0".into(),
            tier: Tier::Hot,
            claim_ids: vec!["claim-1".into(), "claim-3".into(), "claim-4".into()],
        }];
        let latest_manifest =
            persist_segments_atomic(&root, &third_segments).expect("third persist should work");
        let removed = prune_unreferenced_segment_files_with_min_stale_age(
            &root,
            &latest_manifest,
            Some(&active_manifest),
            Duration::ZERO,
        )
        .expect("prune should remove stale files");
        // Publishes now write unique files, so both files of the first
        // manifest (hot-0 and warm-0) are stale at this point.
        assert_eq!(removed, 2);
        let old_only_exists = previous_manifest
            .entries
            .iter()
            .filter(|entry| {
                !active_manifest
                    .entries
                    .iter()
                    .any(|current| current.file_name == entry.file_name)
            })
            .any(|entry| root.join(&entry.file_name).exists());
        assert!(!old_only_exists);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn maintain_segment_root_scans_tenants_and_prunes_orphans() {
        let root = temp_dir("segment-maintenance-root");
        let tenant_a = root.join("tenant-a");
        let tenant_b = root.join("tenant-b");
        fs::create_dir_all(&tenant_b).expect("tenant-b dir should be created");

        let active_manifest = persist_segments_atomic(
            &tenant_a,
            &[Segment {
                segment_id: "hot-0".into(),
                tier: Tier::Hot,
                claim_ids: vec!["claim-1".into()],
            }],
        )
        .expect("segment persist should succeed");
        assert_eq!(active_manifest.entries.len(), 1);

        let orphan_path = tenant_a.join("orphan.seg");
        fs::write(&orphan_path, "stale segment file").expect("orphan file write should succeed");
        assert!(orphan_path.exists());

        let stats = maintain_segment_root(&root, Duration::ZERO)
            .expect("segment maintenance should succeed");
        assert_eq!(stats.tenant_dirs_scanned, 2);
        assert_eq!(stats.tenant_manifests_found, 1);
        assert_eq!(stats.pruned_file_count, 1);
        assert!(!orphan_path.exists());

        let _ = fs::remove_dir_all(root);
    }

    // ---- SEC-19: tenant directory mapping ----------------------------------

    #[test]
    fn tenant_dir_name_is_injective_for_legacy_collision_pairs() {
        let ids = [
            "a.b", "a_b", "a-b", "a b", "a/b", "a\\b", "A", "a", "a:b", "ab", "", "_", "..", ".",
            "../x", "x", "..%2fx", "é", "e", "e\u{301}", "日本", "tenant-a", "Tenant-A", "a\0b",
            "a\nb",
        ];
        let mut seen: HashMap<String, &str> = HashMap::new();
        for id in ids {
            let name = tenant_dir_name(id);
            if let Some(previous) = seen.insert(name.clone(), id) {
                panic!("'{previous}' and '{id}' both map to '{name}'");
            }
        }
    }

    #[test]
    fn tenant_dir_name_is_a_single_safe_path_component() {
        for id in [
            "../x",
            "../../etc/passwd",
            "/abs/path",
            "a/b",
            "a\\b",
            ".",
            "..",
            "",
            "\0",
            "日本語",
            &"x".repeat(10_000),
            &"/".repeat(500),
        ] {
            let name = tenant_dir_name(id);
            assert!(!name.is_empty());
            assert!(name.len() < 255, "name too long for '{id:.20}'");
            assert!(
                name.bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b"-_~".contains(&b)),
                "unsafe byte in '{name}'"
            );
            assert_ne!(name, ".");
            assert_ne!(name, "..");
            let joined = Path::new("/root").join(&name);
            assert_eq!(joined.parent(), Some(Path::new("/root")));
            assert_eq!(joined.components().count(), 3);
        }
    }

    #[test]
    fn tenant_dir_name_long_ids_stay_distinct_and_bounded() {
        let base = "t".repeat(300);
        let a = format!("{base}a");
        let b = format!("{base}b");
        let (na, nb) = (tenant_dir_name(&a), tenant_dir_name(&b));
        assert_ne!(na, nb);
        assert!(na.len() <= 96 && nb.len() <= 96);
        assert!(na.starts_with("ttttt") && na.contains('~'));
        // Escaped (short) names can never contain '~', so no cross-form clash.
        assert!(!tenant_dir_name("a~b").is_empty());
        assert!(!tenant_dir_name("a~b").contains("~b"));
        assert_eq!(tenant_dir_name("tenant-a"), "tenant-a");
    }

    #[test]
    fn resolve_tenant_dir_migrates_legacy_directory_once() {
        let root = temp_dir("migrate");
        let legacy = root.join(legacy_tenant_dir_name("acme.corp"));
        assert_eq!(legacy.file_name().unwrap(), "acme_corp");
        persist_segments_atomic(
            &legacy,
            &[Segment {
                segment_id: "hot-0".into(),
                tier: Tier::Hot,
                claim_ids: vec!["c1".into()],
            }],
        )
        .unwrap();

        let resolved = resolve_tenant_dir(&root, "acme.corp");
        assert_eq!(resolved, root.join(tenant_dir_name("acme.corp")));
        assert!(!legacy.exists(), "legacy dir must have been renamed");
        let (_, segments) = load_current_segments(&resolved).unwrap().unwrap();
        assert_eq!(segments[0].claim_ids, vec!["c1".to_string()]);

        // Second call is a no-op; a different tenant colliding on the legacy
        // name gets its own, empty directory.
        assert_eq!(resolve_tenant_dir(&root, "acme.corp"), resolved);
        let other = resolve_tenant_dir(&root, "acme_corp");
        assert_ne!(other, resolved);
        assert!(!other.exists());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn resolve_tenant_dir_concurrent_first_use_is_safe() {
        let root = temp_dir("migrate-race");
        let legacy = root.join("a_b");
        persist_segments_atomic(
            &legacy,
            &[Segment {
                segment_id: "hot-0".into(),
                tier: Tier::Hot,
                claim_ids: vec!["c1".into()],
            }],
        )
        .unwrap();
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(16));
        let handles: Vec<_> = (0..16)
            .map(|_| {
                let (root, barrier) = (root.clone(), barrier.clone());
                std::thread::spawn(move || {
                    barrier.wait();
                    resolve_tenant_dir(&root, "a.b")
                })
            })
            .collect();
        let expected = root.join(tenant_dir_name("a.b"));
        for handle in handles {
            assert_eq!(handle.join().unwrap(), expected);
        }
        assert!(!legacy.exists());
        let (_, segments) = load_current_segments(&expected).unwrap().unwrap();
        assert_eq!(segments[0].claim_ids, vec!["c1".to_string()]);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn colliding_tenants_no_longer_share_segment_directories() {
        let root = temp_dir("collide");
        let dir_a = resolve_tenant_dir(&root, "a.b");
        let dir_b = resolve_tenant_dir(&root, "a_b");
        assert_ne!(dir_a, dir_b);
        let one = |id: &str| {
            vec![Segment {
                segment_id: "hot-0".into(),
                tier: Tier::Hot,
                claim_ids: vec![id.into()],
            }]
        };
        persist_segments_atomic(&dir_a, &one("claim-a")).unwrap();
        persist_segments_atomic(&dir_b, &one("claim-b")).unwrap();
        let (_, a) = load_current_segments(&dir_a).unwrap().unwrap();
        let (_, b) = load_current_segments(&dir_b).unwrap().unwrap();
        assert_eq!(a[0].claim_ids, vec!["claim-a".to_string()]);
        assert_eq!(b[0].claim_ids, vec!["claim-b".to_string()]);
        let _ = fs::remove_dir_all(root);
    }

    // ---- IDX-07: publish safety ---------------------------------------------

    fn single_segment(ids: &[&str]) -> Vec<Segment> {
        vec![Segment {
            segment_id: "hot-0".into(),
            tier: Tier::Hot,
            claim_ids: ids.iter().map(|id| (*id).to_string()).collect(),
        }]
    }

    #[test]
    fn publish_never_overwrites_existing_segment_files() {
        let root = temp_dir("no-overwrite");
        let first = persist_segments_atomic(&root, &single_segment(&["c1"])).unwrap();
        let first_path = root.join(&first.entries[0].file_name);
        let first_bytes = fs::read(&first_path).unwrap();

        let second = persist_segments_atomic(&root, &single_segment(&["c1", "c2"])).unwrap();
        assert_ne!(first.entries[0].file_name, second.entries[0].file_name);
        assert_eq!(fs::read(&first_path).unwrap(), first_bytes);

        // Identical content still gets a fresh generation, never the same name.
        let third = persist_segments_atomic(&root, &single_segment(&["c1", "c2"])).unwrap();
        assert_ne!(second.entries[0].file_name, third.entries[0].file_name);
        assert_eq!(second.entries[0].checksum, third.entries[0].checksum);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn prune_respects_grace_period_and_current_manifest() {
        let root = temp_dir("grace");
        let first = persist_segments_atomic(&root, &single_segment(&["c1"])).unwrap();
        let second = persist_segments_atomic(&root, &single_segment(&["c2"])).unwrap();
        let old = root.join(&first.entries[0].file_name);

        let removed = prune_unreferenced_segment_files_with_min_stale_age(
            &root,
            &second,
            None,
            Duration::from_secs(60),
        )
        .unwrap();
        assert_eq!(removed, 0);
        assert!(old.exists(), "file inside the grace period must survive");

        // Passing a stale manifest must never delete files of the current one.
        let removed = prune_unreferenced_segment_files_with_min_stale_age(
            &root,
            &first,
            None,
            Duration::ZERO,
        )
        .unwrap();
        assert_eq!(removed, 0);
        assert!(root.join(&second.entries[0].file_name).exists());
        let removed = prune_unreferenced_segment_files_with_min_stale_age(
            &root,
            &second,
            None,
            Duration::ZERO,
        )
        .unwrap();
        assert_eq!(removed, 1, "only the superseded file is removed");
        assert!(!old.exists());
        assert!(root.join(&second.entries[0].file_name).exists());
        assert!(load_current_segments(&root).unwrap().is_some());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn default_prune_has_a_grace_period() {
        let root = temp_dir("default-grace");
        let first = persist_segments_atomic(&root, &single_segment(&["c1"])).unwrap();
        let second = persist_segments_atomic(&root, &single_segment(&["c2"])).unwrap();
        assert_eq!(
            prune_unreferenced_segment_files(&root, &second, None).unwrap(),
            0
        );
        assert!(root.join(&first.entries[0].file_name).exists());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn maintenance_cleans_stale_tmp_files_but_not_fresh_ones() {
        let root = temp_dir("tmp-clean");
        let tenant = root.join("t1");
        persist_segments_atomic(&tenant, &single_segment(&["c1"])).unwrap();
        let crashed = tenant.join("hot-0-0000.seg.4242.0.tmp");
        fs::write(&crashed, "partial").unwrap();

        let stats = maintain_segment_root(&root, Duration::from_secs(3600)).unwrap();
        assert_eq!(stats.tmp_files_removed, 0, "fresh tmp may be in flight");
        assert!(crashed.exists());

        let stats = maintain_segment_root(&root, Duration::ZERO).unwrap();
        assert_eq!(stats.tmp_files_removed, 1);
        assert!(!crashed.exists());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn maintenance_isolates_per_tenant_failures() {
        let root = temp_dir("isolate");
        let bad = root.join("a-bad");
        let good = root.join("z-good");
        persist_segments_atomic(&bad, &single_segment(&["c1"])).unwrap();
        persist_segments_atomic(&good, &single_segment(&["c1"])).unwrap();
        // Corrupt the bad tenant's manifest so loading it fails.
        fs::write(bad.join(MANIFEST_FILE_NAME), "garbage\n").unwrap();
        let orphan = good.join("orphan.seg");
        fs::write(&orphan, "stale").unwrap();

        let report = maintain_segment_root_report(&root, Duration::ZERO).unwrap();
        assert_eq!(report.stats.tenant_dirs_scanned, 2);
        assert_eq!(report.tenant_errors.len(), 1);
        assert_eq!(report.tenant_errors[0].0, "a-bad");
        assert_eq!(report.stats.tenant_error_count, 1);
        assert!(
            !orphan.exists(),
            "tenant after the failing one must still be maintained"
        );

        // Compat wrapper does not abort either; strict mode reports failure.
        assert!(maintain_segment_root(&root, Duration::ZERO).is_ok());
        assert!(maintain_segment_root_strict(&root, Duration::ZERO).is_err());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn load_current_segments_reloads_manifest_when_file_vanishes() {
        let root = temp_dir("reload");
        let first = persist_segments_atomic(&root, &single_segment(&["c1"])).unwrap();
        // A reader holding the old manifest sees a missing file...
        persist_segments_atomic(&root, &single_segment(&["c2"])).unwrap();
        fs::remove_file(root.join(&first.entries[0].file_name)).unwrap();
        assert!(matches!(
            load_segments_from_manifest(&root, &first),
            Err(SegmentStoreError::MissingFile(_))
        ));
        // ...while the reload-aware loader lands on the new manifest.
        let (_, segments) = load_current_segments(&root).unwrap().unwrap();
        assert_eq!(segments[0].claim_ids, vec!["c2".to_string()]);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn reader_survives_200_concurrent_publishes_with_aggressive_pruning() {
        use std::sync::atomic::{AtomicBool, Ordering};
        let root = temp_dir("race");
        persist_segments_atomic(&root, &single_segment(&["seed"])).unwrap();
        let done = std::sync::Arc::new(AtomicBool::new(false));

        let reader = {
            let (root, done) = (root.clone(), done.clone());
            std::thread::spawn(move || {
                let mut reads = 0usize;
                while !done.load(Ordering::Relaxed) {
                    match load_current_segments(&root) {
                        Ok(Some((_, segments))) => assert_eq!(segments.len(), 1),
                        Ok(None) => panic!("manifest disappeared"),
                        Err(err) => panic!("reader saw unrecovered error: {err:?}"),
                    }
                    reads += 1;
                }
                reads
            })
        };

        for generation in 0..200 {
            let ids: Vec<String> = (0..=generation).map(|i| format!("claim-{i:04}")).collect();
            let refs: Vec<&str> = ids.iter().map(String::as_str).collect();
            let manifest = persist_segments_atomic(&root, &single_segment(&refs)).unwrap();
            prune_unreferenced_segment_files_with_min_stale_age(
                &root,
                &manifest,
                None,
                Duration::from_millis(30),
            )
            .unwrap();
        }
        done.store(true, Ordering::Relaxed);
        let reads = reader.join().expect("reader must never fail");
        assert!(reads > 0);
        let (_, segments) = load_current_segments(&root).unwrap().unwrap();
        assert_eq!(segments[0].claim_ids.len(), 200);
        let _ = fs::remove_dir_all(root);
    }

    // ---- determinism and skip-unchanged -------------------------------------

    #[test]
    fn segment_membership_is_deterministic_regardless_of_input_order() {
        let forward = vec![claim("c3", 0.9), claim("c1", 0.9), claim("c2", 0.9)];
        let mut reversed = forward.clone();
        reversed.reverse();
        let a = build_segments(&forward, 10);
        let b = build_segments(&reversed, 10);
        assert_eq!(a, b);
        assert_eq!(a[0].claim_ids, vec!["c1", "c2", "c3"]);
        assert_eq!(
            segment_checksum(&a[0].tier, &a[0].claim_ids),
            segment_checksum(&b[0].tier, &b[0].claim_ids)
        );
        assert_eq!(
            claim_set_fingerprint(&forward, "s"),
            claim_set_fingerprint(&reversed, "s")
        );
    }

    fn publish_options() -> SegmentPublishOptions {
        SegmentPublishOptions {
            max_segment_size: 10,
            scheduler: CompactionSchedulerConfig::default(),
            prune_grace: Duration::ZERO,
        }
    }

    #[test]
    fn republishing_unchanged_claims_skips_rebuild_and_keeps_manifest_bytes() {
        let root = temp_dir("skip");
        let claims = vec![claim("c2", 0.9), claim("c1", 0.7), claim("c3", 0.2)];
        let first = publish_claims_to_dir(&root, &claims, &publish_options()).unwrap();
        assert!(!first.skipped_unchanged);
        let manifest_bytes = fs::read(root.join(MANIFEST_FILE_NAME)).unwrap();
        let files_before = list_seg_files(&root);

        let mut shuffled = claims.clone();
        shuffled.reverse();
        let second = publish_claims_to_dir(&root, &shuffled, &publish_options()).unwrap();
        assert!(second.skipped_unchanged);
        assert_eq!(second.segment_count, first.segment_count);
        assert_eq!(
            fs::read(root.join(MANIFEST_FILE_NAME)).unwrap(),
            manifest_bytes
        );
        assert_eq!(list_seg_files(&root), files_before);

        // A real change (new claim, or tier change) republishes.
        let mut changed = claims.clone();
        changed.push(claim("c4", 0.9));
        assert!(
            !publish_claims_to_dir(&root, &changed, &publish_options())
                .unwrap()
                .skipped_unchanged
        );
        let mut retiered = changed.clone();
        retiered[0].confidence = 0.1;
        assert!(
            !publish_claims_to_dir(&root, &retiered, &publish_options())
                .unwrap()
                .skipped_unchanged
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn skip_is_not_taken_when_segment_files_are_missing() {
        let root = temp_dir("skip-missing");
        let claims = vec![claim("c1", 0.9)];
        publish_claims_to_dir(&root, &claims, &publish_options()).unwrap();
        for file in list_seg_files(&root) {
            fs::remove_file(root.join(file)).unwrap();
        }
        let again = publish_claims_to_dir(&root, &claims, &publish_options()).unwrap();
        assert!(!again.skipped_unchanged);
        assert!(load_current_segments(&root).unwrap().is_some());
        let _ = fs::remove_dir_all(root);
    }

    fn list_seg_files(dir: &Path) -> Vec<String> {
        let mut names: Vec<String> = fs::read_dir(dir)
            .unwrap()
            .filter_map(|entry| entry.ok())
            .filter_map(|entry| entry.file_name().into_string().ok())
            .filter(|name| name.ends_with(".seg"))
            .collect();
        names.sort();
        names
    }
}
