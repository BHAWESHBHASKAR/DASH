use schema::Claim;
use std::{
    collections::{HashMap, HashSet},
    fs::{File, OpenOptions, create_dir_all, read_dir, remove_file, rename},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SegmentStoreError {
    Io(String),
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
        let ids = buckets.remove(&tier).unwrap_or_default();
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
    let mut entries = Vec::with_capacity(segments.len());
    for segment in segments {
        let checksum = segment_checksum(&segment.tier, &segment.claim_ids);
        let file_name = format!(
            "{}-{:016x}{}",
            sanitize_segment_id(&segment.segment_id),
            stable_hash64(&segment.segment_id),
            SEGMENT_FILE_SUFFIX
        );
        let path = root_dir.join(&file_name);
        write_segment_file_atomic(&path, segment, checksum)?;
        entries.push(SegmentManifestEntry {
            segment_id: segment.segment_id.clone(),
            tier: segment.tier.clone(),
            file_name,
            claim_count: segment.claim_ids.len(),
            checksum,
        });
    }
    let manifest = SegmentManifest { entries };
    write_manifest_atomic(root_dir, &manifest)?;
    Ok(manifest)
}

pub fn prune_unreferenced_segment_files(
    root_dir: &Path,
    active_manifest: &SegmentManifest,
    previous_manifest: Option<&SegmentManifest>,
) -> Result<usize, SegmentStoreError> {
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

pub fn load_manifest(root_dir: &Path) -> Result<Option<SegmentManifest>, SegmentStoreError> {
    let manifest_path = root_dir.join(MANIFEST_FILE_NAME);
    if !manifest_path.exists() {
        return Ok(None);
    }
    let file = File::open(manifest_path)?;
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
    let file = File::open(path)?;
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
    tmp.push(".tmp");
    PathBuf::from(tmp)
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
        time::{SystemTime, UNIX_EPOCH},
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

        let removed =
            prune_unreferenced_segment_files(&root, &active_manifest, Some(&previous_manifest))
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
        let removed =
            prune_unreferenced_segment_files(&root, &latest_manifest, Some(&active_manifest))
                .expect("prune should remove stale files");
        assert_eq!(removed, 1);
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
}
