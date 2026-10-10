use store::{CheckpointPolicy, FileWal, StoreError};

use crate::IngestInput;

/// WAL size that triggers an automatic checkpoint when
/// `DASH_CHECKPOINT_MAX_WAL_BYTES` is not set (256 MiB). Followers cross a
/// checkpoint without a resync and a resync downloads the export in chunks,
/// so a checkpoint no longer costs replication anything; it bounds the WAL
/// on disk and the replay time at restart. See
/// `docs/operations/replication-limits.md`.
pub const DEFAULT_CHECKPOINT_MAX_WAL_BYTES: u64 = 256 * 1024 * 1024;

/// The ingestion service's checkpoint policy from the raw values of
/// `DASH_CHECKPOINT_MAX_WAL_RECORDS` and `DASH_CHECKPOINT_MAX_WAL_BYTES`.
/// The byte threshold defaults to [`DEFAULT_CHECKPOINT_MAX_WAL_BYTES`];
/// `0` turns a threshold off (both `0`: no automatic checkpoints). The
/// record threshold is off unless set. Unparseable values are ignored, as
/// before (the byte threshold then keeps its default).
pub fn checkpoint_policy_from_values(
    max_wal_records: Option<&str>,
    max_wal_bytes: Option<&str>,
) -> CheckpointPolicy {
    let records = max_wal_records
        .and_then(|raw| raw.trim().parse::<usize>().ok())
        .filter(|n| *n > 0);
    let bytes = match max_wal_bytes.and_then(|raw| raw.trim().parse::<u64>().ok()) {
        Some(0) => None,
        Some(n) => Some(n),
        None => Some(DEFAULT_CHECKPOINT_MAX_WAL_BYTES),
    };
    CheckpointPolicy {
        max_wal_records: records,
        max_wal_bytes: bytes,
    }
}

#[cfg(test)]
mod checkpoint_policy_tests {
    use super::*;

    #[test]
    fn bytes_default_to_256_mib_and_zero_turns_a_threshold_off() {
        let policy = checkpoint_policy_from_values(None, None);
        assert_eq!(policy.max_wal_records, None);
        assert_eq!(policy.max_wal_bytes, Some(DEFAULT_CHECKPOINT_MAX_WAL_BYTES));
        let policy = checkpoint_policy_from_values(Some("500"), Some("1048576"));
        assert_eq!(policy.max_wal_records, Some(500));
        assert_eq!(policy.max_wal_bytes, Some(1_048_576));
        let off = checkpoint_policy_from_values(Some("0"), Some("0"));
        assert_eq!(off, CheckpointPolicy::default(), "both 0: no checkpoints");
        let garbage = checkpoint_policy_from_values(Some("x"), Some("y"));
        assert_eq!(garbage.max_wal_records, None);
        assert_eq!(
            garbage.max_wal_bytes,
            Some(DEFAULT_CHECKPOINT_MAX_WAL_BYTES)
        );
    }
}

pub(super) fn map_store_error(error: &StoreError) -> (u16, String) {
    match error {
        StoreError::Validation(err) => (400, format!("validation error: {err:?}")),
        StoreError::MissingClaim(claim_id) => (400, format!("missing claim: {claim_id}")),
        StoreError::Conflict(message) => (409, format!("state conflict: {message}")),
        StoreError::InvalidVector(message) => (400, format!("invalid vector: {message}")),
        // A poisoned WAL refuses every write until restart: unavailable,
        // not a per-request failure.
        StoreError::Io(message) if message.starts_with(store::WAL_POISONED_PREFIX) => {
            (503, store::WAL_POISONED_PREFIX.to_string())
        }
        StoreError::Io(message) | StoreError::Parse(message) => {
            (500, format!("internal persistence error: {message}"))
        }
    }
}

pub(super) fn append_input_to_wal(
    wal: &mut FileWal,
    input: &IngestInput,
) -> Result<(), StoreError> {
    wal.append_claim(&input.claim)?;
    for evidence in &input.evidence {
        wal.append_evidence(evidence)?;
    }
    for edge in &input.edges {
        wal.append_edge(edge)?;
    }
    if let Some(vector) = input.claim_embedding.as_deref() {
        wal.append_claim_vector(&input.claim.claim_id, vector)?;
    }
    Ok(())
}

pub(super) fn should_checkpoint_now(
    policy: &CheckpointPolicy,
    wal: &FileWal,
) -> Result<bool, StoreError> {
    if let Some(max_wal_records) = policy.max_wal_records
        && wal.wal_record_count()? >= max_wal_records
    {
        return Ok(true);
    }
    if let Some(max_wal_bytes) = policy.max_wal_bytes
        && wal.wal_size_bytes()? >= max_wal_bytes
    {
        return Ok(true);
    }
    Ok(false)
}
