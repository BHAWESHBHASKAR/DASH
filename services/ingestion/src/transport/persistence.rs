use store::{CheckpointPolicy, FileWal, StoreError};

use crate::IngestInput;

pub(super) fn map_store_error(error: &StoreError) -> (u16, String) {
    match error {
        StoreError::Validation(err) => (400, format!("validation error: {err:?}")),
        StoreError::MissingClaim(claim_id) => (400, format!("missing claim: {claim_id}")),
        StoreError::Conflict(message) => (409, format!("state conflict: {message}")),
        StoreError::InvalidVector(message) => (400, format!("invalid vector: {message}")),
        StoreError::Io(message) | StoreError::Parse(message) => {
            (500, format!("internal persistence error: {message}"))
        }
    }
}

pub(super) fn append_input_to_wal(
    wal: &mut FileWal,
    input: &IngestInput,
) -> Result<(), StoreError> {
    let tenant_id = &input.claim.tenant_id;
    wal.append_claim(&input.claim)?;
    for evidence in &input.evidence {
        wal.append_evidence(evidence, tenant_id)?;
    }
    for edge in &input.edges {
        wal.append_edge(edge, tenant_id)?;
    }
    if let Some(vector) = input.claim_embedding.as_deref() {
        wal.append_claim_vector(&input.claim.claim_id, vector, tenant_id)?;
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
