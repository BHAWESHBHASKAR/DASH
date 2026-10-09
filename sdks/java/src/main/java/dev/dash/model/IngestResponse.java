package dev.dash.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Response body of {@code POST /v1/ingest}.
 *
 * <p>Mirrors {@code IngestApiResponse} in
 * {@code services/ingestion/src/api.rs}. {@code commitStatus} reflects
 * replication progress ({@code ackCount} of {@code requiredAcks}).</p>
 */
public record IngestResponse(
        @JsonProperty("ingested_claim_id") String ingestedClaimId,
        @JsonProperty("claims_total") int claimsTotal,
        @JsonProperty("commit_epoch") Long commitEpoch,
        @JsonProperty("ack_count") int ackCount,
        @JsonProperty("required_acks") int requiredAcks,
        @JsonProperty("commit_status") String commitStatus,
        @JsonProperty("checkpoint_triggered") boolean checkpointTriggered,
        @JsonProperty("checkpoint_snapshot_records") Integer checkpointSnapshotRecords,
        @JsonProperty("checkpoint_truncated_wal_records") Integer checkpointTruncatedWalRecords) {
}
