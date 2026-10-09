package dev.dash.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * An {@code evidence} item of {@code POST /v1/ingest}.
 *
 * <p>Mirrors {@code EvidenceWire} in {@code services/ingestion/src/api.rs}.
 * {@code stance} is one of {@code supports}, {@code contradicts} or
 * {@code neutral}; {@code sourceQuality} is in 0..1.</p>
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record IngestEvidence(
        @JsonProperty("evidence_id") String evidenceId,
        @JsonProperty("claim_id") String claimId,
        @JsonProperty("source_id") String sourceId,
        @JsonProperty("stance") String stance,
        @JsonProperty("source_quality") double sourceQuality,
        @JsonProperty("chunk_id") String chunkId,
        @JsonProperty("span_start") Integer spanStart,
        @JsonProperty("span_end") Integer spanEnd,
        @JsonProperty("doc_id") String docId,
        @JsonProperty("extraction_model") String extractionModel,
        @JsonProperty("ingested_at") Long ingestedAt) {

    /** Minimal evidence with only the required fields. */
    public IngestEvidence(String evidenceId, String claimId, String sourceId,
                          String stance, double sourceQuality) {
        this(evidenceId, claimId, sourceId, stance, sourceQuality,
                null, null, null, null, null, null);
    }
}
