package dev.dash.model;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A single ingest bundle: one claim with its evidence and (optional)
 * contradictions.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record IngestBundle(
        @JsonProperty("claim") IngestClaim claim,
        @JsonProperty("evidence") List<IngestEvidence> evidence,
        @JsonProperty("contradicts") List<IngestEvidence> contradicts) {

    public IngestBundle(IngestClaim claim, List<IngestEvidence> evidence) {
        this(claim, evidence, null);
    }
}
