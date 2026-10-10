package dev.dash.model;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Request body for {@code POST {ingestionBaseUrl}/v1/ingest}.
 *
 * <p>EXPERIMENTAL: the ingest surface will be reworked when the server
 * API is versioned. It currently matches {@code IngestApiRequestWire} in
 * {@code services/ingestion/src/api.rs}:
 * {@code {"claim": {...}, "evidence": [...], "edges": [...]}}.</p>
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record IngestRequest(
        @JsonProperty("claim") IngestClaim claim,
        @JsonProperty("evidence") List<IngestEvidence> evidence,
        @JsonProperty("edges") List<IngestEdge> edges) {

    public IngestRequest(IngestClaim claim) {
        this(claim, List.of(), List.of());
    }

    public IngestRequest(IngestClaim claim, List<IngestEvidence> evidence) {
        this(claim, evidence, List.of());
    }
}
