package dev.dash.model;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Response body for {@code POST /v1/retrieve}.
 *
 * <p>Wire format is {@code {"results": [...]}}.</p>
 */
public record RetrievalResponse(
        @JsonProperty("results") List<RetrievalHit> results) {
}
