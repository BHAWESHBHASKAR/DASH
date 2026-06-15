package dev.dash.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Request body for {@code POST /v1/retrieve}.
 *
 * <p>Mirrors {@code schema::RetrievalRequest} and the test JSON in
 * {@code services/retrieval/src/transport/tests.rs}:</p>
 *
 * <pre>
 *     {"tenant_id": "...", "query": "...",
 *      "top_k": 10, "stance_mode": "balanced",
 *      "return_graph": false}
 * </pre>
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record RetrievalRequest(
        @JsonProperty("tenant_id") String tenantId,
        @JsonProperty("query") String query,
        @JsonProperty("top_k") int topK,
        @JsonProperty("stance_mode") String stanceMode,
        @JsonProperty("return_graph") Boolean returnGraph) {

    public RetrievalRequest {
        if (topK == 0) {
            topK = 10;
        }
        if (stanceMode == null || stanceMode.isEmpty()) {
            stanceMode = "balanced";
        }
    }

    public RetrievalRequest(String tenantId, String query) {
        this(tenantId, query, 10, "balanced", null);
    }
}
