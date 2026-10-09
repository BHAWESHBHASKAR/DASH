package dev.dash.model;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Request body for {@code POST /v1/retrieve}.
 *
 * <p>Mirrors the JSON accepted by
 * {@code build_retrieve_transport_request_from_json} in
 * {@code services/retrieval/src/transport/payload.rs}. Every field except
 * {@code tenantId} and {@code query} is optional and omitted from the
 * body when null, so the server defaults apply ({@code top_k} 5,
 * {@code stance_mode} {@code balanced}, {@code return_graph} false).</p>
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record RetrievalRequest(
        @JsonProperty("tenant_id") String tenantId,
        @JsonProperty("query") String query,
        @JsonProperty("top_k") Integer topK,
        @JsonProperty("stance_mode") String stanceMode,
        @JsonProperty("return_graph") Boolean returnGraph,
        @JsonProperty("query_embedding") List<Float> queryEmbedding,
        @JsonProperty("entity_filters") List<String> entityFilters,
        @JsonProperty("embedding_id_filters") List<String> embeddingIdFilters,
        @JsonProperty("time_range") TimeRange timeRange,
        @JsonProperty("read_consistency") String readConsistency) {

    /** Inclusive unix-second time window; either bound may be null. */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record TimeRange(
            @JsonProperty("from_unix") Long fromUnix,
            @JsonProperty("to_unix") Long toUnix) {
    }

    public RetrievalRequest(String tenantId, String query, Integer topK,
                            String stanceMode, Boolean returnGraph) {
        this(tenantId, query, topK, stanceMode, returnGraph, null, null, null, null, null);
    }

    public RetrievalRequest(String tenantId, String query) {
        this(tenantId, query, null, null, null);
    }
}
