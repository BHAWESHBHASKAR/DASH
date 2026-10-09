package dev.dash.model;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Response body for {@code POST /v1/retrieve}
 * ({@code render_retrieve_response_json} in the retrieval service).
 * Fields other than {@code results} are optional.
 */
public record RetrievalResponse(
        @JsonProperty("results") List<RetrievalHit> results,
        @JsonProperty("graph") Graph graph,
        @JsonProperty("read_policy") String readPolicy,
        @JsonProperty("read_quorum_met") Boolean readQuorumMet,
        @JsonProperty("serving_replica") String servingReplica) {

    /** Evidence graph, present only when {@code return_graph} was true. */
    public record Graph(
            @JsonProperty("nodes") List<RetrievalHit> nodes,
            @JsonProperty("edges") List<GraphEdge> edges) {
    }

    public record GraphEdge(
            @JsonProperty("from_claim_id") String fromClaimId,
            @JsonProperty("to_claim_id") String toClaimId,
            @JsonProperty("relation") String relation,
            @JsonProperty("strength") double strength) {
    }
}
