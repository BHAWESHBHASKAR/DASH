package dev.dash.model;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Response body for {@code POST /v1/ingest}.
 *
 * <p>Mirrors the per-bundle acknowledgments returned by the server.
 * The server may accept all bundles, reject some, or accept none;
 * callers should walk {@code results} and inspect {@code status}.</p>
 */
public record IngestResponse(
        @JsonProperty("results") List<IngestClaim> results,
        @JsonProperty("accepted") int accepted,
        @JsonProperty("rejected") int rejected) {
}
