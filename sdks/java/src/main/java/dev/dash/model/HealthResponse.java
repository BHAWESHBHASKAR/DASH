package dev.dash.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Response body for {@code GET /health}.
 *
 * <p>Mirrors the lightweight liveness probe exposed by the DASH
 * service. {@code status} is conventionally {@code "ok"} when the
 * service is up; presence of replica / lag fields depends on the
 * deployment.</p>
 */
public record HealthResponse(
        @JsonProperty("status") String status,
        @JsonProperty("version") String version,
        @JsonProperty("replicas_healthy") Integer replicasHealthy,
        @JsonProperty("ingest_to_visible_lag_ms_p50") Double ingestToVisibleLagMsP50,
        @JsonProperty("ingest_to_visible_lag_ms_p95") Double ingestToVisibleLagMsP95) {
}
