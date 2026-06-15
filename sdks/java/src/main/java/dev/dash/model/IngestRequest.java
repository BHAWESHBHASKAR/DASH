package dev.dash.model;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Request body for {@code POST /v1/ingest}.
 *
 * <p>A bundle is a single claim plus its supporting/contradicting
 * evidence. The server returns a per-claim acknowledgment in
 * {@link IngestResponse}.</p>
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record IngestRequest(
        @JsonProperty("tenant_id") String tenantId,
        @JsonProperty("bundles") List<IngestBundle> bundles) {

    public IngestRequest(String tenantId, IngestBundle bundle) {
        this(tenantId, List.of(bundle));
    }
}
