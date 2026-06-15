package dev.dash;

import java.time.Duration;

import dev.dash.internal.HttpTransport;
import dev.dash.model.DeleteRequest;
import dev.dash.model.DeleteResponse;
import dev.dash.model.EmbedRequest;
import dev.dash.model.EmbeddingResponse;
import dev.dash.model.HealthResponse;
import dev.dash.model.IngestRequest;
import dev.dash.model.IngestResponse;
import dev.dash.model.RetrievalRequest;
import dev.dash.model.RetrievalResponse;

/**
 * Synchronous client for the DASH retrieval engine.
 *
 * <p>Mirrors the layout of the official {@code openai} SDK
 * ({@code client.embeddings.create(...)}) so that switching from
 * OpenAI to DASH is a near drop-in change. The native
 * {@code /v1/retrieve} endpoint is exposed directly via
 * {@link #retrieve(RetrievalRequest)}, and the typed
 * {@link EmbeddingResponse} / {@link RetrievalResponse} objects make
 * downstream code IDE-discoverable.</p>
 *
 * <p>Example:</p>
 * <pre>
 *     DashClient client = new DashClient("http://localhost:8080", "sk-live-...");
 *     EmbeddingResponse resp = client.embed(EmbedRequest.of("hello world"));
 *     System.out.println(resp.data().get(0).embedding().subList(0, 5));
 * </pre>
 */
public class DashClient {

    private final HttpTransport transport;
    private final String apiKey;

    /**
     * Construct a client with default timeouts (connect 10s, read 30s,
     * write 30s) and default retry policy (3 attempts, 100ms base).
     */
    public DashClient(String baseUrl, String apiKey) {
        this.apiKey = apiKey;
        this.transport = new HttpTransport(baseUrl, apiKey);
    }

    /**
     * Construct a client backed by a pre-built transport. Useful for
     * sharing a configured {@link HttpTransport} across multiple
     * {@code DashClient} instances (for example, in tests).
     */
    public DashClient(HttpTransport transport) {
        this.transport = transport;
        this.apiKey = null;
    }

    // ------------------------------------------------------------------
    // Fluent configuration
    // ------------------------------------------------------------------

    /**
     * Return a new client with the given per-request connect/read/write
     * timeout applied. The original instance is left unchanged so the
     * builder is safe to share.
     */
    public DashClient withTimeout(Duration timeout) {
        HttpTransport rebuilt = new HttpTransport(
                transport.baseUrl(),
                apiKey,
                timeout,
                timeout,
                timeout,
                HttpTransport.DEFAULT_MAX_ATTEMPTS,
                HttpTransport.DEFAULT_BACKOFF_MS);
        return new DashClient(rebuilt);
    }

    /**
     * Return a new client with the given max-attempts count (initial
     * try + retries). Must be {@code >= 1}.
     */
    public DashClient withMaxRetries(int maxRetries) {
        if (maxRetries < 1) {
            throw new IllegalArgumentException("maxRetries must be >= 1");
        }
        HttpTransport rebuilt = new HttpTransport(
                transport.baseUrl(),
                apiKey,
                HttpTransport.DEFAULT_CONNECT_TIMEOUT,
                HttpTransport.DEFAULT_READ_TIMEOUT,
                HttpTransport.DEFAULT_WRITE_TIMEOUT,
                maxRetries,
                HttpTransport.DEFAULT_BACKOFF_MS);
        return new DashClient(rebuilt);
    }

    // ------------------------------------------------------------------
    // Public API
    // ------------------------------------------------------------------

    /**
     * Call {@code POST /v1/embeddings} and return a typed response.
     * The endpoint is byte-for-byte compatible with the OpenAI
     * {@code /v1/embeddings} spec.
     */
    public EmbeddingResponse embed(EmbedRequest request) {
        return transport.post("/v1/embeddings", request, EmbeddingResponse.class);
    }

    /**
     * Call {@code POST /v1/ingest} and return a typed per-bundle
     * response.
     */
    public IngestResponse ingest(IngestRequest request) {
        return transport.post("/v1/ingest", request, IngestResponse.class);
    }

    /**
     * Call {@code POST /v1/retrieve} and return the structured
     * Claim + Evidence + Contradiction response.
     */
    public RetrievalResponse retrieve(RetrievalRequest request) {
        return transport.post("/v1/retrieve", request, RetrievalResponse.class);
    }

    /**
     * Call {@code POST /v1/delete} and return a per-claim delete
     * acknowledgment.
     */
    public DeleteResponse delete(DeleteRequest request) {
        return transport.post("/v1/delete", request, DeleteResponse.class);
    }

    /**
     * Call {@code GET /health} and return the liveness response.
     */
    public HealthResponse health() {
        return transport.get("/health", HealthResponse.class);
    }
}
