package dev.dash;

import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

import dev.dash.internal.HttpTransport;
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
 * <p>DASH runs two HTTP services: retrieval (embeddings, retrieve, health;
 * default port 8080) and ingestion (ingest; default port 8081). This client
 * takes a {@code baseUrl} for the retrieval service and a separate
 * {@code ingestionBaseUrl} for the ingestion service. When no ingestion URL
 * is given it is derived only for the conventional local layout
 * ({@code baseUrl} on port 8080 maps to the same host on port 8081);
 * otherwise {@link #ingest(IngestRequest)} fails with
 * {@link IllegalStateException} until one is configured.</p>
 *
 * <p>Retries: embeddings, retrieve and health are retried on 429/5xx and I/O
 * errors (jittered backoff, {@code Retry-After} honoured). Ingest is sent
 * exactly once unless the caller passes {@link RequestOptions}.</p>
 *
 * <pre>
 *     DashClient client = new DashClient("http://localhost:8080",
 *             "http://localhost:8081", "sk-live-...");
 *     EmbeddingResponse resp = client.embed(EmbedRequest.of("hello world"));
 * </pre>
 */
public class DashClient {

    private final HttpTransport transport;
    private final HttpTransport ingestTransport;
    private final String apiKey;
    private final Duration connectTimeout;
    private final Duration readTimeout;
    private final Duration writeTimeout;
    private final int maxAttempts;

    /**
     * Construct a client with default timeouts (connect 10s, read 30s,
     * write 30s) and default retry policy (3 attempts, 100ms base). The
     * ingestion URL is derived from {@code baseUrl} when it uses port 8080.
     */
    public DashClient(String baseUrl, String apiKey) {
        this(baseUrl, null, apiKey);
    }

    /**
     * Construct a client with an explicit ingestion service URL.
     *
     * @param baseUrl          retrieval service URL (embeddings, retrieve, health)
     * @param ingestionBaseUrl ingestion service URL, or null to derive it
     * @param apiKey           bearer token, or null for none
     * @throws IllegalArgumentException if a URL is not an absolute http(s) URL
     */
    public DashClient(String baseUrl, String ingestionBaseUrl, String apiKey) {
        this(new HttpTransport(validateUrl("baseUrl", baseUrl), apiKey),
                ingestionTransport(baseUrl, ingestionBaseUrl, apiKey), apiKey, true);
    }

    private static HttpTransport ingestionTransport(String baseUrl, String ingestionBaseUrl,
                                                    String apiKey) {
        String ingestUrl = ingestionBaseUrl != null
                ? validateUrl("ingestionBaseUrl", ingestionBaseUrl)
                : deriveIngestionUrl(baseUrl);
        return ingestUrl == null ? null : new HttpTransport(ingestUrl, apiKey);
    }

    /**
     * Construct a client backed by pre-built transports. Useful for
     * sharing configured {@link HttpTransport}s (for example, in tests).
     * {@code ingestTransport} may be null.
     */
    public DashClient(HttpTransport transport, HttpTransport ingestTransport) {
        this(transport, ingestTransport, (String) null, true);
    }

    private DashClient(HttpTransport transport, HttpTransport ingestTransport,
                       String apiKey, boolean internal) {
        this(transport, ingestTransport, apiKey, HttpTransport.DEFAULT_CONNECT_TIMEOUT,
                HttpTransport.DEFAULT_READ_TIMEOUT, HttpTransport.DEFAULT_WRITE_TIMEOUT,
                HttpTransport.DEFAULT_MAX_ATTEMPTS);
    }

    private DashClient(HttpTransport transport, HttpTransport ingestTransport, String apiKey,
                       Duration connect, Duration read, Duration write, int attempts) {
        this.transport = transport;
        this.ingestTransport = ingestTransport;
        this.apiKey = apiKey;
        this.connectTimeout = connect;
        this.readTimeout = read;
        this.writeTimeout = write;
        this.maxAttempts = attempts;
    }

    /** Construct a client backed by a single pre-built transport (no ingest). */
    public DashClient(HttpTransport transport) {
        this(transport, null);
    }

    // ------------------------------------------------------------------
    // URL handling
    // ------------------------------------------------------------------

    static String validateUrl(String name, String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        URI uri;
        try {
            uri = URI.create(value.trim());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(name + " is not a valid URL: " + value, e);
        }
        String scheme = uri.getScheme();
        if (uri.getHost() == null || scheme == null
                || !(scheme.equalsIgnoreCase("http") || scheme.equalsIgnoreCase("https"))) {
            throw new IllegalArgumentException(name + " must be an absolute http(s) URL: " + value);
        }
        return value.trim();
    }

    /** baseUrl on port 8080 maps to port 8081 on the same host; otherwise null. */
    static String deriveIngestionUrl(String baseUrl) {
        URI uri = URI.create(baseUrl.trim());
        if (uri.getPort() != 8080) {
            return null;
        }
        String path = uri.getRawPath() == null ? "" : uri.getRawPath();
        return uri.getScheme() + "://" + uri.getHost() + ":8081" + path;
    }

    // ------------------------------------------------------------------
    // Fluent configuration
    // ------------------------------------------------------------------

    /**
     * Return a new client with the given per-request connect/read/write
     * timeout applied. The original instance is left unchanged.
     */
    public DashClient withTimeout(Duration timeout) {
        return rebuild(timeout, timeout, timeout, maxAttempts);
    }

    /**
     * Return a new client with the given max-attempts count (initial
     * try + retries). Must be {@code >= 1}. Applies only to requests that
     * are safe to retry.
     */
    public DashClient withMaxRetries(int maxRetries) {
        if (maxRetries < 1) {
            throw new IllegalArgumentException("maxRetries must be >= 1");
        }
        return rebuild(connectTimeout, readTimeout, writeTimeout, maxRetries);
    }

    /** Return a new client that sends ingest requests to {@code url}. */
    public DashClient withIngestionBaseUrl(String url) {
        String valid = validateUrl("ingestionBaseUrl", url);
        HttpTransport ingest = new HttpTransport(valid, apiKey, connectTimeout, readTimeout,
                writeTimeout, maxAttempts, HttpTransport.DEFAULT_BACKOFF_MS);
        return new DashClient(transport, ingest, apiKey, connectTimeout, readTimeout,
                writeTimeout, maxAttempts);
    }

    private DashClient rebuild(Duration connect, Duration read, Duration write, int attempts) {
        HttpTransport main = new HttpTransport(transport.baseUrl(), apiKey, connect, read, write,
                attempts, HttpTransport.DEFAULT_BACKOFF_MS);
        HttpTransport ingest = ingestTransport == null ? null
                : new HttpTransport(ingestTransport.baseUrl(), apiKey, connect, read, write,
                        attempts, HttpTransport.DEFAULT_BACKOFF_MS);
        return new DashClient(main, ingest, apiKey, connect, read, write, attempts);
    }

    // ------------------------------------------------------------------
    // Public API
    // ------------------------------------------------------------------

    /**
     * Call {@code POST /v1/embeddings} (retrieval service). The endpoint is
     * compatible with the OpenAI {@code /v1/embeddings} spec.
     */
    public EmbeddingResponse embed(EmbedRequest request) {
        return transport.post("/v1/embeddings", request, EmbeddingResponse.class,
                RequestOptions.IDEMPOTENT);
    }

    /**
     * EXPERIMENTAL. Call {@code POST /v1/ingest} on the ingestion service.
     * The request is sent exactly once; it is never retried.
     */
    public IngestResponse ingest(IngestRequest request) {
        return ingest(request, RequestOptions.NONE);
    }

    /**
     * EXPERIMENTAL. Call {@code POST /v1/ingest} with explicit retry
     * options, e.g. {@link RequestOptions#withIdempotencyKey(String)}.
     */
    public IngestResponse ingest(IngestRequest request, RequestOptions options) {
        if (ingestTransport == null) {
            throw new IllegalStateException(
                    "ingestionBaseUrl is not configured; pass it to the constructor "
                            + "or call withIngestionBaseUrl(...)");
        }
        return ingestTransport.post("/v1/ingest", request, IngestResponse.class, options);
    }

    /**
     * Call {@code DELETE /v1/claims/{claimId}?tenant_id=...} on the ingestion
     * service: remove the claim with its vector, evidence and every edge from
     * or to it. Needs the {@code ingest} role. Idempotent: the response has
     * {@code deleted == false} when the claim does not exist in this tenant.
     */
    public DeleteResponse deleteClaim(String tenantId, String claimId) {
        return sendDelete("/v1/claims/" + segment("claimId", claimId)
                + "?tenant_id=" + segment("tenantId", tenantId));
    }

    /**
     * Call {@code DELETE /v1/evidence/{evidenceId}?tenant_id=...}: remove
     * every evidence row with this id on the tenant's claims (the claims
     * stay). Needs the {@code ingest} role.
     */
    public DeleteResponse deleteEvidence(String tenantId, String evidenceId) {
        return sendDelete("/v1/evidence/" + segment("evidenceId", evidenceId)
                + "?tenant_id=" + segment("tenantId", tenantId));
    }

    /**
     * Call {@code DELETE /v1/tenants/{tenantId}}: erase all of the tenant's
     * data. Needs the {@code admin} role for that tenant.
     */
    public DeleteResponse deleteTenant(String tenantId) {
        return sendDelete("/v1/tenants/" + segment("tenantId", tenantId));
    }

    private DeleteResponse sendDelete(String path) {
        if (ingestTransport == null) {
            throw new IllegalStateException(
                    "ingestionBaseUrl is not configured; pass it to the constructor "
                            + "or call withIngestionBaseUrl(...)");
        }
        return ingestTransport.delete(path, DeleteResponse.class);
    }

    /** Percent-encodes one path segment or query value (space as %20). */
    static String segment(String name, String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
    }

    /**
     * Call {@code POST /v1/retrieve} (retrieval service) and return the
     * structured claim, evidence and contradiction response.
     */
    public RetrievalResponse retrieve(RetrievalRequest request) {
        return transport.post("/v1/retrieve", request, RetrievalResponse.class,
                RequestOptions.IDEMPOTENT);
    }

    /**
     * Call {@code GET /health} on the retrieval service.
     */
    public HealthResponse health() {
        return transport.get("/health", HealthResponse.class);
    }
}
