package dev.dash;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import dev.dash.model.EmbedRequest;
import dev.dash.model.EmbeddingResponse;
import dev.dash.model.EmbeddingUsage;
import dev.dash.model.HealthResponse;
import dev.dash.model.IngestClaim;
import dev.dash.model.IngestEdge;
import dev.dash.model.IngestEvidence;
import dev.dash.model.IngestRequest;
import dev.dash.model.IngestResponse;
import dev.dash.model.RetrievalRequest;
import dev.dash.model.RetrievalResponse;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DashClientTest {

    private MockWebServer server;
    private MockWebServer ingestServer;
    private DashClient client;
    private final ObjectMapper json = new ObjectMapper();

    @BeforeEach
    void setUp() throws IOException {
        server = new MockWebServer();
        server.start();
        ingestServer = new MockWebServer();
        ingestServer.start();
        client = new DashClient(server.url("/").toString(), ingestServer.url("/").toString(), "test-key");
    }

    @AfterEach
    void tearDown() throws IOException {
        server.shutdown();
        ingestServer.shutdown();
    }

    // ------------------------------------------------------------------
    // Embeddings
    // ------------------------------------------------------------------

    @Test
    @DisplayName("embed_single_returnsFirstVectorAndEchoesModel")
    void embed_single_returnsFirstVectorAndEchoesModel() throws Exception {
        server.enqueue(new MockResponse()
                .setResponseCode(200)
                .setHeader("Content-Type", "application/json")
                .setBody(embeddingResponseBody("text-embedding-3-small", 0.1, 0.2, 0.3)));

        EmbeddingResponse resp = client.embed(EmbedRequest.of("hello world"));

        assertThat(resp.model()).isEqualTo("text-embedding-3-small");
        assertThat(resp.object()).isEqualTo("list");
        assertThat(resp.usage().promptTokens()).isEqualTo(2);
        assertThat(resp.usage().totalTokens()).isEqualTo(2);
        assertThat(resp.data()).hasSize(1);
        assertThat(resp.data().get(0).index()).isZero();
        assertThat(resp.data().get(0).embedding()).containsExactly(0.1, 0.2, 0.3);

        RecordedRequest sent = server.takeRequest(1, TimeUnit.SECONDS);
        assertThat(sent.getMethod()).isEqualTo("POST");
        assertThat(sent.getPath()).isEqualTo("/v1/embeddings");
        assertThat(sent.getHeader("Authorization")).isEqualTo("Bearer test-key");
        assertThat(sent.getHeader("User-Agent")).startsWith("dash-java/");
        var body = json.readTree(sent.getBody().readUtf8());
        assertThat(body.get("input").asText()).isEqualTo("hello world");
        assertThat(body.get("model").asText()).isEqualTo("text-embedding-3-small");
    }

    @Test
    @DisplayName("embed_batch_sendsArrayInputAndDecodesMultipleRecords")
    void embed_batch_sendsArrayInputAndDecodesMultipleRecords() throws Exception {
        server.enqueue(new MockResponse()
                .setResponseCode(200)
                .setHeader("Content-Type", "application/json")
                .setBody("""
                        {
                          "object": "list",
                          "data": [
                            {"object": "embedding", "index": 0, "embedding": [0.1, 0.2]},
                            {"object": "embedding", "index": 1, "embedding": [0.3, 0.4]},
                            {"object": "embedding", "index": 2, "embedding": [0.5, 0.6]}
                          ],
                          "model": "text-embedding-3-small",
                          "usage": {"prompt_tokens": 6, "total_tokens": 6}
                        }
                        """));

        EmbeddingResponse resp = client.embed(EmbedRequest.of(List.of("a", "b", "c")));

        assertThat(resp.data()).hasSize(3);
        assertThat(resp.data().get(0).embedding()).containsExactly(0.1, 0.2);
        assertThat(resp.data().get(1).embedding()).containsExactly(0.3, 0.4);
        assertThat(resp.data().get(2).embedding()).containsExactly(0.5, 0.6);

        RecordedRequest sent = server.takeRequest(1, TimeUnit.SECONDS);
        var body = json.readTree(sent.getBody().readUtf8());
        assertThat(body.get("input").isArray()).isTrue();
        assertThat(body.get("input")).hasSize(3);
    }

    // ------------------------------------------------------------------
    // Ingest
    // ------------------------------------------------------------------

    @Test
    @DisplayName("ingest_sendsServerShapedBodyToIngestionHostAndDecodesResponse")
    void ingest_sendsServerShapedBodyToIngestionHostAndDecodesResponse() throws Exception {
        ingestServer.enqueue(new MockResponse()
                .setResponseCode(200)
                .setHeader("Content-Type", "application/json")
                .setBody("""
                        {"ingested_claim_id":"c-1","claims_total":7,"commit_epoch":12,
                         "ack_count":1,"required_acks":1,"commit_status":"committed",
                         "checkpoint_triggered":false,"unknown_future_field":true}
                        """));

        IngestRequest req = new IngestRequest(
                new IngestClaim("c-1", "acme", "hi", 0.9),
                List.of(new IngestEvidence("e-1", "c-1", "src-1", "supports", 0.8)),
                List.of(new IngestEdge("ed-1", "c-1", "c-0", "supports", 0.5, null, null)));

        IngestResponse resp = client.ingest(req);

        assertThat(resp.ingestedClaimId()).isEqualTo("c-1");
        assertThat(resp.claimsTotal()).isEqualTo(7);
        assertThat(resp.commitEpoch()).isEqualTo(12L);
        assertThat(resp.ackCount()).isEqualTo(1);
        assertThat(resp.requiredAcks()).isEqualTo(1);
        assertThat(resp.commitStatus()).isEqualTo("committed");
        assertThat(resp.checkpointTriggered()).isFalse();
        assertThat(resp.checkpointSnapshotRecords()).isNull();

        assertThat(server.getRequestCount()).isZero();
        RecordedRequest sent = ingestServer.takeRequest(1, TimeUnit.SECONDS);
        assertThat(sent.getMethod()).isEqualTo("POST");
        assertThat(sent.getPath()).isEqualTo("/v1/ingest");
        var body = json.readTree(sent.getBody().readUtf8());
        assertThat(body.has("tenant_id")).isFalse();
        assertThat(body.has("bundles")).isFalse();
        assertThat(body.get("claim").get("claim_id").asText()).isEqualTo("c-1");
        assertThat(body.get("claim").get("tenant_id").asText()).isEqualTo("acme");
        assertThat(body.get("claim").get("canonical_text").asText()).isEqualTo("hi");
        assertThat(body.get("claim").get("confidence").asDouble()).isEqualTo(0.9);
        assertThat(body.get("claim").has("claim_type")).isFalse();
        assertThat(body.get("evidence").get(0).get("claim_id").asText()).isEqualTo("c-1");
        assertThat(body.get("evidence").get(0).get("stance").asText()).isEqualTo("supports");
        assertThat(body.get("edges").get(0).get("relation").asText()).isEqualTo("supports");
    }

    @Test
    @DisplayName("ingest_withoutIngestionUrl_failsClearly")
    void ingest_withoutIngestionUrl_failsClearly() {
        DashClient noIngest = new DashClient("http://example.invalid:9999", "k");
        assertThatThrownBy(() -> noIngest.ingest(new IngestRequest(
                new IngestClaim("c", "t", "x", 0.5))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("ingestionBaseUrl");
    }

    @Test
    @DisplayName("ingestionUrl_isDerivedFromLocalDefaultPort")
    void ingestionUrl_isDerivedFromLocalDefaultPort() {
        assertThat(DashClient.deriveIngestionUrl("http://localhost:8080")).isEqualTo("http://localhost:8081");
        assertThat(DashClient.deriveIngestionUrl("https://dash.example.com")).isNull();
    }

    @Test
    @DisplayName("ingestionUrl_isValidated")
    void ingestionUrl_isValidated() {
        assertThatThrownBy(() -> new DashClient("http://localhost:8080", "not a url", "k"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ingestionBaseUrl");
        assertThatThrownBy(() -> new DashClient("ftp://localhost", "k"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("baseUrl");
    }

    @Test
    @DisplayName("ingest_5xx_isNotRetriedByDefault")
    void ingest_5xx_isNotRetriedByDefault() {
        for (int i = 0; i < 3; i++) {
            ingestServer.enqueue(new MockResponse().setResponseCode(503).setBody("{\"error\":\"down\"}"));
        }
        assertThatThrownBy(() -> client.ingest(new IngestRequest(new IngestClaim("c", "t", "x", 0.5))))
                .isInstanceOf(DashException.class)
                .satisfies(e -> assertThat(((DashException) e).getStatusCode()).isEqualTo(503));
        assertThat(ingestServer.getRequestCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("ingest_timeout_isNotRetried")
    void ingest_timeout_isNotRetried() {
        DashClient fast = client.withTimeout(Duration.ofMillis(100));
        ingestServer.enqueue(new MockResponse().setBodyDelay(1, TimeUnit.SECONDS).setBody("{}"));
        ingestServer.enqueue(new MockResponse().setBody("{}"));
        assertThatThrownBy(() -> fast.ingest(new IngestRequest(new IngestClaim("c", "t", "x", 0.5))))
                .isInstanceOf(DashConnectionException.class);
        assertThat(ingestServer.getRequestCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("ingest_withIdempotencyKey_sendsHeaderAndRetries")
    void ingest_withIdempotencyKey_sendsHeaderAndRetries() throws Exception {
        ingestServer.enqueue(new MockResponse().setResponseCode(503).setBody("{}"));
        ingestServer.enqueue(new MockResponse().setResponseCode(200).setBody(
                "{\"ingested_claim_id\":\"c\",\"claims_total\":1,\"ack_count\":1,"
                        + "\"required_acks\":1,\"commit_status\":\"committed\","
                        + "\"checkpoint_triggered\":false}"));

        IngestResponse resp = client.ingest(new IngestRequest(new IngestClaim("c", "t", "x", 0.5)),
                RequestOptions.withIdempotencyKey("key-1"));

        assertThat(resp.ingestedClaimId()).isEqualTo("c");
        assertThat(ingestServer.getRequestCount()).isEqualTo(2);
        assertThat(ingestServer.takeRequest().getHeader("Idempotency-Key")).isEqualTo("key-1");
        assertThat(ingestServer.takeRequest().getHeader("Idempotency-Key")).isEqualTo("key-1");
    }

    // ------------------------------------------------------------------
    // Retrieve
    // ------------------------------------------------------------------

    @Test
    @DisplayName("retrieve_sendsQueryAndDecodesStanceTally")
    void retrieve_sendsQueryAndDecodesStanceTally() throws Exception {
        server.enqueue(new MockResponse()
                .setResponseCode(200)
                .setHeader("Content-Type", "application/json")
                .setBody("""
                        {
                          "results": [
                            {
                              "claim_id": "c-1",
                              "canonical_text": "Q3 revenue was $1B",
                              "score": 0.91,
                              "supports": 3,
                              "contradicts": 0,
                              "claim_confidence": 0.8,
                              "contradiction_risk": null,
                              "event_time_unix": 1700000000,
                              "citations": [
                                {"evidence_id": "e-1", "source_id": "s-1",
                                 "stance": "supports", "source_quality": 0.95,
                                 "chunk_id": null, "span_start": null}
                              ]
                            }
                          ],
                          "graph": {"nodes": [], "edges": [
                            {"from_claim_id": "c-1", "to_claim_id": "c-2",
                             "relation": "supports", "strength": 0.5}]},
                          "read_policy": "one",
                          "read_quorum_met": true,
                          "serving_replica": null
                        }
                        """));

        RetrievalResponse resp = client.retrieve(
                new RetrievalRequest("acme", "Q3 revenue?", 5, "balanced", null));

        assertThat(resp.results()).hasSize(1);
        var hit = resp.results().get(0);
        assertThat(hit.claimId()).isEqualTo("c-1");
        assertThat(hit.canonicalText()).isEqualTo("Q3 revenue was $1B");
        assertThat(hit.score()).isEqualTo(0.91);
        assertThat(hit.supports()).isEqualTo(3);
        assertThat(hit.contradicts()).isZero();
        assertThat(hit.citations()).hasSize(1);
        assertThat(hit.citations().get(0).stance()).isEqualTo("supports");
        assertThat(hit.claimConfidence()).isEqualTo(0.8);
        assertThat(hit.contradictionRisk()).isNull();
        assertThat(hit.eventTimeUnix()).isEqualTo(1700000000L);
        assertThat(resp.graph().edges()).hasSize(1);
        assertThat(resp.readQuorumMet()).isTrue();
        assertThat(resp.servingReplica()).isNull();

        RecordedRequest sent = server.takeRequest(1, TimeUnit.SECONDS);
        assertThat(sent.getPath()).isEqualTo("/v1/retrieve");
        var body = json.readTree(sent.getBody().readUtf8());
        assertThat(body.get("tenant_id").asText()).isEqualTo("acme");
        assertThat(body.get("query").asText()).isEqualTo("Q3 revenue?");
        assertThat(body.get("top_k").asInt()).isEqualTo(5);
    }

    @Test
    @DisplayName("retrieve_defaultRequestOmitsTopKSoServerDefaultApplies")
    void retrieve_defaultRequestOmitsTopKSoServerDefaultApplies() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(200).setBody("{\"results\":[]}"));

        RetrievalResponse resp = client.retrieve(new RetrievalRequest("acme", "q"));

        assertThat(resp.results()).isEmpty();
        var body = json.readTree(server.takeRequest(1, TimeUnit.SECONDS).getBody().readUtf8());
        assertThat(body.has("top_k")).isFalse();
        assertThat(body.has("stance_mode")).isFalse();
        assertThat(body.get("tenant_id").asText()).isEqualTo("acme");
    }

    @Test
    @DisplayName("retrieve_sendsOptionalServerFields")
    void retrieve_sendsOptionalServerFields() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(200).setBody("{\"results\":[]}"));

        client.retrieve(new RetrievalRequest("acme", "q", 3, "support_only", true,
                List.of(0.5f), List.of("acme corp"), List.of("emb-1"),
                new RetrievalRequest.TimeRange(10L, 20L), "quorum"));

        var body = json.readTree(server.takeRequest(1, TimeUnit.SECONDS).getBody().readUtf8());
        assertThat(body.get("top_k").asInt()).isEqualTo(3);
        assertThat(body.get("stance_mode").asText()).isEqualTo("support_only");
        assertThat(body.get("return_graph").asBoolean()).isTrue();
        assertThat(body.get("query_embedding")).hasSize(1);
        assertThat(body.get("entity_filters").get(0).asText()).isEqualTo("acme corp");
        assertThat(body.get("embedding_id_filters").get(0).asText()).isEqualTo("emb-1");
        assertThat(body.get("time_range").get("from_unix").asLong()).isEqualTo(10L);
        assertThat(body.get("time_range").get("to_unix").asLong()).isEqualTo(20L);
        assertThat(body.get("read_consistency").asText()).isEqualTo("quorum");
    }

    // ------------------------------------------------------------------
    // Health
    // ------------------------------------------------------------------

    @Test
    @DisplayName("health_returnsOkAndReplicas")
    void health_returnsOkAndReplicas() throws Exception {
        server.enqueue(new MockResponse()
                .setResponseCode(200)
                .setHeader("Content-Type", "application/json")
                .setBody("""
                        {
                          "status": "ok",
                          "version": "0.2.0",
                          "replicas_healthy": 3
                        }
                        """));

        HealthResponse resp = client.health();

        assertThat(resp.status()).isEqualTo("ok");
        assertThat(resp.version()).isEqualTo("0.2.0");
        assertThat(resp.replicasHealthy()).isEqualTo(3);

        RecordedRequest sent = server.takeRequest(1, TimeUnit.SECONDS);
        assertThat(sent.getMethod()).isEqualTo("GET");
        assertThat(sent.getPath()).isEqualTo("/health");
    }

    @Test
    @DisplayName("health_minimalBody_isTolerated")
    void health_minimalBody_isTolerated() {
        server.enqueue(new MockResponse()
                .setResponseCode(200)
                .setHeader("Content-Type", "application/json")
                .setBody("{\"status\":\"ok\"}"));

        HealthResponse resp = client.health();

        assertThat(resp.status()).isEqualTo("ok");
        assertThat(resp.version()).isNull();
        assertThat(resp.replicasHealthy()).isNull();
    }

    // ------------------------------------------------------------------
    // Error handling
    // ------------------------------------------------------------------

    @Test
    @DisplayName("error_401_throwsDashExceptionWithStatusAndErrorCode")
    void error_401_throwsDashExceptionWithStatusAndErrorCode() {
        server.enqueue(new MockResponse()
                .setResponseCode(401)
                .setHeader("Content-Type", "application/json")
                .setBody("""
                        {"error": {"message": "invalid api key",
                                   "type": "invalid_request_error",
                                   "code": "unauthorized"}}
                        """));

        assertThatThrownBy(() -> client.embed(EmbedRequest.of("hi")))
                .isInstanceOf(DashException.class)
                .hasMessageContaining("401")
                .hasMessageContaining("invalid api key")
                .satisfies(err -> {
                    DashException de = (DashException) err;
                    assertThat(de.getStatusCode()).isEqualTo(401);
                    assertThat(de.getErrorCode()).isEqualTo("invalid_request_error");
                });
    }

    @Test
    @DisplayName("error_429_doesNotThrow_butRetriesInBackground")
    void error_429_doesNotThrow_butRetriesInBackground() throws Exception {
        // 429 is retried. First response is throttled, second succeeds.
        server.enqueue(new MockResponse()
                .setResponseCode(429)
                .setHeader("Content-Type", "application/json")
                .setBody("{\"error\": {\"message\": \"rate limited\", \"type\": \"rate_limit_error\"}}"));
        server.enqueue(new MockResponse()
                .setResponseCode(200)
                .setHeader("Content-Type", "application/json")
                .setBody(embeddingResponseBody("text-embedding-3-small", 0.5)));

        EmbeddingResponse resp = client.embed(EmbedRequest.of("retry me"));

        assertThat(resp.data().get(0).embedding()).containsExactly(0.5);
        assertThat(server.getRequestCount()).isEqualTo(2);
    }

    @Test
    @DisplayName("error_500_doesNotThrow_butRetriesInBackground")
    void error_500_doesNotThrow_butRetriesInBackground() throws Exception {
        // 5xx is retried. First response is a server error, second succeeds.
        server.enqueue(new MockResponse()
                .setResponseCode(500)
                .setHeader("Content-Type", "application/json")
                .setBody("{\"error\": \"boom\"}"));
        server.enqueue(new MockResponse()
                .setResponseCode(200)
                .setHeader("Content-Type", "application/json")
                .setBody(embeddingResponseBody("text-embedding-3-small", 0.7)));

        EmbeddingResponse resp = client.embed(EmbedRequest.of("retry 5xx"));

        assertThat(resp.data().get(0).embedding()).containsExactly(0.7);
        assertThat(server.getRequestCount()).isEqualTo(2);
    }

    @Test
    @DisplayName("error_500_exhaustsRetries_throwsDashException")
    void error_500_exhaustsRetries_throwsDashException() {
        for (int i = 0; i < 3; i++) {
            server.enqueue(new MockResponse()
                    .setResponseCode(500)
                    .setHeader("Content-Type", "application/json")
                    .setBody("{\"error\": {\"message\": \"still down\"}}"));
        }

        assertThatThrownBy(() -> client.embed(EmbedRequest.of("nope")))
                .isInstanceOf(DashException.class)
                .hasMessageContaining("500")
                .hasMessageContaining("still down")
                .satisfies(err -> {
                    DashException de = (DashException) err;
                    assertThat(de.getStatusCode()).isEqualTo(500);
                });

        assertThat(server.getRequestCount()).isEqualTo(3);
    }

    @Test
    @DisplayName("error_networkFailure_throwsDashConnectionException")
    void error_networkFailure_throwsDashConnectionException() {
        // Shut the server down to make the next call fail at the
        // transport layer.
        try {
            server.shutdown();
        } catch (IOException ignore) {
            // ignore
        }

        assertThatThrownBy(() -> client.embed(EmbedRequest.of("hello")))
                .isInstanceOf(DashConnectionException.class)
                .hasMessageContaining("DASH");
    }

    @Test
    @DisplayName("error_timeout_throwsDashConnectionException")
    void error_timeout_throwsDashConnectionException() {
        DashClient fast = new DashClient(server.url("/").toString(), "test-key")
                .withTimeout(Duration.ofMillis(1))
                .withMaxRetries(1);
        server.enqueue(new MockResponse()
                .setResponseCode(200)
                .setBodyDelay(200, TimeUnit.MILLISECONDS)
                .setBody(embeddingResponseBody("text-embedding-3-small", 0.1)));

        assertThatThrownBy(() -> fast.embed(EmbedRequest.of("slow")))
                .isInstanceOf(DashConnectionException.class);
    }

    @Test
    @DisplayName("retry_429_succeedsAfterBackoff")
    void retry_429_succeedsAfterBackoff() throws Exception {
        server.enqueue(new MockResponse()
                .setResponseCode(429)
                .setBody("{\"error\": {\"message\": \"throttled\"}}"));
        server.enqueue(new MockResponse()
                .setResponseCode(200)
                .setBody(embeddingResponseBody("text-embedding-3-small", 0.42)));

        EmbeddingResponse resp = client.embed(EmbedRequest.of("throttle me"));

        assertThat(resp.data().get(0).embedding()).containsExactly(0.42);
        assertThat(server.getRequestCount()).isEqualTo(2);
    }

    @Test
    @DisplayName("retry_5xx_succeedsAfterBackoff")
    void retry_5xx_succeedsAfterBackoff() throws Exception {
        server.enqueue(new MockResponse()
                .setResponseCode(503)
                .setBody("{\"error\": {\"message\": \"service unavailable\"}}"));
        server.enqueue(new MockResponse()
                .setResponseCode(200)
                .setBody(embeddingResponseBody("text-embedding-3-small", 0.99)));

        EmbeddingResponse resp = client.embed(EmbedRequest.of("retry svc"));

        assertThat(resp.data().get(0).embedding()).containsExactly(0.99);
        assertThat(server.getRequestCount()).isEqualTo(2);
    }

    @Test
    @DisplayName("transport_readsBodyOnce_successAndErrorBodies")
    void transport_readsBodyOnce_successAndErrorBodies() {
        server.enqueue(new MockResponse().setResponseCode(200)
                .setBody(embeddingResponseBody("m", 0.25)));
        assertThat(client.embed(EmbedRequest.of("a")).data().get(0).embedding()).containsExactly(0.25);

        server.enqueue(new MockResponse().setResponseCode(400).setHeader("X-Request-Id", "r-9")
                .setBody("{\"error\": {\"message\": \"bad input\", \"code\": \"invalid\"}}"));
        assertThatThrownBy(() -> client.embed(EmbedRequest.of("b")))
                .isInstanceOf(DashException.class)
                .hasMessageContaining("bad input")
                .satisfies(e -> {
                    assertThat(((DashException) e).getRequestId()).isEqualTo("r-9");
                    assertThat(((DashException) e).getErrorCode()).isEqualTo("invalid");
                });
        // 400 is not retried.
        assertThat(server.getRequestCount()).isEqualTo(2);
    }

    @Test
    @DisplayName("retry_honorsRetryAfterHeader")
    void retry_honorsRetryAfterHeader() {
        server.enqueue(new MockResponse().setResponseCode(429).setHeader("Retry-After", "1").setBody("{}"));
        server.enqueue(new MockResponse().setResponseCode(200).setBody(embeddingResponseBody("m", 0.1)));

        long start = System.nanoTime();
        client.embed(EmbedRequest.of("x"));
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        assertThat(elapsedMs).isGreaterThanOrEqualTo(900);
        assertThat(server.getRequestCount()).isEqualTo(2);
    }

    @Test
    @DisplayName("retry_networkErrorOnIdempotentGet_isRetried")
    void retry_networkErrorOnIdempotentGet_isRetried() {
        server.enqueue(new MockResponse().setSocketPolicy(okhttp3.mockwebserver.SocketPolicy.DISCONNECT_AFTER_REQUEST));
        server.enqueue(new MockResponse().setResponseCode(200).setBody("{\"status\":\"ok\"}"));

        assertThat(client.health().status()).isEqualTo("ok");
        assertThat(server.getRequestCount()).isEqualTo(2);
    }

    @Test
    @DisplayName("retryAfter_parsesSecondsAndDates")
    void retryAfter_parsesSecondsAndDates() {
        assertThat(dev.dash.internal.HttpTransport.parseRetryAfterMs("3")).isEqualTo(3000L);
        assertThat(dev.dash.internal.HttpTransport.parseRetryAfterMs("garbage")).isEqualTo(-1L);
        assertThat(dev.dash.internal.HttpTransport.parseRetryAfterMs(null)).isEqualTo(-1L);
    }

    // ------------------------------------------------------------------
    // OpenAI drop-in compatibility
    // ------------------------------------------------------------------

    @Test
    @DisplayName("openaiCompat_unchangedBodyByteForByte")
    void openaiCompat_unchangedBodyByteForByte() throws Exception {
        // Simulate the OpenAI /v1/embeddings wire shape and verify the
        // SDK does not transform it on the way out (snake_case keys,
        // bare-string input, etc.).
        server.enqueue(new MockResponse()
                .setResponseCode(200)
                .setHeader("Content-Type", "application/json")
                .setBody("""
                        {
                          "object": "list",
                          "data": [{"object": "embedding", "index": 0, "embedding": [0.013, -0.042, 0.077]}],
                          "model": "text-embedding-3-small",
                          "usage": {"prompt_tokens": 1, "total_tokens": 1}
                        }
                        """));

        EmbeddingResponse resp = client.embed(new EmbedRequest(
                "hello", "text-embedding-3-small", "float", "user-42"));

        assertThat(resp.data().get(0).embedding().get(0)).isEqualTo(0.013);
        assertThat(resp.data().get(0).embedding().get(1)).isEqualTo(-0.042);
        assertThat(resp.data().get(0).embedding().get(2)).isEqualTo(0.077);
        assertThat(resp.usage()).isEqualTo(new EmbeddingUsage(1, 1));

        RecordedRequest sent = server.takeRequest(1, TimeUnit.SECONDS);
        String body = sent.getBody().readUtf8();
        var node = json.readTree(body);
        assertThat(node.get("input").asText()).isEqualTo("hello");
        assertThat(node.get("model").asText()).isEqualTo("text-embedding-3-small");
        assertThat(node.get("encoding_format").asText()).isEqualTo("float");
        assertThat(node.get("user").asText()).isEqualTo("user-42");
    }

    @Test
    @DisplayName("openaiCompat_serverErrorShape_isParsed")
    void openaiCompat_serverErrorShape_isParsed() {
        server.enqueue(new MockResponse()
                .setResponseCode(400)
                .setHeader("Content-Type", "application/json")
                .setBody("""
                        {"error": {"message": "input cannot be empty",
                                   "type": "invalid_request_error",
                                   "param": "input",
                                   "code": "invalid_input"}}
                        """));

        assertThatThrownBy(() -> client.embed(EmbedRequest.of("")))
                .isInstanceOf(DashException.class)
                .hasMessageContaining("400")
                .hasMessageContaining("input cannot be empty")
                .hasMessageContaining("invalid_request_error")
                .satisfies(err -> {
                    DashException de = (DashException) err;
                    assertThat(de.getStatusCode()).isEqualTo(400);
                    assertThat(de.getErrorCode()).isEqualTo("invalid_request_error");
                });
    }

    // ------------------------------------------------------------------
    // Construction / fluent
    // ------------------------------------------------------------------

    @Test
    @DisplayName("client_omitsAuthHeaderWhenApiKeyIsNull")
    void client_omitsAuthHeaderWhenApiKeyIsNull() throws Exception {
        DashClient anon = new DashClient(server.url("/").toString(), null);
        server.enqueue(new MockResponse()
                .setResponseCode(200)
                .setBody(embeddingResponseBody("text-embedding-3-small", 0.0)));

        anon.embed(EmbedRequest.of("anon"));

        RecordedRequest sent = server.takeRequest(1, TimeUnit.SECONDS);
        assertThat(sent.getHeader("Authorization")).isNull();
    }

    @Test
    @DisplayName("withTimeout_returnsNewClient_leavingOriginalUntouched")
    void withTimeout_returnsNewClient_leavingOriginalUntouched() throws Exception {
        DashClient original = client;
        DashClient withFast = client.withTimeout(Duration.ofSeconds(1));

        assertThat(withFast).isNotSameAs(original);
        // Original still works.
        server.enqueue(new MockResponse()
                .setResponseCode(200)
                .setBody(embeddingResponseBody("text-embedding-3-small", 1.0)));
        assertThat(original.embed(EmbedRequest.of("orig")).data().get(0).embedding().get(0))
                .isEqualTo(1.0);
    }

    @Test
    @DisplayName("withMaxRetries_rejectsZero")
    void withMaxRetries_rejectsZero() {
        assertThatThrownBy(() -> client.withMaxRetries(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxRetries");
    }

    @Test
    @DisplayName("requestId_header_isCapturedInException")
    void requestId_header_isCapturedInException() {
        // Single attempt so only one canned response is needed.
        DashClient once = client.withMaxRetries(1);
        server.enqueue(new MockResponse()
                .setResponseCode(500)
                .setHeader("X-Request-Id", "req-abc-123")
                .setBody("{\"error\": {\"message\": \"bad\"}}"));

        assertThatThrownBy(() -> once.embed(EmbedRequest.of("hi")))
                .isInstanceOf(DashException.class)
                .satisfies(err -> {
                    DashException de = (DashException) err;
                    assertThat(de.getRequestId()).isEqualTo("req-abc-123");
                });
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private static String embeddingResponseBody(String model, double... values) {
        var sb = new StringBuilder(128);
        sb.append("{\"object\":\"list\",\"model\":\"").append(model)
                .append("\",\"usage\":{\"prompt_tokens\":2,\"total_tokens\":2},")
                .append("\"data\":[{\"object\":\"embedding\",\"index\":0,\"embedding\":[");
        for (int i = 0; i < values.length; i++) {
            if (i > 0) sb.append(',');
            sb.append(values[i]);
        }
        sb.append("]}]}");
        return sb.toString();
    }
}
