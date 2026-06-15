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

import dev.dash.model.DeleteRequest;
import dev.dash.model.DeleteResponse;
import dev.dash.model.EmbedRequest;
import dev.dash.model.EmbeddingResponse;
import dev.dash.model.EmbeddingUsage;
import dev.dash.model.HealthResponse;
import dev.dash.model.IngestBundle;
import dev.dash.model.IngestClaim;
import dev.dash.model.IngestEvidence;
import dev.dash.model.IngestRequest;
import dev.dash.model.IngestResponse;
import dev.dash.model.RetrievalRequest;
import dev.dash.model.RetrievalResponse;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DashClientTest {

    private MockWebServer server;
    private DashClient client;
    private final ObjectMapper json = new ObjectMapper();

    @BeforeEach
    void setUp() throws IOException {
        server = new MockWebServer();
        server.start();
        client = new DashClient(server.url("/").toString(), "test-key");
    }

    @AfterEach
    void tearDown() throws IOException {
        server.shutdown();
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
    @DisplayName("ingest_sendsBundlesAndDecodesPerClaimStatus")
    void ingest_sendsBundlesAndDecodesPerClaimStatus() throws Exception {
        server.enqueue(new MockResponse()
                .setResponseCode(200)
                .setHeader("Content-Type", "application/json")
                .setBody("""
                        {
                          "results": [
                            {"claim_id": "c-1", "tenant_id": "acme", "canonical_text": "hi", "status": "accepted"},
                            {"claim_id": "c-2", "tenant_id": "acme", "canonical_text": "bye", "status": "rejected"}
                          ],
                          "accepted": 1,
                          "rejected": 1
                        }
                        """));

        IngestRequest req = new IngestRequest("acme", new IngestBundle(
                new IngestClaim("c-1", "acme", "hi", 0.9, null),
                List.of(new IngestEvidence("e-1", "src-1", "supports", 0.8,
                        null, null, null, null, null, "raw")),
                null));

        IngestResponse resp = client.ingest(req);

        assertThat(resp.accepted()).isEqualTo(1);
        assertThat(resp.rejected()).isEqualTo(1);
        assertThat(resp.results()).hasSize(2);
        assertThat(resp.results().get(0).claimId()).isEqualTo("c-1");
        assertThat(resp.results().get(0).status()).isEqualTo("accepted");
        assertThat(resp.results().get(1).status()).isEqualTo("rejected");

        RecordedRequest sent = server.takeRequest(1, TimeUnit.SECONDS);
        assertThat(sent.getPath()).isEqualTo("/v1/ingest");
        var body = json.readTree(sent.getBody().readUtf8());
        assertThat(body.get("tenant_id").asText()).isEqualTo("acme");
        assertThat(body.get("bundles")).hasSize(1);
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
                              "citations": [
                                {"evidence_id": "e-1", "source_id": "s-1",
                                 "stance": "supports", "source_quality": 0.95}
                              ]
                            }
                          ]
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

        RecordedRequest sent = server.takeRequest(1, TimeUnit.SECONDS);
        assertThat(sent.getPath()).isEqualTo("/v1/retrieve");
        var body = json.readTree(sent.getBody().readUtf8());
        assertThat(body.get("tenant_id").asText()).isEqualTo("acme");
        assertThat(body.get("query").asText()).isEqualTo("Q3 revenue?");
        assertThat(body.get("top_k").asInt()).isEqualTo(5);
    }

    // ------------------------------------------------------------------
    // Delete
    // ------------------------------------------------------------------

    @Test
    @DisplayName("delete_sendsClaimIdsAndDecodesResults")
    void delete_sendsClaimIdsAndDecodesResults() throws Exception {
        server.enqueue(new MockResponse()
                .setResponseCode(200)
                .setHeader("Content-Type", "application/json")
                .setBody("""
                        {
                          "deleted": 1,
                          "missing": 1,
                          "results": [
                            {"claim_id": "c-1", "status": "deleted"},
                            {"claim_id": "c-2", "status": "missing"}
                          ]
                        }
                        """));

        DeleteResponse resp = client.delete(
                new DeleteRequest("acme", List.of("c-1", "c-2"), Boolean.TRUE));

        assertThat(resp.deleted()).isEqualTo(1);
        assertThat(resp.missing()).isEqualTo(1);
        assertThat(resp.results()).hasSize(2);
        assertThat(resp.results().get(0).claimId()).isEqualTo("c-1");
        assertThat(resp.results().get(0).status()).isEqualTo("deleted");
        assertThat(resp.results().get(1).status()).isEqualTo("missing");

        RecordedRequest sent = server.takeRequest(1, TimeUnit.SECONDS);
        assertThat(sent.getPath()).isEqualTo("/v1/delete");
        var body = json.readTree(sent.getBody().readUtf8());
        assertThat(body.get("claim_ids")).hasSize(2);
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
                    assertThat(de.getErrorCode()).isEqualTo("unauthorized");
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
                .hasMessageContaining("invalid_input")
                .satisfies(err -> {
                    DashException de = (DashException) err;
                    assertThat(de.getStatusCode()).isEqualTo(400);
                    assertThat(de.getErrorCode()).isEqualTo("invalid_input");
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
        server.enqueue(new MockResponse()
                .setResponseCode(500)
                .setHeader("X-Request-Id", "req-abc-123")
                .setBody("{\"error\": {\"message\": \"bad\"}}"));

        assertThatThrownBy(() -> client.embed(EmbedRequest.of("hi")))
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
