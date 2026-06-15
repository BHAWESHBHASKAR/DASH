package dev.dash

import dev.dash.model.DeleteRequest
import dev.dash.model.EmbedRequest
import dev.dash.model.EmbeddingResponse
import dev.dash.model.HealthResponse
import dev.dash.model.IngestBundle
import dev.dash.model.IngestClaim
import dev.dash.model.IngestRequest
import dev.dash.model.IngestResponse
import dev.dash.model.RetrievalRequest
import dev.dash.model.RetrievalResponse
import kotlinx.coroutines.test.runTest
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.util.concurrent.TimeUnit

class DashClientAsyncTest {

    private lateinit var server: MockWebServer
    private lateinit var client: DashClientAsync

    @BeforeEach
    fun setUp() {
        server = MockWebServer()
        server.start()
        client = DashClientAsync(DashClient(server.url("/").toString(), "kt-key"))
    }

    @AfterEach
    fun tearDown() {
        server.shutdown()
    }

    // ------------------------------------------------------------------
    // Happy paths
    // ------------------------------------------------------------------

    @Test
    @DisplayName("embed_single_returnsDecodedResponse")
    fun embed_single_returnsDecodedResponse() = runTest {
        server.enqueue(
            MockResponse()
                .setResponseCode(200)
                .setHeader("Content-Type", "application/json")
                .setBody(EMBEDDING_BODY)
        )

        val resp: EmbeddingResponse = client.embed(EmbedRequest.of("hello coroutine"))

        assertThat(resp.model).isEqualTo("text-embedding-3-small")
        assertThat(resp.data).hasSize(1)
        assertThat(resp.data[0].embedding).containsExactly(0.013, -0.042, 0.077)
        assertThat(resp.usage.promptTokens).isEqualTo(2)
        assertThat(resp.usage.totalTokens).isEqualTo(2)
        val sent = server.takeRequest(1, TimeUnit.SECONDS)!!
        assertThat(sent.method).isEqualTo("POST")
        assertThat(sent.path).isEqualTo("/v1/embeddings")
        assertThat(sent.getHeader("Authorization")).isEqualTo("Bearer kt-key")
    }

    @Test
    @DisplayName("retrieve_sendsQueryAndDecodesResults")
    fun retrieve_sendsQueryAndDecodesResults() = runTest {
        server.enqueue(
            MockResponse()
                .setResponseCode(200)
                .setHeader("Content-Type", "application/json")
                .setBody(RETRIEVE_BODY)
        )

        val resp: RetrievalResponse =
            client.retrieve(RetrievalRequest("acme", "Q3 revenue?", 5, "balanced", null))

        assertThat(resp.results).hasSize(1)
        val hit = resp.results[0]
        assertThat(hit.claimId).isEqualTo("c-1")
        assertThat(hit.canonicalText).isEqualTo("Q3 revenue was $1B")
        assertThat(hit.score).isEqualTo(0.91)
        assertThat(hit.supports).isEqualTo(3)
        assertThat(hit.contradicts).isZero()
        assertThat(hit.citations).hasSize(1)
        assertThat(hit.citations[0].stance).isEqualTo("supports")
        val sent = server.takeRequest(1, TimeUnit.SECONDS)!!
        assertThat(sent.path).isEqualTo("/v1/retrieve")
    }

    @Test
    @DisplayName("ingest_sendsBundlesAndDecodesResults")
    fun ingest_sendsBundlesAndDecodesResults() = runTest {
        server.enqueue(
            MockResponse()
                .setResponseCode(200)
                .setHeader("Content-Type", "application/json")
                .setBody(INGEST_BODY)
        )

        val resp: IngestResponse = client.ingest(
            IngestRequest(
                "acme",
                IngestBundle(
                    claim = IngestClaim("c-1", "acme", "hi", 0.9, null),
                    evidence = emptyList(),
                )
            )
        )

        assertThat(resp.accepted).isEqualTo(1)
        assertThat(resp.rejected).isZero()
        assertThat(resp.results).hasSize(1)
        assertThat(resp.results[0].claimId).isEqualTo("c-1")
        val sent = server.takeRequest(1, TimeUnit.SECONDS)!!
        assertThat(sent.path).isEqualTo("/v1/ingest")
    }

    @Test
    @DisplayName("delete_sendsClaimIdsAndDecodesResults")
    fun delete_sendsClaimIdsAndDecodesResults() = runTest {
        server.enqueue(
            MockResponse()
                .setResponseCode(200)
                .setHeader("Content-Type", "application/json")
                .setBody(DELETE_BODY)
        )

        val resp = client.delete(DeleteRequest("acme", listOf("c-1", "c-2"), true))

        assertThat(resp.deleted).isEqualTo(1)
        assertThat(resp.missing).isEqualTo(1)
        assertThat(resp.results).hasSize(2)
        assertThat(resp.results[0].claimId).isEqualTo("c-1")
        assertThat(resp.results[0].status).isEqualTo("deleted")
        val sent = server.takeRequest(1, TimeUnit.SECONDS)!!
        assertThat(sent.path).isEqualTo("/v1/delete")
    }

    @Test
    @DisplayName("health_returnsDecodedResponse")
    fun health_returnsDecodedResponse() = runTest {
        server.enqueue(
            MockResponse()
                .setResponseCode(200)
                .setHeader("Content-Type", "application/json")
                .setBody(HEALTH_BODY)
        )

        val resp: HealthResponse = client.health()

        assertThat(resp.status).isEqualTo("ok")
        assertThat(resp.version).isEqualTo("0.2.0")
        assertThat(resp.replicasHealthy).isEqualTo(3)
        val sent = server.takeRequest(1, TimeUnit.SECONDS)!!
        assertThat(sent.method).isEqualTo("GET")
        assertThat(sent.path).isEqualTo("/health")
    }

    // ------------------------------------------------------------------
    // Error paths
    // ------------------------------------------------------------------

    @Test
    @DisplayName("error_401_throwsDashException")
    fun error_401_throwsDashException() = runTest {
        server.enqueue(
            MockResponse()
                .setResponseCode(401)
                .setHeader("Content-Type", "application/json")
                .setBody(
                    """{"error": {"message": "invalid api key", "type": "invalid_request_error", "code": "unauthorized"}}"""
                )
        )

        assertThatThrownBy {
            kotlinx.coroutines.runBlocking { client.embed(EmbedRequest.of("hi")) }
        }
            .isInstanceOf(DashException::class.java)
            .hasMessageContaining("401")
            .hasMessageContaining("invalid api key")
            .satisfies({ err ->
                val de = err as DashException
                assertThat(de.statusCode).isEqualTo(401)
                assertThat(de.errorCode).isEqualTo("unauthorized")
            })
    }

    @Test
    @DisplayName("error_429_throwsDashException")
    fun error_429_throwsDashException() = runTest {
        server.enqueue(
            MockResponse()
                .setResponseCode(429)
                .setBody("""{"error": {"message": "rate limited", "type": "rate_limit_error"}}""")
        )

        assertThatThrownBy {
            kotlinx.coroutines.runBlocking { client.embed(EmbedRequest.of("hi")) }
        }
            .isInstanceOf(DashException::class.java)
            .hasMessageContaining("429")
            .hasMessageContaining("rate limited")
    }

    @Test
    @DisplayName("error_500_throwsDashException")
    fun error_500_throwsDashException() = runTest {
        for (i in 0 until 3) {
            server.enqueue(
                MockResponse()
                    .setResponseCode(500)
                    .setBody("""{"error": {"message": "still down"}}""")
            )
        }

        assertThatThrownBy {
            kotlinx.coroutines.runBlocking { client.embed(EmbedRequest.of("nope")) }
        }
            .isInstanceOf(DashException::class.java)
            .hasMessageContaining("500")
        assertThat(server.requestCount).isEqualTo(3)
    }

    @Test
    @DisplayName("error_networkFailure_throwsDashConnectionException")
    fun error_networkFailure_throwsDashConnectionException() {
        server.shutdown()

        assertThatThrownBy {
            kotlinx.coroutines.runBlocking { client.embed(EmbedRequest.of("hi")) }
        }
            .isInstanceOf(DashConnectionException::class.java)
    }

    @Test
    @DisplayName("retry_429_succeedsAfterBackoff")
    fun retry_429_succeedsAfterBackoff() = runTest {
        server.enqueue(
            MockResponse()
                .setResponseCode(429)
                .setBody("""{"error": {"message": "throttled"}}""")
        )
        server.enqueue(
            MockResponse()
                .setResponseCode(200)
                .setBody(EMBEDDING_BODY)
        )

        val resp = client.embed(EmbedRequest.of("retry me"))

        assertThat(resp.data[0].embedding).containsExactly(0.013, -0.042, 0.077)
        assertThat(server.requestCount).isEqualTo(2)
    }

    // ------------------------------------------------------------------
    // Wiring / convenience
    // ------------------------------------------------------------------

    @Test
    @DisplayName("sync_returnsUnderlyingDashClient")
    fun sync_returnsUnderlyingDashClient() {
        val underlying = client.sync()
        assertThat(underlying).isNotNull()
    }

    @Test
    @DisplayName("defaultConstructor_usesLocalhostBaseUrl")
    fun defaultConstructor_usesLocalhostBaseUrl() {
        // The default constructor should not throw; it just configures
        // a localhost client. We never actually call it.
        val local = DashClientAsync()
        assertThat(local.sync()).isNotNull()
    }

    companion object {
        private val EMBEDDING_BODY = """
            {
              "object": "list",
              "data": [{"object": "embedding", "index": 0, "embedding": [0.013, -0.042, 0.077]}],
              "model": "text-embedding-3-small",
              "usage": {"prompt_tokens": 2, "total_tokens": 2}
            }
        """.trimIndent()

        private val RETRIEVE_BODY = """
            {
              "results": [
                {
                  "claim_id": "c-1",
                  "canonical_text": "Q3 revenue was $1B",
                  "score": 0.91,
                  "supports": 3,
                  "contradicts": 0,
                  "citations": [
                    {"evidence_id": "e-1", "source_id": "s-1", "stance": "supports", "source_quality": 0.95}
                  ]
                }
              ]
            }
        """.trimIndent()

        private val INGEST_BODY = """
            {
              "results": [{"claim_id": "c-1", "tenant_id": "acme", "canonical_text": "hi", "status": "accepted"}],
              "accepted": 1,
              "rejected": 0
            }
        """.trimIndent()

        private val DELETE_BODY = """
            {
              "deleted": 1,
              "missing": 1,
              "results": [
                {"claim_id": "c-1", "status": "deleted"},
                {"claim_id": "c-2", "status": "missing"}
              ]
            }
        """.trimIndent()

        private val HEALTH_BODY = """
            {
              "status": "ok",
              "version": "0.2.0",
              "replicas_healthy": 3
            }
        """.trimIndent()
    }
}
