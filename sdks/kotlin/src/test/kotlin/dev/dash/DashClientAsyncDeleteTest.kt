package dev.dash

import kotlinx.coroutines.test.runTest
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.concurrent.TimeUnit

class DashClientAsyncDeleteTest {

    private lateinit var ingestServer: MockWebServer
    private lateinit var client: DashClientAsync

    @BeforeEach
    fun setUp() {
        ingestServer = MockWebServer()
        ingestServer.start()
        client = DashClientAsync(
            DashClient("http://retrieval.invalid:8080", ingestServer.url("/").toString(), "kt-key")
        )
    }

    @AfterEach
    fun tearDown() {
        ingestServer.shutdown()
    }

    private fun reply(status: Int, body: String) {
        ingestServer.enqueue(
            MockResponse().setResponseCode(status)
                .setHeader("Content-Type", "application/json").setBody(body)
        )
    }

    @Test
    fun deleteMethods_callTheIngestionRoutes() = runTest {
        reply(200, """{"deleted":true,"scope":"claim","tenant_id":"t","claim_id":"c 1","claims_deleted":1}""")
        reply(200, """{"deleted":false,"scope":"evidence","tenant_id":"t","evidence_id":"e1"}""")
        reply(200, """{"deleted":true,"scope":"tenant","tenant_id":"t","claims_deleted":4}""")

        val claim = client.deleteClaim("t", "c 1")
        val evidence = client.deleteEvidence("t", "e1")
        val tenant = client.deleteTenant("t")

        val paths = (1..3).map { ingestServer.takeRequest(1, TimeUnit.SECONDS)!! }
        assertThat(paths.map { it.method }).containsOnly("DELETE")
        assertThat(paths.map { it.path }).containsExactly(
            "/v1/claims/c%201?tenant_id=t",
            "/v1/evidence/e1?tenant_id=t",
            "/v1/tenants/t",
        )
        assertThat(paths[0].getHeader("Authorization")).isEqualTo("Bearer kt-key")
        assertThat(claim.deleted()).isTrue()
        assertThat(claim.claimId()).isEqualTo("c 1")
        assertThat(evidence.deleted()).isFalse()
        assertThat(tenant.claimsDeleted()).isEqualTo(4)
    }

    @Test
    fun deleteTenant_forbidden_throwsDashException() = runTest {
        reply(403, """{"error":"missing required role: admin"}""")
        assertThatThrownBy { kotlinx.coroutines.runBlocking { client.deleteTenant("t") } }
            .isInstanceOf(DashException::class.java)
    }
}
