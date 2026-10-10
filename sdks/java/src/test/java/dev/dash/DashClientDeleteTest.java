package dev.dash;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import dev.dash.model.DeleteResponse;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Delete methods against a mock ingestion server. */
class DashClientDeleteTest {

    private static final String CLAIM_DELETED = "{\"deleted\":true,\"scope\":\"claim\","
            + "\"tenant_id\":\"tenant a\",\"claim_id\":\"c/1\",\"claims_deleted\":1,"
            + "\"evidence_deleted\":2,\"edges_deleted\":1,\"vectors_deleted\":1,"
            + "\"claims_total\":41,\"checkpoint_triggered\":false,\"checkpoint_deferred\":false}";

    private MockWebServer retrieval;
    private MockWebServer ingestion;
    private DashClient client;

    @BeforeEach
    void setUp() throws IOException {
        retrieval = new MockWebServer();
        retrieval.start();
        ingestion = new MockWebServer();
        ingestion.start();
        client = new DashClient(retrieval.url("/").toString(), ingestion.url("/").toString(),
                "test-key");
    }

    @AfterEach
    void tearDown() throws IOException {
        retrieval.shutdown();
        ingestion.shutdown();
    }

    private void reply(int status, String body) {
        ingestion.enqueue(new MockResponse().setResponseCode(status)
                .setHeader("Content-Type", "application/json").setBody(body));
    }

    @Test
    void deleteClaim_sendsDeleteToTheIngestionHostAndDecodesTheResponse() throws Exception {
        reply(200, CLAIM_DELETED);
        DeleteResponse resp = client.deleteClaim("tenant a", "c/1");

        RecordedRequest sent = ingestion.takeRequest(1, TimeUnit.SECONDS);
        assertThat(sent.getMethod()).isEqualTo("DELETE");
        assertThat(sent.getPath()).isEqualTo("/v1/claims/c%2F1?tenant_id=tenant%20a");
        assertThat(sent.getBodySize()).isZero();
        assertThat(sent.getHeader("Authorization")).isEqualTo("Bearer test-key");
        assertThat(retrieval.getRequestCount()).isZero();
        assertThat(resp.deleted()).isTrue();
        assertThat(resp.scope()).isEqualTo("claim");
        assertThat(resp.claimId()).isEqualTo("c/1");
        assertThat(resp.evidenceDeleted()).isEqualTo(2);
        assertThat(resp.claimsTotal()).isEqualTo(41);
    }

    @Test
    void deleteEvidenceAndTenant_useTheirRoutes() throws Exception {
        reply(200, "{\"deleted\":false,\"scope\":\"evidence\",\"tenant_id\":\"t\",\"evidence_id\":\"e1\"}");
        reply(200, "{\"deleted\":true,\"scope\":\"tenant\",\"tenant_id\":\"t\",\"claims_deleted\":3}");

        DeleteResponse evidence = client.deleteEvidence("t", "e1");
        DeleteResponse tenant = client.deleteTenant("t");

        assertThat(ingestion.takeRequest(1, TimeUnit.SECONDS).getPath())
                .isEqualTo("/v1/evidence/e1?tenant_id=t");
        assertThat(ingestion.takeRequest(1, TimeUnit.SECONDS).getPath())
                .isEqualTo("/v1/tenants/t");
        assertThat(evidence.deleted()).isFalse();
        assertThat(evidence.claimId()).isNull();
        assertThat(tenant.claimsDeleted()).isEqualTo(3);
    }

    @Test
    void delete_isRetriedOnServerErrorsBecauseItIsIdempotent() throws Exception {
        reply(503, "{\"error\":\"busy\"}");
        reply(200, CLAIM_DELETED);
        assertThat(client.deleteClaim("tenant a", "c/1").deleted()).isTrue();
        assertThat(ingestion.getRequestCount()).isEqualTo(2);
    }

    @Test
    void delete_surfacesAForbiddenAsDashException() {
        reply(403, "{\"error\":\"missing required role: admin\"}");
        assertThatThrownBy(() -> client.deleteTenant("t"))
                .isInstanceOf(DashException.class)
                .satisfies(err -> assertThat(((DashException) err).getStatusCode()).isEqualTo(403));
    }

    @Test
    void delete_withoutAnIngestionUrlOrWithBlankIdsFailsBeforeAnyRequest() {
        DashClient noIngest = new DashClient("https://dash.example.com", "k");
        assertThatThrownBy(() -> noIngest.deleteTenant("t"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("ingestionBaseUrl");
        assertThatThrownBy(() -> client.deleteClaim("", "c"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("tenantId");
        assertThatThrownBy(() -> client.deleteEvidence("t", " "))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("evidenceId");
        assertThat(ingestion.getRequestCount()).isZero();
    }
}
