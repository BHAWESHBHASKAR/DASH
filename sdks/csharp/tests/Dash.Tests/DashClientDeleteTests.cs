using System;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using FluentAssertions;
using Xunit;

namespace Dash.Tests;

/// <summary>
/// Tests for the scoped delete methods (<c>DELETE /v1/claims/{id}</c>,
/// <c>/v1/evidence/{id}</c>, <c>/v1/tenants/{id}</c> on the ingestion
/// service). HTTP is mocked at the <see cref="HttpMessageHandler"/>
/// boundary.
/// </summary>
public class DashClientDeleteTests
{
    private const string IngestionUrl = "http://localhost:8081";

    [Fact]
    public async Task DeleteClaimAsync_SendsDeleteToTheIngestionHostAndDecodesTheResponse()
    {
        var handler = new FakeHttpMessageHandler()
            .Enqueue(HttpStatusCode.OK, TestData.SampleDeleteClaimResponseJson);
        using var client = NewClient(handler, apiKey: "sk-test-123");

        var response = await client.DeleteClaimAsync("t1", "c1");

        response.Deleted.Should().BeTrue();
        response.Scope.Should().Be("claim");
        response.TenantId.Should().Be("t1");
        response.ClaimId.Should().Be("c1");
        response.EvidenceId.Should().BeNull();
        response.ClaimsDeleted.Should().Be(1);
        response.EvidenceDeleted.Should().Be(2);
        response.EdgesDeleted.Should().Be(1);
        response.VectorsDeleted.Should().Be(1);
        response.ClaimsTotal.Should().Be(41);
        response.CheckpointTriggered.Should().BeFalse();
        response.CheckpointDeferred.Should().BeFalse();

        var request = handler.Requests.Single();
        request.Method.Should().Be(HttpMethod.Delete);
        request.RequestUri!.AbsoluteUri.Should().Be("http://localhost:8081/v1/claims/c1?tenant_id=t1");
        request.Headers.Authorization!.ToString().Should().Be("Bearer sk-test-123");
        handler.RequestBodies.Single().Should().BeNull();
    }

    [Fact]
    public async Task DeleteEvidenceAsync_SendsTenantInTheQuery()
    {
        var handler = new FakeHttpMessageHandler()
            .Enqueue(HttpStatusCode.OK, TestData.SampleDeleteEvidenceResponseJson);
        using var client = NewClient(handler);

        var response = await client.DeleteEvidenceAsync("t1", "ev-1");

        response.Deleted.Should().BeTrue();
        response.Scope.Should().Be("evidence");
        response.EvidenceId.Should().Be("ev-1");
        response.ClaimId.Should().BeNull();
        response.EvidenceDeleted.Should().Be(3);

        var request = handler.Requests.Single();
        request.Method.Should().Be(HttpMethod.Delete);
        request.RequestUri!.AbsoluteUri.Should().Be("http://localhost:8081/v1/evidence/ev-1?tenant_id=t1");
    }

    [Fact]
    public async Task DeleteTenantAsync_SendsNoTenantQuery()
    {
        var handler = new FakeHttpMessageHandler()
            .Enqueue(HttpStatusCode.OK, TestData.SampleDeleteTenantResponseJson);
        using var client = NewClient(handler);

        var response = await client.DeleteTenantAsync("t1");

        response.Deleted.Should().BeTrue();
        response.Scope.Should().Be("tenant");
        response.ClaimsDeleted.Should().Be(5);
        response.ClaimsTotal.Should().Be(0);

        var request = handler.Requests.Single();
        request.Method.Should().Be(HttpMethod.Delete);
        request.RequestUri!.AbsoluteUri.Should().Be("http://localhost:8081/v1/tenants/t1");
        request.RequestUri!.Query.Should().BeEmpty();
    }

    [Fact]
    public async Task Deletes_PercentEncodePathSegmentsAndQueryValues()
    {
        var handler = new FakeHttpMessageHandler()
            .Enqueue(HttpStatusCode.OK, TestData.SampleDeleteClaimResponseJson)
            .Enqueue(HttpStatusCode.OK, TestData.SampleDeleteEvidenceResponseJson)
            .Enqueue(HttpStatusCode.OK, TestData.SampleDeleteTenantResponseJson);
        using var client = NewClient(handler);

        await client.DeleteClaimAsync("acme corp&x=1", "a/b c+d?e#f");
        await client.DeleteEvidenceAsync("t+1", "ev/é");
        await client.DeleteTenantAsync("acme/../x y");

        handler.Requests[0].RequestUri!.AbsoluteUri.Should().Be(
            "http://localhost:8081/v1/claims/a%2Fb%20c%2Bd%3Fe%23f?tenant_id=acme%20corp%26x%3D1");
        handler.Requests[1].RequestUri!.AbsoluteUri.Should().Be(
            "http://localhost:8081/v1/evidence/ev%2F%C3%A9?tenant_id=t%2B1");
        handler.Requests[2].RequestUri!.AbsoluteUri.Should().Be(
            "http://localhost:8081/v1/tenants/acme%2F..%2Fx%20y");
    }

    [Fact]
    public async Task DeleteClaimAsync_NothingToDelete_ReturnsDeletedFalse()
    {
        var handler = new FakeHttpMessageHandler()
            .Enqueue(HttpStatusCode.OK, TestData.SampleDeleteNothingResponseJson);
        using var client = NewClient(handler);

        var response = await client.DeleteClaimAsync("t1", "missing");

        response.Deleted.Should().BeFalse();
        response.ClaimsDeleted.Should().Be(0);
        response.ClaimsTotal.Should().Be(41);
    }

    [Fact]
    public async Task Deletes_UseTheDerivedIngestionUrl()
    {
        var handler = new FakeHttpMessageHandler()
            .Enqueue(HttpStatusCode.OK, TestData.SampleDeleteTenantResponseJson);
        using var client = new DashClient("http://localhost:8080", null, new DashClientOptions
        {
            HttpClient = new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(5) },
        });

        await client.DeleteTenantAsync("t1");

        handler.Requests.Single().RequestUri!.AbsoluteUri.Should().Be("http://localhost:8081/v1/tenants/t1");
    }

    [Fact]
    public async Task Deletes_WithoutIngestionBaseUrl_Throw()
    {
        var handler = new FakeHttpMessageHandler();
        using var client = new DashClient("https://dash.example.com", null, new DashClientOptions
        {
            HttpClient = new HttpClient(handler),
        });

        var claim = async () => await client.DeleteClaimAsync("t1", "c1");
        (await claim.Should().ThrowAsync<InvalidOperationException>())
            .Which.Message.Should().Contain("IngestionBaseUrl");
        var evidence = async () => await client.DeleteEvidenceAsync("t1", "ev-1");
        await evidence.Should().ThrowAsync<InvalidOperationException>();
        var tenant = async () => await client.DeleteTenantAsync("t1");
        await tenant.Should().ThrowAsync<InvalidOperationException>();
        handler.Requests.Should().BeEmpty();
    }

    [Theory]
    [InlineData("", "c1")]
    [InlineData("t1", "")]
    [InlineData("  ", "c1")]
    [InlineData("t1", " ")]
    public async Task DeleteClaimAsync_BlankIds_ThrowArgumentException(string tenantId, string claimId)
    {
        var handler = new FakeHttpMessageHandler();
        using var client = NewClient(handler);

        var act = async () => await client.DeleteClaimAsync(tenantId, claimId);
        await act.Should().ThrowAsync<ArgumentException>().WithMessage("*must not be blank*");
        handler.Requests.Should().BeEmpty();
    }

    [Fact]
    public async Task DeleteEvidenceAndTenant_BlankOrNullIds_ThrowArgumentException()
    {
        var handler = new FakeHttpMessageHandler();
        using var client = NewClient(handler);

        var evidence = async () => await client.DeleteEvidenceAsync("t1", "");
        await evidence.Should().ThrowAsync<ArgumentException>().WithMessage("*evidenceId*");
        var tenant = async () => await client.DeleteTenantAsync(null!);
        await tenant.Should().ThrowAsync<ArgumentException>().WithMessage("*tenantId*");
        handler.Requests.Should().BeEmpty();
    }

    [Fact]
    public async Task DeleteTenantAsync_5xx_IsRetriedBecauseDeletesAreIdempotent()
    {
        var handler = new FakeHttpMessageHandler()
            .Enqueue(HttpStatusCode.ServiceUnavailable, TestData.ServerErrorBody)
            .Enqueue(HttpStatusCode.OK, TestData.SampleDeleteTenantResponseJson);
        using var client = NewClient(handler);

        var response = await client.DeleteTenantAsync("t1");

        response.Deleted.Should().BeTrue();
        handler.Requests.Should().HaveCount(2);
        handler.Requests.Should().OnlyContain(r => r.Method == HttpMethod.Delete);
    }

    [Fact]
    public async Task DeleteTenantAsync_Forbidden_RaisesDashAuthException()
    {
        var handler = new FakeHttpMessageHandler()
            .Enqueue(HttpStatusCode.Forbidden, """{"error":{"type":"forbidden","message":"admin role required"}}""");
        using var client = NewClient(handler);

        var act = async () => await client.DeleteTenantAsync("t1");
        var ex = await act.Should().ThrowAsync<DashAuthException>();
        ex.Which.StatusCode.Should().Be(403);
        handler.Requests.Should().HaveCount(1);
    }

    [Fact]
    public void Delete_SyncVariants_ReturnTypedResponses()
    {
        var handler = new FakeHttpMessageHandler()
            .Enqueue(HttpStatusCode.OK, TestData.SampleDeleteClaimResponseJson)
            .Enqueue(HttpStatusCode.OK, TestData.SampleDeleteEvidenceResponseJson)
            .Enqueue(HttpStatusCode.OK, TestData.SampleDeleteTenantResponseJson);
        using var client = NewClient(handler);

        client.DeleteClaim("t1", "c1").Scope.Should().Be("claim");
        client.DeleteEvidence("t1", "ev-1").Scope.Should().Be("evidence");
        client.DeleteTenant("t1").Scope.Should().Be("tenant");
        handler.Requests.Select(r => r.RequestUri!.AbsolutePath).Should().Equal(
            "/v1/claims/c1", "/v1/evidence/ev-1", "/v1/tenants/t1");
    }

    [Fact]
    public async Task DeleteClaimAsync_AfterDispose_ThrowsObjectDisposedException()
    {
        var client = NewClient(new FakeHttpMessageHandler());
        client.Dispose();

        var act = async () => await client.DeleteClaimAsync("t1", "c1");
        await act.Should().ThrowAsync<ObjectDisposedException>();
    }

    private static DashClient NewClient(FakeHttpMessageHandler handler, string? apiKey = null)
    {
        return new DashClient(TestData.BaseUrl, apiKey, new DashClientOptions
        {
            HttpClient = new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(5) },
            IngestionBaseUrl = IngestionUrl,
            RetryBaseDelay = TimeSpan.FromMilliseconds(1),
        });
    }
}
