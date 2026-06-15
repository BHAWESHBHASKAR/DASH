using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using Moq;
using Moq.Protected;
using Xunit;

namespace Dash.Tests;

/// <summary>
/// End-to-end tests for <see cref="DashClient"/>. The HTTP
/// transport is mocked at the <see cref="HttpMessageHandler"/>
/// boundary so no live DASH instance is required.
/// </summary>
public class DashClientTests
{
    private const string Bearer = "Bearer sk-test-123";

    // -----------------------------------------------------------------
    // Construction
    // -----------------------------------------------------------------

    [Fact]
    public void Constructor_StripsTrailingSlash_NormalizesBaseUrl()
    {
        using var client = new DashClient("http://localhost:8080/");
        client.BaseUrl.Should().Be("http://localhost:8080");
    }

    [Fact]
    public void Constructor_PreservesBaseUrlWithoutSlash()
    {
        using var client = new DashClient("http://localhost:8080");
        client.BaseUrl.Should().Be("http://localhost:8080");
    }

    [Fact]
    public void Constructor_RejectsEmptyBaseUrl()
    {
        Action act = () => _ = new DashClient("");
        act.Should().Throw<ArgumentException>();
    }

    [Fact]
    public void Constructor_WithNullApiKey_LeavesApiKeyNull()
    {
        using var client = new DashClient(TestData.BaseUrl);
        client.ApiKey.Should().BeNull();
    }

    [Fact]
    public void Constructor_WithApiKey_StoresApiKey()
    {
        using var client = new DashClient(TestData.BaseUrl, apiKey: "sk-live");
        client.ApiKey.Should().Be("sk-live");
    }

    [Fact]
    public void Constructor_AppliesDefaultUserAgent()
    {
        using var client = new DashClient(TestData.BaseUrl);
        client.Options.UserAgent.Should().Be("dash-csharp/0.2.0");
    }

    // -----------------------------------------------------------------
    // Headers
    // -----------------------------------------------------------------

    [Fact]
    public async Task EmbedAsync_WithApiKey_SendsBearerAuthorizationHeader()
    {
        var handler = new FakeHttpMessageHandler()
            .Enqueue(HttpStatusCode.OK, TestData.SampleEmbeddingResponseJson);
        using var client = NewClient(apiKey: "sk-test-123", handler: handler);

        await client.EmbedAsync(TestData.SampleEmbeddingRequest());

        var request = handler.Requests.Single();
        request.Headers.Authorization!.Parameter.Should().Be("sk-test-123");
        request.Headers.Authorization.Scheme.Should().Be("Bearer");
    }

    [Fact]
    public async Task EmbedAsync_WithoutApiKey_OmitsAuthorizationHeader()
    {
        var handler = new FakeHttpMessageHandler()
            .Enqueue(HttpStatusCode.OK, TestData.SampleEmbeddingResponseJson);
        using var client = NewClient(handler: handler);

        await client.EmbedAsync(TestData.SampleEmbeddingRequest());

        handler.Requests.Single().Headers.Authorization.Should().BeNull();
    }

    [Fact]
    public async Task EmbedAsync_AlwaysSetsDashUserAgentAndJsonContentType()
    {
        var handler = new FakeHttpMessageHandler()
            .Enqueue(HttpStatusCode.OK, TestData.SampleEmbeddingResponseJson);
        using var client = NewClient(handler: handler);

        await client.EmbedAsync(TestData.SampleEmbeddingRequest());

        var request = handler.Requests.Single();
        request.Headers.UserAgent!.ToString().Should().Contain("dash-csharp/0.2.0");
        request.Content!.Headers.ContentType!.MediaType.Should().Be("application/json");
    }

    // -----------------------------------------------------------------
    // Embeddings
    // -----------------------------------------------------------------

    [Fact]
    public async Task EmbedAsync_SingleString_ReturnsTypedResponse()
    {
        var handler = new FakeHttpMessageHandler()
            .Enqueue(HttpStatusCode.OK, TestData.SampleEmbeddingResponseJson);
        using var client = NewClient(handler: handler);

        var response = await client.EmbedAsync(TestData.SampleEmbeddingRequest());

        response.Object.Should().Be("list");
        response.Model.Should().Be("text-embedding-3-small");
        response.Usage.PromptTokens.Should().Be(2);
        response.Usage.TotalTokens.Should().Be(2);
        response.Data.Should().HaveCount(1);
        response.Data[0].Index.Should().Be(0);
        response.Data[0].Values[0].Should().BeApproximately(0.013f, 0.0001f);

        var sentBody = handler.RequestBodies.Single();
        using var doc = JsonDocument.Parse(sentBody!);
        doc.RootElement.GetProperty("input").GetString().Should().Be("hello world");
        doc.RootElement.GetProperty("model").GetString().Should().Be("text-embedding-3-small");
    }

    [Fact]
    public async Task EmbedAsync_BatchInputs_PreservesIndices()
    {
        var handler = new FakeHttpMessageHandler()
            .Enqueue(HttpStatusCode.OK, TestData.SampleEmbeddingArrayResponseJson);
        using var client = NewClient(handler: handler);

        var response = await client.EmbedAsync(new EmbeddingRequest
        {
            Input = new[] { "a", "b", "c" },
        });

        response.Data.Should().HaveCount(3);
        response.Data.Select(d => d.Index).Should().Equal(0, 1, 2);

        var sentBody = handler.RequestBodies.Single();
        using var doc = JsonDocument.Parse(sentBody!);
        doc.RootElement.GetProperty("input").ValueKind.Should().Be(JsonValueKind.Array);
        doc.RootElement.GetProperty("input").GetArrayLength().Should().Be(3);
    }

    [Fact]
    public async Task EmbedAsync_DefaultModel_SentAsTextEmbedding3Small()
    {
        var handler = new FakeHttpMessageHandler()
            .Enqueue(HttpStatusCode.OK, TestData.SampleEmbeddingResponseJson);
        using var client = NewClient(handler: handler);

        await client.EmbedAsync(new EmbeddingRequest { Input = "hi" });

        var body = handler.RequestBodies.Single()!;
        body.Should().Contain("\"model\":\"text-embedding-3-small\"");
    }

    [Fact]
    public async Task EmbedAsync_OptionalFields_AreOmittedWhenNull()
    {
        var handler = new FakeHttpMessageHandler()
            .Enqueue(HttpStatusCode.OK, TestData.SampleEmbeddingResponseJson);
        using var client = NewClient(handler: handler);

        await client.EmbedAsync(new EmbeddingRequest { Input = "hi" });

        var body = handler.RequestBodies.Single()!;
        body.Should().NotContain("encoding_format");
        body.Should().NotContain("user");
    }

    [Fact]
    public async Task EmbedAsync_OptionalFields_PassedThroughWhenSet()
    {
        var handler = new FakeHttpMessageHandler()
            .Enqueue(HttpStatusCode.OK, TestData.SampleEmbeddingResponseJson);
        using var client = NewClient(handler: handler);

        await client.EmbedAsync(new EmbeddingRequest
        {
            Input = "hi",
            Model = "text-embedding-3-large",
            EncodingFormat = "float",
            User = "u-1",
        });

        var body = handler.RequestBodies.Single()!;
        body.Should().Contain("\"encoding_format\":\"float\"");
        body.Should().Contain("\"user\":\"u-1\"");
    }

    // -----------------------------------------------------------------
    // Ingest / Retrieve / Delete / Health
    // -----------------------------------------------------------------

    [Fact]
    public async Task IngestAsync_Bundles_AreSerializedWithSnakeCase()
    {
        var handler = new FakeHttpMessageHandler()
            .Enqueue(HttpStatusCode.OK, TestData.SampleIngestResponseJson);
        using var client = NewClient(handler: handler);

        var response = await client.IngestAsync(new IngestRequest
        {
            TenantId = "tenant-a",
            Bundles = new[]
            {
                new IngestBundle
                {
                    SourceId = "src-1",
                    Title = "Acme 10-K",
                    Claims = new[]
                    {
                        new IngestClaim
                        {
                            ClaimId = "c-1",
                            Text = "Acme Co. was acquired in 2024.",
                            Evidence = new[]
                            {
                                new IngestEvidence
                                {
                                    EvidenceId = "ev-1",
                                    Stance = "supports",
                                    SourceId = "source://reuters",
                                    ChunkId = "chunk-7",
                                    SpanStart = 120,
                                    SpanEnd = 168,
                                },
                            },
                        },
                    },
                },
            },
        });

        response.BundlesIngested.Should().Be(1);
        response.ClaimsIngested.Should().Be(2);
        response.EvidenceIngested.Should().Be(3);

        var body = handler.RequestBodies.Single()!;
        using var doc = JsonDocument.Parse(body);
        doc.RootElement.GetProperty("tenant_id").GetString().Should().Be("tenant-a");
        doc.RootElement.GetProperty("bundles")[0].GetProperty("source_id").GetString().Should().Be("src-1");
        doc.RootElement.GetProperty("bundles")[0].GetProperty("claims")[0].GetProperty("claim_id").GetString().Should().Be("c-1");
    }

    [Fact]
    public async Task RetrieveAsync_DefaultsTopKAndStanceMode_AreApplied()
    {
        var handler = new FakeHttpMessageHandler()
            .Enqueue(HttpStatusCode.OK, TestData.SampleRetrieveResponseJson);
        using var client = NewClient(handler: handler);

        var response = await client.RetrieveAsync(new RetrievalRequest
        {
            TenantId = "tenant-a",
            Query = "company x",
        });

        response.Hits.Should().HaveCount(1);
        var hit = response.Hits[0];
        hit.ClaimId.Should().Be("claim-1");
        hit.CanonicalText.Should().Be("Acme Co. was acquired in 2024.");
        hit.Score.Overall.Should().BeApproximately(0.93, 0.0001);
        hit.Supports.Should().Be(4);
        hit.Contradicts.Should().Be(1);
        hit.Citations.Should().HaveCount(1);
        hit.Citations[0].Stance.Should().Be("supports");
        hit.Citations[0].SourceQuality.Should().BeApproximately(0.88, 0.0001);
        hit.Citations[0].SpanStart.Should().Be(120);

        var body = handler.RequestBodies.Single()!;
        using var doc = JsonDocument.Parse(body);
        doc.RootElement.GetProperty("top_k").GetInt32().Should().Be(10);
        doc.RootElement.GetProperty("stance_mode").GetString().Should().Be("balanced");
        doc.RootElement.TryGetProperty("return_graph", out _).Should().BeFalse();
    }

    [Fact]
    public async Task RetrieveAsync_StanceModeSupportOnly_IsSent()
    {
        var handler = new FakeHttpMessageHandler()
            .Enqueue(HttpStatusCode.OK, TestData.SampleRetrieveResponseJson);
        using var client = NewClient(handler: handler);

        await client.RetrieveAsync(new RetrievalRequest
        {
            TenantId = "tenant-a",
            Query = "q",
            TopK = 3,
            StanceMode = "support_only",
        });

        var body = handler.RequestBodies.Single()!;
        body.Should().Contain("\"stance_mode\":\"support_only\"");
        body.Should().Contain("\"top_k\":3");
    }

    [Fact]
    public async Task DeleteAsync_ByClaimIds_CallsDeleteEndpoint()
    {
        var handler = new FakeHttpMessageHandler()
            .Enqueue(HttpStatusCode.OK, TestData.SampleDeleteResponseJson);
        using var client = NewClient(handler: handler);

        var response = await client.DeleteAsync(new DeleteRequest
        {
            TenantId = "tenant-a",
            ClaimIds = new[] { "c-1", "c-2" },
        });

        response.ClaimsDeleted.Should().Be(5);
        response.EvidenceDeleted.Should().Be(9);

        handler.Requests.Single().RequestUri!.AbsolutePath.Should().Be("/v1/delete");
    }

    [Fact]
    public async Task HealthAsync_ReturnsTypedResponse()
    {
        var handler = new FakeHttpMessageHandler()
            .Enqueue(HttpStatusCode.OK, TestData.SampleHealthResponseJson);
        using var client = NewClient(handler: handler);

        var response = await client.HealthAsync();

        response.Status.Should().Be("ok");
        response.Version.Should().Be("0.4.0");
        response.Details.Should().NotBeNull();
        response.Details!["uptime_seconds"].ToString().Should().Be("12345");
        handler.Requests.Single().Method.Should().Be(HttpMethod.Get);
        handler.Requests.Single().RequestUri!.AbsolutePath.Should().Be("/health");
    }

    // -----------------------------------------------------------------
    // Error mapping
    // -----------------------------------------------------------------

    [Fact]
    public async Task EmbedAsync_401_RaisesDashAuthException()
    {
        var handler = new FakeHttpMessageHandler()
            .Enqueue(HttpStatusCode.Unauthorized, TestData.OpenAIStyleErrorBody);
        using var client = NewClient(handler: handler);

        var act = async () => await client.EmbedAsync(TestData.SampleEmbeddingRequest());
        var ex = await act.Should().ThrowAsync<DashException>();
        ex.Which.Should().BeOfType<DashAuthException>();
        ex.Which.StatusCode.Should().Be(401);
        ex.Which.ErrorCode.Should().Be("invalid_request_error");
    }

    [Fact]
    public async Task EmbedAsync_403_RaisesDashAuthException()
    {
        var handler = new FakeHttpMessageHandler()
            .Enqueue(HttpStatusCode.Forbidden, TestData.OpenAIStyleErrorBody);
        using var client = NewClient(handler: handler);

        var act = async () => await client.EmbedAsync(TestData.SampleEmbeddingRequest());
        var ex = await act.Should().ThrowAsync<DashException>();
        ex.Which.Should().BeOfType<DashAuthException>();
        ex.Which.StatusCode.Should().Be(403);
    }

    [Fact]
    public async Task EmbedAsync_404_RaisesDashNotFoundException()
    {
        var handler = new FakeHttpMessageHandler()
            .Enqueue(HttpStatusCode.NotFound, new { error = "tenant not found" });
        using var client = NewClient(handler: handler);

        var act = async () => await client.RetrieveAsync(new RetrievalRequest
        {
            TenantId = "missing", Query = "q",
        });
        var ex = await act.Should().ThrowAsync<DashException>();
        ex.Which.Should().BeOfType<DashNotFoundException>();
        ex.Which.StatusCode.Should().Be(404);
        ex.Which.Body.Should().Contain("tenant not found");
    }

    [Fact]
    public async Task EmbedAsync_429_RaisesDashRateLimitException()
    {
        var handler = new FakeHttpMessageHandler()
            .Enqueue(HttpStatusCode.TooManyRequests, new { error = "rate limit exceeded" });
        using var client = NewClient(handler: handler);

        var act = async () => await client.EmbedAsync(TestData.SampleEmbeddingRequest());
        var ex = await act.Should().ThrowAsync<DashException>();
        ex.Which.Should().BeOfType<DashRateLimitException>();
        ex.Which.StatusCode.Should().Be(429);
    }

    [Fact]
    public async Task EmbedAsync_500_RaisesDashExceptionWithServerErrorType()
    {
        var handler = new FakeHttpMessageHandler()
            .Enqueue(HttpStatusCode.InternalServerError, TestData.ServerErrorBody);
        using var client = NewClient(handler: handler);

        var act = async () => await client.EmbedAsync(TestData.SampleEmbeddingRequest());
        var ex = await act.Should().ThrowAsync<DashException>();
        ex.Which.Should().BeOfType<DashException>();
        ex.Which.StatusCode.Should().Be(500);
        ex.Which.ErrorCode.Should().Be("server_error");
    }

    [Fact]
    public async Task EmbedAsync_NetworkError_RaisesDashConnectionException()
    {
        var handler = new FakeHttpMessageHandler()
            .EnqueueException(_ => new HttpRequestException("ECONNREFUSED"));
        using var client = NewClient(handler: handler);

        var act = async () => await client.EmbedAsync(TestData.SampleEmbeddingRequest());
        var ex = await act.Should().ThrowAsync<DashConnectionException>();
        ex.Which.InnerException.Should().BeOfType<HttpRequestException>();
    }

    [Fact]
    public async Task EmbedAsync_NonJson5xx_FallsBackToRawBody()
    {
        var handler = new FakeHttpMessageHandler()
            .Enqueue(HttpStatusCode.BadGateway, rawText: "Bad Gateway");
        using var client = NewClient(handler: handler);

        var act = async () => await client.EmbedAsync(TestData.SampleEmbeddingRequest());
        var ex = await act.Should().ThrowAsync<DashException>();
        ex.Which.Body.Should().Contain("Bad Gateway");
    }

    // -----------------------------------------------------------------
    // Retry behaviour
    // -----------------------------------------------------------------

    [Fact]
    public async Task EmbedAsync_RetriesOnTransientError_ThenSucceeds()
    {
        var handler = new FakeHttpMessageHandler()
            .Enqueue(HttpStatusCode.InternalServerError, TestData.ServerErrorBody)
            .Enqueue(HttpStatusCode.OK, TestData.SampleEmbeddingResponseJson);
        var options = new DashClientOptions
        {
            MaxRetries = 3,
            RetryBaseDelay = TimeSpan.FromMilliseconds(1),
        };
        using var client = NewClient(handler: handler, options: options);

        var response = await client.EmbedAsync(TestData.SampleEmbeddingRequest());

        response.Data.Should().HaveCount(1);
        handler.Requests.Should().HaveCount(2);
    }

    [Fact]
    public async Task EmbedAsync_ExhaustsRetries_ThrowsLastError()
    {
        var handler = new FakeHttpMessageHandler()
            .Enqueue(HttpStatusCode.InternalServerError, TestData.ServerErrorBody)
            .Enqueue(HttpStatusCode.InternalServerError, TestData.ServerErrorBody)
            .Enqueue(HttpStatusCode.InternalServerError, TestData.ServerErrorBody)
            .Enqueue(HttpStatusCode.InternalServerError, TestData.ServerErrorBody);
        var options = new DashClientOptions
        {
            MaxRetries = 3,
            RetryBaseDelay = TimeSpan.FromMilliseconds(1),
        };
        using var client = NewClient(handler: handler, options: options);

        var act = async () => await client.EmbedAsync(TestData.SampleEmbeddingRequest());
        var ex = await act.Should().ThrowAsync<DashException>();
        ex.Which.StatusCode.Should().Be(500);
        // 1 initial + 3 retries = 4 total attempts.
        handler.Requests.Should().HaveCount(4);
    }

    [Fact]
    public async Task EmbedAsync_429_RetriesThenSucceeds()
    {
        var handler = new FakeHttpMessageHandler()
            .Enqueue(HttpStatusCode.TooManyRequests, new { error = "slow down" })
            .Enqueue(HttpStatusCode.OK, TestData.SampleEmbeddingResponseJson);
        var options = new DashClientOptions
        {
            MaxRetries = 2,
            RetryBaseDelay = TimeSpan.FromMilliseconds(1),
        };
        using var client = NewClient(handler: handler, options: options);

        var response = await client.EmbedAsync(TestData.SampleEmbeddingRequest());

        response.Data.Should().HaveCount(1);
        handler.Requests.Should().HaveCount(2);
    }

    [Fact]
    public async Task EmbedAsync_4xxIsNotRetried()
    {
        var handler = new FakeHttpMessageHandler()
            .Enqueue(HttpStatusCode.BadRequest, TestData.OpenAIStyleErrorBody);
        using var client = NewClient(handler: handler);

        var act = async () => await client.EmbedAsync(TestData.SampleEmbeddingRequest());
        await act.Should().ThrowAsync<DashException>();
        handler.Requests.Should().HaveCount(1);
    }

    // -----------------------------------------------------------------
    // Cancellation
    // -----------------------------------------------------------------

    [Fact]
    public async Task EmbedAsync_PreCancelledToken_ThrowsWithoutRequest()
    {
        var handler = new FakeHttpMessageHandler()
            .Enqueue(HttpStatusCode.OK, TestData.SampleEmbeddingResponseJson);
        using var client = NewClient(handler: handler);

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        var act = async () => await client.EmbedAsync(TestData.SampleEmbeddingRequest(), cts.Token);
        await act.Should().ThrowAsync<OperationCanceledException>();
        handler.Requests.Should().BeEmpty();
    }

    // -----------------------------------------------------------------
    // Sync variants
    // -----------------------------------------------------------------

    [Fact]
    public void Embed_SyncVariant_ReturnsTypedResponse()
    {
        var handler = new FakeHttpMessageHandler()
            .Enqueue(HttpStatusCode.OK, TestData.SampleEmbeddingResponseJson);
        using var client = NewClient(handler: handler);

        var response = client.Embed(TestData.SampleEmbeddingRequest());

        response.Data.Should().HaveCount(1);
        response.Data[0].Values[0].Should().BeApproximately(0.013f, 0.0001f);
    }

    [Fact]
    public void Health_SyncVariant_ReturnsTypedResponse()
    {
        var handler = new FakeHttpMessageHandler()
            .Enqueue(HttpStatusCode.OK, TestData.SampleHealthResponseJson);
        using var client = NewClient(handler: handler);

        var response = client.Health();

        response.Status.Should().Be("ok");
    }

    [Fact]
    public void Ingest_SyncVariant_ReturnsTypedResponse()
    {
        var handler = new FakeHttpMessageHandler()
            .Enqueue(HttpStatusCode.OK, TestData.SampleIngestResponseJson);
        using var client = NewClient(handler: handler);

        var response = client.Ingest(new IngestRequest
        {
            TenantId = "t",
            Bundles = new[]
            {
                new IngestBundle { SourceId = "s", Claims = Array.Empty<IngestClaim>() },
            },
        });

        response.ClaimsIngested.Should().Be(2);
    }

    // -----------------------------------------------------------------
    // Dispose
    // -----------------------------------------------------------------

    [Fact]
    public void Dispose_IsIdempotent()
    {
        using var client = new DashClient(TestData.BaseUrl);
        client.Dispose();
        client.Dispose();
    }

    [Fact]
    public void EmbedAsync_AfterDispose_ThrowsObjectDisposedException()
    {
        var client = new DashClient(TestData.BaseUrl);
        client.Dispose();

        var act = async () => await client.EmbedAsync(TestData.SampleEmbeddingRequest());
        act.Should().ThrowAsync<ObjectDisposedException>();
    }

    // -----------------------------------------------------------------
    // OpenAI drop-in compatibility
    // -----------------------------------------------------------------

    [Fact]
    public async Task EmbedAsync_OpenAiCompat_UsesV1EmbeddingsPath()
    {
        var handler = new FakeHttpMessageHandler()
            .Enqueue(HttpStatusCode.OK, TestData.SampleEmbeddingResponseJson);
        using var client = NewClient(handler: handler);

        await client.EmbedAsync(TestData.SampleEmbeddingRequest());

        handler.Requests.Single().RequestUri!.AbsolutePath.Should().Be("/v1/embeddings");
    }

    [Fact]
    public async Task EmbedAsync_OpenAiStyleErrorBody_IsTranslatedVerbatim()
    {
        var handler = new FakeHttpMessageHandler()
            .Enqueue(HttpStatusCode.BadRequest, TestData.OpenAIStyleErrorBody);
        using var client = NewClient(handler: handler);

        var act = async () => await client.EmbedAsync(TestData.SampleEmbeddingRequest());
        var ex = await act.Should().ThrowAsync<DashException>();
        ex.Which.StatusCode.Should().Be(400);
        ex.Which.ErrorCode.Should().Be("invalid_request_error");
        ex.Which.Body.Should().Contain("input must contain at least one text");
    }

    [Fact]
    public async Task EmbedAsync_TrailingSlashOnBaseUrl_DoesNotProduceDoubleSlash()
    {
        var handler = new FakeHttpMessageHandler()
            .Enqueue(HttpStatusCode.OK, TestData.SampleEmbeddingResponseJson);
        using var client = new DashClient("http://localhost:8080/", options: new DashClientOptions
        {
            HttpClient = new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(5) },
        });

        await client.EmbedAsync(TestData.SampleEmbeddingRequest());

        handler.Requests.Single().RequestUri!.AbsolutePath.Should().Be("/v1/embeddings");
        handler.Requests.Single().RequestUri!.ToString().Should().NotContain("//v1");
    }

    [Fact]
    public async Task EmbedAsync_CustomRequestIdHeader_IsExtracted()
    {
        var handler = new FakeHttpMessageHandler()
            .Enqueue(
                HttpStatusCode.InternalServerError,
                TestData.ServerErrorBody,
                headers: new Dictionary<string, string> { ["X-Request-Id"] = "req-abc-123" });
        using var client = NewClient(handler: handler);

        var act = async () => await client.EmbedAsync(TestData.SampleEmbeddingRequest());
        var ex = await act.Should().ThrowAsync<DashException>();
        ex.Which.RequestId.Should().Be("req-abc-123");
    }

    // -----------------------------------------------------------------
    // Moq-based handler (alternative mock style)
    // -----------------------------------------------------------------

    [Fact]
    public async Task EmbedAsync_UsingMoqHandler_RespondsAsConfigured()
    {
        // Demonstrates that the SDK works with a Moq-based
        // HttpMessageHandler too, not just the hand-rolled fake.
        var moqHandler = new Moq.Mock<HttpMessageHandler>(Moq.MockBehavior.Strict);
        moqHandler
            .Protected()
            .Setup<Task<HttpResponseMessage>>(
                "SendAsync",
                ItExpr.IsAny<HttpRequestMessage>(),
                ItExpr.IsAny<CancellationToken>())
            .ReturnsAsync(() => new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(
                    TestData.SampleEmbeddingResponseJson, Encoding.UTF8, "application/json"),
            });

        using var client = new DashClient(TestData.BaseUrl, options: new DashClientOptions
        {
            HttpClient = new HttpClient(moqHandler.Object) { Timeout = TimeSpan.FromSeconds(5) },
        });

        var response = await client.EmbedAsync(TestData.SampleEmbeddingRequest());

        response.Data.Should().HaveCount(1);
        moqHandler.Protected().Verify(
            "SendAsync",
            Moq.Times.Once(),
            ItExpr.IsAny<HttpRequestMessage>(),
            ItExpr.IsAny<CancellationToken>());
    }

    // -----------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------

    private static DashClient NewClient(
        string? apiKey = null,
        FakeHttpMessageHandler? handler = null,
        DashClientOptions? options = null)
    {
        handler ??= new FakeHttpMessageHandler();
        var opts = options ?? new DashClientOptions
        {
            HttpClient = new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(5) },
        };
        if (opts.HttpClient is null)
        {
            opts.HttpClient = new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(5) };
        }
        return new DashClient(TestData.BaseUrl, apiKey, opts);
    }
}
