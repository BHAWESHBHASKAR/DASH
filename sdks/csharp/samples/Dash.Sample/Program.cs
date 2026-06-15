using System;
using System.Threading.Tasks;
using Dash;

namespace Dash.Sample;

/// <summary>
/// A small console sample that exercises the SDK against a live
/// DASH instance. The sample no-ops when <c>DASH_URL</c> is unset
/// so that <c>dotnet run</c> works in CI without a server.
///
/// <code>
/// DASH_URL=http://localhost:8080 DASH_API_KEY=sk-live dotnet run
/// </code>
/// </summary>
public static class Program
{
    public static async Task Main()
    {
        var baseUrl = Environment.GetEnvironmentVariable("DASH_URL");
        if (string.IsNullOrWhiteSpace(baseUrl))
        {
            Console.WriteLine("DASH_URL is not set; skipping live sample.");
            Console.WriteLine("Run with: DASH_URL=http://localhost:8080 dotnet run");
            return;
        }

        var apiKey = Environment.GetEnvironmentVariable("DASH_API_KEY");
        using var client = new DashClient(baseUrl, apiKey);

        var health = await client.HealthAsync();
        Console.WriteLine($"health: {health.Status} ({health.Version ?? "unknown"})");

        var embed = await client.EmbedAsync(new EmbeddingRequest
        {
            Input = "hello world",
            Model = "text-embedding-3-small",
        });
        Console.WriteLine($"embeddings.model: {embed.Model}");
        Console.WriteLine($"embeddings.first 5 dims: [{string.Join(", ", embed.Data[0].Values.Take(5))}]");

        var retrieve = await client.RetrieveAsync(new RetrievalRequest
        {
            TenantId = "acme-corp",
            Query = "Q3 2024 revenue",
            TopK = 3,
            StanceMode = "balanced",
        });
        foreach (var hit in retrieve.Hits)
        {
            Console.WriteLine($"[{hit.Score.Overall:F2}] supports={hit.Supports} contradicts={hit.Contradicts}");
            Console.WriteLine($"    {hit.CanonicalText}");
        }
    }
}
