# dash-csharp

A thin, idiomatic C# client for the [DASH](https://github.com/BHAWESHBHASKAR/DASH)
retrieval engine. DASH serves an OpenAI-compatible `/v1/embeddings`
endpoint and a native `/v1/retrieve` endpoint that returns
structured **Claim + Evidence + Contradiction** results — the
differentiator that makes it a real retrieval engine, not a vector
store.

```bash
dotnet add package Dash
```

Targets `net8.0` and `netstandard2.0` (for broad library
compatibility). Zero dependencies beyond `System.Text.Json` and
`System.Net.Http`.

## 5-minute quickstart

### 1. Embeddings (OpenAI-compatible)

```csharp
using Dash;

using var client = new DashClient("http://localhost:8080");
var response = await client.EmbedAsync(new EmbeddingRequest
{
    Input = "hello world",
    Model = "text-embedding-3-small",
});

foreach (var data in response.Data)
{
    // data.Values is the embedding vector.
    Console.WriteLine($"[{data.Index}] {data.Values[0]} {data.Values[1]} ...");
}
```

That's it — the same shape as
`new OpenAIClient(...).GetEmbeddingClient("text-embedding-3-small").GenerateEmbedding(...)`.
DASH's `/v1/embeddings` is byte-for-byte compatible with the OpenAI
spec, so any tool that speaks the OpenAI protocol just works.

### 2. Drop-in OpenAI compatibility (zero code change)

If you already use the official `OpenAI` .NET package, you don't
even need `dash-csharp` to talk to DASH — just point the base URL
at DASH:

```csharp
using OpenAI.Embeddings;

// The DASH server is OpenAI-compatible, so the official
// OpenAIClient works against it as long as the base URL points
// at DASH's /v1 prefix.
var openAi = new OpenAIClient(
    new System.ClientModel.ApiKeyCredential("not-required-for-local"),
    new OpenAIClientOptions
    {
        Endpoint = new Uri("http://localhost:8080/v1"),
    });

var embedding = await openAi
    .GetEmbeddingClient("text-embedding-3-small")
    .GenerateEmbeddingAsync("hello world");
```

This is the same path the OpenAI CLI, Semantic Kernel, and any
other tool that respects the `OPENAI_BASE_URL` / endpoint knob use,
so they all work too.

### 3. Native retrieve (the differentiator)

Where DASH pulls ahead of a vector store is the retrieval
endpoint, which returns Claims with explicit **support** and
**contradiction** counts and a structured **Citation** list per
claim. That's the substance you actually need to build a RAG
pipeline that doesn't confidently echo back the wrong thing.

```csharp
using Dash;

using var client = new DashClient("http://localhost:8080");

var response = await client.RetrieveAsync(new RetrievalRequest
{
    TenantId = "acme-corp",
    Query = "what was the Q3 2024 revenue?",
    TopK = 5,
    StanceMode = "balanced",
});

// A RAG pipeline that wants "claims that are actually supported
// and not contradicted" can filter the typed results directly:
var cleanClaims = response.Results
    .Where(r => r.Contradicts == 0 && r.Supports > 0)
    .ToList();

foreach (var claim in response.Results)
{
    Console.WriteLine($"[{claim.Score:F2}] {claim.CanonicalText}");
    Console.WriteLine($"   supports={claim.Supports}  contradicts={claim.Contradicts}");
    foreach (var cite in claim.Citations)
    {
        Console.WriteLine($"   - {cite.Stance,-11}  {cite.SourceId}  (q={cite.SourceQuality:F2})");
    }
}
```

Use `StanceMode = "support_only"` to filter out claims whose
contradiction tally exceeds their support tally before they reach
your prompt.

### 4. Authentication & error handling

```csharp
using Dash;

using var client = new DashClient(
    baseUrl: "https://dash.example.com",
    apiKey: "sk-live-...",
    options: new DashClientOptions { Timeout = TimeSpan.FromSeconds(10) });

try
{
    var response = await client.EmbedAsync(new EmbeddingRequest { Input = "hello" });
}
catch (DashAuthException ex)
{
    // 401 / 403. ex.StatusCode, ex.ErrorCode, ex.RequestId available.
    Console.Error.WriteLine($"auth: {ex.StatusCode} {ex.ErrorCode}");
}
catch (DashRateLimitException ex)
{
    // 429. The SDK has already retried MaxRetries times; back off
    // before retrying from the caller.
    Console.Error.WriteLine($"rate limit: {ex.StatusCode}");
}
catch (DashNotFoundException ex)
{
    // 404. Tenant or claim not found.
    Console.Error.WriteLine($"not found: {ex.Body}");
}
catch (DashConnectionException ex)
{
    // Network failure (DNS, refused, timeout). The underlying
    // HttpRequestException is chained via ex.InnerException.
    Console.Error.WriteLine($"connectivity: {ex.Message}");
}
catch (DashException ex)
{
    // Catch-all for any other API error (5xx, malformed body, etc).
    Console.Error.WriteLine($"DASH {ex.StatusCode} {ex.ErrorCode}: {ex.Body}");
}
```

Every API exception inherits from `DashException`, so a single
`catch (DashException ex)` is enough to catch "anything went wrong
talking to DASH at the HTTP layer". Network-level failures raise
`DashConnectionException` instead.

## API surface

| Symbol | Purpose |
| --- | --- |
| `DashClient(baseUrl, apiKey?, options?)` | Construct a client. |
| `DashClient.EmbedAsync(req)` / `.Embed(req)` | `POST /v1/embeddings`, async + sync. |
| `DashClient.IngestAsync(req[, RequestOptions])` / `.Ingest(req)` | `POST {IngestionBaseUrl}/v1/ingest` (experimental, never retried by default). |
| `DashClient.DeleteClaimAsync(tenantId, claimId)` / `.DeleteClaim(...)` | `DELETE {IngestionBaseUrl}/v1/claims/{claim_id}?tenant_id=...` (claim, vector, evidence, edges). |
| `DashClient.DeleteEvidenceAsync(tenantId, evidenceId)` / `.DeleteEvidence(...)` | `DELETE {IngestionBaseUrl}/v1/evidence/{evidence_id}?tenant_id=...` (evidence rows; claims stay). |
| `DashClient.DeleteTenantAsync(tenantId)` / `.DeleteTenant(...)` | `DELETE {IngestionBaseUrl}/v1/tenants/{tenant_id}` (all of the tenant's data). |
| `DashClient.RetrieveAsync(req)` / `.Retrieve(req)` | `POST /v1/retrieve` with `{ tenant_id, query, top_k?, stance_mode?, return_graph? }`. |
| `DashClient.HealthAsync()` / `.Health()` | `GET /health`. |
| `DashException` and subclasses | Exception hierarchy. |
| `DashClientOptions` | Timeout, retries, user-agent, retry delay. |

## Configuration reference

```csharp
public class DashClientOptions
{
    public TimeSpan Timeout { get; set; }           // default 30s
    public string? IngestionBaseUrl { get; set; }   // default: derived for :8080 -> :8081, else null
    public int MaxRetries { get; set; }             // default 3 (max 10)
    public string UserAgent { get; set; }           // default "dash-csharp/0.2.0"
    public TimeSpan RetryBaseDelay { get; set; }    // default 100ms
    public HttpClient? HttpClient { get; set; }     // optional injection (testing)
}
```

The transport retries idempotent requests (embeddings, retrieve,
health, deletes) on transient failures (network errors, HTTP 5xx, HTTP 429)
with jittered exponential backoff (up to `RetryBaseDelay * 2^(N-1)`,
capped at 5 s), honouring a server-supplied `Retry-After` header
(up to 30 s). `POST /v1/ingest` is sent exactly once unless you pass
`new RequestOptions { IdempotencyKey = "..." }` (or `Retry = true`).

### Ingest (experimental)

Ingestion is a separate service from retrieval (default ports 8081
and 8080). Set `DashClientOptions.IngestionBaseUrl`; when omitted it
is derived only for `http://host:8080` (becomes `:8081`), otherwise
`IngestAsync` throws `InvalidOperationException`. The models mirror
the server's `{ claim, evidence, edges }` body and its
`ingested_claim_id` / `claims_total` / `commit_status` response:

```csharp
using var client = new DashClient("http://localhost:8080", apiKey: null,
    new DashClientOptions { IngestionBaseUrl = "http://localhost:8081" });

var result = await client.IngestAsync(new IngestRequest
{
    Claim = new IngestClaim
    {
        ClaimId = "c-1", TenantId = "acme",
        CanonicalText = "Q3 revenue was $1B", Confidence = 0.9,
    },
});
Console.WriteLine($"{result.IngestedClaimId} {result.CommitStatus}");
```

### Deletes

Deletes are served by the ingestion service, so they use the same
`IngestionBaseUrl` as ingest (derived only for `http://host:8080`;
otherwise the delete methods throw `InvalidOperationException`). Ids
are percent-encoded into the path and query. Each method has a sync
variant without the `Async` suffix.

```csharp
using var client = new DashClient("http://localhost:8080", apiKey: "sk-ingest-...",
    new DashClientOptions { IngestionBaseUrl = "http://localhost:8081" });

await client.DeleteClaimAsync("tenant-a", "claim-1");    // claim, vector, evidence, edges
await client.DeleteEvidenceAsync("tenant-a", "ev-1");    // every evidence row with this id
DeleteResponse r = await client.DeleteTenantAsync("tenant-a"); // erase the tenant (admin role)
Console.WriteLine($"{r.Deleted} {r.Scope} claims={r.ClaimsDeleted} left={r.ClaimsTotal}");
```

Every delete is idempotent and answers HTTP 200: `Deleted` is `false`
when there was nothing to remove (an unknown id, an id already deleted,
or a claim of another tenant). `DeleteResponse` also carries
`EvidenceDeleted`, `EdgesDeleted`, `VectorsDeleted`,
`CheckpointTriggered` and `CheckpointDeferred`; `ClaimId` and
`EvidenceId` are set only for the matching scope. Deletes are retried on
429/5xx like reads. Claim and evidence deletes need the `ingest` role; a
tenant delete needs `admin` for that tenant. Backups and WAL archives
taken before a delete still hold the data (see
`docs/operations/data-deletion.md`).

The generic `DeleteAsync` / `DeleteRequest` were removed in 0.2.0: the
server has no `/v1/delete` endpoint.

Cancellation uses the standard `CancellationToken` API:

```csharp
using var cts = new CancellationTokenSource();
cts.CancelAfter(TimeSpan.FromSeconds(1));
await client.EmbedAsync(new EmbeddingRequest { Input = "hi" }, cts.Token);
```

## Why a C# SDK for DASH?

DASH already speaks the OpenAI wire format, so most users can use
the official `OpenAI` .NET package. `dash-csharp` exists for the
cases that need a typed, first-class client:

- Typed response objects (so IDEs autocomplete `.CanonicalText`,
  `.Supports`, etc.).
- Native access to `/v1/retrieve` with `StanceMode` and
  `ReturnGraph` flags.
- Native access to `/v1/ingest` for Claim + Evidence ingestion, and
  to the scoped claim, evidence and tenant deletes.
- A consistent exception hierarchy (`DashConnectionException` vs
  `DashException` subclasses) instead of inspecting
  `HttpRequestException` by hand.
- Targets `netstandard2.0` as well as `net8.0`, so the same DLL
  works in legacy .NET Framework apps, Unity, Xamarin, etc.

## Changelog

See [CHANGELOG.md](CHANGELOG.md).

## License

Apache-2.0.
