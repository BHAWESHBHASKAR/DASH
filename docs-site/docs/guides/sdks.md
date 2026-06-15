# SDKs

DASH ships first-party SDKs for the five most common languages. All of them implement the OpenAI-drop-in pattern, so any code that uses an OpenAI client can be pointed at DASH by changing one environment variable.

## Python — `dash-py`

**Install:**

```bash
pip install dash-py
```

**Quickstart:**

```python
from dash_py import DashClient

client = DashClient(base_url="http://localhost:8080", api_key="not_needed")

# 1. Embed
emb = client.embeddings.create(input="hello world", model="text-embedding-3-small")
vec = emb.data[0].embedding

# 2. Ingest
client.ingest(
    claim={
        "claim_id": "c1",
        "tenant_id": "t1",
        "canonical_text": "Company X acquired Company Y",
        "confidence": 0.95,
    },
    evidence=[{
        "evidence_id": "e1",
        "claim_id": "c1",
        "tenant_id": "t1",
        "source_id": "news://nyt",
        "stance": "supports",
        "source_quality": 0.95,
    }],
    edges=[],
)

# 3. Retrieve
resp = client.retrieve(
    tenant_id="t1",
    query="Company X acquired Company Y",
    top_k=5,
    stance_mode="support_only",
)
for hit in resp.results:
    print(hit.claim.canonical_text, hit.score, hit.citations)
```

The `dash-py` package also exports an `AsyncDashClient` for `asyncio` callers. The OpenAI-drop-in example is in `sdks/python/examples/openai_dropin.py`. The full RAG walkthrough that exercises the Claim + Evidence + Contradiction differentiator is in `sdks/python/examples/rag_walkthrough.py`.

**Tests:** 59 tests passing.

## Go — `dash-go`

**Install:**

```bash
go get github.com/BHAWESHBHASKAR/dash-go
```

**Quickstart:**

```go
package main

import (
    "context"
    "fmt"
    "log"

    dash "github.com/BHAWESHBHASKAR/dash-go"
)

func main() {
    client := dash.NewClient("http://localhost:8080",
        dash.WithAPIKey("not_needed"),
    )

    ctx := context.Background()

    // 1. Embed
    emb, err := client.Embeddings.Create(ctx, &dash.EmbeddingRequest{
        Model: "text-embedding-3-small",
        Input: "hello world",
    })
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println(emb.Data[0].Embedding[:5])

    // 2. Ingest
    if _, err := client.Ingest.Ingest(ctx, &dash.IngestRequest{
        Claim: dash.Claim{
            ClaimID:       "c1",
            TenantID:      "t1",
            CanonicalText: "Company X acquired Company Y",
            Confidence:    0.95,
        },
        Evidence: []dash.Evidence{{
            EvidenceID:    "e1",
            ClaimID:       "c1",
            TenantID:      "t1",
            SourceID:      "news://nyt",
            Stance:        dash.StanceSupports,
            SourceQuality: 0.95,
        }},
    }); err != nil {
        log.Fatal(err)
    }

    // 3. Retrieve
    resp, err := client.Retrieve.Retrieve(ctx, &dash.RetrieveRequest{
        TenantID:   "t1",
        Query:      "Company X acquired Company Y",
        TopK:       5,
        StanceMode: dash.StanceModeSupportOnly,
    })
    if err != nil {
        log.Fatal(err)
    }
    for _, hit := range resp.Results {
        fmt.Println(hit.Claim.CanonicalText, hit.Score, hit.Citations)
    }
}
```

The package uses functional options and a service pattern (`client.Embeddings`, `client.Ingest`, `client.Retrieve`). `errors.Is` and `errors.As` are supported for typed error matching. **Tests:** 86 tests passing.

## TypeScript — `dash-ts`

**Install:**

```bash
npm install dash-ts
```

**Quickstart:**

```typescript
import { DashClient } from "dash-ts";

const client = new DashClient({
    baseUrl: "http://localhost:8080",
    apiKey: "not_needed",
});

// 1. Embed
const emb = await client.embeddings.create({
    model: "text-embedding-3-small",
    input: "hello world",
});
console.log(emb.data[0].embedding.slice(0, 5));

// 2. Ingest
await client.ingest({
    claim: {
        claim_id: "c1",
        tenant_id: "t1",
        canonical_text: "Company X acquired Company Y",
        confidence: 0.95,
    },
    evidence: [{
        evidence_id: "e1",
        claim_id: "c1",
        tenant_id: "t1",
        source_id: "news://nyt",
        stance: "supports",
        source_quality: 0.95,
    }],
    edges: [],
});

// 3. Retrieve
const resp = await client.retrieve({
    tenant_id: "t1",
    query: "Company X acquired Company Y",
    top_k: 5,
    stance_mode: "support_only",
});
for (const hit of resp.results) {
    console.log(hit.claim.canonical_text, hit.score, hit.citations);
}
```

ESM-first, Node 18+, zero runtime dependencies. Full type safety with discriminated unions on the response shapes. **Tests:** 65 tests passing.

## Java — `dash-java`

**Install (Maven):**

```xml
<dependency>
    <groupId>io.bhaweshbhaskar</groupId>
    <artifactId>dash-java</artifactId>
    <version>0.1.0</version>
</dependency>
```

**Quickstart:**

```java
import io.bhaweshbhaskar.dash.DashClient;
import io.bhaweshbhaskar.dash.model.*;

public class Example {
    public static void main(String[] args) {
        DashClient client = DashClient.builder()
            .baseUrl("http://localhost:8080")
            .apiKey("not_needed")
            .build();

        // 1. Embed
        EmbeddingResponse emb = client.embeddings().create(
            EmbeddingRequest.builder()
                .model("text-embedding-3-small")
                .input("hello world")
                .build()
        );
        System.out.println(emb.data().get(0).embedding().subList(0, 5));

        // 2. Ingest
        client.ingest().ingest(
            IngestRequest.builder()
                .claim(Claim.builder()
                    .claimId("c1")
                    .tenantId("t1")
                    .canonicalText("Company X acquired Company Y")
                    .confidence(0.95)
                    .build())
                .evidence(Evidence.builder()
                    .evidenceId("e1")
                    .claimId("c1")
                    .tenantId("t1")
                    .sourceId("news://nyt")
                    .stance(Stance.SUPPORTS)
                    .sourceQuality(0.95)
                    .build())
                .build()
        );

        // 3. Retrieve
        RetrieveResponse resp = client.retrieve().retrieve(
            RetrieveRequest.builder()
                .tenantId("t1")
                .query("Company X acquired Company Y")
                .topK(5)
                .stanceMode(StanceMode.SUPPORT_ONLY)
                .build()
        );
        resp.results().forEach(hit ->
            System.out.println(hit.claim().canonicalText() + " " + hit.score())
        );
    }
}
```

The Java SDK is built on the official `java.net.http` client (no Apache HttpClient, no OkHttp). **Tests:** unit tests in `dash-java/src/test/java`.

## C# — `Dash.NET`

**Install (NuGet):**

```bash
dotnet add package Dash.NET
```

**Quickstart:**

```csharp
using Dash;
using Dash.Models;

var client = new DashClient(new DashClientOptions
{
    BaseUrl = "http://localhost:8080",
    ApiKey = "not_needed",
});

// 1. Embed
var emb = await client.Embeddings.CreateAsync(new EmbeddingRequest
{
    Model = "text-embedding-3-small",
    Input = "hello world",
});
Console.WriteLine(string.Join(", ", emb.Data[0].Embedding.Take(5)));

// 2. Ingest
await client.Ingest.IngestAsync(new IngestRequest
{
    Claim = new Claim
    {
        ClaimId = "c1",
        TenantId = "t1",
        CanonicalText = "Company X acquired Company Y",
        Confidence = 0.95,
    },
    Evidence = new List<Evidence>
    {
        new Evidence
        {
            EvidenceId = "e1",
            ClaimId = "c1",
            TenantId = "t1",
            SourceId = "news://nyt",
            Stance = Stance.Supports,
            SourceQuality = 0.95,
        },
    },
});

// 3. Retrieve
var resp = await client.Retrieve.RetrieveAsync(new RetrieveRequest
{
    TenantId = "t1",
    Query = "Company X acquired Company Y",
    TopK = 5,
    StanceMode = StanceMode.SupportOnly,
});
foreach (var hit in resp.Results)
{
    Console.WriteLine($"{hit.Claim.CanonicalText} {hit.Score}");
}
```

Targets .NET 8+ and uses `System.Net.Http` directly. **Tests:** xUnit suite in `tests/Dash.NET.Tests`.

## Choosing an SDK

- **Python** — the path of least resistance. Most RAG frameworks have an OpenAI integration; `dash-py` is the simplest way to swap it for DASH.
- **Go** — for services that already have a Go control plane. The functional-options API matches the rest of the Go ecosystem.
- **TypeScript** — for Node.js backends and edge functions. Zero deps keeps the cold-start cost low.
- **Java** — for JVM shops. The builder pattern is idiomatic and the `java.net.http` dependency is JDK-built-in.
- **C#** — for .NET shops. The async/await surface is the same as `HttpClient`.

If you need a language that is not listed, the [HTTP API](../reference/api.md) is small and stable; a generated client (e.g. `openapi-generator`) is the recommended path.
