# dash-java

A thin, idiomatic Java client for the [DASH](https://github.com/BHAWESHBHASKAR/DASH)
retrieval engine. DASH serves an OpenAI-compatible `/v1/embeddings`
endpoint and a native `/v1/retrieve` endpoint that returns structured
**Claim + Evidence + Contradiction** results — the differentiator
that makes it a real retrieval engine, not a vector store.

```xml
<dependency>
  <groupId>dev.dash</groupId>
  <artifactId>dash-java</artifactId>
  <version>0.2.0</version>
</dependency>
```

Java 17+ is required. The SDK ships zero Spring, zero Guava — just
OkHttp 4.12 and Jackson 2.17.

## 5-minute quickstart

### 1. Embeddings (OpenAI-compatible)

```java
import dev.dash.DashClient;
import dev.dash.model.EmbedRequest;
import dev.dash.model.EmbeddingResponse;

DashClient client = new DashClient("http://localhost:8080", null);
EmbeddingResponse response = client.embed(EmbedRequest.of("hello world"));
System.out.println(response.data().get(0).embedding().subList(0, 5));
// [0.013, -0.042, 0.077, 0.0, 0.5]
```

That's it — the same wire shape as OpenAI's `/v1/embeddings`.

### 2. Drop-in OpenAI compatibility (zero code change)

If you already use the official OpenAI Java SDK, you don't even need
`dash-java` to talk to DASH — just point the base URL at DASH:

```java
import com.openai.client.OpenAIClient;
import com.openai.client.okhttp.OpenAIClientOkHttp;

OpenAIClient openai = OpenAIClientOkHttp.builder()
        .baseUrl("http://localhost:8080/v1")
        .apiKey("not-required-for-local")
        .build();

var resp = openai.embeddings().createBuilder()
        .input("hello world")
        .model("text-embedding-3-small")
        .build();
System.out.println(resp.data().get(0).embedding());
```

This is the same path LangChain, LlamaIndex, and the OpenAI CLI use.

### 3. Native retrieve (the differentiator)

Where DASH pulls ahead of a vector store is the retrieval endpoint,
which returns Claims with explicit **support** and **contradiction**
counts and a structured `Citation[]` list per claim.

```java
import dev.dash.DashClient;
import dev.dash.model.RetrievalRequest;
import dev.dash.model.RetrievalResponse;

DashClient client = new DashClient("http://localhost:8080", null);
RetrievalResponse response = client.retrieve(
        new RetrievalRequest("acme-corp", "what was the Q3 2024 revenue?", 5, "balanced", null));

// A RAG pipeline that wants "claims that are actually supported and
// not contradicted" can filter the typed results directly:
var cleanClaims = response.results().stream()
        .filter(r -> r.contradicts() == 0 && r.supports() > 0)
        .toList();

for (var claim : response.results()) {
    System.out.printf("[%.2f] %s%n", claim.score(), claim.canonicalText());
    System.out.printf("    supports=%d contradicts=%d%n", claim.supports(), claim.contradicts());
    for (var cite : claim.citations()) {
        System.out.printf("    - %-11s  %s  (q=%.2f)%n",
                cite.stance(), cite.sourceId(), cite.sourceQuality());
    }
}
```

Use `stanceMode = "support_only"` to filter out claims whose
contradiction tally exceeds their support tally before they reach
your prompt.

### 4. Ingest (experimental)

DASH runs retrieval (embeddings, retrieve, health) and ingestion as
two services, by default on ports 8080 and 8081. Pass the ingestion
URL as the second constructor argument:

```java
DashClient client = new DashClient(
        "http://localhost:8080", "http://localhost:8081", "sk-live-...");
```

If `ingestionBaseUrl` is omitted it is derived only for the local
layout (`:8080` becomes `:8081`); otherwise `ingest(...)` throws
`IllegalStateException`. Both URLs are validated at construction.

The ingest models mirror the server's `POST /v1/ingest` body
(`claim`, `evidence`, `edges`) and response (`ingested_claim_id`,
`claims_total`, `commit_status`, ...):

```java
import dev.dash.model.*;

var claim = new IngestClaim("c-1", "acme", "Q3 revenue was $1B", 0.9);
var evidence = List.of(
        new IngestEvidence("e-1", "c-1", "src-1", "supports", 0.8));

IngestResponse ingest = client.ingest(new IngestRequest(claim, evidence));
System.out.println(ingest.ingestedClaimId() + " " + ingest.commitStatus());
```

The ingest API is **experimental** and will change when the server
API is versioned. Ingest is sent exactly once and is never retried
automatically; to allow retries pass an idempotency key:
`client.ingest(req, RequestOptions.withIdempotencyKey("..."))`.

> `client.delete(...)` was removed in 0.2.0: the server has no
> `/v1/delete` endpoint.

### 5. Authentication & error handling

```java
import dev.dash.DashClient;
import dev.dash.DashException;
import dev.dash.DashConnectionException;
import dev.dash.model.EmbedRequest;
import java.time.Duration;

DashClient client = new DashClient("https://dash.example.com", "sk-live-...")
        .withTimeout(Duration.ofSeconds(10))
        .withMaxRetries(5);

try {
    var resp = client.embed(EmbedRequest.of("hello"));
} catch (DashConnectionException exc) {
    // Network failure (DNS, refused, timeout). The underlying
    // OkHttp IOException is on getCause().
    System.err.println("connectivity: " + exc.getMessage());
} catch (DashException exc) {
    // Non-2xx response. statusCode, errorCode, and requestId are
    // populated when the server provides them.
    System.err.printf("DASH %d %s: %s (request=%s)%n",
            exc.getStatusCode(), exc.getErrorCode(),
            exc.getMessage(), exc.getRequestId());
}
```

Both `DashException` and `DashConnectionException` are unchecked
(`RuntimeException`), so they don't force a `throws` clause on
your methods. Catch the base `DashException` to handle "anything
went wrong talking to DASH" without caring about the specific
failure mode.

## API surface

| Symbol | Purpose |
| --- | --- |
| `DashClient(baseUrl, apiKey)` | Construct a client (ingestion URL derived for `:8080`). |
| `DashClient(baseUrl, ingestionBaseUrl, apiKey)` | Construct a client with an explicit ingestion service URL. |
| `withTimeout(Duration)` | Set connect/read/write timeouts. |
| `withMaxRetries(int)` | Set max attempts (1 initial + N-1 retries). |
| `client.embed(EmbedRequest)` | `POST /v1/embeddings`. |
| `client.ingest(IngestRequest[, RequestOptions])` | `POST /v1/ingest` on the ingestion service (experimental, never retried by default). |
| `client.retrieve(RetrievalRequest)` | `POST /v1/retrieve`. |
| `client.health()` | `GET /health`. |
| `DashException` / `DashConnectionException` | Exception hierarchy. |

## Configuration reference

```java
new DashClient(baseUrl, ingestionBaseUrl, apiKey)  // defaults: 10s connect, 30s read, 30s write, 3 attempts
        .withTimeout(Duration.ofSeconds(5))
        .withMaxRetries(5);
```

`withTimeout` and `withMaxRetries` return a *new* client; the
original is untouched, so the builder is safe to share across
threads.

Retries apply only to idempotent requests (embeddings, retrieve,
health) or requests sent with an `Idempotency-Key`. They use
jittered exponential backoff (capped at 5 s), honour `Retry-After`
(up to 30 s) and are capped at 10 attempts.

## Why a Java SDK for DASH?

DASH already speaks the OpenAI wire format, so most users can use
the official OpenAI Java SDK. `dash-java` exists for the cases
that need a typed, first-class client:

- Typed response records (so IDEs autocomplete `.canonicalText()`,
  `.supports()`, etc.).
- Native access to `/v1/retrieve` with `stanceMode` and
  `returnGraph` flags.
- A consistent exception hierarchy (`DashConnectionException` vs
  `DashException`) instead of inspecting OkHttp exceptions by
  hand.
- A fluent timeout / retry configuration that doesn't require
  Spring or Guava.
- Kotlin-friendly records and `suspend` coroutine wrappers live
  in the sibling `dash-kotlin` artifact.

## License

Apache-2.0.
