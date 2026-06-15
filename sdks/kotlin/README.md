# dash-kotlin

Kotlin coroutine wrappers around [`dash-java`](../java/), the
[OpenAI-compatible](https://github.com/dash-retrieval/dash) DASH
retrieval engine. Every blocking call is wrapped in a `suspend fun`
that hops to `Dispatchers.IO` before delegating to the synchronous
Java client, so you can `await` results from any coroutine context.

```kotlin
implementation("dev.dash:dash-kotlin:0.2.0")
```

Requires Kotlin 1.9+, JVM target 17, and `kotlinx-coroutines-core`
1.8.x.

## Gradle setup

```kotlin
// build.gradle.kts
plugins {
    kotlin("jvm") version "1.9.24"
}

dependencies {
    implementation("dev.dash:dash-kotlin:0.2.0")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.8.1")
}
```

`dash-kotlin` transitively pulls in `dash-java`, OkHttp 4.12, and the
Kotlin-aware Jackson module, so you do not need to add them
separately.

## 5-minute quickstart

### 1. Embeddings (Kotlin, coroutines)

```kotlin
import dev.dash.DashClientAsync
import dev.dash.model.EmbedRequest
import kotlinx.coroutines.runBlocking

fun main() = runBlocking {
    val client = DashClientAsync(dev.dash.DashClient("http://localhost:8080", null))
    val resp = client.embed(EmbedRequest.of("hello world"))
    println(resp.data.first().embedding.take(5))
    // [0.013, -0.042, 0.077, 0.0, 0.5]
}
```

That's it — the same wire shape as OpenAI's `/v1/embeddings`.

### 2. Native retrieve (the differentiator)

Where DASH pulls ahead of a vector store is the retrieval endpoint,
which returns Claims with explicit **support** and **contradiction**
counts and a structured `Citation[]` list per claim.

```kotlin
import dev.dash.DashClientAsync
import dev.dash.DashClient
import dev.dash.model.RetrievalRequest
import kotlinx.coroutines.runBlocking

fun main() = runBlocking {
    val client = DashClientAsync(DashClient("http://localhost:8080", null))
    val resp = client.retrieve(
        RetrievalRequest("acme-corp", "what was the Q3 2024 revenue?", 5, "balanced", null)
    )

    // A RAG pipeline that wants "claims that are actually supported
    // and not contradicted" can filter the typed results directly.
    val clean = resp.results.filter { it.contradicts == 0 && it.supports > 0 }

    resp.results.forEach { claim ->
        println("[${"%.2f".format(claim.score)}] ${claim.canonicalText}")
        println("    supports=${claim.supports} contradicts=${claim.contradicts}")
        claim.citations.forEach { cite ->
            println("    - ${cite.stance.padStart(11)}  ${cite.sourceId}  (q=${"%.2f".format(cite.sourceQuality)})")
        }
    }
}
```

Use `stanceMode = "support_only"` to filter out claims whose
contradiction tally exceeds their support tally before they reach
your prompt.

### 3. Drop-in OpenAI compatibility (zero code change)

If you already use the official `openai-java` SDK, you don't even
need `dash-kotlin` to talk to DASH — just point the base URL at
DASH and the wire format is byte-for-byte compatible:

```kotlin
import com.openai.client.OpenAIClient
import com.openai.client.okhttp.OpenAIClientOkHttp

val openai: OpenAIClient = OpenAIClientOkHttp.builder()
    .baseUrl("http://localhost:8080/v1")
    .apiKey("not-required-for-local")
    .build()

val resp = openai.embeddings().createBuilder()
    .input("hello world")
    .model("text-embedding-3-small")
    .build()
println(resp.data().first().embedding())
```

This is the same path LangChain, LlamaIndex, and the OpenAI CLI
use, so they all work too.

### 4. Ingest and delete

```kotlin
import dev.dash.model.*

val bundle = IngestBundle(
    claim = IngestClaim("c-1", "acme", "Q3 revenue was $1B", 0.9, null),
    evidence = listOf(
        IngestEvidence("e-1", "src-1", "supports", 0.8,
            chunkId = null, spanStart = null, spanEnd = null,
            docId = null, extractionModel = null, rawText = "raw chunk")
    )
)

val ingest = client.ingest(IngestRequest("acme", bundle))
println("accepted=${ingest.accepted} rejected=${ingest.rejected}")

val deleted = client.delete(DeleteRequest("acme", listOf("c-1"), deleteEvidence = true))
println("deleted=${deleted.deleted} missing=${deleted.missing}")
```

### 5. Error handling

```kotlin
import dev.dash.DashException
import dev.dash.DashConnectionException

try {
    val resp = client.embed(EmbedRequest.of("hello"))
} catch (e: DashConnectionException) {
    // Network failure (DNS, refused, timeout).
    println("connectivity: ${e.message}")
} catch (e: DashException) {
    // Non-2xx response. statusCode, errorCode, and requestId are
    // populated when the server provides them.
    println("DASH ${e.statusCode} ${e.errorCode}: ${e.message} (request=${e.requestId})")
}
```

Both exceptions are unchecked, so they don't force a `throws`
clause. Catch the base `DashException` to handle "anything went
wrong talking to DASH" without caring about the specific failure
mode.

## Java interop

Because `DashClientAsync` is a thin wrapper around
`dev.dash.DashClient`, you can mix and match. For example, configure
the underlying Java client (timeout, retries) and then hand it to
the async wrapper:

```kotlin
import dev.dash.DashClient
import dev.dash.DashClientAsync
import java.time.Duration

val sync = DashClient("http://localhost:8080", "sk-live-...")
    .withTimeout(Duration.ofSeconds(5))
    .withMaxRetries(5)

val client = DashClientAsync(sync)
```

The Kotlin SDK exposes the underlying client via `client.sync()` for
escaping back to the blocking API when needed (e.g. inside a
`runBlocking { ... }` test or a Java method).

## API surface

| Symbol | Purpose |
| --- | --- |
| `DashClientAsync(delegate)` | Construct an async wrapper. |
| `suspend fun embed(req)` | `POST /v1/embeddings`. |
| `suspend fun ingest(req)` | `POST /v1/ingest`. |
| `suspend fun retrieve(req)` | `POST /v1/retrieve`. |
| `suspend fun delete(req)` | `POST /v1/delete`. |
| `suspend fun health()` | `GET /health`. |
| `fun sync(): DashClient` | Escape hatch to the blocking Java client. |

## Why a Kotlin SDK for DASH?

`dash-java` is already perfectly callable from Kotlin — but the
async wrapper is worth the 80 lines:

- A single `import` exposes `suspend` functions for every
  endpoint, so call sites read like ordinary coroutines.
- The wrapper handles the `Dispatchers.IO` hop, so callers don't
  have to wrap each call in `withContext`.
- A consistent exception hierarchy (`DashConnectionException` vs
  `DashException`) translates cleanly into Kotlin's checked-free
  `try` / `catch` style.

If you're already on the JVM but not yet on Kotlin, the records in
`dev.dash.model` are still consumable from Java — see the
[`dash-java` README](../java/README.md) for that flow.

## License

Apache-2.0.
