# SDKs

Six client SDKs live under `sdks/` in the repository. None is published to a package registry yet (no PyPI, npm, Maven Central or NuGet release exists), so install them from a checkout. This page lists what each one covers and links to the SDK's own README for the API details; the READMEs are the reference for method names.

## Coverage

| SDK | Directory | Version | Embeddings | Retrieve | Ingest | Notes |
|---|---|---|---|---|---|---|
| Python (`dash-py`, import `dash`) | `sdks/python` | 0.1.0 | yes | yes | no (types only) | Sync `Client` and `AsyncClient`. |
| Go | `sdks/go` | untagged | yes | yes | no | Module path in `go.mod` is `github.com/anomalyco/dash-go`; this does not match the repository owner, so `go get` from the repository URL does not work yet. |
| TypeScript (`dash-ts`) | `sdks/typescript` | 0.1.0 | yes | yes | no | ESM, Node 18+. |
| Java | `sdks/java` | 0.2.0 | yes | yes | yes | Also exposes `delete`, which calls `POST /v1/delete`. The server has no such route; do not use it. |
| Kotlin | `sdks/kotlin` | 0.2.0 | yes | yes | yes | Suspend API. Same `delete` caveat. |
| C# | `sdks/csharp` | 0.2.0 | yes | yes | yes | Same `delete` caveat. |

In v0.3.0 the Java, Kotlin and C# SDKs are fixed and unified at version 0.2.0 (see the [changelog](../about/changelog.md)); the ingest methods in particular should be re-checked against the [HTTP API](../reference/api.md#ingestion-service) request shape (`claim`, `evidence`, `edges`) until then. For ingest from Python, Go or TypeScript, call `POST /v1/ingest` directly.

Static test counts (declarations, including live-integration tests that need a running server): Python 64, Go 89, TypeScript 69, Java 21, Kotlin 12, C# 41.

## Authentication

The SDKs take an API key and send it as `Authorization: Bearer <key>`, which the services accept. Use the **retrieval** key for retrieve and embeddings calls and the **ingestion** key for ingest calls; they are different credentials on different services (`:8080` and `:8081` by default). One client instance targets one base URL.

## Python

Install from a checkout and use the retrieval service URL:

```bash
pip install ./sdks/python
```

```python
import os
from dash import Client

client = Client(base_url="http://localhost:8080", api_key=os.environ["DASH_RETRIEVAL_API_KEY"])

emb = client.embeddings.create("hello world", model="text-embedding-3-small")
print(emb.data[0].embedding[:5])

resp = client.retrieve(
    tenant_id="t1",
    query="Company X acquired Company Y",
    top_k=5,
    stance_mode="support_only",
)
for result in resp.results:
    print(result.canonical_text, result.score, result.supports, result.contradicts)
    for cite in result.citations:
        print("  ", cite.stance, cite.source_id, cite.source_quality)
```

An `AsyncClient` offers the same calls for `asyncio`. See `sdks/python/README.md` and `sdks/python/examples/`.

## Go, TypeScript, Java, Kotlin, C#

Each SDK has a README with install-from-source instructions and examples:

- Go: `sdks/go/README.md` (functional options, `client.Embeddings()`, retrieve with typed results, `errors.Is` / `errors.As` support).
- TypeScript: `sdks/typescript/README.md` (ESM, typed errors).
- Java: `sdks/java/README.md` (OkHttp and Jackson based, Java 17 builder API).
- Kotlin: `sdks/kotlin/README.md` (coroutines).
- C#: `sdks/csharp/README.md` (`DashClient`, sync and async methods).

Where an SDK README shows a `delete` call, an unpublished package registry install command, or the phrase "byte-for-byte compatible", read it with the caveats above.

## OpenAI clients

Any OpenAI embeddings client can use the retrieval service's `/v1/embeddings` by setting its base URL to `http://localhost:8080/v1` and its API key to your retrieval key. See the [Embeddings guide](embeddings.md) for provider selection and the v0.3.0 authentication change.

## Live integration tests

`sdks/LIVE_INTEGRATION_TESTS.md` describes running each SDK's live suite against a running stack. Pass the keys explicitly; the earlier assumption that no key is needed does not hold once credentials are configured.
