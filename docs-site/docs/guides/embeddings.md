# Embeddings

The embeddings endpoint is the OpenAI drop-in. It accepts the same request body as `POST https://api.openai.com/v1/embeddings` and returns the same response shape. Any client that works with OpenAI works with DASH.

## `POST /v1/embeddings`

```text
POST /v1/embeddings
Content-Type: application/json
Authorization: Bearer <jwt>   (optional; some OpenAI clients send a dummy key)

{
  "input": "Company X acquired Company Y",
  "model": "text-embedding-3-small",
  "encoding_format": "float"
}
```

The response is byte-compatible with the OpenAI v1 embeddings response:

```json
{
  "object": "list",
  "data": [
    {
      "object": "embedding",
      "embedding": [0.0123, -0.0456, 0.0789, ...],
      "index": 0
    }
  ],
  "model": "text-embedding-3-small",
  "usage": { "prompt_tokens": 7, "total_tokens": 7 }
}
```

The `usage` field is populated heuristically (1 token ≈ 4 characters); the number is approximate and intended for parity with OpenAI's response shape, not for billing.

## OpenAI drop-in

To use DASH as a drop-in for any OpenAI client, point the client at DASH with one environment variable:

=== "Python"

    ```python
    import os
    import openai

    os.environ["OPENAI_API_BASE"] = "http://localhost:8080/v1"
    client = openai.OpenAI(api_key="not_needed")
    resp = client.embeddings.create(
        input="hello world",
        model="text-embedding-3-small",
    )
    print(resp.data[0].embedding[:5])
    ```

=== "TypeScript"

    ```typescript
    import OpenAI from "openai";

    const client = new OpenAI({
        apiKey: "not_needed",
        baseURL: "http://localhost:8080/v1",
    });

    const resp = await client.embeddings.create({
        model: "text-embedding-3-small",
        input: "hello world",
    });
    console.log(resp.data[0].embedding.slice(0, 5));
    ```

=== "Go"

    ```go
    package main

    import (
        "context"
        "fmt"
        openai "github.com/sashabaranov/go-openai"
    )

    func main() {
        cfg := openai.DefaultConfig("not_needed")
        cfg.BaseURL = "http://localhost:8080/v1"
        client := openai.NewClientWithConfig(cfg)

        resp, err := client.CreateEmbeddings(context.Background(), openai.EmbeddingRequest{
            Model: openai.SmallEmbeddingModel,
            Input: []string{"hello world"},
        })
        if err != nil {
            panic(err)
        }
        fmt.Println(resp.Data[0].Embedding[:5])
    }
    ```

=== "curl"

    ```bash
    curl -X POST http://localhost:8080/v1/embeddings \
      -H "Content-Type: application/json" \
      -d '{
        "input": "hello world",
        "model": "text-embedding-3-small"
      }'
    ```

The wire-level compatibility is exercised by 17 tests in `tests/retrieval/integration_embeddings.rs`. The tests cover request/response byte parity, error envelope parity, and HTTP-level integration with the `openai` Python SDK.

## Provider selection

The embedding backend is selected by `DASH_EMBEDDING_PROVIDER`. The supported values:

| Provider | Env var                                                       | Network    | Notes                                                                 |
| -------- | ------------------------------------------------------------- | ---------- | --------------------------------------------------------------------- |
| `hash`   | (none)                                                        | none       | Deterministic, dimension-pinned, 768-dim. The default.                |
| `ollama` | `DASH_OLLAMA_BASE_URL`, `DASH_EMBEDDING_MODEL`                | local      | Calls a local Ollama server. Default model: `nomic-embed-text`.       |
| `openai` | `DASH_OPENAI_API_KEY`, `DASH_OPENAI_BASE_URL`, `DASH_EMBEDDING_MODEL` | outbound | Proxies to OpenAI's `/v1/embeddings`. Useful for migration windows.    |
| custom   | (implement the `EmbeddingProvider` trait)                     | varies     | Plug in any HTTP backend that takes text and returns a float vector.  |

The selection is process-wide for the retrieval service. A multi-tenant deployment that needs different providers per tenant is **not** supported in the current release; the choice is a deployment-time decision.

### Hash provider

The default. The `HashEmbeddingProvider` in `pkg/embeddings` produces a deterministic 768-dim vector from the input text by hashing the tokens into the dimension space and L2-normalizing. It is **not** a semantic embedding — it exists so that the retrieval path is exercisable without any external dependency. A deployment that needs real semantic search must set `DASH_EMBEDDING_PROVIDER=ollama` or `=openai`.

### Ollama provider

```bash
DASH_EMBEDDING_PROVIDER=ollama \
DASH_OLLAMA_BASE_URL=http://ollama:11434 \
DASH_EMBEDDING_MODEL=nomic-embed-text \
  docker compose -f deploy/container/docker-compose.yml up -d
```

The Ollama provider sends `POST /api/embeddings` to the configured base URL. The model must already be pulled (`ollama pull nomic-embed-text`). The returned vector is the model's native dimension; DASH pins the tenant dimension on first use.

### OpenAI provider

```bash
DASH_EMBEDDING_PROVIDER=openai \
DASH_OPENAI_API_KEY=sk-... \
DASH_EMBEDDING_MODEL=text-embedding-3-small \
  docker compose -f deploy/container/docker-compose.yml up -d
```

The OpenAI provider is a passthrough — it forwards the request to `https://api.openai.com/v1/embeddings` and returns the response. Useful for migration windows where DASH is in the path but the embedding is still coming from OpenAI.

## Base64 encoding

The OpenAI spec supports two `encoding_format` values:

- `float` (default) — the response carries the vector as an array of `float`. The HTTP body is JSON.
- `base64` — the response carries the vector as a base64-encoded little-endian float32 buffer. The HTTP body is JSON; the `embedding` field is a string.

DASH supports both. The base64 form is ~3× cheaper on the wire for a 768-dim vector and is recommended for high-throughput callers.

```bash
curl -X POST http://localhost:8080/v1/embeddings \
  -H "Content-Type: application/json" \
  -d '{
    "input": "hello world",
    "model": "text-embedding-3-small",
    "encoding_format": "base64"
  }'
```

The response's `data[0].embedding` is a string of base64 characters; decode as `base64::decode(...).chunks(4).map(f32::from_le_bytes)`.

## Failure modes

| Condition                                          | HTTP | `error.code`                |
| -------------------------------------------------- | ---: | --------------------------- |
| Missing or invalid JWT                             |  401 | `dash_unauthenticated`      |
| `model` not recognized                             |  400 | `dash_invalid_model`        |
| `input` empty                                      |  400 | `dash_validation_error`     |
| `input` exceeds `MAX_EMBEDDING_INPUT_CHARS`        |  400 | `dash_input_too_long`       |
| Provider returns an error (e.g. Ollama down)       |  502 | `dash_provider_error`       |
| Internal error                                     |  500 | `dash_internal_error`       |

For the full error envelope, see [HTTP API → Error codes](../reference/api.md#error-codes).
