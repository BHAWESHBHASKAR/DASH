# Embeddings

The embeddings endpoint on the retrieval service accepts the same request body as `POST https://api.openai.com/v1/embeddings` and returns the same response shape, so OpenAI clients can point at it. Compatibility covers `input` (string or array), `model` (echoed back), `encoding_format` (`float` or `base64`) and the response envelope. It is not a full OpenAI clone: `dimensions` is not supported, and the vectors come from whichever provider DASH is configured with, not from the model named in the request.

## `POST /v1/embeddings`

!!! warning "Authentication"
    In v0.2.x this endpoint is **unauthenticated** (register SEC-09), which allows anonymous use of any paid embedding provider you configure. v0.3.0 requires authentication on `/v1/embeddings`. Until then, do not expose the retrieval port publicly. OpenAI SDKs send the key as `Authorization: Bearer <api_key>`, which DASH accepts, so set `api_key` to your retrieval key.

```text
POST /v1/embeddings
Content-Type: application/json
x-api-key: <retrieval api key>      (or Authorization: Bearer <key>)

{
  "input": "Company X acquired Company Y",
  "model": "text-embedding-3-small",
  "encoding_format": "float"
}
```

The response has the OpenAI v1 embeddings shape:

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

The `usage` field is a rough estimate (the count of whitespace-separated words); it exists for response-shape parity, not for billing.

## OpenAI drop-in

To use DASH with an OpenAI client, set the client base URL to `http://localhost:8080/v1` and use your retrieval API key:

=== "Python"

    ```python
    import os
    import openai

    client = openai.OpenAI(
        base_url="http://localhost:8080/v1",
        api_key=os.environ["DASH_RETRIEVAL_API_KEY"],
    )
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
        apiKey: process.env.DASH_RETRIEVAL_API_KEY,
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
        "os"
        openai "github.com/sashabaranov/go-openai"
    )

    func main() {
        cfg := openai.DefaultConfig(os.Getenv("DASH_RETRIEVAL_API_KEY"))
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
      -H "x-api-key: $DASH_RETRIEVAL_API_KEY" \
      -d '{
        "input": "hello world",
        "model": "text-embedding-3-small"
      }'
    ```

Request and response shapes are covered by unit tests in `services/retrieval/src/openai_embeddings.rs` (for example `response_shape_is_openai_compatible_for_single_input`, `error_response_shape_is_openai_compatible`) and HTTP-level tests in `services/retrieval/tests/transport_http.rs` (`transport_openai_embeddings_single_string_returns_openai_shape`, `transport_openai_embeddings_array_input_returns_indexed_results`). There is no test against the real `openai` SDK in this repository.

## Provider selection

The embedding backend is selected by `DASH_EMBEDDING_PROVIDER`. The supported values:

| Provider | Env vars | Network | Notes |
| -------- | -------- | ------- | ----- |
| `hash` | none | none | Deterministic, 384-dimension by default. The default provider. Not semantic. |
| `ollama` | `DASH_OLLAMA_ENDPOINT` (default `http://localhost:11434`), `DASH_OLLAMA_MODEL` (default `nomic-embed-text`) | local | Calls a local Ollama server. |
| `openai` | `DASH_OPENAI_API_KEY`, `DASH_OPENAI_MODEL` (default `text-embedding-3-small`) | outbound | In v0.2.x the client uses plain TCP without TLS and cannot reach `api.openai.com`; it also sends the key unencrypted (register SEC-23). v0.3.0 adds TLS. |
| custom | implement the `EmbeddingProvider` trait in `pkg/embeddings` | varies | Compile-time extension, not a runtime plugin. |

An unknown `DASH_EMBEDDING_PROVIDER` value, or `openai` without a key, falls back to `hash` with a warning on stderr. There is no `DASH_EMBEDDING_MODEL`, `DASH_OLLAMA_BASE_URL` or `DASH_OPENAI_BASE_URL`.

The selection is process-wide for the retrieval service. Per-tenant providers are not supported; the choice is a deployment-time decision.

### Hash provider

The default. The `HashEmbeddingProvider` in `pkg/embeddings` produces a deterministic 384-dimension vector (by default) from the input text by hashing the tokens into the dimension space. It is **not** a semantic embedding — it exists so that the retrieval path is exercisable without any external dependency. A deployment that needs semantic search should set `DASH_EMBEDDING_PROVIDER=ollama` (or `openai` once the v0.3.0 TLS support lands).

### Ollama provider

```bash
DASH_EMBEDDING_PROVIDER=ollama \
DASH_OLLAMA_ENDPOINT=http://ollama:11434 \
DASH_OLLAMA_MODEL=nomic-embed-text \
  docker compose -f deploy/container/docker-compose.yml up -d
```

The Ollama provider sends `POST /api/embeddings` to the configured endpoint. The model must already be pulled (`ollama pull nomic-embed-text`). The compose file passes only the variables it lists, so add these to the service `environment` block. The returned vector has the model's native dimension; DASH pins the tenant dimension at that tenant's first stored vector.

### OpenAI provider

```bash
DASH_EMBEDDING_PROVIDER=openai \
DASH_OPENAI_API_KEY=sk-... \
DASH_OPENAI_MODEL=text-embedding-3-small \
  docker compose -f deploy/container/docker-compose.yml up -d
```

The OpenAI provider forwards texts to `https://api.openai.com/v1/embeddings`. **It requires v0.3.0** (TLS support); see the table above.

## Base64 encoding

The OpenAI spec supports two `encoding_format` values:

- `float` (default) — the response carries the vector as an array of `float`. The HTTP body is JSON.
- `base64` — the response carries the vector as a base64-encoded little-endian float32 buffer. The HTTP body is JSON; the `embedding` field is a string.

DASH supports both. Base64 is smaller on the wire than a JSON float array.

```bash
curl -X POST http://localhost:8080/v1/embeddings \
  -H "Content-Type: application/json" \
  -H "x-api-key: $DASH_RETRIEVAL_API_KEY" \
  -d '{
    "input": "hello world",
    "model": "text-embedding-3-small",
    "encoding_format": "base64"
  }'
```

The response's `data[0].embedding` is a string of base64 characters; decode the base64 string and read it as little-endian `f32` values (four bytes each).

## Failure modes

| Condition | HTTP | Body |
| --- | ---: | --- |
| Missing or invalid key (v0.3.0) | 401 | `{"error":"..."}` |
| Invalid JSON, empty `input` array, unsupported `encoding_format` | 400 | OpenAI-style `{"error":{"message","type","param","code"}}` |
| Provider failure (for example Ollama is down) | 400 | OpenAI-style error with type `server_error`. The status is 400, not 502. |

There is no model validation and no input-length limit. For the other status codes, see [HTTP API](../reference/api.md#errors).
