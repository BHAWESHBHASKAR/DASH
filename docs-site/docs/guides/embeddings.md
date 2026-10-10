# Embeddings

The embeddings endpoint on the retrieval service accepts the same request body as `POST https://api.openai.com/v1/embeddings` and returns the same response shape, so OpenAI clients can point at it. Compatibility covers `input` (string or array), `model` (echoed back), `encoding_format` (`float` or `base64`) and the response envelope. It is not a full OpenAI clone: `dimensions` is accepted only if it equals the configured provider's dimension, and the vectors come from whichever provider DASH is configured with, not from the model named in the request.

## `POST /v1/embeddings`

!!! warning "Authentication"
    Since 0.3.0 this endpoint requires a credential with the `retrieve` role (or `admin`), and authentication runs before any provider call, so anonymous callers cannot spend a paid embedding provider's quota (register SEC-09). Before 0.3.0 the endpoint was open. OpenAI SDKs send the key as `Authorization: Bearer <api_key>`, which DASH accepts, so set `api_key` to your retrieval key. Requests are subject to the per-tenant rate limit (429).

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

The `usage` field is an estimate: `ceil(characters / 4)` per input (at least 1), not a tokenizer count, and `total_tokens` equals `prompt_tokens`. It exists for response-shape parity, not for billing.

**Input rules.** `input` is a string or an array of strings: at most 2048 inputs, none empty, at most `DASH_EMBEDDING_MAX_TOTAL_CHARS` (default 524288) characters in total. Arrays of token ids (what some clients send by default) are **rejected** with 400 `unsupported_input_type`, because ids cannot be decoded without the client's tokenizer; send text, or set `DASH_EMBEDDING_ALLOW_TOKEN_IDS=1` to embed the decimal ids joined by spaces (deterministic, but not equivalent to embedding the decoded text). `dimensions`, if sent, must equal the provider's dimension.

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
| `openai` | `DASH_OPENAI_API_KEY`, `DASH_OPENAI_MODEL` (default `text-embedding-3-small`) | outbound HTTPS | Calls `https://api.openai.com/v1/embeddings` (the endpoint is fixed). The client refuses to send the key over plaintext HTTP to a non-loopback host. Tests do not call the real API. |
| custom | implement the `EmbeddingProvider` trait in `pkg/embeddings` | varies | Compile-time extension, not a runtime plugin. |

An unknown `DASH_EMBEDDING_PROVIDER` value, or `openai` without a key, falls back to `hash` with a warning on stderr. There is no `DASH_EMBEDDING_MODEL`, `DASH_OLLAMA_BASE_URL` or `DASH_OPENAI_BASE_URL`.

The selection is process-wide for the retrieval service. Per-tenant providers are not supported; the choice is a deployment-time decision.

### Hash provider

The default. The `HashEmbeddingProvider` in `pkg/embeddings` produces a deterministic 384-dimension vector (by default) from the input text by hashing the tokens into the dimension space. It is **not** a semantic embedding — it exists so that the retrieval path is exercisable without any external dependency. A deployment that needs semantic search should set `DASH_EMBEDDING_PROVIDER=ollama` (or `openai`).

### Ollama provider

```bash
DASH_EMBEDDING_PROVIDER=ollama \
DASH_OLLAMA_ENDPOINT=http://ollama:11434 \
DASH_OLLAMA_MODEL=nomic-embed-text \
  docker compose -f deploy/container/docker-compose.yml up -d
```

The Ollama provider sends `POST /api/embed` (a bare base URL such as `http://ollama:11434` is expanded to that path; an endpoint ending in `/api/embeddings` uses the older prompt-shaped request). `DASH_OLLAMA_BASE_URL` is accepted as a deprecated alias of `DASH_OLLAMA_ENDPOINT`. The model must already be pulled (`ollama pull nomic-embed-text`). The compose file passes only the variables it lists, so add these to the service `environment` block. The returned vector has the model's native dimension; DASH pins the tenant dimension at that tenant's first stored vector.

### OpenAI provider

```bash
DASH_EMBEDDING_PROVIDER=openai \
DASH_OPENAI_API_KEY=sk-... \
DASH_OPENAI_MODEL=text-embedding-3-small \
  docker compose -f deploy/container/docker-compose.yml up -d
```

The OpenAI provider forwards texts to `https://api.openai.com/v1/embeddings` over TLS. Query and claim text leave your network, so treat it as sensitive-data egress. Provider calls are bounded (response size cap, total deadline, no redirects, jittered retries honoring `Retry-After`) and wrapped by a circuit breaker.

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
| Missing or invalid credential | 401 | `{"error":"..."}` (DASH error shape) |
| Credential lacks the `retrieve` role | 403 | `{"error":"..."}` |
| Rate limit exceeded | 429 | `{"error":"rate limit exceeded"}` plus `Retry-After` |
| Invalid JSON, empty or too many inputs, unsupported `encoding_format`, token-id input, wrong `dimensions`, input too long | 400 | OpenAI-style `{"error":{"message","type","param","code"}}` with a code such as `empty_input`, `too_many_inputs`, `input_too_long`, `invalid_encoding_format`, `unsupported_input_type`, `unsupported_dimensions` |
| Provider unavailable (timeout, circuit breaker open, invalid provider configuration) | 503 | OpenAI-style error, type `server_error`, code `embedding_unavailable` |
| Provider returned an error | 502 | OpenAI-style error, type `server_error`, code `embedding_provider_error` (details are logged, not returned) |

There is no model validation. For the other status codes, see [HTTP API](../reference/api.md#errors).
