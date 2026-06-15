# HTTP API

DASH exposes its API as plain HTTP/1.1 with JSON request and response bodies. There is no gRPC, no GraphQL, no WebSocket. The endpoints are documented here in the order a new integrator would encounter them.

## Base URLs

| Environment | Ingestion base          | Retrieval base          |
| ----------- | ----------------------- | ----------------------- |
| Local       | `http://localhost:8081` | `http://localhost:8080` |
| Staging     | `https://ingest.staging.dash.example.com` | `https://dash.staging.dash.example.com` |
| Production  | `https://ingest.dash.example.com`         | `https://dash.dash.example.com`         |

The ingestion and retrieval services have **separate** base URLs and **separate** auth scopes. A JWT scoped to `claims:read` is sufficient for the retrieval service; the same JWT cannot write to the ingestion service.

## Authentication

All endpoints (except `/v1/health` and `/metrics`) require authentication. Two schemes are supported:

```text
Authorization: Bearer <jwt>          # JWT bearer token
Authorization: DashKey <ak_...>      # scoped API key
```

A request without an `Authorization` header returns HTTP 401 with `dash_unauthenticated`. A request with a valid token but insufficient scope returns HTTP 403 with `dash_forbidden_scope`. See [Security](../concepts/security.md) for the full model.

## Endpoints

### `POST /v1/embeddings`

The OpenAI-drop-in embeddings endpoint. See [Embeddings guide](../guides/embeddings.md) for the full walkthrough.

=== "Request"

    ```json
    {
      "input": "Company X acquired Company Y",
      "model": "text-embedding-3-small",
      "encoding_format": "float"
    }
    ```

=== "Response (200 OK)"

    ```json
    {
      "object": "list",
      "data": [
        {
          "object": "embedding",
          "embedding": [0.0123, -0.0456, 0.0789, "..."],
          "index": 0
        }
      ],
      "model": "text-embedding-3-small",
      "usage": { "prompt_tokens": 7, "total_tokens": 7 }
    }
    ```

### `POST /v1/ingest`

The write path. See [Ingest guide](../guides/ingest.md) for the full walkthrough.

=== "Request"

    ```json
    {
      "claim": {
        "claim_id": "c1",
        "tenant_id": "t1",
        "canonical_text": "Company X acquired Company Y",
        "confidence": 0.95,
        "event_time_unix": 1718300000,
        "valid_from_unix": 1718300000,
        "valid_to_unix": null,
        "extraction_model": "claude-opus-4-7"
      },
      "evidence": [
        {
          "evidence_id": "e1",
          "claim_id": "c1",
          "tenant_id": "t1",
          "source_id": "news://nyt/2024/05/12/acme-x",
          "stance": "supports",
          "source_quality": 0.95,
          "chunk_id": "chunk-7",
          "span_start": 1024,
          "span_end": 1280
        }
      ],
      "edges": [],
      "idempotency_key": "ingest-2024-05-12-c1"
    }
    ```

=== "Response (202 Accepted)"

    ```json
    {
      "claim_id": "c1",
      "evidence_ids": ["e1"],
      "edge_ids": [],
      "ingest_seq": 17,
      "idempotent_replay": false
    }
    ```

### `POST /v1/retrieve`

The read path. See [Retrieve guide](../guides/retrieve.md) for the full walkthrough.

=== "Request"

    ```json
    {
      "tenant_id": "t1",
      "query": "Company X acquired Company Y",
      "top_k": 5,
      "stance_mode": "support_only",
      "time_range": { "from_unix": 1718300000, "to_unix": 1735609600 },
      "hybrid_alpha": 0.7
    }
    ```

=== "Response (200 OK)"

    ```json
    {
      "results": [
        {
          "claim": {
            "claim_id": "c1",
            "tenant_id": "t1",
            "canonical_text": "Company X acquired Company Y",
            "confidence": 0.95
          },
          "score": 1.0,
          "supports": 1,
          "contradicts": 0,
          "citations": [
            {
              "source_id": "news://nyt/2024/05/12/acme-x",
              "stance": "supports",
              "source_quality": 0.95,
              "chunk_id": "chunk-7",
              "span_start": 1024,
              "span_end": 1280
            }
          ]
        }
      ],
      "took_us": 1240
    }
    ```

### `GET /v1/health`

Liveness/readiness. Always returns 200 if the process is up.

=== "Response (200 OK)"

    ```json
    { "status": "ok", "service": "retrieval", "version": "0.1.0" }
    ```

### `GET /metrics`

Prometheus exposition. See [Observability → /metrics](../operations/observability.md#metrics-endpoint) for the full metrics list.

=== "Response (200 OK)"

    ```text
    # HELP dash_retrieve_requests_total Total number of retrieve requests.
    # TYPE dash_retrieve_requests_total counter
    dash_retrieve_requests_total{tenant_id="t1",outcome="ok"} 1283
    ...
    ```

## Error codes

Every error response is a JSON object with the same envelope:

```json
{
  "error": {
    "code": "dash_validation_error",
    "message": "confidence must be in [0, 1]",
    "request_id": "01HMRX..."
  }
}
```

The `request_id` is a ULID that is also emitted in the structured log and the `dash_request_id` HTTP response header. Use it when filing a bug.

| `code`                          | HTTP | Meaning                                                          |
| ------------------------------- | ---: | ---------------------------------------------------------------- |
| `dash_unauthenticated`          |  401 | No or invalid `Authorization` header.                            |
| `dash_forbidden_scope`          |  403 | Token is valid but missing the required scope.                   |
| `dash_tenant_mismatch`          |  403 | `tenant_id` in the request does not match the JWT's tenant.      |
| `dash_validation_error`         |  400 | Request body fails schema validation.                            |
| `dash_claim_conflict`           |  409 | `claim_id` already exists with different content.                |
| `dash_idempotency_mismatch`     |  409 | Same `idempotency_key` was used with a different body.           |
| `dash_tenant_not_found`         |  404 | The named tenant does not exist.                                 |
| `dash_dimension_mismatch`       |  400 | `query_vector` length does not match the tenant's pinned dim.    |
| `dash_input_too_long`           |  400 | `input` exceeds the max length for the embeddings endpoint.      |
| `dash_invalid_model`            |  400 | The `model` field is not recognized.                             |
| `dash_payload_too_large`        |  413 | Request body exceeds the configured cap.                         |
| `dash_rate_limited`             |  429 | Per-tenant rate limit exceeded; `Retry-After` header included.   |
| `dash_provider_error`           |  502 | Upstream embedding provider returned an error.                   |
| `dash_internal_error`           |  500 | Server-side error. The `request_id` is the link to the logs.     |

## Request and response headers

| Header                  | Direction | Notes                                                                                  |
| ----------------------- | --------- | -------------------------------------------------------------------------------------- |
| `Authorization`         | request   | `Bearer <jwt>` or `DashKey <ak_...>`. Required for all endpoints except health/metrics.|
| `Content-Type`          | request   | Must be `application/json` for all POST endpoints.                                     |
| `Idempotency-Key`       | request   | Optional. Equivalent to the body's `idempotency_key`.                                  |
| `dash_request_id`       | response  | The ULID of the request. Echo it back when filing a bug.                               |
| `Retry-After`           | response  | Set on 429 responses. Seconds to wait before retrying.                                  |
| `X-RateLimit-Limit`     | response  | The configured per-tenant RPS.                                                         |
| `X-RateLimit-Remaining` | response  | Tokens remaining in the bucket.                                                        |

## Versioning

The API is versioned by the URL prefix (`/v1/...`). Breaking changes ship as `/v2/...` and the old version is supported for at least 6 months after the new version's GA. Non-breaking additions (new optional fields, new endpoints) ship as additions to the current version.

The current version is **v1**, which is the version this document describes. A deprecation notice for any v1 endpoint will be posted on the [GitHub releases page](https://github.com/BHAWESHBHASKAR/DASH/releases) and in the [Changelog](../about/changelog.md).
