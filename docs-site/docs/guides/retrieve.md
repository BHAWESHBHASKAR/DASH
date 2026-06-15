# Retrieve

The retrieval endpoint is the read path. It accepts a `tenant_id`, a `query`, an optional pre-computed `query_vector`, a `top_k`, an optional `time_range`, and a `stance_mode`, and returns the top-*k* claims with their citations.

## `POST /v1/retrieve`

```text
POST /v1/retrieve
Content-Type: application/json
Authorization: Bearer <jwt>

{
  "tenant_id": "t1",
  "query": "Company X acquired Company Y",
  "top_k": 5,
  "stance_mode": "support_only",
  "time_range": { "from_unix": 1718300000, "to_unix": 1735609600 }
}
```

The response:

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

The `took_us` field is the wall-clock time the server spent on the request, in microseconds. It is intended for client-side latency dashboards; the canonical server-side latency is exposed via `dash_retrieve_latency_seconds` on `/metrics`.

## `RetrievalRequest`

| Field            | Type             | Required | Default        | Notes                                                       |
| ---------------- | ---------------- | -------- | -------------- | ----------------------------------------------------------- |
| `tenant_id`      | string           | yes      | —              | Must match the JWT's tenant scope.                         |
| `query`          | string           | yes      | —              | The natural-language query (≤ 4 KiB).                       |
| `query_vector`   | array of float   | no       | (computed)     | A pre-computed 768-dim vector; bypasses the embed call.     |
| `top_k`          | integer          | no       | `10`           | Number of results to return (≤ 100).                        |
| `stance_mode`    | enum             | no       | `all`          | `all` / `support_only` / `contradict_only`.                 |
| `time_range`     | object           | no       | (no filter)    | `{ from_unix: int, to_unix: int }`.                         |
| `min_confidence` | float            | no       | `0.0`          | Drop claims with `confidence < min_confidence`.             |
| `hybrid_alpha`   | float            | no       | `0.7`          | The semantic-vs-lexical weight; see [Ranking](#ranking-dense-vs-sparse-vs-hybrid). |
| `ann_top_n`      | integer          | no       | `top_k * 10`   | The number of ANN candidates before reranking.              |

## Ranking: dense vs sparse vs hybrid

DASH has three ranking modes, controlled by `hybrid_alpha`:

- **Dense (semantic-first)** — `hybrid_alpha = 1.0`. The cosine similarity between `query_vector` (or the embedding of `query`) and each candidate's vector is the primary score, in `[0, 1]`. The lexical/BM25 score is computed but used only as a tie-breaker. This is the default in the OpenAI-drop-in path.
- **Sparse (lexical-first)** — `hybrid_alpha = 0.0`. The BM25 score is the primary score. The vector is still used to recall candidates (the ANN search), but the reranker weights the lexical match. This mode is useful for queries that are exact terms of art (legal citations, drug names).
- **Hybrid** — `hybrid_alpha ∈ (0, 1)`. The final score is `alpha * dense + (1 - alpha) * sparse`. The default `0.7` favors dense but lets a strong lexical match break through.

The semantic-first path is `InMemoryStore::retrieve_semantic` in `pkg/store`. It is exercised by 3 integration tests in `tests/store/integration_retrieval.rs`.

## Stance filter

The `stance_mode` field controls which claims survive the filter:

| Mode               | Behavior                                              |
| ------------------ | ----------------------------------------------------- |
| `all` (default)    | Every claim is returned, in score order.              |
| `support_only`     | Claims with `contradicts > 0` are dropped.            |
| `contradict_only`  | Only claims with `contradicts > 0` are returned.      |

The "supports" / "contradicts" counts on the result are computed from the `Evidence` records and the `ClaimEdge`s with `relation: contradicts`. A single `Evidence` with `stance: contradicts` is enough to set `contradicts: 1` on the claim.

## Time-range filter

The `time_range` field constrains the result to claims whose `[valid_from_unix, valid_to_unix]` window **intersects** the requested range. A claim with `valid_to_unix: null` (valid indefinitely) is included in any range whose `from_unix` is greater than `valid_from_unix`.

A claim with `valid_from_unix > valid_to_unix` is rejected at ingest time and will never appear in a retrieval result.

## Failure modes

| Condition                                       | HTTP | `error.code`                 |
| ----------------------------------------------- | ---: | ---------------------------- |
| Missing or invalid JWT                          |  401 | `dash_unauthenticated`       |
| JWT scope does not include `claims:read`        |  403 | `dash_forbidden_scope`       |
| `tenant_id` mismatch with JWT                   |  403 | `dash_tenant_mismatch`       |
| Tenant not found                                |  404 | `dash_tenant_not_found`      |
| `query_vector` length does not match tenant dim |  400 | `dash_dimension_mismatch`    |
| `top_k > 100`                                   |  400 | `dash_validation_error`      |
| Rate limit exceeded                             |  429 | `dash_rate_limited`          |
| Internal error                                  |  500 | `dash_internal_error`        |

For the full error envelope, see [HTTP API → Error codes](../reference/api.md#error-codes).
