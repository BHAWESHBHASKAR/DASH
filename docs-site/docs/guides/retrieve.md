# Retrieve

The retrieval endpoint is the read path. It takes a `tenant_id`, a `query`, an optional pre-computed `query_embedding`, a `top_k`, an optional `time_range` and a `stance_mode`, and returns the top-*k* claims with their citations. The exact request fields and response shape are in the [HTTP API reference](../reference/api.md#retrieval-service); this page explains behavior.

## `POST /v1/retrieve`

```text
POST /v1/retrieve
Content-Type: application/json
x-api-key: <retrieval api key>      (or Authorization: Bearer <key or JWT>)
```

```json
{
  "tenant_id": "t1",
  "query": "Company X acquired Company Y",
  "top_k": 5,
  "stance_mode": "support_only",
  "time_range": { "from_unix": 1718300000, "to_unix": 1735609600 }
}
```

The response is `{ "results": [ ... ], "graph": null, "read_policy": "one", "read_quorum_met": true, "serving_replica": null }`. Each result is flat: `claim_id`, `canonical_text`, `score`, `claim_confidence`, `confidence_band`, `dominant_stance`, `contradiction_risk`, `supports`, `contradicts`, `citations[]`, and the claim's temporal fields. There is no nested `claim` object and no `took_us` field; latency is exposed on `/metrics`. `GET /v1/retrieve` takes the same fields as query parameters.

Use the retrieval key, not the ingestion key. The retrieval service sees new writes only after it has replicated them from ingestion, which takes up to one poll interval.

## Request fields

| Field | Default | Notes |
|---|---|---|
| `tenant_id` | required | Non-empty. The credential must allow this tenant. |
| `query` | required | Non-empty string, at most 8 KiB. |
| `query_embedding` | computed | Array of floats. If absent, the query is embedded with the configured provider (`DASH_EMBEDDING_PROVIDER`). The dimension must match the tenant's stored vectors. |
| `top_k` | `5` | Positive integer, at most `DASH_RETRIEVAL_MAX_TOP_K` (default 1000); larger values get 400. |
| `stance_mode` | `balanced` | `balanced` or `support_only`. There is no `all` or `contradict_only` mode. |
| `time_range` | none | `{ "from_unix", "to_unix" }`, either bound optional. |
| `entity_filters`, `embedding_id_filters` | `[]` | String arrays (at most 256 values each) that restrict candidates. |
| `return_graph` | `false` | Include expanded graph nodes and edges. |
| `read_consistency` | `one` | `one`, `quorum`, `all`; relevant only with placement routing. |

There are no `min_confidence`, `hybrid_alpha` or `ann_top_n` parameters. A `query_embedding` has at most 8192 values and must be finite; a vector of the wrong dimension returns no results rather than an error.

## Ranking

Candidates come from lexical (BM25-style), entity, temporal and ANN lookups, restricted to the tenant. When a query vector is present (it is by default, because the query is embedded), dense similarity is the primary signal and the lexical score is a tie-breaker (`InMemoryStore::retrieve_semantic`). The final score also reflects claim confidence, evidence `source_quality`, and a penalty for contradicting evidence (`pkg/ranking`). Since 0.3.0 the support bonus and the contradiction penalty saturate (capped at 0.4 and 0.5; the slope for small counts is unchanged), evidence from one `source_id` counts once toward that signal, and an edge `from supports to` counts for the target claim, not the author. With the default hash embedder, vector similarity is not semantic, so ranking is effectively lexical; configure a real provider for semantic search. There is no tunable blend between dense and sparse scores.

Tests: `retrieve_semantic_ranks_aligned_claim_first`, `retrieve_semantic_uses_dense_similarity_as_primary_signal`, `retrieve_semantic_with_tenant_isolation_filters_other_tenants` in `pkg/store/tests/integration_retrieval.rs`; `scoring_penalizes_contradictions` in `pkg/ranking`.

## Stance filter

| Mode | Behavior |
|---|---|
| `balanced` (default) | Contradicted claims are kept, with a lower score. |
| `support_only` | Claims with more contradictions than supports are dropped. |

The `supports` and `contradicts` counts come from the claim's evidence (`stance`) and from incoming `supports` and `contradicts` edges (one per distinct source claim in the same tenant). One contradicting evidence record gives `contradicts: 1`; whether that drops the claim in `support_only` depends on the supports count. Tests: `support_only_drops_claim_with_more_contradictions_than_supports`, `balanced_mode_keeps_contradicted_claims_with_neutral_score` in `pkg/store/tests/integration_retrieval.rs`.

## Time-range filter

`time_range` filters on the claim's `event_time_unix` and its validity window `[valid_from, valid_to]` (field names without a `_unix` suffix). When both an event time and a window exist, both must match; when only a window exists, it must overlap the range; a claim with neither an event time nor a validity window never matches a request that sets a time range. The matching mode is reported per result in `temporal_match_mode`. An open bound on either side of the request range is unbounded. A claim with `valid_from > valid_to` is rejected at ingest. Tests: `temporal_event_time_filter_excludes_older_claims`, `temporal_validity_window_inclusive`, `retrieve_with_time_range_requires_event_and_validity_match_when_both_present`.

## Failure modes

| Condition | HTTP | Body |
|---|---:|---|
| Missing or invalid credential | 401 | `{"error":"..."}` |
| Tenant or role not allowed | 403 | `{"error":"..."}` |
| Bad JSON, missing `tenant_id` or `query`, bad `top_k` (or above the maximum), bad `stance_mode`, wrong `Content-Type`, invalid `query_embedding` | 400 | `{"error":"..."}` |
| Request body over 16 MiB, oversized headers, slow request | 413, 431, 408 | `{"error":"..."}` |
| Embedding provider unavailable (`embedding_unavailable`) or read route rejected by placement | 503 | `{"error":"..."}` |
| Embedding provider error (`embedding_provider_error`) | 502 | `{"error":"embedding_provider_error"}` |
| Rate limit exceeded | 429 | `{"error":"rate limit exceeded"}` plus `Retry-After` |

There is no "tenant not found" error: a tenant with no data returns an empty `results` array. See the [HTTP API errors table](../reference/api.md#errors).
