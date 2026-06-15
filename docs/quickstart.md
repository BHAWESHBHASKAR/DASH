# Quickstart

Five minutes from `git clone` to a working retrieval query against DASH.

## Prerequisites

- **Docker** (recommended) — Docker Engine 20+ and Docker Compose v2
- **OR** Rust 1.83+ and `cargo`, for the build-from-source path
- `curl` for the example commands

## Option 1: Docker (recommended)

Clone the repo and start the two-service stack (ingestion on `:8081`, retrieval on `:8080`):

```bash
git clone https://github.com/anomalyco/dash.git
cd dash
docker compose -f deploy/container/docker-compose.yml up -d
```

The compose file mounts a `dash_data` volume for the WAL and segments, and sets safe defaults for the WAL durability guardrails. Both services come up healthy; confirm with:

```bash
curl http://localhost:8080/health
curl http://localhost:8081/health
```

To stop and remove the stack:

```bash
docker compose -f deploy/container/docker-compose.yml down
```

## Option 2: Build from source

```bash
git clone https://github.com/anomalyco/dash.git
cd dash

# Build the two services you'll run for ingestion and retrieval.
cargo build --release -p ingestion -p retrieval

# Optional: build the full workspace (indexer, benchmarks, all libraries).
cargo build --release --workspace
```

In one terminal, start the ingestion service. Without `DASH_INGEST_WAL_PATH` it runs in-memory and seeds a sample claim on startup:

```bash
cargo run --release -p ingestion -- --serve
```

In a second terminal, start the retrieval service:

```bash
cargo run --release -p retrieval -- --serve
```

Defaults: ingestion binds `127.0.0.1:8081`, retrieval binds `127.0.0.1:8080`. Health endpoints are at `/health`, Prometheus metrics at `/metrics`.

To run with a durable WAL, set `DASH_INGEST_WAL_PATH` and `DASH_RETRIEVAL_WAL_PATH` to the same file before starting the services, and `DASH_INGEST_API_KEY` / `DASH_RETRIEVAL_API_KEY` to a shared key.

## Ingest your first claim

A DASH claim is `{ claim, evidence[], edges[] }`. The claim is the atomic assertion; each piece of evidence records the source that supports, contradicts, or is neutral toward the claim. Edges connect this claim to other claims (`supports`, `contradicts`, `refines`, `duplicates`, `depends_on`).

```bash
curl -X POST http://localhost:8081/v1/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "claim": {
      "claim_id": "c1",
      "tenant_id": "t1",
      "canonical_text": "Company X acquired Company Y",
      "confidence": 0.95
    },
    "evidence": [{
      "evidence_id": "e1",
      "claim_id": "c1",
      "source_id": "news://nyt/2025-09-03",
      "stance": "supports",
      "source_quality": 0.95
    }],
    "edges": []
  }'
```

The response is `200 OK` with `{ "ingested_claim_id": "c1", ... }`. Every field on the claim and every evidence item is validated server-side: `confidence` and `source_quality` must be in `[0.0, 1.0]`, `valid_from <= valid_to`, IDs must be non-empty.

For high-volume ingest, use `POST /v1/ingest/batch` with an `items` array, or `POST /v1/ingest/document` to extract claims from a raw document blob.

## Retrieve with citations

Retrieve returns ranked claims with their supporting/contradicting evidence as inline citations. The `stance_mode: support_only` filter drops any claim that has been contradicted by another evidence record.

```bash
curl -X POST http://localhost:8080/v1/retrieve \
  -H "Content-Type: application/json" \
  -d '{
    "tenant_id": "t1",
    "query": "Company X acquired Company Y",
    "top_k": 5,
    "stance_mode": "support_only"
  }'
```

The response is a JSON array of `RetrievalResult`:

```json
[{
  "claim_id": "c1",
  "canonical_text": "Company X acquired Company Y",
  "score": 0.93,
  "supports": 1,
  "contradicts": 0,
  "citations": [{
    "evidence_id": "e1",
    "source_id": "news://nyt/2025-09-03",
    "stance": "supports",
    "source_quality": 0.95
  }]
}]
```

You can also constrain the result set to a temporal window with `time_range: { "from_unix": 1700000000, "to_unix": 1800000000 }`, filter by entity, or pass a precomputed `query_embedding` instead of a string. `stance_mode: "balanced"` (the default) keeps contradicted claims but demotes them in the score; `support_only` removes them.

## Use the OpenAI-compatible API

DASH exposes `POST /v1/embeddings` with a request and response shape that is byte-compatible with the OpenAI v1 embeddings API. The default embedding backend is the deterministic `HashEmbeddingProvider` (no network, no API key required) so the endpoint works out of the box; swap in Ollama, OpenAI, or a custom model by implementing the `EmbeddingProvider` trait.

From `curl`:

```bash
curl -X POST http://localhost:8080/v1/embeddings \
  -H "Content-Type: application/json" \
  -d '{"input": "Company X acquired Company Y", "model": "text-embedding-3-small"}'
```

From the OpenAI Python SDK — point `base_url` at the DASH retrieval service and DASH is a drop-in replacement:

```python
import openai

client = openai.OpenAI(
    base_url="http://localhost:8080/v1",
    api_key="not-needed",
)
response = client.embeddings.create(
    input="hello world",
    model="text-embedding-3-small",
)
print(response.data[0].embedding[:5])
```

The same pattern works for `langchain`, `llama-index`, `semantic-kernel`, the `openai` CLI, and any other client that speaks the OpenAI embeddings protocol — set `OPENAI_API_BASE=http://localhost:8080/v1` and the request/response shape is handled for you.

## Try the Claim + Evidence + Contradiction differentiator

This is the part that doesn't exist in any other vector database. Ingest the same claim twice with conflicting evidence and watch DASH demote and then filter it.

Ingest claim `c1` with one supporting source:

```bash
curl -X POST http://localhost:8081/v1/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "claim": {
      "claim_id": "c1",
      "tenant_id": "t1",
      "canonical_text": "Company X acquired Company Y",
      "confidence": 0.9
    },
    "evidence": [{
      "evidence_id": "e1",
      "claim_id": "c1",
      "source_id": "news://nyt",
      "stance": "supports",
      "source_quality": 0.95
    }],
    "edges": []
  }'
```

Add a contradicting piece of evidence (a different `evidence_id`, same `claim_id`):

```bash
curl -X POST http://localhost:8081/v1/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "claim": {
      "claim_id": "c1",
      "tenant_id": "t1",
      "canonical_text": "Company X acquired Company Y",
      "confidence": 0.9
    },
    "evidence": [{
      "evidence_id": "e2",
      "claim_id": "c1",
      "source_id": "news://reuters",
      "stance": "contradicts",
      "source_quality": 0.9
    }],
    "edges": []
  }'
```

Now retrieve with `balanced` mode (default — claim appears, score is demoted):

```bash
curl -X POST http://localhost:8080/v1/retrieve \
  -H "Content-Type: application/json" \
  -d '{
    "tenant_id": "t1",
    "query": "Company X acquired Company Y",
    "top_k": 5
  }'
# -> result for c1 includes "supports": 1, "contradicts": 1, score lower than before
```

Now retrieve with `support_only` — the contradicted claim is dropped:

```bash
curl -X POST http://localhost:8080/v1/retrieve \
  -H "Content-Type: application/json" \
  -d '{
    "tenant_id": "t1",
    "query": "Company X acquired Company Y",
    "top_k": 5,
    "stance_mode": "support_only"
  }'
# -> result is empty: c1 was contradicted, so it is not returned
```

This is the foundation for RAG that has to be defensible: your retrieval layer doesn't just find "similar text," it knows when the evidence graph says the claim is no longer true and ranks accordingly.

## Next steps

- [Comparison with other vector databases](comparison.md) — full feature table and "when to use / when not to use" guidance
- [Architecture](architecture/eme-architecture.md) — data model, WAL protocol, retrieval planner, operational model
- [Contributing](../CONTRIBUTING.md) — workflow, code standards, "good first issue" list
