# Quickstart

The five-minute path from `git clone` to a first retrieval query.

## 1. Install

=== "Docker"

    The fastest path. Multi-arch images (`linux/amd64`, `linux/arm64`),
    non-root runtime, healthcheck, dependency ordering, and a dev overlay
    with hot-reload.

    ```bash
    git clone https://github.com/BHAWESHBHASKAR/DASH.git
    cd DASH
    docker compose -f deploy/container/docker-compose.yml up -d
    ```

    Both services come up on a shared `dash-net` bridge network:

    | Service       | Port  | Purpose                          |
    | ------------- | ----- | -------------------------------- |
    | `dash-ingest` | 8081  | `/v1/ingest`, `/v1/health`       |
    | `dash-retrieval` | 8080 | `/v1/retrieve`, `/v1/embeddings` |

=== "From source"

    Requires **Rust 1.83+** and `cargo`.

    ```bash
    git clone https://github.com/BHAWESHBHASKAR/DASH.git
    cd DASH
    cargo build --workspace --release

    # Two terminals
    DASH_INGEST_PORT=8081 \
      cargo run --release -p services-ingestion
    DASH_RETRIEVAL_PORT=8080 \
      DASH_EMBEDDING_PROVIDER=hash \
      cargo run --release -p services-retrieval
    ```

## 2. Run

Confirm both services are healthy:

```bash
curl -s http://localhost:8081/v1/health
# {"status":"ok","service":"ingestion"}

curl -s http://localhost:8080/v1/health
# {"status":"ok","service":"retrieval"}
```

## 3. Query

Ingest a claim with its supporting evidence:

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
      "source_id": "news://nyt",
      "stance": "supports",
      "source_quality": 0.95
    }],
    "edges": []
  }'
```

Retrieve with citations, dropping any claim that has been contradicted:

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

The response is the canonical DASH retrieval shape:

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
          "source_id": "news://nyt",
          "stance": "supports",
          "source_quality": 0.95,
          "chunk_id": null,
          "span_start": null,
          "span_end": null
        }
      ]
    }
  ]
}
```

## 4. Embeddings (OpenAI drop-in)

Point any OpenAI client at DASH:

```bash
export OPENAI_API_BASE=http://localhost:8080/v1
```

```python
import openai
client = openai.OpenAI(base_url="http://localhost:8080/v1", api_key="not_needed")
resp = client.embeddings.create(
    input="hello world",
    model="text-embedding-3-small",
)
print(resp.data[0].embedding[:5])
```

## 5. Scale

Turn on durable storage and a real embedding model:

```bash
# Persistence: claims survive a process restart
DASH_INGEST_PERSISTENCE_PATH=/var/lib/dash/ingest.redb \
DASH_RETRIEVAL_PERSISTENCE_PATH=/var/lib/dash/retrieval.redb \
  docker compose -f deploy/container/docker-compose.yml up -d

# Real embeddings
DASH_EMBEDDING_PROVIDER=ollama \
DASH_OLLAMA_BASE_URL=http://ollama:11434 \
DASH_EMBEDDING_MODEL=nomic-embed-text \
  docker compose -f deploy/container/docker-compose.yml up -d
```

To run more replicas of the retrieval service, see [Scaling](operations/scaling.md). To ship to production on Kubernetes, see [Deploy](operations/deploy.md).

## Next steps

- Read [Why DASH](concepts/why-dash.md) for the design rationale.
- Browse the [HTTP API reference](reference/api.md) for the full request/response shapes.
- Pick an SDK: [Python](guides/sdks.md#python-dash-py), [Go](guides/sdks.md#go-dash-go), [TypeScript](guides/sdks.md#typescript-dash-ts).
