# Quickstart

From `git clone` to a first retrieval query against DASH, with authentication on. DASH is pre-1.0; see the Status section of the [README](../README.md) before using it for real data.

## Prerequisites

- **Docker** with Compose v2 (recommended), **or** Rust 1.85+ (edition 2024) and `cargo` for the build-from-source path.
- `git`, `curl`, and `openssl` (or any way to generate random hex).

## Option 1: Docker Compose

No release images are published yet, so Compose builds them from source (several minutes the first time).

```bash
git clone https://github.com/BHAWESHBHASKAR/DASH.git
cd DASH

# Generate strong credentials into deploy/container/.env (git-ignored), then load them
# into this shell so the curl examples below can use them.
./scripts/generate-secrets.sh
set -a; source deploy/container/.env; set +a

docker compose -f deploy/container/docker-compose.yml up -d --build
```

The compose file refuses to start without the generated secrets and turns on strict secret validation. It starts ingestion (`:8081`), retrieval (`:8080`), a control plane (`:8090`) and the segment maintenance daemon. Check health (health routes need no key):

```bash
curl -fsS http://localhost:8081/health
curl -fsS http://localhost:8080/health
```

Stop the stack with `docker compose -f deploy/container/docker-compose.yml down` (add `-v` to delete the data volume).

## Option 2: Build from source

```bash
git clone https://github.com/BHAWESHBHASKAR/DASH.git
cd DASH
cargo build --release -p ingestion -p retrieval
```

Generate keys (strict validation, on by default, needs at least 16 characters for keys and tokens and 32 for JWT secrets, and rejects placeholders):

```bash
export DASH_INGEST_API_KEY=$(openssl rand -hex 32)
export DASH_RETRIEVAL_API_KEY=$(openssl rand -hex 32)
export DASH_INGEST_REPLICATION_TOKEN=$(openssl rand -hex 32)
export DASH_RETRIEVAL_REPLICATION_TOKEN=$DASH_INGEST_REPLICATION_TOKEN
mkdir -p data
```

In one terminal, start ingestion with a WAL so data survives restarts (without `DASH_INGEST_WAL_PATH` it is purely in-memory):

```bash
DASH_INGEST_WAL_PATH=./data/ingest.wal \
  cargo run --release -p ingestion -- --serve
```

In a second terminal, start retrieval as a follower of ingestion:

```bash
DASH_RETRIEVAL_REPLICATION_SOURCE_URL=http://127.0.0.1:8081 \
DASH_RETRIEVAL_REPLICATION_OFFSET_PATH=./data/retrieval.offset \
DASH_RETRIEVAL_PERSISTENCE_DISABLE=1 \
  cargo run --release -p retrieval -- --serve
```

Defaults: ingestion binds `127.0.0.1:8081`, retrieval `127.0.0.1:8080`. Both default to a redb file under `./data` when a WAL is configured; the retrieval command above disables it for simplicity. The full variable list is in [Configuration](../docs-site/docs/reference/configuration.md).

Without a replication source, the retrieval service does not see what ingestion receives. A service refuses to start with no credentials unless `DASH_INSECURE_DEV_MODE=1` is set (throwaway local use only; it also forces a loopback bind); the examples here always set keys. If you build from source, `--serve` is the default and can be omitted.

## Ingest your first claim

A DASH write is `{ claim, evidence[], edges[] }`. The claim is the atomic assertion; each evidence item records a source that supports, contradicts, or is neutral toward it. Send the **ingestion** key:

```bash
curl -fsS -X POST http://localhost:8081/v1/ingest \
  -H "Content-Type: application/json" \
  -H "x-api-key: $DASH_INGEST_API_KEY" \
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

The response is `200 OK` with `{"ingested_claim_id":"c1","claims_total":1,...}`. Validation is server-side: `confidence` and `source_quality` in `[0, 1]`, `valid_from <= valid_to` if both are given, non-empty IDs. Retries are safe since 0.3.0: evidence is upserted by `evidence_id` and the whole bundle is written atomically, so re-sending leaves one copy.

For bulk loads use `POST /v1/ingest/batch` (an `items` array and an optional `commit_id`), or `POST /v1/ingest/document` to extract sentence claims from text. See the [API reference](../docs-site/docs/reference/api.md).

## Retrieve with citations

The retrieval service follows ingestion by polling, so wait a moment after writing (up to one poll interval). Send the **retrieval** key:

```bash
sleep 2
curl -fsS -X POST http://localhost:8080/v1/retrieve \
  -H "Content-Type: application/json" \
  -H "x-api-key: $DASH_RETRIEVAL_API_KEY" \
  -d '{
    "tenant_id": "t1",
    "query": "Company X acquired Company Y",
    "top_k": 5,
    "stance_mode": "support_only"
  }'
```

The response is an object whose `results` array holds flat result records (abridged):

```json
{
  "results": [{
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
  }],
  "graph": null,
  "read_policy": "one",
  "read_quorum_met": true,
  "serving_replica": null
}
```

(The score value is illustrative.) Constrain the window with `time_range: {"from_unix": ..., "to_unix": ...}`, filter by entity, or pass a precomputed `query_embedding`. `stance_mode: "balanced"` (the default) keeps contradicted claims with a lower score; `support_only` removes claims that have **more** contradicting than supporting evidence.

## Use the OpenAI-compatible API

The retrieval service exposes `POST /v1/embeddings` with the OpenAI v1 embeddings request and response shape. It requires a credential with the `retrieve` role (it was open before 0.3.0). The default provider is a deterministic hash embedder, which is for development and not semantic; set `DASH_EMBEDDING_PROVIDER=ollama` for real vectors.

```bash
curl -fsS -X POST http://localhost:8080/v1/embeddings \
  -H "Content-Type: application/json" \
  -H "x-api-key: $DASH_RETRIEVAL_API_KEY" \
  -d '{"input": "Company X acquired Company Y", "model": "text-embedding-3-small"}'
```

From the OpenAI Python SDK, point `base_url` at the retrieval service and use the retrieval key as the API key:

```python
import os
import openai

client = openai.OpenAI(
    base_url="http://localhost:8080/v1",
    api_key=os.environ["DASH_RETRIEVAL_API_KEY"],
)
response = client.embeddings.create(input="hello world", model="text-embedding-3-small")
print(response.data[0].embedding[:5])
```

## Try the contradiction behavior

Ingest claim `c1` with one supporting source (the first example above), then send two contradicting evidence records for the same claim:

```bash
for n in 2 3; do
curl -fsS -X POST http://localhost:8081/v1/ingest \
  -H "Content-Type: application/json" \
  -H "x-api-key: $DASH_INGEST_API_KEY" \
  -d '{
    "claim": {
      "claim_id": "c1",
      "tenant_id": "t1",
      "canonical_text": "Company X acquired Company Y",
      "confidence": 0.9
    },
    "evidence": [{
      "evidence_id": "e'"$n"'",
      "claim_id": "c1",
      "source_id": "news://reuters/'"$n"'",
      "stance": "contradicts",
      "source_quality": 0.9
    }],
    "edges": []
  }'
done
```

Retrieve with the default `balanced` mode: the claim is returned with `"supports": 1, "contradicts": 2` and a lower score. Retrieve with `"stance_mode": "support_only"`: the result list is empty, because contradictions (2) outnumber supports (1). With a single contradicting record (1 versus 1) the claim would still be returned in `support_only` mode.

## Next steps

- [Comparison with other vector databases](comparison.md)
- [Architecture](architecture/eme-architecture.md)
- [Configuration reference](../docs-site/docs/reference/configuration.md) and [HTTP API](../docs-site/docs/reference/api.md)
- [Contributing](../CONTRIBUTING.md)
