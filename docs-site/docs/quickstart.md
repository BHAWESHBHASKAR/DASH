# Quickstart

From `git clone` to a first retrieval query, with authentication on. DASH is pre-1.0; no release images are published yet, so the Docker path builds from source. The full walkthrough, including the contradiction example and the build-from-source path, is in [`docs/quickstart.md`](https://github.com/BHAWESHBHASKAR/DASH/blob/main/docs/quickstart.md).

## 1. Install and run

```bash
git clone https://github.com/BHAWESHBHASKAR/DASH.git
cd DASH
./scripts/generate-secrets.sh                     # writes deploy/container/.env (git-ignored)
set -a; source deploy/container/.env; set +a      # makes the keys available to curl below
docker compose -f deploy/container/docker-compose.yml up -d --build
```

| Service | Port | Purpose |
| --- | --- | --- |
| ingestion | 8081 | `/v1/ingest`, `/v1/ingest/batch`, `/v1/ingest/raw`, `/v1/ingest/document`, health |
| retrieval | 8080 | `/v1/retrieve`, `/v1/embeddings`, health |
| control-plane | 8090 | placement and leader state (internal) |

```bash
curl -fsS http://localhost:8081/health     # {"status":"ok"}
curl -fsS http://localhost:8080/health
```

Building from source instead: use `cargo build --release -p ingestion -p retrieval` and the variables in [Configuration](reference/configuration.md). Ingestion binds `127.0.0.1:8081` and retrieval `127.0.0.1:8080` by default; retrieval needs `DASH_RETRIEVAL_REPLICATION_SOURCE_URL` to see ingestion's data. The Rust toolchain must support edition 2024 (1.85 or newer).

## 2. Ingest

Use the ingestion key in the `x-api-key` header (or `Authorization: Bearer <key>`):

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
      "source_id": "news://nyt",
      "stance": "supports",
      "source_quality": 0.95
    }],
    "edges": []
  }'
```

The response is HTTP 200 with `ingested_claim_id` and commit fields. On v0.2.x, do not blindly retry ingest calls: re-sent evidence is duplicated (fixed in v0.3.0).

## 3. Retrieve

Retrieval follows ingestion by polling, so allow a moment for the write to appear. Use the retrieval key:

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

The response is an object with a `results` array. Each result is flat: `claim_id`, `canonical_text`, `score`, `supports`, `contradicts`, `citations[]` and the claim's temporal fields. See the [HTTP API reference](reference/api.md#retrieval-service) for the full shape. `support_only` drops claims that have more contradicting than supporting evidence.

## 4. Embeddings (OpenAI-compatible)

```python
import os
import openai

client = openai.OpenAI(base_url="http://localhost:8080/v1", api_key=os.environ["DASH_RETRIEVAL_API_KEY"])
resp = client.embeddings.create(input="hello world", model="text-embedding-3-small")
print(resp.data[0].embedding[:5])
```

`/v1/embeddings` requires authentication as of v0.3.0 (open in v0.2.x). The default provider is a deterministic hash embedder for development. See the [Embeddings guide](guides/embeddings.md).

## 5. Persistence and real embeddings

With a WAL path configured, ingestion keeps data across restarts (the compose file already sets one under the `dash-ingestion-state` volume) and mirrors it into a redb file by default. For semantic embeddings, set `DASH_EMBEDDING_PROVIDER=ollama`, `DASH_OLLAMA_ENDPOINT` and `DASH_OLLAMA_MODEL` on the retrieval service (add them to its `environment` block in the compose file). The OpenAI provider needs v0.3.0 for TLS support.

To deploy beyond a single host, read [Deploy](operations/deploy.md) and [Scaling](operations/scaling.md); the Kubernetes and Helm manifests are being corrected in v0.3.0.

## Next steps

- [Why DASH](concepts/why-dash.md)
- [HTTP API reference](reference/api.md)
- SDKs: [overview](guides/sdks.md). Python, Go and TypeScript cover embeddings and retrieve only; Java, Kotlin and C# also cover ingest.
