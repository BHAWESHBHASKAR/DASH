# DASH API

This directory contains the DASH REST API specification. DASH exposes three
HTTP services:

| Service | Default port | Purpose |
|---|---|---|
| `ingestion` | `8081` | Write claims, evidence and claim edges |
| `retrieval` | `8080` | Search + OpenAI-compatible embeddings |
| `control-plane` | `8082` | Shard placement and leader election |

The OpenAPI 3.0 spec is at [`openapi.yaml`](openapi.yaml).

## Authentication

Two methods are supported:

- **API key**: send `x-api-key` in the request header.
- **Bearer JWT**: send `Authorization: Bearer <jwt>`. The token can be verified
  with HS256 (`DASH_*_JWT_HS256_SECRET`) or OIDC (`DASH_*_JWT_PROVIDER=oidc`).

Tenants are isolated by `tenant_id` on each request. API keys can be scoped to
a tenant and role (`admin`, `ingest`, `retrieve`, `read_only`) using
`DASH_INGEST_API_KEY_SCOPES` / `DASH_RETRIEVAL_API_KEY_SCOPES`.

## Quick start

```bash
# Start the stack
docker compose -f deploy/container/docker-compose.yml up

# Generate secrets if you have not already
./scripts/generate-secrets.sh
```

### Ingest a claim

```bash
curl -X POST http://localhost:8081/v1/ingest \
  -H 'x-api-key: $INGEST_API_KEY' \
  -H 'content-type: application/json' \
  -d '{
    "claim": {
      "claim_id": "claim-1",
      "tenant_id": "tenant-a",
      "canonical_text": "DASH supports customer-managed encryption keys.",
      "confidence": 0.95,
      "claim_type": "factual"
    },
    "evidence": [{
      "evidence_id": "ev-1",
      "claim_id": "claim-1",
      "source_id": "docs/api/README.md",
      "stance": "supports",
      "source_quality": 0.9
    }]
  }'
```

Use `?write_consistency=quorum` (or `all`) and set `DASH_REPLICA_ACK_ENDPOINTS`
to make the leader wait for synchronous follower acks before returning.

```bash
curl -X POST 'http://localhost:8081/v1/ingest?write_consistency=quorum' \
  -H 'x-api-key: $INGEST_API_KEY' \
  -H 'content-type: application/json' \
  -d '{"claim":{"claim_id":"c1","tenant_id":"tenant-a","canonical_text":"quorum write","confidence":0.9}}'
```

### List tenants and usage

```bash
curl http://localhost:8081/v1/tenants \
  -H 'x-api-key: $INGEST_API_KEY'

curl http://localhost:8081/v1/usage \
  -H 'x-api-key: $INGEST_API_KEY'
```

### Retrieve with citations

```bash
curl -G http://localhost:8080/v1/retrieve \
  -H 'x-api-key: $RETRIEVAL_API_KEY' \
  --data-urlencode 'tenant_id=tenant-a' \
  --data-urlencode 'query=encryption keys' \
  --data-urlencode 'top_k=3' \
  --data-urlencode 'stance_mode=balanced'
```

The response contains:

- `results`: ranked claims with `score`, `supports`, `contradicts` and `citations`.
- `graph`: optional evidence graph when `return_graph=true`.
- `read_policy`, `read_quorum_met`, `serving_replica`: consistency metadata.

### Embeddings (OpenAI-compatible)

```bash
export OPENAI_API_BASE=http://localhost:8080/v1
export OPENAI_API_KEY=$RETRIEVAL_API_KEY

curl $OPENAI_API_BASE/embeddings \
  -H "Authorization: Bearer $OPENAI_API_KEY" \
  -H 'content-type: application/json' \
  -d '{
    "input": ["DASH is an evidence-first vector database."],
    "model": "dash-embeddings-v1"
  }'
```

DASH accepts the OpenAI wire format. The model name is accepted and echoed but
the active embedding provider is selected by `DASH_EMBEDDING_PROVIDER`
(`hash`, `ollama`, `openai`). List supported model names with:

```bash
curl $OPENAI_API_BASE/models -H "Authorization: Bearer $OPENAI_API_KEY"
```

### Operational dashboard

Open `http://localhost:8080/dashboard` in a browser. It shows service health,
per-tenant usage, available models, and a retrieve form.

### gRPC

A gRPC server is available in the `grpc` service. By default it listens on
port `50051` and exposes `dash.v1.Dash` with `Embed` and `ListModels` methods:

```bash
grpcurl -plaintext -d '{"input":["hello world"],"model":"dash-hash"}' \
  localhost:50051 dash.v1.Dash/Embed

grpcurl -plaintext localhost:50051 dash.v1.Dash/ListModels
```

### Running with real semantic embeddings (Ollama)

```bash
# Start the stack with the Ollama overlay
docker compose \
  -f deploy/container/docker-compose.yml \
  -f deploy/container/docker-compose.ollama.yml \
  --profile ollama up
```

This starts an `ollama` container and sets `DASH_EMBEDDING_PROVIDER=ollama`
with `nomic-embed-text`. The model is pulled on the first embedding request.

## Customer-managed encryption keys

Set `DASH_ENCRYPTION_PROVIDER=env` and `DASH_ENCRYPTION_MASTER_KEY` to encrypt
WAL and snapshot lines at rest. For AWS KMS:

```
DASH_ENCRYPTION_PROVIDER=aws-kms
DASH_AWS_KMS_KEY_ID=arn:aws:kms:us-east-1:111111111111:key/...
DASH_AWS_KMS_REGION=us-east-1
```

See `deploy/container/.env.example` for all supported variables.

## SDKs

The OpenAPI spec can generate clients with any OpenAPI generator. Example:

```bash
# TypeScript-axios
docker run --rm -v $(pwd)/docs/api:/local openapitools/openapi-generator-cli generate \
  -i /local/openapi.yaml \
  -g typescript-axios \
  -o /local/out/ts

# Python
docker run --rm -v $(pwd)/docs/api:/local openapitools/openapi-generator-cli generate \
  -i /local/openapi.yaml \
  -g python \
  -o /local/out/python
```

The repository CI (`.github/workflows/sdks.yml`) regenerates and publishes SDKs
on tagged releases.

## Monitoring

A Prometheus + Grafana overlay is available for production observability:

```bash
docker compose \
  -f deploy/container/docker-compose.yml \
  -f deploy/container/docker-compose.monitoring.yml \
  --profile monitoring up
```

Prometheus scrapes the `/metrics` endpoints on `ingestion` (8081) and `retrieval` (8080). Grafana is available at http://localhost:3000 with the `DASH Overview` dashboard. Set `GRAFANA_ADMIN_PASSWORD` in `.env`.

## Health and metrics

| Endpoint | Purpose |
|---|---|
| `GET /health` | Kubernetes liveness/health |
| `GET /ready` | Readiness; returns `503` if disk persistence fails |
| `GET /metrics` | Prometheus `/metrics` text |

## More

- [Deployment guide](../deploy/helm/dash/README.md)
- [Production readiness plan](../plans/2026-08-09-production-readiness-remediation.md)
- [SOC 2 readiness](../compliance/soc2-readiness.md)
