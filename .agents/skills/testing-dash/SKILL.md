---
name: DASH end-to-end validation
description: |
  How to validate the DASH Rust workspace, SDKs, Docker Compose deployment,
  and the ingest/retrieve round-trip in the current working tree.
---

# DASH end-to-end validation

## One-liner

Run the full validation from the repo root (`BHAWESHBHASKAR/DASH`):

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --all-features -- --test-threads=1
./scripts/ci.sh
cd sdks/python && pip install -e '.[test]' && pytest -v
cd sdks/typescript && npm install && npm test
cd deploy/container && docker compose -f docker-compose.yml up -d --build
```

## Environment prerequisites

- Rust `>=1.97` (edition 2024 workspace).
- `docker compose` v2.
- Python 3.9+ with `pip`.
- Node.js 18+ with `npm`.

## Devin Secrets Needed

- None for local validation. The compose file uses the default keys
  `change-me-ingest-key` and `change-me-retrieval-key`.

## Service endpoints and keys

| Service | Port | Default API key | Health endpoint |
|---|---|---|---|
| ingestion | `8081` | `change-me-ingest-key` | `http://127.0.0.1:8081/health` |
| retrieval | `8080` | `change-me-retrieval-key` | `http://127.0.0.1:8080/health` |

Auth is `x-api-key` (or `Authorization: Bearer <key>`).

## Minimal end-to-end payload

Ingest:

```bash
curl -X POST http://127.0.0.1:8081/v1/ingest \
  -H 'Content-Type: application/json' \
  -H 'x-api-key: change-me-ingest-key' \
  -d '{
    "claim": {
      "claim_id": "c1",
      "tenant_id": "t1",
      "canonical_text": "DASH end-to-end test claim",
      "confidence": 0.95
    },
    "evidence": [],
    "edges": []
  }'
```

Retrieve:

```bash
curl -X POST http://127.0.0.1:8080/v1/retrieve \
  -H 'Content-Type: application/json' \
  -H 'x-api-key: change-me-retrieval-key' \
  -d '{"tenant_id":"t1","query":"DASH end-to-end test claim","top_k":3}'
```

## Known pitfalls in the default compose setup

1. `segment-maintenance` container crash loop: the image `CMD`
   `["--serve"]` was passed to `segment-maintenance-daemon`, which does not
   accept `--serve`. The daemon runs its loop when started with no arguments.
2. End-to-end data gap: ingestion writes WAL/segments to
   `/var/lib/dash/wal/ingestion.wal` and `/var/lib/dash/segments/ingestion`,
   while retrieval reads from `/var/lib/dash/wal/retrieval.wal` and
   `/var/lib/dash/segments/retrieval` (`docker-compose.yml` defaults). No
   compose-level replication path is configured, so a retrieve after an
   ingest returns an empty `results` array by default.
3. Python live integration tests (`sdks/python/tests/test_live_integration.py`)
   use `api_key="not_needed"`. That works for the unauthenticated
   `/v1/embeddings` endpoint, but `/v1/retrieve` rejects it when the retrieval
   service requires the configured `change-me-retrieval-key`.

## Useful diagnostics

```bash
# container health
docker compose -f deploy/container/docker-compose.yml ps

# per-service logs
docker compose -f deploy/container/docker-compose.yml logs --tail=50 <service>

# live SDK tests (requires running compose)
cd sdks/python
DASH_LIVE_URL=http://127.0.0.1:8080 \
DASH_LIVE_RETRIEVAL_URL=http://127.0.0.1:8080 \
DASH_LIVE_INGESTION_URL=http://127.0.0.1:8081 \
DASH_LIVE_API_KEY=change-me-retrieval-key \
DASH_LIVE_INGEST_API_KEY=change-me-ingest-key \
  pytest -v tests/test_live_integration.py
```

## Notes

- `cargo test` and `./scripts/ci.sh` are single-threaded (`--test-threads=1`)
  because retrieval integration tests rely on process-global environment
  variables and locks.
- The `segment-maintenance` service healthcheck uses `pidof` because the
  daemon has no HTTP port.
