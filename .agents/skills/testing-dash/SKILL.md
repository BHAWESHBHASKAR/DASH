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

- None for local validation. Set `DASH_INGEST_API_KEY` and
  `DASH_RETRIEVAL_API_KEY` in a `.env` file or shell before starting compose.
- The Docker Compose defaults use `change-me-*` placeholder keys. Services
  will emit a startup warning; set `DASH_STRICT_SECRETS=1` to fail on
  placeholder or short secrets in production.

## Service endpoints and keys

| Service | Port | API key env var | Health endpoint |
|---|---|---|---|
| ingestion | `8081` | `DASH_INGEST_API_KEY` | `http://127.0.0.1:8081/health` |
| retrieval | `8080` | `DASH_RETRIEVAL_API_KEY` | `http://127.0.0.1:8080/health` |

Auth is `x-api-key` (or `Authorization: Bearer <key>`).

## Minimal end-to-end payload

Ingest:

Set the keys in your shell first (do not commit real secrets):

```bash
export DASH_INGEST_API_KEY='<YOUR_INGEST_API_KEY>'
export DASH_RETRIEVAL_API_KEY='<YOUR_RETRIEVAL_API_KEY>'
```

Ingest:

```bash
curl -X POST http://127.0.0.1:8081/v1/ingest \
  -H 'Content-Type: application/json' \
  -H "x-api-key: $DASH_INGEST_API_KEY" \
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
  -H "x-api-key: $DASH_RETRIEVAL_API_KEY" \
  -d '{"tenant_id":"t1","query":"DASH end-to-end test claim","top_k":3}'
```

## Known pitfalls in the default compose setup

1. `segment-maintenance` container crash loop: the image `CMD`
   `["--serve"]` was passed to `segment-maintenance-daemon`, which does not
   accept `--serve`. The daemon runs its loop when started with no arguments.
2. End-to-end data gap: **fixed**. `retrieval` now polls `ingestion`'s
   `/internal/replication/wal` endpoint every 250ms in the default compose
   (`DASH_RETRIEVAL_REPLICATION_SOURCE_URL: http://ingestion:8081`). A retrieve
   after an ingest now returns the ingested claim.
3. Follower replication offset is currently in-memory only, so a restarted
   `retrieval` replica re-applies the full upstream WAL on startup.
4. Python live integration tests (`sdks/python/tests/test_live_integration.py`)
   read API keys from `DASH_LIVE_API_KEY` (retrieval) and
   `DASH_LIVE_INGEST_API_KEY` (ingestion, falling back to `DASH_LIVE_API_KEY`).
   Set these before running the tests; no default secrets are included.

## Useful diagnostics

```bash
# container health
docker compose -f deploy/container/docker-compose.yml ps

# per-service logs
docker compose -f deploy/container/docker-compose.yml logs --tail=50 <service>

# live SDK tests (requires running compose and exported keys)
export DASH_LIVE_URL=http://127.0.0.1:8080
export DASH_LIVE_RETRIEVAL_URL=http://127.0.0.1:8080
export DASH_LIVE_INGESTION_URL=http://127.0.0.1:8081
export DASH_LIVE_API_KEY='<YOUR_RETRIEVAL_API_KEY>'
export DASH_LIVE_INGEST_API_KEY='<YOUR_INGEST_API_KEY>'
cd sdks/python
pytest -v tests/test_live_integration.py
```

## Notes

- `cargo test` and `./scripts/ci.sh` are single-threaded (`--test-threads=1`)
  because retrieval integration tests rely on process-global environment
  variables and locks.
- The `segment-maintenance` service healthcheck uses `pidof` because the
  daemon has no HTTP port.
