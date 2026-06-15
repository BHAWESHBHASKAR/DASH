# Configuration

DASH is configured entirely by environment variables. There is no config file. The variables are grouped by the service they apply to (`DASH_INGEST_*` for the ingestion service, `DASH_RETRIEVAL_*` for the retrieval service) and the shared variables are `DASH_LOG_LEVEL` and a few auth / rate-limit knobs.

## Conventions

- Every variable is `UPPER_SNAKE_CASE`.
- The service prefix is the first segment: `DASH_INGEST_*` or `DASH_RETRIEVAL_*`.
- Booleans are `true` / `false` (case-insensitive).
- Durations are encoded as a number of seconds unless noted.
- Paths are absolute. Relative paths are rejected at startup.
- An empty value (`DASH_INGEST_PORT=`) is treated as "unset" — the default applies.

## Logging

| Variable           | Default | Notes                                                              |
| ------------------ | ------- | ------------------------------------------------------------------ |
| `DASH_LOG_LEVEL`   | `info`  | One of `trace`, `debug`, `info`, `warn`, `error`.                  |
| `DASH_LOG_FORMAT`  | `json`  | Currently only `json` is supported.                                |

## Embedding provider

| Variable                                | Default  | Notes                                                                                |
| --------------------------------------- | -------- | ------------------------------------------------------------------------------------ |
| `DASH_EMBEDDING_PROVIDER`               | `hash`   | `hash`, `ollama`, or `openai`.                                                       |
| `DASH_EMBEDDING_MODEL`                  | (varies) | Model name. For `hash`, pinned to 768-dim. For `ollama`/`openai`, the model to call. |
| `DASH_EMBEDDING_DIM`                    | `768`    | The vector dimension. Pinned per tenant at first ingest.                             |
| `DASH_OLLAMA_BASE_URL`                  | (none)   | Required if `DASH_EMBEDDING_PROVIDER=ollama`.                                         |
| `DASH_OLLAMA_TIMEOUT_MS`                | `5000`   | HTTP timeout for Ollama calls.                                                       |
| `DASH_OPENAI_API_KEY`                   | (none)   | Required if `DASH_EMBEDDING_PROVIDER=openai`.                                        |
| `DASH_OPENAI_BASE_URL`                  | `https://api.openai.com` | Override for OpenAI-compatible providers.                    |
| `DASH_OPENAI_TIMEOUT_MS`                | `10000`  | HTTP timeout for OpenAI calls.                                                       |

## Persistence

| Variable                                | Default                              | Notes                                                              |
| --------------------------------------- | ------------------------------------ | ------------------------------------------------------------------ |
| `DASH_INGEST_PERSISTENCE_PATH`          | (unset)                              | Path to the `redb` file for the ingestion service. Unset = WAL-only.|
| `DASH_RETRIEVAL_PERSISTENCE_PATH`       | (unset)                              | Path to the `redb` file for the retrieval service. Unset = WAL-only.|
| `DASH_INGEST_WAL_PATH`                  | `/var/lib/dash/ingest.wal`           | Path to the WAL.                                                   |
| `DASH_INGEST_WAL_SEGMENT_BYTES`         | `67108864`                           | WAL segment size (bytes).                                          |
| `DASH_INGEST_WAL_FSYNC_POLICY`          | `data`                               | `data` or `none`. The `none` policy is for stress testing only.    |
| `DASH_INGEST_ALLOW_UNSAFE_WAL_DURABILITY` | `false`                            | Required to set `DASH_INGEST_WAL_FSYNC_POLICY=none`.               |
| `DASH_INGEST_CHECKPOINT_ON_SIGHUP`      | `true`                               | Whether `SIGHUP` triggers a checkpoint.                            |

## Auth

| Variable                                | Default  | Notes                                                                                       |
| --------------------------------------- | -------- | ------------------------------------------------------------------------------------------- |
| `DASH_INGEST_JWT_PUBLIC_KEY`            | (none)   | PEM-encoded public key. Either this or `DASH_INGEST_JWT_PUBLIC_KEY_PATH` is required.      |
| `DASH_INGEST_JWT_PUBLIC_KEY_PATH`       | (none)   | Path to the PEM file.                                                                       |
| `DASH_INGEST_JWT_ALGORITHM`             | `RS256`  | `RS256`, `RS384`, `RS512`, `ES256`, `EdDSA`.                                               |
| `DASH_INGEST_JWT_AUDIENCE`              | `dash`   | The required `aud` claim.                                                                   |
| `DASH_RETRIEVAL_JWT_PUBLIC_KEY`         | (none)   | As above, for the retrieval service.                                                        |
| `DASH_RETRIEVAL_JWT_PUBLIC_KEY_PATH`    | (none)   | As above, for the retrieval service.                                                        |
| `DASH_RETRIEVAL_JWT_ALGORITHM`          | `RS256`  | As above, for the retrieval service.                                                        |
| `DASH_RETRIEVAL_JWT_AUDIENCE`           | `dash`   | As above, for the retrieval service.                                                        |
| `DASH_API_KEY_OVERLAP_SECONDS`          | `60`     | How long an old API key remains valid after rotation.                                       |

## Network

| Variable               | Default | Notes                                                            |
| ---------------------- | ------- | ---------------------------------------------------------------- |
| `DASH_INGEST_PORT`     | `8081`  | Port the ingestion service binds to.                             |
| `DASH_INGEST_WORKERS`  | `4`     | Number of thread-per-connection workers.                         |
| `DASH_INGEST_BIND`     | `0.0.0.0` | The bind address. Override for unix sockets.                   |
| `DASH_RETRIEVAL_PORT`  | `8080`  | Port the retrieval service binds to.                             |
| `DASH_RETRIEVAL_WORKERS` | `4`   | Number of thread-per-connection workers.                         |
| `DASH_RETRIEVAL_BIND`  | `0.0.0.0` | The bind address. Override for unix sockets.                   |
| `DASH_TCP_READ_TIMEOUT_MS` | `30000` | TCP read timeout (slow-loris guard).                            |
| `DASH_MAX_BODY_BYTES`  | `10485760` | 10 MiB. The cap on request body size.                          |

## Rate limiting

| Variable                                | Default | Notes                                                            |
| --------------------------------------- | ------- | ---------------------------------------------------------------- |
| `DASH_RETRIEVAL_RATE_LIMIT_RPS`         | `100`   | Per-tenant tokens per second.                                    |
| `DASH_RETRIEVAL_RATE_LIMIT_BURST`       | `200`   | Per-tenant burst size.                                           |
| `DASH_INGEST_RATE_LIMIT_RPS`            | `200`   | Per-tenant tokens per second for writes.                         |
| `DASH_INGEST_RATE_LIMIT_BURST`          | `400`   | Per-tenant burst size for writes.                                |

## ANN

| Variable                                  | Default | Notes                                                            |
| ----------------------------------------- | ------- | ---------------------------------------------------------------- |
| `DASH_ANN_M`                              | `16`    | HNSW `M` (the out-degree).                                       |
| `DASH_ANN_EF_CONSTRUCTION`                | `200`   | HNSW `efConstruction`.                                            |
| `DASH_ANN_EF_SEARCH`                      | `64`    | HNSW `efSearch`.                                                 |
| `DASH_ANN_REBUILD_THRESHOLD`              | `1000`  | Rebuild the index every N inserts.                               |

## Audit

| Variable                          | Default | Notes                                                            |
| --------------------------------- | ------- | ---------------------------------------------------------------- |
| `DASH_AUDIT_LOG_PATH`             | `/var/lib/dash/audit.log` | Path to the audit log file.                  |
| `DASH_AUDIT_RETENTION_DAYS`       | `2555`  | 7 years by default.                                             |
| `DASH_AUDIT_COMPACT_AT_RATIO`     | `0.8`   | When the log is 80% full, compact to summary records.            |
| `DASH_IDEMPOTENCY_RETENTION_DAYS` | `30`    | How long `(tenant_id, idempotency_key)` records are kept.        |

## Metrics

| Variable                       | Default | Notes                                                            |
| ------------------------------ | ------- | ---------------------------------------------------------------- |
| `DASH_METRICS_ENABLED`         | `true`  | Whether `/metrics` is served.                                    |
| `DASH_METRICS_NAMESPACE`       | `dash`  | The Prometheus metric prefix.                                   |

## Putting it all together

A production retrieval service:

```bash
DASH_LOG_LEVEL=info \
DASH_RETRIEVAL_PORT=8080 \
DASH_RETRIEVAL_PERSISTENCE_PATH=/var/lib/dash/retrieval.redb \
DASH_EMBEDDING_PROVIDER=ollama \
DASH_EMBEDDING_MODEL=nomic-embed-text \
DASH_OLLAMA_BASE_URL=http://ollama.internal:11434 \
DASH_RETRIEVAL_JWT_PUBLIC_KEY_PATH=/etc/dash/jwt.pub \
DASH_RETRIEVAL_RATE_LIMIT_RPS=500 \
DASH_RETRIEVAL_RATE_LIMIT_BURST=1000 \
  /usr/local/bin/dash-retrieval
```

A production ingestion service:

```bash
DASH_LOG_LEVEL=info \
DASH_INGEST_PORT=8081 \
DASH_INGEST_PERSISTENCE_PATH=/var/lib/dash/ingest.redb \
DASH_INGEST_WAL_PATH=/var/lib/dash/ingest.wal \
DASH_INGEST_JWT_PUBLIC_KEY_PATH=/etc/dash/jwt.pub \
DASH_INGEST_RATE_LIMIT_RPS=200 \
DASH_INGEST_RATE_LIMIT_BURST=400 \
  /usr/local/bin/dash-ingestion
```

For the full deployment story, see [Deploy](../operations/deploy.md).
