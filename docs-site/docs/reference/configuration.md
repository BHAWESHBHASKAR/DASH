# Configuration

DASH is configured entirely through environment variables. There is no config file. This page lists **only variables that the Rust code reads today**, plus a short list of items that are new in v0.3.0 (marked `v0.3.0`) and a list of names that earlier versions of this page documented but that do not exist (see [Variables that do not exist](#variables-that-do-not-exist)).

The list was derived from the source by searching for `DASH_` string literals in `services/` and `pkg/`. A generated reference is planned (register item DOC-03, P1); until then, if this page and the code disagree, the code wins.

## Conventions

- **Legacy `EME_` aliases.** Every `DASH_*` service variable below is also read under the legacy name `EME_*` (for example `EME_INGEST_BIND`) when the `DASH_*` name is unset. New deployments should use `DASH_*`. Exceptions: `DASH_STRICT_SECRETS`, `DASH_LOG_FORMAT`, `DASH_EMBEDDING_PROVIDER`, `DASH_OLLAMA_*`, `DASH_OPENAI_*`, `DASH_VECTOR_BACKEND` have no `EME_` alias.
- **Booleans** accept `1`, `true`, `yes`, `on` (true) and `0`, `false`, `no`, `off` (false), case-insensitive, unless a row says otherwise.
- **Unparseable numbers** are silently ignored and the default applies, except where a row says the service exits.
- **Bind addresses** are full `host:port` strings, not a bare port. There is no `DASH_INGEST_PORT` or `DASH_RETRIEVAL_PORT`.
- There is no config-file path, no relative-path rejection and no "empty value means unset" rule; do not rely on them.
- Services are restarted to pick up changes, with two exceptions: the API-key revocation file and the API key / JWT / tenant-allowlist variables are re-read on every request.

## Defaults at a glance

| Service | Default bind | Role |
|---|---|---|
| ingestion | `127.0.0.1:8081` | write API, WAL owner, replication source |
| retrieval | `127.0.0.1:8080` | read API, embeddings endpoint, replication follower |
| control-plane | `127.0.0.1:8090` | placement and leader state |

The container image and compose file override the bind addresses to `0.0.0.0:<port>`.

## Startup, secrets and logging

| Variable | Default | Read by | Description |
|---|---|---|---|
| `DASH_STRICT_SECRETS` | off today; **on by default in v0.3.0** | ingestion, retrieval | When on, the service exits (code 2) if a configured secret is empty, a placeholder (`change-me`, `placeholder`, `replace-me`, `example`, `sample`) or too short, and if no API key is configured. Today's minimum length is 16 characters and only `1`/`true`/`yes` enable it. v0.3.0: minimum 32 characters, default on. |
| `DASH_INSECURE_DEV_MODE` | unset | ingestion, retrieval, control-plane | `v0.3.0`. Set to `1` to allow starting without credentials. Dev mode binds localhost only. Without it, services refuse to start with no credentials configured. |
| `DASH_LOG_FORMAT` | compact text | all services using `dash_common` | `json` switches log output to JSON lines. Any other value uses the compact text format. |
| `RUST_LOG` | `info` | all services using `dash_common` | Standard `tracing` env filter. |

## Network and transport

| Variable | Default | Read by | Description |
|---|---|---|---|
| `DASH_INGEST_BIND` | `127.0.0.1:8081` | ingestion | Listen address. |
| `DASH_RETRIEVAL_BIND` | `127.0.0.1:8080` | retrieval | Listen address. |
| `DASH_CONTROL_PLANE_BIND` | `127.0.0.1:8090` | control-plane | Listen address. |
| `DASH_INGEST_HTTP_WORKERS` | `min(CPU count, 32)`, or 4 if undetectable | ingestion | Worker threads. Must be > 0. |
| `DASH_RETRIEVAL_HTTP_WORKERS` | same as above | retrieval | Worker threads. Must be > 0. |
| `DASH_INGEST_HTTP_QUEUE_CAPACITY` | `workers * 64` | ingestion | Bounded accept queue. When full, the service answers 503 `worker queue full`. |
| `DASH_RETRIEVAL_HTTP_QUEUE_CAPACITY` | `workers * 64` | retrieval | Same, for retrieval. |

Fixed limits (compiled in, **not** configurable): request body cap 16 MiB on ingestion, per-connection socket timeout 5 seconds. There is no `DASH_MAX_BODY_BYTES` or `DASH_TCP_READ_TIMEOUT_MS`.

## Authentication and authorization

Ingestion variables use the `DASH_INGEST_` prefix; retrieval variables use `DASH_RETRIEVAL_`. Both services support the same set. Credentials are separate per service. Clients send an API key in the `x-api-key` header or as `Authorization: Bearer <key>`.

| Suffix | Default | Description |
|---|---|---|
| `_API_KEY` | unset | A single accepted API key. |
| `_API_KEYS` | unset | Comma-separated accepted API keys. |
| `_API_KEY_SCOPES` | unset | Scoped keys, entries separated by `;`, each `key:tenantA,tenantB[:role1,role2]`. A tenant of `*` means all tenants. Roles are `admin`, `ingest`, `retrieve`, `read_only`; if omitted the key has the service's default role. A scoped key that is not in `_API_KEYS` is still accepted. |
| `_ALLOWED_TENANTS` | any tenant | Comma-separated tenant allowlist applied to every authenticated request; `*` or empty means any. |
| `_REVOKED_API_KEYS` | unset | Comma-separated keys rejected with 401 `API key revoked`. |
| `_REVOKED_KEYS_PATH` | unset | File with one revoked key per line, re-read on every request. |
| `_AUDIT_LOG_PATH` | unset (audit off) | Path of the SHA-256 hash-chained JSON-lines audit log. See [Security audit](../operations/security-audit.md). |
| `_RATE_LIMIT_PER_TENANT_RPS` | `100` | Per-tenant rate limit setting. In v0.2.x the limiter state is rebuilt on every request, so this does **not** throttle and a rejection would return 401. v0.3.0 enforces it with HTTP 429. |
| `_RATE_LIMIT_BURST` | `200` | Per-tenant burst ceiling, with the same caveat. |

Full names, for example: `DASH_INGEST_API_KEY`, `DASH_RETRIEVAL_API_KEYS`, `DASH_INGEST_API_KEY_SCOPES`, `DASH_RETRIEVAL_ALLOWED_TENANTS`, `DASH_INGEST_RATE_LIMIT_PER_TENANT_RPS`, `DASH_RETRIEVAL_RATE_LIMIT_BURST`.

If no API key, scoped key or JWT secret is configured, today's services accept unauthenticated requests. v0.3.0 refuses to start in that state unless `DASH_INSECURE_DEV_MODE=1`.

### JWT (HS256) and OIDC

| Suffix (`DASH_INGEST_` / `DASH_RETRIEVAL_`) | Default | Description |
|---|---|---|
| `_JWT_HS256_SECRET` | unset | Primary HS256 secret. HS256 validation is active only when this is set. |
| `_JWT_HS256_SECRETS` | unset | Comma-separated fallback secrets accepted during rotation. |
| `_JWT_HS256_SECRETS_BY_KID` | unset | `kid:secret;kid2:secret2`, selected by the token's `kid` header. |
| `_JWT_ISSUER` | unset | Required `iss`, if set. |
| `_JWT_AUDIENCE` | unset | Required `aud`, if set. |
| `_JWT_LEEWAY_SECS` | `0` | Clock skew allowance. |
| `_JWT_REQUIRE_EXP` | `true` | Require an `exp` claim. |
| `_JWT_ROLE_CLAIM` | `dash_roles` | Claim carrying the roles. |
| `_JWT_PROVIDER` | `hs256` | Set to `oidc` to validate against a JWKS instead. |
| `_JWT_JWKS_URL` | unset | JWKS URL (OIDC mode; required with the issuer). |
| `_JWT_JWKS_REFRESH_MINUTES` | `15` | JWKS cache refresh interval. |
| `_JWT_TENANT_CLAIMS` | `tenant_id,tenants,tenant_ids` | Comma-separated claim names that carry the tenant list (OIDC mode). |

HS256 tokens carry tenants in `tenant_id` (string) or `tenants` / `tenant_ids` (array); `*` grants all tenants. There are no RS256/ES256/EdDSA PEM-key variables (`..._JWT_PUBLIC_KEY`, `..._JWT_ALGORITHM` do not exist); asymmetric validation is only available through the OIDC/JWKS path.

### Replication and control-plane authentication

| Variable | Default | Read by | Description |
|---|---|---|---|
| `DASH_INGEST_REPLICATION_TOKEN` | unset (open today); **required in v0.3.0** | ingestion | Shared token checked against the `x-replication-token` header on `/internal/replication/*`. Also used by an ingestion node that pulls from another source. |
| `DASH_RETRIEVAL_REPLICATION_TOKEN` | unset | retrieval | Token the retrieval follower sends to the source. Must equal the source's `DASH_INGEST_REPLICATION_TOKEN`. |
| `DASH_CONTROL_PLANE_TOKEN` | unset (open today); **required in v0.3.0** | control-plane | `v0.3.0`. Token required to read or change placement and to promote replicas. |

## Persistence and WAL

| Variable | Default | Read by | Description |
|---|---|---|---|
| `DASH_INGEST_WAL_PATH` | unset | ingestion | WAL file. When unset the ingestion service is **purely in-memory** (data is lost on restart). The compose file sets it. |
| `DASH_RETRIEVAL_WAL_PATH` | unset | retrieval | Optional local WAL replayed at startup. |
| `DASH_INGEST_PERSISTENCE_PATH` | `./data/dash-ingestion.redb` | ingestion | redb file. Used only when a WAL path is set. |
| `DASH_RETRIEVAL_PERSISTENCE_PATH` | `./data/dash-retrieval.redb` | retrieval | redb file. |
| `DASH_INGEST_PERSISTENCE_DISABLE` | off | ingestion | Set to exactly `1` to skip redb. If the redb file cannot be opened, the service logs an error and continues in memory. |
| `DASH_RETRIEVAL_PERSISTENCE_DISABLE` | off | retrieval | Same, for retrieval. |
| `DASH_INGEST_WAL_SYNC_EVERY_RECORDS` | `1` | ingestion | fsync after this many records. |
| `DASH_INGEST_WAL_APPEND_BUFFER_RECORDS` | `1` | ingestion | Buffered records before write. |
| `DASH_INGEST_WAL_SYNC_INTERVAL_MS` | unset | ingestion | Time-based fsync interval. |
| `DASH_INGEST_WAL_BACKGROUND_FLUSH_ONLY` | `false` | ingestion | Flush only from the background flusher. |
| `DASH_INGEST_WAL_ASYNC_FLUSH_INTERVAL_MS` | auto (250 ms when batching is enabled) | ingestion | Positive integer, `auto`, or `off`. |
| `DASH_INGEST_ALLOW_UNSAFE_WAL_DURABILITY` | `false` | ingestion | Required to run with durability settings that exceed the safe limits: sync-every or append buffer above 256 records, sync interval or async flush interval above 5000 ms, or batched writes without a sync interval. Without it the service exits with code 2. There is no `DASH_INGEST_WAL_FSYNC_POLICY`. |
| `DASH_CHECKPOINT_MAX_WAL_RECORDS` | unset | ingestion | Trigger a checkpoint after this many WAL records. |
| `DASH_CHECKPOINT_MAX_WAL_BYTES` | unset | ingestion | Trigger a checkpoint after this many WAL bytes. |
| `DASH_INGEST_BATCH_MAX_ITEMS` | `128` | ingestion | Maximum items in `POST /v1/ingest/batch`. |

## Replication follower (retrieval)

| Variable | Default | Description |
|---|---|---|
| `DASH_RETRIEVAL_REPLICATION_SOURCE_URL` | unset (follower off) | Base URL of the ingestion service, for example `http://ingestion:8081`. |
| `DASH_RETRIEVAL_REPLICATION_POLL_INTERVAL_MS` | `1000` | Poll interval. |
| `DASH_RETRIEVAL_REPLICATION_MAX_RECORDS` | `512` | Records per pull. |
| `DASH_RETRIEVAL_REPLICATION_OFFSET_PATH` | `/var/lib/dash/state/retrieval-replication.offset` | File storing the last applied offset. |
| `DASH_INGEST_REPLICATION_SOURCE_URL` | unset (off) | An ingestion node can itself follow another ingestion node. |
| `DASH_INGEST_REPLICATION_POLL_INTERVAL_MS` | `500` | Poll interval for ingestion-to-ingestion pulls. |
| `DASH_INGEST_REPLICATION_MAX_RECORDS` | `512` | Records per pull. |

## Placement and routing

Placement routing is enabled when `DASH_ROUTER_PLACEMENT_FILE` or `DASH_ROUTER_CONTROL_PLANE_URL` is set. Both services then require a local node id: ingestion exits (code 2) if it is missing, retrieval reports a configuration error on requests.

| Variable | Default | Read by | Description |
|---|---|---|---|
| `DASH_ROUTER_PLACEMENT_FILE` | unset | ingestion, retrieval, control-plane | CSV of shard placements. The control plane uses it as its initial state. |
| `DASH_ROUTER_CONTROL_PLANE_URL` | unset | ingestion, retrieval | Fetch placement from the control plane. |
| `DASH_ROUTER_LOCAL_NODE_ID` | unset | ingestion, retrieval | This node's id. Falls back to `DASH_NODE_ID`. |
| `DASH_NODE_ID` | unset | ingestion, retrieval | Fallback for the local node id. |
| `DASH_ROUTER_READ_PREFERENCE` | `any_healthy` | retrieval | Replica read preference. |
| `DASH_ROUTER_PLACEMENT_RELOAD_INTERVAL_MS` | off | ingestion, retrieval | Reload placement on this interval. |
| `DASH_ROUTER_SHARD_IDS` | from placement | ingestion, retrieval | Comma-separated shard ids override. |
| `DASH_ROUTER_REPLICA_COUNT` | from placement | ingestion, retrieval | Replica count override. |
| `DASH_ROUTER_VIRTUAL_NODES_PER_SHARD` | `64` | ingestion, retrieval | Virtual nodes for shard hashing. |
| `DASH_CONTROL_PLANE_NODE_ID` | `control-plane-<pid>` | control-plane | Node id used in leader election. |
| `DASH_CONTROL_PLANE_STATE_PATH` | unset (state not persisted) | control-plane | Persisted placement CSV. |
| `DASH_CONTROL_PLANE_STATE_SHA256_PATH` | unset | control-plane | Checksum file for the persisted state; verified at startup. |
| `DASH_CONTROL_PLANE_LEASE_PATH` | unset (standalone, always leader) | control-plane | File lease for leader election. |
| `DASH_CONTROL_PLANE_LEASE_DURATION_MS` | `30000` | control-plane | Lease length. |
| `DASH_CONTROL_PLANE_LEASE_RENEWAL_MS` | `10000` | control-plane | Renewal interval. |

## ANN tuning

Each variable can be set per service (`DASH_INGEST_ANN_*`, `DASH_RETRIEVAL_ANN_*`) or shared (`DASH_ANN_*`); the per-service name wins. The index is an in-repo HNSW-style graph, not `usearch`.

| Suffix | Default | Description |
|---|---|---|
| `ANN_MAX_NEIGHBORS_BASE` | `12` | Max neighbors on the base layer. |
| `ANN_MAX_NEIGHBORS_UPPER` | `6` | Max neighbors on upper layers. |
| `ANN_SEARCH_EXPANSION_FACTOR` | `16` | Candidate expansion multiplier. |
| `ANN_SEARCH_EXPANSION_MIN` | `64` | Minimum candidates examined. |
| `ANN_SEARCH_EXPANSION_MAX` | `4096` | Maximum candidates examined. |

There is no `DASH_ANN_M`, `DASH_ANN_EF_CONSTRUCTION`, `DASH_ANN_EF_SEARCH` or `DASH_ANN_REBUILD_THRESHOLD`.

## Retrieval tuning

| Variable | Default | Description |
|---|---|---|
| `DASH_RETRIEVAL_GRAPH_MAX_HOPS` | `3` | Graph expansion depth. |
| `DASH_RETRIEVAL_GRAPH_EDGE_DEPTH_DECAY` | `0.75` | Per-hop weight decay. |
| `DASH_RETRIEVAL_GRAPH_SUPPORT_PATH_BONUS` | `0.16` | Score bonus per support path. |
| `DASH_RETRIEVAL_GRAPH_CONTRADICTION_DEPTH_PENALTY` | `0.20` | Score penalty for contradiction chains. |
| `DASH_RETRIEVAL_SEGMENT_DIR` | unset | Directory of published index segments to prefilter from. |
| `DASH_RETRIEVAL_SEGMENT_CACHE_REFRESH_MS` | `1000` | Segment cache refresh interval. |
| `DASH_RETRIEVAL_DISK_NATIVE_SEGMENT_EXECUTION` | `true` | Execute against disk segments natively. |
| `DASH_RETRIEVAL_STORAGE_DIVERGENCE_WARN_DELTA_COUNT` | `1000` | Divergence warning threshold (records) in `/debug/storage-visibility`. |
| `DASH_RETRIEVAL_STORAGE_DIVERGENCE_WARN_RATIO` | `0.25` | Divergence warning threshold (ratio). |
| `DASH_VECTOR_BACKEND` | auto | `cpu`, `gpu` or unset. The GPU backend is a stub that always reports "unavailable", so everything runs on CPU. |

## Segments and maintenance

Read by the ingestion service (segment publishing) and the `segment-maintenance-daemon` binary.

| Variable | Default | Description |
|---|---|---|
| `DASH_INGEST_SEGMENT_DIR` | unset (segment publishing off) | Segment root directory. |
| `DASH_INGEST_SEGMENT_MAX_SEGMENT_SIZE` (alias `DASH_SEGMENT_MAX_SEGMENT_SIZE`) | `10000` | Max claims per segment. |
| `DASH_INGEST_SEGMENT_MAX_SEGMENTS_PER_TIER` (alias `DASH_SEGMENT_MAX_SEGMENTS_PER_TIER`) | `8` | Segments per tier before compaction. |
| `DASH_INGEST_SEGMENT_MAX_COMPACTION_INPUT_SEGMENTS` (alias `DASH_SEGMENT_MAX_COMPACTION_INPUT_SEGMENTS`) | `4` | Max segments merged at once. |
| `DASH_INGEST_SEGMENT_MAINTENANCE_INTERVAL_MS` | `30000` | Maintenance loop interval; `0` disables in-process maintenance. |
| `DASH_INGEST_SEGMENT_GC_MIN_STALE_AGE_MS` | `60000` | Minimum age before an unreferenced segment file is deleted. |

## Embedding providers

| Variable | Default | Read by | Description |
|---|---|---|---|
| `DASH_EMBEDDING_PROVIDER` | `hash` | retrieval (`/v1/embeddings`, query embedding), ingestion | `hash`, `ollama` or `openai`. Unknown values fall back to `hash` with a warning on stderr. |
| `DASH_OLLAMA_ENDPOINT` | `http://localhost:11434` | embeddings library | Ollama endpoint. (`DASH_OLLAMA_BASE_URL` does not exist.) |
| `DASH_OLLAMA_MODEL` | `nomic-embed-text` | embeddings library | Ollama model. |
| `DASH_OPENAI_API_KEY` | unset | embeddings library | Required for `openai`; without it the provider falls back to `hash`. |
| `DASH_OPENAI_MODEL` | `text-embedding-3-small` | embeddings library | OpenAI model. |

The hash provider's default dimension is 384. The OpenAI provider in v0.2.x opens a plain TCP connection and cannot reach `https://api.openai.com`; TLS support is a v0.3.0 change. There is no `DASH_EMBEDDING_MODEL`, `DASH_EMBEDDING_DIM`, `DASH_OPENAI_BASE_URL`, `DASH_OPENAI_TIMEOUT_MS` or `DASH_OLLAMA_TIMEOUT_MS` (timeouts are compiled in: Ollama 5 s, OpenAI 30 s).

### Ingestion extraction and parsing

These control `POST /v1/ingest/raw` and `/v1/ingest/document` (see `GET /debug/document-parser`).

| Variable | Default | Description |
|---|---|---|
| `DASH_INGEST_RAW_EXTRACTION_PROVIDER` | `rule_sentence` | `rule_sentence` or `adapter_command`. |
| `DASH_INGEST_RAW_ADAPTER_CMD` | unset | Command used when the provider is `adapter_command`. |
| `DASH_INGEST_DOCUMENT_PARSER_PROVIDER` | `builtin_utf8` | `builtin_utf8` or `adapter_command`. |
| `DASH_INGEST_DOCUMENT_ADAPTER_CMD` | unset | Command used by the document parser adapter; receives `DASH_DOCUMENT_MIME_TYPE`. |
| `DASH_INGEST_EMBEDDING_PROVIDER` | `hash_vector` | `hash_vector`, `off`, or `adapter_command`. |
| `DASH_INGEST_EMBEDDING_ADAPTER_CMD` | unset | Command used when the provider is `adapter_command`. |
| `DASH_INGEST_EMBEDDING_DIMENSIONS` | `64` | Hash vector dimensions, clamped to 8..4096. |

## Encryption (library only)

`DASH_ENCRYPTION_PROVIDER` (`none` or `env`) and `DASH_ENCRYPTION_MASTER_KEY` (64 hex characters or base64 of 32 bytes) are read by `pkg/encryption::provider_from_env`. **No service calls it**, so setting them has no effect on stored data. Encryption at rest is planned (P4).

## Benchmarks and load tests

`DASH_BENCH_*` (benchmark thresholds and fixtures in `tests/benchmarks`), `DASH_LIVE_URL`, `DASH_API_KEY` and `DASH_RETRIEVAL_PERSISTENCE_DISABLE` (load-test binary) are read only by the benchmark binaries. See the comments at the top of `tests/benchmarks/src/main.rs` and `tests/benchmarks/src/bin/load_test.rs`.

## Container-only variables

`DASH_BIN` (which binary the image entrypoint runs: `ingestion`, `retrieval`, `control-plane` or `segment-maintenance-daemon`; default `retrieval`), `DASH_HOME` (default `/opt/dash`) and `DASH_HEALTHCHECK_URL` are read by the shell scripts in `deploy/container/scripts/`, not by the Rust services. The compose file also sets `DASH_INGEST_TRANSPORT_RUNTIME` and `DASH_RETRIEVAL_TRANSPORT_RUNTIME`; no code reads them.

## Variables that do not exist

Earlier versions of this page documented the following. No code reads them; setting them does nothing.

`DASH_LOG_LEVEL` (use `RUST_LOG`), `DASH_EMBEDDING_MODEL`, `DASH_EMBEDDING_DIM`, `DASH_OLLAMA_BASE_URL`, `DASH_OLLAMA_TIMEOUT_MS`, `DASH_OPENAI_BASE_URL`, `DASH_OPENAI_TIMEOUT_MS`, `DASH_INGEST_WAL_SEGMENT_BYTES`, `DASH_INGEST_WAL_FSYNC_POLICY`, `DASH_INGEST_CHECKPOINT_ON_SIGHUP`, `DASH_INGEST_JWT_PUBLIC_KEY`, `DASH_INGEST_JWT_PUBLIC_KEY_PATH`, `DASH_INGEST_JWT_ALGORITHM`, `DASH_RETRIEVAL_JWT_PUBLIC_KEY`, `DASH_RETRIEVAL_JWT_PUBLIC_KEY_PATH`, `DASH_RETRIEVAL_JWT_ALGORITHM`, `DASH_API_KEY_OVERLAP_SECONDS`, `DASH_INGEST_PORT`, `DASH_RETRIEVAL_PORT`, `DASH_INGEST_WORKERS`, `DASH_RETRIEVAL_WORKERS`, `DASH_TCP_READ_TIMEOUT_MS`, `DASH_MAX_BODY_BYTES`, `DASH_RETRIEVAL_RATE_LIMIT_RPS`, `DASH_INGEST_RATE_LIMIT_RPS` (the real names are `*_RATE_LIMIT_PER_TENANT_RPS`), `DASH_ANN_M`, `DASH_ANN_EF_CONSTRUCTION`, `DASH_ANN_EF_SEARCH`, `DASH_ANN_REBUILD_THRESHOLD`, `DASH_AUDIT_LOG_PATH` (use `DASH_INGEST_AUDIT_LOG_PATH` / `DASH_RETRIEVAL_AUDIT_LOG_PATH`), `DASH_AUDIT_RETENTION_DAYS`, `DASH_AUDIT_COMPACT_AT_RATIO`, `DASH_IDEMPOTENCY_RETENTION_DAYS`, `DASH_METRICS_ENABLED`, `DASH_METRICS_NAMESPACE`.

## Example

A local development retrieval + ingestion pair, using real variable names (generate real secrets with `scripts/generate-secrets.sh`):

```bash
export DASH_INGEST_API_KEY=$(openssl rand -hex 16)      # 32 hex chars
export DASH_RETRIEVAL_API_KEY=$(openssl rand -hex 16)
export DASH_INGEST_REPLICATION_TOKEN=$(openssl rand -hex 16)
export DASH_RETRIEVAL_REPLICATION_TOKEN=$DASH_INGEST_REPLICATION_TOKEN

DASH_INGEST_BIND=127.0.0.1:8081 \
DASH_INGEST_WAL_PATH=./data/ingest.wal \
  cargo run --release -p ingestion -- --serve &

DASH_RETRIEVAL_BIND=127.0.0.1:8080 \
DASH_RETRIEVAL_REPLICATION_SOURCE_URL=http://127.0.0.1:8081 \
DASH_RETRIEVAL_REPLICATION_OFFSET_PATH=./data/retrieval.offset \
  cargo run --release -p retrieval -- --serve
```

For the full deployment story, see [Deploy](../operations/deploy.md).
