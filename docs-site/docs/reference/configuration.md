# Configuration

DASH is configured through environment variables. There is no configuration file; the only file input is the optional [auth overlay](#config-reload-and-sighup) used for SIGHUP reload. This page lists **only variables that the Rust code reads**. It was regenerated from the sources in `services/`, `pkg/` and `tools/` for 0.3.0 (unreleased). If this page and the code disagree, the code wins.

A script keeps the list complete: `scripts/check_config_docs.sh` fails when an environment variable read by the code is not mentioned on this page. See [CONTRIBUTING](../about/contributing.md).

## Conventions

- **Legacy `EME_` aliases.** Most `DASH_*` service variables are also read under the legacy name `EME_*` (for example `EME_INGEST_BIND`) when the `DASH_*` name is unset. New deployments should use `DASH_*`. Variables with **no** `EME_` alias are marked "DASH only" in the tables below; in general these are the ones added in 0.3.0 plus the embedding, Ollama, OpenAI, vector-backend, strict-mode and logging variables.
- **Booleans** accept `1`, `true`, `yes`, `on` (true) and `0`, `false`, `no`, `off` (false), case-insensitive, unless a row says otherwise. A few legacy switches accept only the literal `1`; those rows say so.
- **Unparseable numbers** are silently ignored and the default applies, except where a row says the service exits.
- **Bind addresses** are full `host:port` strings, not a bare port. There is no `DASH_INGEST_PORT` or `DASH_RETRIEVAL_PORT`.
- **Exit codes.** A service that refuses to start because of invalid configuration exits with code 2 (auth, secrets, WAL durability guardrails, placement routing in ingestion) or 1 (failed to open or replay the WAL, retrieval placement configuration, listener failure).
- Variables are read at process start, with these exceptions: the authentication settings can be reloaded with SIGHUP (see below); the revoked-key and revoked-`jti` files are re-read when their modification time or size changes (checked at most once per second); the embedding provider settings (`DASH_EMBEDDING_PROVIDER`, `DASH_OLLAMA_*`, `DASH_OPENAI_*`) and the audit-log path variables are looked up per request, but treat a restart as the supported way to change them.

## Defaults at a glance

| Service | Default bind | Role |
|---|---|---|
| ingestion | `127.0.0.1:8081` | write API, WAL owner, replication source |
| retrieval | `127.0.0.1:8080` | read API, embeddings endpoint, replication follower |
| control-plane | `127.0.0.1:8090` | placement and leader state |

The container image and compose file override the bind addresses to `0.0.0.0:<port>` (and the compose file publishes the host ports on `127.0.0.1`).

## Startup, secrets and logging

| Variable | Default | Read by | Description |
|---|---|---|---|
| `DASH_INSECURE_DEV_MODE` | off | ingestion, retrieval, control-plane | Allows a service to start with **no credentials configured**, and with them accept every request. Also forces a loopback bind (see `DASH_INSECURE_DEV_MODE_ALLOW_NON_LOOPBACK`) and allows an unset replication token. The control plane accepts only the literal `1` and, with no token configured, binds loopback only. Never set it in production. DASH only. |
| `DASH_INSECURE_DEV_MODE_ALLOW_NON_LOOPBACK` | off | ingestion, retrieval | With dev mode on, keep the requested non-loopback bind host instead of replacing it with `127.0.0.1`. DASH only. |
| `DASH_STRICT_SECRETS` | **on** | ingestion, retrieval | Strict secret validation is on by default. It can only be turned off by setting `DASH_STRICT_SECRETS=0` **and** `DASH_INSECURE_DEV_MODE=1`. When on, the service exits (code 2) if a configured secret is empty, a placeholder (contains `change-me`, `changeme`, `placeholder`, `replace-me`, `example`, `sample`, `<...>` markers, or starts with `secret`/`password`) or too short. Minimum lengths: **16** characters for API keys, scoped keys and replication tokens; **32** for HS256 JWT secrets. Error messages name the setting, never the value. The control plane does not use this check. DASH only. |
| `DASH_LOG_FORMAT` | compact text | ingestion, retrieval | `json` switches log output to JSON lines. Any other value uses the compact text format. DASH only. |
| `RUST_LOG` | `info` | ingestion, retrieval | Standard `tracing` env filter. |
| `DASH_CONFIG_RELOAD_FILE` | unset | ingestion, retrieval | Path of a `KEY=VALUE` overlay file for authentication settings; see [Config reload and SIGHUP](#config-reload-and-sighup). DASH only. |

## Network and transport

| Variable | Default | Read by | Description |
|---|---|---|---|
| `DASH_INGEST_BIND` | `127.0.0.1:8081` | ingestion | Listen address. |
| `DASH_RETRIEVAL_BIND` | `127.0.0.1:8080` | retrieval | Listen address. |
| `DASH_CONTROL_PLANE_BIND` | `127.0.0.1:8090` | control-plane | Listen address. |
| `DASH_INGEST_HTTP_WORKERS` | `min(CPU count, 32)`, or 4 if undetectable | ingestion | Worker threads. Must be > 0. |
| `DASH_RETRIEVAL_HTTP_WORKERS` | same as above | retrieval | Worker threads. Must be > 0. |
| `DASH_INGEST_HTTP_QUEUE_CAPACITY` | `workers * 64` | ingestion | Bounded accept queue. When full, the service answers 503. |
| `DASH_RETRIEVAL_HTTP_QUEUE_CAPACITY` | `workers * 64` | retrieval | Same, for retrieval. |
| `DASH_HTTP_REQUEST_TIMEOUT_MS` | `10000` | ingestion, retrieval | Whole-request deadline for reading one request (headers plus body). A slow client gets 408 and is dropped. DASH only. |

Fixed limits (compiled in, **not** configurable): request body cap 16 MiB (413), request line and each header line 8 KiB, header block 32 KiB, at most 100 headers (431), per-connection socket timeout 5 seconds, `Transfer-Encoding` is not supported (501). The control plane has its own limits (below).

### Control-plane HTTP server

| Variable | Default | Description |
|---|---|---|
| `DASH_CONTROL_PLANE_WORKERS` | `8` | Worker threads. |
| `DASH_CONTROL_PLANE_QUEUE_DEPTH` | `64` | Accepted-but-unserved connections buffered before new ones get 503. |
| `DASH_CONTROL_PLANE_MAX_BODY_BYTES` | `8388608` (8 MiB) | Maximum accepted `Content-Length`. The header block is capped at 16 KiB. |
| `DASH_CONTROL_PLANE_READ_TIMEOUT_MS` | `5000` | Timeout for any single read. |
| `DASH_CONTROL_PLANE_REQUEST_DEADLINE_MS` | `10000` | Total time allowed to receive one request. |
| `DASH_CONTROL_PLANE_WRITE_TIMEOUT_MS` | `5000` | Timeout for any single write. |

All six are DASH only and ignore values of 0 or unparseable values.

## Authentication and authorization

Ingestion variables use the `DASH_INGEST_` prefix; retrieval variables use `DASH_RETRIEVAL_`. Both services support the same set, listed once below with both full names. Credentials are separate per service: an ingestion key does not work on retrieval. Clients send an API key in the `x-api-key` header or as `Authorization: Bearer <key>`. The full decision order, role table and JWT/OIDC behavior are in the operations guide `docs/operations/auth.md`.

The service refuses to start (exit 2) when no authentication method is configured and `DASH_INSECURE_DEV_MODE` is not set. A configured but invalid setting (unknown role name, malformed scoped key, OIDC without issuer/audience, weak secret under strict mode) also stops startup.

| Variables (ingestion / retrieval) | Default | Description |
|---|---|---|
| `DASH_INGEST_API_KEY` / `DASH_RETRIEVAL_API_KEY` | unset | A single accepted API key (a "legacy" unscoped key). |
| `DASH_INGEST_API_KEYS` / `DASH_RETRIEVAL_API_KEYS` | unset | Comma-separated accepted API keys (legacy unscoped keys). |
| `DASH_INGEST_API_KEY_DEFAULT_ROLES` / `DASH_RETRIEVAL_API_KEY_DEFAULT_ROLES` | the service's primary role: `ingest` for ingestion, `retrieve` for retrieval | Roles granted to legacy unscoped keys and to scoped keys that list no roles. Comma or space separated list of `admin`, `ingest`, `retrieve`, `read_only`; an unknown name stops startup. |
| `DASH_INGEST_API_KEY_SCOPES` / `DASH_RETRIEVAL_API_KEY_SCOPES` | unset | Scoped keys, entries separated by `;`, each `key:tenantA,tenantB[:role1,role2]`. A tenant of `*` means all tenants. A scoped key need not also appear in `..._API_KEYS`. |
| `DASH_INGEST_ALLOWED_TENANTS` / `DASH_RETRIEVAL_ALLOWED_TENANTS` | any tenant | Comma-separated tenant allowlist applied to every authenticated request (403 otherwise); `*` or empty means any. |
| `DASH_INGEST_REVOKED_API_KEYS` / `DASH_RETRIEVAL_REVOKED_API_KEYS` | unset | Comma-separated keys rejected with 401 `API key revoked`. |
| `DASH_INGEST_REVOKED_KEYS_PATH` / `DASH_RETRIEVAL_REVOKED_KEYS_PATH` | unset | File with one revoked key per line. Re-read when its mtime or size changes (checked at most once per second). |
| `DASH_INGEST_RATE_LIMIT_PER_TENANT_RPS` / `DASH_RETRIEVAL_RATE_LIMIT_PER_TENANT_RPS` | `100` (ingestion), `500` (retrieval) | Per-tenant token-bucket refill rate. `0` disables limiting. Applies to API keys, JWTs and OIDC alike. Excess requests get HTTP 429 with a `Retry-After` header. Non-tenant requests (`/metrics`, `/debug/placement`, `/v1/embeddings`) share one bucket. |
| `DASH_INGEST_RATE_LIMIT_BURST` / `DASH_RETRIEVAL_RATE_LIMIT_BURST` | `200` (ingestion), `1000` (retrieval); never below the rate | Per-tenant bucket size. |

Rate-limit state is in memory per process and starts fresh after a restart or a SIGHUP reload.

### JWT (HS256) and OIDC

| Variables (ingestion / retrieval) | Default | Description |
|---|---|---|
| `DASH_INGEST_JWT_HS256_SECRET` / `DASH_RETRIEVAL_JWT_HS256_SECRET` | unset | Primary HS256 secret. HS256 validation is active when this, `..._SECRETS` or `..._SECRETS_BY_KID` is set. An empty key is never accepted. |
| `DASH_INGEST_JWT_HS256_SECRETS` / `DASH_RETRIEVAL_JWT_HS256_SECRETS` | unset | Comma-separated secrets accepted during rotation. |
| `DASH_INGEST_JWT_HS256_SECRETS_BY_KID` / `DASH_RETRIEVAL_JWT_HS256_SECRETS_BY_KID` | unset | `kid:secret;kid2:secret2`, selected by the token's `kid` header. |
| `DASH_INGEST_JWT_ISSUER` / `DASH_RETRIEVAL_JWT_ISSUER` | unset (HS256), **required** (OIDC) | Required `iss`. |
| `DASH_INGEST_JWT_AUDIENCE` / `DASH_RETRIEVAL_JWT_AUDIENCE` | unset (HS256), **required** (OIDC) | Required `aud`. |
| `DASH_INGEST_JWT_LEEWAY_SECS` / `DASH_RETRIEVAL_JWT_LEEWAY_SECS` | `0` | Clock skew allowance, capped at 60. |
| `DASH_INGEST_JWT_MAX_LIFETIME_SECS` / `DASH_RETRIEVAL_JWT_MAX_LIFETIME_SECS` | `86400` | Maximum `exp - iat` (or `exp - now` when `iat` is absent). Longer-lived tokens are rejected. |
| `DASH_INGEST_JWT_REQUIRE_EXP` / `DASH_RETRIEVAL_JWT_REQUIRE_EXP` | n/a | **Ignored.** `exp` is always required; setting it to a false value only logs a warning at startup. |
| `DASH_INGEST_JWT_ROLES_CLAIM` / `DASH_RETRIEVAL_JWT_ROLES_CLAIM` | `dash_roles` | Name of the claim carrying roles. The older name `DASH_INGEST_JWT_ROLE_CLAIM` / `DASH_RETRIEVAL_JWT_ROLE_CLAIM` is still read. |
| `DASH_INGEST_JWT_DEFAULT_ROLES` / `DASH_RETRIEVAL_JWT_DEFAULT_ROLES` | none | Roles for tokens that **lack** the role claim. With no default, a role-less token is authenticated but gets 403 on every role-checked route. |
| `DASH_INGEST_JWT_ALLOW_WILDCARD_TENANT` / `DASH_RETRIEVAL_JWT_ALLOW_WILDCARD_TENANT` | off | Honor a `"*"` tenant in a token. Otherwise `"*"` matches nothing. |
| `DASH_INGEST_JWT_REVOKED_JTIS` / `DASH_RETRIEVAL_JWT_REVOKED_JTIS` | unset | Comma-separated `jti` values rejected with 401 `JWT revoked`. |
| `DASH_INGEST_JWT_REVOKED_JTIS_PATH` / `DASH_RETRIEVAL_JWT_REVOKED_JTIS_PATH` | unset | File with one revoked `jti` per line, reloaded like the key revocation file. |
| `DASH_INGEST_JWT_PROVIDER` / `DASH_RETRIEVAL_JWT_PROVIDER` | `hs256` | `hs256` or `oidc`. Any other value stops startup. |
| `DASH_INGEST_JWT_JWKS_URL` / `DASH_RETRIEVAL_JWT_JWKS_URL` | unset | JWKS URL (required for `oidc`). Must be `https://`, or `http://` to a loopback host. |
| `DASH_INGEST_JWT_JWKS_REFRESH_MINUTES` / `DASH_RETRIEVAL_JWT_JWKS_REFRESH_MINUTES` | `15` | JWKS cache lifetime. |
| `DASH_INGEST_JWT_TENANT_CLAIMS` / `DASH_RETRIEVAL_JWT_TENANT_CLAIMS` | `tenant_id,tenants,tenant_ids` | Comma-separated claim names that carry the tenant list (OIDC mode). |
| `DASH_OIDC_ALLOW_INSECURE_JWKS` | off | Allow a plain `http://` JWKS URL on a non-loopback host. DASH only; do not use in production. |

HS256 tokens carry tenants in `tenant_id` (string) or `tenants` / `tenant_ids` (array). OIDC mode accepts only asymmetric algorithms (RS256/384/512, PS256/384/512, ES256/384, EdDSA) and requires a `kid`. The automated tests exercise RS256 signature verification, RS256-vs-RS384 key restriction and the JWKS cache behavior; the other listed algorithms are accepted by the allow-list but have no dedicated test. There are no RS256/ES256 PEM-key variables (`..._JWT_PUBLIC_KEY`, `..._JWT_ALGORITHM` do not exist).

### Metrics and replication/control-plane authentication

| Variable | Default | Read by | Description |
|---|---|---|---|
| `DASH_METRICS_PUBLIC` | off | ingestion, retrieval | Expose `/metrics` without credentials. Without it `/metrics` and `/debug/*` need a credential holding `read_only` or `admin`. DASH only. |
| `DASH_INGEST_REPLICATION_TOKEN` | unset | ingestion, retrieval (follower) | Shared token checked against the `x-replication-token` header on `/internal/replication/*`. Without it the replication endpoints answer 403 (open only in dev mode). An ingestion node configured as a follower (`DASH_INGEST_REPLICATION_SOURCE_URL`) refuses to start without it outside dev mode. With strict secrets it must be a non-placeholder of at least 16 characters. A retrieval follower also falls back to this variable when `DASH_RETRIEVAL_REPLICATION_TOKEN` is unset. |
| `DASH_RETRIEVAL_REPLICATION_TOKEN` | unset | retrieval | Token the retrieval follower sends to the source. Must equal the source's `DASH_INGEST_REPLICATION_TOKEN`. |
| `DASH_CONTROL_PLANE_TOKEN` | unset | control-plane; ingestion and retrieval (router client) | Bearer token required on every `/v1/control-plane/*` route except health and ready. The control plane refuses to start without it unless dev mode is on. The router client in ingestion and retrieval presents it when fetching placement. The control plane applies no minimum length, so use a random 32-byte value. |
| `DASH_ROUTER_CONTROL_PLANE_TOKEN` | falls back to `DASH_CONTROL_PLANE_TOKEN` | ingestion, retrieval | Token the router client sends to the control plane. DASH only. |

## Audit log

| Variable | Default | Read by | Description |
|---|---|---|---|
| `DASH_INGEST_AUDIT_LOG_PATH` | unset (audit off) | ingestion | Path of the SHA-256 hash-chained JSON-lines audit log. |
| `DASH_RETRIEVAL_AUDIT_LOG_PATH` | unset (audit off) | retrieval | Same, for retrieval. |
| `DASH_INGEST_AUDIT_FSYNC` | `1` | ingestion | `fdatasync` after every audit append. DASH only. |
| `DASH_RETRIEVAL_AUDIT_FSYNC` | `1` | retrieval | Same, for retrieval. DASH only. |
| `DASH_INGEST_AUDIT_FAIL_CLOSED` | `0` | ingestion | When on, a write to `/v1/ingest*` is refused with 503 if the audit log cannot be opened, locked and its tail recovered. The check is made before the mutation; the append itself still happens after it (see `docs/operations/audit-chain.md`). DASH only. |
| `DASH_RETRIEVAL_AUDIT_FAIL_CLOSED` | `0` | retrieval | Same gate for `/v1/retrieve`. DASH only. |

Verify a log with `tools/audit-verify` or `scripts/verify_audit_chain.sh`.

## Persistence and WAL

| Variable | Default | Read by | Description |
|---|---|---|---|
| `DASH_INGEST_WAL_PATH` | unset | ingestion | WAL file. When unset the ingestion service is **purely in-memory** (data is lost on restart). The compose file sets it. |
| `DASH_RETRIEVAL_WAL_PATH` | unset | retrieval | Optional local WAL; replayed at startup and, when a replication follower is configured, mirrored from the leader so a restart can resume. |
| `DASH_INGEST_PERSISTENCE_PATH` | `./data/dash-ingestion.redb` | ingestion | redb file. Used only when a WAL path is set. |
| `DASH_RETRIEVAL_PERSISTENCE_PATH` | `./data/dash-retrieval.redb` | retrieval | redb file. |
| `DASH_INGEST_PERSISTENCE_DISABLE` | off | ingestion | Set to exactly `1` to skip redb. If the redb file cannot be opened the service logs an error and continues in memory. |
| `DASH_RETRIEVAL_PERSISTENCE_DISABLE` | off | retrieval | Same, for retrieval. |
| `DASH_INGEST_WAL_SYNC_EVERY_RECORDS` | `1` | ingestion | fsync after this many records (values < 1 become 1). |
| `DASH_INGEST_WAL_APPEND_BUFFER_RECORDS` | `1` | ingestion | Buffered records before write (values < 1 become 1). |
| `DASH_INGEST_WAL_SYNC_INTERVAL_MS` | unset | ingestion | Time-based fsync interval (values <= 0 are ignored). |
| `DASH_INGEST_WAL_BACKGROUND_FLUSH_ONLY` | `false` | ingestion | Flush only from the background flusher. An unparseable value exits with code 2. |
| `DASH_INGEST_WAL_ASYNC_FLUSH_INTERVAL_MS` | auto (250 ms when batching is enabled, otherwise off) | ingestion | Positive integer, `auto`, or `off`/`none`/`false`/`disabled`/`0`. An empty value disables it. Anything else exits with code 2. |
| `DASH_INGEST_ALLOW_UNSAFE_WAL_DURABILITY` | `false` | ingestion | Required to run with durability settings that exceed the safe limits: sync-every or append buffer above 256 records, sync interval or async flush interval above 5000 ms, batched writes without a sync interval, or background-only flushing without an async worker. Without it the service exits with code 2. |
| `DASH_CHECKPOINT_MAX_WAL_RECORDS` | unset | ingestion | Trigger a checkpoint after this many WAL records. |
| `DASH_CHECKPOINT_MAX_WAL_BYTES` | unset | ingestion | Trigger a checkpoint after this many WAL bytes. |
| `DASH_INGEST_BATCH_MAX_ITEMS` | `128` | ingestion | Maximum items in `POST /v1/ingest/batch`. |
| `DASH_WAL_REPLAY_STRICT` | off | ingestion, retrieval | When on, WAL replay fails on the first record that lenient mode would quarantine, instead of quarantining it and continuing. See `docs/operations/wal-recovery.md`. DASH only. |

## Replication follower

Both followers pull WAL frames from an ingestion node, persist `(generation, offset)` together and resync from a full export when the leader's WAL generation changes. Retrieval's offset is saved next to its WAL as `<DASH_RETRIEVAL_WAL_PATH>.replication` when a retrieval WAL is configured; without a retrieval WAL nothing replicated survives a restart and the follower always starts with a full resync.

| Variable | Default | Description |
|---|---|---|
| `DASH_RETRIEVAL_REPLICATION_SOURCE_URL` | unset (follower off) | Base URL of the ingestion service, for example `http://ingestion:8081`. |
| `DASH_RETRIEVAL_REPLICATION_POLL_INTERVAL_MS` | `1000` | Poll interval. |
| `DASH_RETRIEVAL_REPLICATION_MAX_RECORDS` | `512` | Records per pull (the leader caps a pull at 10000). |
| `DASH_RETRIEVAL_REPLICATION_MAX_RESPONSE_BYTES` | `67108864` (64 MiB) | Upper bound for one response body. |
| `DASH_RETRIEVAL_REPLICATION_MAX_BACKOFF_MS` | `30000` | Upper bound for the failure backoff (the poll interval doubles per consecutive failure). |
| `DASH_RETRIEVAL_REPLICATION_MAX_LAG_RECORDS` | `100000` | `/ready` fails with `replication_lag_exceeded` when the leader is further ahead than this. |
| `DASH_RETRIEVAL_REPLICATION_MAX_STALENESS_MS` | `300000` | `/ready` fails with `replication_stale` when the last successful poll is older than this. `/ready` also fails with `replication_initial_sync_pending` until the first sync completes. |
| `DASH_RETRIEVAL_REPLICATION_OFFSET_PATH` | `<retrieval WAL path>.replication` if a retrieval WAL is set, otherwise none | File storing the last applied generation and offset. |
| `DASH_INGEST_REPLICATION_SOURCE_URL` | unset (off) | An ingestion node can itself follow another ingestion node. Requires `DASH_INGEST_REPLICATION_TOKEN`. |
| `DASH_INGEST_REPLICATION_POLL_INTERVAL_MS` | `500` | Poll interval for ingestion-to-ingestion pulls. |
| `DASH_INGEST_REPLICATION_MAX_RECORDS` | `512` | Records per pull. |
| `DASH_INGEST_REPLICATION_MAX_RESPONSE_BYTES` | `67108864` (64 MiB) | Upper bound for one response body. |
| `DASH_INGEST_REPLICATION_MAX_BACKOFF_MS` | `30000` | Upper bound for the failure backoff. |
| `DASH_INGEST_REPLICATION_MAX_LAG_RECORDS` | `100000` | Readiness lag threshold. |
| `DASH_INGEST_REPLICATION_MAX_STALENESS_MS` | `300000` | Readiness staleness threshold. |
| `DASH_INGEST_REPLICATION_OFFSET_PATH` | derived from the ingestion WAL path (`<wal>.replication`) | File storing the last applied generation and offset. |

## Placement and routing

Placement routing is enabled when `DASH_ROUTER_PLACEMENT_FILE` or `DASH_ROUTER_CONTROL_PLANE_URL` is set. Both services then require a local node id: ingestion exits (code 2) and retrieval fails to start (exit 1) if it is missing or if the placement source cannot be loaded.

| Variable | Default | Read by | Description |
|---|---|---|---|
| `DASH_ROUTER_PLACEMENT_FILE` | unset | ingestion, retrieval, control-plane | CSV of shard placements. The control plane uses it as its initial state. |
| `DASH_ROUTER_CONTROL_PLANE_URL` | unset | ingestion, retrieval | Fetch placement from the control plane. If it is configured and unreachable, the placement file is **not** used as a fallback unless `DASH_ROUTER_ALLOW_STALE_PLACEMENT` is set. |
| `DASH_ROUTER_ALLOW_STALE_PLACEMENT` | off | ingestion, retrieval | `1` or `true`: when the control plane is configured but unreachable, fall back to `DASH_ROUTER_PLACEMENT_FILE`. Off by default because a stale file can name a deposed leader (split-brain writes). DASH only. |
| `DASH_ROUTER_CONTROL_PLANE_CONNECT_TIMEOUT_MS` | `2000` | ingestion, retrieval | Connect timeout of the control-plane client. DASH only. |
| `DASH_ROUTER_CONTROL_PLANE_READ_TIMEOUT_MS` | `5000` | ingestion, retrieval | Read timeout of the control-plane client. DASH only. |
| `DASH_ROUTER_CONTROL_PLANE_WRITE_TIMEOUT_MS` | `5000` | ingestion, retrieval | Write timeout of the control-plane client. DASH only. |
| `DASH_ROUTER_LOCAL_NODE_ID` | unset | ingestion, retrieval | This node's id. Falls back to `DASH_NODE_ID`. An ingestion follower also uses it as its replica id in acknowledgements. |
| `DASH_NODE_ID` | unset | ingestion, retrieval | Fallback for the local node id. |
| `DASH_ROUTER_READ_PREFERENCE` | `any_healthy` | retrieval | Replica read preference: `any_healthy`, `leader_only` or `prefer_follower`. Any other value is a configuration error. |
| `DASH_ROUTER_PLACEMENT_RELOAD_INTERVAL_MS` | off | ingestion, retrieval | Reload placement on this interval (values <= 0 disable). |
| `DASH_INGEST_PLACEMENT_STALE_GRACE_MS` | `30000` | ingestion | With placement reload enabled: how long writes keep being accepted on the last known placement after reloads start failing. After the grace, writes are refused with 503 `placement is stale` until a reload succeeds. DASH only. |
| `DASH_ROUTER_SHARD_IDS` | from placement | ingestion, retrieval | Comma-separated shard ids override. |
| `DASH_ROUTER_REPLICA_COUNT` | from placement | ingestion, retrieval | Replica count override. |
| `DASH_ROUTER_VIRTUAL_NODES_PER_SHARD` | `64` | ingestion, retrieval | Virtual nodes for shard hashing. |
| `DASH_CONTROL_PLANE_NODE_ID` | `control-plane-<pid>` | control-plane | Node id used in leader election. |
| `DASH_CONTROL_PLANE_STATE_PATH` | unset (state not persisted) | control-plane | Persisted placement CSV. |
| `DASH_CONTROL_PLANE_STATE_SHA256_PATH` | unset | control-plane | Checksum file for the persisted state; verified at startup (mismatch exits with code 2). |
| `DASH_CONTROL_PLANE_LEASE_PATH` | unset (standalone, always leader) | control-plane | File lease for leader election. |
| `DASH_CONTROL_PLANE_LEASE_DURATION_MS` | `30000` | control-plane | Lease length. |
| `DASH_CONTROL_PLANE_LEASE_RENEWAL_MS` | `10000` | control-plane | Renewal interval. |
| `DASH_CONTROL_PLANE_LEASE_SAFETY_MARGIN_MS` | `1000` | control-plane | Clock-skew margin: a leader stops reporting leadership this long before the lease expires, and other nodes wait this long after expiry before taking over (capped at half the lease duration). |

## ANN tuning

Each variable can be set per service (`DASH_INGEST_ANN_*`, `DASH_RETRIEVAL_ANN_*`) or shared (`DASH_ANN_*`); the per-service name wins, then the shared name, then the `EME_` forms. Values must be positive integers. The index is an in-repo HNSW-style graph, not `usearch`.

| Variables | Default | Description |
|---|---|---|
| `DASH_INGEST_ANN_MAX_NEIGHBORS_BASE`, `DASH_RETRIEVAL_ANN_MAX_NEIGHBORS_BASE`, `DASH_ANN_MAX_NEIGHBORS_BASE` | `12` | Max neighbors on the base layer. |
| `DASH_INGEST_ANN_MAX_NEIGHBORS_UPPER`, `DASH_RETRIEVAL_ANN_MAX_NEIGHBORS_UPPER`, `DASH_ANN_MAX_NEIGHBORS_UPPER` | `6` | Max neighbors on upper layers. |
| `DASH_INGEST_ANN_SEARCH_EXPANSION_FACTOR`, `DASH_RETRIEVAL_ANN_SEARCH_EXPANSION_FACTOR`, `DASH_ANN_SEARCH_EXPANSION_FACTOR` | `16` | Candidate expansion multiplier. |
| `DASH_INGEST_ANN_SEARCH_EXPANSION_MIN`, `DASH_RETRIEVAL_ANN_SEARCH_EXPANSION_MIN`, `DASH_ANN_SEARCH_EXPANSION_MIN` | `64` | Minimum candidates examined. |
| `DASH_INGEST_ANN_SEARCH_EXPANSION_MAX`, `DASH_RETRIEVAL_ANN_SEARCH_EXPANSION_MAX`, `DASH_ANN_SEARCH_EXPANSION_MAX` | `4096` | Maximum candidates examined. |

There is no `DASH_ANN_M`, `DASH_ANN_EF_CONSTRUCTION`, `DASH_ANN_EF_SEARCH` or `DASH_ANN_REBUILD_THRESHOLD`.

## Retrieval tuning and request bounds

| Variable | Default | Description |
|---|---|---|
| `DASH_RETRIEVAL_MAX_TOP_K` | `1000` | Upper bound for `top_k` on `/v1/retrieve`; larger values get 400. DASH only. |
| `DASH_RETRIEVAL_GRAPH_MAX_HOPS` | `3` | Graph expansion depth (must be > 0). |
| `DASH_RETRIEVAL_GRAPH_EDGE_DEPTH_DECAY` | `0.75` | Per-hop weight decay, clamped to 0..1. |
| `DASH_RETRIEVAL_GRAPH_SUPPORT_PATH_BONUS` | `0.16` | Score bonus per support path (>= 0). |
| `DASH_RETRIEVAL_GRAPH_CONTRADICTION_DEPTH_PENALTY` | `0.20` | Score penalty for contradiction chains (>= 0). |
| `DASH_RETRIEVAL_SEGMENT_DIR` | unset | Directory of published index segments to prefilter from. |
| `DASH_RETRIEVAL_SEGMENT_CACHE_REFRESH_MS` | `1000` | Segment cache refresh interval (must be > 0). |
| `DASH_RETRIEVAL_DISK_NATIVE_SEGMENT_EXECUTION` | `true` | Execute against disk segments natively. |
| `DASH_RETRIEVAL_STORAGE_DIVERGENCE_WARN_DELTA_COUNT` | `1000` | Divergence warning threshold (records) in `/debug/storage-visibility`. |
| `DASH_RETRIEVAL_STORAGE_DIVERGENCE_WARN_RATIO` | `0.25` | Divergence warning threshold (ratio). |
| `DASH_VECTOR_BACKEND` | auto | `cpu`, `gpu` or unset. There is no GPU implementation: `gpu` is reported as `cpu (gpu-feature-disabled)` or, with the `gpu-backend` build feature, `cpu (gpu-unavailable)`; scoring always runs on CPU. DASH only. |

Fixed retrieve bounds (not configurable): `query` at most 8 KiB, at most 256 values in each of `entity_filters` and `embedding_id_filters`, `query_embedding` at most 8192 values.

## Segments and maintenance

Read by the ingestion service (segment publishing) and the `segment-maintenance-daemon` binary.

| Variable | Default | Description |
|---|---|---|
| `DASH_INGEST_SEGMENT_DIR` | unset (segment publishing off) | Segment root directory. |
| `DASH_INGEST_SEGMENT_MAX_SEGMENT_SIZE` (alias `DASH_SEGMENT_MAX_SEGMENT_SIZE`) | `10000` | Max claims per segment. |
| `DASH_INGEST_SEGMENT_MAX_SEGMENTS_PER_TIER` (alias `DASH_SEGMENT_MAX_SEGMENTS_PER_TIER`) | `8` | Segments per tier before compaction. |
| `DASH_INGEST_SEGMENT_MAX_COMPACTION_INPUT_SEGMENTS` (alias `DASH_SEGMENT_MAX_COMPACTION_INPUT_SEGMENTS`) | `4` | Max segments merged at once (must be > 1). |
| `DASH_INGEST_SEGMENT_MAINTENANCE_INTERVAL_MS` | `30000` | Maintenance loop interval; `0` disables in-process maintenance. |
| `DASH_INGEST_SEGMENT_GC_MIN_STALE_AGE_MS` | `60000` | Minimum age before an unreferenced segment file is deleted. |
| `DASH_INGEST_SEGMENT_MAINTENANCE_STRICT` | off | `segment-maintenance-daemon` only: with `--once`, exit non-zero if any tenant failed (the pass still visits every tenant). |

Tenant directories under the segment root are named by an injective escaping of the tenant id (bytes outside `a-z0-9-` become `_xx`; ids over 96 characters get a hash suffix). Existing directories created by the older lossy sanitizer are renamed automatically the first time a tenant is touched.

## Embedding providers

| Variable | Default | Read by | Description |
|---|---|---|---|
| `DASH_EMBEDDING_PROVIDER` | `hash` | retrieval (`/v1/embeddings`, query embedding), ingestion (claim embedding) | `hash`, `ollama` or `openai`. Unknown values fall back to `hash` with a warning on stderr. |
| `DASH_OLLAMA_ENDPOINT` | `http://localhost:11434` | embeddings library | Ollama endpoint; a bare base URL is expanded to `/api/embed`. |
| `DASH_OLLAMA_BASE_URL` | unset | embeddings library | Deprecated alias of `DASH_OLLAMA_ENDPOINT`, read when that is unset; logs a one-time deprecation warning. |
| `DASH_OLLAMA_MODEL` | `nomic-embed-text` | embeddings library | Ollama model. |
| `DASH_OPENAI_API_KEY` | unset | embeddings library | Required for `openai`; without it the provider cannot be built and the service falls back to `hash` (logged on stderr). |
| `DASH_OPENAI_MODEL` | `text-embedding-3-small` | embeddings library | OpenAI model. The endpoint is fixed at `https://api.openai.com/v1/embeddings`. |
| `DASH_EMBEDDING_ALLOW_INSECURE_HTTP` | off | embeddings library, ingestion | Set to exactly `1` to allow sending a bearer API key over plaintext `http://` to a non-loopback host. Off by default: the client refuses that. |
| `DASH_EMBEDDING_ALLOW_TOKEN_IDS` | off | retrieval | `1`, `true` or `yes`: accept token-id array `input` on `/v1/embeddings`, embedding the decimal ids joined by spaces. Off by default: token-id inputs are rejected with 400 `unsupported_input_type`, because ids cannot be decoded without the client's tokenizer. |
| `DASH_EMBEDDING_MAX_TOTAL_CHARS` | `524288` | retrieval | Maximum total characters across all inputs of one `/v1/embeddings` request. |

The provider clients use HTTPS (rustls) where the URL is `https://`, do not follow redirects, cap response bodies, retry with jittered backoff and sit behind a circuit breaker whose half-open state admits a single probe. The hash provider's default dimension is 384. Timeouts are compiled in (Ollama 5 s, OpenAI 30 s). There is no `DASH_EMBEDDING_MODEL`, `DASH_EMBEDDING_DIM`, `DASH_OPENAI_BASE_URL`, `DASH_OPENAI_TIMEOUT_MS` or `DASH_OLLAMA_TIMEOUT_MS`.

### Ingestion extraction and parsing

These control `POST /v1/ingest/raw` and `/v1/ingest/document` (see `GET /debug/document-parser`).

| Variable | Default | Description |
|---|---|---|
| `DASH_INGEST_RAW_EXTRACTION_PROVIDER` | `rule_sentence` | `rule_sentence` or `adapter_command`. The adapter needs the `model-extraction-adapter` cargo feature of `ingestion`, which is off by default; without it requests fail. |
| `DASH_INGEST_RAW_ADAPTER_CMD` | unset | Command (run with `sh -c`) used when the provider is `adapter_command`. |
| `DASH_INGEST_DOCUMENT_PARSER_PROVIDER` | `builtin_utf8` | `builtin_utf8` or `adapter_command`. |
| `DASH_INGEST_DOCUMENT_ADAPTER_CMD` | unset | Command used by the document parser adapter; receives the media type in `DASH_DOCUMENT_MIME_TYPE`. |
| `DASH_INGEST_EMBEDDING_PROVIDER` | `hash_vector` | Provider for embeddings generated during raw/document ingest: `hash_vector`, `off`, or `adapter_command`. |
| `DASH_INGEST_EMBEDDING_ADAPTER_CMD` | unset | Command used when the provider is `adapter_command`. |
| `DASH_INGEST_EMBEDDING_DIMENSIONS` | `64` | Hash vector dimensions, clamped to 8..4096. |

Adapter commands are operator-controlled shell command lines; anyone who can set the service environment can run code as the service user.

## Config reload and SIGHUP

On unix, `kill -HUP <pid>` makes `ingestion` and `retrieval` rebuild their whole authentication policy from the process environment, overlaid with the file named by `DASH_CONFIG_RELOAD_FILE`. The overlay holds `KEY=VALUE` lines (blank lines and `#` comments ignored, optional quotes stripped), only `DASH_*` / `EME_*` keys are read, and an overlay value wins over the process environment. The file is also read at startup. A new policy that fails validation is rejected and the previous one stays in force. Only authentication settings are reloaded. Details: `docs/operations/auth.md`.

## Encryption (library only)

`DASH_ENCRYPTION_PROVIDER` (`none` or `env`) and `DASH_ENCRYPTION_MASTER_KEY` (64 hex characters or base64 of 32 bytes) are read by `pkg/encryption::provider_from_env` (also under the `EME_` names). **No service calls it**, so setting them has no effect on stored data. Encryption at rest is planned (P4).

## Benchmarks and load tests

`DASH_BENCH_*` (benchmark thresholds and fixtures in `tests/benchmarks`), `DASH_LIVE_URL` and `DASH_API_KEY` (load-test binary) are read only by the benchmark binaries. See the comments at the top of `tests/benchmarks/src/main.rs` and `tests/benchmarks/src/bin/load_test.rs`.

## Container-only variables

`DASH_BIN` (which binary the image entrypoint runs: `ingestion`, `retrieval`, `control-plane` or `segment-maintenance-daemon`; default `retrieval`), `DASH_HOME` (default `/opt/dash`) and `DASH_HEALTHCHECK_URL` are read by the shell scripts in `deploy/container/scripts/`, not by the Rust services. `DASH_PUBLISH_ADDR` (default `127.0.0.1`) is read by the compose file for published ports. The compose file may also set `DASH_INGEST_TRANSPORT_RUNTIME` and `DASH_RETRIEVAL_TRANSPORT_RUNTIME`; no code reads them.

## Variables that do not exist

Earlier versions of this page documented the following. No code reads them; setting them does nothing.

`DASH_LOG_LEVEL` (use `RUST_LOG`), `DASH_EMBEDDING_MODEL`, `DASH_EMBEDDING_DIM`, `DASH_OPENAI_BASE_URL`, `DASH_OPENAI_TIMEOUT_MS`, `DASH_OLLAMA_TIMEOUT_MS`, `DASH_INGEST_WAL_SEGMENT_BYTES`, `DASH_INGEST_WAL_FSYNC_POLICY`, `DASH_INGEST_CHECKPOINT_ON_SIGHUP`, `DASH_INGEST_JWT_PUBLIC_KEY`, `DASH_INGEST_JWT_PUBLIC_KEY_PATH`, `DASH_INGEST_JWT_ALGORITHM`, `DASH_RETRIEVAL_JWT_PUBLIC_KEY`, `DASH_RETRIEVAL_JWT_PUBLIC_KEY_PATH`, `DASH_RETRIEVAL_JWT_ALGORITHM`, `DASH_API_KEY_OVERLAP_SECONDS`, `DASH_INGEST_PORT`, `DASH_RETRIEVAL_PORT`, `DASH_INGEST_WORKERS`, `DASH_RETRIEVAL_WORKERS`, `DASH_TCP_READ_TIMEOUT_MS`, `DASH_MAX_BODY_BYTES`, `DASH_RETRIEVAL_RATE_LIMIT_RPS`, `DASH_INGEST_RATE_LIMIT_RPS` (the real names are `*_RATE_LIMIT_PER_TENANT_RPS`), `DASH_ANN_M`, `DASH_ANN_EF_CONSTRUCTION`, `DASH_ANN_EF_SEARCH`, `DASH_ANN_REBUILD_THRESHOLD`, `DASH_AUDIT_LOG_PATH` (use `DASH_INGEST_AUDIT_LOG_PATH` / `DASH_RETRIEVAL_AUDIT_LOG_PATH`), `DASH_AUDIT_RETENTION_DAYS`, `DASH_AUDIT_COMPACT_AT_RATIO`, `DASH_IDEMPOTENCY_RETENTION_DAYS`, `DASH_METRICS_ENABLED`, `DASH_METRICS_NAMESPACE`.

## Example

A local development retrieval + ingestion pair, using real variable names (generate real secrets with `scripts/generate-secrets.sh`):

```bash
export DASH_INGEST_API_KEY=$(openssl rand -hex 32)       # 64 hex chars
export DASH_RETRIEVAL_API_KEY=$(openssl rand -hex 32)
export DASH_INGEST_REPLICATION_TOKEN=$(openssl rand -hex 32)
export DASH_RETRIEVAL_REPLICATION_TOKEN=$DASH_INGEST_REPLICATION_TOKEN

DASH_INGEST_BIND=127.0.0.1:8081 \
DASH_INGEST_WAL_PATH=./data/ingest.wal \
  cargo run --release -p ingestion

DASH_RETRIEVAL_BIND=127.0.0.1:8080 \
DASH_RETRIEVAL_REPLICATION_SOURCE_URL=http://127.0.0.1:8081 \
DASH_RETRIEVAL_REPLICATION_OFFSET_PATH=./data/retrieval.offset \
  cargo run --release -p retrieval
```

(Run each command in its own terminal. The services serve by default; `--cli` runs a one-shot smoke path instead.)

For the full deployment story, see [Deploy](../operations/deploy.md).
