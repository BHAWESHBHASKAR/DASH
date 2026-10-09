# Configuration

<!-- This page is generated from the settings registry in pkg/config (`dash-config docs`). Do not edit it by hand: change pkg/config/src/registry.rs (or the prose in pkg/config/src/docs.rs) and regenerate. -->

DASH is configured through environment variables. Any setting can also be given in an optional TOML file named by `DASH_CONFIG_FILE`; the environment always wins over the file (see [Configuration file](../operations/configuration-file.md)). Another file input is the [auth overlay](#config-reload-and-sighup) used for SIGHUP reload. This page lists **only variables that the Rust code reads**. It is generated from the settings registry (`pkg/config`), which a test keeps in step with the sources in `services/`, `pkg/` and `tools/`. If this page and the code disagree, the code wins.

Two checks keep the list complete: the test `registry_covers_every_env_var_read_by_code` fails when the code reads a variable the registry does not know (or the registry lists one no code reads), and `scripts/check_config_docs.sh` fails when this page differs from what the registry generates. To change this page, edit the registry and run `cargo run -p dash-config -- docs`. See [CONTRIBUTING](../about/contributing.md).

## Conventions

- **Legacy `EME_` aliases.** Most `DASH_*` service variables are also read under the legacy name `EME_*` (for example `EME_INGEST_BIND`) when the `DASH_*` name is unset. The `EME_` prefix is deprecated: a service logs a warning at startup for every `EME_*` variable that is set, and the fallbacks will be dropped after one deprecation release. New deployments should use `DASH_*`. Variables with **no** `EME_` alias are marked "DASH only" in the tables below; in general these are the ones added in 0.3.0 plus the embedding, Ollama, OpenAI, vector-backend, strict-mode and logging variables.
- **Booleans** accept `1`, `true`, `yes`, `on` (true) and `0`, `false`, `no`, `off` (false), case-insensitive, unless a row says otherwise. A few legacy switches enable only on the literal `1` (or `1`/`true`/`yes`); those rows say so, and startup validation warns when another truthy spelling is used for them.
- **Startup validation.** Every service validates its environment before it starts (see [Configuration file](../operations/configuration-file.md)). A malformed value of a typed setting (a non-number where a number is expected, a value out of range, an unknown word for an enumerated setting, an unparsable boolean, a blank value where blank is meaningless) is an error: the service prints every error and exits with code 2. `DASH_CONFIG_VALIDATION=warn` downgrades errors to warnings. Unknown `DASH_*` / `EME_*` variables are reported as warnings with a did-you-mean suggestion. The readers themselves still fall back to the default silently on an unparseable number; validation is what makes that visible.
- **Bind addresses** are full `host:port` strings, not a bare port. There is no `DASH_INGEST_PORT` or `DASH_RETRIEVAL_PORT`.
- **Exit codes.** A service that refuses to start because of invalid configuration exits with code 2 (auth, secrets, WAL durability guardrails, placement routing in ingestion, startup validation) or 1 (failed to open or replay the WAL, retrieval placement configuration, listener failure).
- Variables are read at process start, with these exceptions: the authentication settings can be reloaded with SIGHUP (see below); the revoked-key and revoked-`jti` files are re-read when their modification time or size changes (checked at most once per second); the embedding provider settings (`DASH_EMBEDDING_PROVIDER`, `DASH_OLLAMA_*`, `DASH_OPENAI_*`) and the audit-log path variables are looked up per request, but treat a restart as the supported way to change them.

## Defaults at a glance

| Service | Default bind | Role |
|---|---|---|
| ingestion | `127.0.0.1:8081` | write API, WAL owner, replication source |
| retrieval | `127.0.0.1:8080` | read API, embeddings endpoint, replication follower |
| control-plane | `127.0.0.1:8090` | placement and leader state |

The container image and compose file override the bind addresses to `0.0.0.0:<port>` (and the compose file publishes the host ports on `127.0.0.1`).

## Common settings

Settings shared by the services (or built per service from the `DASH_INGEST_` / `DASH_RETRIEVAL_` prefixes). The **Notes** column names the services that read a setting when that is not both ingestion and retrieval.

### Startup, secrets and logging

| Variable | Default | Type | Description | Notes |
|---|---|---|---|---|
| `DASH_INSECURE_DEV_MODE` | `off` | bool | Allows a service to start with **no credentials configured**, and with them accept every request. Also forces a loopback bind (see `DASH_INSECURE_DEV_MODE_ALLOW_NON_LOOPBACK`) and allows an unset replication token. The control plane accepts only the literal `1` and, with no token configured, binds loopback only. Never set it in production. | DASH only. Read by ingestion, retrieval, control-plane. |
| `DASH_INSECURE_DEV_MODE_ALLOW_NON_LOOPBACK` | `off` | bool | With dev mode on, keep the requested non-loopback bind host instead of replacing it with `127.0.0.1`. | DASH only. |
| `DASH_STRICT_SECRETS` | **on** | bool | Strict secret validation is on by default. It can only be turned off by setting `DASH_STRICT_SECRETS=0` **and** `DASH_INSECURE_DEV_MODE=1`. When on, the service exits (code 2) if a configured secret is empty, a placeholder (contains `change-me`, `changeme`, `placeholder`, `replace-me`, `example`, `sample`, `<...>` markers, or starts with `secret`/`password`) or too short. Minimum lengths: **16** characters for API keys, scoped keys and replication tokens; **32** for HS256 JWT secrets and the control-plane token. Error messages name the setting, never the value. | DASH only. Read by ingestion, retrieval, control-plane. |
| `DASH_LOG_FORMAT` | compact text | string | `json` switches log output to JSON lines. Any other value uses the compact text format. | DASH only. |
| `RUST_LOG` | `info` | string | Standard `tracing` env filter. |  |
| `DASH_CONFIG_RELOAD_FILE` | unset | path | Path of a `KEY=VALUE` overlay file for authentication settings; see [Config reload and SIGHUP](#config-reload-and-sighup). | DASH only. |
| `DASH_CONFIG_FILE` | unset | path | Path of a TOML file whose values fill settings that are not set in the environment (environment wins). See [Configuration file](../operations/configuration-file.md). | DASH only. Read by ingestion, retrieval, control-plane. |
| `DASH_CONFIG_VALIDATION` | `error` | `error` \| `warn` | Startup validation mode. With `error` a malformed value or a bad configuration file stops the service with exit code 2; `warn` downgrades those errors to warnings and starts anyway. See [Configuration file](../operations/configuration-file.md). | DASH only. Read by ingestion, retrieval, control-plane. |

### Network and transport

| Variable | Default | Type | Description | Notes |
|---|---|---|---|---|
| `DASH_HTTP_REQUEST_TIMEOUT_MS` | `10000` | milliseconds >= 1 | Whole-request deadline for reading one request (headers plus body), measured from accept (queue wait counts). A slow client gets 408 and is dropped; connections that already waited it out in the queue are closed without work. | DASH only. |
| `DASH_HTTP_FIRST_BYTE_TIMEOUT_MS` | `2000` | milliseconds >= 1 | A new connection that sends nothing within this time is closed before it reaches a worker. | DASH only. |
| `DASH_HTTP_MAX_CONNS_PER_IP` | `64` | integer | Concurrent connections allowed per client IP; excess connections get 503. `0` disables the cap. Raise it (or set `0`) behind a load balancer that presents a single source IP. | DASH only. |

`/health`, `/live`, `/ready`, their `/v1/` forms and `/metrics` are served by two reserved workers, so slow requests cannot starve probes. Read-stage failures (400/408/413/417/431/501/505) are counted in `dash_*_transport_read_error_total{status_class}`.

Fixed limits (compiled in, **not** configurable): request body cap 16 MiB (413), request line and each header line 8 KiB, header block 32 KiB, at most 100 headers (431), per-connection socket timeout 5 seconds, `Transfer-Encoding` is not supported (501). The control plane has its own limits (see the control-plane settings below).

### Authentication and authorization

Ingestion variables use the `DASH_INGEST_` prefix; retrieval variables use `DASH_RETRIEVAL_`. Both services support the same set, listed once below with both full names. Credentials are separate per service: an ingestion key does not work on retrieval. Clients send an API key in the `x-api-key` header or as `Authorization: Bearer <key>`. The full decision order, role table and JWT/OIDC behavior are in the operations guide `docs/operations/auth.md`.

The service refuses to start (exit 2) when no authentication method is configured and `DASH_INSECURE_DEV_MODE` is not set. A configured but invalid setting (unknown role name, malformed scoped key, OIDC without issuer/audience, weak secret under strict mode) also stops startup.

| Variable | Default | Type | Description | Notes |
|---|---|---|---|---|
| `DASH_INGEST_API_KEY` / `DASH_RETRIEVAL_API_KEY` | unset | secret | A single accepted API key (a "legacy" unscoped key). |  |
| `DASH_INGEST_API_KEYS` / `DASH_RETRIEVAL_API_KEYS` | unset | secret list (`,`) | Comma-separated accepted API keys (legacy unscoped keys). |  |
| `DASH_INGEST_API_KEY_DEFAULT_ROLES` / `DASH_RETRIEVAL_API_KEY_DEFAULT_ROLES` | `ingest` (ingestion), `retrieve` (retrieval) | list (`,`) | Roles granted to legacy unscoped keys and to scoped keys that list no roles. Comma or space separated list of `admin`, `ingest`, `retrieve`, `read_only`; an unknown name stops startup. | Default is the service's primary role. |
| `DASH_INGEST_API_KEY_SCOPES` / `DASH_RETRIEVAL_API_KEY_SCOPES` | unset | secret list (`;`) | Scoped keys, entries separated by `;`, each `key:tenantA,tenantB[:role1,role2]`. A tenant of `*` means all tenants. A scoped key need not also appear in `..._API_KEYS`. |  |
| `DASH_INGEST_ALLOWED_TENANTS` / `DASH_RETRIEVAL_ALLOWED_TENANTS` | any tenant | list (`,`) | Comma-separated tenant allowlist applied to every authenticated request (403 otherwise); `*` means any. Leave it unset for any tenant: a value that is set but empty or only separators is a startup error. |  |
| `DASH_INGEST_REVOKED_API_KEYS` / `DASH_RETRIEVAL_REVOKED_API_KEYS` | unset | secret list (`,`) | Comma-separated keys rejected with 401 `API key revoked`. |  |
| `DASH_INGEST_REVOKED_KEYS_PATH` / `DASH_RETRIEVAL_REVOKED_KEYS_PATH` | unset | path | File with one revoked key per line. Re-read when its mtime or size changes (checked at most once per second). |  |
| `DASH_INGEST_RATE_LIMIT_PER_TENANT_RPS` / `DASH_RETRIEVAL_RATE_LIMIT_PER_TENANT_RPS` | `100` (ingestion), `500` (retrieval) | integer | Token-bucket refill rate per credential and route class (data, embeddings, ops); the tenant joins the key only for keys bound to a fixed tenant set, so a wildcard key cannot gain buckets by rotating tenant ids. `0` disables limiting. Applies to API keys, JWTs and OIDC alike. Excess requests get HTTP 429 with a `Retry-After` header. At most 50,000 buckets are kept. |  |
| `DASH_INGEST_RATE_LIMIT_BURST` / `DASH_RETRIEVAL_RATE_LIMIT_BURST` | `200` (ingestion), `1000` (retrieval) | integer | Bucket size. Never below the rate. |  |

Rate-limit state is in memory per process and starts fresh after a restart or a SIGHUP reload.

### JWT (HS256) and OIDC

| Variable | Default | Type | Description | Notes |
|---|---|---|---|---|
| `DASH_INGEST_JWT_HS256_SECRET` / `DASH_RETRIEVAL_JWT_HS256_SECRET` | unset | secret | Primary HS256 secret. HS256 validation is active when this, `..._SECRETS` or `..._SECRETS_BY_KID` is set. An empty key is never accepted. |  |
| `DASH_INGEST_JWT_HS256_SECRETS` / `DASH_RETRIEVAL_JWT_HS256_SECRETS` | unset | secret list (`,`) | Comma-separated secrets accepted during rotation. |  |
| `DASH_INGEST_JWT_HS256_SECRETS_BY_KID` / `DASH_RETRIEVAL_JWT_HS256_SECRETS_BY_KID` | unset | secret list (`;`) | `kid:secret;kid2:secret2`, selected by the token's `kid` header. |  |
| `DASH_INGEST_JWT_ISSUER` / `DASH_RETRIEVAL_JWT_ISSUER` | unset (HS256), **required** (OIDC) | string | Required `iss`. |  |
| `DASH_INGEST_JWT_AUDIENCE` / `DASH_RETRIEVAL_JWT_AUDIENCE` | unset (HS256), **required** (OIDC) | string | Required `aud`. |  |
| `DASH_INGEST_JWT_LEEWAY_SECS` / `DASH_RETRIEVAL_JWT_LEEWAY_SECS` | `0` | integer | Clock skew allowance, capped at 60. |  |
| `DASH_INGEST_JWT_MAX_LIFETIME_SECS` / `DASH_RETRIEVAL_JWT_MAX_LIFETIME_SECS` | `86400` | integer >= 1 | Maximum `exp - iat` (or `exp - now` when `iat` is absent). Longer-lived tokens are rejected. |  |
| `DASH_INGEST_JWT_REQUIRE_EXP` / `DASH_RETRIEVAL_JWT_REQUIRE_EXP` | `n/a` | bool | **Ignored.** `exp` is always required; setting it to a false value only logs a warning at startup. |  |
| `DASH_INGEST_JWT_ROLES_CLAIM` / `DASH_RETRIEVAL_JWT_ROLES_CLAIM` | `dash_roles` | string | Name of the claim carrying roles. | The older name `DASH_<SVC>_JWT_ROLE_CLAIM` is still read. |
| `DASH_INGEST_JWT_DEFAULT_ROLES` / `DASH_RETRIEVAL_JWT_DEFAULT_ROLES` | `none` | list (`,`) | Roles for tokens that **lack** the role claim. With no default, a role-less token is authenticated but gets 403 on every role-checked route. |  |
| `DASH_INGEST_JWT_ALLOW_WILDCARD_TENANT` / `DASH_RETRIEVAL_JWT_ALLOW_WILDCARD_TENANT` | `off` | bool | Honor a `"*"` tenant in a token. Otherwise `"*"` matches nothing. |  |
| `DASH_INGEST_JWT_REVOKED_JTIS` / `DASH_RETRIEVAL_JWT_REVOKED_JTIS` | unset | list (`,`) | Comma-separated `jti` values rejected with 401 `JWT revoked`. |  |
| `DASH_INGEST_JWT_REVOKED_JTIS_PATH` / `DASH_RETRIEVAL_JWT_REVOKED_JTIS_PATH` | unset | path | File with one revoked `jti` per line, reloaded like the key revocation file. |  |
| `DASH_INGEST_JWT_PROVIDER` / `DASH_RETRIEVAL_JWT_PROVIDER` | `hs256` | `hs256` \| `oidc` | `hs256` or `oidc`. Any other value stops startup. |  |
| `DASH_INGEST_JWT_JWKS_URL` / `DASH_RETRIEVAL_JWT_JWKS_URL` | unset | URL | JWKS URL (required for `oidc`). Must be `https://`, or `http://` to a loopback host. |  |
| `DASH_INGEST_JWT_JWKS_REFRESH_MINUTES` / `DASH_RETRIEVAL_JWT_JWKS_REFRESH_MINUTES` | `15` | integer | JWKS cache lifetime. |  |
| `DASH_INGEST_JWT_TENANT_CLAIMS` / `DASH_RETRIEVAL_JWT_TENANT_CLAIMS` | `tenant_id,tenants,tenant_ids` | list (`,`) | Comma-separated claim names that carry the tenant list (OIDC mode). |  |
| `DASH_OIDC_ALLOW_INSECURE_JWKS` | `off` | bool | Allow a plain `http://` JWKS URL on a non-loopback host. Do not use in production. | DASH only. |

HS256 tokens carry tenants in `tenant_id` (string) or `tenants` / `tenant_ids` (array). OIDC mode accepts only asymmetric algorithms (RS256/384/512, PS256/384/512, ES256/384, EdDSA) and requires a `kid`. The automated tests exercise RS256 signature verification, RS256-vs-RS384 key restriction and the JWKS cache behavior; the other listed algorithms are accepted by the allow-list but have no dedicated test. There are no RS256/ES256 PEM-key variables (`..._JWT_PUBLIC_KEY`, `..._JWT_ALGORITHM` do not exist).

### Metrics and replication/control-plane authentication

| Variable | Default | Type | Description | Notes |
|---|---|---|---|---|
| `DASH_METRICS_PUBLIC` | `off` | bool | Expose `/metrics` without credentials. Without it `/metrics` and `/debug/*` need a credential holding `read_only` or `admin`. | DASH only. |
| `DASH_REPLICATION_ALLOW_INSECURE_HTTP` | `off` | bool | Set to exactly `1` to let a replication follower send the replication token over plaintext `http://` to a non-loopback host. Off by default: the follower refuses (an ingestion follower refuses to start). Enable TLS on the leader (`DASH_INGEST_TLS_CERT_FILE`) and use an `https://` source URL instead; see `docs/operations/tls.md`. | DASH only. Enabled by only the literal `1`. |
| `DASH_REPLICATION_CA_FILE` | unset | path | PEM file with extra CA certificates a replication follower trusts for an `https://` source URL (a private or mesh CA). The public web roots are always trusted. Server certificates are always verified; there is no switch to turn verification off. | DASH only. |
| `DASH_REPLICATION_CLIENT_CERT_FILE` | unset | path | PEM client certificate chain a replication follower presents to an `https://` source (mutual TLS, for a leader with `DASH_INGEST_TLS_CLIENT_CA_FILE`). Needs `DASH_REPLICATION_CLIENT_KEY_FILE`; one without the other refuses to start an ingestion follower and fails every retrieval poll. Re-read on every poll, so rotation needs no restart. | DASH only. |
| `DASH_REPLICATION_CLIENT_KEY_FILE` | unset | path | PEM private key of `DASH_REPLICATION_CLIENT_CERT_FILE`. | DASH only. |
| `DASH_ROUTER_CONTROL_PLANE_TOKEN` | falls back to `DASH_CONTROL_PLANE_TOKEN` | secret | Token the router client sends to the control plane. | DASH only. |

### Audit log

| Variable | Default | Type | Description | Notes |
|---|---|---|---|---|
| `DASH_INGEST_AUDIT_LOG_PATH` / `DASH_RETRIEVAL_AUDIT_LOG_PATH` | unset (audit off) | path | Path of the SHA-256 hash-chained JSON-lines audit log. |  |
| `DASH_INGEST_AUDIT_FSYNC` / `DASH_RETRIEVAL_AUDIT_FSYNC` | ``1`` | bool | `fdatasync` after every audit append. | DASH only. |
| `DASH_INGEST_AUDIT_FAIL_CLOSED` / `DASH_RETRIEVAL_AUDIT_FAIL_CLOSED` | ``0`` | bool | When on, a write to `/v1/ingest*` (ingestion) or a call to `/v1/retrieve` (retrieval) is refused with 503 if the audit log cannot be opened, locked and its tail recovered. The check is made before the mutation; the append itself still happens after it (see `docs/operations/audit-chain.md`). | DASH only. |
| `DASH_AUDIT_FINGERPRINT_KEY` | random per process | secret | Key for the HMAC-SHA256 credential fingerprints in audit records. Set the same value on every node whose fingerprints should be comparable; without it a random key is used and a warning is logged. | DASH only. |
| `DASH_AUDIT_DENIAL_MAX_PER_SEC` | `50` | number >= 0 | Maximum denial (401/403/429) audit records per second per audit file (burst 10x); extra denials are dropped and counted in `dash_audit_denials_dropped_total`. `0` disables the limit. | DASH only. |

Verify a log with `tools/audit-verify` or `scripts/verify_audit_chain.sh`.

### Persistence and WAL

| Variable | Default | Type | Description | Notes |
|---|---|---|---|---|
| `DASH_WAL_REPLAY_STRICT` | `off` | bool | When on, WAL replay fails on the first record that lenient mode would quarantine, instead of quarantining it and continuing. See `docs/operations/wal-recovery.md`. | DASH only. |
| `DASH_INGEST_VECTOR_INDEX_PERSIST` / `DASH_RETRIEVAL_VECTOR_INDEX_PERSIST` (shared: `DASH_VECTOR_INDEX_PERSIST`) | **on** | bool | Save the vector indexes to `DASH_{SVC}_VECTOR_INDEX_PATH` and, at startup, load them instead of rebuilding every HNSW from the replayed vectors; only the vector records written after the save are re-applied. A file that is corrupt, of another format version, built with other ANN tuning or saved for another WAL generation (for example before a checkpoint) is discarded with a warning and the indexes are rebuilt from the WAL. Needs a WAL path. `0`, `false`, `no` or `off` turns it off. | DASH only. |
| `DASH_INGEST_VECTOR_INDEX_PATH` / `DASH_RETRIEVAL_VECTOR_INDEX_PATH` | `<WAL path>.vindex` | path | File the vector indexes are saved to (written atomically through `<path>.tmp`). Keep it on the same volume as the WAL. Safe to delete while the service is stopped: the next start rebuilds and saves it again. | DASH only. |
| `DASH_INGEST_VECTOR_INDEX_SAVE_INTERVAL_MS` / `DASH_RETRIEVAL_VECTOR_INDEX_SAVE_INTERVAL_MS` (shared: `DASH_VECTOR_INDEX_SAVE_INTERVAL_MS`) | `300000` | milliseconds | How often a background thread saves the vector indexes when the WAL moved since the last save. `0` turns periodic saves off; the indexes are still saved after every WAL checkpoint (ingestion) and at a clean shutdown. | DASH only. |

### ANN tuning

Each variable can be set per service (`DASH_INGEST_ANN_*`, `DASH_RETRIEVAL_ANN_*`) or shared (`DASH_ANN_*`); the per-service name wins, then the shared name, then the `EME_` forms. Values must be positive integers (`VECTOR_RERANK` may be `0`). The index is a per-tenant exact flat scan below `VECTOR_FLAT_THRESHOLD` vectors and a `usearch` HNSW (cosine, `i8` quantisation, exact `f32` rerank) above it. The previous in-repo graph and its `ANN_MAX_NEIGHBORS_UPPER`, `ANN_SEARCH_EXPANSION_FACTOR` and `ANN_SEARCH_EXPANSION_MAX` settings were removed; if still set they are ignored.

| Variable | Default | Type | Description | Notes |
|---|---|---|---|---|
| `DASH_INGEST_ANN_MAX_NEIGHBORS_BASE` / `DASH_RETRIEVAL_ANN_MAX_NEIGHBORS_BASE` (shared: `DASH_ANN_MAX_NEIGHBORS_BASE`) | `16` | integer >= 1 | HNSW connectivity `M` (usearch `connectivity`): neighbours per node. Higher is more accurate and uses more memory. The previous in-repo graph used 12. |  |
| `DASH_INGEST_ANN_EXPANSION_ADD` / `DASH_RETRIEVAL_ANN_EXPANSION_ADD` (shared: `DASH_ANN_EXPANSION_ADD`) | `128` | integer >= 1 | HNSW `ef_construction` (usearch `expansion_add`): beam width while inserting a vector. Higher builds a better graph more slowly. | DASH only. |
| `DASH_INGEST_ANN_SEARCH_EXPANSION_MIN` / `DASH_RETRIEVAL_ANN_SEARCH_EXPANSION_MIN` (shared: `DASH_ANN_SEARCH_EXPANSION_MIN`) | `128` | integer >= 1 | HNSW `ef_search` floor (usearch `expansion_search`): beam width while searching; it is widened to the number of requested candidates when that is larger. |  |
| `DASH_INGEST_VECTOR_FLAT_THRESHOLD` / `DASH_RETRIEVAL_VECTOR_FLAT_THRESHOLD` (shared: `DASH_VECTOR_FLAT_THRESHOLD`) | `8192` | integer >= 1 | A tenant with at most this many vectors is searched exactly (flat scan); above it an HNSW index is built. The same number bounds filtered searches that are scanned exactly instead of through the HNSW. | DASH only. |
| `DASH_INGEST_VECTOR_RERANK` / `DASH_RETRIEVAL_VECTOR_RERANK` (shared: `DASH_VECTOR_RERANK`) | `50` | integer | HNSW candidates re-scored with exact `f32` cosine after the quantised (`i8`) search. `0` returns the quantised scores unchanged. | DASH only. |

There is no `DASH_ANN_M`, `DASH_ANN_EF_CONSTRUCTION`, `DASH_ANN_EF_SEARCH` or `DASH_ANN_REBUILD_THRESHOLD`.

### Retrieval tuning and request bounds

| Variable | Default | Type | Description | Notes |
|---|---|---|---|---|
| `DASH_VECTOR_BACKEND` | `auto` | `cpu` \| `gpu` | `cpu`, `gpu` or unset. There is no GPU implementation: `gpu` is reported as `cpu (gpu-feature-disabled)` or, with the `gpu-backend` build feature, `cpu (gpu-unavailable)`; scoring always runs on CPU. | DASH only. |

### Placement and routing

Placement routing is enabled when `DASH_ROUTER_PLACEMENT_FILE` or `DASH_ROUTER_CONTROL_PLANE_URL` is set. Both services then require a local node id: ingestion exits (code 2) and retrieval fails to start (exit 1) if it is missing or if the placement source cannot be loaded.

| Variable | Default | Type | Description | Notes |
|---|---|---|---|---|
| `DASH_ROUTER_PLACEMENT_FILE` | unset | path | CSV of shard placements. The control plane uses it as its initial state. | Read by ingestion, retrieval, control-plane. |
| `DASH_ROUTER_CONTROL_PLANE_URL` | unset | URL | Fetch placement from the control plane (`http://` or `https://`). If it is configured and unreachable, the placement file is **not** used as a fallback unless `DASH_ROUTER_ALLOW_STALE_PLACEMENT` is set. |  |
| `DASH_ROUTER_ALLOW_STALE_PLACEMENT` | `off` | bool | `1` or `true`: when the control plane is configured but unreachable, fall back to `DASH_ROUTER_PLACEMENT_FILE`. Off by default because a stale file can name a deposed leader (split-brain writes). | DASH only. Enabled by `1`, `true`, `TRUE`. |
| `DASH_ROUTER_ALLOW_INSECURE_HTTP` | `off` | bool | `1` or `true`: allow the router to send `DASH_ROUTER_CONTROL_PLANE_TOKEN` over plain http to a non-loopback control plane. Off by default, so the token never crosses the network in clear text. | DASH only. Enabled by `1`, `true`, `TRUE`. |
| `DASH_ROUTER_CONTROL_PLANE_CONNECT_TIMEOUT_MS` | `2000` | milliseconds >= 1 | Connect timeout of the control-plane client. | DASH only. |
| `DASH_ROUTER_CONTROL_PLANE_READ_TIMEOUT_MS` | `5000` | milliseconds >= 1 | Read timeout of the control-plane client. | DASH only. |
| `DASH_ROUTER_CONTROL_PLANE_WRITE_TIMEOUT_MS` | `5000` | milliseconds >= 1 | Write timeout of the control-plane client. | DASH only. |
| `DASH_ROUTER_CONTROL_PLANE_CA_FILE` | unset | path | PEM bundle of extra CAs trusted for an `https://` `DASH_ROUTER_CONTROL_PLANE_URL` (the public web roots are always trusted; verification is never off). | DASH only. |
| `DASH_ROUTER_CONTROL_PLANE_CLIENT_CERT_FILE` | unset | path | PEM client certificate chain presented to an `https://` control plane that verifies client certificates. Needs `DASH_ROUTER_CONTROL_PLANE_CLIENT_KEY_FILE`. | DASH only. |
| `DASH_ROUTER_CONTROL_PLANE_CLIENT_KEY_FILE` | unset | path | PEM private key of `DASH_ROUTER_CONTROL_PLANE_CLIENT_CERT_FILE`. | DASH only. |
| `DASH_ROUTER_LOCAL_NODE_ID` | unset | string | This node's id. Falls back to `DASH_NODE_ID`. An ingestion follower also uses it as its replica id in acknowledgements. |  |
| `DASH_NODE_ID` | unset | string | Fallback for the local node id. |  |
| `DASH_ROUTER_PLACEMENT_RELOAD_INTERVAL_MS` | `off` | milliseconds | Reload placement on this interval (values <= 0 disable). |  |
| `DASH_ROUTER_SHARD_IDS` | from placement | list of integers (`,`) | Comma-separated shard ids override. |  |
| `DASH_ROUTER_REPLICA_COUNT` | from placement | integer >= 1 | Replica count override. |  |
| `DASH_ROUTER_VIRTUAL_NODES_PER_SHARD` | `64` | integer >= 1 | Virtual nodes for shard hashing. |  |

### Embedding providers

| Variable | Default | Type | Description | Notes |
|---|---|---|---|---|
| `DASH_EMBEDDING_PROVIDER` | `hash` | `hash` \| `ollama` \| `openai` | `hash`, `ollama` or `openai`. Used by retrieval (`/v1/embeddings`, query embedding) and ingestion (claim embedding). Unknown values fall back to `hash` with a warning on stderr; startup validation reports them as errors. | DASH only. |
| `DASH_OLLAMA_ENDPOINT` | `http://localhost:11434` | URL | Ollama endpoint; a bare base URL is expanded to `/api/embed`. | `DASH_OLLAMA_BASE_URL` is a deprecated alias, read when this is unset; it logs a one-time deprecation warning. DASH only. |
| `DASH_OLLAMA_MODEL` | `nomic-embed-text` | string | Ollama model. | DASH only. |
| `DASH_OPENAI_API_KEY` | unset | secret | Required for `openai`; without it the provider cannot be built and the service falls back to `hash` (logged on stderr). | DASH only. |
| `DASH_OPENAI_MODEL` | `text-embedding-3-small` | string | OpenAI model. The endpoint is fixed at `https://api.openai.com/v1/embeddings`. | DASH only. |
| `DASH_EMBEDDING_ALLOW_INSECURE_HTTP` | `off` | bool | Set to exactly `1` to allow sending a bearer API key over plaintext `http://` to a non-loopback host. Off by default: the client refuses that. | DASH only. Enabled by only the literal `1`. |
| `DASH_EMBEDDING_MAX_CONCURRENCY` | `8` | integer | Concurrent provider calls per process; `0` = unlimited. When no slot frees within the queue wait the call fails with 503 `embedding_unavailable` and `Retry-After`. | DASH only. |
| `DASH_EMBEDDING_QUEUE_WAIT_MS` | `250` | milliseconds | How long a call waits for a slot. | DASH only. |
| `DASH_EMBEDDING_BREAKER_THRESHOLD` | `5` | integer | Consecutive upstream failures that open the breaker; `0` disables it. Only transport errors, timeouts and 5xx count; 4xx, 429 and payload/dimension errors never do. | DASH only. |
| `DASH_EMBEDDING_BREAKER_RESET_MS` | `10000` | milliseconds >= 1 | Time before one probe call is admitted. | DASH only. |

The provider clients use HTTPS (rustls) where the URL is `https://`, do not follow redirects, cap response bodies, retry with jittered backoff and sit behind a circuit breaker whose half-open state admits a single probe. The hash provider's default dimension is 384. Timeouts are compiled in (Ollama 5 s, OpenAI 30 s). There is no `DASH_EMBEDDING_MODEL`, `DASH_EMBEDDING_DIM`, `DASH_OPENAI_BASE_URL`, `DASH_OPENAI_TIMEOUT_MS` or `DASH_OLLAMA_TIMEOUT_MS`.

Network providers (`ollama`, `openai`) are wrapped in a circuit breaker and a concurrency cap (`DASH_EMBEDDING_MAX_CONCURRENCY`, `DASH_EMBEDDING_QUEUE_WAIT_MS`, `DASH_EMBEDDING_BREAKER_THRESHOLD`, `DASH_EMBEDDING_BREAKER_RESET_MS`). Provider outages (breaker open, timeout, connection error, 429, 5xx, concurrency cap) answer 503 `embedding_unavailable` with `Retry-After` (the upstream's value when present, otherwise 1). Unusable provider output (non-finite values, wrong dimensions, malformed payload, other 4xx) answers 502 `embedding_provider_error` (retrieval) or `embedding_upstream_error` (ingestion).

### Encryption (library only)

`DASH_ENCRYPTION_PROVIDER` (`none` or `env`) and `DASH_ENCRYPTION_MASTER_KEY` (64 hex characters or base64 of 32 bytes) are read by `pkg/encryption::provider_from_env` (also under the `EME_` names). **No service calls it**, so setting them has no effect on stored data. Encryption at rest is planned (P4).

| Variable | Default | Type | Description | Notes |
|---|---|---|---|---|
| `DASH_ENCRYPTION_PROVIDER` | `none` | `none` \| `env` | Read by `pkg/encryption::provider_from_env`. **No service calls it**, so setting it has no effect on stored data. Encryption at rest is planned (P4). | Not read by any service. |
| `DASH_ENCRYPTION_MASTER_KEY` | unset | secret | 64 hex characters or base64 of 32 bytes, for provider `env`. Not used by any service yet. | Not read by any service. |

### Container and compose variables

`DASH_BIN` (which binary the image entrypoint runs: `ingestion`, `retrieval`, `control-plane` or `segment-maintenance-daemon`; default `retrieval`), `DASH_HOME` (default `/opt/dash`) and `DASH_HEALTHCHECK_URL` are read by the shell scripts in `deploy/container/scripts/`, not by the Rust services. `DASH_PUBLISH_ADDR` (default `127.0.0.1`) is read by the compose file for published ports. The compose file may also set `DASH_INGEST_TRANSPORT_RUNTIME` and `DASH_RETRIEVAL_TRANSPORT_RUNTIME`; no code reads them. These are known to the validator so that they do not produce unknown-variable warnings.

| Variable | Default | Type | Description | Notes |
|---|---|---|---|---|
| `DASH_BIN` | `retrieval` | `ingestion` \| `retrieval` \| `control-plane` \| `segment-maintenance-daemon` | Which binary the image entrypoint runs. Read by the shell scripts in `deploy/container/scripts/`, not by the Rust services. |  |
| `DASH_HOME` | `/opt/dash` | path | Install directory inside the image. Read by the container shell scripts. |  |
| `DASH_HEALTHCHECK_URL` | unset | URL | URL probed by the container health check. Read by the container shell scripts. |  |
| `DASH_HEALTHCHECK_CA_FILE` | unset | path | CA bundle the container health check verifies an `https://` service against (the service certificate must name `127.0.0.1`). Without it the loopback probe of a TLS listener skips verification; it sends no credentials. Read by the container shell scripts. |  |
| `DASH_PUBLISH_ADDR` | `127.0.0.1` | string | Host address the compose file publishes ports on. Read by docker compose, not by the services. |  |
| `DASH_DEV_UID` | `1000` | integer | User id for the development compose stack. Read by docker compose. |  |
| `DASH_DEV_GID` | `1000` | integer | Group id for the development compose stack. Read by docker compose. |  |

## Ingestion settings

Read by the `ingestion` service.

### Metrics and replication/control-plane authentication

| Variable | Default | Type | Description | Notes |
|---|---|---|---|---|
| `DASH_INGEST_REPLICATION_TOKEN` | unset | secret | Shared token checked against the `x-replication-token` header on `/internal/replication/*`. Without it the replication endpoints answer 403 (open only in dev mode). An ingestion node configured as a follower (`DASH_INGEST_REPLICATION_SOURCE_URL`) refuses to start without it outside dev mode. With strict secrets it must be a non-placeholder of at least 16 characters. A retrieval follower also falls back to this variable when `DASH_RETRIEVAL_REPLICATION_TOKEN` is unset. | Read by ingestion, retrieval. |
| `DASH_INGEST_REPLICATION_REQUIRE_CLIENT_CERT` | `off` | bool | Require, on top of the replication token, a client certificate verified against `DASH_INGEST_TLS_CLIENT_CA_FILE` on `/internal/replication/*` (403 otherwise). Other routes are unaffected. Startup is refused when the listener does not verify client certificates. | DASH only. |
| `DASH_INGEST_REPLICATION_ALLOWED_CLIENT_CERTS` | unset | list (`,`) | Comma-separated SHA-256 fingerprints (64 hex digits, colons allowed) of the follower certificates allowed on `/internal/replication/*`; implies `DASH_INGEST_REPLICATION_REQUIRE_CLIENT_CERT`. Gives each follower its own identity: remove a fingerprint to cut one follower off. Compute one with `openssl x509 -in follower.pem -outform der \| sha256sum`. A malformed entry refuses startup. | DASH only. |

### Container and compose variables

| Variable | Default | Type | Description | Notes |
|---|---|---|---|---|
| `DASH_INGEST_TRANSPORT_RUNTIME` | unset | string | The compose file may set this; no code reads it. |  |

### Network and transport

| Variable | Default | Type | Description | Notes |
|---|---|---|---|---|
| `DASH_INGEST_BIND` | `127.0.0.1:8081` | host:port | Listen address (`host:port`). |  |
| `DASH_INGEST_HTTP_WORKERS` | min(CPU count, 32), or 4 if undetectable | integer >= 1 | Worker threads. Must be > 0. |  |
| `DASH_INGEST_HTTP_QUEUE_CAPACITY` | `workers * 64` | integer >= 1 | Bounded accept queue. When full, the service answers 503. |  |
| `DASH_INGEST_TLS_CERT_FILE` | unset (plain HTTP) | path | PEM certificate chain (leaf first) for the ingestion listener. With `DASH_INGEST_TLS_KEY_FILE` the listener serves HTTPS only (TLS 1.2 and 1.3, ALPN `http/1.1`); setting only one of the two is a startup error. The handshake must finish within `DASH_HTTP_FIRST_BYTE_TIMEOUT_MS`. The file is re-read when its content changes (checked at most once per second), so rotation needs no restart; a broken new file keeps the previous certificate. See `docs/operations/tls.md`. | DASH only. |
| `DASH_INGEST_TLS_KEY_FILE` | `unset` | path | PEM private key (PKCS#8, PKCS#1 or SEC1) for `DASH_INGEST_TLS_CERT_FILE`. Keep it readable by the service user only. Reloaded with the certificate. | DASH only. |
| `DASH_INGEST_TLS_CLIENT_CA_FILE` | unset (no client certificates) | path | PEM bundle of CAs that client certificates must chain to (mutual TLS). Clients without a certificate are still accepted unless `DASH_INGEST_TLS_REQUIRE_CLIENT_CERT` is on; a certificate that is presented must verify. Needs the certificate and key. Reloaded when it changes. To require a client certificate only on `/internal/replication/*`, leave the next setting off and set `DASH_INGEST_REPLICATION_REQUIRE_CLIENT_CERT` or `DASH_INGEST_REPLICATION_ALLOWED_CLIENT_CERTS`. | DASH only. |
| `DASH_INGEST_TLS_REQUIRE_CLIENT_CERT` | `off` | bool | Refuse every client that presents no certificate chaining to `DASH_INGEST_TLS_CLIENT_CA_FILE` (the handshake fails). This covers probes too: use exec or TCP probes, or leave it off and require certificates per route instead. | DASH only. |
| `DASH_INGEST_BATCH_MAX_ITEMS` | `128` | integer >= 1 | Maximum items in `POST /v1/ingest/batch`. |  |

### Persistence and WAL

| Variable | Default | Type | Description | Notes |
|---|---|---|---|---|
| `DASH_INGEST_WAL_PATH` | unset | path | WAL file. When unset the ingestion service is **purely in-memory** (data is lost on restart). The compose file sets it. |  |
| `DASH_INGEST_PERSISTENCE_PATH` | `./data/dash-ingestion.redb` | path | redb file. Used only when a WAL path is set. |  |
| `DASH_INGEST_PERSISTENCE_DISABLE` | `off` | bool | Set to `1` to skip redb. If the redb file cannot be opened the service logs an error and continues in memory. | The startup path honors only the literal `1`; the readiness probe also accepts `true` and `yes`. Use `1`. Enabled by `1`, `true`, `yes`. |
| `DASH_INGEST_WAL_SYNC_EVERY_RECORDS` | `1` | integer >= 1 | fsync after this many records (values < 1 become 1). |  |
| `DASH_INGEST_WAL_APPEND_BUFFER_RECORDS` | `1` | integer >= 1 | Buffered records before write (values < 1 become 1). |  |
| `DASH_INGEST_WAL_SYNC_INTERVAL_MS` | `unset` | milliseconds | Time-based fsync interval (values <= 0 are ignored). |  |
| `DASH_INGEST_WAL_BACKGROUND_FLUSH_ONLY` | ``false`` | bool | Flush only from the background flusher. An unparseable value exits with code 2. |  |
| `DASH_INGEST_WAL_ASYNC_FLUSH_INTERVAL_MS` | auto (250 ms when batching is enabled, otherwise off) | integer >= 1 or `auto`, `off`, `none`, `false`, `disabled`, `0` | Positive integer, `auto`, or `off`/`none`/`false`/`disabled`/`0`. An empty value disables it. Anything else exits with code 2. |  |
| `DASH_INGEST_ALLOW_UNSAFE_WAL_DURABILITY` | ``false`` | bool | Required to run with durability settings that exceed the safe limits: sync-every or append buffer above 256 records, sync interval or async flush interval above 5000 ms, batched writes without a sync interval, or background-only flushing without an async worker. Without it the service exits with code 2. |  |
| `DASH_CHECKPOINT_MAX_WAL_RECORDS` | `unset` | integer >= 1 | Trigger a checkpoint after this many WAL records. |  |
| `DASH_CHECKPOINT_MAX_WAL_BYTES` | `unset` | integer >= 1 | Trigger a checkpoint after this many WAL bytes. |  |

### Replication follower

An ingestion node can itself follow another ingestion node; it then pulls WAL frames from the source, persists `(generation, offset)` together and resyncs from a full export when the leader's WAL generation changes.

| Variable | Default | Type | Description | Notes |
|---|---|---|---|---|
| `DASH_INGEST_REPLICATION_SOURCE_URL` | unset (off) | URL | An ingestion node can itself follow another ingestion node. Requires `DASH_INGEST_REPLICATION_TOKEN`. |  |
| `DASH_INGEST_REPLICATION_COMMIT_STATUS_MAX` | `100000` | integer >= 1 | Leader-side cap on tracked per-commit replication status entries. Past the cap the oldest completed entries are evicted first; entries still pending quorum are never evicted. Exposed as `dash_ingest_replication_commit_status_entries` and `dash_ingest_replication_commit_status_evicted_total`. | DASH only. |
| `DASH_INGEST_REPLICATION_COMMIT_STATUS_TTL_SECS` | `3600` | integer >= 1 | Seconds a completed (quorum met) commit status entry is kept before it expires. Pending entries do not expire. A late ack for an expired commit gets 404 and the follower ignores it. | DASH only. |
| `DASH_INGEST_REPLICATION_POLL_INTERVAL_MS` | `500` | milliseconds >= 1 | Poll interval for ingestion-to-ingestion pulls. |  |
| `DASH_INGEST_REPLICATION_MAX_RECORDS` | `512` | integer >= 1 | Records per pull. |  |
| `DASH_INGEST_REPLICATION_MAX_RESPONSE_BYTES` | 67108864 (64 MiB) | integer >= 1 | Upper bound for one response body. |  |
| `DASH_INGEST_REPLICATION_MAX_BACKOFF_MS` | `30000` | milliseconds >= 1 | Upper bound for the failure backoff. |  |
| `DASH_INGEST_REPLICATION_MAX_LAG_RECORDS` | `100000` | integer >= 1 | Readiness lag threshold. |  |
| `DASH_INGEST_REPLICATION_MAX_STALENESS_MS` | `300000` | milliseconds >= 1 | Readiness staleness threshold. |  |
| `DASH_INGEST_REPLICATION_OFFSET_PATH` | derived from the ingestion WAL path (`<wal>.replication`) | path | File storing the last applied generation and offset. |  |

### Placement and routing

| Variable | Default | Type | Description | Notes |
|---|---|---|---|---|
| `DASH_INGEST_PLACEMENT_STALE_GRACE_MS` | `30000` | milliseconds | With placement reload enabled: how long writes keep being accepted on the last known placement after reloads start failing. After the grace, writes are refused with 503 `placement is stale` until a reload succeeds. | DASH only. |

### Segments and maintenance

Read by the ingestion service (segment publishing) and the `segment-maintenance-daemon` binary.

| Variable | Default | Type | Description | Notes |
|---|---|---|---|---|
| `DASH_INGEST_SEGMENT_DIR` | unset (segment publishing off) | path | Segment root directory. Read by the ingestion service (segment publishing) and the `segment-maintenance-daemon` binary. |  |
| `DASH_INGEST_SEGMENT_MAX_SEGMENT_SIZE` (shared: `DASH_SEGMENT_MAX_SEGMENT_SIZE`) | `10000` | integer >= 1 | Max claims per segment. |  |
| `DASH_INGEST_SEGMENT_MAX_SEGMENTS_PER_TIER` (shared: `DASH_SEGMENT_MAX_SEGMENTS_PER_TIER`) | `8` | integer >= 1 | Segments per tier before compaction. |  |
| `DASH_INGEST_SEGMENT_MAX_COMPACTION_INPUT_SEGMENTS` (shared: `DASH_SEGMENT_MAX_COMPACTION_INPUT_SEGMENTS`) | `4` | integer >= 2 | Max segments merged at once (must be > 1). |  |
| `DASH_INGEST_SEGMENT_MAINTENANCE_INTERVAL_MS` | `30000` | milliseconds | Maintenance loop interval; `0` disables in-process maintenance. |  |
| `DASH_INGEST_SEGMENT_GC_MIN_STALE_AGE_MS` | `60000` | milliseconds | Minimum age before an unreferenced segment file is deleted. |  |
| `DASH_INGEST_SEGMENT_MAINTENANCE_STRICT` | `off` | bool | `segment-maintenance-daemon` only: with `--once`, exit non-zero if any tenant failed (the pass still visits every tenant). | Enabled by `1`, `true`, `TRUE`, `yes`. |

Tenant directories under the segment root are named by an injective escaping of the tenant id (bytes outside `a-z0-9-` become `_xx`; ids over 96 characters get a hash suffix). Existing directories created by the older lossy sanitizer are renamed automatically the first time a tenant is touched.

### Extraction and parsing

These control `POST /v1/ingest/raw` and `/v1/ingest/document` (see `GET /debug/document-parser`).

| Variable | Default | Type | Description | Notes |
|---|---|---|---|---|
| `DASH_INGEST_RAW_EXTRACTION_PROVIDER` | `rule_sentence` | `rule_sentence` \| `rule` \| `adapter_command` \| `model_adapter` | `rule_sentence` or `adapter_command`. The adapter needs the `model-extraction-adapter` cargo feature of `ingestion`, which is off by default; without it requests fail. |  |
| `DASH_INGEST_RAW_ADAPTER_CMD` | unset | string | Command (run with `sh -c`) used when the provider is `adapter_command`. |  |
| `DASH_INGEST_DOCUMENT_PARSER_PROVIDER` | `builtin_utf8` | `builtin_utf8` \| `utf8` \| `adapter_command` \| `document_adapter` | `builtin_utf8` or `adapter_command`. |  |
| `DASH_INGEST_DOCUMENT_ADAPTER_CMD` | unset | string | Command used by the document parser adapter; receives the media type in `DASH_DOCUMENT_MIME_TYPE`. |  |
| `DASH_INGEST_EMBEDDING_PROVIDER` | `hash_vector` | `hash_vector` \| `hash` \| `builtin_hash` \| `off` \| `none` \| `disabled` \| `adapter_command` \| `model_adapter` | Provider for embeddings generated during raw/document ingest: `hash_vector`, `off`, or `adapter_command`. |  |
| `DASH_INGEST_EMBEDDING_ADAPTER_CMD` | unset | string | Command used when the provider is `adapter_command`. |  |
| `DASH_INGEST_EMBEDDING_DIMENSIONS` | `64` | integer >= 1 | Hash vector dimensions, clamped to 8..4096. |  |
| `DASH_DOCUMENT_MIME_TYPE` | unset | string | Set by the service in the environment of the document adapter command (the media type of the document). Not a setting. |  |

Adapter commands are operator-controlled shell command lines; anyone who can set the service environment can run code as the service user.

## Retrieval settings

Read by the `retrieval` service.

### Metrics and replication/control-plane authentication

| Variable | Default | Type | Description | Notes |
|---|---|---|---|---|
| `DASH_RETRIEVAL_REPLICATION_TOKEN` | unset | secret | Token the retrieval follower sends to the source. Must equal the source's `DASH_INGEST_REPLICATION_TOKEN`. |  |

### Container and compose variables

| Variable | Default | Type | Description | Notes |
|---|---|---|---|---|
| `DASH_RETRIEVAL_TRANSPORT_RUNTIME` | unset | string | The compose file may set this; no code reads it. |  |

### Network and transport

| Variable | Default | Type | Description | Notes |
|---|---|---|---|---|
| `DASH_RETRIEVAL_BIND` | `127.0.0.1:8080` | host:port | Listen address (`host:port`). |  |
| `DASH_RETRIEVAL_HTTP_WORKERS` | min(CPU count, 32), or 4 if undetectable | integer >= 1 | Worker threads. Must be > 0. |  |
| `DASH_RETRIEVAL_HTTP_QUEUE_CAPACITY` | `workers * 64` | integer >= 1 | Bounded accept queue. When full, the service answers 503. |  |
| `DASH_RETRIEVAL_TLS_CERT_FILE` | unset (plain HTTP) | path | PEM certificate chain (leaf first) for the retrieval listener. With `DASH_RETRIEVAL_TLS_KEY_FILE` the listener serves HTTPS only (TLS 1.2 and 1.3, ALPN `http/1.1`); setting only one of the two is a startup error. The handshake must finish within `DASH_HTTP_FIRST_BYTE_TIMEOUT_MS`. The file is re-read when its content changes (checked at most once per second), so rotation needs no restart; a broken new file keeps the previous certificate. See `docs/operations/tls.md`. | DASH only. |
| `DASH_RETRIEVAL_TLS_KEY_FILE` | `unset` | path | PEM private key (PKCS#8, PKCS#1 or SEC1) for `DASH_RETRIEVAL_TLS_CERT_FILE`. Keep it readable by the service user only. Reloaded with the certificate. | DASH only. |
| `DASH_RETRIEVAL_TLS_CLIENT_CA_FILE` | unset (no client certificates) | path | PEM bundle of CAs that client certificates must chain to (mutual TLS). Clients without a certificate are still accepted unless `DASH_RETRIEVAL_TLS_REQUIRE_CLIENT_CERT` is on; a certificate that is presented must verify. Needs the certificate and key. Reloaded when it changes. | DASH only. |
| `DASH_RETRIEVAL_TLS_REQUIRE_CLIENT_CERT` | `off` | bool | Refuse every client that presents no certificate chaining to `DASH_RETRIEVAL_TLS_CLIENT_CA_FILE` (the handshake fails). This covers probes too: use exec or TCP probes, or leave it off and require certificates per route instead. | DASH only. |

### Persistence and WAL

| Variable | Default | Type | Description | Notes |
|---|---|---|---|---|
| `DASH_RETRIEVAL_WAL_PATH` | unset | path | Optional local WAL; replayed at startup and, when a replication follower is configured, mirrored from the leader so a restart can resume. |  |
| `DASH_RETRIEVAL_PERSISTENCE_PATH` | `./data/dash-retrieval.redb` | path | redb file. |  |
| `DASH_RETRIEVAL_PERSISTENCE_DISABLE` | `off` | bool | Set to `1` to skip redb. | The startup path honors only the literal `1`; the readiness probe also accepts `true` and `yes`. Use `1`. Enabled by `1`, `true`, `yes`. |

### Replication follower

Both followers pull WAL frames from an ingestion node, persist `(generation, offset)` together and resync from a full export when the leader's WAL generation changes. Retrieval's offset is saved next to its WAL as `<DASH_RETRIEVAL_WAL_PATH>.replication` when a retrieval WAL is configured; without a retrieval WAL nothing replicated survives a restart and the follower always starts with a full resync.

| Variable | Default | Type | Description | Notes |
|---|---|---|---|---|
| `DASH_RETRIEVAL_REPLICATION_SOURCE_URL` | unset (follower off) | URL | Base URL of the ingestion service, for example `https://ingestion:8081` (leader with `DASH_INGEST_TLS_CERT_FILE`) or `http://127.0.0.1:8081`. |  |
| `DASH_RETRIEVAL_REPLICATION_POLL_INTERVAL_MS` | `1000` | milliseconds >= 1 | Poll interval. |  |
| `DASH_RETRIEVAL_REPLICATION_MAX_RECORDS` | `512` | integer >= 1 | Records per pull (the leader caps a pull at 10000). |  |
| `DASH_RETRIEVAL_REPLICATION_MAX_RESPONSE_BYTES` | 67108864 (64 MiB) | integer >= 1 | Upper bound for one response body. |  |
| `DASH_RETRIEVAL_REPLICATION_MAX_BACKOFF_MS` | `30000` | milliseconds >= 1 | Upper bound for the failure backoff (the poll interval doubles per consecutive failure). |  |
| `DASH_RETRIEVAL_REPLICATION_MAX_LAG_RECORDS` | `100000` | integer >= 1 | `/ready` fails with `replication_lag_exceeded` when the leader is further ahead than this. |  |
| `DASH_RETRIEVAL_REPLICATION_MAX_STALENESS_MS` | `300000` | milliseconds >= 1 | `/ready` fails with `replication_stale` when the last successful poll is older than this. `/ready` also fails with `replication_initial_sync_pending` until the first sync completes. |  |
| `DASH_RETRIEVAL_REPLICATION_OFFSET_PATH` | `<retrieval WAL path>.replication` if a retrieval WAL is set, otherwise none | path | File storing the last applied generation and offset. |  |

### Placement and routing

| Variable | Default | Type | Description | Notes |
|---|---|---|---|---|
| `DASH_ROUTER_READ_PREFERENCE` | `any_healthy` | `any_healthy` \| `leader_only` \| `prefer_follower` | Replica read preference: `any_healthy`, `leader_only` or `prefer_follower`. Any other value is a configuration error. |  |

### Retrieval tuning and request bounds

| Variable | Default | Type | Description | Notes |
|---|---|---|---|---|
| `DASH_RETRIEVAL_MAX_TOP_K` | `1000` | integer >= 1 | Upper bound for `top_k` on `/v1/retrieve`; larger values get 400. | DASH only. |
| `DASH_RETRIEVAL_GRAPH_MAX_HOPS` | `3` | integer >= 1 | Graph expansion depth (must be > 0). |  |
| `DASH_RETRIEVAL_GRAPH_EDGE_DEPTH_DECAY` | `0.75` | number | Per-hop weight decay, clamped to 0..1. |  |
| `DASH_RETRIEVAL_GRAPH_SUPPORT_PATH_BONUS` | `0.16` | number | Score bonus per support path (negative values are treated as 0). |  |
| `DASH_RETRIEVAL_GRAPH_CONTRADICTION_DEPTH_PENALTY` | `0.20` | number | Score penalty for contradiction chains (negative values are treated as 0). |  |
| `DASH_RETRIEVAL_SEGMENT_DIR` | unset | path | Directory of published index segments to prefilter from. |  |
| `DASH_RETRIEVAL_SEGMENT_CACHE_REFRESH_MS` | `1000` | milliseconds >= 1 | Segment cache refresh interval (must be > 0). |  |
| `DASH_RETRIEVAL_DISK_NATIVE_SEGMENT_EXECUTION` | ``true`` | bool | Execute against disk segments natively. |  |
| `DASH_RETRIEVAL_STORAGE_DIVERGENCE_WARN_DELTA_COUNT` | `1000` | integer >= 1 | Divergence warning threshold (records) in `/debug/storage-visibility`. |  |
| `DASH_RETRIEVAL_STORAGE_DIVERGENCE_WARN_RATIO` | `0.25` | number >= 0 | Divergence warning threshold (ratio). |  |

Fixed retrieve bounds (not configurable): `query` at most 8 KiB, at most 256 values in each of `entity_filters` and `embedding_id_filters`, `query_embedding` at most 8192 values.

### Embedding providers

| Variable | Default | Type | Description | Notes |
|---|---|---|---|---|
| `DASH_EMBEDDING_ALLOW_TOKEN_IDS` | `off` | bool | `1`, `true` or `yes`: accept token-id array `input` on `/v1/embeddings`, embedding the decimal ids joined by spaces. Off by default: token-id inputs are rejected with 400 `unsupported_input_type`, because ids cannot be decoded without the client's tokenizer. | DASH only. Enabled by `1`, `true`, `yes`. |
| `DASH_EMBEDDING_MAX_TOTAL_CHARS` | `524288` | integer >= 1 | Maximum total characters across all inputs of one `/v1/embeddings` request. | DASH only. |

## Control-plane settings

Read by the `control-plane` service.

### Metrics and replication/control-plane authentication

| Variable | Default | Type | Description | Notes |
|---|---|---|---|---|
| `DASH_CONTROL_PLANE_TOKEN` | unset | secret | Bearer token required on every `/v1/control-plane/*` route except health and ready. The control plane refuses to start without it unless dev mode is on. The router client in ingestion and retrieval presents it when fetching placement. With strict secrets the control plane requires at least 32 characters; use a random 32-byte value. | Read by ingestion, retrieval, control-plane. |

### HTTP server

| Variable | Default | Type | Description | Notes |
|---|---|---|---|---|
| `DASH_CONTROL_PLANE_BIND` | `127.0.0.1:8090` | host:port | Listen address (`host:port`). |  |
| `DASH_CONTROL_PLANE_TLS_CERT_FILE` | unset (plain HTTP) | path | PEM certificate chain (leaf first) for the control-plane listener. With `DASH_CONTROL_PLANE_TLS_KEY_FILE` the listener serves HTTPS only (TLS 1.2 and 1.3, ALPN `http/1.1`); setting only one of the two is a startup error. The handshake must finish within `DASH_HTTP_FIRST_BYTE_TIMEOUT_MS`. The file is re-read when its content changes (checked at most once per second), so rotation needs no restart; a broken new file keeps the previous certificate. See `docs/operations/tls.md`. | DASH only. |
| `DASH_CONTROL_PLANE_TLS_KEY_FILE` | `unset` | path | PEM private key (PKCS#8, PKCS#1 or SEC1) for `DASH_CONTROL_PLANE_TLS_CERT_FILE`. Keep it readable by the service user only. Reloaded with the certificate. | DASH only. |
| `DASH_CONTROL_PLANE_TLS_CLIENT_CA_FILE` | unset (no client certificates) | path | PEM bundle of CAs that client certificates must chain to (mutual TLS). Clients without a certificate are still accepted unless `DASH_CONTROL_PLANE_TLS_REQUIRE_CLIENT_CERT` is on; a certificate that is presented must verify. Needs the certificate and key. Reloaded when it changes. The placement client in ingestion and retrieval presents `DASH_ROUTER_CONTROL_PLANE_CLIENT_CERT_FILE`. | DASH only. |
| `DASH_CONTROL_PLANE_TLS_REQUIRE_CLIENT_CERT` | `off` | bool | Refuse every client that presents no certificate chaining to `DASH_CONTROL_PLANE_TLS_CLIENT_CA_FILE` (the handshake fails). This covers probes too: use exec or TCP probes, or leave it off and require certificates per route instead. | DASH only. |
| `DASH_CONTROL_PLANE_WORKERS` | `8` | integer >= 1 | Worker threads. | DASH only. |
| `DASH_CONTROL_PLANE_QUEUE_DEPTH` | `64` | integer >= 1 | Accepted-but-unserved connections buffered before new ones get 503. | DASH only. |
| `DASH_CONTROL_PLANE_MAX_BODY_BYTES` | 8388608 (8 MiB) | integer >= 1 | Maximum accepted `Content-Length`. The header block is capped at 16 KiB. | DASH only. |
| `DASH_CONTROL_PLANE_READ_TIMEOUT_MS` | `5000` | milliseconds >= 1 | Timeout for any single read. | DASH only. |
| `DASH_CONTROL_PLANE_REQUEST_DEADLINE_MS` | `10000` | milliseconds >= 1 | Total time allowed to receive one request. | DASH only. |
| `DASH_CONTROL_PLANE_WRITE_TIMEOUT_MS` | `5000` | milliseconds >= 1 | Timeout for any single write. | DASH only. |

All six server settings are DASH only and the server ignores values of 0 or unparseable values; startup validation reports them as errors instead.

### State and leader lease

| Variable | Default | Type | Description | Notes |
|---|---|---|---|---|
| `DASH_CONTROL_PLANE_NODE_ID` | none (required; dev mode falls back to `control-plane-<pid>`) | string | Unique, stable node id used in leader election. Startup fails without it unless `DASH_INSECURE_DEV_MODE=1`. |  |
| `DASH_CONTROL_PLANE_LEASE_RESET` | ``0`` | bool | Set to `1` for one start to discard a forged or corrupt lease record (or epoch sidecar) that the node otherwise refuses to run with. Remove it afterwards. | Enabled by only the literal `1`. |
| `DASH_CONTROL_PLANE_STATE_PATH` | unset (state not persisted) | path | Persisted placement CSV. |  |
| `DASH_CONTROL_PLANE_STATE_SHA256_PATH` | unset | path | Checksum file for the persisted state; verified at startup (mismatch exits with code 2). |  |
| `DASH_CONTROL_PLANE_LEASE_PATH` | unset (standalone, always leader) | path | File lease for leader election. |  |
| `DASH_CONTROL_PLANE_LEASE_DURATION_MS` | `30000` | milliseconds | Lease length. |  |
| `DASH_CONTROL_PLANE_LEASE_RENEWAL_MS` | `10000` | milliseconds | Renewal interval. |  |
| `DASH_CONTROL_PLANE_LEASE_SAFETY_MARGIN_MS` | `1000` | milliseconds | Clock-skew margin: a leader stops reporting leadership this long before the lease expires, and other nodes wait this long after expiry before taking over (capped at half the lease duration). |  |

## Benchmarks and load tests

`DASH_BENCH_*` (benchmark thresholds and fixtures in `tests/benchmarks`), `DASH_LIVE_URL` and `DASH_API_KEY` (load-test binary) are read only by the benchmark binaries. See the comments at the top of `tests/benchmarks/src/main.rs` and `tests/benchmarks/src/bin/load_test.rs`.

### Load test

| Variable | Default | Type | Description | Notes |
|---|---|---|---|---|
| `DASH_LIVE_URL` | `http://127.0.0.1:8080` | URL | Base URL the load-test binary drives. |  |
| `DASH_API_KEY` | unset | secret | API key the load-test binary sends (`--api-key` overrides). |  |

### Benchmarks

| Variable | Default | Type | Description | Notes |
|---|---|---|---|---|
| `DASH_BENCH_FIXTURE_SIZE` | per profile | integer >= 1 | Override the fixture size of the benchmark profile. |  |
| `DASH_BENCH_MIN_ITERATIONS` | `5` | integer >= 1 | Minimum iterations for a valid benchmark run. |  |
| `DASH_BENCH_GUARD_MIN_ITERATIONS` | `5` | integer >= 1 | Minimum iterations required by the history regression guard. |  |
| `DASH_BENCH_HISTORY_CSV_OUT` | unset | path | Write the benchmark history as CSV to this path. |  |
| `DASH_BENCH_ANN_MAX_NEIGHBORS_BASE` | `16` | integer >= 1 | Vector index tuning used by the benchmark store (HNSW connectivity). |  |
| `DASH_BENCH_ANN_EXPANSION_ADD` | `128` | integer >= 1 | Vector index tuning used by the benchmark store (HNSW construction beam). |  |
| `DASH_BENCH_ANN_SEARCH_EXPANSION_MIN` | `128` | integer >= 1 | Vector index tuning used by the benchmark store (HNSW search beam floor). |  |
| `DASH_BENCH_VECTOR_FLAT_THRESHOLD` | `8192` | integer >= 1 | Vector index tuning used by the benchmark store (flat-to-HNSW threshold). |  |
| `DASH_BENCH_VECTOR_RERANK` | `50` | integer | Vector index tuning used by the benchmark store (exact rerank width; 0 disables). |  |
| `DASH_BENCH_LARGE_MIN_CANDIDATE_REDUCTION_PCT` | `95.0` | number | Gate for the `large` profile: minimum candidate reduction in percent. |  |
| `DASH_BENCH_LARGE_MAX_DASH_LATENCY_MS` | `120.0` | number | Gate for the `large` profile: maximum average latency in milliseconds. |  |
| `DASH_BENCH_LARGE_MIN_ANN_RECALL_AT_100` | `0.98` | number | Gate for the `large` profile: minimum ANN recall at 100. |  |
| `DASH_BENCH_XLARGE_MIN_CANDIDATE_REDUCTION_PCT` | `96.0` | number | Gate for the `xlarge` profile: minimum candidate reduction in percent. |  |
| `DASH_BENCH_XLARGE_MAX_DASH_LATENCY_MS` | `250.0` | number | Gate for the `xlarge` profile: maximum average latency in milliseconds. |  |
| `DASH_BENCH_XLARGE_MIN_ANN_RECALL_AT_100` | `0.98` | number | Gate for the `xlarge` profile: minimum ANN recall at 100. |  |
| `DASH_BENCH_XXLARGE_MIN_CANDIDATE_REDUCTION_PCT` | `97.0` | number | Gate for the `xxlarge` profile: minimum candidate reduction in percent. |  |
| `DASH_BENCH_XXLARGE_MAX_DASH_LATENCY_MS` | `350.0` | number | Gate for the `xxlarge` profile: maximum average latency in milliseconds. |  |
| `DASH_BENCH_XXLARGE_MIN_ANN_RECALL_AT_100` | `0.98` | number | Gate for the `xxlarge` profile: minimum ANN recall at 100. |  |
| `DASH_BENCH_LARGE_PLUS_MIN_GRAPH_SCORE_COVERAGE` | `1.0` | number | Gate for the large profiles: minimum graph score coverage. |  |
| `DASH_BENCH_LARGE_PLUS_MIN_GRAPH_SUPPORT_PATH_COUNT` | `1` | integer >= 1 | Gate for the large profiles: minimum support path count. |  |
| `DASH_BENCH_LARGE_PLUS_MIN_GRAPH_CONTRADICTION_CHAIN_DEPTH` | `2` | integer >= 1 | Gate for the large profiles: minimum contradiction chain depth. |  |
| `DASH_BENCH_MIN_SEGMENT_REFRESH_SUCCESSES` | `0` | integer | Gate: minimum successful segment cache refreshes. |  |
| `DASH_BENCH_MIN_SEGMENT_CACHE_HITS` | `0` | integer | Gate: minimum segment cache hits. |  |
| `DASH_BENCH_REQUIRE_VECTOR_BACKEND` | unset | `cpu` \| `gpu` | Fail the run unless the store reports this vector backend. |  |
| `DASH_BENCH_WAL_SCALE_CLAIMS` | per profile | integer >= 1 | Claims seeded for the WAL scale measurement (10000 large, 20000 xlarge, 50000 xxlarge, otherwise 5000; capped at the fixture size). |  |

## Config reload and SIGHUP

On unix, `kill -HUP <pid>` makes `ingestion` and `retrieval` rebuild their whole authentication policy from the process environment, overlaid with the file named by `DASH_CONFIG_RELOAD_FILE`. The overlay holds `KEY=VALUE` lines (blank lines and `#` comments ignored, optional quotes stripped), only `DASH_*` / `EME_*` keys are read, and an overlay value wins over the process environment. The file is also read at startup. A new policy that fails validation is rejected and the previous one stays in force. Only authentication settings are reloaded. Details: `docs/operations/auth.md`.

Values that come from `DASH_CONFIG_FILE` (the TOML file) are applied to the process environment once at startup, so a SIGHUP reload sees them as ordinary environment variables; the file is not re-read.

## Variables that do not exist

Earlier versions of this page documented the following. No code reads them; setting them does nothing (startup validation reports them as unknown variables).

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

The same settings can live in a TOML file instead; see [Configuration file](../operations/configuration-file.md). To check a configuration without starting a service, run `dash-config validate --service ingestion`.

For the full deployment story, see [Deploy](../operations/deploy.md).
