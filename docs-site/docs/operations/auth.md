# Authentication and authorization (retrieval and ingestion)

!!! note
    This page is a copy of [`docs/operations/auth.md`](https://github.com/BHAWESHBHASKAR/DASH/blob/main/docs/operations/auth.md) in the repository, which is canonical.

This page describes what the `retrieval` and `ingestion` services enforce
today. In every variable name below, `*` is `RETRIEVAL` or `INGEST`
(`DASH_RETRIEVAL_*`, `DASH_INGEST_*`; the legacy `EME_*` names still work as
a fallback). The implementation is `services/common/src/policy.rs` and
`pkg/auth`.

## Decision order

1. No authentication configured: startup fails unless
   `DASH_INSECURE_DEV_MODE=1`.
2. A JWT-shaped bearer token (three dot-separated parts) is judged by the JWT
   verifier (HS256, or OIDC when `DASH_*_JWT_PROVIDER=oidc`). It never falls
   through to API-key matching.
3. Otherwise the `x-api-key` header (or a non-JWT bearer token) is matched
   against scoped keys, then legacy keys. Revoked keys are rejected.
4. The credential's roles must allow the route's required role (403 if not).
5. The service tenant allowlist and the per-tenant rate limit (429) apply.

## Roles

| Role        | Grants                                                         |
|-------------|----------------------------------------------------------------|
| `admin`     | every role                                                     |
| `ingest`    | ingest routes only                                             |
| `retrieve`  | retrieve routes only                                           |
| `read_only` | `retrieve` and the debug/metrics read routes; never `ingest`   |

`ingest` and `retrieve` are independent. Neither implies `read_only`, so a
credential that needs `/metrics` or `/debug/*` must carry `read_only` (or
`admin`), or `DASH_METRICS_PUBLIC=1` can expose `/metrics` alone.

Required role per route:

| Route                                                            | Role        |
|------------------------------------------------------------------|-------------|
| retrieval `/v1/retrieve`, `/v1/embeddings`                       | `retrieve`  |
| retrieval `/debug/*`, `/metrics`; ingestion `/debug/*`, `/metrics` | `read_only` |
| ingestion `/v1/ingest`, `/v1/ingest/raw`, `/v1/ingest/document`, `/v1/ingest/batch` | `ingest` |

## Where roles come from

| Credential                          | Roles                                                                                     |
|-------------------------------------|-------------------------------------------------------------------------------------------|
| Scoped key `key:tenants[:roles]`    | the listed roles; when omitted, `DASH_*_API_KEY_DEFAULT_ROLES`                            |
| Legacy key (`API_KEY`, `API_KEYS`)  | `DASH_*_API_KEY_DEFAULT_ROLES`; default is the service primary role (`retrieve` for retrieval, `ingest` for ingestion) |
| JWT / OIDC token                    | the role claim; when the claim is absent, `DASH_*_JWT_DEFAULT_ROLES`; default is **no roles** |

A role claim that is present but not usable (wrong type, only unknown names)
grants nothing, even when a default is configured. The claim may be an array
of strings, or a string separated by commas and/or spaces. Unknown role names
are ignored in claims and are a startup error in the `*_DEFAULT_ROLES`
settings.

## Environment variables

| Variable | Default | Meaning |
|----------|---------|---------|
| `DASH_*_API_KEY`, `DASH_*_API_KEYS` | unset | legacy unscoped keys |
| `DASH_*_API_KEY_SCOPES` | unset | `key:tenant1,tenant2[:role1,role2];...` |
| `DASH_*_API_KEY_DEFAULT_ROLES` | service primary role | roles for legacy keys and scoped keys without roles |
| `DASH_*_REVOKED_API_KEYS`, `DASH_*_REVOKED_KEYS_PATH` | unset | revoked keys (list / file, file reloaded when mtime or size changes, checked at most once per second) |
| `DASH_*_JWT_HS256_SECRET`, `_SECRETS`, `_SECRETS_BY_KID` | unset | HS256 signing secrets; with strict secrets (the default) each must be at least 32 characters and not a placeholder |
| `DASH_STRICT_SECRETS` | on | strict validation; API keys, scoped keys and the replication token need at least 16 characters and no placeholder text. Only `DASH_STRICT_SECRETS=0` together with `DASH_INSECURE_DEV_MODE=1` turns it off |
| `DASH_*_JWT_ISSUER`, `DASH_*_JWT_AUDIENCE` | unset (HS256), **required** (OIDC) | expected `iss` / `aud` |
| `DASH_*_JWT_ROLES_CLAIM` (alias `DASH_*_JWT_ROLE_CLAIM`) | `dash_roles` | name of the role claim |
| `DASH_*_JWT_DEFAULT_ROLES` | none | comma/space list of roles for tokens without the role claim |
| `DASH_*_JWT_ALLOW_WILDCARD_TENANT` | `0` | honor a `"*"` tenant in a token |
| `DASH_*_JWT_MAX_LIFETIME_SECS` | `86400` | maximum `exp - iat` (or `exp - now` when `iat` is absent) |
| `DASH_*_JWT_LEEWAY_SECS` | `0` | clock skew allowance, capped at 60 |
| `DASH_*_JWT_REQUIRE_EXP` | n/a | ignored; `exp` is always required (a false value logs a warning) |
| `DASH_*_JWT_REVOKED_JTIS` | unset | comma separated `jti` values to reject |
| `DASH_*_JWT_REVOKED_JTIS_PATH` | unset | file with one revoked `jti` per line, reloaded like the key revocation file |
| `DASH_*_JWT_PROVIDER` | `hs256` | `hs256` or `oidc` |
| `DASH_*_JWT_JWKS_URL` | unset | OIDC JWKS URL (required for `oidc`) |
| `DASH_*_JWT_JWKS_REFRESH_MINUTES` | `15` | JWKS cache lifetime |
| `DASH_*_JWT_TENANT_CLAIMS` | `tenant_id,tenants,tenant_ids` (OIDC) | claims holding the tenant list |
| `DASH_OIDC_ALLOW_INSECURE_JWKS` | `0` | allow a plain `http://` JWKS URL on a non-loopback host |
| `DASH_INSECURE_DEV_MODE` | `0` | local development only |
| `DASH_INSECURE_DEV_MODE_ALLOW_NON_LOOPBACK` | `0` | with dev mode, allow a non-loopback bind |
| `DASH_CONFIG_RELOAD_FILE` | unset | `KEY=VALUE` overlay file, see Reload |
| `DASH_*_RATE_LIMIT_PER_TENANT_RPS`, `DASH_*_RATE_LIMIT_BURST` | per service | per-tenant token bucket (`rps=0` disables) |
| `DASH_METRICS_PUBLIC` | `0` | expose `/metrics` without credentials |

## JWT checks (HS256 and OIDC)

* `exp` is always required. A token is rejected when its lifetime exceeds
  `DASH_*_JWT_MAX_LIFETIME_SECS`; an `iat` in the future (beyond leeway) is
  rejected as not yet valid.
* A `"*"` tenant in the token matches every tenant only with
  `DASH_*_JWT_ALLOW_WILDCARD_TENANT=1`; otherwise it matches nothing.
* A token whose `jti` is on the denylist gets 401.
* Error responses use fixed messages and never repeat claim values.

## OIDC

* Startup fails when `JWKS_URL`, `ISSUER` or `AUDIENCE` is missing, or when
  the JWKS URL is `http://` to a non-loopback host (unless
  `DASH_OIDC_ALLOW_INSECURE_JWKS=1`). `iss`, `aud` and `exp` are required in
  every token.
* The JWT header is parsed first. Only `RS256/384/512`, `PS256/384/512`,
  `ES256/384` and `EdDSA` are accepted, and a `kid` is required. Garbage or
  disallowed tokens never trigger a JWKS fetch.
* When a JWK has its own `alg` it must equal the token's `alg`; a JWK with
  `use` other than `sig` is not used for verification.
* Fetch: 3 second total timeout, 256 KiB response cap, redirects are not
  followed.
* One fetch at a time per URL. While one is running, other requests use the
  cached keys (or wait up to about 4 seconds if there are none).
* When the cache is older than the refresh interval and the refresh fails,
  cached keys are served for up to 24 hours. A failed fetch is remembered for
  30 seconds. A token with an unknown `kid` forces at most one refresh per 60
  seconds.
* When no usable keys exist and the IdP is unreachable the decision is 401
  ("OIDC provider unreachable").

## Dev mode bind

With `DASH_INSECURE_DEV_MODE=1`, retrieval and ingestion bind to
`127.0.0.1:<port>` whatever host `DASH_RETRIEVAL_BIND` / `DASH_INGEST_BIND`
names, and log a warning. `DASH_INSECURE_DEV_MODE_ALLOW_NON_LOOPBACK=1`
disables the override. The control plane keeps its own rule (it forces
loopback only when no token is configured).

## Reload (SIGHUP)

On unix, sending SIGHUP to `retrieval` or `ingestion` rebuilds the whole
authentication policy from the process environment, overlaid with
`DASH_CONFIG_RELOAD_FILE` when set. The overlay file holds `KEY=VALUE` lines
(blank lines and `#` comments ignored, optional surrounding quotes
stripped). Only `DASH_*` / `EME_*` keys are read, and an overlay value wins
over the environment (an empty value clears the setting). The file is also
read at startup. Use `DASH_*` names in the file.

* A new policy that fails validation is rejected, logged without any key
  material, and the previous policy stays in force.
* The rate-limit buckets start fresh after a reload.
* Only authentication settings are reloaded; other service settings
  (bind address, storage, workers) need a restart.
* Independently of SIGHUP, the API-key revocation file and the `jti` file are
  re-read when their mtime or size changes.
* Windows: SIGHUP reload is not available.
