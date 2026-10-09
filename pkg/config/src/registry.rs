//! The registry: one row for every setting DASH reads.
//!
//! This table is the single source of truth. The configuration reference page,
//! the startup validator, the TOML file overlay and the "known variable" check
//! are all derived from it, and the test `registry_covers_every_env_var_read_by_code`
//! keeps it in step with the Rust sources.
//!
//! Conventions:
//! * Names are canonical `DASH_*` names. `{SVC}` is a per-service pattern that
//!   expands to `INGEST` and `RETRIEVAL` (these are the names the shared auth
//!   policy and the audit options build at runtime).
//! * `.eme()` records that the code also reads the legacy `EME_` twin.
//! * Defaults are text and were checked against the code that reads them.
//! * Add a row here when you add an environment read; the coverage test fails
//!   otherwise.

use crate::model::{ALL_SERVICES, Alias, DATA, Entry, Honors, INGEST, Kind, Scope};
use Scope::{Common, ControlPlane, Ingestion, Retrieval, Tools};

// ---- topics (docs sub-headings) -------------------------------------------
pub const T_STARTUP: &str = "Startup, secrets and logging";
pub const T_TRANSPORT: &str = "Network and transport";
pub const T_AUTH: &str = "Authentication and authorization";
pub const T_JWT: &str = "JWT (HS256) and OIDC";
pub const T_REPL_AUTH: &str = "Metrics and replication/control-plane authentication";
pub const T_AUDIT: &str = "Audit log";
pub const T_WAL: &str = "Persistence and WAL";
pub const T_REPLICATION: &str = "Replication follower";
pub const T_PLACEMENT: &str = "Placement and routing";
pub const T_ANN: &str = "ANN tuning";
pub const T_RETRIEVAL: &str = "Retrieval tuning and request bounds";
pub const T_SEGMENTS: &str = "Segments and maintenance";
pub const T_EMBEDDING: &str = "Embedding providers";
pub const T_EXTRACTION: &str = "Extraction and parsing";
pub const T_SERVER: &str = "HTTP server";
pub const T_LEASE: &str = "State and leader lease";
pub const T_ENCRYPTION: &str = "Encryption (library only)";
pub const T_CONTAINER: &str = "Container and compose variables";
pub const T_BENCH: &str = "Benchmarks";
pub const T_LOAD: &str = "Load test";

const ROLE_CLAIM_ALIAS: &[Alias] = &[Alias {
    name: "DASH_{SVC}_JWT_ROLE_CLAIM",
    deprecated: true,
    eme: true,
}];
const ANN_BASE: &[Alias] = &[Alias::shared("DASH_ANN_MAX_NEIGHBORS_BASE")];
const ANN_MIN: &[Alias] = &[Alias::shared("DASH_ANN_SEARCH_EXPANSION_MIN")];
const ANN_ADD: &[Alias] = &[Alias {
    name: "DASH_ANN_EXPANSION_ADD",
    deprecated: false,
    eme: false,
}];
const VEC_FLAT: &[Alias] = &[Alias {
    name: "DASH_VECTOR_FLAT_THRESHOLD",
    deprecated: false,
    eme: false,
}];
const VEC_RERANK: &[Alias] = &[Alias {
    name: "DASH_VECTOR_RERANK",
    deprecated: false,
    eme: false,
}];
const SEG_SIZE: &[Alias] = &[Alias::shared("DASH_SEGMENT_MAX_SEGMENT_SIZE")];
const SEG_TIER: &[Alias] = &[Alias::shared("DASH_SEGMENT_MAX_SEGMENTS_PER_TIER")];
const SEG_COMPACT: &[Alias] = &[Alias::shared("DASH_SEGMENT_MAX_COMPACTION_INPUT_SEGMENTS")];
const OLLAMA_ALIAS: &[Alias] = &[Alias::deprecated("DASH_OLLAMA_BASE_URL")];

const READ_PREFERENCE: Kind = Kind::Enum {
    values: &["any_healthy", "leader_only", "prefer_follower"],
};
const EXTRACTION_PROVIDER: Kind = Kind::Enum {
    values: &["rule_sentence", "rule", "adapter_command", "model_adapter"],
};
const PARSER_PROVIDER: Kind = Kind::Enum {
    values: &[
        "builtin_utf8",
        "utf8",
        "adapter_command",
        "document_adapter",
    ],
};
const INGEST_EMBEDDING_PROVIDER: Kind = Kind::Enum {
    values: &[
        "hash_vector",
        "hash",
        "builtin_hash",
        "off",
        "none",
        "disabled",
        "adapter_command",
        "model_adapter",
    ],
};

/// Every setting the code reads, in documentation order.
pub static REGISTRY: &[Entry] = &[
    // =====================================================================
    // Common
    // =====================================================================
    // ---- startup, secrets, logging
    Entry::new(
        "DASH_INSECURE_DEV_MODE",
        Common,
        T_STARTUP,
        Kind::BOOL,
        "off",
        "Allows a service to start with **no credentials configured**, and with them accept every request. Also forces a loopback bind (see `DASH_INSECURE_DEV_MODE_ALLOW_NON_LOOPBACK`) and allows an unset replication token. The control plane accepts only the literal `1` and, with no token configured, binds loopback only. Never set it in production.",
    )
    .readers(ALL_SERVICES),
    Entry::new(
        "DASH_INSECURE_DEV_MODE_ALLOW_NON_LOOPBACK",
        Common,
        T_STARTUP,
        Kind::BOOL,
        "off",
        "With dev mode on, keep the requested non-loopback bind host instead of replacing it with `127.0.0.1`.",
    ),
    Entry::new(
        "DASH_STRICT_SECRETS",
        Common,
        T_STARTUP,
        Kind::BOOL,
        "**on**",
        "Strict secret validation is on by default. It can only be turned off by setting `DASH_STRICT_SECRETS=0` **and** `DASH_INSECURE_DEV_MODE=1`. When on, the service exits (code 2) if a configured secret is empty, a placeholder (contains `change-me`, `changeme`, `placeholder`, `replace-me`, `example`, `sample`, `<...>` markers, or starts with `secret`/`password`) or too short. Minimum lengths: **16** characters for API keys, scoped keys and replication tokens; **32** for HS256 JWT secrets and the control-plane token. Error messages name the setting, never the value.",
    )
    .readers(ALL_SERVICES),
    Entry::new(
        "DASH_LOG_FORMAT",
        Common,
        T_STARTUP,
        Kind::Str,
        "compact text",
        "`json` switches log output to JSON lines. Any other value uses the compact text format.",
    ),
    Entry::new(
        "RUST_LOG",
        Common,
        T_STARTUP,
        Kind::Str,
        "info",
        "Standard `tracing` env filter.",
    )
    .external(),
    Entry::new(
        "DASH_CONFIG_RELOAD_FILE",
        Common,
        T_STARTUP,
        Kind::Path,
        "",
        "Path of a `KEY=VALUE` overlay file for authentication settings; see [Config reload and SIGHUP](#config-reload-and-sighup).",
    ),
    Entry::new(
        "DASH_CONFIG_FILE",
        Common,
        T_STARTUP,
        Kind::Path,
        "",
        "Path of a TOML file whose values fill settings that are not set in the environment (environment wins). See [Configuration file](../operations/configuration-file.md).",
    )
    .readers(ALL_SERVICES),
    Entry::new(
        "DASH_CONFIG_VALIDATION",
        Common,
        T_STARTUP,
        Kind::Enum {
            values: &["error", "warn"],
        },
        "error",
        "Startup validation mode. With `error` a malformed value or a bad configuration file stops the service with exit code 2; `warn` downgrades those errors to warnings and starts anyway. See [Configuration file](../operations/configuration-file.md).",
    )
    .readers(ALL_SERVICES),
    // ---- transport (shared)
    Entry::new(
        "DASH_HTTP_REQUEST_TIMEOUT_MS",
        Common,
        T_TRANSPORT,
        Kind::POSITIVE_MILLIS,
        "10000",
        "Whole-request deadline for reading one request (headers plus body), measured from accept (queue wait counts). A slow client gets 408 and is dropped; connections that already waited it out in the queue are closed without work.",
    ),
    Entry::new(
        "DASH_HTTP_FIRST_BYTE_TIMEOUT_MS",
        Common,
        T_TRANSPORT,
        Kind::POSITIVE_MILLIS,
        "2000",
        "A new connection that sends nothing within this time is closed before it reaches a worker.",
    ),
    Entry::new(
        "DASH_HTTP_MAX_CONNS_PER_IP",
        Common,
        T_TRANSPORT,
        Kind::UINT,
        "64",
        "Concurrent connections allowed per client IP; excess connections get 503. `0` disables the cap. Raise it (or set `0`) behind a load balancer that presents a single source IP.",
    ),
    // ---- authentication (per-service patterns)
    Entry::new(
        "DASH_{SVC}_API_KEY",
        Common,
        T_AUTH,
        Kind::Secret,
        "",
        "A single accepted API key (a \"legacy\" unscoped key).",
    )
    .eme(),
    Entry::new(
        "DASH_{SVC}_API_KEYS",
        Common,
        T_AUTH,
        Kind::SecretList { sep: ',' },
        "",
        "Comma-separated accepted API keys (legacy unscoped keys).",
    )
    .eme(),
    Entry::new(
        "DASH_{SVC}_API_KEY_DEFAULT_ROLES",
        Common,
        T_AUTH,
        Kind::CSV,
        "",
        "Roles granted to legacy unscoped keys and to scoped keys that list no roles. Comma or space separated list of `admin`, `ingest`, `retrieve`, `read_only`; an unknown name stops startup.",
    )
    .svc_default("ingest", "retrieve")
    .notes("Default is the service's primary role.")
    .eme(),
    Entry::new(
        "DASH_{SVC}_API_KEY_SCOPES",
        Common,
        T_AUTH,
        Kind::SecretList { sep: ';' },
        "",
        "Scoped keys, entries separated by `;`, each `key:tenantA,tenantB[:role1,role2]`. A tenant of `*` means all tenants. A scoped key need not also appear in `..._API_KEYS`.",
    )
    .eme(),
    Entry::new(
        "DASH_{SVC}_ALLOWED_TENANTS",
        Common,
        T_AUTH,
        Kind::CSV,
        "any tenant",
        "Comma-separated tenant allowlist applied to every authenticated request (403 otherwise); `*` means any. Leave it unset for any tenant: a value that is set but empty or only separators is a startup error.",
    )
    .reject_blank()
    .eme(),
    Entry::new(
        "DASH_{SVC}_REVOKED_API_KEYS",
        Common,
        T_AUTH,
        Kind::SecretList { sep: ',' },
        "",
        "Comma-separated keys rejected with 401 `API key revoked`.",
    )
    .eme(),
    Entry::new(
        "DASH_{SVC}_REVOKED_KEYS_PATH",
        Common,
        T_AUTH,
        Kind::Path,
        "",
        "File with one revoked key per line. Re-read when its mtime or size changes (checked at most once per second).",
    )
    .eme(),
    Entry::new(
        "DASH_{SVC}_RATE_LIMIT_PER_TENANT_RPS",
        Common,
        T_AUTH,
        Kind::UINT,
        "100",
        "Token-bucket refill rate per credential and route class (data, embeddings, ops); the tenant joins the key only for keys bound to a fixed tenant set, so a wildcard key cannot gain buckets by rotating tenant ids. `0` disables limiting. Applies to API keys, JWTs and OIDC alike. Excess requests get HTTP 429 with a `Retry-After` header. At most 50,000 buckets are kept.",
    )
    .svc_default("100", "500")
    .eme(),
    Entry::new(
        "DASH_{SVC}_RATE_LIMIT_BURST",
        Common,
        T_AUTH,
        Kind::UINT,
        "200",
        "Bucket size. Never below the rate.",
    )
    .svc_default("200", "1000")
    .eme(),
    // ---- JWT / OIDC
    Entry::new(
        "DASH_{SVC}_JWT_HS256_SECRET",
        Common,
        T_JWT,
        Kind::Secret,
        "",
        "Primary HS256 secret. HS256 validation is active when this, `..._SECRETS` or `..._SECRETS_BY_KID` is set. An empty key is never accepted.",
    )
    .eme(),
    Entry::new(
        "DASH_{SVC}_JWT_HS256_SECRETS",
        Common,
        T_JWT,
        Kind::SecretList { sep: ',' },
        "",
        "Comma-separated secrets accepted during rotation.",
    )
    .eme(),
    Entry::new(
        "DASH_{SVC}_JWT_HS256_SECRETS_BY_KID",
        Common,
        T_JWT,
        Kind::SecretList { sep: ';' },
        "",
        "`kid:secret;kid2:secret2`, selected by the token's `kid` header.",
    )
    .eme(),
    Entry::new(
        "DASH_{SVC}_JWT_ISSUER",
        Common,
        T_JWT,
        Kind::Str,
        "unset (HS256), **required** (OIDC)",
        "Required `iss`.",
    )
    .eme(),
    Entry::new(
        "DASH_{SVC}_JWT_AUDIENCE",
        Common,
        T_JWT,
        Kind::Str,
        "unset (HS256), **required** (OIDC)",
        "Required `aud`.",
    )
    .eme(),
    Entry::new(
        "DASH_{SVC}_JWT_LEEWAY_SECS",
        Common,
        T_JWT,
        Kind::UINT,
        "0",
        "Clock skew allowance, capped at 60.",
    )
    .eme(),
    Entry::new(
        "DASH_{SVC}_JWT_MAX_LIFETIME_SECS",
        Common,
        T_JWT,
        Kind::POSITIVE,
        "86400",
        "Maximum `exp - iat` (or `exp - now` when `iat` is absent). Longer-lived tokens are rejected.",
    )
    .eme(),
    Entry::new(
        "DASH_{SVC}_JWT_REQUIRE_EXP",
        Common,
        T_JWT,
        Kind::BOOL,
        "n/a",
        "**Ignored.** `exp` is always required; setting it to a false value only logs a warning at startup.",
    )
    .eme(),
    Entry::new(
        "DASH_{SVC}_JWT_ROLES_CLAIM",
        Common,
        T_JWT,
        Kind::Str,
        "dash_roles",
        "Name of the claim carrying roles.",
    )
    .aliases(ROLE_CLAIM_ALIAS)
    .notes("The older name `DASH_{SVC}_JWT_ROLE_CLAIM` is still read.")
    .eme(),
    Entry::new(
        "DASH_{SVC}_JWT_DEFAULT_ROLES",
        Common,
        T_JWT,
        Kind::CSV,
        "none",
        "Roles for tokens that **lack** the role claim. With no default, a role-less token is authenticated but gets 403 on every role-checked route.",
    )
    .eme(),
    Entry::new(
        "DASH_{SVC}_JWT_ALLOW_WILDCARD_TENANT",
        Common,
        T_JWT,
        Kind::BOOL,
        "off",
        "Honor a `\"*\"` tenant in a token. Otherwise `\"*\"` matches nothing.",
    )
    .eme(),
    Entry::new(
        "DASH_{SVC}_JWT_REVOKED_JTIS",
        Common,
        T_JWT,
        Kind::CSV,
        "",
        "Comma-separated `jti` values rejected with 401 `JWT revoked`.",
    )
    .eme(),
    Entry::new(
        "DASH_{SVC}_JWT_REVOKED_JTIS_PATH",
        Common,
        T_JWT,
        Kind::Path,
        "",
        "File with one revoked `jti` per line, reloaded like the key revocation file.",
    )
    .eme(),
    Entry::new(
        "DASH_{SVC}_JWT_PROVIDER",
        Common,
        T_JWT,
        Kind::Enum {
            values: &["hs256", "oidc"],
        },
        "hs256",
        "`hs256` or `oidc`. Any other value stops startup.",
    )
    .blank_ok()
    .eme(),
    Entry::new(
        "DASH_{SVC}_JWT_JWKS_URL",
        Common,
        T_JWT,
        Kind::Url,
        "",
        "JWKS URL (required for `oidc`). Must be `https://`, or `http://` to a loopback host.",
    )
    .eme(),
    Entry::new(
        "DASH_{SVC}_JWT_JWKS_REFRESH_MINUTES",
        Common,
        T_JWT,
        Kind::UINT,
        "15",
        "JWKS cache lifetime.",
    )
    .eme(),
    Entry::new(
        "DASH_{SVC}_JWT_TENANT_CLAIMS",
        Common,
        T_JWT,
        Kind::CSV,
        "tenant_id,tenants,tenant_ids",
        "Comma-separated claim names that carry the tenant list (OIDC mode).",
    )
    .eme(),
    Entry::new(
        "DASH_OIDC_ALLOW_INSECURE_JWKS",
        Common,
        T_JWT,
        Kind::BOOL,
        "off",
        "Allow a plain `http://` JWKS URL on a non-loopback host. Do not use in production.",
    ),
    // ---- metrics / replication / control-plane auth
    Entry::new(
        "DASH_METRICS_PUBLIC",
        Common,
        T_REPL_AUTH,
        Kind::BOOL,
        "off",
        "Expose `/metrics` without credentials. Without it `/metrics` and `/debug/*` need a credential holding `read_only` or `admin`.",
    ),
    Entry::new(
        "DASH_INGEST_REPLICATION_TOKEN",
        Ingestion,
        T_REPL_AUTH,
        Kind::Secret,
        "",
        "Shared token checked against the `x-replication-token` header on `/internal/replication/*`. Without it the replication endpoints answer 403 (open only in dev mode). An ingestion node configured as a follower (`DASH_INGEST_REPLICATION_SOURCE_URL`) refuses to start without it outside dev mode. With strict secrets it must be a non-placeholder of at least 16 characters. A retrieval follower also falls back to this variable when `DASH_RETRIEVAL_REPLICATION_TOKEN` is unset.",
    )
    .readers(DATA)
    .eme(),
    Entry::new(
        "DASH_RETRIEVAL_REPLICATION_TOKEN",
        Retrieval,
        T_REPL_AUTH,
        Kind::Secret,
        "",
        "Token the retrieval follower sends to the source. Must equal the source's `DASH_INGEST_REPLICATION_TOKEN`.",
    )
    .eme(),
    Entry::new(
        "DASH_REPLICATION_ALLOW_INSECURE_HTTP",
        Common,
        T_REPL_AUTH,
        Kind::Bool(Honors::One),
        "off",
        "Set to exactly `1` to let a replication follower send the replication token over plaintext `http://` to a non-loopback host. Off by default: the follower refuses (an ingestion follower refuses to start). Use an `https://` source URL behind a TLS-terminating sidecar or ingress instead; see `docs/operations/replication-security.md`.",
    )
    .readers(DATA),
    Entry::new(
        "DASH_REPLICATION_CA_FILE",
        Common,
        T_REPL_AUTH,
        Kind::Path,
        "",
        "PEM file with extra CA certificates a replication follower trusts for an `https://` source URL (a private or mesh CA). The public web roots are always trusted.",
    )
    .readers(DATA),
    Entry::new(
        "DASH_CONTROL_PLANE_TOKEN",
        ControlPlane,
        T_REPL_AUTH,
        Kind::Secret,
        "",
        "Bearer token required on every `/v1/control-plane/*` route except health and ready. The control plane refuses to start without it unless dev mode is on. The router client in ingestion and retrieval presents it when fetching placement. With strict secrets the control plane requires at least 32 characters; use a random 32-byte value.",
    )
    .readers(ALL_SERVICES)
    .eme(),
    Entry::new(
        "DASH_ROUTER_CONTROL_PLANE_TOKEN",
        Common,
        T_REPL_AUTH,
        Kind::Secret,
        "falls back to `DASH_CONTROL_PLANE_TOKEN`",
        "Token the router client sends to the control plane.",
    ),
    // ---- audit
    Entry::new(
        "DASH_{SVC}_AUDIT_LOG_PATH",
        Common,
        T_AUDIT,
        Kind::Path,
        "unset (audit off)",
        "Path of the SHA-256 hash-chained JSON-lines audit log.",
    )
    .eme(),
    Entry::new(
        "DASH_{SVC}_AUDIT_FSYNC",
        Common,
        T_AUDIT,
        Kind::BOOL,
        "`1`",
        "`fdatasync` after every audit append.",
    ),
    Entry::new(
        "DASH_{SVC}_AUDIT_FAIL_CLOSED",
        Common,
        T_AUDIT,
        Kind::BOOL,
        "`0`",
        "When on, a write to `/v1/ingest*` (ingestion) or a call to `/v1/retrieve` (retrieval) is refused with 503 if the audit log cannot be opened, locked and its tail recovered. The check is made before the mutation; the append itself still happens after it (see `docs/operations/audit-chain.md`).",
    ),
    Entry::new(
        "DASH_AUDIT_FINGERPRINT_KEY",
        Common,
        T_AUDIT,
        Kind::Secret,
        "random per process",
        "Key for the HMAC-SHA256 credential fingerprints in audit records. Set the same value on every node whose fingerprints should be comparable; without it a random key is used and a warning is logged.",
    ),
    Entry::new(
        "DASH_AUDIT_DENIAL_MAX_PER_SEC",
        Common,
        T_AUDIT,
        Kind::NON_NEGATIVE_FLOAT,
        "50",
        "Maximum denial (401/403/429) audit records per second per audit file (burst 10x); extra denials are dropped and counted in `dash_audit_denials_dropped_total`. `0` disables the limit.",
    ),
    // ---- WAL (shared)
    Entry::new(
        "DASH_WAL_REPLAY_STRICT",
        Common,
        T_WAL,
        Kind::BOOL,
        "off",
        "When on, WAL replay fails on the first record that lenient mode would quarantine, instead of quarantining it and continuing. See `docs/operations/wal-recovery.md`.",
    ),
    // ---- Vector index (patterns, shared alias)
    Entry::new(
        "DASH_{SVC}_ANN_MAX_NEIGHBORS_BASE",
        Common,
        T_ANN,
        Kind::POSITIVE,
        "16",
        "HNSW connectivity `M` (usearch `connectivity`): neighbours per node. Higher is more accurate and uses more memory. The previous in-repo graph used 12.",
    )
    .aliases(ANN_BASE)
    .eme(),
    Entry::new(
        "DASH_{SVC}_ANN_EXPANSION_ADD",
        Common,
        T_ANN,
        Kind::POSITIVE,
        "128",
        "HNSW `ef_construction` (usearch `expansion_add`): beam width while inserting a vector. Higher builds a better graph more slowly.",
    )
    .aliases(ANN_ADD),
    Entry::new(
        "DASH_{SVC}_ANN_SEARCH_EXPANSION_MIN",
        Common,
        T_ANN,
        Kind::POSITIVE,
        "128",
        "HNSW `ef_search` floor (usearch `expansion_search`): beam width while searching; it is widened to the number of requested candidates when that is larger.",
    )
    .aliases(ANN_MIN)
    .eme(),
    Entry::new(
        "DASH_{SVC}_VECTOR_FLAT_THRESHOLD",
        Common,
        T_ANN,
        Kind::POSITIVE,
        "8192",
        "A tenant with at most this many vectors is searched exactly (flat scan); above it an HNSW index is built. The same number bounds filtered searches that are scanned exactly instead of through the HNSW.",
    )
    .aliases(VEC_FLAT),
    Entry::new(
        "DASH_{SVC}_VECTOR_RERANK",
        Common,
        T_ANN,
        Kind::UINT,
        "50",
        "HNSW candidates re-scored with exact `f32` cosine after the quantised (`i8`) search. `0` returns the quantised scores unchanged.",
    )
    .aliases(VEC_RERANK),
    Entry::new(
        "DASH_VECTOR_BACKEND",
        Common,
        T_RETRIEVAL,
        Kind::Enum {
            values: &["cpu", "gpu"],
        },
        "auto",
        "`cpu`, `gpu` or unset. There is no GPU implementation: `gpu` is reported as `cpu (gpu-feature-disabled)` or, with the `gpu-backend` build feature, `cpu (gpu-unavailable)`; scoring always runs on CPU.",
    )
    .blank_ok(),
    // ---- placement (shared router settings)
    Entry::new(
        "DASH_ROUTER_PLACEMENT_FILE",
        Common,
        T_PLACEMENT,
        Kind::Path,
        "",
        "CSV of shard placements. The control plane uses it as its initial state.",
    )
    .readers(ALL_SERVICES)
    .eme(),
    Entry::new(
        "DASH_ROUTER_CONTROL_PLANE_URL",
        Common,
        T_PLACEMENT,
        Kind::Url,
        "",
        "Fetch placement from the control plane. If it is configured and unreachable, the placement file is **not** used as a fallback unless `DASH_ROUTER_ALLOW_STALE_PLACEMENT` is set.",
    )
    .eme(),
    Entry::new(
        "DASH_ROUTER_ALLOW_STALE_PLACEMENT",
        Common,
        T_PLACEMENT,
        Kind::Bool(Honors::OneTrueCaps),
        "off",
        "`1` or `true`: when the control plane is configured but unreachable, fall back to `DASH_ROUTER_PLACEMENT_FILE`. Off by default because a stale file can name a deposed leader (split-brain writes).",
    )
    .blank_ok(),
    Entry::new(
        "DASH_ROUTER_ALLOW_INSECURE_HTTP",
        Common,
        T_PLACEMENT,
        Kind::Bool(Honors::OneTrueCaps),
        "off",
        "`1` or `true`: allow the router to send `DASH_ROUTER_CONTROL_PLANE_TOKEN` over plain http to a non-loopback control plane. Off by default, so the token never crosses the network in clear text.",
    )
    .blank_ok(),
    Entry::new(
        "DASH_ROUTER_CONTROL_PLANE_CONNECT_TIMEOUT_MS",
        Common,
        T_PLACEMENT,
        Kind::POSITIVE_MILLIS,
        "2000",
        "Connect timeout of the control-plane client.",
    )
    .blank_ok(),
    Entry::new(
        "DASH_ROUTER_CONTROL_PLANE_READ_TIMEOUT_MS",
        Common,
        T_PLACEMENT,
        Kind::POSITIVE_MILLIS,
        "5000",
        "Read timeout of the control-plane client.",
    )
    .blank_ok(),
    Entry::new(
        "DASH_ROUTER_CONTROL_PLANE_WRITE_TIMEOUT_MS",
        Common,
        T_PLACEMENT,
        Kind::POSITIVE_MILLIS,
        "5000",
        "Write timeout of the control-plane client.",
    )
    .blank_ok(),
    Entry::new(
        "DASH_ROUTER_LOCAL_NODE_ID",
        Common,
        T_PLACEMENT,
        Kind::Str,
        "",
        "This node's id. Falls back to `DASH_NODE_ID`. An ingestion follower also uses it as its replica id in acknowledgements.",
    )
    .eme(),
    Entry::new(
        "DASH_NODE_ID",
        Common,
        T_PLACEMENT,
        Kind::Str,
        "",
        "Fallback for the local node id.",
    )
    .eme(),
    Entry::new(
        "DASH_ROUTER_PLACEMENT_RELOAD_INTERVAL_MS",
        Common,
        T_PLACEMENT,
        Kind::MILLIS,
        "off",
        "Reload placement on this interval (values <= 0 disable).",
    )
    .eme(),
    Entry::new(
        "DASH_ROUTER_SHARD_IDS",
        Common,
        T_PLACEMENT,
        Kind::List {
            sep: ',',
            u32_items: true,
        },
        "from placement",
        "Comma-separated shard ids override.",
    )
    .eme(),
    Entry::new(
        "DASH_ROUTER_REPLICA_COUNT",
        Common,
        T_PLACEMENT,
        Kind::POSITIVE,
        "from placement",
        "Replica count override.",
    )
    .eme(),
    Entry::new(
        "DASH_ROUTER_VIRTUAL_NODES_PER_SHARD",
        Common,
        T_PLACEMENT,
        Kind::POSITIVE,
        "64",
        "Virtual nodes for shard hashing.",
    )
    .eme(),
    // ---- embeddings (shared)
    Entry::new(
        "DASH_EMBEDDING_PROVIDER",
        Common,
        T_EMBEDDING,
        Kind::Enum {
            values: &["hash", "ollama", "openai"],
        },
        "hash",
        "`hash`, `ollama` or `openai`. Used by retrieval (`/v1/embeddings`, query embedding) and ingestion (claim embedding). Unknown values fall back to `hash` with a warning on stderr; startup validation reports them as errors.",
    ),
    Entry::new(
        "DASH_OLLAMA_ENDPOINT",
        Common,
        T_EMBEDDING,
        Kind::Url,
        "http://localhost:11434",
        "Ollama endpoint; a bare base URL is expanded to `/api/embed`.",
    )
    .aliases(OLLAMA_ALIAS)
    .notes("`DASH_OLLAMA_BASE_URL` is a deprecated alias, read when this is unset; it logs a one-time deprecation warning."),
    Entry::new(
        "DASH_OLLAMA_MODEL",
        Common,
        T_EMBEDDING,
        Kind::Str,
        "nomic-embed-text",
        "Ollama model.",
    ),
    Entry::new(
        "DASH_OPENAI_API_KEY",
        Common,
        T_EMBEDDING,
        Kind::Secret,
        "",
        "Required for `openai`; without it the provider cannot be built and the service falls back to `hash` (logged on stderr).",
    ),
    Entry::new(
        "DASH_OPENAI_MODEL",
        Common,
        T_EMBEDDING,
        Kind::Str,
        "text-embedding-3-small",
        "OpenAI model. The endpoint is fixed at `https://api.openai.com/v1/embeddings`.",
    ),
    Entry::new(
        "DASH_EMBEDDING_ALLOW_INSECURE_HTTP",
        Common,
        T_EMBEDDING,
        Kind::Bool(Honors::One),
        "off",
        "Set to exactly `1` to allow sending a bearer API key over plaintext `http://` to a non-loopback host. Off by default: the client refuses that.",
    ),
    Entry::new(
        "DASH_EMBEDDING_MAX_CONCURRENCY",
        Common,
        T_EMBEDDING,
        Kind::UINT,
        "8",
        "Concurrent provider calls per process; `0` = unlimited. When no slot frees within the queue wait the call fails with 503 `embedding_unavailable` and `Retry-After`.",
    ),
    Entry::new(
        "DASH_EMBEDDING_QUEUE_WAIT_MS",
        Common,
        T_EMBEDDING,
        Kind::MILLIS,
        "250",
        "How long a call waits for a slot.",
    ),
    Entry::new(
        "DASH_EMBEDDING_BREAKER_THRESHOLD",
        Common,
        T_EMBEDDING,
        Kind::UINT,
        "5",
        "Consecutive upstream failures that open the breaker; `0` disables it. Only transport errors, timeouts and 5xx count; 4xx, 429 and payload/dimension errors never do.",
    ),
    Entry::new(
        "DASH_EMBEDDING_BREAKER_RESET_MS",
        Common,
        T_EMBEDDING,
        Kind::POSITIVE_MILLIS,
        "10000",
        "Time before one probe call is admitted.",
    ),
    // ---- encryption (library only)
    Entry::new(
        "DASH_ENCRYPTION_PROVIDER",
        Common,
        T_ENCRYPTION,
        Kind::Enum {
            values: &["none", "env"],
        },
        "none",
        "Read by `pkg/encryption::provider_from_env`. **No service calls it**, so setting it has no effect on stored data. Encryption at rest is planned (P4).",
    )
    .readers(0)
    .blank_ok()
    .eme(),
    Entry::new(
        "DASH_ENCRYPTION_MASTER_KEY",
        Common,
        T_ENCRYPTION,
        Kind::Secret,
        "",
        "64 hex characters or base64 of 32 bytes, for provider `env`. Not used by any service yet.",
    )
    .readers(0)
    .eme(),
    // ---- container / compose (not read by Rust)
    Entry::new(
        "DASH_BIN",
        Common,
        T_CONTAINER,
        Kind::Enum {
            values: &[
                "ingestion",
                "retrieval",
                "control-plane",
                "segment-maintenance-daemon",
            ],
        },
        "retrieval",
        "Which binary the image entrypoint runs. Read by the shell scripts in `deploy/container/scripts/`, not by the Rust services.",
    )
    .readers(ALL_SERVICES)
    .external(),
    Entry::new(
        "DASH_HOME",
        Common,
        T_CONTAINER,
        Kind::Path,
        "/opt/dash",
        "Install directory inside the image. Read by the container shell scripts.",
    )
    .readers(ALL_SERVICES)
    .external(),
    Entry::new(
        "DASH_HEALTHCHECK_URL",
        Common,
        T_CONTAINER,
        Kind::Url,
        "",
        "URL probed by the container health check. Read by the container shell scripts.",
    )
    .readers(ALL_SERVICES)
    .external(),
    Entry::new(
        "DASH_PUBLISH_ADDR",
        Common,
        T_CONTAINER,
        Kind::Str,
        "127.0.0.1",
        "Host address the compose file publishes ports on. Read by docker compose, not by the services.",
    )
    .readers(ALL_SERVICES)
    .external(),
    Entry::new(
        "DASH_DEV_UID",
        Common,
        T_CONTAINER,
        Kind::UINT,
        "1000",
        "User id for the development compose stack. Read by docker compose.",
    )
    .readers(ALL_SERVICES)
    .external(),
    Entry::new(
        "DASH_DEV_GID",
        Common,
        T_CONTAINER,
        Kind::UINT,
        "1000",
        "Group id for the development compose stack. Read by docker compose.",
    )
    .readers(ALL_SERVICES)
    .external(),
    Entry::new(
        "DASH_INGEST_TRANSPORT_RUNTIME",
        Ingestion,
        T_CONTAINER,
        Kind::Str,
        "",
        "The compose file may set this; no code reads it.",
    )
    .external(),
    Entry::new(
        "DASH_RETRIEVAL_TRANSPORT_RUNTIME",
        Retrieval,
        T_CONTAINER,
        Kind::Str,
        "",
        "The compose file may set this; no code reads it.",
    )
    .external(),
    // =====================================================================
    // Ingestion
    // =====================================================================
    Entry::new(
        "DASH_INGEST_BIND",
        Ingestion,
        T_TRANSPORT,
        Kind::Addr,
        "127.0.0.1:8081",
        "Listen address (`host:port`).",
    )
    .eme(),
    Entry::new(
        "DASH_INGEST_HTTP_WORKERS",
        Ingestion,
        T_TRANSPORT,
        Kind::POSITIVE,
        "min(CPU count, 32), or 4 if undetectable",
        "Worker threads. Must be > 0.",
    )
    .eme(),
    Entry::new(
        "DASH_INGEST_HTTP_QUEUE_CAPACITY",
        Ingestion,
        T_TRANSPORT,
        Kind::POSITIVE,
        "`workers * 64`",
        "Bounded accept queue. When full, the service answers 503.",
    )
    .eme(),
    Entry::new(
        "DASH_INGEST_WAL_PATH",
        Ingestion,
        T_WAL,
        Kind::Path,
        "",
        "WAL file. When unset the ingestion service is **purely in-memory** (data is lost on restart). The compose file sets it.",
    )
    .eme(),
    Entry::new(
        "DASH_INGEST_PERSISTENCE_PATH",
        Ingestion,
        T_WAL,
        Kind::Path,
        "./data/dash-ingestion.redb",
        "redb file. Used only when a WAL path is set.",
    )
    .eme(),
    Entry::new(
        "DASH_INGEST_PERSISTENCE_DISABLE",
        Ingestion,
        T_WAL,
        Kind::Bool(Honors::OneTrueYes),
        "off",
        "Set to `1` to skip redb. If the redb file cannot be opened the service logs an error and continues in memory.",
    )
    .notes("The startup path honors only the literal `1`; the readiness probe also accepts `true` and `yes`. Use `1`.")
    .eme(),
    Entry::new(
        "DASH_INGEST_WAL_SYNC_EVERY_RECORDS",
        Ingestion,
        T_WAL,
        Kind::POSITIVE,
        "1",
        "fsync after this many records (values < 1 become 1).",
    )
    .eme(),
    Entry::new(
        "DASH_INGEST_WAL_APPEND_BUFFER_RECORDS",
        Ingestion,
        T_WAL,
        Kind::POSITIVE,
        "1",
        "Buffered records before write (values < 1 become 1).",
    )
    .eme(),
    Entry::new(
        "DASH_INGEST_WAL_SYNC_INTERVAL_MS",
        Ingestion,
        T_WAL,
        Kind::MILLIS,
        "unset",
        "Time-based fsync interval (values <= 0 are ignored).",
    )
    .eme(),
    Entry::new(
        "DASH_INGEST_WAL_BACKGROUND_FLUSH_ONLY",
        Ingestion,
        T_WAL,
        Kind::BOOL,
        "`false`",
        "Flush only from the background flusher. An unparseable value exits with code 2.",
    )
    .eme(),
    Entry::new(
        "DASH_INGEST_WAL_ASYNC_FLUSH_INTERVAL_MS",
        Ingestion,
        T_WAL,
        Kind::IntOrWords {
            words: &["auto", "off", "none", "false", "disabled", "0"],
            min: 1,
        },
        "auto (250 ms when batching is enabled, otherwise off)",
        "Positive integer, `auto`, or `off`/`none`/`false`/`disabled`/`0`. An empty value disables it. Anything else exits with code 2.",
    )
    .blank_ok()
    .eme(),
    Entry::new(
        "DASH_INGEST_ALLOW_UNSAFE_WAL_DURABILITY",
        Ingestion,
        T_WAL,
        Kind::BOOL,
        "`false`",
        "Required to run with durability settings that exceed the safe limits: sync-every or append buffer above 256 records, sync interval or async flush interval above 5000 ms, batched writes without a sync interval, or background-only flushing without an async worker. Without it the service exits with code 2.",
    )
    .eme(),
    Entry::new(
        "DASH_CHECKPOINT_MAX_WAL_RECORDS",
        Ingestion,
        T_WAL,
        Kind::POSITIVE,
        "unset",
        "Trigger a checkpoint after this many WAL records.",
    )
    .eme(),
    Entry::new(
        "DASH_CHECKPOINT_MAX_WAL_BYTES",
        Ingestion,
        T_WAL,
        Kind::POSITIVE,
        "unset",
        "Trigger a checkpoint after this many WAL bytes.",
    )
    .eme(),
    Entry::new(
        "DASH_INGEST_BATCH_MAX_ITEMS",
        Ingestion,
        T_TRANSPORT,
        Kind::POSITIVE,
        "128",
        "Maximum items in `POST /v1/ingest/batch`.",
    )
    .eme(),
    // ---- ingestion replication follower
    Entry::new(
        "DASH_INGEST_REPLICATION_SOURCE_URL",
        Ingestion,
        T_REPLICATION,
        Kind::Url,
        "unset (off)",
        "An ingestion node can itself follow another ingestion node. Requires `DASH_INGEST_REPLICATION_TOKEN`.",
    )
    .readers(INGEST)
    .eme(),
    Entry::new(
        "DASH_INGEST_REPLICATION_COMMIT_STATUS_MAX",
        Ingestion,
        T_REPLICATION,
        Kind::POSITIVE,
        "100000",
        "Leader-side cap on tracked per-commit replication status entries. Past the cap the oldest completed entries are evicted first; entries still pending quorum are never evicted. Exposed as `dash_ingest_replication_commit_status_entries` and `dash_ingest_replication_commit_status_evicted_total`.",
    ),
    Entry::new(
        "DASH_INGEST_REPLICATION_COMMIT_STATUS_TTL_SECS",
        Ingestion,
        T_REPLICATION,
        Kind::POSITIVE,
        "3600",
        "Seconds a completed (quorum met) commit status entry is kept before it expires. Pending entries do not expire. A late ack for an expired commit gets 404 and the follower ignores it.",
    ),
    Entry::new(
        "DASH_INGEST_REPLICATION_POLL_INTERVAL_MS",
        Ingestion,
        T_REPLICATION,
        Kind::POSITIVE_MILLIS,
        "500",
        "Poll interval for ingestion-to-ingestion pulls.",
    )
    .eme(),
    Entry::new(
        "DASH_INGEST_REPLICATION_MAX_RECORDS",
        Ingestion,
        T_REPLICATION,
        Kind::POSITIVE,
        "512",
        "Records per pull.",
    )
    .eme(),
    Entry::new(
        "DASH_INGEST_REPLICATION_MAX_RESPONSE_BYTES",
        Ingestion,
        T_REPLICATION,
        Kind::POSITIVE,
        "67108864 (64 MiB)",
        "Upper bound for one response body.",
    )
    .eme(),
    Entry::new(
        "DASH_INGEST_REPLICATION_MAX_BACKOFF_MS",
        Ingestion,
        T_REPLICATION,
        Kind::POSITIVE_MILLIS,
        "30000",
        "Upper bound for the failure backoff.",
    )
    .eme(),
    Entry::new(
        "DASH_INGEST_REPLICATION_MAX_LAG_RECORDS",
        Ingestion,
        T_REPLICATION,
        Kind::POSITIVE,
        "100000",
        "Readiness lag threshold.",
    )
    .eme(),
    Entry::new(
        "DASH_INGEST_REPLICATION_MAX_STALENESS_MS",
        Ingestion,
        T_REPLICATION,
        Kind::POSITIVE_MILLIS,
        "300000",
        "Readiness staleness threshold.",
    )
    .eme(),
    Entry::new(
        "DASH_INGEST_REPLICATION_OFFSET_PATH",
        Ingestion,
        T_REPLICATION,
        Kind::Path,
        "derived from the ingestion WAL path (`<wal>.replication`)",
        "File storing the last applied generation and offset.",
    )
    .blank_ok()
    .eme(),
    // ---- ingestion placement
    Entry::new(
        "DASH_INGEST_PLACEMENT_STALE_GRACE_MS",
        Ingestion,
        T_PLACEMENT,
        Kind::MILLIS,
        "30000",
        "With placement reload enabled: how long writes keep being accepted on the last known placement after reloads start failing. After the grace, writes are refused with 503 `placement is stale` until a reload succeeds.",
    ),
    // ---- ingestion segments
    Entry::new(
        "DASH_INGEST_SEGMENT_DIR",
        Ingestion,
        T_SEGMENTS,
        Kind::Path,
        "unset (segment publishing off)",
        "Segment root directory. Read by the ingestion service (segment publishing) and the `segment-maintenance-daemon` binary.",
    )
    .eme(),
    Entry::new(
        "DASH_INGEST_SEGMENT_MAX_SEGMENT_SIZE",
        Ingestion,
        T_SEGMENTS,
        Kind::POSITIVE,
        "10000",
        "Max claims per segment.",
    )
    .aliases(SEG_SIZE)
    .eme(),
    Entry::new(
        "DASH_INGEST_SEGMENT_MAX_SEGMENTS_PER_TIER",
        Ingestion,
        T_SEGMENTS,
        Kind::POSITIVE,
        "8",
        "Segments per tier before compaction.",
    )
    .aliases(SEG_TIER)
    .eme(),
    Entry::new(
        "DASH_INGEST_SEGMENT_MAX_COMPACTION_INPUT_SEGMENTS",
        Ingestion,
        T_SEGMENTS,
        Kind::Int {
            min: 2,
            max: u64::MAX,
        },
        "4",
        "Max segments merged at once (must be > 1).",
    )
    .aliases(SEG_COMPACT)
    .eme(),
    Entry::new(
        "DASH_INGEST_SEGMENT_MAINTENANCE_INTERVAL_MS",
        Ingestion,
        T_SEGMENTS,
        Kind::MILLIS,
        "30000",
        "Maintenance loop interval; `0` disables in-process maintenance.",
    )
    .eme(),
    Entry::new(
        "DASH_INGEST_SEGMENT_GC_MIN_STALE_AGE_MS",
        Ingestion,
        T_SEGMENTS,
        Kind::MILLIS,
        "60000",
        "Minimum age before an unreferenced segment file is deleted.",
    )
    .eme(),
    Entry::new(
        "DASH_INGEST_SEGMENT_MAINTENANCE_STRICT",
        Ingestion,
        T_SEGMENTS,
        Kind::Bool(Honors::OneTrueCapsYes),
        "off",
        "`segment-maintenance-daemon` only: with `--once`, exit non-zero if any tenant failed (the pass still visits every tenant).",
    )
    .eme(),
    // ---- ingestion extraction / parsing
    Entry::new(
        "DASH_INGEST_RAW_EXTRACTION_PROVIDER",
        Ingestion,
        T_EXTRACTION,
        EXTRACTION_PROVIDER,
        "rule_sentence",
        "`rule_sentence` or `adapter_command`. The adapter needs the `model-extraction-adapter` cargo feature of `ingestion`, which is off by default; without it requests fail.",
    )
    .blank_ok()
    .eme(),
    Entry::new(
        "DASH_INGEST_RAW_ADAPTER_CMD",
        Ingestion,
        T_EXTRACTION,
        Kind::Str,
        "",
        "Command (run with `sh -c`) used when the provider is `adapter_command`.",
    )
    .eme(),
    Entry::new(
        "DASH_INGEST_DOCUMENT_PARSER_PROVIDER",
        Ingestion,
        T_EXTRACTION,
        PARSER_PROVIDER,
        "builtin_utf8",
        "`builtin_utf8` or `adapter_command`.",
    )
    .blank_ok()
    .eme(),
    Entry::new(
        "DASH_INGEST_DOCUMENT_ADAPTER_CMD",
        Ingestion,
        T_EXTRACTION,
        Kind::Str,
        "",
        "Command used by the document parser adapter; receives the media type in `DASH_DOCUMENT_MIME_TYPE`.",
    )
    .eme(),
    Entry::new(
        "DASH_INGEST_EMBEDDING_PROVIDER",
        Ingestion,
        T_EXTRACTION,
        INGEST_EMBEDDING_PROVIDER,
        "hash_vector",
        "Provider for embeddings generated during raw/document ingest: `hash_vector`, `off`, or `adapter_command`.",
    )
    .blank_ok()
    .eme(),
    Entry::new(
        "DASH_INGEST_EMBEDDING_ADAPTER_CMD",
        Ingestion,
        T_EXTRACTION,
        Kind::Str,
        "",
        "Command used when the provider is `adapter_command`.",
    )
    .eme(),
    Entry::new(
        "DASH_INGEST_EMBEDDING_DIMENSIONS",
        Ingestion,
        T_EXTRACTION,
        Kind::POSITIVE,
        "64",
        "Hash vector dimensions, clamped to 8..4096.",
    )
    .eme(),
    Entry::new(
        "DASH_DOCUMENT_MIME_TYPE",
        Ingestion,
        T_EXTRACTION,
        Kind::Str,
        "",
        "Set by the service in the environment of the document adapter command (the media type of the document). Not a setting.",
    )
    .external(),
    // =====================================================================
    // Retrieval
    // =====================================================================
    Entry::new(
        "DASH_RETRIEVAL_BIND",
        Retrieval,
        T_TRANSPORT,
        Kind::Addr,
        "127.0.0.1:8080",
        "Listen address (`host:port`).",
    )
    .eme(),
    Entry::new(
        "DASH_RETRIEVAL_HTTP_WORKERS",
        Retrieval,
        T_TRANSPORT,
        Kind::POSITIVE,
        "min(CPU count, 32), or 4 if undetectable",
        "Worker threads. Must be > 0.",
    )
    .eme(),
    Entry::new(
        "DASH_RETRIEVAL_HTTP_QUEUE_CAPACITY",
        Retrieval,
        T_TRANSPORT,
        Kind::POSITIVE,
        "`workers * 64`",
        "Bounded accept queue. When full, the service answers 503.",
    )
    .eme(),
    Entry::new(
        "DASH_RETRIEVAL_WAL_PATH",
        Retrieval,
        T_WAL,
        Kind::Path,
        "",
        "Optional local WAL; replayed at startup and, when a replication follower is configured, mirrored from the leader so a restart can resume.",
    )
    .eme(),
    Entry::new(
        "DASH_RETRIEVAL_PERSISTENCE_PATH",
        Retrieval,
        T_WAL,
        Kind::Path,
        "./data/dash-retrieval.redb",
        "redb file.",
    )
    .eme(),
    Entry::new(
        "DASH_RETRIEVAL_PERSISTENCE_DISABLE",
        Retrieval,
        T_WAL,
        Kind::Bool(Honors::OneTrueYes),
        "off",
        "Set to `1` to skip redb.",
    )
    .notes("The startup path honors only the literal `1`; the readiness probe also accepts `true` and `yes`. Use `1`.")
    .eme(),
    // ---- retrieval replication follower
    Entry::new(
        "DASH_RETRIEVAL_REPLICATION_SOURCE_URL",
        Retrieval,
        T_REPLICATION,
        Kind::Url,
        "unset (follower off)",
        "Base URL of the ingestion service, for example `http://ingestion:8081`.",
    )
    .eme(),
    Entry::new(
        "DASH_RETRIEVAL_REPLICATION_POLL_INTERVAL_MS",
        Retrieval,
        T_REPLICATION,
        Kind::POSITIVE_MILLIS,
        "1000",
        "Poll interval.",
    )
    .eme(),
    Entry::new(
        "DASH_RETRIEVAL_REPLICATION_MAX_RECORDS",
        Retrieval,
        T_REPLICATION,
        Kind::POSITIVE,
        "512",
        "Records per pull (the leader caps a pull at 10000).",
    )
    .eme(),
    Entry::new(
        "DASH_RETRIEVAL_REPLICATION_MAX_RESPONSE_BYTES",
        Retrieval,
        T_REPLICATION,
        Kind::POSITIVE,
        "67108864 (64 MiB)",
        "Upper bound for one response body.",
    )
    .eme(),
    Entry::new(
        "DASH_RETRIEVAL_REPLICATION_MAX_BACKOFF_MS",
        Retrieval,
        T_REPLICATION,
        Kind::POSITIVE_MILLIS,
        "30000",
        "Upper bound for the failure backoff (the poll interval doubles per consecutive failure).",
    )
    .eme(),
    Entry::new(
        "DASH_RETRIEVAL_REPLICATION_MAX_LAG_RECORDS",
        Retrieval,
        T_REPLICATION,
        Kind::POSITIVE,
        "100000",
        "`/ready` fails with `replication_lag_exceeded` when the leader is further ahead than this.",
    )
    .eme(),
    Entry::new(
        "DASH_RETRIEVAL_REPLICATION_MAX_STALENESS_MS",
        Retrieval,
        T_REPLICATION,
        Kind::POSITIVE_MILLIS,
        "300000",
        "`/ready` fails with `replication_stale` when the last successful poll is older than this. `/ready` also fails with `replication_initial_sync_pending` until the first sync completes.",
    )
    .eme(),
    Entry::new(
        "DASH_RETRIEVAL_REPLICATION_OFFSET_PATH",
        Retrieval,
        T_REPLICATION,
        Kind::Path,
        "`<retrieval WAL path>.replication` if a retrieval WAL is set, otherwise none",
        "File storing the last applied generation and offset.",
    )
    .blank_ok()
    .eme(),
    // ---- retrieval placement / tuning
    Entry::new(
        "DASH_ROUTER_READ_PREFERENCE",
        Retrieval,
        T_PLACEMENT,
        READ_PREFERENCE,
        "any_healthy",
        "Replica read preference: `any_healthy`, `leader_only` or `prefer_follower`. Any other value is a configuration error.",
    )
    .blank_ok()
    .eme(),
    Entry::new(
        "DASH_RETRIEVAL_MAX_TOP_K",
        Retrieval,
        T_RETRIEVAL,
        Kind::POSITIVE,
        "1000",
        "Upper bound for `top_k` on `/v1/retrieve`; larger values get 400.",
    ),
    Entry::new(
        "DASH_RETRIEVAL_GRAPH_MAX_HOPS",
        Retrieval,
        T_RETRIEVAL,
        Kind::POSITIVE,
        "3",
        "Graph expansion depth (must be > 0).",
    )
    .eme(),
    Entry::new(
        "DASH_RETRIEVAL_GRAPH_EDGE_DEPTH_DECAY",
        Retrieval,
        T_RETRIEVAL,
        Kind::ANY_FLOAT,
        "0.75",
        "Per-hop weight decay, clamped to 0..1.",
    )
    .eme(),
    Entry::new(
        "DASH_RETRIEVAL_GRAPH_SUPPORT_PATH_BONUS",
        Retrieval,
        T_RETRIEVAL,
        Kind::ANY_FLOAT,
        "0.16",
        "Score bonus per support path (negative values are treated as 0).",
    )
    .eme(),
    Entry::new(
        "DASH_RETRIEVAL_GRAPH_CONTRADICTION_DEPTH_PENALTY",
        Retrieval,
        T_RETRIEVAL,
        Kind::ANY_FLOAT,
        "0.20",
        "Score penalty for contradiction chains (negative values are treated as 0).",
    )
    .eme(),
    Entry::new(
        "DASH_RETRIEVAL_SEGMENT_DIR",
        Retrieval,
        T_RETRIEVAL,
        Kind::Path,
        "",
        "Directory of published index segments to prefilter from.",
    )
    .eme(),
    Entry::new(
        "DASH_RETRIEVAL_SEGMENT_CACHE_REFRESH_MS",
        Retrieval,
        T_RETRIEVAL,
        Kind::POSITIVE_MILLIS,
        "1000",
        "Segment cache refresh interval (must be > 0).",
    )
    .eme(),
    Entry::new(
        "DASH_RETRIEVAL_DISK_NATIVE_SEGMENT_EXECUTION",
        Retrieval,
        T_RETRIEVAL,
        Kind::BOOL,
        "`true`",
        "Execute against disk segments natively.",
    )
    .eme(),
    Entry::new(
        "DASH_RETRIEVAL_STORAGE_DIVERGENCE_WARN_DELTA_COUNT",
        Retrieval,
        T_RETRIEVAL,
        Kind::POSITIVE,
        "1000",
        "Divergence warning threshold (records) in `/debug/storage-visibility`.",
    )
    .eme(),
    Entry::new(
        "DASH_RETRIEVAL_STORAGE_DIVERGENCE_WARN_RATIO",
        Retrieval,
        T_RETRIEVAL,
        Kind::NON_NEGATIVE_FLOAT,
        "0.25",
        "Divergence warning threshold (ratio).",
    )
    .eme(),
    Entry::new(
        "DASH_EMBEDDING_ALLOW_TOKEN_IDS",
        Retrieval,
        T_EMBEDDING,
        Kind::Bool(Honors::OneTrueYes),
        "off",
        "`1`, `true` or `yes`: accept token-id array `input` on `/v1/embeddings`, embedding the decimal ids joined by spaces. Off by default: token-id inputs are rejected with 400 `unsupported_input_type`, because ids cannot be decoded without the client's tokenizer.",
    ),
    Entry::new(
        "DASH_EMBEDDING_MAX_TOTAL_CHARS",
        Retrieval,
        T_EMBEDDING,
        Kind::POSITIVE,
        "524288",
        "Maximum total characters across all inputs of one `/v1/embeddings` request.",
    ),
    // =====================================================================
    // Control plane
    // =====================================================================
    Entry::new(
        "DASH_CONTROL_PLANE_BIND",
        ControlPlane,
        T_SERVER,
        Kind::Addr,
        "127.0.0.1:8090",
        "Listen address (`host:port`).",
    )
    .eme(),
    Entry::new(
        "DASH_CONTROL_PLANE_WORKERS",
        ControlPlane,
        T_SERVER,
        Kind::POSITIVE,
        "8",
        "Worker threads.",
    ),
    Entry::new(
        "DASH_CONTROL_PLANE_QUEUE_DEPTH",
        ControlPlane,
        T_SERVER,
        Kind::POSITIVE,
        "64",
        "Accepted-but-unserved connections buffered before new ones get 503.",
    ),
    Entry::new(
        "DASH_CONTROL_PLANE_MAX_BODY_BYTES",
        ControlPlane,
        T_SERVER,
        Kind::POSITIVE,
        "8388608 (8 MiB)",
        "Maximum accepted `Content-Length`. The header block is capped at 16 KiB.",
    ),
    Entry::new(
        "DASH_CONTROL_PLANE_READ_TIMEOUT_MS",
        ControlPlane,
        T_SERVER,
        Kind::POSITIVE_MILLIS,
        "5000",
        "Timeout for any single read.",
    ),
    Entry::new(
        "DASH_CONTROL_PLANE_REQUEST_DEADLINE_MS",
        ControlPlane,
        T_SERVER,
        Kind::POSITIVE_MILLIS,
        "10000",
        "Total time allowed to receive one request.",
    ),
    Entry::new(
        "DASH_CONTROL_PLANE_WRITE_TIMEOUT_MS",
        ControlPlane,
        T_SERVER,
        Kind::POSITIVE_MILLIS,
        "5000",
        "Timeout for any single write.",
    ),
    Entry::new(
        "DASH_CONTROL_PLANE_NODE_ID",
        ControlPlane,
        T_LEASE,
        Kind::Str,
        "none (required; dev mode falls back to `control-plane-<pid>`)",
        "Unique, stable node id used in leader election. Startup fails without it unless `DASH_INSECURE_DEV_MODE=1`.",
    )
    .eme(),
    Entry::new(
        "DASH_CONTROL_PLANE_LEASE_RESET",
        ControlPlane,
        T_LEASE,
        Kind::Bool(Honors::One),
        "`0`",
        "Set to `1` for one start to discard a forged or corrupt lease record (or epoch sidecar) that the node otherwise refuses to run with. Remove it afterwards.",
    )
    .eme(),
    Entry::new(
        "DASH_CONTROL_PLANE_STATE_PATH",
        ControlPlane,
        T_LEASE,
        Kind::Path,
        "unset (state not persisted)",
        "Persisted placement CSV.",
    )
    .eme(),
    Entry::new(
        "DASH_CONTROL_PLANE_STATE_SHA256_PATH",
        ControlPlane,
        T_LEASE,
        Kind::Path,
        "",
        "Checksum file for the persisted state; verified at startup (mismatch exits with code 2).",
    )
    .eme(),
    Entry::new(
        "DASH_CONTROL_PLANE_LEASE_PATH",
        ControlPlane,
        T_LEASE,
        Kind::Path,
        "unset (standalone, always leader)",
        "File lease for leader election.",
    )
    .eme(),
    Entry::new(
        "DASH_CONTROL_PLANE_LEASE_DURATION_MS",
        ControlPlane,
        T_LEASE,
        Kind::MILLIS,
        "30000",
        "Lease length.",
    )
    .eme(),
    Entry::new(
        "DASH_CONTROL_PLANE_LEASE_RENEWAL_MS",
        ControlPlane,
        T_LEASE,
        Kind::MILLIS,
        "10000",
        "Renewal interval.",
    )
    .eme(),
    Entry::new(
        "DASH_CONTROL_PLANE_LEASE_SAFETY_MARGIN_MS",
        ControlPlane,
        T_LEASE,
        Kind::MILLIS,
        "1000",
        "Clock-skew margin: a leader stops reporting leadership this long before the lease expires, and other nodes wait this long after expiry before taking over (capped at half the lease duration).",
    )
    .eme(),
    // =====================================================================
    // Tools
    // =====================================================================
    Entry::new(
        "DASH_LIVE_URL",
        Tools,
        T_LOAD,
        Kind::Url,
        "http://127.0.0.1:8080",
        "Base URL the load-test binary drives.",
    ),
    Entry::new(
        "DASH_API_KEY",
        Tools,
        T_LOAD,
        Kind::Secret,
        "",
        "API key the load-test binary sends (`--api-key` overrides).",
    ),
    Entry::new(
        "DASH_BENCH_FIXTURE_SIZE",
        Tools,
        T_BENCH,
        Kind::POSITIVE,
        "per profile",
        "Override the fixture size of the benchmark profile.",
    ),
    Entry::new(
        "DASH_BENCH_MIN_ITERATIONS",
        Tools,
        T_BENCH,
        Kind::POSITIVE,
        "5",
        "Minimum iterations for a valid benchmark run.",
    ),
    Entry::new(
        "DASH_BENCH_GUARD_MIN_ITERATIONS",
        Tools,
        T_BENCH,
        Kind::POSITIVE,
        "5",
        "Minimum iterations required by the history regression guard.",
    ),
    Entry::new(
        "DASH_BENCH_HISTORY_CSV_OUT",
        Tools,
        T_BENCH,
        Kind::Path,
        "",
        "Write the benchmark history as CSV to this path.",
    ),
    Entry::new(
        "DASH_BENCH_ANN_MAX_NEIGHBORS_BASE",
        Tools,
        T_BENCH,
        Kind::POSITIVE,
        "16",
        "Vector index tuning used by the benchmark store (HNSW connectivity).",
    ),
    Entry::new(
        "DASH_BENCH_ANN_EXPANSION_ADD",
        Tools,
        T_BENCH,
        Kind::POSITIVE,
        "128",
        "Vector index tuning used by the benchmark store (HNSW construction beam).",
    ),
    Entry::new(
        "DASH_BENCH_ANN_SEARCH_EXPANSION_MIN",
        Tools,
        T_BENCH,
        Kind::POSITIVE,
        "128",
        "Vector index tuning used by the benchmark store (HNSW search beam floor).",
    ),
    Entry::new(
        "DASH_BENCH_VECTOR_FLAT_THRESHOLD",
        Tools,
        T_BENCH,
        Kind::POSITIVE,
        "8192",
        "Vector index tuning used by the benchmark store (flat-to-HNSW threshold).",
    ),
    Entry::new(
        "DASH_BENCH_VECTOR_RERANK",
        Tools,
        T_BENCH,
        Kind::UINT,
        "50",
        "Vector index tuning used by the benchmark store (exact rerank width; 0 disables).",
    ),
    Entry::new(
        "DASH_BENCH_LARGE_MIN_CANDIDATE_REDUCTION_PCT",
        Tools,
        T_BENCH,
        Kind::ANY_FLOAT,
        "95.0",
        "Gate for the `large` profile: minimum candidate reduction in percent.",
    ),
    Entry::new(
        "DASH_BENCH_LARGE_MAX_DASH_LATENCY_MS",
        Tools,
        T_BENCH,
        Kind::ANY_FLOAT,
        "120.0",
        "Gate for the `large` profile: maximum average latency in milliseconds.",
    ),
    Entry::new(
        "DASH_BENCH_LARGE_MIN_ANN_RECALL_AT_100",
        Tools,
        T_BENCH,
        Kind::ANY_FLOAT,
        "0.98",
        "Gate for the `large` profile: minimum ANN recall at 100.",
    ),
    Entry::new(
        "DASH_BENCH_XLARGE_MIN_CANDIDATE_REDUCTION_PCT",
        Tools,
        T_BENCH,
        Kind::ANY_FLOAT,
        "96.0",
        "Gate for the `xlarge` profile: minimum candidate reduction in percent.",
    ),
    Entry::new(
        "DASH_BENCH_XLARGE_MAX_DASH_LATENCY_MS",
        Tools,
        T_BENCH,
        Kind::ANY_FLOAT,
        "250.0",
        "Gate for the `xlarge` profile: maximum average latency in milliseconds.",
    ),
    Entry::new(
        "DASH_BENCH_XLARGE_MIN_ANN_RECALL_AT_100",
        Tools,
        T_BENCH,
        Kind::ANY_FLOAT,
        "0.98",
        "Gate for the `xlarge` profile: minimum ANN recall at 100.",
    ),
    Entry::new(
        "DASH_BENCH_XXLARGE_MIN_CANDIDATE_REDUCTION_PCT",
        Tools,
        T_BENCH,
        Kind::ANY_FLOAT,
        "97.0",
        "Gate for the `xxlarge` profile: minimum candidate reduction in percent.",
    ),
    Entry::new(
        "DASH_BENCH_XXLARGE_MAX_DASH_LATENCY_MS",
        Tools,
        T_BENCH,
        Kind::ANY_FLOAT,
        "350.0",
        "Gate for the `xxlarge` profile: maximum average latency in milliseconds.",
    ),
    Entry::new(
        "DASH_BENCH_XXLARGE_MIN_ANN_RECALL_AT_100",
        Tools,
        T_BENCH,
        Kind::ANY_FLOAT,
        "0.98",
        "Gate for the `xxlarge` profile: minimum ANN recall at 100.",
    ),
    Entry::new(
        "DASH_BENCH_LARGE_PLUS_MIN_GRAPH_SCORE_COVERAGE",
        Tools,
        T_BENCH,
        Kind::ANY_FLOAT,
        "1.0",
        "Gate for the large profiles: minimum graph score coverage.",
    ),
    Entry::new(
        "DASH_BENCH_LARGE_PLUS_MIN_GRAPH_SUPPORT_PATH_COUNT",
        Tools,
        T_BENCH,
        Kind::POSITIVE,
        "1",
        "Gate for the large profiles: minimum support path count.",
    ),
    Entry::new(
        "DASH_BENCH_LARGE_PLUS_MIN_GRAPH_CONTRADICTION_CHAIN_DEPTH",
        Tools,
        T_BENCH,
        Kind::POSITIVE,
        "2",
        "Gate for the large profiles: minimum contradiction chain depth.",
    ),
    Entry::new(
        "DASH_BENCH_MIN_SEGMENT_REFRESH_SUCCESSES",
        Tools,
        T_BENCH,
        Kind::UINT,
        "0",
        "Gate: minimum successful segment cache refreshes.",
    ),
    Entry::new(
        "DASH_BENCH_MIN_SEGMENT_CACHE_HITS",
        Tools,
        T_BENCH,
        Kind::UINT,
        "0",
        "Gate: minimum segment cache hits.",
    ),
    Entry::new(
        "DASH_BENCH_REQUIRE_VECTOR_BACKEND",
        Tools,
        T_BENCH,
        Kind::Enum {
            values: &["cpu", "gpu"],
        },
        "",
        "Fail the run unless the store reports this vector backend.",
    ),
    Entry::new(
        "DASH_BENCH_WAL_SCALE_CLAIMS",
        Tools,
        T_BENCH,
        Kind::POSITIVE,
        "per profile",
        "Claims seeded for the WAL scale measurement (10000 large, 20000 xlarge, 50000 xxlarge, otherwise 5000; capped at the fixture size).",
    ),
];
