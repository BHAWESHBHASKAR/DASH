# Upgrading to 0.3.0

This page is the upgrade guide for moving from 0.2.x to 0.3.0 (unreleased). It mirrors the "Upgrading to 0.3.0" section of the root [`CHANGELOG.md`](https://github.com/BHAWESHBHASKAR/DASH/blob/main/CHANGELOG.md), which also lists everything that changed. Read it before upgrading: 0.3.0 changes defaults in ways that stop a 0.2.x configuration from starting.

Do these in order. Environment variable meanings are in
[Configuration](../reference/configuration.md).

1. **Back up and verify the WAL before upgrading.** Stop writes, then run
   `scripts/backup_state_bundle.sh` (it bundles the WAL, segments and
   placement file). Build the new tool with
   `cargo build --release -p wal-inspect` and run
   `target/release/wal-inspect verify <wal>` on the ingestion WAL, the
   retrieval WAL (if `DASH_RETRIEVAL_WAL_PATH` is set) and each
   `<wal>.snapshot`. Exit status 0 means no problem, 1 means an interior
   line is damaged (fix it first with the procedure in
   [WAL recovery](wal-recovery.md)),
   2 means a usage or I/O error. A torn final line is not a failure; the
   service truncates it on start. `wal-inspect inspect <wal>` also shows
   how many records are legacy. Legacy records stay readable; ones that can
   no longer be parsed or validated are moved to `<wal>.quarantine` at
   startup instead of blocking it (set `DASH_WAL_REPLAY_STRICT=1` in
   staging to see them as errors). WAL records written by 0.3.0 use new
   record kinds (`C2`, `E2`, `G2`, `V2`, `B2`) that 0.2.x is not expected
   to read (not tested), so keep the backup if you may roll back.
2. **Generate secrets.** For Docker Compose run `scripts/generate-secrets.sh`
   (it writes `deploy/container/.env` with mode 0600: the API keys, JWT
   secrets, the replication token on both sides and the control-plane
   token). Elsewhere create each value yourself, for example
   `openssl rand -hex 32`. Strict validation is on by default: API keys,
   scoped keys and replication tokens need at least 16 characters, HS256
   JWT secrets at least 32, and placeholders such as `change-me`,
   `example` or `<...>` are rejected. Replace any shorter or placeholder
   secret you used before.
3. **Set the variables the services now require.** Ingestion needs a
   credential (`DASH_INGEST_API_KEY`, `DASH_INGEST_API_KEY_SCOPES` or
   `DASH_INGEST_JWT_HS256_SECRET`, or OIDC) and
   `DASH_INGEST_REPLICATION_TOKEN` if retrieval or another node follows it.
   Retrieval needs a credential and, when it follows ingestion,
   `DASH_RETRIEVAL_REPLICATION_TOKEN` equal to ingestion's
   `DASH_INGEST_REPLICATION_TOKEN`. The control plane needs
   `DASH_CONTROL_PLANE_TOKEN`; ingestion and retrieval present it to the
   control plane through `DASH_ROUTER_CONTROL_PLANE_TOKEN` (or the same
   `DASH_CONTROL_PLANE_TOKEN`). A service with no credentials exits with
   code 2; for a local, throwaway setup only, `DASH_INSECURE_DEV_MODE=1`
   allows it (and forces a loopback bind). Kubernetes and Helm manifests
   used the wrong names `DASH_INGESTION_API_KEY` and
   `DASH_INGESTION_JWT_HS256_SECRET`; the code reads `DASH_INGEST_*`. Use
   `DASH_OLLAMA_ENDPOINT` (not `DASH_OLLAMA_BASE_URL`, which is only a
   deprecated alias).
4. **Check roles.** A JWT without a roles claim (`dash_roles` by default)
   is now authenticated but gets 403 on every role-checked route: add the
   claim, or set `DASH_INGEST_JWT_DEFAULT_ROLES` /
   `DASH_RETRIEVAL_JWT_DEFAULT_ROLES`. Unscoped API keys
   (`DASH_*_API_KEY`, `DASH_*_API_KEYS`) used to bypass role checks; they
   now get `DASH_*_API_KEY_DEFAULT_ROLES`, which defaults to `ingest` on
   ingestion and `retrieve` on retrieval. A key that must also read
   `/metrics` or `/debug/*` needs `read_only` (or `admin`), for example
   `DASH_RETRIEVAL_API_KEY_DEFAULT_ROLES=retrieve,read_only`. Tokens whose
   lifetime exceeds 24 hours, tokens without `exp`, and wildcard (`*`)
   tenants are now rejected unless you set
   `DASH_*_JWT_MAX_LIFETIME_SECS` or `DASH_*_JWT_ALLOW_WILDCARD_TENANT=1`.
   OIDC now requires `DASH_*_JWT_ISSUER`, `DASH_*_JWT_AUDIENCE` and an
   `https://` JWKS URL.
5. **Update clients and monitoring.** `/metrics`, `/debug/*` and
   `/v1/embeddings` now require credentials (`read_only` for the first
   two, `retrieve` for embeddings): give Prometheus an `x-api-key` or bearer
   header, or set `DASH_METRICS_PUBLIC=1` to exempt `/metrics` only. Health
   probes (`/health`, `/live`, `/ready`) stay open. Rate limits are now
   enforced per tenant (defaults 100 rps / burst 200 on ingestion, 500 rps /
   burst 1000 on retrieval; HTTP 429 with `Retry-After`): raise
   `DASH_*_RATE_LIMIT_PER_TENANT_RPS` for bulk loaders or set it to `0` to
   disable. `/v1/retrieve` rejects `top_k` above 1000
   (`DASH_RETRIEVAL_MAX_TOP_K`). Token-id array inputs on `/v1/embeddings`
   are rejected unless `DASH_EMBEDDING_ALLOW_TOKEN_IDS=1`.
6. **Upgrade ingestion and retrieval together.** Replication frames now
   carry the WAL generation. A follower that has only an old bare-number
   offset file does a full resync on its first poll; a retrieval follower
   without a retrieval WAL always starts with a full resync. Mixed-version
   replication is not tested.
7. **Tenant segment directories migrate automatically.** The first time a
   tenant is touched, an old lossy-named segment directory (for example
   `acme_corp`) is renamed to its collision-free name. Nothing to do; the
   segment data is derived and rewritten on the next publish.
8. **Update SDK code.** Remove calls to `delete` in the Java, Kotlin and C#
   SDKs (the server never had that route). Go users change the import path
   to `github.com/BHAWESHBHASKAR/DASH/sdks/go`. SDK versions are 0.2.0.
9. **After the upgrade.** Check `/ready` on both services, look for
   `<wal>.quarantine` files and a startup warning about quarantined
   records, re-run `wal-inspect verify`, and, if the audit log is enabled,
   run `target/release/audit-verify --path <log>` (build it with
   `cargo build --release -p audit-verify`). New audit records use the v2
   encoding and continue the existing chain.
