# Changelog

All notable changes to DASH are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/) and the project adheres
to [Semantic Versioning](https://semver.org/).

## [Unreleased]

The 0.3.0 release has not been tagged yet; its content is below.

### Added (P1 typed configuration)

- **Typed settings registry** (`pkg/config`, crate `dash-config`): one table
  lists every environment setting the code reads (name, scope, value type,
  default, description, deprecated aliases). The configuration reference page is
  now generated from it (`cargo run -p dash-config -- docs`); `scripts/check_config_docs.sh`
  wraps `dash-config docs --check`. The test
  `registry_covers_every_env_var_read_by_code` fails when the code reads a
  variable the registry lacks, or the registry lists one no code reads.
- **Startup validation.** `ingestion`, `retrieval` and `control-plane` validate
  their environment at startup: a malformed value of a typed setting (non-number,
  out of range, unknown enum word, unparsable boolean, blank where blank is
  meaningless) prints every error and exits with code 2. Unknown `DASH_*` /
  `EME_*` variables and deprecated spellings (`EME_*`, `DASH_OLLAMA_BASE_URL`,
  `DASH_*_JWT_ROLE_CLAIM`) are logged as warnings, with a did-you-mean
  suggestion. `DASH_CONFIG_VALIDATION=warn` downgrades errors to warnings.
  The validator accepts every value the existing readers accept; where readers
  differ it accepts the union.
- **Configuration file.** `DASH_CONFIG_FILE=/path/dash.toml` (tables
  `[ingestion]`, `[retrieval]`, `[control_plane]`, `[common]`, lowercase keys
  named after the variable suffix) fills settings that are not set in the
  environment; the environment wins. Unknown keys are startup errors with a
  suggestion; a file holding secret-typed keys must have mode 0600 or 0640.
  See [Configuration file](docs-site/docs/operations/configuration-file.md).
- **`dash-config` command line tool:** `validate`, `print` (effective value and
  source, secrets redacted), `docs [--check]`, `list`.

### Changed

- `scripts/check_deploy_env.sh` no longer counts the names listed in
  `pkg/config` as "read by the code".
- The `DASH_STRICT_SECRETS` description now states that the control plane
  applies it to its bearer token (it already did).

## 0.3.0 (unreleased) - P0 production-readiness hardening

All code listed here is merged in this tree; nothing is tagged or published.
Defect IDs refer to
[`docs/plans/2026-10-09-issue-register.md`](docs/plans/2026-10-09-issue-register.md);
what each area covers, with test evidence and what is still open, is in
[`docs/plans/2026-10-09-p0-status.md`](docs/plans/2026-10-09-p0-status.md).
The authoritative scope and exit criteria are in
[`docs/plans/2026-10-09-production-readiness-master-plan.md`](docs/plans/2026-10-09-production-readiness-master-plan.md).
Items marked **Breaking** need action when upgrading from 0.2.x; the
steps are in [Upgrading to 0.3.0](#upgrading-to-030).

### Upgrading to 0.3.0

Do these in order. Environment variable meanings are in
[`docs-site/docs/reference/configuration.md`](docs-site/docs/reference/configuration.md).

1. **Back up and verify the WAL before upgrading.** Stop writes, then run
   `scripts/backup_state_bundle.sh` (it bundles the WAL, segments and
   placement file). Build the new tool with
   `cargo build --release -p wal-inspect` and run
   `target/release/wal-inspect verify <wal>` on the ingestion WAL, the
   retrieval WAL (if `DASH_RETRIEVAL_WAL_PATH` is set) and each
   `<wal>.snapshot`. Exit status 0 means no problem, 1 means an interior
   line is damaged (fix it first with the procedure in
   [`docs/operations/wal-recovery.md`](docs/operations/wal-recovery.md)),
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

### Security
- **Breaking: deny by default.** Services refuse to start (exit 2) with no
  credentials configured unless `DASH_INSECURE_DEV_MODE=1`; dev mode binds
  loopback only unless `DASH_INSECURE_DEV_MODE_ALLOW_NON_LOOPBACK=1`
  (SEC-01). A JWT-only configuration no longer falls through to an open
  API-key branch (SEC-02).
- **Breaking: strict secret validation on by default** (placeholders
  rejected, at least 16 characters for keys and tokens, 32 for JWT secrets);
  it can only be disabled with `DASH_STRICT_SECRETS=0` together with dev
  mode (SEC-04, SEC-05). Errors never echo the secret.
- **Breaking: roles.** `admin` implies every role and `read_only` implies
  `retrieve`; a JWT without a roles claim gets no roles unless
  `DASH_*_JWT_DEFAULT_ROLES` is set; legacy unscoped API keys get
  `DASH_*_API_KEY_DEFAULT_ROLES` (primary role of the service by default)
  instead of all roles (SEC-11).
- **Breaking:** `/v1/embeddings`, `/debug/*` and `/metrics` require
  authentication; `/metrics` can be exempted with `DASH_METRICS_PUBLIC=1`.
  Authentication and authorization run before any embedding provider call
  (SEC-09, SEC-10).
- **Breaking:** replication endpoints require `DASH_INGEST_REPLICATION_TOKEN`
  (constant-time comparison; 403 without it) and an ingestion follower will
  not start without it (SEC-08).
- **Breaking:** the control plane requires `DASH_CONTROL_PLANE_TOKEN`
  (bearer, constant-time comparison) on every route except health and ready,
  and refuses to start without it outside dev mode (SEC-07). Leases are
  durable and fenced, the leader renews in the background, and promotion is
  refused unless the replica reported zero replication lag (or `force=1`)
  (CP-01, CP-02). The placement client in ingestion and retrieval presents
  the token, has bounded timeouts, and no longer falls back to a stale
  placement file unless `DASH_ROUTER_ALLOW_STALE_PLACEMENT=1`; ingestion
  refuses writes when placement reloads have failed for longer than
  `DASH_INGEST_PLACEMENT_STALE_GRACE_MS` (REP-08).
- Per-tenant token-bucket rate limiting is enforced for API keys, JWTs and
  OIDC, state is kept across requests, and the response is HTTP 429 with
  `Retry-After` (SEC-06).
- JWT and OIDC hardening: `exp` always required, maximum lifetime
  (`DASH_*_JWT_MAX_LIFETIME_SECS`, default 86400), leeway capped at 60 s,
  `jti` denylist, wildcard tenants opt-in, empty HS256 keys rejected;
  OIDC requires `iss` and `aud`, an `https` JWKS URL, an asymmetric-algorithm
  allow-list and a `kid`; the JWKS cache has single-flight fetches,
  stale-while-revalidate (24 h), negative caching, a forced-refresh limit,
  3 s / 256 KiB / no-redirect fetch bounds, and garbage tokens never trigger
  a fetch (SEC-12, SEC-13, SEC-14).
- Authentication settings reload on SIGHUP (optionally from
  `DASH_CONFIG_RELOAD_FILE`); the key and `jti` revocation files reload when
  their mtime or size changes, checked at most once per second (SEC-15 in
  part; keys are still held in plaintext).
- The OpenAI embedding provider uses HTTPS (rustls); the provider clients
  are bounded (response cap, no redirects, total deadline, jittered
  retries), the circuit breaker admits a single half-open probe, and sending
  a key over plaintext HTTP to a non-loopback host is refused unless
  `DASH_EMBEDDING_ALLOW_INSECURE_HTTP=1` (SEC-23, EMB-01, EMB-03, EMB-05).
- Cross-tenant claim-id conflict errors no longer name the other tenant
  (SEC-18, in part); tenant directories are collision-free (SEC-19);
  identifier fields reject control characters.
- Audit chain: one canonical v2 encoding shared by both services, file
  locking, `fdatasync`, torn-tail truncation, explicit `chain_restart`
  records instead of silent restarts, actor fingerprints, an optional
  fail-closed gate (`DASH_*_AUDIT_FAIL_CLOSED`), and the shared verifier
  `tools/audit-verify` (SEC-17). The chain is still **unkeyed** (no HMAC).

### Reliability
- Network embedding providers are wrapped in a circuit breaker (transport
  errors, timeouts and 5xx only) and a concurrency cap; outages answer 503
  with `Retry-After` in both services.
- Health probes use reserved workers; idle connections are closed after a
  first-byte timeout, the request deadline starts at accept, and
  `DASH_HTTP_MAX_CONNS_PER_IP` caps connections per client.
- Stricter request parsing: invalid percent-encoding, malformed HTTP
  versions, header folding, `Expect` and (retrieval) duplicate JSON keys
  are rejected.
- Non-finite provider output is rejected; ingest rejects cross-tenant edge
  targets and all-zero claim embeddings.
- Python, TypeScript and Go SDKs decode `encoding_format="base64"`
  embeddings and expose `dimensions`.

### Data integrity and recovery
- Evidence is upserted by `evidence_id` and edges by
  `(from, to, relation)` in memory, in redb, on bulk load and on replication
  re-apply, so retries, restarts and replays no longer duplicate citations
  (DATA-01).
- **WAL v2 records** (`C2`, `E2`, `G2`, `V2`, `B2`): every field escaped and
  each record ends with a CRC-32 suffix (`crc=<8 hex>`). Legacy records are
  still read. Tabs and newlines in claim text no longer poison replay
  (DATA-02, DATA-07).
- Edge `reason_codes` and `created_at` are persisted in `G2` records and
  survive restarts; checkpoints and WAL repairs fsync the containing
  directory after renames (DATA-06, DATA-08).
- **Torn-tail recovery:** a partial or checksum-failing last line is
  truncated at open instead of failing startup (DATA-07). A damaged interior
  record is a hard error naming the line.
- **WAL generation** (`<wal>.gen`) changes on checkpoint; replication frames
  and exports carry it, and followers persist `(generation, offset)`
  together and resync when it changes (REP-01, REP-02, REP-05).
- **Atomic single ingest:** one bundle is one WAL commit group; a crash
  inside a group replays as nothing and a torn group is discarded (DATA-10).
  Batches are staged on a detached copy of the store and committed as a unit
  (DATA-09).
- **Quarantine of unreadable legacy records:** replay moves legacy records
  that cannot be parsed or validated (and the records that depend on them)
  into `<wal>.quarantine` instead of failing startup; `DASH_WAL_REPLAY_STRICT=1`
  restores fail-fast.
- Vector writes are validated before the WAL append; re-upserting a claim
  keeps its vector and ANN entry; query vectors are validated and invalid
  ones never produce NaN scores (DATA-03, DATA-04, DATA-05).
- Replication protocol: bounded responses, atomic frame application, backoff,
  generation-aware offsets, replace-on-resync keeping the redb handle, and
  readiness (`/ready`, metrics) that reports lag, staleness and initial-sync
  state. Frames never end inside a commit group (REP-03, REP-04, REP-06).
- redb: single-transaction claim writes; the in-memory event buffer is a
  bounded ring (PERF-05).
- Batch idempotency is content aware: replaying a `commit_id` with the same
  content is a no-op, reusing it with different content is applied as an
  update (`updated: true`). Document-derived ids are collision-free and
  evidence spans refer to offsets in the original text (DATA-12, DATA-13).

### Behavior changes
- **Breaking: edge direction.** An edge `from supports to` supports the
  **target** claim (previously the author); contradicts edges penalize the
  target. Dangling, cross-tenant and self edges are ignored and each distinct
  source counts once. `supports` counts in results can differ from 0.2.x
  (IDX-05).
- **Ranking saturation.** The support bonus is `tanh`-saturated and capped
  at 0.4 (historical slope of 0.08 per source for small counts), the
  contradiction penalty is capped at 0.5; evidence from one `source_id`
  counts once toward the ranking signal (the reported `supports` and
  `contradicts` counts stay raw).
- `/v1/embeddings`: errors use OpenAI's `{"error":{...}}` shape; provider
  failures are 503 `embedding_unavailable` or 502 `embedding_provider_error`
  (they were 400); token-id array inputs are rejected with 400
  `unsupported_input_type` unless `DASH_EMBEDDING_ALLOW_TOKEN_IDS=1`;
  `usage.prompt_tokens` is an estimate (`ceil(chars / 4)`); `dimensions`
  must match the provider; at most 2048 inputs and
  `DASH_EMBEDDING_MAX_TOTAL_CHARS` characters (EMB-06).
- **HTTP status codes and bounds:** 413 for bodies over 16 MiB, 431 for
  oversized or too many headers, 408 for a request that exceeds
  `DASH_HTTP_REQUEST_TIMEOUT_MS`, 501 for `Transfer-Encoding`, 429 with
  `Retry-After`, 502/503 for embedding provider failures, 503 when the
  audit fail-closed gate or stale placement refuses a write. JSON is parsed
  with a depth limit (a deeply nested body used to abort retrieval) and
  UTF-8 decodes identically over GET and POST (ROB-01 to ROB-04).
- **Retrieve bounds:** `top_k` at most 1000 (`DASH_RETRIEVAL_MAX_TOP_K`),
  `query` at most 8 KiB, at most 256 filter values, query vectors of at most
  8192 values; an invalid `query_embedding` is a 400.
- The retrieve path resolves the segment prefilter before taking the store
  read lock, makes one candidate scan per request, and scopes every vector
  fallback to the requesting tenant (IDX-03, PERF-02, PERF-04).
- Accept-loop errors no longer end the server, ingestion escapes control
  characters in JSON output and percent-decodes query values (ROB-06,
  ROB-09, ROB-10).
- Retrieval fails to start (exit 1) on an invalid placement configuration;
  ingestion exits 2.
- `/metrics` gains per-route latency histograms, auth and rate-limit
  counters, replication follower gauges and audit counters.
- `DASH_OLLAMA_ENDPOINT` is the Ollama variable (`DASH_OLLAMA_BASE_URL`
  remains as a deprecated alias); Ollama defaults to `/api/embed`
  (EMB-02).

### Operations and deployment
- New tools: `tools/wal-inspect` (`inspect`, `verify`, `repair`;
  [`docs/operations/wal-recovery.md`](docs/operations/wal-recovery.md)) and
  `tools/audit-verify` (wrapped by `scripts/verify_audit_chain.sh`;
  [`docs/operations/audit-chain.md`](docs/operations/audit-chain.md)).
- New operations guide for authentication, roles, OIDC and reload:
  [`docs/operations/auth.md`](docs/operations/auth.md).
- Docker Compose: required generated secrets (`${VAR:?}`), published ports
  on `127.0.0.1` (`DASH_PUBLISH_ADDR` to change), read-only root filesystem,
  all capabilities dropped, replication wiring, audit logs, an optional
  `control-plane` profile; images take a per-service `SERVICE` build arg and
  a readiness healthcheck (DEP-08, DEP-10, SEC-20, SEC-24).
- Kubernetes manifests: secrets are no longer committed (you create them),
  replication is wired, ingest paths are routed, a control plane is added,
  HorizontalPodAutoscalers are removed. Helm: secrets have no defaults and
  are required, env names fixed, image naming matches the release workflow,
  HPA removed. systemd: separate state directories per service,
  sandboxing, a control-plane unit, generated environment secrets
  (DEP-01 to DEP-06, DEP-09, SEC-03, SEC-04).
- CI: deploy artifacts are validated, images are built on pull requests,
  a control-plane release image is published, the Trivy action is pinned,
  and Java, Kotlin and C# SDK jobs were added. There is no separate
  end-to-end suite; end-to-end coverage lives in the Rust integration tests
  (`services/retrieval/tests/end_to_end.rs`,
  `services/retrieval/tests/replication_e2e.rs`,
  `services/ingestion/tests/replication_follower.rs`).
- `scripts/check_config_docs.sh` fails when an environment variable read by
  the code is not documented in the configuration reference.

### SDKs
- Java, Kotlin and C# SDKs fixed and unified at version 0.2.0: response
  bodies are read once, models follow the server contract, ingest targets the
  ingestion service (separate base URL), retries are limited to idempotent
  requests or requests with an `Idempotency-Key` and honor `Retry-After`
  (SDK-01 to SDK-06).
- **Breaking:** `delete()` removed from the Java, Kotlin and C# SDKs (there
  is no `/v1/delete`).
- Python, TypeScript and Go send and decode the full retrieve contract
  (`query_embedding`, `entity_filters`, `embedding_id_filters`,
  `time_range`, `read_consistency`, `return_graph`, graph and confidence
  fields); the default `top_k` is 5 like the server; versions are 0.2.0.
- **Breaking:** the Go module path is now
  `github.com/BHAWESHBHASKAR/DASH/sdks/go` (was `github.com/anomalyco/dash-go`).

### Documentation
- README, configuration reference (regenerated from the code),
  HTTP API reference, deploy guide, threat model, SOC 2 readiness mapping
  and the operations guides were rewritten to describe only what the code
  does (DOC-01 to DOC-07). Every README capability claim is tracked in
  `docs/claims-ledger.md` and checked by `scripts/check_claims_ledger.sh`.

### Not in 0.3.0
HMAC-keyed audit chain, encryption at rest (the `pkg/encryption` library is
not wired into storage), mTLS, an external penetration test, consensus
replication and automatic failover, delete and tenant-management APIs, a real
GPU backend, signed release images.

## M11 - Enterprise identity, RBAC, encryption library, SOC 2 package (in tree, untagged; 2026-08-10)

Delivered in commits `c55e8cb` (M11a/M11b) and `e38cd0b` (M11c/M11d). What
exists and what does not:

### Added
- **OIDC/JWKS token validation** (`pkg/auth/src/oidc.rs`): JWKS fetch with
  a refresh interval, issuer/audience/expiry checks, tenant claim
  extraction. Enabled per service with `DASH_*_JWT_PROVIDER=oidc`. Unit
  tests use a symmetric (`oct`/HS256) JWK; there is no end-to-end test
  against an identity provider and no RSA/EC key test. *Correction
  (0.3.0):* the OIDC path now has RS256 tests against generated keys and
  an in-process stub IdP, plus the JWKS hardening listed above; there is
  still no test against a real identity provider.
- **Role-based access control**: roles `admin`, `ingest`, `retrieve`,
  `read_only` parsed from JWT claims and scoped API keys
  (`key:tenants:roles`) and checked per route in ingestion and retrieval.
  Known gaps at the time: roles had no hierarchy, a JWT without a roles
  claim got all roles, unscoped keys skipped role checks (SEC-11), and the
  planned control-plane `admin` enforcement was not implemented (SEC-07).
  *Correction (0.3.0):* the role hierarchy, role-less JWT handling,
  default roles for unscoped keys and control-plane token authentication
  are implemented; see 0.3.0 above.
- **`pkg/encryption`**: an AES-256-GCM `EncryptionProvider` trait with an
  environment master-key provider. This is a library only: no storage
  code calls it, so no data is encrypted at rest by DASH (SEC-16).
- **SOC 2 readiness package**: `docs/compliance/soc2-readiness.md`,
  policy templates, and `scripts/soc2_evidence_collector.sh`. These
  describe a target state; see the "Current status" column added in the
  2026-10-09 documentation correction.

## 0.2.x line - 2026-06-13 modernization (in tree, untagged)

Everything below this heading was previously listed under `[Unreleased]`.
Corrections made on 2026-10-09 are marked *Correction*.


### Added
- **OpenAI-compatible `/v1/embeddings` endpoint** on the retrieval
  service. Any OpenAI client (langchain, llama-index, semantic-kernel,
  the `openai` Python SDK) can point at DASH with one environment
  variable (`OPENAI_API_BASE=http://localhost:8080/v1`). 17 tests
  cover the wire-format compatibility, error envelope, and HTTP-level
  integration.
- **Python SDK** `dash-py` — idiomatic Python client with sync + async,
  typed dataclasses, OpenAI drop-in examples, RAG example showing
  the Claim + Evidence + Contradiction differentiator. 59 tests pass.
- **Go SDK** `dash-go` — Go 1.21+ module, idiomatic functional options
  + service pattern, full OpenAI drop-in compatibility, errors.Is/As
  support. 86 tests pass.
- **TypeScript SDK** `dash-ts` — ESM-first, Node 18+, zero runtime deps,
  full type safety with discriminated unions, drop-in OpenAI
  compatibility. 65 tests pass.
- **Docker + docker-compose** — multi-stage, multi-arch (linux/amd64,
  linux/arm64), non-root runtime, healthcheck, dependency ordering,
  dev overlay with hot-reload. `docker compose up -d` brings the
  ingestion + retrieval services online.
- **Quickstart + README + comparison docs** — `docs/quickstart.md`
  (5-minute path from clone to first query), `README.md` (rewrite
  with hero section and the differentiator showcase), `docs/comparison.md`
  (DASH vs Pinecone/Weaviate/Milvus/Qdrant/Chroma).
- **Real embedding integration** — env-driven provider selection
  (`DASH_EMBEDDING_PROVIDER=hash|ollama|openai`) so deployments
  can wire up real semantic embeddings instead of the
  hash-based default.
- **Semantic-first retrieval** — new `InMemoryStore::retrieve_semantic`
  method. When the caller passes a pre-computed query vector, the
  dense-similarity score becomes the primary ranking signal (cosine
  in `[-1, 1]`, mapped to `[0, 1]`); the lexical/BM25 score becomes a
  small tie-breaker. 3 new integration tests cover the semantic-first
  guarantee.
- **redb persistence (PR 1, additive)** (*Correction:* PR 1 shipped default-off, but commit `6a242d8` (PR 2) made persistence default-on when a WAL path is set; disable with `DASH_*_PERSISTENCE_DISABLE=1`. The text below describes PR 1.) — `DiskBackedStore`
  struct in `pkg/store/src/disk.rs` provides on-disk durability for
  claims, evidence, edges, vectors, and the tenant→claim set. Enabled
  via `DASH_INGEST_PERSISTENCE_PATH` / `DASH_RETRIEVAL_PERSISTENCE_PATH`
  env vars. If unset (the default), DASH runs in WAL-only mode — the
  pre-redb behavior is preserved bit-for-bit. 5 new integration
  tests cover round-trip, fallback, tenant consistency, ANN rebuild,
  and open-failure paths. The design doc at
  `docs/plans/2026-06-13-redb-persistence-design.md` describes the
  full 3-PR phasing.
- **cargo-fuzz harnesses** — `fuzz/` directory with 4 focused targets
  (`fuzz_jwt`, `fuzz_openai_embeddings`, `fuzz_ranking`, `fuzz_wal_parse`)
  that exercise the JWT verifier, OpenAI request parser, ranking
  function, and WAL record parser against arbitrary input. Run via
  `cargo +nightly fuzz run <target>`. See `fuzz/README.md`.
- **Performance benchmark suite** — `tests/benchmarks/src/perf_bench.rs`
  with 5 scenarios: `ingest_throughput_sequential` (in-memory and
  persistent), `retrieve_throughput_lexical`, `retrieve_throughput_semantic`,
  `ann_search_throughput_at_scale`, `wal_replay_throughput`. CLI
  flags `--scenario`, `--iterations`, `--warmup`. Baseline numbers
  published in `docs/benchmarks/performance.md`.
- **Public `evidence_for_claim` accessor** on `InMemoryStore` for
  test introspection of ingested evidence.
- **`OpenAIErrorResponse`** with `DashError` interface and
  `DashAPIError`/`DashConnectionError` concrete types; `from_response`
  factory tolerates both OpenAI and ad-hoc error shapes.

### Changed
- **JSON parsing in services/ingestion** — replaced 633 lines of
  hand-rolled JSON parsing in `transport/payload.rs` with serde-based
  deserialization. `transport/json.rs` is now a thin compat shim for
  the test suite.
- **Hand-rolled crypto in pkg/auth** — replaced SHA-256, HMAC-SHA256,
  base64url, and the recursive-descent JSON parser with
  `jsonwebtoken`, `serde_json`, `base64`, `sha2`, and `hex`. Public
  API preserved. 16 tests pass (was 7 in the hand-rolled impl —
  added 9 new edge-case tests including a FIPS SHA-256 known vector).
- **Hand-rolled HNSW in pkg/store** — *Correction (2026-10-09):* this
  entry originally claimed the in-repo HNSW scaffolding was replaced by
  the `usearch` crate and was "substantially faster". That did not
  happen. `usearch` is declared in `Cargo.toml` but no source file uses
  it; the ANN index is the in-repo HNSW-style graph in
  `pkg/store/src/ann.rs` (register IDX-01). No benchmark in the
  repository supports a speedup claim.
- **Schema types** in `pkg/schema` — added `Serialize`/`Deserialize`
  derives to all public domain types with `#[serde(default)]` on
  optional fields.
- **API request/response types** in `services/ingestion/src/api.rs` —
  added `Serialize`/`Deserialize` derives. 5 new serde round-trip tests.
- **Half-built axum runtime paths removed** from both services. The
  `TransportRuntime::Axum` branch that exited with an error when the
  `async-transport` feature was absent is gone; the runtime path is
  now a single direct call to `serve_http_with_workers`.
- **`.gitignore`** updated to ignore SDK build artifacts
  (`node_modules/`, `dist/`, `__pycache__/`, `*.pyc`, etc.).

### Fixed
- All 24 prior build warnings (cfg conditions, dead code, lazy
  doc-continuation) resolved.
- 4 pre-existing test bugs (one in `wal_persistence_and_replay_round_trip`
  where the support_only test split `Vec`s and passed only one to
  `ingest_bundle`; fixed by merging into a single evidence vec).

### Test counts
*Correction (2026-10-09):* the counts below are the figures recorded on
2026-06-13 and were not re-verified. On 2026-10-09 the repository
contains 420 Rust `#[test]`/`#[tokio::test]` declarations (static
count). SDK test declarations: Python 64, Go 89, TypeScript 69, Java 21,
Kotlin 12, C# 41. Pass/fail status is the CI result, not these numbers.

- **Rust unit + integration tests:** 379 passing (was 333 at the start
  of this modernization campaign; +46 new tests across schema,
  auth, store (unit), store (integration_retrieval), retrieval,
  embeddings, retrieval HTTP integration, and disk persistence).
- **Python SDK:** 59 tests passing.
- **Go SDK:** 86 tests passing.
- **TypeScript SDK:** 65 tests passing.
- **Total across all stacks:** **589 tests passing.**

### Known limitations
- *Superseded:* the `InMemoryStore::Clone` limitation (disk handle dropped
  on clone) was addressed by redb PR 2 (`Arc<DiskBackedStore>`, commit
  `6a242d8`).
- The `embeddings_for_claim` returns the full evidence vec; for tenants
  with thousands of evidence per claim, this is unbounded. A future
  PR will add pagination.
- The performance benchmark suite does not include comparison
  against Pinecone/Weaviate/Milvus. Baseline numbers are internal
  (DASH against itself). See `docs/benchmarks/performance.md` for the
  methodology and the roadmap for the competitor-comparison work.

## Earlier releases

### Pre-modernization
(Note: the architecture document referred to here is
`docs/architecture/eme-architecture.md`; the root `EME_ARCHITECTURE.md`
is now a pointer to it.)
The original EME/DASH architecture is documented in
`docs/architecture/eme-architecture.md` (the "Evidence Memory Engine"
phase 0 design). The 11-phase production rollout plan lives in
`docs/execution/phases/`. The 2026-06-13 modernization roadmap at
`docs/plans/2026-06-13-dash-modernization-roadmap.md` records the
session-by-session deltas in detail.
