# DASH Threat Model

Status: rewritten 2026-10-09 (register item DOC-06). The earlier version described a gRPC service backed by one shared `redb` file, with HMAC-signed WAL entries, a `tests/tenant_isolation.rs` suite, ADR-006, JWT `jti` and request/response hashes in the audit log, retention compaction, `--max-vector-bytes`, per-IP connection caps, signed container images and a deployment audit. None of those exist in this repository. This version describes the system as the code implements it, and states for every control whether it is implemented, partial, planned, or **NOT IMPLEMENTED**.

Status legend used below:

- **Implemented**: present in code, with a test or code path cited.
- **Partial**: present but with a known defect (register ID given).
- **v0.3.0**: scheduled for the P0 hardening release; not in v0.2.x.
- **Planned (Pn)**: scheduled in a later phase of the [master plan](plans/2026-10-09-production-readiness-master-plan.md).
- **NOT IMPLEMENTED**: does not exist; do not rely on it.

Issue IDs refer to [`plans/2026-10-09-issue-register.md`](plans/2026-10-09-issue-register.md).

## 1. System overview

| Component | Role | Persistence | Default bind |
|---|---|---|---|
| `ingestion` (`services/ingestion`) | HTTP/1.1 JSON write API; owns the WAL; serves WAL and full export to followers on `/internal/replication/*` | line-oriented WAL file plus `.snapshot`; redb mirror | `127.0.0.1:8081` |
| `retrieval` (`services/retrieval`) | HTTP read API (`/v1/retrieve`), OpenAI-compatible `/v1/embeddings` proxy to the configured embedding provider; polls ingestion for replication | in-memory store; redb mirror; replication offset file | `127.0.0.1:8080` |
| `control-plane` (`services/control-plane`) | Placement CSV state, file-lease leader election, failover promotion | CSV state file plus SHA-256 checksum; lease file | `127.0.0.1:8090` |
| `segment-maintenance-daemon` (`services/indexer`) | Builds and garbage-collects index segment files | segment directory | no listener |
| Embedding provider (external) | Hash (in-process), Ollama (HTTP), OpenAI (HTTPS, v0.3.0) | n/a | n/a |
| Identity provider (external) | OIDC JWKS endpoint, optional | n/a | n/a |

There is no gRPC. There is no single shared redb file: each service has its own. Containers and Compose bind `0.0.0.0` and publish ports `8080`, `8081` and `8090` on the host.

```
client ─HTTP─► retrieval :8080 ──poll /internal/replication/*──► ingestion :8081
client ─HTTP─► ingestion :8081                                        │
operator/automation ─HTTP─► control-plane :8090                       ▼
retrieval ──HTTP(S)──► embedding provider (Ollama / OpenAI)      WAL, redb, segments, audit log (local disk)
services ──HTTPS──► OIDC JWKS endpoint (optional)
```

## 2. Trust boundaries

| # | Boundary | Untrusted side | Trusted side |
|---|---|---|---|
| 1 | Network to ingestion / retrieval public routes | any HTTP client | service process |
| 2 | Network to **`/internal/replication/*`** on ingestion | any host that can reach port 8081 | WAL contents of all tenants |
| 3 | Network to **control-plane** | any host that can reach port 8090 | placement and leader state |
| 4 | Retrieval to **embedding provider** | provider response and network path | retrieval process, provider API key |
| 5 | Service to OIDC JWKS URL | JWKS response | token validation |
| 6 | Process to local disk (WAL, redb, segments, audit, revocation file, offset file) | other local users, volume snapshots | service process |
| 7 | Operator environment (env vars, secrets, compose `.env`) | operator workstation, CI | service configuration |
| 8 | Ingestion to adapter commands (`sh -c` from `DASH_INGEST_*_ADAPTER_CMD`) | document and text bytes sent on stdin; adapter stdout | ingestion process |
| 9 | CI/CD to registry and release artifacts | CI runner, third-party actions | release artifact |

TLS is not implemented by any DASH service. It is assumed to be terminated by a proxy for boundaries 1 to 3. Boundary 2 and 3 traffic between services is plain HTTP.

## 3. Attack surface

| Surface | Routes / interface | Auth today (v0.2.x) | Auth in v0.3.0 | Notes |
|---|---|---|---|---|
| Ingestion write API | `POST /v1/ingest`, `/v1/ingest/batch`, `/v1/ingest/raw`, `/v1/ingest/document` | API key, scoped key, HS256 JWT or OIDC; **open if none configured** (SEC-01); JWT-only config lets a headerless request through (SEC-02) | credentials required to start; dev-mode opt-out | Embeddings are computed before auth (SEC-09) |
| Retrieval read API | `GET/POST /v1/retrieve` | same as above | same | Embedding computed before auth (SEC-09); recursive JSON parser with no depth limit runs pre-auth (ROB-01) |
| **Embeddings proxy** | `POST /v1/embeddings` | **none** (SEC-09) | required | Spends the configured provider's quota; with `openai` the key is sent without TLS (SEC-23, TLS in v0.3.0) |
| **Replication endpoints** | `GET /internal/replication/wal`, `/export`, `/commit-status`; `POST /internal/replication/ack` | `x-replication-token`, only if `DASH_INGEST_REPLICATION_TOKEN` is set; no deploy sets it (SEC-08); token compared with `==`, sent in plaintext | token required | `/export` dumps all tenants' claims, evidence, edges, vectors; ack can forge commit status |
| **Control plane** | `PUT /v1/control-plane/placement`, `POST .../failover/promote`, `POST .../leader/acquire`, `GET .../placement`, `GET .../leader` | **none** (SEC-07) | `DASH_CONTROL_PLANE_TOKEN` | Can re-route writes and reads to attacker-controlled nodes |
| Observability and debug | `GET /metrics`, `GET /debug/placement`, `/debug/planner`, `/debug/storage-visibility`, `/debug/document-parser` | **none** (SEC-10) | required | Leaks tenant ids, topology, planner internals |
| Health | `/health`, `/live`, `/ready` (+ `/v1/` forms) | none (intended) | none | |
| OIDC JWKS fetch | outbound HTTPS (`http://` also accepted) | n/a | n/a | Blocking 15 s fetch can stall workers if the IdP is slow (SEC-14) |
| Adapter commands | `sh -c` of operator-supplied command lines | env-var controlled | same | Input bytes come from authenticated users; the command line is operator-controlled |
| Local files | WAL, snapshot, redb, segment dirs, audit log, revocation file, offset file | filesystem permissions | same | Revocation file re-read on every request |
| Deployment manifests | compose, Helm, k8s, systemd | n/a | being corrected | Wrong secret variable names, default secrets (SEC-03, SEC-04) |
| Supply chain | GitHub Actions, `cargo`, release workflow | n/a | n/a | No signing or provenance (SEC-21) |

## 4. STRIDE analysis

### 4.1 Spoofing

| Threat | Control and status | Evidence | Residual risk |
|---|---|---|---|
| Unauthenticated access to write/read API when no credentials are configured | **Partial.** Auth is fail-open today (SEC-01). **v0.3.0:** startup refuses without credentials unless `DASH_INSECURE_DEV_MODE=1` (localhost only). | `services/*/src/transport/authz.rs` | High until v0.3.0 |
| JWT-only configuration lets a request with no `Authorization` header through | **Partial** (SEC-02). v0.3.0 P0. | `authz.rs` | High until fixed |
| Forged JWT with guessed or leaked HS256 secret | **Implemented** (signature, `exp`, optional `iss`/`aud`, `kid` rotation). Secret strength: opt-in validation, minimum 16 characters today; v0.3.0 default-on, minimum 32 and placeholder rejection. No `jti` revocation or max-lifetime check (SEC-13). | `pkg/auth/src/lib.rs` tests (`verify_hs256_token_*`) | Secret leak enables forgery until rotation |
| Tenant impersonation | **Partial.** The tenant comes from the **request** (`claim.tenant_id`, `tenant_id`) and is checked against the credential's tenant scope and the service allowlist; it is not taken only from the token. Wildcard `*` grants all tenants (SEC-12). | `transport_denies_cross_tenant_ingest_for_scoped_key`, `transport_denies_cross_tenant_retrieval_for_scoped_key` | Wildcard and unconfigured-service cases |
| Spoofed replica or control-plane client | **NOT IMPLEMENTED today** (SEC-07, SEC-08). v0.3.0: replication token and `DASH_CONTROL_PLANE_TOKEN`. Tokens are plain shared secrets over HTTP; mutual TLS is **Planned (P1/P3)**. | `replication.rs`, `control-plane/src/lib.rs` | Network position equals trust until then |
| Role escalation | **Partial.** Roles (`admin`, `ingest`, `retrieve`, `read_only`) are checked, but they have no hierarchy, a JWT without a roles claim gets all roles, and unscoped keys skip role checks (SEC-11, P1). | `pkg/auth/src/lib.rs:parse_role_claim` | Medium |

### 4.2 Tampering

| Threat | Control and status | Evidence | Residual risk |
|---|---|---|---|
| Audit log modification | **Partial.** Each line carries `seq`, `prev_hash` and `hash = SHA-256(canonical record)`. The chain is **unkeyed**: anyone who can write the file can recompute it. **NOT IMPLEMENTED:** HMAC signing, WAL signatures, external anchoring (Planned, P4). Ingestion-written chains do not verify with the bundled verifier (SEC-17); truncation of the tail is undetected; audit is off unless a path is configured and no shipped deployment enables it. | `services/ingestion/src/transport/audit.rs`, `scripts/verify_audit_chain.sh`, tests `append_audit_record_writes_chained_hash_and_seq` | An attacker with file access can rewrite history |
| WAL or snapshot tampering on disk | **NOT IMPLEMENTED.** The WAL is a text file with no record checksum or signature; replay parses whatever is there. Torn-tail truncation is **v0.3.0**. | `pkg/store/src/wal.rs` | Disk access equals full control |
| Data at rest modification | **NOT IMPLEMENTED.** DASH does not encrypt or sign WAL, redb, segments or audit files. `pkg/encryption` is a library no service calls (SEC-16, **Planned P4**). Require an encrypted, access-controlled volume. | no crate depends on `pkg/encryption` | Operator configuration |
| Placement or failover tampering | **NOT IMPLEMENTED today** (SEC-07). The control plane does enforce epoch monotonicity and a leader-only write rule. | tests `replace_placements_monotonic_rejects_epoch_regression`, `put_placement_rejects_stale_expected_epoch` | Open API until v0.3.0 |
| Replication poisoning (stale or forged WAL delta applied by a follower) | **Partial.** Offsets are tracked; no generation id, so a compacted source can desync followers silently. **v0.3.0:** WAL generation ids force resync. No signature on frames. | `services/retrieval/src/replication.rs` | Medium |
| Segment file corruption | **Implemented:** segments carry checksums and are rejected on mismatch. | test `rejects_segment_file_with_checksum_mismatch` | Low |
| Duplicate evidence inflating rankings | **Partial** (DATA-01): evidence is duplicated on retry, restart and replication re-apply. **v0.3.0:** idempotent upserts. | register DATA-01 | Integrity of citations |

### 4.3 Repudiation

| Threat | Control and status | Evidence | Residual risk |
|---|---|---|---|
| User denies an action | **Partial.** Audited requests record `ts_unix_ms`, `service`, `action`, `tenant_id`, `claim_id`, `status`, `outcome`, `reason`. **NOT IMPLEMENTED:** principal or key identity, JWT `jti`, request hash, response hash. There is no actor field, so a record cannot say who acted. Only the audited write and retrieve actions are recorded; audit is off by default. | `audit.rs` (`AuditEvent` fields) | An audit entry cannot attribute an action to a credential |
| Per-tenant audit retention | **NOT IMPLEMENTED.** One chain per service log file; no retention setting, compaction job or per-tenant chain. (`DASH_AUDIT_RETENTION_DAYS` does not exist.) | `docs-site/docs/reference/configuration.md` | Retention is an operator concern (logrotate, off-host shipping) |
| Audit write failure | **Partial.** Failures increment a metric and are logged to stderr; the request still succeeds (SEC-17). | `emit_audit_event` | Silent audit gaps |

### 4.4 Information disclosure

| Threat | Control and status | Evidence | Residual risk |
|---|---|---|---|
| Cross-tenant leak through retrieval | **Partial.** Candidate generation and final filtering compare tenant ids; ANN graphs are per tenant; claim ids reused across tenants are rejected on write. Known leaks: conflict errors name the owning tenant; `claim_id` namespace is global (existence oracle); edge `to_claim_id` is not tenant-checked; graph output can emit another tenant's ids (SEC-18). Segment directory names collide (`a.b` and `a_b`) (SEC-19, P0). **NOT IMPLEMENTED:** a dedicated isolation test suite (`tests/tenant_isolation.rs` does not exist) and `assert_tenant(request, record)` calls. | `retrieve_semantic_with_tenant_isolation_filters_other_tenants` (`pkg/store/tests/integration_retrieval.rs`); `ingest_bundle_persistent_rejects_cross_tenant_claim_id_before_wal_append` (`pkg/store`) | Do not host mutually untrusted tenants until SEC-18/19 are closed |
| Full data export through replication endpoints | **NOT IMPLEMENTED today** (SEC-08); v0.3.0 token required | `GET /internal/replication/export` | Critical until v0.3.0 |
| Tenant ids and topology via `/metrics`, `/debug/*` | **NOT IMPLEMENTED today** (SEC-10); v0.3.0 requires auth | `retrieval/src/transport.rs` | Medium |
| Provider key or query text leakage to a third party | **Partial.** With `DASH_EMBEDDING_PROVIDER=openai` query and claim text go to OpenAI and the key is sent without TLS in v0.2.x (SEC-23); v0.3.0 adds TLS. Ollama is plain HTTP by design (local network). | `pkg/embeddings/src/lib.rs` | Treat as sensitive-data egress |
| Anonymous use of embedding provider quota | **NOT IMPLEMENTED today** (SEC-09); v0.3.0 requires auth on `/v1/embeddings` and authenticates before embedding | `retrieval/src/transport.rs` | Cost and DoS |
| Verbose errors leaking internals | **Partial.** Errors are `{"error":"message"}` with store messages such as validation details and tenant names in conflicts. There is no regression test that errors never contain paths or keys. A placeholder-secret error logs the secret value (SEC-13). | `HttpResponse::*` | Low to medium |
| Data at rest readable by anyone with disk or snapshot access | **NOT IMPLEMENTED** (no application-level encryption; SEC-16). Use an encrypted volume. An earlier claim that ADR-006 records this decision was wrong: there is no ADR-006. | n/a | Operator controls |
| Secrets in deployment files and `.env` | **Partial.** `scripts/generate-secrets.sh` creates random keys but writes `.env` without restrictive permissions (SEC-24); Helm/k8s defaults are well-known strings (SEC-03, SEC-04). | `scripts/generate-secrets.sh`, `deploy/` | Operator controls until P0 deploy fixes land |
| Timing side channels | **NOT IMPLEMENTED / not assessed.** API keys are compared as plain strings (SEC-15). No side-channel review has been done. | `authz.rs` | Unknown |

### 4.5 Denial of service

| Threat | Control and status | Evidence | Residual risk |
|---|---|---|---|
| Oversized request body | **Implemented:** 16 MiB cap on `Content-Length` (returns 400). **NOT IMPLEMENTED:** `--max-vector-bytes`, `--max-batch-vectors` flags (do not exist). Batch item count is capped by `DASH_INGEST_BATCH_MAX_ITEMS` (default 128). Header size and count are unbounded (ROB-03). | `services/ingestion/src/transport/request.rs`, `config.rs` | Memory exhaustion via headers |
| Deeply nested JSON | **NOT IMPLEMENTED** (ROB-01): retrieval's recursive parser can abort the process pre-auth. P0. | `retrieval/src/transport/payload.rs` | High until fixed |
| Slow-loris / connection starvation | **Partial.** 5 s socket timeout is per read, not per request, and a fixed worker pool with a bounded queue (503 when full) bounds memory but not stalls (ROB-04). **NOT IMPLEMENTED:** per-IP connection caps (do not exist). | `transport.rs` | Worker pool exhaustion |
| Accept-loop exit | **Partial** (ROB-06): some accept errors end the server with status 0. P0. | `transport.rs` | Availability |
| Request-rate abuse | **NOT IMPLEMENTED today:** the per-tenant limiter is rebuilt per request, skips JWT paths and returns 401 (SEC-06). **v0.3.0:** enforced, HTTP 429. Not per-IP. | `authz.rs` | High until v0.3.0 |
| Expensive queries | **Partial.** `top_k` has no enforced maximum; ANN insert path is quadratic in total vectors (IDX-01). Bounded worker pool. | `payload.rs` | Medium |
| JWKS outage stalls auth | **Partial** (SEC-14). | `pkg/auth/src/oidc.rs` | Medium |
| Embedding provider failure | **Implemented:** circuit breaker wraps providers. | tests `circuit_breaker_*`, `breaker_*` and `select_from_env_returns_breaker_wrapped_network_provider` in `pkg/embeddings` | Low |

### 4.6 Elevation of privilege

| Threat | Control and status | Evidence | Residual risk |
|---|---|---|---|
| Cross-tenant access via a scoped or role-limited credential | **Partial**, see 4.4. | tests in 4.1 | see 4.4 |
| Control-plane takeover to redirect traffic | **NOT IMPLEMENTED today** (SEC-07); v0.3.0 token | `control-plane/src/lib.rs` | Critical until v0.3.0 |
| Command execution through adapter variables | **By design**: `DASH_INGEST_RAW_ADAPTER_CMD`, `..._DOCUMENT_ADAPTER_CMD`, `..._EMBEDDING_ADAPTER_CMD` are run with `sh -c`. They are operator-controlled environment variables; callers supply only stdin. No sandboxing. | `services/ingestion/src/extraction.rs` | Anyone who can set the service environment owns the host |
| Container breakout | **Partial.** Non-root user, `no-new-privileges`, capabilities dropped except `CHOWN, SETUID, SETGID, DAC_OVERRIDE`, ports on `0.0.0.0` (SEC-20). | `deploy/container/docker-compose.yml` | Reduce caps in P0/P7 |
| Tampered build or image | **NOT IMPLEMENTED:** signed images, provenance, digest pinning. The release workflow generates an SBOM with `anchore/sbom-action` only; CI uses `trivy-action@master` and tag-pinned actions (SEC-21, **Planned P7**). | `.github/workflows/release.yml`, `docs/supply-chain.md` (aspirational) | Supply-chain risk |

## 5. Controls this document previously claimed that do not exist

For clarity, the following were described as controls in earlier versions and are **NOT IMPLEMENTED**:

- HMAC-signed WAL entries and an `audit-hmac` key.
- `tests/tenant_isolation.rs` and `assert_tenant(request, record)`.
- ADR-006 (redb encryption decision).
- JWT `jti`, request hash and response hash in audit entries.
- Per-tenant audit retention configuration and a compaction job.
- `--max-vector-bytes`, `--max-batch-vectors`, `--rate-limit-rps` flags (limits are environment variables; see the configuration reference).
- Per-IP connection caps; `request_timeout` and keep-alive budgets as configurable settings.
- Signed container images and host package-manager signature verification.
- Per-tenant JWT signing keys behind a feature flag.
- Key review metadata and a periodic access review process.
- Encryption of redb at rest by DASH.
- gRPC services.

## 6. Accepted residual risks

1. HS256 shared-secret JWTs; asymmetric validation exists only through the OIDC/JWKS path, which is covered by unit tests with symmetric JWKs only.
2. No TLS in DASH; plaintext HTTP between services and to Ollama.
3. No application-level encryption at rest.
4. Single-writer ingestion with polling followers; no consensus, no automatic failover.
5. Operator compromise (environment, adapter commands, volumes) equals full compromise.
6. No external security review, penetration test or side-channel assessment has been performed.

## 7. Work tracked elsewhere

- P0 (v0.3.0): SEC-01 to SEC-10, SEC-23, ROB-01 to ROB-04, ROB-06, DATA-01, SEC-18 leak fixes, SEC-19.
- P1: SEC-11 to SEC-15, SEC-22, fuzzing of parsers and authz.
- P4: encryption at rest and key management, audit integrity (keyed chain, anchoring), tenant lifecycle.
- P7: supply chain signing and provenance, container hardening, external review.
