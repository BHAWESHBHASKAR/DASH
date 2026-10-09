# DASH Threat Model

Status: rewritten 2026-10-09 (register item DOC-06) and updated for the 0.3.0 (unreleased) tree after the P0 hardening code merged. The earlier version described a gRPC service backed by one shared `redb` file, with HMAC-signed WAL entries, a `tests/tenant_isolation.rs` suite, ADR-006, JWT `jti` and request/response hashes in the audit log, retention compaction, `--max-vector-bytes`, per-IP connection caps, signed container images and a deployment audit. None of those exist in this repository. This version describes the system as the code implements it, and states for every control whether it is implemented, partial, planned, or **NOT IMPLEMENTED**, with the test or code that evidences it.

Status legend used below:

- **Implemented**: present in code, with a test or code path cited. "Implemented" does not mean externally reviewed: no penetration test or security audit has been done.
- **Partial**: present but with a known gap (stated, with register ID where one exists).
- **Planned (Pn)**: scheduled in a later phase of the [master plan](plans/2026-10-09-production-readiness-master-plan.md); not in this tree.
- **NOT IMPLEMENTED**: does not exist; do not rely on it.

Issue IDs refer to [`plans/2026-10-09-issue-register.md`](plans/2026-10-09-issue-register.md). What the P0 code covers, area by area, is in [`plans/2026-10-09-p0-status.md`](plans/2026-10-09-p0-status.md). Tests are cited as `path::name`.

## 1. System overview

| Component | Role | Persistence | Default bind |
|---|---|---|---|
| `ingestion` (`services/ingestion`) | HTTP/1.1 JSON write API; owns the WAL; serves WAL frames and full export to followers on `/internal/replication/*` | line-oriented checksummed WAL file plus `.snapshot` and `.gen`; redb mirror | `127.0.0.1:8081` |
| `retrieval` (`services/retrieval`) | HTTP read API (`/v1/retrieve`), OpenAI-compatible `/v1/embeddings` proxy to the configured embedding provider; polls ingestion for replication | in-memory store; redb mirror; optional local WAL; replication offset file | `127.0.0.1:8080` |
| `control-plane` (`services/control-plane`) | Placement CSV state, durable fenced file-lease leader election, failover promotion | CSV state file plus SHA-256 checksum; lease file | `127.0.0.1:8090` |
| `segment-maintenance-daemon` (`services/indexer`) | Builds and garbage-collects index segment files | segment directory | no listener |
| `wal-inspect`, `audit-verify` (`tools/`) | Offline WAL and audit-chain tools | read (and, for `wal-inspect repair`, write) service files | none |
| Embedding provider (external) | Hash (in-process), Ollama (HTTP), OpenAI (HTTPS) | n/a | n/a |
| Identity provider (external) | OIDC JWKS endpoint, optional | n/a | n/a |

There is no gRPC. There is no single shared redb file: each service has its own. Containers and Compose bind `0.0.0.0` inside the container; Compose publishes the host ports on `127.0.0.1` by default (`DASH_PUBLISH_ADDR`).

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
| 4 | Retrieval and ingestion to **embedding provider** | provider response and network path | service process, provider API key |
| 5 | Service to OIDC JWKS URL | JWKS response | token validation |
| 6 | Process to local disk (WAL, redb, segments, audit, revocation files, offset file) | other local users, volume snapshots | service process |
| 7 | Operator environment (env vars, secrets, compose `.env`) | operator workstation, CI | service configuration |
| 8 | Ingestion to adapter commands (`sh -c` from `DASH_INGEST_*_ADAPTER_CMD`) | document and text bytes sent on stdin; adapter stdout | ingestion process |
| 9 | CI/CD to registry and release artifacts | CI runner, third-party actions | release artifact |

TLS is not implemented by any DASH service. It is assumed to be terminated by a proxy for boundaries 1 to 3. Boundary 2 and 3 traffic between services is plain HTTP authenticated by a shared token; **mTLS is NOT IMPLEMENTED (Planned P1/P3)**.

## 3. Attack surface

| Surface | Routes / interface | Authentication (0.3.0) | Notes |
|---|---|---|---|
| Ingestion write API | `POST /v1/ingest`, `/v1/ingest/batch`, `/v1/ingest/raw`, `/v1/ingest/document` | API key, scoped key, HS256 JWT or OIDC; role `ingest`; startup refused with none configured unless `DASH_INSECURE_DEV_MODE=1` | Embeddings are computed only after authorization. Per-tenant rate limit (429). |
| Retrieval read API | `GET/POST /v1/retrieve` | same; role `retrieve` | Depth-limited JSON parser; request bounds (`top_k`, filters, query size). |
| **Embeddings proxy** | `POST /v1/embeddings` | same; role `retrieve` | Spends the configured provider's quota; authentication runs first; OpenAI key goes over HTTPS only (plaintext to a remote host is refused). |
| **Replication endpoints** | `GET /internal/replication/wal`, `/export`, `/commit-status`; `POST /internal/replication/ack` | `x-replication-token` (constant-time compare); 403 when no token is configured outside dev mode | `/export` dumps all tenants' claims, evidence, edges, vectors; `ack` can influence commit status. |
| **Control plane** | `PUT /v1/control-plane/placement`, `POST .../failover/promote`, `POST .../leader/acquire`, `POST .../replica-lag`, `GET .../placement`, `GET .../leader` | `Authorization: Bearer <DASH_CONTROL_PLANE_TOKEN>`; startup refused without a token unless dev mode | Can re-route writes and reads. The token has no minimum-length check. |
| Observability and debug | `GET /metrics`, `GET /debug/placement`, `/debug/planner`, `/debug/storage-visibility`, `/debug/document-parser` | credential with `read_only` (or `admin`); `/metrics` only can be exempted with `DASH_METRICS_PUBLIC=1` | Tenant ids, topology, planner internals. |
| Health | `/health`, `/live`, `/ready` (+ `/v1/` forms) | none (intended) | `/ready` reports replication lag and staleness details. |
| OIDC JWKS fetch | outbound HTTPS (`http://` only to loopback unless `DASH_OIDC_ALLOW_INSECURE_JWKS=1`) | n/a | 3 s timeout, 256 KiB cap, no redirects, single-flight. |
| Adapter commands | `sh -c` of operator-supplied command lines | env-var controlled | Input bytes come from authenticated users; the command line is operator-controlled. |
| Local files | WAL, snapshot, generation file, redb, segment dirs, audit log, revocation files, offset file | filesystem permissions | Revocation files re-read when their mtime or size changes. |
| Deployment manifests | compose, Helm, k8s, systemd | n/a | Secrets required (no defaults); see Section 4.4. |
| Supply chain | GitHub Actions, `cargo`, release workflow | n/a | No signing or provenance (SEC-21). |

## 4. STRIDE analysis

### 4.1 Spoofing

| Threat | Control and status | Evidence | Residual risk |
|---|---|---|---|
| Unauthenticated access when no credentials are configured | **Implemented.** Deny by default: startup exits (code 2) without credentials unless `DASH_INSECURE_DEV_MODE=1`, which also forces a loopback bind. A policy that cannot be built denies every request. | `services/retrieval/src/transport/authz_matrix_tests.rs::unconfigured_service_denies_everything_except_probes`, `explicit_dev_mode_allows_unauthenticated_requests`; `services/common/src/lib.rs::dev_mode_forces_loopback_bind_unless_explicitly_allowed`; `services/control-plane/src/tests.rs::startup_refuses_without_token_unless_insecure_dev` | An operator who sets dev mode in production |
| JWT-only configuration lets a headerless request through | **Implemented.** A JWT-only configuration has no open API-key branch. | `authz_matrix_tests.rs::jwt_only_policy_does_not_fall_through_to_an_open_api_key_branch`; `services/ingestion/tests/transport_http.rs::transport_jwt_only_config_rejects_ingest_without_a_token` | Low |
| Forged JWT with guessed or leaked HS256 secret | **Implemented.** Signature, mandatory `exp`, maximum lifetime (default 24 h), leeway capped at 60 s, optional `iss`/`aud`, `kid` rotation, `jti` denylist, empty keys rejected. Secret strength: strict validation is on by default (HS256 secrets at least 32 characters, placeholders rejected). | `pkg/auth/src/lib.rs::verify_hs256_token_for_tenant_uses_kid_secret_map`; `services/common/src/policy/hardening_tests.rs::jwt_lifetime_exp_and_leeway_limits_apply`, `revoked_jti_is_rejected_from_env_list_and_reloaded_file`, `short_hs256_secrets_are_rejected_by_the_strict_validator` | A leaked HS256 secret enables forgery until rotation; there is no per-token revocation other than the `jti` list |
| Forged or confused OIDC token | **Implemented** for RS256 against a stub IdP: `iss`, `aud`, `exp` required, algorithm allow-list (no symmetric or `none`), `kid` required, JWK `alg`/`use` enforced. ES256, EdDSA and PS* are accepted by the allow-list but have no dedicated test. | `pkg/auth/src/oidc_tests.rs::rs256_token_is_accepted_with_matching_key`, `rs256_token_signed_by_another_key_is_rejected`, `symmetric_algorithms_are_not_in_the_allow_list`, `aud_iss_and_exp_are_required_and_checked`, `jwk_own_alg_and_use_are_enforced` | No test against a real IdP |
| Tenant impersonation | **Partial.** The tenant comes from the **request** (`claim.tenant_id`, `tenant_id`) and is checked against the credential's tenant scope and the service allowlist. A `*` tenant in a token is honored only with `DASH_*_JWT_ALLOW_WILDCARD_TENANT=1`. | `transport_denies_cross_tenant_ingest_for_scoped_key`, `transport_denies_cross_tenant_retrieval_for_scoped_key`; `hardening_tests.rs::wildcard_tenant_token_needs_the_explicit_flag` | A scoped key with tenant `*` is all-tenant by design |
| Spoofed replica or control-plane client | **Implemented** as shared secrets: replication requires `x-replication-token`, the control plane requires a bearer token, both compared in constant time. **NOT IMPLEMENTED:** mutual TLS and per-node identities (Planned P1/P3); the tokens travel over plain HTTP. | `services/ingestion/tests/transport_http.rs::transport_replication_endpoints_are_closed_without_a_token_outside_dev_mode`, `transport_replication_token_must_match_exactly`; `services/control-plane/src/tests.rs::protected_routes_require_bearer_token`, `bearer_scheme_is_case_insensitive_and_token_exact`, `constant_time_eq_matches_plain_equality` | Network position plus a leaked token equals trust |
| Role escalation | **Implemented.** `admin` implies all roles, `read_only` implies `retrieve` only, role-less JWTs get no roles, legacy keys get an explicit default role set (never "all roles"). | `authz_matrix_tests.rs::role_matrix_for_jwts_and_scoped_keys`, `jwt_without_role_claim_gets_no_roles_unless_a_default_is_configured`, `legacy_unscoped_keys_get_an_explicit_default_role_set` | Misconfigured `*_DEFAULT_ROLES` |

### 4.2 Tampering

| Threat | Control and status | Evidence | Residual risk |
|---|---|---|---|
| Audit log modification | **Partial.** One canonical v2 encoding for both services; each line carries `seq`, `prev_hash` and `hash = SHA-256(canonical record)`; the shared verifier detects edited, deleted and reordered records, unexplained restarts and torn tails. The chain is **unkeyed**: anyone who can write the file can recompute it. Tail truncation is detected only if the last `seq`/hash are recorded out of band. **NOT IMPLEMENTED:** HMAC or signed checkpoints, external anchoring (Planned P4). Audit is off unless a path is configured; the Compose, Kubernetes, Helm and systemd examples set one. | `services/common/tests/audit_chain.rs::tamper_modified_record_detected`, `tamper_deleted_middle_record_detected`, `tamper_reordered_records_detected`, `tail_truncation_detected_only_with_checkpoint`, `concurrent_writers_do_not_fork_chain`; `docs/operations/audit-chain.md` | An attacker with file access can rewrite history |
| WAL or snapshot tampering on disk | **Partial.** Every WAL record kind written since 0.3.0 carries a CRC-32 (`crc=`): this detects accidental corruption and torn writes, not an attacker, who can recompute it. Replay refuses a damaged interior record and truncates a damaged tail. **NOT IMPLEMENTED:** keyed signatures on WAL or snapshot. | `pkg/store/src/wal.rs::versioned_records_require_a_valid_checksum`; `pkg/store/tests/wal_torn_tail.rs::flipped_byte_in_the_final_record_is_treated_as_torn`; `tools/wal-inspect/tests/cli.rs::verify_fails_on_a_middle_checksum_failure_but_not_on_a_torn_tail` | Disk access equals full control |
| Data at rest modification | **NOT IMPLEMENTED.** DASH does not encrypt or sign WAL, redb, segments or audit files. `pkg/encryption` is a library no service calls (SEC-16, **Planned P4**). Require an encrypted, access-controlled volume. | no crate depends on `pkg/encryption` | Operator configuration |
| Placement or failover tampering | **Implemented** at the application layer: token authentication, leader-only writes, monotonic epochs, optimistic `expected_epoch`, fenced durable leases, promotion refused unless the replica reported zero lag. | `services/control-plane/src/tests.rs::unauthenticated_mutation_does_not_change_state`, `put_placement_rejects_stale_expected_epoch`, `replace_placements_monotonic_rejects_epoch_regression`, `promotion_refuses_unknown_or_behind_replica_unless_forced`, `leader_responses_expose_fencing_token` | A leaked control-plane token |
| Stale placement causing split-brain writes | **Implemented.** No silent fallback to a placement file when the control plane is unreachable (unless `DASH_ROUTER_ALLOW_STALE_PLACEMENT=1`); ingestion refuses writes when reloads have failed longer than the grace. | `services/metadata-router/src/lib.rs::unreachable_control_plane_does_not_fall_back_to_stale_file_by_default`; `services/ingestion/src/transport/placement_routing.rs::writes_are_refused_after_the_stale_grace_and_recover_with_the_source`, `epoch_regressions_are_rejected_and_the_current_placement_is_kept` | No consensus; a single lease file decides leadership |
| Replication poisoning (stale or forged WAL delta applied by a follower) | **Partial.** Token-authenticated; generation-aware offsets force a resync after leader compaction; bounded response sizes; frames apply atomically; frames never end inside a commit group. **NOT IMPLEMENTED:** signatures on frames, TLS. | `services/ingestion/tests/replication_follower.rs::leader_frames_and_exports_carry_the_wal_generation`, `leader_checkpoint_between_groups_forces_resync_and_converges`; `services/retrieval/tests/replication_e2e.rs::resync_replaces_stale_state_and_keeps_redb_in_sync` | A network attacker can alter frames in flight |
| Segment file corruption | **Implemented:** segments carry checksums and are rejected on mismatch; segment publishing is race-free. | `services/indexer/src/lib.rs::rejects_segment_file_with_checksum_mismatch` | Low |
| Duplicate evidence inflating rankings | **Implemented.** Evidence and edges are idempotent upserts; evidence from one `source_id` counts once toward the ranking signal; contributions saturate. | `pkg/store/tests/semantics_idempotency.rs::evidence_is_upserted_by_evidence_id`, `replication_reapply_is_idempotent`; `pkg/store/tests/semantics_query.rs::repeated_evidence_from_one_source_counts_once_for_ranking`, `support_bonus_saturates_for_many_supporting_edges` | Distinct sources can still be fabricated by a credentialed writer |

### 4.3 Repudiation

| Threat | Control and status | Evidence | Residual risk |
|---|---|---|---|
| User denies an action | **Partial.** Audited requests record `ts_unix_ms`, `service`, `action`, `tenant_id`, `claim_id`, `status`, `outcome`, `reason`, an `actor` (kind `api_key` or `jwt`, plus the first 8 hex characters of SHA-256 of the presented credential, never the credential) and `request_id`. **NOT IMPLEMENTED:** client IP, JWT `sub`, request and response hashes. Only the audited write and retrieve actions are recorded; audit is off unless a path is configured. | `services/common/tests/audit_chain.rs::actor_context_is_recorded_without_the_secret` | A fingerprint identifies a credential, not a person |
| Per-tenant audit retention | **NOT IMPLEMENTED.** One chain per service log file; no retention setting, compaction job or per-tenant chain. | `docs-site/docs/reference/configuration.md` | Retention is an operator concern (logrotate, off-host shipping) |
| Audit write failure | **Partial.** With `DASH_*_AUDIT_FAIL_CLOSED=1` the service refuses `/v1/ingest*` or `/v1/retrieve` (503) when the audit log cannot be opened, locked and its tail recovered. The append itself happens after the mutation, so a failure between the gate and the append leaves a committed write without a record; this is logged and counted in `dash_audit_write_failures_total`. | `docs/operations/audit-chain.md` | Silent gap possible in that window |

### 4.4 Information disclosure

| Threat | Control and status | Evidence | Residual risk |
|---|---|---|---|
| Cross-tenant leak through retrieval | **Partial.** Candidate generation and final filtering compare tenant ids; ANN graphs and every vector fallback are per tenant; conflict errors no longer name the owning tenant; segment directories are collision-free. Remaining: the `claim_id` namespace is global, so a 409 on write is an existence oracle for another tenant's id (SEC-18); edge `to_claim_id` existence and tenant are not checked at write (ranking ignores such edges); whether graph output can emit another tenant's ids was not re-verified in this pass. **NOT IMPLEMENTED:** a dedicated isolation test suite (`tests/tenant_isolation.rs` does not exist) and `assert_tenant(request, record)` calls. | `pkg/store/tests/integration_retrieval.rs::retrieve_semantic_with_tenant_isolation_filters_other_tenants`; `pkg/store/tests/semantics_query.rs::wrong_dimension_query_returns_empty_and_never_scans_other_tenants`, `cross_tenant_conflict_error_does_not_name_the_other_tenant`, `dangling_and_cross_tenant_edges_are_ignored`; `services/indexer/src/lib.rs::tenant_dir_name_is_injective_for_legacy_collision_pairs` | Do not host mutually untrusted tenants until SEC-18 is closed |
| Full data export through replication endpoints | **Implemented.** A token is required; without one the endpoints answer 403 (outside dev mode). | `transport_replication_endpoints_are_closed_without_a_token_outside_dev_mode`, `transport_replication_token_must_match_exactly` | Anyone holding the token can read all tenants |
| Tenant ids and topology via `/metrics`, `/debug/*` | **Implemented.** `read_only` or `admin` credential required; `/metrics` can be exempted explicitly. | `services/ingestion/tests/transport_http.rs::transport_metrics_and_debug_require_authentication_but_probes_stay_open`; `authz_matrix_tests.rs::metrics_can_only_be_made_public_with_the_explicit_flag` | `DASH_METRICS_PUBLIC=1` exposes `/metrics` |
| Provider key or query text leakage to a third party | **Partial.** The OpenAI client speaks HTTPS, refuses to send a key over plaintext HTTP to a non-loopback host, does not follow redirects and never prints the key. Ollama is plain HTTP by design (local network). Query and claim text still leave the process for hosted providers. | `pkg/embeddings/src/lib.rs::https_endpoint_speaks_tls_and_never_leaks_key_in_cleartext`, `openai_provider_refuses_plain_http_to_remote_host_without_connecting`, `redirects_are_not_followed`, `openai_debug_does_not_print_api_key` | Treat as sensitive-data egress; no test with a completed TLS exchange |
| Anonymous use of embedding provider quota | **Implemented.** `/v1/embeddings` requires `retrieve`; retrieve and ingest authorize before embedding; rate limiting applies per tenant. | `services/ingestion/tests/transport_http.rs::transport_ingest_authorizes_before_calling_the_embedding_provider`; `services/retrieval/tests/transport_http.rs::transport_embeddings_metrics_and_debug_require_authentication` | Authenticated abuse is bounded only by the rate limit |
| Verbose errors leaking internals | **Partial.** Auth errors use fixed messages and never echo claim values; embedding failures return short codes with details logged; secret-validation errors name the setting, not the value. Store validation errors still carry field details. No test asserts that no error ever contains a path or key. | `hardening_tests.rs::jwt_error_decisions_never_echo_claim_values`; `services/retrieval/tests/http_robustness.rs::embedding_provider_failure_maps_to_502_without_leaking_detail`; `services/common/src/lib.rs::placeholders_are_rejected_without_echoing_the_value` | Low to medium |
| Data at rest readable by anyone with disk or snapshot access | **NOT IMPLEMENTED** (no application-level encryption; SEC-16). Use an encrypted volume. There is no ADR-006. | n/a | Operator controls |
| Secrets in deployment files and `.env` | **Partial.** `scripts/generate-secrets.sh` writes random keys and tokens to a mode-0600 file; Compose requires them (`${VAR:?}`); Kubernetes manifests no longer contain secrets; Helm has no secret defaults; systemd secrets are generated at install. Strict validation rejects placeholders and short values at startup. | `scripts/generate-secrets.sh`, `deploy/container/docker-compose.yml`, `deploy/k8s/11-secrets.yaml`, `deploy/helm/dash/values.yaml` | Operator handling of the secret store |
| Timing side channels | **Partial.** API keys, the replication token and the control-plane token are compared in constant time. No wider side-channel review has been done. | `services/control-plane/src/tests.rs::constant_time_eq_matches_plain_equality`; `services/common/src/lib.rs::constant_time_eq` | Unknown beyond the compared secrets |

### 4.5 Denial of service

| Threat | Control and status | Evidence | Residual risk |
|---|---|---|---|
| Oversized request | **Implemented:** `Content-Length` over 16 MiB is rejected with 413 before the body is read; request line and header lines over 8 KiB, header blocks over 32 KiB and more than 100 headers get 431; `Transfer-Encoding` gets 501. Batch item count is capped by `DASH_INGEST_BATCH_MAX_ITEMS` (default 128). **NOT IMPLEMENTED:** `--max-vector-bytes`, `--max-batch-vectors` flags (do not exist). | `services/ingestion/tests/http_robustness.rs::chunked_is_501_and_oversized_length_is_413`, `huge_header_and_too_many_headers_are_rejected_with_431`; `services/retrieval/tests/http_robustness.rs::oversized_content_length_is_rejected_with_413_before_body_is_sent` | Low |
| Deeply nested JSON | **Implemented:** the JSON parser has a depth limit; the process no longer aborts. | `services/retrieval/tests/http_robustness.rs::deeply_nested_json_returns_400_and_does_not_abort_process`; `services/ingestion/tests/http_robustness.rs::deeply_nested_json_is_rejected_and_server_survives` | Low |
| Slow-loris / connection starvation | **Implemented:** a whole-request deadline measured from accept (default 10 s, 408), a first-byte timeout (default 2 s), connections that outwait the deadline in the queue are dropped unserved, a per-IP concurrent-connection cap (default 64, `DASH_HTTP_MAX_CONNS_PER_IP`), a fixed worker pool with a bounded queue (503 when full), and reserved workers for health probes. | `services/retrieval/tests/http_robustness.rs::slow_trickle_client_is_dropped_at_whole_request_deadline`, `idle_sockets_do_not_starve_health_or_normal_requests`, `per_ip_connection_cap_sheds_excess_and_recovers` (same names in ingestion) | A distributed attacker with many source IPs can still fill the queue; the per-IP cap also throttles clients behind a single-IP load balancer unless raised |
| Accept-loop exit | **Implemented:** accept errors back off and the loop continues; only the shutdown flag ends it. | `services/ingestion/src/transport/server_runtime.rs` (accept loop) | Low |
| Request-rate abuse | **Implemented:** per-tenant token bucket (API keys, JWT and OIDC), 429 with `Retry-After`. Not per IP; unauthenticated requests fail fast without consuming a tenant bucket. | `authz_matrix_tests.rs::rate_limit_returns_429_with_retry_after_for_api_keys_and_jwts`; `services/ingestion/tests/transport_http.rs::transport_rate_limiter_keeps_state_across_requests_and_sets_retry_after` | Per-process state; not shared across replicas |
| Expensive queries | **Partial.** `top_k` is bounded (default 1000), as are query size, filter counts and vector length; one candidate scan per request; the segment prefilter resolves before the store lock is taken. The cost of the in-repo ANN insert path at scale was not re-measured (IDX-01). | `services/retrieval/src/transport/payload.rs::retrieve_limits_are_enforced`; `services/retrieval/tests/http_robustness.rs::store_write_lock_is_not_blocked_by_slow_embedding_provider` | Medium |
| JWKS outage stalls auth | **Implemented:** 3 s fetch timeout, single-flight, stale keys served for up to 24 h, failures cached 30 s, garbage tokens never trigger a fetch. | `pkg/auth/src/oidc_tests.rs::stub_idp_outage_fails_fast_and_is_negatively_cached`, `stale_keys_are_served_when_refresh_fails_for_up_to_24_hours`, `slow_idp_is_cut_off_by_the_fetch_timeout`, `garbage_tokens_never_cause_a_fetch` | Low |
| Embedding provider failure | **Implemented:** the circuit breaker wraps every network provider (single half-open probe; only transport errors, timeouts and 5xx count), a per-process concurrency cap fails fast, outages answer 503 `embedding_unavailable` with `Retry-After`, and unusable provider output answers 502. | `pkg/embeddings/src/lib.rs::breaker_opens_on_5xx_fails_fast_probes_and_recovers`, `breaker_never_opens_on_client_errors`, `select_from_env_returns_breaker_wrapped_network_provider`, `concurrency_limit_fails_fast_with_overloaded`; `health_stays_fast_while_embedding_calls_are_slow` in both services | Low |

### 4.6 Elevation of privilege

| Threat | Control and status | Evidence | Residual risk |
|---|---|---|---|
| Cross-tenant access via a scoped or role-limited credential | **Partial**, see 4.4. | tests in 4.1 | see 4.4 |
| Control-plane takeover to redirect traffic | **Implemented** (bearer token required; see 4.1 and 4.2). | `services/control-plane/src/tests.rs::protected_routes_require_bearer_token`, `state_without_configured_auth_denies_protected_routes` | A leaked token; no mTLS |
| Command execution through adapter variables | **By design**: `DASH_INGEST_RAW_ADAPTER_CMD`, `..._DOCUMENT_ADAPTER_CMD`, `..._EMBEDDING_ADAPTER_CMD` are run with `sh -c`. They are operator-controlled environment variables; callers supply only stdin. No sandboxing. | `services/ingestion/src/extraction.rs` | Anyone who can set the service environment owns the host |
| Container breakout | **Partial.** Compose: non-root user, `no-new-privileges`, all capabilities dropped, read-only root filesystem, host ports bound to `127.0.0.1`. The Kubernetes manifests set a read-only root filesystem, no privilege escalation and drop all capabilities. systemd units add sandboxing and per-service state directories. Image digests are not pinned. | `deploy/container/docker-compose.yml`, `deploy/k8s/30-ingestion.yaml`, `deploy/systemd/dash-ingestion.service` | Digest pinning is Planned P7 |
| Tampered build or image | **NOT IMPLEMENTED:** signed images, provenance, digest pinning. The release workflow generates an SBOM with `anchore/sbom-action` only; the Trivy action is pinned but other actions are tag-pinned (SEC-21, **Planned P7**). | `.github/workflows/release.yml`, `docs/supply-chain.md` (aspirational) | Supply-chain risk |

## 5. Controls this document previously claimed that do not exist

For clarity, the following were described as controls in earlier versions and are **NOT IMPLEMENTED**:

- HMAC-signed WAL entries and an `audit-hmac` key. (The audit chain and the WAL have unkeyed integrity data only.)
- `tests/tenant_isolation.rs` and `assert_tenant(request, record)`.
- ADR-006 (redb encryption decision).
- JWT `jti`, request hash and response hash in audit entries. (A `jti` **denylist** exists for rejecting tokens; `jti` is not recorded in audit entries.)
- Per-tenant audit retention configuration and a compaction job.
- `--max-vector-bytes`, `--max-batch-vectors`, `--rate-limit-rps` flags (limits are environment variables; see the configuration reference).
- Per-IP connection caps. (The whole-request deadline is configurable with `DASH_HTTP_REQUEST_TIMEOUT_MS`.)
- Signed container images and host package-manager signature verification.
- Per-tenant JWT signing keys behind a feature flag.
- Key review metadata and a periodic access review process.
- Encryption of redb at rest by DASH.
- gRPC services.

## 6. Accepted residual risks

1. HS256 shared-secret JWTs; asymmetric validation exists only through the OIDC/JWKS path, which is covered by tests with generated RS256 keys and an in-process stub IdP, not a real identity provider.
2. No TLS in DASH; plaintext HTTP between services and to Ollama; replication and control-plane credentials are shared tokens.
3. No application-level encryption at rest; integrity data (CRC-32, SHA-256 chain) is unkeyed.
4. Single-writer ingestion with polling followers; no consensus, no automatic failover; a single lease file decides control-plane leadership.
5. Operator compromise (environment, adapter commands, volumes) equals full compromise.
6. No external security review, penetration test or side-channel assessment has been performed.
7. The `claim_id` namespace is global across tenants (existence oracle, SEC-18).

## 7. Work tracked elsewhere

- Remaining P0 verification and open items: [`plans/2026-10-09-p0-status.md`](plans/2026-10-09-p0-status.md).
- P1: SEC-15 (key storage), SEC-22 (fuzzing of parsers and authz), mTLS between services.
- P3: consensus replication.
- P4: encryption at rest and key management, audit integrity (keyed chain, anchoring), tenant lifecycle, SEC-18 namespacing.
- P7: supply chain signing and provenance, container digest pinning, external review and penetration test.
