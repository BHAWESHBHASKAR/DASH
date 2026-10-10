# DASH Issue Register — 2026-10-09 Deep Review

Date: 2026-10-09
Status: Open — tracked by [`2026-10-09-production-readiness-master-plan.md`](./2026-10-09-production-readiness-master-plan.md)
Closure status (P0 phase, refreshed at `6988dd5`): 66 of 74 P0 rows CLOSED, 8 PARTIAL; 21 non-P0 rows closed early. See [`2026-10-09-p0-status.md`](./2026-10-09-p0-status.md).
Source: full-repository review (all Rust crates, six SDKs, deploy, CI, docs).
Baseline at review time: `cargo test --workspace` 416 passed / 0 failed, clippy clean;
Go/Python/TypeScript SDK suites pass; Java SDK 4 failures + 14 errors.

Every issue has a stable ID. The master plan references these IDs; an issue is
closed only when the fix is merged **and** a regression test that would have
caught it runs in CI (see master plan §9, "Definition of Done").

Severity: **S0** = exploitable / data loss / service cannot run;
**S1** = serious correctness or security gap; **S2** = degraded behavior,
performance or operability; **S3** = hygiene, docs, maintainability.

Phase column refers to the master plan phases (P0 … P7).

---

## SEC — Security, authentication, authorization, audit

| ID | Sev | Issue | Evidence | Phase |
|---|---|---|---|---|
| SEC-01 | S0 | Auth is **fail-open**: no keys/JWT configured ⇒ every request `Allowed` for every tenant. Strict mode is opt-in. | `services/*/src/transport/authz.rs` ~L225-264; `services/common/src/lib.rs:119` | P0 |
| SEC-02 | S0 | **JWT-only bypass**: when only a JWT secret is set, a request with no `Authorization` header falls through to the API-key branch and is allowed (reproduced: cross-tenant read). | `authz.rs` ~L186-264 (both services) | P0 |
| SEC-03 | S0 | k8s/Helm set `DASH_INGESTION_API_KEY` / `DASH_INGESTION_JWT_HS256_SECRET`; code reads `DASH_INGEST_*` ⇒ ingestion runs unauthenticated. | `deploy/k8s/11-secrets.yaml:46-48`; `deploy/helm/dash/templates/config.yaml:83-85`; `services/ingestion/src/main.rs:392` | P0 |
| SEC-04 | S0 | Helm/k8s default to well-known secrets (`change-me-retrieval-key`, `change-me-retrieval-jwt`) and do not set `DASH_STRICT_SECRETS`, so they are accepted. | `deploy/helm/dash/templates/config.yaml`; `deploy/k8s/11-secrets.yaml:28-30` | P0 |
| SEC-05 | S1 | `.env.example` placeholders (`<generate-a-32-char-random-string>`) pass secret validation even in strict mode. | `deploy/container/.env.example:9-14`; `services/common/src/lib.rs:79-89` | P0 |
| SEC-06 | S0 | **Rate limiter never limits**: `AuthPolicy::from_env` (and a new `TenantRateLimiter`) is built per request; JWT paths skip it; `rps` ignored; returns 401 instead of 429. | `ingestion/src/transport/routes.rs:5`; `retrieval/src/transport.rs:1124`; `authz.rs:91/99, 633` | P0 |
| SEC-07 | S0 | **Control plane has no auth**: `PUT /v1/control-plane/placement`, `POST …/failover/promote`, `…/leader/acquire` open; published on `0.0.0.0:8090` in compose. | `services/control-plane/src/lib.rs:494-562` | P0 |
| SEC-08 | S0 | Replication endpoints (`/internal/replication/{export,wal,ack}`) open when `DASH_INGEST_REPLICATION_TOKEN` unset (no deploy sets it) ⇒ full multi-tenant data dump, fake acks. Token compared with `==`, sent over plaintext HTTP. | `ingestion/src/transport/replication.rs:266-269`; `transport.rs:674` | P0 |
| SEC-09 | S0 | `/v1/embeddings` unauthenticated; `/v1/retrieve` and `/v1/ingest` compute embeddings **before** auth ⇒ anonymous spend of paid provider quota / DoS. | `retrieval/src/transport.rs:1336, 1438, 1515-1518`; `ingestion/.../ingest_routes.rs:29` | P0 |
| SEC-10 | S1 | `/debug/placement` and `/metrics` unauthenticated (tenant IDs, topology). | `retrieval/src/transport.rs:1199` | P0 |
| SEC-11 | S1 | JWT with no `dash_roles` claim gets **all roles**; roles have no hierarchy (`admin` grants nothing else); legacy (unscoped) keys bypass role checks. | `pkg/auth/src/lib.rs:74, 88-91`; `authz.rs:219-224` | P1 |
| SEC-12 | S1 | OIDC `aud` optional; incomplete OIDC config silently falls back to HS256/API keys/open; `"*"` tenant wildcard in token grants all tenants. | `pkg/auth/src/oidc.rs:141, 173-177`; `authz.rs:443-459`; `lib.rs:293` | P1 |
| SEC-13 | S2 | No JWT revocation (`jti`), no max-lifetime/`iat` check, no leeway cap; HS256 only checked for length ≥16; scoped keys/replication token never validated; placeholder error logs the secret value. | `pkg/auth/src/lib.rs`; `services/common/src/lib.rs:89` | P1 |
| SEC-14 | S1 | JWKS: fetched before header parse; no single-flight; no stale-while-revalidate; failures uncached; unknown `kid` never refreshes; `http://` accepted; blocking 15 s fetch on 8 workers ⇒ IdP outage stalls service. | `pkg/auth/src/oidc.rs:53-56, 163, 206` | P1 |
| SEC-15 | S2 | API keys stored/compared in plaintext; revocation file re-read on every request; `revoke*` dead code; non-atomic persist. | `authz.rs:532`; `pkg/auth` | P1 |
| SEC-16 | S1 | CMEK/encryption library is **not wired** into any storage path (no crate depends on `pkg/encryption`); default is silent NoOp; no key IDs/rotation/zeroization. | `pkg/encryption/src/lib.rs`; workspace `Cargo.toml` deps | P4 |
| SEC-17 | S1 | Audit chain: ingestion hashes `serde_json::json!` (sorted keys) but verifier hashes insertion order ⇒ ingestion logs never verify; unkeyed SHA-256 (rewritable); tail truncation undetected; `{}` line restarts chain; torn line disables auditing; failures only logged; no actor/principal; in-process mutex only; disabled in every deploy. | `ingestion/src/transport/audit.rs:45-50, 129-141, 183-193`; `scripts/verify_audit_chain.sh:101-107` | P1 (fix) / P4 (redesign) |
| SEC-18 | S1 | Cross-tenant leakage: conflict errors name the owning tenant; `claim_id` namespace is global (existence oracle); edge `to_claim_id` not tenant-checked; graph output emits other tenants' IDs. | `pkg/store/src/lib.rs:1274-1277, 1345-1348`; `retrieval/src/api.rs:369` | P0 (leak) / P2 (namespacing) |
| SEC-19 | S1 | Tenant directory sanitization collides (`a.b` ≡ `a_b`) ⇒ one tenant's segments overwrite / mix with another's. | `ingestion/src/transport/config.rs:79`; `retrieval/src/api/segment_storage.rs:233-245` | P0 |
| SEC-20 | S2 | Containers: extra caps (`CHOWN, SETUID, SETGID, DAC_OVERRIDE`), ports on 0.0.0.0, binaries owned by runtime user, images tag-pinned not digest-pinned. | `deploy/container/docker-compose.yml:11`; `Dockerfile:79-86` | P0/P7 |
| SEC-21 | S2 | CI supply chain: `trivy-action@master`, actions pinned by tag not SHA, `cargo install` without `--locked`, broad release permissions, no SBOM / signing / provenance. | `.github/workflows/security.yml:63`, `release.yml` | P7 |
| SEC-22 | S2 | Fuzzing ineffective: `fuzz_jwt` never passes signature check; no targets for the hand-rolled HTTP/JSON parsers, OIDC or authz; corpora not wired. | `fuzz/fuzz_targets/*` | P1 |
| SEC-23 | S1 | OpenAI provider sends the Bearer key in **plaintext** over a raw `TcpStream` to port 443 (no TLS). | `pkg/embeddings/src/lib.rs:199, 326-372` | P0 |
| SEC-24 | S3 | `generate-secrets.sh` writes `.env` with default permissions (not 0600). | `scripts/generate-secrets.sh` | P0 |

## ROB — HTTP/JSON robustness

| ID | Sev | Issue | Evidence | Phase |
|---|---|---|---|---|
| ROB-01 | S0 | **Process abort** from deeply nested JSON (recursive parser, no depth limit, runs pre-auth). One 200 KB request kills retrieval. | `retrieval/src/transport/payload.rs:369-454` | P0 |
| ROB-02 | S1 | POST JSON strings decode each UTF-8 byte as a Latin-1 char (`café` → `cafÃ©`); surrogate pairs rejected; GET and POST disagree. | `retrieval/src/transport/payload.rs:476-488` | P0 |
| ROB-03 | S1 | Unbounded header line length / header count ⇒ memory exhaustion. | `retrieval/src/transport/http.rs:12-35`; `ingestion/src/transport/request.rs:13-25` | P0 |
| ROB-04 | S1 | Slowloris: 5 s timeout is per `read()`, not per request; ~32 trickling connections exhaust the worker pool. | `retrieval/src/transport.rs:1025` | P0 (mitigate) / P1 (fix) |
| ROB-05 | S2 | `Transfer-Encoding: chunked` ignored; duplicate `Content-Length` last-wins; bare `\n` rejected. | `http.rs:34-50` | P1 |
| ROB-06 | S1 | Any non-`WouldBlock` accept error (`EMFILE`, `ECONNABORTED`) breaks the loop ⇒ server exits with status 0. | `retrieval/src/transport.rs:916-919` (same in ingestion) | P0 |
| ROB-07 | S2 | Status mapping: 413/429/… rendered as 500; provider failures returned as 400. | `http.rs:113-122`; `transport.rs:1544` | P1 |
| ROB-08 | S3 | No keep-alive (one request per TCP connection); accepted sockets may inherit `O_NONBLOCK` on BSD/macOS. | `http.rs:125-128` | P1 |
| ROB-09 | S2 | Ingestion `json_escape` leaves control chars < 0x20 unescaped ⇒ invalid JSON output. | `ingestion/src/transport/json.rs:46` | P0 |
| ROB-10 | S1 | Ingestion `split_target` does not percent-decode (retrieval copy does) ⇒ replication acks for IDs containing `:` 404. | `ingestion/src/transport/request.rs:61` | P0 |
| ROB-11 | S2 | Up to 16 MiB body allocated before authentication. | `retrieval/src/transport.rs:61` | P1 |
| ROB-12 | S2 | Raw internal errors (disk, provider) leak into response bodies; `/ready` double-encodes JSON. | `retrieval/src/transport.rs:1176-1183` | P1 |

## DATA — Durability and data integrity

| ID | Sev | Issue | Evidence | Phase |
|---|---|---|---|---|
| DATA-01 | S0 | **Evidence duplicated** on every restart, retry, and replication re-apply (append without `evidence_id` dedup; redb bulk-load + full WAL replay). Reproduced: citations 2→3→4 across reloads; inflates ranking. Edges duplicate in retrieval resync. | `pkg/store/src/lib.rs:1365-1371, 1401-1404, 1446-1449, 261-265`; `wal.rs:457-458` | P0 |
| DATA-02 | S0 | WAL record fields are tab-separated without escaping; validation allows `\t`/`\n` in entities/embedding IDs ⇒ one accepted request makes the WAL **unreplayable** (service cannot start). | `pkg/store/src/wal.rs:618-889`; `pkg/schema/src/lib.rs:217-226` | P0 |
| DATA-03 | S0 | Vector upsert appended to WAL **before** validation; rejected request still poisons replay with `InvalidVector`. | `pkg/store/src/lib.rs:386-388` vs `1457-1470` | P0 |
| DATA-04 | S1 | Re-upserting a claim silently deletes its vector in memory (redb keeps it) ⇒ memory/disk divergence. | `pkg/store/src/lib.rs:1350, 1884-1887, 1990-1997` | P0 |
| DATA-05 | S1 | Query vectors never validated; finite-but-huge stored vectors overflow ⇒ NaN scores with sign-dependent ordering. | `pkg/store/src/lib.rs:433-439, 2105-2135, 652` | P0 |
| DATA-06 | S2 | Edge `reason_codes` and `created_at` not persisted in WAL; reset to defaults on restart. | `pkg/store/src/wal.rs:677-684, 837-838` | P0 |
| DATA-07 | S0 | No per-record checksum/length framing; torn final line aborts the whole replay; truncated numeric fields can parse to wrong values. | `pkg/store/src/wal.rs:508-513` | P0 (tolerate) / P2 (new format) |
| DATA-08 | S1 | Checkpoint: no directory fsync after rename; truncate not fsynced ⇒ possible empty-WAL + old-snapshot state after crash. | `pkg/store/src/wal.rs:554-578, 592` | P0 |
| DATA-09 | S1 | Staged batch clones share the `Arc` redb handle ⇒ redb written before WAL; failed/rolled-back batches leak into redb. | `pkg/store/src/lib.rs:91-118`; `ingestion/src/transport.rs:504-506, 887` | P0 |
| DATA-10 | S1 | Single ingest is not atomic (claim / evidence / vector appended separately, no commit marker); checkpoint failure after commit returns 500 and loses the vector; retry duplicates (DATA-01). | `pkg/store/src/lib.rs:344-350`; `ingestion/src/transport.rs:419` | P0 (commit marker) / P2 |
| DATA-11 | S2 | redb is write-only overhead: services always cold-start from WAL; HWM unused; multi-table writes not in one transaction. | `services/*/src/main.rs`; `pkg/store/src/disk.rs:107-135`; `lib.rs:1326-1328` | P2 |
| DATA-12 | S1 | Document/batch idempotency fingerprint covers IDs only ⇒ edited document with same sentence count is reported `idempotent_replay` and **new text dropped**; `sanitize_id_component` collisions across tenant/doc IDs and long IDs. | `pkg/store/src/lib.rs:68`; `ingestion/src/extraction.rs:261, 819` | P0 |
| DATA-13 | S2 | Extraction spans are offsets into trimmed text, not the original document (breaks citation-grade spans). | `ingestion/src/extraction.rs:216, 230` | P1 |
| DATA-14 | S1 | No delete/tombstone API for claims, evidence, edges or tenants (blocks GDPR erasure, corrections). | API surface | P2 |

## REP — Replication and distribution

| ID | Sev | Issue | Evidence | Phase |
|---|---|---|---|---|
| REP-01 | S0 | Replication offsets are line numbers in the current WAL file; after leader compaction they reset ⇒ followers **silently skip** records (no WAL generation/epoch). | `pkg/store/src/wal.rs:309-334, 592` | P0 (resync on generation change) / P3 |
| REP-02 | S0 | Retrieval follower sets `next_offset = snapshot_lines + wal_lines` after resync but leader counts WAL lines only ⇒ **infinite resync loop** that multiplies edges. | `retrieval/src/replication.rs:153`; `wal.rs:316-322` | P0 |
| REP-03 | S1 | Resync merges onto existing state instead of replacing it. | `retrieval/src/replication.rs` | P0 |
| REP-04 | S1 | Partial-batch apply failure retries forever; unbounded `read_to_end`; upstream `records=` fed to `Vec::with_capacity` (panic kills follower thread silently). | `retrieval/src/replication.rs:167-180, 213, 253` | P0 |
| REP-05 | S1 | Ingestion follower offset is in-memory only ⇒ full WAL re-apply after restart (combines with DATA-01). | `ingestion/src/transport.rs:240` | P0 |
| REP-06 | S1 | `apply_replication_export` builds a store without the disk handle ⇒ persistence silently stops after first resync. | `ingestion/src/transport.rs:925` | P0 |
| REP-07 | S1 | No real quorum: writes acked with `ack_count=1`; quorum mode only pre-checks replica health; single `/v1/ingest` writes no commit record so it is never acked. | `ingestion/src/transport.rs:419`; `ingest_routes.rs:110` | P3 |
| REP-08 | S0 | Split-brain: failed placement reload keeps last placement and keeps accepting writes; epoch regressions accepted; silent fallback to stale placement file; promotion ignores replica lag. | `ingestion/src/transport/placement_routing.rs:231-244`; `metadata-router/src/lib.rs:251, 290-302` | P3 |
| REP-09 | S1 | Consistent-hash ring built from all tenants' shards but lookup is per (tenant, shard) ⇒ `PlacementNotFound`. | `ingestion/src/transport/placement_routing.rs:273` | P3 |
| REP-10 | S3 | `dedup()` on unsorted replica list; `route_to_shard` concatenates tenant+entity without separator; ring rebuilt per claim. | `metadata-router/src/lib.rs:112, 126` | P3 |
| REP-11 | S2 | `ingest_to_visible_lag` measures `now − event_time`, not ingest time; failures push 0 ms latencies; `/ready` ignores replication lag / dead follower. | `retrieval/src/transport.rs:1349, 2063-2093` | P1 |
| REP-12 | S1 | Replicated data is durable only if redb attaches; with persistence disabled a restart resumes from saved offset with an empty store. | `retrieval/src/main.rs`; `replication.rs` | P0 |

## CP — Control plane and leader election

| ID | Sev | Issue | Evidence | Phase |
|---|---|---|---|---|
| CP-01 | S0 | Control plane reads requests with `read_to_end` ⇒ every normal HTTP client hangs (reproduced with curl). Metadata-router client has no timeouts ⇒ ingestion hangs at startup / while holding the runtime mutex. | `services/control-plane/src/lib.rs:352`; `metadata-router/src/lib.rs:317-330` | P0 |
| CP-02 | S0 | `LeaderLease::renew` holds `self.lock` then calls `try_acquire`, which re-locks the non-reentrant mutex ⇒ **deadlock** whenever a lease has lapsed. | `services/control-plane/src/leader.rs:86, 113-119` | P0 |
| CP-03 | S0 | Lease race (read-then-write, no file lock), wall-clock expiry, no fencing token, epoch not monotonic, no fsync. | `services/control-plane/src/leader.rs:87-103` | P3 (replace) |
| CP-04 | S1 | Followers never re-acquire (renew thread exits); a follower that acquires persists stale startup placements; followers serve stale `GET /placement`. | `services/control-plane/src/main.rs:121` | P3 |
| CP-05 | S2 | Thread-per-connection with no timeouts. | `services/control-plane/src/lib.rs` | P1 |
| CP-06 | S3 | Panics on poisoned locks / pre-epoch clock. | `leader.rs:86, 172` | P1 |

## IDX — Indexing, search and ranking

| ID | Sev | Issue | Evidence | Phase |
|---|---|---|---|---|
| IDX-01 | S1 | ANN is a home-grown string-keyed graph, not `usearch` (dependency unused). Insert scans every vector of every tenant per level ⇒ O(N²) cold start. **Fixed in P2 step 1** (flat/`usearch` HNSW vector index; evidence in `2026-10-09-p0-status.md`). | `pkg/store/src/ann.rs`; `lib.rs:1678-1695` | P2 |
| IDX-02 | S2 | ANN deletes leave graph unrepaired; pruning one-directional ⇒ recall decay. **Fixed in P2 step 1** (usearch soft delete with slot reuse; the graph code is gone). | `pkg/store/src/lib.rs:1639-1668, 1746-1792` | P2 |
| IDX-03 | S1 | Retrieval fallbacks: no token hits ⇒ score every claim in tenant; empty ANN result (e.g. wrong dim) ⇒ brute force over **all tenants'** vectors. | `pkg/store/src/lib.rs:843-853, 946-960, 1004-1015` | P0 (tenant scope) / P2 |
| IDX-04 | S2 | Tokenizer ASCII-only (no Unicode, no stemming, no multilingual); BM25 average length recomputed over tenant per query. | `pkg/schema/src/lib.rs` `tokenize`; `pkg/store/src/lib.rs:1179-1186` | P2 |
| IDX-05 | S1 | **Ranking can be gamed / is wrong**: edge direction inverted ("A supports B" counts for A); dangling edges count; unbounded linear support bonus (+0.08 per edge) from self-authored edges. | `pkg/store/src/lib.rs:551-567`; `pkg/graph/src/lib.rs:12-28`; `pkg/ranking/src/lib.rs:86` | P0 (direction/dangling/cap) / P5 |
| IDX-06 | S2 | Lexical score unbounded vs dense `(cos+1)/2`; "tie-breaker" can dominate; default `hash` embedding is not semantic. | `pkg/store/src/lib.rs:610-625`; `pkg/ranking` | P5 |
| IDX-07 | S1 | Segment publish race: filenames depend only on segment ID (overwritten in place), old files pruned with `Duration::ZERO`, one tenant's error aborts the pass, `.tmp` never cleaned. | `services/indexer/src/lib.rs:240, 257, 342` | P0 / P2 |
| IDX-08 | S2 | Segments fully rebuilt and fsynced on every ingest; HashMap order churns membership; in-process maintenance and daemon duplicate work. | `ingestion/src/transport/segment_runtime.rs:247` | P2 |
| IDX-09 | S3 | `gpu.rs` is a stub that always returns `None`. | `pkg/store/src/gpu.rs:9-18` | P2 (remove) / P6+ |

## PERF — Performance and resource bounds

| ID | Sev | Issue | Evidence | Phase |
|---|---|---|---|---|
| PERF-01 | S1 | All ingestion (WAL fsync, segment rebuild, placement HTTP fetch) serialized on one global `Mutex<IngestionRuntime>`. | `services/ingestion/src/transport.rs` | P2 |
| PERF-02 | S1 | `claims_for_tenant` scans and clones **all** claims across all tenants; called multiple times per retrieve and per ingest. | `pkg/store/src/lib.rs:656`; `retrieval/src/api.rs:262`; `transport.rs:2072` | P0 (index) / P2 |
| PERF-03 | S1 | Batch ingest and every replication pull deep-clone the whole store including ANN graphs. | `ingestion/src/transport.rs:504, 887` | P2 |
| PERF-04 | S1 | Retrieval holds the store read lock for the entire request, including remote embedding calls (≤30 s) and the response write ⇒ writer starvation ⇒ global stall. | `retrieval/src/transport.rs:1057-1065` | P0 |
| PERF-05 | S1 | Unbounded growth: in-memory `wal: Vec<WalEvent>` on the leader; `replication_commit_status` map. | `pkg/store/src/lib.rs:117, 1354`; ingestion transport | P0 |
| PERF-06 | S2 | fsync per record (no group commit); redb evidence/edge blobs read-modify-write (O(k²)); WAL file reopened per flush; whole WAL read per follower poll. | `pkg/store/src/wal.rs:263-273, 315, 413, 427` | P2 |
| PERF-07 | S2 | Per-request rebuild of auth policy, ~30 env reads, revocation file read, embedding provider construction. | `retrieval/src/transport.rs:1123` | P0 |
| PERF-08 | S2 | Audit write takes a global mutex and `create_dir_all` + `open` per request. | `services/*/src/transport/audit.rs:36-58` | P1 |
| PERF-09 | S3 | Segment cache refresh without single-flight (thundering herd under read lock). | `retrieval/src/api/segment_storage.rs:68-127` | P1 |
| PERF-10 | S2 | Extraction adapters write all stdin before reading stdout (pipe deadlock), no timeout, one process per sentence. | `ingestion/src/extraction.rs:437, 600` | P1 |

## EMB — Embedding providers and OpenAI compatibility

| ID | Sev | Issue | Evidence | Phase |
|---|---|---|---|---|
| EMB-01 | S0 | OpenAI provider cannot work (no TLS; see SEC-23). | `pkg/embeddings/src/lib.rs:199, 326-372` | P0 |
| EMB-02 | S1 | Env-selected Ollama default `http://localhost:11434` has no path ⇒ POSTs to `/`. Deploy uses `DASH_OLLAMA_BASE_URL`, code reads `DASH_OLLAMA_ENDPOINT`. | `pkg/embeddings/src/lib.rs:132, 611-615` | P0 |
| EMB-03 | S2 | Circuit breaker half-open admits all callers, not a single probe. | `pkg/embeddings/src/lib.rs:434-437, 504-517` | P1 |
| EMB-04 | S2 | `dimensions()` returns 0 for network providers ⇒ no dimension check per tenant/collection. | `pkg/embeddings/src/lib.rs:162, 245` | P2 |
| EMB-05 | S2 | HTTP client ignores chunked responses; unbounded `read_to_end`. | `pkg/embeddings/src/lib.rs:388-393` | P0 (replace client) |
| EMB-06 | S2 | `/v1/embeddings` rejects token-ID array input (LangChain default); transport path skips empty-text / count-mismatch validation; no input-count cap. | `retrieval/src/openai_embeddings.rs:44-47, 194-283` | P1 |

## SDK — Client libraries

| ID | Sev | Issue | Evidence | Phase |
|---|---|---|---|---|
| SDK-01 | S0 | Java SDK reads the response body twice ⇒ every successful call throws (`IllegalStateException: closed`); Kotlin inherits it. | `sdks/java/src/main/java/dev/dash/internal/HttpTransport.java:191-221` | P0 |
| SDK-02 | S0 | Java/Kotlin and C# ingest models match neither the server nor each other; they send ingest to the retrieval port. | `sdks/java/.../model/Ingest*.java`; `sdks/csharp/src/Dash/Models/IngestModels.cs:48-101` | P6 (P0: mark experimental) |
| SDK-03 | S1 | Java/Kotlin/C# expose `delete()` against `/v1/delete`, which does not exist. | `DashClient.java:108-139`; `DashClient.cs:35-39` | P0 (remove) / P6 |
| SDK-04 | S0 | C# retrieve expects `"hits"` (server sends `"results"`) and an object `score` (server sends number); fixtures encode the wrong contract. | `sdks/csharp/src/Dash/Models/RetrievalModels.cs:75, 124-130`; `tests/.../TestData.cs:43` | P0 |
| SDK-05 | S2 | No SDK sends `query_embedding`, `entity_filters`, `embedding_id_filters`, `time_range`, `read_consistency` or decodes `graph`, `claim_confidence`, `contradiction_risk`; `top_k` default 10 vs server 5. | `retrieval/src/transport/payload.rs:41, 122-165, 598-700` | P6 |
| SDK-06 | S1 | Java and C# retry non-idempotent POSTs (double ingest risk). | `HttpTransport.java:138-172`; `HttpTransport.cs:145-168` | P0 |
| SDK-07 | S2 | Python/Go/TS have no ingest API. | `sdks/{python,go,typescript}` | P6 |
| SDK-08 | S3 | Packaging: version split (0.1.0 vs 0.2.0); wrong repository URLs; Python import name `dash` collides with Plotly Dash; NuGet ID `Dash`; Kotlin depends on unpublished artifact. | `pyproject.toml:44`; `go.mod:1`; `pom.xml:14`; `build.gradle.kts:20` | P6 |
| SDK-09 | S1 | CI runs only Go/Python/TS SDK tests; no server-contract tests. | `.github/workflows/sdks.yml` | P1 |

## DEP — Deployment artifacts

| ID | Sev | Issue | Evidence | Phase |
|---|---|---|---|---|
| DEP-01 | S0 | k8s and Helm mount an `emptyDir` at `/opt/dash`, hiding `/opt/dash/bin/dash-entrypoint` ⇒ **pods cannot start**. | `deploy/k8s/20-retrieval.yaml:148`, `30-ingestion.yaml:146`; Helm `retrieval.yaml:131`, `ingestion.yaml:127`, `controlplane.yaml:129` | P0 |
| DEP-02 | S0 | Env names never read by code: `DASH_INGESTION_*`, `DASH_OLLAMA_BASE_URL`, `DASH_*_JWT_PUBLIC_KEY`, `DASH_*_TRANSPORT_RUNTIME`, `DASH_LOG_LEVEL`. | `deploy/k8s/10-config.yaml`, `11-secrets.yaml`; Helm `config.yaml` | P0 |
| DEP-03 | S1 | k8s `DASH_RETRIEVAL_PERSISTENCE_PATH=/var/lib/dash` is a directory ⇒ silent in-memory fallback. | `deploy/k8s/10-config.yaml:22`; `retrieval/src/main.rs:317-334` | P0 |
| DEP-04 | S0 | k8s/Helm: no replication source ⇒ ingested data never reaches retrieval; HPA scales StatefulSets 2→10 with independent data. | `deploy/k8s/70-hpa.yaml:21-22, 58-59` | P0 |
| DEP-05 | S0 | Ingress routes all `/v1` to retrieval (`/v1/ingest` 404); default-deny NetworkPolicy has no ingress rule for ingestion; egress excludes private CIDRs (blocks in-cluster Ollama). | `deploy/k8s/40-ingress.yaml:41-47`; `50-networkpolicy.yaml:77-80` | P0 |
| DEP-06 | S2 | Readiness probes use `/v1/health`; `/v1/ready` unused. | k8s/Helm manifests | P0 |
| DEP-07 | S2 | Control plane absent from raw k8s, systemd, and release images. | `deploy/k8s`, `deploy/systemd`, `release.yml` | P3/P7 |
| DEP-08 | S1 | Release pushes `ghcr.io/<owner>/dash-{service}`; manifests pull `…/dash/retrieval:0.2.0`; `SERVICE` build-arg ignored. | `.github/workflows/release.yml:104-115` | P0 |
| DEP-09 | S1 | systemd: both services share `/var/lib/dash/claims.wal`; retrieval redb path outside `ReadWritePaths` under `ProtectSystem=strict`. | `deploy/systemd/*.env.example`, `dash-retrieval.service:9-18` | P0 |
| DEP-10 | S2 | Quickstart and CI backup drill run compose without the `:?`-required secrets ⇒ fail; curl examples omit auth. | `docker-compose.yml:63-64, 96-97`; `rust.yml:75-76`; `README.md:49-87` | P0 |
| DEP-11 | S2 | No deploy enables audit logging (`DASH_*_AUDIT_LOG_PATH`). | all deploy targets | P1 |

## QA — Testing and CI

| ID | Sev | Issue | Evidence | Phase |
|---|---|---|---|---|
| QA-01 | S1 | `scripts/ci.sh` (bench regression guard) not run in CI; no `helm lint`, kubeconform, kustomize build, or Docker build on PRs. | `.github/workflows/*` | P1 |
| QA-02 | S1 | Retrieval replication has zero tests; the real socket HTTP parser is untested (tests use a separate parser); no tests for default-open auth, deep/non-ASCII JSON, header limits, crash recovery. | `services/retrieval/src/replication.rs`; `transport.rs:950` | P0/P1 |
| QA-03 | S2 | Tests mutate process env vars behind ad-hoc locks (flaky, order-dependent). | `services/*/src/transport/tests.rs` | P1 |
| QA-04 | S2 | No crash-consistency, deterministic-simulation, linearizability or recall/quality regression testing. | — | P1–P3 |
| QA-05 | S3 | Benchmark history has a single row; drill outputs (incl. `.tar.gz`) committed under `docs/`. | `docs/benchmarks/history/` | P1 |

## DOC — Documentation truthfulness

| ID | Sev | Issue | Evidence | Phase |
|---|---|---|---|---|
| DOC-01 | S2 | README: wrong clone URL (`anomalyco/dash`), quickstart without auth, stale test count (379), "Next" lists shipped features, says no LICENSE exists, incomplete architecture list. | `README.md:34, 49, 57-87, 125, 145-155` | P0 |
| DOC-02 | S1 | False capability claims: "HNSW via usearch", "redb default off", GPU backend, "byte-compatible" embeddings. | `README.md:27, 98, 132` | P0 |
| DOC-03 | S2 | ~35 of 58 documented `DASH_*` vars are read nowhere; `POST /v1/tenants` documented but absent. | `docs-site/docs/reference/configuration.md`; `concepts/multi-tenancy.md:62` | P0 (remove) / P1 (generate) |
| DOC-04 | S2 | `/v1/delete`, `/v1/sources`, `/v1/claims:upsert`, `/v1/admin/reindex` documented but not built; two divergent copies of the architecture doc. | `docs/operations/services.md:83`; `EME_ARCHITECTURE.md:282-296` | P0 |
| DOC-05 | S3 | `feasibility.md` (4,192 LOC, 36 tests), roadmap (354 tests), CHANGELOG missing M11. | `feasibility.md:29-33` | P0 |
| DOC-06 | S1 | Threat model and SOC 2 readiness claim controls that do not exist (HMAC audit, tenant isolation test suite, signed images, encryption at rest, working rate limits, key review metadata). | `docs/threat-model.md`; `docs/compliance/soc2-readiness.md:20, 48` | P0 (correct) / P4 |
| DOC-07 | S1 | **Process issue**: previous plans mark items "DONE"/"Fixed" that are broken (e.g. secret hygiene, production hardening). Status was not tied to verifying tests. | `docs/plans/2026-06-13-*.md`, `2026-08-09-production-readiness-remediation.md` | P0 (process) |

## MNT — Maintainability

| ID | Sev | Issue | Evidence | Phase |
|---|---|---|---|---|
| MNT-01 | S2 | Copy-pasted and already-diverged transport/authz/audit/HTTP/rate-limit code between ingestion and retrieval (~85% identical `authz.rs`); four hand-rolled HTTP clients/servers with different bugs; `services/common` holds 149 lines. | `services/*/src/transport/*` | P1 |
| MNT-02 | S2 | Very large files with duplicated entry points: `pkg/store/src/lib.rs` (~4k lines, many `retrieve_with_*`), `retrieval/src/transport.rs` (~3.6k lines), route blocks copied 4×. | — | P1/P2 |
| MNT-03 | S2 | Three JSON stacks (hand-rolled parser, `format!` renderers, serde). | `retrieval/src/transport/payload.rs`, `debug_render.rs` | P1 |
| MNT-04 | S3 | Dead dependencies (`axum`, `tokio`, `tower`, `usearch` unused), `unreachable!` branches from infallible `Result`, `#[allow(dead_code)]` APIs. | `services/*/Cargo.toml`; `ingestion/src/main.rs:243`; `retrieval/src/main.rs:333` | P1 |
| MNT-05 | S3 | `IngestionRuntime` is a ~50-field god object with two hand-written constructors. | `services/ingestion/src/transport.rs` | P1 |
| MNT-06 | S3 | Every env var has an `EME_*` fallback; `env_with_fallback` defined ~6 times; config scattered across modules. | services | P1 |

---

## Totals

| Severity | Count |
|---|---|
| S0 | 27 |
| S1 | 51 |
| S2 | 41 |
| S3 | 12 |
| **Total** | **131** |

(Counts are of register rows; several rows bundle closely related defects.)
