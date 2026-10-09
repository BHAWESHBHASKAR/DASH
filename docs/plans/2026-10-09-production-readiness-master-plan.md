# DASH 1.0 — From Prototype to Production Database

Date: 2026-10-09
Status: **Proposed** — supersedes the status claims in `2026-06-13-dash-modernization-roadmap.md`
and `2026-08-09-production-readiness-remediation.md` (see §9 on why).
Companion: [`2026-10-09-issue-register.md`](./2026-10-09-issue-register.md) — 131 tracked issues
(27 S0, 51 S1, 41 S2, 12 S3). Every work item below cites register IDs.

---

## 0. Executive summary

**Where DASH is.** DASH has a genuinely differentiated idea — the *claim* (an atomic,
source-bound assertion with evidence, stance, provenance spans and a validity window)
as the primary data primitive, so RAG answers can be cited, contradiction-aware and
auditable. The workspace is well factored into crates, builds clean, and 416 Rust tests pass.

But the deep review found that it is a **prototype that presents as production-ready**:

- **Security fails open.** No configured keys ⇒ everyone can read every tenant; a JWT-only
  configuration is bypassed by omitting the header; the control plane, replication
  endpoints and `/v1/embeddings` have no auth; the rate limiter is rebuilt per request
  and never limits; k8s/Helm ship known secrets and misnamed ones.
- **Data is not safe.** Evidence duplicates on every restart; one request containing a tab
  can make the WAL unreplayable; replication silently skips records after compaction and
  can loop forever; the leader-lease code deadlocks; the control plane hangs every HTTP client.
- **One request can kill the process** (unbounded JSON recursion), and non-ASCII text is corrupted.
- **Deployments cannot run** (k8s/Helm hide the binary behind an `emptyDir`; ingested data
  never reaches retrieval).
- **The engine does not scale** (O(N²) vector-index build, whole-store scans and deep clones
  per request, a global ingest mutex) and the README overstates what exists
  (`usearch` HNSW, encryption at rest, quorum replication).

**Where DASH should go.** A single, secure-by-default database binary (`dashd`) with:

1. a **real storage engine** — checksummed LSN-ordered WAL with group commit, memtable +
   immutable segments, proven index libraries (`usearch` HNSW with quantization, `tantivy`
   BM25), idempotent versioned upserts, tombstones and bitemporal history;
2. **real distribution** — Raft-replicated shards (quorum commit, fencing by term) and a
   Raft metadata group, replacing the file lease, pull-based replication and CSV placement;
3. **enterprise security that is actually wired** — deny-by-default authn/authz middleware,
   hashed managed API keys, hardened OIDC, mTLS, per-tenant envelope encryption with KMS
   and crypto-shredding, HMAC-chained audit with signed checkpoints;
4. **the evidence layer as the moat** — evidence-aware ranking v2 that cannot be gamed,
   explainable scores, NLI-based contradiction detection, claim canonicalization, and a
   `verify` endpoint that checks an LLM answer sentence-by-sentence against cited claims;
5. **an honest engineering process** — every claim in the docs backed by a test in CI.

**How long.** With 4–5 engineers: roughly **9–10 months to 1.0 GA** on the critical path
(P0 → P1 → P2 → P3 → P7), with security, retrieval-quality and DX tracks in parallel.
A 3-week P0 ("make it safe") ships first as v0.3 because people may already run DASH.

### Key decisions (recommended — confirm before P1, see §10.3)

| # | Decision | Recommendation | Why |
|---|---|---|---|
| D1 | Process model | **Single binary `dashd` with roles** (`all`, `query`, `ingest`, `meta`); single-node is first-class | Removes the two-service data-path problem, halves deploy surface, matches Qdrant/Weaviate ergonomics |
| D2 | Network stack | **tokio + hyper + axum + tower + rustls**; internal RPC via **tonic gRPC over mTLS** | Deletes four hand-rolled HTTP stacks and their ~25 bugs; deps already declared |
| D3 | Storage | **Own WAL + LSM-style segments**, using `usearch` (vectors), `tantivy` (text), `roaring` (bitmaps); `redb` only for manifests/metadata | Owning the log is the core of a database; indexes are solved problems |
| D4 | Tenancy | **Tenant = physical partition** (own memtable/segments/DEK), collections inside tenants | Hard isolation, per-tenant encryption + crypto-shredding, tenant offload, trivial erasure |
| D5 | Replication/consensus | **Embedded Raft (`openraft`)** per shard group + one metadata group | Real quorum + fencing; no external etcd to operate |
| D6 | Durability tier | **Object storage** (`object_store`: S3/GCS/Azure) for segment snapshots + WAL archive | Backups/PITR, replica bootstrap, cold-tenant tiering |
| D7 | API contract | **OpenAPI 3.1 generated from code** (`utoipa`), RFC 9457 errors, SDK cores generated, contract-tested | Ends SDK/server drift (SDK-01…09) |
| D8 | Config | **Typed config** (TOML file + `DASH__SECTION__KEY` env overlay), validated at startup, docs generated from code | Ends env-name drift (DEP-02, DOC-03) permanently |

---

## 1. What "production-ready" means for DASH (the 1.0 GA gate)

DASH 1.0 ships only when **every** item below is demonstrated by an automated check in CI
or a recorded drill — not by a status line in a plan.

### 1.1 Safety
- [ ] Deny-by-default: a fresh `dashd` with no credentials configured refuses to start in
      `production` profile and serves only `/live`; in `dev` profile it binds to localhost only.
- [ ] Every route has a declared permission; the authz matrix test covers 100% of routes × roles × tenants.
- [ ] No endpoint other than `/live` and `/ready` is reachable without authentication.
- [ ] External penetration test: no open High/Critical findings.
- [ ] Data at rest encrypted (WAL frames + segments) when encryption is enabled; a test scans
      the data directory for plaintext canaries.

### 1.2 Durability & correctness
- [ ] Acknowledged writes survive `kill -9` at any point: crash harness ≥ 10,000 randomized
      kill cycles, zero lost acked writes, zero duplicates.
- [ ] Replicated mode: deterministic-simulation and Jepsen-style tests (partitions, crashes,
      clock skew, disk stalls) show no lost quorum-acked writes and linearizable leader reads.
- [ ] Re-ingesting identical input is a no-op; re-ingesting changed input is an update — never a silent drop.
- [ ] On-disk format is versioned; N-1 → N upgrade and rollback tested every release.

### 1.3 Performance (single node: 16 vCPU / 64 GB / NVMe, 10M claims, 768-d, int8 HNSW)
- [ ] Hybrid query with tenant + time filter: p50 < 15 ms, p99 < 50 ms at 500 QPS, top_k = 10.
- [ ] Vector recall@10 ≥ 0.95 vs exact search.
- [ ] Sustained ingest ≥ 5,000 claims/s with group commit; durable-ack p99 < 20 ms.
- [ ] Cold start < 30 s (mmap segments + WAL tail), not O(N²) rebuild.
- [ ] Cluster: failover RTO < 10 s, RPO = 0 for quorum writes; replication lag p99 < 1 s.

*(These are targets to validate in the P2 benchmark spike; if a target proves wrong, it is
changed in this document with the measurement that justifies it.)*

### 1.4 Operability
- [ ] Helm chart installs on kind in CI and passes an ingest → query → failover → backup → restore e2e.
- [ ] Continuous backup to object storage with point-in-time restore; restore drill automated weekly.
- [ ] Every alert has a runbook; dashboards and alert rules ship with the chart and are `promtool`-tested.
- [ ] 7-day soak at target load with no memory growth, no error-budget burn.

### 1.5 Truthfulness
- [ ] Every capability claim in README/docs maps to a passing test in the claims ledger (§9.2).
- [ ] Configuration reference and OpenAPI spec are generated from code and diff-checked in CI.
- [ ] All six SDKs pass the same contract suite against a real `dashd`.

---

## 2. Engineering principles

1. **Secure by default, insecure by explicit, loud opt-in.** Never the reverse.
2. **The log is the source of truth.** Everything else (memtable, segments, indexes, redb
   manifests, replicas, backups) is derived from it and rebuildable from it.
3. **Every mutation is idempotent and versioned.** Retries, replays and replication re-applies
   must converge to the same state.
4. **No unbounded anything on a request path** — body size, header size, JSON depth, request
   time, result size, queue depth, memory per tenant, fan-out.
5. **Never hold a lock across I/O.** Readers work on immutable snapshots.
6. **Don't hand-roll solved problems** (HTTP, JSON, TLS, consensus primitives, ANN, BM25).
   Spend the innovation budget on the evidence layer.
7. **Evidence-first engineering.** DASH asks users to trust only claims with evidence; the
   project holds its own docs to the same bar (§9).
8. **Black-box tests outlive rewrites.** P0 regression tests are written against the HTTP API
   so they become the acceptance suite for the v2 engine and cluster.

---

## 3. Target architecture

### 3.1 Component view

```
                    ┌──────────────────────── clients ────────────────────────┐
                    │  SDKs (py/ts/go/java/kotlin/c#) · OpenAI-compat · MCP   │
                    └───────────────────────────┬─────────────────────────────┘
                                                │ HTTPS (rustls) / OIDC / API keys
┌───────────────────────────────────────────────▼──────────────────────────────────────────┐
│ dashd (single binary, roles: all | query | ingest | meta)                                 │
│                                                                                           │
│  dash-server   axum router · tower stack:                                                 │
│                request-id → trace → limits(body/header/depth/time) → authn → rate-limit   │
│                → authz(route permission) → handler → audit                                │
│                                                                                           │
│  dash-api      v1 handlers: collections · claims · evidence · edges · documents(jobs)     │
│                query(hybrid) · verify · embeddings(OpenAI) · admin(keys/tenants/backup)   │
│                                                                                           │
│  dash-query    planner: filters → candidate gen (ANN ∪ BM25 ∪ entity) → fusion (RRF)      │
│                → evidence scoring v2 → optional rerank → explain                           │
│                                                                                           │
│  dash-evidence contradiction (NLI) · canonicalization · supersession · source reputation  │
│                                                                                           │
│  dash-engine   per tenant partition:                                                      │
│                  WAL v2 ──► memtable ──flush──► immutable segments ──compact──► segments  │
│                  manifest (redb) · snapshots via arc-swap (lock-free readers)             │
│                  segment = rows(postcard blocks+crc) · usearch HNSW(i8) · tantivy ·       │
│                            roaring filters · live-docs bitmap · encrypted blocks          │
│                                                                                           │
│  dash-cluster  openraft: meta group (placement, schemas, keys, tenants)                   │
│                         shard groups (Raft log == WAL v2; commit on majority fsync)        │
│                gRPC/mTLS internal transport · request forwarding to shard leaders         │
│                                                                                           │
│  dash-objstore segment snapshots · WAL archive · PITR · cold-tenant offload               │
│  dash-crypto   envelope encryption: per-tenant DEK, KEK in KMS/Vault, rotation, shredding │
│  dash-auth     keys(hashed) · OIDC/JWKS · HS256(legacy) · mTLS principals · RBAC          │
│  dash-audit    HMAC chain · signed checkpoints · async durable writer · verifier          │
│  dash-observe  OpenTelemetry traces · Prometheus metrics · JSON logs · usage metering     │
│  dash-config   typed config, validation, generated reference                              │
└───────────────────────────────────────────────────────────────────────────────────────────┘
```

### 3.2 Crate layout (migration from today)

| Target crate | Built from | Notes |
|---|---|---|
| `dash-core` | `pkg/schema` | Typed IDs (`TenantId`, `ClaimId`, …), validation, versions, errors |
| `dash-wal` | `pkg/store/src/wal.rs` | Rewritten: binary frames, LSN, segments, group commit |
| `dash-engine` | `pkg/store/src/lib.rs`, `disk.rs` | Memtable/segments/manifest; `Engine` trait |
| `dash-index` | `pkg/store/src/ann.rs` (retired) | `usearch` + `tantivy` + `roaring` wrappers |
| `dash-query` | `pkg/ranking`, `pkg/graph`, `services/retrieval/src/api.rs` | Planner, fusion, scoring v2, explain |
| `dash-evidence` | `services/ingestion/src/extraction.rs` (parts) | New: NLI, canonicalization, verify |
| `dash-embed` | `pkg/embeddings` | `reqwest`+rustls, async, batching, cache |
| `dash-auth` | `pkg/auth`, both `transport/authz.rs` | One implementation, tower layers |
| `dash-audit` | both `transport/audit.rs`, `scripts/verify_audit_chain.sh` | One implementation + Rust verifier |
| `dash-crypto` | `pkg/encryption` | Wired into WAL + segments |
| `dash-cluster` | `services/control-plane`, `services/metadata-router`, replication modules | Replaced by Raft |
| `dash-objstore` | `scripts/backup_*`, S3 bits | Native backups/PITR |
| `dash-server`, `dash-api` | both `transport.rs` trees | axum; declarative route table |
| `dash-config`, `dash-observe` | `services/common` | Typed config, OTel |
| `dashd`, `dash` (CLI) | `services/*/src/main.rs`, `tools/buddy-chat` | Binary + admin/data CLI |

`services/indexer` is absorbed into engine compaction. `tests/benchmarks` stays as `dash-bench`.

### 3.3 Write path (single node; replicated mode in §4 ADR-05)

1. axum handler deserializes with `serde_json` (bounded body, depth-limited), validates against
   the collection schema, resolves tenant **from the authenticated principal**.
2. Embedding (if requested) runs **after** authz, outside any engine lock, with provider batching.
3. The whole request becomes **one transaction frame** (bundle or batch) → the tenant partition's
   single writer task. Atomicity is by construction: one frame, one CRC.
4. Group commit: the WAL writer coalesces frames, `fdatasync`s once, assigns LSNs.
5. The frame is applied to the memtable (idempotent upsert keyed by entity ID, versioned by LSN);
   a new read snapshot is published via `arc-swap`.
6. Response returns `commit_lsn` (a **consistency token**) so clients can request
   read-your-writes on any replica (`min_lsn`).
7. Background: memtable flush → segment; tiered compaction purges tombstones and versions
   beyond the history retention window; segments + WAL archived to object storage.

### 3.4 Read path

1. Authn/authz/rate-limit; tenant from principal; collection schema lookup.
2. Grab the current snapshot (`Arc`), **no lock held** thereafter.
3. Planner: pre-filters (tenant partition is physical; time range, entity, metadata via roaring
   bitmaps) → candidate generation in parallel per segment: `usearch` filtered ANN
   (quantized, then exact rerank on full-precision vectors) ∪ `tantivy` BM25 ∪ entity index.
4. Fusion (RRF default; calibrated weighted fusion optional) → evidence scoring v2 (§4 ADR-08)
   → optional cross-encoder rerank → stance/time filters → top-k.
5. Response: claims, calibrated score + **explain breakdown**, citations with spans,
   contradiction set, optional graph, `snapshot_lsn`.

### 3.5 Data model v2

- **Namespace:** `tenant / collection / entity`. No global ID space (fixes SEC-18).
- **Collection schema:** named vectors (`{name, dims, metric, quantization, embedding_model}`),
  text analyzers per language, metadata field types for filters, history retention, stance policy.
- **Claim:** existing fields + `version` (LSN), `tx_time`, `deleted` (tombstone),
  `canonical_id` (if merged), `supersedes[]`.
- **Evidence:** keyed `(claim_id, evidence_id)` — upsert, never append (fixes DATA-01);
  spans always reference the **original** document bytes plus a document content hash.
- **Edge:** keyed `edge_id = hash(from, to, relation)` unless provided; both endpoints must
  exist in the same tenant; `reason_codes`, `created_at` persisted (DATA-06).
- **Document:** first-class: `doc_id`, content hash, source URI, extraction job ID, claims derived.
- **Bitemporal:** valid time (`valid_from/valid_to`, already present) × transaction time (LSN /
  commit timestamp) ⇒ `as_of` queries: "what did DASH believe on 2026-03-01, and why?" — a
  natural audit feature for legal/finance/medical users.

---

## 4. Architecture decision records (summaries)

Each becomes a full ADR in `docs/adr/` at the start of the phase that implements it.

**ADR-01 Single binary with roles (D1).** Separate ingestion/retrieval services forced a
replication protocol just to make writes readable, and deploy artifacts drifted (DEP-01…09).
`dashd --role=all` is the default; `query`/`ingest` roles exist for scale-out but share code,
config and auth. *Rejected:* keeping two services — doubles the attack surface and the
duplicated code that has already diverged (MNT-01, ROB-10).

**ADR-02 tokio/hyper/axum/tower/rustls (D2).** Fixes ROB-01…12 and CP-01/05 by deletion.
Limits (body, headers, request deadline, concurrency) are tower layers configured once.
Internal traffic (Raft, forwarding, replication) uses `tonic` gRPC with mTLS.
*Rejected:* hardening the hand-rolled server — four copies with distinct bugs prove the cost.

**ADR-03 WAL v2 format.** Segment files `wal/<first_lsn>.log`. Frame:
`magic u32 | format u16 | flags u16 | len u32 | crc32c u32 | lsn u64 | tx_time i64 | kind u8 | payload (postcard)`.
Encryption (when enabled) applies to `payload` with an AEAD whose associated data is the header.
Recovery: scan from manifest checkpoint LSN; a bad CRC **at the tail** is truncated with a
metric + log (torn write); a bad CRC **mid-log** halts with an actionable error and
`dash wal inspect|repair`. LSNs are never reused or reset (fixes REP-01). WAL segments are
deleted only when (a) flushed into durable segments, (b) archived if archiving is on, and
(c) all replicas have passed them. *Rejected:* keeping the text WAL — no checksums, escaping
bugs (DATA-02/07), offsets that reset.

**ADR-04 LSM-style engine with proven indexes (D3).**
Memtable (exact vector search up to a threshold, then a small HNSW; in-RAM tantivy index) →
immutable segments: rows (postcard blocks with per-block CRC), `usearch` HNSW (f16/i8/binary
quantization, filtered search via predicate), `tantivy` segment, roaring bitmaps for filters
and live docs. Manifest in `redb` (atomic segment-set swaps). Segments are mmap'd → fast cold start.
A thin `Engine` trait lets P2 run the v1 store and v2 engine side by side (shadow reads).
*Alternatives evaluated in the P2 spike:* Lance (columnar + vector + object storage) as the
segment format. Recommendation stands unless the spike shows Lance meets latency targets
with less code; decision recorded either way.
*Status (P2 step 1, 2026-10-09):* the vector half is implemented in `pkg/store/src/vector_index.rs` (flat memtable-style index that converts to `usearch` HNSW with `i8` quantization, exact rerank and predicate filtering; `ann.rs` is retired); see ADR 0003 section 10 for the measured outcome. Segments, `tantivy`, the `Engine` trait and the persisted/mmap'd index are still open.

**ADR-05 Raft for replication and metadata (D5).** `openraft` shard groups: the Raft log *is*
the WAL v2 (Raft index = LSN), so commit = majority fsync = true quorum (fixes REP-07).
Fencing is the Raft term (fixes CP-03, REP-08). Metadata group stores placement, schemas,
tenants, keys. Membership changes via learners: a new replica bootstraps from the latest
segment snapshot in object storage, tails the log, then is promoted (joint consensus).
Read consistency levels: `linearizable` (ReadIndex on leader), `bounded_staleness(ms)`,
`session` (`min_lsn` token), `eventual`. *Rejected:* external etcd (another stateful system for
users to run) and async pull replication (cannot provide RPO = 0, already buggy).

**ADR-06 Tenant = physical partition (D4).** Each tenant has its own memtable, segments,
WAL stream within a shard group, and DEK. Small tenants are cheap (memtable + ≤1 segment,
lazily loaded); idle tenants can be **offloaded** to object storage and rehydrated on access
(Weaviate-style ACTIVE/INACTIVE/OFFLOADED). Very large tenants span multiple shards by hash.
*Rejected:* shared segments with tenant bitmaps — weaker isolation, no per-tenant crypto-shredding.

**ADR-07 Security model.** One `dash-auth` implementation as tower layers:
- *Authn:* managed API keys (`dash_<keyid>_<secret>`, stored as HMAC-SHA-256 with a server
  pepper, shown once, with tenant scope, roles, expiry, `last_used_at`); OIDC with required
  `iss` + `aud`, JWK-pinned algorithms, https-only JWKS, single-flight refresh,
  stale-while-revalidate, rate-limited refresh on unknown `kid`; HS256 kept as legacy with
  required `exp` and max lifetime; mTLS principals for internal traffic.
- *Authz:* permissions (`claims:read|write|delete`, `embeddings:create`, `collections:admin`,
  `keys:admin`, `cluster:admin`, `audit:read`, `debug:read`); roles are permission bundles
  (`reader`, `writer`, `admin`, `auditor`, `operator`); missing role claim ⇒ **no** roles
  (configurable default role, never "all"); tenant always from the principal — a body
  `tenant_id` must match or the request is rejected.
- *Rate limiting:* `governor` (GCRA) per principal and per tenant, process-lifetime state,
  `429` with `Retry-After`; cost-weighted for embeddings.
- *Audit:* see ADR-09.
*Fixes:* SEC-01…15, SEC-18.

**ADR-08 Evidence scoring v2.** Today's linear bonus per supporting edge is gameable and
the edge direction is inverted (IDX-05). v2:
- Relevance `R ∈ [0,1]` from calibrated fusion.
- Support `S = 1 − exp(−Σ_sources q_s / κ)` — summed over **distinct independent sources**
  (dedup by `source_id`, optionally by source domain), weighted by source quality `q_s`;
  saturating, so 50 copies ≠ 50× evidence.
- Contradiction `C` likewise; only incoming edges from existing same-tenant claims count,
  weighted by the *source claim's* own support (one-hop credibility), capped.
- Evidence factor `E = σ(a·S − b·C + c·confidence + d·freshness(valid window))`.
- Final `score = R^α · E^β` (weights per collection, defaults learned on the eval set).
- Every response can return `explain` with each term. Anti-gaming property tests:
  adding duplicate evidence from one source cannot raise rank beyond the cap; self-authored
  edge spam is bounded.

**ADR-09 Audit v2.** Structured event: `seq, ts, actor{type, id, key_id}, tenant, action,
resource, outcome, reason, request_id, client_ip, request_hash, prev_mac`. Chain uses
**HMAC-SHA-256** with a KMS-held key; canonical encoding is defined (sorted-key JSON or
postcard) and shared by writer and the Rust verifier (`dash audit verify`) — no more
script drift (SEC-17). Every N events or T seconds the writer emits an **Ed25519-signed
checkpoint** shipped to object storage with object lock (truncation/rewrite detection).
Writer is an async bounded channel with fsync batching; `audit.mode = fail_closed` rejects
writes when the audit log cannot persist (default for `production` profile).

**ADR-10 Envelope encryption (D4, SEC-16).** Per-tenant DEK (AES-256-GCM); per-file subkeys
derived with HKDF(DEK, file_id) so random-nonce usage per key stays far below GCM limits;
DEKs wrapped by a KEK in AWS KMS / GCP KMS / Azure Key Vault / Vault Transit (local file
provider for dev only). KEK rotation rewraps DEKs; DEK rotation applies to new writes and is
completed by compaction. **Deleting a tenant's DEK is crypto-shredding** — fast, verifiable
GDPR erasure even for backups. Keys zeroized in memory (`zeroize`).

**ADR-11 API contract (D7).** OpenAPI 3.1 generated from handler types (`utoipa`), committed
and diff-checked; RFC 9457 problem+json errors with stable `code`s; `Idempotency-Key` on all
writes (24 h dedupe window); cursor pagination; optimistic concurrency (`if_version`);
consistency tokens. Current `/v1/ingest`, `/v1/ingest/batch`, `/v1/retrieve` remain as
compatibility shims onto the `default` collection until 2.0.

**ADR-12 Typed configuration (D8).** `figment`-style layering: defaults → `dash.toml` →
`DASH__SECTION__KEY` env. Unknown keys are errors. Profiles `dev` / `production`.
`dashd config validate|print --redacted`. The configuration reference in docs is generated
from the config structs; CI fails if a deploy manifest sets an env var that does not exist
(prevents DEP-02 from ever recurring). `EME_*` fallbacks are dropped after one deprecation release.

---

## 5. Roadmap

```
Weeks:     0    3         8                  18                 28        34   38
P0 Safe    ███
P1 Found.     █████
P2 Engine          ██████████
P3 Cluster                    ██████████
P4 Security         ██████ (keys/RBAC/OIDC)      ████ (CMEK on v2 format)
P5 Quality                    ████████
P6 API/DX                          ██████
P7 Ops/GA                                      ██████   ████ (GA hardening)
Releases:  v0.3 ▲   v0.4 ▲          v0.6 ▲            v0.8β ▲   v0.9rc ▲  v1.0 ▲
```

### P0 — Make it safe (weeks 0–3) → **v0.3.0**

Goal: nobody running DASH today is exposed to an S0. Work happens in the current
architecture, but **every fix ships with a black-box HTTP-level regression test** that
becomes part of the v2 acceptance suite.

| Work package | Scope | Issues |
|---|---|---|
| **P0.1 Auth deny-by-default** | Build `AuthPolicy` + rate limiter once at startup (`Arc`), reload on SIGHUP. Close the JWT-only fall-through: when any auth method is configured, a request without valid credentials is 401. When none is configured, refuse to start unless `DASH_INSECURE_DEV_MODE=1`, which also forces a localhost bind. Require auth on `/v1/embeddings`, `/debug/*`, control-plane mutations, and replication (token mandatory; `subtle` constant-time compare). Move embedding after authz. Turn strict secrets on by default; treat `<…>` and `change-me` as placeholders; never log secret values. Return 429 with `Retry-After`. | SEC-01…10, SEC-05, SEC-24, PERF-07 |
| **P0.2 Crash & parser fixes** | Replace the hand-rolled JSON parser with `serde_json` (depth limit, correct UTF-8, surrogates). Cap headers at 8 KiB per line and 100 headers. Enforce a whole-request deadline (10 s default). On accept errors, back off and continue instead of exiting. Fix ingestion `json_escape` and percent-decoding. Return correct 413/429/502 status codes. | ROB-01…07, ROB-09, ROB-10, MNT-03 |
| **P0.3 Data-integrity hotfixes** | Upsert evidence by `evidence_id` and edges by `(from,to,relation)`. Validate everything before the WAL append. Reject control chars in WAL-bound string fields. Keep the vector on claim re-upsert. Validate query vectors and accumulate dot products in f64. Persist edge `reason_codes`/`created_at` (versioned `G2` record). Truncate a torn tail with a warning. fsync the directory after rename. Stage batches without the disk handle and write to redb only after the WAL commit. Add a commit marker for single ingests. Fingerprint documents by content hash. Make IDs collision-free with hash suffixes. | DATA-01…10, DATA-12 |
| **P0.4 Replication hotfixes** | Add a WAL generation ID to replication frames; a mismatch forces a resync. Fix `next_offset` after resync. Make resync *replace* state rather than merge. Bound reads and `records=`. Persist the ingestion follower offset. Keep the disk handle across resync. Report follower lag and death in `/ready`. | REP-01…06, REP-11, REP-12 |
| **P0.5 Control-plane hotfixes** | Read requests by `Content-Length` with timeouts. Add connect/read timeouts to the router client. Fix the `renew` → `try_acquire` re-lock deadlock. Add `flock` around the lease file. Require a bearer token. | CP-01, CP-02, SEC-07 |
| **P0.6 Query-path hotfixes** | Scope every fallback scan to the tenant. Add a per-tenant claim-ID index so `claims_for_tenant` is O(tenant). Correct edge direction and ignore dangling edges. Cap the support contribution. Release the read lock before embedding and before writing the response. Bound the in-memory WAL event buffer. Remove tenant names from conflict errors. Make segment directories collision-free. Give segment files unique names per publish, with delayed pruning. | IDX-03, IDX-05, IDX-07, PERF-02, PERF-04, PERF-05, SEC-18 (leak), SEC-19 |
| **P0.7 Embedding providers** | Replace the raw-TCP client with `ureq` + rustls (already a dependency through OIDC) to get TLS, chunked responses and bounded bodies. Use the correct Ollama path and env name. | EMB-01, EMB-02, EMB-05, SEC-23 |
| **P0.8 Deploy fixes** | Remove the `/opt/dash` emptyDir. Rename env vars and add a CI check against the code. Use a redb *file* path. Add the replication source URL and ingestion ingress paths. Fix NetworkPolicy ingress/egress. Point readiness probes at `/v1/ready`. Align image names. Separate the systemd WAL paths and add them to `ReadWritePaths`. Trim compose capabilities and bind to `127.0.0.1` by default. Make the quickstart and CI drill generate secrets first. Replace HPA on StatefulSets with documented manual scaling until P3. | DEP-01…06, DEP-08…10, SEC-04, SEC-20 (partial) |
| **P0.9 SDK hotfixes** | Fix the Java double body read. Fix C# `results`/`score`. Remove `delete()`. Stop retrying non-idempotent POSTs. Mark Java/Kotlin/C# ingest as experimental until P6. | SDK-01, SDK-03, SDK-04, SDK-06, SDK-02 (flag) |
| **P0.10 Truth pass** | Correct README (clone URL, quickstart auth, counts, roadmap, license, `usearch`/GPU/encryption claims), docs-site config reference, threat model, and SOC 2 doc. Add "superseded/inaccurate" banners to earlier plans. Adopt the Definition of Done (§9). | DOC-01…07 |

**Exit criteria:** every S0 in the register has a merged fix and a CI regression test;
new suites exist for the authz decision matrix, deep/non-ASCII JSON, restart-without-duplicates,
tab-in-entity replay, a kill -9 smoke loop (1,000 cycles), replication across compaction,
and control-plane requests via curl; Helm chart installs on kind and passes ingest → retrieve.

### P1 — Foundation (weeks 3–8) → **v0.4.0**

Goal: one secure, observable, configurable server shell; the old transports deleted.

- **`dashd` + `dash-server`:** axum on hyper/tokio; listeners `public` (API), `admin` (metrics,
  debug, admin API; default localhost/pod-network), `internal` (gRPC, mTLS). Graceful
  shutdown with drain. Ingestion/retrieval become role modules; old ports kept for compatibility.
  (ROB-*, CP-05, MNT-01, MNT-02, MNT-04, MNT-05)
- **`dash-config`:** typed config, profiles, validation, generated reference, deploy-manifest
  linter, `EME_*` deprecation warnings. (MNT-06, DOC-03, DEP-02)
- **`dash-auth` v2 (first half of ADR-07):** tower layers, declarative per-route permissions,
  role hierarchy, OIDC hardening (required aud/iss, JWKS SWR + single-flight + kid refresh,
  https-only), HS256 max lifetime + `jti` denylist, rate limiting with `governor`.
  (SEC-11…15)
- **`dash-audit` v1.5:** single implementation, defined canonical encoding, actor fields,
  async writer, torn-line tolerant, Rust verifier replacing the shell script; on by default
  in `production` profile. (SEC-17 fix, DEP-11, PERF-08)
- **`dash-observe`:** OpenTelemetry traces (OTLP), request IDs, Prometheus histograms per
  route, correct ingest-to-visible lag, JSON logs, per-tenant usage counters. (REP-11)
- **`dash-embed`:** async `reqwest` + rustls providers, request batching, timeouts, retries
  with jittered backoff, single-probe half-open breaker, provider-reported dimensions,
  content-hash embedding cache; OpenAI compatibility completed (token-ID arrays, input caps,
  validation). Extraction adapters get timeouts and streaming I/O. (EMB-03, EMB-04, EMB-06, PERF-10)
- **Test infrastructure:** config injection (no env mutation in tests), `testcontainers`
  harness for the real binary, authz matrix generator, `schemathesis` property-based API
  testing from OpenAPI, fixed fuzz targets (valid-signature JWT harness, payload, WAL frames),
  CI for all six SDKs, `helm lint` + kubeconform + Docker build on PRs, bench guard on a
  dedicated runner. (QA-01…03, SEC-22, SDK-09)

**Exit:** all P0 black-box tests pass against `dashd`; zero hand-rolled HTTP/JSON code remains;
config docs generated; middleware adds < 1 ms p99.

### P2 — Storage engine v2 (weeks 8–18) → **v0.6.0**

Goal: a real, durable, fast single-node database.

1. **Spike (week 8–9):** benchmark `usearch` (i8/f16) and `tantivy` against the §1.3 targets
   on 1M/10M datasets; evaluate Lance as the segment format; fix final targets; write ADR-03/04.
2. **WAL v2** with group commit, LSNs, segment files, torn-tail handling, `dash wal inspect|repair`.
   (DATA-07, DATA-08, PERF-06, REP-01)
3. **Engine:** per-tenant partitions; single writer task per partition; memtable; flush;
   immutable segments; manifest in redb; lock-free snapshots (`arc-swap`); tiered compaction;
   tombstones; versions; history retention. Global ingest mutex, store deep clones and
   whole-store scans are gone. (DATA-04, DATA-09…11, PERF-01…03, PERF-05, IDX-08)
4. **Indexes:** `usearch` HNSW per segment with quantization + exact rerank and filtered
   search; `tantivy` with Unicode/language analyzers; roaring filter bitmaps; entity index.
   Retire `ann.rs` and the GPU stub. (IDX-01, IDX-02, IDX-04, IDX-09)
5. **Data model v2:** collections with schemas and named vectors (dimension enforced per
   vector), tenant-scoped IDs, documents as first-class, delete/tombstone APIs for claims,
   evidence, edges, documents, and whole-tenant deletion; `as_of` bitemporal queries.
   (DATA-14, SEC-18, EMB-04)
6. **Migration:** `dash migrate --from v0` reads the text WAL + snapshot, de-duplicates
   evidence/edges, writes v2; verification report with per-tenant checksums. Shadow-read mode
   runs v1 and v2 side by side and diffs top-k results.
7. **Crash-consistency harness:** failpoints (`fail` crate) at every write/fsync/rename
   boundary plus a randomized `kill -9` loop with an oracle of acknowledged writes;
   `loom` tests for the snapshot structures; property tests for codecs.

**Exit:** 10,000-cycle crash test clean; recall@10 ≥ 0.95; §1.3 single-node targets met (or
re-baselined with evidence); v0 → v2 migration verified on a production-shaped dataset.

### P3 — Distribution & HA (weeks 18–28) → **v0.8.0-beta**

Goal: replicated, fault-tolerant cluster with honest consistency semantics.

- `openraft` meta group (placement, tenants, collection schemas, key registry, node membership);
  shard groups whose Raft log is WAL v2; commit on majority fsync. (REP-07, CP-03, CP-04)
- Internal gRPC/mTLS transport; any node accepts requests and forwards to the shard leader;
  routing table versioned by meta-group term (REP-08…10).
- Read consistency levels + session tokens (`min_lsn`); follower reads with bounded staleness.
- Rebalancing: learner bootstrap from object-storage snapshot, catch-up, promote, retire;
  tenant moves; shard splits for very large tenants.
- `dash-objstore`: continuous WAL archiving, periodic segment snapshots, PITR to LSN/time,
  cold-tenant offload/rehydrate.
- Delete the file lease, `services/control-plane`, `services/metadata-router`, CSV placement,
  pull replication. (CP-*, REP-*, DEP-07)
- **Deterministic simulation testing** with `turmoil` (partitions, message loss/reorder,
  crashes, clock skew, slow disks) over thousands of seeds per CI run; histories checked by a
  Porcupine/Knossos-style linearizability checker; nightly long runs.

**Exit:** DST + chaos show zero lost quorum-acked writes and linearizable leader reads; failover
RTO < 10 s; rolling restart under load with zero client-visible errors (with SDK retries);
PITR drill restores to an exact LSN.

### P4 — Enterprise security & compliance (weeks 8–14 and 26–30, parallel)

- Managed API keys (admin API + CLI: create/list/rotate/revoke, last-used, expiry, dormant-key
  report), RBAC v2 completion, mTLS client principals, optional IP allowlists. (SEC-11, SEC-15)
- Envelope encryption over WAL v2 frames and segment blocks (ADR-10) with KMS providers,
  KEK/DEK rotation, crypto-shredding on tenant delete, at-rest plaintext canary test. (SEC-16)
- Audit v2: HMAC chain, signed checkpoints to object-locked storage, export API, retention.
  (SEC-17)
- Supply chain: SHA-pinned actions, `cargo-deny` (advisories, licenses, bans, sources),
  CycloneDX SBOM, cosign keyless signing, SLSA provenance, digest-pinned distroless/chainguard
  base images, read-only root FS, no added capabilities. (SEC-20, SEC-21)
- Threat model rewritten for the v2 architecture (STRIDE per trust boundary, incl. Raft and
  object storage); SOC 2 control matrix lists only implemented, tested controls, each with an
  evidence-collection script; external penetration test before RC. (DOC-06)

**Exit:** authz matrix 100% route coverage; pentest clean of High/Critical; signed, attested
releases; compliance docs reviewed against code.

### P5 — Retrieval quality & evidence intelligence (weeks 18–26, parallel)

The moat. Make "citation-grade" measurable and better than generic vector databases.

- **Evaluation harness (first):** ANN recall vs exact; BEIR subset nDCG@10 for relevance;
  a new **DASH-Cite-Bench** gold set measuring citation precision/recall, span accuracy,
  contradiction detection and temporal correctness; tracked per commit, gated nightly.
- **Ranking v2 (ADR-08)** with `explain`, hybrid fusion (RRF + calibrated weighted), optional
  cross-encoder reranker (local ONNX via `ort` or remote), per-collection weights. (IDX-05, IDX-06)
- **Ingest intelligence:** LLM-pluggable claim extraction (any provider; local models
  supported) with spans on the original bytes (DATA-13); entity resolution; near-duplicate
  claim canonicalization (merge evidence into a canonical claim, keep lineage); NLI-based
  contradiction detection that proposes `contradicts` edges with confidence; temporal
  supersession edges; async document jobs with progress and retries.
- **`POST /v1/verify`:** given an answer (from any LLM), split into sentences, retrieve
  supporting/contradicting claims, return per-sentence support verdicts with citations —
  a grounding/hallucination check that makes DASH useful *after* generation, not just before.
- **Source reputation:** per-tenant source quality learned from contradiction outcomes and
  operator feedback, bounded and explainable.

**Exit:** quality metrics meet thresholds set after the first baseline (e.g. no regression
> 1 pt nDCG; citation precision target agreed with design partners); anti-gaming property tests pass.

### P6 — API v1 final & developer experience (weeks 24–30, parallel)

- API v1 final per ADR-11: collections, documents/jobs, query, verify, admin, embeddings;
  compatibility shims for current endpoints; OpenAPI committed and diff-checked. (SDK-05)
- SDKs: generated low-level clients + hand-written ergonomic layers for Python, TypeScript,
  Go, Java, Kotlin (coroutines over Java), C#; ingest everywhere; idempotency keys and safe
  retries; consistency tokens; one shared contract suite run against a real `dashd` in CI;
  unified versioning tied to API version; non-colliding package names (e.g. Python import
  `dashdb`); correct repository URLs; automated publishing. (SDK-02, SDK-05, SDK-07, SDK-08)
- `dash` CLI: data (ingest/query/verify), admin (keys, tenants, collections), ops (backup,
  restore, wal, audit verify, migrate, bench).
- **Agent-native:** `dash mcp` — a Model Context Protocol server exposing `search_claims`,
  `get_citations`, `verify_answer`, `add_claim` so agents can use DASH as verifiable memory.
- Framework integrations: LangChain and LlamaIndex retrievers that return citations and
  contradiction flags; OpenAI-compatible embeddings remain.
- Docs site rebuilt: generated API + config reference, concepts, tutorials (citations,
  contradictions, time travel, verify), migration guide v0 → v1.

### P7 — Operations & GA (weeks 28–38) → **v0.9.0-rc → v1.0.0**

- Helm chart v2 for `dashd` (StatefulSet, PVC, PDB, anti-affinity, topology spread, headless
  service for Raft, ServiceMonitor, secrets from external secret stores); kind-based e2e in CI
  (install → ingest → query → kill leader → backup → restore → upgrade N-1 → N).
- Compose (single node, secure defaults) and systemd (single node) kept, generated from the
  same config schema.
- Upgrade/rollback compatibility tests for on-disk and wire formats every release.
- SLOs, Grafana dashboards, `promtool`-tested alert rules, a runbook per alert, capacity
  planning guide.
- Nightly chaos in staging (pod kill, disk full, network partition, clock skew, KMS outage,
  IdP outage); 7-day soak; quarterly DR drill.
- Release engineering: semantic versioning, changelog generated from conventional commits,
  signed artifacts, LTS policy for 1.x.
- **GA gate (§1) review** — every box ticked with a link to the CI job or drill record.

---

## 6. Cross-cutting strategies

### 6.1 Testing pyramid

| Layer | Tooling | Guards against |
|---|---|---|
| Unit + property | `proptest`, exhaustive authz decision tables | codec round-trips, ranking invariants, permission logic |
| Fuzz (nightly + PR smoke) | `cargo-fuzz` with wired corpora | WAL/segment decoders, payloads, OIDC claim logic, query planner |
| Concurrency | `loom`, `shuttle` | snapshot publication, writer/reader races |
| Crash consistency | `fail` failpoints + kill -9 oracle | lost/duplicated acknowledged writes |
| Distributed | `turmoil` DST + linearizability checker | split-brain, lost writes, stale reads |
| API | `schemathesis` from OpenAPI, contract suite × 6 SDKs | contract drift, 500s on odd input |
| System | `testcontainers`, kind + Helm e2e | deploy drift, config drift |
| Quality | recall, BEIR, DASH-Cite-Bench | silent relevance/citation regressions |
| Performance | criterion + load generator on dedicated runner, regression gates | latency/throughput regressions |
| Security | authz matrix, DAST, pentest, secret scanning | auth bypass, leaks |

### 6.2 Data compatibility & migration

- Every persisted artifact (WAL frame, segment, manifest, audit record, backup) carries a format
  version; readers support N and N-1; writers gated by a cluster feature flag raised only after
  all nodes upgrade.
- v0 → v1 migration is a supported, tested, reversible-by-backup path (§P2.6).

### 6.3 Observability contract

Metrics (minimum): request latency histograms by route/status, auth failures by reason,
rate-limit rejections, WAL fsync latency and group size, memtable size, flush/compaction
backlog, segment counts, cache hit ratios, ANN recall sampler, embedding provider
latency/errors/breaker state, Raft term/commit index/apply lag, replication lag (LSN and
seconds), object-store upload lag, audit write latency/failures, per-tenant storage and query
usage. Traces span HTTP → planner → per-segment search → rerank → audit.

### 6.4 Performance engineering

Benchmarks run on fixed hardware with versioned datasets (1M, 10M; 384/768/1536-d).
Results append to `docs/benchmarks/history` **as data only** (drill workdirs and binaries move
to CI artifacts — QA-05). A >5% p99 or >2% recall regression fails the gate unless the PR
updates the baseline with justification.

---

## 7. Beyond 1.0 — keeping DASH ahead

Designed-for but not required for GA:

- **Object-storage-native tiering:** serve warm tenants directly from object storage with a
  local NVMe cache (stateless query nodes).
- **GPU index builds** (cuVS/CAGRA) for large rebuilds; quantization advances (RaBitQ-style).
- **Multi-modal evidence:** image regions, table cells and PDF bounding boxes as spans.
- **Change feeds:** subscribe to claim changes and newly detected contradictions (alerts when
  a fact an application relies on becomes disputed).
- **Learned ranking** per tenant from feedback; federated retrieval across DASH clusters.
- **Graph queries** over claim edges (support/contradiction paths, provenance lineage).
- **PII detection/redaction at ingest**, field-level retention policies.
- **Kubernetes operator**, managed cloud / BYOC.
- **WASM plugins** for custom extraction and scoring without forking.

---

## 8. Release train

| Version | Contents | Compatibility |
|---|---|---|
| v0.3.0 | P0: safe defaults, crash/data hotfixes, working deploys, truthful docs | Breaking: auth now required; strict secrets on |
| v0.4.0 | P1: `dashd`, typed config, auth v2, audit v1.5, observability | Old env names accepted with deprecation warnings |
| v0.6.0 | P2: engine v2, collections, deletes, `as_of` | `dash migrate` from v0 data required |
| v0.8.0-beta | P3: Raft cluster, PITR | Single-node data upgrades in place |
| v0.9.0-rc | P4–P6 converged: encryption, audit v2, API v1 final, SDKs | API frozen |
| v1.0.0 | P7 + GA gate | 1.x LTS, N-1 upgrade guarantee |

---

## 9. Definition of Done and truth-keeping

### 9.1 Why this section exists

The 2026-06 and 2026-08 plans marked items "DONE"/"Fixed" (e.g. secret hygiene, production
hardening) that the 2026-10 review found broken (DOC-07). The cause was status driven by
intent rather than verification. From now on:

### 9.2 Rules

1. **A register issue is closed only when** the fix is merged **and** a test that fails on the
   old code runs in CI. The closing PR links both.
2. **Plan status lines link evidence** (CI job, test name, drill record). No link ⇒ not done.
3. **Claims ledger:** `docs/claims-ledger.md` lists every capability claim made in README and
   docs, each with the test(s) proving it. A CI script verifies every referenced test exists
   and passed in the current run. Docs may not claim what the ledger cannot prove.
4. **Generated, not written:** config reference, API reference and metrics reference are
   generated from code.
5. **Every PR touching security, storage format or replication** requires a second reviewer
   and an updated threat-model/ADR line if behavior changes.

---

## 10. Execution

### 10.1 Team shape (recommended)

| Role | Phases | Focus |
|---|---|---|
| Storage engineer ×2 | P0.3, P2, P3 | WAL, engine, indexes, Raft |
| Platform/security engineer | P0.1/0.5/0.8, P1, P4, P7 | server shell, auth, crypto, deploy, CI |
| Retrieval/ML engineer | P0.6, P5 | ranking, evaluation, evidence intelligence |
| DX engineer (from ~week 16) | P0.9, P6 | API, SDKs, CLI, MCP, docs |

With 1–2 engineers, follow the same order but expect ~18–24 months; P5/P6 would be cut to
their essentials (evaluation harness, ranking v2, SDK regeneration).

### 10.2 Critical path & risks

Critical path: **P0 → P1 → P2 → P3 → P7** (~34 weeks + 4 weeks GA hardening).

| Risk | Impact | Mitigation |
|---|---|---|
| Rewrite stalls; nothing ships | High | Strangler approach: `Engine` trait, shadow reads, P0 black-box suite as acceptance tests, release every phase |
| Consensus bugs | High | Mature `openraft`; DST from day one of P3; Jepsen-style nightly; feature-flag cluster mode until beta |
| Performance targets miss | Medium | P2 spike first; targets revised with evidence, not hope |
| Index library limitations (filtering, deletes) | Medium | Spike validates filtered search and tombstone handling; segment-level rebuilds as fallback |
| Migration data loss | High | Checksummed migration report, mandatory backup step, shadow-read diff before cutover |
| Scope creep from "future" features | Medium | §7 is explicitly post-GA; P5 limited to evaluation + ranking v2 + verify if time is short |
| Doc drift returns | Medium | §9 automation (ledger, generated references, deploy linter) |

### 10.3 Decisions to confirm before P1

1. D1 single binary with roles (recommended) vs keeping two services.
2. D5 embedded Raft (recommended) vs requiring external etcd/Kubernetes leases.
3. D4 tenant-as-physical-partition (recommended) vs shared segments with tenant bitmaps.
4. Licensing/open-core boundary (affects which P4 features live in the OSS tree).
5. Managed-cloud ambition (changes P7 priority of operator/metering).

### 10.4 First two weeks — PR-sized backlog

1. `fix(auth)`: build the policy once at startup and fail closed on a missing bearer when JWT is configured. Includes the authz matrix test (SEC-01/02/06).
2. `fix(transport)`: switch to `serde_json` with depth limits, add header caps, and back off on accept errors. Includes deep-JSON and UTF-8 tests (ROB-01/02/03/06).
3. `fix(store)`: dedupe evidence and edges by ID. Includes a restart-no-duplicates test (DATA-01).
4. `fix(store)`: validate before the WAL append, reject or escape control chars, and truncate a torn tail. Includes a replay test (DATA-02/03/07).
5. `fix(control-plane)`: read by Content-Length, add client timeouts, and fix the renew deadlock. Includes a curl test (CP-01/02).
6. `fix(auth)`: require auth on embeddings, debug, replication and control-plane routes, and move embedding after authz (SEC-07…10).
7. `fix(deploy)`: remove the emptyDir, fix env names and ingress/netpol, and add kind smoke CI (DEP-01…05).
8. `fix(sdk-java)`: single body read; `fix(sdk-csharp)`: results/score contract (SDK-01/04).
9. `fix(replication)`: generation-aware offsets, next_offset fix, replace-on-resync. Includes a compaction test (REP-01…03).
10. `docs`: truth pass, plan banners, and claims ledger skeleton (DOC-*).

---

## Appendix A — Issue → phase traceability

| Register prefix | P0 | P1 | P2 | P3 | P4 | P5 | P6 | P7 |
|---|---|---|---|---|---|---|---|---|
| SEC (24) | 01–10, 18–19, 23–24 | 11–15, 17, 22 | 18 (namespaces) | — | 16, 17 (v2), 20, 21 | — | — | 20 |
| ROB (12) | 01–04, 06, 09, 10 | 04, 05, 07, 08, 11, 12 | — | — | — | — | — | — |
| DATA (14) | 01–10, 12 | 13 | 07, 10, 11, 14 | — | — | 13 | — | — |
| REP (12) | 01–06, 11, 12 | 11 | — | 07–10 | — | — | — | — |
| CP (6) | 01, 02 | 05, 06 | — | 03, 04 | — | — | — | — |
| IDX (9) | 03, 05, 07 | — | 01, 02, 04, 07, 08, 09 | — | — | 05, 06 | — | — |
| PERF (10) | 02, 04, 05, 07 | 08, 09, 10 | 01, 02, 03, 06 | — | — | — | — | — |
| EMB (6) | 01, 02, 05 | 03, 06 | 04 | — | — | — | — | — |
| SDK (9) | 01, 02 (flag), 03, 04, 06 | 09 | — | — | — | — | 02, 03, 05, 07, 08 | — |
| DEP (11) | 01–06, 08–10 | 11 | — | 07 | — | — | — | 07 |
| QA (5) | 02 | 01, 02, 03, 05 | 04 | 04 | — | — | — | — |
| DOC (7) | 01–07 | 03 | — | — | 06 | — | — | — |
| MNT (6) | — | 01–06 | 02 | — | — | — | — | — |

The per-issue phase assignment lives in the register's *Phase* column; this table summarizes it.
