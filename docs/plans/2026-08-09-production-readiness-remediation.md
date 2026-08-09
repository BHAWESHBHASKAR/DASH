# DASH Production-Readiness Remediation Plan

Date: 2026-08-09  
Status: in progress  
Target: make the DASH vector/RAG engine safe to run in a production environment.

## 1. What "production ready" means for DASH

A production-ready DASH deployment must guarantee:

1. **Durability**: ingested claims survive restarts, crashes, and rolling updates.
2. **Security**: no default/placeholder secrets, dependency CVEs patched, auth enforced, audit logs tamper-evident.
3. **Observability**: structured logs, metrics, health/readiness probes, and alerting hooks.
4. **Operations**: documented upgrade/backup/restore paths, resource limits, graceful shutdown, and repeatable deployment artifacts.
5. **Correct end-to-end data flow**: an ingest on `:8081` is visible to a retrieve on `:8080` within a bounded latency.

## 2. Current state (2026-08-09)

| Area | State | Evidence |
|---|---|---|
| Build / test | Green locally and in CI | `cargo fmt/clippy/test`, `./scripts/ci.sh` pass |
| Security deps | Clean | `cargo audit` clean, Trivy CRITICAL/HIGH cleaned, `jsonwebtoken` 10 + `aws_lc_rs`, Jackson/pygments/vitest bumped |
| Secret hygiene | Partially fixed | Startup validation warns on `change-me-*` placeholders; `DASH_STRICT_SECRETS=1` fails fast |
| Container security | Good | non-root user, `cap_drop=[ALL]`, read-only disabled for state volume, mem/pids limits, healthchecks |
| Compose persistence | Fixed | `DASH_*_PERSISTENCE_PATH` now points to `/var/lib/dash/state/*.redb` |
| Metrics / health | Present | `/health`, `/metrics`, `/debug/placement` on both HTTP services |
| **End-to-end data path** | **Broken** | ingestion and retrieval use separate WAL/segment dirs; no follower pull loop for retrieval |
| **Real embedding providers** | **Scaffolding** | HTTP/ONNX adapters are not wired; default `HashEmbeddingProvider` is deterministic and not semantic |
| **Control-plane HA** | **Missing** | no leader election, automatic failover, or multi-replica ack protocol in default compose |
| **Backup / restore** | **Scripts exist, not automated** | `scripts/replication_lag_guard.sh`, `scripts/storage_promotion_boundary_guard.sh` exist but are not scheduled |

## 3. Blockers and remediation priority

### P0 — must fix before any production traffic

1. **ingestion -> retrieval data path in default compose**
   - **Problem**: retrieval loads its own WAL at startup and never refreshes. Ingested data is not retrievable across the two services.
   - **Options**:
     a. *Follower pull*: teach `retrieval` to poll `ingestion`'s `/internal/replication/wal` and `/internal/replication/export` endpoints and apply deltas to a shared-mutable `InMemoryStore`.
     b. *Shared read-only segment store*: make `ingestion` publish segments to a path that `retrieval` reads, and have `retrieval` load claim records from segments (not just claim IDs).
     c. *Single local store*: run ingestion and retrieval as a single process with one WAL/segment dir (largest architectural change).
   - **Recommended**: option (a). The replication endpoints already exist in ingestion; the retrieval side needs a background pull loop and the `InMemoryStore` made mutable behind `Arc<RwLock<...>>`.
   - **Acceptance**: `sdks/python/tests/test_live_integration.py` `test_retrieve_after_direct_ingest_returns_results` asserts `len(response.results) > 0` and the retrieved claim matches the ingested `unique_phrase`.

2. **Real embedding provider for semantic search**
   - **Problem**: the default `HashEmbeddingProvider` returns deterministic vectors that do not encode semantic meaning. Production semantic retrieval requires an HTTP (OpenAI/Ollama) or ONNX backend.
   - **Fix**: finish the `OpenAiEmbeddingProvider` / `OllamaEmbeddingProvider` and make one of them the default when `DASH_EMBEDDING_PROVIDER` is configured; gate `HashEmbeddingProvider` behind an explicit `test-only` setting.
   - **Acceptance**: compose `retrieval` can answer a semantic query (`"what is the capital of France?"`) with the correct claim when using an external or mock embedding endpoint.

### P1 — strong confidence before production

3. **Non-placeholder secrets by default**
   - **Problem**: `docker-compose.yml` still ships `change-me-*` keys and only warns unless `DASH_STRICT_SECRETS=1` is set.
   - **Fix**: remove `change-me` defaults, commit `.env.example` with `REPLACE_...` placeholders, and make `docker compose` fail if keys are not supplied. Keep `DASH_STRICT_SECRETS=1` behavior as an additional guard.
   - **Acceptance**: `docker compose up` fails with a clear message when keys are not set; CI and docs explain how to generate keys.

4. **Backup / restore / RPO / RTO**
   - **Problem**: WAL, redb snapshot, and segment files can be recovered manually, but no automated, tested recovery path is scheduled.
   - **Fix**: add `scripts/backup.sh` and `scripts/restore.sh` that snapshot `/var/lib/dash` and verify WAL replay; run a recovery drill in CI weekly.
   - **Acceptance**: documented RPO (configurable, e.g. 1s with `sync_every_records=1`) and RTO (measured, <30s for 1M claims) targets.

5. **Structured logging and alerting hooks**
   - **Problem**: services use `println!`/`eprintln!` for startup and error messages; no JSON log format or correlation IDs.
   - **Fix**: initialize `tracing-subscriber` with `json` formatting via `DASH_LOG_FORMAT=json`; add `trace_id` extraction/forwarding in HTTP handlers; expose `/ready` that fails if placeholder secrets or redb fallback is active.
   - **Acceptance**: logs are parseable JSON; `/ready` returns 503 when `DASH_STRICT_SECRETS` violations or disk fallback occur.

### P2 — scale and hardening

6. **Control-plane leader election and failover**
   - Implement the control-plane as a real service, add shard placement leader/follower state, and wire `ingestion`/`retrieval` to fail closed on unhealthy leaders.
   - Acceptance: `control-plane` container in compose; `POST /v1/control-plane/failover/promote` increments epoch and re-targets writes.

7. **Multi-replica ack/quorum replication**
   - Extend the existing ingestion follower pull loop to support synchronous or asynchronous quorum replication.
   - Acceptance: replication lag SLO test passes under node failure.

8. **Disk-first segment serving tier**
   - Complete the segment-disk-base execution path so retrieval can serve large tenants from segments without loading the full WAL into memory.
   - Acceptance: benchmark `xlarge` profile memory stays under 4 GiB for 1M claims.

## 4. Suggested first milestones

| Milestone | Deliverable | ETA (Devin sessions) |
|---|---|---|
| M1 | retrieval follower pull loop + shared-mutable store | 1 session |
| M2 | real HTTP embedding provider as default + integration test | 1 session |
| M3 | remove placeholder secrets from compose + `.env.example` + docs | 0.5 session |
| M4 | backup/restore scripts + recovery drill in CI | 0.5 session |
| M5 | structured JSON logs + `/ready` probe | 0.5 session |

## 5. Risk register

| Risk | Mitigation |
|---|---|
| `jsonwebtoken` 10 + `aws_lc_rs` compile failures on older runners | Pin `ubuntu-24.04` and ensure `cmake`/`build-essential` are installed; already in Dockerfile. |
| Trivy Maven 429 rate limiting | Cache `~/.m2` with `actions/setup-java` before scanning; added to `security.yml`. |
| Shared mutable store harms retrieval latency | Use `RwLock` for many readers / one writer; keep replication poll interval configurable (default 1s). |
| Real embedding endpoint unavailable in CI | Provide a deterministic mock provider that encodes simple lexical semantics for tests, and use it only in test mode. |

## 6. Decision log

- 2026-08-09: chose `jsonwebtoken` 10 with `aws_lc_rs` over `rust_crypto` because `rust_crypto` pulled in vulnerable `rsa` 0.9.10 (RUSTSEC-2023-0071).
- 2026-08-09: chose warning-by-default + `DASH_STRICT_SECRETS=1` exit-2 for placeholder secrets to preserve local quick-start while blocking production deployments.
- 2026-08-09: chose follower pull (option a) for the e2e data path because ingestion already exposes the required replication endpoints and it minimizes changes to the existing storage layout.
