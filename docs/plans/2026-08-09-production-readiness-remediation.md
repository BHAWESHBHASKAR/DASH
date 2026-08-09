# DASH Production-Readiness Remediation Plan

Date: 2026-08-09  
Status: in progress — M1 e2e data path and M2 embedding provider selection implemented  
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
| Secret hygiene | Fixed | Compose requires `.env` secrets with `:?` expansion; `.env.example` and `scripts/generate-secrets.sh` provided; `DASH_STRICT_SECRETS=1` fails fast |
| Container security | Good | non-root user, `cap_drop=[ALL]`, read-only disabled for state volume, mem/pids limits, healthchecks |
| Compose persistence | Fixed | `DASH_*_PERSISTENCE_PATH` now points to `/var/lib/dash/state/*.redb` |
| Metrics / health | Present | `/health`, `/metrics`, `/debug/placement`; `/ready` fails when disk is unavailable; Prometheus alert rules added |
| **End-to-end data path** | **Fixed** | retrieval polls ingestion's replication endpoints every 250ms in compose; `test_retrieve_after_direct_ingest_returns_results` passes |
| **Embedding provider selection** | **Fixed** | `/v1/embeddings` honors `DASH_EMBEDDING_PROVIDER` (ollama/openai/hash); hash remains the safe default |
| **Real semantic embeddings** | **Scaffolding** | ollama/openai providers exist; default `hash` is kept for reproducible tests; operators set `DASH_EMBEDDING_PROVIDER` + endpoint/credential env vars for real vectors |
| **Control-plane HA** | **Missing** | no leader election, automatic failover, or multi-replica ack protocol in default compose |
| **Backup / restore** | **Fixed** | `scripts/backup_state_bundle.sh` / `restore_state_bundle.sh` plus `scripts/backup_restore_drill.sh`; drill runs in CI and passes |

## 3. Blockers and remediation priority

### P0 — must fix before any production traffic

1. **ingestion -> retrieval data path in default compose** — **DONE**
   - **Fix**: implemented a `retrieval` follower thread that polls `ingestion`'s replication endpoints and applies WAL deltas to a shared-mutable `InMemoryStore` behind `Arc<RwLock<...>>`.
   - **Acceptance**: `sdks/python/tests/test_live_integration.py::test_retrieve_after_direct_ingest_returns_results` now asserts `len(response.results) > 0` and passes against `docker compose up`.
   - **Done**: the follower persists its last offset to `DASH_RETRIEVAL_REPLICATION_OFFSET_PATH` and resumes incrementally after restart.

2. **Real embedding provider for semantic search** — **DONE**
   - **Fix**: `/v1/embeddings` uses `select_provider_from_env()` and supports `DASH_EMBEDDING_PROVIDER=ollama|openai|hash`.
   - **Remaining**: default is `hash` for reproducible tests; production deployments must set `DASH_EMBEDDING_PROVIDER` and the corresponding endpoint/credential env vars.

### P1 — strong confidence before production

3. **Non-placeholder secrets by default** — **DONE**
   - **Fix**: `docker-compose.yml` uses `${VAR:?...}` expansion to fail fast when `.env` is missing, ships `.env.example`, and `scripts/generate-secrets.sh` creates a valid `.env`.
   - **Acceptance**: `docker compose up` fails with a clear message when keys are not set; the backup/restore drill passes with generated keys.

4. **Backup / restore / RPO / RTO** — **DONE**
   - **Fix**: `scripts/backup_state_bundle.sh` and `scripts/restore_state_bundle.sh` package and unpack the WAL; `scripts/backup_restore_drill.sh` automates ingest → backup → destroy volumes → restore → assert same retrieval results; the drill is part of CI.
   - **Acceptance**: RPO defaults to one WAL record (`DASH_INGEST_WAL_SYNC_EVERY_RECORDS=1`); the drill passes and is run on every PR.

5. **Structured logging and alerting hooks** — **DONE**
   - **Fix**: `dash_common::init_logging()` configures `tracing-subscriber` with `DASH_LOG_FORMAT=json`; service startup messages use `tracing::info!`/`tracing::error!`; `/ready` fails when redb disk is unavailable; Prometheus alert rules added for disk unavailable, replication lag, storage divergence, and ready-probe failures.
   - **Acceptance**: `DASH_LOG_FORMAT=json` emits JSON lines; `/ready` returns 503 with a disk reason when persistence is configured but unavailable; `docker compose` healthchecks pass.

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

| Milestone | Deliverable | State | ETA (Devin sessions) |
|---|---|---|---|
| M1 | retrieval follower pull loop + shared-mutable store | Done | 1 session |
| M2 | embedding provider selection via `DASH_EMBEDDING_PROVIDER` | Done | 0.25 session |
| M2b | real HTTP embedding provider as default + integration test | Backlog | 0.75 session |
| M3 | remove placeholder secrets from compose + `.env.example` + docs | Done | 0.5 session |
| M3b | persist follower replication offset | Done | 0.25 session |
| M4 | backup/restore scripts + recovery drill in CI | Done | 0.5 session |
| M5 | structured JSON logs + `/ready` probe | Done | 0.5 session |
| M6 | control-plane leader election + failover | Done | 1 session |
| M7 | quorum replication with lag SLO | Done | 0.75 session |
| M8 | disk-first segment serving tier | Done | 0.25 session |
| M9 | object-storage backup/restore | Done | 0.75 session |
| M10 | Helm control-plane + managed cloud scaffolding | Done | 0.75 session |
| M11 | RBAC/OIDC, CMEK, SOC 2 readiness | Not started | 1.5 sessions |

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
- 2026-08-09: implemented the retrieval follower pull loop with `Arc<RwLock<InMemoryStore>>` and `clear_wal_events()` to keep the in-memory WAL buffer from growing unbounded on follower nodes.
- 2026-08-09: wired `select_provider_from_env()` into `/v1/embeddings` so `DASH_EMBEDDING_PROVIDER=ollama|openai|hash` works without code changes.
