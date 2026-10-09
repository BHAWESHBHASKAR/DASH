# Planned API

Everything on this page is **not implemented**. None of these routes, headers or fields exist in the code, and calling them returns 404 or is ignored. Earlier versions of the documentation described some of them as available; they were removed from the [HTTP API](api.md) reference and collected here so that nobody builds against them by mistake. Inclusion on this page is not a release commitment; scheduling lives in the [master plan](https://github.com/BHAWESHBHASKAR/DASH/blob/main/docs/plans/2026-10-09-production-readiness-master-plan.md).

| Planned capability | Earlier doc said | Reality today | Tracking |
|---|---|---|---|
| Delete / tombstone claims, evidence, edges (`POST /v1/delete`) | `{ tenant_id, claim_ids }` | No delete path of any kind. The Java, Kotlin and C# SDKs used to expose a `delete` call that targeted this missing route; it was removed in 0.3.0. | register DATA-14 (P2) |
| Tenant management (`POST /v1/tenants`) | Operators create tenants with a scoped JWT | There is no tenant registry. A tenant exists implicitly once a claim with that `tenant_id` is ingested, subject to the service tenant allowlist. There is no "unknown tenant" error path in the code, so a tenant with no data simply yields no results. | P4 |
| Source registration (`POST /v1/sources`) | Register a source before use | `source_id` is a free-form string on evidence. | design doc only |
| Claim upsert by key (`POST /v1/claims:upsert`) | Upsert API | `POST /v1/ingest` writes claims, and as of 0.3.0 re-sending a claim, evidence or edge is an idempotent upsert (see the [HTTP API](api.md#post-v1ingest)). There is no separate upsert route. | design doc only |
| Admin reindex (`POST /v1/admin/reindex`) | Rebuild an index | No admin API. The ANN graph is rebuilt in memory from the WAL / redb on startup. | design doc only |
| Request idempotency (`idempotency_key` field, `Idempotency-Key` header) | Replay-safe ingest | Not implemented as a header or field. Writes are idempotent by content key (evidence by `evidence_id`, edges by endpoints and relation), and batch, raw and document ingest accept or derive a `commit_id` that makes replays safe. | P2 |
| Rate-limit headers (`X-RateLimit-*`) | Sent with every response | Not sent. A 429 response carries `Retry-After` only (0.3.0). | register SEC-06 |
| Structured error envelope with codes and `request_id` | `{"error":{"code":...}}` | Errors are `{"error":"message"}`. | P6 |
| `DashKey` authorization scheme | `Authorization: DashKey ak_...` | Only `x-api-key` and `Authorization: Bearer`. | none |
| Per-tenant tiers (free / pro / enterprise) | Default rate limits per tier | No tier concept. | none |
| Audit read API and retention compaction | Query and compact the audit log | The audit log is an append-only file written by each service when `DASH_*_AUDIT_LOG_PATH` is set. No read API, no retention job. | P4 |
| Hosted base URLs (`*.dash.example.com`) | Staging and production | None exist. | none |
