# Changelog

All notable changes to `dash-kotlin` are documented in this file.

## [Unreleased]

### Added

- Suspend `deleteClaim`, `deleteEvidence` and `deleteTenant` wrappers over the Java client (`DELETE /v1/claims/{id}`, `/v1/evidence/{id}`, `/v1/tenants/{id}`).

## [0.2.0]

### Fixed

- Inherits the `dash-java` fix for the double response-body read that made
  every successful call throw `IllegalStateException: closed`.
- Retrieval models now match the server (`results`, numeric `score`,
  optional `claim_confidence`, `contradiction_risk`, `graph`,
  `read_policy`, ...). `top_k` is omitted by default so the server
  default (5) applies.
- Ingest models now match `POST /v1/ingest` (`claim`, `evidence`,
  `edges`; response `ingested_claim_id`, `claims_total`,
  `commit_status`, ...). Ingest is **experimental**.

### Added

- Separate ingestion service URL (`ingestionBaseUrl`); the ingestion
  service listens on a different port (8081) than retrieval (8080).
- `RequestOptions` (idempotency key / explicit retry opt-in).
- Optional retrieve request fields: `query_embedding`,
  `entity_filters`, `embedding_id_filters`, `time_range`,
  `read_consistency`.

### Changed

- Retries only happen for idempotent requests (embeddings, retrieve,
  health) or when an idempotency key is supplied; ingest is never
  retried by default. `Retry-After` is honoured; backoff is jittered
  and capped (5 s), attempts are capped at 10.

### Removed

- `DashClientAsync.delete()` and the `DeleteRequest` / `DeleteResponse` models: the
  server has no `/v1/delete` endpoint.
- `IngestBundle` (replaced by the server-shaped `IngestRequest`).

### Build

- Added the missing `maven-publish` plugin and removed the unconfigured
  Nexus publishing plugin; the build targets Java 17 bytecode on any
  JDK 17+ without toolchain provisioning.
- `dash-java` is not yet published: build it first with
  `mvn -q install -DskipTests` in `sdks/java` (resolved via
  `mavenLocal()`).
