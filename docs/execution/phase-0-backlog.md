# Phase 0 Backlog (Vertical Slice)

Date: 2026-02-17
Target Window: 10 working days

## Scope

Deliver ingest -> claim/evidence persistence -> retrieval with citations on a single-node deployment.

## Tasks

1. Define initial schemas
- create claim/evidence/edge JSON schema files under `pkg/schema`
- add schema validation tests

2. Implement ingestion service skeleton
- create `POST /v1/sources` and WAL append path in `services/ingestion`
- emit structured ingest events

3. Implement retrieval service skeleton
- create `POST /v1/retrieve` in `services/retrieval`
- return top-k claims with provenance payload format

4. Implement initial indexer hooks
- create delta index interfaces in `services/indexer`
- connect ingestion updates to index mutations

5. Add integration tests
- ingest fixture corpus and verify retrieval responses
- validate citation span presence and schema compliance

6. Add benchmark smoke run
- run fixed query subset across B0 and B2 systems
- output first scorecard artifact

## Definition of Done

- integration test pipeline is green
- retrieval response includes citations for all returned claims
- p95 latency and freshness metrics are emitted to dashboard/log stream
- first benchmark scorecard is committed in `docs/benchmarks`

