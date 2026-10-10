# DASH

> Evidence-first vector database for citation-grade RAG.

DASH stores atomic claims with provenance, retrieves them with citation-bearing rankings, and exposes an OpenAI-compatible embeddings endpoint so existing clients can call it without code changes.

**Status: pre-1.0, not yet production-ready.** This tree is **0.3.0 (unreleased)**, the security and correctness hardening release described in [`docs/plans/2026-10-09-production-readiness-master-plan.md`](docs/plans/2026-10-09-production-readiness-master-plan.md); nothing is tagged or published yet. Upgrading from 0.2.x needs configuration changes: see [`CHANGELOG.md`](CHANGELOG.md#upgrading-to-030). Read the [Status](#status) section before deploying anything that holds real data. Every capability claim in this file is tracked in [`docs/claims-ledger.md`](docs/claims-ledger.md) with the test or document that backs it.

## Quickstart

Requires Docker with Compose v2, `git`, and `curl`. This builds the images from source (no release images have been published yet), so the first run takes several minutes.

```bash
git clone https://github.com/BHAWESHBHASKAR/DASH.git
cd DASH

# 1. Generate strong random credentials into deploy/container/.env (git-ignored).
./scripts/generate-secrets.sh
set -a; source deploy/container/.env; set +a

# 2. Build and start ingestion (:8081), retrieval (:8080) and the segment maintenance
#    daemon. The control plane (:8090) is optional: add `--profile control-plane`.
docker compose -f deploy/container/docker-compose.yml up -d --build
curl -fsS http://localhost:8081/health
curl -fsS http://localhost:8080/health
```

The services refuse to be left open: since 0.3.0 they will not start without credentials unless `DASH_INSECURE_DEV_MODE=1` is set (dev mode binds localhost only). The compose file requires the keys generated above. Send them in the `x-api-key` header (or as `Authorization: Bearer <key>`). Ingestion and retrieval use separate keys.

Ingest a claim with supporting evidence:

```bash
curl -fsS -X POST http://localhost:8081/v1/ingest \
  -H "Content-Type: application/json" \
  -H "x-api-key: $DASH_INGEST_API_KEY" \
  -d '{
    "claim": {
      "claim_id": "c1",
      "tenant_id": "t1",
      "canonical_text": "Company X acquired Company Y",
      "confidence": 0.95
    },
    "evidence": [{
      "evidence_id": "e1",
      "claim_id": "c1",
      "source_id": "news://nyt",
      "stance": "supports",
      "source_quality": 0.95
    }],
    "edges": []
  }'
```

The retrieval service is a read replica that follows the ingestion WAL by polling (250 ms in the compose file), so wait a moment, then retrieve with citations, dropping claims that have more contradicting than supporting evidence:

```bash
sleep 2
curl -fsS -X POST http://localhost:8080/v1/retrieve \
  -H "Content-Type: application/json" \
  -H "x-api-key: $DASH_RETRIEVAL_API_KEY" \
  -d '{
    "tenant_id": "t1",
    "query": "Company X acquired Company Y",
    "top_k": 5,
    "stance_mode": "support_only"
  }'
```

The OpenAI-compatible embeddings endpoint is on the retrieval service. It requires a credential with the `retrieve` role (it was open before 0.3.0, which was a defect). The default provider is a deterministic hash embedder with no network access; it is for development and tests, not semantic quality. Use `DASH_EMBEDDING_PROVIDER=ollama` for real vectors (see [Configuration](docs-site/docs/reference/configuration.md)).

```bash
curl -fsS -X POST http://localhost:8080/v1/embeddings \
  -H "Content-Type: application/json" \
  -H "x-api-key: $DASH_RETRIEVAL_API_KEY" \
  -d '{"input": "Company X acquired Company Y", "model": "text-embedding-3-small"}'
```

```python
import openai
client = openai.OpenAI(base_url="http://localhost:8080/v1", api_key=RETRIEVAL_API_KEY)
resp = client.embeddings.create(input="hello world", model="text-embedding-3-small")
```

To run from source instead of Docker, see [`docs/quickstart.md`](docs/quickstart.md). To stop and wipe the stack: `docker compose -f deploy/container/docker-compose.yml down -v`.

## Why DASH

Naive RAG ranks documents by vector similarity and returns the top *k* chunks. That fails in three common enterprise cases: (1) two sources say opposite things and nothing demotes the contradicted one, (2) a fact has a temporal window and the retrieved version is stale, (3) an auditor asks "why did the model say that" and the answer is "a vector was close." DASH treats the **claim**, an atomic source-bound assertion, as the primary data primitive, with **evidence** and **citation** as first-class fields on every result.

A retrieval result carries the claim text, a score, `supports` / `contradicts` counts, and a `citations[]` array. Each citation has `evidence_id`, `source_id`, `stance` (supports / contradicts / neutral), `source_quality`, and optional `chunk_id`, `span_start`, `span_end`, `doc_id`, `extraction_model`. The retrieval API accepts `stance_mode: support_only` to drop claims with more contradictions than supports, and `time_range: {from_unix, to_unix}` to constrain results to a validity window. DASH does not aim to be the fastest pure vector index; the goal is defensible retrieval.

## Status

This is the honest state of the 0.3.0 (unreleased) tree. The authoritative plan, with owners, phases and exit criteria, is [`docs/plans/2026-10-09-production-readiness-master-plan.md`](docs/plans/2026-10-09-production-readiness-master-plan.md); the defect list is [`docs/plans/2026-10-09-issue-register.md`](docs/plans/2026-10-09-issue-register.md); what the P0 code changes cover, with evidence and what is still open, is [`docs/plans/2026-10-09-p0-status.md`](docs/plans/2026-10-09-p0-status.md).

**Stable (behavior covered by tests, unlikely to change shape):**
- Claim, Evidence and Edge schema and validation (`pkg/schema`).
- Retrieval semantics: `balanced` and `support_only` stance modes, contradiction demotion, temporal `time_range` filtering, optional graph payload.
- WAL write, replay, checkpoint and snapshot compaction in a single process (`pkg/store`): checksummed records, torn-tail truncation, one commit group per single ingest, quarantine of unreadable legacy records (`tools/wal-inspect` to inspect and repair).
- Authentication and authorization (`services/common`, `pkg/auth`): deny by default, HS256 JWT validation (kid rotation, `iss`/`aud`, tenant claims, `exp` required, lifetime cap, `jti` denylist), scoped API keys, key revocation lists, a role hierarchy (`admin`, `ingest`, `retrieve`, `read_only`), per-tenant token-bucket rate limiting (429) and SIGHUP reload. Details: [`docs/operations/auth.md`](docs/operations/auth.md).

**Beta (works, with known gaps listed in the register or below):**
- HTTP services on a hand-written thread-per-connection transport (ingestion, retrieval, control-plane) with bounded headers, bodies and whole-request deadlines. Since 0.3.0 the services refuse to start without credentials unless `DASH_INSECURE_DEV_MODE=1`; strict secret validation (>= 16 characters for keys and tokens, >= 32 for JWT secrets, placeholders rejected) is on by default; `/v1/embeddings`, `/debug/*` and `/metrics` require auth; replication requires `DASH_INGEST_REPLICATION_TOKEN`; the control plane requires `DASH_CONTROL_PLANE_TOKEN`. Optional native TLS on every listener (rustls, TLS 1.2/1.3, ALPN `http/1.1`), client-certificate verification, replication over mutual TLS with per-follower certificate pinning, and certificate reload without restart; off by default ([`docs/operations/tls.md`](docs/operations/tls.md)).
- Evidence and edge writes are idempotent upserts (evidence by `evidence_id`, edges by endpoints and relation), so retries, restarts and replication re-apply do not duplicate citations.
- Deletes: `DELETE /v1/claims/{claim_id}` (the claim with its vector, evidence and edges), `DELETE /v1/evidence/{evidence_id}` and `DELETE /v1/tenants/{tenant_id}` (admin only) on ingestion. Each is idempotent (200 with `deleted: true|false`), audit-logged, written to the WAL as a checksummed tombstone before it takes effect, and replicated to followers; a checkpoint drops the deleted data from the log. Backups and WAL archives taken earlier still hold it ([`docs/operations/data-deletion.md`](docs/operations/data-deletion.md)).
- Single-writer ingestion plus polling read replicas that follow the WAL generation-aware (a replica that keeps up crosses a checkpoint without a resync; a full resync downloads the export in resumable, checksummed chunks, so data sets larger than one response replicate; `/ready` reflects lag), a file-lease control plane with fenced leases and lag-guarded promotion, and CSV placement files. This is not consensus replication.
- `redb` on-disk persistence is **on by default** when a WAL path is configured (default `./data/dash-ingestion.redb` and `./data/dash-retrieval.redb`; opt out with `DASH_INGEST_PERSISTENCE_DISABLE=1` / `DASH_RETRIEVAL_PERSISTENCE_DISABLE=1`). If the file cannot be opened the service logs an error and continues in memory. Snapshot values are written by an in-tree versioned codec; snapshots written by earlier releases (the `bincode` 1.x layout) still load.
- Embedding providers: `hash` (default, dev only), `ollama`, `openai` (HTTPS). `/v1/embeddings` rejects token-id array inputs unless `DASH_EMBEDDING_ALLOW_TOKEN_IDS=1`, and `usage.prompt_tokens` is an estimate. The Ollama endpoint variable is `DASH_OLLAMA_ENDPOINT`. Live Ollama and OpenAI calls are not exercised by CI.
- SHA-256 hash-chained audit log (`DASH_*_AUDIT_LOG_PATH`) with one canonical record encoding, file locking, torn-tail recovery and the `tools/audit-verify` verifier (`scripts/verify_audit_chain.sh` wraps it). The chain is **unkeyed**, so it detects accidental edits but not an attacker who can rewrite the file; there is no HMAC. Details: [`docs/operations/audit-chain.md`](docs/operations/audit-chain.md).
- OIDC/JWKS validation: asymmetric algorithms only, `iss`/`aud`/`exp` required, HTTPS JWKS, a hardened JWKS cache. Tests use RS256 keys against an in-process stub IdP; there is no test against a real identity provider, and the ES256/EdDSA/PS* algorithms are accepted but untested.
- Segment build, manifest, compaction and maintenance daemon (`services/indexer`), with collision-free tenant directories. Retrieval's use of segments is partial.
- Vector search: a per-tenant index in `pkg/store/src/vector_index.rs`. A tenant is searched exactly (flat scan) up to `DASH_*_VECTOR_FLAT_THRESHOLD` vectors (default 8192) and through a [`usearch`](https://github.com/unum-cloud/usearch) HNSW (cosine, `i8` quantization, exact `f32` rerank of 50 candidates) above it. Time-range, entity and claim-id filters are applied as predicates, and allowed sets no larger than the threshold are scanned exactly. Recall@10 against brute force is at least 0.95 on seeded clustered data (`pkg/store/tests/vector_recall.rs`, `vector_filtered.rs`). With a WAL path set, the index is **saved next to the WAL** (`<WAL path>.vindex`; periodically, after ingestion checkpoints and at clean shutdown) and loaded at startup instead of rebuilt; only vectors written after the save are re-applied, and a damaged or mismatched file (format, tuning, WAL generation) is discarded with a warning and rebuilt from the WAL. In a release build on 4 vCPUs, 100,000 x 384-d vectors start in 8.8 s instead of 23 to 27.5 s; the rest is WAL replay, which still grows with the data ([measurements](docs/benchmarks/performance.md)). Turn it off with `DASH_*_VECTOR_INDEX_PERSIST=0`. Tuning via `DASH_*_ANN_*` and `DASH_*_VECTOR_*`.
- Crash and disk-full behavior of the ingestion service, tested against the real binary: writes acknowledged with a 2xx survive `kill -9` at random moments with no duplicates and no half-applied bundle or batch (`tools/crash-test`, 25 cycles per PR and 1000 nightly); on a full volume writes fail with a 5xx, `/ready` reports `wal_write_failed` until space is back (a failed fsync instead poisons the WAL until restart), and nothing acknowledged is lost. A process crash is not a power cut: the page cache survives `kill -9`. `tools/loadgen` measures throughput and latency and runs the nightly soak. Details: [`docs/operations/testing-durability.md`](docs/operations/testing-durability.md).
- Full-text search: a per-tenant BM25 index in `pkg/store/src/text_index.rs` (Unicode word segmentation, lowercasing, English stemming of ASCII words, stop words dropped from queries). Lexical candidates are the `top_k * 20` best BM25 matches (clamped to 100..5000) after filters; hybrid queries blend cosine with normalised BM25. The index is kept in step with ingest, updates, tombstones and replication, and rebuilt by the WAL replay at startup. On a labelled relevance set (`pkg/store/tests/relevance_eval.rs`, nDCG@10 gated in CI) BM25 scores 0.874 against 0.769 for the previous shared-word rule.
- Upgrades from 0.2, tested against state captured from the 0.2 release (`tests/compat`): the current code replays its WAL and snapshot, holds exactly the data written to it, serves the same retrieve answers except for listed, intentional ranking changes, migrates the redb mirror and snapshot forward, continues the audit chain and takes over the control-plane lease. Replication between 0.2 and 0.3 is refused in both directions (upgrade the leader first), and rolling back to 0.2 means restoring the pre-upgrade backup: 0.2 cannot read what 0.3 writes. Procedures and the format table: [`docs/operations/upgrades.md`](docs/operations/upgrades.md).
- Observability: `/metrics` on all three services is valid Prometheus exposition (checked by a strict parser in the tests) with per-route request counters by status code, latency histograms (`dash_http_server_request_duration_seconds`), an in-flight gauge, WAL append/fsync, group-commit, checkpoint and vector index histograms, embedding call latency, errors and breaker state, follower lag in records and seconds, process metrics and `dash_build_info`; labels are bounded (no tenant or claim ids). Every request carries an `X-Request-Id` (a valid client id is kept, otherwise one is generated) that is echoed in the response, added to JSON error bodies, stored in audit records and attached to log events; `DASH_LOG_FORMAT=json` gives JSON logs. `deploy/observability/` ships 21 alerts, each with a runbook in [`docs/operations/runbooks/`](docs/operations/runbooks/) and unit-tested with `promtool test rules` in CI, SLO recording rules ([`docs/operations/slos.md`](docs/operations/slos.md)) and three Grafana dashboards; the Helm chart can install them (ServiceMonitor, PrometheusRule, dashboard ConfigMap). No OpenTelemetry trace export yet.
- Ranking: support and contradiction contributions saturate (support bonus capped at 0.4, contradiction penalty at 0.5), and an edge `from supports to` credits the target claim.
- Kubernetes: the Helm chart and the raw manifests install on a kind cluster and pass an end-to-end test (`scripts/kind_e2e.sh`, CI workflow `kind-e2e.yml`): authenticated ingest, retrieve from every retrieval replica, delete, ingestion and retrieval pod kills (including a lost follower PVC), cold backup and restore of the ingestion volume (`scripts/k8s_backup_restore.sh`), `helm upgrade` with changed values and from an older chart, and native TLS with a self-signed CA, under Pod Security "restricted" with NetworkPolicy enforced. One ingestion pod (no writer failover) and manual retrieval scaling ([`docs/operations/kubernetes.md`](docs/operations/kubernetes.md)).
- Benchmark and drill scripts. Numbers in `docs/benchmarks/` are drill output, not a published performance claim.

**Planned (do not rely on):**
- Keyed (HMAC) audit chain and external anchoring; the current chain is unkeyed.
- Encryption at rest / CMEK. `pkg/encryption` is a standalone AES-256-GCM library with an env-key provider; no storage code calls it, so nothing on disk is encrypted by DASH.
- An external penetration test and published SOC 2 evidence. TLS on by default in the shipped deployment artifacts (today it is one setting away; see above).
- GPU vector backend. `pkg/store/src/gpu.rs` is a placeholder that never returns a GPU engine; scoring runs on CPU.
- Consensus replication (Raft), automatic failover, sharded cluster mode.
- Tenant management (creating or listing tenants) and reindex APIs (`POST /v1/tenants`, `/v1/admin/reindex` do not exist; see [Planned API](docs-site/docs/reference/planned-api.md)). Per-tenant write parallelism is deferred ([ADR 0004](docs/adr/0004-per-tenant-write-partitioning.md)).
- Published release artifacts. The release workflow builds CycloneDX/SPDX SBOMs, keyless cosign signatures and build-provenance attestations for every binary and image ([`docs/operations/supply-chain.md`](docs/operations/supply-chain.md)), but no release has been tagged with it yet. CI actions are pinned by commit SHA, base images by digest, and `cargo deny` gates licenses, sources and advisories.

## Architecture

DASH is a Rust workspace (edition 2024).

| Crate | Role |
|---|---|
| `pkg/schema` | Claim, Evidence, ClaimEdge types and validation |
| `pkg/store` | In-memory store, WAL (checksummed records, commit groups, generations), redb disk layer, per-tenant vector index (flat scan plus `usearch` HNSW), GPU placeholder |
| `pkg/ranking` | Scoring (confidence, stance, source quality, contradiction penalty) |
| `pkg/graph` | Claim graph expansion and support/contradiction path reasoning |
| `pkg/auth` | HS256 JWT, OIDC/JWKS, roles, SHA-256 helper |
| `pkg/embeddings` | `EmbeddingProvider` trait; hash, Ollama, OpenAI providers; circuit breaker |
| `pkg/encryption` | AES-256-GCM envelope library with env-key provider (not wired into storage) |
| `services/ingestion` | Write API (`/v1/ingest*`), WAL owner, replication source |
| `services/retrieval` | Read API (`/v1/retrieve`, `/v1/embeddings`), WAL/replication follower |
| `services/control-plane` | Placement state, file-lease leader election, failover promotion |
| `services/metadata-router` | Shard placement and read/write routing library used by the services |
| `services/indexer` | Segment builder, compaction planner, `segment-maintenance-daemon` |
| `services/common` | Deny-by-default auth policy and rate limiter, secret validation, audit chain writer and verifier, shutdown signaling, logging |
| `tools/wal-inspect` | Inspect, verify and repair WAL and snapshot files |
| `tools/audit-verify` | Verify audit-chain files (v2 and legacy records) |
| `tools/crash-test` | Randomized `kill -9` crash-consistency harness for the ingestion service |
| `tools/loadgen` | Closed-loop load and soak generator (throughput, latency percentiles, server RSS) |
| `tests/benchmarks` | Benchmark and load-test binaries |

Ingested claims are written to a WAL, replayed into an in-memory Claim + Evidence + Edge store, and indexed for ANN candidate generation. The retrieval path combines ANN and BM25 full-text candidates with tenant, time-range and stance filters and optional graph expansion, then projects citation-bearing results. A claim that shares no term with the query and is not a vector candidate is never returned, even when fewer than `top_k` claims match, and the answer is the same with or without a segment directory. Design detail: [`docs/architecture/eme-architecture.md`](docs/architecture/eme-architecture.md) (the design document predates several renames; where it disagrees with the code, the code and this README win).

## SDKs

Six client SDKs live in `sdks/`, all at version 0.2.0 (the Go module is untagged). They send the API key as `Authorization: Bearer <api_key>`, which the servers accept. Test counts are static counts of test declarations and include live-integration tests that are skipped without a running server.

| SDK | Path | Version | Coverage | Tests |
|---|---|---|---|---|
| Python (`dash-py`) | `sdks/python` | 0.2.0 | embeddings, retrieve (full request and response contract); sync and async; OpenAI-compat helper; claim, evidence and tenant deletes. No ingest method. | 77 |
| Go | `sdks/go` (module `github.com/BHAWESHBHASKAR/DASH/sdks/go`) | untagged | embeddings, retrieve, OpenAI-compat, deletes. No ingest method. | 98 |
| TypeScript (`dash-ts`) | `sdks/typescript` | 0.2.0 | embeddings, retrieve, OpenAI-compat, deletes. No ingest method. | 77 |
| Java | `sdks/java` | 0.2.0 | embeddings, ingest (separate ingestion base URL), retrieve, deletes. | 37 |
| Kotlin | `sdks/kotlin` | 0.2.0 | embeddings, ingest, retrieve, deletes (suspend API, wraps the Java client). | 14 |
| C# | `sdks/csharp` | 0.2.0 | embeddings, ingest (`IngestionBaseUrl` option), retrieve, deletes; sync and async. | 63 |

The generic `delete` call that the Java, Kotlin and C# SDKs used to expose (it targeted a `POST /v1/delete` route that does not exist) was removed in 0.3.0; the scoped delete methods (`delete_claim` / `deleteClaim` / `DeleteClaim`, and the evidence and tenant variants) call the real delete routes on the ingestion service. The Go module path changed from `github.com/anomalyco/dash-go` to `github.com/BHAWESHBHASKAR/DASH/sdks/go`; update your imports. The Java, Kotlin and C# SDKs retry only idempotent requests (or requests with an `Idempotency-Key`). See [`sdks/LIVE_INTEGRATION_TESTS.md`](sdks/LIVE_INTEGRATION_TESTS.md) for running SDK tests against a live stack.

## Tests

Counts are static (computed 2026-10-10: the Rust figure with `cargo test --workspace -- --list 2>/dev/null | grep -c ': test$'`, the SDK figures by counting test declarations); they are not a pass/fail report. CI is the source of truth for what passes.

| Suite | Declared tests |
|---|---|
| Rust workspace (`#[test]` and `#[tokio::test]`) | 1424 |
| Python SDK | 77 |
| Go SDK | 98 |
| TypeScript SDK | 77 |
| Java SDK | 37 |
| Kotlin SDK | 14 |
| C# SDK | 63 |

There are also four `cargo-fuzz` targets in `fuzz/` (JWT, OpenAI embeddings parser, ranking, WAL parser) and a benchmark suite in `tests/benchmarks`.

## Comparison

See [`docs/comparison.md`](docs/comparison.md) for a feature comparison with other vector databases and guidance on when not to use DASH. DASH's differentiators are the claim/evidence/contradiction/temporal data model and per-result citations. It does not match mature vector databases on scale, operational tooling, or ANN performance, and none of those comparisons are backed by head-to-head benchmarks yet.

## Roadmap

Shipped in this tree (0.3.0, unreleased): claim/evidence/edge model, WAL with checkpoints, checksums, commit groups and generations, retrieval with stance and time filters, deny-by-default scoped-key, JWT and OIDC auth with role hierarchy and rate limiting, OpenAI-compatible authenticated embeddings endpoint, hash-chained audit log with verifier, segment indexer and maintenance daemon, control plane with authenticated, fenced file-lease leadership, redb persistence layer, WAL and audit tools, six SDKs, Docker/Helm/systemd packaging, benchmark suite.

Remaining P0 and later work is tracked in [`docs/plans/2026-10-09-p0-status.md`](docs/plans/2026-10-09-p0-status.md) and the issue register.

After that, in plan order: container/Helm/CI hardening (P1), storage engine and index rework (P2), consensus replication (P3), encryption at rest and tenant lifecycle (P4), retrieval quality evaluation (P5), API and SDK regeneration (P6), GA hardening (P7). See the master plan for scope and exit criteria.

## Contributing

See [`CONTRIBUTING.md`](CONTRIBUTING.md). New code must pass `cargo fmt --check`, `cargo clippy --workspace --all-targets -- -D warnings` and `cargo test --workspace`. An issue is closed only when the fix is merged with a regression test that fails on the old code; plan status lines must link to evidence.

## License

Apache License 2.0. See [`LICENSE`](LICENSE).
