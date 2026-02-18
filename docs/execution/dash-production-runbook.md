# DASH Production Runbook

Date: 2026-02-17  
Status: draft

## 1. Scope

This runbook covers single-region production deployment for DASH ingestion and retrieval services with WAL durability and replay.

## 2. Build

```bash
cargo build --release -p ingestion -p retrieval -p benchmark-smoke
```

Optional async runtime build (for `DASH_*_TRANSPORT_RUNTIME=axum`):

```bash
cargo build --release -p ingestion --features async-transport
cargo build --release -p retrieval --features async-transport
```

Artifacts:

- `target/release/ingestion`
- `target/release/retrieval`
- `target/release/benchmark-smoke`

## 3. Required Paths

- persistent data dir example: `/var/lib/dash`
- shared WAL path example: `/var/lib/dash/claims.wal`
- ensure process user can read/write WAL path

## 4. Startup Order

1. Start ingestion service.
2. Start retrieval service with `--serve`.
3. Verify `/health`.
4. Verify `/metrics`.
5. Run a retrieval probe request.

## 5. Service Commands

Ingestion:

```bash
DASH_INGEST_WAL_PATH=/var/lib/dash/claims.wal \
DASH_INGEST_BIND=0.0.0.0:8081 \
DASH_INGEST_HTTP_WORKERS=8 \
DASH_INGEST_TRANSPORT_RUNTIME=std \
DASH_INGEST_API_KEY=change-me-ingest-key \
DASH_INGEST_ALLOWED_TENANTS=tenant-a,tenant-b \
DASH_INGEST_API_KEY_SCOPES="change-me-ingest-key:tenant-a,tenant-b" \
DASH_INGEST_AUDIT_LOG_PATH=/var/log/dash/ingestion-audit.jsonl \
DASH_INGEST_SEGMENT_DIR=/var/lib/dash/segments \
DASH_INGEST_WAL_SYNC_EVERY_RECORDS=1 \
DASH_INGEST_WAL_APPEND_BUFFER_RECORDS=1 \
DASH_INGEST_ALLOW_UNSAFE_WAL_DURABILITY=false \
DASH_CHECKPOINT_MAX_WAL_RECORDS=50000 \
DASH_CHECKPOINT_MAX_WAL_BYTES=52428800 \
target/release/ingestion --serve
```

Retrieval:

```bash
DASH_RETRIEVAL_WAL_PATH=/var/lib/dash/claims.wal \
DASH_RETRIEVAL_BIND=0.0.0.0:8080 \
DASH_RETRIEVAL_HTTP_WORKERS=8 \
DASH_RETRIEVAL_TRANSPORT_RUNTIME=std \
DASH_RETRIEVAL_API_KEY=change-me-retrieval-key \
DASH_RETRIEVAL_ALLOWED_TENANTS=tenant-a,tenant-b \
DASH_RETRIEVAL_API_KEY_SCOPES="change-me-retrieval-key:tenant-a,tenant-b" \
DASH_RETRIEVAL_AUDIT_LOG_PATH=/var/log/dash/retrieval-audit.jsonl \
DASH_RETRIEVAL_SEGMENT_DIR=/var/lib/dash/segments \
target/release/retrieval --serve
```

Compatibility note: legacy `EME_*` env vars are still accepted as fallback.
Compatibility note: `DASH_*_TRANSPORT_RUNTIME=axum` requires binaries built with `async-transport` feature.
Policy note: if `DASH_*_API_KEY_SCOPES` is set, key scope checks are enforced before tenant allowlist checks.
Policy note: `DASH_*_API_KEYS` enables rotation overlap (multiple active keys); `DASH_*_REVOKED_API_KEYS` hard-denies compromised keys.

## 6. Smoke Checks

Health:

```bash
curl -sS http://127.0.0.1:8081/health
```

Metrics:

```bash
curl -sS http://127.0.0.1:8081/metrics
```

Ingest:

```bash
curl -sS -X POST http://127.0.0.1:8081/v1/ingest \
  -H "X-API-Key: change-me-ingest-key" \
  -H "Content-Type: application/json" \
  -d '{"claim":{"claim_id":"claim-1","tenant_id":"tenant-a","canonical_text":"Company X acquired Company Y","confidence":0.9},"evidence":[{"evidence_id":"ev-1","claim_id":"claim-1","source_id":"source://doc-1","stance":"supports","source_quality":0.95}]}'
```

Retrieve:

```bash
curl -sS -H "X-API-Key: change-me-retrieval-key" "http://127.0.0.1:8080/v1/retrieve?tenant_id=sample-tenant&query=retrieval+initialized&top_k=5&stance_mode=balanced"
```

## 7. Operational Guardrails

- set checkpoint limits to avoid unbounded WAL growth
- keep `DASH_INGEST_WAL_SYNC_EVERY_RECORDS=1` for strict per-record durability; increase only when explicitly trading crash-window durability for ingestion throughput
- keep `DASH_INGEST_WAL_APPEND_BUFFER_RECORDS=1` for no in-process batching; increase only for controlled throughput experiments
- optionally set `DASH_INGEST_WAL_SYNC_INTERVAL_MS` to cap maximum durability lag window when batching is enabled
- ingestion startup enforces WAL durability guardrails by default:
  - batched durability (`sync_every>1` or `append_buffer>1`) requires `DASH_INGEST_WAL_SYNC_INTERVAL_MS`
  - excessive durability windows are rejected unless explicit override is set
- only set `DASH_INGEST_ALLOW_UNSAFE_WAL_DURABILITY=true` for controlled stress/benchmark runs (not production default)
- before changing WAL durability defaults, run:
  - `scripts/benchmark_ingest_wal_durability.sh --workers 4 --clients 16 --requests-per-worker 25 --warmup-requests 5`
  - archive the generated markdown under `docs/benchmarks/history/concurrency/wal-durability/`
- monitor replay startup logs (`snapshot_records`, `wal_delta_records`)
- monitor ingestion `/metrics` for:
  - `dash_ingest_success_total`
  - `dash_ingest_failed_total`
  - `dash_ingest_segment_publish_success_total`, `dash_ingest_segment_publish_failure_total`
  - `dash_ingest_segment_last_claim_count`, `dash_ingest_segment_last_segment_count`
  - `dash_ingest_wal_unsynced_records`, `dash_ingest_wal_buffered_records`
  - `dash_ingest_wal_flush_due_total`, `dash_ingest_wal_flush_success_total`, `dash_ingest_wal_flush_failure_total`
  - `dash_ingest_auth_success_total`, `dash_ingest_auth_failure_total`, `dash_ingest_authz_denied_total`
  - `dash_ingest_audit_events_total`, `dash_ingest_audit_write_error_total`
  - `dash_ingest_placement_route_reject_total`, `dash_ingest_placement_last_epoch`
  - `dash_ingest_placement_reload_enabled`, `dash_ingest_placement_reload_attempt_total`, `dash_ingest_placement_reload_failure_total`
  - `dash_ingest_claims_total`
- monitor `/metrics` for:
  - `dash_retrieve_latency_ms_p50`, `dash_retrieve_latency_ms_p95`, `dash_retrieve_latency_ms_p99`
  - `dash_ingest_to_visible_lag_ms_p50`, `dash_ingest_to_visible_lag_ms_p95` (estimated from claim event-time where present)
  - `dash_transport_auth_success_total`, `dash_transport_auth_failure_total`, `dash_transport_authz_denied_total`
  - `dash_transport_audit_events_total`, `dash_transport_audit_write_error_total`
  - `dash_retrieve_placement_route_reject_total`, `dash_retrieve_placement_last_epoch`
  - `dash_retrieve_placement_reload_enabled`, `dash_retrieve_placement_reload_attempt_total`, `dash_retrieve_placement_reload_failure_total`
- keep benchmark history guard active in CI before production promotion
- run benchmark trend automation for release candidates:
  - `scripts/benchmark_trend.sh --run-tag release-candidate`
- run placement failover drill before release candidates:
  - `scripts/failover_drill.sh --mode both --placement-reload-interval-ms 200 --keep-artifacts true`

### 7.1 API key rotation and revocation

- rotation overlap (old + new key accepted during rollout):
  - set both keys in `DASH_INGEST_API_KEYS` / `DASH_RETRIEVAL_API_KEYS` (comma-separated)
  - keep scoped mappings in `DASH_*_API_KEY_SCOPES` for tenant-level allow rules
- revocation:
  - add compromised key to `DASH_INGEST_REVOKED_API_KEYS` and `DASH_RETRIEVAL_REVOKED_API_KEYS`
  - revoked keys are denied even if present in `DASH_*_API_KEYS` or `DASH_*_API_KEY_SCOPES`
- completion:
  - remove old key from `DASH_*_API_KEYS` once clients finish migration

## 8. Incident Response (Minimal)

If retrieval fails at startup:

1. Check WAL file permissions and disk space.
2. Validate WAL readability by running ingestion locally against same path.
3. Restore from last known-good snapshot/WAL backup if corruption is detected.
4. Re-run benchmark smoke before re-enabling traffic.

## 9. Deployment Assets

- systemd units:
  - `deploy/systemd/dash-ingestion.service`
  - `deploy/systemd/dash-retrieval.service`
- env templates:
  - `deploy/systemd/ingestion.env.example`
  - `deploy/systemd/retrieval.env.example`
- container assets:
  - `deploy/container/Dockerfile`
  - `deploy/container/docker-compose.yml`
- deployment automation scripts:
  - `scripts/deploy_systemd.sh` (plan/apply for systemd unit installation)
  - `scripts/deploy_container.sh` (build/up/down/ps/logs wrappers for compose)

## 10. Deployment Automation Examples

systemd plan (non-destructive):

```bash
scripts/deploy_systemd.sh --mode plan --service all
```

systemd apply:

```bash
sudo scripts/deploy_systemd.sh --mode apply --service all
```

container workflow:

```bash
scripts/deploy_container.sh build
scripts/deploy_container.sh up
scripts/deploy_container.sh ps
```
