# DASH Release Checklist

Date: 2026-02-17  
Status: active

## 1. Pre-Release Validation

- [ ] preferred: run consolidated gate with release settings:
  - `scripts/release_candidate_gate.sh --run-tag release-candidate --run-benchmark-trend true --slo-include-recovery-drill true --run-incident-simulation-guard true --verify-ingestion-audit <ingestion-audit.jsonl> --verify-retrieval-audit <retrieval-audit.jsonl> --ingest-min-rps <floor>`
- [ ] preferred sign-off wrapper (mandatory audit verify + incident simulation):
  - `scripts/security_signoff_gate.sh --run-tag release-candidate-signoff --ingestion-audit <ingestion-audit.jsonl> --retrieval-audit <retrieval-audit.jsonl>`
- [ ] `cargo fmt --all --check`
- [ ] `cargo clippy --workspace --all-targets -- -D warnings`
- [ ] `cargo test --workspace`
- [ ] `scripts/ci.sh`
- [ ] benchmark history guard passes for smoke profile
- [ ] `scripts/benchmark_trend.sh --run-tag release-candidate` (smoke + large history append + scorecards)
- [ ] `scripts/slo_guard.sh --profile smoke --run-tag release-candidate --include-recovery-drill true`
- [ ] `scripts/verify_audit_chain.sh --path <ingestion-audit.jsonl> --service ingestion` and `scripts/verify_audit_chain.sh --path <retrieval-audit.jsonl> --service retrieval`
- [ ] optional transport concurrency benchmark: `scripts/benchmark_transport_concurrency.sh --workers-list 1,4 --clients 16 --requests-per-worker 30`
- [ ] ingestion throughput floor gate passes (included in consolidated release gate):
  - `scripts/release_candidate_gate.sh --run-ingest-throughput-guard true --ingest-min-rps <floor>`
  - keep `<floor> > 0` (release gate now rejects non-positive floors)
  - if using `--ingest-wal-background-flush-only true`, keep async flush enabled (`--ingest-wal-async-flush-interval-ms auto|<ms>`)

## 2. Build and Package

- [ ] optional: `DASH_RELEASE_RUN_BENCH_TRENDS=true scripts/package_release.sh` (runs trend automation before packaging)
- [ ] `scripts/package_release.sh`
- [ ] verify archive includes `ingestion`, `retrieval`, `segment-maintenance-daemon`, and benchmark binary
- [ ] verify release notes include benchmark metrics and quality probe status

## 3. Deployment Readiness

- [ ] production WAL path capacity and permissions verified
- [ ] `DASH_*` runtime envs configured (or explicit `EME_*` fallback plan documented)
- [ ] if JWT auth mode is enabled, `DASH_*_JWT_*` envs validated (active secret, optional rotation secret set and/or `kid` map, issuer/audience policy, exp requirement)
- [ ] health endpoint, ingest endpoint, and retrieve endpoint probes prepared
- [ ] metrics endpoint probe prepared (`/metrics`)
- [ ] rollback operator and fallback data restore path documented
- [ ] deployment assets selected and validated (`deploy/systemd/*` or `deploy/container/*`)
- [ ] segment maintenance deployment mode selected and validated:
  - managed service mode (`dash-segment-maintenance.service` / `dash-segment-maintenance` container)
  - if managed daemon mode is enabled, ingestion `DASH_INGEST_SEGMENT_MAINTENANCE_INTERVAL_MS=0`
- [ ] backup bundle generated and archived:
  - `scripts/backup_state_bundle.sh --wal-path <wal> --segment-dir <segments> --output-dir /var/backups/dash`
- [ ] restore verification pass completed:
  - `scripts/restore_state_bundle.sh --bundle <bundle.tar.gz> --wal-path <wal> --verify-only true`
- [ ] recovery drill pass with measured RTO/RPO:
  - `scripts/recovery_drill.sh --max-rto-seconds 60`
- [ ] incident simulation gate pass (routing failover + auth revocation + recovery):
  - `scripts/incident_simulation_gate.sh --failover-mode no-restart --recovery-max-rto-seconds 60`

## 4. Post-Deploy Verification

- [ ] `/health` returns `{"status":"ok"}`
- [ ] ingestion endpoint accepts a valid payload and updates claim count
- [ ] retrieval query returns citation-bearing results
- [ ] retrieval metrics export is available and latency gauges are non-zero under load
- [ ] startup replay metrics captured in deploy log
- [ ] smoke benchmark executed in staging/prod shadow and archived
