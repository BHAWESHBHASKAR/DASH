# DASH Release Checklist

Date: 2026-02-17  
Status: active

## 1. Pre-Release Validation

- [ ] `cargo fmt --all --check`
- [ ] `cargo clippy --workspace --all-targets -- -D warnings`
- [ ] `cargo test --workspace`
- [ ] `scripts/ci.sh`
- [ ] benchmark history guard passes for smoke profile
- [ ] `scripts/benchmark_trend.sh --run-tag release-candidate` (smoke + large history append + scorecards)
- [ ] optional transport concurrency benchmark: `scripts/benchmark_transport_concurrency.sh --workers-list 1,4 --clients 16 --requests-per-worker 30`

## 2. Build and Package

- [ ] optional: `DASH_RELEASE_RUN_BENCH_TRENDS=true scripts/package_release.sh` (runs trend automation before packaging)
- [ ] `scripts/package_release.sh`
- [ ] verify archive includes `ingestion`, `retrieval`, and benchmark binary
- [ ] verify release notes include benchmark metrics and quality probe status

## 3. Deployment Readiness

- [ ] production WAL path capacity and permissions verified
- [ ] `DASH_*` runtime envs configured (or explicit `EME_*` fallback plan documented)
- [ ] health endpoint, ingest endpoint, and retrieve endpoint probes prepared
- [ ] metrics endpoint probe prepared (`/metrics`)
- [ ] rollback operator and fallback data restore path documented
- [ ] deployment assets selected and validated (`deploy/systemd/*` or `deploy/container/*`)
- [ ] backup bundle generated and archived:
  - `scripts/backup_state_bundle.sh --wal-path <wal> --segment-dir <segments> --output-dir /var/backups/dash`
- [ ] restore verification pass completed:
  - `scripts/restore_state_bundle.sh --bundle <bundle.tar.gz> --wal-path <wal> --verify-only true`
- [ ] recovery drill pass with measured RTO/RPO:
  - `scripts/recovery_drill.sh --max-rto-seconds 60`

## 4. Post-Deploy Verification

- [ ] `/health` returns `{"status":"ok"}`
- [ ] ingestion endpoint accepts a valid payload and updates claim count
- [ ] retrieval query returns citation-bearing results
- [ ] retrieval metrics export is available and latency gauges are non-zero under load
- [ ] startup replay metrics captured in deploy log
- [ ] smoke benchmark executed in staging/prod shadow and archived
