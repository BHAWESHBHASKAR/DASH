# DASH Incident Simulation Gate

- run_id: 20260220-204502-phase4-tierc-quick-rehearsal-tierc
- started_utc: 2026-02-20T20:45:02Z
- finished_utc: 2026-02-20T20:45:06Z
- summary_status: PASS
- pass_count: 3
- fail_count: 0
- skip_count: 0

| step | status | duration_s | command |
|---|---|---:|---|
| failover drill | PASS | 2 | `scripts/failover_drill.sh --mode both --max-wait-seconds 30` |
| auth revocation drill | PASS | 1 | `scripts/auth_revocation_drill.sh --max-wait-seconds 30` |
| recovery drill | PASS | 1 | `scripts/recovery_drill.sh --max-rto-seconds 120` |

## failover drill

- status: PASS
- duration_s: 2
- command: `scripts/failover_drill.sh --mode both --max-wait-seconds 30`
- log: `docs/benchmarks/history/incidents/20260220-204502-phase4-tierc-quick-rehearsal-tierc-incident-logs/failover-drill.log`

```text
[failover-drill] artifact_dir=/var/folders/md/cj3zgrq94_d04dl1yrh1c00m0000gn/T//dash-failover-drill-CNrxV5
[failover-drill] scenario=restart
[failover-drill] scenario=restart phase=2 promote local node and restart services
[failover-drill] scenario=no-restart
[failover-drill] scenario=no-restart phase=2 promote local node without restart
[failover-drill] no-restart phase2 ingest route acceptance: observed HTTP 200
[failover-drill] no-restart phase2 retrieve route acceptance: observed HTTP 200
[failover-drill] success
[failover-drill] mode=both
[failover-drill] placement_reload_interval_ms=200
[failover-drill] placement_file=/var/folders/md/cj3zgrq94_d04dl1yrh1c00m0000gn/T//dash-failover-drill-CNrxV5/placements.csv
[failover-drill] ingestion_log=/var/folders/md/cj3zgrq94_d04dl1yrh1c00m0000gn/T//dash-failover-drill-CNrxV5/ingestion.log
[failover-drill] retrieval_log=/var/folders/md/cj3zgrq94_d04dl1yrh1c00m0000gn/T//dash-failover-drill-CNrxV5/retrieval.log
```

## auth revocation drill

- status: PASS
- duration_s: 1
- command: `scripts/auth_revocation_drill.sh --max-wait-seconds 30`
- log: `docs/benchmarks/history/incidents/20260220-204502-phase4-tierc-quick-rehearsal-tierc-incident-logs/auth-revocation-drill.log`

```text
[auth-drill] artifact_dir=/var/folders/md/cj3zgrq94_d04dl1yrh1c00m0000gn/T//dash-auth-revoke-drill-7u1OH9
[auth-drill] tenant_id=sample-tenant
[auth-drill] ingest_active_status=200
[auth-drill] ingest_revoked_status=401
[auth-drill] ingest_missing_status=401
[auth-drill] retrieve_active_status=200
[auth-drill] retrieve_revoked_status=401
[auth-drill] retrieve_missing_status=401
[auth-drill] success
```

## recovery drill

- status: PASS
- duration_s: 1
- command: `scripts/recovery_drill.sh --max-rto-seconds 120`
- log: `docs/benchmarks/history/incidents/20260220-204502-phase4-tierc-quick-rehearsal-tierc-incident-logs/recovery-drill.log`

```text
[drill] generating source state via ingestion bootstrap
[drill] creating backup bundle
[backup] bundle: /var/folders/md/cj3zgrq94_d04dl1yrh1c00m0000gn/T//dash-recovery-drill-Dll0Wz/backup/dash-backup-drill.tar.gz
[backup] wal: /var/folders/md/cj3zgrq94_d04dl1yrh1c00m0000gn/T//dash-recovery-drill-Dll0Wz/source/claims.wal
[backup] snapshot: /var/folders/md/cj3zgrq94_d04dl1yrh1c00m0000gn/T//dash-recovery-drill-Dll0Wz/source/claims.wal.snapshot
[backup] segments: /var/folders/md/cj3zgrq94_d04dl1yrh1c00m0000gn/T//dash-recovery-drill-Dll0Wz/source/segments
[drill] restoring backup bundle
data/segments/tenant-a/.marker: OK
data/wal/claims.wal: OK
data/wal/claims.wal.snapshot: OK
[restore] wal restored: /var/folders/md/cj3zgrq94_d04dl1yrh1c00m0000gn/T//dash-recovery-drill-Dll0Wz/restore/claims.wal
[restore] snapshot restored: /var/folders/md/cj3zgrq94_d04dl1yrh1c00m0000gn/T//dash-recovery-drill-Dll0Wz/restore/claims.wal.snapshot
[restore] segments restored: /var/folders/md/cj3zgrq94_d04dl1yrh1c00m0000gn/T//dash-recovery-drill-Dll0Wz/restore/segments
[restore] completed from bundle: /var/folders/md/cj3zgrq94_d04dl1yrh1c00m0000gn/T//dash-recovery-drill-Dll0Wz/backup/dash-backup-drill.tar.gz
[drill] verifying retrieval replay from restored WAL
[drill] source_claims=1 restored_claims=1
[drill] rpo_claim_gap=0
[drill] rto_seconds=0
[drill] artifacts_dir=/var/folders/md/cj3zgrq94_d04dl1yrh1c00m0000gn/T//dash-recovery-drill-Dll0Wz
[drill] success
```
