# DASH Tier-C Recovery Drill Artifact

- run_id: phase4-tierc-quick-rehearsal
- generated_utc: 2026-02-20T20:45:02Z
- max_rto_seconds: 120
- measured_rto_seconds: 0
- measured_rpo_claim_gap: 0
- artifacts_dir: docs/benchmarks/history/recovery/phase4-tierc-quick-rehearsal-recovery-workdir
- work_dir: docs/benchmarks/history/recovery/phase4-tierc-quick-rehearsal-recovery-workdir
- result: PASS

## Command

`scripts/recovery_drill.sh --work-dir docs/benchmarks/history/recovery/phase4-tierc-quick-rehearsal-recovery-workdir --max-rto-seconds 120 --keep-artifacts true`

## Command Output

```text
[drill] generating source state via ingestion bootstrap
[drill] creating backup bundle
[backup] bundle: docs/benchmarks/history/recovery/phase4-tierc-quick-rehearsal-recovery-workdir/backup/dash-backup-drill.tar.gz
[backup] wal: docs/benchmarks/history/recovery/phase4-tierc-quick-rehearsal-recovery-workdir/source/claims.wal
[backup] snapshot: docs/benchmarks/history/recovery/phase4-tierc-quick-rehearsal-recovery-workdir/source/claims.wal.snapshot
[backup] segments: docs/benchmarks/history/recovery/phase4-tierc-quick-rehearsal-recovery-workdir/source/segments
[drill] restoring backup bundle
data/segments/tenant-a/.marker: OK
data/wal/claims.wal: OK
data/wal/claims.wal.snapshot: OK
[restore] wal restored: docs/benchmarks/history/recovery/phase4-tierc-quick-rehearsal-recovery-workdir/restore/claims.wal
[restore] snapshot restored: docs/benchmarks/history/recovery/phase4-tierc-quick-rehearsal-recovery-workdir/restore/claims.wal.snapshot
[restore] segments restored: docs/benchmarks/history/recovery/phase4-tierc-quick-rehearsal-recovery-workdir/restore/segments
[restore] completed from bundle: docs/benchmarks/history/recovery/phase4-tierc-quick-rehearsal-recovery-workdir/backup/dash-backup-drill.tar.gz
[drill] verifying retrieval replay from restored WAL
[drill] source_claims=1 restored_claims=1
[drill] rpo_claim_gap=0
[drill] rto_seconds=0
[drill] artifacts_dir=docs/benchmarks/history/recovery/phase4-tierc-quick-rehearsal-recovery-workdir
[drill] success
```
