# Backup

DASH's recoverable state is the **WAL** (plus its `.snapshot` file after a checkpoint), the optional **segment directory**, and the **placement file** when placement routing is used. The `redb` file is a mirror that the service can run without, and is not included in the backup bundle. This page describes the scripts that exist and how to use them. It replaces an earlier version that referenced `dash-admin`, `redb-checksum`, `dash-wal-replay`, a `tests/backup/` suite and RPO/RTO figures; none of those exist and no recovery-time numbers have been measured.

## Create a bundle

```bash
scripts/backup_state_bundle.sh \
  --wal-path /var/lib/dash/wal/ingestion.wal \
  --segment-dir /var/lib/dash/segments/ingestion \
  --output-dir /var/backups/dash
```

Options (from `--help`): `--wal-path` (default `DASH_INGEST_WAL_PATH` or `DASH_RETRIEVAL_WAL_PATH`), `--segment-dir`, `--placement-file`, `--output-dir` (default `dist/backups`), `--bundle-label`, and `--s3-uri` to upload the bundle. The output is `dash-backup-<label>.tar.gz` containing the WAL, its snapshot file, the optional directories, and a `CHECKSUMS.sha256` file.

The script reads the files while the service may be running. To get a quiesced copy, stop ingestion (or snapshot the volume with your filesystem or cloud tooling) before running it.

## Restore

```bash
# Verify checksums only, no writes:
scripts/restore_state_bundle.sh --bundle dist/backups/dash-backup-<label>.tar.gz --verify-only true

# Restore (stop the services first):
scripts/restore_state_bundle.sh \
  --bundle dist/backups/dash-backup-<label>.tar.gz \
  --wal-path /var/lib/dash/wal/ingestion.wal \
  --segment-dir /var/lib/dash/segments/ingestion \
  --force true
```

`--bundle` also accepts an `s3://` URI. The script refuses to overwrite existing targets unless `--force true`. After restoring, start ingestion: it replays the snapshot and WAL into memory. If a stale `redb` file exists from before the restore, remove it (or leave `DASH_INGEST_PERSISTENCE_DISABLE=1` for the first start) so it cannot disagree with the restored WAL. Retrieval followers resync from ingestion by replication; reset their offset file (`DASH_RETRIEVAL_REPLICATION_OFFSET_PATH`) if the restored WAL is shorter than what they had applied.

## Drill

`scripts/backup_restore_drill.sh` runs ingest, backup, state destruction, restore and a retrieval comparison through Docker Compose. It is wired into the "Backup/Restore Drill" job in `.github/workflows/rust.yml`; check that workflow's latest run for the current result. The drill uses `DASH_INGEST_API_KEY` when it is set.

## Cadence and gaps

- There is no built-in continuous WAL archiving, point-in-time recovery tooling or cross-region replication. Archiving WAL files with a generic tool (rsync, object-storage sync) is possible but untested here.
- Backups are not encrypted by DASH. Encrypt the destination.
- Restore drills should be run on a schedule that you own; none is automated beyond the CI job above.
- State-bundle consistency while the service is writing is not guaranteed (see register DATA-10 on non-atomic multi-record writes).
