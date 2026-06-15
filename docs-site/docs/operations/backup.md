# Backup

DASH state lives in two places: the `redb` snapshot files and the WAL. A complete backup procedure copies both, verifies the copies, and ships them off-host. This page describes the recommended procedure for each layer, the cross-region replication pattern, and the restore procedure.

## redb snapshots

The `redb` file is a single, self-contained, page-aligned file. A filesystem-level snapshot is the recommended backup primitive.

### Procedure

1. **Trigger a checkpoint** on the live service. The service exposes a checkpoint on `SIGHUP`; the `dash` CLI exposes it as `dash-admin checkpoint` (a thin wrapper over `kill -HUP $(pidof dash-ingestion)`).

2. **Copy the file** with a method that guarantees a consistent read:
   - On a filesystem that supports it (LVM, ZFS, EBS), take a snapshot first and copy from the snapshot.
   - On a plain filesystem, stop the service, copy, and start it back up. The downtime is sub-second because the service restart is bounded by the WAL replay time.

3. **Verify the copy** with the bundled `redb-checksum` tool:

   ```bash
   redb-checksum /var/backups/dash/ingest.redb
   # [OK] redb file verifies, 17842 keys across 4 tables
   ```

4. **Ship off-host** to object storage, a second disk, or a second region.

### Script

```bash
#!/usr/bin/env bash
# /usr/local/bin/dash-backup.sh — run from cron or systemd timer.
set -euo pipefail

DATA=/var/lib/dash
BACKUP=/var/backups/dash/$(date -u +%Y%m%dT%H%M%SZ)
S3=s3://my-bucket/dash-backups
mkdir -p "$BACKUP"

# 1. Checkpoint
systemctl reload dash-ingestion
systemctl reload dash-retrieval
sleep 1

# 2. Snapshot (LVM example; adapt to your filesystem)
lvcreate -L 10G -s -n dash-snap /dev/vg0/dash
mount -o ro /dev/vg0/dash-snap /mnt/snap

# 3. Copy
install -m 0644 /mnt/snap/ingest.redb    "$BACKUP/ingest.redb"
install -m 0644 /mnt/snap/retrieval.redb "$BACKUP/retrieval.redb"

# 4. Verify
redb-checksum "$BACKUP/ingest.redb"
redb-checksum "$BACKUP/retrieval.redb"

# 5. Cleanup snapshot
umount /mnt/snap
lvremove -f /dev/vg0/dash-snap

# 6. Ship off-host
aws s3 cp --recursive "$BACKUP" "$S3/$(basename "$BACKUP")/"

# 7. Local retention
find /var/backups/dash -maxdepth 1 -type d -mtime +14 -exec rm -rf {} +
```

## WAL archiving

The WAL is the **first** thing written on every mutation. A backup that misses the WAL head has a gap between the snapshot and the most recent state. The recommended pattern is to **archive the WAL continuously** with a sidecar.

```bash
#!/usr/bin/env bash
# Tail the WAL and ship every closed segment to object storage.
# Run as a systemd service or a Kubernetes sidecar.
set -euo pipefail

WAL=/var/lib/dash/ingest.wal
ARCHIVE=s3://my-bucket/dash-wal
LAST_SEG=0

while true; do
    CUR_SEG=$(stat -c %Y "$WAL")
    if [ "$CUR_SEG" != "$LAST_SEG" ]; then
        SEG_NAME="ingest-$(date -u +%Y%m%dT%H%M%SZ).wal"
        aws s3 cp "$WAL" "$ARCHIVE/$SEG_NAME" --no-progress
        LAST_SEG="$CUR_SEG"
    fi
    sleep 5
done
```

The archive is a series of immutable segments. On restore, the segments are replayed in order after the redb snapshot is loaded.

## Cross-region replication

For cross-region durability, the recommended pattern is **continuous WAL archive + periodic snapshot replication**:

1. Ship every WAL segment to a second region as it is rotated (above script, with `DEST_REGION=us-west-2`).
2. Once an hour, copy the most recent `redb` snapshot to the second region (`aws s3 cp --recursive ... --region us-west-2`).
3. The recovery-point objective (RPO) is the WAL rotation interval (default 5 minutes).
4. The recovery-time objective (RTO) is `redb load time + WAL replay time`. For 1 M claims, that is ~10 seconds. For 100 M claims, ~2 minutes.

A second region can be promoted to read-write by running a new ingestion replica against the replicated `redb` file; the WAL tail is paused on the original region, and the new region takes over as the leader.

## Restore procedure

To restore from a backup:

1. **Stop** both services.
2. **Copy** the `redb` files from the backup to the data directory.
3. **Replay** the WAL segments in order:

   ```bash
   ls -1 /var/backups/dash/wal/ingest-*.wal | sort | \
     xargs -I {} dash-wal-replay /var/lib/dash/ingest.redb {}
   ```

   The `dash-wal-replay` tool (in `tools/`) is idempotent — it skips records whose `(tenant_id, idempotency_key)` is already in the redb file.

4. **Start** the services. The services will pick up the restored state on boot.

### Test the backup

A backup that has never been restored is a backup that will fail when it matters. The recommended cadence:

- **Weekly**: restore the latest backup to a staging cluster, run the test suite, and tear down.
- **Monthly**: full DR drill — take a fresh backup, restore it to a clean cluster in a second region, fail over the load balancer, run the production read traffic for an hour, and fail back.

Automate both in CI. The `tests/backup/` directory has a script that orchestrates a restore against a kind cluster.
