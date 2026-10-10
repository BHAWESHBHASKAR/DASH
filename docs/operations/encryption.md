# Encryption at rest

DASH can encrypt the tenant data it writes to disk with envelope encryption:
every file gets its own random 256-bit data key (DEK), the DEK is stored in
the file wrapped by a key-encryption key (KEK) that you provide, and records
are sealed with AES-256-GCM. Encryption is **off by default**. The design,
file formats and threat analysis are in
[ADR 0005](../adr/0005-encryption-at-rest.md).

Implementation: `pkg/encryption` (KEK providers, file headers, the line and
sealed formats), `pkg/store/src/crypt.rs` (store files, start-up check),
`pkg/store/src/disk.rs` (redb values), `services/indexer/src/lib.rs`
(segments), `tools/wal-inspect` (`keys`, `rewrap`).

## What is encrypted

| File | How |
|---|---|
| WAL (`DASH_*_WAL_PATH`), `<wal>.snapshot`, `<wal>.closed.<gen>` | one encrypted line per record (format A); torn-tail and damaged-line rules unchanged |
| `<wal>.quarantine`, `<wal>.truncated-*` sidecars | format A (a sidecar starts with the file's header line, so it decrypts on its own) |
| Leader exports `<wal>.exports/<id>.export` and their staging files | sealed in 64 KiB chunks (format B) |
| Follower download `<wal>.resync.part` | format A |
| Persisted vector index (`<wal>.vindex` or `DASH_*_VECTOR_INDEX_PATH`) | format B |
| Segment files, `segments.manifest`, `segments.tenant` | format B |
| redb mirror (`DASH_*_PERSISTENCE_PATH`) | every value sealed, bound to its table and key |

Not encrypted, because they hold no tenant payload or must stay readable
without the key:

* **audit logs** (`DASH_*_AUDIT_LOG_PATH`): hash-chained JSON lines with
  request metadata (action, tenant and claim ids, status, actor fingerprint);
  never claim text, evidence or vectors. They stay verifiable by
  `audit-verify` and shippable to a SIEM. Protect them with file permissions
  and off-host shipping (`docs/operations/audit-chain.md`).
* **identifiers used as keys**: redb keys (tenant, claim, evidence and commit
  ids) and segment directory names (derived from the tenant id).
* `<wal>.gen`, `<wal>.gen.transitions`, export manifests (counts and the
  SHA-256 of the plaintext export), replication offset files,
  `segments.fingerprint`, control-plane state and lease files.
* file sizes, timestamps and write patterns.

**Replication on the wire stays plaintext**: frames and export chunks are the
same bytes with or without encryption. Protect them with TLS
([tls.md](tls.md)). Each node encrypts what it stores with **its own** key, so
a follower does not need the leader's key and keys can differ and rotate per
node.

## Setup

### 1. Generate a key

```bash
openssl rand -hex 32 > dash.key      # 64 hex characters = 32 bytes
chmod 0400 dash.key
```

A key file holds 64 hex characters (surrounding whitespace allowed) or exactly
32 raw bytes. An all-zero key is refused. The key's id is derived from the
key (`local-` plus 16 hex digits of a SHA-256 over a label and the key); it is
written into every file header, never the key itself.

The service checks the file at start-up and refuses to start when it is not a
regular file, is accessible to other users or is writable by its group (mode
`0600` or `0400`; group read is tolerated because Kubernetes adds it to Secret
volumes when `fsGroup` is set).

### 2. Configure

| Setting | Meaning |
|---|---|
| `DASH_ENCRYPTION_KEY_FILE` | Path of the active key. Setting it turns encryption on. |
| `DASH_ENCRYPTION_PREVIOUS_KEY_FILES` | Comma-separated retired keys that still decrypt (rotation). Requires the active key. |

Both are read by ingestion, retrieval, `segment-maintenance-daemon` and
`wal-inspect`. Give every process that touches a node's data directory the
same settings.

* **Helm**: create a Secret and set `encryption.enabled=true`,
  `encryption.secretName=<secret>` (`encryption.keyName`, default
  `active.key`; `encryption.previousKeyNames` for a rotation). The Secret is
  mounted read-only at `/etc/dash/encryption` (mode `0400`).

  ```bash
  kubectl -n dash-system create secret generic dash-encryption-key --from-file=active.key=dash.key
  helm upgrade --install dash ./deploy/helm/dash -n dash-system \
    --set encryption.enabled=true --set encryption.secretName=dash-encryption-key ...
  ```

* **Compose**: the overlay `deploy/container/docker-compose.encryption.yml`
  mounts `deploy/container/encryption` (git-ignored) read-only; the key must
  be owned by the image user (UID 10001) with mode `0400`.
* **systemd**: keep the key at `/etc/dash/encryption/active.key` (root,
  `0400`), uncomment `LoadCredential=dash-kek:...` in the unit and
  `DASH_ENCRYPTION_KEY_FILE=/run/credentials/<unit>/dash-kek` in the env
  file (systemd 247 or later).

### 3. Check

At start-up each service logs one line:

```text
ingestion encryption at rest: on (provider local, active key id local-3f9c..., 1 key id(s) configured)
```

List the key id of every file (no key needed):

```bash
wal-inspect keys /var/lib/dash/ingestion
```

Files show as `encrypted lines`, `sealed`, `redb (encrypted values)` or
`plaintext`.

## Fail closed

* A service started **without** `DASH_ENCRYPTION_KEY_FILE` that finds an
  encrypted file (WAL family, exports, vector index, segments, redb data key)
  exits with status 2 before opening anything:
  `startup refused: encryption at rest: <file> is encrypted (key id ...) but no encryption key is configured`.
* A file encrypted under a key id that is not configured fails the same way
  and names the missing key id (`add that key to DASH_ENCRYPTION_PREVIOUS_KEY_FILES`).
* A misconfigured key file (missing, wrong size, too permissive) exits with
  status 2 and names the problem.

## Turning encryption on for existing data

Plaintext files stay readable with a key configured; they are rewritten
encrypted as follows:

| File | Encrypted |
|---|---|
| live WAL, quarantine file | on the first start with the key (rewritten in place, atomically) |
| snapshot, closed generation | by the next checkpoint (the closed file one checkpoint later) |
| vector index | by the next save (periodic, after a checkpoint, at shutdown) |
| segment files | by the next publish; turning encryption on (or rotating) republishes each tenant on its next write |
| redb values | each value when it is next written; all of them on a full mirror rebuild |
| followers | on the follower's next resync, or as above for its own files |

To finish quickly, lower `DASH_CHECKPOINT_MAX_WAL_RECORDS` for one write (or
let the byte threshold trigger), then confirm with `wal-inspect keys`.

The old plaintext bytes are not overwritten in place: the rewritten files are
new files renamed over the old ones, and redb (copy-on-write) may keep freed
pages with old plaintext values until it reuses them. For a clean result
delete the redb file after the first start with encryption on and let the
service rebuild it from the WAL (it is a materialized view), and treat the
underlying volume and older backups as holding plaintext until they are
retired. Enabling encryption on a fresh node avoids all of this.

There is no supported path from encrypted back to plaintext.

## Key rotation

1. Generate a new key. Configure it as `DASH_ENCRYPTION_KEY_FILE` and move
   the old key to `DASH_ENCRYPTION_PREVIOUS_KEY_FILES`. Restart.
   New files use the new key; the redb data key is rewrapped on open; files
   under the old key stay readable.
2. Move the remaining files to the new key, either online (two checkpoints
   rewrite WAL, snapshot and closed generation; the next vector index save and
   segment publishes do the rest) or offline with the node stopped:

   ```bash
   DASH_ENCRYPTION_KEY_FILE=/path/new.key \
   DASH_ENCRYPTION_PREVIOUS_KEY_FILES=/path/old.key \
     wal-inspect rewrap /var/lib/dash/ingestion
   ```

   `rewrap` rewrites only file headers (the DEK wrapped by the new KEK); the
   data is not re-encrypted, so it is fast. Each file is replaced atomically.
3. Run `wal-inspect keys <dir>` on every node; when no file shows the old key
   id, remove the old key from the configuration and restart.

Rotating the KEK does not change the data keys. To replace data keys (for
example after a suspected DEK exposure) let checkpoints and saves rewrite the
files: every new file gets a new DEK.

## Backups

* Backups of the data directories (`scripts/backup_state_bundle.sh`, volume
  snapshots, `scripts/k8s_backup_restore.sh`) contain **ciphertext**. They are
  useless without the key that was active, or listed as previous, when each
  file was written.
* **Back up the keys separately** (a secrets manager or offline media), never
  next to the data, and keep every retired key as long as any backup written
  under it is retained.
* A restore needs the same key settings as the node that wrote the backup;
  `wal-inspect keys` on the restored directory lists the key ids it needs.

## Recovery

| Symptom | Cause and action |
|---|---|
| `no encryption key is configured` at start-up | The data is encrypted; set `DASH_ENCRYPTION_KEY_FILE`. |
| `... encrypted with key id X, which is not configured` | Add the key with id X (see `wal-inspect keys`) to `DASH_ENCRYPTION_PREVIOUS_KEY_FILES`. |
| `wal line N: authentication failed` | That record was modified or damaged on disk (or the file is spliced from another one). Same handling as a damaged plaintext line: `wal-inspect verify`, then `wal-inspect repair --quarantine` (with the key settings) or restore. |
| `discarding torn tail` warning | A crash during an append; the incomplete encrypted line is cut off and saved to a sidecar, exactly as without encryption. |
| `incomplete encryption header` | A crash while a new WAL file was being created; the file is treated as empty. |
| A lost key | Data written under it cannot be recovered. Restore from a backup whose key you still have, or rebuild the node from a replica (followers encrypt with their own keys). |

`wal-inspect inspect|verify|repair` read encrypted files with the same
settings; `repair --quarantine` writes the quarantine file encrypted.

## Tenant deletion and crypto-shredding

Data keys are per file, not per tenant: one WAL, snapshot and redb file hold
every tenant, so deleting a key cannot erase one tenant. A tenant or claim
delete keeps the guarantees in [data-deletion.md](data-deletion.md): the data
leaves the live state at once, the snapshot at the next checkpoint and the
closed generation one checkpoint later, and backups when they expire.
Destroying the KEK (and every copy) does shred a whole node, including its
backups. Per-tenant data keys need per-tenant files (master plan ADR-06) and
are not implemented.

## Performance

Release builds, 4 vCPUs on a shared VM (other jobs were running).

| Measurement | Encryption off | Encryption on |
|---|---|---|
| Ingest throughput, `loadgen --concurrency 16 --ingest-percent 100 --dim 384 --preload 2000 --id-space 2000 --seed 7`, 30 s, two runs each | 1007.7, 1043.1 ingests/s (p50 13.3 to 13.6 ms, p95 22.9 to 23.2 ms) | 971.9, 957.9 ingests/s (p50 14.4 to 14.6 ms, p95 26.3 to 26.9 ms) |
| WAL size for 50k claims with 384-d vectors | 215.4 MB | 293.0 MB (+36%: base64 plus 40 bytes per record) |
| Cold start, `cold_start 50000 384 1000 3`: replay floor | 2.47 s | 2.92 s |
| Cold start: load with saved vector index | 2.34, 2.36, 2.28 s | 2.84, 2.86, 2.69 s |
| Cold start: full HNSW rebuild | 6.40, 6.19, 6.06 s | 7.02, 6.76, 7.07 s |
| Vector index save (28.2 MB) | 0.22 s | 0.42 s |
| Crash recovery (`crash-test`, 100 kill cycles, restart to ready) | p50 177 ms, p95 243 ms | p50 213 ms, p95 251 ms |

The WAL is decrypted once at open, on all cores, and the result feeds the
first replay. On x86-64 AES-GCM uses the AES-NI and carry-less multiply
instructions when the CPU has them (detected at run time).

## Cloud KMS

The KEK provider is an interface (`encryption::KekProvider`: `wrap`, `unwrap`,
`active_key_id`, `key_ids`); a wrap happens once per file created and an
unwrap once per file opened, never per record. Only the local key-file
provider ships. ADR 0005 section 6 describes how AWS KMS, GCP KMS, Azure Key
Vault or Vault Transit map onto it.

## Limitations

* Keys live in process memory while the service runs; they are cleared on
  drop (`zeroize`) on a best-effort basis. An attacker who can read process
  memory or the key file defeats the encryption.
* Dropping whole records from the end of a WAL is indistinguishable from a
  torn tail (as without encryption); reordering whole lines inside one WAL
  file is not detected by the encryption layer.
* Identifiers, file names, sizes and timing are visible (see above).
* No per-tenant keys and no crypto-shredding of a single tenant.
* No KMS provider ships yet.
