# Encryption at rest

DASH can encrypt the tenant data it writes to disk with envelope encryption:
every file gets its own random 256-bit data key (DEK), the DEK is stored in
the file wrapped by a key-encryption key (KEK), and records are sealed with
AES-256-GCM. The KEK lives in a local key file, in a HashiCorp Vault /
OpenBao Transit engine, or in AWS KMS (see [Key managers](#key-managers-vault-transit-and-aws-kms)).
Encryption is **off by default**. The design, file formats and threat
analysis are in [ADR 0005](../adr/0005-encryption-at-rest.md).

Implementation: `pkg/encryption` (KEK providers, file headers, the line and
sealed formats; `src/kms/` holds the Vault and AWS KMS providers),
`pkg/store/src/crypt.rs` (store files, start-up check),
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

This section sets up local key files (`DASH_ENCRYPTION_PROVIDER` unset or
`local`). For Vault or AWS KMS skip to
[Key managers](#key-managers-vault-transit-and-aws-kms).

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

`DASH_CHECKPOINT_MAX_WAL_BYTES` counts the bytes on disk, which grow by
about a third with encryption: with the same threshold checkpoints come
earlier.

## Fail closed

* A service started **without** `DASH_ENCRYPTION_KEY_FILE` that finds an
  encrypted file (WAL family, exports, vector index, segments, redb data key)
  exits with status 2 before opening anything:
  `startup refused: encryption at rest: <file> is encrypted (key id ...) but no encryption key is configured`.
* A file encrypted under a key id that is not configured fails the same way
  and names the missing key id (`add that key to DASH_ENCRYPTION_PREVIOUS_KEY_FILES`).
* A misconfigured key file (missing, wrong size, too permissive) exits with
  status 2 and names the problem.
* With a key manager, the service wraps and unwraps a test data key at
  start-up; an unreachable Vault or KMS, a rejected token or credentials, a
  wrong key or a missing policy exits with status 2:
  `startup refused: encryption at rest: start-up check of the vault provider ... failed: ...`.

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

## Key managers: Vault Transit and AWS KMS

`DASH_ENCRYPTION_PROVIDER` selects who holds the KEK: `local` (key files, the
default), `vault` (HashiCorp Vault or OpenBao Transit) or `aws-kms`. With a
key manager the KEK never leaves it: DASH sends each new file's data key to
the key manager's encrypt call and stores the returned ciphertext in the file
header. A wrap happens once per file created and an unwrap once per file
opened (unwrapped keys are cached in memory for the life of the process),
never per record, so the key manager is on the start-up path and on file
creation, not on the request path. Design details: ADR 0005 section 6.

Rules for every key manager:

* `DASH_ENCRYPTION_KEY_FILE` must be unset (it would be ambiguous which key
  wraps new files). Local keys still needed for old files go to
  `DASH_ENCRYPTION_PREVIOUS_KEY_FILES` (see [Migrating](#migrating-from-local-keys-to-a-key-manager)).
* Setting `DASH_ENCRYPTION_VAULT_ADDR` or `DASH_ENCRYPTION_AWS_KMS_KEY_ID`
  without `DASH_ENCRYPTION_PROVIDER` is an error, so encryption never stays
  off by accident.
* At start-up the process wraps and unwraps a test data key; any problem stops
  it with exit code 2 and a message naming the service, key and setting.
* Each request times out after `DASH_ENCRYPTION_KMS_TIMEOUT_MS` (default
  5000) and is retried up to `DASH_ENCRYPTION_KMS_MAX_RETRIES` times (default
  3) on connection errors, timeouts, 429, 5xx and KMS throttling, with
  jittered exponential backoff. Redirects are never followed. Plain `http://`
  is refused for non-loopback hosts unless
  `DASH_ENCRYPTION_KMS_ALLOW_INSECURE_HTTP=1`.
* Tokens, AWS secrets and key material never appear in logs or errors.
* Give ingestion, retrieval, `segment-maintenance-daemon` and `wal-inspect`
  the same settings: each process unwraps the files it opens.

### Vault / OpenBao Transit

```bash
vault secrets enable transit
vault write -f transit/keys/dash            # aes256-gcm96 by default
vault policy write dash-encryption - <<'EOF'
path "transit/encrypt/dash" { capabilities = ["update"] }
path "transit/decrypt/dash" { capabilities = ["update"] }
EOF
```

| Setting | Meaning |
|---|---|
| `DASH_ENCRYPTION_PROVIDER=vault` | Use Vault Transit. |
| `DASH_ENCRYPTION_VAULT_ADDR` | `https://vault.example:8200` (required). |
| `DASH_ENCRYPTION_VAULT_TOKEN_FILE` | File holding a token with the policy above (required). Must not be readable by other users. |
| `DASH_ENCRYPTION_VAULT_KEY` | Transit key name (required). |
| `DASH_ENCRYPTION_VAULT_MOUNT` | Mount path, default `transit`. |
| `DASH_ENCRYPTION_VAULT_CA_FILE` | PEM bundle of extra CAs for the Vault server. |
| `DASH_ENCRYPTION_VAULT_NAMESPACE` | Vault Enterprise / OpenBao namespace. |
| `DASH_ENCRYPTION_VAULT_PREVIOUS_KEYS` | Other Transit keys (same mount) that must still unwrap. |

File headers record the key id `vault:<mount>/<key>` (for example
`vault:transit/dash`). The token file is re-read whenever Vault answers 403,
so a Vault Agent (or any process) that renews the token by rewriting the
file needs no restart. Keep the token renewable: when the token in the file
is rejected, file creation and opening fail (open files keep working).

* **Rotation**: `vault write -f transit/keys/dash/rotate`. New files use the
  new key version; every older file stays readable (the Transit ciphertext
  names its version). Nothing changes in DASH. Do not raise
  `min_decryption_version` above the oldest version a file or backup still
  uses: a file keeps the version it was written with until a checkpoint,
  save or publish rewrites it (`wal-inspect rewrap` skips files whose key id
  is already the active one, so it does not move key versions).
* **Another key**: point `DASH_ENCRYPTION_VAULT_KEY` at the new key and list
  the old one in `DASH_ENCRYPTION_VAULT_PREVIOUS_KEYS`, then `wal-inspect
  rewrap` and remove it once `wal-inspect keys` shows no file under it.
* The file id is bound inside the encrypted payload, so the binding does not
  depend on the Transit `associated_data` support of the Vault version.

### AWS KMS

Create a symmetric encryption key (or use an existing one) and give the
role DASH runs as `kms:Encrypt` and `kms:Decrypt` on it, for example with the
condition `kms:EncryptionContextKeys` = `dash-file-id`.

| Setting | Meaning |
|---|---|
| `DASH_ENCRYPTION_PROVIDER=aws-kms` | Use AWS KMS. |
| `DASH_ENCRYPTION_AWS_KMS_KEY_ID` | Key id, key ARN, alias name (`alias/dash`) or alias ARN (required). |
| `DASH_ENCRYPTION_AWS_REGION` | Region; falls back to `AWS_REGION`, `AWS_DEFAULT_REGION`, then the region of a key ARN. |
| `DASH_ENCRYPTION_AWS_KMS_ENDPOINT` | Endpoint override (VPC endpoint, LocalStack); falls back to `AWS_ENDPOINT_URL_KMS`. |
| `DASH_ENCRYPTION_AWS_KMS_PREVIOUS_KEY_IDS` | Other keys that must still unwrap after switching keys. |

Credentials, in this order (as the AWS SDKs do):

1. `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and optionally
   `AWS_SESSION_TOKEN`.
2. A web identity: `AWS_WEB_IDENTITY_TOKEN_FILE` and `AWS_ROLE_ARN` (and
   optionally `AWS_ROLE_SESSION_NAME`), as EKS IAM Roles for Service Accounts
   sets them. Exchanged with STS `AssumeRoleWithWebIdentity`
   (`AWS_ENDPOINT_URL_STS` overrides the regional STS endpoint).
3. The container credentials endpoint: `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`
   (ECS task roles) or `AWS_CONTAINER_CREDENTIALS_FULL_URI` with
   `AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE` / `AWS_CONTAINER_AUTHORIZATION_TOKEN`
   (EKS Pod Identity).

Temporary credentials are refreshed five minutes before they expire and at
once when KMS reports them expired. **Not supported**: EC2 instance metadata
(IMDS), `~/.aws/credentials` and `~/.aws/config` profiles, SSO and
`credential_process`; on EC2 pass credentials through one of the three
sources above.

File headers record `aws-kms:<configured key>`; each wrap uses the
encryption context `{"dash-file-id": "<hex file id>"}`, which CloudTrail
logs with every call. Automatic key rotation in KMS needs nothing from DASH.
With an alias, Decrypt is sent without a key id so a re-pointed alias still
decrypts files written under the previous key (the principal needs
`kms:Decrypt` on that key too).

### Migrating from local keys to a key manager

1. Configure the key manager and move the current local key from
   `DASH_ENCRYPTION_KEY_FILE` to `DASH_ENCRYPTION_PREVIOUS_KEY_FILES`:

   ```bash
   DASH_ENCRYPTION_PROVIDER=vault
   DASH_ENCRYPTION_VAULT_ADDR=https://vault.example:8200
   DASH_ENCRYPTION_VAULT_TOKEN_FILE=/etc/dash/encryption/vault-token
   DASH_ENCRYPTION_VAULT_KEY=dash
   DASH_ENCRYPTION_PREVIOUS_KEY_FILES=/etc/dash/encryption/active.key
   ```

   Restart. New files are wrapped by the key manager, the redb data key is
   rewrapped on open, and files under the local key stay readable.
2. Move the remaining files, online (checkpoints and saves rewrite them) or
   offline with the node stopped: `wal-inspect rewrap /var/lib/dash/ingestion`
   with the same settings.
3. When `wal-inspect keys <dir>` shows no `local-...` key id on any node
   (and no backup you keep needs it), drop
   `DASH_ENCRYPTION_PREVIOUS_KEY_FILES`. Keep a copy of the local key as long
   as any backup written under it is retained.

Moving from a key manager back to local keys is not supported (a key
manager can only be the active provider).

### Deployment

* **Helm**: `encryption.provider=vault` with `encryption.vault.*`, or
  `encryption.provider=aws-kms` with `encryption.awsKms.*` (see
  `deploy/helm/dash/values.yaml`). `encryption.secretName` is optional for
  key managers; when set it is mounted at `encryption.mountPath` and can hold
  the Vault token, a CA bundle and local keys for a migration. For AWS use
  IRSA or Pod Identity on the service account (`serviceAccount.annotations`).
* **systemd / Compose**: set the variables above in the env file or the
  service environment.

### Troubleshooting

| Message | Cause and action |
|---|---|
| `... permission denied (HTTP 403) ...; the token in <file> was rejected` | Token expired, revoked or missing the policy; renew it in the file (it is re-read on the next call). |
| `... no response within N ms (gave up after M attempts)` | Vault or KMS unreachable or slow; check network policy and DNS, or raise `DASH_ENCRYPTION_KMS_TIMEOUT_MS`. |
| `refusing plain http to non-loopback host` | Use `https://` (or, on a trusted network only, `DASH_ENCRYPTION_KMS_ALLOW_INSECURE_HTTP=1`). |
| `AWS KMS ... AccessDeniedException` | The principal lacks `kms:Encrypt` / `kms:Decrypt` on the key, or a key policy condition does not match. |
| `AWS KMS ... UnrecognizedClientException` / `InvalidSignatureException` | Wrong or stale credentials (the message names the credential source) or a clock skew of more than five minutes. |
| `data key (... refused the ciphertext ...): authentication failed` | The header was modified, moved to another file, or encrypted under a different key. |
| `no AWS credentials found` | None of the supported credential sources is configured. |

## Limitations

* Keys live in process memory while the service runs; they are cleared on
  drop (`zeroize`) on a best-effort basis. An attacker who can read process
  memory or the key file defeats the encryption.
* Dropping whole records from the end of a WAL is indistinguishable from a
  torn tail (as without encryption); reordering whole lines inside one WAL
  file is not detected by the encryption layer.
* Identifiers, file names, sizes and timing are visible (see above).
* No per-tenant keys and no crypto-shredding of a single tenant.
* Key managers: Vault / OpenBao Transit and AWS KMS only (no GCP KMS or Azure
  Key Vault). They are tested against in-process mock servers that implement
  the Transit and KMS endpoints (the SigV4 signer against AWS's published
  test vectors), not against live services in CI. AWS credentials from
  instance metadata or `~/.aws` files are not read.
