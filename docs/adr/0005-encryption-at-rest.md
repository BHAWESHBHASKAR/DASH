# ADR 0005: Encryption at rest

Status: Accepted (implemented). Date: 2026-10-10. Phase: P4. Implements the file-level part of
ADR-10 of the [master plan](../plans/2026-10-09-production-readiness-master-plan.md) and closes
the "library not wired" part of SEC-16 in the
[issue register](../plans/2026-10-09-issue-register.md). Operator guide:
[docs/operations/encryption.md](../operations/encryption.md). Amended 2026-10-10: Vault Transit and
AWS KMS key providers (section 6).

## 1. Question

How does DASH keep tenant data that it writes to disk unreadable to someone who obtains the
disk, a volume snapshot or a backup, without breaking the WAL's recovery rules (torn tail is
truncated, a damaged interior record is a hard error), replication, or the existing on-disk data?

## 2. Decision summary

| Topic | Decision |
|---|---|
| Cipher | AES-256-GCM (RustCrypto `aes-gcm` 0.10, already in the dependency graph and allowed by `deny.toml`; uses AES-NI and carry-less multiply on x86-64 when the CPU has them). No new third-party crate except `zeroize` (already in the graph). Subkeys use HMAC-SHA256 (`hmac`, `sha2`, already workspace dependencies). |
| Key hierarchy | Envelope encryption. Every file (WAL file, snapshot, closed generation, export, vector index, segment file, quarantine file, part file) and every redb database gets its own random 256-bit data-encryption key (DEK). The DEK is stored in the file header, wrapped (AES-256-GCM) by a key-encryption key (KEK). The header names the KEK by id. |
| KEK providers | `KekProvider` trait (`pkg/encryption`): `active_key_id`, `wrap`, `unwrap(key_id, ...)`. Three implementations ship, selected by `DASH_ENCRYPTION_PROVIDER`: `LocalKekProvider` (`local`, the default), 32-byte keys read from files (`DASH_ENCRYPTION_KEY_FILE`, plus `DASH_ENCRYPTION_PREVIOUS_KEY_FILES` for rotation) with a permission check; `VaultTransitKekProvider` (`vault`, HashiCorp Vault / OpenBao Transit) and `AwsKmsKekProvider` (`aws-kms`), which call the key manager's encrypt/decrypt API (section 6) over the workspace's existing HTTP client, without an SDK. |
| Nonces | Never repeat under a key. Whole files written once (format B) use the file's fresh DEK with a chunk counter nonce. Appended records (format A, redb values) use a per-write-session subkey `HMAC-SHA256(DEK, "dash-record-subkey-v1" ‖ salt)` with a fresh random 128-bit salt per session and a strictly increasing in-memory 64-bit counter as nonce; the salt is stored with every record. A counter is never reset under a subkey (a rollback truncates the file, the counter keeps increasing). |
| Line framing (format A) | WAL, snapshot, closed generation, quarantine file and follower `.resync.part` stay line files. Line 1 is `~DASHENC1 <base64 header>`; every other line is `~E1 <base64(salt ‖ counter ‖ ciphertext ‖ tag)>`, one encrypted line per plaintext line. The plaintext line still carries its CRC. A torn final line fails to decode and is truncated exactly like a torn plaintext line; an interior line that fails authentication is a hard error naming the line. Byte offsets of physical lines keep their meaning, so the replication index works unchanged. |
| Whole-file framing (format B) | Vector index, replication export, export staging file, segment files, segment manifest and tenant marker: `DASHSEAL` magic, header, then fixed 64 KiB plaintext chunks, each sealed with nonce `chunk_index ‖ last_flag`. Truncation, reordering or appending chunks fails authentication. Random access by plaintext offset is possible (the leader serves export chunks by offset). |
| redb mirror | Each value is sealed (`DASHe1` value header + record) with AAD = table name ‖ key, so a value cannot be moved to another key. The database's wrapped DEK is kept in table `dash_crypto`. Keys (claim ids, tenant ids) stay plaintext because redb looks them up and range-scans them. |
| AAD | Every record and chunk authenticates the file's random 128-bit file id and a format label; wrapped DEKs authenticate the file id and the KEK id. The KEK id and wrapped DEK are not in the record AAD, so rewrapping replaces only the header. |
| Mixed mode | With a key configured, existing plaintext files are read. A plaintext WAL and quarantine file are rewritten encrypted when opened; the snapshot, vector index, segments and redb values are rewritten encrypted by the next checkpoint / save / publish (redb: `checkpoint_from`). With no key configured, any encrypted file makes the service refuse to start with an error naming the file and its KEK id (fail closed). |
| Rotation | Key managers rotate inside Vault / KMS without any change here (the ciphertext names the key version). Local keys: make the new key active and keep the old one in `DASH_ENCRYPTION_PREVIOUS_KEY_FILES`. New files use the new KEK; old files stay readable; the redb DEK is rewrapped on open. `wal-inspect rewrap <dir>` rewraps every remaining file header offline (only headers change, the data is not re-encrypted). `wal-inspect keys <dir>` lists the KEK id of every file, so the old key can be removed once nothing uses it. |
| Replication | Frames and exports on the wire stay plaintext and are protected by TLS (`docs/operations/tls.md`); every node encrypts what it stores with its own key. A follower therefore does not need the leader's key, and keys can be rotated per node. |
| Audit log | Kept plaintext, hash-chained (SEC-17 adds an HMAC chain). It holds request metadata (action, tenant and claim ids, status, actor fingerprint, client IP), never claim text, evidence or vectors, and it must stay verifiable by `audit-verify` and shippable to a SIEM without the data key. |
| Crypto-shredding | Not implemented. One WAL, snapshot and redb file hold all tenants, so a per-tenant DEK would need per-tenant files (ADR-06, tenant = partition). A tenant delete writes a tombstone; the data leaves the snapshot at the next checkpoint and the closed generation one checkpoint later; backups keep it until they expire. Destroying the KEK shreds the whole node, including its backups. |
| Off by default | Encryption is on exactly when `DASH_ENCRYPTION_KEY_FILE` is set or `DASH_ENCRYPTION_PROVIDER` names a key manager (`vault`, `aws-kms`). |

## 3. Formats

### 3.1 File header (both formats)

```text
u8  version (1)
u8  key id length, then the KEK id (ASCII [A-Za-z0-9._:/-], 1..=128 bytes)
16  file id (random)
u16 wrapped DEK length (LE), then the wrapped DEK
```

Local provider wrapping: `nonce(12) ‖ AES-256-GCM(KEK, nonce, DEK, aad = "dash-dek-wrap-v1" ‖ key id ‖ file id)`.
Random nonces under a KEK are acceptable: one wrap per file created, far below the 2^32 limit.
The local key id is `local-` plus the first 8 bytes (hex) of `SHA-256("dash-kek-id-v1" ‖ key)`,
so a key file always maps to the same id and two keys cannot share one.

### 3.2 Format A: encrypted line files

```text
~DASHENC1 <base64 header>
~E1 <base64(salt[16] ‖ counter[8, BE] ‖ ciphertext ‖ tag[16])>
...
```

AEAD: subkey as above, nonce = `0x00000000 ‖ counter`, AAD = `"dash-line-v1" ‖ file id`.
Per line overhead is 40 bytes plus base64 expansion (about 4/3), measured in section 7.

A file holding only a torn header line (a crash while creating a new WAL) is treated as empty.
A complete header that cannot be unwrapped is an error, never "empty".

### 3.3 Format B: sealed files

```text
"DASHSEAL" ‖ u8 version (1) ‖ u32 chunk size (LE) ‖ u16 header length (LE) ‖ header
chunk 0 ‖ chunk 1 ‖ ... ‖ chunk n (last)        each chunk = ciphertext ‖ tag[16]
```

Nonce = `chunk index (u64 BE) ‖ u32 BE last flag`, AAD = `"dash-stream-v1" ‖ file id`. Every
chunk except the last holds exactly the chunk size; an empty file is one empty last chunk.

### 3.4 redb values

`"DASHe1\0\xff" ‖ salt ‖ counter ‖ ciphertext ‖ tag`, AAD = `"dash-redb-v1" ‖ file id ‖ table ‖ 0x00 ‖ key`.
The `DASH....\xff` prefix is the value-codec version marker, so a release without encryption
refuses these values instead of misreading them.

## 4. What is encrypted

| File | Format | Notes |
|---|---|---|
| `<wal>` (live WAL) | A | Torn tail and checksum rules unchanged. |
| `<wal>.snapshot`, `.snapshot.tmp` | A | |
| `<wal>.closed.<gen>` | A | Renamed live WAL; keeps its header. |
| `<wal>.quarantine` | A | Plaintext quarantine files are rewritten encrypted on load. |
| `<wal>.truncated-<ts>` sidecars | A | The header line is prepended, so the sidecar decodes on its own. |
| `<wal>.exports/<id>.export`, `<id>.wal.tmp` | B | Manifest (`<id>.manifest`) holds counts and the plaintext SHA-256 only. |
| `<wal>.resync.part` (follower download) | A | Resume offset is the plaintext length. |
| persisted vector index | B | |
| segment files, `segments.manifest`, `segments.tenant` | B | |
| redb mirror values | redb values | Keys stay plaintext. |

Not encrypted (no tenant payload): `<wal>.gen`, `<wal>.gen.transitions`, export manifests,
replication offset files, `segments.fingerprint`, control-plane state and lease files, audit logs
(section 2), redb keys (tenant ids, claim ids, evidence ids, commit ids). File sizes and write
timing are visible.

## 5. Threats addressed and not addressed

Addressed: reading tenant data from a stolen disk, volume snapshot or backup without the KEK;
undetected modification of individual records or chunks (AEAD); moving a record between files
or a redb value between keys.

Not addressed: an attacker with access to the running process or its memory (keys are in
memory; `zeroize` clears DEK and KEK copies on drop, best effort); an attacker who has both the
disk and the key file; deletion of whole trailing WAL records (indistinguishable from a torn
tail, as for plaintext) or reordering of whole WAL lines inside one file; metadata in file
names, sizes and redb keys.

## 6. Key manager providers

```rust
pub trait KekProvider: Send + Sync {
    fn name(&self) -> &'static str;
    fn active_key_id(&self) -> &str;
    fn key_ids(&self) -> Vec<String>;
    fn wrap(&self, dek: &[u8; 32], context: &[u8]) -> Result<WrappedDek, EncryptionError>;
    fn unwrap(&self, key_id: &str, wrapped: &[u8], context: &[u8])
        -> Result<Zeroizing<[u8; 32]>, EncryptionError>;
}
```

A key manager provider maps `wrap` to the key manager's encrypt call and `unwrap` to its
decrypt call; the KEK never leaves the key manager. `context` is the file id. Wrapping happens
once per file and unwrapping once per file open (a bounded in-process cache of unwrapped DEKs,
cleared on drop, avoids repeat calls for a file opened twice), so key manager latency is paid
at file creation and start-up, never per record. The headers are the same as with local keys:
only the key id and the wrapped bytes differ, so files of every provider can coexist.

Selection: `DASH_ENCRYPTION_PROVIDER=local|vault|aws-kms` (unset: `local`). Setting
`DASH_ENCRYPTION_KEY_FILE` together with a key manager is a configuration error; setting key
manager variables without the provider is one too (encryption would otherwise stay off).
`DASH_ENCRYPTION_PREVIOUS_KEY_FILES` stays valid with a key manager: those local keys only
unwrap, so a node moves from local keys to a key manager with `wal-inspect rewrap`. At start-up
a key manager provider wraps and unwraps a random key once; a wrong address, token, key or
policy stops the process (exit code 2) before any data file is opened.

**Vault / OpenBao Transit** (`vault`). `wrap` is `POST /v1/<mount>/encrypt/<key>`, `unwrap` is
`POST /v1/<mount>/decrypt/<key>`. The key id in headers is `vault:<mount>/<key>`; the stored
wrapped bytes are Transit's ciphertext string (`vault:v<N>:...`), which names the key version,
so `vault write -f <mount>/keys/<key>/rotate` keeps every older file readable without a rewrap.
The plaintext sent to Vault is `DEK ‖ SHA-256("dash-vault-wrap-v1" ‖ len(key id) ‖ key id ‖
file id)`; `unwrap` checks the suffix, and Transit authenticates its ciphertext, so a wrapped DEK
cannot be moved to another file. Transit's `associated_data` parameter is not used: older Vault
releases ignore unknown parameters, so a ciphertext written against one of them would stop
decrypting after an upgrade that honours it, and the in-payload binding works on every version
and on OpenBao. The token is read from a file (`DASH_ENCRYPTION_VAULT_TOKEN_FILE`, for example a
Vault Agent sink) and re-read when Vault answers 403, so renewed tokens are picked up without a
restart. Other Transit keys of the same mount can stay readable
(`DASH_ENCRYPTION_VAULT_PREVIOUS_KEYS`).

**AWS KMS** (`aws-kms`). `wrap` is `TrentService.Encrypt`, `unwrap` is `TrentService.Decrypt`
(JSON 1.1 protocol), with `EncryptionContext = {"dash-file-id": hex(file id)}`, so KMS refuses a
wrapped DEK under another file id and CloudTrail records the file id with every call. The key
id in headers is `aws-kms:<configured key>` (key id, key ARN, alias or alias ARN); the KMS
ciphertext blob names the key and its material version, so automatic KMS key rotation needs no
rewrap. Decrypt names the key except for aliases (an alias can be re-pointed; KMS then picks the
key from the blob). Requests are signed with SigV4, implemented in-tree (`hmac` and `sha2` are
already dependencies) and checked against the AWS SigV4 test-suite vectors, instead of adding
the AWS SDK and its async runtime. Credentials: environment keys, web identity
(`AssumeRoleWithWebIdentity`, EKS IRSA) or the container credentials endpoint (ECS task roles,
EKS Pod Identity), refreshed before expiry and when KMS reports them expired. Not supported:
EC2 instance metadata, `~/.aws` files, SSO and `credential_process`.

**Transport.** Both use the workspace's blocking `ureq` client with rustls (public web roots
plus an optional CA bundle for Vault), never follow redirects, refuse plain `http://` to a
non-loopback host unless `DASH_ENCRYPTION_KMS_ALLOW_INSECURE_HTTP=1`, time out each attempt
(`DASH_ENCRYPTION_KMS_TIMEOUT_MS`, default 5 s) and retry connection errors, timeouts, 429, 5xx
and KMS throttling up to `DASH_ENCRYPTION_KMS_MAX_RETRIES` times (default 3) with jittered
exponential backoff. Error messages name the service, operation, key and token file; tokens,
secret keys and key material are never logged or put in errors (service error bodies are cut
to a short snippet with known secrets removed). The crate does no logging of its own.

Not provided: GCP KMS and Azure Key Vault (the same trait fits them: `encrypt` with
`additionalAuthenticatedData`, `wrapKey`), and moving from a key manager back to local keys (a
key manager can only be the active provider).

## 7. Consequences

* WAL files grow by about a third (base64 of the record plus 40 bytes of salt, counter and tag,
  and a 4-byte line prefix); snapshot likewise (215 MB to 293 MB for 50k claims with 384-d
  vectors). Measured ingest throughput (about -6%) and cold start (about +20%) are in
  `docs/operations/encryption.md`.
* Downgrading to a release without encryption cannot read encrypted files. Disable encryption
  only after the data has been rewritten in plaintext (not provided; restore from a plaintext
  backup or rebuild by replication from a plaintext node).
* Backups contain ciphertext. Keys must be backed up separately; losing every copy of a KEK
  loses the data written under it.
