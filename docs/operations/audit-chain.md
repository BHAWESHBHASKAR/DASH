# Audit chain (SEC-17 hotfix)

Both services append JSON lines to `DASH_INGEST_AUDIT_LOG_PATH` /
`DASH_RETRIEVAL_AUDIT_LOG_PATH`. The writer, the canonical encoding and the
verifier live in one place: `services/common/src/audit.rs`. The verifier is
the `audit-verify` binary (`tools/audit-verify`); `scripts/verify_audit_chain.sh`
is a thin wrapper around it. The HMAC/KMS/signed-checkpoint redesign (ADR-09)
is a later phase.

## Canonical encoding (record version 2)

`hash = SHA-256(canonical payload)`, where the payload is compact JSON with
these fields in exactly this order (strings escaped like `serde_json`,
absent values `null`):

```
{"v":2,"seq","ts_unix_ms","service","action","tenant_id","claim_id","status",
 "outcome","reason","actor":{"kind","id"}|null,"request_id","client_ip",
 "restart":{"prev_tail_seq","prev_tail_hash","why"}|null,"prev_hash"}
```

The stored line is that payload plus `"hash"` as the last key. Unknown keys
are rejected by the verifier. Records without `"v"` are legacy: retrieval
hashed the same fields in insertion order (v1 fields only, hand-rolled
escaping); ingestion hashed a `serde_json::json!` payload with alphabetically
sorted keys (which the old shell verifier could never reproduce). The verifier
accepts both legacy forms; the new writer continues a legacy chain seamlessly
(`seq` and `prev_hash` carry over).

## Writer behaviour

- Advisory exclusive file lock (`File::lock`) around tail read + append: two
  threads or processes appending to one file cannot fork the chain. The tail
  is re-read from the file on every append (no in-process cache).
- One `write` per append (plus `fdatasync` unless `DASH_*_AUDIT_FSYNC=0`;
  default on, no batching yet).
- Torn final line (crash mid-write): on the next append it is truncated with a
  warning and auditing continues. A complete record that merely lost its
  newline is kept.
- Corrupt final line (valid JSON without seq/hash such as `{}`, or invalid
  JSON that is newline-terminated): never silently restarts at genesis. The
  writer appends a `chain_restart` record (seq 1, genesis `prev_hash`, with
  `restart.prev_tail_seq/prev_tail_hash` naming the last valid record) and the
  corrupt line stays in the file as evidence. The verifier accepts such lines
  only when immediately followed by a matching `chain_restart`.
- Actor fields: `actor.kind` is `api_key`, `jwt`, `oidc` or `none`. The
  authorization policy sets the actor from the credential it actually
  evaluated (the same precedence it uses to authenticate: a JWT-shaped bearer
  first, then `x-api-key`, then any other bearer token), not from a separate
  header parse. `actor.id` is 16 hex chars of HMAC-SHA256 over the presented
  credential, keyed by `DASH_AUDIT_FINGERPRINT_KEY` (never an unsalted hash,
  never the credential). Without that variable each process uses a random key
  and logs a warning: fingerprints are then not comparable across restarts or
  replicas, so set the same key on every node that should correlate.
  `request_id` comes from `X-Request-Id`/`X-Correlation-Id`.
- Bounded fields: any request-derived string (`tenant_id`, `claim_id`,
  `reason`, ...) longer than 256 bytes is stored as a 64-byte prefix plus
  `...[len=N sha256=<16 hex>]`. Identifiers longer than 256 bytes are rejected
  with 400 before authentication, so anonymous callers cannot inflate the log.
- Denial throttle: records with status 401/403/429 (or outcome `denied`) are
  limited per audit file to `DASH_AUDIT_DENIAL_MAX_PER_SEC` (default 50, burst
  10x, `0` disables the limit). Dropped denials are counted in
  `dash_audit_denials_dropped_total` and summarized in one log line at most
  every 10 seconds. Successful and error records are never throttled.
- Permissions: the audit file is created with mode 0600, and a directory the
  writer has to create with mode 0700. Existing directories are left as is.

## Verifier

```
scripts/verify_audit_chain.sh --path audit.jsonl [--service ingestion|retrieval] \
    [--expect-last-seq N --expect-last-hash HEX]
```

Checks: hash per record, strictly consecutive `seq`, `prev_hash` linkage,
service filter, torn tail, unexplained restarts at genesis, unexplained
non-chain lines, `chain_restart` referencing the real previous tail. Unchained
lines before the first chained record are counted as legacy prefix.

Store `last_seq`/`last_hash` from each successful run out of band (object
storage, ticket, etc.) and pass them back as `--expect-last-*`: without such a
checkpoint note, truncating the tail yields a log that still verifies.

## Fail-closed and metrics

`DASH_INGEST_AUDIT_FAIL_CLOSED=1` / `DASH_RETRIEVAL_AUDIT_FAIL_CLOSED=1`
(default `0`; use `1` in production). With `0` a failed audit write is only
logged and counted and the request is still served; at startup a service with
an audit path configured and fail-closed off logs a warning saying so. What is
and is not guaranteed with `1`:

- Guaranteed: before `POST /v1/ingest*` (and `/v1/retrieve`) is processed, the
  service checks the audit log can be opened, locked and its tail recovered; if
  not, it answers 503 and nothing is committed.
- Not guaranteed: the append itself happens after the mutation. If the disk
  fills or the write fails between the gate and the append, the mutation is
  already committed without an audit record. This is logged as
  `audit FAIL_CLOSED violation` and counted in `dash_audit_write_failures_total`
  (alert on it). True write-ahead audit needs the v2 design.
- Not covered: events that are not behind the gate (auth failures on other
  routes) are only logged and counted.

`/metrics` additionally exposes `dash_audit_records_total`,
`dash_audit_write_failures_total` and `dash_audit_denials_dropped_total`
(process-wide). Existing
`dash_*_audit_events_total` / `*_audit_write_error_total` are unchanged.

## Known gaps

- Unkeyed hash: anyone who can write the file can recompute the whole chain.
- Client IP is not recorded (no socket address plumbing into the handlers).
- The JWT `sub` is not recorded; the credential fingerprint is used instead.
- The thread-local actor context is set per request; events emitted from other
  threads carry no actor.
- Audit logging is still disabled unless the path env var is set.
