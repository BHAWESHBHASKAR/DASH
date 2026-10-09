# Audit chain

!!! note
    This page is a copy of [`docs/operations/audit-chain.md`](https://github.com/BHAWESHBHASKAR/DASH/blob/main/docs/operations/audit-chain.md) in the repository, which is canonical.

Both services append JSON lines to `DASH_INGEST_AUDIT_LOG_PATH` /
`DASH_RETRIEVAL_AUDIT_LOG_PATH`. The writer, the canonical encoding and the
verifier live in one place: `services/common/src/audit.rs`. The verifier is
the `audit-verify` binary (`tools/audit-verify`); `scripts/verify_audit_chain.sh`
is a thin wrapper around it. The HMAC/KMS/signed-checkpoint redesign (ADR-09
in the master plan) is a later phase (P4); the chain described here is
**unkeyed**.

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
- Actor fields: `actor.kind` is `api_key`, `jwt` or `none`, `actor.id` is the
  first 8 hex chars of SHA-256 of the presented credential (never the
  credential); `request_id` comes from `X-Request-Id`/`X-Correlation-Id`.

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
(default `0`). What is and is not guaranteed:

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

`/metrics` additionally exposes `dash_audit_records_total` and
`dash_audit_write_failures_total` (process-wide). Existing
`dash_*_audit_events_total` / `*_audit_write_error_total` are unchanged.

## Known gaps

- Unkeyed hash: anyone who can write the file can recompute the whole chain.
- Client IP is not recorded (no socket address plumbing into the handlers).
- JWT `sub` / OIDC distinction not recorded: the auth decision does not carry
  the principal; the credential fingerprint is used instead.
- The thread-local actor context is set per request; events emitted from other
  threads carry no actor.
- Audit logging is still disabled unless the path env var is set.
