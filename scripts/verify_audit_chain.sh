#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  scripts/verify_audit_chain.sh --path <audit_log.jsonl> [--service ingestion|retrieval]

Options:
  --path       Audit JSONL file path to verify.
  --service    Optional service filter. When set, every chained record must match.
  --help       Show this help text.

Verification checks:
  - chained fields exist together: seq, prev_hash, hash
  - seq is strictly increasing by 1
  - prev_hash matches prior record hash
  - hash matches SHA-256 over canonical record payload
EOF
}

AUDIT_PATH=""
SERVICE_FILTER=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --path)
      AUDIT_PATH="${2:-}"
      shift 2
      ;;
    --service)
      SERVICE_FILTER="${2:-}"
      shift 2
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "[audit-verify] unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ -z "${AUDIT_PATH}" ]]; then
  echo "[audit-verify] --path is required" >&2
  usage >&2
  exit 1
fi

python3 - "${AUDIT_PATH}" "${SERVICE_FILTER}" <<'PY'
import hashlib
import json
import os
import sys


def fail(message: str) -> None:
    print(f"[audit-verify] {message}", file=sys.stderr)
    raise SystemExit(1)


def json_escape(raw: str) -> str:
    out = []
    for ch in raw:
        if ch == "\\":
            out.append("\\\\")
        elif ch == "\"":
            out.append("\\\"")
        elif ch == "\n":
            out.append("\\n")
        elif ch == "\r":
            out.append("\\r")
        elif ch == "\t":
            out.append("\\t")
        else:
            out.append(ch)
    return "".join(out)


def optional_json_string(value) -> str:
    if value is None:
        return "null"
    if not isinstance(value, str):
        fail("tenant_id/claim_id must be string or null in chained records")
    return f"\"{json_escape(value)}\""


def canonical_record(record: dict) -> str:
    seq = record["seq"]
    timestamp_ms = record["ts_unix_ms"]
    service = record["service"]
    action = record["action"]
    tenant_id = record.get("tenant_id")
    claim_id = record.get("claim_id")
    status = record["status"]
    outcome = record["outcome"]
    reason = record["reason"]
    prev_hash = record["prev_hash"]
    return (
        f'{{"seq":{seq},"ts_unix_ms":{timestamp_ms},"service":"{json_escape(service)}",'
        f'"action":"{json_escape(action)}","tenant_id":{optional_json_string(tenant_id)},'
        f'"claim_id":{optional_json_string(claim_id)},"status":{status},'
        f'"outcome":"{json_escape(outcome)}","reason":"{json_escape(reason)}",'
        f'"prev_hash":"{prev_hash}"}}'
    )


def parse_required_int(record: dict, key: str, lineno: int) -> int:
    value = record.get(key)
    if not isinstance(value, int):
        fail(f"line {lineno}: '{key}' must be an integer")
    return value


def parse_required_str(record: dict, key: str, lineno: int) -> str:
    value = record.get(key)
    if not isinstance(value, str):
        fail(f"line {lineno}: '{key}' must be a string")
    return value


path = sys.argv[1]
service_filter = sys.argv[2].strip()
if not os.path.exists(path):
    fail(f"audit file does not exist: {path}")

last_seq = None
last_hash = None
chained_records = 0
legacy_records = 0

with open(path, "r", encoding="utf-8") as handle:
    for lineno, raw_line in enumerate(handle, 1):
        line = raw_line.strip()
        if not line:
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError as err:
            fail(f"line {lineno}: invalid JSON ({err})")
        if not isinstance(record, dict):
            fail(f"line {lineno}: record must be a JSON object")

        chain_fields = {"seq", "prev_hash", "hash"}
        has_any_chain_field = any(field in record for field in chain_fields)
        has_all_chain_fields = all(field in record for field in chain_fields)
        if has_any_chain_field and not has_all_chain_fields:
            fail(f"line {lineno}: seq/prev_hash/hash must be present together")
        if not has_all_chain_fields:
            legacy_records += 1
            continue

        seq = parse_required_int(record, "seq", lineno)
        timestamp_ms = parse_required_int(record, "ts_unix_ms", lineno)
        if timestamp_ms < 0:
            fail(f"line {lineno}: ts_unix_ms must be non-negative")
        service = parse_required_str(record, "service", lineno)
        parse_required_str(record, "action", lineno)
        parse_required_int(record, "status", lineno)
        parse_required_str(record, "outcome", lineno)
        parse_required_str(record, "reason", lineno)
        prev_hash = parse_required_str(record, "prev_hash", lineno)
        record_hash = parse_required_str(record, "hash", lineno)

        if len(prev_hash) != 64 or any(ch not in "0123456789abcdefABCDEF" for ch in prev_hash):
            fail(f"line {lineno}: prev_hash must be 64-char hex")
        if len(record_hash) != 64 or any(ch not in "0123456789abcdefABCDEF" for ch in record_hash):
            fail(f"line {lineno}: hash must be 64-char hex")

        if service_filter and service != service_filter:
            fail(
                f"line {lineno}: service filter '{service_filter}' mismatch (found '{service}')"
            )

        if last_seq is not None:
            if seq != last_seq + 1:
                fail(f"line {lineno}: seq expected {last_seq + 1}, found {seq}")
            if prev_hash.lower() != last_hash.lower():
                fail(f"line {lineno}: prev_hash does not match previous hash")
        elif seq < 1:
            fail(f"line {lineno}: first chained seq must be >= 1")

        expected_hash = hashlib.sha256(canonical_record(record).encode("utf-8")).hexdigest()
        if record_hash.lower() != expected_hash:
            fail(f"line {lineno}: hash mismatch")

        last_seq = seq
        last_hash = record_hash
        chained_records += 1

if chained_records == 0:
    fail("no chained audit records found (seq/prev_hash/hash missing)")

print(
    f"[audit-verify] ok: path={path} chained_records={chained_records} "
    f"legacy_records={legacy_records} last_seq={last_seq} last_hash={last_hash}"
)
PY
