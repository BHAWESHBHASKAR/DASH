# Data deletion and tenant erasure

DASH deletes data through three ingestion routes (reference:
[HTTP API, Deletes](../../docs-site/docs/reference/api.md#deletes)):

| Route | Role | Removes |
|---|---|---|
| `DELETE /v1/claims/{claim_id}?tenant_id=...` | `ingest` | the claim, its vector, its evidence and every edge from or to it |
| `DELETE /v1/evidence/{evidence_id}?tenant_id=...` | `ingest` | every evidence row with that id on the tenant's claims |
| `DELETE /v1/tenants/{tenant_id}` | `admin` for that tenant | all of the tenant's claims, vectors, evidence and edges, its vector dimension and index, and batch-commit metadata naming its claims |

Every delete is idempotent and answers 200 with `"deleted": true|false`.
Send deletes to the ingestion leader; followers receive them by replication.

## What happens on a delete

1. The request is authorized (and refused with 503 when the audit log is
   required and unusable, like an ingest).
2. The ingestion runtime waits for in-flight pipelined ingests to be applied
   (group-commit drain), so the delete is ordered after every write that was
   acknowledged before it.
3. If the target exists, one checksummed tombstone record (`T2`) is appended
   to the WAL, framed as a one-record commit group, under the configured WAL
   write policy (with the default policy it is fsynced before the response).
   A delete of something absent writes nothing.
4. Only then are memory, the redb mirror and the vector index changed, and the
   tenant's segment manifest is republished without the deleted claim ids.
5. An audit record (`delete_claim`, `delete_evidence`, `delete_tenant`) with
   the removed counts is appended.

What a tombstone removes is decided when it is applied, from the state at
that point of the log, so a WAL replay, a replication follower and a resync
all reach the same state as the leader. A persisted vector index saved before
the delete is corrected by the WAL catch-up at the next start (it is never
served with the deleted vectors).

## When the data is physically gone

| Copy | Gone after |
|---|---|
| In-memory state, query results | the 200 response |
| redb mirror (`DASH_*_PERSISTENCE_PATH`) | the 200 response (same write as memory; a redb failure detaches the mirror and is logged, the WAL stays authoritative) |
| Persisted vector index (`<WAL path>.vindex`) | the next save (periodic, after a checkpoint, at shutdown); until then the file may still hold the vectors, but they are removed on load |
| Segment files | the manifest stops listing the claim ids immediately; unreferenced segment files are pruned by segment maintenance after `DASH_INGEST_SEGMENT_GC_MIN_STALE_AGE_MS`. Segment files hold claim ids only, no text. |
| Closed generation (`<wal>.closed.<gen>`, kept for followers) | the checkpoint after the next one replaces it |
| Leader WAL | the next checkpoint (`DASH_CHECKPOINT_MAX_WAL_RECORDS` / `DASH_CHECKPOINT_MAX_WAL_BYTES`): the snapshot is written from the current state, which no longer holds the rows, and the WAL is truncated. Until then the original records **and** the tombstone are in the WAL. |
| Follower WALs and redb files | the follower applies the tombstone when it pulls it; its own WAL keeps the original records until the follower resyncs after a leader checkpoint (generation change) |
| `<wal>.quarantine`, `<wal>.truncated-*` sidecars | never automatically: they are operator-managed recovery files |
| Audit log | never: audit records keep tenant and claim ids (not claim text) by design |

To make an erasure final on one node without waiting for the checkpoint
policy, lower the checkpoint threshold temporarily or trigger a write that
crosses it, then confirm with `tools/wal-inspect` that no `T2` record and no
record of the erased tenant remains.

## Backups and archives

**Backups, WAL archives and disk snapshots taken before a delete still contain
the deleted data**, and restoring one brings the data back. DASH cannot reach
into them. For an erasure request:

- record the erasure (the audit log does this) and keep the list of erased
  tenant or claim ids;
- after restoring any backup older than the erasure, replay the deletes
  (they are idempotent) before serving traffic;
- expire backups according to your retention policy. With encryption at rest
  ([encryption.md](encryption.md)) backups hold ciphertext, but the data keys
  are per file, not per tenant: deleting a key cannot erase a single tenant
  (no crypto-shredding); destroying the key erases the whole node and every
  backup taken under it.

## Compatibility

Binaries older than this release do not know the `T2` record kind. They stop
on it during replay instead of skipping it (an interior unknown record fails
replay), but a binary that predates the fail-closed change may treat an
unknown record at the very end of the log as a torn write. Tombstones are
therefore never written as the last line (the commit marker follows them).
Do not downgrade below this release while the WAL holds tombstones; run a
checkpoint first, after which the WAL holds no tombstone.

## Sharded deployments

With placement routing, a claim delete is routed like an ingest of the stored
claim. Evidence and tenant deletes touch every shard of the tenant: send them
to the leader of each shard (a follower or wrong node answers with the usual
routing error).
