# ADR 0004: Per-tenant write partitioning of the ingestion runtime

Status: Deferred. Date: 2026-10-10. Phase: P2. Relates to ADR-06 (tenant = physical partition) of
the [master plan](../plans/2026-10-09-production-readiness-master-plan.md) and to the WAL group
commit described in [ADR 0003](0003-storage-engine-indexes.md).

## 1. Question

Every write to the ingestion service takes one process-wide lock,
`Arc<Mutex<IngestionRuntime>>` (`services/ingestion/src/transport.rs`, `SharedRuntime`). Can it be
replaced by per-tenant (or tenant-hash striped) locks, so writes for different tenants run in
parallel, without breaking the WAL's total order and without replay producing a different state
than the live process had?

This record gives the design that would do it and explains why it is not implemented now.

## 2. What the lock protects today

The runtime lock guards, in one struct:

| State | Scope | Notes |
|---|---|---|
| `InMemoryStore` (`pkg/store/src/lib.rs`) | global | `claims`, `evidence_by_claim`, `edges_by_claim`, `edges_in`, `claim_vectors` and `batch_commits` are single maps keyed by claim id or commit id, not by tenant. Only the secondary indexes (`tenant_claim_ids`, inverted, entity, embedding, temporal, vector index, `tenant_vector_dims`) are per tenant. |
| The redb mirror | global | One file; redb allows one write transaction at a time. |
| WAL handle (`SharedWal`) | global | One file, one record sequence, one generation. Replication offsets are positions in that sequence. |
| `GroupCommitPipeline` | global | Enqueue order is WAL order; the apply step waits for its sequence number (`services/ingestion/src/transport/group_commit.rs`). |
| Checkpoint | global | `checkpoint_and_compact` snapshots the whole store at one WAL position and starts a new generation. |
| Segment publishing, counters, placement state | per tenant / global | Cheap, but run under the lock. |

What the lock costs is already small on the hot path. Since group commit, a single `POST /v1/ingest`
holds the lock only to validate, encode and enqueue (step 1), and again to apply in sequence order
(step 3). The fsync, which dominates latency, runs with the lock released and is shared by every
request queued meanwhile. ADR 0003 measured the pipelined log at 37,800-55,000 records/s with 64
producers; the remaining serial work is in-memory map updates.

## 3. Cross-tenant invariants that a stripe lock would have to keep

1. **Claim ids are a global namespace.** `check_claim_applicable` rejects a claim whose id exists in
   another tenant (`claim_id already exists`), and `validate_ingest_bundles` looks up an edge target
   by id across all tenants to reject cross-tenant edges. Two tenants writing the same new claim id
   concurrently must still see exactly one winner, decided in WAL order.
2. **Batch commit ids are a global namespace** (`batch_commits`), used for idempotent retries.
3. **The WAL is one totally ordered log.** Replay applies it in file order; a follower applies
   frames in that order; a checkpoint is a consistent cut of the whole store at one position.
4. **Rejected writes never reach the WAL.** Validation runs before the append, so replay never
   re-decides anything: it must see every record in an order under which each record is valid.

## 4. Design (if implemented)

1. **Split the store.** `InMemoryStore` becomes `N` tenant shards (`TenantShard`: the claim, evidence,
   edge and vector maps plus the per-tenant indexes for tenants whose `hash(tenant_id) % N` selects
   it), each behind its own `Mutex`, plus a global `ClaimDirectory`
   (`RwLock<HashMap<claim_id, tenant_id>>`) and a global `CommitDirectory` for batch commit ids. The
   directories enforce invariants 1 and 2. Every read path (`retrieve*`, `claim_by_id`, replication
   export) is rewritten against the shard + directory pair.
2. **Sequencer.** A small `Mutex<Sequencer>` (separate from every shard lock) assigns the WAL
   sequence number and enqueues on the `GroupCommitter` in one critical section, so enqueue order
   stays WAL order (invariant 3). Lock order: shard(s) in ascending index, then directories, then
   sequencer, then the WAL mutex. Never the reverse.
3. **Conflict keys stay the ordering rule.** The pipeline's keys (`claim\0<id>`, edge targets,
   `tenant-dim\0<tenant>`) are global strings; a request holds them from validation to apply. A
   claim key is held across tenants, which is what makes the directory decision equal to the WAL
   order decision. The "apply in sequence order" rule is relaxed from global to per shard: a record
   may be applied before an earlier record of another shard, because no key conflicts between them
   (and the directory update for a new claim id is made at enqueue time, under the key).
4. **Writers that are not pipelined** (batch, raw, document ingest, deletes, replication apply)
   take the shard locks of every tenant they touch (a tenant delete: one shard; a batch: the set of
   its tenants in ascending order) and drain only those shards' in-flight work.
5. **Checkpoint, resync and anything that needs a consistent cut** take all shard locks in order,
   drain the whole pipeline, then snapshot. This is a stop-the-world point, as today.
6. **redb** stays one file: mirror writes are already serialized by redb. Either keep one mirror
   thread fed in WAL order, or move to one redb file per shard (which changes the on-disk layout and
   the cold-start path).
7. **Proof obligations** (tests): N threads x M tenants ingesting and deleting concurrently, including
   the same new claim id in two tenants and edges to just-deleted targets; the live state, a WAL
   replay, a follower that applied the frames, and a resync must be identical; throughput across
   tenants must scale with cores while a single tenant does not regress.

## 5. Why it is deferred

- **It is not contained.** Steps 1 and 3 touch every read and write path of `pkg/store` (the store
  is one struct with global maps), the ingestion runtime, the retrieval follower (which reads the
  same store type under an `RwLock`), redb mirroring and checkpointing. Half of it (for example,
  stripes over an unsplit store) would only move the global lock elsewhere.
- **The expected gain is small today.** The fsync, the expensive part, is already off the lock and
  shared across tenants by group commit. What remains serialized is map updates measured in
  microseconds. No measurement in `docs/benchmarks` or ADR 0003 attributes ingest latency to the
  runtime lock; the measured costs are fsync latency and index inserts.
- **Correctness risk is concentrated where it is hardest to test.** Invariants 1 and 4 turn into
  cross-shard ordering arguments, and a mistake shows up as a replay that differs from the live
  state, which is the failure DASH's citation guarantees cannot tolerate.
- **ADR-06 supersedes it.** The master plan's target is one memtable and one WAL stream per tenant
  within a shard group. That gives per-tenant parallelism together with per-tenant crypto-shredding
  and offload, and it changes the WAL layout anyway; doing the stripe design first would be thrown
  away.

## 6. When to revisit

Revisit when either holds: a benchmark on target hardware shows ingest throughput across many
tenants limited by the runtime lock (lock hold time above roughly a third of request time at
saturation with group commit on), or ADR-06's per-tenant WAL streams are scheduled. The deletes
added in P2 already follow the rule this design needs (drain, write the WAL, then apply), so they
carry over unchanged.

## 7. Consequences now

- Writes for all tenants share one lock; parallelism comes from group commit overlapping fsyncs.
- Deletes, like batch ingest, drain the group-commit pipeline before writing their tombstone
  (`services/ingestion/src/transport/delete_routes.rs`), so their outcome equals serial execution
  in WAL order.
