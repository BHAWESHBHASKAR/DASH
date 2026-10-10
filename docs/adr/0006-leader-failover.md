# ADR 0006: Automatic failover of the ingestion (write) leader

Status: Accepted (implemented, P3 step 1). Date: 2026-10-10. Phase: P3. Relates to ADR-05 (Raft for
replication and metadata) of the [master plan](../plans/2026-10-09-production-readiness-master-plan.md),
which remains the long-term target; this record is the step that can be built and verified now.
Operator documentation: [failover.md](../operations/failover.md).

## 1. Problem

One ingestion process accepts writes (the leader). Ingestion and retrieval followers pull its WAL
(`/internal/replication/wal`). When the leader's host dies, writes stop until an operator points a
follower's configuration at nothing (making it a writer) and every other follower at it. Doing that
by hand is slow, and doing it wrong produces two writers whose WALs diverge (split brain): both
accept writes, followers apply whichever they reach, and no one can tell which state is right.

We want: losing the leader node does not stop writes for longer than a configured window, two nodes
never accept writes at the same time, and the durability of an acknowledged write is stated
precisely for each replication mode.

## 2. Options

**(a) Control-plane-driven promotion with fencing.** The control plane already runs a fenced,
file-backed lease for its own leadership and holds placement. It becomes the failure detector and
the only authority that hands out a monotonically increasing **term** (the fencing token for the
ingestion leader). Ingestion nodes heartbeat it; the leader holds a time-bounded lease derived from
each heartbeat; when the lease lapses, the control plane promotes the most up-to-date follower with
a higher term and updates placement. Terms are carried in heartbeats, replication polls and frames.

**(b) Embed `openraft` for the WAL.** The Raft log becomes the WAL (index = LSN), commit = majority
fsync, fencing = Raft term. This is the master plan's ADR-05 and the right end state, but it
replaces the WAL format, the group-commit pipeline, checkpoints and generation switches, the chunked
export, and both followers' apply paths; it needs a network transport between ingestion nodes,
membership changes, snapshot transfer and a deterministic-simulation test harness before it can be
trusted. None of that exists. A partial embedding (Raft for leader election only) would add a
second consensus component next to the control plane's lease without making the WAL quorum-durable.

**Decision: (a).** It reuses the WAL, replication protocol, checkpoint transitions and resync that
are already tested, it can be verified deterministically (injected clocks) and end to end with the
real binaries, and it does not preclude (b): the term introduced here maps onto the Raft term later.

## 3. Design

### 3.1 Roles and terms

* **Term** (`u64`, starts at 1): issued only by the control plane, persisted durably
  (`DASH_CONTROL_PLANE_INGEST_STATE_PATH`, default `<DASH_CONTROL_PLANE_STATE_PATH>.ingest-failover`,
  temp file + fsync + rename + directory fsync) **before** anyone is told about it, and never reused:
  the control plane also adopts the highest term any node reports, so even a control plane that lost
  its state cannot issue a term twice to two different nodes.
* Each ingestion node with `DASH_INGEST_FAILOVER_CONTROL_PLANE_URL` is a **member**. It starts in
  role `unknown` and **accepts no writes** until the control plane names it leader. It persists the
  highest term it has seen (`<wal>.failover`).

### 3.2 Failure detection: the leader lease

Every member sends `POST /v1/control-plane/ingest/heartbeat` every
`DASH_INGEST_FAILOVER_HEARTBEAT_INTERVAL_MS` (1 s) with its node id, advertised URL, a per-process
instance id, its term, role and replication position. The answer names the leader, its URL, the
term and the lease length `L` (`DASH_CONTROL_PLANE_INGEST_LEASE_MS`, 5 s).

The leader may accept a write only while `now < t_send + L`, where `t_send` is its **monotonic**
clock reading taken *before* it sent the heartbeat that the control plane answered with
"you are leader". The control plane records `t_seen` (its own monotonic clock, when it processed
that heartbeat) and does not consider the leader dead before `t_seen + L + G`
(`G` = `DASH_CONTROL_PLANE_INGEST_PROMOTION_GRACE_MS`, 1 s). Because `t_send` happens before `t_seen`
in real time, the leader's lease always ends before the control plane may promote anyone, provided
the two monotonic clocks do not drift apart by more than `G` over `L` (they drift by parts per
million). No wall-clock synchronisation is needed. `G` also covers the time between the leader's
lease check and the WAL append of a write that passed it.

A leader that cannot reach the control plane stops accepting writes when its lease ends (503,
retryable). This is the price of fencing without a quorum among ingestion nodes: a control-plane
outage longer than `L` pauses writes (reads and replication continue).

### 3.3 Candidate selection

When the leader's lease has lapsed (`now > t_seen + L + G`), the control plane picks a candidate:

1. Members seen within the last `L` before the lease lapsed are *live*. The control plane waits
   until every live member has sent a heartbeat **after** the lapse (or stopped being live). After
   the lapse no new write can be acknowledged by the old leader, so positions reported after it are
   final: they include every write that any follower confirmed before the lapse. (Without this rule
   a follower whose last report was a second old could lose to one that reported later but holds
   less, which would break the synchronous-replication guarantee below.)
2. Eligible: reported term equal to the current term, `synced` (initial sync done, no resync
   pending, not blocked), and a position in the **current lineage**.
3. The highest position wins; ties go to the node the operator asked for (`step-down?prefer=`), then
   to the smallest node id. With no eligible member there is no promotion (and a metric says so).

**Positions and lineage.** A follower's position is `(generation, offset)` in the leader's WAL
lineage; the leader's is its own `(generation, view length)`. Generations are random ids that change
at every checkpoint, so they are ordered by the control plane's lineage list: generations reported
by the current-term leader (with the transition `prev_generation:prev_records` it reports from its
last checkpoint) and by followers that crossed a checkpoint (`switch_from`). Each lineage entry may
carry an end (`records` at which it was closed); an offset beyond the end is a divergent history and
not eligible. A member in a generation the lineage does not know is not eligible.

### 3.4 Promotion

The control plane increments the term, persists `(term, leader)` and then, in the same request,
moves every placement whose leader was the old node to the candidate (bumping the placement epoch)
and persists placement. A crash between the two writes is repaired on the next heartbeat: the
placement is reconciled with the failover leader idempotently.

The candidate learns it is leader from its next heartbeat answer. Under the drained runtime lock it:

1. stops applying replication frames (a frame fetched before promotion is discarded on apply);
2. checks that its WAL holds exactly what its replication cursor claims;
3. **adopts the old leader's generation id** for its WAL and immediately **checkpoints**, which
   closes that generation at its own offset and records the transition
   `(old generation, offset, new generation)`;
4. removes its follower cursor, persists the term, and only then starts accepting writes.

Step 3 is the fence for followers. Followers behind the new leader keep receiving the rest of the
closed generation and cross the checkpoint with a generation switch (no resync); a follower exactly
at the new leader's offset switches directly; a follower **ahead** of it (it applied records the new
leader never received from the old one) no longer matches the transition and gets a full resync,
which discards those records. No follower can silently mix histories.

A node that is re-elected while owning the lineage (the old leader restarting before any promotion)
needs no fence: no other history exists.

### 3.5 Fencing the old leader

* **Lease**: it stops accepting writes when its lease lapses, before anyone else is promoted (3.2).
* **Heartbeat**: a heartbeat with an older term, or from another instance of the leader while the
  lease is held, is answered `role=follower` with the new leader's URL. The node demotes itself:
  it refuses writes immediately, copies its WAL aside as `<wal>.deposed-t<term>-<unix ms>` (the only
  place unreplicated writes of an asynchronous leader survive), forces a full resync and follows the
  new leader. Its own `/internal/replication/*` endpoints keep serving only its local, now-follower
  state.
* **Replication**: followers send `term=` with each poll and leaders answer with a `term=` line. A
  follower refuses a frame whose term is older than the one it knows, and a leader that sees a poll
  with a newer term than its own stops accepting writes at once (it has been deposed even if the
  control plane is unreachable).
* **Writes**: every mutation (`POST /v1/ingest*`, `DELETE`) is checked at the HTTP entry and again
  under the runtime lock immediately before it is appended, so a write that queued behind a demotion
  is refused too.

A non-leader answers mutations with **503** `{"error":"not_leader: ..."}`, `Retry-After: 1`,
`X-Dash-Leader: false`, `X-Dash-Leader-Node` and `X-Dash-Leader-Url` (when known) and
`X-Dash-Term`. 503 rather than 421 keeps existing clients and SDKs retrying as they already do for
503; the headers let a smarter client go straight to the leader.

### 3.6 Clients and followers find the new leader

* Ingestion followers learn the leader's URL from every heartbeat answer and re-point their pull.
* Placement routing (`DASH_ROUTER_CONTROL_PLANE_URL`) sees the moved placement on its next reload
  (a promoted node reloads at once).
* On Kubernetes a leader Service selects only the pod whose `GET /v1/ready/leader` answers 200;
  retrieval followers and clients use it, so they follow the leader without configuration changes.
* Elsewhere, put a load balancer with the same health check in front, or use the leader hint
  headers.

## 4. Durability guarantees

Let a write be *acknowledged* when the client got a 2xx.

**Asynchronous replication (`DASH_INGEST_MIN_SYNC_REPLICAS=0`, the default).** An acknowledged write
is durable on the leader's disk (with the default WAL fsync policy). If the leader node is lost, any
acknowledged write that no follower had applied yet is **lost** from the cluster's history: the most
up-to-date follower is promoted and the old leader's extra records are discarded when it rejoins (a
copy stays in `<wal>.deposed-*`). The exposure is bounded by replication lag (typically one poll
interval). If the old leader restarts before its lease lapses, or is the most up-to-date member when
the election runs, it is re-elected and nothing is lost.

**Synchronous replication (`DASH_INGEST_MIN_SYNC_REPLICAS=N`, N >= 1).** The leader answers a write
only after at least N promotable followers (ingestion followers of the current term) have durably
applied a WAL position at or after it: a follower's poll from offset `p` proves it fsynced its WAL and
cursor up to `p`. With N = 1 and at least one follower, **an acknowledged write survives the loss of
any single node**: if the leader dies, the confirming follower is live, the election waits for its
post-lapse report (3.3) and the highest position is at least the write's position. Losing the leader
and the confirming follower together can lose it (use N = 2 with three followers for two failures).

If N confirmations do not arrive within `DASH_INGEST_SYNC_REPLICATION_TIMEOUT_MS` (5 s),
`DASH_INGEST_SYNC_REPLICATION_ON_TIMEOUT` decides:

* `fail` (default): the write is answered **503** `sync_replication_timeout` with `Retry-After`.
  The write *is* in the leader's WAL and may still replicate; its outcome is unknown to the client,
  which retries (ingest and delete are idempotent by id). No write is ever acknowledged without N
  confirmations.
* `degrade`: the write is answered 200 with `"commit_status":"sync_degraded"` and header
  `X-Dash-Sync-Replication: degraded`, counted in `dash_ingest_sync_replication_degraded_total`.
  Availability is kept and the guarantee is waived for that write.

Followers long-poll the leader while caught up, so a synchronous write costs about two round trips
plus the follower's fsync, not a poll interval.

## 5. What this does not provide (remaining for full consensus, ADR-05)

* **The control plane is a single point of decision.** It is not replicated by consensus: several
  control-plane processes share a file lease on one volume. A control-plane outage longer than the
  leader lease pauses writes. Raft for the metadata group removes this.
* **No quorum commit.** Synchronous mode waits for N followers chosen by speed, not a majority
  quorum; it gives "survives N node losses", not linearizability under arbitrary partitions.
* **Lease safety rests on bounded clock-rate drift** between the leader and the control plane
  (monotonic clocks, `G` per `L`) and on a process not being paused longer than `G` between its
  lease check and the WAL append. Raft's term check on every append does not.
* **Retrieval followers outside Kubernetes** do not discover the leader themselves; they need a
  stable URL (leader Service or a health-checked load balancer).
* **Membership is implicit** (whoever heartbeats); there is no joint consensus for adding or
  removing nodes, and node identity (`DASH_NODE_ID`) must be unique.
* **A failover resyncs followers that were ahead** of the promoted node and rewrites the promoted
  node's snapshot once (the fencing checkpoint).
* The fencing token is not yet checked by storage below the WAL (there is no shared storage; each
  node writes only its own disk, so a deposed leader can only corrupt its own copy, which it then
  replaces by a resync).
