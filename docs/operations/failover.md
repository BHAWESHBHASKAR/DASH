# Automatic failover of the ingestion leader

One ingestion node accepts writes at a time (the leader). With automatic
failover, the control plane detects a dead leader and promotes the most
up-to-date ingestion follower, so losing the leader's node pauses writes for
a few seconds instead of until an operator intervenes. The design, its proof
sketch and what it does not provide are in
[ADR 0006](../adr/0006-leader-failover.md); this page is the operator's view.

Failover is **off by default**. Without it nothing changes: a single
ingestion node writes, followers are configured by hand, and a follower that
is not given a source URL accepts writes on its own (do not run two of those
against the same followers).

## How it works

```
             heartbeat (every 1 s): node id, URL, term, role, WAL position
  ingestion  ───────────────────────────────────────────────────────────▶  control plane
  n1, n2, n3 ◀───────────────────────────────────────────────────────────  (leader of its own
             answer: leader id + URL, term, lease (5 s)                      file lease)
```

* **Terms.** The control plane names the leader and gives it a term (a
  fencing token that only grows). It writes the term to disk before telling
  anyone, and adopts any higher term a node reports, so a term is never
  handed to two nodes.
* **Lease.** The leader accepts writes only until `lease` after it *sent*
  the heartbeat the control plane last answered with "you are leader" (its
  own monotonic clock). The control plane waits `lease + grace` after it
  *received* that heartbeat before it promotes anyone else. The leader's
  lease therefore always ends first; no wall-clock synchronisation is needed.
* **Detection and choice.** When the lease has lapsed, the control plane
  waits until every node it heard from in the last lease has reported once
  more (their positions are then final), and promotes the eligible node with
  the highest replication position: a synced follower of the current term,
  in the current WAL lineage. Ties go to the node named in a step-down, then
  to the smallest node id.
* **Promotion.** The term goes up by one and is persisted; every placement
  led by the old node moves to the new one (placement epoch + 1). The new
  leader stops following, names its WAL after the old leader's WAL
  generation and checkpoints once. That checkpoint is the fence for the
  other followers: those behind or exactly at its position continue without
  a resync (they cross the checkpoint like any other); one that holds records
  the new leader never received resyncs and drops them.
* **The old leader.** Whether it was dead, paused or cut off, it stops
  writing when its lease lapses. When it comes back it is told the new term
  and leader: it copies its WAL aside (`<wal>.deposed-t<term>-<unix ms>`),
  resyncs from the new leader and follows it. A follower that polls it with a
  newer term also makes it stop writing immediately.
* **Writes on a non-leader** are answered `503` with `Retry-After: 1`,
  `X-Dash-Leader: false`, `X-Dash-Leader-Node`, `X-Dash-Leader-Url` (when
  known) and `X-Dash-Term`; the body is `{"error":"not_leader: ..."}`. The
  check runs at the HTTP entry and again under the runtime lock right before
  the WAL append. Existing clients retry 503 as before; a client can use the
  URL header to go straight to the leader.

## Guarantees

| Replication mode | An acknowledged write survives | Measured |
|---|---|---|
| Asynchronous (`DASH_INGEST_MIN_SYNC_REPLICAS=0`, default) | the loss of any follower; the loss of the leader **only if a follower had applied it** (or the leader comes back before a promotion). Unreplicated writes are lost from the cluster (a copy stays in `<wal>.deposed-*`). | `crash-test --failover --env DASH_INGEST_MIN_SYNC_REPLICAS=0`: 4 of 337 acknowledged writes lost over 3 leader kills |
| Synchronous, `DASH_INGEST_MIN_SYNC_REPLICAS=1` (one follower or more) | the loss of **any single node**. Two nodes lost together (the leader and the only follower that confirmed) can lose it. Use `N=2` with three followers for two losses. | `crash-test --failover`: 0 lost over 20 kills; e2e `s14_leader_failover.rs` |

In both modes:

* No two nodes accept writes at the same time, given the clock assumption
  below.
* A follower never mixes the histories of two leaders (the fencing
  checkpoint, plus term checks on every frame).
* Writes are unavailable from the leader's death until a promotion: about
  `lease + grace + one heartbeat` (2.6 s measured with a 2 s lease, 0.5 s
  grace and 200 ms heartbeats; about 6 to 7 s with the defaults).
* A control-plane outage longer than the lease **pauses writes** (the leader
  cannot renew). Reads and replication continue.

**Synchronous replication.** The leader answers a write after N followers of
the current term have polled from a WAL position at or after it (a poll from
offset `p` proves the follower fsynced its WAL up to `p`). Followers long-poll
a caught-up leader, so a write waits for about two round trips and one
follower fsync, not a poll interval. If the confirmations do not arrive
within `DASH_INGEST_SYNC_REPLICATION_TIMEOUT_MS`:

* `DASH_INGEST_SYNC_REPLICATION_ON_TIMEOUT=fail` (default): `503
  sync_replication_timeout` with `Retry-After`. The write is in the leader's
  WAL and may still replicate: the outcome is unknown, retry it (ingest,
  batch ingest with a `commit_id`, and deletes are idempotent).
* `degrade`: `200` with `"commit_status":"sync_degraded"` and
  `X-Dash-Sync-Replication: degraded`. The write is acknowledged without the
  guarantee; `dash_ingest_sync_replication_degraded_total` counts it.

Successful synchronous writes carry `X-Dash-Sync-Replicas: <n>`. Every HTTP
worker that answers a synchronous write is held until its confirmation:
size `DASH_INGEST_HTTP_WORKERS` for the concurrency you need. Followers'
polls and commit acks use a reserved worker lane, so they never queue behind
those writes.

**Assumptions.** The leader's and the control plane's monotonic clocks run
at the same rate to within `grace` per `lease` (parts per million in
practice), and a leader process is not frozen for longer than `grace`
between the lease check of a write and its WAL append. Node ids are unique.
The control plane is one decision point (its replicas share a file lease on
one volume); it is not replicated by consensus. See ADR 0006, section 5.

## Configuration

Control plane (all replicas):

| Setting | Default | |
|---|---|---|
| `DASH_CONTROL_PLANE_INGEST_FAILOVER` | off | `1` turns the coordinator on. |
| `DASH_CONTROL_PLANE_INGEST_LEASE_MS` | 5000 | Leader lease. |
| `DASH_CONTROL_PLANE_INGEST_PROMOTION_GRACE_MS` | 1000 | Extra wait before a promotion. |
| `DASH_CONTROL_PLANE_INGEST_STATE_PATH` | `<DASH_CONTROL_PLANE_STATE_PATH>.ingest-failover` | Durable term, leader and lineage; put it on the shared volume. |

Every ingestion node:

| Setting | |
|---|---|
| `DASH_INGEST_FAILOVER_CONTROL_PLANE_URL` | Turns failover on for this node. |
| `DASH_NODE_ID` | Unique per node (also the placement node id). |
| `DASH_INGEST_FAILOVER_ADVERTISE_URL` | This node's base URL as the others reach it. |
| `DASH_CONTROL_PLANE_TOKEN` or `DASH_ROUTER_CONTROL_PLANE_TOKEN` | Heartbeat credential (same TLS options as the placement fetch, `DASH_ROUTER_CONTROL_PLANE_*`). |
| `DASH_INGEST_REPLICATION_TOKEN` | Same value on every node: each node pulls from whichever is leader. |
| `DASH_INGEST_FAILOVER_HEARTBEAT_INTERVAL_MS` | 1000; keep it at a fifth of the lease or less. |
| `DASH_INGEST_MIN_SYNC_REPLICAS`, `DASH_INGEST_SYNC_REPLICATION_TIMEOUT_MS`, `DASH_INGEST_SYNC_REPLICATION_ON_TIMEOUT` | Synchronous replication (above). |

`DASH_INGEST_REPLICATION_SOURCE_URL` is optional with failover: a node
without it may be chosen as the first leader of a new cluster (the control
plane waits one lease after its start, then picks the bootstrap candidate
with the most data, ties to the smallest node id); a node with it starts as
that node's follower. After the first election the control plane decides,
whatever the configuration says.

**Tuning.** Write unavailability after a leader loss is about
`lease + grace + heartbeat`. A shorter lease fails over faster but makes the
leader more sensitive to control-plane hiccups (a renewal must succeed every
lease). Keep `grace` at least a few hundred milliseconds above the longest
pause you expect on a leader (GC-free Rust processes pause rarely; VMs and
containers under CPU throttling do).

## Endpoints

| Endpoint | |
|---|---|
| `POST /v1/control-plane/ingest/heartbeat` | Used by the nodes. |
| `GET /v1/control-plane/ingest` | Term, leader, lease remaining, members with their positions, `blocked` reason. |
| `POST /v1/control-plane/ingest/step-down?prefer=<node>` | Planned handover (below). |
| `GET /v1/ready/leader` (ingestion) | 200 only on the leader with a valid lease: the health check of a leader Service or load balancer. |
| `GET /v1/ready` (ingestion) | Adds a `failover` object (role, term, lease, leader). |

## Clients, followers and load balancers

* Ingestion followers learn the leader from every heartbeat.
* Retrieval followers and clients need a URL that follows the leader: on
  Kubernetes the chart's leader Service ([kubernetes.md](kubernetes.md));
  elsewhere a load balancer that health-checks `GET /v1/ready/leader`.
  Retrieval followers that point at a dead leader keep serving their last
  state and report `replication_stale` in `/ready`.
* Placement routing (`DASH_ROUTER_CONTROL_PLANE_URL`) sees the moved
  placement on its next reload; the promoted node reloads at once.

## Split-brain protections

1. The leader's lease ends before anyone else may be promoted.
2. Terms only grow and are persisted before use.
3. Every write is checked against the lease under the runtime lock.
4. Followers send their term with every poll; a leader that sees a newer
   term stops writing. Frames from an older term are refused
   (`dash_ingest_failover_stale_term_frames_total`).
5. The fencing checkpoint on promotion makes any follower that applied
   records of the old leader beyond the new leader's position resync.
6. A deposed node never writes again before it has resynced from the new
   leader.

## Runbook

**Leader died.** Nothing to do: `dash_control_plane_ingest_promotions_total`
goes up and `GET /v1/control-plane/ingest` names the new leader. Bring the
old node back (or replace its disk); it rejoins as a follower and resyncs.
If you need writes the old leader acknowledged but never replicated
(asynchronous mode only), they are in `<wal>.deposed-*` on that node:
inspect with `wal-inspect`, re-ingest by hand if needed, then delete the
copy.

**No promotion (`DashIngestFailoverBlocked`).** `GET
/v1/control-plane/ingest` shows `blocked`:

* `waiting_for_reports`: a member that was live has not reported since the
  lease lapsed. It clears by itself within one lease (the member counts as
  dead after a lease without a report).
* `no_eligible_candidate`: no live member is synced in the current term and
  lineage (all followers were resyncing or lagging behind a checkpoint the
  control plane cannot order). Check the followers' `/ready`; once one is
  synced it is promoted.

**No leader at all (`DashIngestNoLeader`).** The control plane is down or
unreachable from the nodes (`dash_ingest_failover_heartbeat_failures_total`
grows), or no member reports. Restore the control plane first: writes stay
paused while it is gone.

**Planned handover (maintenance on the leader's host).** `POST
/v1/control-plane/ingest/step-down?prefer=n2`. The leader stops being
renewed and stops writing at once (it learns at its next heartbeat); after
the lease the best member is promoted (`n2` on a tie). Writes pause for
about one lease; followers first catch up from the stepping-down leader, so
nothing is lost even in asynchronous mode. Then stop the old leader.

**Manual override.** The automatic promotion is the only safe way to change
the leader: never point clients at a node by hand while failover is on. To
force a specific node, make it the only eligible one (stop the others) or
step down with `prefer=`. The placement-level
`/v1/control-plane/failover/promote` endpoint still moves one placement;
with failover on, the coordinator moves it back to the failover leader on
the next heartbeat.

**Turning failover off.** Stop the nodes, remove
`DASH_INGEST_FAILOVER_CONTROL_PLANE_URL`, configure the last leader without
a replication source and the others with it, start the leader first. The
followers keep their cursors (they resync only if they were ahead).

**Control-plane state lost.** Restart it with an empty state path: it adopts
the highest term the nodes report and re-recognises the leader of that term
when it reports. Lineage knowledge is rebuilt from the leader's reports; a
promotion before that is refused (`no_eligible_candidate`) until the leader
or a follower crossing a checkpoint reports it.

## Metrics and alerts

Control plane: `dash_control_plane_ingest_term`,
`dash_control_plane_ingest_leader_known`,
`dash_control_plane_ingest_members`,
`dash_control_plane_ingest_promotions_total`,
`dash_control_plane_ingest_failover_blocked`,
`dash_control_plane_ingest_last_failover_seconds`.

Ingestion: `dash_ingest_failover_role{role}`, `dash_ingest_failover_term`,
`dash_ingest_failover_accepts_writes`,
`dash_ingest_failover_promotions_total`,
`dash_ingest_failover_demotions_total`,
`dash_ingest_failover_promotion_failures_total`,
`dash_ingest_failover_heartbeat_failures_total`,
`dash_ingest_failover_stale_term_frames_total`, and for synchronous
replication `dash_ingest_sync_replication_{min_replicas,waits_total,confirmed_total,timeouts_total,degraded_total,wait_seconds_total}`.

Alerts: `DashIngestNoLeader`, `DashIngestFailoverBlocked`,
`DashIngestFailoverHappened`, `DashIngestSyncReplicationTimeouts`
(runbooks in [runbooks/](runbooks/)).

## Testing

* Control plane, deterministic with an explicit clock:
  `services/control-plane/src/ingest_failover_tests.rs` (bootstrap, renewal,
  promotion window, waiting for post-lapse reports, divergent followers,
  lineage across an unreported checkpoint, step-down, persistence, the HTTP
  endpoint moving the placement).
* Ingestion: `services/ingestion/src/transport/failover_tests.rs` (write
  gate and lease expiry, demotion with WAL copy and resync, a newer term in a
  poll, the fencing checkpoint, stale frames refused, synchronous writes
  against a long-polling follower, timeout policies).
* End to end with the real binaries: `tests/e2e/tests/s14_leader_failover.rs`
  (SIGKILL of the leader under load with synchronous replication; SIGSTOP of
  the leader and resume).
* Randomised: `crash-test --failover [--checkpoint-every N]`
  ([testing-durability.md](testing-durability.md)).
