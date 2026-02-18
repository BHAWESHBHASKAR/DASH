# DASH Shard and Replication P0

Date: 2026-02-18
Status: implementation baseline

## 1. Scope

P0 introduces control-plane primitives required for real shard/replica operation:

- placement metadata shape
- health-aware write/read route selection
- failover promotion semantics
- routing epoch management

This phase does not yet include network replication logs or multi-process consensus.

## 2. Data Model

### 2.1 Replica metadata

- `ReplicaRole`: `Leader | Follower`
- `ReplicaHealth`: `Healthy | Degraded | Unavailable`
- `ReplicaPlacement`: node id + role + health

### 2.2 Shard placement

- `ShardPlacement`: tenant + shard id + epoch + replicas

### 2.3 Routed target

- `RoutedReplica`: selected node and epoch for a specific tenant/entity route

## 3. Routing Semantics

### 3.1 Write route

- Resolve target shard via existing deterministic hash-ring route.
- Find matching shard placement entry.
- Select leader only when leader health is `Healthy`.
- Fail closed if no writable leader exists.

### 3.2 Read route

Supports:

- `LeaderOnly`
- `PreferFollower`
- `AnyHealthy`

Readable health:

- `Healthy`
- `Degraded`

### 3.3 Failover promotion

- Promotion target must exist and be readable (`Healthy` or `Degraded`).
- Existing leader is demoted to follower.
- Target follower is promoted to leader.
- Placement epoch increments by one.

## 4. Error Policy

Routing errors are explicit and typed:

- `PlacementNotFound`
- `NoWritableLeader`
- `NoReadableReplica`
- `ReplicaNotFound`
- `ReplicaUnhealthy`

## 5. Test Matrix

Required tests:

- deterministic base routing retained
- write route picks healthy leader
- read route honors `PreferFollower`
- write route rejects unhealthy leader
- failover promotion flips roles and increments epoch
- read route rejects when all replicas unreadable

## 6. Next Steps (P1)

After P0 primitives are complete:

1. Load placement state from runtime source (file/service) at startup.
2. Wire ingestion/retrieval services to route through placement-aware APIs.
3. Add placement epoch observability into metrics endpoints.
4. Add staged CI checks that enforce placement-route guardrails.
