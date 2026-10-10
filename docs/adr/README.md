# Architecture decision records

| ADR | Title | Status |
|---|---|---|
| [0003](0003-storage-engine-indexes.md) | Storage engine indexes: vector (usearch), text (tantivy), filtering, durability | Proposed (measured by the P2 engine spike; vector index implemented in P2 step 1, see section 10; text index built in-tree with BM25 instead of tantivy, see section 12) |
| [0004](0004-per-tenant-write-partitioning.md) | Per-tenant write partitioning of the ingestion runtime lock | Deferred (design recorded; superseded in direction by ADR-06) |
| [0005](0005-encryption-at-rest.md) | Encryption at rest: envelope encryption of WAL, snapshot, export, vector index, segment and redb data | Accepted (implemented, P4) |
| [0006](0006-leader-failover.md) | Automatic failover of the ingestion leader: control-plane terms and leases, fenced promotion, optional synchronous replication | Accepted (implemented, P3 step 1; Raft remains the target, ADR-05) |

ADRs 0001 and 0002 are not present in this repository. The master plan
([`docs/plans/2026-10-09-production-readiness-master-plan.md`](../plans/2026-10-09-production-readiness-master-plan.md))
carries ADR summaries (ADR-03 WAL v2, ADR-04 LSM engine, ADR-06 tenant partition); this
record supplies the measurements behind ADR-04 and the WAL group-commit part of ADR-03.
