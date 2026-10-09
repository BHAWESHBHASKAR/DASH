# Architecture decision records

| ADR | Title | Status |
|---|---|---|
| [0003](0003-storage-engine-indexes.md) | Storage engine indexes: vector (usearch), text (tantivy), filtering, durability | Proposed (measured by the P2 engine spike) |

ADRs 0001 and 0002 are not present in this repository. The master plan
([`docs/plans/2026-10-09-production-readiness-master-plan.md`](../plans/2026-10-09-production-readiness-master-plan.md))
carries ADR summaries (ADR-03 WAL v2, ADR-04 LSM engine, ADR-06 tenant partition); this
record supplies the measurements behind ADR-04 and the WAL group-commit part of ADR-03.
