# Concepts

These pages describe **what DASH is** and **why it is shaped the way it is**. They are written to be read once, end-to-end, the first time you adopt DASH — and to be returned to as reference when you have a specific design question.

## Pages

- [**Why DASH**](why-dash.md) — the problem with vanilla vector databases, and the claim/evidence/contradiction model that fixes it.
- [**Architecture**](architecture.md) — the two-service topology (ingestion + retrieval), the data flow, and the persistence stack.
- [**Data model**](data-model.md) — `Tenant`, `Vector`, `Claim`, `Evidence`, `Contradiction`, `AuditEvent`. The fields, the relationships, and JSON examples for each.
- [**Multi-tenancy**](multi-tenancy.md) — isolation guarantees, per-tenant rate limits, and the per-tenant key spaces inside `redb`.
- [**Persistence**](persistence.md) — `redb` architecture, the WAL fallback path, crash-recovery semantics, and the backup procedure.
- [**Security**](security.md) — the JWT auth model, scoped API keys, the hash-chained audit log, and the threat model.

## How to read this section

If you are evaluating DASH, start with [**Why DASH**](why-dash.md) and then [**Architecture**](architecture.md). If you are integrating, jump straight to the [Guides](../guides/index.md). If you are deploying, the [Operations](../operations/index.md) section is the entry point.
