# DASH vs Other Vector Databases

A feature-by-feature comparison focused on the dimensions that matter to RAG users: citation-grade provenance, contradiction handling, temporal correctness, open-source posture, and production-grade multi-tenancy. The full DASH feature set is in the main [README](../README.md) and the architecture lives in [eme-architecture.md](architecture/eme-architecture.md).

## Feature matrix

| Feature | DASH | Pinecone | Weaviate | Milvus | Qdrant | Chroma |
|---|---|---|---|---|---|---|
| Open source | yes (see [LICENSE](../LICENSE); intended Apache-2.0) | no, proprietary | yes, BSD-3 | yes, Apache-2.0 | yes, Apache-2.0 | yes, Apache-2.0 |
| Claim + Evidence model | first-class | no | no | no | no | no |
| Citation provenance per result | first-class | not modeled | not modeled | not modeled | not modeled | not modeled |
| Contradiction handling (evidence + edges) | first-class (`Stance::Contradicts`, `Relation::Contradicts`) | no | manual, via modules | no | no | no |
| `stance_mode: support_only` filter | yes | no | no | no | no | no |
| Temporal validity windows | first-class (`event_time_unix`, `valid_from`, `valid_to`, `time_range` filter) | metadata only | manual | manual | manual | manual |
| OpenAI-compatible `/v1/embeddings` | yes, native | partial | yes | via proxy layer | via proxy layer | yes |
| Swap embedding provider | trait-based (`EmbeddingProvider`) | n/a | plugin-based | n/a | n/a | function-based |
| HNSW ANN | yes (`usearch`) | yes, proprietary | yes | yes | yes | yes |
| Graph primitives (edges, multi-hop) | first-class (`supports`, `contradicts`, `refines`, `duplicates`, `depends_on`) | no | yes, but no contradiction semantics | no | payload-based only | no |
| Hash-chained audit log | yes, SHA-256 chain | no | no | no | no | no |
| Tenant isolation (strict authz) | yes, allowlist + scoped keys | yes | yes (OIDC) | yes | partial | no |
| Per-tenant rate limits | yes | yes | yes | yes | partial | no |
| Scoped API keys | yes (`key:tenant[,tenant...]`) | limited | yes | yes | limited | no |
| API key revocation (hot reload) | yes | yes | yes | yes | partial | no |
| JWT auth (HS256) with kid rotation | yes | JWT only | yes (OIDC) | yes | partial | no |
| Durable WAL with replay + checkpoints | yes, built-in | managed | yes | yes | yes | no |
| Backpressure-aware HTTP transport | yes (queue + 503) | managed | yes | yes | yes | no |
| Docker Compose / systemd units | yes | n/a | yes | yes | yes | no |
| Benchmark suite with CI regression guard | yes | n/a | partial | yes | partial | no |
| Managed cloud option | no (self-hosted) | yes | yes | yes | yes | yes |
| Backing storage | local WAL + segments | managed | pluggable (disk, S3, GCS, MinIO) | pluggable (disk, S3, GCS, MinIO) | local or S3-compatible | in-memory or local |

## When to use DASH

- You are building RAG where wrong answers have a cost: legal research, clinical decision support, financial due diligence, enterprise knowledge bases, regulatory compliance.
- You need citation-grade provenance on every retrieved claim — a source, a stance, a quality score, and ideally a span.
- You need the retrieval layer to know that a claim has been contradicted by another source and either demote or filter it (`stance_mode: support_only`).
- You need temporal validity windows on claims (a fact is true between `valid_from` and `valid_to`; the API should be able to ask "what was true in Q3 2024?") rather than a metadata filter hack.
- You need an audit trail that is tamper-evident: every state change is SHA-256-chained, and you can verify the chain with `scripts/verify_audit_chain.sh`.
- You are deploying into a multi-tenant SaaS and need per-tenant rate limits, scoped API keys, key revocation, and strict tenant allowlists in the same process.
- You want to be able to audit the storage and retrieval path yourself — DASH's core is open source, with no managed-cloud component and no proprietary extension.

## When NOT to use DASH

- You are doing pure image, audio, or video similarity search. DASH is text-claim-centric. Use Milvus or Pinecone.
- You need sub-millisecond latency at billion-vector scale. DASH is not optimized for this. Use Milvus with GPU indexing, or Pinecone serverless.
- You want a fully managed cloud with no ops responsibility. DASH is self-hosted only today. Use Pinecone, Weaviate Cloud, or Qdrant Cloud.
- You have no notion of evidence, citation, contradiction, or temporal validity in your data. DASH's differentiators are not free — you will be writing Claim/Evidence/Edge payloads into an API that is designed for them. A simpler vector DB (Qdrant, Chroma) will get you to "vector search" faster.
- You need a general-purpose graph database. DASH has typed claim edges with RAG-specific semantics, not a full property graph. Use Neo4j, Memgraph, or a graph database.
- You need an OLAP analytics engine over your vector store. Use ClickHouse, Druid, or a dedicated analytics warehouse.

## How the table is compiled

The "yes" entries are based on documented features in each project's official documentation and source-of-truth release notes. The "partial" and "manual" entries are areas where the feature exists but requires the operator to assemble the primitives themselves (e.g., Qdrant supports payload filtering and basic multi-tenancy, but does not ship per-tenant rate limits as a first-class authz concept). The "no" entries are features that are simply not part of the system. This document is intentionally a feature inventory, not a benchmark — for raw vector-search throughput and recall numbers, see [docs/benchmarks/](benchmarks/) for DASH's own numbers and the other projects' published benchmarks for theirs.
