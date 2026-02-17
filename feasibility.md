# Dash — Feasibility Study

**Date:** 2026-02-17  
**Project Model:** Open-source, community-built  
**Purpose:** Assess the technical feasibility, market positioning, and community viability of Dash as an open-source next-generation vector database with evidence-first retrieval.

---

## 1. Project Vision

Dash aims to be an **open-source, evidence-first retrieval engine** for RAG, where the primary data primitive is an atomic claim with provenance — not a standalone embedding vector. The project invites the community to collectively build a next-generation system that addresses limitations current vector databases ignore:

- **Contradiction awareness** — surface conflicting evidence instead of hiding it
- **Temporal correctness** — enforce time-window validity on claims
- **Citation-grade provenance** — every retrieved claim traces back to source spans
- **Multi-hop evidence assembly** — graph-based reasoning, not just nearest-neighbor

The project is designed to be **built in the open**, starting with a solid Rust foundation that contributors can extend.

---

## 2. Current State of the Codebase

### 2.1 Quantitative Overview

| Metric | Value |
|---|---|
| Language | Rust |
| Total LOC | **4,192** |
| Workspace crates | 9 (4 libs + 4 services + 1 benchmark) |
| Unit tests | 36 passing |
| External dependencies | 0 (pure `std` — see §2.3) |
| CI pipeline | ✅ `scripts/ci.sh` (clippy + tests + benchmarks) |

### 2.2 Crate Architecture

```
pkg/
  schema/     — 183 LOC — Core domain types: Claim, Evidence, ClaimEdge, validators
  ranking/    — 100 LOC — Lexical overlap scoring, contradiction-aware ranking
  graph/      —  57 LOC — Edge summarization (support/contradiction counts)
  store/      — 1,272 LOC — In-memory store, FileWal, snapshot/compaction, replay

services/
  ingestion/  — 162 LOC — Ingest pipeline with persistent WAL + checkpoint policy
  retrieval/  — 1,022 LOC — Retrieval API, HTTP transport, hand-rolled JSON parser
  indexer/    —  54 LOC — Tier classification scaffolding (hot/warm/cold)
  metadata-router/ — 40 LOC — Deterministic FNV shard routing

tests/
  benchmarks/ — Benchmark harness with profiles, history guards, quality probes
```

### 2.3 Why Zero External Dependencies (For Now)

The `progress.md` notes an **offline/no-network constraint** during initial development. The pure-`std` approach was a pragmatic choice, not a philosophical one. Moving forward, adopting crates like `serde`, `tokio`, and ANN libraries is essential and planned.

### 2.4 What's Functional Today

| Component | Status | Notes |
|---|---|---|
| Claim/Evidence/Edge schema + validation | ✅ Working | Core domain model with field validators |
| WAL-backed persistent ingestion | ✅ Working | Append, replay, snapshot, compaction |
| Snapshot checkpointing + recovery | ✅ Working | Snapshot-aware WAL delta recovery path |
| Lexical overlap retrieval | ✅ Working | Brute-force scan with token matching |
| Contradiction-aware scoring | ✅ Working | Support/contradiction signal weighting |
| Temporal range filtering | ✅ Working | Filter by `event_time_unix` window |
| HTTP retrieval API | ✅ Working | `POST /v1/retrieve` with JSON body |
| Deterministic shard routing | ✅ Working | FNV-based tenant+entity→shard mapping |
| Benchmark harness | ✅ Working | Profiles (smoke/standard/large), quality probes, regression guards |
| CI pipeline | ✅ Working | clippy + tests + benchmark guards |

### 2.5 What Needs To Be Built

| Component | Priority | Complexity | Good for Open-Source Contributors? |
|---|---|---|---|
| ANN vector index (HNSW / DiskANN) | **Critical** | Very High | ✅ Yes — major standalone module, well-researched algorithms |
| BM25 / sparse lexical index | High | Medium | ✅ Yes — well-defined scope, lots of reference implementations |
| Async HTTP server (`tokio` + `axum`) | **Critical** | Medium | ✅ Yes — straightforward migration, great first contribution |
| `serde` JSON serialization | High | Low | ✅ Yes — excellent "good first issue" |
| Embedding model integration API | **Critical** | Medium | ✅ Yes — API boundary design, pluggable providers |
| Multi-hop graph traversal | High | High | ⚠️ Moderate — requires graph algorithm knowledge |
| Entity extraction pipeline | High | High | ⚠️ Moderate — ML/NLP expertise needed |
| Contradiction detection model | High | Very High | ⚠️ Specialized — NLP research contribution |
| Segment storage + object store (S3) | Medium | High | ✅ Yes — well-understood patterns |
| Distributed consensus / replication | Low (Phase 2) | Very High | ⚠️ Expert-level — defer until core is solid |
| Multi-tenant isolation + encryption | Low (Phase 3) | High | ✅ Yes — security-focused contributors |
| Docker / K8s packaging | Medium | Low | ✅ Yes — DevOps/infra contributions |
| Client SDKs (Python, JS, Go) | Medium | Medium | ✅ Yes — language-specific community modules |
| Documentation + tutorials | High | Low | ✅ Yes — critical for community growth |

---

## 3. Market Context & Differentiation

### 3.1 The Market Is Large and Growing

- **Vector DB market:** $1–4B by 2026, 10–30% CAGR through 2031
- **RAG market:** $1.94B (2025) → $9.86B (2030), 38.4% CAGR
- **Graph-RAG** projected to reach maturity within 2–5 years (Gartner)
- Hybrid retrieval (vector + lexical + graph + metadata) is becoming the industry standard

### 3.2 Existing Players

| System | Model | Strengths | What Dash Does Differently |
|---|---|---|---|
| **Qdrant** | Open-source (Rust) | Fast ANN, strong filtering, great DX | Dash adds evidence graph + contradiction awareness |
| **Milvus** | Open-source | Industrial scale, GPU acceleration | Dash adds claim-level provenance + temporal validity |
| **Weaviate** | Open-source | Hybrid search, knowledge graph layer | Dash makes evidence quality the primary optimization target |
| **Pinecone** | Proprietary SaaS | Managed serverless, zero-ops | Dash is open-source and optimizes for evidence quality, not just similarity |
| **Neo4j + GraphRAG** | Graph DB + RAG layer | Entity-relation graphs for RAG | Dash is purpose-built for claim evidence, not general-purpose graph |
| **Graphwise** | Proprietary | Enterprise "Trust Layer" with KG backbone | Dash is open-source alternative with deeper contradiction handling |
| **Writer KG** | Proprietary | Graph-based RAG for enterprise | Dash exposes the evidence graph directly to consumers |

### 3.3 Dash's Unique Position

No existing open-source system combines all of:

1. **Claim as the first-class primitive** (not chunk, not document, not entity — _claim_)
2. **Support/contradiction graph** baked into the retrieval ranking
3. **Temporal validity enforcement** at query time
4. **Citation-grade provenance** for every result
5. **Built in Rust** for systems-level performance

The closest analogy: **Dash is to evidence-based retrieval what Qdrant was to vector search in 2021** — an early-stage Rust project that could grow into a category-defining system if the community rallies behind it.

### 3.4 Timing

| Signal | Implication |
|---|---|
| GraphRAG is a top trend in 2025–2026 | ✅ Market demand exists |
| Enterprise customers want citation + provenance | ✅ Evidence-first thesis is validated by buyer behavior |
| Contradiction detection is an active research area | ✅ Academic interest → potential research contributors |
| RAG is transitioning from "experimental" to "table stakes" | ✅ Growing user base that needs better retrieval quality |
| Major VCs still funding RAG infrastructure | ✅ Ecosystem energy is high |

---

## 4. Open-Source Feasibility Assessment

### 4.1 Strengths for Community Building

| Factor | Assessment |
|---|---|
| **Language choice (Rust)** | ✅ Rust has a passionate, quality-focused open-source community. Rust DB projects (SurrealDB, TiKV, Qdrant) attract strong contributors. |
| **Modular crate architecture** | ✅ Clean workspace with isolated crates means contributors can work on `pkg/ranking` without understanding `pkg/store`. |
| **Novel problem domain** | ✅ Evidence-first retrieval is genuinely new. Contributors are more motivated by unsolved problems than by reimplementing existing solutions. |
| **CI + benchmark discipline** | ✅ CI pipeline and regression guards from day one signal a serious project. |
| **Well-articulated architecture** | ✅ `EME_ARCHITECTURE.md` gives contributors a clear map of where the project is going. |
| **Clear "good first issues"** | ✅ Replacing hand-rolled JSON with `serde`, adding async HTTP, writing client SDKs — all well-scoped entry points. |

### 4.2 Risks for Community Building

| Risk | Severity | Mitigation |
|---|---|---|
| **Too much still in "architecture doc" vs "working code"** | High | Rapidly build the core ANN index + async HTTP to make the system usable for real workloads |
| **No external users yet** | High | Publish benchmarks comparing Dash quality vs vector-only baseline on public datasets |
| **Single contributor bottleneck** | High | Write contributor guide, define RFC process, actively mentor early contributors |
| **"NIH syndrome" perception** (why not just use Qdrant + a layer?) | Medium | Clearly articulate why evidence-first needs to be at the storage/index level, not bolted on top |
| **Scope creep toward "distributed database"** | Medium | Keep Phase 0–1 focused on single-node quality. Distribution comes later. |
| **Missing documentation** | High | Write README, CONTRIBUTING.md, architecture walkthrough, and API docs before seeking contributors |

### 4.3 Community Growth Playbook

**Phase 1 — Seed (Months 1–3):**
- Polish README with clear vision statement and architecture diagram
- Add CONTRIBUTING.md with setup instructions, code style, PR process
- Label issues as `good-first-issue`, `help-wanted`, `research-contribution`
- Publish a blog post / HN launch explaining the evidence-first thesis
- Set up Discord/Matrix for contributor discussion

**Phase 2 — Early Adopters (Months 3–6):**
- Publish benchmark comparisons on public datasets (BEIR, MS MARCO, NQ)
- Release a Python client SDK for easy experimentation
- Create tutorials: "Build a contradiction-aware RAG pipeline with Dash"
- Engage with RAG/LLM communities (r/LocalLLaMA, LangChain Discord, etc.)

**Phase 3 — Growth (Months 6–12):**
- RFC process for major features
- Regular releases with changelogs
- Conference talks (RustConf, AI Engineer Summit, etc.)
- Integration guides for LangChain, LlamaIndex, Haystack

---

## 5. Technical Feasibility Deep Dive

### 5.1 ANN Index — The Critical Path

The most important missing piece is a **vector similarity index**. Without it, retrieval is O(n) linear scan — unusable beyond ~100K claims.

**Options:**

| Approach | Pros | Cons | Recommendation |
|---|---|---|---|
| Build HNSW from scratch in Rust | Full control, optimized for Dash's data model | 3–6 months effort, complex | ✅ Best long-term option for an open-source DB |
| Use `usearch` / `hora` Rust crate | Fast to integrate, proven | External dependency, less control | ⚠️ Good bootstrap, replace later |
| Integrate with FAISS via FFI | Battle-tested, GPU support | C++ dependency, complex build | ❌ Against Rust-native philosophy |

**Recommendation:** Start with an existing Rust ANN crate to unblock real workloads, then build a custom index optimized for Dash's claim-aware access patterns as a community effort.

### 5.2 Claim Extraction Pipeline — The Innovation Frontier

Dash assumes claims are extracted from raw documents. This pipeline is where the most novel engineering happens:

```
Raw Document → Chunking → Claim Extraction (LLM) → Entity Linking → 
Confidence Scoring → Contradiction Detection → Evidence Graph Construction
```

This pipeline is inherently **model-dependent** and should be designed as a **pluggable interface**:
- Define a `ClaimExtractor` trait in Rust
- Provide a reference implementation using an LLM API (OpenAI, Anthropic, or local models via Ollama)
- Let the community build specialized extractors for domains (legal, medical, financial)

### 5.3 Scalability Path

| Scale | Architecture | Estimate |
|---|---|---|
| **<1M claims** | Single-node, in-memory index | Current architecture + ANN index |
| **1M–100M claims** | Single-node, memory-mapped segments | 6–12 months of work |
| **100M–1B claims** | Sharded nodes, segment replication | 12–24 months of work |
| **>1B claims** | Full distributed system | 24+ months, requires dedicated team |

**For open-source launch, target <1M claims.** This is sufficient for most individual and small-team RAG use cases and proves the evidence-quality thesis.

### 5.4 Performance Targets (Realistic for Phase 0–1)

| Metric | Target | Rationale |
|---|---|---|
| Ingestion throughput | 10K claims/sec (single node) | Achievable with WAL + in-memory indexing |
| Query latency (p95) | <50ms at 1M claims | Requires ANN index, not linear scan |
| Contradiction detection accuracy | F1 ≥ 0.70 on adversarial set | Depends on extraction model quality |
| Citation coverage | ≥ 0.95 on labeled queries | Already architecturally guaranteed by design |

---

## 6. Comparison with Qdrant's Journey (Precedent Study)

Dash's trajectory can be benchmarked against Qdrant — the most relevant precedent as an open-source, Rust-native vector DB:

| Milestone | Qdrant | Dash (Projected) |
|---|---|---|
| First public release on GitHub | May 2021 | **Now** |
| Core search functionality working | ~6 months | **3–4 months** (with ANN crate integration) |
| v0.1 usable for real workloads | ~12 months | **6–8 months** |
| v1.0 production-ready | Feb 2023 (~2 years) | **18–24 months** (optimistic) |
| Team size at v1.0 | 75+ people | Depends on community traction |
| First external funding | 2022 | Not required but helpful |

**Key insight from Qdrant's story:** Andrey Vasnetsov built the initial prototype alone, then community + funding accelerated it. Dash is following the same path — the initial prototype exists, now it needs to reach "usable for real workloads" quality to attract contributors.

---

## 7. Roadmap with Open-Source Milestones

### Phase 0: Foundation (Current → Month 2)
**Goal:** Make the system usable for basic evidence retrieval experiments.

- [ ] Replace hand-rolled JSON with `serde` + `serde_json`
- [ ] Replace blocking TCP with `tokio` + `axum` async HTTP
- [ ] Integrate an existing Rust ANN index crate for vector search
- [ ] Add embedding API interface (pluggable: OpenAI, local models)
- [ ] Publish README, CONTRIBUTING.md, architecture guide
- [ ] Docker image for one-command local startup
- [ ] **Exit criteria:** A user can ingest documents, query with semantic search, and get contradiction-aware results with citations

### Phase 1: Evidence Graph Core (Months 2–6)
**Goal:** Prove that evidence-first retrieval beats vector-only on quality benchmarks.

- [ ] Multi-hop graph traversal for evidence assembly
- [ ] BM25 lexical index for hybrid retrieval
- [ ] Entity index for constrained queries
- [ ] Reference claim extraction pipeline (LLM-based)
- [ ] Contradiction detection integration
- [ ] Published benchmark comparisons on public datasets
- [ ] Python client SDK
- [ ] **Exit criteria:** Measurable quality lift over dense-only retrieval on contradiction-heavy benchmarks

### Phase 2: Production Hardening (Months 6–12)
**Goal:** Make Dash reliable enough for production RAG pipelines.

- [ ] Segment storage with memory-mapped access
- [ ] Proper temporal index structure
- [ ] Authentication + API keys
- [ ] TLS support
- [ ] Metrics + tracing (OpenTelemetry)
- [ ] Rate limiting + graceful shutdown
- [ ] Comprehensive documentation + tutorials
- [ ] **Exit criteria:** Used in at least 3 external production RAG pipelines

### Phase 3: Scale (Months 12+)
**Goal:** Horizontal scaling for large corpora.

- [ ] Distributed shard routing (activate metadata-router)
- [ ] Segment replication + failover
- [ ] Multi-tenant isolation
- [ ] Object store tiering (S3/GCS)
- [ ] **Exit criteria:** 100M+ claim benchmark with target SLOs met

---

## 8. Risk Matrix

| Risk | Severity | Likelihood | Mitigation |
|---|---|---|---|
| No contributors join the project | High | Medium | Strong launch messaging, clear contribution paths, active community engagement |
| ANN index integration proves harder than expected | Medium | Medium | Use proven crate first, custom build later |
| Claim extraction quality is too LLM-dependent | High | High | Design pluggable interface, benchmark multiple providers, invest in prompt engineering |
| Existing projects (Qdrant, Weaviate) add graph-RAG features | Medium | High | Move fast on the evidence-quality thesis proof; depth beats breadth |
| Scope expands beyond what a small core team can maintain | High | Medium | Strict RFC process, phased roadmap, say "no" to Phase 3 features until Phase 1 is proven |
| Performance doesn't meet expectations without custom ANN | Medium | Low | Rust's performance characteristics are forgiving; optimize later |
| Project perceived as "just another vector DB" | High | Medium | Marketing and documentation must emphasize the evidence-first thesis clearly |

---

## 9. Cost & Resource Model

### 9.1 Infrastructure for Development

| Resource | Cost | Notes |
|---|---|---|
| GitHub hosting | Free | Open-source repo |
| CI/CD (GitHub Actions) | Free tier sufficient | Rust builds are ~5 min |
| Benchmark compute | ~$50/month | Dedicated machine for nightly benchmarks |
| Domain + docs hosting | ~$20/year | GitHub Pages or similar |
| Discord/community | Free | Community communication |

### 9.2 Human Capital

| Contributor Type | Expected Source | Role |
|---|---|---|
| Core maintainer (1–2 people) | Founders | Architecture decisions, code review, community |
| Rust systems contributors | Open-source community | ANN index, storage, networking |
| ML/NLP contributors | Research community | Claim extraction, contradiction detection |
| SDK/Integration contributors | RAG/LLM community | Python SDK, LangChain/LlamaIndex plugins |
| Documentation contributors | Community | Tutorials, guides, examples |

---

## 10. Final Feasibility Verdict

### Feasibility Score: 7/10 (as open-source community project)

| Dimension | Score | Rationale |
|---|---|---|
| **Technical Vision** | 9/10 | Evidence-first retrieval is a genuine gap; architecture doc is comprehensive and well-reasoned |
| **Current Foundation** | 5/10 | Functional scaffolding with WAL, tests, CI — but no ANN search yet |
| **Market Timing** | 8/10 | Graph-RAG demand is surging; contradiction awareness is an emerging need |
| **Open-Source Viability** | 7/10 | Rust community is strong; modular crate design enables parallel contributions; novel problem domain attracts interest |
| **Competitive Differentiation** | 7/10 | No open-source project currently combines claim-first + contradiction-aware + temporal + provenance retrieval |
| **Execution Risk** | 5/10 | Critical path (ANN index + async HTTP) must be completed quickly to attract contributors |

### Verdict

**Dash is feasible as an open-source project** if the following conditions are met:

1. **Ship the ANN index integration within 4–6 weeks.** Without vector search, the system is not usable for real workloads and won't attract contributors.

2. **Prove the evidence-quality thesis with published benchmarks.** Show that Dash's contradiction-aware, graph-enriched retrieval measurably outperforms vector-only baselines. This is the marketing material that will make people care.

3. **Invest heavily in contributor experience.** Clear README, onboarding guide, labeled issues, responsive code review. The code exists — the community bridge doesn't yet.

4. **Stay focused on single-node quality.** Distributed systems are a Phase 3 concern. The evidence thesis must be proven before scaling it.

The project sits at a genuine frontier — the intersection of vector search, knowledge graphs, and evidence reasoning. If the core maintainers can bridge the gap between the architecture vision and a usable open-source release, Dash has the potential to define a new category in retrieval infrastructure.

**The window is open. Build fast, ship the proof, and let the community do the rest.**
