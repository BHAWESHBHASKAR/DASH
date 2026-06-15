# About

DASH is a production-grade vector database that stores atomic **claims** with their supporting **evidence** and recorded **contradictions**. It is built to be deployed in environments that are subject to audit — legal, medical, financial, enterprise knowledge workflows — where "we found a similar chunk" is not an acceptable answer to "where did the model get that from?"

## Pages

- [**Changelog**](changelog.md) — every release, the deltas, and the known limitations.
- [**License**](license.md) — Apache 2.0, with a short summary.
- [**Contributing**](contributing.md) — how to file a PR, the code standards, and the review process.
- [**Security**](security.md) — how to report a vulnerability, the supported versions, and the threat model.

## Project facts

| | |
| --- | --- |
| **License** | Apache 2.0 |
| **Language** | Rust (services), Python / Go / TypeScript / Java / C# (SDKs) |
| **Source** | <https://github.com/BHAWESHBHASKAR/DASH> |
| **Issues** | <https://github.com/BHAWESHBHASKAR/DASH/issues> |
| **Test count** | 379 Rust + 86 Go + 65 TypeScript + 59 Python = **589** |
| **First release** | 2026-06-15 |

## Why we built it

Naive RAG ranks documents by vector similarity and returns the top *k* chunks. That works for "summarize this article" but fails in three common enterprise cases: (1) two sources say opposite things and there is no way to demote the contradicted one, (2) a fact has a temporal window and the version retrieved is stale, (3) the auditor asks "why did the model say that?" and the answer is "because a 768-dimensional number was close to a query."

DASH treats the **claim** — an atomic, source-bound assertion — as the primary data primitive, with **evidence** and **citation** as first-class fields on every result. Every retrieval response is shaped `{ claim, score, supports, contradicts, citations[] }`. Each `citation` carries its `source_id`, `stance` (supports/contradicts/neutral), `source_quality`, and an optional `chunk_id` plus `span_start`/`span_end` for character-level traceability. The retrieval API exposes `stance_mode: support_only` to filter out claims that have been contradicted, and `time_range: {from_unix, to_unix}` to constrain results to a validity window.

That is the entire differentiator. DASH is not the fastest pure-vector index, and it does not pretend to be. It is the most **defensible** vector database for RAG that has to ship to environments where the answer to "where did that come from?" matters.

## Acknowledgments

DASH stands on the shoulders of giants:

- [`redb`](https://github.com/cberner/redb) — pure-Rust, ACID, embedded KV.
- [`usearch`](https://github.com/unum-cloud/usearch) — the HNSW implementation that powers the ANN search.
- [`jsonwebtoken`](https://github.com/Keats/jsonwebtoken), [`serde`](https://serde.rs/), [`tokio`](https://tokio.rs/) (build-time only) — the Rust crates that make the auth and the wire format tractable.
- The OpenAI v1 embeddings spec — the wire format DASH is a drop-in for.
- The 46 open-source contributors who have filed issues, sent PRs, and reviewed code.

## Maintainers

DASH is maintained by the [BHAWESHBHASKAR](https://github.com/BHAWESHBHASKAR) org and a rotating set of community maintainers. See [`AUTHORS`](https://github.com/BHAWESHBHASKAR/DASH/blob/main/AUTHORS) for the full list.
