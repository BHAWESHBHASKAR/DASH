# Why DASH

## The problem with vanilla vector databases

Most vector databases — Pinecone, Weaviate, Milvus, Qdrant, Chroma — share a single shape: a corpus of **chunks** indexed by an embedding, ranked by cosine similarity, returned in order. That shape is fast and simple, and it is the right shape for *some* retrieval problems. It is the wrong shape for the problems that come up when a retrieval system is asked to ship to legal, medical, financial, or other enterprise knowledge workflows.

Three failure modes appear over and over:

1. **Contradiction is invisible.** A chunk and its opposite can both be in the top *k*. The system has no concept of "this source says the opposite of that source" — it has a single similarity score. So a downstream model is free to confidently assert the *wrong* of the two.

2. **Time is invisible.** A fact has a validity window. A chunk retrieved from a 2019 article is not the same fact as a chunk retrieved from a 2024 article, but a vector database does not know that. A `time_range` filter is a *workaround* in some systems, not a first-class field on the data.

3. **Provenance is opaque.** When a model says something confidently, the auditor asks "where did that come from?" A vanilla vector database can answer "we found a chunk whose vector was close to the query" — which is not an answer. The auditor wants the `source_id`, the `chunk_id`, and ideally the character span.

DASH is built around the claim that **a defensible retrieval system is one that can answer all three questions on every result**.

## The claim/evidence/contradiction model

DASH treats the **claim** — an atomic, source-bound assertion — as the primary data primitive, with **evidence** and **citation** as first-class fields on every result.

```text
Claim
  ├── canonical_text           the atomic assertion
  ├── confidence               0..1, model- or human-assigned
  ├── event_time_unix          when the underlying event happened
  ├── valid_from / valid_to    the temporal validity window
  └── evidence[]               the records that justify or oppose it
        ├── source_id          which document / URL / dataset
        ├── stance             supports | contradicts | neutral
        ├── source_quality     0..1, trust in the source
        ├── chunk_id           optional, for character-level cites
        └── span_start/end     optional, byte offsets inside the chunk
```

Every retrieval response is shaped:

```json
{
  "claim": { ... },
  "score": 0.93,
  "supports": 3,
  "contradicts": 0,
  "citations": [
    { "source_id": "...", "stance": "supports", "source_quality": 0.95, ... }
  ]
}
```

A `contradicts` count above zero demotes the claim's score; a retrieval-time `stance_mode: support_only` filter removes it from the response entirely. The client decides what to do with that — but the information is *there*, on every result, in a stable shape.

## Real-world use cases

### Legal — case law and regulatory citations

A junior associate asks "what did the court decide on issue *X*?" The retrieval system returns the *holding* (a `Claim`) plus the *citations* in the brief that support it (`Evidence`). If a later brief contradicts the holding, the contradicting evidence shows up on the result — and a `stance_mode: support_only` query simply omits the overruled claims.

### Medical — clinical decision support

A clinician asks "what is the first-line treatment for condition *Y* as of 2024?" The `valid_from` / `valid_to` window constrains the result to current guidelines, the `event_time_unix` lets the system prefer recent studies, and the `source_id` (`pubmed://...`, `clinicaltrials.gov://...`) is the audit trail the regulator wants.

### Financial — filings and disclosures

A compliance analyst asks "what did company *X* disclose about risk *Z* in its 10-K?" The retrieval returns the *claim* and the *filing* it came from. If a later 10-K retracts the claim, the contradicting evidence is on the result — the analyst can see the timeline without leaving the response.

### Enterprise knowledge — wiki and runbook

A new engineer asks "how do I deploy service *F*?" The runbook is a `Claim`; the steps are the `Evidence` with `stance: supports`. If a follow-up incident post-mortem contradicts a step ("do *not* do X"), the contradicting post-mortem shows up on the result, and the engineer sees the correction.

## What DASH is *not*

DASH is not the fastest pure-vector index. It is not a metadata store, a feature store, or a graph database. It is not a general-purpose search engine.

DASH is a **defensible vector database** for retrieval-augmented generation that has to ship to an environment where "we found a similar chunk" is not an acceptable answer. That is the audience it is built for.
