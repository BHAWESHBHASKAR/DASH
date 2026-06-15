"""Typed request and response models for the DASH retrieval engine.

These dataclasses are stdlib only (no pydantic) and mirror the wire
shapes documented in:

- ``services/retrieval/src/openai_embeddings.rs`` for the OpenAI-compatible
  ``/v1/embeddings`` endpoint.
- ``pkg/schema/src/lib.rs`` (``RetrievalResult`` / ``Citation``) and
  ``services/retrieval/src/transport/tests.rs`` for the native
  ``/v1/retrieve`` endpoint.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Union


# ---------------------------------------------------------------------------
# OpenAI-compatible /v1/embeddings
# ---------------------------------------------------------------------------


@dataclass
class EmbeddingRequest:
    """Request body for ``POST /v1/embeddings``.

    Mirrors ``OpenAIEmbeddingsRequest`` in
    ``services/retrieval/src/openai_embeddings.rs``.
    """

    input: Union[str, List[str]]
    model: str = "text-embedding-3-small"
    encoding_format: Optional[str] = None
    user: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        body: Dict[str, Any] = {"input": self.input, "model": self.model}
        if self.encoding_format is not None:
            body["encoding_format"] = self.encoding_format
        if self.user is not None:
            body["user"] = self.user
        return body

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "EmbeddingRequest":
        return cls(
            input=data["input"],
            model=data.get("model", "text-embedding-3-small"),
            encoding_format=data.get("encoding_format"),
            user=data.get("user"),
        )


@dataclass
class EmbeddingData:
    """Single embedding record from a response.

    Mirrors ``OpenAIEmbeddingData``.
    """

    embedding: List[float]
    index: int
    object: str = "embedding"

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "EmbeddingData":
        return cls(
            embedding=list(data["embedding"]),
            index=int(data["index"]),
            object=data.get("object", "embedding"),
        )


@dataclass
class EmbeddingUsage:
    """Token usage block from a response.

    Mirrors ``OpenAIUsage``.
    """

    prompt_tokens: int
    total_tokens: int

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "EmbeddingUsage":
        return cls(
            prompt_tokens=int(data["prompt_tokens"]),
            total_tokens=int(data["total_tokens"]),
        )


@dataclass
class EmbeddingResponse:
    """Response body for ``POST /v1/embeddings``.

    Mirrors ``OpenAIEmbeddingsResponse``.
    """

    object: str
    data: List[EmbeddingData]
    model: str
    usage: EmbeddingUsage

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "EmbeddingResponse":
        return cls(
            object=data["object"],
            data=[EmbeddingData.from_dict(d) for d in data.get("data", [])],
            model=data["model"],
            usage=EmbeddingUsage.from_dict(data["usage"]),
        )


# ---------------------------------------------------------------------------
# Native /v1/retrieve
# ---------------------------------------------------------------------------


@dataclass
class RetrieveRequest:
    """Request body for ``POST /v1/retrieve``.

    Mirrors ``schema::RetrievalRequest`` and the test JSON in
    ``services/retrieval/src/transport/tests.rs``:

        {"tenant_id": "...", "query": "...",
         "top_k": 10, "stance_mode": "balanced",
         "return_graph": false}
    """

    tenant_id: str
    query: str
    top_k: int = 10
    stance_mode: str = "balanced"
    return_graph: Optional[bool] = None

    def to_dict(self) -> Dict[str, Any]:
        body: Dict[str, Any] = {
            "tenant_id": self.tenant_id,
            "query": self.query,
            "top_k": self.top_k,
            "stance_mode": self.stance_mode,
        }
        if self.return_graph is not None:
            body["return_graph"] = self.return_graph
        return body

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RetrieveRequest":
        return cls(
            tenant_id=data["tenant_id"],
            query=data["query"],
            top_k=int(data.get("top_k", 10)),
            stance_mode=data.get("stance_mode", "balanced"),
            return_graph=data.get("return_graph"),
        )


@dataclass
class Citation:
    """A citation attached to a retrieval result.

    Mirrors ``schema::Citation``. Kept as a typed dataclass so callers
    can introspect fields like ``stance`` and ``source_quality`` without
    reaching into raw dicts.
    """

    evidence_id: str
    source_id: str
    stance: str
    source_quality: float
    chunk_id: Optional[str] = None
    span_start: Optional[int] = None
    span_end: Optional[int] = None
    doc_id: Optional[str] = None
    extraction_model: Optional[str] = None
    ingested_at: Optional[int] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Citation":
        return cls(
            evidence_id=data["evidence_id"],
            source_id=data["source_id"],
            stance=data["stance"],
            source_quality=float(data["source_quality"]),
            chunk_id=data.get("chunk_id"),
            span_start=(int(data["span_start"]) if data.get("span_start") is not None else None),
            span_end=(int(data["span_end"]) if data.get("span_end") is not None else None),
            doc_id=data.get("doc_id"),
            extraction_model=data.get("extraction_model"),
            ingested_at=(int(data["ingested_at"]) if data.get("ingested_at") is not None else None),
        )


@dataclass
class RetrieveResult:
    """A single claim returned by ``/v1/retrieve``.

    Mirrors ``schema::RetrievalResult``. The ``Claim + Evidence +
    Contradiction`` differentiator lives here: ``supports`` and
    ``contradicts`` give the caller the stance tally for the claim
    without having to walk citations manually.
    """

    claim_id: str
    canonical_text: str
    score: float
    supports: int
    contradicts: int
    citations: List[Citation] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RetrieveResult":
        return cls(
            claim_id=data["claim_id"],
            canonical_text=data["canonical_text"],
            score=float(data["score"]),
            supports=int(data.get("supports", 0)),
            contradicts=int(data.get("contradicts", 0)),
            citations=[Citation.from_dict(c) for c in data.get("citations", [])],
        )


@dataclass
class RetrieveResponse:
    """Response body for ``POST /v1/retrieve``.

    Wire format is ``{"results": [...]}``; the typed wrapper makes the
    field discoverable and lets us add aggregate helpers later without
    breaking the wire contract.
    """

    results: List[RetrieveResult]

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RetrieveResponse":
        return cls(
            results=[RetrieveResult.from_dict(r) for r in data.get("results", [])],
        )


__all__ = [
    "Citation",
    "EmbeddingData",
    "EmbeddingRequest",
    "EmbeddingResponse",
    "EmbeddingUsage",
    "RetrieveRequest",
    "RetrieveResponse",
    "RetrieveResult",
]
