"""Typed request and response models for the DASH retrieval engine.

These dataclasses are stdlib only (no pydantic) and mirror the wire
shapes documented in:

- ``services/retrieval/src/openai_embeddings.rs`` for the OpenAI-compatible
  ``/v1/embeddings`` endpoint.
- ``services/retrieval/src/transport/payload.rs`` for the native
  ``/v1/retrieve`` request parser and response renderer.
"""

from __future__ import annotations

import base64
import binascii
import struct
from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional, Union


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
    dimensions: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        body: Dict[str, Any] = {"input": self.input, "model": self.model}
        if self.encoding_format is not None:
            body["encoding_format"] = self.encoding_format
        if self.user is not None:
            body["user"] = self.user
        if self.dimensions is not None:
            body["dimensions"] = self.dimensions
        return body

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "EmbeddingRequest":
        return cls(
            input=data["input"],
            model=data.get("model", "text-embedding-3-small"),
            encoding_format=data.get("encoding_format"),
            user=data.get("user"),
            dimensions=data.get("dimensions"),
        )


def decode_base64_embedding(encoded: str) -> List[float]:
    """Decode the server's ``encoding_format="base64"`` embedding.

    The payload is the float32 components packed little-endian and encoded
    with the standard base64 alphabet. Raises :class:`ValueError` for input
    that is not valid base64 or not a whole number of float32 values.
    """
    try:
        raw = base64.b64decode(encoded, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise ValueError(f"embedding is not valid base64: {exc}") from exc
    if len(raw) % 4 != 0:
        raise ValueError(
            f"base64 embedding has {len(raw)} bytes, not a multiple of 4"
        )
    return list(struct.unpack(f"<{len(raw) // 4}f", raw))


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
        raw = data["embedding"]
        # ``encoding_format="base64"`` makes the server return a string.
        embedding = decode_base64_embedding(raw) if isinstance(raw, str) else list(raw)
        return cls(
            embedding=embedding,
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
class TimeRange:
    """Inclusive unix-second window for ``time_range`` (either bound optional)."""

    from_unix: Optional[int] = None
    to_unix: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        body: Dict[str, Any] = {}
        if self.from_unix is not None:
            body["from_unix"] = self.from_unix
        if self.to_unix is not None:
            body["to_unix"] = self.to_unix
        return body

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "TimeRange":
        return cls(from_unix=data.get("from_unix"), to_unix=data.get("to_unix"))


@dataclass
class RetrieveRequest:
    """Request body for ``POST /v1/retrieve``.

    Mirrors the JSON accepted by ``build_retrieve_transport_request_from_json``
    in ``services/retrieval/src/transport/payload.rs``. Only ``tenant_id``
    and ``query`` are required; optional fields that are ``None`` are
    omitted from the body so server defaults apply (``top_k`` 5,
    ``stance_mode`` ``balanced``, ``return_graph`` false).
    """

    tenant_id: str
    query: str
    top_k: int = 5
    stance_mode: str = "balanced"
    return_graph: Optional[bool] = None
    query_embedding: Optional[List[float]] = None
    entity_filters: Optional[List[str]] = None
    embedding_id_filters: Optional[List[str]] = None
    time_range: Optional[Union[TimeRange, Mapping[str, Any]]] = None
    read_consistency: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        body: Dict[str, Any] = {
            "tenant_id": self.tenant_id,
            "query": self.query,
            "top_k": self.top_k,
            "stance_mode": self.stance_mode,
        }
        if self.return_graph is not None:
            body["return_graph"] = self.return_graph
        if self.query_embedding is not None:
            body["query_embedding"] = [float(v) for v in self.query_embedding]
        if self.entity_filters is not None:
            body["entity_filters"] = list(self.entity_filters)
        if self.embedding_id_filters is not None:
            body["embedding_id_filters"] = list(self.embedding_id_filters)
        if self.time_range is not None:
            tr = self.time_range
            body["time_range"] = (
                tr.to_dict() if isinstance(tr, TimeRange) else dict(tr)
            )
        if self.read_consistency is not None:
            body["read_consistency"] = self.read_consistency
        return body

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RetrieveRequest":
        time_range = data.get("time_range")
        return cls(
            tenant_id=data["tenant_id"],
            query=data["query"],
            top_k=int(data.get("top_k", 5)),
            stance_mode=data.get("stance_mode", "balanced"),
            return_graph=data.get("return_graph"),
            query_embedding=data.get("query_embedding"),
            entity_filters=data.get("entity_filters"),
            embedding_id_filters=data.get("embedding_id_filters"),
            time_range=TimeRange.from_dict(time_range) if time_range else None,
            read_consistency=data.get("read_consistency"),
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


def _opt(data: Mapping[str, Any], key: str, cast: Any) -> Any:
    value = data.get(key)
    return None if value is None else cast(value)


@dataclass
class RetrieveResult:
    """A single claim returned by ``/v1/retrieve``.

    Mirrors ``render_evidence_node_json`` in
    ``services/retrieval/src/transport/payload.rs``. ``supports`` and
    ``contradicts`` give the stance tally for the claim without walking
    citations. Fields after ``citations`` are optional and ``None`` when
    the server omits them or sends ``null``; unknown fields are ignored.
    """

    claim_id: str
    canonical_text: str
    score: float
    supports: int
    contradicts: int
    citations: List[Citation] = field(default_factory=list)
    claim_confidence: Optional[float] = None
    confidence_band: Optional[str] = None
    dominant_stance: Optional[str] = None
    contradiction_risk: Optional[float] = None
    graph_score: Optional[float] = None
    support_path_count: Optional[int] = None
    contradiction_chain_depth: Optional[int] = None
    event_time_unix: Optional[int] = None
    temporal_match_mode: Optional[str] = None
    temporal_in_range: Optional[bool] = None
    claim_type: Optional[str] = None
    valid_from: Optional[int] = None
    valid_to: Optional[int] = None
    created_at: Optional[int] = None
    updated_at: Optional[int] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RetrieveResult":
        return cls(
            claim_id=data["claim_id"],
            canonical_text=data["canonical_text"],
            score=float(data["score"]),
            supports=int(data.get("supports", 0)),
            contradicts=int(data.get("contradicts", 0)),
            citations=[Citation.from_dict(c) for c in data.get("citations") or []],
            claim_confidence=_opt(data, "claim_confidence", float),
            confidence_band=data.get("confidence_band"),
            dominant_stance=data.get("dominant_stance"),
            contradiction_risk=_opt(data, "contradiction_risk", float),
            graph_score=_opt(data, "graph_score", float),
            support_path_count=_opt(data, "support_path_count", int),
            contradiction_chain_depth=_opt(data, "contradiction_chain_depth", int),
            event_time_unix=_opt(data, "event_time_unix", int),
            temporal_match_mode=data.get("temporal_match_mode"),
            temporal_in_range=data.get("temporal_in_range"),
            claim_type=data.get("claim_type"),
            valid_from=_opt(data, "valid_from", int),
            valid_to=_opt(data, "valid_to", int),
            created_at=_opt(data, "created_at", int),
            updated_at=_opt(data, "updated_at", int),
        )


@dataclass
class GraphEdge:
    """An edge of the evidence graph (``return_graph=True``)."""

    from_claim_id: str
    to_claim_id: str
    relation: str
    strength: float

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "GraphEdge":
        return cls(
            from_claim_id=data["from_claim_id"],
            to_claim_id=data["to_claim_id"],
            relation=data["relation"],
            strength=float(data["strength"]),
        )


@dataclass
class RetrieveGraph:
    """Evidence graph returned when ``return_graph=True``."""

    nodes: List[RetrieveResult] = field(default_factory=list)
    edges: List[GraphEdge] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RetrieveGraph":
        return cls(
            nodes=[RetrieveResult.from_dict(n) for n in data.get("nodes") or []],
            edges=[GraphEdge.from_dict(e) for e in data.get("edges") or []],
        )


@dataclass
class RetrieveResponse:
    """Response body for ``POST /v1/retrieve``.

    Wire format (``render_retrieve_response_json``) is
    ``{"results": [...], "graph": ..., "read_policy": ...,
    "read_quorum_met": ..., "serving_replica": ...}``. Everything except
    ``results`` is optional.
    """

    results: List[RetrieveResult]
    graph: Optional[RetrieveGraph] = None
    read_policy: Optional[str] = None
    read_quorum_met: Optional[bool] = None
    serving_replica: Optional[str] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RetrieveResponse":
        graph = data.get("graph")
        return cls(
            results=[RetrieveResult.from_dict(r) for r in data.get("results") or []],
            graph=RetrieveGraph.from_dict(graph) if graph else None,
            read_policy=data.get("read_policy"),
            read_quorum_met=data.get("read_quorum_met"),
            serving_replica=data.get("serving_replica"),
        )


__all__ = [
    "Citation",
    "EmbeddingData",
    "EmbeddingRequest",
    "EmbeddingResponse",
    "EmbeddingUsage",
    "GraphEdge",
    "RetrieveGraph",
    "RetrieveRequest",
    "RetrieveResponse",
    "RetrieveResult",
    "TimeRange",
]
