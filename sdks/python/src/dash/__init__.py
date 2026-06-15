"""dash-py — idiomatic Python client for the DASH retrieval engine.

This package is a thin, typed wrapper around DASH's HTTP API. The
public surface area is intentionally small: a :class:`Client` for
synchronous code, an :class:`AsyncClient` for asyncio, and a handful of
typed dataclasses in :mod:`dash.types` for the request/response shapes.

Quickstart:

    >>> from dash import Client
    >>> client = Client(base_url="http://localhost:8080")
    >>> response = client.embeddings.create("hello world")
    >>> len(response.data[0].embedding)
    768
"""

from __future__ import annotations

from ._version import __version__
from .async_client import AsyncClient, AsyncEmbeddingsNamespace
from .client import Client, EmbeddingsNamespace
from .errors import DashAPIError, DashConnectionError, DashError
from .types import (
    Citation,
    EmbeddingData,
    EmbeddingRequest,
    EmbeddingResponse,
    EmbeddingUsage,
    RetrieveRequest,
    RetrieveResponse,
    RetrieveResult,
)

__all__ = [
    # Version
    "__version__",
    # Clients
    "AsyncClient",
    "AsyncEmbeddingsNamespace",
    "Client",
    "EmbeddingsNamespace",
    # Errors
    "DashAPIError",
    "DashConnectionError",
    "DashError",
    # Types
    "Citation",
    "EmbeddingData",
    "EmbeddingRequest",
    "EmbeddingResponse",
    "EmbeddingUsage",
    "RetrieveRequest",
    "RetrieveResponse",
    "RetrieveResult",
]
