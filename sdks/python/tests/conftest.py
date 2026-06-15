"""Shared pytest fixtures for the dash-py test suite.

The fixtures only depend on stdlib + pytest, so the suite can run in
any environment without a live DASH instance. Each test patches the
HTTP transport at the boundary (``requests.Session.request`` or
``httpx.AsyncClient.request``) and feeds in canned responses.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List

import pytest


# ---------------------------------------------------------------------------
# Wire-format fixtures (exactly what the Rust server emits)
# ---------------------------------------------------------------------------


@pytest.fixture
def sample_embedding_response() -> Dict[str, Any]:
    """An OpenAI-compatible embedding response (single string input)."""
    return {
        "object": "list",
        "data": [
            {
                "object": "embedding",
                "embedding": [0.013, -0.042, 0.077, 0.0, 0.5],
                "index": 0,
            }
        ],
        "model": "text-embedding-3-small",
        "usage": {"prompt_tokens": 2, "total_tokens": 2},
    }


@pytest.fixture
def sample_embedding_array_response() -> Dict[str, Any]:
    """An OpenAI-compatible embedding response for a 3-string array."""
    return {
        "object": "list",
        "data": [
            {
                "object": "embedding",
                "embedding": [0.1, 0.2, 0.3],
                "index": 0,
            },
            {
                "object": "embedding",
                "embedding": [0.4, 0.5, 0.6],
                "index": 1,
            },
            {
                "object": "embedding",
                "embedding": [0.7, 0.8, 0.9],
                "index": 2,
            },
        ],
        "model": "text-embedding-3-small",
        "usage": {"prompt_tokens": 6, "total_tokens": 6},
    }


@pytest.fixture
def sample_retrieve_response() -> Dict[str, Any]:
    """A native /v1/retrieve response with one claim + citation."""
    return {
        "results": [
            {
                "claim_id": "claim-1",
                "canonical_text": "Acme Co. was acquired in 2024.",
                "score": 0.93,
                "supports": 4,
                "contradicts": 1,
                "citations": [
                    {
                        "evidence_id": "ev-1",
                        "source_id": "source://reuters",
                        "stance": "supports",
                        "source_quality": 0.88,
                        "chunk_id": "chunk-7",
                        "span_start": 120,
                        "span_end": 168,
                        "doc_id": "doc://reuters-acme",
                        "extraction_model": "extractor-v5",
                        "ingested_at": 1735689700000,
                    }
                ],
            }
        ]
    }


@pytest.fixture
def openai_style_error_body() -> Dict[str, Any]:
    """An OpenAI-shaped error body as emitted by DASH."""
    return {
        "error": {
            "message": "input must contain at least one text",
            "type": "invalid_request_error",
            "param": None,
            "code": None,
        }
    }


@pytest.fixture
def base_url() -> str:
    return "http://localhost:8080"


# ---------------------------------------------------------------------------
# Mock response helper
# ---------------------------------------------------------------------------


class MockResponse:
    """Tiny stand-in for ``requests.Response`` / ``httpx.Response``.

    Both libraries expose the same handful of attributes our client
    touches (``status_code``, ``text``, ``content``) and the same
    shape, so a single helper class is enough for both transports.
    """

    def __init__(
        self,
        status_code: int,
        body: Any = None,
        *,
        raw_text: str = "",
    ) -> None:
        self.status_code = status_code
        if raw_text:
            self.text = raw_text
        elif body is None:
            self.text = ""
        else:
            self.text = json.dumps(body)
        # requests exposes ``content`` as bytes; mirror that so tests
        # covering the non-JSON fallback path can assert on it.
        self.content = self.text.encode("utf-8") if self.text else b""

    def json(self) -> Any:  # pragma: no cover - convenience, not used here
        return json.loads(self.text)
