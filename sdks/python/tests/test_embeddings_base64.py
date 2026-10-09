"""``encoding_format="base64"`` and ``dimensions`` support for /v1/embeddings.

The base64 fixture is the exact shape the server emits: the float32 values
packed little-endian and encoded with the standard alphabet (with padding).
``AACAPwAAAMAAAGBAAAAAPg==`` is ``[1.0, -2.0, 3.5, 0.125]``.
"""

from __future__ import annotations

import base64
import struct
from typing import Any, Dict

import pytest
import requests

from dash import Client
from dash.types import EmbeddingData, EmbeddingRequest, EmbeddingResponse

from tests.conftest import MockResponse

FLOATS = [1.0, -2.0, 3.5, 0.125]
SERVER_BASE64 = "AACAPwAAAMAAAGBAAAAAPg=="


def _response(embedding: Any) -> Dict[str, Any]:
    return {
        "object": "list",
        "data": [{"object": "embedding", "embedding": embedding, "index": 0}],
        "model": "m",
        "usage": {"prompt_tokens": 1, "total_tokens": 1},
    }


def test_fixture_is_the_servers_little_endian_f32_encoding() -> None:
    assert base64.b64encode(struct.pack("<4f", *FLOATS)).decode() == SERVER_BASE64


def test_embedding_data_decodes_base64_string_into_floats() -> None:
    data = EmbeddingData.from_dict(
        {"object": "embedding", "embedding": SERVER_BASE64, "index": 0}
    )
    assert data.embedding == FLOATS


def test_embedding_data_keeps_float_lists_unchanged() -> None:
    data = EmbeddingData.from_dict({"embedding": [0.5, 0.25], "index": 3})
    assert data.embedding == [0.5, 0.25]
    assert data.index == 3


def test_embedding_data_rejects_malformed_base64() -> None:
    with pytest.raises(ValueError):
        EmbeddingData.from_dict({"embedding": "not base64!!", "index": 0})
    # 5 bytes is not a whole number of float32 values.
    with pytest.raises(ValueError):
        EmbeddingData.from_dict(
            {"embedding": base64.b64encode(b"12345").decode(), "index": 0}
        )


def test_create_with_base64_returns_float_lists(
    base_url: str, mocker: pytest.MockFixture
) -> None:
    spy = mocker.patch.object(
        requests.Session,
        "request",
        return_value=MockResponse(200, _response(SERVER_BASE64)),
    )
    client = Client(base_url=base_url)
    try:
        response = client.embeddings.create("hi", encoding_format="base64")
    finally:
        client.close()
    assert isinstance(response, EmbeddingResponse)
    assert response.data[0].embedding == FLOATS
    assert spy.call_args.kwargs["json"]["encoding_format"] == "base64"


def test_dimensions_is_sent_only_when_provided(
    base_url: str, mocker: pytest.MockFixture
) -> None:
    spy = mocker.patch.object(
        requests.Session,
        "request",
        return_value=MockResponse(200, _response([0.5, 0.25])),
    )
    client = Client(base_url=base_url)
    try:
        client.embeddings.create("hi", dimensions=2)
        assert spy.call_args.kwargs["json"]["dimensions"] == 2
        client.embeddings.create("hi")
        assert "dimensions" not in spy.call_args.kwargs["json"]
    finally:
        client.close()
    assert EmbeddingRequest(input="x", dimensions=8).to_dict()["dimensions"] == 8
    assert EmbeddingRequest.from_dict({"input": "x", "dimensions": 8}).dimensions == 8
