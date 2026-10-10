"""Async counterpart of ``test_embeddings_base64``."""

from __future__ import annotations

from unittest.mock import AsyncMock

import httpx
import pytest

from dash import AsyncClient
from tests.conftest import MockResponse

SERVER_BASE64 = "AACAPwAAAMAAAGBAAAAAPg=="  # [1.0, -2.0, 3.5, 0.125] little-endian f32

pytestmark = pytest.mark.asyncio


async def test_async_create_decodes_base64_and_sends_dimensions(
    base_url: str, mocker: pytest.MockFixture
) -> None:
    payload = {
        "object": "list",
        "data": [{"object": "embedding", "embedding": SERVER_BASE64, "index": 0}],
        "model": "m",
        "usage": {"prompt_tokens": 1, "total_tokens": 1},
    }
    spy = mocker.patch.object(
        httpx.AsyncClient,
        "request",
        new=AsyncMock(return_value=MockResponse(200, payload)),
    )
    client = AsyncClient(base_url=base_url)
    try:
        response = await client.embeddings.create(
            "hi", encoding_format="base64", dimensions=4
        )
    finally:
        await client.close()
    assert response.data[0].embedding == [1.0, -2.0, 3.5, 0.125]
    body = spy.call_args.kwargs["json"]
    assert body["dimensions"] == 4
    assert body["encoding_format"] == "base64"
