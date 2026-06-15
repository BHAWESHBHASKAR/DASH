"""Tests for the asynchronous :class:`dash.AsyncClient`.

The HTTP transport is mocked at the ``httpx.AsyncClient.request``
boundary using an ``AsyncMock`` so the suite does not require a live
DASH instance or an event loop bound to a real socket.
"""

from __future__ import annotations

import json
from typing import Any, Dict
from unittest.mock import AsyncMock

import httpx
import pytest

from dash import (
    AsyncClient,
    DashAPIError,
    DashConnectionError,
    EmbeddingResponse,
    RetrieveResponse,
)
from tests.conftest import MockResponse


pytestmark = pytest.mark.asyncio


def _patch_async_request(mocker: pytest.MockFixture, mock_response: MockResponse) -> AsyncMock:
    """Patch ``httpx.AsyncClient.request`` to return *mock_response*.

    Returns the mock so individual tests can assert on call args.
    """
    return mocker.patch.object(
        httpx.AsyncClient,
        "request",
        new=AsyncMock(return_value=mock_response),
    )


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


async def test_async_client_strips_trailing_slash() -> None:
    client = AsyncClient(base_url="http://localhost:8080/")
    assert client.base_url == "http://localhost:8080"


async def test_async_client_rejects_empty_base_url() -> None:
    with pytest.raises(ValueError):
        AsyncClient(base_url="")


async def test_async_client_rejects_non_positive_timeout() -> None:
    with pytest.raises(ValueError):
        AsyncClient(timeout=0)


async def test_async_client_lazy_creates_httpx_client() -> None:
    client = AsyncClient(base_url="http://localhost:8080")
    assert client._client is None
    await client.close()  # must not raise even though _client is None


# ---------------------------------------------------------------------------
# Context manager
# ---------------------------------------------------------------------------


async def test_async_client_works_as_context_manager(
    base_url: str,
    mocker: pytest.MockFixture,
    sample_embedding_response: Dict[str, Any],
) -> None:
    _patch_async_request(mocker, MockResponse(200, sample_embedding_response))

    async with AsyncClient(base_url=base_url) as client:
        response = await client.embeddings.create("hi")

    assert response.data[0].embedding[0] == pytest.approx(0.013)
    assert client._client is None  # closed on exit


async def test_async_client_close_is_idempotent(base_url: str) -> None:
    client = AsyncClient(base_url=base_url)
    await client.close()
    await client.close()  # must not raise


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------


async def test_async_client_sends_bearer_token(
    base_url: str,
    mocker: pytest.MockFixture,
    sample_embedding_response: Dict[str, Any],
) -> None:
    spy = _patch_async_request(mocker, MockResponse(200, sample_embedding_response))
    client = AsyncClient(base_url=base_url, api_key="sk-async")
    try:
        await client.embeddings.create("hi")
    finally:
        await client.close()
    assert spy.call_args.kwargs["headers"]["Authorization"] == "Bearer sk-async"


async def test_async_client_omits_authorization_when_no_api_key(
    base_url: str,
    mocker: pytest.MockFixture,
    sample_embedding_response: Dict[str, Any],
) -> None:
    spy = _patch_async_request(mocker, MockResponse(200, sample_embedding_response))
    client = AsyncClient(base_url=base_url)
    try:
        await client.embeddings.create("hi")
    finally:
        await client.close()
    assert "Authorization" not in spy.call_args.kwargs["headers"]


# ---------------------------------------------------------------------------
# Embeddings: success
# ---------------------------------------------------------------------------


async def test_async_embeddings_create_single_string(
    base_url: str,
    mocker: pytest.MockFixture,
    sample_embedding_response: Dict[str, Any],
) -> None:
    spy = _patch_async_request(mocker, MockResponse(200, sample_embedding_response))
    client = AsyncClient(base_url=base_url)
    try:
        response = await client.embeddings.create("hello world")
    finally:
        await client.close()

    assert isinstance(response, EmbeddingResponse)
    assert response.data[0].embedding[0] == pytest.approx(0.013)
    body = spy.call_args.kwargs["json"]
    assert body == {"input": "hello world", "model": "text-embedding-3-small"}


async def test_async_embeddings_create_array(
    base_url: str,
    mocker: pytest.MockFixture,
    sample_embedding_array_response: Dict[str, Any],
) -> None:
    spy = _patch_async_request(mocker, MockResponse(200, sample_embedding_array_response))
    client = AsyncClient(base_url=base_url)
    try:
        response = await client.embeddings.create(["a", "b", "c"])
    finally:
        await client.close()
    assert len(response.data) == 3
    assert spy.call_args.kwargs["json"]["input"] == ["a", "b", "c"]


async def test_async_embeddings_create_hits_correct_url(
    base_url: str,
    mocker: pytest.MockFixture,
    sample_embedding_response: Dict[str, Any],
) -> None:
    spy = _patch_async_request(mocker, MockResponse(200, sample_embedding_response))
    client = AsyncClient(base_url=base_url)
    try:
        await client.embeddings.create("hi")
    finally:
        await client.close()
    assert spy.call_args.args[0] == "POST"
    assert spy.call_args.args[1] == "http://localhost:8080/v1/embeddings"


# ---------------------------------------------------------------------------
# Embeddings: errors
# ---------------------------------------------------------------------------


async def test_async_embeddings_create_raises_api_error_on_4xx(
    base_url: str,
    mocker: pytest.MockFixture,
    openai_style_error_body: Dict[str, Any],
) -> None:
    _patch_async_request(mocker, MockResponse(400, openai_style_error_body))
    client = AsyncClient(base_url=base_url)
    try:
        with pytest.raises(DashAPIError) as exc_info:
            await client.embeddings.create("hi")
    finally:
        await client.close()
    assert exc_info.value.status_code == 400
    assert exc_info.value.error_type == "invalid_request_error"


async def test_async_embeddings_create_raises_connection_error_on_timeout(
    base_url: str, mocker: pytest.MockFixture
) -> None:
    mocker.patch.object(
        httpx.AsyncClient,
        "request",
        new=AsyncMock(side_effect=httpx.TimeoutException("slow")),
    )
    client = AsyncClient(base_url=base_url)
    try:
        with pytest.raises(DashConnectionError):
            await client.embeddings.create("hi")
    finally:
        await client.close()


async def test_async_embeddings_create_raises_connection_error_on_connect_error(
    base_url: str, mocker: pytest.MockFixture
) -> None:
    mocker.patch.object(
        httpx.AsyncClient,
        "request",
        new=AsyncMock(side_effect=httpx.ConnectError("refused")),
    )
    client = AsyncClient(base_url=base_url)
    try:
        with pytest.raises(DashConnectionError):
            await client.embeddings.create("hi")
    finally:
        await client.close()


async def test_async_embeddings_create_raises_connection_error_on_request_error(
    base_url: str, mocker: pytest.MockFixture
) -> None:
    mocker.patch.object(
        httpx.AsyncClient,
        "request",
        new=AsyncMock(side_effect=httpx.RequestError("ssl")),
    )
    client = AsyncClient(base_url=base_url)
    try:
        with pytest.raises(DashConnectionError):
            await client.embeddings.create("hi")
    finally:
        await client.close()


# ---------------------------------------------------------------------------
# Retrieve: success
# ---------------------------------------------------------------------------


async def test_async_retrieve_default_stance_mode(
    base_url: str,
    mocker: pytest.MockFixture,
    sample_retrieve_response: Dict[str, Any],
) -> None:
    spy = _patch_async_request(mocker, MockResponse(200, sample_retrieve_response))
    client = AsyncClient(base_url=base_url)
    try:
        response = await client.retrieve("tenant-a", "company x")
    finally:
        await client.close()
    body = spy.call_args.kwargs["json"]
    assert body["stance_mode"] == "balanced"
    assert body["top_k"] == 10
    assert isinstance(response, RetrieveResponse)
    assert len(response.results) == 1


async def test_async_retrieve_support_only(
    base_url: str,
    mocker: pytest.MockFixture,
    sample_retrieve_response: Dict[str, Any],
) -> None:
    spy = _patch_async_request(mocker, MockResponse(200, sample_retrieve_response))
    client = AsyncClient(base_url=base_url)
    try:
        await client.retrieve("tenant-a", "q", top_k=5, stance_mode="support_only")
    finally:
        await client.close()
    body = spy.call_args.kwargs["json"]
    assert body["stance_mode"] == "support_only"
    assert body["top_k"] == 5


async def test_async_retrieve_hits_correct_url(
    base_url: str,
    mocker: pytest.MockFixture,
    sample_retrieve_response: Dict[str, Any],
) -> None:
    spy = _patch_async_request(mocker, MockResponse(200, sample_retrieve_response))
    client = AsyncClient(base_url=base_url)
    try:
        await client.retrieve("tenant-a", "q")
    finally:
        await client.close()
    assert spy.call_args.args[0] == "POST"
    assert spy.call_args.args[1] == "http://localhost:8080/v1/retrieve"


# ---------------------------------------------------------------------------
# Concurrency smoke test
# ---------------------------------------------------------------------------


async def test_concurrent_embedding_requests_all_succeed(
    base_url: str,
    mocker: pytest.MockFixture,
    sample_embedding_response: Dict[str, Any],
) -> None:
    """``AsyncClient`` is safe to use with ``asyncio.gather``."""
    import asyncio

    _patch_async_request(mocker, MockResponse(200, sample_embedding_response))
    client = AsyncClient(base_url=base_url)
    try:
        results = await asyncio.gather(
            *(client.embeddings.create(f"text-{i}") for i in range(5))
        )
    finally:
        await client.close()
    assert len(results) == 5
    assert all(isinstance(r, EmbeddingResponse) for r in results)
