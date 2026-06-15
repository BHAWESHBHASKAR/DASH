"""Tests for the synchronous :class:`dash.Client`.

The HTTP transport is mocked at the ``requests.Session.request``
boundary, so no live DASH instance is required.
"""

from __future__ import annotations

import json
from typing import Any, Dict, Optional

import pytest
import requests

from dash import (
    Client,
    DashAPIError,
    DashConnectionError,
    DashError,
    EmbeddingData,
    EmbeddingRequest,
    EmbeddingResponse,
    EmbeddingUsage,
    RetrieveResponse,
    RetrieveResult,
)
from dash.client import _USER_AGENT
from tests.conftest import MockResponse


# ---------------------------------------------------------------------------
# Construction & basic plumbing
# ---------------------------------------------------------------------------


def test_client_strips_trailing_slash_from_base_url() -> None:
    client = Client(base_url="http://localhost:8080/")
    assert client.base_url == "http://localhost:8080"


def test_client_rejects_empty_base_url() -> None:
    with pytest.raises(ValueError):
        Client(base_url="")


def test_client_rejects_non_positive_timeout() -> None:
    with pytest.raises(ValueError):
        Client(base_url="http://localhost:8080", timeout=0)
    with pytest.raises(ValueError):
        Client(base_url="http://localhost:8080", timeout=-1)


def test_client_sends_dash_py_user_agent_on_every_request(
    base_url: str,
    mocker: pytest.MockFixture,
    sample_embedding_response: Dict[str, Any],
) -> None:
    """The User-Agent must travel on the wire as ``dash-py/<version>``,
    not as ``python-requests/<version>`` (the requests default).
    """
    spy = mocker.patch.object(
        requests.Session, "request", return_value=MockResponse(200, sample_embedding_response)
    )
    client = Client(base_url=base_url)
    try:
        client.embeddings.create("hi")
    finally:
        client.close()
    assert spy.call_args.kwargs["headers"]["User-Agent"] == _USER_AGENT
    assert spy.call_args.kwargs["headers"]["Accept"] == "application/json"


def test_client_exposes_embeddings_namespace(base_url: str) -> None:
    client = Client(base_url=base_url)
    try:
        assert client.embeddings is not None
        assert callable(client.embeddings.create)
    finally:
        client.close()


# ---------------------------------------------------------------------------
# Context manager
# ---------------------------------------------------------------------------


def test_client_works_as_context_manager(
    base_url: str,
    mocker: pytest.MockFixture,
    sample_embedding_response: Dict[str, Any],
) -> None:
    spy = mocker.patch.object(
        requests.Session, "request", return_value=MockResponse(200, sample_embedding_response)
    )

    with Client(base_url=base_url) as client:
        response = client.embeddings.create("hello world")

    assert response.data[0].embedding[0] == pytest.approx(0.013)
    assert spy.call_count == 1
    # After exit the session is closed — the next call would be a
    # no-op since ``_session`` is None.
    assert client._session is None


def test_client_close_is_idempotent(base_url: str) -> None:
    client = Client(base_url=base_url)
    client.close()
    client.close()  # second call must not raise


# ---------------------------------------------------------------------------
# Auth headers
# ---------------------------------------------------------------------------


def test_api_key_sets_bearer_authorization_header(
    base_url: str,
    mocker: pytest.MockFixture,
    sample_embedding_response: Dict[str, Any],
) -> None:
    spy = mocker.patch.object(
        requests.Session, "request", return_value=MockResponse(200, sample_embedding_response)
    )
    client = Client(base_url=base_url, api_key="sk-test-123")
    try:
        client.embeddings.create("hi")
        kwargs = spy.call_args.kwargs
        assert kwargs["headers"]["Authorization"] == "Bearer sk-test-123"
    finally:
        client.close()


def test_no_api_key_omits_authorization_header(
    base_url: str,
    mocker: pytest.MockFixture,
    sample_embedding_response: Dict[str, Any],
) -> None:
    spy = mocker.patch.object(
        requests.Session, "request", return_value=MockResponse(200, sample_embedding_response)
    )
    client = Client(base_url=base_url)
    try:
        client.embeddings.create("hi")
        kwargs = spy.call_args.kwargs
        assert "Authorization" not in kwargs["headers"]
    finally:
        client.close()


# ---------------------------------------------------------------------------
# Embeddings: success paths
# ---------------------------------------------------------------------------


def test_embeddings_create_with_single_string(
    base_url: str,
    mocker: pytest.MockFixture,
    sample_embedding_response: Dict[str, Any],
) -> None:
    spy = mocker.patch.object(
        requests.Session, "request", return_value=MockResponse(200, sample_embedding_response)
    )
    client = Client(base_url=base_url)
    try:
        response = client.embeddings.create("hello world")
    finally:
        client.close()

    # Typed response shape.
    assert isinstance(response, EmbeddingResponse)
    assert response.object == "list"
    assert response.model == "text-embedding-3-small"
    assert response.usage == EmbeddingUsage(prompt_tokens=2, total_tokens=2)
    assert len(response.data) == 1
    assert isinstance(response.data[0], EmbeddingData)
    assert response.data[0].object == "embedding"
    assert response.data[0].index == 0
    assert response.data[0].embedding[0] == pytest.approx(0.013)

    # The wire body must mirror OpenAI's shape exactly: input as a
    # string, no list wrapping, model preserved, encoding_format and
    # user absent.
    sent_body = spy.call_args.kwargs["json"]
    assert sent_body == {
        "input": "hello world",
        "model": "text-embedding-3-small",
    }
    assert spy.call_args.args[0] == "POST"
    assert spy.call_args.args[1] == "http://localhost:8080/v1/embeddings"


def test_embeddings_create_with_array(
    base_url: str,
    mocker: pytest.MockFixture,
    sample_embedding_array_response: Dict[str, Any],
) -> None:
    spy = mocker.patch.object(
        requests.Session,
        "request",
        return_value=MockResponse(200, sample_embedding_array_response),
    )
    client = Client(base_url=base_url)
    try:
        response = client.embeddings.create(["a", "b", "c"])
    finally:
        client.close()

    assert len(response.data) == 3
    assert [d.index for d in response.data] == [0, 1, 2]
    # The input is sent as a list (not a single string).
    sent_body = spy.call_args.kwargs["json"]
    assert sent_body["input"] == ["a", "b", "c"]


def test_embeddings_create_sends_optional_fields_when_provided(
    base_url: str,
    mocker: pytest.MockFixture,
    sample_embedding_response: Dict[str, Any],
) -> None:
    spy = mocker.patch.object(
        requests.Session, "request", return_value=MockResponse(200, sample_embedding_response)
    )
    client = Client(base_url=base_url)
    try:
        client.embeddings.create(
            "hi",
            model="text-embedding-3-large",
            encoding_format="float",
            user="user-42",
        )
    finally:
        client.close()

    body = spy.call_args.kwargs["json"]
    assert body["model"] == "text-embedding-3-large"
    assert body["encoding_format"] == "float"
    assert body["user"] == "user-42"


def test_embeddings_create_omits_optional_fields_when_none(
    base_url: str,
    mocker: pytest.MockFixture,
    sample_embedding_response: Dict[str, Any],
) -> None:
    spy = mocker.patch.object(
        requests.Session, "request", return_value=MockResponse(200, sample_embedding_response)
    )
    client = Client(base_url=base_url)
    try:
        client.embeddings.create("hi")
    finally:
        client.close()

    body = spy.call_args.kwargs["json"]
    assert "encoding_format" not in body
    assert "user" not in body


def test_embeddings_create_default_model_is_text_embedding_3_small(
    base_url: str,
    mocker: pytest.MockFixture,
    sample_embedding_response: Dict[str, Any],
) -> None:
    spy = mocker.patch.object(
        requests.Session, "request", return_value=MockResponse(200, sample_embedding_response)
    )
    client = Client(base_url=base_url)
    try:
        client.embeddings.create("hi")
    finally:
        client.close()
    assert spy.call_args.kwargs["json"]["model"] == "text-embedding-3-small"


def test_embeddings_create_per_request_timeout_overrides_default(
    base_url: str,
    mocker: pytest.MockFixture,
    sample_embedding_response: Dict[str, Any],
) -> None:
    spy = mocker.patch.object(
        requests.Session, "request", return_value=MockResponse(200, sample_embedding_response)
    )
    client = Client(base_url=base_url, timeout=30.0)
    try:
        client.embeddings.create("hi", timeout=2.5)
    finally:
        client.close()
    assert spy.call_args.kwargs["timeout"] == 2.5


# ---------------------------------------------------------------------------
# Embeddings: error paths
# ---------------------------------------------------------------------------


def test_embeddings_create_raises_api_error_on_4xx(
    base_url: str,
    mocker: pytest.MockFixture,
    openai_style_error_body: Dict[str, Any],
) -> None:
    mocker.patch.object(
        requests.Session,
        "request",
        return_value=MockResponse(400, openai_style_error_body),
    )
    client = Client(base_url=base_url)
    try:
        with pytest.raises(DashAPIError) as exc_info:
            client.embeddings.create("hi")
    finally:
        client.close()

    err = exc_info.value
    assert err.status_code == 400
    assert err.error_type == "invalid_request_error"
    assert err.error_message == "input must contain at least one text"
    assert err.body == json.dumps(openai_style_error_body)


def test_embeddings_create_raises_api_error_on_5xx(
    base_url: str,
    mocker: pytest.MockFixture,
) -> None:
    body = {
        "error": {
            "message": "embedding failed: backend unavailable",
            "type": "server_error",
            "param": None,
            "code": None,
        }
    }
    mocker.patch.object(
        requests.Session, "request", return_value=MockResponse(500, body)
    )
    client = Client(base_url=base_url)
    try:
        with pytest.raises(DashAPIError) as exc_info:
            client.embeddings.create("hi")
    finally:
        client.close()

    err = exc_info.value
    assert err.status_code == 500
    assert err.error_type == "server_error"


def test_embeddings_create_falls_back_to_status_text_on_non_json_error(
    base_url: str, mocker: pytest.MockFixture
) -> None:
    mocker.patch.object(
        requests.Session,
        "request",
        return_value=MockResponse(502, raw_text="Bad Gateway"),
    )
    client = Client(base_url=base_url)
    try:
        with pytest.raises(DashAPIError) as exc_info:
            client.embeddings.create("hi")
    finally:
        client.close()

    err = exc_info.value
    assert err.status_code == 502
    assert err.error_message == "Bad Gateway"
    assert err.error_type == "api_error"


def test_embeddings_create_raises_connection_error_on_timeout(
    base_url: str, mocker: pytest.MockFixture
) -> None:
    mocker.patch.object(
        requests.Session, "request", side_effect=requests.exceptions.Timeout("slow")
    )
    client = Client(base_url=base_url, timeout=5.0)
    try:
        with pytest.raises(DashConnectionError) as exc_info:
            client.embeddings.create("hi")
    finally:
        client.close()

    assert "timed out" in str(exc_info.value)
    assert isinstance(exc_info.value.__cause__, requests.exceptions.Timeout)


def test_embeddings_create_raises_connection_error_on_connection_error(
    base_url: str, mocker: pytest.MockFixture
) -> None:
    mocker.patch.object(
        requests.Session,
        "request",
        side_effect=requests.exceptions.ConnectionError("refused"),
    )
    client = Client(base_url=base_url)
    try:
        with pytest.raises(DashConnectionError):
            client.embeddings.create("hi")
    finally:
        client.close()


def test_dash_api_error_is_subclass_of_dash_error(
    base_url: str, mocker: pytest.MockFixture
) -> None:
    mocker.patch.object(
        requests.Session,
        "request",
        return_value=MockResponse(400, {"error": {"message": "x", "type": "y"}}),
    )
    client = Client(base_url=base_url)
    try:
        with pytest.raises(DashError):
            client.embeddings.create("hi")
    finally:
        client.close()


# ---------------------------------------------------------------------------
# Retrieve: success paths
# ---------------------------------------------------------------------------


def test_retrieve_default_stance_mode_is_balanced(
    base_url: str,
    mocker: pytest.MockFixture,
    sample_retrieve_response: Dict[str, Any],
) -> None:
    spy = mocker.patch.object(
        requests.Session, "request", return_value=MockResponse(200, sample_retrieve_response)
    )
    client = Client(base_url=base_url)
    try:
        response = client.retrieve("tenant-a", "company x")
    finally:
        client.close()

    body = spy.call_args.kwargs["json"]
    assert body == {
        "tenant_id": "tenant-a",
        "query": "company x",
        "top_k": 10,
        "stance_mode": "balanced",
    }
    assert spy.call_args.args[1] == "http://localhost:8080/v1/retrieve"
    assert isinstance(response, RetrieveResponse)
    assert len(response.results) == 1


def test_retrieve_supports_all_stance_modes(
    base_url: str,
    mocker: pytest.MockFixture,
    sample_retrieve_response: Dict[str, Any],
) -> None:
    spy = mocker.patch.object(
        requests.Session, "request", return_value=MockResponse(200, sample_retrieve_response)
    )
    client = Client(base_url=base_url)
    try:
        for mode in ("balanced", "support_only"):
            client.retrieve("tenant-a", "q", top_k=3, stance_mode=mode)
    finally:
        client.close()

    sent_bodies = [c.kwargs["json"] for c in spy.call_args_list]
    assert sent_bodies[0]["stance_mode"] == "balanced"
    assert sent_bodies[1]["stance_mode"] == "support_only"
    assert sent_bodies[1]["top_k"] == 3


def test_retrieve_parses_typed_result_with_citation(
    base_url: str,
    mocker: pytest.MockFixture,
    sample_retrieve_response: Dict[str, Any],
) -> None:
    mocker.patch.object(
        requests.Session, "request", return_value=MockResponse(200, sample_retrieve_response)
    )
    client = Client(base_url=base_url)
    try:
        response = client.retrieve("tenant-a", "q")
    finally:
        client.close()

    result = response.results[0]
    assert isinstance(result, RetrieveResult)
    assert result.claim_id == "claim-1"
    assert result.canonical_text == "Acme Co. was acquired in 2024."
    assert result.score == pytest.approx(0.93)
    assert result.supports == 4
    assert result.contradicts == 1
    assert len(result.citations) == 1
    citation = result.citations[0]
    assert citation.evidence_id == "ev-1"
    assert citation.stance == "supports"
    assert citation.source_quality == pytest.approx(0.88)
    assert citation.span_start == 120
    assert citation.span_end == 168
    assert citation.ingested_at == 1735689700000


def test_retrieve_sends_return_graph_flag(
    base_url: str,
    mocker: pytest.MockFixture,
    sample_retrieve_response: Dict[str, Any],
) -> None:
    spy = mocker.patch.object(
        requests.Session, "request", return_value=MockResponse(200, sample_retrieve_response)
    )
    client = Client(base_url=base_url)
    try:
        client.retrieve("tenant-a", "q", return_graph=True)
    finally:
        client.close()
    body = spy.call_args.kwargs["json"]
    assert body["return_graph"] is True


def test_retrieve_omits_return_graph_when_none(
    base_url: str,
    mocker: pytest.MockFixture,
    sample_retrieve_response: Dict[str, Any],
) -> None:
    spy = mocker.patch.object(
        requests.Session, "request", return_value=MockResponse(200, sample_retrieve_response)
    )
    client = Client(base_url=base_url)
    try:
        client.retrieve("tenant-a", "q")
    finally:
        client.close()
    body = spy.call_args.kwargs["json"]
    assert "return_graph" not in body


def test_retrieve_handles_empty_results(
    base_url: str, mocker: pytest.MockFixture
) -> None:
    mocker.patch.object(
        requests.Session,
        "request",
        return_value=MockResponse(200, {"results": []}),
    )
    client = Client(base_url=base_url)
    try:
        response = client.retrieve("tenant-a", "q")
    finally:
        client.close()
    assert response.results == []


# ---------------------------------------------------------------------------
# Retrieve: error paths
# ---------------------------------------------------------------------------


def test_retrieve_raises_api_error_on_5xx(
    base_url: str, mocker: pytest.MockFixture
) -> None:
    mocker.patch.object(
        requests.Session,
        "request",
        return_value=MockResponse(
            503,
            {"error": "routing unavailable", "code": "no_healthy_node"},
        ),
    )
    client = Client(base_url=base_url)
    try:
        with pytest.raises(DashAPIError) as exc_info:
            client.retrieve("tenant-a", "q")
    finally:
        client.close()

    err = exc_info.value
    assert err.status_code == 503
    # /v1/retrieve uses an ad-hoc error shape; the client tolerates it.
    assert err.error_message == "routing unavailable"


def test_retrieve_raises_api_error_on_403(
    base_url: str, mocker: pytest.MockFixture
) -> None:
    mocker.patch.object(
        requests.Session,
        "request",
        return_value=MockResponse(
            403,
            {"error": "tenant is not allowed for this API key"},
        ),
    )
    client = Client(base_url=base_url, api_key="sk-test")
    try:
        with pytest.raises(DashAPIError) as exc_info:
            client.retrieve("tenant-other", "q")
    finally:
        client.close()
    assert exc_info.value.status_code == 403


def test_retrieve_raises_connection_error_on_timeout(
    base_url: str, mocker: pytest.MockFixture
) -> None:
    mocker.patch.object(
        requests.Session, "request", side_effect=requests.exceptions.Timeout("slow")
    )
    client = Client(base_url=base_url)
    try:
        with pytest.raises(DashConnectionError):
            client.retrieve("tenant-a", "q")
    finally:
        client.close()
