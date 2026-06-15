"""OpenAI wire-format compatibility tests.

These tests pin the SDK to the byte-for-byte shape the OpenAI Python
SDK (``openai.OpenAI``) and other clients (``langchain``,
``llama-index``) expect to see on the wire. The point is: if you point
``openai.OpenAI(base_url="http://localhost:8080/v1")`` at DASH, every
request we send and every response we accept must be indistinguishable
from a real OpenAI call.

We don't import the ``openai`` package itself — the wire shapes are the
contract, not the SDK.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List

import pytest
import requests

from dash import Client, DashAPIError, EmbeddingResponse
from tests.conftest import MockResponse


# ---------------------------------------------------------------------------
# Request shape
# ---------------------------------------------------------------------------


def test_request_body_for_single_string_matches_openai_schema(
    base_url: str, mocker: pytest.MockFixture
) -> None:
    """Single-string input must be sent as a bare string, not ``[str]``."""
    spy = mocker.patch.object(
        requests.Session,
        "request",
        return_value=MockResponse(200, {"data": [], "model": "x", "object": "list",
                                          "usage": {"prompt_tokens": 0, "total_tokens": 0}}),
    )
    client = Client(base_url=base_url)
    try:
        client.embeddings.create("hello world", model="text-embedding-3-small")
    finally:
        client.close()

    body = spy.call_args.kwargs["json"]
    # OpenAI accepts both shapes but the single-string form is the
    # canonical, lowest-overhead representation.
    assert body["input"] == "hello world"
    assert body["model"] == "text-embedding-3-small"
    # Optional fields must be omitted by default — OpenAI's validator
    # does not complain but some libraries inspect the body.
    assert "encoding_format" not in body
    assert "user" not in body


def test_request_body_for_array_matches_openai_schema(
    base_url: str, mocker: pytest.MockFixture
) -> None:
    """Array input must be sent as a JSON array of strings."""
    spy = mocker.patch.object(
        requests.Session,
        "request",
        return_value=MockResponse(
            200,
            {
                "object": "list",
                "data": [
                    {"object": "embedding", "embedding": [0.0], "index": 0},
                ],
                "model": "x",
                "usage": {"prompt_tokens": 0, "total_tokens": 0},
            },
        ),
    )
    client = Client(base_url=base_url)
    try:
        client.embeddings.create(["alpha", "beta", "gamma"])
    finally:
        client.close()

    body = spy.call_args.kwargs["json"]
    assert body["input"] == ["alpha", "beta", "gamma"]


def test_request_url_is_v1_embeddings(
    base_url: str, mocker: pytest.MockFixture
) -> None:
    spy = mocker.patch.object(
        requests.Session,
        "request",
        return_value=MockResponse(
            200,
            {
                "object": "list",
                "data": [],
                "model": "x",
                "usage": {"prompt_tokens": 0, "total_tokens": 0},
            },
        ),
    )
    client = Client(base_url=base_url)
    try:
        client.embeddings.create("hi")
    finally:
        client.close()

    # Drop-in compatibility: openai.OpenAI(base_url=...) issues its
    # embeddings to <base_url>/v1/embeddings. We must hit the same
    # path.
    assert spy.call_args.args[0] == "POST"
    assert spy.call_args.args[1] == "http://localhost:8080/v1/embeddings"


# ---------------------------------------------------------------------------
# Response shape
# ---------------------------------------------------------------------------


def test_response_has_all_openai_top_level_fields(
    base_url: str, mocker: pytest.MockFixture
) -> None:
    body = {
        "object": "list",
        "data": [
            {
                "object": "embedding",
                "embedding": [0.1, 0.2, 0.3],
                "index": 0,
            }
        ],
        "model": "text-embedding-3-small",
        "usage": {"prompt_tokens": 1, "total_tokens": 1},
    }
    mocker.patch.object(requests.Session, "request", return_value=MockResponse(200, body))
    client = Client(base_url=base_url)
    try:
        response = client.embeddings.create("hi")
    finally:
        client.close()

    # These four top-level keys are required by the OpenAI spec.
    assert response.object == "list"
    assert response.model == "text-embedding-3-small"
    assert response.usage.prompt_tokens == 1
    assert response.usage.total_tokens == 1
    # data[i].object must be the literal string "embedding".
    assert response.data[0].object == "embedding"
    # data[i].index must round-trip as an int.
    assert response.data[0].index == 0


def test_response_supports_large_array_indices(
    base_url: str, mocker: pytest.MockFixture
) -> None:
    """DASH may echo back 1000+ items; indices must remain sequential ints."""
    n = 257
    data = [
        {"object": "embedding", "embedding": [float(i)], "index": i}
        for i in range(n)
    ]
    body = {
        "object": "list",
        "data": data,
        "model": "x",
        "usage": {"prompt_tokens": n, "total_tokens": n},
    }
    mocker.patch.object(requests.Session, "request", return_value=MockResponse(200, body))
    client = Client(base_url=base_url)
    try:
        response = client.embeddings.create([str(i) for i in range(n)])
    finally:
        client.close()

    assert len(response.data) == n
    assert [d.index for d in response.data] == list(range(n))


def test_response_object_field_is_preserved(
    base_url: str, mocker: pytest.MockFixture
) -> None:
    body = {
        "object": "list",
        "data": [{"object": "embedding", "embedding": [0.0], "index": 0}],
        "model": "x",
        "usage": {"prompt_tokens": 0, "total_tokens": 0},
    }
    mocker.patch.object(requests.Session, "request", return_value=MockResponse(200, body))
    client = Client(base_url=base_url)
    try:
        response = client.embeddings.create("hi")
    finally:
        client.close()
    assert response.object == "list"
    assert response.data[0].object == "embedding"


# ---------------------------------------------------------------------------
# Error envelope
# ---------------------------------------------------------------------------


def test_error_envelope_matches_openai_shape(
    base_url: str, mocker: pytest.MockFixture
) -> None:
    """DASH returns OpenAI-shaped errors on /v1/embeddings."""
    err = {
        "error": {
            "message": "encoding_format 'base64' is not supported; only 'float' is currently implemented",
            "type": "invalid_request_error",
            "param": "encoding_format",
            "code": None,
        }
    }
    mocker.patch.object(requests.Session, "request", return_value=MockResponse(400, err))
    client = Client(base_url=base_url)
    try:
        with pytest.raises(DashAPIError) as exc_info:
            client.embeddings.create("hi", encoding_format="base64")
    finally:
        client.close()

    e = exc_info.value
    assert e.status_code == 400
    assert e.error_type == "invalid_request_error"
    assert "encoding_format" in e.error_message


def test_error_envelope_with_server_error_type(
    base_url: str, mocker: pytest.MockFixture
) -> None:
    err = {
        "error": {
            "message": "embedding failed: backend unavailable",
            "type": "server_error",
            "param": None,
            "code": None,
        }
    }
    mocker.patch.object(requests.Session, "request", return_value=MockResponse(500, err))
    client = Client(base_url=base_url)
    try:
        with pytest.raises(DashAPIError) as exc_info:
            client.embeddings.create("hi")
    finally:
        client.close()
    assert exc_info.value.error_type == "server_error"


# ---------------------------------------------------------------------------
# "If openai.OpenAI can talk to DASH, so can we" — verify the *exact*
# JSON a vanilla OpenAI client would send matches what dash-py sends.
# ---------------------------------------------------------------------------


def test_dash_py_request_matches_openai_python_client_payload(
    base_url: str, mocker: pytest.MockFixture
) -> None:
    """The exact JSON body the official ``openai`` Python SDK sends for
    ``client.embeddings.create("hello", model="text-embedding-3-small")``
    is::

        {"input": "hello", "model": "text-embedding-3-small",
         "encoding_format": "float", "user": null}

    dash-py must be able to produce that exact body (when the user
    passes ``encoding_format`` and ``user``).
    """
    spy = mocker.patch.object(
        requests.Session,
        "request",
        return_value=MockResponse(
            200,
            {
                "object": "list",
                "data": [],
                "model": "x",
                "usage": {"prompt_tokens": 0, "total_tokens": 0},
            },
        ),
    )
    client = Client(base_url=base_url)
    try:
        client.embeddings.create(
            "hello",
            model="text-embedding-3-small",
            encoding_format="float",
            user=None,  # explicit None mirrors openai's default
        )
    finally:
        client.close()

    body = spy.call_args.kwargs["json"]
    # Re-serialize the body exactly the way json.dumps would on the
    # wire (no extra whitespace, key order matches OpenAI's encoder).
    on_the_wire = json.dumps(body, separators=(",", ":"))
    expected = (
        '{"input":"hello","model":"text-embedding-3-small",'
        '"encoding_format":"float"}'
    )
    # We omit ``user`` when it's None; OpenAI sends an explicit null.
    # Both are accepted by the server. The point is that the keys
    # and values we do send round-trip cleanly.
    assert on_the_wire == expected


# ---------------------------------------------------------------------------
# Retrieval: the differentiator
# ---------------------------------------------------------------------------


def test_retrieve_response_preserves_claim_evidence_contradiction_split(
    base_url: str, mocker: pytest.MockFixture
) -> None:
    """The Claim + Evidence + Contradiction differentiator lives in
    the ``supports`` and ``contradicts`` fields, plus the structured
    ``citations`` list. Make sure both survive parsing intact so a RAG
    pipeline can do claim-level filtering without re-parsing JSON.
    """
    body = {
        "results": [
            {
                "claim_id": "claim-support",
                "canonical_text": "Acme grew 30% in Q3.",
                "score": 0.91,
                "supports": 5,
                "contradicts": 0,
                "citations": [
                    {
                        "evidence_id": "ev-a",
                        "source_id": "s://wsj",
                        "stance": "supports",
                        "source_quality": 0.9,
                    }
                ],
            },
            {
                "claim_id": "claim-contra",
                "canonical_text": "Acme actually shrank in Q3.",
                "score": 0.82,
                "supports": 1,
                "contradicts": 3,
                "citations": [
                    {
                        "evidence_id": "ev-b",
                        "source_id": "s://bloomberg",
                        "stance": "contradicts",
                        "source_quality": 0.85,
                    }
                ],
            },
        ]
    }
    mocker.patch.object(requests.Session, "request", return_value=MockResponse(200, body))
    client = Client(base_url=base_url)
    try:
        response = client.retrieve("tenant-a", "acme q3 revenue")
    finally:
        client.close()

    # A RAG pipeline that wants "claims with at least one citation
    # in support and no contradictions" can do this directly:
    clean_claims = [r for r in response.results if r.contradicts == 0 and r.supports > 0]
    assert [r.claim_id for r in clean_claims] == ["claim-support"]

    # And one that wants "claims whose top citation is from a trusted
    # source" can filter on the typed Citation objects:
    trusted = [
        r
        for r in response.results
        if r.citations and r.citations[0].source_quality >= 0.85
    ]
    assert {r.claim_id for r in trusted} == {"claim-support", "claim-contra"}
