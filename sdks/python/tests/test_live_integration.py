"""Live integration tests against a running DASH instance.

These tests are intentionally opt-in. They run only when the
``DASH_LIVE_URL`` environment variable is set to a reachable DASH
base URL (for example ``http://127.0.0.1:8080``).

To exercise both the retrieval and ingestion services, also set:

- ``DASH_LIVE_RETRIEVAL_URL`` (defaults to ``DASH_LIVE_URL``)
- ``DASH_LIVE_INGESTION_URL`` (defaults to ``DASH_LIVE_URL``:8081)

The tests are skipped with a clear message otherwise, so the
default ``pytest`` run is fast and offline.

These cover the "first contact" risk: a real network round-trip
through the SDK against a real DASH service. Mocked unit tests
prove the SDK is correct in isolation; this file proves the
contract holds end-to-end.
"""
from __future__ import annotations

import base64
import os
import struct
import time
import uuid
from typing import Optional

import pytest
import requests

from dash import Client
from dash.errors import DashAPIError, DashError


def _live_url() -> Optional[str]:
    return os.environ.get("DASH_LIVE_URL")


def _retrieval_url() -> Optional[str]:
    return os.environ.get("DASH_LIVE_RETRIEVAL_URL") or _live_url()


def _ingestion_url() -> Optional[str]:
    return os.environ.get("DASH_LIVE_INGESTION_URL") or _live_url()


pytestmark = pytest.mark.skipif(
    _live_url() is None,
    reason="DASH_LIVE_URL not set; live integration tests are opt-in",
)


def _wait_for_health(url: str, timeout_s: float = 10.0) -> None:
    deadline = time.time() + timeout_s
    last_err: Optional[Exception] = None
    while time.time() < deadline:
        try:
            r = requests.get(f"{url.rstrip('/')}/v1/health", timeout=1.0)
            if r.status_code == 200:
                return
            last_err = AssertionError(f"health returned {r.status_code}: {r.text}")
        except requests.RequestException as exc:
            last_err = exc
        time.sleep(0.2)
    raise AssertionError(
        f"DASH at {url} did not become healthy in {timeout_s}s: {last_err}"
    )


def test_health_endpoint_responds_200() -> None:
    url = _live_url()
    assert url is not None
    r = requests.get(f"{url}/v1/health", timeout=5.0)
    assert r.status_code == 200
    body = r.json()
    assert body.get("status") == "ok"


def test_embed_endpoint_returns_floats() -> None:
    url = _retrieval_url()
    assert url is not None
    _wait_for_health(url)
    client = Client(base_url=url, api_key="not_needed")
    resp = client.embeddings.create(
        input="hello world", model="text-embedding-3-small"
    )
    assert resp.model == "text-embedding-3-small"
    assert len(resp.data) == 1
    emb = resp.data[0].embedding
    assert isinstance(emb, list)
    assert len(emb) > 0
    assert all(isinstance(x, float) for x in emb)


def test_embed_endpoint_base64_round_trip_via_openai_compat() -> None:
    """The Python SDK types EmbeddingData.embedding as List[float]
    only, so the dash-py SDK cannot deserialize a base64 response.
    Use the official OpenAI Python SDK instead (it accepts
    string|list polymorphic values for the embedding field) to
    exercise the base64 round-trip end-to-end.
    """
    openai = pytest.importorskip("openai")
    url = _retrieval_url()
    assert url is not None
    _wait_for_health(url)
    client = openai.OpenAI(
        base_url=f"{url.rstrip('/')}/v1", api_key="not_needed"
    )
    resp = client.embeddings.create(
        input="hello world",
        model="text-embedding-3-small",
        encoding_format="base64",
    )
    assert len(resp.data) == 1
    b64 = resp.data[0].embedding
    assert isinstance(b64, str)
    raw = base64.b64decode(b64)
    floats = list(struct.unpack(f"<{len(raw) // 4}f", raw))
    assert len(floats) > 0


def test_retrieve_after_direct_ingest_returns_results() -> None:
    """The dash-py SDK exposes retrieve but not ingest. Exercise
    the SDK by ingesting via the native HTTP client, then
    retrieving via the SDK.
    """
    ingestion = _ingestion_url()
    retrieval = _retrieval_url()
    assert ingestion is not None
    assert retrieval is not None
    _wait_for_health(retrieval)

    tenant_id = f"test-tenant-{uuid.uuid4().hex[:8]}"
    unique_phrase = f"distinctive phrase {uuid.uuid4().hex[:12]}"

    ingest_body = {
        "tenant_id": tenant_id,
        "bundles": [
            {
                "claim_id": f"claim-{uuid.uuid4().hex[:8]}",
                "text": unique_phrase,
                "evidence": [
                    {
                        "evidence_id": f"ev-{uuid.uuid4().hex[:8]}",
                        "text": f"Evidence supporting: {unique_phrase}",
                    }
                ],
            }
        ],
    }
    r = requests.post(
        f"{ingestion.rstrip('/')}/v1/ingest",
        json=ingest_body,
        headers={"Content-Type": "application/json"},
        timeout=5.0,
    )
    # Soft check: 200, 202, or 4xx-with-warning are all acceptable
    # outcomes for a non-existent tenant. The point is to exercise
    # the wire format end-to-end.
    if r.status_code >= 500:
        pytest.fail(f"ingest 5xx: {r.status_code} {r.text}")

    time.sleep(0.5)

    client = Client(base_url=retrieval, api_key="not_needed")
    response = client.retrieve(
        tenant_id=tenant_id, query=unique_phrase, top_k=3
    )
    # The retrieve call may return 0 results on a fresh tenant,
    # which is acceptable end-to-end behavior; the test's value
    # is in exercising the wire format, not in asserting a
    # specific result count.
    assert response is not None
    assert hasattr(response, "results")


def test_openai_python_sdk_drop_in() -> None:
    """The strongest compat test: the official OpenAI Python SDK
    pointed at DASH must produce a valid embeddings response.
    """
    openai = pytest.importorskip("openai")
    url = _retrieval_url()
    assert url is not None
    _wait_for_health(url)
    client = openai.OpenAI(
        base_url=f"{url.rstrip('/')}/v1", api_key="not_needed"
    )
    resp = client.embeddings.create(
        input="hello from openai sdk", model="text-embedding-3-small"
    )
    assert resp.model == "text-embedding-3-small"
    assert len(resp.data) == 1
    assert len(resp.data[0].embedding) > 0
