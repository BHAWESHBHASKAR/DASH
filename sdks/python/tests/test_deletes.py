"""Tests for the delete methods (ingestion service).

The HTTP transport is mocked at ``requests.Session.request`` (sync) and
``httpx.AsyncClient.request`` (async), so no live DASH instance is needed.
"""

from __future__ import annotations

from typing import Any, Dict
from unittest.mock import AsyncMock

import httpx
import pytest
import requests

from dash import AsyncClient, Client, DashAPIError, DashError, DeleteResponse
from dash._ingestion import derive_ingestion_url
from tests.conftest import MockResponse


def _claim_body(deleted: bool = True) -> Dict[str, Any]:
    return {
        "deleted": deleted,
        "scope": "claim",
        "tenant_id": "tenant a",
        "claim_id": "c/1",
        "claims_deleted": 1 if deleted else 0,
        "evidence_deleted": 2 if deleted else 0,
        "edges_deleted": 1 if deleted else 0,
        "vectors_deleted": 1 if deleted else 0,
        "claims_total": 41,
        "checkpoint_triggered": False,
        "checkpoint_deferred": False,
    }


def test_ingestion_url_is_derived_only_for_port_8080() -> None:
    assert derive_ingestion_url("http://localhost:8080") == "http://localhost:8081"
    assert derive_ingestion_url("https://dash.local:8080/base/") == "https://dash.local:8081/base"
    assert derive_ingestion_url("http://[::1]:8080") == "http://[::1]:8081"
    assert derive_ingestion_url("https://dash.example.com") is None
    assert derive_ingestion_url("http://localhost:9000") is None


def test_delete_claim_sends_delete_to_the_ingestion_service(mocker: pytest.MockFixture) -> None:
    spy = mocker.patch.object(
        requests.Session, "request", return_value=MockResponse(200, _claim_body())
    )
    with Client(base_url="http://localhost:8080", api_key="k") as client:
        response = client.delete_claim("tenant a", "c/1")

    method, url = spy.call_args.args[:2]
    assert method == "DELETE"
    assert url == "http://localhost:8081/v1/claims/c%2F1?tenant_id=tenant%20a"
    assert spy.call_args.kwargs["json"] is None
    assert spy.call_args.kwargs["headers"]["Authorization"] == "Bearer k"
    assert isinstance(response, DeleteResponse)
    assert response.deleted is True
    assert (response.claims_deleted, response.evidence_deleted) == (1, 2)
    assert response.claim_id == "c/1"
    assert response.claims_total == 41


def test_delete_evidence_and_tenant_use_their_routes(mocker: pytest.MockFixture) -> None:
    spy = mocker.patch.object(
        requests.Session,
        "request",
        side_effect=[
            MockResponse(
                200,
                {"deleted": False, "scope": "evidence", "tenant_id": "t", "evidence_id": "e1"},
            ),
            MockResponse(200, {"deleted": True, "scope": "tenant", "tenant_id": "t"}),
        ],
    )
    client = Client(base_url="https://dash.example.com", ingestion_base_url="https://ingest.example.com/")
    evidence = client.delete_evidence("t", "e1")
    tenant = client.delete_tenant("t")
    urls = [call.args[1] for call in spy.call_args_list]
    assert urls == [
        "https://ingest.example.com/v1/evidence/e1?tenant_id=t",
        "https://ingest.example.com/v1/tenants/t",
    ]
    assert evidence.deleted is False and evidence.evidence_id == "e1"
    assert tenant.deleted is True and tenant.scope == "tenant"


def test_delete_without_an_ingestion_url_fails_before_any_request(
    mocker: pytest.MockFixture,
) -> None:
    spy = mocker.patch.object(requests.Session, "request")
    client = Client(base_url="https://dash.example.com")
    with pytest.raises(DashError, match="ingestion_base_url"):
        client.delete_claim("t", "c")
    assert spy.call_count == 0


def test_delete_rejects_empty_identifiers(mocker: pytest.MockFixture) -> None:
    spy = mocker.patch.object(requests.Session, "request")
    client = Client(base_url="http://localhost:8080")
    for call in (
        lambda: client.delete_claim("", "c"),
        lambda: client.delete_claim("t", " "),
        lambda: client.delete_evidence("t", ""),
        lambda: client.delete_tenant(""),
    ):
        with pytest.raises(ValueError):
            call()
    assert spy.call_count == 0


def test_delete_errors_surface_as_api_errors(mocker: pytest.MockFixture) -> None:
    mocker.patch.object(
        requests.Session,
        "request",
        return_value=MockResponse(403, {"error": "missing required role: admin"}),
    )
    client = Client(base_url="http://localhost:8080")
    with pytest.raises(DashAPIError) as info:
        client.delete_tenant("t")
    assert info.value.status_code == 403


@pytest.mark.asyncio
async def test_async_delete_methods(mocker: pytest.MockFixture) -> None:
    mock = mocker.patch.object(
        httpx.AsyncClient,
        "request",
        new=AsyncMock(
            side_effect=[
                MockResponse(200, _claim_body(deleted=False)),
                MockResponse(200, {"deleted": True, "scope": "evidence", "tenant_id": "t"}),
                MockResponse(200, {"deleted": True, "scope": "tenant", "tenant_id": "t"}),
            ]
        ),
    )
    async with AsyncClient(base_url="http://127.0.0.1:8080") as client:
        claim = await client.delete_claim("tenant a", "c/1")
        await client.delete_evidence("t", "e 1")
        tenant = await client.delete_tenant("t")
    calls = [(call.args[0], call.args[1]) for call in mock.call_args_list]
    assert calls == [
        ("DELETE", "http://127.0.0.1:8081/v1/claims/c%2F1?tenant_id=tenant%20a"),
        ("DELETE", "http://127.0.0.1:8081/v1/evidence/e%201?tenant_id=t"),
        ("DELETE", "http://127.0.0.1:8081/v1/tenants/t"),
    ]
    assert claim.deleted is False
    assert tenant.scope == "tenant"


@pytest.mark.asyncio
async def test_async_delete_without_an_ingestion_url_fails() -> None:
    client = AsyncClient(base_url="https://dash.example.com")
    with pytest.raises(DashError, match="ingestion_base_url"):
        await client.delete_tenant("t")
    await client.close()
