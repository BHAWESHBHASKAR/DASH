"""Helpers shared by the sync and async clients for the ingestion service.

DASH runs two HTTP services: retrieval (embeddings, retrieve; default port
8080) and ingestion (writes and deletes; default port 8081). The delete
methods talk to the ingestion service.
"""

from __future__ import annotations

from typing import Optional
from urllib.parse import quote, urlsplit, urlunsplit


def derive_ingestion_url(base_url: str) -> Optional[str]:
    """Map a retrieval URL on port 8080 to the same host on port 8081.

    Any other URL has no conventional ingestion counterpart: ``None``.
    """
    parts = urlsplit(base_url)
    if parts.port != 8080 or not parts.hostname:
        return None
    host = parts.hostname
    if ":" in host:  # IPv6 literal
        host = f"[{host}]"
    return urlunsplit((parts.scheme, f"{host}:8081", parts.path, "", "")).rstrip("/")


def _segment(name: str, value: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")
    return quote(value, safe="")


def claim_delete_path(tenant_id: str, claim_id: str) -> str:
    """``/v1/claims/{claim_id}?tenant_id=...`` with both ids escaped."""
    return f"/v1/claims/{_segment('claim_id', claim_id)}?tenant_id={_segment('tenant_id', tenant_id)}"


def evidence_delete_path(tenant_id: str, evidence_id: str) -> str:
    """``/v1/evidence/{evidence_id}?tenant_id=...`` with both ids escaped."""
    return (
        f"/v1/evidence/{_segment('evidence_id', evidence_id)}"
        f"?tenant_id={_segment('tenant_id', tenant_id)}"
    )


def tenant_delete_path(tenant_id: str) -> str:
    """``/v1/tenants/{tenant_id}`` with the id escaped."""
    return f"/v1/tenants/{_segment('tenant_id', tenant_id)}"


MISSING_INGESTION_URL = (
    "the delete methods call the ingestion service: pass ingestion_base_url "
    "(it is only derived automatically when base_url uses port 8080)"
)
