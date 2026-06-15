"""Asynchronous DASH client.

Mirror of :mod:`dash.client` built on top of :mod:`httpx`'s async
client. Every public method is an ``async def`` coroutine; the
namespace layout (``client.embeddings.create(...)``,
``client.retrieve(...)``) is identical to the sync client.

Example::

    import asyncio
    from dash import AsyncClient

    async def main() -> None:
        async with AsyncClient(base_url="http://localhost:8080") as c:
            response = await c.embeddings.create("hello world")
            print(response.data[0].embedding[:3])

    asyncio.run(main())
"""

from __future__ import annotations

import json
from types import TracebackType
from typing import Any, Dict, List, Mapping, Optional, Type, Union

import httpx

from ._version import __version__
from .errors import DashAPIError, DashConnectionError, DashError
from .types import (
    EmbeddingRequest,
    EmbeddingResponse,
    RetrieveRequest,
    RetrieveResponse,
)

_DEFAULT_TIMEOUT: float = 30.0
_USER_AGENT: str = f"dash-py/{__version__} (async)"


class AsyncEmbeddingsNamespace:
    """Async namespace object exposed as ``AsyncClient.embeddings``."""

    def __init__(self, client: "AsyncClient") -> None:
        self._client = client

    async def create(
        self,
        input: Union[str, List[str]],
        model: str = "text-embedding-3-small",
        *,
        encoding_format: Optional[str] = None,
        user: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> EmbeddingResponse:
        """Call ``POST /v1/embeddings`` asynchronously.

        See :meth:`dash.client.EmbeddingsNamespace.create` for
        parameter semantics.
        """
        request = EmbeddingRequest(
            input=input,
            model=model,
            encoding_format=encoding_format,
            user=user,
        )
        return await self._client._request_embeddings(request, timeout=timeout)


class AsyncClient:
    """Asynchronous DASH client.

    Parameters match :class:`dash.client.Client` exactly. The
    underlying :class:`httpx.AsyncClient` is created lazily and is
    reused across calls for connection pooling.
    """

    def __init__(
        self,
        base_url: str = "http://localhost:8080",
        api_key: Optional[str] = None,
        timeout: float = _DEFAULT_TIMEOUT,
    ) -> None:
        if not base_url:
            raise ValueError("base_url is required")
        if timeout is not None and timeout <= 0:
            raise ValueError("timeout must be positive")

        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout = timeout
        self._client: Optional[httpx.AsyncClient] = None
        self.embeddings = AsyncEmbeddingsNamespace(self)

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    async def __aenter__(self) -> "AsyncClient":
        # Eagerly create the underlying httpx client so that errors
        # surface inside the ``async with`` block rather than on the
        # first request.
        self._ensure_client()
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> None:
        await self.close()

    async def close(self) -> None:
        """Close the underlying :class:`httpx.AsyncClient`.

        Safe to call multiple times. After ``close()`` the next call
        will lazily open a new client (mirroring the behaviour of
        :class:`httpx.AsyncClient`).
        """
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    def _ensure_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                headers={
                    "User-Agent": _USER_AGENT,
                    "Accept": "application/json",
                },
                timeout=self.timeout,
            )
        return self._client

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def retrieve(
        self,
        tenant_id: str,
        query: str,
        top_k: int = 10,
        stance_mode: str = "balanced",
        *,
        return_graph: Optional[bool] = None,
        timeout: Optional[float] = None,
    ) -> RetrieveResponse:
        """Call ``POST /v1/retrieve`` asynchronously.

        See :meth:`dash.client.Client.retrieve` for parameter
        semantics.
        """
        request = RetrieveRequest(
            tenant_id=tenant_id,
            query=query,
            top_k=top_k,
            stance_mode=stance_mode,
            return_graph=return_graph,
        )
        return await self._request_retrieve(request, timeout=timeout)

    # ------------------------------------------------------------------
    # Internal HTTP plumbing
    # ------------------------------------------------------------------

    def _auth_headers(self) -> Dict[str, str]:
        if self.api_key:
            return {"Authorization": f"Bearer {self.api_key}"}
        return {}

    async def _request(
        self,
        method: str,
        path: str,
        *,
        json_body: Optional[Mapping[str, Any]] = None,
        timeout: Optional[float] = None,
    ) -> Any:
        url = f"{self.base_url}{path}"
        headers = {**self._auth_headers()}
        client = self._ensure_client()

        try:
            response = await client.request(
                method,
                url,
                json=json_body,
                headers=headers,
                timeout=timeout if timeout is not None else self.timeout,
            )
        except httpx.TimeoutException as exc:
            raise DashConnectionError(
                f"request to DASH timed out: {exc}"
            ) from exc
        except httpx.ConnectError as exc:
            raise DashConnectionError(
                f"failed to connect to DASH at {self.base_url}: {exc}"
            ) from exc
        except httpx.RequestError as exc:
            raise DashConnectionError(f"DASH request failed: {exc}") from exc

        return self._parse_response(response)

    @staticmethod
    def _parse_response(response: httpx.Response) -> Any:
        raw_text: str
        try:
            raw_text = response.text
        except Exception:  # pragma: no cover - extremely defensive
            raw_text = ""

        parsed_body: Any = None
        if raw_text:
            try:
                parsed_body = json.loads(raw_text)
            except (ValueError, TypeError):
                parsed_body = None

        if 200 <= response.status_code < 300:
            return parsed_body

        raise DashAPIError.from_response(
            status_code=response.status_code,
            body=raw_text if raw_text else response.content,
            parsed=parsed_body if isinstance(parsed_body, (dict, list)) else None,
        )

    async def _request_embeddings(
        self,
        request: EmbeddingRequest,
        *,
        timeout: Optional[float] = None,
    ) -> EmbeddingResponse:
        body = await self._request(
            "POST",
            "/v1/embeddings",
            json_body=request.to_dict(),
            timeout=timeout,
        )
        if not isinstance(body, dict):
            raise DashAPIError(
                "DASH returned a non-object body for /v1/embeddings",
                status_code=200,
                body=body,
            )
        return EmbeddingResponse.from_dict(body)

    async def _request_retrieve(
        self,
        request: RetrieveRequest,
        *,
        timeout: Optional[float] = None,
    ) -> RetrieveResponse:
        body = await self._request(
            "POST",
            "/v1/retrieve",
            json_body=request.to_dict(),
            timeout=timeout,
        )
        if not isinstance(body, dict):
            raise DashAPIError(
                "DASH returned a non-object body for /v1/retrieve",
                status_code=200,
                body=body,
            )
        return RetrieveResponse.from_dict(body)


__all__ = ["AsyncClient", "AsyncEmbeddingsNamespace"]
