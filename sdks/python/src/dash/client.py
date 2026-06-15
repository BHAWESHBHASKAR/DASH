"""Synchronous DASH client.

A thin wrapper around :mod:`requests` that turns DASH's HTTP API into
typed Python objects. Mirrors the layout of the official ``openai``
SDK (``client.embeddings.create(...)``) so that switching from OpenAI
to DASH is a one-line import change.

Example:

    >>> from dash import Client
    >>> client = Client(base_url="http://localhost:8080")
    >>> response = client.embeddings.create("hello world")
    >>> response.data[0].embedding[:3]
    [0.13, -0.42, 0.07]

Use :class:`Client` as a context manager to release the underlying
HTTP connection pool when you're done::

    >>> with Client(base_url="http://localhost:8080") as c:
    ...     results = c.retrieve("tenant-a", "company x")
"""

from __future__ import annotations

import json
from types import TracebackType
from typing import Any, Dict, List, Mapping, Optional, Type, Union

import requests

from ._version import __version__
from .errors import DashAPIError, DashConnectionError, DashError
from .types import (
    EmbeddingData,
    EmbeddingRequest,
    EmbeddingResponse,
    EmbeddingUsage,
    RetrieveRequest,
    RetrieveResponse,
    RetrieveResult,
)

# Default timeout (seconds) for connect+read. ``None`` would disable
# the timeout entirely, which is almost never what a user wants.
_DEFAULT_TIMEOUT: float = 30.0

# User agent reported to the server. Includes the SDK version so the
# DASH team can attribute traffic to ``dash-py``.
_USER_AGENT: str = f"dash-py/{__version__}"


class EmbeddingsNamespace:
    """Namespace object exposed as ``Client.embeddings``.

    The shape matches ``openai.resources.embeddings.Embeddings`` so
    that users porting from OpenAI find the same method names.
    """

    def __init__(self, client: "Client") -> None:
        self._client = client

    def create(
        self,
        input: Union[str, List[str]],
        model: str = "text-embedding-3-small",
        *,
        encoding_format: Optional[str] = None,
        user: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> EmbeddingResponse:
        """Call ``POST /v1/embeddings`` and return a typed response.

        Parameters
        ----------
        input:
            A single string or a list of strings to embed.
        model:
            Model name. DASH treats this as a hint and uses its
            configured embedding provider for the actual vector.
        encoding_format:
            Currently DASH only supports ``"float"``; anything else
            returns an error from the server.
        user:
            Optional OpenAI-style opaque user identifier.
        timeout:
            Per-request timeout in seconds. Overrides the
            :class:`Client`-level default.
        """
        request = EmbeddingRequest(
            input=input,
            model=model,
            encoding_format=encoding_format,
            user=user,
        )
        return self._client._request_embeddings(request, timeout=timeout)


class Client:
    """Synchronous DASH client.

    Parameters
    ----------
    base_url:
        Root URL of the DASH service, e.g. ``"http://localhost:8080"``.
        The trailing slash is optional; the client normalises it.
    api_key:
        Optional bearer token. When set, sent as
        ``Authorization: Bearer <api_key>``. When ``None`` (the
        default), no auth header is added. This is convenient for local
        DASH instances that have auth disabled.
    timeout:
        Default per-request timeout in seconds. Individual calls can
        override it with their own ``timeout=`` argument.
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
        # Use a Session for connection pooling + sensible default
        # headers. The session is created lazily so that import cost is
        # zero for users who only want to inspect the type definitions.
        self._session = requests.Session()
        # ``setdefault`` doesn't always survive requests' internal
        # header-merge logic (the library has a hard-coded default
        # User-Agent fallback). We set it again on every request via
        # ``_default_headers`` below; setting it on the session here
        # is best-effort and harmless.
        self._session.headers.setdefault("User-Agent", _USER_AGENT)
        self._session.headers.setdefault("Accept", "application/json")
        # Expose the same shape the openai SDK uses.
        self.embeddings = EmbeddingsNamespace(self)

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    def __enter__(self) -> "Client":
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> None:
        self.close()

    def close(self) -> None:
        """Release the underlying HTTP connection pool.

        Safe to call multiple times.
        """
        if self._session is not None:
            self._session.close()
            self._session = None  # type: ignore[assignment]

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def retrieve(
        self,
        tenant_id: str,
        query: str,
        top_k: int = 10,
        stance_mode: str = "balanced",
        *,
        return_graph: Optional[bool] = None,
        timeout: Optional[float] = None,
    ) -> RetrieveResponse:
        """Call ``POST /v1/retrieve`` and return a typed response.

        Parameters
        ----------
        tenant_id:
            The tenant namespace to search within.
        query:
            Free-text query.
        top_k:
            Maximum number of claims to return.
        stance_mode:
            Either ``"balanced"`` (default) or ``"support_only"``.
            ``"support_only"`` filters out claims whose contradiction
            tally exceeds their support tally.
        return_graph:
            Optional flag to also return the claim graph. DASH will
            silently ignore it if the feature is not enabled.
        timeout:
            Per-request timeout in seconds. Overrides the client
            default.
        """
        request = RetrieveRequest(
            tenant_id=tenant_id,
            query=query,
            top_k=top_k,
            stance_mode=stance_mode,
            return_graph=return_graph,
        )
        return self._request_retrieve(request, timeout=timeout)

    # ------------------------------------------------------------------
    # Internal HTTP plumbing
    # ------------------------------------------------------------------

    def _auth_headers(self) -> Dict[str, str]:
        if self.api_key:
            return {"Authorization": f"Bearer {self.api_key}"}
        return {}

    def _default_headers(self) -> Dict[str, str]:
        """Headers sent on every request.

        Set explicitly per-request (rather than only on the
        ``Session``) so they survive ``requests``' internal header
        normalisation, which has hard-coded fallbacks for User-Agent
        and Accept.
        """
        return {"User-Agent": _USER_AGENT, "Accept": "application/json"}

    def _request(
        self,
        method: str,
        path: str,
        *,
        json_body: Optional[Mapping[str, Any]] = None,
        timeout: Optional[float] = None,
    ) -> Any:
        url = f"{self.base_url}{path}"
        headers = {**self._default_headers(), **self._auth_headers()}
        effective_timeout = self.timeout if timeout is None else timeout

        try:
            response = self._session.request(
                method,
                url,
                json=json_body,
                headers=headers,
                timeout=effective_timeout,
            )
        except requests.exceptions.Timeout as exc:
            raise DashConnectionError(
                f"request to DASH timed out after {effective_timeout}s: {exc}"
            ) from exc
        except requests.exceptions.ConnectionError as exc:
            raise DashConnectionError(
                f"failed to connect to DASH at {self.base_url}: {exc}"
            ) from exc
        except requests.exceptions.RequestException as exc:
            # Catch-all for other requests-level errors (SSL, etc).
            raise DashConnectionError(f"DASH request failed: {exc}") from exc

        return self._parse_response(response)

    @staticmethod
    def _parse_response(response: requests.Response) -> Any:
        # Parse JSON once, even on errors — the body is what the user
        # will want to inspect via DashAPIError.body.
        raw_body: Any
        parsed_body: Any
        try:
            raw_body = response.text
        except Exception:  # pragma: no cover - extremely defensive
            raw_body = None

        if raw_body:
            try:
                parsed_body = json.loads(raw_body)
            except (ValueError, TypeError):
                parsed_body = None
        else:
            parsed_body = None

        if 200 <= response.status_code < 300:
            return parsed_body

        raise DashAPIError.from_response(
            status_code=response.status_code,
            body=raw_body if raw_body is not None else response.content,
            parsed=parsed_body if isinstance(parsed_body, (dict, list)) else None,
        )

    def _request_embeddings(
        self,
        request: EmbeddingRequest,
        *,
        timeout: Optional[float] = None,
    ) -> EmbeddingResponse:
        body = self._request(
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

    def _request_retrieve(
        self,
        request: RetrieveRequest,
        *,
        timeout: Optional[float] = None,
    ) -> RetrieveResponse:
        body = self._request(
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


__all__ = ["Client", "EmbeddingsNamespace"]
