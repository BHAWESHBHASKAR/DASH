"""Exception hierarchy for the DASH Python client.

All public exceptions inherit from :class:`DashError`, so callers can
catch the whole family with a single ``except DashError`` clause while
still being able to distinguish network failures from HTTP errors.
"""

from __future__ import annotations

from typing import Any, Dict, Optional


class DashError(Exception):
    """Base class for every error raised by the DASH client.

    Catch this if you want to handle "anything went wrong talking to
    DASH" without caring about the specific failure mode.
    """


class DashConnectionError(DashError):
    """Raised when the client cannot reach DASH at all.

    Examples: DNS resolution failure, connection refused, read timeout.
    The underlying :class:`requests.RequestsException` (or
    :class:`httpx.RequestError` for the async client) is chained via
    ``__cause__`` so it is still inspectable.
    """


class DashAPIError(DashError):
    """Raised when DASH returns a non-2xx HTTP response.

    DASH's ``/v1/embeddings`` endpoint returns OpenAI-shaped errors:

        {"error": {"message": "...", "type": "invalid_request_error",
                   "param": null, "code": null}}

    The native ``/v1/retrieve`` endpoint returns a more ad-hoc shape, so
    the constructor tolerates both forms (and falls back to the raw
    body when no parseable ``error`` object is present).
    """

    def __init__(
        self,
        message: str,
        *,
        status_code: int,
        body: Optional[Any] = None,
        error_type: Optional[str] = None,
        error_message: Optional[str] = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.body = body
        self.error_type = error_type
        self.error_message = error_message

    @classmethod
    def from_response(
        cls,
        status_code: int,
        body: Any,
        parsed: Optional[Dict[str, Any]] = None,
    ) -> "DashAPIError":
        """Build a :class:`DashAPIError` from an HTTP response.

        ``parsed`` is the JSON-decoded body when available; when the
        server returned non-JSON we fall back to the raw ``body``
        string. The returned error always has a sensible
        ``error_message`` and ``error_type``, defaulting to
        ``"api_error"`` and the response status text when the body
        cannot be interpreted.
        """
        error_type: Optional[str] = None
        error_message: Optional[str] = None

        if isinstance(parsed, dict):
            err = parsed.get("error")
            if isinstance(err, dict):
                error_type = err.get("type") or err.get("code")
                error_message = err.get("message")
            elif isinstance(err, str):
                error_type = "api_error"
                error_message = err
            elif "message" in parsed and isinstance(parsed["message"], str):
                # Some endpoints (e.g. /v1/retrieve) put the message
                # directly on the top-level object.
                error_message = parsed["message"]
                error_type = "api_error"

        if error_message is None:
            if isinstance(body, str) and body:
                error_message = body
            else:
                error_message = f"HTTP {status_code}"
        if error_type is None:
            error_type = "api_error"

        return cls(
            f"DASH API error ({status_code} {error_type}): {error_message}",
            status_code=status_code,
            body=body,
            error_type=error_type,
            error_message=error_message,
        )

    def __repr__(self) -> str:
        return (
            f"DashAPIError(status_code={self.status_code!r}, "
            f"error_type={self.error_type!r}, "
            f"error_message={self.error_message!r})"
        )


__all__ = ["DashAPIError", "DashConnectionError", "DashError"]
