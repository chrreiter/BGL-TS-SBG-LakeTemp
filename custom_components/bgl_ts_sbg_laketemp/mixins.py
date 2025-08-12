from __future__ import annotations

"""Reusable mixins and helpers for async HTTP session management.

This module provides a small mixin, :class:`AsyncSessionMixin`, that encapsulates
``aiohttp.ClientSession`` lifecycle and configuration. It supports both
externally managed sessions and internally owned sessions, exposes an async
context manager API, and allows callers to configure request timeouts and
headers (including a default User-Agent).

Usage:

    class MyScraper(AsyncSessionMixin):
        def __init__(self, *, user_agent: str, request_timeout_seconds: float = 15.0, session: aiohttp.ClientSession | None = None) -> None:
            super().__init__(
                session=session,
                user_agent=user_agent,
                request_timeout_seconds=request_timeout_seconds,
                default_headers={"Accept": "text/plain, */*"},
            )

        async def fetch(self) -> str:
            session = await self._ensure_session()
            async with session.get("https://example.com") as resp:
                resp.raise_for_status()
                return await resp.text()

The mixin never closes an external session. It only closes an internally
created session on :meth:`close` or when exiting the async context manager.
"""

from typing import Mapping, MutableMapping, Optional
import logging

import aiohttp


_LOGGER = logging.getLogger(__name__)


class AsyncSessionMixin:
    """Mixin that manages an ``aiohttp.ClientSession`` for subclasses.

    Subclasses should call ``super().__init__`` from their own ``__init__`` and
    then use :meth:`_ensure_session` to obtain a live session for requests. The
    mixin supports both externally supplied sessions (which it will not close)
    and internally created sessions (which it will close on :meth:`close`).

    All parameters are stored on the instance to aid introspection and testing.
    """

    def __init__(
        self,
        *,
        session: Optional[aiohttp.ClientSession],
        user_agent: str,
        request_timeout_seconds: float,
        default_headers: Optional[Mapping[str, str]] = None,
        extra_headers: Optional[Mapping[str, str]] = None,
    ) -> None:
        self._session_external: Optional[aiohttp.ClientSession] = session
        self._session_owned: Optional[aiohttp.ClientSession] = None
        self._request_timeout_seconds: float = float(request_timeout_seconds)
        self._user_agent: str = user_agent

        # Compose headers: start with defaults, ensure User-Agent, then overlay extras
        composed_headers: dict[str, str] = {}
        if default_headers is not None:
            composed_headers.update(dict(default_headers))
        # Ensure a realistic UA is always present unless explicitly overridden
        composed_headers.setdefault("User-Agent", self._user_agent)
        if extra_headers is not None:
            composed_headers.update(dict(extra_headers))
        self._session_headers: Mapping[str, str] = composed_headers

    async def __aenter__(self):  # noqa: ANN001 - typing varies by subclass
        await self._ensure_session()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001
        await self.close()

    async def _ensure_session(self) -> aiohttp.ClientSession:
        """Return a live ``ClientSession``, creating an internal one if needed."""
        if self._session_external is not None and not self._session_external.closed:
            return self._session_external
        if self._session_owned is None or self._session_owned.closed:
            _LOGGER.debug("Creating internal aiohttp session (timeout=%s)", self._request_timeout_seconds)
            timeout = aiohttp.ClientTimeout(total=self._request_timeout_seconds)
            self._session_owned = aiohttp.ClientSession(headers=self._session_headers, timeout=timeout)
        return self._session_owned

    async def close(self) -> None:
        """Close the internally created session if present.

        External sessions (provided by the caller) are never closed here.
        """
        if self._session_owned is not None and not self._session_owned.closed:
            await self._session_owned.close()


__all__ = ["AsyncSessionMixin"]


