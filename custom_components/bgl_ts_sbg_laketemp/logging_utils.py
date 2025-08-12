from __future__ import annotations

"""Structured logging helpers for the BGL-TS-SBG-LakeTemp integration.

This module provides small utilities to emit consistent key=value style logs
and to measure operation durations. It intentionally does not change or depend
on global logging configuration; Home Assistant controls log levels/formatting.

Usage examples:

    from .logging_utils import kv, log_operation

    _LOGGER.debug("%s", kv(component="scraper.gkd_bayern", op="start", url=url))

    async with log_operation(_LOGGER, component="scraper.gkd_bayern", operation="http_get", url=url) as log:
        async with session.get(url) as resp:
            text = await resp.text()
            log.set(status=resp.status, bytes=len(text))

All helpers are safe no-ops at higher log levels; they only format strings when
the target logger level would emit them.
"""

from dataclasses import dataclass, field
import logging
import time
from typing import Any, Dict, Mapping, MutableMapping


def kv(mapping: Mapping[str, Any] | None = None, /, **kwargs: Any) -> str:
    """Return a stable key=value string for structured logs.

    - Keys are sorted alphabetically for stable output.
    - Values are rendered compactly. Strings containing whitespace or '=' are
      quoted with double quotes. None renders as '-'. Booleans render as
      lower-case.

    Args:
        mapping: Optional mapping providing base fields.
        **kwargs: Additional fields to include; override ``mapping`` on conflicts.

    Returns:
        A single string like ``"component=scraper op=parse rows=12 duration_ms=34"``.
    """

    fields: Dict[str, Any] = {}
    if mapping:
        fields.update(mapping)
    fields.update(kwargs)

    def _render_value(value: Any) -> str:
        if value is None:
            return "-"
        if isinstance(value, bool):
            return "true" if value else "false"
        text = str(value)
        if (" " in text) or ("=" in text) or ("\t" in text):
            return f'"{text}"'
        return text

    items = [f"{k}={_render_value(v)}" for k, v in sorted(fields.items(), key=lambda it: it[0])]
    return " ".join(items)


@dataclass
class _OperationLogger:
    """Context manager to log start/end with duration and extra fields.

    Use via :func:`log_operation` to get proper typing and defaults.
    """

    logger: logging.Logger
    component: str
    operation: str
    level_start: int = logging.DEBUG
    level_end: int = logging.DEBUG
    base_fields: MutableMapping[str, Any] = field(default_factory=dict)

    _start_monotonic: float = field(init=False, default=0.0)
    _fields: Dict[str, Any] = field(init=False, default_factory=dict)

    def set(self, **fields: Any) -> None:
        """Set or update fields to be included in the end log record."""

        self._fields.update(fields)

    # Synchronous context manager support
    def __enter__(self):  # noqa: ANN001 - context manager protocol
        self._start_monotonic = time.monotonic()
        if self.logger.isEnabledFor(self.level_start):
            self.logger.log(
                self.level_start,
                "%s",
                kv(self.base_fields, component=self.component, op="start", operation=self.operation),
            )
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001 - context manager protocol
        self._finish(exc)
        return False  # propagate exceptions

    # Async context manager support
    async def __aenter__(self):  # noqa: ANN001 - context manager protocol
        return self.__enter__()

    async def __aexit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001 - context manager protocol
        self.__exit__(exc_type, exc, tb)

    def _finish(self, exc: BaseException | None) -> None:
        duration_ms = int((time.monotonic() - self._start_monotonic) * 1000)
        level = logging.ERROR if exc is not None else self.level_end
        status = "error" if exc is not None else "finish"
        fields: Dict[str, Any] = dict(self.base_fields)
        fields.update(self._fields)
        fields.update({
            "component": self.component,
            "op": status,
            "operation": self.operation,
            "duration_ms": duration_ms,
        })
        if self.logger.isEnabledFor(level):
            if exc is not None:
                # Include exception type and message as fields; stack via exc_info
                fields.setdefault("exc_type", type(exc).__name__)
                fields.setdefault("error", str(exc))
                self.logger.log(level, "%s", kv(fields), exc_info=True)
            else:
                self.logger.log(level, "%s", kv(fields))


def log_operation(
    logger: logging.Logger,
    *,
    component: str,
    operation: str,
    level_start: int = logging.DEBUG,
    level_end: int = logging.DEBUG,
    **base_fields: Any,
) -> _OperationLogger:
    """Create a context manager to log start/finish with duration.

    Example:

        async with log_operation(_LOGGER, component="scraper.gkd", operation="parse", rows_seen=0) as log:
            ... do work ...
            log.set(rows_seen=42)

    Args:
        logger: Target logger.
        component: Logical component identifier (e.g., ``"scraper.gkd_bayern"``).
        operation: Short operation name (e.g., ``"http_get"``, ``"parse_table"``).
        level_start: Log level for the start record (default DEBUG).
        level_end: Log level for the finish record if no error (default DEBUG).
        **base_fields: Additional fields to include in both records.

    Returns:
        An async-aware context manager. On exit, logs either a finish or error
        record with ``duration_ms`` included.
    """

    return _OperationLogger(
        logger=logger,
        component=component,
        operation=operation,
        level_start=level_start,
        level_end=level_end,
        base_fields=dict(base_fields),
    )


__all__ = ["kv", "log_operation"]


