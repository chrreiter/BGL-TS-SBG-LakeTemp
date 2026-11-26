from __future__ import annotations

"""Data source abstraction for lake temperature providers.

This module defines a small interface to unify how the integration fetches
lake temperatures from different providers. It includes:

- An abstract base class ``DataSourceInterface`` that enforces the minimal API
  required by the sensor platform
- A concrete ``GKDBayernSource`` implementation that composes the existing
  ``GKDBayernScraper`` for HTML table extraction
- A ``create_data_source`` factory to instantiate per-lake sources from ``LakeConfig``

Shared-dataset sources (Hydro OÖ, Salzburg OGD) do not go through this module;
they are served by the dataset coordinators in ``dataset_coordinators.py``.
"""

from dataclasses import dataclass
from datetime import datetime
import abc
import logging
from typing import Optional

import aiohttp

from .const import (
    DEFAULT_USER_AGENT,
    LakeConfig,
    LakeSourceType,
    GkdBayernOptions,
)
from .scrapers.gkd_bayern import GKDBayernScraper


_LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class TemperatureReading:
    """Represents a single temperature reading from a data source.

    Attributes:
        timestamp: Timezone-aware timestamp when the measurement was taken.
        temperature_c: Temperature value in Celsius.
        source: Identifier for the underlying provider/source type.
    """

    timestamp: datetime
    temperature_c: float
    source: str

    def __post_init__(self) -> None:
        """Validate and normalize the ``source`` identifier.

        Ensures the ``source`` field corresponds to a supported ``LakeSourceType``
        value.

        Raises:
            ValueError: If the provided source value is not recognized.
        """
        # Normalize if caller passed an enum instance despite the type hint
        if isinstance(self.source, LakeSourceType):
            object.__setattr__(self, "source", self.source.value)
            return

        # Validate provided string maps to a known LakeSourceType
        try:
            LakeSourceType(str(self.source))
        except Exception as exc:  # noqa: BLE001 - surface a clearer error
            raise ValueError(f"Invalid source identifier for TemperatureReading: {self.source!r}") from exc


class DataSourceInterface(abc.ABC):
    """Abstract base class for temperature data sources.

    Implementations should be lightweight and stateless, delegating any heavy
    lifting (HTTP, parsing) to helpers. They may accept a shared
    ``aiohttp.ClientSession`` to enable connection reuse across sensors.
    """

    @abc.abstractmethod
    async def fetch_temperature(self) -> TemperatureReading:
        """Fetch the most recent temperature reading.

        Returns:
            TemperatureReading: Latest measurement from the source.
        """

    async def close(self) -> None:  # noqa: D401 - trivial no-op override point
        """Optional hook to close resources if the implementation created any."""
        return None


class GKDBayernSource(DataSourceInterface):
    """Concrete data source using the GKD Bayern HTML table pages.

    This wraps ``GKDBayernScraper`` to provide the interface required by the
    sensor platform while allowing reuse of an external ``aiohttp`` session.

    Args:
        url: Measurement page URL (either the standard "messwerte" view or the
            explicit ".../tabelle" page).
        user_agent: User-Agent string to send on HTTP requests.
        table_selector: Optional CSS selector to target a specific table on the page.
        request_timeout_seconds: Total timeout for individual HTTP requests.
        session: Optional externally managed ``aiohttp.ClientSession``.
    """

    def __init__(
        self,
        *,
        url: str,
        user_agent: str = DEFAULT_USER_AGENT,
        table_selector: str | None = None,
        request_timeout_seconds: float = 15.0,
        session: Optional[aiohttp.ClientSession] = None,
    ) -> None:
        self._url = url
        self._user_agent = user_agent
        self._table_selector = table_selector
        self._timeout = request_timeout_seconds
        self._session = session

    async def fetch_temperature(self) -> TemperatureReading:
        """Fetch the latest temperature from the configured GKD Bayern page."""
        if self._session is not None:
            scraper = GKDBayernScraper(
                self._url,
                user_agent=self._user_agent,
                request_timeout_seconds=self._timeout,
                table_selector=self._table_selector,
                session=self._session,
            )
            latest = await scraper.fetch_latest()
        else:
            async with GKDBayernScraper(
                self._url,
                user_agent=self._user_agent,
                request_timeout_seconds=self._timeout,
                table_selector=self._table_selector,
            ) as scraper:
                latest = await scraper.fetch_latest()

        return TemperatureReading(
            timestamp=latest.timestamp,
            temperature_c=latest.temperature_c,
            source=LakeSourceType.GKD_BAYERN.value,
        )


def create_data_source(
    lake: LakeConfig,
    *,
    session: Optional[aiohttp.ClientSession] = None,
) -> DataSourceInterface:
    """Factory for creating a data source instance from ``LakeConfig``.

    Args:
        lake: Validated configuration for a single lake sensor.
        session: Optional shared ``aiohttp.ClientSession`` for HTTP reuse.

    Returns:
        DataSourceInterface: Implementation for the configured source type.

    Raises:
        NotImplementedError: If the source type has no per-lake implementation
            (e.g. shared-dataset sources ``hydro_ooe`` / ``salzburg_ogd``, which
            are handled by dataset coordinators).
    """

    source_type = lake.source.type

    if source_type is LakeSourceType.GKD_BAYERN:
        options = lake.source.options
        table_selector: str | None = None
        if isinstance(options, GkdBayernOptions):
            table_selector = options.table_selector
        return GKDBayernSource(
            url=lake.url,
            user_agent=lake.user_agent,
            table_selector=table_selector,
            session=session,
        )

    raise NotImplementedError(f"Unsupported source type: {source_type}")


__all__ = [
    "TemperatureReading",
    "DataSourceInterface",
    "GKDBayernSource",
    "create_data_source",
]
