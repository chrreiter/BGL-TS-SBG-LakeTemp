from __future__ import annotations

"""Data source abstraction for lake temperature providers.

This module defines a small interface to unify how the integration fetches
lake temperatures from different providers. It includes:

- An abstract base class ``DataSourceInterface`` that enforces the minimal API
  required by the sensor platform
- A typing ``Protocol`` to allow duck-typed implementations in tests
- A concrete ``GKDBayernSource`` implementation that composes the existing
  ``GKDBayernScraper`` for HTML table extraction
- A ``create_data_source`` factory to instantiate sources from ``LakeConfig``

The goal is to make it easy to add additional providers without changing the
sensor logic.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
import abc
import logging
from typing import Protocol, runtime_checkable, Optional

import aiohttp

from .const import (
    DEFAULT_USER_AGENT,
    LakeConfig,
    LakeSourceType,
    GkdBayernOptions,
    HydroOOEOptions,
)
from .scrapers.gkd_bayern import GKDBayernScraper
from .scrapers.hydro_ooe import HydroOOEScraper


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
            A ``TemperatureReading`` containing the latest measurement.
        """

    @abc.abstractmethod
    def get_update_frequency(self) -> timedelta:
        """Return how frequently the upstream data is expected to update.

        This is advisory and can inform default polling intervals.
        """

    async def close(self) -> None:  # noqa: D401 - trivial no-op override point
        """Optional hook to close resources if the implementation created any."""
        return None


@runtime_checkable
class DataSourceProtocol(Protocol):
    """Typing protocol for duck-typed data sources.

    Any class implementing these methods is considered a valid data source,
    regardless of inheritance.
    """

    async def fetch_temperature(self) -> TemperatureReading:  # pragma: no cover - signature only
        ...

    def get_update_frequency(self) -> timedelta:  # pragma: no cover - signature only
        ...


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

    def get_update_frequency(self) -> timedelta:
        # GKD Bayern lake temperature tables typically update hourly; we choose
        # a conservative 30 minutes to pick up changes reasonably quickly.
        return timedelta(minutes=30)


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
        A ``DataSourceInterface`` implementation for the configured source type.

    Raises:
        NotImplementedError: If the source type is not supported yet.
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
    if source_type is LakeSourceType.HYDRO_OOE:
        # Prefer explicit station_id from options; otherwise derive from URL when possible.
        station_id: str | None = None
        api_base: str | None = None
        parameter: str | None = None
        period: str | None = None
        if isinstance(lake.source.options, HydroOOEOptions) and lake.source.options.station_id:
            station_id = str(lake.source.options.station_id)
            api_base = lake.source.options.api_base
            parameter = lake.source.options.parameter
            period = lake.source.options.period
        else:
            # Best-effort extraction; may not match SANR used in ZRXP. Scraper also uses name hint.
            try:
                station_id = _extract_station_id_from_url(lake.url)
            except Exception:
                station_id = None
        return _HydroOOESourceAdapter(
            station_id=station_id,
            user_agent=lake.user_agent,
            session=session,
            api_base=api_base,
            parameter=parameter,
            period=period,
            name_hint=lake.name,
        )

    raise NotImplementedError(f"Unsupported source type: {source_type}")


__all__ = [
    "TemperatureReading",
    "DataSourceInterface",
    "DataSourceProtocol",
    "GKDBayernSource",
    "create_data_source",
]


class _HydroOOESourceAdapter(DataSourceInterface):
    """Adapter to use HydroOOEScraper as a data source implementation."""

    def __init__(
        self,
        *,
        station_id: str | None,
        user_agent: str,
        session: Optional[aiohttp.ClientSession],
        request_timeout_seconds: float = 15.0,
        api_base: str | None = None,
        parameter: str | None = None,
        period: str | None = None,
        name_hint: str | None = None,
    ) -> None:
        self._station_id = station_id
        self._user_agent = user_agent
        self._session = session
        self._timeout = request_timeout_seconds
        self._api_base = api_base
        self._parameter = parameter or "temperature"
        self._period = period or "P7D"
        self._name_hint = name_hint

    async def fetch_temperature(self) -> TemperatureReading:
        if self._session is not None:
            scraper = HydroOOEScraper(
                station_id=self._station_id,
                session=self._session,
                user_agent=self._user_agent,
                request_timeout_seconds=self._timeout,
                name_hint=self._name_hint,
            )
            latest = await scraper.fetch_latest()
        else:
            async with HydroOOEScraper(
                station_id=self._station_id,
                user_agent=self._user_agent,
                request_timeout_seconds=self._timeout,
                name_hint=self._name_hint,
            ) as scraper:
                latest = await scraper.fetch_latest()

        return TemperatureReading(
            timestamp=latest.timestamp,
            temperature_c=latest.temperature_c,
            source="hydro_ooe",
        )

    def get_update_frequency(self) -> timedelta:
        return timedelta(minutes=30)


def _extract_station_id_from_url(url: str) -> str:
    # Hydro OOE SPA URLs often contain a segment like "/station/<id>/".
    # We parse the path and return the last purely numeric token as a heuristic.
    try:
        path = url.split("?")[0]
        tokens = [t for t in path.split("/") if t]
        numeric = [t for t in tokens if t.isdigit()]
        if numeric:
            return numeric[-1]
    except Exception:
        pass
    raise ValueError("Unable to extract station_id from Hydro OOE URL")
