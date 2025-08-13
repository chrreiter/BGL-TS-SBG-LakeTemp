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
import re
from urllib.parse import urlparse
from typing import Protocol, runtime_checkable, Optional

import aiohttp

from .const import (
    DEFAULT_USER_AGENT,
    LakeConfig,
    LakeSourceType,
    GkdBayernOptions,
    HydroOOEOptions,
    SalzburgOGDOptions,
)
from .scrapers.gkd_bayern import GKDBayernScraper
from .scrapers.hydro_ooe import HydroOOEScraper
from .scrapers.salzburg_ogd import SalzburgOGDScraper


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
        value. Accepts either the enum value string or a ``LakeSourceType``
        instance, normalizing the latter to its ``.value``.
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
        # Prefer explicit station_id from options; otherwise rely on name-based selection.
        station_id: str | None = None
        if isinstance(lake.source.options, HydroOOEOptions) and lake.source.options.station_id:
            station_id = str(lake.source.options.station_id)
        else:
            _LOGGER.debug(
                "No explicit Hydro OOE station_id provided; relying on name-based selection: %s",
                lake.name,
            )
        return _HydroOOESourceAdapter(
            station_id=station_id,
            user_agent=lake.user_agent,
            session=session,
            name_hint=lake.name,
        )
    if source_type is LakeSourceType.SALZBURG_OGD:
        lake_name_opt: str | None = None
        if isinstance(lake.source.options, SalzburgOGDOptions):
            lake_name_opt = lake.source.options.lake_name
        return _SalzburgOGDSourceAdapter(
            lake_name=lake_name_opt or lake.name,
            user_agent=lake.user_agent,
            session=session,
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
            source=LakeSourceType.HYDRO_OOE.value,
        )

    def get_update_frequency(self) -> timedelta:
        return timedelta(minutes=30)


def _extract_station_id_from_url(url: str) -> str:
    """Extract a Hydro OOE station id (SANR) from a variety of URL formats.

    Supported patterns include (in either path or hash fragment):
    - ``.../station/<id>/...``
    - ``.../sanr/<id>/...``
    - ``.../id/<id>/...``
    - ``...?sanr=<id>`` (also inside the fragment query part)
    - ``...?station=<id>`` or ``...?id=<id>``
    - fallback: a unique 3-8 digit token anywhere in the path/fragment

    Args:
        url: The Hydro OOE URL as configured by the user.

    Returns:
        The extracted station id as a string.

    Raises:
        ValueError: If the URL is invalid for Hydro OOE or no station id can be
            confidently extracted.
    """

    if not isinstance(url, str) or not url:
        raise ValueError("URL must be a non-empty string for Hydro OOE station extraction")

    parsed = urlparse(url)
    lowered_netloc = (parsed.netloc or "").lower()
    # Validate known domain for Hydro OOE; we only attempt strong parsing in this case.
    if "hydro.ooe.gv.at" not in lowered_netloc:
        raise ValueError(
            f"Unrecognized domain for Hydro OOE URL: '{parsed.netloc}'. Expected 'hydro.ooe.gv.at'"
        )

    # Build a unified search string from fragment and path to catch SPA formats.
    # Fragment usually contains the SPA router path (e.g., '#/overview/.../station/16579/...').
    fragment = parsed.fragment or ""
    path = parsed.path or ""
    search_spaces = [fragment, path, url]

    # First, try explicit and reliable patterns.
    reliable_patterns: list[str] = [
        r"(?:^|[/#?&])station/(\d{3,8})(?=[/#?&]|$)",
        r"(?:^|[/#?&])sanr/(\d{3,8})(?=[/#?&]|$)",
        r"(?:^|[/#?&])id/(\d{3,8})(?=[/#?&]|$)",
        r"[?&#](?:sanr|station|id)=(\d{3,8})(?=[&#]|$)",
        r"station[-_](\d{3,8})(?=[/#?&]|$)",
    ]

    for space in search_spaces:
        for pat in reliable_patterns:
            m = re.search(pat, space, re.IGNORECASE)
            if m:
                station_id = m.group(1)
                _LOGGER.debug("Extracted Hydro OOE station id via pattern %r: %s", pat, station_id)
                return station_id

    # Fallback: collect all standalone 3-8 digit tokens within path/fragment.
    candidates: set[str] = set()
    token_re = re.compile(r"(?<!\d)(\d{3,8})(?!\d)")
    for space in (fragment, path):
        for m in token_re.finditer(space):
            candidates.add(m.group(1))

    if len(candidates) == 1:
        only = next(iter(candidates))
        _LOGGER.debug("Falling back to unique numeric token for Hydro OOE station id: %s", only)
        return only

    hint = "; found none" if not candidates else f"; ambiguous candidates={sorted(candidates)}"
    raise ValueError(f"Unable to extract Hydro OOE station id from URL{hint}: {url}")


def _extract_gkd_station_id_from_url(url: str) -> str:
    """Extract the numeric station id from a GKD Bayern lake URL.

    Typical formats:
    - https://www.gkd.bayern.de/de/seen/wassertemperatur/<region>/<slug>-<id>/messwerte
    - https://www.gkd.bayern.de/de/seen/wassertemperatur/<region>/<slug>-<id>/messwerte/tabelle

    Args:
        url: GKD Bayern URL for a lake measurement page.

    Returns:
        The extracted numeric station id as a string.

    Raises:
        ValueError: If the URL is not a GKD Bayern URL or no id can be extracted.
    """

    if not isinstance(url, str) or not url:
        raise ValueError("URL must be a non-empty string for GKD station extraction")

    parsed = urlparse(url)
    netloc = (parsed.netloc or "").lower()
    if "gkd.bayern.de" not in netloc:
        raise ValueError(f"Unrecognized domain for GKD Bayern URL: '{parsed.netloc}'")

    path = parsed.path or ""
    # Ensure this looks like a seen/wassertemperatur path
    if "/seen/wassertemperatur/" not in path:
        raise ValueError("URL does not look like a GKD 'seen/wassertemperatur' path")

    # Extract id from the station slug token, which typically ends with '-<digits>'
    tokens = [t for t in path.split("/") if t]
    id_match: str | None = None
    for token in tokens:
        # Only consider tokens that reasonably could be the station slug
        # e.g., 'seethal-18673955', 'koenigssee-18624806'
        m = re.search(r"-(\d{5,10})$", token)
        if m:
            id_match = m.group(1)
            break

    if id_match:
        _LOGGER.debug("Extracted GKD station id from URL: %s", id_match)
        return id_match

    # Fallback: search anywhere in the path for '-<digits>' pattern
    m2 = re.search(r"-(\d{5,10})(?=/|$)", path)
    if m2:
        station_id = m2.group(1)
        _LOGGER.debug("Extracted GKD station id via fallback: %s", station_id)
        return station_id

    raise ValueError(f"Unable to extract GKD station id from URL: {url}")


class _SalzburgOGDSourceAdapter(DataSourceInterface):
    """Adapter to use SalzburgOGDScraper as a data source implementation.

    Fetches the full OGD file and selects the newest measurement for a specific
    lake name (case/diacritic-insensitive matching handled by the scraper).
    """

    def __init__(
        self,
        *,
        lake_name: str,
        user_agent: str,
        session: Optional[aiohttp.ClientSession],
        request_timeout_seconds: float = 20.0,
        url: str = "https://www.salzburg.gv.at/ogd/56c28e2d-8b9e-41ba-b7d6-fa4896b5b48b/Hydrografie%20Seen.txt",
    ) -> None:
        self._lake_name = lake_name
        self._user_agent = user_agent
        self._session = session
        self._timeout = request_timeout_seconds
        self._url = url

    async def fetch_temperature(self) -> TemperatureReading:
        if self._session is not None:
            scraper = SalzburgOGDScraper(
                url=self._url,
                session=self._session,
                user_agent=self._user_agent,
                request_timeout_seconds=self._timeout,
            )
            latest = await scraper.fetch_latest_for_lake(self._lake_name)
        else:
            async with SalzburgOGDScraper(
                url=self._url,
                user_agent=self._user_agent,
                request_timeout_seconds=self._timeout,
            ) as scraper:
                latest = await scraper.fetch_latest_for_lake(self._lake_name)

        return TemperatureReading(
            timestamp=latest.timestamp,
            temperature_c=latest.temperature_c,
            source=LakeSourceType.SALZBURG_OGD.value,
        )

    def get_update_frequency(self) -> timedelta:
        # Source notes: updates every 2-3 hours; choose 2 hours conservatively
        return timedelta(hours=2)
