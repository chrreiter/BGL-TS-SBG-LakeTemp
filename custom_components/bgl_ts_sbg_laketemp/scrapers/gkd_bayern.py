from __future__ import annotations

"""Web scraping utilities for GKD Bayern lake temperature data.

This module provides an async scraper implementation to fetch and parse
weekly lake temperature measurements from GKD Bayern pages. It performs
robust HTML table extraction, handles German decimal formats, validates
data ranges, and exposes convenient APIs to get the latest and historical
values.

Design goals:
- Async-first networking with a reusable ``aiohttp.ClientSession``
- Minimal assumptions about the page layout beyond the two-column table
  ("Datum" and "Wassertemperatur [°C]") found in the "Tabelle" view
- Graceful error handling, with specific exception types
- Timezone-aware timestamps using Europe/Berlin
"""

from dataclasses import dataclass
from datetime import datetime
import logging
from typing import Sequence
from zoneinfo import ZoneInfo

import aiohttp
from aiohttp import ClientConnectorError, ClientResponseError
from bs4 import BeautifulSoup

from ..const import DEFAULT_USER_AGENT
from ..mixins import AsyncSessionMixin
from ..logging_utils import kv, log_operation


_LOGGER = logging.getLogger(__name__)


# ---------- Exceptions ----------


class ScraperError(Exception):
    """Base class for scraper-related errors."""


class NetworkError(ScraperError):
    """Network connectivity or DNS issues."""


class HttpError(ScraperError):
    """Non-2xx HTTP response or protocol error."""


class ParseError(ScraperError):
    """The HTML structure could not be parsed as expected."""


class NoDataError(ScraperError):
    """No usable measurement rows found in the page."""


# ---------- Data Structures ----------


BERLIN_TZ = ZoneInfo("Europe/Berlin")


@dataclass(frozen=True)
class GKDBayernRecord:
    """A single measurement parsed from the table.

    Attributes:
        timestamp: Timezone-aware timestamp in Europe/Berlin.
        temperature_c: Temperature in Celsius.
    """

    timestamp: datetime
    temperature_c: float


# ---------- Scraper Implementation ----------


class GKDBayernScraper(AsyncSessionMixin):
    """Async scraper for GKD Bayern lake temperatures.

    Example usage:

        async with GKDBayernScraper(url) as scraper:
            latest = await scraper.fetch_latest()

    The scraper can also be used with an externally managed session:

        session = aiohttp.ClientSession()
        try:
            scraper = GKDBayernScraper(url, session=session)
            records = await scraper.fetch_records()
        finally:
            await session.close()

    Args:
        url: The GKD Bayern base URL for the measurement page. The scraper
             will always target the explicit ".../tabelle" view derived from
             this URL, regardless of whether the provided URL already ends
             with "/tabelle" or not.
        user_agent: User-Agent header to use for requests.
        request_timeout_seconds: Total timeout per HTTP request.
        table_selector: Optional CSS selector to target a specific table.
        session: Optional externally managed aiohttp session.
    """

    def __init__(
        self,
        url: str,
        *,
        user_agent: str = DEFAULT_USER_AGENT,
        request_timeout_seconds: float = 15.0,
        table_selector: str | None = None,
        session: aiohttp.ClientSession | None = None,
    ) -> None:
        self._url = url
        self._user_agent = user_agent
        self._timeout = request_timeout_seconds
        self._table_selector = table_selector
        super().__init__(
            session=session,
            user_agent=user_agent,
            request_timeout_seconds=request_timeout_seconds,
            default_headers={"Accept": "text/html,application/xhtml+xml"},
        )

    # ----- Session management -----

    # Session management now provided by AsyncSessionMixin

    # ----- Public API -----

    async def fetch_latest(self) -> GKDBayernRecord:
        """Fetch and return the most recent record.

        Returns:
            GKDBayernRecord: The newest parsed measurement.

        Raises:
            NoDataError: If the page contains no usable measurement rows.
            NetworkError: On connectivity issues/timeouts.
            HttpError: On non-2xx HTTP responses or client errors.
            ParseError: If the HTML cannot be parsed as expected.
        """

        records = await self.fetch_records()
        if not records:
            raise NoDataError("No measurement rows found")
        return records[-1]

    async def fetch_records(self) -> list[GKDBayernRecord]:
        """Fetch and parse all available rows from the weekly table.

        Returns:
            list[GKDBayernRecord]: Records sorted by timestamp ascending.

        Raises:
            NetworkError: On connectivity issues/timeouts.
            HttpError: On non-2xx HTTP responses or client errors.
            ParseError: If the HTML cannot be parsed as expected.
            NoDataError: If no rows can be parsed from the table.
        """

        target_url = self._to_table_url(self._url)
        html = await self._fetch_html(target_url)
        records = self.parse_html_table(html)
        # Sort and deduplicate by timestamp
        records.sort(key=lambda r: r.timestamp)
        deduped: list[GKDBayernRecord] = []
        seen: set[datetime] = set()
        for record in records:
            if record.timestamp not in seen:
                seen.add(record.timestamp)
                deduped.append(record)
        return deduped

    # ----- Networking -----

    @staticmethod
    def _to_table_url(url: str) -> str:
        """Return the explicit table URL for a given GKD Bayern lake page.

        Args:
            url: Base GKD Bayern lake URL.

        Returns:
            str: URL that explicitly ends with "/tabelle".

        """
        stripped = url.rstrip("/")
        if stripped.endswith("tabelle"):
            return stripped
        return stripped + "/tabelle"

    async def _fetch_html(self, url: str) -> str:
        """Download and return the HTML content for the given URL.

        Args:
            url: Target URL to fetch.

        Returns:
            str: Raw HTML text.

        Raises:
            NetworkError: On connectivity or timeout issues.
            HttpError: On non-2xx HTTP responses or client errors.
        """
        session = await self._ensure_session()
        try:
            async with log_operation(
                _LOGGER,
                component="scraper.gkd_bayern",
                operation="http_get",
                url=url,
            ) as op:
                async with session.get(url) as resp:
                    # Raise for non-2xx
                    try:
                        resp.raise_for_status()
                    except ClientResponseError as exc:  # noqa: PERF203 - explicit branch fine here
                        raise HttpError(f"HTTP error {exc.status} for {url}") from exc
                    text = await resp.text()
                    op.set(status=resp.status, bytes=len(text))
                    return text
        except ClientConnectorError as exc:
            raise NetworkError(f"Network error while connecting to {url}") from exc
        except aiohttp.ServerTimeoutError as exc:
            raise NetworkError(f"Timeout while fetching {url}") from exc
        except aiohttp.ClientError as exc:
            raise HttpError(f"Client error while fetching {url}: {exc}") from exc

    # ----- Parsing -----

    @staticmethod
    def parse_html_table(html: str) -> list[GKDBayernRecord]:
        """Parse HTML content and return measurement rows.

        Args:
            html: HTML content of the table page.

        Returns:
            list[GKDBayernRecord]: Parsed measurement records.

        Raises:
            ParseError: When the table structure is unexpected.
            NoDataError: When no measurement rows can be parsed.
        """

        with log_operation(_LOGGER, component="scraper.gkd_bayern", operation="parse_table") as op:
            soup = BeautifulSoup(html, "html.parser")

            candidate_tables: list = []
            if soup and soup.body:
                if tables := soup.select("table"):
                    candidate_tables = list(tables)

            if not candidate_tables:
                raise ParseError("No <table> elements found in page")

            # Prefer a table with appropriate headers; otherwise, fallback to the first table
            chosen_table = None
            for table in candidate_tables:
                header_texts = GKDBayernScraper._extract_header_texts(table)
                if GKDBayernScraper._header_looks_like_measurement(header_texts):
                    chosen_table = table
                    break
            if chosen_table is None:
                chosen_table = candidate_tables[0]
                _LOGGER.debug(
                    "%s",
                    kv(
                        component="scraper.gkd_bayern",
                        operation="parse_table",
                        note="fallback_first_table",
                    ),
                )

            # Extract rows
            body = chosen_table.find("tbody") or chosen_table
            rows = body.find_all("tr") if body else []
            records: list[GKDBayernRecord] = []

            for row in rows:
                cells = row.find_all(["td", "th"])  # Some tables may not use <th> exclusively for headers
                if len(cells) < 2:
                    continue
                date_text = GKDBayernScraper._clean_text(cells[0].get_text(" "))
                temp_text = GKDBayernScraper._clean_text(cells[1].get_text(" "))

                # Ignore rows that are obviously non-data (e.g., links or empty second column)
                if not date_text or not temp_text or temp_text == "-":
                    continue

                # Parse timestamp and temperature
                try:
                    ts = GKDBayernScraper._parse_german_datetime(date_text)
                    temp_c = GKDBayernScraper._parse_temperature_c(temp_text)
                except ValueError:
                    # Skip unparseable rows, but keep parsing subsequent rows
                    _LOGGER.debug(
                        "%s",
                        kv(
                            component="scraper.gkd_bayern",
                            operation="parse_row_skip",
                            reason="unparsable",
                            date=date_text,
                            temp=temp_text,
                        ),
                    )
                    continue

                records.append(GKDBayernRecord(timestamp=ts, temperature_c=temp_c))

            op.set(tables=len(candidate_tables), rows=len(rows), records=len(records))

            if not records:
                raise NoDataError("No measurement rows parsed from table")

            return records

    # ----- Helpers -----

    @staticmethod
    def _extract_header_texts(table) -> list[str]:  # type: ignore[no-untyped-def]
        """Extract header cell texts from a table element.

        Args:
            table: BeautifulSoup table element.

        Returns:
            list[str]: Header texts in display order.

        Notes:
            Attempts to read from ``<thead>`` first, falling back to the first row
            if no explicit header section exists.
        """
        header_texts: list[str] = []
        # Try thead first
        thead = table.find("thead")
        if thead:
            for th in thead.find_all("th"):
                header_texts.append(GKDBayernScraper._clean_text(th.get_text(" ")))
        if not header_texts:
            # Try first row as header if <thead> not used
            first_row = table.find("tr")
            if first_row:
                for cell in first_row.find_all(["th", "td"]):
                    header_texts.append(GKDBayernScraper._clean_text(cell.get_text(" ")))
        return header_texts

    @staticmethod
    def _header_looks_like_measurement(header_texts: Sequence[str]) -> bool:
        """Return True if headers resemble a measurement table for date/temperature.

        Args:
            header_texts: Header strings extracted from the table.

        Returns:
            bool: True if likely a measurement table.
        """
        combined = " ".join(h.lower() for h in header_texts)
        return ("datum" in combined or "date" in combined) and ("wassertemperatur" in combined or "°c" in combined)

    @staticmethod
    def _clean_text(text: str) -> str:
        """Normalize whitespace in text and trim leading/trailing spaces.

        Args:
            text: Input string.

        Returns:
            str: Cleaned string.
        """
        return " ".join(text.split()).strip()

    @staticmethod
    def _parse_german_datetime(text: str) -> datetime:
        """Parse a German-style datetime string like '07.08.2025 16:00'.

        Args:
            text: Date/time string in German formats.

        Returns:
            datetime: Timezone-aware datetime in Europe/Berlin.

        Raises:
            ValueError: If none of the supported formats match.
        """

        # Remove potential trailing labels or non-breaking spaces
        cleaned = GKDBayernScraper._clean_text(text)
        # Support cases where seconds might be present, though uncommon on these pages
        fmt_candidates = ["%d.%m.%Y %H:%M:%S", "%d.%m.%Y %H:%M"]
        last_error: Exception | None = None
        for fmt in fmt_candidates:
            try:
                dt_naive = datetime.strptime(cleaned, fmt)
                return dt_naive.replace(tzinfo=BERLIN_TZ)
            except Exception as exc:  # noqa: BLE001 - try next format
                last_error = exc
        raise ValueError(f"Unrecognized date/time format: {text!r}; last_error={last_error}")

    @staticmethod
    def _parse_temperature_c(text: str) -> float:
        """Parse Celsius temperature with possible German decimal comma.

        Args:
            text: Temperature string such as '22,0', '21.3', or '21,3 °C'.

        Returns:
            float: Temperature in Celsius.

        Raises:
            ValueError: If the value is missing or out of plausible range.
        """

        cleaned = GKDBayernScraper._clean_text(text)
        # Remove units and normalize decimal comma
        cleaned = cleaned.lower().replace("°c", "").replace("°", "").replace("c", "")
        cleaned = cleaned.replace(" ", "").replace(",", ".")

        # Extract the leading float-like token
        number_chars = "0123456789.+-"
        numeric = "".join(ch for ch in cleaned if ch in number_chars)
        if numeric in ("", "+", "-", "."):
            raise ValueError(f"No numeric value in temperature: {text!r}")

        value = float(numeric)

        # Plausibility bounds for water temperature; adjust if needed
        if not (-5.0 <= value <= 45.0):
            raise ValueError(f"Out-of-range temperature value: {value}")

        return value


__all__ = [
    "GKDBayernScraper",
    "GKDBayernRecord",
    "ScraperError",
    "NetworkError",
    "HttpError",
    "ParseError",
    "NoDataError",
]


