from __future__ import annotations

"""Scraper for Hydro OOE (hydro.ooe.gv.at) water temperature data.

The Hydro OOE website is a JavaScript SPA and does not expose a stable public
JSON API for scraping. Instead, the provider publishes a bulk export in ZRXP
format containing water temperature series for all stations:

    https://data.ooe.gv.at/files/hydro/HDOOE_Export_WT.zrxp

This scraper downloads that bulk file and extracts the series for a specific
station, selected by SANR (station number) or by a case-insensitive name
substring. The parsed records are returned as timezone-aware timestamps with
plausibility-checked Celsius values.

We keep the same exception types as the GKD scraper for consistency.
"""

from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
import logging
from typing import Any, Iterable, List, Optional

import aiohttp
from aiohttp import ClientConnectorError, ClientResponseError
import re

from ..mixins import AsyncSessionMixin


_LOGGER = logging.getLogger(__name__)


class ScraperError(Exception):
    """Base class for scraper-related errors."""


class NetworkError(ScraperError):
    """Network connectivity or DNS issues."""


class HttpError(ScraperError):
    """Non-2xx HTTP response or protocol error."""


class ParseError(ScraperError):
    """The payload structure could not be parsed as expected."""


class NoDataError(ScraperError):
    """No usable measurement points found in the response."""


@dataclass(frozen=True)
class HydroOOERecord:
    """Single measurement record from Hydro OOE timeseries."""

    timestamp: datetime
    temperature_c: float


class HydroOOEScraper(AsyncSessionMixin):
    """Async scraper for Hydro OOE water temperatures via ZRXP bulk export.

    Selection heuristics:
    - Prefer explicit ``sanr`` (station number in ZRXP blocks)
    - Else prefer explicit ``sname_contains`` substring match on SNAME
    - Else attempt to use ``name_hint`` to match SNAME
    - Else, if ``station_id`` is provided and numeric, treat it like SANR
    """

    def __init__(
        self,
        *,
        station_id: str | None = None,
        sanr: str | None = None,
        sname_contains: str | None = None,
        name_hint: str | None = None,
        session: aiohttp.ClientSession | None = None,
        user_agent: str | None = None,
        request_timeout_seconds: float = 15.0,
    ) -> None:
        self._station_id = station_id
        self._sanr = str(sanr) if sanr is not None else None
        self._sname_contains = sname_contains
        self._name_hint = name_hint
        self._timeout = request_timeout_seconds
        self._user_agent = user_agent or (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)"
        )
        super().__init__(
            session=session,
            user_agent=self._user_agent,
            request_timeout_seconds=request_timeout_seconds,
            default_headers={"Accept": "text/plain, */*"},
        )

    # Session management provided by AsyncSessionMixin

    async def fetch_latest(self) -> HydroOOERecord:
        records = await self.fetch_records()
        if not records:
            raise NoDataError("No measurement rows found")
        return records[-1]

    async def fetch_records(self) -> list[HydroOOERecord]:
        """Fetch recent timeseries points from the ZRXP bulk export.

        Returns a sorted, de-duplicated list of records for the selected station.
        """

        # Download bulk export
        zrxp_url = "https://data.ooe.gv.at/files/hydro/HDOOE_Export_WT.zrxp"
        text = await self._fetch_text(zrxp_url)

        # Parse into station blocks and choose target
        try:
            blocks = self._split_zrxp_blocks(text)
        except Exception as exc:  # noqa: BLE001
            raise ParseError(f"Failed to split ZRXP content: {exc}") from exc

        target_block: Optional[str] = self._select_block(blocks)
        if target_block is None:
            raise NoDataError("No matching station found in ZRXP export")

        try:
            records = self._parse_zrxp_block(target_block)
        except NoDataError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise ParseError(f"Failed to parse ZRXP block: {exc}") from exc

        # Sort and deduplicate
        records.sort(key=lambda r: r.timestamp)
        unique: list[HydroOOERecord] = []
        seen: set[datetime] = set()
        for r in records:
            if r.timestamp not in seen:
                seen.add(r.timestamp)
                unique.append(r)
        if not unique:
            raise NoDataError("No measurement rows found in selected station block")
        return unique

    async def _fetch_text(self, url: str) -> str:
        session = await self._ensure_session()
        try:
            _LOGGER.debug("Fetching Hydro OOE ZRXP: %s", url)
            async with session.get(url) as resp:
                try:
                    resp.raise_for_status()
                except ClientResponseError as exc:
                    raise HttpError(f"HTTP error {exc.status} for {url}") from exc
                # Robust decoding: try server-declared/UTF-8 first, then latin-1 and cp1252 as fallbacks
                raw = await resp.read()
                encodings = []
                if resp.charset:
                    encodings.append(resp.charset)
                encodings.extend(["utf-8", "latin-1", "cp1252"])
                last_error: Exception | None = None
                for enc in encodings:
                    try:
                        return raw.decode(enc)
                    except Exception as dec_err:  # noqa: BLE001
                        last_error = dec_err
                        continue
                # As a last resort, replace undecodable bytes
                try:
                    return raw.decode("utf-8", errors="replace")
                except Exception as exc2:  # noqa: BLE001
                    if last_error is not None:
                        raise last_error
                    raise exc2
        except ClientConnectorError as exc:
            raise NetworkError(f"Network error while connecting to {url}") from exc
        except aiohttp.ServerTimeoutError as exc:
            raise NetworkError(f"Timeout while fetching {url}") from exc
        except aiohttp.ClientError as exc:
            raise HttpError(f"Client error while fetching {url}: {exc}") from exc
        except HttpError:
            raise

    @staticmethod
    def _split_zrxp_blocks(text: str) -> list[str]:
        """Split bulk ZRXP content into station-specific blocks.

        Blocks are delimited by occurrences of "#SANR". The very first part
        before the first station header is ignored.
        """

        # Normalize newlines, but the file may be one long line
        parts = text.split("#SANR")
        blocks: list[str] = []
        for part in parts[1:]:
            blocks.append("#SANR" + part)
        return blocks

    def _select_block(self, blocks: list[str]) -> Optional[str]:
        """Select the best matching block using configured identifiers."""

        # Prepare candidates based on priority
        sanr_target: Optional[str] = None
        if self._sanr and self._sanr.isdigit():
            sanr_target = self._sanr
        elif self._station_id and str(self._station_id).isdigit():
            sanr_target = str(self._station_id)

        sname_target = self._sname_contains or self._name_hint
        sname_target = sname_target.strip() if sname_target else None

        chosen: Optional[str] = None
        for block in blocks:
            # Extract SANR and SNAME
            sanr_match = re.search(r"#SANR(\d+)", block)
            sname_match = re.search(r"\|\*\|SNAME([^|]*)\|\*\|", block)
            swater_match = re.search(r"\|\*\|SWATER([^|]*)\|\*\|", block)
            sanr_val = sanr_match.group(1) if sanr_match else None
            sname_val = sname_match.group(1).strip() if sname_match else None
            swater_val = swater_match.group(1).strip() if swater_match else None

            if sanr_target and sanr_val == sanr_target:
                return block
            if sname_target:
                cand_fields = [s for s in [sname_val, swater_val] if s]
                if any(sname_target.lower() in f.lower() for f in cand_fields):
                    # Keep first match if no SANR found
                    if chosen is None:
                        chosen = block
        return chosen

    def _parse_zrxp_block(self, block: str) -> list[HydroOOERecord]:
        """Parse a single station block into records.

        Expects a header with TZ info and a "#LAYOUT(timestamp,value)" marker
        followed by pairs of "YYYYMMDDhhmmss value".
        """

        # Extract timezone offset like #TZUTC+1 or #TZUTC-2
        tz_match = re.search(r"#TZUTC([+-])(\d+)", block)
        tzinfo = timezone.utc
        if tz_match:
            sign = 1 if tz_match.group(1) == "+" else -1
            hours = int(tz_match.group(2))
            tzinfo = timezone(timedelta(hours=sign * hours))

        # Extract invalid sentinel if present (e.g., RINVAL-777)
        rinval_match = re.search(r"RINVAL\s*([+-]?\d+(?:[.,]\d+)?)", block)
        rinval_val: Optional[float] = None
        if rinval_match:
            rinval_text = rinval_match.group(1).replace(",", ".")
            try:
                rinval_val = float(rinval_text)
            except Exception:  # noqa: BLE001
                rinval_val = None

        # Find the layout marker; everything after contains timestamp/value pairs
        layout_pos = block.find("#LAYOUT(timestamp,value)")
        if layout_pos == -1:
            raise ParseError("Missing #LAYOUT(timestamp,value) in ZRXP block")

        # After the marker, there is a delimiter "|*|" and then the data
        data_start = block.find("|*|", layout_pos)
        if data_start == -1:
            raise ParseError("Malformed ZRXP block: missing data delimiter after LAYOUT")
        series_text = block[data_start + 3 :]

        # Extract all timestamp/value pairs
        pair_re = re.compile(r"(\d{14})\s+([+-]?\d+(?:[.,]\d+)?)")
        records: list[HydroOOERecord] = []
        for m in pair_re.finditer(series_text):
            ts_raw = m.group(1)
            val_raw = m.group(2)
            try:
                ts = datetime.strptime(ts_raw, "%Y%m%d%H%M%S").replace(tzinfo=tzinfo)
                temp_text = val_raw.replace(",", ".")
                temp = float(temp_text)
            except Exception:  # noqa: BLE001
                continue

            # Skip invalid sentinel and out-of-range values
            if rinval_val is not None and abs(temp - rinval_val) < 1e-9:
                continue
            if temp < -5.0 or temp > 45.0:
                continue
            records.append(HydroOOERecord(timestamp=ts, temperature_c=temp))

        if not records:
            raise NoDataError("No usable data points in ZRXP block")
        return records

    @staticmethod
    def _parse_timestamp(text: str) -> datetime:
        # Retained for backward compatibility if ever re-used; not used in ZRXP path
        if text.endswith("Z"):
            base = datetime.fromisoformat(text[:-1])
            return base.replace(tzinfo=timezone.utc)
        return datetime.fromisoformat(text)


__all__ = [
    "HydroOOEScraper",
    "HydroOOERecord",
    "ScraperError",
    "NetworkError",
    "HttpError",
    "ParseError",
    "NoDataError",
]


