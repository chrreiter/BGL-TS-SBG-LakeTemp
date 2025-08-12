from __future__ import annotations

"""Scraper for Salzburg OGD hydrology lakes (Hydrografie Seen) semicolon text.

Primary data source (updates every ~2-3 hours):
  - https://www.salzburg.gv.at/ogd/56c28e2d-8b9e-41ba-b7d6-fa4896b5b48b/Hydrografie%20Seen.txt

The payload is a semicolon-delimited text file with a header row. Column names
and order are not strictly guaranteed, so this scraper performs tolerant header
detection to find the lake name, measurement timestamp and water temperature.

Design goals:
- Async-first I/O via aiohttp with reusable session
- Robust decoding (UTF-8 first, then common fallbacks)
- Graceful error handling with consistent exception types
- Flexible header detection and timestamp parsing
- Normalized lake-name matching including diacritics and common variants

This module exposes a high-level API to fetch the latest temperature for a
given lake name, as well as a bulk method that aggregates the newest reading
per lake across the entire file.
"""

from dataclasses import dataclass
from datetime import datetime
import logging
import re
import unicodedata
from typing import Dict, Iterable, List, Optional, Tuple
from zoneinfo import ZoneInfo

import aiohttp
from aiohttp import ClientConnectorError, ClientResponseError

from ..mixins import AsyncSessionMixin


_LOGGER = logging.getLogger(__name__)


# ---------- Exceptions (aligned with other scrapers) ----------


class ScraperError(Exception):
    """Base class for scraper-related errors."""


class NetworkError(ScraperError):
    """Network connectivity or DNS issues."""


class HttpError(ScraperError):
    """Non-2xx HTTP response or protocol error."""


class ParseError(ScraperError):
    """The text structure could not be parsed as expected."""


class NoDataError(ScraperError):
    """No usable measurement rows found in the payload."""


VIENNA_TZ = ZoneInfo("Europe/Vienna")


@dataclass(frozen=True)
class SalzburgOGDRecord:
    """Single measurement record for a named lake.

    Attributes:
        lake_name: Canonical lake name from the dataset row.
        timestamp: Timezone-aware measurement timestamp (Europe/Vienna).
        temperature_c: Water temperature in Celsius.
        station_name: Optional station or site description for the row.
    """

    lake_name: str
    timestamp: datetime
    temperature_c: float
    station_name: str | None = None


class SalzburgOGDScraper(AsyncSessionMixin):
    """Async scraper for Salzburg OGD Hydrografie "Seen" semicolon text.

    Usage:

        async with SalzburgOGDScraper() as scraper:
            latest = await scraper.fetch_latest_for_lake("Fuschlsee")

    Or with an externally managed session:

        session = aiohttp.ClientSession()
        try:
            scraper = SalzburgOGDScraper(session=session)
            mapping = await scraper.fetch_all_latest(target_lakes=["Fuschlsee", ...])
        finally:
            await session.close()

    The scraper performs a full-file download on each call and filters/aggregates
    locally. The file size is modest and update frequency is low (2-3 hours).
    """

    def __init__(
        self,
        *,
        url: str = "https://www.salzburg.gv.at/ogd/56c28e2d-8b9e-41ba-b7d6-fa4896b5b48b/Hydrografie%20Seen.txt",
        session: aiohttp.ClientSession | None = None,
        user_agent: str | None = None,
        request_timeout_seconds: float = 20.0,
    ) -> None:
        self._url = url
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

    # ----- Public API -----

    async def fetch_latest_for_lake(self, lake_name: str) -> SalzburgOGDRecord:
        """Fetch latest measurement for a single lake.

        Args:
            lake_name: Target lake name, e.g., "Fuschlsee".

        Raises:
            NoDataError: If the file contains no row matching the requested lake.
            NetworkError, HttpError, ParseError: On failures.
        """

        rows = await self._download_and_parse()
        key_target = self._normalize_lake_key(lake_name)
        newest: Optional[SalzburgOGDRecord] = None
        for rec in rows:
            if self._normalize_lake_key(rec.lake_name) == key_target:
                if newest is None or rec.timestamp > newest.timestamp:
                    newest = rec
        if newest is None:
            raise NoDataError(f"No measurement found for lake: {lake_name}")
        return newest

    async def fetch_all_latest(
        self, *, target_lakes: Iterable[str] | None = None
    ) -> Dict[str, SalzburgOGDRecord]:
        """Fetch and aggregate the newest record per lake.

        Args:
            target_lakes: Optional iterable of target names to filter. If None,
                all lakes in the file are processed. Keys in the result use the
                original dataset lake names.

        Returns:
            Mapping of lake name -> newest record for that lake.
        """

        rows = await self._download_and_parse()
        allow_keys: Optional[set[str]] = None
        if target_lakes is not None:
            allow_keys = {self._normalize_lake_key(n) for n in target_lakes}

        newest_by_key: Dict[str, SalzburgOGDRecord] = {}
        for rec in rows:
            key = self._normalize_lake_key(rec.lake_name)
            if allow_keys is not None and key not in allow_keys:
                continue
            prev = newest_by_key.get(key)
            if prev is None or rec.timestamp > prev.timestamp:
                newest_by_key[key] = rec

        # Re-key by original lake names for readability
        result: Dict[str, SalzburgOGDRecord] = {}
        for rec in newest_by_key.values():
            result[rec.lake_name] = rec
        return result

    # ----- Networking and Parsing -----

    async def _download_and_parse(self) -> List[SalzburgOGDRecord]:
        text = await self._fetch_text(self._url)
        try:
            headers, rows = self._split_header_rows(text)
            column_map = self._detect_columns(headers)
        except Exception as exc:  # noqa: BLE001
            raise ParseError(f"Failed to detect header/columns: {exc}") from exc

        results: List[SalzburgOGDRecord] = []
        for raw in rows:
            try:
                rec = self._parse_row(raw, column_map)
            except ValueError:
                # Skip unparsable rows
                continue
            if rec is not None:
                results.append(rec)

        if not results:
            raise NoDataError("No measurement rows parsed from payload")
        return results

    async def _fetch_text(self, url: str) -> str:
        session = await self._ensure_session()
        try:
            _LOGGER.debug("Fetching Salzburg OGD text: %s", url)
            async with session.get(url) as resp:
                try:
                    resp.raise_for_status()
                except ClientResponseError as exc:
                    raise HttpError(f"HTTP error {exc.status} for {url}") from exc
                raw = await resp.read()
                # try declared, then utf-8, latin-1, cp1252; finally replace errors
                candidates: List[str] = []
                if resp.charset:
                    candidates.append(resp.charset)
                candidates.extend(["utf-8", "latin-1", "cp1252"])
                last_error: Exception | None = None
                for enc in candidates:
                    try:
                        return raw.decode(enc)
                    except Exception as dec_err:  # noqa: BLE001
                        last_error = dec_err
                        continue
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

    @staticmethod
    def _split_header_rows(text: str) -> Tuple[List[str], List[List[str]]]:
        """Split the payload into headers and rows using semicolon as delimiter.

        Handles both CRLF and LF newlines and ignores empty trailing lines.
        """

        # Normalize newlines
        lines = re.split(r"\r?\n", text.strip())
        if not lines:
            raise ParseError("Empty payload")

        # Some OGD exports may include a BOM in the first cell
        header_line = lines[0].lstrip("\ufeff").strip()
        headers = [h.strip() for h in header_line.split(";")]
        if len(headers) < 2:
            raise ParseError("Header has fewer than 2 columns")

        rows: List[List[str]] = []
        for line in lines[1:]:
            if not line.strip():
                continue
            rows.append([c.strip() for c in line.split(";")])
        return headers, rows

    @staticmethod
    def _normalize_header_token(token: str) -> str:
        # lowercase, remove diacritics and non alnum, collapse spaces
        t = unicodedata.normalize("NFKD", token)
        t = "".join(ch for ch in t if not unicodedata.combining(ch))
        t = t.lower()
        t = re.sub(r"[^a-z0-9]+", " ", t).strip()
        return t

    def _detect_columns(self, headers: List[str]) -> Dict[str, int]:
        """Detect column indices for name, timestamp and temperature.

        Returns a mapping with keys: name, date, time (optional), timestamp (optional),
        temp. At least (name, temp) and one of (timestamp) or (date[,time]) must be
        present, otherwise raises ParseError.
        """

        tokens = [self._normalize_header_token(h) for h in headers]

        name_idx = self._find_first(
            tokens,
            [
                r"gewassername",
                r"gewasser bezeichnung",
                r"gewasser",
                r"gewsser",
                r"stationsname",
                r"see",
                r"bezeichnung",
                r"\bname\b",
            ],
        )

        temp_idx = self._find_first(
            tokens,
            [
                r"wassertemperatur",
                r"wasser.*temperatur",
                r"\btemperatur\b",
                r"\bwassertemp\b",
                r"\btemp\b",
                r"cunit",
                r"celsius",
            ],
        )

        # Time/Date may be single or separate columns
        timestamp_idx = self._find_first(tokens, [
            r"zeitstempel",
            r"messzeitpunkt",
            r"zeit punkt",
            r"zeitpunkt",
            r"timestamp",
        ])
        date_idx = self._find_first(tokens, [r"datum", r"messdatum", r"date"])
        time_idx = self._find_first(tokens, [r"zeit", r"uhrzeit", r"time"])

        # Optional alternative scheme: PARAMETER + VALUE (+ UNIT) instead of explicit temp column
        value_idx = self._find_first(tokens, [r"messwert", r"wert", r"value"])
        parameter_idx = self._find_first(tokens, [r"parameter", r"param", r"messgrosse", r"messgroesse"])
        unit_idx = self._find_first(tokens, [r"einheit", r"unit", r"cunit"])

        if name_idx is None:
            raise ParseError("Missing required 'name' column")
        if temp_idx is None and (value_idx is None or parameter_idx is None):
            raise ParseError("Missing required 'temperature' column or ('parameter' + 'value') columns")
        if timestamp_idx is None and date_idx is None:
            raise ParseError("Missing measurement time columns ('timestamp' or 'date')")

        mapping: Dict[str, int] = {"name": name_idx}
        if temp_idx is not None:
            mapping["temp"] = temp_idx
        if value_idx is not None:
            mapping["value"] = value_idx
        if parameter_idx is not None:
            mapping["parameter"] = parameter_idx
        if unit_idx is not None:
            mapping["unit"] = unit_idx
        if timestamp_idx is not None:
            mapping["timestamp"] = timestamp_idx
        if date_idx is not None:
            mapping["date"] = date_idx
        if time_idx is not None:
            mapping["time"] = time_idx

        # Optional station/site column
        site_idx = self._find_first(tokens, [r"station", r"standort", r"stelle", r"messstelle", r"messort", r"\bort\b", r"stationsname"])
        if site_idx is not None:
            mapping["station"] = site_idx

        return mapping

    @staticmethod
    def _find_first(tokens: List[str], patterns: List[str]) -> Optional[int]:
        for idx, tok in enumerate(tokens):
            for pat in patterns:
                if re.search(pat, tok):
                    return idx
        return None

    def _parse_row(self, row: List[str], column_map: Dict[str, int]) -> Optional[SalzburgOGDRecord]:
        # Ensure row has at least the referenced indices
        max_idx = max(column_map.values())
        if len(row) <= max_idx:
            return None

        name = row[column_map["name"]].strip()
        if not name:
            return None

        temp_c: Optional[float] = None
        if "temp" in column_map:
            temp_text = row[column_map["temp"]]
            try:
                temp_c = self._parse_temperature_c(temp_text)
            except ValueError:
                temp_c = None
        if temp_c is None and "value" in column_map and "parameter" in column_map:
            param_text = row[column_map["parameter"]].lower().strip()
            # Accept when parameter indicates water temperature; in the live dataset
            # 'WT' denotes Wassertemperatur.
            if ("temperatur" in param_text) or (param_text == "wt") or (" wt" in param_text):
                try:
                    temp_c = self._parse_temperature_c(row[column_map["value"]])
                except ValueError:
                    temp_c = None
        if temp_c is None:
            return None

        # Timestamp assembly
        ts: Optional[datetime] = None
        if "timestamp" in column_map:
            ts_text = row[column_map["timestamp"]]
            ts = self._parse_datetime_any(ts_text)
        else:
            date_text = row[column_map.get("date", -1)] if "date" in column_map else ""
            time_text = row[column_map.get("time", -1)] if "time" in column_map else ""
            ts = self._parse_datetime_from_parts(date_text, time_text)

        if ts is None:
            return None

        station_name: Optional[str] = None
        if "station" in column_map:
            station_name = row[column_map["station"]] or None

        return SalzburgOGDRecord(lake_name=name, timestamp=ts, temperature_c=temp_c, station_name=station_name)

    @staticmethod
    def _parse_temperature_c(text: str) -> float:
        cleaned = (text or "").strip().lower()
        cleaned = cleaned.replace("°c", "").replace("°", "").replace(" c", " ")
        cleaned = cleaned.replace(" ", "").replace(",", ".")
        # Extract leading numeric (supports sign and decimal point)
        num_chars = "+-.0123456789"
        numeric = "".join(ch for ch in cleaned if ch in num_chars)
        if numeric in ("", "+", "-", "."):
            raise ValueError("No numeric temperature")
        value = float(numeric)
        if not (-5.0 <= value <= 45.0):
            raise ValueError("Out-of-range temperature")
        return value

    @staticmethod
    def _parse_datetime_any(text: str) -> Optional[datetime]:
        t = (text or "").strip()
        if not t:
            return None
        # Normalize: drop trailing zone abbreviations (e.g., MEZ, MESZ),
        # convert date 2025.08.11 to 2025-08-11, and add colon in +0100 => +01:00
        import re as _re

        t_norm = _re.sub(r"\s+[A-ZÄÖÜ]{2,6}$", "", t)
        t_norm = _re.sub(r"^(\d{4})\.(\d{2})\.(\d{2})", r"\1-\2-\3", t_norm)
        # Add colon in numeric offset if missing
        t_norm = _re.sub(r"(T\d{2}:\d{2}(?::\d{2})?)([+-])(\d{2})(\d{2})$", r"\1\2\3:\4", t_norm)
        # Replace Z with +00:00
        if t_norm.endswith("Z"):
            t_norm = t_norm[:-1] + "+00:00"
        # Try fromisoformat
        try:
            dt = datetime.fromisoformat(t_norm)
            if dt.tzinfo is not None:
                return dt
        except Exception:  # noqa: BLE001
            pass

        candidates = [
            "%d.%m.%Y %H:%M:%S",
            "%d.%m.%Y %H:%M",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M",
        ]
        for fmt in candidates:
            try:
                dt = datetime.strptime(t, fmt)
                return dt.replace(tzinfo=VIENNA_TZ)
            except Exception:  # noqa: BLE001
                continue
        # Try to split if contains space
        if " " in t:
            parts = t.split()
            if len(parts) >= 2:
                return SalzburgOGDScraper._parse_datetime_from_parts(parts[0], parts[1])
        return None

    @staticmethod
    def _parse_datetime_from_parts(date_text: str, time_text: str) -> Optional[datetime]:
        d = (date_text or "").strip()
        tm = (time_text or "").strip()
        if not d:
            return None
        date_formats = ["%d.%m.%Y", "%Y-%m-%d", "%d.%m.%y"]
        time_formats = ["%H:%M:%S", "%H:%M"]
        last_exc: Exception | None = None
        for df in date_formats:
            try:
                base = datetime.strptime(d, df)
                if tm:
                    for tf in time_formats:
                        try:
                            parsed = datetime.strptime(tm, tf)
                            combined = base.replace(hour=parsed.hour, minute=parsed.minute, second=getattr(parsed, "second", 0))
                            return combined.replace(tzinfo=VIENNA_TZ)
                        except Exception as exc:  # noqa: BLE001
                            last_exc = exc
                            continue
                # No time provided; assume 12:00 to maintain ordering without biasing early/late
                return base.replace(hour=12, minute=0, second=0, tzinfo=VIENNA_TZ)
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                continue
        return None

    # ----- Name normalization and matching -----

    @staticmethod
    def _normalize_lake_key(name: str) -> str:
        # Remove diacritics, lowercase, strip, remove common suffix 'see', compress spaces
        base = unicodedata.normalize("NFKD", name)
        base = "".join(ch for ch in base if not unicodedata.combining(ch))
        base = base.lower().strip()
        base = base.replace("zeller see", "zellersee").replace("obertrumer see", "obertrumersee")
        base = re.sub(r"\bsee\b", "", base)  # drop literal word 'see'
        base = re.sub(r"[^a-z0-9]+", "", base)
        # Map known aliases
        aliases = {
            "abersee": "wolfgang",  # local name for part of Wolfgangsee
            "zellamsee": "zeller",
            "zell": "zeller",
            "zellsee": "zeller",
        }
        # reduce to a stable stem
        stems = [
            ("obertrumersee", "obertrumer"),
            ("untertrumersee", "untertrumer"),
            ("mattsee", "matt"),
            ("grabensee", "graben"),
            ("wolfgangsee", "wolfgang"),
            ("zellersee", "zeller"),
            ("wallersee", "waller"),
            ("fuschlsee", "fuschl"),
            ("mondsee", "mond"),
            ("attersee", "atter"),
        ]
        for alias_src, alias_dst in aliases.items():
            if base == alias_src:
                base = alias_dst
        for pattern, stem in stems:
            if pattern in base:
                return stem
        return base


__all__ = [
    "SalzburgOGDScraper",
    "SalzburgOGDRecord",
    "ScraperError",
    "NetworkError",
    "HttpError",
    "ParseError",
    "NoDataError",
]


