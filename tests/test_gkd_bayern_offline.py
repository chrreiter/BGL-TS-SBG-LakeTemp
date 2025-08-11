from __future__ import annotations

"""Offline unit tests for GKDBayernScraper using mocked HTTP responses.

Covers:
- happy path using fixture HTML
- invalid URL, HTTP errors, timeouts
- missing table and fallback to "/tabelle"
- unparseable rows leading to NoDataError
- out-of-range values are skipped
- deduplication and sorting
"""

import pathlib
import pytest
from aioresponses import aioresponses
import aiohttp

from custom_components.bgl_ts_sbg_laketemp.scrapers.gkd_bayern import (
    GKDBayernScraper,
    HttpError,
    NetworkError,
    ParseError,
    NoDataError,
)


FIXTURE_PATH = pathlib.Path(__file__).parent / "fixtures" / "gkd_bayern_table_sample.html"


# Test: Fixture latest record is returned
# Expect: Latest is 23.1°C at 2025-08-08 16:00 Europe/Berlin
@pytest.mark.asyncio
async def test_fetch_latest_returns_newest_record() -> None:
    url = "https://www.gkd.bayern.de/de/seen/wassertemperatur/inn/seethal-18673955/messwerte"
    html = FIXTURE_PATH.read_text(encoding="utf-8")

    with aioresponses() as mocked:
        mocked.get(
            url,
            status=200,
            body=html,
            headers={"Content-Type": "text/html; charset=utf-8"},
        )

        async with GKDBayernScraper(url) as scraper:
            latest = await scraper.fetch_latest()

    assert latest.temperature_c == 23.1
    assert latest.timestamp.year == 2025
    assert latest.timestamp.month == 8
    assert latest.timestamp.day == 8
    assert latest.timestamp.hour == 16
    assert latest.timestamp.tzinfo is not None


# Test: Invalid URL format
# Expect: HttpError is raised
@pytest.mark.asyncio
async def test_invalid_url_format_raises_http_error() -> None:
    bad_url = "http:///bad_url"
    async with GKDBayernScraper(bad_url) as scraper:
        with pytest.raises(HttpError):
            await scraper.fetch_latest()


# Test: HTTP 404 from server
# Expect: HttpError is raised
@pytest.mark.asyncio
async def test_http_404_raises_http_error() -> None:
    url = "https://www.gkd.bayern.de/de/seen/wassertemperatur/inn/seethal-18673955/messwerte"
    with aioresponses() as mocked:
        mocked.get(url, status=404)
        async with GKDBayernScraper(url) as scraper:
            with pytest.raises(HttpError):
                await scraper.fetch_latest()


# Test: Request timeout
# Expect: NetworkError is raised
@pytest.mark.asyncio
async def test_timeout_raises_network_error() -> None:
    url = "https://www.gkd.bayern.de/de/seen/wassertemperatur/inn/seethal-18673955/messwerte"
    with aioresponses() as mocked:
        mocked.get(url, exception=aiohttp.ServerTimeoutError())
        async with GKDBayernScraper(url) as scraper:
            with pytest.raises(NetworkError):
                await scraper.fetch_latest()


# Test: Missing measurement table on both primary and fallback
# Expect: ParseError is raised
@pytest.mark.asyncio
async def test_missing_table_raises_parse_error() -> None:
    url = "https://www.gkd.bayern.de/de/seen/wassertemperatur/inn/seethal-18673955/messwerte"
    html_no_table = """
    <html><head><title>Aktuelle Messwerte</title></head>
    <body>
      <h1>Aktuelle Messwerte Seethal / Abtsdorfer See</h1>
      <p>Keine Tabelle vorhanden</p>
    </body></html>
    """
    url_fallback = url.rstrip("/") + "/tabelle"
    with aioresponses() as mocked:
        mocked.get(url, status=200, body=html_no_table)
        mocked.get(url_fallback, status=200, body=html_no_table)
        async with GKDBayernScraper(url) as scraper:
            with pytest.raises(ParseError):
                await scraper.fetch_records()


# Test: Structure changed; rows unparseable even after fallback
# Expect: NoDataError is raised
@pytest.mark.asyncio
async def test_structure_changed_unparseable_rows_raise_nodata() -> None:
    url = "https://www.gkd.bayern.de/de/seen/wassertemperatur/inn/seethal-18673955/messwerte"
    url_fallback = url.rstrip("/") + "/tabelle"
    html_changed = """
    <html><body>
      <table>
        <thead><tr><th>Zeit</th><th>Temperatur</th></tr></thead>
        <tbody>
          <tr><td>2025-08-07 16:00</td><td>k.A.</td></tr>
          <tr><td>2025/08/07 15:00</td><td>-</td></tr>
        </tbody>
      </table>
    </body></html>
    """
    with aioresponses() as mocked:
        mocked.get(url, status=200, body=html_changed)
        mocked.get(url_fallback, status=200, body=html_changed)
        async with GKDBayernScraper(url) as scraper:
            with pytest.raises(NoDataError):
                await scraper.fetch_latest()


# Test: Fallback to '/tabelle' when primary lacks table
# Expect: Successfully parse latest record (23.1 at 16:00)
@pytest.mark.asyncio
async def test_fallback_to_tabelle_when_primary_lacks_table() -> None:
    url = "https://www.gkd.bayern.de/de/seen/wassertemperatur/inn/seethal-18673955/messwerte"
    url_fallback = url.rstrip("/") + "/tabelle"

    html_no_table = """
    <html><body><h1>Aktuelle Messwerte</h1><p>Diagrammansicht ohne Tabelle</p></body></html>
    """
    html_with_table = """
    <html><body>
      <table>
        <thead><tr><th>Datum</th><th>Wassertemperatur [°C]</th></tr></thead>
        <tbody>
          <tr><td>08.08.2025 15:00</td><td>22,8</td></tr>
          <tr><td>08.08.2025 16:00</td><td>23,1</td></tr>
        </tbody>
      </table>
    </body></html>
    """

    with aioresponses() as mocked:
        mocked.get(url, status=200, body=html_no_table)
        mocked.get(url_fallback, status=200, body=html_with_table)
        async with GKDBayernScraper(url) as scraper:
            latest = await scraper.fetch_latest()
            assert latest.temperature_c == 23.1
            assert latest.timestamp.hour == 16


# Test: Out-of-range temperature values only
# Expect: NoDataError is raised
@pytest.mark.asyncio
async def test_out_of_range_temperature_values_are_skipped_and_nodata_if_all_invalid() -> None:
    url = "https://www.gkd.bayern.de/de/seen/wassertemperatur/inn/seethal-18673955/messwerte"
    html_bad_values = """
    <html><body>
      <table>
        <thead><tr><th>Datum</th><th>Wassertemperatur [°C]</th></tr></thead>
        <tbody>
          <tr><td>08.08.2025 15:00</td><td>100,0</td></tr>
          <tr><td>08.08.2025 16:00</td><td>-10</td></tr>
        </tbody>
      </table>
    </body></html>
    """
    with aioresponses() as mocked:
        mocked.get(url, status=200, body=html_bad_values)
        async with GKDBayernScraper(url) as scraper:
            with pytest.raises(NoDataError):
                await scraper.fetch_latest()


# Test: Deduplication and sorting of records
# Expect: 3 unique records sorted by time
@pytest.mark.asyncio
async def test_deduplication_and_sorting() -> None:
    url = "https://www.gkd.bayern.de/de/seen/wassertemperatur/inn/seethal-18673955/messwerte"
    html_unsorted = """
    <html><body>
      <table>
        <thead><tr><th>Datum</th><th>Wassertemperatur [°C]</th></tr></thead>
        <tbody>
          <tr><td>08.08.2025 16:00</td><td>23,1</td></tr>
          <tr><td>08.08.2025 14:00</td><td>23,1</td></tr>
          <tr><td>08.08.2025 15:00</td><td>22,8</td></tr>
          <tr><td>08.08.2025 16:00</td><td>23,1</td></tr>
        </tbody>
      </table>
    </body></html>
    """
    with aioresponses() as mocked:
        mocked.get(url, status=200, body=html_unsorted)
        async with GKDBayernScraper(url) as scraper:
            records = await scraper.fetch_records()
            assert len(records) == 3
            assert [r.timestamp.hour for r in records] == [14, 15, 16]
            assert records[-1].temperature_c == 23.1


