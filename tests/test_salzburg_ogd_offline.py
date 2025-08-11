from __future__ import annotations

"""Offline tests for SalzburgOGDScraper using mocked HTTP.

- Success: parse newest reading for target lake
- Error: HTTP 404 -> HttpError
- Error: timeout -> NetworkError
- Error: malformed header -> ParseError
- Error: no matching lake -> NoDataError
"""

import pytest
from aioresponses import aioresponses

from custom_components.bgl_ts_sbg_laketemp.scrapers.salzburg_ogd import (
    SalzburgOGDScraper,
    HttpError,
    NetworkError,
    ParseError,
    NoDataError,
)


OGD_URL = "https://www.salzburg.gv.at/ogd/56c28e2d-8b9e-41ba-b7d6-fa4896b5b48b/Hydrografie%20Seen.txt"


# Test: Successful parsing; latest record for Fuschlsee at 14:00 with 22.4
# Expect: temperature 22.4 and tz info set
@pytest.mark.asyncio
async def test_salzburg_ogd_success_latest_for_fuschlsee() -> None:
    # Header variants to exercise tolerant detection
    payload = (
        "Gewässer;Messdatum;Uhrzeit;Wassertemperatur [°C];Station\n"
        "Fuschlsee;2025-08-08;13:00;22,0;Westufer\n"
        "Fuschlsee;2025-08-08;14:00;22,4;Westufer\n"
        "Mattsee;2025-08-08;14:00;23,1;Nord\n"
    )

    with aioresponses() as mocked:
        mocked.get(OGD_URL, status=200, body=payload, headers={"Content-Type": "text/plain; charset=utf-8"})
        async with SalzburgOGDScraper(url=OGD_URL) as scraper:
            rec = await scraper.fetch_latest_for_lake("Fuschlsee")

    assert rec.temperature_c == 22.4
    assert rec.timestamp.hour == 14
    assert rec.timestamp.tzinfo is not None
    assert rec.lake_name.lower().startswith("fuschl")


# Test: HTTP 404 returns HttpError
# Expect: HttpError
@pytest.mark.asyncio
async def test_salzburg_ogd_http_404() -> None:
    with aioresponses() as mocked:
        mocked.get(OGD_URL, status=404)
        async with SalzburgOGDScraper(url=OGD_URL) as scraper:
            with pytest.raises(HttpError):
                await scraper.fetch_latest_for_lake("Mattsee")


# Test: Timeout -> NetworkError
# Expect: NetworkError
@pytest.mark.asyncio
async def test_salzburg_ogd_timeout() -> None:
    import aiohttp

    with aioresponses() as mocked:
        mocked.get(OGD_URL, exception=aiohttp.ServerTimeoutError())
        async with SalzburgOGDScraper(url=OGD_URL) as scraper:
            with pytest.raises(NetworkError):
                await scraper.fetch_latest_for_lake("Mondsee")


# Test: Malformed header -> ParseError
# Expect: ParseError
@pytest.mark.asyncio
async def test_salzburg_ogd_malformed_header_parse_error() -> None:
    payload = "foo;bar\n1;2\n"
    with aioresponses() as mocked:
        mocked.get(OGD_URL, status=200, body=payload)
        async with SalzburgOGDScraper(url=OGD_URL) as scraper:
            with pytest.raises(ParseError):
                await scraper.fetch_all_latest()


# Test: No matching lake -> NoDataError
# Expect: NoDataError
@pytest.mark.asyncio
async def test_salzburg_ogd_no_matching_lake() -> None:
    payload = (
        "Gewässer;Messdatum;Uhrzeit;Wassertemperatur [°C]\n"
        "Grabensee;2025-08-08;14:00;23,1\n"
    )
    with aioresponses() as mocked:
        mocked.get(OGD_URL, status=200, body=payload)
        async with SalzburgOGDScraper(url=OGD_URL) as scraper:
            with pytest.raises(NoDataError):
                await scraper.fetch_latest_for_lake("NonExistingLake")


