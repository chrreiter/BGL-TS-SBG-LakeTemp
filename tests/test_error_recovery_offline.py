from __future__ import annotations

"""Offline tests for error scenarios and recovery mechanisms.

- Network timeout/HTTP errors and manual recovery across calls
- Partial parsing failures with graceful degradation
- Malformed data handling across all scrapers
- Session cleanup on errors (owned vs external sessions)
"""

import asyncio
import pathlib

import aiohttp
import pytest
from aioresponses import aioresponses

from custom_components.bgl_ts_sbg_laketemp.scrapers.gkd_bayern import (
    GKDBayernScraper,
    HttpError as GKDHttpError,
    NetworkError as GKDNetworkError,
)
from custom_components.bgl_ts_sbg_laketemp.scrapers.hydro_ooe import (
    HydroOOEScraper,
    HttpError as HydroHttpError,
)
from custom_components.bgl_ts_sbg_laketemp.scrapers.salzburg_ogd import (
    SalzburgOGDScraper,
)


FIXTURES = pathlib.Path(__file__).parent / "fixtures"
GKD_URL = "https://www.gkd.bayern.de/de/seen/wassertemperatur/inn/seethal-18673955/messwerte"
GKD_URL_FALLBACK = GKD_URL.rstrip("/") + "/tabelle"
ZRXP_URL = "https://data.ooe.gv.at/files/hydro/HDOOE_Export_WT.zrxp"
OGD_URL = "https://www.salzburg.gv.at/ogd/56c28e2d-8b9e-41ba-b7d6-fa4896b5b48b/Hydrografie%20Seen.txt"


# Title: Network timeout then success (manual recovery) — Expect: first call raises, second call succeeds
@pytest.mark.asyncio
async def test_gkd_timeout_then_success_manual_recovery() -> None:
    html = (FIXTURES / "gkd_bayern_table_sample.html").read_text(encoding="utf-8")

    # First attempt: timeout on /tabelle
    with aioresponses() as mocked:
        mocked.get(GKD_URL_FALLBACK, exception=aiohttp.ServerTimeoutError())
        async with GKDBayernScraper(GKD_URL) as scraper:
            with pytest.raises(GKDNetworkError):
                await scraper.fetch_latest()

    # Second attempt: success
    with aioresponses() as mocked:
        mocked.get(GKD_URL_FALLBACK, status=200, body=html, headers={"Content-Type": "text/html; charset=utf-8"})
        async with GKDBayernScraper(GKD_URL) as scraper:
            latest = await scraper.fetch_latest()
            assert latest.temperature_c == 23.1


# Title: HTTP 429 then success on retry (manual) — Expect: first raises HttpError, second returns records
@pytest.mark.asyncio
async def test_hydro_http_429_then_success_manual_retry() -> None:
    zrxp_text = (
        "#ZRXPVERSION2300.100|*|ZRXPCREATORKiIOSystem.ZRXPV2R2_E|*| "
        "#SANR16579|*|SNAMEIrrsee / Zell am Moos|*|SWATERIrrsee|*|CNRWT|*|CNAMEWassertemperatur|*| "
        "#TZUTC+1|*|RINVAL-777|*| #CUNIT°C|*| #LAYOUT(timestamp,value)|*| "
        "20250808140000 22.8 20250808150000 23.0 20250808160000 23.1"
    )

    # First attempt: 429 Too Many Requests
    with aioresponses() as mocked:
        mocked.get(ZRXP_URL, status=429)
        async with HydroOOEScraper(station_id="16579") as scraper:
            with pytest.raises(HydroHttpError):
                await scraper.fetch_records()

    # Second attempt: success
    with aioresponses() as mocked:
        mocked.get(ZRXP_URL, status=200, body=zrxp_text)
        async with HydroOOEScraper(station_id="16579") as scraper:
            recs = await scraper.fetch_records()
            assert recs and recs[-1].temperature_c == 23.1


# Title: Partial parsing (GKD) — Expect: skip bad rows, return latest valid
@pytest.mark.asyncio
async def test_gkd_partial_parsing_skips_bad_rows() -> None:
    html_mixed = """
    <html><body>
      <table>
        <thead><tr><th>Datum</th><th>Wassertemperatur [°C]</th></tr></thead>
        <tbody>
          <tr><td>08.08.2025 14:00</td><td>k.A.</td></tr>
          <tr><td>08.08.2025 15:00</td><td>22,8</td></tr>
          <tr><td>08.08.2025 16:00</td><td>23,1</td></tr>
        </tbody>
      </table>
    </body></html>
    """

    with aioresponses() as mocked:
        mocked.get(GKD_URL_FALLBACK, status=200, body=html_mixed)
        async with GKDBayernScraper(GKD_URL) as scraper:
            latest = await scraper.fetch_latest()
    assert latest.temperature_c == 23.1
    assert latest.timestamp.hour == 16


# Title: Partial parsing (Hydro OOE) — Expect: skip RINVAL and out-of-range, keep valid
@pytest.mark.asyncio
async def test_hydro_partial_parsing_skips_invalid_points() -> None:
    zrxp_text = (
        "#SANR16579|*|SNAMEIrrsee|*| #TZUTC+1|*| RINVAL -777|*| #LAYOUT(timestamp,value)|*| "
        "20250808140000 -777 20250808143000 100.0 20250808150000 23.0 20250808160000 23.1"
    )

    with aioresponses() as mocked:
        mocked.get(ZRXP_URL, status=200, body=zrxp_text)
        async with HydroOOEScraper(station_id="16579") as scraper:
            recs = await scraper.fetch_records()
    assert [round(r.temperature_c, 1) for r in recs] == [23.0, 23.1]


# Title: Partial parsing (Salzburg OGD) — Expect: skip bad rows, keep valid latest
@pytest.mark.asyncio
async def test_ogd_partial_parsing_skips_bad_rows() -> None:
    payload = (
        "Gewässer;Messdatum;Uhrzeit;Wassertemperatur [°C];Station\n"
        "Fuschlsee;2025-08-08;13:00;k.A.;Westufer\n"
        "Fuschlsee;2025-08-08;14:00;22,4;Westufer\n"
    )

    with aioresponses() as mocked:
        mocked.get(OGD_URL, status=200, body=payload)
        async with SalzburgOGDScraper(url=OGD_URL) as scraper:
            latest = await scraper.fetch_latest_for_lake("Fuschlsee")
    assert latest.temperature_c == 22.4
    assert latest.timestamp.hour == 14


# Title: Session cleanup on error (owned session) — Expect: internal session closed after context
@pytest.mark.asyncio
async def test_owned_session_closed_on_error() -> None:
    with aioresponses() as mocked:
        mocked.get(GKD_URL_FALLBACK, status=404)
        scraper_ref = None
        try:
            async with GKDBayernScraper(GKD_URL) as scraper:
                scraper_ref = scraper
                with pytest.raises(GKDHttpError):
                    await scraper.fetch_latest()
        finally:
            # Access internal attribute for test purposes
            assert scraper_ref is not None
            # If owned session was created, it must be closed
            if getattr(scraper_ref, "_session_owned", None) is not None:
                assert scraper_ref._session_owned.closed  # type: ignore[attr-defined]


# Title: External session remains open on error — Expect: external session not closed by scraper
@pytest.mark.asyncio
async def test_external_session_not_closed_on_error() -> None:
    session = aiohttp.ClientSession()
    try:
        with aioresponses() as mocked:
            mocked.get(GKD_URL_FALLBACK, status=404)
            scraper = GKDBayernScraper(GKD_URL, session=session)
            with pytest.raises(GKDHttpError):
                await scraper.fetch_latest()
        # Scraper must not close the external session
        assert not session.closed
    finally:
        await session.close()


# Title: Encoding fallback works (OGD cp1252) — Expect: decode succeeds and latest is returned
@pytest.mark.asyncio
async def test_ogd_encoding_fallback_cp1252() -> None:
    text = (
        "Stationsname;Zeitstempel;Messwert;Parameter;Einheit\n"
        "Fuschlsee;2025-08-08T14:00:00Z;22,4;WT;°C\n"
    )
    payload_bytes = text.encode("cp1252")

    with aioresponses() as mocked:
        mocked.get(OGD_URL, status=200, body=payload_bytes)
        async with SalzburgOGDScraper(url=OGD_URL) as scraper:
            latest = await scraper.fetch_latest_for_lake("Fuschlsee")
    assert latest.temperature_c == 22.4


