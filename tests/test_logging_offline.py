from __future__ import annotations

"""Structured logging tests (offline) for all scrapers.

Each test includes a short description and expected outcome.
"""

import logging
import pathlib
from typing import List

import pytest
from aioresponses import aioresponses

from custom_components.bgl_ts_sbg_laketemp.scrapers.gkd_bayern import GKDBayernScraper, ParseError, NoDataError
from custom_components.bgl_ts_sbg_laketemp.scrapers.hydro_ooe import HydroOOEScraper, ParseError as HydroParseError, NoDataError as HydroNoDataError
from custom_components.bgl_ts_sbg_laketemp.scrapers.salzburg_ogd import SalzburgOGDScraper, ParseError as OGDParseError


GKD_LOGGER = "custom_components.bgl_ts_sbg_laketemp.scrapers.gkd_bayern"
HYDRO_LOGGER = "custom_components.bgl_ts_sbg_laketemp.scrapers.hydro_ooe"
OGD_LOGGER = "custom_components.bgl_ts_sbg_laketemp.scrapers.salzburg_ogd"


def _messages_for(caplog, logger_name: str) -> List[str]:  # type: ignore[no-untyped-def]
    return [rec.getMessage() for rec in caplog.records if rec.name == logger_name]


# Test: GKD success emits http_get and parse_table finish logs with fields
# Expect: messages contain operation=http_get and operation=parse_table with duration_ms and records
@pytest.mark.asyncio
async def test_gkd_success_emits_http_and_parse_logs(caplog) -> None:  # type: ignore[no-untyped-def]
    caplog.set_level(logging.DEBUG)
    url = "https://www.gkd.bayern.de/de/seen/wassertemperatur/inn/seethal-18673955/messwerte"
    html = (pathlib.Path(__file__).parent / "fixtures" / "gkd_bayern_table_sample.html").read_text(encoding="utf-8")

    with aioresponses() as mocked:
        mocked.get(url, status=200, body=html, headers={"Content-Type": "text/html; charset=utf-8"})
        async with GKDBayernScraper(url) as scraper:
            _ = await scraper.fetch_records()

    msgs = _messages_for(caplog, GKD_LOGGER)
    assert any("operation=http_get" in m and "op=start" in m for m in msgs)
    assert any("operation=http_get" in m and "op=finish" in m and "status=200" in m and "bytes=" in m and "duration_ms=" in m for m in msgs)
    assert any("operation=parse_table" in m and "op=finish" in m and "records=" in m and "rows=" in m and "tables=" in m for m in msgs)


# Test: GKD fallback logs a structured 'fallback' event
# Expect: a debug message with operation=fallback and reason=no_table and alt_url
@pytest.mark.asyncio
async def test_gkd_fallback_message_logged(caplog) -> None:  # type: ignore[no-untyped-def]
    caplog.set_level(logging.DEBUG)
    url = "https://www.gkd.bayern.de/de/seen/wassertemperatur/inn/seethal-18673955/messwerte"
    url_fallback = url.rstrip("/") + "/tabelle"
    html_no_table = """
    <html><body><p>diagramm ohne tabelle</p></body></html>
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
            _ = await scraper.fetch_latest()

    msgs = _messages_for(caplog, GKD_LOGGER)
    assert any("operation=fallback" in m and "reason=no_table" in m and "alt_url=" in m for m in msgs)


# Test: GKD parse error logs error with operation=parse_table
# Expect: error message with op=error and operation=parse_table
@pytest.mark.asyncio
async def test_gkd_parse_error_logs_error(caplog) -> None:  # type: ignore[no-untyped-def]
    caplog.set_level(logging.DEBUG)
    url = "https://www.gkd.bayern.de/de/seen/wassertemperatur/inn/seethal-18673955/messwerte"
    html_no_table = "<html><body><p>no table</p></body></html>"
    url_fallback = url.rstrip("/") + "/tabelle"

    with aioresponses() as mocked:
        mocked.get(url, status=200, body=html_no_table)
        mocked.get(url_fallback, status=200, body=html_no_table)
        async with GKDBayernScraper(url) as scraper:
            with pytest.raises(ParseError):
                await scraper.fetch_records()

    msgs = _messages_for(caplog, GKD_LOGGER)
    assert any("operation=parse_table" in m and "op=error" in m for m in msgs)


# Test: HYDRO OOE success emits http_get, split_blocks, select_block, parse_block logs
# Expect: finish messages with fields: blocks, match_type or rows_seen/records
@pytest.mark.asyncio
async def test_hydro_success_emits_all_operation_logs(caplog) -> None:  # type: ignore[no-untyped-def]
    caplog.set_level(logging.DEBUG)
    zrxp_url = "https://data.ooe.gv.at/files/hydro/HDOOE_Export_WT.zrxp"
    zrxp_text = (
        "#ZRXPVERSION2300.100|*|ZRXPCREATORKiIOSystem.ZRXPV2R2_E|*| "
        "#SANR16579|*|SNAMEIrrsee / Zell am Moos|*|SWATERIrrsee|*|CNRWT|*|CNAMEWassertemperatur|*| "
        "#TZUTC+1|*|RINVAL-777|*| #CUNIT°C|*| #LAYOUT(timestamp,value)|*| "
        "20250808140000 22.8 20250808150000 23.0 20250808160000 23.1"
    )

    with aioresponses() as mocked:
        mocked.get(zrxp_url, status=200, body=zrxp_text, headers={"Content-Type": "text/plain"})
        async with HydroOOEScraper(station_id="16579") as scraper:
            _ = await scraper.fetch_records()

    msgs = _messages_for(caplog, HYDRO_LOGGER)
    assert any("operation=http_get" in m and "op=finish" in m and "status=200" in m and "bytes=" in m and "duration_ms=" in m for m in msgs)
    assert any("operation=split_blocks" in m and "op=finish" in m and "blocks=" in m for m in msgs)
    assert any("operation=select_block" in m and "op=finish" in m for m in msgs)
    assert any("operation=parse_block" in m and "op=finish" in m and "rows_seen=" in m and "records=" in m for m in msgs)


# Test: HYDRO OOE parse error logs error with parse_block
# Expect: error message with operation=parse_block and op=error and zero counts
@pytest.mark.asyncio
async def test_hydro_parse_block_error_logs(caplog) -> None:  # type: ignore[no-untyped-def]
    caplog.set_level(logging.DEBUG)
    zrxp_url = "https://data.ooe.gv.at/files/hydro/HDOOE_Export_WT.zrxp"
    bad_text = "#SANR16579|*|SNAMEIrrsee|*| #TZUTC+1|*| RINVAL-777|*|"  # missing layout marker

    with aioresponses() as mocked:
        mocked.get(zrxp_url, status=200, body=bad_text)
        async with HydroOOEScraper(station_id="16579") as scraper:
            with pytest.raises(HydroParseError):
                await scraper.fetch_records()

    msgs = _messages_for(caplog, HYDRO_LOGGER)
    assert any("operation=parse_block" in m and "op=error" in m for m in msgs)


# Test: OGD success emits http_get and parse_payload logs
# Expect: finish messages with rows_seen and records present
@pytest.mark.asyncio
async def test_ogd_success_emits_http_and_parse_logs(caplog) -> None:  # type: ignore[no-untyped-def]
    caplog.set_level(logging.DEBUG)
    url = "https://www.salzburg.gv.at/ogd/56c28e2d-8b9e-41ba-b7d6-fa4896b5b48b/Hydrografie%20Seen.txt"
    payload = (
        "Gewässer;Messdatum;Uhrzeit;Wassertemperatur [°C];Station\n"
        "Fuschlsee;2025-08-08;13:00;22,0;Westufer\n"
        "Fuschlsee;2025-08-08;14:00;22,4;Westufer\n"
    )

    with aioresponses() as mocked:
        mocked.get(url, status=200, body=payload, headers={"Content-Type": "text/plain; charset=utf-8"})
        async with SalzburgOGDScraper(url=url) as scraper:
            _ = await scraper.fetch_all_latest(target_lakes=["Fuschlsee"]) 

    msgs = _messages_for(caplog, OGD_LOGGER)
    assert any("operation=http_get" in m and "op=finish" in m and "status=200" in m and "bytes=" in m and "duration_ms=" in m for m in msgs)
    assert any("operation=parse_payload" in m and "op=finish" in m and "rows_seen=" in m and "records=" in m for m in msgs)


# Test: OGD parse error logs error with operation=parse_payload
# Expect: error message with op=error and operation=parse_payload
@pytest.mark.asyncio
async def test_ogd_parse_error_logs(caplog) -> None:  # type: ignore[no-untyped-def]
    caplog.set_level(logging.DEBUG)
    url = "https://www.salzburg.gv.at/ogd/56c28e2d-8b9e-41ba-b7d6-fa4896b5b48b/Hydrografie%20Seen.txt"
    payload = "foo;bar\n1;2\n"

    with aioresponses() as mocked:
        mocked.get(url, status=200, body=payload)
        async with SalzburgOGDScraper(url=url) as scraper:
            with pytest.raises(OGDParseError):
                await scraper.fetch_all_latest()

    msgs = _messages_for(caplog, OGD_LOGGER)
    assert any("operation=parse_payload" in m and "op=error" in m for m in msgs)


