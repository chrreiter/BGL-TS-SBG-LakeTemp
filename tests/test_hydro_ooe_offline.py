from __future__ import annotations

# Tests for HydroOOEScraper (ZRXP bulk parsing)
# - Success: ZRXP bulk file returns the latest reading for the station
# - Error paths: HTTP 404, timeout, malformed payload, no data -> appropriate errors

import pytest
from aioresponses import aioresponses

from custom_components.bgl_ts_sbg_laketemp.const import LAKE_SCHEMA, build_lake_config
from custom_components.bgl_ts_sbg_laketemp.scrapers.hydro_ooe import (
    HttpError,
    HydroOOEScraper,
    NetworkError,
    ParseError,
    NoDataError,
)


ZRXP_URL = "https://data.ooe.gv.at/files/hydro/HDOOE_Export_WT.zrxp"


async def _fetch_latest(lake_cfg):  # type: ignore[no-untyped-def]
    """Fetch the latest reading for a Hydro OÖ lake config via ``HydroOOEScraper``.

    Mirrors how the dataset coordinator selects a series: explicit
    ``options.station_id`` (SANR) first, the lake name as hint otherwise.
    """
    options = lake_cfg.source.options
    station_id = str(options.station_id) if getattr(options, "station_id", None) else None
    async with HydroOOEScraper(station_id=station_id, name_hint=lake_cfg.name) as scraper:
        return await scraper.fetch_latest()


# Test: Successful ZRXP parsing when SANR is explicitly provided (5005 Irrsee)
# Expect: Latest reading has 23.1 at 2025-08-08 16:00 with TZ +01:00
@pytest.mark.asyncio
async def test_hydro_ooe_success_latest_from_json_series() -> None:
    url = "https://data.ooe.gv.at/files/hydro/HDOOE_Export_WT.zrxp"
    raw = {
        "name": "Irrsee / Zell am Moos",
        "url": url,
        "entity_id": "irrsee_zell",
        "source": {"type": "hydro_ooe", "options": {"station_id": "5005"}},
    }
    validated = LAKE_SCHEMA(raw)
    lake_cfg = build_lake_config(validated)

    zrxp_text = (
        "#ZRXPVERSION2300.100|*|ZRXPCREATORKiIOSystem.ZRXPV2R2_E|*| "
        "#SANR5005|*|SNAMEZell am Moos|*|SWATERZeller See (Irrsee)|*|CNRWT|*|CNAMEWassertemperatur|*| "
        "#TZUTC+1|*|RINVAL-777|*| #CUNIT°C|*| #LAYOUT(timestamp,value)|*| "
        "20250808140000 22.8 20250808150000 23.0 20250808160000 23.1"
    )

    with aioresponses() as mocked:
        mocked.get(ZRXP_URL, status=200, body=zrxp_text, headers={"Content-Type": "text/plain"})
        reading = await _fetch_latest(lake_cfg)

    assert reading.temperature_c == 23.1
    assert reading.timestamp.hour == 16
    assert reading.timestamp.tzinfo is not None


# Test: Prefer water temperature when multiple parameter blocks exist for same SANR
# Expect: Chooses CNRWT/CNAME=Wassertemperatur over e.g., air temperature
@pytest.mark.asyncio
async def test_hydro_ooe_prefers_water_temperature_block_for_same_sanr() -> None:
    url = ZRXP_URL
    raw = {
        "name": "Irrsee / Zell am Moos",
        "url": url,
        "entity_id": "irrsee_zell",
        "source": {"type": "hydro_ooe", "options": {"station_id": "5005"}},
    }
    validated = LAKE_SCHEMA(raw)
    lake_cfg = build_lake_config(validated)

    # Two blocks with the same SANR 5005: first is air temp (simulate), second is water temp
    zrxp_text = (
        "#ZRXPVERSION2300.100|*|ZRXPCREATORKiIOSystem.ZRXPV2R2_E|*| "
        "#SANR5005|*|SNAMEZell am Moos|*|SWATERZeller See (Irrsee)|*|CNRLT|*|CNAMELufttemperatur|*| "
        "#TZUTC+1|*|RINVAL-777|*| #CUNIT°C|*| #LAYOUT(timestamp,value)|*| "
        "20250808140000 15.1 20250808150000 15.0 "
        "#SANR5005|*|SNAMEZell am Moos|*|SWATERZeller See (Irrsee)|*|CNRWT|*|CNAMEWassertemperatur|*| "
        "#TZUTC+1|*|RINVAL-777|*| #CUNIT°C|*| #LAYOUT(timestamp,value)|*| "
        "20250808140000 24.8 20250808150000 25.0"
    )

    with aioresponses() as mocked:
        mocked.get(ZRXP_URL, status=200, body=zrxp_text, headers={"Content-Type": "text/plain"})
        reading = await _fetch_latest(lake_cfg)

    assert reading.temperature_c == 25.0


# Test: Name-based selection prefers water temperature block when SANR is not provided
# Expect: Chooses WT block for exact SNAME match
@pytest.mark.asyncio
async def test_hydro_ooe_name_based_selection_prefers_wt() -> None:
    zrxp_text = (
        "#SANR5005|*|SNAMEZell am Moos|*|SWATERZeller See (Irrsee)|*|CNRLT|*|CNAMELufttemperatur|*| "
        "#TZUTC+1|*|RINVAL-777|*| #CUNIT°C|*| #LAYOUT(timestamp,value)|*| "
        "20250808140000 15.1 20250808150000 15.0 "
        "#SANR5005|*|SNAMEZell am Moos|*|SWATERZeller See (Irrsee)|*|CNRWT|*|CNAMEWassertemperatur|*| "
        "#TZUTC+1|*|RINVAL-777|*| #CUNIT°C|*| #LAYOUT(timestamp,value)|*| "
        "20250808140000 24.8 20250808150000 25.0"
    )

    with aioresponses() as mocked:
        mocked.get(ZRXP_URL, status=200, body=zrxp_text, headers={"Content-Type": "text/plain"})
        # Use exact name matching (name_hint) rather than deprecated substring filtering
        async with HydroOOEScraper(name_hint="Zell am Moos") as scraper:
            latest = await scraper.fetch_latest()

    assert latest.temperature_c == 25.0

# Test: HTTP 404 from ZRXP endpoint (SANR specified)
# Expect: HttpError is raised
@pytest.mark.asyncio
async def test_hydro_ooe_http_404_raises() -> None:
    url = "https://data.ooe.gv.at/files/hydro/HDOOE_Export_WT.zrxp"
    raw = {"name": "Irrsee", "url": url, "entity_id": "irrsee", "source": {"type": "hydro_ooe", "options": {"station_id": "5005"}}}
    validated = LAKE_SCHEMA(raw)
    lake_cfg = build_lake_config(validated)

    with aioresponses() as mocked:
        mocked.get(ZRXP_URL, status=404)
        with pytest.raises(HttpError):
            await _fetch_latest(lake_cfg)


# Test: Timeout during ZRXP request (SANR specified)
# Expect: NetworkError
@pytest.mark.asyncio
async def test_hydro_ooe_timeout_raises_network() -> None:
    import aiohttp

    url = "https://data.ooe.gv.at/files/hydro/HDOOE_Export_WT.zrxp"
    raw = {"name": "Irrsee", "url": url, "entity_id": "irrsee", "source": {"type": "hydro_ooe", "options": {"station_id": "5005"}}}
    validated = LAKE_SCHEMA(raw)
    lake_cfg = build_lake_config(validated)

    with aioresponses() as mocked:
        mocked.get(ZRXP_URL, exception=aiohttp.ServerTimeoutError())
        with pytest.raises(NetworkError):
            await _fetch_latest(lake_cfg)


# Test: Malformed ZRXP payload structure (missing layout marker)
# Expect: ParseError
@pytest.mark.asyncio
async def test_hydro_ooe_malformed_payload_raises_parse() -> None:
    url = "https://data.ooe.gv.at/files/hydro/HDOOE_Export_WT.zrxp"
    # No SANR provided here so the test exercises parsing errors after a name match
    raw = {"name": "Irrsee", "url": url, "entity_id": "irrsee", "source": {"type": "hydro_ooe", "options": {}}}
    validated = LAKE_SCHEMA(raw)
    lake_cfg = build_lake_config(validated)

    with aioresponses() as mocked:
        bad_text = "#SANR16579|*|SNAMEIrrsee|*| #TZUTC+1|*| RINVAL-777|*|"
        mocked.get(ZRXP_URL, status=200, body=bad_text, headers={"Content-Type": "text/plain"})
        with pytest.raises(ParseError):
            await _fetch_latest(lake_cfg)


# Test: No data in series (no numeric pairs after layout)
# Expect: NoDataError
@pytest.mark.asyncio
async def test_hydro_ooe_no_data_in_series_raises_nodata() -> None:
    url = "https://data.ooe.gv.at/files/hydro/HDOOE_Export_WT.zrxp"
    raw = {"name": "Irrsee", "url": url, "entity_id": "irrsee", "source": {"type": "hydro_ooe", "options": {"station_id": "5005"}}}
    validated = LAKE_SCHEMA(raw)
    lake_cfg = build_lake_config(validated)

    with aioresponses() as mocked:
        empty_text = (
            "#SANR16579|*|SNAMEIrrsee|*| #TZUTC+1|*| RINVAL-777|*| #LAYOUT(timestamp,value)|*| "
        )
        mocked.get(ZRXP_URL, status=200, body=empty_text, headers={"Content-Type": "text/plain"})
        with pytest.raises(NoDataError):
            await _fetch_latest(lake_cfg)
