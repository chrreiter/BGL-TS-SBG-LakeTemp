from __future__ import annotations

# Tests for HydroOOE data source using the factory (ZRXP bulk parsing)
# - Success: ZRXP bulk file returns latest TemperatureReading for station
# - Error paths: HTTP 404, timeout, malformed payload, no data -> appropriate errors

import pytest
from aioresponses import aioresponses

from custom_components.bgl_ts_sbg_laketemp.data_source import create_data_source, TemperatureReading
from custom_components.bgl_ts_sbg_laketemp.const import LAKE_SCHEMA, build_lake_config
from custom_components.bgl_ts_sbg_laketemp.scrapers.hydro_ooe import (
    HttpError,
    NetworkError,
    ParseError,
    NoDataError,
)


ZRXP_URL = "https://data.ooe.gv.at/files/hydro/HDOOE_Export_WT.zrxp"


# Test: Successful ZRXP parsing for a named station
# Expect: Latest reading has 23.1 at 2025-08-08 16:00 with TZ +01:00
@pytest.mark.asyncio
async def test_hydro_ooe_success_latest_from_json_series() -> None:
    url = "https://hydro.ooe.gv.at/#/overview/Wassertemperatur/station/16579/Zell%20am%20Moos/Wassertemperatur?period=P7D"
    raw = {
        "name": "Irrsee / Zell am Moos",
        "url": url,
        "entity_id": "irrsee_zell",
        "source": {"type": "hydro_ooe", "options": {}},
    }
    validated = LAKE_SCHEMA(raw)
    lake_cfg = build_lake_config(validated)

    zrxp_text = (
        "#ZRXPVERSION2300.100|*|ZRXPCREATORKiIOSystem.ZRXPV2R2_E|*| "
        "#SANR16579|*|SNAMEIrrsee / Zell am Moos|*|SWATERIrrsee|*|CNRWT|*|CNAMEWassertemperatur|*| "
        "#TZUTC+1|*|RINVAL-777|*| #CUNIT°C|*| #LAYOUT(timestamp,value)|*| "
        "20250808140000 22.8 20250808150000 23.0 20250808160000 23.1"
    )

    with aioresponses() as mocked:
        mocked.get(ZRXP_URL, status=200, body=zrxp_text, headers={"Content-Type": "text/plain"})
        source = create_data_source(lake_cfg)
        reading = await source.fetch_temperature()

    assert isinstance(reading, TemperatureReading)
    assert reading.temperature_c == 23.1
    assert reading.timestamp.hour == 16
    assert reading.timestamp.tzinfo is not None
    assert reading.source == "hydro_ooe"


# Test: HTTP 404 from ZRXP endpoint
# Expect: HttpError is raised
@pytest.mark.asyncio
async def test_hydro_ooe_http_404_raises() -> None:
    url = "https://hydro.ooe.gv.at/#/overview/Wassertemperatur/station/16579/Zell%20am%20Moos/Wassertemperatur?period=P7D"
    raw = {"name": "Irrsee", "url": url, "entity_id": "irrsee", "source": {"type": "hydro_ooe", "options": {}}}
    validated = LAKE_SCHEMA(raw)
    lake_cfg = build_lake_config(validated)

    with aioresponses() as mocked:
        mocked.get(ZRXP_URL, status=404)
        source = create_data_source(lake_cfg)
        with pytest.raises(HttpError):
            await source.fetch_temperature()


# Test: Timeout during ZRXP request
# Expect: NetworkError
@pytest.mark.asyncio
async def test_hydro_ooe_timeout_raises_network() -> None:
    import aiohttp

    url = "https://hydro.ooe.gv.at/#/overview/Wassertemperatur/station/16579/Zell%20am%20Moos/Wassertemperatur?period=P7D"
    raw = {"name": "Irrsee", "url": url, "entity_id": "irrsee", "source": {"type": "hydro_ooe", "options": {}}}
    validated = LAKE_SCHEMA(raw)
    lake_cfg = build_lake_config(validated)

    with aioresponses() as mocked:
        mocked.get(ZRXP_URL, exception=aiohttp.ServerTimeoutError())
        source = create_data_source(lake_cfg)
        with pytest.raises(NetworkError):
            await source.fetch_temperature()


# Test: Malformed ZRXP payload structure (missing layout marker)
# Expect: ParseError
@pytest.mark.asyncio
async def test_hydro_ooe_malformed_payload_raises_parse() -> None:
    url = "https://hydro.ooe.gv.at/#/overview/Wassertemperatur/station/16579/Zell%20am%20Moos/Wassertemperatur?period=P7D"
    raw = {"name": "Irrsee", "url": url, "entity_id": "irrsee", "source": {"type": "hydro_ooe", "options": {}}}
    validated = LAKE_SCHEMA(raw)
    lake_cfg = build_lake_config(validated)

    with aioresponses() as mocked:
        bad_text = "#SANR16579|*|SNAMEIrrsee|*| #TZUTC+1|*| RINVAL-777|*|"
        mocked.get(ZRXP_URL, status=200, body=bad_text, headers={"Content-Type": "text/plain"})
        source = create_data_source(lake_cfg)
        with pytest.raises(ParseError):
            await source.fetch_temperature()


# Test: No data in series (no numeric pairs after layout)
# Expect: NoDataError
@pytest.mark.asyncio
async def test_hydro_ooe_no_data_in_series_raises_nodata() -> None:
    url = "https://hydro.ooe.gv.at/#/overview/Wassertemperatur/station/16579/Zell%20am%20Moos/Wassertemperatur?period=P7D"
    raw = {"name": "Irrsee", "url": url, "entity_id": "irrsee", "source": {"type": "hydro_ooe", "options": {}}}
    validated = LAKE_SCHEMA(raw)
    lake_cfg = build_lake_config(validated)

    with aioresponses() as mocked:
        empty_text = (
            "#SANR16579|*|SNAMEIrrsee|*| #TZUTC+1|*| RINVAL-777|*| #LAYOUT(timestamp,value)|*| "
        )
        mocked.get(ZRXP_URL, status=200, body=empty_text, headers={"Content-Type": "text/plain"})
        source = create_data_source(lake_cfg)
        with pytest.raises(NoDataError):
            await source.fetch_temperature()
