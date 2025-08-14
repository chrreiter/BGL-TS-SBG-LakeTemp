from __future__ import annotations

# Tests for the data source abstraction layer
# - GKDBayernSource: fetch_temperature returns TemperatureReading with latest values
# - Factory: create_data_source builds GKDBayernSource from LakeConfig
# - HydroOOE via factory: fetch_temperature returns TemperatureReading from ZRXP bulk

import pathlib

import pytest
from aioresponses import aioresponses

from custom_components.bgl_ts_sbg_laketemp.data_source import (
    GKDBayernSource,
    TemperatureReading,
    create_data_source,
)
from custom_components.bgl_ts_sbg_laketemp.const import LAKE_SCHEMA, build_lake_config
from custom_components.bgl_ts_sbg_laketemp.scrapers.salzburg_ogd import (
    HttpError as SalzburgHttpError,
)


FIXTURE_PATH = pathlib.Path(__file__).parent / "fixtures" / "gkd_bayern_table_sample.html"
ZRXP_URL = "https://data.ooe.gv.at/files/hydro/HDOOE_Export_WT.zrxp"
OGD_URL = "https://www.salzburg.gv.at/ogd/56c28e2d-8b9e-41ba-b7d6-fa4896b5b48b/Hydrografie%20Seen.txt"


# Test: GKDBayernSource returns latest temperature reading
# Expect: TemperatureReading with 23.1°C and correct timestamp metadata
@pytest.mark.asyncio
async def test_gkd_bayern_source_fetch_temperature() -> None:
    url = "https://www.gkd.bayern.de/de/seen/wassertemperatur/inn/seethal-18673955/messwerte"
    html = FIXTURE_PATH.read_text(encoding="utf-8")

    with aioresponses() as mocked:
        mocked.get(
            url,
            status=200,
            body=html,
            headers={"Content-Type": "text/html; charset=utf-8"},
        )

        source = GKDBayernSource(url=url)
        reading = await source.fetch_temperature()

    assert isinstance(reading, TemperatureReading)
    assert reading.temperature_c == 23.1
    assert reading.timestamp.year == 2025
    assert reading.timestamp.month == 8
    assert reading.timestamp.day == 8
    assert reading.timestamp.hour == 16
    assert reading.timestamp.tzinfo is not None
    assert reading.source == "gkd_bayern"


# Test: Factory creates GKDBayernSource from LakeConfig
# Expect: create_data_source returns a GKDBayernSource instance
@pytest.mark.asyncio
async def test_factory_creates_gkd_bayern_source() -> None:
    raw = {
        "name": "Seethal / Abtsdorfer See",
        "url": "https://www.gkd.bayern.de/de/seen/wassertemperatur/inn/seethal-18673955/messwerte",
        "entity_id": "seethal_abtsdorfer",
        # leave scan_interval, timeout_hours, user_agent defaulted by schema
        "source": {"type": "gkd_bayern", "options": {"table_selector": None}},
    }
    validated = LAKE_SCHEMA(raw)
    lake_cfg = build_lake_config(validated)

    source = create_data_source(lake_cfg)
    # duck-type rather than isinstance to allow protocol or subclassing
    assert hasattr(source, "fetch_temperature") and hasattr(source, "get_update_frequency")

    # Sanity: using the same fixture path should produce latest reading
    html = FIXTURE_PATH.read_text(encoding="utf-8")
    with aioresponses() as mocked:
        mocked.get(
            lake_cfg.url,
            status=200,
            body=html,
            headers={"Content-Type": "text/html; charset=utf-8"},
        )
        reading = await source.fetch_temperature()

    assert reading.temperature_c == 23.1


# Test: Factory creates HydroOOE source and returns latest temperature via ZRXP
# Expect: TemperatureReading 23.1°C at 16:00 with tz info and source 'hydro_ooe'
@pytest.mark.asyncio
async def test_factory_creates_hydro_ooe_source_and_fetches_latest() -> None:
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
        source = create_data_source(lake_cfg)
        reading = await source.fetch_temperature()

    assert isinstance(reading, TemperatureReading)
    assert reading.temperature_c == 23.1
    assert reading.timestamp.hour == 16
    assert reading.timestamp.tzinfo is not None
    assert reading.source == "hydro_ooe"


# Test: Factory creates Salzburg OGD source and returns latest temperature
# Expect: TemperatureReading with source 'salzburg_ogd'
@pytest.mark.asyncio
async def test_factory_creates_salzburg_ogd_source_and_fetches_latest() -> None:
    raw = {
        "name": "Fuschlsee",
        "url": OGD_URL,
        "entity_id": "fuschlsee",
        "source": {"type": "salzburg_ogd", "options": {"lake_name": "Fuschlsee"}},
    }
    validated = LAKE_SCHEMA(raw)
    lake_cfg = build_lake_config(validated)

    payload = (
        "Gewässer;Messdatum;Uhrzeit;Wassertemperatur [°C];Station\n"
        "Fuschlsee;2025-08-08;13:00;22,0;Westufer\n"
        "Fuschlsee;2025-08-08;14:00;22,4;Westufer\n"
    )

    with aioresponses() as mocked:
        mocked.get(OGD_URL, status=200, body=payload)
        source = create_data_source(lake_cfg)
        reading = await source.fetch_temperature()

    assert isinstance(reading, TemperatureReading)
    assert reading.temperature_c == 22.4
    assert reading.timestamp.hour == 14
    assert reading.source == "salzburg_ogd"
