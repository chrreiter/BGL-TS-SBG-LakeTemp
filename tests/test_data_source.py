from __future__ import annotations

# Tests for the data source abstraction layer
# - GKDBayernSource: fetch_temperature returns TemperatureReading with latest values
# - Factory: create_data_source builds GKDBayernSource from LakeConfig
# - Factory: shared-dataset sources (hydro_ooe, salzburg_ogd) are not per-lake sources

import pathlib

import pytest
from aioresponses import aioresponses

from custom_components.bgl_ts_sbg_laketemp.data_source import (
    GKDBayernSource,
    TemperatureReading,
    create_data_source,
)
from custom_components.bgl_ts_sbg_laketemp.const import LAKE_SCHEMA, build_lake_config


FIXTURE_PATH = pathlib.Path(__file__).parent / "fixtures" / "gkd_bayern_table_sample.html"


# Test: GKDBayernSource returns latest temperature reading
# Expect: TemperatureReading with 23.1°C and correct timestamp metadata
@pytest.mark.asyncio
async def test_gkd_bayern_source_fetch_temperature() -> None:
    url = "https://www.gkd.bayern.de/de/seen/wassertemperatur/inn/seethal-18673955/messwerte"
    url_tab = url.rstrip("/") + "/tabelle"
    html = FIXTURE_PATH.read_text(encoding="utf-8")

    with aioresponses() as mocked:
        mocked.get(
            url_tab,
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
    assert isinstance(source, GKDBayernSource)

    # Sanity: using the same fixture path should produce latest reading
    html = FIXTURE_PATH.read_text(encoding="utf-8")
    with aioresponses() as mocked:
        mocked.get(
            lake_cfg.url.rstrip("/") + "/tabelle",
            status=200,
            body=html,
            headers={"Content-Type": "text/html; charset=utf-8"},
        )
        reading = await source.fetch_temperature()

    assert reading.temperature_c == 23.1


# Test: Shared-dataset sources have no per-lake data source
# Expect: create_data_source raises NotImplementedError for hydro_ooe / salzburg_ogd
# (these lakes are served by the dataset coordinators instead)
@pytest.mark.parametrize(
    "source",
    [
        {"type": "hydro_ooe", "options": {"station_id": "5005"}},
        {"type": "salzburg_ogd", "options": {"lake_name": "Fuschlsee"}},
    ],
)
def test_factory_rejects_shared_dataset_sources(source) -> None:  # type: ignore[no-untyped-def]
    raw = {"name": "Some Lake", "entity_id": "some_lake", "source": source}
    lake_cfg = build_lake_config(LAKE_SCHEMA(raw))
    with pytest.raises(NotImplementedError):
        create_data_source(lake_cfg)
