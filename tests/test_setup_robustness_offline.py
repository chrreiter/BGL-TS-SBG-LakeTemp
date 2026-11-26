from __future__ import annotations

"""Offline regression tests for platform setup and Hydro OOE dataset robustness.

- Invalid lake configs log the lake name and source type (not a swallowed NameError)
- A per-lake (GKD) sensor is fetched once at startup, not again in async_added_to_hass
- A shared-dataset sensor triggers the initial dataset refresh in async_added_to_hass
- The Hydro OOE coordinator picks the newest point even if a block is out of order
- The Hydro OOE coordinator decodes non-UTF-8 exports so name matching still works
"""

import logging
from typing import List

import pytest
from aioresponses import aioresponses
from yarl import URL

from custom_components.bgl_ts_sbg_laketemp.const import CONF_LAKES
from custom_components.bgl_ts_sbg_laketemp.sensor import async_setup_platform


GKD_URL = "https://www.gkd.bayern.de/de/seen/wassertemperatur/inn/seethal-18673955/messwerte"
GKD_TABLE_URL = GKD_URL + "/tabelle"
GKD_HTML = """
<html><body><table>
  <thead><tr><th>Datum</th><th>Wassertemperatur [°C]</th></tr></thead>
  <tbody><tr><td>08.08.2025 16:00</td><td>23,1</td></tr></tbody>
</table></body></html>
"""
ZRXP_URL = "https://data.ooe.gv.at/files/hydro/HDOOE_Export_WT.zrxp"


class _EntityList:
    def __init__(self) -> None:
        self.entities: List[object] = []

    def __call__(self, entities):  # type: ignore[no-untyped-def]
        self.entities.extend(entities)


def _get_count(mocked: aioresponses, url: str) -> int:
    return len(mocked.requests.get(("GET", URL(url)), []))


def _zrxp_block(sanr: str, sname: str, swater: str, values: list[tuple[str, str]]) -> str:
    layout = "#LAYOUT(timestamp,value)|*|" + " ".join(f"{ts} {val}" for ts, val in values)
    return (
        f"#SANR{sanr}|*|SNAME{sname}|*|SWATER{swater}|*|CNRWT|*|CNAMEWassertemperatur\n"
        f"#TZUTC+1\n{layout}\n"
    )


@pytest.mark.asyncio
async def test_invalid_lake_error_log_includes_name_and_source(caplog) -> None:  # type: ignore[no-untyped-def]
    # Title: Invalid lake config — Expect: error log names the lake and its source type
    caplog.set_level(logging.ERROR)
    discovery_info = {
        CONF_LAKES: [
            {
                "name": "Broken Lake",
                "entity_id": "Not A Slug",
                "source": {"type": "gkd_bayern", "options": {}},
            }
        ]
    }
    added = _EntityList()
    await async_setup_platform(hass={}, config={}, async_add_entities=added, discovery_info=discovery_info)

    assert added.entities == []
    assert "name='Broken Lake', source='gkd_bayern'" in caplog.text


@pytest.mark.asyncio
async def test_per_lake_sensor_fetched_once_on_startup() -> None:  # type: ignore[no-untyped-def]
    # Title: GKD sensor startup — Expect: exactly one GET across setup + async_added_to_hass
    discovery_info = {
        CONF_LAKES: [
            {
                "name": "Abtsdorfer See",
                "url": GKD_URL,
                "entity_id": "abtsdorfer_see_once",
                "timeout_hours": 336,
                "source": {"type": "gkd_bayern", "options": {}},
            }
        ]
    }
    added = _EntityList()
    with aioresponses() as mocked:
        mocked.get(GKD_TABLE_URL, status=200, body=GKD_HTML, repeat=True)
        await async_setup_platform(hass={}, config={}, async_add_entities=added, discovery_info=discovery_info)
        (sensor,) = added.entities
        await sensor.async_added_to_hass()

        assert sensor.native_value == 23.1
        assert _get_count(mocked, GKD_TABLE_URL) == 1


@pytest.mark.asyncio
async def test_dataset_sensors_refresh_once_when_added() -> None:  # type: ignore[no-untyped-def]
    # Title: Two Hydro OOE sensors added — Expect: first add triggers one dataset GET, second reuses it
    discovery_info = {
        CONF_LAKES: [
            {
                "name": "Irrsee",
                "entity_id": "irrsee_added",
                "timeout_hours": 336,
                "source": {"type": "hydro_ooe", "options": {"station_id": "5005"}},
            },
            {
                "name": "Mondsee",
                "entity_id": "mondsee_added",
                "timeout_hours": 336,
                "source": {"type": "hydro_ooe", "options": {"station_id": "5020"}},
            },
        ]
    }
    payload = _zrxp_block("5005", "Zell am Moos", "Irrsee", [("20250808140000", "21.0")]) + _zrxp_block(
        "5020", "Mondsee", "Mondsee", [("20250808140000", "22.0")]
    )
    hass: dict = {}
    added = _EntityList()
    with aioresponses() as mocked:
        mocked.get(ZRXP_URL, status=200, body=payload, repeat=True)
        await async_setup_platform(hass=hass, config={}, async_add_entities=added, discovery_info=discovery_info)
        assert _get_count(mocked, ZRXP_URL) == 0  # dataset refresh is deferred to entity add

        for sensor in added.entities:
            await sensor.async_added_to_hass()

        assert [s.native_value for s in added.entities] == [21.0, 22.0]
        assert _get_count(mocked, ZRXP_URL) == 1


@pytest.mark.asyncio
async def test_hydro_ooe_coordinator_uses_newest_point_and_decodes_cp1252() -> None:  # type: ignore[no-untyped-def]
    # Title: Out-of-order, cp1252-encoded export matched by name — Expect: newest value, umlaut name matches
    discovery_info = {
        CONF_LAKES: [
            {
                "name": "Hallstätter See",
                "entity_id": "hallstaetter_see_enc",
                "timeout_hours": 336,
                "source": {"type": "hydro_ooe", "options": {}},
            }
        ]
    }
    block = _zrxp_block(
        "5111",
        "Steeg",
        "Hallstätter See",
        # Newest point (15:00) deliberately not last
        [("20250808140000", "20.0"), ("20250808150000", "21.5"), ("20250808130000", "19.0")],
    )
    hass: dict = {}
    added = _EntityList()
    with aioresponses() as mocked:
        # No charset declared, body is not valid UTF-8
        mocked.get(ZRXP_URL, status=200, body=block.encode("cp1252"), headers={"Content-Type": "text/plain"})
        await async_setup_platform(hass=hass, config={}, async_add_entities=added, discovery_info=discovery_info)
        (sensor,) = added.entities
        await sensor.async_added_to_hass()

    assert sensor.native_value == 21.5
    assert sensor.extra_state_attributes["data_timestamp"].startswith("2025-08-08T15:00:00")
