from __future__ import annotations

"""URL optionality tests.

Cases:
- Hydro OOE: missing top-level url is allowed; the dataset coordinator fetches the official ZRXP
- Salzburg OGD: missing top-level url is allowed; the dataset coordinator fetches the official TXT
- GKD Bayern: missing url is rejected by schema
"""

import pytest
from aioresponses import aioresponses
import voluptuous as vol

from custom_components.bgl_ts_sbg_laketemp.const import CONF_LAKES, LAKE_SCHEMA
from custom_components.bgl_ts_sbg_laketemp.sensor import async_setup_platform


ZRXP_URL = "https://data.ooe.gv.at/files/hydro/HDOOE_Export_WT.zrxp"
OGD_URL = "https://www.salzburg.gv.at/ogd/56c28e2d-8b9e-41ba-b7d6-fa4896b5b48b/Hydrografie%20Seen.txt"


async def _setup_sensor(raw: dict, url: str, body: str):  # type: ignore[no-untyped-def]
    """Create the sensor for ``raw`` via discovery and refresh its dataset once."""
    added: list = []
    await async_setup_platform(
        hass={}, config={}, async_add_entities=added.extend, discovery_info={CONF_LAKES: [raw]}
    )
    assert len(added) == 1
    sensor = added[0]
    with aioresponses() as mocked:
        mocked.get(url, status=200, body=body)
        await sensor.coordinator.async_refresh()
    return sensor


# Title: Hydro OOE accepts missing url — Expect: dataset coordinator fetches from official ZRXP
@pytest.mark.asyncio
async def test_hydro_ooe_missing_url_is_allowed_and_uses_default() -> None:
    raw = {
        "name": "Irrsee / Zell am Moos",
        # url omitted intentionally
        "entity_id": "irrsee_zell",
        "timeout_hours": 336,  # fixture data is old; disable the staleness check
        "source": {"type": "hydro_ooe", "options": {"station_id": "5005"}},
    }

    zrxp_text = (
        "#ZRXPVERSION2300.100|*|ZRXPCREATORKiIOSystem.ZRXPV2R2_E|*| "
        "#SANR5005|*|SNAMEZell am Moos|*|SWATERZeller See (Irrsee)|*|CNRWT|*|CNAMEWassertemperatur|*| "
        "#TZUTC+1|*|RINVAL-777|*| #CUNIT°C|*| #LAYOUT(timestamp,value)|*| "
        "20250808140000 22.8 20250808150000 23.0 20250808160000 23.1"
    )

    sensor = await _setup_sensor(raw, ZRXP_URL, zrxp_text)
    assert sensor.native_value == 23.1


# Title: Salzburg OGD accepts missing url — Expect: dataset coordinator fetches from official TXT
@pytest.mark.asyncio
async def test_salzburg_ogd_missing_url_is_allowed_and_uses_default() -> None:
    raw = {
        "name": "Fuschlsee",
        # url omitted intentionally
        "entity_id": "fuschlsee",
        "timeout_hours": 336,  # fixture data is old; disable the staleness check
        "source": {"type": "salzburg_ogd", "options": {"lake_name": "Fuschlsee"}},
    }

    payload = (
        "Gewässer;Messdatum;Uhrzeit;Wassertemperatur [°C];Station\n"
        "Fuschlsee;2025-08-08;13:00;22,0;Westufer\n"
        "Fuschlsee;2025-08-08;14:00;22,4;Westufer\n"
    )

    sensor = await _setup_sensor(raw, OGD_URL, payload)
    assert sensor.native_value == 22.4


# Title: GKD Bayern requires url — Expect: schema validation raises Invalid
def test_gkd_bayern_missing_url_is_rejected() -> None:
    raw = {
        "name": "Seethal / Abtsdorfer See",
        # url omitted intentionally
        "entity_id": "seethal_abtsdorfer",
        "source": {"type": "gkd_bayern", "options": {"table_selector": None}},
    }
    with pytest.raises(vol.Invalid):
        LAKE_SCHEMA(raw)


