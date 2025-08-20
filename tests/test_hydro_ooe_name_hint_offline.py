from __future__ import annotations

"""Name-hint based selection for Hydro OOE (offline).

- Title: Name-hint selects series by SNAME/SWATER tokens; latest value returned
- Expectation: With no SANR configured, the name hint (lake name) is tokenized
  and matched against SNAME/SWATER fields; the matching series is parsed and the
  latest value is returned.
"""

import pytest
from aioresponses import aioresponses

from custom_components.bgl_ts_sbg_laketemp.const import LAKE_SCHEMA, build_lake_config
from custom_components.bgl_ts_sbg_laketemp.data_source import TemperatureReading, create_data_source


ZRXP_URL = "https://data.ooe.gv.at/files/hydro/HDOOE_Export_WT.zrxp"


@pytest.mark.asyncio
async def test_hydro_ooe_name_hint_selects_and_returns_latest() -> None:
    # Title: Exact name-hint selects series — Expect: latest value returned
    raw = {
        "name": "Zell am Moos",  # exact SNAME
        "url": ZRXP_URL,
        "entity_id": "irrsee_zell_hint",
        "source": {"type": "hydro_ooe", "options": {}},
    }
    validated = LAKE_SCHEMA(raw)
    lake_cfg = build_lake_config(validated)

    # Single matching block with WT parameter and two datapoints; ensure latest picked
    zrxp_text = (
        "#ZRXPVERSION2300.100|*|ZRXPCREATORKiIOSystem.ZRXPV2R2_E|*| "
        "#SANR5005|*|SNAMEZell am Moos|*|SWATERZeller See (Irrsee)|*|CNRWT|*|CNAMEWassertemperatur|*| "
        "#TZUTC+1|*|RINVAL-777|*| #CUNIT°C|*| #LAYOUT(timestamp,value)|*| "
        "20250808150000 23.0 20250808160000 23.3"
    )

    with aioresponses() as mocked:
        mocked.get(ZRXP_URL, status=200, body=zrxp_text, headers={"Content-Type": "text/plain"})
        source = create_data_source(lake_cfg)
        reading = await source.fetch_temperature()

    assert isinstance(reading, TemperatureReading)
    assert reading.temperature_c == 23.3
    assert reading.timestamp.tzinfo is not None
    assert reading.timestamp.hour == 16  # latest sample hour
    assert reading.source == "hydro_ooe"


