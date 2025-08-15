from __future__ import annotations

"""URL optionality tests.

Cases:
- Hydro OOE: missing top-level url is allowed; fetch uses official ZRXP
- Salzburg OGD: missing top-level url is allowed; fetch uses official TXT
- GKD Bayern: missing url is rejected by schema
"""

import pytest
from aioresponses import aioresponses
import voluptuous as vol

from custom_components.bgl_ts_sbg_laketemp.const import LAKE_SCHEMA, build_lake_config
from custom_components.bgl_ts_sbg_laketemp.data_source import create_data_source, TemperatureReading


ZRXP_URL = "https://data.ooe.gv.at/files/hydro/HDOOE_Export_WT.zrxp"
OGD_URL = "https://www.salzburg.gv.at/ogd/56c28e2d-8b9e-41ba-b7d6-fa4896b5b48b/Hydrografie%20Seen.txt"


# Title: Hydro OOE accepts missing url — Expect: factory fetches from official ZRXP
@pytest.mark.asyncio
async def test_hydro_ooe_missing_url_is_allowed_and_uses_default() -> None:
    raw = {
        "name": "Irrsee / Zell am Moos",
        # url omitted intentionally
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
        mocked.get(ZRXP_URL, status=200, body=zrxp_text)
        source = create_data_source(lake_cfg)
        reading = await source.fetch_temperature()

    assert isinstance(reading, TemperatureReading)
    assert reading.temperature_c == 23.1


# Title: Salzburg OGD accepts missing url — Expect: factory fetches from official TXT
@pytest.mark.asyncio
async def test_salzburg_ogd_missing_url_is_allowed_and_uses_default() -> None:
    raw = {
        "name": "Fuschlsee",
        # url omitted intentionally
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


