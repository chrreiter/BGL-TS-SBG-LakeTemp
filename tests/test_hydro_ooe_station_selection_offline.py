from __future__ import annotations

"""Hydro OOE station selection rules (offline).

- SANR preferred over name, strict match; name is only used when SANR absent
- Exact (case-insensitive) name matching only; ambiguity -> error
"""

import pytest
from aioresponses import aioresponses

from custom_components.bgl_ts_sbg_laketemp.const import LAKE_SCHEMA, build_lake_config
from custom_components.bgl_ts_sbg_laketemp.data_source import create_data_source, TemperatureReading
from custom_components.bgl_ts_sbg_laketemp.scrapers.hydro_ooe import NoDataError


ZRXP_URL = "https://data.ooe.gv.at/files/hydro/HDOOE_Export_WT.zrxp"


def _block(sanr: str, sname: str, swater: str, values: list[tuple[str, str]]) -> str:
    layout = "#LAYOUT(timestamp,value)|*| " + " ".join([f"{ts} {val}" for ts, val in values])
    return (
        f"#SANR{sanr}|*|SNAME{sname}|*|SWATER{swater}|*|CNRWT|*|CNAMEWassertemperatur|*| "
        f"#TZUTC+1|*|RINVAL-777|*| #CUNIT°C|*| {layout}"
    )


# Title: SANR provided, name hint mismatching — Expect: selects by SANR and succeeds
@pytest.mark.asyncio
async def test_sanr_overrides_mismatching_name() -> None:
    raw = {
        "name": "Completely Wrong Name",
        "url": ZRXP_URL,
        "entity_id": "irrsee_wrong_name",
        "source": {"type": "hydro_ooe", "options": {"station_id": "5005"}},
    }
    validated = LAKE_SCHEMA(raw)
    lake = build_lake_config(validated)

    payload = _block("5005", "Zell am Moos", "Zeller See (Irrsee)", [("20250808160000", "23.1")])

    with aioresponses() as mocked:
        mocked.get(ZRXP_URL, status=200, body=payload, headers={"Content-Type": "text/plain"})
        src = create_data_source(lake)
        reading = await src.fetch_temperature()

    assert isinstance(reading, TemperatureReading)
    assert reading.temperature_c == 23.1


# Title: SANR unknown — Expect: clear NoDataError mentioning SANR
@pytest.mark.asyncio
async def test_unknown_sanr_raises_nodata_with_message() -> None:
    raw = {
        "name": "Irrsee / Zell am Moos",
        "url": ZRXP_URL,
        "entity_id": "irrsee_unknown_sanr",
        "source": {"type": "hydro_ooe", "options": {"station_id": "999999"}},
    }
    validated = LAKE_SCHEMA(raw)
    lake = build_lake_config(validated)

    # Payload contains only a different station
    payload = _block("5005", "Zell am Moos", "Zeller See (Irrsee)", [("20250808160000", "23.1")])

    with aioresponses() as mocked:
        mocked.get(ZRXP_URL, status=200, body=payload, headers={"Content-Type": "text/plain"})
        src = create_data_source(lake)
        with pytest.raises(NoDataError) as e:
            await src.fetch_temperature()

    # Error message should mention SANR and the value
    assert "SANR" in str(e.value) and "999999" in str(e.value)


# Title: Unique exact name match, no SANR — Expect: success
@pytest.mark.asyncio
async def test_unique_exact_name_match_succeeds() -> None:
    raw = {
        "name": "Zell am Moos",  # exact SNAME below
        "url": ZRXP_URL,
        "entity_id": "irrsee_exact_name",
        "source": {"type": "hydro_ooe", "options": {}},
    }
    validated = LAKE_SCHEMA(raw)
    lake = build_lake_config(validated)

    payload = _block("5005", "Zell am Moos", "Zeller See (Irrsee)", [("20250808160000", "23.3")])

    with aioresponses() as mocked:
        mocked.get(ZRXP_URL, status=200, body=payload, headers={"Content-Type": "text/plain"})
        src = create_data_source(lake)
        reading = await src.fetch_temperature()

    assert isinstance(reading, TemperatureReading)
    assert reading.temperature_c == 23.3


# Title: Ambiguous exact name across multiple SANR — Expect: NoDataError indicating ambiguity
@pytest.mark.asyncio
async def test_ambiguous_exact_name_raises() -> None:
    raw = {
        "name": "Attersee",  # exact for SWATER in both blocks
        "url": ZRXP_URL,
        "entity_id": "attersee_ambiguous",
        "source": {"type": "hydro_ooe", "options": {}},
    }
    validated = LAKE_SCHEMA(raw)
    lake = build_lake_config(validated)

    payload = (
        _block("12345", "Attersee-Ort", "Attersee", [("20250808160000", "22.0")])
        + _block("67890", "Attersee-Nord", "Attersee", [("20250808160000", "22.2")])
    )

    with aioresponses() as mocked:
        mocked.get(ZRXP_URL, status=200, body=payload, headers={"Content-Type": "text/plain"})
        src = create_data_source(lake)
        with pytest.raises(NoDataError) as e:
            await src.fetch_temperature()

    assert "Ambiguous" in str(e.value) or "ambiguous" in str(e.value)


# Title: Fuzzy/close name without exact match — Expect: NoDataError (do not pick a best-effort)
@pytest.mark.asyncio
async def test_fuzzy_name_does_not_match() -> None:
    raw = {
        "name": "Zell am Mos",  # typo; should NOT match
        "url": ZRXP_URL,
        "entity_id": "irrsee_fuzzy",
        "source": {"type": "hydro_ooe", "options": {}},
    }
    validated = LAKE_SCHEMA(raw)
    lake = build_lake_config(validated)

    payload = _block("5005", "Zell am Moos", "Zeller See (Irrsee)", [("20250808160000", "23.0")])

    with aioresponses() as mocked:
        mocked.get(ZRXP_URL, status=200, body=payload, headers={"Content-Type": "text/plain"})
        src = create_data_source(lake)
        with pytest.raises(NoDataError):
            await src.fetch_temperature()


