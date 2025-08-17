from __future__ import annotations

"""Shared polling tests for Hydro OOE using mocked ZRXP.

- Two lakes from the Hydro OOE dataset share one coordinator and one HTTP GET per refresh
- The dataset coordinator update_interval is the minimum scan_interval across members
"""

from datetime import timedelta
from typing import List

import pytest
from aioresponses import aioresponses

from custom_components.bgl_ts_sbg_laketemp.sensor import async_setup_platform
from custom_components.bgl_ts_sbg_laketemp.const import CONF_LAKES


ZRXP_URL = "https://data.ooe.gv.at/files/hydro/HDOOE_Export_WT.zrxp"


def _zrxp_block(sanr: str, sname: str, swater: str, tz="+#1", values: list[tuple[str, str]] | None = None) -> str:
    if values is None:
        values = [("20250808140000", "22.4")]
    layout = "#LAYOUT(timestamp,value)|*|" + " ".join([f"{ts} {val}" for ts, val in values])
    return (
        f"#SANR{sanr}|*|SNAME{sname}|*|SWATER{swater}|*|CNRWT|*|CNAMEWassertemperatur\n"
        f"#TZUTC{tz}\n"
        f"{layout}\n"
    )


class _EntityList:
    def __init__(self) -> None:
        self.entities: List[object] = []

    def __call__(self, entities):  # type: ignore[no-untyped-def]
        if isinstance(entities, list):
            self.entities.extend(entities)
        else:
            self.entities.append(entities)


@pytest.mark.asyncio
async def test_hydro_ooe_shared_polling_min_interval_and_single_get() -> None:  # type: ignore[no-untyped-def]
    # Title: Two Hydro OOE lakes share polling — Expect: single GET, shared coordinator, min interval

    discovery_info = {
        CONF_LAKES: [
            {
                "name": "Irrsee / Zell am Moos",
                "url": "https://hydro.ooe.gv.at/#/overview/Wassertemperatur/station/16579/Zell%20am%20Moos/Wassertemperatur?period=P7D",
                "entity_id": "irrsee_zell",
                "scan_interval": 1800,
                "timeout_hours": 336,
                "source": {"type": "hydro_ooe", "options": {"station_id": "16579"}},
            },
            {
                "name": "Attersee",
                "url": "https://hydro.ooe.gv.at/#/overview/Wassertemperatur",
                "entity_id": "attersee",
                "scan_interval": 120,
                "timeout_hours": 336,
                "source": {"type": "hydro_ooe", "options": {}},
            },
        ]
    }

    # Build a ZRXP payload with two stations: 16579 (Irrsee/Zell am Moos) and another (e.g., 12345 Attersee)
    block1 = _zrxp_block("16579", "Zell am Moos", "Irrsee", values=[("20250808140000", "22.4")])
    block2 = _zrxp_block("12345", "Attersee", "Attersee", values=[("20250808140500", "23.7")])
    payload = block1 + block2

    added = _EntityList()

    with aioresponses() as mocked:
        mocked.get(ZRXP_URL, status=200, body=payload, headers={"Content-Type": "text/plain; charset=utf-8"})
        await async_setup_platform(hass={}, config={}, async_add_entities=added, discovery_info=discovery_info)

        # Must have created two entities
        assert len(added.entities) == 2
        s0, s1 = added.entities

        # Trigger a single refresh via the first sensor's shared coordinator
        await s0.coordinator.async_refresh()

        # Both sensors should be available and reflect values from the same refresh
        assert s0.available is True and s1.available is True
        assert s0.native_value == 22.4
        assert s1.native_value == 23.7

        # Both sensors must share the same coordinator
        assert s0.coordinator is s1.coordinator

        # Update interval should be min(scan_interval) = 120s
        assert s0.coordinator.update_interval == timedelta(seconds=120)

        # Only one HTTP GET should have occurred for the dataset
        from yarl import URL
        key = ("GET", URL(ZRXP_URL))
        assert len(mocked.requests.get(key, [])) == 1

        # Cleanup: ensure dataset coordinator session is closed
        if getattr(s0, "_dataset_manager", None) is not None:  # type: ignore[attr-defined]
            await s0._dataset_manager.async_close()  # type: ignore[attr-defined]


