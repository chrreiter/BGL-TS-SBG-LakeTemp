from __future__ import annotations

"""Scan interval defaults and min recomputation behavior for aggregated datasets (offline)."""

from datetime import timedelta
from typing import List

import pytest
from aioresponses import aioresponses

from custom_components.bgl_ts_sbg_laketemp.sensor import async_setup_platform
from custom_components.bgl_ts_sbg_laketemp.const import CONF_LAKES, DEFAULT_SCAN_INTERVAL_SECONDS


OGD_URL = "https://www.salzburg.gv.at/ogd/56c28e2d-8b9e-41ba-b7d6-fa4896b5b48b/Hydrografie%20Seen.txt"
ZRXP_URL = "https://data.ooe.gv.at/files/hydro/HDOOE_Export_WT.zrxp"


class _EntityList:
    def __init__(self) -> None:
        self.entities: List[object] = []

    def __call__(self, entities):  # type: ignore[no-untyped-def]
        if isinstance(entities, list):
            self.entities.extend(entities)
        else:
            self.entities.append(entities)


@pytest.mark.asyncio
async def test_ogd_min_interval_uses_default_when_omitted() -> None:  # type: ignore[no-untyped-def]
    # Title: Omitted scan_interval uses default 1800 in dataset min calculation

    discovery_info = {
        CONF_LAKES: [
            {
                "name": "Fuschlsee",
                "url": OGD_URL,
                "entity_id": "fuschlsee",
                # scan_interval omitted -> default 1800
                "timeout_hours": 336,
                "source": {"type": "salzburg_ogd", "options": {"lake_name": "Fuschlsee"}},
            },
            {
                "name": "Mattsee",
                "url": OGD_URL,
                "entity_id": "mattsee",
                "scan_interval": 90,
                "timeout_hours": 336,
                "source": {"type": "salzburg_ogd", "options": {"lake_name": "Mattsee"}},
            },
        ]
    }

    payload = (
        "Gewässer;Messdatum;Uhrzeit;Wassertemperatur [°C];Station\n"
        "Fuschlsee;2025-08-08;14:00;22,4;Westufer\n"
        "Mattsee;2025-08-08;14:05;23,1;Nord\n"
    )

    added = _EntityList()

    with aioresponses() as mocked:
        mocked.get(OGD_URL, status=200, body=payload, headers={"Content-Type": "text/plain; charset=utf-8"})
        await async_setup_platform(hass={}, config={}, async_add_entities=added, discovery_info=discovery_info)

        assert len(added.entities) == 2
        s0 = added.entities[0]
        await s0.coordinator.async_refresh()
        # Min should be 90s due to second lake, considering default 1800 for first
        assert s0.coordinator.update_interval == timedelta(seconds=90)

        # After removing the 90s lake, min should revert to default 1800
        s1 = added.entities[1]
        await s1.async_will_remove_from_hass()
        assert s0.coordinator.update_interval == timedelta(seconds=DEFAULT_SCAN_INTERVAL_SECONDS)

        # Cleanup: ensure dataset session is closed
        if getattr(s0, "_dataset_manager", None) is not None:  # type: ignore[attr-defined]
            await s0._dataset_manager.async_close()  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_hydro_ooe_min_interval_with_default_and_custom() -> None:  # type: ignore[no-untyped-def]
    # Title: Hydro OOE dataset min uses custom when lower than default 1800

    discovery_info = {
        CONF_LAKES: [
            {
                "name": "Irrsee / Zell am Moos",
                "url": "https://hydro.ooe.gv.at/#/overview/Wassertemperatur/station/16579/Zell%20am%20Moos/Wassertemperatur?period=P7D",
                "entity_id": "irrsee_zell",
                # scan_interval omitted -> default 1800
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

    # Two stations payload
    payload = (
        "#SANR16579|*|SNAMEZell am Moos|*|SWATERIrrsee|*|CNRWT|*|CNAMEWassertemperatur\n"
        "#TZUTC+1\n#LAYOUT(timestamp,value)|*|20250808140000 22.4\n"
        "#SANR12345|*|SNAMEAttersee|*|SWATERAttersee|*|CNRWT|*|CNAMEWassertemperatur\n"
        "#TZUTC+1\n#LAYOUT(timestamp,value)|*|20250808140500 23.7\n"
    )

    added = _EntityList()

    with aioresponses() as mocked:
        mocked.get(ZRXP_URL, status=200, body=payload, headers={"Content-Type": "text/plain; charset=utf-8"})
        await async_setup_platform(hass={}, config={}, async_add_entities=added, discovery_info=discovery_info)

        assert len(added.entities) == 2
        s0 = added.entities[0]
        await s0.coordinator.async_refresh()

        # Min should be 120s (override lower than default 1800)
        assert s0.coordinator.update_interval == timedelta(seconds=120)

        # Cleanup: ensure dataset session is closed
        if getattr(s0, "_dataset_manager", None) is not None:  # type: ignore[attr-defined]
            await s0._dataset_manager.async_close()  # type: ignore[attr-defined]


