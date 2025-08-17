from __future__ import annotations

"""Shared polling tests for Salzburg OGD using mocked HTTP.

- Two lakes from the same dataset share one coordinator and one HTTP GET per refresh
- The dataset coordinator update_interval is the minimum scan_interval across members
- Unregistering a lake recomputes the interval to the remaining lake's scan_interval
"""

from datetime import timedelta
from typing import List

import pytest
from aioresponses import aioresponses

from custom_components.bgl_ts_sbg_laketemp.sensor import async_setup_platform
from custom_components.bgl_ts_sbg_laketemp.const import CONF_LAKES


OGD_URL = "https://www.salzburg.gv.at/ogd/56c28e2d-8b9e-41ba-b7d6-fa4896b5b48b/Hydrografie%20Seen.txt"


class _EntityList:
    def __init__(self) -> None:
        self.entities: List[object] = []

    def __call__(self, entities):  # type: ignore[no-untyped-def]
        if isinstance(entities, list):
            self.entities.extend(entities)
        else:
            self.entities.append(entities)


@pytest.mark.asyncio
async def test_salzburg_ogd_shared_polling_min_interval_and_single_get() -> None:  # type: ignore[no-untyped-def]
    # Title: Two Salzburg OGD lakes share polling — Expect: single GET, shared coordinator, min interval

    discovery_info = {
        CONF_LAKES: [
            {
                "name": "Fuschlsee",
                "url": OGD_URL,
                "entity_id": "fuschlsee",
                "scan_interval": 1800,
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

        # Must have created two entities
        assert len(added.entities) == 2
        s0, s1 = added.entities

        # Trigger a single refresh via the first sensor's shared coordinator
        await s0.coordinator.async_refresh()

        # Both sensors should be available and reflect values from the same refresh
        assert s0.available is True and s1.available is True
        assert s0.native_value == 22.4
        assert s1.native_value == 23.1

        # Both sensors must share the same coordinator
        assert s0.coordinator is s1.coordinator

        # Update interval should be min(scan_interval) = 90s
        assert s0.coordinator.update_interval == timedelta(seconds=90)

        # Only one HTTP GET should have occurred for the dataset
        from yarl import URL
        key = ("GET", URL(OGD_URL))
        assert len(mocked.requests.get(key, [])) == 1

        # Unregister one lake and ensure interval recomputes to remaining sensor's value (1800s)
        await s1.async_will_remove_from_hass()
        assert s0.coordinator.update_interval == timedelta(seconds=1800)

        # Cleanup: ensure dataset coordinator session is closed
        if getattr(s0, "_dataset_manager", None) is not None:  # type: ignore[attr-defined]
            await s0._dataset_manager.async_close()  # type: ignore[attr-defined]


