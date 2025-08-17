from __future__ import annotations

"""Availability and staleness behavior for aggregated datasets (offline).

- If dataset refresh succeeds but a registered lake is missing in the payload,
  that lake's sensor should be unavailable (available=False) and state None.
- When the lake appears again in a subsequent refresh, the sensor becomes available.
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
async def test_aggregated_availability_missing_then_present() -> None:  # type: ignore[no-untyped-def]
    # Title: Missing lake becomes unavailable then available again

    discovery_info = {
        CONF_LAKES: [
            {
                "name": "Fuschlsee",
                "url": OGD_URL,
                "entity_id": "fuschlsee",
                "scan_interval": 300,
                "timeout_hours": 336,
                "source": {"type": "salzburg_ogd", "options": {"lake_name": "Fuschlsee"}},
            },
            {
                "name": "Mattsee",
                "url": OGD_URL,
                "entity_id": "mattsee",
                "scan_interval": 300,
                "timeout_hours": 336,
                "source": {"type": "salzburg_ogd", "options": {"lake_name": "Mattsee"}},
            },
        ]
    }

    added = _EntityList()

    with aioresponses() as mocked:
        # First payload: only Mattsee present
        payload1 = (
            "Gewässer;Messdatum;Uhrzeit;Wassertemperatur [°C];Station\n"
            "Mattsee;2025-08-08;14:05;23,1;Nord\n"
        )
        mocked.get(OGD_URL, status=200, body=payload1, headers={"Content-Type": "text/plain; charset=utf-8"})

        await async_setup_platform(hass={}, config={}, async_add_entities=added, discovery_info=discovery_info)

        assert len(added.entities) == 2
        fuschl, matt = added.entities

        # Single refresh
        await fuschl.coordinator.async_refresh()
        assert fuschl.available is False
        assert fuschl.native_value is None
        assert matt.available is True
        assert matt.native_value == 23.1

        # Cleanup: ensure dataset session is closed
        if getattr(fuschl, "_dataset_manager", None) is not None:  # type: ignore[attr-defined]
            await fuschl._dataset_manager.async_close()  # type: ignore[attr-defined]

        # Second payload: both present now
        payload2 = (
            "Gewässer;Messdatum;Uhrzeit;Wassertemperatur [°C];Station\n"
            "Fuschlsee;2025-08-08;14:00;22,4;Westufer\n"
            "Mattsee;2025-08-08;14:05;23,1;Nord\n"
        )
        mocked.get(OGD_URL, status=200, body=payload2, headers={"Content-Type": "text/plain; charset=utf-8"})

        await fuschl.coordinator.async_refresh()
        assert fuschl.available is True
        assert fuschl.native_value == 22.4
        assert matt.available is True
        assert matt.native_value == 23.1


