from __future__ import annotations

"""Offline test for custom scan_interval propagation.

Title: Non-default scan_interval is applied — Expect: coordinator.update_interval == timedelta(seconds=custom)
"""

import logging
from datetime import timedelta
from typing import List

import pytest
from aioresponses import aioresponses

from custom_components.bgl_ts_sbg_laketemp.sensor import async_setup_platform
from custom_components.bgl_ts_sbg_laketemp.const import CONF_LAKES


GKD_URL = "https://www.gkd.bayern.de/de/seen/wassertemperatur/inn/seethal-18673955/messwerte"
GKD_HTML = (
    """
    <html><body>
      <table>
        <thead><tr><th>Datum</th><th>Wassertemperatur [°C]</th></tr></thead>
        <tbody>
          <tr><td>08.08.2025 15:00</td><td>22,8</td></tr>
          <tr><td>08.08.2025 16:00</td><td>23,1</td></tr>
        </tbody>
      </table>
    </body></html>
    """
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
async def test_custom_scan_interval_sets_coordinator_interval(caplog) -> None:  # type: ignore[no-untyped-def]
    # Title: Non-default scan_interval — Expect: coordinator.update_interval equals configured seconds
    caplog.set_level(logging.DEBUG)
    custom_scan_seconds = 90

    discovery_info = {
        CONF_LAKES: [
            {
                "name": "Seethal / Abtsdorfer See",
                "url": GKD_URL,
                "entity_id": "seethal_abtsdorfer",
                "scan_interval": custom_scan_seconds,
                "timeout_hours": 336,  # avoid staleness check in tests (max allowed)
                "source": {"type": "gkd_bayern", "options": {}},
            }
        ]
    }

    added = _EntityList()

    with aioresponses() as mocked:
        mocked.get(
            GKD_URL,
            status=200,
            body=GKD_HTML,
            headers={"Content-Type": "text/html; charset=utf-8"},
        )
        await async_setup_platform(
            hass={},
            config={},
            async_add_entities=added,
            discovery_info=discovery_info,
        )

    assert len(added.entities) == 1
    sensor = added.entities[0]

    assert sensor.coordinator.update_interval == timedelta(seconds=custom_scan_seconds)

    # Ensure we close client sessions to avoid resource warnings
    await sensor.async_will_remove_from_hass()
    await sensor.async_will_remove_from_hass()


