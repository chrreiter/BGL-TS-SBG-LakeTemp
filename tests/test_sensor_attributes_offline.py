from __future__ import annotations

"""Offline tests asserting sensor extra_state_attributes.

Checks that attributes include ISO timestamp, lake name, source type, URL, and attribution.
"""

import logging
from datetime import datetime
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
async def test_sensor_extra_state_attributes_contains_expected_fields(caplog) -> None:  # type: ignore[no-untyped-def]
    # Title: extra_state_attributes fields — Expect: data_timestamp ISO, lake_name, source_type, url, attribution present
    caplog.set_level(logging.DEBUG)

    discovery_info = {
        CONF_LAKES: [
            {
                "name": "Seethal / Abtsdorfer See",
                "url": GKD_URL,
                "entity_id": "seethal_abtsdorfer",
                "timeout_hours": 336,  # avoid staleness in tests
                "source": {"type": "gkd_bayern", "options": {}},
            }
        ]
    }

    added = _EntityList()

    with aioresponses() as mocked:
        mocked.get(GKD_URL, status=200, body=GKD_HTML, headers={"Content-Type": "text/html; charset=utf-8"})
        await async_setup_platform(hass={}, config={}, async_add_entities=added, discovery_info=discovery_info)

    assert len(added.entities) == 1
    sensor = added.entities[0]

    attrs = sensor.extra_state_attributes
    # Required keys present
    for key in ("data_timestamp", "lake_name", "source_type", "url", "attribution"):
        assert key in attrs

    # Validate values
    assert attrs["lake_name"] == "Seethal / Abtsdorfer See"
    assert attrs["source_type"] == "gkd_bayern"
    assert attrs["url"] == GKD_URL
    assert isinstance(attrs["data_timestamp"], str)

    # ISO-8601 parseable
    _ = datetime.fromisoformat(attrs["data_timestamp"])  # will raise if not ISO

    # Cleanup sessions
    await sensor.async_will_remove_from_hass()
    await sensor.async_will_remove_from_hass()


