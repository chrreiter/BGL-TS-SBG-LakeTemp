from __future__ import annotations

"""Offline tests for the Home Assistant sensor platform integration.

Scenarios:
- Valid YAML discovery creates sensors; initial refresh succeeds
- Invalid lake definitions are logged and skipped
- Update failure surfaces as unavailable and logs an error
"""

import logging
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
async def test_sensor_created_from_valid_discovery_and_refreshes(caplog) -> None:  # type: ignore[no-untyped-def]
    # Title: Valid discovery yields one sensor — Expect: entity created and initial refresh OK
    caplog.set_level(logging.DEBUG)
    discovery_info = {
        CONF_LAKES: [
            {
                "name": "Seethal / Abtsdorfer See",
                "url": GKD_URL,
                "entity_id": "seethal_abtsdorfer",
                "timeout_hours": 336,  # avoid staleness check in tests (max allowed)
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
    # State should be 23.1 (not None) after first refresh
    assert sensor.native_value == 23.1
    assert sensor.available is True
    # Ensure we close client sessions to avoid resource warnings
    await sensor.async_will_remove_from_hass()
    await sensor.async_will_remove_from_hass()


@pytest.mark.asyncio
async def test_invalid_lake_is_skipped_and_logged(caplog) -> None:  # type: ignore[no-untyped-def]
    # Title: Invalid lake missing url — Expect: logged error and no entities created
    caplog.set_level(logging.DEBUG)
    discovery_info = {
        CONF_LAKES: [
            {
                "name": "Bad Lake",
                # missing url
                "entity_id": "bad_lake",
                "source": {"type": "gkd_bayern", "options": {}},
            }
        ]
    }

    added = _EntityList()
    await async_setup_platform(hass={}, config={}, async_add_entities=added, discovery_info=discovery_info)

    assert len(added.entities) == 0
    # Look for our validation error message
    assert any("Invalid lake configuration" in rec.getMessage() for rec in caplog.records)


@pytest.mark.asyncio
async def test_update_failure_sets_unavailable_and_logs(caplog) -> None:  # type: ignore[no-untyped-def]
    # Title: HTTP 404 on initial fetch — Expect: last_update_success False and error log
    caplog.set_level(logging.DEBUG)
    discovery_info = {
        CONF_LAKES: [
            {
                "name": "Seethal / Abtsdorfer See",
                "url": GKD_URL,
                "entity_id": "seethal_abtsdorfer",
                "source": {"type": "gkd_bayern", "options": {}},
            }
        ]
    }

    added = _EntityList()

    with aioresponses() as mocked:
        mocked.get(GKD_URL, status=404)
        await async_setup_platform(hass={}, config={}, async_add_entities=added, discovery_info=discovery_info)

    assert len(added.entities) == 1
    sensor = added.entities[0]
    assert sensor.available is False
    # Check coordinator logged refresh failure
    assert any("refresh failed" in rec.getMessage().lower() for rec in caplog.records)
    # Ensure cleanup closes aiohttp sessions
    await sensor.async_will_remove_from_hass()
    await sensor.async_will_remove_from_hass()


