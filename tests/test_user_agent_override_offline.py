from __future__ import annotations

"""Offline test to verify custom User-Agent override is applied.

Title: Custom user_agent in config → Expect: sensor session uses that header
"""

import logging

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
        self.entities: list[object] = []

    def __call__(self, entities):  # type: ignore[no-untyped-def]
        if isinstance(entities, list):
            self.entities.extend(entities)
        else:
            self.entities.append(entities)


@pytest.mark.asyncio
async def test_custom_user_agent_is_used_in_session_headers(caplog) -> None:  # type: ignore[no-untyped-def]
    # Title: Custom UA override — Expect: session.headers['User-Agent'] equals custom value
    caplog.set_level(logging.DEBUG)

    custom_ua = "MyCustomUA/9.9 LAKETEMP-TEST"
    discovery_info = {
        CONF_LAKES: [
            {
                "name": "Seethal / Abtsdorfer See",
                "url": GKD_URL,
                "entity_id": "seethal_abtsdorfer",
                "user_agent": custom_ua,
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
        await async_setup_platform(hass={}, config={}, async_add_entities=added, discovery_info=discovery_info)

    assert len(added.entities) == 1
    sensor = added.entities[0]

    # The sensor manages an external session with default headers.
    session = getattr(sensor, "_session")
    assert session is not None
    # aiohttp exposes default headers via session.headers (CIMultiDict)
    assert session.headers.get("User-Agent") == custom_ua

    # Cleanup: ensure sessions are closed to avoid warnings
    await sensor.async_will_remove_from_hass()
    await sensor.async_will_remove_from_hass()


