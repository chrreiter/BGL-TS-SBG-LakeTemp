from __future__ import annotations

"""Offline tests for rate limiting and session reuse.

Scenarios:
- Session reuse: multiple per-lake sensors share a single ClientSession
- Per-domain rate limiting: concurrent refreshes against same domain are spaced
- Jitter: optional randomized delay is bounded and applied
"""

import asyncio
import logging
import time
import random
from typing import List

import aiohttp
import pytest
from aioresponses import aioresponses

from custom_components.bgl_ts_sbg_laketemp.sensor import async_setup_platform
from custom_components.bgl_ts_sbg_laketemp.const import CONF_LAKES
from custom_components.bgl_ts_sbg_laketemp.dataset_coordinators import (
    get_domain_rate_limiter,
    get_shared_client_session,
    _close_shared_session_on_stop,
)


GKD_URL_1 = "https://www.gkd.bayern.de/de/seen/wassertemperatur/inn/seethal-18673955/messwerte"
GKD_URL_2 = "https://www.gkd.bayern.de/de/seen/wassertemperatur/inn/koenigssee-18624806/messwerte"

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
async def test_session_reused_across_per_lake_sensors(caplog) -> None:  # type: ignore[no-untyped-def]
    # Title: Session reuse — Expect: both per-lake sensors use the same ClientSession instance
    caplog.set_level(logging.DEBUG)
    discovery_info = {
        CONF_LAKES: [
            {
                "name": "Seethal / Abtsdorfer See",
                "url": GKD_URL_1,
                "entity_id": "seethal_abtsdorfer",
                "timeout_hours": 336,
                "source": {"type": "gkd_bayern", "options": {}},
            },
            {
                "name": "Königssee",
                "url": GKD_URL_2,
                "entity_id": "koenigssee",
                "timeout_hours": 336,
                "source": {"type": "gkd_bayern", "options": {}},
            },
        ]
    }

    added = _EntityList()

    with aioresponses() as mocked:
        mocked.get(GKD_URL_1.rstrip("/") + "/tabelle", status=200, body=GKD_HTML)
        mocked.get(GKD_URL_2.rstrip("/") + "/tabelle", status=200, body=GKD_HTML)
        hass: dict = {}
        await async_setup_platform(hass=hass, config={}, async_add_entities=added, discovery_info=discovery_info)

    assert len(added.entities) == 2
    s1, s2 = added.entities
    # Both keep a reference to the shared session
    assert getattr(s1, "_session", None) is not None
    assert getattr(s2, "_session", None) is not None
    assert s1._session is s2._session  # type: ignore[attr-defined]
    assert isinstance(s1._session, aiohttp.ClientSession)  # type: ignore[attr-defined]
    assert s1._session.closed is False  # type: ignore[attr-defined]

    # Removing sensors should not close the shared session
    await s1.async_will_remove_from_hass()
    await s2.async_will_remove_from_hass()
    # Close shared session to avoid resource warnings in tests
    await _close_shared_session_on_stop(hass)
    assert s1._session.closed is True  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_per_domain_rate_limiting_spacings(caplog) -> None:  # type: ignore[no-untyped-def]
    # Title: Per-domain limiter spacing — Expect: second request starts at least min_delay later
    caplog.set_level(logging.DEBUG)
    hass: dict = {}

    # Create limiter with known parameters before sensors are created
    _ = get_domain_rate_limiter(hass, max_concurrent=2, min_delay_seconds=0.2, jitter_seconds=0.0)

    discovery_info = {
        CONF_LAKES: [
            {
                "name": "Seethal / Abtsdorfer See",
                "url": GKD_URL_1,
                "entity_id": "seethal_abtsdorfer",
                "timeout_hours": 336,
                "source": {"type": "gkd_bayern", "options": {}},
            },
            {
                "name": "Königssee",
                "url": GKD_URL_2,
                "entity_id": "koenigssee",
                "timeout_hours": 336,
                "source": {"type": "gkd_bayern", "options": {}},
            },
        ]
    }

    added = _EntityList()
    times: List[float] = []

    from aioresponses import CallbackResult  # type: ignore

    def _record_cb(url, **kwargs):  # type: ignore[no-untyped-def]
        times.append(time.perf_counter())
        return CallbackResult(status=200, body=GKD_HTML, headers={"Content-Type": "text/html"})

    with aioresponses() as mocked:
        mocked.get(GKD_URL_1.rstrip("/") + "/tabelle", callback=_record_cb, repeat=True)
        mocked.get(GKD_URL_2.rstrip("/") + "/tabelle", callback=_record_cb, repeat=True)
        await async_setup_platform(hass=hass, config={}, async_add_entities=added, discovery_info=discovery_info)

        # Clear initial refresh timestamps recorded during setup
        times.clear()

        s1, s2 = added.entities
        await asyncio.gather(s1.coordinator.async_refresh(), s2.coordinator.async_refresh())

    assert len(times) == 2
    dt = abs(times[1] - times[0])
    assert dt >= 0.18  # allow small scheduling jitter tolerance for CI


@pytest.mark.asyncio
async def test_per_domain_rate_limiting_with_jitter(caplog) -> None:  # type: ignore[no-untyped-def]
    # Title: Jitter applied — Expect: second start between [min_delay, min_delay + jitter]
    caplog.set_level(logging.DEBUG)
    hass: dict = {}
    random.seed(42)

    _ = get_domain_rate_limiter(hass, max_concurrent=2, min_delay_seconds=0.2, jitter_seconds=0.1)

    discovery_info = {
        CONF_LAKES: [
            {
                "name": "Seethal / Abtsdorfer See",
                "url": GKD_URL_1,
                "entity_id": "seethal_abtsdorfer",
                "timeout_hours": 336,
                "source": {"type": "gkd_bayern", "options": {}},
            },
            {
                "name": "Königssee",
                "url": GKD_URL_2,
                "entity_id": "koenigssee",
                "timeout_hours": 336,
                "source": {"type": "gkd_bayern", "options": {}},
            },
        ]
    }

    added = _EntityList()
    times: List[float] = []

    from aioresponses import CallbackResult  # type: ignore

    def _record_cb(url, **kwargs):  # type: ignore[no-untyped-def]
        times.append(time.perf_counter())
        return CallbackResult(status=200, body=GKD_HTML, headers={"Content-Type": "text/html"})

    with aioresponses() as mocked:
        mocked.get(GKD_URL_1.rstrip("/") + "/tabelle", callback=_record_cb, repeat=True)
        mocked.get(GKD_URL_2.rstrip("/") + "/tabelle", callback=_record_cb, repeat=True)
        await async_setup_platform(hass=hass, config={}, async_add_entities=added, discovery_info=discovery_info)

        times.clear()
        s1, s2 = added.entities
        await asyncio.gather(s1.coordinator.async_refresh(), s2.coordinator.async_refresh())

    assert len(times) == 2
    dt = abs(times[1] - times[0])
    # Lower bound at min_delay; upper bound at min_delay + jitter + small epsilon
    assert dt >= 0.18
    assert dt <= 0.35


