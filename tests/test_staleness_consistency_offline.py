from __future__ import annotations

"""Offline tests: stale readings surface the same way for every source type.

Title: Stale reading -> state unknown for per-lake and shared-dataset sensors
Expect:
  - A reading older than ``timeout_hours`` yields ``native_value is None`` (state
    ``unknown``) while ``available`` stays ``True`` (the fetch itself succeeded)
  - ``timeout_hours == 336`` (max) disables the check and shows the value
  - A failed fetch still yields ``available is False``
"""

from datetime import datetime, timedelta, timezone
from typing import List

import pytest
from aioresponses import aioresponses

from custom_components.bgl_ts_sbg_laketemp.const import CONF_LAKES
from custom_components.bgl_ts_sbg_laketemp.sensor import async_setup_platform


GKD_URL = "https://www.gkd.bayern.de/de/seen/wassertemperatur/inn/seethal-18673955/messwerte"
OGD_URL = "https://www.salzburg.gv.at/ogd/56c28e2d-8b9e-41ba-b7d6-fa4896b5b48b/Hydrografie%20Seen.txt"


class _EntityList:
    def __init__(self) -> None:
        self.entities: List[object] = []

    def __call__(self, entities):  # type: ignore[no-untyped-def]
        self.entities.extend(entities if isinstance(entities, list) else [entities])


def _gkd_html(ts: datetime) -> str:
    local = ts.strftime("%d.%m.%Y %H:00")
    return (
        "<html><body><table>"
        "<thead><tr><th>Datum</th><th>Wassertemperatur [°C]</th></tr></thead>"
        f"<tbody><tr><td>{local}</td><td>18,4</td></tr></tbody>"
        "</table></body></html>"
    )


def _ogd_payload(ts: datetime) -> str:
    return "Gewässer;Zeitstempel;Wassertemperatur [°C]\n" f"Fuschlsee;{ts.isoformat()};19.2\n"


async def _setup_gkd(timeout_hours: int, html: str | None, status: int = 200):  # type: ignore[no-untyped-def]
    discovery_info = {
        CONF_LAKES: [
            {
                "name": "Seethal",
                "url": GKD_URL,
                "entity_id": "seethal",
                "timeout_hours": timeout_hours,
                "source": {"type": "gkd_bayern", "options": {}},
            }
        ]
    }
    added = _EntityList()
    with aioresponses() as mocked:
        mocked.get(GKD_URL + "/tabelle", status=status, body=html or "", headers={"Content-Type": "text/html; charset=utf-8"})
        await async_setup_platform(hass={}, config={}, async_add_entities=added, discovery_info=discovery_info)
    assert len(added.entities) == 1
    return added.entities[0]


async def _setup_ogd(timeout_hours: int, payload: str):  # type: ignore[no-untyped-def]
    discovery_info = {
        CONF_LAKES: [
            {
                "name": "Fuschlsee",
                "entity_id": "fuschlsee",
                "timeout_hours": timeout_hours,
                "source": {"type": "salzburg_ogd", "options": {"lake_name": "Fuschlsee"}},
            }
        ]
    }
    added = _EntityList()
    await async_setup_platform(hass={}, config={}, async_add_entities=added, discovery_info=discovery_info)
    assert len(added.entities) == 1
    sensor = added.entities[0]
    with aioresponses() as mocked:
        mocked.get(OGD_URL, status=200, body=payload, headers={"Content-Type": "text/plain; charset=utf-8"})
        await sensor.coordinator.async_refresh()
    return sensor


@pytest.mark.asyncio
async def test_stale_gkd_reading_is_unknown_but_available() -> None:
    old = datetime.now(timezone.utc) - timedelta(hours=10)
    sensor = await _setup_gkd(timeout_hours=2, html=_gkd_html(old))
    assert sensor.available is True
    assert sensor.native_value is None


@pytest.mark.asyncio
async def test_stale_shared_dataset_reading_is_unknown_but_available() -> None:
    old = datetime.now(timezone.utc) - timedelta(hours=10)
    sensor = await _setup_ogd(timeout_hours=2, payload=_ogd_payload(old))
    assert sensor.available is True
    assert sensor.native_value is None


@pytest.mark.asyncio
async def test_fresh_shared_dataset_reading_shows_value() -> None:
    sensor = await _setup_ogd(timeout_hours=2, payload=_ogd_payload(datetime.now(timezone.utc)))
    assert sensor.available is True
    assert sensor.native_value == 19.2


@pytest.mark.asyncio
async def test_max_timeout_disables_staleness_for_both_models() -> None:
    old = datetime.now(timezone.utc) - timedelta(days=30)
    gkd = await _setup_gkd(timeout_hours=336, html=_gkd_html(old))
    ogd = await _setup_ogd(timeout_hours=336, payload=_ogd_payload(old))
    assert gkd.native_value == 18.4
    assert ogd.native_value == 19.2


@pytest.mark.asyncio
async def test_fetch_failure_is_unavailable() -> None:
    sensor = await _setup_gkd(timeout_hours=24, html=None, status=500)
    assert sensor.available is False
    assert sensor.native_value is None
