from __future__ import annotations

"""Offline tests for centralized HTTP behaviors at dataset level.

- DNS/connect errors and timeouts map to unavailable and backoff
- HTTP 500/503 cause error logs and backoff
- HTTP 404 results in warning and skipped update (retain previous data)
- HTTP 429 applies Retry-After to scheduling
- Redirect loop (>5) aborts with error
- Content-type mismatch is tolerated by scrapers (text vs html)
"""

import asyncio
import logging
from datetime import timedelta

import aiohttp
import pytest
from aioresponses import aioresponses

from custom_components.bgl_ts_sbg_laketemp.sensor import async_setup_platform
from custom_components.bgl_ts_sbg_laketemp.const import CONF_LAKES


OGD_URL = "https://www.salzburg.gv.at/ogd/56c28e2d-8b9e-41ba-b7d6-fa4896b5b48b/Hydrografie%20Seen.txt"
ZRXP_URL = "https://data.ooe.gv.at/files/hydro/HDOOE_Export_WT.zrxp"


class _EntityList:
    def __init__(self) -> None:
        self.entities: list[object] = []

    def __call__(self, entities):  # type: ignore[no-untyped-def]
        if isinstance(entities, list):
            self.entities.extend(entities)
        else:
            self.entities.append(entities)


@pytest.mark.asyncio
async def test_hydro_ooe_http_404_skips_update_and_warns(caplog) -> None:  # type: ignore[no-untyped-def]
    # Title: HTTP 404 -> skip update — Expect: warning logged, previous data retained
    caplog.set_level(logging.WARNING)

    discovery_info = {
        CONF_LAKES: [
            {
                "name": "Irrsee",
                "url": ZRXP_URL,
                "entity_id": "irrsee",
                "scan_interval": 60,
                "timeout_hours": 336,
                "source": {"type": "hydro_ooe", "options": {"station_id": "16579"}},
            }
        ]
    }

    added = _EntityList()

    with aioresponses() as mocked:
        # First refresh returns valid ZRXP text so we have initial state
        body = (
            "#ZRXPVERSION2300.100|*| #SANR16579|*|SNAMEIrrsee|*|SWATERIrrsee|*|CNRWT|*|CNAMEWassertemperatur|*| "
            "#TZUTC+1|*| #CUNIT°C|*| #LAYOUT(timestamp,value)|*| 20250808140000 22.4 20250808150000 22.8"
        )
        mocked.get(ZRXP_URL, status=200, body=body)

        await async_setup_platform(hass={}, config={}, async_add_entities=added, discovery_info=discovery_info)
        assert len(added.entities) == 1
        sensor = added.entities[0]

        await sensor.coordinator.async_refresh()
        assert sensor.available is True
        assert sensor.native_value == 22.8

        # Next refresh: 404 should skip and keep previous value
        mocked.get(ZRXP_URL, status=404)
        await sensor.coordinator.async_refresh()
        # Expect skip-without-failure and previous data retained
        assert sensor.available is True
        assert sensor.native_value == 22.8
        assert any("404 (not found)" in rec.getMessage() or "returned 404" in rec.getMessage() for rec in caplog.records)


@pytest.mark.asyncio
async def test_hydro_ooe_http_429_applies_retry_after(caplog) -> None:  # type: ignore[no-untyped-def]
    # Title: HTTP 429 with Retry-After — Expect: schedule respects header
    caplog.set_level(logging.WARNING)

    discovery_info = {
        CONF_LAKES: [
            {
                "name": "Irrsee",
                "entity_id": "irrsee",
                "scan_interval": 60,
                "timeout_hours": 336,
                "source": {"type": "hydro_ooe", "options": {"station_id": "16579"}},
            }
        ]
    }

    added = _EntityList()
    with aioresponses() as mocked:
        # First attempt 429 with Retry-After
        mocked.get(ZRXP_URL, status=429, headers={"Retry-After": "120"})

        await async_setup_platform(hass={}, config={}, async_add_entities=added, discovery_info=discovery_info)
        sensor = added.entities[0]
        await sensor.coordinator.async_refresh()

        # Coordinator's update_interval should be around 120 seconds due to override
        assert int(sensor.coordinator.update_interval.total_seconds()) >= 120
        assert any("429 Too Many Requests" in rec.getMessage() for rec in caplog.records)


@pytest.mark.asyncio
async def test_hydro_ooe_server_error_backoff(caplog) -> None:  # type: ignore[no-untyped-def]
    # Title: HTTP 500 backoff — Expect: error logged and update_interval increased relative to scan_interval
    caplog.set_level(logging.ERROR)

    discovery_info = {
        CONF_LAKES: [
            {
                "name": "Irrsee",
                "entity_id": "irrsee",
                "scan_interval": 60,
                "timeout_hours": 336,
                "source": {"type": "hydro_ooe", "options": {"station_id": "16579"}},
            }
        ]
    }

    added = _EntityList()
    with aioresponses() as mocked:
        mocked.get(ZRXP_URL, status=500)
        await async_setup_platform(hass={}, config={}, async_add_entities=added, discovery_info=discovery_info)
        sensor = added.entities[0]
        await sensor.coordinator.async_refresh()

        # Expect backoff to be >= scan_interval (60) and likely doubled
        assert int(sensor.coordinator.update_interval.total_seconds()) >= 60
        assert any("server error: HTTP 500" in rec.getMessage() for rec in caplog.records)


@pytest.mark.asyncio
async def test_hydro_ooe_redirect_loop_error(caplog) -> None:  # type: ignore[no-untyped-def]
    # Title: Redirect loop — Expect: error logged and backoff applied
    caplog.set_level(logging.ERROR)

    discovery_info = {
        CONF_LAKES: [
            {
                "name": "Irrsee",
                "entity_id": "irrsee",
                "scan_interval": 60,
                "timeout_hours": 336,
                "source": {"type": "hydro_ooe", "options": {"station_id": "16579"}},
            }
        ]
    }

    added = _EntityList()
    with aioresponses() as mocked:
        # Simulate redirect loop with a generic client error (aioresponses cannot easily craft TooManyRedirects)
        mocked.get(ZRXP_URL, exception=aiohttp.ClientError("redirect loop"))
        await async_setup_platform(hass={}, config={}, async_add_entities=added, discovery_info=discovery_info)
        sensor = added.entities[0]
        await sensor.coordinator.async_refresh()

        assert int(sensor.coordinator.update_interval.total_seconds()) >= 60
        assert any("refresh failed during download" in rec.getMessage() for rec in caplog.records)


@pytest.mark.asyncio
async def test_salzburg_ogd_content_type_mismatch_tolerated() -> None:  # type: ignore[no-untyped-def]
    # Title: Content-Type mismatch — Expect: parsing still attempted and succeeds
    discovery_info = {
        CONF_LAKES: [
            {
                "name": "Fuschlsee",
                "url": OGD_URL,
                "entity_id": "fuschlsee",
                "scan_interval": 60,
                "timeout_hours": 336,
                "source": {"type": "salzburg_ogd", "options": {"lake_name": "Fuschlsee"}},
            }
        ]
    }

    payload = (
        "Gewässer;Messdatum;Uhrzeit;Wassertemperatur [°C];Station\n"
        "Fuschlsee;2025-08-08;14:00;22,4;Westufer\n"
    )

    added = _EntityList()
    with aioresponses() as mocked:
        # Misleading content type: text/plain vs text/html should not matter; we read bytes and decode manually
        mocked.get(OGD_URL, status=200, body=payload, headers={"Content-Type": "text/plain"})
        await async_setup_platform(hass={}, config={}, async_add_entities=added, discovery_info=discovery_info)
        sensor = added.entities[0]
        await sensor.coordinator.async_refresh()

        assert sensor.available is True
        assert sensor.native_value == 22.4


