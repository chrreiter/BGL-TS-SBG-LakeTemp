from __future__ import annotations

"""Offline test: SalzburgOGD omissions retain values and honor timeout_hours.

Title: OGD omission -> retain until timeout; reappear updates; timeout logs
Expect:
  1) Initial payload with 4 lakes -> all sensors created/updated
  2) Next payload omits two lakes -> no warnings; those two retain last values
     while others update
  3) One omitted lake reappears before timeout -> no warnings; updates to new value
  4) The other omitted lake exceeds timeout_hours -> becomes unavailable; a transition
     WARNING is logged from the base dataset availability tracker
"""

import logging
from datetime import datetime, timedelta, timezone
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


def _ts_hours_ago(hours: int) -> str:
    # Helper to generate a Vienna-like ISO timestamp; parser accepts ISO with tz
    t = datetime.now(timezone.utc) - timedelta(hours=hours)
    # Use ISO with timezone offset +00:00; scraper will accept and keep tz
    return t.isoformat()


@pytest.mark.asyncio
async def test_salzburg_ogd_omission_retention_and_timeout(caplog) -> None:  # type: ignore[no-untyped-def]
    # Title: Omission retains last values and respects timeout_hours
    caplog.set_level(logging.DEBUG)

    # Lakes A-D; B and C will be omitted in step 2
    name_a = "Fuschlsee"
    name_b = "Mattsee"
    name_c = "Grabensee"
    name_d = "Wallersee"

    # Configure short timeout for faster test: 2 hours
    timeout_hours = 2

    discovery_info = {
        CONF_LAKES: [
            {"name": name_a, "entity_id": "lake_a", "timeout_hours": timeout_hours, "source": {"type": "salzburg_ogd", "options": {"lake_name": name_a}}},
            {"name": name_b, "entity_id": "lake_b", "timeout_hours": timeout_hours, "source": {"type": "salzburg_ogd", "options": {"lake_name": name_b}}},
            {"name": name_c, "entity_id": "lake_c", "timeout_hours": timeout_hours, "source": {"type": "salzburg_ogd", "options": {"lake_name": name_c}}},
            {"name": name_d, "entity_id": "lake_d", "timeout_hours": timeout_hours, "source": {"type": "salzburg_ogd", "options": {"lake_name": name_d}}},
        ]
    }

    added = _EntityList()

    # 1) Initial payload with all four lakes present, recent timestamps
    # Expect: all sensors available with their values
    payload_step1 = (
        "Gewässer;Zeitstempel;Wassertemperatur [°C]\n"
        f"{name_a};{_ts_hours_ago(1)};22.4\n"
        f"{name_b};{_ts_hours_ago(1)};23.1\n"
        f"{name_c};{_ts_hours_ago(1)};21.9\n"
        f"{name_d};{_ts_hours_ago(1)};20.3\n"
    )

    with aioresponses() as mocked:
        mocked.get(OGD_URL, status=200, body=payload_step1, headers={"Content-Type": "text/plain; charset=utf-8"})
        await async_setup_platform(hass={}, config={}, async_add_entities=added, discovery_info=discovery_info)

    assert len(added.entities) == 4
    s_a, s_b, s_c, s_d = added.entities

    # Trigger a refresh to ensure coordinator state is initialized
    with aioresponses() as mocked:
        mocked.get(OGD_URL, status=200, body=payload_step1, headers={"Content-Type": "text/plain; charset=utf-8"})
        await s_a.coordinator.async_refresh()

    assert s_a.available is True and s_b.available is True and s_c.available is True and s_d.available is True

    v_a_1 = s_a.native_value
    v_b_1 = s_b.native_value
    v_c_1 = s_c.native_value
    v_d_1 = s_d.native_value

    # 2) Next payload omits B and C; A and D update to new values
    # Expect: no warnings for missing lakes (we carry forward), A and D update, B/C retain prior values
    payload_step2 = (
        "Gewässer;Zeitstempel;Wassertemperatur [°C]\n"
        f"{name_a};{_ts_hours_ago(0)};22.6\n"
        f"{name_d};{_ts_hours_ago(0)};20.5\n"
    )

    caplog.clear()
    with aioresponses() as mocked:
        mocked.get(OGD_URL, status=200, body=payload_step2, headers={"Content-Type": "text/plain; charset=utf-8"})
        await s_a.coordinator.async_refresh()

    # No warning logs about missing lakes with prior readings
    warnings = [rec.getMessage() for rec in caplog.records if rec.levelno >= logging.WARNING and "SalzburgOGD dataset" in rec.getMessage()]
    assert not warnings

    # A and D updated, B and C retained
    assert s_a.native_value != v_a_1 and s_d.native_value != v_d_1
    assert s_b.native_value == v_b_1 and s_c.native_value == v_c_1

    # 3) B reappears before timeout with new value
    # Expect: no warnings; B updates to new value; A and D can change again; C remains retained
    payload_step3 = (
        "Gewässer;Zeitstempel;Wassertemperatur [°C]\n"
        f"{name_a};{_ts_hours_ago(0)};22.7\n"
        f"{name_b};{_ts_hours_ago(0)};23.4\n"
        f"{name_d};{_ts_hours_ago(0)};20.7\n"
    )

    caplog.clear()
    with aioresponses() as mocked:
        mocked.get(OGD_URL, status=200, body=payload_step3, headers={"Content-Type": "text/plain; charset=utf-8"})
        await s_a.coordinator.async_refresh()

    warnings = [rec.getMessage() for rec in caplog.records if rec.levelno >= logging.WARNING and "SalzburgOGD dataset" in rec.getMessage()]
    assert not warnings

    assert s_b.native_value != v_b_1  # updated
    # C still retained and within timeout -> still available and unchanged
    assert s_c.available is True
    assert s_c.native_value == v_c_1

    # 4) Simulate C exceeding timeout by making its retained reading older than timeout
    # Mutate the coordinator's mapping for C to an old timestamp, then refresh with C omitted again.
    from custom_components.bgl_ts_sbg_laketemp.scrapers.salzburg_ogd import SalzburgOGDScraper
    from custom_components.bgl_ts_sbg_laketemp.data_source import TemperatureReading

    c_key = SalzburgOGDScraper._normalize_lake_key(name_c)
    # Ensure mapping exists for C
    assert isinstance(s_c.coordinator.data, dict) and c_key in s_c.coordinator.data
    s_c.coordinator.data[c_key] = TemperatureReading(
        timestamp=datetime.now(timezone.utc) - timedelta(hours=timeout_hours + 1),
        temperature_c=float(v_c_1),
        source="salzburg_ogd",
    )

    payload_step4 = (
        "Gewässer;Zeitstempel;Wassertemperatur [°C]\n"
        f"{name_a};{_ts_hours_ago(0)};22.8\n"
        f"{name_d};{_ts_hours_ago(0)};20.8\n"
    )

    caplog.clear()
    with aioresponses() as mocked:
        mocked.get(OGD_URL, status=200, body=payload_step4, headers={"Content-Type": "text/plain; charset=utf-8"})
        await s_a.coordinator.async_refresh()

    # After timeout exceeded for C: becomes unavailable
    assert s_c.available is False

    # A debug log from the sensor indicates stale reading
    # Access native_value to trigger the staleness check/log in the sensor property
    _ = s_c.native_value
    stale_logs = [rec.getMessage() for rec in caplog.records if "latest reading is stale" in rec.getMessage()]
    assert stale_logs, "Expected a log entry indicating the reading is stale"


