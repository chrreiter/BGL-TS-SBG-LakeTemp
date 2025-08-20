"""Offline tests for aggregated dataset error recovery (Hydro OOE).

Title: Recovery after missing lake — Expect: lake becomes available again on first subsequent success; INFO log emitted for recovery
"""

import logging
from typing import List

import pytest
from aioresponses import aioresponses

from custom_components.bgl_ts_sbg_laketemp.sensor import async_setup_platform
from custom_components.bgl_ts_sbg_laketemp.const import CONF_LAKES


HYDRO_URL = "https://data.ooe.gv.at/files/hydro/HDOOE_Export_WT.zrxp"


def _zrxp_block(*, sanr: str, name: str, values: list[tuple[str, float]]) -> str:
    header = (
        f"#SANR{sanr}\n"
        "|*|SNAME "
        f"{name}|*|\n"
        "|*|SWATER "
        f"{name}|*|\n"
        "|*|CNRWT|*|\n"
        "#TZUTC+1\n"
        "#LAYOUT(timestamp,value)\n"
        "|*|\n"
    )
    series = "\n".join(f"{ts} {val}" for ts, val in values)
    return header + series + "\n"


class _EntityList:
    def __init__(self) -> None:
        self.entities: List[object] = []

    def __call__(self, entities):  # type: ignore[no-untyped-def]
        if isinstance(entities, list):
            self.entities.extend(entities)
        else:
            self.entities.append(entities)


@pytest.mark.asyncio
async def test_aggregated_recovery_logs_and_state(caplog) -> None:  # type: ignore[no-untyped-def]
    # Title: Recovery on subsequent success — Expect: INFO log for recovery, available True again
    caplog.set_level(logging.DEBUG)

    sanr_a = "12345"
    sanr_b = "67890"
    name_a = "Lake A"
    name_b = "Lake B"

    discovery_info = {
        CONF_LAKES: [
            {
                "name": name_a,
                "entity_id": "lake_a",
                "timeout_hours": 336,
                "source": {"type": "hydro_ooe", "options": {"station_id": sanr_a}},
            },
            {
                "name": name_b,
                "entity_id": "lake_b",
                "timeout_hours": 336,
                "source": {"type": "hydro_ooe", "options": {"station_id": sanr_b}},
            },
        ]
    }

    added = _EntityList()

    # Initial setup: only lake A present, so lake B starts unavailable
    body_only_a = _zrxp_block(sanr=sanr_a, name=name_a, values=[("20250101140000", 5.7)])
    with aioresponses() as mocked:
        mocked.get(HYDRO_URL, status=200, body=body_only_a, headers={"Content-Type": "text/plain; charset=utf-8"})
        await async_setup_platform(hass={}, config={}, async_add_entities=added, discovery_info=discovery_info)

    assert len(added.entities) == 2
    s_a, s_b = added.entities

    # Ensure coordinator has run once so baseline availability is recorded
    with aioresponses() as mocked:
        mocked.get(HYDRO_URL, status=200, body=body_only_a, headers={"Content-Type": "text/plain; charset=utf-8"})
        await s_a.coordinator.async_refresh()

    assert s_a.available is True
    assert s_b.available is False

    # Next refresh includes both lakes; lake B should recover and log INFO
    body_both = (
        _zrxp_block(sanr=sanr_a, name=name_a, values=[("20250101150000", 5.8)])
        + _zrxp_block(sanr=sanr_b, name=name_b, values=[("20250101150000", 7.3)])
    )
    with aioresponses() as mocked:
        mocked.get(HYDRO_URL, status=200, body=body_both, headers={"Content-Type": "text/plain; charset=utf-8"})
        await s_a.coordinator.async_refresh()

    assert s_a.available is True
    assert s_b.available is True

    info_found = any(
        (rec.levelno == logging.INFO and "recovered to available" in rec.getMessage() and "Lake B" in rec.getMessage())
        for rec in caplog.records
    )
    assert info_found, "Expected INFO recovery log for Lake B"

