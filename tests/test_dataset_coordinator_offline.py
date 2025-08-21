from __future__ import annotations

"""Offline tests for dataset coordinators (scheduling, sessions, lookup keys).

Covers:
- Min-based update_interval recomputation on register/unregister
- Backoff override precedence and restoration
- Salzburg OGD shared session UA stickiness and close scheduling
- Hydro OOE stable lookup keys (SANR vs. name) and unregister cleanup
"""

import asyncio
from datetime import timedelta

import pytest

from custom_components.bgl_ts_sbg_laketemp.const import (
    DEFAULT_SCAN_INTERVAL_SECONDS,
    DEFAULT_USER_AGENT,
    LakeConfig,
    LakeSourceType,
    SourceConfig,
)
from custom_components.bgl_ts_sbg_laketemp.dataset_coordinators import (
    BaseDatasetCoordinator,
    SalzburgOGDDatasetCoordinator,
    HydroOoeDatasetCoordinator,
)


class _DummyCoordinator(BaseDatasetCoordinator):
    async def async_update_data(self):  # type: ignore[override]
        return {}


def _lake_cfg(
    *,
    name: str,
    entity_id: str,
    scan_seconds: int,
    source: LakeSourceType,
    user_agent: str | None = None,
    url: str | None = None,
) -> LakeConfig:
    return LakeConfig(
        name=name,
        url=url,
        entity_id=entity_id,
        scan_interval=scan_seconds,
        timeout_hours=24,
        user_agent=user_agent or DEFAULT_USER_AGENT,
        source=SourceConfig(type=source, options=None),
    )


@pytest.mark.asyncio
async def test_first_registration_sets_update_interval_to_scan_interval():  # noqa: D401
    # Title: First registration — Expect: update_interval equals lake scan_interval
    hass: dict = {}
    c = _DummyCoordinator(hass, "dummy")

    cfg = _lake_cfg(
        name="Lake A",
        entity_id="lake_a",
        scan_seconds=5,
        source=LakeSourceType.SALZBURG_OGD,
    )

    coord, _ = c.register_lake(cfg)
    assert int(coord.update_interval.total_seconds()) == 5


@pytest.mark.asyncio
async def test_second_registration_with_lower_scan_recomputes_min():  # noqa: D401
    # Title: Second registration lower — Expect: update_interval recomputes to min
    hass: dict = {}
    c = _DummyCoordinator(hass, "dummy")

    c.register_lake(_lake_cfg(name="Lake A", entity_id="lake_a", scan_seconds=5, source=LakeSourceType.SALZBURG_OGD))
    coord, _ = c.register_lake(_lake_cfg(name="Lake B", entity_id="lake_b", scan_seconds=3, source=LakeSourceType.SALZBURG_OGD))

    assert int(coord.update_interval.total_seconds()) == 3


@pytest.mark.asyncio
async def test_unregistration_recomputes_min_and_default_when_empty():  # noqa: D401
    # Title: Unregister min lake — Expect: update_interval recomputes to new min, then default when empty
    hass: dict = {}
    c = _DummyCoordinator(hass, "dummy")

    c.register_lake(_lake_cfg(name="Lake A", entity_id="lake_a", scan_seconds=5, source=LakeSourceType.SALZBURG_OGD))
    coord, _ = c.register_lake(_lake_cfg(name="Lake B", entity_id="lake_b", scan_seconds=3, source=LakeSourceType.SALZBURG_OGD))

    # Initially min=3
    assert int(coord.update_interval.total_seconds()) == 3

    # Remove the min lake => min=5
    c.unregister_lake("lake_b")
    assert int(coord.update_interval.total_seconds()) == 5

    # Remove the last lake => default
    c.unregister_lake("lake_a")
    assert int(coord.update_interval.total_seconds()) == DEFAULT_SCAN_INTERVAL_SECONDS


@pytest.mark.asyncio
async def test_backoff_override_precedence_and_restoration():  # noqa: D401
    # Title: Backoff override — Expect: override takes precedence; clearing restores min behavior
    hass: dict = {}
    c = _DummyCoordinator(hass, "dummy")

    c.register_lake(_lake_cfg(name="Lake A", entity_id="lake_a", scan_seconds=5, source=LakeSourceType.SALZBURG_OGD))
    c.register_lake(_lake_cfg(name="Lake B", entity_id="lake_b", scan_seconds=3, source=LakeSourceType.SALZBURG_OGD))

    # Without override -> min=3
    c.recompute_update_interval()
    assert int(c.coordinator.update_interval.total_seconds()) == 3

    # Apply override manually
    c._backoff_override_seconds = 10  # type: ignore[attr-defined]
    c.recompute_update_interval()
    assert int(c.coordinator.update_interval.total_seconds()) == 10

    # Clear override -> back to min
    c._backoff_override_seconds = None  # type: ignore[attr-defined]
    c.recompute_update_interval()
    assert int(c.coordinator.update_interval.total_seconds()) == 3


@pytest.mark.asyncio
async def test_salzburg_session_ua_and_close_on_last_unregister():  # noqa: D401
    # Title: Salzburg session/UA — Expect: first UA sticks, later regs don't change UA; last unregister closes
    # Use a hass stub exposing a loop so unregister schedules async_close
    loop = asyncio.get_event_loop()
    class _Hass:
        pass
    hass = _Hass()
    hass.data = {}
    hass.loop = loop
    c = SalzburgOGDDatasetCoordinator(hass, dataset_id="salzburg_ogd_seen")

    ua1 = "UA-One/1.0"
    ua2 = "UA-Two/2.0"

    cfg1 = _lake_cfg(
        name="Irrsee",
        entity_id="irrsee",
        scan_seconds=5,
        source=LakeSourceType.SALZBURG_OGD,
        user_agent=ua1,
        url=None,
    )
    cfg2 = _lake_cfg(
        name="Wolfgangsee",
        entity_id="wolfgangsee",
        scan_seconds=5,
        source=LakeSourceType.SALZBURG_OGD,
        user_agent=ua2,
        url=None,
    )

    # First registration creates session with UA1
    c.register_lake(cfg1)
    assert c._session is not None  # type: ignore[attr-defined]
    assert c._ua == ua1  # type: ignore[attr-defined]
    assert c._session.headers.get("User-Agent") == ua1  # type: ignore[attr-defined]

    sess_ref = c._session  # type: ignore[attr-defined]

    # Second registration must not change UA or replace session
    c.register_lake(cfg2)
    assert c._session is sess_ref  # type: ignore[attr-defined]
    assert c._session.headers.get("User-Agent") == ua1  # type: ignore[attr-defined]

    # Unregister both; last unregister should schedule async_close
    c.unregister_lake("irrsee")
    c.unregister_lake("wolfgangsee")

    # Give the loop a few ticks for the scheduled close task (be generous for CI)
    for _ in range(5):
        if getattr(sess_ref, "closed", False) is True or getattr(c, "_session", None) is None:
            break
        await asyncio.sleep(0)

    # Session reference should be closed
    assert getattr(sess_ref, "closed", False) is True


@pytest.mark.asyncio
async def test_hydro_ooe_lookup_key_and_unregister_cleanup():  # noqa: D401
    # Title: Hydro OOE lookup — Expect: prefer numeric SANR else name; unregister cleans maps
    hass: dict = {}
    c = HydroOoeDatasetCoordinator(hass)

    # Lake with numeric station_id => key is SANR
    cfg1 = _lake_cfg(
        name="Zell am Moos",
        entity_id="irrsee_zell",
        scan_seconds=5,
        source=LakeSourceType.HYDRO_OOE,
        url=None,
    )
    # Inject options by creating a new config with same fields but station_id numeric via SourceConfig at register time
    from custom_components.bgl_ts_sbg_laketemp.const import HydroOOEOptions

    cfg1 = LakeConfig(
        name=cfg1.name,
        url=cfg1.url,
        entity_id=cfg1.entity_id,
        scan_interval=cfg1.scan_interval,
        timeout_hours=cfg1.timeout_hours,
        user_agent=cfg1.user_agent,
        source=SourceConfig(type=LakeSourceType.HYDRO_OOE, options=HydroOOEOptions(station_id="16579")),
    )

    # Lake without station_id => key falls back to normalized name
    cfg2 = _lake_cfg(
        name="Attersee",
        entity_id="attersee",
        scan_seconds=3,
        source=LakeSourceType.HYDRO_OOE,
        url=None,
    )

    coord, key1 = c.register_lake(cfg1)
    assert key1 == "16579"

    coord, key2 = c.register_lake(cfg2)
    # Normalized: lowercase alnum only
    assert key2 == "attersee"

    # Unregister both -> maps emptied
    c.unregister_lake(cfg1.entity_id)
    c.unregister_lake(cfg2.entity_id)

    assert c._sanr_by_entity_id == {}  # type: ignore[attr-defined]
    assert c._name_hint_by_entity_id == {}  # type: ignore[attr-defined]
    assert c._last_sanr_by_entity_id == {}  # type: ignore[attr-defined]


