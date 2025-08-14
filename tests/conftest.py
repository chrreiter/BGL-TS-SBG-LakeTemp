from __future__ import annotations

"""Pytest configuration.

- Ensure the repo root is importable so ``custom_components`` resolves
- Enable sockets during tests so Windows' asyncio event loop can create
  its internal socketpair, while network I/O remains mocked by tests
"""

import sys
from pathlib import Path
import os
import inspect
import os
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


@pytest.fixture(autouse=True)
def _use_selector_event_loop_policy(monkeypatch):  # type: ignore[no-untyped-def]
    """Force SelectorEventLoopPolicy on Windows to avoid proactor self-pipe.

    Home Assistant sets a ProactorEventLoopPolicy which uses socketpair; with
    socket plugins disabled or restricted this can fail. Selector policy avoids
    that path on Windows in tests.
    """
    if os.name == "nt":
        # Ensure we use the standard asyncio policy, not HA's custom runner policy
        import asyncio

        try:
            policy = asyncio.WindowsSelectorEventLoopPolicy()  # type: ignore[attr-defined]
            asyncio.set_event_loop_policy(policy)
        except Exception:
            # On non-Windows, or if attribute not present, ignore
            pass
    yield


# ---- Minimal Home Assistant stubs so importing the integration works without HA installed ----
import types  # noqa: E402

if "homeassistant" not in sys.modules:
    ha_pkg = types.ModuleType("homeassistant")
    sys.modules["homeassistant"] = ha_pkg

    ha_const = types.ModuleType("homeassistant.const")
    # Minimal Platform stub with SENSOR only (enough for our __init__.py)
    class Platform(str):
        SENSOR = "sensor"

    ha_const.Platform = Platform
    # Common constants used by sensor platform
    ha_const.ATTR_ATTRIBUTION = "attribution"
    class UnitOfTemperature:
        CELSIUS = "°C"

    ha_const.UnitOfTemperature = UnitOfTemperature
    sys.modules["homeassistant.const"] = ha_const

    ha_core = types.ModuleType("homeassistant.core")

    class HomeAssistant(dict):
        pass

    ha_core.HomeAssistant = HomeAssistant
    sys.modules["homeassistant.core"] = ha_core

    # Components: sensor
    ha_components = types.ModuleType("homeassistant.components")
    sys.modules["homeassistant.components"] = ha_components

    ha_components_sensor = types.ModuleType("homeassistant.components.sensor")

    class SensorDeviceClass:
        TEMPERATURE = "temperature"

    class SensorStateClass:
        MEASUREMENT = "measurement"

    class SensorEntity:
        # Minimal base to satisfy attribute assignments
        _attr_name = None
        _attr_unique_id = None
        _attr_device_class = None
        _attr_native_unit_of_measurement = None
        _attr_should_poll = False
        _attr_device_info = None
        _attr_state_class = None

        @property
        def name(self):  # noqa: D401 - test stub
            return getattr(self, "_attr_name", None)

        @property
        def unique_id(self):  # noqa: D401 - test stub
            return getattr(self, "_attr_unique_id", None)

    ha_components_sensor.SensorEntity = SensorEntity
    ha_components_sensor.SensorDeviceClass = SensorDeviceClass
    ha_components_sensor.SensorStateClass = SensorStateClass
    sys.modules["homeassistant.components.sensor"] = ha_components_sensor

    # helpers.entity
    ha_helpers = types.ModuleType("homeassistant.helpers")
    sys.modules["homeassistant.helpers"] = ha_helpers

    ha_helpers_entity = types.ModuleType("homeassistant.helpers.entity")

    class DeviceInfo(dict):
        def __init__(self, **kwargs):  # noqa: D401 - test stub
            super().__init__(**kwargs)

    ha_helpers_entity.DeviceInfo = DeviceInfo
    sys.modules["homeassistant.helpers.entity"] = ha_helpers_entity

    # helpers.update_coordinator
    ha_helpers_ucoord = types.ModuleType("homeassistant.helpers.update_coordinator")
    import logging as _logging  # local import inside stub
    from datetime import timedelta as _timedelta

    class UpdateFailed(Exception):
        pass

    class DataUpdateCoordinator:
        def __init__(self, hass, logger, *, name, update_method, update_interval: _timedelta):  # noqa: D401 - test stub
            self.hass = hass
            self.logger = logger or _logging.getLogger(__name__)
            self.name = name
            self.update_method = update_method
            self.update_interval = update_interval
            self.data = None
            self.last_update_success = False

        async def async_config_entry_first_refresh(self):  # noqa: D401 - test stub
            try:
                self.data = await self.update_method()
                self.last_update_success = True
            except Exception as exc:  # noqa: BLE001 - test stub behavior
                self.last_update_success = False
                self.data = None
                # Log update failure to help tests assert logging
                self.logger.error("Coordinator '%s' initial refresh failed: %s", self.name, exc)

        async def async_refresh(self):  # noqa: D401 - test stub
            """Mimic Home Assistant coordinator refresh used in YAML/discovery path."""
            try:
                self.data = await self.update_method()
                self.last_update_success = True
            except Exception as exc:  # noqa: BLE001 - test stub behavior
                self.last_update_success = False
                self.data = None
                self.logger.error("Coordinator '%s' refresh failed: %s", self.name, exc)

        # Allow generic subscripting syntax used by integration (DataUpdateCoordinator[...])
        @classmethod
        def __class_getitem__(cls, item):  # type: ignore[no-untyped-def]
            return cls

    class CoordinatorEntity:
        def __init__(self, coordinator):  # noqa: D401 - test stub
            self.coordinator = coordinator

        @property
        def available(self):  # noqa: D401 - test stub
            return bool(getattr(self.coordinator, "last_update_success", False))

        # Allow generic subscripting syntax used by integration (CoordinatorEntity[...])
        @classmethod
        def __class_getitem__(cls, item):  # type: ignore[no-untyped-def]
            return cls

    ha_helpers_ucoord.DataUpdateCoordinator = DataUpdateCoordinator
    ha_helpers_ucoord.CoordinatorEntity = CoordinatorEntity
    ha_helpers_ucoord.UpdateFailed = UpdateFailed
    sys.modules["homeassistant.helpers.update_coordinator"] = ha_helpers_ucoord


# ---- Minimal async test support without external pytest-asyncio plugin ----
import asyncio  # noqa: E402


def _run_coroutine(func, kwargs):  # type: ignore[no-untyped-def]
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(func(**kwargs))


def pytest_pyfunc_call(pyfuncitem):  # type: ignore[no-untyped-def]
    """Allow async def tests to run without pytest-asyncio.

    If the test function is a coroutine function and no async plugin is active,
    execute it in the current event loop.
    """
    test_func = pyfuncitem.obj
    if inspect.iscoroutinefunction(test_func):
        # Collect fixture-injected arguments
        kwargs = {arg: pyfuncitem.funcargs[arg] for arg in pyfuncitem._fixtureinfo.argnames}
        _run_coroutine(test_func, kwargs)
        return True
    return None

