from __future__ import annotations

"""Pytest configuration.

- Ensure the repo root is importable so ``custom_components`` resolves
- Enable sockets during tests so Windows' asyncio event loop can create
  its internal socketpair, while network I/O remains mocked by tests
"""

import sys
from pathlib import Path
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
    sys.modules["homeassistant.const"] = ha_const

    ha_core = types.ModuleType("homeassistant.core")

    class HomeAssistant(dict):
        pass

    ha_core.HomeAssistant = HomeAssistant
    sys.modules["homeassistant.core"] = ha_core


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

