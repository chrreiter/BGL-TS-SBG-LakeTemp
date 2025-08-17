from __future__ import annotations

"""Unified test runner and live data fetcher for BGL-TS-SBG-LakeTemp.

This script provides a single entry point to:

1) Execute the project's full pytest suite (offline and online tests)
2) Fetch and print the latest live data from each implemented data source

Usage (Windows PowerShell):
  .\\.venv\\Scripts\\python .\\run_tests_and_fetch_online.py

Notes:
- The script sets RUN_ONLINE=1 for the pytest subprocess so that online tests
  are executed as part of the run.
- To minimize interference from globally installed pytest plugins, the script
  also sets PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 for the pytest subprocess.
"""

import asyncio
import json
import os
import subprocess
import sys
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
from pathlib import Path


# Default user-agent used by scrapers when session is shared by this script.
DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"
)


@dataclass
class TestRunResult:
    """Represents the outcome of the pytest execution.

    Attributes:
        return_code: Pytest process exit code.
        command: The exact command used to invoke pytest.
    """

    return_code: int
    command: List[str]


@dataclass
class ProviderLiveReading:
    """A normalized live reading value for a provider."""

    provider: str
    timestamp_iso: Optional[str]
    temperature_c: Optional[float]
    error: Optional[str] = None


def _run_pytest_all() -> TestRunResult:
    """Run the full pytest suite, enabling online tests.

    Returns:
        TestRunResult: Summary of the pytest subprocess execution.
    """

    _ensure_repo_on_sys_path()
    _ensure_test_dependencies_installed()

    python_exe = sys.executable
    # Run full suite; let pytest auto-load installed plugins (e.g., pytest-asyncio)
    cmd = [python_exe, "-m", "pytest", "-q", "tests"]

    env = os.environ.copy()
    # Ensure online tests are included
    env["RUN_ONLINE"] = "1"
    # Isolate from external plugins for stability
    # Do not disable plugin autoload; tests rely on pytest-asyncio plugin

    proc = subprocess.run(cmd, env=env, text=True)
    return TestRunResult(return_code=proc.returncode, command=cmd)


async def _fetch_gkd_bayern_latest(session: aiohttp.ClientSession) -> ProviderLiveReading:
    """Fetch the latest reading from the GKD Bayern scraper using a known test URL."""

    # Keep this URL aligned with tests/test_gkd_bayern_online.py
    _ensure_repo_on_sys_path()
    _install_homeassistant_stubs()
    from custom_components.bgl_ts_sbg_laketemp.scrapers.gkd_bayern import GKDBayernScraper

    url = (
        "https://www.gkd.bayern.de/de/seen/wassertemperatur/inn/"
        "seethal-18673955/messwerte"
    )

    try:
        scraper = GKDBayernScraper(url, session=session)
        latest = await scraper.fetch_latest()
        return ProviderLiveReading(
            provider="gkd_bayern",
            timestamp_iso=latest.timestamp.isoformat(),
            temperature_c=latest.temperature_c,
        )
    except Exception as exc:  # noqa: BLE001 - surfaced in output
        return ProviderLiveReading(
            provider="gkd_bayern", timestamp_iso=None, temperature_c=None, error=str(exc)
        )


async def _fetch_hydro_ooe_latest(session: aiohttp.ClientSession) -> ProviderLiveReading:
    """Fetch the latest reading from the Hydro OOE scraper for Irrsee."""

    # Keep selection aligned with tests/test_hydro_ooe_online.py
    _ensure_repo_on_sys_path()
    _install_homeassistant_stubs()
    from custom_components.bgl_ts_sbg_laketemp.scrapers.hydro_ooe import HydroOOEScraper

    try:
        scraper = HydroOOEScraper(sname_contains="Irrsee", session=session)
        latest = await scraper.fetch_latest()
        return ProviderLiveReading(
            provider="hydro_ooe",
            timestamp_iso=latest.timestamp.isoformat(),
            temperature_c=latest.temperature_c,
        )
    except Exception as exc:  # noqa: BLE001 - surfaced in output
        return ProviderLiveReading(
            provider="hydro_ooe", timestamp_iso=None, temperature_c=None, error=str(exc)
        )


async def _fetch_salzburg_ogd_mattsee_latest(session: aiohttp.ClientSession) -> ProviderLiveReading:
    """Fetch the latest water temperature for Mattsee from Salzburg OGD.

    Uses the robust SalzburgOGDScraper to parse the semicolon text export.
    """

    _ensure_repo_on_sys_path()
    _install_homeassistant_stubs()
    from custom_components.bgl_ts_sbg_laketemp.scrapers.salzburg_ogd import SalzburgOGDScraper, NoDataError

    def _norm(name: str) -> str:
        import unicodedata, re
        base = unicodedata.normalize("NFKD", name)
        base = "".join(ch for ch in base if not unicodedata.combining(ch))
        base = base.lower().strip()
        base = base.replace("zeller see", "zellersee").replace("obertrumer see", "obertrumersee")
        base = re.sub(r"\bsee\b", "", base)
        base = re.sub(r"[^a-z0-9]+", "", base)
        return base

    try:
        scraper = SalzburgOGDScraper(session=session)
        try:
            latest = await scraper.fetch_latest_for_lake("Mattsee")
        except NoDataError:
            # Fallback: scan all and pick the first lake whose normalized key contains 'matt'
            mapping = await scraper.fetch_all_latest()
            target_key = "matt"
            latest = None
            for rec in mapping.values():
                if target_key in _norm(rec.lake_name):
                    latest = rec
                    break
            if latest is None:
                raise NoDataError("No temperature for Mattsee found in Salzburg OGD payload")
        return ProviderLiveReading(
            provider="salzburg_ogd_mattsee",
            timestamp_iso=latest.timestamp.isoformat(),
            temperature_c=latest.temperature_c,
        )
    except Exception as exc:  # noqa: BLE001
        return ProviderLiveReading(
            provider="salzburg_ogd_mattsee", timestamp_iso=None, temperature_c=None, error=str(exc)
        )


async def _fetch_all_live_readings() -> List[ProviderLiveReading]:
    """Fetch latest readings for all implemented providers in parallel."""

    timeout = aiohttp.ClientTimeout(total=20)
    headers = {"User-Agent": DEFAULT_UA}
    async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
        results = await asyncio.gather(
            _fetch_gkd_bayern_latest(session),
            _fetch_hydro_ooe_latest(session),
            _fetch_salzburg_ogd_mattsee_latest(session),
            return_exceptions=False,
        )
    return list(results)


def _ensure_repo_on_sys_path() -> None:
    """Ensure repository root is on sys.path for absolute package imports."""

    root = Path(__file__).resolve().parent
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))


def _install_homeassistant_stubs() -> None:
    """Install minimal Home Assistant stubs to satisfy integration imports.

    This mirrors the lightweight stubs used in tests so we can import
    `custom_components.bgl_ts_sbg_laketemp` without having Home Assistant
    installed for the purpose of direct scraper usage.
    """

    if "homeassistant" in sys.modules:
        # Ensure helpers stub exists even if root was pre-injected
        if "homeassistant.helpers.update_coordinator" in sys.modules:
            return

    import types

    # Root package stub (mark as package by setting __path__)
    ha_pkg = sys.modules.get("homeassistant")
    if ha_pkg is None:
        ha_pkg = types.ModuleType("homeassistant")
        ha_pkg.__path__ = []  # mark as namespace/package
        sys.modules["homeassistant"] = ha_pkg
    else:
        setattr(ha_pkg, "__path__", getattr(ha_pkg, "__path__", []))

    # homeassistant.const
    if "homeassistant.const" not in sys.modules:
        ha_const = types.ModuleType("homeassistant.const")
        class Platform(str):  # type: ignore[too-many-ancestors]
            SENSOR = "sensor"
        ha_const.Platform = Platform
        sys.modules["homeassistant.const"] = ha_const

    # homeassistant.core
    if "homeassistant.core" not in sys.modules:
        ha_core = types.ModuleType("homeassistant.core")
        class HomeAssistant(dict):
            pass
        ha_core.HomeAssistant = HomeAssistant
        sys.modules["homeassistant.core"] = ha_core

    # homeassistant.helpers and homeassistant.helpers.update_coordinator
    if "homeassistant.helpers" not in sys.modules:
        ha_helpers = types.ModuleType("homeassistant.helpers")
        sys.modules["homeassistant.helpers"] = ha_helpers

    if "homeassistant.helpers.update_coordinator" not in sys.modules:
        ha_helpers_uc = types.ModuleType("homeassistant.helpers.update_coordinator")

        # Minimal classes used by dataset_coordinators
        import logging as _logging
        from datetime import timedelta as _timedelta
        from typing import Any as _Any, Dict as _Dict, Callable as _Callable, Generic as _Generic, TypeVar as _TypeVar

        _T = _TypeVar("_T")

        class UpdateFailed(Exception):
            pass

        class DataUpdateCoordinator(_Generic[_T]):
            def __init__(self, hass: _Any, logger: _logging.Logger | None, *, name: str, update_method: _Callable[[], _Any], update_interval: _timedelta):
                self.hass = hass
                self.logger = logger or _logging.getLogger(__name__)
                self.name = name
                self.update_method = update_method
                self.update_interval = update_interval
                self.data: _Any = None
                self.last_update_success: bool = False

            async def async_refresh(self) -> None:
                try:
                    self.data = await self.update_method()
                    self.last_update_success = True
                except Exception as exc:  # noqa: BLE001
                    self.last_update_success = False
                    self.data = None
                    self.logger.error("Coordinator '%s' refresh failed: %s", self.name, exc)

            @classmethod
            def __class_getitem__(cls, item):  # type: ignore[no-untyped-def]
                return cls

        ha_helpers_uc.DataUpdateCoordinator = DataUpdateCoordinator
        ha_helpers_uc.UpdateFailed = UpdateFailed
        sys.modules["homeassistant.helpers.update_coordinator"] = ha_helpers_uc


def _ensure_test_dependencies_installed() -> None:
    """Install test/runtime deps into the current interpreter if missing.

    This checks for the imports used by tests and scrapers and installs
    the corresponding PyPI packages only if the import fails. It uses
    the active interpreter (ideally from .venv).
    """

    checks: List[Tuple[str, str]] = [
        ("pytest", "pytest"),
        ("pytest_asyncio", "pytest-asyncio"),
        ("aioresponses", "aioresponses"),
        ("tzdata", "tzdata"),
        ("bs4", "beautifulsoup4==4.12.3"),
        ("aiohttp", "aiohttp>=3.9.1"),
    ]

    missing: List[str] = []
    for import_name, pkg in checks:
        try:
            __import__(import_name)
        except Exception:
            missing.append(pkg)

    if not missing:
        return

    cmd = [sys.executable, "-m", "pip", "install", "-U", *missing]
    print(f"Installing test dependencies: {' '.join(missing)}")
    subprocess.run(cmd, check=False)


def main() -> int:
    """Entry point: run tests, then fetch and print live readings.

    Returns:
        int: Exit code, matching the pytest exit code to preserve CI semantics.
    """

    print("Running tests (including online tests)...", flush=True)
    test_result = _run_pytest_all()
    print(
        f"pytest exited with code {test_result.return_code} using command: {' '.join(test_result.command)}",
        flush=True,
    )

    print("\nFetching latest live data from providers...", flush=True)
    live_results = asyncio.run(_fetch_all_live_readings())

    # Human-friendly summary
    for res in live_results:
        if res.error:
            print(f"- {res.provider}: ERROR - {res.error}", flush=True)
        else:
            print(
                f"- {res.provider}: {res.temperature_c:.2f} °C at {res.timestamp_iso}",
                flush=True,
            )

    # Machine-readable JSON for consumers/CI
    payload: Dict[str, Any] = {
        "tests": {
            "return_code": test_result.return_code,
            "command": test_result.command,
        },
        "live_data": [asdict(r) for r in live_results],
    }
    print("\nJSON:")
    print(json.dumps(payload, indent=2, ensure_ascii=False))

    return test_result.return_code


if __name__ == "__main__":
    raise SystemExit(main())


