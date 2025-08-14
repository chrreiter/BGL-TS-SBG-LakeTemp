from __future__ import annotations

"""Optional online test that validates the example configuration end-to-end.

This test reads ``examples/configuration.yaml``, validates each configured lake
with the integration's schema, constructs a data source via the factory, and
fetches the latest datapoint (real HTTP). It asserts that both temperature and
timestamp are present and plausible.

- Title: Example configuration online check — Expect: latest datapoint with tz-aware timestamp for each lake

Skipped by default unless RUN_ONLINE=1.
"""

import os
from pathlib import Path
from typing import Any, Dict, List

import pytest
import asyncio

from custom_components.bgl_ts_sbg_laketemp.const import LAKE_SCHEMA, build_lake_config
from custom_components.bgl_ts_sbg_laketemp.data_source import create_data_source, TemperatureReading


ONLINE = os.getenv("RUN_ONLINE") == "1"


def _load_yaml_config(path: Path) -> Dict[str, Any]:
    try:
        import yaml  # type: ignore
    except Exception as exc:  # pragma: no cover - import guard for optional dep
        pytest.skip(f"PyYAML not installed: {exc}")
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


@pytest.mark.online
@pytest.mark.skipif(not ONLINE, reason="Set RUN_ONLINE=1 to enable online tests")
@pytest.mark.asyncio
async def test_examples_configuration_latest_for_all_lakes() -> None:
    # Load and validate YAML
    root = Path(__file__).resolve().parents[1]
    yaml_path = root / "examples" / "configuration.yaml"
    raw = _load_yaml_config(yaml_path)

    assert isinstance(raw, dict) and "bgl_ts_sbg_laketemp" in raw
    lakes_raw = raw["bgl_ts_sbg_laketemp"].get("lakes")
    assert isinstance(lakes_raw, list) and len(lakes_raw) > 0

    validated_lakes = [LAKE_SCHEMA(l) for l in lakes_raw]
    lake_cfgs = [build_lake_config(v) for v in validated_lakes]

    # Fetch sequentially to minimize load and be gentle to providers
    results: List[TemperatureReading] = []
    for lake in lake_cfgs:
        source = create_data_source(lake)
        reading = await source.fetch_temperature()
        results.append(reading)

    # Basic plausibility assertions
    assert len(results) == len(lake_cfgs)
    for reading in results:
        assert isinstance(reading, TemperatureReading)
        assert -5.0 <= reading.temperature_c <= 45.0
        assert reading.timestamp.tzinfo is not None


