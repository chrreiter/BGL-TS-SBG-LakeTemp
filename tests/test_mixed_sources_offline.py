from __future__ import annotations

"""Mixed-source behavior: GKD per-lake remains independent of Salzburg OGD dataset."""

from datetime import timedelta
from typing import List

import pytest
from aioresponses import aioresponses

from custom_components.bgl_ts_sbg_laketemp.sensor import async_setup_platform
from custom_components.bgl_ts_sbg_laketemp.const import CONF_LAKES


GKD_URL = "https://www.gkd.bayern.de/de/seen/wassertemperatur/inn/seethal-18673955/messwerte"
GKD_TABLE_URL = GKD_URL.rstrip("/") + "/tabelle"
OGD_URL = "https://www.salzburg.gv.at/ogd/56c28e2d-8b9e-41ba-b7d6-fa4896b5b48b/Hydrografie%20Seen.txt"

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
async def test_mixed_sources_gkd_independent_of_ogd() -> None:  # type: ignore[no-untyped-def]
    # Title: Mixed sources — Expect: GKD per-lake coordinator independent of OGD dataset

    discovery_info = {
        CONF_LAKES: [
            {
                "name": "Seethal / Abtsdorfer See",
                "url": GKD_URL,
                "entity_id": "seethal_abtsdorfer",
                "scan_interval": 600,
                "timeout_hours": 336,
                "source": {"type": "gkd_bayern", "options": {}},
            },
            {
                "name": "Fuschlsee",
                "url": OGD_URL,
                "entity_id": "fuschlsee",
                "scan_interval": 1800,
                "timeout_hours": 336,
                "source": {"type": "salzburg_ogd", "options": {"lake_name": "Fuschlsee"}},
            },
        ]
    }

    ogd_payload = (
        "Gewässer;Messdatum;Uhrzeit;Wassertemperatur [°C];Station\n"
        "Fuschlsee;2025-08-08;14:00;22,4;Westufer\n"
    )

    added = _EntityList()

    with aioresponses() as mocked:
        mocked.get(GKD_TABLE_URL, status=200, body=GKD_HTML, headers={"Content-Type": "text/html; charset=utf-8"})
        mocked.get(OGD_URL, status=200, body=ogd_payload, headers={"Content-Type": "text/plain; charset=utf-8"})

        await async_setup_platform(hass={}, config={}, async_add_entities=added, discovery_info=discovery_info)

        assert len(added.entities) == 2
        gkd_sensor = next(e for e in added.entities if getattr(e._lake.source.type, "value", e._lake.source.type) == "gkd_bayern")  # type: ignore[attr-defined]
        ogd_sensor = next(e for e in added.entities if getattr(e._lake.source.type, "value", e._lake.source.type) == "salzburg_ogd")  # type: ignore[attr-defined]

        # GKD per-lake sensor should have state ready after setup refresh
        assert gkd_sensor.native_value == 23.1

        # OGD sensor needs an explicit refresh (shared dataset)
        await ogd_sensor.coordinator.async_refresh()
        assert ogd_sensor.native_value == 22.4

        # Coordinators must be different instances
        assert gkd_sensor.coordinator is not ogd_sensor.coordinator

        # Verify update intervals: GKD uses its own, OGD uses its own (only one OGD lake)
        assert gkd_sensor.coordinator.update_interval == timedelta(seconds=600)
        assert ogd_sensor.coordinator.update_interval == timedelta(seconds=1800)

        # One GET per resource
        from yarl import URL
        assert len(mocked.requests.get(("GET", URL(GKD_TABLE_URL)), [])) == 1
        assert len(mocked.requests.get(("GET", URL(OGD_URL)), [])) == 1

        # Cleanup: close per-lake and dataset sessions to avoid warnings
        await gkd_sensor.async_will_remove_from_hass()
        if getattr(ogd_sensor, "_dataset_manager", None) is not None:  # type: ignore[attr-defined]
            await ogd_sensor._dataset_manager.async_close()  # type: ignore[attr-defined]


