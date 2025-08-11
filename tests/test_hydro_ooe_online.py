from __future__ import annotations

"""Optional online test for Hydro OOE ZRXP download.

This test performs a real HTTP request to the Hydro OOE bulk export
(`https://data.ooe.gv.at/files/hydro/HDOOE_Export_WT.zrxp`) and parses
the latest value for Irrsee (SANR 16579).

By default, the test is skipped. To run it locally set RUN_ONLINE=1:
  - PowerShell:  $env:RUN_ONLINE = '1'; pytest -q tests/test_hydro_ooe_online.py

We keep assertions conservative to minimize flakiness.
"""

import os
import pytest

from custom_components.bgl_ts_sbg_laketemp.scrapers.hydro_ooe import HydroOOEScraper


ONLINE = os.getenv("RUN_ONLINE") == "1"


@pytest.mark.online
@pytest.mark.skipif(not ONLINE, reason="Set RUN_ONLINE=1 to enable online tests")
@pytest.mark.asyncio
async def test_hydro_ooe_real_zrxp_latest_for_irrsee() -> None:
    # Match by name substring; SANR in ZRXP may differ from SPA station_id
    async with HydroOOEScraper(sname_contains="Irrsee") as scraper:
        latest = await scraper.fetch_latest()

    assert latest.temperature_c is not None
    assert -5.0 <= latest.temperature_c <= 45.0
    assert latest.timestamp.tzinfo is not None

