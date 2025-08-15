from __future__ import annotations

"""Optional online test for GKDBayernScraper.

Fetches a real GKDBayern table page and checks that a plausible latest
record is parsed. Skipped by default unless RUN_ONLINE=1.
"""

import os
import pytest

from custom_components.bgl_ts_sbg_laketemp.scrapers.gkd_bayern import GKDBayernScraper


ONLINE = os.getenv("RUN_ONLINE") == "1"


@pytest.mark.online
@pytest.mark.skipif(not ONLINE, reason="Set RUN_ONLINE=1 to enable online tests")
@pytest.mark.asyncio
async def test_gkd_bayern_real_table_latest() -> None:
    # Base URL; scraper will target the explicit '/tabelle' view automatically
    url = "https://www.gkd.bayern.de/de/seen/wassertemperatur/inn/seethal-18673955/messwerte"
    async with GKDBayernScraper(url) as scraper:
        latest = await scraper.fetch_latest()

    assert latest.temperature_c is not None
    assert -5.0 <= latest.temperature_c <= 45.0
    assert latest.timestamp.tzinfo is not None


