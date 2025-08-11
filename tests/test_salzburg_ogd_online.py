from __future__ import annotations

"""Optional online test for Salzburg OGD Hydrografie Seen text.

Run only when RUN_ONLINE=1 to avoid flakiness in CI.
"""

import os
import pytest

from custom_components.bgl_ts_sbg_laketemp.scrapers.salzburg_ogd import SalzburgOGDScraper


ONLINE = os.getenv("RUN_ONLINE") == "1"


@pytest.mark.online
@pytest.mark.skipif(not ONLINE, reason="Set RUN_ONLINE=1 to enable online tests")
@pytest.mark.asyncio
async def test_salzburg_ogd_real_latest_for_target_lakes() -> None:
    target = [
        "Fuschlsee",
        "Grabensee",
        "Mattsee",
        "Mondsee",
        "Obertrumer See",
        "Wolfgangsee",
        "Attersee",
        "Wallersee",
        "Zeller See",
    ]
    async with SalzburgOGDScraper() as scraper:
        mapping = await scraper.fetch_all_latest(target_lakes=target)

    # Assert at least a subset is available, values are plausible
    assert len(mapping) >= 3
    for rec in mapping.values():
        assert -5.0 <= rec.temperature_c <= 45.0
        assert rec.timestamp.tzinfo is not None


