from __future__ import annotations

"""Optional online test for GKDBayernScraper.

Fetches a real GKDBayern table page and checks that a plausible latest
record is parsed. Skipped by default unless RUN_ONLINE=1.
"""

from datetime import datetime, timezone, timedelta
import os
import pytest

from custom_components.bgl_ts_sbg_laketemp.scrapers.gkd_bayern import GKDBayernScraper


ONLINE = os.getenv("RUN_ONLINE") == "1"

# Freshness bound for the newest live record. Mirrors the default
# ``timeout_hours`` (24) used by the GKD lakes in examples/configuration.yaml:
# beyond this age the sensor would report ``unknown`` in Home Assistant, so a
# newest record older than this means the source is effectively stale even if
# parsing succeeds. This is the check that distinguishes "parses fine but serves
# stale data" from a genuinely healthy feed (CR-001, T6).
MAX_FRESH_HOURS = 24


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

    # T6: the newest record must be recent, not merely parseable. A relaunch that
    # leaves the table parseable but serves stale data would pass every other
    # assertion here; this is the only guard that catches it.
    now = datetime.now(timezone.utc)
    age = now - latest.timestamp.astimezone(timezone.utc)
    assert age <= timedelta(hours=MAX_FRESH_HOURS), (
        f"Newest GKD record is stale: age={age}, timestamp={latest.timestamp.isoformat()}"
    )
    # Guard against clock skew / bad timezone handling producing a future record.
    assert age >= timedelta(hours=-1), (
        f"Newest GKD record is in the future: timestamp={latest.timestamp.isoformat()}"
    )
