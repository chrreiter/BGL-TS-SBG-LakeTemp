from __future__ import annotations

"""Offline resilience tests focusing on concurrency-like scenarios and limits.

While the scrapers themselves do not implement internal rate limiting or
concurrency guards, this suite simulates parallel usage with a shared
``aiohttp.ClientSession`` and ensures correct behavior and isolation per task.

Cases:
- Multiple concurrent fetches with an external shared session
- Ensuring fallback path is independent per concurrent call
- Verifying no shared mutable state causes cross-talk
"""

import asyncio

import aiohttp
import pytest
from aioresponses import aioresponses

from custom_components.bgl_ts_sbg_laketemp.scrapers.gkd_bayern import GKDBayernScraper


GKD_URL = "https://www.gkd.bayern.de/de/seen/wassertemperatur/inn/seethal-18673955/messwerte"
GKD_URL_TAB = GKD_URL.rstrip("/") + "/tabelle"


def _html_with_value(val: str) -> str:
    return f"""
    <html><body>
      <table>
        <thead><tr><th>Datum</th><th>Wassertemperatur [°C]</th></tr></thead>
        <tbody>
          <tr><td>08.08.2025 16:00</td><td>{val}</td></tr>
        </tbody>
      </table>
    </body></html>
    """


# Title: Concurrent fetches with shared session — Expect: both complete and return their respective values
@pytest.mark.asyncio
async def test_concurrent_gkd_fetches_with_shared_session() -> None:
    html_a = _html_with_value("23,1")
    html_b = _html_with_value("21,3")

    async with aiohttp.ClientSession() as session:
        with aioresponses() as mocked:
            # Two different lakes could be different URLs; here simulate by primary lacking table and using /tabelle
            mocked.get(GKD_URL, status=200, body="<html><body><p>diagramm</p></body></html>")
            mocked.get(GKD_URL_TAB, status=200, body=html_a)

            # For a second call, respond with a different body on /tabelle
            mocked.get("https://www.gkd.bayern.de/de/seen/wassertemperatur/inn/other-18673956/messwerte", status=200, body="<html><body><p>diagramm</p></body></html>")
            mocked.get("https://www.gkd.bayern.de/de/seen/wassertemperatur/inn/other-18673956/messwerte/tabelle", status=200, body=html_b)

            s1 = GKDBayernScraper(GKD_URL, session=session)
            s2 = GKDBayernScraper("https://www.gkd.bayern.de/de/seen/wassertemperatur/inn/other-18673956/messwerte", session=session)

            latest_a, latest_b = await asyncio.gather(s1.fetch_latest(), s2.fetch_latest())

    assert latest_a.temperature_c == 23.1
    assert latest_b.temperature_c == 21.3


# Title: Independent fallback per concurrent call — Expect: each call uses its own fallback URL
@pytest.mark.asyncio
async def test_independent_fallback_per_call() -> None:
    html = _html_with_value("22,0")
    with aioresponses() as mocked:
        mocked.get(GKD_URL, status=200, body="<html><body><p>diagramm</p></body></html>", repeat=True)
        mocked.get(GKD_URL_TAB, status=200, body=html, repeat=True)

        # Launch two parallel calls to the same URL; both should use fallback independently
        async with GKDBayernScraper(GKD_URL) as scraper:
            res1, res2 = await asyncio.gather(scraper.fetch_latest(), scraper.fetch_latest())

    assert res1.temperature_c == 22.0 and res2.temperature_c == 22.0


