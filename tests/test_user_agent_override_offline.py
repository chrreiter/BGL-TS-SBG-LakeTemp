from __future__ import annotations

"""Offline test to verify custom User-Agent override is applied.

Title: Custom user_agent in config → Expect: sensor session uses that header
Also: per-lake UAs are sent per request on the shared session (GKD), and shared
datasets keep the first lake's UA and warn about a differing later one.
"""

import logging

import pytest
from aioresponses import aioresponses

from custom_components.bgl_ts_sbg_laketemp.sensor import async_setup_platform
from custom_components.bgl_ts_sbg_laketemp.const import CONF_LAKES


GKD_URL = "https://www.gkd.bayern.de/de/seen/wassertemperatur/inn/seethal-18673955/messwerte"
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
        self.entities: list[object] = []

    def __call__(self, entities):  # type: ignore[no-untyped-def]
        if isinstance(entities, list):
            self.entities.extend(entities)
        else:
            self.entities.append(entities)


@pytest.mark.asyncio
async def test_custom_user_agent_is_used_in_session_headers(caplog) -> None:  # type: ignore[no-untyped-def]
    # Title: Custom UA override — Expect: session.headers['User-Agent'] equals custom value
    caplog.set_level(logging.DEBUG)

    custom_ua = "MyCustomUA/9.9 LAKETEMP-TEST"
    discovery_info = {
        CONF_LAKES: [
            {
                "name": "Seethal / Abtsdorfer See",
                "url": GKD_URL,
                "entity_id": "seethal_abtsdorfer",
                "user_agent": custom_ua,
                "timeout_hours": 336,  # avoid staleness check in tests (max allowed)
                "source": {"type": "gkd_bayern", "options": {}},
            }
        ]
    }

    added = _EntityList()

    with aioresponses() as mocked:
        mocked.get(
            GKD_URL,
            status=200,
            body=GKD_HTML,
            headers={"Content-Type": "text/html; charset=utf-8"},
        )
        await async_setup_platform(hass={}, config={}, async_add_entities=added, discovery_info=discovery_info)

    assert len(added.entities) == 1
    sensor = added.entities[0]

    # The sensor manages an external session with default headers.
    session = getattr(sensor, "_session")
    assert session is not None
    # aiohttp exposes default headers via session.headers (CIMultiDict)
    assert session.headers.get("User-Agent") == custom_ua

    # Cleanup: ensure sessions are closed to avoid warnings
    await sensor.async_will_remove_from_hass()
    await sensor.async_will_remove_from_hass()




GKD_URL_2 = "https://www.gkd.bayern.de/de/seen/wassertemperatur/inn/koenigssee-18624806/messwerte"


def _sent_user_agents(mocked, url: str) -> list[str]:  # type: ignore[no-untyped-def]
    """Return the User-Agent header of each recorded GET to ``url``."""
    from yarl import URL

    calls = mocked.requests.get(("GET", URL(url)), [])
    return [(call.kwargs.get("headers") or {}).get("User-Agent") for call in calls]


@pytest.mark.asyncio
async def test_per_lake_user_agent_is_sent_per_request_on_shared_session() -> None:
    # Title: Two GKD lakes with different UAs — Expect: one shared session, each request carries its own lake's UA
    ua_1 = "LakeOneUA/1.0"
    ua_2 = "LakeTwoUA/2.0"
    discovery_info = {
        CONF_LAKES: [
            {
                "name": "Seethal",
                "url": GKD_URL,
                "entity_id": "seethal",
                "user_agent": ua_1,
                "timeout_hours": 336,
                "source": {"type": "gkd_bayern", "options": {}},
            },
            {
                "name": "Königssee",
                "url": GKD_URL_2,
                "entity_id": "koenigssee",
                "user_agent": ua_2,
                "timeout_hours": 336,
                "source": {"type": "gkd_bayern", "options": {}},
            },
        ]
    }

    added = _EntityList()
    with aioresponses() as mocked:
        for url in (GKD_URL + "/tabelle", GKD_URL_2 + "/tabelle"):
            mocked.get(url, status=200, body=GKD_HTML, headers={"Content-Type": "text/html; charset=utf-8"})
        await async_setup_platform(hass={}, config={}, async_add_entities=added, discovery_info=discovery_info)
        sent_1 = _sent_user_agents(mocked, GKD_URL + "/tabelle")
        sent_2 = _sent_user_agents(mocked, GKD_URL_2 + "/tabelle")

    assert len(added.entities) == 2
    s1, s2 = added.entities
    assert s1._session is s2._session  # session is still shared
    assert sent_1 == [ua_1]
    assert sent_2 == [ua_2]


@pytest.mark.asyncio
async def test_shared_dataset_warns_when_later_lake_user_agent_differs(caplog) -> None:  # type: ignore[no-untyped-def]
    # Title: Shared dataset UA — Expect: first lake's UA is kept; a differing later UA logs a WARNING, an equal one does not
    caplog.set_level(logging.WARNING)
    ua_first = "FirstUA/1.0"

    def _lake(name: str, entity_id: str, ua: str) -> dict:
        return {
            "name": name,
            "entity_id": entity_id,
            "user_agent": ua,
            "source": {"type": "salzburg_ogd", "options": {"lake_name": name}},
        }

    discovery_info = {
        CONF_LAKES: [
            _lake("Fuschlsee", "fuschlsee", ua_first),
            _lake("Wallersee", "wallersee", ua_first),
            _lake("Mattsee", "mattsee", "OtherUA/2.0"),
        ]
    }

    added = _EntityList()
    await async_setup_platform(hass={}, config={}, async_add_entities=added, discovery_info=discovery_info)
    assert len(added.entities) == 3

    manager = added.entities[0]._dataset_manager
    assert manager._session.headers.get("User-Agent") == ua_first

    ua_warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING and "user_agent" in r.getMessage()]
    assert len(ua_warnings) == 1
    msg = ua_warnings[0]
    assert "entity_id=mattsee" in msg
    assert "configured_user_agent=OtherUA/2.0" in msg
    assert "used_user_agent=FirstUA/1.0" in msg
