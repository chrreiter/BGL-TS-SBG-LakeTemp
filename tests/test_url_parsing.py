from __future__ import annotations

"""URL parsing tests for Hydro OOE station id extraction.

Each test includes a short title and expected outcome as a comment, per project convention.
"""

import pytest

from custom_components.bgl_ts_sbg_laketemp.data_source import (
    _extract_station_id_from_url,
    _extract_gkd_station_id_from_url,
)


# Title: SPA fragment with /station/<id>/ — Expect: extract 16579
def test_extract_station_id_from_spa_fragment_station() -> None:
    url = (
        "https://hydro.ooe.gv.at/#/overview/Wassertemperatur/station/16579/"
        "Zell%20am%20Moos/Wassertemperatur?period=P7D"
    )
    assert _extract_station_id_from_url(url) == "16579"


# Title: Path with sanr/<id> — Expect: extract 12345
def test_extract_station_id_from_path_sanr() -> None:
    url = "https://hydro.ooe.gv.at/some/route/sanr/12345/details"
    assert _extract_station_id_from_url(url) == "12345"


# Title: Query param sanr — Expect: extract 45678
def test_extract_station_id_from_query_param() -> None:
    url = "https://hydro.ooe.gv.at/?view=overview&sanr=45678"
    assert _extract_station_id_from_url(url) == "45678"


# Title: Fragment query param station — Expect: extract 9876
def test_extract_station_id_from_fragment_query() -> None:
    url = "https://hydro.ooe.gv.at/#/map?station=9876&foo=bar"
    assert _extract_station_id_from_url(url) == "9876"


# Title: Unique numeric token fallback — Expect: extract 321
def test_extract_station_id_fallback_unique_token() -> None:
    url = "https://hydro.ooe.gv.at/#/random/route/value/321/extra"
    assert _extract_station_id_from_url(url) == "321"


# Title: Ambiguous numeric tokens — Expect: ValueError
def test_extract_station_id_ambiguous_tokens() -> None:
    url = "https://hydro.ooe.gv.at/#/route/111/then/222"
    with pytest.raises(ValueError):
        _extract_station_id_from_url(url)


# Title: Wrong domain — Expect: ValueError
def test_extract_station_id_wrong_domain() -> None:
    url = "https://example.com/#/overview/Wassertemperatur/station/16579/whatever"
    with pytest.raises(ValueError):
        _extract_station_id_from_url(url)


# Title: Invalid type — Expect: ValueError
def test_extract_station_id_invalid_type() -> None:
    with pytest.raises(ValueError):
        _extract_station_id_from_url(123)  # type: ignore[arg-type]


# ----- GKD Bayern URL parsing -----


# Title: GKD seethal url — Expect: extract 18673955
def test_extract_gkd_station_id_seethal() -> None:
    url = "https://www.gkd.bayern.de/de/seen/wassertemperatur/main_unten/seethal-18673955/messwerte"
    assert _extract_gkd_station_id_from_url(url) == "18673955"


# Title: GKD buchwinkel url — Expect: extract 18682507
def test_extract_gkd_station_id_buchwinkel() -> None:
    url = "https://www.gkd.bayern.de/de/seen/wassertemperatur/main_unten/buchwinkel-18682507/messwerte"
    assert _extract_gkd_station_id_from_url(url) == "18682507"


# Title: GKD koenigssee url — Expect: extract 18624806
def test_extract_gkd_station_id_koenigssee() -> None:
    url = "https://www.gkd.bayern.de/de/seen/wassertemperatur/main_unten/koenigssee-18624806/messwerte"
    assert _extract_gkd_station_id_from_url(url) == "18624806"


# Title: GKD table subpath — Expect: extract id via fallback
def test_extract_gkd_station_id_with_tabelle_suffix() -> None:
    url = (
        "https://www.gkd.bayern.de/de/seen/wassertemperatur/main_unten/koenigssee-18624806/"
        "messwerte/tabelle"
    )
    assert _extract_gkd_station_id_from_url(url) == "18624806"


# Title: GKD wrong domain — Expect: ValueError
def test_extract_gkd_station_id_wrong_domain() -> None:
    url = "https://example.com/de/seen/wassertemperatur/main_unten/seethal-18673955/messwerte"
    with pytest.raises(ValueError):
        _extract_gkd_station_id_from_url(url)


