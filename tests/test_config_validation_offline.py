from __future__ import annotations

"""Offline tests for YAML config validation and deprecations.

- Unknown/invalid source.type -> schema validation error with clear message
- Deprecated custom OGD URL option in source.options -> validation error mentioning 'deprecated'
- scan_interval: zero/negative/too-small/too-large/wrong-type -> validation errors
- timeout_hours: zero/negative/too-small/too-large/wrong-type -> validation errors
- url optional for discovery-capable sources; malformed url -> helpful error
"""

import pytest
import voluptuous as vol

from custom_components.bgl_ts_sbg_laketemp.const import LAKE_SCHEMA, CONFIG_SCHEMA


# Test: Invalid source.type
# Expect: vol.Invalid with message mentioning 'source.type'
def test_invalid_source_type_rejected() -> None:
    raw = {
        "name": "Some Lake",
        "url": "https://example.com/page",
        "entity_id": "some_lake",
        "source": {"type": "unknown_provider", "options": {}},
    }
    with pytest.raises(vol.Invalid) as ei:
        LAKE_SCHEMA(raw)
    assert "source.type" in str(ei.value).lower()


# Test: Deprecated custom OGD URL option in source.options
# Expect: vol.Invalid mentioning 'deprecated'
def test_deprecated_custom_ogd_url_option_rejected() -> None:
    raw = {
        "name": "Fuschlsee",
        "entity_id": "fuschlsee",
        "source": {"type": "salzburg_ogd", "options": {"lake_name": "Fuschlsee", "custom_url": "https://example.test/ogd.txt"}},
    }
    with pytest.raises(vol.Invalid) as ei:
        LAKE_SCHEMA(raw)
    err = str(ei.value).lower()
    assert "deprecated" in err and "custom_url" in err


# Test: scan_interval boundary and type validation
# Expect: vol.Invalid for each invalid case
@pytest.mark.parametrize(
    "scan_interval",
    [0, -10, 5, 86_400 + 1, "30"],
)
def test_scan_interval_invalid_values(scan_interval) -> None:  # type: ignore[no-untyped-def]
    raw = {
        "name": "Test Lake",
        "url": "https://www.gkd.bayern.de/de/seen/wassertemperatur/inn/seethal-18673955/messwerte",
        "entity_id": "test_lake",
        "scan_interval": scan_interval,
        "source": {"type": "gkd_bayern", "options": {}},
    }
    with pytest.raises(vol.Invalid):
        LAKE_SCHEMA(raw)


# Test: timeout_hours boundary and type validation
# Expect: vol.Invalid for each invalid case
@pytest.mark.parametrize(
    "timeout_hours",
    [0, -1, 337, "24"],
)
def test_timeout_hours_invalid_values(timeout_hours) -> None:  # type: ignore[no-untyped-def]
    raw = {
        "name": "Test Lake",
        "url": "https://www.gkd.bayern.de/de/seen/wassertemperatur/inn/seethal-18673955/messwerte",
        "entity_id": "test_lake_timeout",
        "timeout_hours": timeout_hours,
        "source": {"type": "gkd_bayern", "options": {}},
    }
    with pytest.raises(vol.Invalid):
        LAKE_SCHEMA(raw)


# Test: url optional for discovery-capable sources (hydro_ooe, salzburg_ogd)
# Expect: schema accepts when url omitted
@pytest.mark.parametrize("stype", ["hydro_ooe", "salzburg_ogd"])
def test_url_optional_for_discovery_sources(stype: str) -> None:
    raw = {
        "name": "Discovery Lake",
        "entity_id": f"{stype}_lake",
        "source": {"type": stype, "options": {}},
    }
    validated = LAKE_SCHEMA(raw)
    assert isinstance(validated, dict)


# Test: malformed provided url is rejected with clear message
# Expect: vol.Invalid mentioning 'Invalid URL' or 'Invalid url'
@pytest.mark.parametrize(
    "url",
    [
        "ftp://example.com/file",  # wrong scheme
        "http://",  # missing host
        "https://bad host.com",  # space in host
    ],
)
def test_malformed_url_rejected(url: str) -> None:
    raw = {
        "name": "Bad URL Lake",
        "entity_id": "bad_url_lake",
        "url": url,
        "source": {"type": "hydro_ooe", "options": {}},
    }
    with pytest.raises(vol.Invalid) as ei:
        LAKE_SCHEMA(raw)
    assert "invalid url" in str(ei.value).lower()


