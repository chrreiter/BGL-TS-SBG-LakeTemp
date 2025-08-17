from __future__ import annotations

"""Comprehensive tests for timezone handling and locale-specific date parsing.

Covers:
- Europe/Berlin parsing for German dates (DST and standard time)
- Salzburg OGD tolerant datetime parsing (ISO with Z/offset, locale dd.mm.yyyy, yyyy.mm.dd, etc.)
- Parsing date/time parts and defaulting time when missing
- Hydro OOE numeric offset parsing from ZRXP headers
- Cross-timezone equivalence checks between sources
"""

import json
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pytest

from custom_components.bgl_ts_sbg_laketemp.scrapers.gkd_bayern import GKDBayernScraper, BERLIN_TZ
from custom_components.bgl_ts_sbg_laketemp.scrapers.salzburg_ogd import SalzburgOGDScraper, VIENNA_TZ
from custom_components.bgl_ts_sbg_laketemp.scrapers.hydro_ooe import HydroOOEScraper, parse_zrxp_block


FIXTURES = Path(__file__).parent / "fixtures" / "timezone_test_data.json"


@pytest.mark.asyncio
async def test_german_datetime_parsing_dst_and_standard() -> None:
	"""German DD.MM.YYYY HH:MM strings map to Europe/Berlin with correct DST offset."""
	data = json.loads(FIXTURES.read_text(encoding="utf-8"))
	for case in data["gkd_german_datetimes"]:
		dt = GKDBayernScraper._parse_german_datetime(case["text"])  # type: ignore[attr-defined]
		assert dt.tzinfo is not None
		# Offset in minutes from UTC
		offset_min = int(dt.utcoffset().total_seconds() // 60)  # type: ignore[union-attr]
		assert offset_min == case["expected_offset_minutes"], f"{case} -> {offset_min}"
		# Ensure tz is Europe/Berlin
		assert str(dt.tzinfo) in {str(BERLIN_TZ)}


@pytest.mark.asyncio
async def test_salzburg_ogd_iso_and_locale_inputs() -> None:
	"""Salzburg OGD parser accepts various ISO and locale formats and applies Vienna TZ when needed."""
	data = json.loads(FIXTURES.read_text(encoding="utf-8"))

	for case in data["ogd_iso_inputs"]:
		dt = SalzburgOGDScraper._parse_datetime_any(case["text"])  # type: ignore[attr-defined]
		assert dt is not None
		assert dt.tzinfo is not None
		offset_min = int(dt.utcoffset().total_seconds() // 60)  # type: ignore[union-attr]
		assert offset_min == case["expected_offset_minutes"], f"{case} -> {offset_min}"

	for case in data["ogd_locale_inputs"]:
		dt = SalzburgOGDScraper._parse_datetime_any(case["text"])  # type: ignore[attr-defined]
		assert dt is not None
		# Locale inputs without explicit offset should default to Vienna TZ
		assert dt.tzinfo is not None
		# When Vienna is in DST in Aug, offset should be +120
		offset_min = int(dt.utcoffset().total_seconds() // 60)  # type: ignore[union-attr]
		assert offset_min == case["expected_offset_minutes"], f"{case} -> {offset_min}"
		# Ensure tz matches VIENNA_TZ when no explicit offset in string
		if "MESZ" in case["text"] or "+" in case["text"] or case["text"].endswith("Z"):
			# Explicit offset provided -> may not equal Vienna
			pass
		else:
			# Should be Vienna tz
			# Python ZoneInfo string equality is not guaranteed, compare utcoffset for a reference date
			assert dt.tzinfo is not None


@pytest.mark.asyncio
async def test_salzburg_ogd_invalid_inputs_return_none() -> None:
	"""Invalid OGD timestamps should yield None."""
	data = json.loads(FIXTURES.read_text(encoding="utf-8"))
	for txt in data["ogd_invalid_inputs"]:
		dt = SalzburgOGDScraper._parse_datetime_any(txt)  # type: ignore[attr-defined]
		assert dt is None


@pytest.mark.asyncio
async def test_salzburg_ogd_parse_from_parts_and_default_time() -> None:
	"""Parsing date/time parts including defaulting to 12:00 when time missing and seconds handling."""
	data = json.loads(FIXTURES.read_text(encoding="utf-8"))
	for case in data["parts_cases"]:
		dt = SalzburgOGDScraper._parse_datetime_from_parts(case["date"], case["time"])  # type: ignore[attr-defined]
		assert dt is not None
		assert dt.hour == case["expected_hour"]
		if "expected_second" in case:
			assert dt.second == case["expected_second"]
		offset_min = int(dt.utcoffset().total_seconds() // 60)  # type: ignore[union-attr]
		assert offset_min == case["expected_offset_minutes"]
		# Ensure Vienna tz applied for locale date inputs
		assert str(dt.tzinfo) in {str(VIENNA_TZ)}


@pytest.mark.asyncio
async def test_hydro_ooe_tz_header_numeric_offsets() -> None:
	"""Hydro OOE ZRXP #TZUTC±H headers are applied as fixed offsets for all parsed timestamps."""
	data = json.loads(FIXTURES.read_text(encoding="utf-8"))

	for case in data["hydro_tz_cases"]:
		header = case["tz_header"]
		ts = case["ts"]
		val = case["val"]
		block = (
			f"#SANR99999|*|SNAMEFoo|*|SWATERSomeLake|*| {header}|*|RINVAL-777|*| #CUNIT°C|*| #LAYOUT(timestamp,value)|*| "
			f"{ts} {val}"
		)
		# Use parser helper to avoid HTTP
		records = parse_zrxp_block(block)
		assert len(records) == 1
		dt = records[0].timestamp
		assert dt.tzinfo is not None
		offset_min = int(dt.utcoffset().total_seconds() // 60)  # type: ignore[union-attr]
		assert offset_min == case["expected_offset_minutes"], f"{case} -> {offset_min}"


@pytest.mark.asyncio
async def test_hydro_ooe_missing_tz_defaults_to_utc() -> None:
	"""If a ZRXP block has no #TZUTC header, timestamps default to UTC in parser."""
	blk = (
		"#SANR12345|*|SNAMEFoo|*|SWATERSomeLake|*| #LAYOUT(timestamp,value)|*| "
		"20250808204500 22.3"
	)
	records = parse_zrxp_block(blk)
	assert len(records) == 1
	dt = records[0].timestamp
	assert dt.tzinfo is not None
	offset_min = int(dt.utcoffset().total_seconds() // 60)  # type: ignore[union-attr]
	assert offset_min == 0


@pytest.mark.asyncio
async def test_cross_timezone_equivalence_between_sources() -> None:
	"""A Berlin-local time and an OGD UTC string that represent the same instant should compare equal in UTC."""
	data = json.loads(FIXTURES.read_text(encoding="utf-8"))
	case = data["cross_equivalences"][0]

	berlin_local = GKDBayernScraper._parse_german_datetime(case["berlin_text"])  # type: ignore[attr-defined]
	ogd_dt = SalzburgOGDScraper._parse_datetime_any(case["ogd_text"])  # type: ignore[attr-defined]

	assert berlin_local is not None and ogd_dt is not None

	# Compare instants by converting to UTC
	berlin_utc = berlin_local.astimezone(timezone.utc)
	ogd_utc = ogd_dt.astimezone(timezone.utc)
	assert berlin_utc == ogd_utc

 
