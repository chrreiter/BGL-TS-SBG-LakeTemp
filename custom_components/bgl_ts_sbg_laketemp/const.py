from __future__ import annotations

"""Constants and configuration schema for the BGL-TS-SBG-LakeTemp integration."""

from typing import Any, Dict, Final, Literal, MutableMapping, TypedDict
from dataclasses import dataclass
from enum import Enum
import re
import voluptuous as vol
from urllib.parse import urlparse

DOMAIN: Final[str] = "bgl_ts_sbg_laketemp"
DATASET_STORE: Final[str] = "datasets"

CONF_LAKES: Final[str] = "lakes"
CONF_NAME: Final[str] = "name"
CONF_URL: Final[str] = "url"
CONF_ENTITY_ID: Final[str] = "entity_id"
CONF_SCAN_INTERVAL: Final[str] = "scan_interval"
CONF_TIMEOUT_HOURS: Final[str] = "timeout_hours"
CONF_USER_AGENT: Final[str] = "user_agent"

# Source configuration
CONF_SOURCE: Final[str] = "source"
CONF_SOURCE_TYPE: Final[str] = "type"
CONF_SOURCE_OPTIONS: Final[str] = "options"

DEFAULT_SCAN_INTERVAL_SECONDS: Final[int] = 1800
DEFAULT_TIMEOUT_HOURS: Final[int] = 24
DEFAULT_SOURCE_TYPE: Final[str] = "gkd_bayern"
DEFAULT_USER_AGENT: Final[str] = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)

# Validation bounds
MIN_SCAN_INTERVAL_SECONDS: Final[int] = 15
MAX_SCAN_INTERVAL_SECONDS: Final[int] = 24 * 60 * 60  # 1 day
MIN_TIMEOUT_HOURS: Final[int] = 1
MAX_TIMEOUT_HOURS: Final[int] = 24 * 14  # 14 days


def _is_http_url(value: str) -> str:
    if not isinstance(value, str):
        raise vol.Invalid("Invalid URL: expected a string")
    lowered = value.lower()
    if not (lowered.startswith("http://") or lowered.startswith("https://")):
        raise vol.Invalid("Invalid URL: must start with http:// or https://")
    if " " in value:
        raise vol.Invalid("Invalid URL: must not contain spaces")
    return value


def _optional_http_url(value: Any) -> str | None:
    """Allow None or a valid http(s) URL with clear error messages.

    This wrapper avoids ``vol.Any`` swallowing detailed error strings from
    ``_is_http_url`` and guarantees actionable messages in test assertions.
    """
    if value is None:
        return None
    if not isinstance(value, str):
        raise vol.Invalid("Invalid URL: expected a string or null")
    return _is_http_url(value)


_ENTITY_ID_SLUG_RE: Final[re.Pattern[str]] = re.compile(r"^[a-z0-9_]{1,64}$")


def _is_entity_id_slug(value: str) -> str:
    """Validate the entity_id slug (without domain), e.g., 'thumsee'."""
    if not isinstance(value, str):
        raise vol.Invalid("Invalid entity_id: expected a string")
    if not _ENTITY_ID_SLUG_RE.fullmatch(value):
        raise vol.Invalid(
            "Invalid entity_id: use lowercase letters, numbers, and underscores only (max 64 chars)"
        )
    return value


def _hours(value: int) -> int:
    if not isinstance(value, int):
        raise vol.Invalid("Invalid timeout_hours: expected integer hours")
    if value < MIN_TIMEOUT_HOURS or value > MAX_TIMEOUT_HOURS:
        raise vol.Invalid(
            f"Invalid timeout_hours: must be between {MIN_TIMEOUT_HOURS} and {MAX_TIMEOUT_HOURS} hours (max 14 days)"
        )
    return value


def _scan_seconds(value: int) -> int:
    if not isinstance(value, int):
        raise vol.Invalid("Invalid scan_interval: expected integer seconds")
    if value < MIN_SCAN_INTERVAL_SECONDS or value > MAX_SCAN_INTERVAL_SECONDS:
        raise vol.Invalid(
            f"Invalid scan_interval: must be between {MIN_SCAN_INTERVAL_SECONDS} and {MAX_SCAN_INTERVAL_SECONDS} seconds"
        )
    return value


class LakeSourceType(str, Enum):
    """Supported data source types for scraping."""

    GKD_BAYERN = "gkd_bayern"
    HYDRO_OOE = "hydro_ooe"
    SALZBURG_OGD = "salzburg_ogd"


@dataclass(frozen=True)
class GkdBayernOptions:
    """Options specific to GKD Bayern source."""

    station_id: str | None = None
    table_selector: str | None = None


@dataclass(frozen=True)
class HydroOOEOptions:
    """Options specific to Hydro OOE source."""

    station_id: str | None = None


@dataclass(frozen=True)
class SalzburgOGDOptions:
    """Options specific to Salzburg OGD hydrology 'Seen' text source.

    Attributes:
        lake_name: Optional explicit lake name to match in the dataset. If not
            provided, the integration will use the top-level ``name`` from the
            lake configuration as the match key.
    """

    lake_name: str | None = None


@dataclass(frozen=True)
class SourceConfig:
    """Unified source configuration for a lake."""

    type: LakeSourceType
    options: GkdBayernOptions | HydroOOEOptions | SalzburgOGDOptions | None


@dataclass(frozen=True)
class LakeConfig:
    """Validated configuration for a single lake sensor."""

    name: str
    url: str | None
    entity_id: str
    scan_interval: int
    timeout_hours: int
    user_agent: str
    source: SourceConfig


def _validate_source_block(value: MutableMapping[str, Any]) -> Dict[str, Any]:
    """Validate the source block and its type-specific options.

    Raises a descriptive error if an unknown type is provided or if options are invalid.
    """

    if not isinstance(value, dict):
        raise vol.Invalid("Invalid source: expected a mapping with 'type' and 'options'")

    source_type_raw = value.get(CONF_SOURCE_TYPE, DEFAULT_SOURCE_TYPE)
    try:
        source_type = LakeSourceType(source_type_raw)
    except Exception as exc:  # noqa: BLE001 - convert to voluptuous Invalid
        allowed = ", ".join(t.value for t in LakeSourceType)
        raise vol.Invalid(f"Invalid source.type: expected one of: {allowed}") from exc

    options_in = value.get(CONF_SOURCE_OPTIONS, {})
    if not isinstance(options_in, dict):
        raise vol.Invalid("Invalid source.options: expected a mapping/dict")

    # Type-specific option validation
    if source_type is LakeSourceType.GKD_BAYERN:
        # All fields optional for now; ensure strings if provided
        station_id = options_in.get("station_id")
        if station_id is not None and not isinstance(station_id, str):
            raise vol.Invalid("Invalid source.options.station_id: expected string")
        table_selector = options_in.get("table_selector")
        if table_selector is not None and not isinstance(table_selector, str):
            raise vol.Invalid("Invalid source.options.table_selector: expected string")
    elif source_type is LakeSourceType.HYDRO_OOE:
        station_id = options_in.get("station_id")
        if station_id is not None and not isinstance(station_id, (str, int)):
            raise vol.Invalid("Invalid source.options.station_id: expected string or int")
    elif source_type is LakeSourceType.SALZBURG_OGD:
        lake_name = options_in.get("lake_name")
        if lake_name is not None and not isinstance(lake_name, str):
            raise vol.Invalid("Invalid source.options.lake_name: expected string")
        # Deprecation: any attempt to provide a custom OGD URL via options is rejected
        for deprecated_key in ("url", "custom_url", "ogd_url"):
            if deprecated_key in options_in:
                raise vol.Invalid(
                    f"Invalid source.options.{deprecated_key}: deprecated for 'salzburg_ogd'; remove this option"
                )

    return {CONF_SOURCE_TYPE: source_type.value, CONF_SOURCE_OPTIONS: options_in}


def _enforce_url_requirement_by_source(value: MutableMapping[str, Any]) -> Dict[str, Any]:
    """Enforce that ``url`` is required for some sources but optional for others.

    - Required: gkd_bayern
    - Optional: hydro_ooe, salzburg_ogd
    """

    if not isinstance(value, dict):
        raise vol.Invalid("Invalid lake config: expected a mapping/dict")

    source_block = value.get(CONF_SOURCE, {CONF_SOURCE_TYPE: DEFAULT_SOURCE_TYPE, CONF_SOURCE_OPTIONS: {}})
    try:
        source_type = LakeSourceType(source_block.get(CONF_SOURCE_TYPE, DEFAULT_SOURCE_TYPE))
    except Exception as exc:  # noqa: BLE001
        raise vol.Invalid("Invalid source.type in lake config") from exc

    url_val = value.get(CONF_URL)

    if source_type is LakeSourceType.GKD_BAYERN:
        if not isinstance(url_val, str) or not url_val:
            raise vol.Invalid("'url' is required for source type gkd_bayern")
    else:
        # For hydro_ooe and salzburg_ogd allow None or missing.
        # If provided, validate strictly and surface clear, actionable messages.
        if url_val:
            # Enforce http(s) and no spaces first
            _ = _is_http_url(url_val)
            # Then ensure a host is present
            try:
                parsed = urlparse(url_val)
                if not parsed.netloc:
                    raise vol.Invalid("Invalid url: malformed URL (missing host)")
            except vol.Invalid:
                raise
            except Exception as exc:  # noqa: BLE001
                raise vol.Invalid(f"Invalid url: {exc}")

    return value  # unchanged mapping


_LAKE_FIELDS_SCHEMA: Final = vol.Schema(
    {
        vol.Required(CONF_NAME): vol.All(str, vol.Length(min=1, max=100)),
        vol.Optional(CONF_URL, default=None): _optional_http_url,
        vol.Required(CONF_ENTITY_ID): _is_entity_id_slug,
        # Enforce integers without coercion and clear range bounds
        vol.Optional(CONF_SCAN_INTERVAL, default=DEFAULT_SCAN_INTERVAL_SECONDS): _scan_seconds,
        vol.Optional(CONF_TIMEOUT_HOURS, default=DEFAULT_TIMEOUT_HOURS): _hours,
        vol.Optional(CONF_USER_AGENT, default=DEFAULT_USER_AGENT): vol.All(
            str, vol.Length(min=10)
        ),
        vol.Optional(CONF_SOURCE, default={CONF_SOURCE_TYPE: DEFAULT_SOURCE_TYPE, CONF_SOURCE_OPTIONS: {}}): _validate_source_block,
    }
)

LAKE_SCHEMA: Final = vol.All(_LAKE_FIELDS_SCHEMA, _enforce_url_requirement_by_source)

CONFIG_SCHEMA: Final = vol.Schema(
    {
        DOMAIN: vol.Schema(
            {
                vol.Required(CONF_LAKES): vol.All(
                    [LAKE_SCHEMA], vol.Length(min=1, msg="At least one lake must be configured")
                )
            }
        )
    },
    extra=vol.ALLOW_EXTRA,
)


def build_lake_config(validated: Dict[str, Any]) -> LakeConfig:
    """Convert a validated dict (via LAKE_SCHEMA) into a typed LakeConfig."""

    source_block = validated.get(CONF_SOURCE, {})
    source_type = LakeSourceType(source_block.get(CONF_SOURCE_TYPE, DEFAULT_SOURCE_TYPE))
    options_dict = source_block.get(CONF_SOURCE_OPTIONS, {})

    options: GkdBayernOptions | HydroOOEOptions | SalzburgOGDOptions | None
    if source_type is LakeSourceType.GKD_BAYERN:
        options = GkdBayernOptions(
            station_id=options_dict.get("station_id"),
            table_selector=options_dict.get("table_selector"),
        )
    elif source_type is LakeSourceType.HYDRO_OOE:
        options = HydroOOEOptions(
            station_id=str(options_dict.get("station_id")) if options_dict.get("station_id") is not None else None,
        )
    elif source_type is LakeSourceType.SALZBURG_OGD:
        options = SalzburgOGDOptions(
            lake_name=options_dict.get("lake_name"),
        )
    else:
        options = None

    return LakeConfig(
        name=validated[CONF_NAME],
        url=validated.get(CONF_URL),
        entity_id=validated[CONF_ENTITY_ID],
        scan_interval=validated.get(CONF_SCAN_INTERVAL, DEFAULT_SCAN_INTERVAL_SECONDS),
        timeout_hours=validated.get(CONF_TIMEOUT_HOURS, DEFAULT_TIMEOUT_HOURS),
        user_agent=validated.get(CONF_USER_AGENT, DEFAULT_USER_AGENT),
        source=SourceConfig(type=source_type, options=options),
    )


