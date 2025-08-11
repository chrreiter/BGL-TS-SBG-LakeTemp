from __future__ import annotations

"""Constants and configuration schema for the BGL-TS-SBG-LakeTemp integration."""

from typing import Any, Dict, Final, Literal, MutableMapping, TypedDict
from dataclasses import dataclass
from enum import Enum
import re
import voluptuous as vol

DOMAIN: Final[str] = "bgl_ts_sbg_laketemp"

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


def _is_http_url(value: str) -> str:
    if not isinstance(value, str):
        raise vol.Invalid("Invalid URL: expected a string")
    lowered = value.lower()
    if not (lowered.startswith("http://") or lowered.startswith("https://")):
        raise vol.Invalid("Invalid URL: must start with http:// or https://")
    if " " in value:
        raise vol.Invalid("Invalid URL: must not contain spaces")
    return value


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
    if value < 1 or value > 24 * 14:
        raise vol.Invalid("Invalid timeout_hours: must be between 1 and 336 hours (14 days)")
    return value


def _scan_seconds(value: int) -> int:
    if not isinstance(value, int):
        raise vol.Invalid("Invalid scan_interval: expected integer seconds")
    if value < 60 or value > 24 * 60 * 60:
        raise vol.Invalid("Invalid scan_interval: must be between 60 and 86400 seconds")
    return value


class LakeSourceType(str, Enum):
    """Supported data source types for scraping."""

    GKD_BAYERN = "gkd_bayern"
    GENERIC_HTML = "generic_html"
    HYDRO_OOE = "hydro_ooe"
    SALZBURG_OGD = "salzburg_ogd"


@dataclass(frozen=True)
class GkdBayernOptions:
    """Options specific to GKD Bayern source."""

    station_id: str | None = None
    table_selector: str | None = None


@dataclass(frozen=True)
class GenericHtmlOptions:
    """Options for a generic HTML source extraction."""

    css_selector: str | None = None
    value_regex: str | None = None


@dataclass(frozen=True)
class HydroOOEOptions:
    """Options specific to Hydro OOE source."""

    station_id: str | None = None
    api_base: str | None = None
    parameter: str | None = None  # e.g., "temperature" or localized key
    period: str | None = None  # ISO8601 duration like P7D


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
    options: GkdBayernOptions | GenericHtmlOptions | HydroOOEOptions | SalzburgOGDOptions | None


@dataclass(frozen=True)
class LakeConfig:
    """Validated configuration for a single lake sensor."""

    name: str
    url: str
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
    elif source_type is LakeSourceType.GENERIC_HTML:
        css_selector = options_in.get("css_selector")
        if css_selector is not None and not isinstance(css_selector, str):
            raise vol.Invalid("Invalid source.options.css_selector: expected string")
        value_regex = options_in.get("value_regex")
        if value_regex is not None and not isinstance(value_regex, str):
            raise vol.Invalid("Invalid source.options.value_regex: expected string")
    elif source_type is LakeSourceType.HYDRO_OOE:
        station_id = options_in.get("station_id")
        if station_id is not None and not isinstance(station_id, (str, int)):
            raise vol.Invalid("Invalid source.options.station_id: expected string or int")
        api_base = options_in.get("api_base")
        if api_base is not None and not isinstance(api_base, str):
            raise vol.Invalid("Invalid source.options.api_base: expected string URL")
        parameter = options_in.get("parameter")
        if parameter is not None and not isinstance(parameter, str):
            raise vol.Invalid("Invalid source.options.parameter: expected string")
        period = options_in.get("period")
        if period is not None and not isinstance(period, str):
            raise vol.Invalid("Invalid source.options.period: expected string like 'P7D'")
    elif source_type is LakeSourceType.SALZBURG_OGD:
        lake_name = options_in.get("lake_name")
        if lake_name is not None and not isinstance(lake_name, str):
            raise vol.Invalid("Invalid source.options.lake_name: expected string")

    return {CONF_SOURCE_TYPE: source_type.value, CONF_SOURCE_OPTIONS: options_in}


LAKE_SCHEMA: Final = vol.Schema(
    {
        vol.Required(CONF_NAME): vol.All(str, vol.Length(min=1, max=100)),
        vol.Required(CONF_URL): _is_http_url,
        vol.Required(CONF_ENTITY_ID): _is_entity_id_slug,
        vol.Optional(CONF_SCAN_INTERVAL, default=DEFAULT_SCAN_INTERVAL_SECONDS): _scan_seconds,
        vol.Optional(CONF_TIMEOUT_HOURS, default=DEFAULT_TIMEOUT_HOURS): _hours,
        vol.Optional(CONF_USER_AGENT, default=DEFAULT_USER_AGENT): vol.All(
            str, vol.Length(min=10)
        ),
        vol.Optional(CONF_SOURCE, default={CONF_SOURCE_TYPE: DEFAULT_SOURCE_TYPE, CONF_SOURCE_OPTIONS: {}}): _validate_source_block,
    }
)

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

    options: GkdBayernOptions | GenericHtmlOptions | HydroOOEOptions | SalzburgOGDOptions | None
    if source_type is LakeSourceType.GKD_BAYERN:
        options = GkdBayernOptions(
            station_id=options_dict.get("station_id"),
            table_selector=options_dict.get("table_selector"),
        )
    elif source_type is LakeSourceType.GENERIC_HTML:
        options = GenericHtmlOptions(
            css_selector=options_dict.get("css_selector"),
            value_regex=options_dict.get("value_regex"),
        )
    elif source_type is LakeSourceType.HYDRO_OOE:
        options = HydroOOEOptions(
            station_id=str(options_dict.get("station_id")) if options_dict.get("station_id") is not None else None,
            api_base=options_dict.get("api_base"),
            parameter=options_dict.get("parameter"),
            period=options_dict.get("period"),
        )
    elif source_type is LakeSourceType.SALZBURG_OGD:
        options = SalzburgOGDOptions(
            lake_name=options_dict.get("lake_name"),
        )
    else:
        options = None

    return LakeConfig(
        name=validated[CONF_NAME],
        url=validated[CONF_URL],
        entity_id=validated[CONF_ENTITY_ID],
        scan_interval=validated.get(CONF_SCAN_INTERVAL, DEFAULT_SCAN_INTERVAL_SECONDS),
        timeout_hours=validated.get(CONF_TIMEOUT_HOURS, DEFAULT_TIMEOUT_HOURS),
        user_agent=validated.get(CONF_USER_AGENT, DEFAULT_USER_AGENT),
        source=SourceConfig(type=source_type, options=options),
    )


