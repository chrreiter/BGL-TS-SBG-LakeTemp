from __future__ import annotations

"""Sensor platform scaffold for the BGL-TS-SBG-LakeTemp integration."""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import logging
from typing import Any, Dict, List, Optional

import aiohttp

from homeassistant.components.sensor import (
    SensorEntity,
    SensorDeviceClass,
)
from homeassistant.const import ATTR_ATTRIBUTION, UnitOfTemperature
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity import DeviceInfo
from homeassistant.helpers.update_coordinator import (
    CoordinatorEntity,
    DataUpdateCoordinator,
    UpdateFailed,
)

from .const import (
    CONF_LAKES,
    CONF_NAME,
    CONF_URL,
    CONF_ENTITY_ID,
    CONF_SCAN_INTERVAL,
    CONF_TIMEOUT_HOURS,
    CONF_USER_AGENT,
    DEFAULT_SCAN_INTERVAL_SECONDS,
    DEFAULT_TIMEOUT_HOURS,
    DEFAULT_USER_AGENT,
    DOMAIN,
    LAKE_SCHEMA,
    build_lake_config,
)
from .data_source import DataSourceInterface, TemperatureReading, create_data_source


_LOGGER = logging.getLogger(__name__)


async def async_setup_platform(hass: HomeAssistant, config: dict, async_add_entities, discovery_info: Optional[dict] = None) -> None:  # type: ignore[no-untyped-def]
    """Set up lake temperature sensors from YAML via discovery.

    The integration uses a domain-level YAML schema under ``bgl_ts_sbg_laketemp:``.
    This platform is loaded via discovery and expects ``discovery_info`` to
    contain the validated list of lake mappings.
    """

    if not discovery_info or CONF_LAKES not in discovery_info:
        _LOGGER.warning("No discovery info provided to sensor platform; nothing to set up")
        return

    raw_lakes: List[dict] = discovery_info.get(CONF_LAKES, [])
    if not isinstance(raw_lakes, list) or not raw_lakes:
        _LOGGER.warning("Discovery info lakes list is empty or invalid; nothing to set up")
        return

    entities: List[LakeTemperatureSensor] = []
    for idx, raw in enumerate(raw_lakes):
        try:
            validated = LAKE_SCHEMA(raw)
            lake_cfg = build_lake_config(validated)
        except Exception as exc:  # noqa: BLE001 - surface clear context in logs
            _LOGGER.error("Invalid lake configuration at index %s: %s", idx, exc, exc_info=True)
            continue

        sensor = await LakeTemperatureSensor.create(hass=hass, lake_config=lake_cfg)
        entities.append(sensor)

    if entities:
        async_add_entities(entities)
    else:
        _LOGGER.warning("No valid lake configurations; no sensors created")


@dataclass
class _EntityState:
    """Internal state cache for derived attributes."""

    last_reading: Optional[TemperatureReading] = None
    last_success_utc: Optional[datetime] = None


class LakeTemperatureSensor(CoordinatorEntity[TemperatureReading | None], SensorEntity):
    """Sensor representing the latest water temperature for a configured lake.

    Each instance manages its own external ``aiohttp.ClientSession`` to ensure a
    stable User-Agent and connection reuse across updates, per project rules.
    """

    _attr_device_class = SensorDeviceClass.TEMPERATURE
    _attr_native_unit_of_measurement = UnitOfTemperature.CELSIUS
    _attr_should_poll = False

    def __init__(
        self,
        *,
        hass: HomeAssistant,
        lake_config,
        coordinator: DataUpdateCoordinator[TemperatureReading | None],
        data_source: DataSourceInterface,
        session: aiohttp.ClientSession,
    ) -> None:
        super().__init__(coordinator)
        self.hass = hass
        self._lake = lake_config
        self._data_source = data_source
        self._session = session
        self._state = _EntityState()

        # Core attributes
        self._attr_name = self._lake.name
        self._attr_unique_id = f"{DOMAIN}-{self._lake.entity_id}"

        # Basic device info for grouping in UI
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, self._lake.entity_id)},
            name=f"{self._lake.name}",
            manufacturer="BGL-TS-SBG-LakeTemp",
            model="Lake Temperature Sensor",
        )

    @classmethod
    async def create(cls, *, hass: HomeAssistant, lake_config) -> "LakeTemperatureSensor":
        """Factory to create an instance with its coordinator and data source."""

        # Create a dedicated external session with UA header for this lake
        timeout = aiohttp.ClientTimeout(total=20)
        headers = {"User-Agent": lake_config.user_agent or DEFAULT_USER_AGENT}
        session = aiohttp.ClientSession(headers=headers, timeout=timeout)

        # Build the data source with the external session for connection reuse
        data_source = create_data_source(lake_config, session=session)

        async def _async_update_data() -> TemperatureReading | None:
            try:
                reading = await data_source.fetch_temperature()
            except Exception as exc:  # noqa: BLE001 - HA expects exceptions for UpdateFailed
                raise UpdateFailed(str(exc)) from exc
            return reading

        coordinator = DataUpdateCoordinator[TemperatureReading | None](
            hass,
            _LOGGER,
            name=f"{DOMAIN}:{lake_config.entity_id}",
            update_method=_async_update_data,
            update_interval=timedelta(seconds=lake_config.scan_interval or DEFAULT_SCAN_INTERVAL_SECONDS),
        )

        sensor = cls(
            hass=hass,
            lake_config=lake_config,
            coordinator=coordinator,
            data_source=data_source,
            session=session,
        )

        # Perform initial refresh using HA's dedicated API to align logs/patterns
        await coordinator.async_config_entry_first_refresh()
        return sensor

    @property
    def available(self) -> bool:
        return self.coordinator.last_update_success

    @property
    def native_value(self) -> float | None:
        reading = self.coordinator.data
        if reading is None:
            return None

        # If the reading is older than the configured timeout threshold, surface unknown
        try:
            now = datetime.now(timezone.utc)
            max_age = timedelta(hours=self._lake.timeout_hours or DEFAULT_TIMEOUT_HOURS)
            if reading.timestamp.tzinfo is None:
                # normalize naive timestamps to UTC for comparison safety
                rec_ts = reading.timestamp.replace(tzinfo=timezone.utc)
            else:
                rec_ts = reading.timestamp.astimezone(timezone.utc)
            if now - rec_ts > max_age:
                return None
        except Exception:  # noqa: BLE001 - be resilient; do not break state
            return None

        return float(reading.temperature_c)

    @property
    def extra_state_attributes(self) -> Dict[str, Any]:
        reading = self.coordinator.data
        attrs: Dict[str, Any] = {
            "lake_name": self._lake.name,
            "source_type": getattr(reading, "source", None) if reading else None,
            "url": self._lake.url,
            ATTR_ATTRIBUTION: "Data courtesy of public hydrology portals",
        }
        if reading is not None:
            try:
                ts = reading.timestamp
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                attrs["data_timestamp"] = ts.isoformat()
            except Exception:  # noqa: BLE001 - defensive
                attrs["data_timestamp"] = None
        return attrs

    async def async_will_remove_from_hass(self) -> None:
        try:
            await self._data_source.close()
        finally:
            if not self._session.closed:
                await self._session.close()


