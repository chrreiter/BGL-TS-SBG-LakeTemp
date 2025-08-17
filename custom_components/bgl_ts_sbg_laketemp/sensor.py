from __future__ import annotations

"""Sensor platform scaffold for the BGL-TS-SBG-LakeTemp integration."""

from datetime import datetime, timedelta, timezone
import logging
from typing import Any, Dict, List, Optional, Callable

import aiohttp

from homeassistant.components.sensor import (
    SensorEntity,
    SensorDeviceClass,
    SensorStateClass,
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
    LakeConfig,
    LakeSourceType,
    build_lake_config,
)
from .data_source import DataSourceInterface, TemperatureReading, create_data_source
from .dataset_coordinators import (
    BaseDatasetCoordinator,
    get_or_create_dataset_manager,
    SalzburgOGDDatasetCoordinator,
    HydroOoeDatasetCoordinator,
)


_LOGGER = logging.getLogger(__name__)


async def async_setup_platform(
    hass: HomeAssistant,
    config: dict[str, Any],
    async_add_entities: Callable[[List[SensorEntity]], None],
    discovery_info: Optional[dict[str, Any]] = None,
) -> None:
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

        try:
            sensor = await LakeTemperatureSensor.create(hass=hass, lake_config=lake_cfg)
            # For per-lake coordinators, perform an immediate refresh so initial state is available.
            # For aggregated datasets, skip here to avoid double-refresh when multiple sensors share one coordinator.
            try:
                if getattr(sensor, "_dataset_manager", None) is None:
                    await sensor.coordinator.async_refresh()
            except Exception:
                # Coordinator stub logs failures; continue to add the entity
                pass
            entities.append(sensor)
        except Exception as exc:  # noqa: BLE001 - resilient per-lake setup
            _LOGGER.error(
                "Failed to create sensor for lake '%s': %s",
                raw.get(CONF_NAME, f"#{idx}"),
                exc,
                exc_info=True,
            )
            continue

    if entities:
        _LOGGER.info("Creating %d lake temperature sensor(s)", len(entities))
        async_add_entities(entities)
    else:
        _LOGGER.warning("No valid lake configurations; no sensors created")

class LakeTemperatureSensor(CoordinatorEntity, SensorEntity):
    """Sensor representing the latest water temperature for a configured lake.

    Each instance manages its own external ``aiohttp.ClientSession`` to ensure a
    stable User-Agent and connection reuse across updates, per project rules.
    """

    _attr_device_class = SensorDeviceClass.TEMPERATURE
    _attr_native_unit_of_measurement = UnitOfTemperature.CELSIUS
    _attr_should_poll = False
    _attr_state_class = SensorStateClass.MEASUREMENT

    def __init__(
        self,
        *,
        hass: HomeAssistant,
        lake_config: LakeConfig,
        coordinator: DataUpdateCoordinator,
        data_source: DataSourceInterface | None,
        session: aiohttp.ClientSession | None,
        dataset_manager: BaseDatasetCoordinator | None = None,
        aggregated_lookup_key: str | None = None,
    ) -> None:
        """Initialize the lake temperature sensor.

        Args:
            hass: Home Assistant instance.
            lake_config: Validated configuration for this lake.
            coordinator: Update coordinator (per-lake or dataset-level).
            data_source: Per-lake data source if applicable; ``None`` for aggregated datasets.
            session: Owned HTTP session for per-lake sensors; ``None`` for aggregated datasets.
            dataset_manager: Dataset coordinator when using aggregated sources.
            aggregated_lookup_key: Key used to extract this lake's reading from the dataset mapping.
        """
        super().__init__(coordinator)
        self.hass = hass
        self._lake = lake_config
        self._data_source = data_source
        self._session = session
        self._dataset_manager = dataset_manager
        self._aggregated_lookup_key = aggregated_lookup_key

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
    async def create(cls, *, hass: HomeAssistant, lake_config: LakeConfig) -> "LakeTemperatureSensor":
        """Create and return a sensor with the appropriate coordinator.

        Depending on the configured source type, this either:
        - Registers the lake with a shared dataset coordinator (Salzburg OGD, Hydro OOE), or
        - Creates a per-lake data source and coordinator (GKD and others).
        """

        source_type = lake_config.source.type

        # Aggregated dataset: Salzburg OGD
        if source_type is LakeSourceType.SALZBURG_OGD:
            manager = get_or_create_dataset_manager(
                hass,
                dataset_id="salzburg_ogd_seen",
                factory=lambda h, did: SalzburgOGDDatasetCoordinator(h, did),
            )
            coordinator, lookup_key = manager.register_lake(lake_config)
            sensor = cls(
                hass=hass,
                lake_config=lake_config,
                coordinator=coordinator,
                data_source=None,
                session=None,
                dataset_manager=manager,
                aggregated_lookup_key=lookup_key,
            )
            return sensor

        # Aggregated dataset: Hydro OOE
        if source_type is LakeSourceType.HYDRO_OOE:
            manager = get_or_create_dataset_manager(
                hass,
                dataset_id=HydroOoeDatasetCoordinator.DATASET_ID,  # type: ignore[attr-defined]
                factory=lambda h, did: HydroOoeDatasetCoordinator(h),
            )
            coordinator, lookup_key = manager.register_lake(lake_config)
            sensor = cls(
                hass=hass,
                lake_config=lake_config,
                coordinator=coordinator,
                data_source=None,
                session=None,
                dataset_manager=manager,
                aggregated_lookup_key=lookup_key,
            )
            return sensor

        # Per-lake coordinator: GKD and others
        timeout = aiohttp.ClientTimeout(total=20)
        headers = {"User-Agent": lake_config.user_agent or DEFAULT_USER_AGENT}
        session = aiohttp.ClientSession(headers=headers, timeout=timeout)

        data_source = create_data_source(lake_config, session=session)

        async def _async_update_data() -> TemperatureReading | None:
            try:
                reading = await data_source.fetch_temperature()
            except Exception as exc:  # noqa: BLE001 - HA expects exceptions for UpdateFailed
                raise UpdateFailed(str(exc)) from exc
            return reading

        coordinator = DataUpdateCoordinator(
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
        return sensor

    @property
    def available(self) -> bool:
        # Aggregated dataset: available only if coordinator succeeded AND this lake has data in the mapping
        data = self.coordinator.data
        if isinstance(data, dict) and self._aggregated_lookup_key:
            return bool(self.coordinator.last_update_success and (self._aggregated_lookup_key in (data or {})))
        # Per-lake: rely on coordinator success
        return self.coordinator.last_update_success

    @property
    def native_value(self) -> float | None:
        data = self.coordinator.data
        reading: TemperatureReading | None
        if isinstance(data, dict) and self._aggregated_lookup_key:
            reading = data.get(self._aggregated_lookup_key)
        else:
            reading = data
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
            age = now - rec_ts
            if age > max_age:
                _LOGGER.debug(
                    "Lake '%s': latest reading is stale (age=%ss > threshold=%ss)",
                    self._lake.name,
                    int(age.total_seconds()),
                    int(max_age.total_seconds()),
                )
                return None
        except Exception:  # noqa: BLE001 - be resilient; do not break state
            return None

        return float(reading.temperature_c)

    @property
    def extra_state_attributes(self) -> Dict[str, Any]:
        data = self.coordinator.data
        reading: TemperatureReading | None
        if isinstance(data, dict) and self._aggregated_lookup_key:
            reading = data.get(self._aggregated_lookup_key)
        else:
            reading = data
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
        """Cleanup resources or unregister from dataset before entity removal."""
        # Unregister from dataset manager if aggregated
        if getattr(self, "_dataset_manager", None) is not None:
            try:
                self._dataset_manager.unregister_lake(self._lake.entity_id)  # type: ignore[union-attr]
            except Exception:
                pass
            return

        # Per-lake cleanup
        try:
            if self._data_source is not None:
                await self._data_source.close()
        finally:
            if self._session is not None and not self._session.closed:
                await self._session.close()

    async def async_added_to_hass(self) -> None:
        """Perform an initial refresh after the entity is added to Home Assistant."""
        await super().async_added_to_hass()
        # Perform an immediate initial refresh so state is available promptly
        try:
            # For aggregated datasets, avoid duplicate refresh if data already present
            if not (isinstance(self.coordinator.data, dict) and self._aggregated_lookup_key):
                await self.coordinator.async_refresh()
        except Exception:  # noqa: BLE001 - best-effort; errors will reflect in availability
            return


