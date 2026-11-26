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
    CONF_SOURCE,
    DEFAULT_SCAN_INTERVAL_SECONDS,
    DEFAULT_TIMEOUT_HOURS,
    MAX_TIMEOUT_HOURS,
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
    get_shared_client_session,
    get_domain_rate_limiter,
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
            try:
                name = raw.get(CONF_NAME)
                source_type = None
                if isinstance(raw.get(CONF_SOURCE), dict):
                    source_type = raw[CONF_SOURCE].get("type")
            except Exception:
                name = None
                source_type = None
            _LOGGER.error(
                "Invalid lake configuration at index %s (name=%r, source=%r): %s",
                idx,
                name,
                source_type,
                exc,
                exc_info=True,
            )
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

    Per-lake sensors (GKD Bayern) own a ``DataUpdateCoordinator`` and share one
    integration-wide ``aiohttp.ClientSession``; shared-dataset sensors (Hydro OÖ,
    Salzburg OGD) read their reading from a dataset coordinator's mapping.
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
            session: Shared HTTP session for per-lake sensors; ``None`` for aggregated datasets.
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
        # Reuse a single shared session across all per-lake sensors
        session = get_shared_client_session(hass, user_agent=lake_config.user_agent or DEFAULT_USER_AGENT, request_timeout_seconds=20.0)

        data_source = create_data_source(lake_config, session=session)

        async def _async_update_data() -> TemperatureReading | None:
            try:
                # Apply per-domain rate limiting if a URL is available
                limiter = get_domain_rate_limiter(hass)
                url = lake_config.url or ""
                async with await limiter.acquire_for(url):
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

    def _current_reading(self) -> TemperatureReading | None:
        """Return this lake's latest reading from the coordinator, if any."""
        data = self.coordinator.data
        if isinstance(data, dict) and self._aggregated_lookup_key:
            return data.get(self._aggregated_lookup_key)
        return data

    def _is_stale(self, reading: TemperatureReading) -> bool:
        """Return ``True`` if ``reading`` is older than the configured ``timeout_hours``.

        ``timeout_hours == MAX_TIMEOUT_HOURS`` disables the check. Naive timestamps
        are treated as UTC. Any error while comparing counts as stale so that a
        reading of unknown age is never shown as fresh.
        """
        configured_timeout_hours = self._lake.timeout_hours or DEFAULT_TIMEOUT_HOURS
        if configured_timeout_hours >= MAX_TIMEOUT_HOURS:
            return False
        try:
            max_age = timedelta(hours=configured_timeout_hours)
            ts = reading.timestamp
            rec_ts = ts.replace(tzinfo=timezone.utc) if ts.tzinfo is None else ts.astimezone(timezone.utc)
            age = datetime.now(timezone.utc) - rec_ts
        except Exception:  # noqa: BLE001 - be resilient; never show data of unknown age
            return True
        if age > max_age:
            _LOGGER.debug(
                "Lake '%s': latest reading is stale (age=%ss > threshold=%ss)",
                self._lake.name,
                int(age.total_seconds()),
                int(max_age.total_seconds()),
            )
            return True
        return False

    @property
    def available(self) -> bool:
        """Return whether the data source could be read for this lake.

        Reflects fetch failures only: the coordinator's last update must have
        succeeded and, for shared datasets, this lake must be present in the
        dataset. Stale readings are handled by :attr:`native_value` (state
        ``unknown``), consistently for all source types.
        """
        if not self.coordinator.last_update_success:
            return False
        if self._aggregated_lookup_key:
            data = self.coordinator.data
            return isinstance(data, dict) and data.get(self._aggregated_lookup_key) is not None
        return True

    @property
    def native_value(self) -> float | None:
        """Return the latest temperature, or ``None`` (``unknown``) if missing or stale."""
        reading = self._current_reading()
        if reading is None or self._is_stale(reading):
            return None
        return float(reading.temperature_c)

    @property
    def extra_state_attributes(self) -> Dict[str, Any]:
        reading = self._current_reading()
        attrs: Dict[str, Any] = {
            "lake_name": self._lake.name,
            "source_type": getattr(reading, "source", None) if reading else None,
            "url": self._lake.url,
            ATTR_ATTRIBUTION: "Data courtesy of public hydrology portals",
        }
        # Surface SANR for Hydro OOE dataset if available
        try:
            if isinstance(self._dataset_manager, HydroOoeDatasetCoordinator):
                sanr = self._dataset_manager.get_last_sanr_for_entity(self._lake.entity_id)  # type: ignore[attr-defined]
                if sanr:
                    attrs["sanr"] = sanr
        except Exception:
            pass
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
            # Session is shared at integration level; do not close here
            pass

    async def async_added_to_hass(self) -> None:
        """Perform the initial dataset refresh after the entity is added to Home Assistant.

        Per-lake coordinators are already refreshed once in ``async_setup_platform``,
        so only shared-dataset coordinators that have no data yet are refreshed here
        (the first sensor of a dataset triggers it; later ones reuse the result).
        """
        await super().async_added_to_hass()
        try:
            if self._dataset_manager is not None and self.coordinator.data is None:
                await self.coordinator.async_refresh()
        except Exception:  # noqa: BLE001 - best-effort; errors will reflect in availability
            return


