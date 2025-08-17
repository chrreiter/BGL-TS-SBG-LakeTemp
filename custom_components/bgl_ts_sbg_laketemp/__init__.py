from __future__ import annotations

"""Home Assistant integration setup for BGL-TS-SBG-LakeTemp.

Initializes shared storage under ``hass.data[DOMAIN]`` and prepares platforms.
"""

from typing import Final
import logging

from homeassistant.const import Platform
from homeassistant.core import HomeAssistant

from .const import DOMAIN, CONFIG_SCHEMA as _INTEGRATION_CONFIG_SCHEMA, CONF_LAKES, DATASET_STORE

_LOGGER = logging.getLogger(__name__)

PLATFORMS: Final[list[Platform]] = [Platform.SENSOR]

"""Expose the integration's YAML config schema to Home Assistant."""
CONFIG_SCHEMA = _INTEGRATION_CONFIG_SCHEMA


async def async_setup(hass: HomeAssistant, config: dict[str, object]) -> bool:
    """Set up the BGL-TS-SBG-LakeTemp integration from YAML (if present).

    Creates the domain data container and forwards YAML-configured lakes to the
    sensor platform via discovery when applicable.
    """

    if DOMAIN not in hass.data:
        hass.data[DOMAIN] = {}
        _LOGGER.debug("Created domain storage at hass.data[%s]", DOMAIN)
    # Ensure dataset store is present for dataset coordinators
    if DATASET_STORE not in hass.data[DOMAIN]:
        hass.data[DOMAIN][DATASET_STORE] = {}
        _LOGGER.debug("Initialized dataset store at hass.data[%s]['%s']", DOMAIN, DATASET_STORE)

    # Forward YAML-based sensor definitions to the sensor platform via discovery
    domain_cfg = config.get(DOMAIN)
    if isinstance(domain_cfg, dict) and domain_cfg.get(CONF_LAKES):
        try:
            # Import lazily to avoid requiring Home Assistant during unit tests
            from homeassistant.helpers.discovery import async_load_platform  # type: ignore

            discovery_info = {CONF_LAKES: domain_cfg.get(CONF_LAKES)}
            hass.async_create_task(
                async_load_platform(hass, Platform.SENSOR, DOMAIN, discovery_info, config)
            )
            _LOGGER.info("Forwarded %d lake(s) to sensor platform", len(discovery_info[CONF_LAKES]))
        except Exception as exc:  # noqa: BLE001 - log and continue
            _LOGGER.error("Failed to forward configuration to sensor platform: %s", exc, exc_info=True)

    return True


# Convenience re-exports for internal use within this integration package only.
# These utilities are not part of any public Home Assistant API surface.
from .dataset_coordinators import (  # noqa: E402
    BaseDatasetCoordinator,
    get_dataset_manager,
    get_or_create_dataset_manager,
    get_or_create_salzburg_coordinator,
    get_or_create_hydro_ooe_coordinator,
    SalzburgOGDDatasetCoordinator,
    HydroOoeDatasetCoordinator,
)

__all__ = [
    "PLATFORMS",
    "CONFIG_SCHEMA",
    # Dataset coordinator utilities (internal convenience)
    "BaseDatasetCoordinator",
    "get_dataset_manager",
    "get_or_create_dataset_manager",
    "get_or_create_salzburg_coordinator",
    "get_or_create_hydro_ooe_coordinator",
    "SalzburgOGDDatasetCoordinator",
    "HydroOoeDatasetCoordinator",
]

