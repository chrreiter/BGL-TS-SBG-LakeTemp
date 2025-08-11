from __future__ import annotations

"""Home Assistant integration setup for BGL-TS-SBG-LakeTemp.

Initializes shared storage under ``hass.data[DOMAIN]`` and prepares platforms.
"""

from typing import Final
import logging

from homeassistant.const import Platform
from homeassistant.core import HomeAssistant

from .const import DOMAIN

_LOGGER = logging.getLogger(__name__)

PLATFORMS: Final[list[Platform]] = [Platform.SENSOR]


async def async_setup(hass: HomeAssistant, config: dict) -> bool:
    """Set up the BGL-TS-SBG-LakeTemp integration from YAML (if present)."""

    if DOMAIN not in hass.data:
        hass.data[DOMAIN] = {}
        _LOGGER.debug("Created domain storage at hass.data[%s]", DOMAIN)
    return True


