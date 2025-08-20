from __future__ import annotations

"""Dataset-level coordinators to share updates across multiple lakes.

This module introduces a base class, :class:`BaseDatasetCoordinator`, that
wraps a Home Assistant :class:`DataUpdateCoordinator` which yields a mapping
of per-lake lookup keys to :class:`TemperatureReading` objects. It allows
multiple sensors that belong to the same upstream dataset (e.g., a bulk file
containing many lakes) to share a single polling task and schedule.

Key features:
- Registration API to add/remove lakes to a dataset group
- Automatic recomputation of ``update_interval`` to the minimum of member
  lakes' ``scan_interval`` values
- Storage under ``hass.data[DOMAIN]["datasets"]`` keyed by a dataset-id string

Subclasses implement :meth:`async_update_data` to fetch/produce the full
mapping for the dataset.

User-Agent behavior
-------------------
- Shared dataset coordinators (e.g., Salzburg OGD, Hydro OOE) create a single
  shared ``aiohttp.ClientSession`` for all registered lakes in that dataset.
  The session's ``User-Agent`` is taken from the first-registered lake's
  ``user_agent`` value, or :data:`DEFAULT_USER_AGENT` if not provided. Later
  registrations do not change the UA for the existing shared session.
- Potential future extension: If needed, the dataset store can be partitioned
  by both dataset id and user-agent (e.g., keys of ``(dataset_id, user_agent)``)
  to maintain separate shared sessions per UA.
"""

from datetime import datetime, timedelta, timezone
import asyncio
import random
from urllib.parse import urlparse
import abc
import logging
from typing import Callable, Dict, Iterable, Mapping, MutableMapping, Tuple

import aiohttp
from aiohttp import TooManyRedirects

from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import (
    DataUpdateCoordinator,
    UpdateFailed,
)

from .const import (
    DOMAIN,
    DEFAULT_SCAN_INTERVAL_SECONDS,
    DEFAULT_USER_AGENT,
    LakeConfig,
    LakeSourceType,
    SalzburgOGDOptions,
    HydroOOEOptions,
)
from .data_source import TemperatureReading
from .scrapers.salzburg_ogd import SalzburgOGDScraper
from .scrapers.hydro_ooe import (
    split_zrxp_blocks,
    select_block,
    parse_zrxp_block,
)


_LOGGER = logging.getLogger(__name__)


DATASETS_KEY = "datasets"


# ---- Domain-level rate limiting ----

class _DomainState:
    """Internal state for a single domain's rate limits.

    Enforces a maximum number of concurrent in-flight requests and a minimum
    spacing between request start times (with optional jitter) to avoid
    thundering herd effects.
    """

    def __init__(self, *, max_concurrent: int, min_delay_seconds: float, jitter_seconds: float) -> None:
        # Guard rails
        max_concurrent = max(1, int(max_concurrent))
        min_delay_seconds = max(0.0, float(min_delay_seconds))
        jitter_seconds = max(0.0, float(jitter_seconds))

        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.min_delay_seconds = min_delay_seconds
        self.jitter_seconds = jitter_seconds
        self._lock = asyncio.Lock()
        self._next_earliest_start: float = 0.0

    async def acquire(self) -> None:
        # Cap concurrency first
        await self.semaphore.acquire()
        # Then serialize spacing between starts
        async with self._lock:
            now = asyncio.get_event_loop().time()
            earliest = max(now, self._next_earliest_start)
            delay = max(0.0, earliest - now)
            if delay > 0:
                await asyncio.sleep(delay)
            # Compute next earliest start including min spacing and optional jitter
            jitter = random.uniform(0.0, self.jitter_seconds) if self.jitter_seconds > 0 else 0.0
            self._next_earliest_start = max(earliest, asyncio.get_event_loop().time()) + self.min_delay_seconds + jitter

    def release(self) -> None:
        try:
            self.semaphore.release()
        except ValueError:
            # Defensive: ignore double-release in case of misuse
            pass


class DomainRateLimiter:
    """A simple async per-domain rate limiter.

    Use as:
        limiter = get_domain_rate_limiter(hass)
        async with limiter.acquire_for("https://example.com/path"):
            ...
    """

    def __init__(self, *, max_concurrent: int = 2, min_delay_seconds: float = 0.25, jitter_seconds: float = 0.0) -> None:
        self._states: dict[str, _DomainState] = {}
        self._defaults = (max(1, int(max_concurrent)), max(0.0, float(min_delay_seconds)), max(0.0, float(jitter_seconds)))
        self._lock = asyncio.Lock()

    def _normalize_domain(self, target: str) -> str:
        try:
            parsed = urlparse(target)
            if parsed.scheme and parsed.netloc:
                return parsed.netloc.lower()
        except Exception:
            pass
        return (target or "").strip().lower()

    async def _get_state(self, domain: str) -> _DomainState:
        async with self._lock:
            state = self._states.get(domain)
            if state is None:
                mc, md, jit = self._defaults
                state = _DomainState(max_concurrent=mc, min_delay_seconds=md, jitter_seconds=jit)
                self._states[domain] = state
            return state

    class _Guard:
        def __init__(self, state: _DomainState) -> None:
            self._state = state

        async def __aenter__(self):  # noqa: ANN001 - context manager protocol
            await self._state.acquire()

        async def __aexit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001
            self._state.release()

    async def acquire_for(self, target: str):  # noqa: ANN201 - returns async context manager
        domain = self._normalize_domain(target)
        state = await self._get_state(domain)
        return DomainRateLimiter._Guard(state)


def get_domain_rate_limiter(
    hass: HomeAssistant,
    *,
    max_concurrent: int | None = None,
    min_delay_seconds: float | None = None,
    jitter_seconds: float | None = None,
) -> DomainRateLimiter:
    """Return a singleton domain rate limiter stored under hass.data.

    Optional parameters are only used on first creation.
    """
    store = _get_dataset_store(hass)
    key = "_domain_rate_limiter"
    existing = store.get(key)
    if isinstance(existing, DomainRateLimiter):
        return existing
    limiter = DomainRateLimiter(
        max_concurrent=max_concurrent or 2,
        min_delay_seconds=min_delay_seconds or 0.25,
        jitter_seconds=jitter_seconds or 0.0,
    )
    store[key] = limiter  # type: ignore[assignment]
    return limiter


# Global fallback limiter for scrapers outside HA context
_GLOBAL_LIMITER: DomainRateLimiter | None = None


def get_global_domain_rate_limiter(
    *,
    max_concurrent: int | None = None,
    min_delay_seconds: float | None = None,
    jitter_seconds: float | None = None,
) -> DomainRateLimiter:
    global _GLOBAL_LIMITER
    if _GLOBAL_LIMITER is None:
        _GLOBAL_LIMITER = DomainRateLimiter(
            max_concurrent=max_concurrent or 2,
            min_delay_seconds=min_delay_seconds or 0.25,
            jitter_seconds=jitter_seconds or 0.0,
        )
    return _GLOBAL_LIMITER


# ---- Shared aiohttp session for per-lake sensors ----

_GLOBAL_SHARED_SESSION: aiohttp.ClientSession | None = None

async def _close_shared_session_on_stop(hass: HomeAssistant) -> None:
    try:
        store = _get_dataset_store(hass)
        sess = store.get("_shared_session")
        if isinstance(sess, aiohttp.ClientSession) and not sess.closed:
            await sess.close()
        store["_shared_session"] = None  # type: ignore[index]
    except Exception:
        pass
    # Also close global fallback session if present
    try:
        global _GLOBAL_SHARED_SESSION
        if _GLOBAL_SHARED_SESSION is not None and not _GLOBAL_SHARED_SESSION.closed:
            await _GLOBAL_SHARED_SESSION.close()
        _GLOBAL_SHARED_SESSION = None
    except Exception:
        pass


def get_shared_client_session(
    hass: HomeAssistant,
    *,
    user_agent: str | None = None,
    request_timeout_seconds: float = 20.0,
) -> aiohttp.ClientSession:
    """Return a single shared ClientSession for the integration.

    The first caller defines the User-Agent header; subsequent callers reuse the
    session regardless of User-Agent to maximize connection reuse.
    """
    store = _get_dataset_store(hass)
    existing = store.get("_shared_session")
    if isinstance(existing, aiohttp.ClientSession) and not existing.closed:
        return existing

    timeout = aiohttp.ClientTimeout(total=request_timeout_seconds)
    headers = {"User-Agent": user_agent or DEFAULT_USER_AGENT}
    session = aiohttp.ClientSession(headers=headers, timeout=timeout)
    store["_shared_session"] = session  # type: ignore[index]
    store["_shared_session_ua"] = headers["User-Agent"]  # type: ignore[index]
    # Save in global fallback so tests/contexts without consistent hass can close it
    global _GLOBAL_SHARED_SESSION
    _GLOBAL_SHARED_SESSION = session

    # Register best-effort shutdown hook in real HA
    try:
        bus = getattr(hass, "bus", None)
        if bus is not None and hasattr(bus, "async_listen_once"):
            async def _on_stop(_event) -> None:  # type: ignore[no-untyped-def]
                try:
                    create_task = getattr(hass, "async_create_task", None)
                    if callable(create_task):
                        create_task(_close_shared_session_on_stop(hass))
                    else:
                        loop = getattr(hass, "loop", None)
                        if loop is not None:
                            loop.create_task(_close_shared_session_on_stop(hass))
                except Exception:
                    pass

            bus.async_listen_once("homeassistant_stop", _on_stop)
    except Exception:
        pass

    return session


def _get_dataset_store(hass: HomeAssistant) -> MutableMapping[str, "BaseDatasetCoordinator"]:
    """Return the dataset registry stored under ``hass.data`` for this domain."""

    # Support both real Home Assistant object (with .data) and test stubs using a bare dict
    if hasattr(hass, "data"):
        container = hass.data  # type: ignore[assignment]
    elif isinstance(hass, dict):
        container = hass.setdefault("data", {})  # type: ignore[assignment]
    else:
        raise TypeError("Unsupported hass type for dataset storage")

    domain_store = container.setdefault(DOMAIN, {})
    datasets = domain_store.setdefault(DATASETS_KEY, {})
    return datasets  # type: ignore[return-value]


def get_dataset_manager(
    hass: HomeAssistant,
    dataset_id: str,
) -> "BaseDatasetCoordinator | None":
    """Look up an existing dataset manager by its id, if present."""

    return _get_dataset_store(hass).get(dataset_id)


def get_or_create_dataset_manager(
    hass: HomeAssistant,
    dataset_id: str,
    *,
    factory: Callable[[HomeAssistant, str], "BaseDatasetCoordinator"],
) -> "BaseDatasetCoordinator":
    """Return an existing dataset manager or create one via the provided factory."""

    store = _get_dataset_store(hass)
    manager = store.get(dataset_id)
    if manager is None:
        manager = factory(hass, dataset_id)
        store[dataset_id] = manager
        _LOGGER.info("Created dataset coordinator for dataset_id=%s", dataset_id)
    return manager


class BaseDatasetCoordinator(abc.ABC):
    """Base class managing a shared dataset update coordinator.

    The coordinator produces a mapping of ``lookup_key`` to
    :class:`TemperatureReading`. The key is computed by
    :meth:`get_lookup_key`, which subclasses may override for
    dataset-specific normalization.
    """

    def __init__(self, hass: HomeAssistant, dataset_id: str) -> None:
        self.hass = hass
        self.dataset_id = dataset_id
        self._members_by_entity_id: Dict[str, LakeConfig] = {}
        # Tracks last known availability per lake lookup key (True if present in last mapping)
        self._last_availability_by_key: Dict[str, bool] = {}

        async def _update_wrapper() -> Dict[str, TemperatureReading]:
            # Never let a single failure take down all members; on error, keep previous data
            try:
                full = await self.async_update_data()
            except Exception as exc:  # noqa: BLE001 - do not fail batch-wide
                _LOGGER.error("Dataset %s: update failed; keeping previous data: %s", self.dataset_id, exc)
                # Keep previous mapping if available; otherwise empty
                previous: Dict[str, TemperatureReading] = {}
                try:
                    if isinstance(self.coordinator.data, dict) and self.coordinator.data is not None:
                        previous = dict(self.coordinator.data)
                except Exception:
                    previous = {}
                return previous

            # Only keep entries for currently registered lakes
            allowed_keys = {self.get_lookup_key(cfg) for cfg in self._members_by_entity_id.values()}
            if not allowed_keys:
                return {}
            filtered: Dict[str, TemperatureReading] = {}
            for key, reading in full.items():
                if key in allowed_keys:
                    filtered[key] = reading

            # Transition-aware availability logging per lake
            try:
                # Compute new availability per key: present in mapping => available
                new_availability: Dict[str, bool] = {}
                # Build helper map: key -> lake name for clearer logs
                key_to_name: Dict[str, str] = {}
                for cfg in self._members_by_entity_id.values():
                    k = self.get_lookup_key(cfg)
                    key_to_name[k] = cfg.name
                    new_availability[k] = k in filtered

                for key, now_avail in new_availability.items():
                    before = self._last_availability_by_key.get(key)
                    if before is None:
                        continue  # no transition on first observation
                    if before and not now_avail:
                        _LOGGER.warning(
                            "Dataset %s: lake '%s' (key=%s) transitioned to unavailable",
                            self.dataset_id,
                            key_to_name.get(key, key),
                            key,
                        )
                    elif (not before) and now_avail:
                        _LOGGER.info(
                            "Dataset %s: lake '%s' (key=%s) recovered to available",
                            self.dataset_id,
                            key_to_name.get(key, key),
                            key,
                        )

                self._last_availability_by_key = new_availability
            except Exception:
                # Never break updates due to logging/transitions
                pass

            return filtered

        self._backoff_attempts: int = 0
        self._backoff_override_seconds: int | None = None

        self.coordinator: DataUpdateCoordinator[Dict[str, TemperatureReading]] = DataUpdateCoordinator(
            hass,
            _LOGGER,
            name=f"{DOMAIN}:dataset:{dataset_id}",
            update_method=_update_wrapper,
            update_interval=timedelta(seconds=DEFAULT_SCAN_INTERVAL_SECONDS),
        )

    # --------- Public API ---------

    def register_lake(self, lake_config: LakeConfig) -> Tuple[DataUpdateCoordinator, str]:
        """Register a lake and return the shared coordinator and its lookup key."""

        if lake_config.entity_id in self._members_by_entity_id:
            # Idempotent: return existing mapping
            return self.coordinator, self.get_lookup_key(lake_config)

        self._members_by_entity_id[lake_config.entity_id] = lake_config
        self.recompute_update_interval()
        _LOGGER.debug(
            "Dataset %s: registered lake '%s' (entity_id=%s) with scan_interval=%ss",
            self.dataset_id,
            lake_config.name,
            lake_config.entity_id,
            lake_config.scan_interval,
        )
        return self.coordinator, self.get_lookup_key(lake_config)

    def unregister_lake(self, entity_id: str) -> None:
        """Unregister a lake by its entity_id and recompute scheduling."""

        if entity_id in self._members_by_entity_id:
            removed = self._members_by_entity_id.pop(entity_id)
            _LOGGER.debug(
                "Dataset %s: unregistered lake '%s' (entity_id=%s)",
                self.dataset_id,
                removed.name,
                entity_id,
            )
            self.recompute_update_interval()

    def get_lookup_key(self, lake_config: LakeConfig) -> str:  # noqa: D401 - trivial
        """Return the stable lookup key for a lake (default: ``entity_id``)."""

        return lake_config.entity_id

    def recompute_update_interval(self) -> None:
        """Set coordinator.update_interval to the min of members' scan_interval.

        If there are no members, fall back to :data:`DEFAULT_SCAN_INTERVAL_SECONDS`.
        """

        if self._backoff_override_seconds is not None:
            # Respect active backoff / retry-after override
            seconds = int(self._backoff_override_seconds)
        else:
            if not self._members_by_entity_id:
                seconds = DEFAULT_SCAN_INTERVAL_SECONDS
            else:
                seconds = min(cfg.scan_interval for cfg in self._members_by_entity_id.values())
        self.coordinator.update_interval = timedelta(seconds=seconds)
        _LOGGER.debug(
            "Dataset %s: update_interval set to %ss (members=%d)",
            self.dataset_id,
            seconds,
            len(self._members_by_entity_id),
        )

    # --------- To be implemented by subclasses ---------

    @abc.abstractmethod
    async def async_update_data(self) -> Dict[str, TemperatureReading]:
        """Fetch the full dataset and return mapping of lookup_key -> reading."""


def get_or_create_salzburg_coordinator(
    hass: HomeAssistant, lake_config: LakeConfig
) -> SalzburgOGDDatasetCoordinator:
    """Return a shared Salzburg OGD dataset coordinator for all lakes.

    The dataset id is a fixed string to group all Salzburg OGD lakes together.
    """

    manager = get_or_create_dataset_manager(
        hass,
        dataset_id="salzburg_ogd_seen",
        factory=lambda h, did: SalzburgOGDDatasetCoordinator(h, did),
    )
    assert isinstance(manager, SalzburgOGDDatasetCoordinator)
    return manager


def get_or_create_hydro_ooe_coordinator(
    hass: HomeAssistant, lake_config: LakeConfig
) -> HydroOoeDatasetCoordinator:
    """Return the single Hydro OOE dataset coordinator shared by all lakes."""

    manager = get_or_create_dataset_manager(
        hass,
        dataset_id=HydroOoeDatasetCoordinator.DATASET_ID,  # type: ignore[attr-defined]
        factory=lambda h, did: HydroOoeDatasetCoordinator(h),
    )
    assert isinstance(manager, HydroOoeDatasetCoordinator)
    return manager


class SalzburgOGDDatasetCoordinator(BaseDatasetCoordinator):
    """Dataset coordinator for Salzburg OGD with shared session (no custom URL).

    - Maintains a single shared ``aiohttp.ClientSession`` using the first
      registered lake's ``user_agent`` or :data:`DEFAULT_USER_AGENT`
    - Stores raw target lake names at registration time for efficient bulk fetch
    - Coordinator data is a dict keyed by normalized lake keys
    """

    def __init__(self, hass: HomeAssistant, dataset_id: str) -> None:
        super().__init__(hass, dataset_id)
        self._session: aiohttp.ClientSession | None = None
        self._raw_target_names_by_entity_id: Dict[str, str] = {}
        self._ua: str | None = None
        _LOGGER.info("Initialized SalzburgOGD dataset coordinator (dataset_id=%s)", dataset_id)

        # Best-effort: close shared session on Home Assistant shutdown (real HA only)
        try:
            bus = getattr(self.hass, "bus", None)
            if bus is not None and hasattr(bus, "async_listen_once"):
                # Avoid importing EVENT_HOMEASSISTANT_STOP constant to keep tests lightweight
                async def _on_stop(_event) -> None:  # type: ignore[no-untyped-def]
                    try:
                        # Schedule close to avoid blocking shutdown flow
                        create_task = getattr(self.hass, "async_create_task", None)
                        if callable(create_task):
                            create_task(self.async_close())
                        else:
                            # Fallback to loop if available
                            loop = getattr(self.hass, "loop", None)
                            if loop is not None:
                                loop.create_task(self.async_close())
                    except Exception:
                        pass

                bus.async_listen_once("homeassistant_stop", _on_stop)
        except Exception:
            # In tests, hass is often a dict without a bus; ignore gracefully
            pass

    def register_lake(self, lake_config: LakeConfig) -> Tuple[DataUpdateCoordinator, str]:
        """Register a lake and ensure a shared session exists.

        Returns the shared coordinator and the lookup key under which the lake's
        readings will be available in the coordinator data mapping.
        """
        # Track raw target name used for fetch_all_latest (options.lake_name or config name)
        raw_name = lake_config.name
        if isinstance(lake_config.source.options, SalzburgOGDOptions):
            if lake_config.source.options.lake_name:
                raw_name = lake_config.source.options.lake_name

        # Create shared session lazily on first registration
        if self._session is None:
            self._ua = lake_config.user_agent or DEFAULT_USER_AGENT
            timeout = aiohttp.ClientTimeout(total=20)
            headers = {"User-Agent": self._ua}
            self._session = aiohttp.ClientSession(headers=headers, timeout=timeout)

        self._raw_target_names_by_entity_id[lake_config.entity_id] = raw_name

        return super().register_lake(lake_config)

    def unregister_lake(self, entity_id: str) -> None:
        """Unregister a lake and best-effort close the shared session if unused."""
        self._raw_target_names_by_entity_id.pop(entity_id, None)
        super().unregister_lake(entity_id)
        if not self._members_by_entity_id and self._session is not None:
            # Best-effort close; do not await here (sync method). Use create_task.
            try:
                loop = getattr(self.hass, "loop", None)
                if loop is not None:
                    loop.create_task(self.async_close())
                else:
                    # In tests without real loop, just drop reference
                    # and let GC handle the actual session object
                    self._session = None
            except Exception:
                self._session = None

    async def async_close(self) -> None:
        """Close the shared aiohttp session if it exists.

        Safe to call multiple times.
        """
        if self._session is not None:
            try:
                if not self._session.closed:
                    await self._session.close()
            finally:
                self._session = None

    def get_lookup_key(self, lake_config: LakeConfig) -> str:
        """Return the normalized key used to index records for this lake."""
        # Normalized key for consistent mapping
        name = lake_config.name
        if isinstance(lake_config.source.options, SalzburgOGDOptions) and lake_config.source.options.lake_name:
            name = lake_config.source.options.lake_name
        return SalzburgOGDScraper._normalize_lake_key(name)

    async def async_update_data(self) -> Dict[str, TemperatureReading]:
        """Fetch the OGD file and build a mapping of normalized lake key to reading."""
        target_lakes = list(self._raw_target_names_by_entity_id.values())

        # Ensure a session exists (registration should have created one, but be defensive)
        if self._session is None:
            timeout = aiohttp.ClientTimeout(total=20)
            headers = {"User-Agent": self._ua or DEFAULT_USER_AGENT}
            self._session = aiohttp.ClientSession(headers=headers, timeout=timeout)

        # Fetch and aggregate using scraper
        try:
            async with SalzburgOGDScraper(session=self._session, user_agent=self._ua or DEFAULT_USER_AGENT) as scraper:
                records = await scraper.fetch_all_latest(target_lakes=target_lakes)
                bytes_downloaded = scraper.last_bytes_downloaded
            # Success clears backoff
            self._backoff_attempts = 0
            self._backoff_override_seconds = None
            # After clearing backoff, recompute to reflect configured scan intervals
            self.recompute_update_interval()
        except Exception as exc:  # noqa: BLE001
            _LOGGER.error("SalzburgOGD refresh failed: %s", exc)
            raise

        result: Dict[str, TemperatureReading] = {}
        for rec in records.values():
            key = SalzburgOGDScraper._normalize_lake_key(rec.lake_name)
            result[key] = TemperatureReading(
                timestamp=rec.timestamp,
                temperature_c=rec.temperature_c,
                source=LakeSourceType.SALZBURG_OGD.value,
            )

        # Log refresh summary
        min_scan = min((cfg.scan_interval for cfg in self._members_by_entity_id.values()), default=DEFAULT_SCAN_INTERVAL_SECONDS)
        _LOGGER.debug(
            "SalzburgOGD refresh: bytes_downloaded=%s, lakes_updated=%d, min_scan_interval=%ss",
            bytes_downloaded if ("bytes_downloaded" in locals() and bytes_downloaded is not None) else "unknown",
            len(result),
            min_scan,
        )

        # Warn about missing members
        expected_keys = {self.get_lookup_key(cfg) for cfg in self._members_by_entity_id.values()}
        missing = expected_keys - set(result.keys())
        if missing:
            for cfg in self._members_by_entity_id.values():
                key = self.get_lookup_key(cfg)
                if key in missing:
                    _LOGGER.warning(
                        "SalzburgOGD dataset missing lake in latest data: name=%s (key=%s)",
                        cfg.name,
                        key,
                    )

        return result


class HydroOoeDatasetCoordinator(BaseDatasetCoordinator):
    """Dataset coordinator for Hydro OOE ZRXP export (single dataset).

    - Dataset key is fixed to "hydro_ooe_zrxp"
    - Maintains a single shared ``aiohttp.ClientSession`` using the first
      registered lake's ``user_agent`` or :data:`DEFAULT_USER_AGENT`
    - Stores per-lake selection hints at registration time:
        - Prefer explicit ``options.station_id`` (SANR) if numeric
        - Otherwise store ``name_hint`` as the lake's configured name
    - Performs one ZRXP download per refresh and maps results by stable keys
      (SANR if known, else normalized name)
    """

    DATASET_ID = "hydro_ooe_zrxp"

    def __init__(self, hass: HomeAssistant, dataset_id: str | None = None) -> None:
        super().__init__(hass, dataset_id or self.DATASET_ID)
        self._session: aiohttp.ClientSession | None = None
        self._ua: str | None = None
        # Per-lake selection info
        self._sanr_by_entity_id: Dict[str, str | None] = {}
        self._name_hint_by_entity_id: Dict[str, str | None] = {}
        # Tracks the last matched SANR for each registered lake after a refresh
        self._last_sanr_by_entity_id: Dict[str, str | None] = {}
        _LOGGER.info("Initialized HydroOOE dataset coordinator (dataset_id=%s)", self.dataset_id)

        # Best-effort: close shared session on Home Assistant shutdown (real HA only)
        try:
            bus = getattr(self.hass, "bus", None)
            if bus is not None and hasattr(bus, "async_listen_once"):
                async def _on_stop(_event) -> None:  # type: ignore[no-untyped-def]
                    try:
                        create_task = getattr(self.hass, "async_create_task", None)
                        if callable(create_task):
                            create_task(self.async_close())
                        else:
                            loop = getattr(self.hass, "loop", None)
                            if loop is not None:
                                loop.create_task(self.async_close())
                    except Exception:
                        pass

                bus.async_listen_once("homeassistant_stop", _on_stop)
        except Exception:
            pass

    def register_lake(self, lake_config: LakeConfig) -> Tuple[DataUpdateCoordinator, str]:
        """Register a lake and compute its stable lookup key (prefer SANR)."""
        sanr_val: str | None = None
        # Always keep a name hint to allow graceful fallback if SANR is wrong or missing
        name_hint: str | None = lake_config.name
        if isinstance(lake_config.source.options, HydroOOEOptions):
            if lake_config.source.options.station_id is not None:
                # Accept both numeric and string station_id; prefer as SANR if numeric
                sid = str(lake_config.source.options.station_id)
                if sid.isdigit():
                    sanr_val = sid

        if self._session is None:
            self._ua = lake_config.user_agent or DEFAULT_USER_AGENT
            timeout = aiohttp.ClientTimeout(total=20)
            headers = {"User-Agent": self._ua}
            self._session = aiohttp.ClientSession(headers=headers, timeout=timeout)

        self._sanr_by_entity_id[lake_config.entity_id] = sanr_val
        self._name_hint_by_entity_id[lake_config.entity_id] = name_hint

        return super().register_lake(lake_config)

    def unregister_lake(self, entity_id: str) -> None:
        """Unregister a lake and best-effort close the shared session if unused."""
        self._sanr_by_entity_id.pop(entity_id, None)
        self._name_hint_by_entity_id.pop(entity_id, None)
        self._last_sanr_by_entity_id.pop(entity_id, None)
        super().unregister_lake(entity_id)
        if not self._members_by_entity_id and self._session is not None:
            try:
                loop = getattr(self.hass, "loop", None)
                if loop is not None:
                    loop.create_task(self.async_close())
                else:
                    self._session = None
            except Exception:
                self._session = None

    async def async_close(self) -> None:
        """Close the shared aiohttp session if it exists.

        Safe to call multiple times.
        """
        if self._session is not None:
            try:
                if not self._session.closed:
                    await self._session.close()
            finally:
                self._session = None

    def get_lookup_key(self, lake_config: LakeConfig) -> str:
        """Return stable key for a lake: SANR if known, else normalized name."""
        # Prefer SANR if we have it; else use normalized name as stable key
        sanr = self._sanr_by_entity_id.get(lake_config.entity_id)
        if sanr and sanr.isdigit():
            return sanr
        # Normalize: lowercase alnum of name (simple stable key)
        import re as _re
        return _re.sub(r"[^a-z0-9]+", "", lake_config.name.lower())

    async def async_update_data(self) -> Dict[str, TemperatureReading]:
        """Download and parse the ZRXP export and return mapping of key -> reading."""
        # Download ZRXP once
        if self._session is None:
            timeout = aiohttp.ClientTimeout(total=20)
            headers = {"User-Agent": self._ua or DEFAULT_USER_AGENT}
            self._session = aiohttp.ClientSession(headers=headers, timeout=timeout)

        # Fetch file
        bytes_downloaded: int = 0
        url = "https://data.ooe.gv.at/files/hydro/HDOOE_Export_WT.zrxp"
        try:
            assert self._session is not None
            async with self._session.get(url, max_redirects=5) as resp:
                # Handle HTTP status cases explicitly
                if resp.status == 404:
                    _LOGGER.warning("HydroOOE dataset returned 404 (not found); skipping update this cycle")
                    # Do not mark failure, keep previous data; apply a small backoff to avoid hot-looping
                    self._apply_backoff(base_seconds=self._current_min_scan_interval_seconds(), factor=1.2, cap_seconds=1800)
                    # Raise sentinel to indicate no-op update
                    raise _SkipUpdate()
                if resp.status == 429:
                    retry_after = resp.headers.get("Retry-After")
                    seconds = _parse_retry_after_seconds(retry_after)
                    if seconds is None:
                        # Default conservative retry of 300s
                        seconds = 300
                    _LOGGER.warning("HydroOOE dataset 429 Too Many Requests; respecting Retry-After=%ss", seconds)
                    self._apply_retry_after(seconds)
                    raise _SkipUpdate()
                if 500 <= resp.status < 600:
                    # Server error: mark failure and apply exponential backoff
                    _LOGGER.error("HydroOOE dataset server error: HTTP %s", resp.status)
                    self._apply_backoff(base_seconds=self._current_min_scan_interval_seconds())
                    resp.raise_for_status()

                # Normal success path
                resp.raise_for_status()
                raw = await resp.read()
                bytes_downloaded = len(raw)
                text = raw.decode(resp.charset or "utf-8", errors="replace")
            # Success: clear backoff and recompute schedule
            self._backoff_attempts = 0
            self._backoff_override_seconds = None
            self.recompute_update_interval()
        except _SkipUpdate:
            # Keep previous data unchanged by returning the current coordinator data
            return dict(self.coordinator.data or {})
        except TooManyRedirects as exc:
            _LOGGER.error("HydroOOE dataset exceeded redirect limit: %s", exc)
            self._apply_backoff(base_seconds=self._current_min_scan_interval_seconds())
            raise
        except Exception as exc:  # noqa: BLE001
            _LOGGER.error("HydroOOE refresh failed during download: %s", exc)
            self._apply_backoff(base_seconds=self._current_min_scan_interval_seconds())
            raise

        blocks = split_zrxp_blocks(text)

        result: Dict[str, TemperatureReading] = {}
        # For each registered lake, select and parse the block
        for entity_id, cfg in list(self._members_by_entity_id.items()):
            sanr = self._sanr_by_entity_id.get(entity_id)
            name_hint = self._name_hint_by_entity_id.get(entity_id)
            try:
                block = select_block(blocks, sanr=sanr, name_hint=name_hint)
            except Exception as exc:  # noqa: BLE001
                _LOGGER.error("HydroOOE selection failed for lake=%s: %s", cfg.name, exc)
                continue
            if not block:
                continue
            try:
                records = parse_zrxp_block(block)
                if not records:
                    continue
                latest = records[-1]
            except Exception:  # noqa: BLE001
                _LOGGER.error("HydroOOE parse failed for lake=%s (sanr=%s)", cfg.name, sanr or "-")
                continue

            # Extract SANR from the selected block and remember it
            try:
                import re as _re
                m = _re.search(r"#SANR(\d+)", block)
                matched_sanr = m.group(1) if m else None
                self._last_sanr_by_entity_id[entity_id] = matched_sanr
            except Exception:
                self._last_sanr_by_entity_id[entity_id] = None

            # Build stable key: SANR if known, else normalized name
            key = sanr if (sanr and sanr.isdigit()) else self.get_lookup_key(cfg)
            # Only keep the best (newest) reading per key in case selection
            # heuristics ever target the same station for two configs
            prev = result.get(key)
            if prev is None or latest.timestamp > prev.timestamp:
                result[key] = TemperatureReading(
                    timestamp=latest.timestamp,
                    temperature_c=latest.temperature_c,
                    source=LakeSourceType.HYDRO_OOE.value,
                )

        # Log refresh summary
        min_scan = min((cfg.scan_interval for cfg in self._members_by_entity_id.values()), default=DEFAULT_SCAN_INTERVAL_SECONDS)
        _LOGGER.debug(
            "HydroOOE refresh: bytes_downloaded=%d, lakes_updated=%d, min_scan_interval=%ss",
            bytes_downloaded,
            len(result),
            min_scan,
        )

        # Warn about missing members
        expected_keys: set[str] = set()
        for cfg in self._members_by_entity_id.values():
            sanr = self._sanr_by_entity_id.get(cfg.entity_id)
            key = sanr if (sanr and sanr.isdigit()) else self.get_lookup_key(cfg)
            expected_keys.add(key)
        missing = expected_keys - set(result.keys())
        if missing:
            for cfg in self._members_by_entity_id.values():
                sanr = self._sanr_by_entity_id.get(cfg.entity_id)
                key = sanr if (sanr and sanr.isdigit()) else self.get_lookup_key(cfg)
                if key in missing:
                    _LOGGER.warning(
                        "HydroOOE dataset missing lake in latest data: name=%s (key=%s)",
                        cfg.name,
                        key,
                    )

        return result

    # ---- Scheduling helpers ----
    def _current_min_scan_interval_seconds(self) -> int:
        if not self._members_by_entity_id:
            return DEFAULT_SCAN_INTERVAL_SECONDS
        return min(cfg.scan_interval for cfg in self._members_by_entity_id.values())

    def _apply_backoff(self, *, base_seconds: int, factor: float = 2.0, cap_seconds: int = 3600) -> None:
        """Apply exponential backoff to coordinator scheduling.

        Increases attempts counter and sets an override update_interval derived
        from the current minimum scan interval.
        """
        try:
            self._backoff_attempts = min(self._backoff_attempts + 1, 8)
            next_seconds = int(min(cap_seconds, base_seconds * (factor ** self._backoff_attempts)))
            self._backoff_override_seconds = max(base_seconds, next_seconds)
            self.coordinator.update_interval = timedelta(seconds=self._backoff_override_seconds)
            _LOGGER.debug(
                "Dataset %s: applied backoff (attempts=%d, update_interval=%ss)",
                self.dataset_id,
                self._backoff_attempts,
                self._backoff_override_seconds,
            )
        except Exception:
            # Never raise from scheduling tweaks
            pass

    def _apply_retry_after(self, seconds: int) -> None:
        """Set coordinator scheduling to respect a Retry-After delay."""
        try:
            self._backoff_override_seconds = max(1, int(seconds))
            self.coordinator.update_interval = timedelta(seconds=self._backoff_override_seconds)
            _LOGGER.debug(
                "Dataset %s: applied Retry-After override (update_interval=%ss)",
                self.dataset_id,
                self._backoff_override_seconds,
            )
        except Exception:
            pass

    # ---- Introspection helpers for sensors ----
    def get_last_sanr_for_entity(self, entity_id: str) -> str | None:
        """Return the last matched SANR for a registered lake, if known.

        This reflects the SANR parsed from the most recent successful selection
        during :meth:`async_update_data`. It may be ``None`` if no selection has
        been made yet or the match could not be determined.
        """
        return self._last_sanr_by_entity_id.get(entity_id)


__all__ = [
    "BaseDatasetCoordinator",
    "DomainRateLimiter",
    "get_domain_rate_limiter",
    "get_dataset_manager",
    "get_or_create_dataset_manager",
    "get_or_create_salzburg_coordinator",
    "get_or_create_hydro_ooe_coordinator",
    "SalzburgOGDDatasetCoordinator",
    "HydroOoeDatasetCoordinator",
    "DATASETS_KEY",
]


# ---- Internal helpers ----

class _SkipUpdate(Exception):
    """Internal sentinel to indicate skipping update without failure."""


def _parse_retry_after_seconds(header_value: str | None) -> int | None:
    """Parse Retry-After header which may be seconds or HTTP-date.

    Returns seconds until retry if parseable; otherwise None.
    """
    if not header_value:
        return None
    try:
        # First try integer seconds
        sec = int(header_value.strip())
        if sec >= 0:
            return sec
    except Exception:
        pass
    # Try HTTP-date formats
    try:
        import email.utils as eut

        dt = eut.parsedate_to_datetime(header_value)
        if dt is None:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        delta = int((dt - now).total_seconds())
        return max(0, delta)
    except Exception:
        return None


