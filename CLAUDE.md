# CLAUDE.md

Guidance for Claude Code when working in this repository. Keep it accurate; update it when the architecture or workflow changes.

## What this is

`bgl_ts_sbg_laketemp` is a **custom Home Assistant integration** (single-user, HACS-distributable) that reports water temperatures of lakes in Berchtesgadener Land, Traunstein and the Salzkammergut. It scrapes/downloads data from public hydrology portals that have no proper API and exposes one HA `sensor` entity per configured lake.

- Domain: `bgl_ts_sbg_laketemp`
- Config: **YAML only** under `bgl_ts_sbg_laketemp:` in `configuration.yaml` (no config flow / UI setup yet — that's on the roadmap).
- `iot_class`: `cloud_polling`. Runtime deps (see `manifest.json`): `beautifulsoup4==4.12.3`, `aiohttp>=3.9.1`.
- The old `laketemp_monitor` cursorrules file is **historical** — the domain, module layout, and data-source design have all moved on. Trust this file and the code, not the cursorrules.

## Layout

```
custom_components/bgl_ts_sbg_laketemp/
├── __init__.py             # async_setup; forwards YAML lakes to sensor platform via discovery
├── const.py                # DOMAIN, voluptuous schema, LakeConfig/SourceConfig dataclasses, validation
├── sensor.py               # LakeTemperatureSensor (CoordinatorEntity) + async_setup_platform
├── data_source.py          # DataSourceInterface + TemperatureReading + create_data_source (per-lake sources)
├── dataset_coordinators.py # Shared-dataset coordinators, DomainRateLimiter, shared session helpers
├── mixins.py               # AsyncSessionMixin (aiohttp session lifecycle)
├── logging_utils.py        # kv() + log_operation() structured logging helpers
└── scrapers/
    ├── gkd_bayern.py       # GKD Bayern HTML table scraper (per-lake)
    ├── hydro_ooe.py        # Hydro OÖ ZRXP bulk file (shared dataset)
    └── salzburg_ogd.py     # Salzburg OGD "Hydrografie Seen" TXT (shared dataset)
tests/                      # pytest suite (offline mocked + opt-in online)
examples/                   # configuration.yaml + per-source station lists
dev/                        # ha_config_for_dev.yaml for a live HA dev instance
```

## Architecture (how a reading flows)

1. YAML is validated by `CONFIG_SCHEMA` (`const.py`) → each lake becomes a typed `LakeConfig` via `build_lake_config`.
2. `sensor.LakeTemperatureSensor.create` picks an integration model by `source.type`:
   - **Per-lake** (`gkd_bayern`): a `DataSourceInterface` (from `create_data_source`) + its own `DataUpdateCoordinator`. Uses one **shared** `aiohttp.ClientSession` across all per-lake sensors and a per-domain `DomainRateLimiter` (≤2 concurrent, ≥250 ms between starts).
   - **Shared dataset** (`hydro_ooe`, `salzburg_ogd`): lakes register with a `BaseDatasetCoordinator` subclass that downloads the whole dataset **once per refresh** and maps results to each lake. Refresh cadence = the minimum `scan_interval` among registered lakes.
3. Coordinators produce `TemperatureReading(timestamp, temperature_c, source)`; the sensor exposes it as native value + `extra_state_attributes`.
4. **Staleness rule:** if the latest reading is older than `timeout_hours`, `native_value` returns `None` (state `unknown`). `timeout_hours == MAX_TIMEOUT_HOURS` (336) disables the check.

### Key invariants — don't break these
- **Never show stale data as fresh.** Preserve the `timeout_hours` staleness logic in `sensor.py`.
- **Shared datasets do one HTTP GET per refresh**, regardless of lake count. Don't turn dataset sources back into per-lake fetches.
- `AsyncSessionMixin` **never closes an externally supplied session** — only sessions it created itself. The integration-level shared session is closed centrally, not by individual sensors/sources.
- All I/O is **async** (`aiohttp`). No blocking HTTP in the event loop.
- Put constants, defaults, and bounds in `const.py`; validate config with `voluptuous` and return **actionable** error messages (existing tests assert on message text).
- Use structured logging via `logging_utils` (`kv`, `log_operation`) — key=value fields, not ad-hoc f-strings — to match existing log-assertion tests.

## Testing

Home Assistant is **stubbed** in `tests/conftest.py`, so the suite runs **without installing Home Assistant**. Offline tests mock HTTP with `aioresponses`; online tests do real HTTP and are opt-in.

```bash
python -m pytest -q                 # offline suite (currently 118 passed, 4 skipped)
RUN_ONLINE=1 python -m pytest -q -m online   # opt-in real-HTTP tests
```

- Test env deps are pinned in **`requirements-dev.txt`** — install with `pip install -r requirements-dev.txt`.
- Offline test files end in `*_offline.py`; online ones in `*_online.py` (skipped unless `RUN_ONLINE=1`).
- `pytest.ini` disables sockets (`-p no:socket`) and the HA custom-component plugin.

### ⚠️ Dependency gotcha (important)
`aioresponses` (0.7.9, the latest) is **incompatible with aiohttp ≥ 3.12** — you'll see `TypeError: ClientResponse.__init__() missing ... 'stream_writer'` and the whole offline suite fails. This is a **test-tooling** issue, not a bug in this repo.
- Fix: use the pinned `aiohttp==3.11.11` from `requirements-dev.txt`.
- Do **not** "fix" it by loosening the runtime pin in `manifest.json` — HA ships its own aiohttp at runtime; `aiohttp>=3.9.1` there is correct. The constraint is a test-only concern.

## Adding a new data source
See the step-by-step in `README.md` ("Adding a new data source"). In short: add a scraper under `scrapers/` (derive from `AsyncSessionMixin`), add a `LakeSourceType` enum + options dataclass in `const.py`, extend `_validate_source_block`/`build_lake_config`, wire it into `create_data_source` (per-lake) or a new `BaseDatasetCoordinator` + a case in `LakeTemperatureSensor.create`, then add example config + offline/online tests.

## Conventions
- Python 3.11+, `from __future__ import annotations`, full type hints, Google-style docstrings.
- Keep changes minimal and match surrounding style. Prefer editing `const.py` defaults over hard-coding.
- When behavior changes, update the matching offline test and `README.md`.

## Git workflow
- Active development branch for Claude Code work: **`claude/home-assistant-integration-setup-wfwqwa`** (default branch is `main`).
- Commit with clear messages; push with `git push -u origin <branch>`. Do not open a PR unless explicitly asked.
