# BGL-TS-SBG-LakeTemp
Monitor water temperatures of lakes in Berchtesgadener Land, Traunstein and the Salzkammergut in Home Assistant.

### Quick start (Home Assistant setup)

Add the following block to your Home Assistant `configuration.yaml` (or include it):

```yaml
bgl_ts_sbg_laketemp:
  lakes:
    # GKD Bayern (per‑lake polling)
    - name: Abtsdorfer See
      url: https://www.gkd.bayern.de/de/seen/wassertemperatur/inn/seethal-18673955/messwerte
      entity_id: abtsdorfer_see_bgl
      scan_interval: 7200
      timeout_hours: 24
      source:
        type: gkd_bayern

    # Hydro OOE (shared dataset)
    - name: Irrsee
      entity_id: zeller_see_irrsee_zell_am_moos
      source:
        type: hydro_ooe
        options:
          station_id: "5005"

    # Salzburg OGD (shared dataset)
    - name: Fuschlsee
      entity_id: fuschlsee
      source:
        type: salzburg_ogd
        options:
          lake_name: Fuschlsee
```

The integration creates one sensor per configured lake. Shared datasets (Hydro OOE, Salzburg OGD) perform a single HTTP download per refresh, regardless of how many lakes are configured.


#### Data sources and station lists

- GKD Bayern: list of known station pages — see [`examples/GKDBayern_stations.yaml`](examples/GKDBayern_stations.yaml)
- Hydro OOE (Upper Austria): definitive station list (SANR, names) — see [`examples/HydroOOE_stations.yaml`](examples/HydroOOE_stations.yaml)
- Salzburg OGD (Hydrografie Seen): lake list — see [`examples/SalzburgODG_stations.yaml`](examples/SalzburgODG_stations.yaml)


### Configuration reference

- Required
  - name: Display name (1–100 chars)
  - entity_id: Lowercase slug (letters, digits, underscores; max 64 chars)

- Optional (defaults shown)
  - url: HTTP(S) URL exposed as a sensor attribute
    - GKD Bayern: required and used for scraping
    - Hydro OOE: optional, informational only (data fetched from official ZRXP bulk file)
    - Salzburg OGD: optional, informational only (data fetched from official “Hydrografie Seen” TXT)
  - scan_interval: Polling interval in seconds. Default 1800. Allowed 15–86400
  - timeout_hours: Max age of the latest reading before state becomes `unknown`. Default 24. Allowed 1–336
  - user_agent: HTTP User‑Agent string. Default `Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36`
  - source: Data source config. Default `{ type: gkd_bayern, options: {} }`

- source.type values
  - gkd_bayern (default)
    - options:
      - table_selector (optional): CSS selector for a specific table (rarely needed)
      - station_id (optional): Usually inferred from the URL
  - hydro_ooe
    - options:
      - station_id (optional): Explicit station id (string or int). If omitted, selection uses the top‑level name as a hint
  - salzburg_ogd
    - options:
      - lake_name (optional): Overrides the name used to match a dataset entry

Notes
- If an optional field is omitted, its default applies
- If the latest reading is older than `timeout_hours`, the sensor state is set to `unknown`


### Software architecture (high level)

- Philosophy
  - Minimize HTTP load on upstreams; maximize reuse via shared datasets and sessions
  - Fail safely: stale data is never shown as fresh; errors are logged with context

- Data flow
  - Configuration is validated into typed `LakeConfig`
  - For dataset sources (`hydro_ooe`, `salzburg_ogd`), a shared dataset coordinator downloads once per refresh and updates all member lakes
  - For per‑lake sources (`gkd_bayern`), each lake has its own coordinator and scraper
  - Scrapers use a shared `aiohttp.ClientSession` per dataset or across per‑lake sensors to reuse connections
  - Parsed readings are normalized to a `TemperatureReading` and exposed via `DataUpdateCoordinator` to the sensor entity

- Scheduling and rate limiting
  - Dataset refresh cadence equals the minimum `scan_interval` of all registered lakes in the dataset
  - Per‑domain client‑side rate limiting for per‑lake requests: up to 2 concurrent requests with ≥250 ms between starts
  - Shared User‑Agent per dataset: taken from the first registered lake (or default)

### Adding a new data source (scraper)

1) Implement a scraper
   - Create `custom_components/bgl_ts_sbg_laketemp/scrapers/<your_source>.py`
   - Derive from `AsyncSessionMixin`; implement async fetch and parse into a small record model, exposing e.g. `fetch_latest()`

2) Choose integration model
   - Dataset‑based: add a `BaseDatasetCoordinator` subclass that downloads once and maps results to lake keys
   - Per‑lake: implement a `DataSourceInterface` adapter that fetches one lake at a time

3) Wire it into the integration
   - Add a new enum to `LakeSourceType` and an options typed dict/dataclass in `const.py`
   - Extend `_validate_source_block` and `build_lake_config` for options
   - Update `data_source.create_data_source` (per‑lake) and/or add a dataset coordinator plus a case in `sensor.LakeTemperatureSensor.create`

4) Documentation and examples
   - Add an example snippet to `examples/configuration.yaml`
   - If applicable, provide a stations list file `examples/<Source>_stations.yaml`

5) Tests
   - Add offline tests (`*_offline.py`) and optional online tests (`*_online.py`) covering parsing, selection, and error handling

### Testing

Recommended local workflow (PowerShell on Windows):

1) Create and activate a virtual environment
   - `python -m venv .venv`
   - `\.venv\Scripts\Activate.ps1`
2) Install test dependencies (plus runtime libs used by the scrapers)
   - `python -m pip install -U pip`
   - `pip install pytest pytest-asyncio aioresponses tzdata beautifulsoup4==4.12.3 aiohttp>=3.9.1`
3) Run tests
   - `pytest -q`

Notes
- If you see async/sockets or Home Assistant plugin related issues, run with a clean plugin set:
  - `$env:PYTEST_DISABLE_PLUGIN_AUTOLOAD = '1'` then `pytest -q`
- If you see a ZoneInfo error for `Europe/Berlin`, ensure `tzdata` is installed in your venv (see step 2)

### Online tests (real HTTP; opt‑in)

Online tests perform real HTTP requests and are skipped by default. Enable them by setting `RUN_ONLINE=1` and (optionally) selecting the `online` marker.

- PowerShell (Windows):

```
$env:RUN_ONLINE = '1'; pytest -q -m online
```

Run a specific online test file:

```
$env:RUN_ONLINE = '1'; pytest -q tests/test_gkd_bayern_online.py
$env:RUN_ONLINE = '1'; pytest -q tests/test_hydro_ooe_online.py
$env:RUN_ONLINE = '1'; pytest -q tests/test_salzburg_ogd_online.py
```

Disable again for the session:

```
Remove-Item Env:\RUN_ONLINE
```

- Bash (macOS/Linux):

```
RUN_ONLINE=1 pytest -q -m online
```

Notes for online runs
- You can combine with the clean plugin run if needed:
  - PowerShell: `$env:PYTEST_DISABLE_PLUGIN_AUTOLOAD='1'; $env:RUN_ONLINE='1'; pytest -q -m online`
- Online tests live in `tests/test_gkd_bayern_online.py`, `tests/test_hydro_ooe_online.py`, and `tests/test_salzburg_ogd_online.py`

### Logging

- INFO: dataset coordinator initialization
- DEBUG: refresh summaries (bytes downloaded, lakes updated, current minimum `scan_interval`)
- WARNING: a registered lake missing in the latest dataset snapshot
- ERROR: HTTP/download/parse failures

### Roadmap

- Support flexible scheduling (fixed interval or specific times) aligned with HA scheduling
- Add a Home Assistant config flow for UI‑based setup
- Provide station discovery and selection within the UI
- Automatically infer upstream update cadence where possible
- Add performance metrics and lightweight telemetry for troubleshooting
- Set up CI/CD and repository automations
- Enforce code style and static analysis across the project
