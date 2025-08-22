# BGL-TS-SBG-LakeTemp
Monitor water temperatures of lakes in Berchtesgadener Land, Traunstein and Salzkammergut in Home Assistant.

### Testing

Recommended local workflow (PowerShell on Windows):

1) Create and activate a virtual environment
   - `python -m venv .venv`
   - `.\.venv\Scripts\Activate.ps1`
2) Install test dependencies (plus runtime libs used by the scraper)
   - `python -m pip install -U pip`
   - `pip install pytest pytest-asyncio aioresponses tzdata beautifulsoup4==4.12.3 aiohttp>=3.9.1`
3) Run tests
   - `pytest -q`

Notes
- If you see async/sockets or Home Assistant plugin related issues, run with a clean plugin set:
  - `$env:PYTEST_DISABLE_PLUGIN_AUTOLOAD = '1'` then `pytest -q`
- If you see a ZoneInfo error for `Europe/Berlin`, ensure `tzdata` is installed in your venv (see step 2).

### Online tests (real HTTP; opt-in)

Online tests perform real HTTP requests and are skipped by default. Enable them by setting `RUN_ONLINE=1` and (optionally) selecting the `online` marker.

- PowerShell (Windows):

```
$env:RUN_ONLINE = '1'; pytest -q -m online
```

Run a specific online test file:

```
$env:RUN_ONLINE = '1'; pytest -q tests/test_gkd_bayern_online.py
$env:RUN_ONLINE = '1'; pytest -q tests/test_hydro_ooe_online.py
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
- Online tests live in `tests/test_gkd_bayern_online.py`, `tests/test_hydro_ooe_online.py`, and `tests/test_salzburg_ogd_online.py`.

### Example YAML configuration

Place in your Home Assistant `configuration.yaml` (or include from a separate file):

```yaml
bgl_ts_sbg_laketemp:
  lakes:
    - name: Seethal / Abtsdorfer See
      url: https://www.gkd.bayern.de/de/seen/wassertemperatur/inn/seethal-18673955/messwerte
      entity_id: seethal_abtsdorfer
      scan_interval: 1800
      timeout_hours: 24
      source:
        type: gkd_bayern
        options:
          table_selector: null

    # Hydro OOE: shared dataset; one ZRXP download for all configured Hydro OOE lakes.
    # The dataset refresh interval is the minimum scan_interval across Hydro OOE lakes.
    - name: Irrsee / Zell am Moos
      url: https://hydro.ooe.gv.at/#/overview/Wassertemperatur/station/16579/Zell%20am%20Moos/Wassertemperatur?period=P7
      entity_id: irrsee_zell
      source:
        type: hydro_ooe
        options:
          station_id: "16579"

    # Salzburg OGD: shared dataset; one TXT download for all configured Salzburg OGD lakes.
    # The dataset refresh interval is the minimum scan_interval across Salzburg OGD lakes.
    - name: Fuschlsee
      entity_id: fuschlsee
      source:
        type: salzburg_ogd
        options:
          lake_name: Fuschlsee
```

### Configuration reference

- **Required fields**
  - **name**: Display name of the lake sensor (1–100 chars).
  - **entity_id**: Sensor entity id slug (lowercase, digits, underscores; max 64 chars), e.g., `seethal_abtsdorfer`.

- **Optional fields (with defaults)**
  - **url**: HTTP(S) URL shown as an informational attribute on the sensor. Semantics by source:
    - GKD Bayern: required and used as the page to scrape.
    - Hydro OOE: optional and informational only; fetching always uses the official ZRXP bulk file.
    - Salzburg OGD: optional and informational only; fetching always uses the official "Hydrografie Seen" TXT file.
  - **entity_id**: Sensor entity id slug (lowercase, digits, underscores; max 64 chars), e.g., `seethal_abtsdorfer`.

- **Optional fields (with defaults)**
  - **scan_interval**: Polling interval in seconds. Default: `1800` (30 minutes). Allowed range: 60–86400.
  - **timeout_hours**: Maximum age of the latest reading before the sensor becomes `unknown`. Default: `24`. Allowed range: 1–336 (14 days).
    - Behavior: If the scraped reading timestamp is older than `timeout_hours`, the sensor state is set to `unknown` to avoid showing stale data. Specify it only when overriding the default; in the example it’s shown once for demonstration.
  - **user_agent**: User-Agent header used for HTTP requests. Default: `Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36`.
    - Per-lake coordinators (e.g., GKD Bayern): the `user_agent` is applied to that lake’s own HTTP session.
    - Shared dataset coordinators (Hydro OOE, Salzburg OGD): a single shared HTTP session is created for the dataset using the first registered lake’s `user_agent` (or the default if omitted). Subsequent lakes’ `user_agent` values are ignored for that dataset. If you need all lakes in a dataset to use a specific UA, configure the same `user_agent` on all of them (or ensure the first registered lake sets it).
  - **source**: Data source configuration block. Default: `{ type: gkd_bayern, options: {} }`.

- **source.type** values
  - **gkd_bayern** (default): Scrapes GKD Bayern lake temperature tables.
    - Behavior: The scraper always targets the explicit `.../tabelle` view derived from the configured URL.
    - **options**:
      - **table_selector** (optional): CSS selector to target a specific table on the page. Usually not needed.
      - **station_id** (optional): Accepted by the schema but typically inferred from the URL; not required for normal use.
  - **hydro_ooe**: Scrapes Hydro OOE (Upper Austria) portal.
    - **options**:
      - **station_id** (optional): Explicit station id (string or int). If omitted, the integration attempts selection based on the top-level `name` as a hint.
  - **salzburg_ogd**: Fetches the Salzburg OGD "Hydrografie Seen" text dataset.
    - **options**:
      - **lake_name** (optional): Overrides the name used to match a lake entry in the dataset. Defaults to the top-level `name`.
    - Note: The configured top-level `url` is informational; the scraper always downloads the official TXT dataset.
Notes
- If an optional field is omitted, its default above applies per lake.
- To override behavior for a specific lake (e.g., stricter freshness), set the optional field in that lake’s block only.

### Behavior change

- Salzburg OGD: The top-level `url` is now informational only. Fetching always uses the official "Hydrografie Seen" TXT dataset endpoint, aligning behavior with Hydro OOE (whose `url` is also informational). Any custom `url` previously set for Salzburg OGD will be ignored for data retrieval.

### How it polls

- Salzburg OGD and Hydro OOE use shared dataset coordinators. All configured lakes for the same source share a single HTTP download per refresh cycle.
- The effective polling interval for a dataset is the minimum `scan_interval` across its registered lakes. Adjust per-lake `scan_interval` to influence how often the dataset is refreshed.
- GKD Bayern continues to use a per-lake coordinator (each lake has its own polling).

#### Rate limiting and connection reuse

- Shared HTTP sessions:
  - Per‑lake scrapers (e.g., GKD Bayern) reuse a single `aiohttp.ClientSession` across all per‑lake sensors to maximize connection reuse.
  - Dataset scrapers (Hydro OOE, Salzburg OGD) have one shared `aiohttp.ClientSession` per dataset.
- Per‑domain rate limiting (client‑side):
  - Applied to per‑lake requests, keyed by the top‑level domain (e.g., `gkd.bayern.de`).
  - Defaults: up to 2 concurrent requests per domain, with at least ~250 ms between request start times (no jitter by default).
  - Purpose: spread out requests to avoid a thundering herd when multiple lakes point to the same domain.
- Download cardinality by source:
  - Hydro OOE: one ZRXP bulk file download per refresh cycle, regardless of how many Hydro OOE lakes you track. Adding lakes does not increase the number of HTTP downloads.
  - Salzburg OGD: one TXT dataset download per refresh cycle, regardless of how many Salzburg OGD lakes you track. Adding lakes does not increase the number of HTTP downloads.
  - GKD Bayern: one table page download per configured lake per refresh cycle. Each additional GKD lake results in an additional HTTP download. Client‑side rate limiting is per domain; any server‑side rate limits are currently untested/unknown.

#### User-Agent behavior for shared datasets

- Shared dataset coordinators use one HTTP session and a single User-Agent string for all lakes in that dataset. The UA is chosen from the first registered lake’s `user_agent` value, falling back to the default if not provided. Changing the `user_agent` on later lakes does not affect the shared session.
- Future extension: the design allows evolving to per-UA dataset partitions (e.g., coordinator keys of `(dataset_id, user_agent)`) if a use case arises. For now, keep `user_agent` consistent across lakes of the same dataset if you need a specific UA.

Logging
- INFO when creating dataset coordinators
- DEBUG on each refresh with bytes downloaded, number of lakes updated, and current minimum `scan_interval`
- WARNING when a registered lake is missing in the latest dataset snapshot
- ERROR on HTTP/download/parse failures

### Testing prompts

- Unit tests cover dataset behavior, including shared polling and error handling. Run all tests with `pytest -q` or select online tests:
  - `tests/test_gkd_bayern_online.py`
  - `tests/test_hydro_ooe_online.py`
  - `tests/test_salzburg_ogd_online.py`
