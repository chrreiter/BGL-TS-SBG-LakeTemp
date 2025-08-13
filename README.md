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
- Online tests live in `tests/test_gkd_bayern_online.py` and `tests/test_hydro_ooe_online.py`.

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

    - name: Irrsee / Zell am Moos
      url: https://hydro.ooe.gv.at/#/overview/Wassertemperatur/station/16579/Zell%20am%20Moos/Wassertemperatur?period=P7D
      entity_id: irrsee_zell
      source:
        type: hydro_ooe
        options:
          station_id: "16579"

    - name: Fuschlsee
      url: https://www.salzburg.gv.at/ogd/56c28e2d-8b9e-41ba-b7d6-fa4896b5b48b/Hydrografie%20Seen.txt
      entity_id: fuschlsee
      source:
        type: salzburg_ogd
        options:
          lake_name: Fuschlsee
```