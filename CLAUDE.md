# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Public dataset and browsable web interface for Milwaukee County Medical Examiner case data, extracted from the office's Power BI government dashboard. The site is hosted on GitHub Pages at `nick.semenkovich.com/milwaukee-medical-examiner/`.

## Commands

```bash
# Full pipeline: fetch → geocode → post-process
python3 fetch_cases.py --output-dir data --full   # Power BI API → CSVs + metadata.json
python3 geocode.py                                 # Census Bureau → geocache.json
python3 postprocess.py                             # CSVs → trends.json

# Serve the site locally (then open http://localhost:8000)
python3 -m http.server
```

Dependencies: `pip install requests`

## Architecture

### Pipeline: fetch → geocode → post-process

Three scripts run in sequence (and in the weekly GitHub Action):

**`fetch_cases.py`** — Queries two Power BI tables via the public REST API (no auth — anonymous embed):

- **Table 1** (`vwPublicDataAccess`): Demographics — CaseNum, DeathDate, Age, Gender, Race, Mode, CauseA, CaseType, etc.
- **Table 2** (`vwPublicDataAccess (2)`): Address detail — DeathAddr, DeathCity, DeathZip, DeathState

Both tables are fetched in full (paginated at 30K rows via RestartTokens), deduplicated by `CaseNum_STR`, merged, then split into files:

- `data/archive/pre-2020.csv` — all cases before 2020
- `data/{year}/{month}.csv` — monthly files from 2020 onward
- `data/metadata.json` — manifest listing all files, row counts, timestamps

Files are sorted by CaseNum_STR for stable git diffs. Only files with actual content changes are overwritten.

**`geocode.py`** — Batch geocodes unique addresses from all CSVs via the US Census Bureau Geocoder API (free, no API key). Results are cached in `data/geocache.json` (address string → `[lat, lng]` or `null` for no match). Runs incrementally — only geocodes addresses not already in the cache. ~96% match rate.

**`postprocess.py`** — Reads all CSVs and generates derived data files. Currently produces `data/trends.json` (yearly aggregates for the Trends tab charts). Keeps derived-data logic separate from the fragile Power BI API fetch code.

### Power BI response format

Responses use compressed dictionary encoding:
- `DS[0].ValueDicts` maps dictionary names (D0, D1, ...) to string arrays
- `DS[0].PH[0].DM0` contains row entries; first entry has `S` (schema) mapping columns to dictionaries via `DN`
- Each row's `C` array holds values (indices into ValueDicts for string columns, literals otherwise)
- `R` bitmask: bit N set = column N repeats from previous row
- `Ø` (U+00D8) bitmask: bit N set = column N is null
- `RT` array: RestartTokens for pagination (pass to next request's `Window.RestartTokens`)

### Frontend (`index.html`)

Single HTML file, no build step. CDN dependencies:
- **Tabulator 6.x** — virtual DOM table with inline column header filters, sorting, pagination
- **Papa Parse 5.x** — CSV parsing in browser
- **Leaflet 1.9** + **MarkerCluster** — map with address-level markers, color-coded by mode of death
- **Chart.js 4.x** — trend visualizations (CDN path is `chart.umd.js`, not `chart.umd.min.js`)

Four tabs with hash routing (`#map`, `#table`, `#trends`, `#downloads`):
- **Map** (default): Leaflet + OpenStreetMap tiles. Markers positioned from `data/geocache.json` (address-level), falling back to zip code centroids with deterministic jitter. Filtered by year range and mode. "Near My Location" button uses browser geolocation, only zooms if within Milwaukee metro bounds.
- **Table**: Tabulator with header filters (dropdowns for categorical, text input for others). Global search syncs to URL hash as `#table?q=...`. Download button exports filtered view as CSV.
- **Trends**: Eight Chart.js charts (total deaths, homicides, suicides, drug-related, gunshot, under-18, infant, stacked modes by year). Excludes current partial year.
- **Downloads**: Lists all CSV files grouped by year with row counts, linked for direct download.

Loads `data/metadata.json` for the file manifest, fetches all CSVs in parallel, concatenates into one array, then initializes all views.

### Automation (`.github/workflows/update-data.yml`)

Weekly cron (Sundays 6:00 UTC) + manual `workflow_dispatch`. Runs `fetch_cases.py` → `geocode.py` → `postprocess.py`, commits to `data/` only if content changed.

### API details

- **Base URL**: `https://wabi-us-gov-iowa-api.analysis.usgovcloudapi.net`
- **Resource Key**: `af2bb42b-16c0-45a8-a235-807597246041`
- **Model ID**: `981063`
- **Query endpoint**: `POST /public/reports/querydata?synchronous=true`
- **Schema endpoint**: `GET /public/reports/{resourceKey}/conceptualschema`
- Required headers: `X-PowerBI-ResourceKey`, `Origin: https://app.powerbigov.us`
