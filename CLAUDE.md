# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Public dataset and browsable web interface for Milwaukee County Medical Examiner case data, extracted from the office's Power BI government dashboard. The site is hosted on GitHub Pages.

## Commands

```bash
# Fetch all data from Power BI API (~70K cases, takes ~30s)
python3 fetch_cases.py --output-dir data --full

# Serve the site locally (then open http://localhost:8000)
python3 -m http.server
```

Dependencies: `pip install requests`

## Architecture

### Data pipeline (`fetch_cases.py`)

Queries two Power BI tables via the public REST API (no auth — anonymous embed):

- **Table 1** (`vwPublicDataAccess`): Demographics — CaseNum, DeathDate, Age, Gender, Race, Mode, CauseA, CaseType, etc.
- **Table 2** (`vwPublicDataAccess (2)`): Address detail — DeathAddr, DeathCity, DeathZip, DeathState

Both tables are fetched in full (paginated at 30K rows via RestartTokens), deduplicated by `CaseNum_STR`, merged, then split into files:

- `data/archive/pre-2020.csv` — all cases before 2020
- `data/{year}/{month}.csv` — monthly files from 2020 onward
- `data/metadata.json` — manifest listing all files, row count, timestamps

Files are sorted by CaseNum_STR for stable git diffs. Only files with actual content changes are overwritten.

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

Loads `data/metadata.json` for the file manifest, fetches all CSVs in parallel, concatenates into one array, renders with Tabulator.

### Automation (`.github/workflows/update-data.yml`)

Weekly cron (Sundays 6:00 UTC) + manual `workflow_dispatch`. Runs `fetch_cases.py`, commits to `data/` only if content changed.

### API details

- **Base URL**: `https://wabi-us-gov-iowa-api.analysis.usgovcloudapi.net`
- **Resource Key**: `af2bb42b-16c0-45a8-a235-807597246041`
- **Model ID**: `981063`
- **Query endpoint**: `POST /public/reports/querydata?synchronous=true`
- **Schema endpoint**: `GET /public/reports/{resourceKey}/conceptualschema`
- Required headers: `X-PowerBI-ResourceKey`, `Origin: https://app.powerbigov.us`
