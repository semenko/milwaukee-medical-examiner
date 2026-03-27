# Milwaukee County Medical Examiner Case Data

Searchable, filterable public case records from the [Milwaukee County Medical Examiner's Office](https://county.milwaukee.gov/EN/Medical-Examiner).

**[Browse the data](https://nick.semenkovich.com/milwaukee-medical-examiner/)**

## About

The Milwaukee County Medical Examiner's Office publishes case data through a [Power BI dashboard](https://app.powerbigov.us/view?r=eyJrIjoiYWYyYmI0MmItMTZjMC00NWE4LWEyMzUtODA3NTk3MjQ2MDQxIiwidCI6ImFiMGMwMWY2LTE5ZTUtNGUyOS05ZGFiLTRkMDNmODJiNjQ5NSJ9). This project extracts that data into downloadable CSV files and provides a web interface for browsing and filtering.

Data is refreshed weekly via GitHub Actions.

## Data

Case records are split into CSV files by time period:

- `data/archive/pre-2020.csv` — All cases before January 2020
- `data/{year}/{month}.csv` — Monthly files from 2020 onward (e.g., `data/2025/03.csv`)
- `data/metadata.json` — File manifest with row counts and last-update timestamp

### Columns

| Column | Description |
|--------|-------------|
| CaseNum_STR | Case number (e.g., "25-08927") |
| CaseNum | Case number (alternate format) |
| DeathDate | Date of death (YYYY-MM-DD) |
| EventDate | Event/incident date |
| Age | Age at death (e.g., "55 Years") |
| Gender | Male, Female |
| Race | White, Black, Hispanic, Asian/Pacific Islander, etc. |
| Mode | Mode of death: Natural, Accident, Suicide, Homicide, Undetermined |
| CauseA | Primary cause of death |
| CauseOther | Contributing causes |
| DeathType | Category (Cardiovascular, Drug Related, Firearms, etc.) |
| DeathSubType | Sub-category |
| CaseType | Exam, Body Released, Case Waived, etc. |
| DeathAddr | Street address |
| DeathCity | City |
| DeathZip | ZIP code |
| DeathState | State |
| Death Location | Full concatenated address |

## Running locally

```bash
pip install requests
python fetch_cases.py --output-dir data --full
python3 -m http.server
# Open http://localhost:8000
```

## License

Code is MIT licensed. The case data is public government record from the Milwaukee County Medical Examiner's Office.
