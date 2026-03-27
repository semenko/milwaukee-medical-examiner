#!/usr/bin/env python3
"""
Fetch Milwaukee County Medical Examiner case data from the public Power BI dashboard.

Queries the Power BI REST API (anonymous public embed, no auth required) and outputs
case data split into monthly CSV files with a JSON manifest.

Data source: Milwaukee County Medical Examiner's Office
Dashboard: https://app.powerbigov.us/view?r=eyJrIjoiYWYyYmI0MmItMTZjMC00NWE4LWEyMzUtODA3NTk3MjQ2MDQxIiwidCI6ImFiMGMwMWY2LTE5ZTUtNGUyOS05ZGFiLTRkMDNmODJiNjQ5NSJ9
"""

import argparse
import csv
import io
import json
import os
import sys
import time
from datetime import datetime, timezone

import requests

# Power BI API configuration
API_BASE = "https://wabi-us-gov-iowa-api.analysis.usgovcloudapi.net"
RESOURCE_KEY = "af2bb42b-16c0-45a8-a235-807597246041"
DATASET_ID = "e8178118-70c0-44ad-8ba4-92ff0c456d69"
REPORT_ID = "af2bb42b-16c0-45a8-a235-807597246041"
MODEL_ID = 981063

HEADERS = {
    "X-PowerBI-ResourceKey": RESOURCE_KEY,
    "Content-Type": "application/json",
    "Origin": "https://app.powerbigov.us",
    "Referer": "https://app.powerbigov.us/",
}

QUERY_URL = f"{API_BASE}/public/reports/querydata?synchronous=true"

TABLE1_ENTITY = "vwPublicDataAccess"
TABLE2_ENTITY = "vwPublicDataAccess (2)"

# Columns to fetch from each table
TABLE1_COLUMNS = [
    "CaseNum_STR", "CaseNum", "DeathDate", "Age", "Gender", "Race",
    "Mode", "CauseA", "CauseOther", "CaseType", "Death Location",
    "DeathType", "DeathSubType", "EventDate",
]

TABLE2_COLUMNS = [
    "CaseNum_STR", "DeathAddr", "DeathCity", "DeathZip", "DeathState",
]

# Final output column order
OUTPUT_COLUMNS = [
    "CaseNum_STR", "CaseNum", "DeathDate", "EventDate", "Age", "Gender",
    "Race", "Mode", "CauseA", "CauseOther", "DeathType", "DeathSubType",
    "CaseType", "DeathAddr", "DeathCity", "DeathZip", "DeathState",
    "Death Location",
]

ARCHIVE_CUTOFF = "2020-01-01"
PAGE_SIZE = 30000


def build_query(entity, columns, count=PAGE_SIZE, restart_tokens=None):
    """Build a Power BI semantic query for the given entity and columns."""
    select = []
    projections = []
    for i, col in enumerate(columns):
        select.append({
            "Column": {
                "Expression": {"SourceRef": {"Source": "v"}},
                "Property": col,
            },
            "Name": f"{entity}.{col}",
        })
        projections.append(i)

    window = {"Count": count}
    if restart_tokens is not None:
        window["RestartTokens"] = restart_tokens

    return {
        "version": "1.0.0",
        "queries": [{
            "Query": {
                "Commands": [{
                    "SemanticQueryDataShapeCommand": {
                        "Query": {
                            "Version": 2,
                            "From": [{"Name": "v", "Entity": entity, "Type": 0}],
                            "Select": select,
                        },
                        "Binding": {
                            "Primary": {
                                "Groupings": [{"Projections": projections}]
                            },
                            "DataReduction": {
                                "DataVolume": 4,
                                "Primary": {"Window": window},
                            },
                            "Version": 1,
                        },
                        "ExecutionMetricsKind": 1,
                    }
                }]
            },
            "QueryId": "",
            "ApplicationContext": {
                "DatasetId": DATASET_ID,
                "Sources": [{"ReportId": REPORT_ID}],
            },
        }],
        "cancelQueries": [],
        "modelId": MODEL_ID,
    }


def parse_response(data, columns):
    """Parse Power BI's compressed dictionary response format into rows.

    Power BI responses use a compact encoding:
    - ValueDicts: maps dictionary names (D0, D1, ...) to string arrays
    - S (schema) in the first DM0 entry maps columns to dictionaries via DN
    - Each row's C array holds values (dict indices for strings, literals otherwise)
    - R bitmask: bit N set = column N repeats from previous row
    - Ø (U+00D8) bitmask: bit N set = column N is null
    """
    rows = []
    results = data.get("results", [])
    if not results:
        return rows, None

    result = results[0]
    ds_data = result.get("result", {}).get("data", {})
    dsr = ds_data.get("dsr", {})
    ds_list = dsr.get("DS", [])

    if not ds_list:
        return rows, None

    ds = ds_list[0]

    if "odata.error" in ds:
        err = ds["odata.error"]
        msg = err.get("message", {}).get("value", "Unknown error")
        print(f"  ERROR: {msg}", file=sys.stderr)
        return rows, None

    value_dicts = ds.get("ValueDicts", {})
    restart_tokens = ds.get("RT", None)

    ph = ds.get("PH", [])
    if not ph:
        return rows, restart_tokens

    primary_data = ph[0].get("DM0", [])
    if not primary_data:
        return rows, restart_tokens

    s_info = primary_data[0].get("S", [])
    prev_values = [None] * len(columns)

    for entry in primary_data:
        c_values = entry.get("C", [])
        r_bitmask = entry.get("R", 0)
        o_bitmask = entry.get("\u00d8", 0)

        current_values = []
        c_idx = 0

        for col_idx in range(len(columns)):
            bit = 1 << col_idx
            if r_bitmask & bit:
                current_values.append(prev_values[col_idx])
            elif o_bitmask & bit:
                current_values.append(None)
            else:
                if c_idx < len(c_values):
                    current_values.append(c_values[c_idx])
                    c_idx += 1
                else:
                    current_values.append(None)

        prev_values = current_values[:]

        # Resolve dictionary references
        resolved = []
        for col_idx, val in enumerate(current_values):
            if val is None:
                resolved.append("")
                continue
            if col_idx < len(s_info):
                dn = s_info[col_idx].get("DN")
                if dn and isinstance(val, int) and dn in value_dicts:
                    resolved.append(value_dicts[dn][val])
                    continue
            resolved.append(val)

        rows.append(dict(zip(columns, resolved)))

    return rows, restart_tokens


def fetch_all(entity, columns):
    """Fetch all rows from a table, handling pagination via RestartTokens."""
    all_rows = []
    restart_tokens = None
    page = 0

    while True:
        page += 1
        query = build_query(entity, columns, restart_tokens=restart_tokens)
        resp = requests.post(QUERY_URL, headers=HEADERS, json=query, timeout=60)
        resp.raise_for_status()

        rows, restart_tokens = parse_response(resp.json(), columns)
        all_rows.extend(rows)
        print(f"  Page {page}: {len(rows)} rows (total: {len(all_rows)})", file=sys.stderr)

        if restart_tokens is None or len(rows) == 0:
            break

        time.sleep(1)  # Be courteous to the API

    return all_rows


def convert_death_date(epoch_ms):
    """Convert milliseconds-since-epoch to YYYY-MM-DD string."""
    if not epoch_ms or not isinstance(epoch_ms, (int, float)):
        return ""
    try:
        return datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
    except (ValueError, OSError):
        return ""


def split_cases_by_period(cases):
    """Split cases into archive (pre-2020) and monthly buckets (2020+).

    Returns dict mapping relative file paths to lists of case rows.
    """
    buckets = {}

    for case in cases:
        dd = case.get("DeathDate", "")
        if not dd or dd < ARCHIVE_CUTOFF:
            key = "archive/pre-2020.csv"
        else:
            # Extract year and month from YYYY-MM-DD
            year = dd[:4]
            month = dd[5:7]
            key = f"{year}/{month}.csv"

        if key not in buckets:
            buckets[key] = []
        buckets[key].append(case)

    # Sort each bucket by CaseNum_STR for stable diffs
    for key in buckets:
        buckets[key].sort(key=lambda r: r.get("CaseNum_STR", ""))

    return buckets


def write_csv_if_changed(filepath, rows, columns):
    """Write a CSV file only if content differs from existing file.

    Returns True if file was written (new or changed), False if unchanged.
    """
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=columns, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)
    new_content = buf.getvalue()

    if os.path.exists(filepath):
        with open(filepath, "r") as f:
            existing_content = f.read()
        if existing_content == new_content:
            return False

    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w", newline="") as f:
        f.write(new_content)
    return True


def parse_age(age_str):
    """Parse age string like '55 Years' or '3 Months' into fractional years."""
    if not age_str:
        return None
    parts = age_str.split()
    if len(parts) < 2:
        return None
    try:
        num = int(parts[0])
    except ValueError:
        return None
    unit = parts[1].lower()
    if unit.startswith("year"):
        return num
    if unit.startswith("month"):
        return num / 12
    if unit.startswith("day") or unit.startswith("hour"):
        return 0
    return None


def compute_trends(cases):
    """Compute yearly aggregate statistics for the trends charts."""
    yearly = {}

    for row in cases:
        dd = row.get("DeathDate", "")
        if not dd or len(dd) < 4:
            continue
        year = dd[:4]
        if year < "2002" or year > "2099":
            continue

        if year not in yearly:
            yearly[year] = {
                "total": 0, "homicide": 0, "suicide": 0, "accident": 0,
                "natural": 0, "undetermined": 0, "drugs": 0, "guns": 0,
                "under18": 0, "infant": 0,
            }

        y = yearly[year]
        y["total"] += 1

        mode = row.get("Mode", "")
        if mode == "Homicide":
            y["homicide"] += 1
        elif mode == "Suicide":
            y["suicide"] += 1
        elif mode == "Accident":
            y["accident"] += 1
        elif mode == "Natural":
            y["natural"] += 1
        else:
            y["undetermined"] += 1

        dt = row.get("DeathType", "")
        if dt == "Drug Related":
            y["drugs"] += 1
        if dt == "Gunshot Injury":
            y["guns"] += 1

        age = parse_age(row.get("Age", ""))
        if age is not None:
            if age < 18:
                y["under18"] += 1
            if age < 1:
                y["infant"] += 1

    return yearly


def main():
    parser = argparse.ArgumentParser(
        description="Fetch Milwaukee County Medical Examiner case data from Power BI"
    )
    parser.add_argument(
        "--output-dir", default="data",
        help="Output directory for CSV files and metadata (default: data)"
    )
    parser.add_argument(
        "--full", action="store_true",
        help="Perform a full data refresh"
    )
    args = parser.parse_args()

    output_dir = args.output_dir

    # Fetch Table 1: demographics and case metadata
    print("Fetching Table 1 (demographics)...", file=sys.stderr)
    table1_rows = fetch_all(TABLE1_ENTITY, TABLE1_COLUMNS)
    print(f"  Total Table 1 rows: {len(table1_rows)}", file=sys.stderr)

    # Deduplicate Table 1 by CaseNum_STR (multiple rows can exist per case
    # with different EventDate values; merge by preferring non-empty fields)
    t1_map = {}
    for r in table1_rows:
        cn = r.get("CaseNum_STR", "")
        if not cn:
            continue
        if cn not in t1_map:
            t1_map[cn] = r
        else:
            # Merge: fill in blanks from this row
            existing = t1_map[cn]
            for col in TABLE1_COLUMNS:
                if not existing.get(col, "") and r.get(col, ""):
                    existing[col] = r[col]
    print(f"  Unique Table 1 cases: {len(t1_map)}", file=sys.stderr)

    # Fetch Table 2: address details
    print("\nFetching Table 2 (addresses)...", file=sys.stderr)
    table2_rows = fetch_all(TABLE2_ENTITY, TABLE2_COLUMNS)
    print(f"  Total Table 2 rows: {len(table2_rows)}", file=sys.stderr)

    # Deduplicate Table 2 by CaseNum_STR
    t2_map = {}
    for r in table2_rows:
        cn = r.get("CaseNum_STR", "")
        if cn and cn not in t2_map:
            t2_map[cn] = r
    print(f"  Unique Table 2 cases: {len(t2_map)}", file=sys.stderr)

    # Merge tables
    print("\nMerging tables...", file=sys.stderr)
    merged = []
    for cn, t1 in t1_map.items():
        t2 = t2_map.get(cn, {})

        row = {}
        for col in OUTPUT_COLUMNS:
            row[col] = t1.get(col, "") or t2.get(col, "")

        # Convert DeathDate from epoch to YYYY-MM-DD
        dd = row.get("DeathDate", "")
        if isinstance(dd, (int, float)):
            row["DeathDate"] = convert_death_date(dd)
        elif isinstance(dd, str) and len(dd) > 10:
            # Truncate ISO datetime strings (e.g., "2002-01-01T15:07:00") to date
            row["DeathDate"] = dd[:10]

        merged.append(row)

    print(f"  Total merged cases: {len(merged)}", file=sys.stderr)

    # Validation
    if len(merged) < 50000:
        print(f"  WARNING: Only {len(merged)} cases found (expected >50,000)", file=sys.stderr)

    # Split into files by period
    buckets = split_cases_by_period(merged)

    # Write CSV files
    print(f"\nWriting {len(buckets)} CSV files to {output_dir}/...", file=sys.stderr)
    files_written = 0
    files_unchanged = 0
    file_manifest = []

    for rel_path in sorted(buckets.keys()):
        filepath = os.path.join(output_dir, rel_path)
        changed = write_csv_if_changed(filepath, buckets[rel_path], OUTPUT_COLUMNS)
        if changed:
            files_written += 1
        else:
            files_unchanged += 1
        file_manifest.append(rel_path)

    print(f"  {files_written} files written, {files_unchanged} unchanged", file=sys.stderr)

    # Find newest death date
    all_dates = [r["DeathDate"] for r in merged if r.get("DeathDate")]
    newest_date = max(all_dates) if all_dates else ""

    # Row counts per file (for metadata)
    file_row_counts = {rel_path: len(rows) for rel_path, rows in buckets.items()}

    # Write metadata.json
    metadata = {
        "last_fetched": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "total_rows": len(merged),
        "newest_death_date": newest_date,
        "files": file_manifest,
        "row_counts": file_row_counts,
    }
    metadata_path = os.path.join(output_dir, "metadata.json")
    os.makedirs(output_dir, exist_ok=True)
    with open(metadata_path, "w") as f:
        json.dump(metadata, f, indent=2)
        f.write("\n")
    print(f"  metadata.json written", file=sys.stderr)

    # Write trends.json — pre-computed yearly aggregates for chart rendering
    trends = compute_trends(merged)
    trends_path = os.path.join(output_dir, "trends.json")
    with open(trends_path, "w") as f:
        json.dump(trends, f, separators=(",", ":"))
        f.write("\n")
    print(f"  trends.json written", file=sys.stderr)

    # Summary
    print(f"\n--- Summary ---", file=sys.stderr)
    print(f"Total cases: {len(merged)}", file=sys.stderr)
    print(f"Date range: {min(all_dates) if all_dates else 'N/A'} to {newest_date}", file=sys.stderr)
    print(f"Files: {len(file_manifest)}", file=sys.stderr)

    modes = {}
    for c in merged:
        mode = c.get("Mode", "") or "Unknown"
        modes[mode] = modes.get(mode, 0) + 1
    print(f"\nBy Mode of Death:", file=sys.stderr)
    for mode, count in sorted(modes.items(), key=lambda x: -x[1]):
        print(f"  {mode}: {count}", file=sys.stderr)

    # Row counts per file
    print(f"\nRows per file:", file=sys.stderr)
    for rel_path in sorted(buckets.keys()):
        print(f"  {rel_path}: {len(buckets[rel_path])}", file=sys.stderr)


if __name__ == "__main__":
    main()
