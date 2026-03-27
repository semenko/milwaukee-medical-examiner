#!/usr/bin/env python3
"""
Post-process case data CSVs to generate derived data files.

Reads all CSV files from data/, computes aggregate statistics,
and writes:
- data/trends.json — yearly aggregate counts for the Trends tab

Run after fetch_cases.py and geocode.py:
    python fetch_cases.py --output-dir data --full
    python geocode.py
    python postprocess.py
"""

import csv
import json
import os
import sys

DATA_DIR = "data"


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


def read_all_cases():
    """Read all case rows from CSV files in the data directory."""
    cases = []
    for root, dirs, files in os.walk(DATA_DIR):
        for filename in sorted(files):
            if not filename.endswith(".csv"):
                continue
            filepath = os.path.join(root, filename)
            with open(filepath) as f:
                for row in csv.DictReader(f):
                    cases.append(row)
    return cases


def compute_trends(cases):
    """Compute yearly aggregate statistics for the Trends charts."""
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
    print("Reading all case CSVs...", file=sys.stderr)
    cases = read_all_cases()
    print(f"  {len(cases)} total rows", file=sys.stderr)

    # Compute and write trends.json
    print("Computing trends...", file=sys.stderr)
    trends = compute_trends(cases)
    trends_path = os.path.join(DATA_DIR, "trends.json")
    with open(trends_path, "w") as f:
        json.dump(trends, f, separators=(",", ":"))
        f.write("\n")

    # Print summary
    years_sorted = sorted(trends.keys())
    print(f"  {len(trends)} years ({years_sorted[0]}-{years_sorted[-1]})", file=sys.stderr)
    print(f"  trends.json written ({os.path.getsize(trends_path)} bytes)", file=sys.stderr)


if __name__ == "__main__":
    main()
