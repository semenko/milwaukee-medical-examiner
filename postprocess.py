#!/usr/bin/env python3
"""
Post-process case data CSVs to generate derived data files.

Reads all CSV files from data/, computes aggregate statistics,
and writes:
- data/trends.json — yearly aggregate counts for the Trends tab
- data/stats.json  — per-city statistics for the Stats tab

Run after fetch_cases.py and geocode.py:
    python fetch_cases.py --output-dir data --full
    python geocode.py
    python postprocess.py
"""

import csv
import json
import math
import os
import sys
from collections import Counter
from datetime import datetime, timezone

DATA_DIR = "data"

# Merge map for DeathType values (source → canonical display name)
DEATH_TYPE_MERGE = {
    "Neoplastic": "Cancer",
    "Motorized Vehicle - Driver": "Motor Vehicle",
    "Motorized Vehicle - Passenger": "Motor Vehicle",
    "Motor Vehicle": "Motor Vehicle",
    "Fall": "Falls",
    "Firearms": "Gunshot Injury",
    "Burn": "Fire/Burn",
    "Fire Related Injury": "Fire/Burn",
    "Water Related Incident": "Drowning/Water",
    "Drowning (non-boat)": "Drowning/Water",
    "Boating": "Drowning/Water",
}

# City name normalization
CITY_ALIASES = {
    "Milw.": "Milwaukee", "MILW": "Milwaukee", "Milwuakee": "Milwaukee",
    "Miwaukee": "Milwaukee", "MILWAKEE": "Milwaukee", "MILWAUKE": "Milwaukee",
    "Miilw.": "Milwaukee", "Milkwaukee": "Milwaukee", "Bay View": "Milwaukee",
    "Wawatosa": "Wauwatosa", "Wauwtosa": "Wauwatosa",
    "Wawautosa": "Wauwatosa", "Wauwaotsa": "Wauwatosa",
    "St. Francis": "Saint Francis",
    "S. Milwaukee": "South Milwaukee",
    "Woods": "Shorewood",
}

CITY_MIN_CASES = 100  # minimum cases to get a city page

# Age bins for histogram
AGE_BINS = [
    ("<1", 0, 1), ("1-4", 1, 5), ("5-9", 5, 10), ("10-14", 10, 15),
    ("15-17", 15, 18), ("18-24", 18, 25), ("25-34", 25, 35),
    ("35-44", 35, 45), ("45-54", 45, 55), ("55-64", 55, 65),
    ("65-74", 65, 75), ("75-84", 75, 85), ("85+", 85, 200),
]

# All death type keys tracked in trends
DT_KEYS = [
    "dt_cardiovascular", "dt_drugs", "dt_falls", "dt_infectious",
    "dt_guns", "dt_cancer", "dt_motor_vehicle", "dt_pedestrian",
    "dt_asphyxia", "dt_alcohol", "dt_fire_burn", "dt_drowning",
    "dt_cns", "dt_blunt_force", "dt_respiratory", "dt_endocrine",
    "dt_sharp_force", "dt_env_exposure",
]

DT_LABELS = {
    "dt_cardiovascular": "Cardiovascular", "dt_drugs": "Drug Related",
    "dt_falls": "Falls", "dt_infectious": "Infectious",
    "dt_guns": "Gunshot", "dt_cancer": "Cancer",
    "dt_motor_vehicle": "Motor Vehicle (Occupant)", "dt_pedestrian": "Pedestrian",
    "dt_asphyxia": "Asphyxia", "dt_alcohol": "Alcohol Related",
    "dt_fire_burn": "Fire/Burn", "dt_drowning": "Drowning/Water",
    "dt_cns": "Central Nervous System", "dt_blunt_force": "Blunt Force Trauma",
    "dt_respiratory": "Respiratory", "dt_endocrine": "Endocrine",
    "dt_sharp_force": "Sharp Force Injury", "dt_env_exposure": "Environmental Exposure",
}

# Categories for trend CI analysis
TREND_CATEGORIES = (
    # (key, start_year) — mode-based trends from 2002, type-based from 2014
    [("homicide", 2002), ("suicide", 2002), ("under18", 2002), ("infant", 2002)]
    + [(k, 2014) for k in DT_KEYS]
)


def merged_death_type(raw):
    return DEATH_TYPE_MERGE.get(raw, raw)


def normalize_city(raw):
    return CITY_ALIASES.get(raw, raw)


def parse_age(age_str):
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


def classify_row(row):
    """Return a dict of classification flags for a single row."""
    raw_dt = row.get("DeathType", "")
    dt = merged_death_type(raw_dt)
    mode = row.get("Mode", "")
    age = parse_age(row.get("Age", ""))

    c = {
        "mode": mode,
        "dt": dt,
        "raw_dt": raw_dt,
        "age": age,
        "homicide": mode == "Homicide",
        "suicide": mode == "Suicide",
        "accident": mode == "Accident",
        "natural": mode == "Natural",
        "under18": age is not None and age < 18,
        "infant": age is not None and age < 1,
        "dt_cardiovascular": dt == "Cardiovascular",
        "dt_drugs": dt == "Drug Related",
        "dt_falls": dt == "Falls",
        "dt_infectious": dt == "Infectious",
        "dt_guns": raw_dt in ("Gunshot Injury", "Firearms"),
        "dt_cancer": dt == "Cancer",
        "dt_motor_vehicle": dt == "Motor Vehicle",
        "dt_pedestrian": raw_dt == "Motorized Vehicle - Pedestrian",
        "dt_asphyxia": dt == "Asphyxia",
        "dt_alcohol": dt == "Alcohol Related",
        "dt_fire_burn": dt == "Fire/Burn",
        "dt_drowning": dt == "Drowning/Water",
        "dt_cns": dt == "Central Nervous System",
        "dt_blunt_force": dt == "Blunt Force Trauma",
        "dt_respiratory": dt == "Respiratory",
        "dt_endocrine": dt == "Endocrine",
        "dt_sharp_force": dt == "Sharp Force Injury",
        "dt_env_exposure": dt == "Environmental Exposure",
    }
    return c


def median(values):
    s = sorted(values)
    n = len(s)
    if n == 0:
        return 0
    if n % 2 == 1:
        return s[n // 2]
    return (s[n // 2 - 1] + s[n // 2]) / 2


def quartiles(values):
    """Return (q1, median, q3) using inclusive median method."""
    s = sorted(values)
    n = len(s)
    if n == 0:
        return (0, 0, 0)
    med = median(s)
    lower = s[:n // 2]
    upper = s[(n + 1) // 2:]
    return (median(lower) if lower else med, med, median(upper) if upper else med)


def box_plot_stats(values, labels=None):
    """Compute box plot five-number summary + outliers with year labels."""
    if not values:
        return None
    s = sorted(values)
    q1, med, q3 = quartiles(values)
    iqr = q3 - q1
    fence_lo = q1 - 1.5 * iqr
    fence_hi = q3 + 1.5 * iqr

    outliers = []
    if labels:
        for val, lbl in zip(values, labels):
            if val < fence_lo or val > fence_hi:
                outliers.append({"year": lbl, "value": val})

    # Whiskers extend to the most extreme non-outlier
    non_outlier = [v for v in s if fence_lo <= v <= fence_hi]
    whisker_lo = min(non_outlier) if non_outlier else s[0]
    whisker_hi = max(non_outlier) if non_outlier else s[-1]

    return {
        "min": whisker_lo, "q1": q1, "median": med, "q3": q3, "max": whisker_hi,
        "outliers": outliers,
    }


# t-distribution critical values for 95% two-tailed CI
T_TABLE = {
    1: 12.706, 2: 4.303, 3: 3.182, 4: 2.776, 5: 2.571,
    6: 2.447, 7: 2.365, 8: 2.306, 9: 2.262, 10: 2.228,
    11: 2.201, 12: 2.179, 13: 2.160, 14: 2.145, 15: 2.131,
    16: 2.120, 17: 2.110, 18: 2.101, 19: 2.093, 20: 2.086,
    21: 2.080, 22: 2.074, 23: 2.069, 24: 2.064, 25: 2.060,
}


def linear_regression_with_ci(values):
    """OLS trend line with 95% confidence interval for the mean response."""
    n = len(values)
    if n < 3:
        return values, values, values

    x = list(range(n))
    y = values
    x_mean = sum(x) / n
    y_mean = sum(y) / n

    ss_xx = sum((xi - x_mean) ** 2 for xi in x)
    ss_xy = sum((xi - x_mean) * (yi - y_mean) for xi, yi in zip(x, y))

    if ss_xx == 0:
        return values, values, values

    slope = ss_xy / ss_xx
    intercept = y_mean - slope * x_mean
    y_hat = [intercept + slope * xi for xi in x]

    ss_res = sum((yi - yhi) ** 2 for yi, yhi in zip(y, y_hat))
    se = math.sqrt(ss_res / (n - 2)) if n > 2 else 0

    df = n - 2
    t_val = T_TABLE.get(df, 1.96)

    ci_lo, ci_hi = [], []
    for xi in x:
        se_pred = se * math.sqrt(1 / n + (xi - x_mean) ** 2 / ss_xx) if ss_xx else 0
        ci_lo.append(round(y_hat[x.index(xi)] - t_val * se_pred, 1))
        ci_hi.append(round(y_hat[x.index(xi)] + t_val * se_pred, 1))

    return [round(v, 1) for v in y_hat], ci_lo, ci_hi


def compute_city_stats(rows_with_classes, all_years):
    """Compute all stats for a group of (row, classification) tuples."""
    # Yearly counts
    yearly = {}
    for row, cls in rows_with_classes:
        dd = row.get("DeathDate", "")
        if not dd or len(dd) < 4:
            continue
        year = int(dd[:4])
        if year < 2002 or year > 2099:
            continue
        if year not in yearly:
            yearly[year] = Counter()
        yearly[year]["total"] += 1
        for key in ["homicide", "suicide", "accident", "natural"] + DT_KEYS + ["under18", "infant"]:
            if cls.get(key):
                yearly[year][key] += 1
        # Track undetermined
        if cls["mode"] not in ("Homicide", "Suicide", "Accident", "Natural") and cls["mode"]:
            yearly[year]["undetermined"] += 1

    total_cases = len(rows_with_classes)
    year_totals = [yearly.get(y, Counter())["total"] for y in all_years]
    non_zero_totals = [t for t in year_totals if t > 0]

    # Summary
    mode_counts = Counter()
    dt_counts = Counter()
    for _, cls in rows_with_classes:
        if cls["mode"]:
            mode_counts[cls["mode"]] += 1
        if cls["dt"]:
            dt_counts[cls["dt"]] += 1

    top_mode = mode_counts.most_common(1)
    top_dt = dt_counts.most_common(1)

    summary = {
        "total_cases": total_cases,
        "year_range": [min(all_years), max(all_years)] if all_years else [0, 0],
        "median_per_year": round(median(non_zero_totals)) if non_zero_totals else 0,
        "mean_per_year": round(sum(non_zero_totals) / len(non_zero_totals)) if non_zero_totals else 0,
        "top_mode": {"name": top_mode[0][0], "count": top_mode[0][1],
                     "pct": round(top_mode[0][1] / total_cases * 100, 1)} if top_mode else None,
        "top_death_type": {"name": top_dt[0][0], "count": top_dt[0][1],
                           "pct": round(top_dt[0][1] / sum(dt_counts.values()) * 100, 1)} if top_dt and sum(dt_counts.values()) > 0 else None,
    }

    # Yearly counts arrays
    yearly_counts = {"years": all_years}
    for key in ["total", "homicide", "suicide", "accident", "natural", "undetermined"] + DT_KEYS + ["under18", "infant"]:
        yearly_counts[key] = [yearly.get(y, Counter()).get(key, 0) for y in all_years]

    # Box plots (for mode-based categories, use full year range)
    bp = {}
    for key in ["total", "homicide", "suicide", "accident"]:
        vals = yearly_counts[key]
        bp[key] = box_plot_stats(vals, all_years)

    # Trends with CI
    trends_ci = {}
    for key, start_year in TREND_CATEGORIES:
        start_idx = all_years.index(start_year) if start_year in all_years else 0
        vals = yearly_counts[key][start_idx:]
        trend_years = all_years[start_idx:]

        # Skip if median < 1 (suppress for small counts)
        med = median(vals) if vals else 0
        if med < 1:
            continue

        trend_line, ci_lo, ci_hi = linear_regression_with_ci(vals)
        trends_ci[key] = {
            "years": trend_years,
            "values": vals,
            "trend_line": trend_line,
            "ci_lower": ci_lo,
            "ci_upper": ci_hi,
            "label": DT_LABELS.get(key, key.replace("_", " ").title()),
        }

    # Mode distribution
    mode_labels = ["Natural", "Accident", "Homicide", "Suicide", "Undetermined"]
    mode_vals = [mode_counts.get(m, 0) for m in mode_labels]
    mode_total = sum(mode_vals) or 1
    mode_dist = {
        "labels": mode_labels,
        "values": mode_vals,
        "percentages": [round(v / mode_total * 100, 1) for v in mode_vals],
    }

    # Demographics
    age_bins_labels = [b[0] for b in AGE_BINS]
    age_hist = [0] * len(AGE_BINS)
    age_by_mode = {m: [0] * len(AGE_BINS) for m in mode_labels}
    race_counts = Counter()
    gender_counts = Counter()

    for row, cls in rows_with_classes:
        age = cls["age"]
        if age is not None:
            for i, (_, lo, hi) in enumerate(AGE_BINS):
                if lo <= age < hi:
                    age_hist[i] += 1
                    mode = cls["mode"] if cls["mode"] in mode_labels else "Undetermined"
                    age_by_mode[mode][i] += 1
                    break

        race = row.get("Race", "").strip()
        if race:
            race_counts[race] += 1
        gender = row.get("Gender", "").strip()
        if gender:
            gender_counts[gender] += 1

    race_sorted = race_counts.most_common()
    gender_sorted = gender_counts.most_common()

    demographics = {
        "age_histogram": {
            "bins": age_bins_labels,
            "counts": age_hist,
            "by_mode": age_by_mode,
        },
        "race": {
            "labels": [r[0] for r in race_sorted],
            "values": [r[1] for r in race_sorted],
        },
        "gender": {
            "labels": [g[0] for g in gender_sorted],
            "values": [g[1] for g in gender_sorted],
        },
    }

    # Vehicular detail (2014+)
    type_years = [y for y in all_years if y >= 2014]
    vehicular = {
        "years": type_years,
        "occupant": [yearly.get(y, Counter()).get("dt_motor_vehicle", 0) for y in type_years],
        "pedestrian": [yearly.get(y, Counter()).get("dt_pedestrian", 0) for y in type_years],
    }

    return {
        "summary": summary,
        "yearly_counts": yearly_counts,
        "box_plot": bp,
        "trends_with_ci": trends_ci,
        "mode_distribution": mode_dist,
        "demographics": demographics,
        "vehicular": vehicular,
        "small_n_warning": total_cases < 1000,
    }


def compute_stats(cases):
    """Compute per-city stats for the Stats tab."""
    # Classify every row once
    classified = []
    city_groups = {}  # city -> list of (row, cls) tuples

    for row in cases:
        cls = classify_row(row)
        classified.append((row, cls))

        city = normalize_city(row.get("DeathCity", "").strip())
        if city:
            if city not in city_groups:
                city_groups[city] = []
            city_groups[city].append((row, cls))

    # Determine year range (excluding current partial year)
    current_year = datetime.now(timezone.utc).year
    all_years = list(range(2002, current_year))

    # Build city list (threshold by case count)
    city_list = ["All Milwaukee County"]
    for city, rows in sorted(city_groups.items(), key=lambda x: -len(x[1])):
        if len(rows) >= CITY_MIN_CASES:
            city_list.append(city)

    by_city = {}

    # All Milwaukee County
    print("  Computing: All Milwaukee County...", file=sys.stderr)
    by_city["All Milwaukee County"] = compute_city_stats(classified, all_years)

    # Individual cities
    for city in city_list[1:]:
        print(f"  Computing: {city} ({len(city_groups[city])} cases)...", file=sys.stderr)
        by_city[city] = compute_city_stats(city_groups[city], all_years)

    return {
        "generated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "cities": city_list,
        "by_city": by_city,
    }


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
                "dt_cardiovascular": 0, "dt_drugs": 0, "dt_falls": 0,
                "dt_infectious": 0, "dt_guns": 0, "dt_cancer": 0,
                "dt_motor_vehicle": 0, "dt_pedestrian": 0, "dt_asphyxia": 0,
                "dt_alcohol": 0, "dt_fire_burn": 0, "dt_drowning": 0,
            }

        y = yearly[year]
        y["total"] += 1
        raw_dt = row.get("DeathType", "")
        dt = merged_death_type(raw_dt)
        mode = row.get("Mode", "")

        if mode == "Homicide": y["homicide"] += 1
        elif mode == "Suicide": y["suicide"] += 1
        elif mode == "Accident": y["accident"] += 1
        elif mode == "Natural": y["natural"] += 1
        else: y["undetermined"] += 1

        if dt == "Drug Related": y["drugs"] += 1; y["dt_drugs"] += 1
        if raw_dt in ("Gunshot Injury", "Firearms"): y["guns"] += 1; y["dt_guns"] += 1
        if dt == "Cardiovascular": y["dt_cardiovascular"] += 1
        if dt == "Falls": y["dt_falls"] += 1
        if dt == "Infectious": y["dt_infectious"] += 1
        if dt == "Cancer": y["dt_cancer"] += 1
        if dt == "Motor Vehicle": y["dt_motor_vehicle"] += 1
        if raw_dt == "Motorized Vehicle - Pedestrian": y["dt_pedestrian"] += 1
        if dt == "Asphyxia": y["dt_asphyxia"] += 1
        if dt == "Alcohol Related": y["dt_alcohol"] += 1
        if dt == "Fire/Burn": y["dt_fire_burn"] += 1
        if dt == "Drowning/Water": y["dt_drowning"] += 1

        age = parse_age(row.get("Age", ""))
        if age is not None:
            if age < 18: y["under18"] += 1
            if age < 1: y["infant"] += 1

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
    years_sorted = sorted(trends.keys())
    print(f"  {len(trends)} years ({years_sorted[0]}-{years_sorted[-1]})", file=sys.stderr)
    print(f"  trends.json written ({os.path.getsize(trends_path)} bytes)", file=sys.stderr)

    # Compute and write stats.json
    print("\nComputing stats...", file=sys.stderr)
    stats = compute_stats(cases)
    stats_path = os.path.join(DATA_DIR, "stats.json")
    with open(stats_path, "w") as f:
        json.dump(stats, f, separators=(",", ":"))
        f.write("\n")
    print(f"  {len(stats['cities'])} cities", file=sys.stderr)
    print(f"  stats.json written ({os.path.getsize(stats_path) // 1024} KB)", file=sys.stderr)


if __name__ == "__main__":
    main()
