#!/usr/bin/env python3
"""
Batch geocode addresses from the case data using the US Census Bureau Geocoder.

Reads all CSVs in data/, extracts unique addresses, geocodes via the Census
batch API (free, no API key), and writes results to data/geocache.json.

Only geocodes addresses not already in the cache (incremental updates).
"""

import csv
import io
import json
import os
import sys
import time

import requests

CENSUS_URL = "https://geocoding.geo.census.gov/geocoder/locations/addressbatch"
BATCH_SIZE = 9000  # Census API limit is 10,000; use 9,000 for safety
DATA_DIR = "data"
CACHE_FILE = os.path.join(DATA_DIR, "geocache.json")


def load_cache():
    """Load existing geocache, or return empty dict."""
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE) as f:
            return json.load(f)
    return {}


def save_cache(cache):
    """Write geocache to disk."""
    with open(CACHE_FILE, "w") as f:
        json.dump(cache, f, separators=(",", ":"))
        f.write("\n")


def collect_addresses():
    """Collect unique (address, city, state, zip) tuples from all CSV files."""
    addresses = {}  # key -> (street, city, state, zip)

    for root, dirs, files in os.walk(DATA_DIR):
        for filename in sorted(files):
            if not filename.endswith(".csv"):
                continue
            filepath = os.path.join(root, filename)
            with open(filepath) as f:
                for row in csv.DictReader(f):
                    street = (row.get("DeathAddr") or "").strip()
                    city = (row.get("DeathCity") or "").strip()
                    state = (row.get("DeathState") or "WI").strip()
                    zipcode = (row.get("DeathZip") or "").strip()

                    if not street or not city or not zipcode:
                        continue

                    # Normalize key for deduplication
                    key = f"{street}, {city}, {state} {zipcode}"
                    if key not in addresses:
                        addresses[key] = (street, city, state, zipcode)

    return addresses


def geocode_batch(rows):
    """Send a batch of addresses to Census geocoder. Returns dict of key -> [lat, lng]."""
    buf = io.StringIO()
    writer = csv.writer(buf)
    for uid, street, city, state, zipcode in rows:
        writer.writerow([uid, street, city, state, zipcode])

    resp = requests.post(
        CENSUS_URL,
        files={"addressFile": ("batch.csv", buf.getvalue(), "text/csv")},
        data={"benchmark": "Public_AR_Current"},
        timeout=120,
    )
    resp.raise_for_status()

    results = {}
    for line in resp.text.strip().split("\n"):
        if not line.strip():
            continue
        parts = line.split('","')
        parts = [p.strip('"') for p in parts]
        if len(parts) < 6:
            continue

        uid = parts[0]
        match_status = parts[2]
        coords_str = parts[5] if len(parts) > 5 else ""

        if match_status == "Match" and coords_str:
            try:
                lng, lat = coords_str.split(",")
                results[uid] = [round(float(lat), 6), round(float(lng), 6)]
            except (ValueError, IndexError):
                pass

    return results


def main():
    print("Loading existing geocache...", file=sys.stderr)
    cache = load_cache()
    print(f"  {len(cache)} cached addresses", file=sys.stderr)

    print("Collecting addresses from CSV files...", file=sys.stderr)
    addresses = collect_addresses()
    print(f"  {len(addresses)} unique addresses", file=sys.stderr)

    # Find addresses not yet geocoded
    to_geocode = {k: v for k, v in addresses.items() if k not in cache}
    print(f"  {len(to_geocode)} need geocoding", file=sys.stderr)

    if not to_geocode:
        print("Nothing to geocode.", file=sys.stderr)
        return

    # Batch geocode
    keys = list(to_geocode.keys())
    total_matched = 0
    total_processed = 0

    for i in range(0, len(keys), BATCH_SIZE):
        batch_keys = keys[i : i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        total_batches = (len(keys) + BATCH_SIZE - 1) // BATCH_SIZE
        print(
            f"\n  Batch {batch_num}/{total_batches} ({len(batch_keys)} addresses)...",
            file=sys.stderr,
        )

        rows = []
        uid_to_key = {}
        for j, key in enumerate(batch_keys):
            uid = str(i + j)
            street, city, state, zipcode = to_geocode[key]
            rows.append((uid, street, city, state, zipcode))
            uid_to_key[uid] = key

        try:
            results = geocode_batch(rows)
        except Exception as e:
            print(f"    ERROR: {e}", file=sys.stderr)
            time.sleep(5)
            continue

        matched = 0
        for uid, coords in results.items():
            key = uid_to_key.get(uid)
            if key:
                cache[key] = coords
                matched += 1

        # Mark unmatched as null so we don't retry them
        for uid, key in uid_to_key.items():
            if key not in cache:
                cache[key] = None

        total_matched += matched
        total_processed += len(batch_keys)
        print(
            f"    {matched}/{len(batch_keys)} matched "
            f"(running total: {total_matched}/{total_processed})",
            file=sys.stderr,
        )

        # Save after each batch in case of interruption
        save_cache(cache)

        if i + BATCH_SIZE < len(keys):
            time.sleep(2)  # Brief pause between batches

    print(f"\n--- Summary ---", file=sys.stderr)
    total_with_coords = sum(1 for v in cache.values() if v is not None)
    total_null = sum(1 for v in cache.values() if v is None)
    print(f"  Addresses with coordinates: {total_with_coords}", file=sys.stderr)
    print(f"  Addresses not matched: {total_null}", file=sys.stderr)
    print(f"  Cache file: {CACHE_FILE}", file=sys.stderr)

    # Print file size
    size = os.path.getsize(CACHE_FILE)
    print(f"  Cache file size: {size / 1024:.0f} KB", file=sys.stderr)


if __name__ == "__main__":
    main()
