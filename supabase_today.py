#!/usr/bin/env python3
"""
Hourly Supabase ETL - fetches TODAY (SGT) for all locations.
Runs every hour via GitHub Actions to keep dashboard current.
"""
import os
import time
import logging
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client
from typing import List, Dict
from supabase_common import API_DEFAULT, LOCATIONS, build_rows, upsert_rows, SGT

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger("supabase-today")

def main():
    load_dotenv()

    api_base = os.getenv("API_BASE_URL", API_DEFAULT).rstrip("/")
    table = os.getenv("SUPABASE_TABLE", "meter_readings")

    try:
        supabase_url = os.environ["SUPABASE_URL"]
        supabase_key = os.getenv("SUPABASE_SERVICE_KEY") or os.environ["SUPABASE_ANON_KEY"]
    except KeyError as e:
        log.error(f"Missing environment variable: {e}")
        return

    supabase = create_client(supabase_url, supabase_key)
    today = datetime.now(SGT).date()

    log.info("=" * 70)
    log.info("HOURLY ETL STARTING")
    log.info(f"Fetching data for: {today}")
    log.info(f"Locations: {len(LOCATIONS)}")
    log.info("=" * 70)

    day_rows: List[Dict[str, object]] = []

    # Per-location fetch with error handling — same as backfill
    for loc in LOCATIONS:
        try:
            loc_rows = build_rows(api_base, loc, today)
            day_rows.extend(loc_rows)

            if loc_rows:
                log.info(f"  ✓ {loc['Name'][:30]:30s} - {len(loc_rows):4d} readings")
            else:
                log.warning(f"  ⚠ {loc['Name'][:30]:30s} - no data returned")

            time.sleep(0.05)

        except Exception as e:
            log.error(f"  ✗ {loc['Name'][:30]:30s} - Error: {e}")
            continue  # skip this location, keep going

    # Upsert all collected rows
    try:
        affected = upsert_rows(supabase, table, day_rows)
        log.info("=" * 70)
        log.info(f"✅ HOURLY ETL COMPLETE — Upserted {affected} rows for {today}")
        log.info("=" * 70)
    except Exception as e:
        log.error(f"✗ Database upsert failed: {e}")

if __name__ == "__main__":
    main()
