#!/usr/bin/env python3
"""
Backfill-All Supabase ETL - Walk backwards until no more data.

Beginner overview:
- This script starts from yesterday (SGT) and goes one day back at a time.
- For each day, it fetches per-minute readings for every device and writes them
  to Supabase with upsert (so reruns are safe).
- It stops when it finds several consecutive empty days (configurable) or when
  it hits a maximum “years back” horizon.

Config via environment variables (with defaults):
- EMPTY_CHUNKS_TO_STOP: how many empty days in a row before stopping (default 2)
- BACKFILL_MAX_YEARS: how far back to go at most (default 5)
- SUPABASE_URL, SUPABASE_ANON_KEY, API_BASE_URL, SUPABASE_TABLE (like daily script)
"""

import os
import time
import logging
from datetime import datetime, timedelta, date
from dotenv import load_dotenv
from supabase import create_client

from supabase_common import API_DEFAULT, LOCATIONS, build_rows, upsert_rows, yesterday_sgt, SGT

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger("supabase-backfill-all")


def main():
    load_dotenv()

    api_base = os.getenv("API_BASE_URL", API_DEFAULT).rstrip("/")
    table = os.getenv("SUPABASE_TABLE", "meter_readings")

    empty_chunks_to_stop = max(1, int(os.getenv("EMPTY_CHUNKS_TO_STOP", "2")))
    max_years = max(1, int(os.getenv("BACKFILL_MAX_YEARS", "5")))

    supabase_url = os.environ["SUPABASE_URL"]
    supabase_key = os.environ["SUPABASE_ANON_KEY"]
    supabase = create_client(supabase_url, supabase_key)

    today_sgt = datetime.now(SGT).date()
    end = yesterday_sgt()
    stop_before = today_sgt - timedelta(days=365 * max_years)

    log.info(f"Starting backfill at {end}, stopping before {stop_before}, empty threshold={empty_chunks_to_stop}")

    empty_streak = 0
    total_affected = 0
    days_processed = 0

    cur = end
    while cur >= stop_before:
        days_processed += 1
        day_rows = []
        for loc in LOCATIONS:
            day_rows.extend(build_rows(api_base, loc, cur))
            time.sleep(0.05)

        affected = upsert_rows(supabase, table, day_rows)
        total_affected += affected
        if affected == 0:
            empty_streak += 1
            log.info(f"{cur}: no data (empty streak {empty_streak}/{empty_chunks_to_stop})")
        else:
            empty_streak = 0
            log.info(f"{cur}: upserted {affected}")

        if empty_streak >= empty_chunks_to_stop:
            log.info("Stopping due to consecutive empty days threshold reached.")
            break

        cur = cur - timedelta(days=1)

    log.info(f"Backfill complete. Days processed={days_processed}, total rows affected={total_affected}")


if __name__ == "__main__":
    main()
