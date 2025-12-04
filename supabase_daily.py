#!/usr/bin/env python3
"""
Daily Supabase ETL (yesterday SGT) – API → Supabase

Beginner overview:
- This script fetches data for "yesterday" in Singapore local time from the API
  for each device, converts the timestamps to UTC, and writes the results into
  your Supabase table.

Environment variables it uses:
- SUPABASE_URL: your Supabase project URL
- SUPABASE_ANON_KEY: your Supabase anon (or service) key
- API_BASE_URL: optional; base URL of the API (default is provided)
- SUPABASE_TABLE: optional; defaults to "meter_readings"
"""

import os
import time
import logging
from dotenv import load_dotenv
from supabase import create_client
from typing import List, Dict

from supabase_common import API_DEFAULT, LOCATIONS, build_rows, upsert_rows, yesterday_sgt

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger("supabase-daily")


def main():
    load_dotenv()

    api_base = os.getenv("API_BASE_URL", API_DEFAULT).rstrip("/")
    table = os.getenv("SUPABASE_TABLE", "meter_readings")

    supabase_url = os.environ["SUPABASE_URL"]
    supabase_key = os.environ["SUPABASE_ANON_KEY"]
    supabase = create_client(supabase_url, supabase_key)

    day = yesterday_sgt()
    day_rows: List[Dict[str, object]] = []
    for loc in LOCATIONS:
        day_rows.extend(build_rows(api_base, loc, day))
        time.sleep(0.05)

    affected = upsert_rows(supabase, table, day_rows)
    log.info(f"Inserted/updated {affected} rows for {day}")


if __name__ == "__main__":
    main()
