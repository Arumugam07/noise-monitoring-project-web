#!/usr/bin/env python3
import os
import time
import logging
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client
from supabase_common import API_DEFAULT, LOCATIONS, build_rows, upsert_rows, SGT

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger("supabase-today")

def main():
    load_dotenv()
    api_base = os.getenv("API_BASE_URL", API_DEFAULT).rstrip("/")
    table = os.getenv("SUPABASE_TABLE", "meter_readings")
    supabase_url = os.environ["SUPABASE_URL"]
    # Use service key to bypass RLS for writes; fall back to anon key
    supabase_key = os.getenv("SUPABASE_SERVICE_KEY") or os.environ["SUPABASE_ANON_KEY"]
    supabase = create_client(supabase_url, supabase_key)

    today = datetime.now(SGT).date()
    log.info(f"Fetching today's data: {today}")

    day_rows = []
    for loc in LOCATIONS:
        rows = build_rows(api_base, loc, today)
        day_rows.extend(rows)
        if rows:
            log.info(f"  ✓ {loc['Name'][:30]:30s} - {len(rows):4d} readings")
        time.sleep(0.05)

    affected = upsert_rows(supabase, table, day_rows)
    log.info(f"✅ Upserted {affected} rows for {today}")

if __name__ == "__main__":
    main()
