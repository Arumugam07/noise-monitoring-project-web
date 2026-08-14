#!/usr/bin/env python3
"""
Backfill Jan 2026 -> yesterday (SGT), rerunnable any time.

Every time this runs, it walks backwards from yesterday (SGT) down to
2026-01-01 and upserts. Safe to rerun on a schedule or manually - upsert
means already-filled days just get overwritten with the same data, no
duplicates.

Config via environment variables (with defaults):
- BACKFILL_START_DATE (default 2026-01-01)
- SUPABASE_URL, SUPABASE_ANON_KEY, SUPABASE_SERVICE_KEY, API_BASE_URL, SUPABASE_TABLE
"""

import os
import time
import logging
from datetime import timedelta, date
from dotenv import load_dotenv
from supabase import create_client
from supabase_common import API_DEFAULT, LOCATIONS, build_rows, upsert_rows, yesterday_sgt

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
log = logging.getLogger("supabase-backfill-2026")


def main():
    load_dotenv()

    api_base = os.getenv("API_BASE_URL", API_DEFAULT).rstrip("/")
    table = os.getenv("SUPABASE_TABLE", "meter_readings")

    try:
        supabase_url = os.environ["SUPABASE_URL"]
        supabase_key = os.getenv("SUPABASE_SERVICE_KEY") or os.environ["SUPABASE_ANON_KEY"]
    except KeyError as e:
        log.error(f"Missing environment variable: {e}")
        log.error("Please set SUPABASE_URL and SUPABASE_ANON_KEY in your .env file")
        return

    supabase = create_client(supabase_url, supabase_key)

    start_date = date.fromisoformat(os.getenv("BACKFILL_START_DATE", "2026-01-01"))
    end_date = yesterday_sgt()  # recalculated fresh every run

    log.info("=" * 70)
    log.info("BACKFILL (2026 -> YESTERDAY) STARTING")
    log.info(f"Date range: {start_date} to {end_date}")
    log.info(f"API base: {api_base}")
    log.info(f"Table: {table}")
    log.info(f"Locations: {len(LOCATIONS)}")
    log.info("=" * 70)

    total_affected = 0
    days_processed = 0
    days_with_data = 0
    days_empty = []

    current_date = end_date

    while current_date >= start_date:
        days_processed += 1
        day_rows = []

        log.info(f"\n[Day {days_processed}] Processing {current_date.strftime('%Y-%m-%d (%A)')}")

        for loc in LOCATIONS:
            try:
                loc_rows = build_rows(api_base, loc, current_date)
                day_rows.extend(loc_rows)

                if loc_rows:
                    log.info(f"  ✓ {loc['Name'][:30]:30s} - {len(loc_rows):4d} readings")
                else:
                    log.debug(f"  - {loc['Name'][:30]:30s} - no data")

                time.sleep(0.05)

            except Exception as e:
                log.error(f"  ✗ {loc['Name'][:30]:30s} - Error: {e}")
                continue

        try:
            affected = upsert_rows(supabase, table, day_rows)
            total_affected += affected

            if affected == 0:
                days_empty.append(current_date.isoformat())
                log.warning(f"  ⚠️  NO DATA for {current_date}")
            else:
                days_with_data += 1
                log.info(f"  ✅ SUCCESS - Upserted {affected} rows")

        except Exception as e:
            log.error(f"  ✗ Database error: {e}")

        current_date = current_date - timedelta(days=1)

        if days_processed % 10 == 0:
            log.info(f"\n📊 Progress Update:")
            log.info(f"   Days processed: {days_processed}")
            log.info(f"   Days with data: {days_with_data}")
            log.info(f"   Total rows: {total_affected:,}")
            log.info(f"   Success rate: {days_with_data/days_processed*100:.1f}%")

    log.info("\n" + "=" * 70)
    log.info("✅ BACKFILL COMPLETE")
    log.info("=" * 70)
    log.info(f"Total days processed: {days_processed}")
    log.info(f"Days with data: {days_with_data} ({days_with_data/days_processed*100:.1f}%)")
    log.info(f"Total rows upserted: {total_affected:,}")
    log.info(f"Date range covered: {start_date} to {end_date}")
    log.info(f"Average rows per day: {total_affected/max(days_with_data,1):,.0f}")
    if days_empty:
        log.warning(f"Empty days ({len(days_empty)}): {', '.join(days_empty)}")
    log.info("=" * 70)


if __name__ == "__main__":
    main()
