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

logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s - %(levelname)s - %(message)s"
)
log = logging.getLogger("supabase-backfill-all")


def main():
    load_dotenv()
    
    # Configuration
    api_base = os.getenv("API_BASE_URL", API_DEFAULT).rstrip("/")
    table = os.getenv("SUPABASE_TABLE", "meter_readings")
    empty_chunks_to_stop = max(1, int(os.getenv("EMPTY_CHUNKS_TO_STOP", "7")))
    
    # Supabase client
    try:
        supabase_url = os.environ["SUPABASE_URL"]
        supabase_key = os.environ["SUPABASE_ANON_KEY"]
    except KeyError as e:
        log.error(f"Missing environment variable: {e}")
        log.error("Please set SUPABASE_URL and SUPABASE_ANON_KEY in your .env file")
        return
    
    supabase = create_client(supabase_url, supabase_key)
    
    # Date range: from yesterday back to May 1, 2025
    end_date = yesterday_sgt()
    start_date = date(2025, 5, 1)
    
    log.info("=" * 70)
    log.info("BACKFILL STARTING")
    log.info(f"Date range: {start_date} to {end_date}")
    log.info(f"API base: {api_base}")
    log.info(f"Table: {table}")
    log.info(f"Locations: {len(LOCATIONS)}")
    log.info(f"Empty day threshold: {empty_chunks_to_stop} consecutive days")
    log.info("=" * 70)
    
    empty_streak = 0
    total_affected = 0
    days_processed = 0
    days_with_data = 0
    
    current_date = end_date
    
    while current_date >= start_date:
        days_processed += 1
        day_rows = []
        
        log.info(f"\n[Day {days_processed}] Processing {current_date.strftime('%Y-%m-%d (%A)')}")
        
        # Fetch data for all locations for this day
        for loc in LOCATIONS:
            try:
                loc_rows = build_rows(api_base, loc, current_date)
                day_rows.extend(loc_rows)
                
                if loc_rows:
                    log.info(f"  ✓ {loc['Name'][:30]:30s} - {len(loc_rows):4d} readings")
                else:
                    log.debug(f"  - {loc['Name'][:30]:30s} - no data")
                
                # Small delay to avoid overwhelming the API
                time.sleep(0.05)
                
            except Exception as e:
                log.error(f"  ✗ {loc['Name'][:30]:30s} - Error: {e}")
                continue
        
        # Upsert the day's data
        try:
            affected = upsert_rows(supabase, table, day_rows)
            total_affected += affected
            
            if affected == 0:
                empty_streak += 1
                log.warning(f"  ⚠️  NO DATA - Empty streak: {empty_streak}/{empty_chunks_to_stop}")
            else:
                empty_streak = 0
                days_with_data += 1
                log.info(f"  ✅ SUCCESS - Upserted {affected} rows")
            
            # Stop if too many consecutive empty days
            if empty_streak >= empty_chunks_to_stop:
                log.warning(f"\n🛑 STOPPING - {empty_chunks_to_stop} consecutive empty days reached")
                break
                
        except Exception as e:
            log.error(f"  ✗ Database error: {e}")
        
        # Move to previous day
        current_date = current_date - timedelta(days=1)
        
        # Progress update every 10 days
        if days_processed % 10 == 0:
            log.info(f"\n📊 Progress Update:")
            log.info(f"   Days processed: {days_processed}")
            log.info(f"   Days with data: {days_with_data}")
            log.info(f"   Total rows: {total_affected:,}")
            log.info(f"   Success rate: {days_with_data/days_processed*100:.1f}%")
    
    # Final summary
    log.info("\n" + "=" * 70)
    log.info("✅ BACKFILL COMPLETE")
    log.info("=" * 70)
    log.info(f"Total days processed: {days_processed}")
    log.info(f"Days with data: {days_with_data} ({days_with_data/days_processed*100:.1f}%)")
    log.info(f"Total rows upserted: {total_affected:,}")
    log.info(f"Date range covered: {current_date + timedelta(days=1)} to {end_date}")
    log.info(f"Average rows per day: {total_affected/max(days_with_data,1):,.0f}")
    log.info("=" * 70)


if __name__ == "__main__":
    main()

