#!/usr/bin/env python3
"""
Targeted Gap Backfill — fills a specific date range only.

Usage:
  python backfill_gap.py                          # auto-detects gap from MV max date to yesterday
  python backfill_gap.py --start 2026-06-03 --end 2026-06-14   # explicit range
  python backfill_gap.py --check-only             # just print what dates are missing, no fetch

Why this exists vs supabase_backfill_all.py:
  The full backfill walks backwards from yesterday to 2025-05-01 and takes hours.
  This script only fills the gap — much faster for routine fixes.
"""

import os
import sys
import time
import logging
import argparse
from datetime import datetime, timedelta, date
from dotenv import load_dotenv
from database import create_client
from supabase_common import API_DEFAULT, LOCATIONS, build_rows, upsert_rows, SGT, yesterday_sgt

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
log = logging.getLogger("backfill-gap")


def get_mv_max_date(supabase) -> date | None:
    """Find the most recent date that actually has data in wide_view_mv."""
    try:
        resp = (
            supabase.table("wide_view_mv")
            .select("Date")
            .order("Date", desc=True)
            .limit(1)
            .execute()
        )
        if resp.data:
            return datetime.strptime(resp.data[0]["Date"], "%Y-%m-%d").date()
    except Exception as e:
        log.error(f"Could not fetch MV max date: {e}")
    return None


def get_missing_dates(supabase, start_date: date, end_date: date) -> list[date]:
    """
    Check which dates in the range have NO data at all in meter_readings.
    Returns list of dates that need backfilling.
    """
    missing = []
    current = start_date
    while current <= end_date:
        start_str = f"{current.isoformat()}T00:00:00+08:00"
        end_str   = f"{(current + timedelta(days=1)).isoformat()}T00:00:00+08:00"
        try:
            resp = (
                supabase.table("meter_readings")
                .select("reading_datetime", count="exact")
                .gte("reading_datetime", start_str)
                .lt("reading_datetime", end_str)
                .limit(1)
                .execute()
            )
            count = resp.count or 0
            if count == 0:
                missing.append(current)
                log.info(f"  ❌ {current} — NO DATA")
            else:
                log.info(f"  ✅ {current} — {count:,} readings found")
        except Exception as e:
            log.warning(f"  ⚠️  {current} — query error: {e}")
            missing.append(current)  # assume missing if query fails
        current += timedelta(days=1)
    return missing


def run_gap_backfill(api_base: str, supabase, table: str,
                     dates_to_fill: list[date]) -> int:
    """Fetch and upsert data for each missing date. Returns total rows inserted."""
    failures = []
    total_affected = 0
    total_dates    = len(dates_to_fill)

    for i, day in enumerate(dates_to_fill, 1):
        log.info(f"\n[{i}/{total_dates}] Backfilling {day.strftime('%Y-%m-%d (%A)')}")
        day_rows = []

        for loc in LOCATIONS:
            try:
                loc_rows = build_rows(api_base, loc, day)
                day_rows.extend(loc_rows)
                if loc_rows:
                    log.info(f"  ✓ {loc['Name'][:30]:30s} — {len(loc_rows):4d} readings")
                else:
                    log.warning(f"  ⚠ {loc['Name'][:30]:30s} — no data returned")
                time.sleep(0.05)
            except Exception as e:
                log.error(f"  ✗ {loc['Name'][:30]:30s} — Error: {e}")
                failures.append(str(e))

        try:
            affected = upsert_rows(supabase, table, day_rows)
            total_affected += affected
            log.info(f"  ✅ Upserted {affected:,} rows for {day}")
        except Exception as e:
            log.error(f"  ✗ Database upsert failed for {day}: {e}")
            failures.append(str(e))

    if failures:
        raise RuntimeError(f"Gap backfill had {len(failures)} fetch/write failures")
    return total_affected


def main():
    load_dotenv()

    parser = argparse.ArgumentParser(description="Backfill a specific date gap")
    parser.add_argument("--start", help="Start date YYYY-MM-DD (default: auto-detect from MV)")
    parser.add_argument("--end",   help="End date YYYY-MM-DD (default: yesterday)")
    parser.add_argument("--check-only", action="store_true",
                        help="Only print missing dates, do not fetch data")
    parser.add_argument("--force", action="store_true",
                        help="Re-fetch every requested day, including partially populated days")
    args = parser.parse_args()

    api_base = os.getenv("API_BASE_URL", API_DEFAULT).rstrip("/")
    table    = os.getenv("DATABASE_TABLE", "meter_readings")

    try:
        supabase_url = os.environ["AIVEN_DATABASE_URL"]
        supabase_key = os.environ["AIVEN_CA_CERT"]
    except KeyError as e:
        log.error(f"Missing environment variable: {e}")
        sys.exit(1)

    supabase  = create_client(supabase_url, supabase_key)
    yesterday = yesterday_sgt()

    # Determine date range
    if args.end:
        end_date = date.fromisoformat(args.end)
    else:
        end_date = yesterday

    if args.start:
        start_date = date.fromisoformat(args.start)
    else:
        mv_max = get_mv_max_date(supabase)
        if mv_max is None:
            log.error("Could not auto-detect gap start. Use --start YYYY-MM-DD")
            sys.exit(1)
        start_date = mv_max  # include the last MV date in case it's partial
        log.info(f"Auto-detected gap start from MV max date: {mv_max}")

    if start_date > end_date:
        log.info(f"✅ No gap: start {start_date} is after end {end_date}. Nothing to do.")
        sys.exit(0)

    total_days = (end_date - start_date).days + 1

    log.info("=" * 70)
    log.info("GAP BACKFILL")
    log.info(f"Checking range: {start_date} → {end_date} ({total_days} days)")
    log.info(f"API base: {api_base}")
    log.info("=" * 70)

    log.info("\n🔍 Checking which dates need backfilling...")
    missing_dates = (
        [start_date + timedelta(days=i) for i in range(total_days)]
        if args.force else get_missing_dates(supabase, start_date, end_date)
    )

    log.info(f"\n📋 Summary: {len(missing_dates)}/{total_days} dates need backfilling")

    if not missing_dates:
        log.info("No completely empty dates found. Partial days may still have gaps; use --force to re-fetch them.")
        sys.exit(0)

    log.info("Missing dates:")
    for d in missing_dates:
        log.info(f"  • {d}")

    if args.check_only:
        log.info("\n--check-only mode: exiting without fetching data.")
        sys.exit(0)

    log.info(f"\n🚀 Starting backfill for {len(missing_dates)} dates...")
    total_rows = run_gap_backfill(api_base, supabase, table, missing_dates)

    log.info("\n" + "=" * 70)
    log.info("✅ GAP BACKFILL COMPLETE")
    log.info(f"Dates filled:  {len(missing_dates)}")
    log.info(f"Total rows:    {total_rows:,}")
    log.info(f"Range covered: {start_date} → {end_date}")
    log.info("=" * 70)
    log.info("\n⚠️  Remember to refresh the materialized view:")
    log.info("   Run: python refresh_mv.py")
    log.info("   Or:  REFRESH MATERIALIZED VIEW public.wide_view_mv;")


if __name__ == "__main__":
    main()

